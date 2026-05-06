# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""JanusGraph cluster deployer (Cassandra-backed).

Topology (per `sut`-role node, three SUT nodes total):
- 1 Cassandra node (storage backend, all three nodes form one Cassandra ring)
- 1 JanusGraph server (Gremlin endpoint on port 8182)

Cassandra seeds are the first SUT node's hostname; subsequent Cassandra nodes
auto-join. JanusGraph servers attach to the local Cassandra node.

Tier 2 in this study: scenario S1 only. Scale-out is supported by joining a
new Cassandra node on the reserve host (`nodetool` performs the actual join
once the daemon is running).

Co-located nodes (single-host playground): Cassandra needs unique
listen+broadcast addresses for gossip, which a single localhost cannot
provide for multiple instances without IP aliasing. To keep the playground
runnable, the deployer emits services for at most one logical node per
unique hostname — the first sut node on that hostname. In production, where
each logical node lives on a different host, this collapses to the standard
3-node ring.
"""

from __future__ import annotations

from graph_bench.orchestration.inventory import Inventory, NodeInfo
from graph_bench.orchestration.plan import DeploymentPlan, HealthCheck, ServiceSpec


_CASSANDRA_IMAGE = "cassandra:4.1"
_JANUSGRAPH_IMAGE = "janusgraph/janusgraph:1.0.0"


def _unique_host_sut_nodes(inventory: Inventory) -> list[NodeInfo]:
    """Return one sut node per distinct hostname (first-seen wins).

    Avoids spawning multiple Cassandra rings on the same physical host in the
    single-host playground.
    """
    seen: set[str] = set()
    picked: list[NodeInfo] = []
    for node in inventory.sut_nodes:
        if node.hostname in seen:
            continue
        seen.add(node.hostname)
        picked.append(node)
    return picked


def _cassandra_seeds(active_nodes: list[NodeInfo]) -> str:
    return active_nodes[0].hostname if active_nodes else ""


def _cassandra_service(node: NodeInfo, seeds: str, cluster_name: str = "graph-bench") -> ServiceSpec:
    host = node.hostname
    return ServiceSpec(
        name="cassandra",
        container_name=f"gb-{node.name}-cassandra",
        image=_CASSANDRA_IMAGE,
        env={
            "CASSANDRA_CLUSTER_NAME": cluster_name,
            "CASSANDRA_SEEDS": seeds,
            "CASSANDRA_LISTEN_ADDRESS": host,
            "CASSANDRA_BROADCAST_ADDRESS": host,
            "CASSANDRA_RPC_ADDRESS": host,
            "CASSANDRA_ENDPOINT_SNITCH": "GossipingPropertyFileSnitch",
            "CASSANDRA_DC": "DC1",
            "CASSANDRA_RACK": "RAC1",
            # Bound heap so three Cassandras + JanusGraph can co-exist on a
            # modest host. Override on production hardware.
            "MAX_HEAP_SIZE": "1G",
            "HEAP_NEWSIZE": "256M",
        },
        volumes=(
            (f"./gb-data/{node.name}/cassandra", "/var/lib/cassandra"),
        ),
        healthcheck=HealthCheck(
            test=["CMD-SHELL", "nodetool status | grep -q 'UN' || exit 1"],
            interval_s=15,
            timeout_s=10,
            retries=10,
            start_period_s=120,
        ),
    )


def _janusgraph_service(node: NodeInfo, cassandra_seed: str) -> ServiceSpec:
    # JanusGraph's official image (`/usr/local/bin/docker-entrypoint.sh`):
    #  - selects `conf/janusgraph-${JANUS_PROPS_TEMPLATE}-server.properties` and
    #    copies it as the active properties file
    #  - rewrites lines matching `janusgraph.X.Y` env vars into that properties
    #    file (so the keys below are NOT compose env vars in spirit, they're
    #    config overrides translated by the entrypoint)
    #  - if `JANUS_STORAGE_TIMEOUT` is set, polls the backend with `gremlin.sh`
    #    until it answers (avoids racing Cassandra startup)
    host = node.hostname
    return ServiceSpec(
        name="janusgraph",
        container_name=f"gb-{node.name}-janusgraph",
        image=_JANUSGRAPH_IMAGE,
        depends_on={"cassandra": "service_healthy"},
        env={
            "JANUS_PROPS_TEMPLATE": "cql",
            "JANUS_STORAGE_TIMEOUT": "300",
            "janusgraph.storage.backend": "cql",
            "janusgraph.storage.hostname": cassandra_seed,
            "janusgraph.storage.cql.keyspace": "janusgraph",
            # Cassandra 4 CQL driver requires an explicit local-datacenter;
            # we set DC1 to match the cassandra service env (CASSANDRA_DC=DC1).
            "janusgraph.storage.cql.local-datacenter": "DC1",
            "janusgraph.cache.db-cache": "true",
        },
        volumes=(
            (f"./gb-data/{node.name}/janusgraph-data", "/var/lib/janusgraph"),
        ),
        # Gremlin server speaks WebSocket on 8182, not plain HTTP, so a
        # `curl /` healthcheck always fails. We do a bash /dev/tcp port-open
        # check — bash is present in this image.
        healthcheck=HealthCheck(
            test=["CMD", "bash", "-c", "exec 3<>/dev/tcp/localhost/8182"],
            interval_s=15,
            timeout_s=10,
            retries=20,
            start_period_s=180,
        ),
    )


class JanusGraphDeployer:
    sut_name = "janusgraph"

    def plan_initial(self, inventory: Inventory) -> DeploymentPlan:
        active = _unique_host_sut_nodes(inventory)
        seeds = _cassandra_seeds(active)
        services_per_node: dict[str, list[ServiceSpec]] = {}
        for node in active:
            services_per_node[node.name] = [
                _cassandra_service(node, seeds),
                _janusgraph_service(node, node.hostname),
            ]
        return DeploymentPlan(sut=self.sut_name, services_per_node=services_per_node)

    def plan_scaleout(self, inventory: Inventory, target: str) -> tuple[str, ServiceSpec]:
        node = inventory.by_name(target)
        active = _unique_host_sut_nodes(inventory)
        seeds = _cassandra_seeds(active)
        return node.name, _cassandra_service(node, seeds)

    def scale_out_endpoint(self, inventory: Inventory, target: str) -> str:
        # Cassandra gossip auto-discovers — `add_node` here is a no-op shim
        # that just confirms the new ring member; pointing the driver at the
        # CQL port is the right "endpoint" semantically.
        node = inventory.by_name(target)
        return f"{node.hostname}:9042"
