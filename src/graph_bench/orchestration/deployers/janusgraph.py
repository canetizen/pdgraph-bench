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
"""

from __future__ import annotations

from graph_bench.orchestration.inventory import Inventory
from graph_bench.orchestration.plan import DeploymentPlan, HealthCheck, ServiceSpec


_CASSANDRA_IMAGE = "cassandra:4.1"
_JANUSGRAPH_IMAGE = "janusgraph/janusgraph:1.0.0"


def _cassandra_seeds(inventory: Inventory) -> str:
    return ",".join(n.hostname for n in inventory.sut_nodes[:1])


def _cassandra_service(host: str, seeds: str, cluster_name: str = "graph-bench") -> ServiceSpec:
    return ServiceSpec(
        name="cassandra",
        container_name=f"gb-{host}-cassandra",
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
            (f"./gb-data/{host}/cassandra", "/var/lib/cassandra"),
        ),
        healthcheck=HealthCheck(
            test=["CMD-SHELL", "nodetool status | grep -q 'UN' || exit 1"],
            interval_s=15,
            timeout_s=10,
            retries=10,
            start_period_s=120,
        ),
    )


def _janusgraph_service(host: str, cassandra_seed: str) -> ServiceSpec:
    # JanusGraph reads connection settings from environment via its
    # `janusgraph.properties` template (the official image generates one when
    # `JANUS_PROPS_TEMPLATE` selects `cassandra-cql-es` or similar).
    return ServiceSpec(
        name="janusgraph",
        container_name=f"gb-{host}-janusgraph",
        image=_JANUSGRAPH_IMAGE,
        env={
            "janusgraph.storage.backend": "cql",
            "janusgraph.storage.hostname": cassandra_seed,
            "janusgraph.storage.cql.keyspace": "janusgraph",
            "janusgraph.cache.db-cache": "true",
        },
        volumes=(
            (f"./gb-data/{host}/janusgraph-data", "/var/lib/janusgraph"),
        ),
        healthcheck=HealthCheck(
            test=["CMD", "curl", "-f", f"http://{host}:8182/"],
            interval_s=15,
            timeout_s=10,
            retries=10,
            start_period_s=180,
        ),
    )


class JanusGraphDeployer:
    sut_name = "janusgraph"

    def plan_initial(self, inventory: Inventory) -> DeploymentPlan:
        seeds = _cassandra_seeds(inventory)
        services_per_node: dict[str, list[ServiceSpec]] = {}
        for node in inventory.sut_nodes:
            host = node.hostname
            services_per_node[host] = [
                _cassandra_service(host, seeds),
                _janusgraph_service(host, host),
            ]
        return DeploymentPlan(sut=self.sut_name, services_per_node=services_per_node)

    def plan_scaleout(self, inventory: Inventory, target: str) -> tuple[str, ServiceSpec]:
        node = inventory.by_name(target)
        return node.hostname, _cassandra_service(node.hostname, _cassandra_seeds(inventory))
