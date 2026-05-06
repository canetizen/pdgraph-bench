# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""Apache HugeGraph cluster deployer (Cassandra-backed).

Topology mirrors the JanusGraph deployer: per `sut`-role node, one Cassandra
container plus one HugeGraph server container that points at the local
Cassandra node. Tier-2 systems share the storage cluster (a single Cassandra
ring), but the JanusGraph and HugeGraph keyspaces are distinct, so they can
be brought up sequentially without data interference.

Co-located nodes: same single-instance-per-hostname rule as the JanusGraph
deployer (Cassandra cannot share a host with another Cassandra without IP
aliasing).
"""

from __future__ import annotations

from graph_bench.orchestration.inventory import Inventory, NodeInfo
from graph_bench.orchestration.plan import DeploymentPlan, HealthCheck, ServiceSpec


_CASSANDRA_IMAGE = "cassandra:4.1"
_HUGEGRAPH_IMAGE = "hugegraph/hugegraph:1.3.0"


def _unique_host_sut_nodes(inventory: Inventory) -> list[NodeInfo]:
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


def _cassandra_service(node: NodeInfo, seeds: str) -> ServiceSpec:
    host = node.hostname
    return ServiceSpec(
        name="cassandra",
        container_name=f"gb-{node.name}-cassandra",
        image=_CASSANDRA_IMAGE,
        env={
            "CASSANDRA_CLUSTER_NAME": "graph-bench",
            "CASSANDRA_SEEDS": seeds,
            "CASSANDRA_LISTEN_ADDRESS": host,
            "CASSANDRA_BROADCAST_ADDRESS": host,
            "CASSANDRA_RPC_ADDRESS": host,
            "CASSANDRA_ENDPOINT_SNITCH": "GossipingPropertyFileSnitch",
            "CASSANDRA_DC": "DC1",
            "CASSANDRA_RACK": "RAC1",
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


def _hugegraph_service(node: NodeInfo, cassandra_seed: str) -> ServiceSpec:
    # The official image's entrypoint reads conf/graphs/hugegraph.properties
    # at startup; the default backend is RocksDB and there are no env vars to
    # switch backends. We patch the file in-place before invoking the original
    # entrypoint chain (`/usr/bin/dumb-init -- ./docker-entrypoint.sh`).
    host = node.hostname
    patch_cmd = (
        "set -e; cd /hugegraph-server; "
        # Drop the init flag so init-store.sh runs every fresh bring-up; if a
        # previous attempt failed mid-way the flag would otherwise short-circuit
        # subsequent runs and leave the graph keyspace half-initialised.
        "rm -f docker/init_complete; "
        "sed -ri "
        "'s/^backend\\s*=.*/backend=cassandra/; "
        " s/^serializer\\s*=.*/serializer=cassandra/' "
        "conf/graphs/hugegraph.properties; "
        f"grep -q '^cassandra.host' conf/graphs/hugegraph.properties || "
        f"echo 'cassandra.host={cassandra_seed}' >> conf/graphs/hugegraph.properties; "
        "grep -q '^cassandra.port' conf/graphs/hugegraph.properties || "
        "echo 'cassandra.port=9042' >> conf/graphs/hugegraph.properties; "
        "exec /usr/bin/dumb-init -- ./docker-entrypoint.sh"
    )
    return ServiceSpec(
        name="hugegraph",
        container_name=f"gb-{node.name}-hugegraph",
        image=_HUGEGRAPH_IMAGE,
        entrypoint=("sh", "-c"),
        command=(patch_cmd,),
        depends_on={"cassandra": "service_healthy"},
        # Root inside the container, so volume permissions don't fight us.
        volumes=(
            (f"./gb-data/{node.name}/hugegraph", "/hugegraph-server/data"),
        ),
        # HugeGraph 1.3.0 exposes graphs at `/graphs` (NOT `/apis/graphs`) —
        # /apis returns the API map but the graph instances live one level up.
        healthcheck=HealthCheck(
            test=["CMD-SHELL", f"curl -sf http://{host}:8080/graphs > /dev/null || exit 1"],
            interval_s=15,
            timeout_s=10,
            retries=20,
            start_period_s=180,
        ),
    )


class HugeGraphDeployer:
    sut_name = "hugegraph"

    def plan_initial(self, inventory: Inventory) -> DeploymentPlan:
        active = _unique_host_sut_nodes(inventory)
        seeds = _cassandra_seeds(active)
        services_per_node: dict[str, list[ServiceSpec]] = {}
        for node in active:
            services_per_node[node.name] = [
                _cassandra_service(node, seeds),
                _hugegraph_service(node, node.hostname),
            ]
        return DeploymentPlan(sut=self.sut_name, services_per_node=services_per_node)

    def plan_scaleout(self, inventory: Inventory, target: str) -> tuple[str, ServiceSpec]:
        node = inventory.by_name(target)
        active = _unique_host_sut_nodes(inventory)
        seeds = _cassandra_seeds(active)
        return node.name, _cassandra_service(node, seeds)

    def scale_out_endpoint(self, inventory: Inventory, target: str) -> str:
        node = inventory.by_name(target)
        return f"{node.hostname}:9042"
