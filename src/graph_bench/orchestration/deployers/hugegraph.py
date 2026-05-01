# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""Apache HugeGraph cluster deployer (Cassandra-backed).

Topology mirrors the JanusGraph deployer: per `sut`-role node, one Cassandra
container plus one HugeGraph server container that points at the local
Cassandra node. Tier-2 systems share the storage cluster (a single Cassandra
ring), but the JanusGraph and HugeGraph keyspaces are distinct, so they can
be brought up sequentially without data interference.
"""

from __future__ import annotations

from graph_bench.orchestration.inventory import Inventory
from graph_bench.orchestration.plan import DeploymentPlan, HealthCheck, ServiceSpec


_CASSANDRA_IMAGE = "cassandra:4.1"
_HUGEGRAPH_IMAGE = "hugegraph/hugegraph:1.3.0"


def _cassandra_seeds(inventory: Inventory) -> str:
    return ",".join(n.hostname for n in inventory.sut_nodes[:1])


def _cassandra_service(host: str, seeds: str) -> ServiceSpec:
    return ServiceSpec(
        name="cassandra",
        container_name=f"gb-{host}-cassandra",
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


def _hugegraph_service(host: str, cassandra_seed: str) -> ServiceSpec:
    return ServiceSpec(
        name="hugegraph",
        container_name=f"gb-{host}-hugegraph",
        image=_HUGEGRAPH_IMAGE,
        env={
            "BACKEND": "cassandra",
            "HOST": host,
            "STORE": "hugegraph",
            "AUTH": "false",
            "GRAPH_NAME": "hugegraph",
            "CASSANDRA_HOST": cassandra_seed,
            "CASSANDRA_PORT": "9042",
        },
        volumes=(
            (f"./gb-data/{host}/hugegraph", "/hugegraph-server/hugegraph"),
        ),
        healthcheck=HealthCheck(
            test=["CMD", "curl", "-f", f"http://{host}:8080/apis/version"],
            interval_s=15,
            timeout_s=10,
            retries=10,
            start_period_s=180,
        ),
    )


class HugeGraphDeployer:
    sut_name = "hugegraph"

    def plan_initial(self, inventory: Inventory) -> DeploymentPlan:
        seeds = _cassandra_seeds(inventory)
        services_per_node: dict[str, list[ServiceSpec]] = {}
        for node in inventory.sut_nodes:
            host = node.hostname
            services_per_node[host] = [
                _cassandra_service(host, seeds),
                _hugegraph_service(host, host),
            ]
        return DeploymentPlan(sut=self.sut_name, services_per_node=services_per_node)

    def plan_scaleout(self, inventory: Inventory, target: str) -> tuple[str, ServiceSpec]:
        node = inventory.by_name(target)
        return node.hostname, _cassandra_service(node.hostname, _cassandra_seeds(inventory))
