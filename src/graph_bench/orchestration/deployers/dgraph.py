# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""Dgraph cluster deployer.

Topology (per `sut`-role node):
- 1 `dgraph zero` (Raft consensus + cluster coordination)
- 1 `dgraph alpha` (data + query)

The first SUT node's zero is the seed; all subsequent zeros and alphas are
configured to point at it for membership.
"""

from __future__ import annotations

from graph_bench.orchestration.inventory import Inventory
from graph_bench.orchestration.plan import DeploymentPlan, HealthCheck, ServiceSpec


_IMAGE = "dgraph/dgraph:v24.0.5"


def _seed_zero(inventory: Inventory) -> str:
    return f"{inventory.sut_nodes[0].hostname}:5080"


def _zero_service(host: str, my_idx: int, seed: str) -> ServiceSpec:
    args = [
        "dgraph", "zero",
        f"--my={host}:5080",
        f"--idx={my_idx + 1}",
        "--replicas=3",
        "--bindall",
        f"--peer={seed}" if my_idx > 0 else "",
    ]
    args = [a for a in args if a]
    return ServiceSpec(
        name="dgraph-zero",
        container_name=f"gb-{host}-zero",
        image=_IMAGE,
        command=tuple(args),
        volumes=(
            (f"./gb-data/{host}/zero", "/dgraph"),
        ),
        healthcheck=HealthCheck(
            test=["CMD", "curl", "-f", f"http://{host}:6080/health"],
        ),
    )


def _alpha_service(host: str, seed: str) -> ServiceSpec:
    args = [
        "dgraph", "alpha",
        f"--my={host}:7080",
        f"--zero={seed}",
        "--security", "whitelist=0.0.0.0/0",
        "--bindall",
    ]
    return ServiceSpec(
        name="dgraph-alpha",
        container_name=f"gb-{host}-alpha",
        image=_IMAGE,
        command=tuple(args),
        volumes=(
            (f"./gb-data/{host}/alpha", "/dgraph"),
        ),
        healthcheck=HealthCheck(
            test=["CMD", "curl", "-f", f"http://{host}:8080/health"],
        ),
    )


class DgraphDeployer:
    sut_name = "dgraph"

    def plan_initial(self, inventory: Inventory) -> DeploymentPlan:
        seed = _seed_zero(inventory)
        services_per_node: dict[str, list[ServiceSpec]] = {}
        for idx, node in enumerate(inventory.sut_nodes):
            host = node.hostname
            services_per_node[host] = [
                _zero_service(host, idx, seed),
                _alpha_service(host, seed),
            ]
        return DeploymentPlan(sut=self.sut_name, services_per_node=services_per_node)

    def plan_scaleout(self, inventory: Inventory, target: str) -> tuple[str, ServiceSpec]:
        node = inventory.by_name(target)
        return node.hostname, _alpha_service(node.hostname, _seed_zero(inventory))
