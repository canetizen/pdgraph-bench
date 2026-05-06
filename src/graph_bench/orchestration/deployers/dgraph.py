# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""Dgraph cluster deployer.

Topology (per `sut`-role node):
- 1 `dgraph zero` (Raft consensus + cluster coordination)
- 1 `dgraph alpha` (data + query)

The first SUT node's zero is the seed; all subsequent zeros and alphas are
configured to point at it for membership.

Co-located nodes (single-host playground): zero and alpha both honour
`--port_offset N`, which shifts every internal port by N. Each co-located
logical node gets `i*10` so the entire `(zero, alpha)` pair stays free of
its peers.
"""

from __future__ import annotations

from graph_bench.orchestration.inventory import Inventory, NodeInfo
from graph_bench.orchestration.plan import DeploymentPlan, HealthCheck, ServiceSpec


_IMAGE = "dgraph/dgraph:v24.0.5"

_ZERO_GRPC_BASE = 5080
_ZERO_HTTP_BASE = 6080
_ALPHA_GRPC_INT_BASE = 7080
_ALPHA_HTTP_BASE = 8080
_OFFSET_STEP = 10


def _port_offsets(inventory: Inventory) -> dict[str, int]:
    counts: dict[str, int] = {}
    offsets: dict[str, int] = {}
    for node in inventory.nodes:
        if not (node.is_sut or node.is_reserve):
            continue
        idx = counts.get(node.hostname, 0)
        offsets[node.name] = idx * _OFFSET_STEP
        counts[node.hostname] = idx + 1
    return offsets


def _seed_zero(inventory: Inventory, offsets: dict[str, int]) -> str:
    seed_node = inventory.sut_nodes[0]
    return f"{seed_node.hostname}:{_ZERO_GRPC_BASE + offsets[seed_node.name]}"


def _zero_service(node: NodeInfo, my_idx: int, seed: str, offset: int) -> ServiceSpec:
    host = node.hostname
    port = _ZERO_GRPC_BASE + offset
    http_port = _ZERO_HTTP_BASE + offset
    # Dgraph v24 moved per-node identity into the `--raft` config blob;
    # `--idx N` from earlier versions is no longer recognised.
    args = [
        "dgraph", "zero",
        f"--my={host}:{port}",
        f"--raft=idx={my_idx + 1}",
        "--replicas=3",
        "--bindall",
        f"--port_offset={offset}",
        f"--peer={seed}" if my_idx > 0 else "",
    ]
    args = [a for a in args if a]
    return ServiceSpec(
        name="dgraph-zero",
        container_name=f"gb-{node.name}-zero",
        image=_IMAGE,
        command=tuple(args),
        volumes=(
            (f"./gb-data/{node.name}/zero", "/dgraph"),
        ),
        healthcheck=HealthCheck(
            test=["CMD", "curl", "-f", f"http://{host}:{http_port}/health"],
        ),
    )


def _alpha_service(node: NodeInfo, seed: str, offset: int) -> ServiceSpec:
    host = node.hostname
    grpc_int = _ALPHA_GRPC_INT_BASE + offset
    http_port = _ALPHA_HTTP_BASE + offset
    args = [
        "dgraph", "alpha",
        f"--my={host}:{grpc_int}",
        f"--zero={seed}",
        "--security", "whitelist=0.0.0.0/0",
        "--bindall",
        f"--port_offset={offset}",
    ]
    return ServiceSpec(
        name="dgraph-alpha",
        container_name=f"gb-{node.name}-alpha",
        image=_IMAGE,
        command=tuple(args),
        volumes=(
            (f"./gb-data/{node.name}/alpha", "/dgraph"),
        ),
        healthcheck=HealthCheck(
            test=["CMD", "curl", "-f", f"http://{host}:{http_port}/health"],
        ),
    )


class DgraphDeployer:
    sut_name = "dgraph"

    def plan_initial(self, inventory: Inventory) -> DeploymentPlan:
        offsets = _port_offsets(inventory)
        seed = _seed_zero(inventory, offsets)
        services_per_node: dict[str, list[ServiceSpec]] = {}
        for idx, node in enumerate(inventory.sut_nodes):
            offset = offsets[node.name]
            services_per_node[node.name] = [
                _zero_service(node, idx, seed, offset),
                _alpha_service(node, seed, offset),
            ]
        return DeploymentPlan(sut=self.sut_name, services_per_node=services_per_node)

    def plan_scaleout(self, inventory: Inventory, target: str) -> tuple[str, ServiceSpec]:
        node = inventory.by_name(target)
        offsets = _port_offsets(inventory)
        seed = _seed_zero(inventory, offsets)
        return node.name, _alpha_service(node, seed, offsets[node.name])

    def scale_out_endpoint(self, inventory: Inventory, target: str) -> str:
        # Dgraph alphas register themselves with zero on startup; the
        # cluster-visible endpoint is the alpha's internal-grpc address.
        node = inventory.by_name(target)
        offsets = _port_offsets(inventory)
        return f"{node.hostname}:{_ALPHA_GRPC_INT_BASE + offsets[node.name]}"
