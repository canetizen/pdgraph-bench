# Created by: Mustafa Can Caliskan
# Date: 2026-05-08

"""Memgraph deployer — single-instance per sut node.

Memgraph 2.18+ supports a HA mode (coordinator + data instances) but the
single-host playground does not benefit from it; we keep one Memgraph
process per logical sut node, mirroring the Tier-1 layout. The driver
connects to the first sut node's Bolt port; the other instances are warm
spares (and become true HA replicas under a multi-host inventory + an
operator-provided REGISTER INSTANCE step in production).

Co-located nodes (single-host playground): Bolt + monitoring ports get
per-node offsets so they don't collide.
"""

from __future__ import annotations

from graph_bench.orchestration.inventory import Inventory, NodeInfo
from graph_bench.orchestration.plan import DeploymentPlan, HealthCheck, ServiceSpec


_IMAGE = "memgraph/memgraph:2.18.0"

_BOLT_BASE = 7687
_HTTP_BASE = 7444
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


def _memgraph_service(node: NodeInfo, offset: int) -> ServiceSpec:
    host = node.hostname
    bolt_port = _BOLT_BASE + offset
    http_port = _HTTP_BASE + offset
    return ServiceSpec(
        name="memgraph",
        container_name=f"gb-{node.name}-memgraph",
        image=_IMAGE,
        # The official image runs as `memgraph` user and writes to /var/lib/memgraph.
        # `--also-log-to-stderr` keeps logs visible via `docker logs`.
        command=(
            f"--bolt-port={bolt_port}",
            f"--monitoring-port={http_port}",
            "--log-level=WARNING",
            "--also-log-to-stderr=true",
            "--storage-properties-on-edges=true",
            "--query-execution-timeout-sec=300",
        ),
        volumes=(
            (f"./gb-data/{node.name}/memgraph", "/var/lib/memgraph"),
            (f"./gb-data/{node.name}/memgraph-log", "/var/log/memgraph"),
        ),
        # Memgraph image's default user (uid 101) can't write to host-side
        # bind-mounted dirs that are owned by the host operator. Run as root.
        user="0:0",
        # Memgraph image lacks curl/nc; bash /dev/tcp probes the Bolt port
        # without needing extra binaries.
        healthcheck=HealthCheck(
            test=["CMD", "bash", "-c", f"exec 3<>/dev/tcp/localhost/{bolt_port}"],
            interval_s=10,
            timeout_s=5,
            retries=12,
            start_period_s=30,
        ),
    )


class MemgraphDeployer:
    sut_name = "memgraph"

    def plan_initial(self, inventory: Inventory) -> DeploymentPlan:
        offsets = _port_offsets(inventory)
        services_per_node: dict[str, list[ServiceSpec]] = {}
        for node in inventory.sut_nodes:
            services_per_node[node.name] = [_memgraph_service(node, offsets[node.name])]
        return DeploymentPlan(sut=self.sut_name, services_per_node=services_per_node)

    def plan_scaleout(self, inventory: Inventory, target: str) -> tuple[str, ServiceSpec]:
        # Memgraph S5 scale-out is out of scope for Tier-2 (S1 only); spinning
        # the reserve up still works if an operator triggers it.
        node = inventory.by_name(target)
        offsets = _port_offsets(inventory)
        return node.name, _memgraph_service(node, offsets[node.name])

    def scale_out_endpoint(self, inventory: Inventory, target: str) -> str:
        node = inventory.by_name(target)
        offsets = _port_offsets(inventory)
        return f"{node.hostname}:{_BOLT_BASE + offsets[node.name]}"
