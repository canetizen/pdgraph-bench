# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""NebulaGraph deployer — multi-host topology + scale-out.

Topology:
- Each `sut`-role node hosts: 1 metad, 1 storaged, 1 graphd. The metad hosts
  jointly form the metadata Raft group; the storaged set forms the storage
  layer; graphd instances are stateless query frontends.
- Each `reserve`-role node hosts a stopped storaged that the harness will
  start during scenario S5 (then `ADD HOSTS` registers it).

`network_mode: host` is used so that `meta_server_addrs=node2:9559,...`
resolves on every container via the host's /etc/hosts.

Co-located nodes (single-host playground): when two or more logical nodes
in the inventory share a hostname, each gets a port offset of `i*10` (where
`i` is its index among co-located peers). NebulaGraph reserves
`service_port + 1` for Raft, so a step of 10 leaves room for the Raft port
without it colliding with the next instance's service port.
"""

from __future__ import annotations

from graph_bench.orchestration.inventory import Inventory, NodeInfo
from graph_bench.orchestration.plan import DeploymentPlan, HealthCheck, ServiceSpec


_IMAGE_METAD = "vesoft/nebula-metad:v3.8.0"
_IMAGE_STORAGED = "vesoft/nebula-storaged:v3.8.0"
_IMAGE_GRAPHD = "vesoft/nebula-graphd:v3.8.0"

_METAD_BASE = 9559
_METAD_HTTP_BASE = 19559
_STORAGED_BASE = 9779
_STORAGED_HTTP_BASE = 19779
_GRAPHD_BASE = 9669
_GRAPHD_HTTP_BASE = 19669
_OFFSET_STEP = 10


def _port_offsets(inventory: Inventory) -> dict[str, int]:
    """Map node.name → port offset based on its position among co-located peers.

    Counts both sut and reserve nodes since reserve must not collide with sut
    when they share a host (playground).
    """
    counts: dict[str, int] = {}
    offsets: dict[str, int] = {}
    for node in inventory.nodes:
        if not (node.is_sut or node.is_reserve):
            continue
        idx = counts.get(node.hostname, 0)
        offsets[node.name] = idx * _OFFSET_STEP
        counts[node.hostname] = idx + 1
    return offsets


def _meta_addrs(inventory: Inventory, offsets: dict[str, int]) -> str:
    return ",".join(
        f"{n.hostname}:{_METAD_BASE + offsets[n.name]}" for n in inventory.sut_nodes
    )


def _metad_service(node: NodeInfo, meta_addrs: str, offset: int) -> ServiceSpec:
    host = node.hostname
    port = _METAD_BASE + offset
    http_port = _METAD_HTTP_BASE + offset
    return ServiceSpec(
        name="nebula-metad",
        container_name=f"gb-{node.name}-metad",
        image=_IMAGE_METAD,
        command=(
            f"--meta_server_addrs={meta_addrs}",
            f"--local_ip={host}",
            f"--ws_ip={host}",
            f"--port={port}",
            f"--ws_http_port={http_port}",
            "--data_path=/data/meta",
            "--log_dir=/logs",
            "--v=0",
            "--minloglevel=0",
        ),
        volumes=(
            (f"./gb-data/{node.name}/metad", "/data/meta"),
            (f"./gb-data/{node.name}/metad-logs", "/logs"),
        ),
        healthcheck=HealthCheck(
            test=["CMD", "curl", "-f", f"http://{host}:{http_port}/status"],
        ),
    )


def _storaged_service(node: NodeInfo, meta_addrs: str, offset: int) -> ServiceSpec:
    host = node.hostname
    port = _STORAGED_BASE + offset
    http_port = _STORAGED_HTTP_BASE + offset
    return ServiceSpec(
        name="nebula-storaged",
        container_name=f"gb-{node.name}-storaged",
        image=_IMAGE_STORAGED,
        command=(
            f"--meta_server_addrs={meta_addrs}",
            f"--local_ip={host}",
            f"--ws_ip={host}",
            f"--port={port}",
            f"--ws_http_port={http_port}",
            "--data_path=/data/storage",
            "--log_dir=/logs",
            "--v=0",
            "--minloglevel=0",
        ),
        volumes=(
            (f"./gb-data/{node.name}/storaged", "/data/storage"),
            (f"./gb-data/{node.name}/storaged-logs", "/logs"),
        ),
        healthcheck=HealthCheck(
            test=["CMD", "curl", "-f", f"http://{host}:{http_port}/status"],
        ),
    )


def _graphd_service(node: NodeInfo, meta_addrs: str, offset: int) -> ServiceSpec:
    host = node.hostname
    port = _GRAPHD_BASE + offset
    http_port = _GRAPHD_HTTP_BASE + offset
    return ServiceSpec(
        name="nebula-graphd",
        container_name=f"gb-{node.name}-graphd",
        image=_IMAGE_GRAPHD,
        command=(
            f"--meta_server_addrs={meta_addrs}",
            f"--local_ip={host}",
            f"--ws_ip={host}",
            f"--port={port}",
            f"--ws_http_port={http_port}",
            "--log_dir=/logs",
            "--v=0",
            "--minloglevel=0",
        ),
        volumes=(
            (f"./gb-data/{node.name}/graphd-logs", "/logs"),
        ),
        healthcheck=HealthCheck(
            test=["CMD", "curl", "-f", f"http://{host}:{http_port}/status"],
        ),
    )


class NebulaGraphDeployer:
    sut_name = "nebulagraph"

    def plan_initial(self, inventory: Inventory) -> DeploymentPlan:
        offsets = _port_offsets(inventory)
        addrs = _meta_addrs(inventory, offsets)
        services_per_node: dict[str, list[ServiceSpec]] = {}
        for node in inventory.sut_nodes:
            offset = offsets[node.name]
            services_per_node[node.name] = [
                _metad_service(node, addrs, offset),
                _storaged_service(node, addrs, offset),
                _graphd_service(node, addrs, offset),
            ]
        return DeploymentPlan(sut=self.sut_name, services_per_node=services_per_node)

    def plan_scaleout(self, inventory: Inventory, target: str) -> tuple[str, ServiceSpec]:
        node = inventory.by_name(target)
        offsets = _port_offsets(inventory)
        addrs = _meta_addrs(inventory, offsets)
        offset = offsets[node.name]
        return node.name, _storaged_service(node, addrs, offset)

    def scale_out_endpoint(self, inventory: Inventory, target: str) -> str:
        node = inventory.by_name(target)
        offsets = _port_offsets(inventory)
        return f"{node.hostname}:{_STORAGED_BASE + offsets[node.name]}"
