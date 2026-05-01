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
"""

from __future__ import annotations

from graph_bench.orchestration.inventory import Inventory
from graph_bench.orchestration.plan import DeploymentPlan, HealthCheck, ServiceSpec


_IMAGE_METAD = "vesoft/nebula-metad:v3.8.0"
_IMAGE_STORAGED = "vesoft/nebula-storaged:v3.8.0"
_IMAGE_GRAPHD = "vesoft/nebula-graphd:v3.8.0"


def _meta_addrs(inventory: Inventory) -> str:
    return ",".join(f"{n.hostname}:9559" for n in inventory.sut_nodes)


def _metad_service(host: str, meta_addrs: str) -> ServiceSpec:
    return ServiceSpec(
        name="nebula-metad",
        container_name=f"gb-{host}-metad",
        image=_IMAGE_METAD,
        command=(
            f"--meta_server_addrs={meta_addrs}",
            f"--local_ip={host}",
            f"--ws_ip={host}",
            "--port=9559",
            "--data_path=/data/meta",
            "--log_dir=/logs",
            "--v=0",
            "--minloglevel=0",
        ),
        volumes=(
            (f"./gb-data/{host}/metad", "/data/meta"),
            (f"./gb-data/{host}/metad-logs", "/logs"),
        ),
        healthcheck=HealthCheck(
            test=["CMD", "curl", "-f", f"http://{host}:19559/status"],
        ),
    )


def _storaged_service(host: str, meta_addrs: str) -> ServiceSpec:
    return ServiceSpec(
        name="nebula-storaged",
        container_name=f"gb-{host}-storaged",
        image=_IMAGE_STORAGED,
        command=(
            f"--meta_server_addrs={meta_addrs}",
            f"--local_ip={host}",
            f"--ws_ip={host}",
            "--port=9779",
            "--data_path=/data/storage",
            "--log_dir=/logs",
            "--v=0",
            "--minloglevel=0",
        ),
        volumes=(
            (f"./gb-data/{host}/storaged", "/data/storage"),
            (f"./gb-data/{host}/storaged-logs", "/logs"),
        ),
    )


def _graphd_service(host: str, meta_addrs: str) -> ServiceSpec:
    return ServiceSpec(
        name="nebula-graphd",
        container_name=f"gb-{host}-graphd",
        image=_IMAGE_GRAPHD,
        command=(
            f"--meta_server_addrs={meta_addrs}",
            f"--local_ip={host}",
            f"--ws_ip={host}",
            "--port=9669",
            "--log_dir=/logs",
            "--v=0",
            "--minloglevel=0",
        ),
        volumes=(
            (f"./gb-data/{host}/graphd-logs", "/logs"),
        ),
        healthcheck=HealthCheck(
            test=["CMD", "curl", "-f", f"http://{host}:19669/status"],
        ),
    )


class NebulaGraphDeployer:
    sut_name = "nebulagraph"

    def plan_initial(self, inventory: Inventory) -> DeploymentPlan:
        addrs = _meta_addrs(inventory)
        services_per_node: dict[str, list[ServiceSpec]] = {}
        for node in inventory.sut_nodes:
            host = node.hostname
            services_per_node[host] = [
                _metad_service(host, addrs),
                _storaged_service(host, addrs),
                _graphd_service(host, addrs),
            ]
        return DeploymentPlan(sut=self.sut_name, services_per_node=services_per_node)

    def plan_scaleout(self, inventory: Inventory, target: str) -> tuple[str, ServiceSpec]:
        node = inventory.by_name(target)
        return node.hostname, _storaged_service(node.hostname, _meta_addrs(inventory))
