# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""Cluster inventory loaded from `configs/inventory/cluster.yaml`.

The inventory enumerates each cluster node's hostname / IP / SSH user and the
roles that node carries (`client`, `sut`, `reserve`). Deployers consume the
inventory to decide which services to start where.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass(frozen=True, slots=True)
class NodeInfo:
    name: str           # logical name (e.g., "node2")
    hostname: str       # DNS-resolvable from the orchestrator and from peers
    ip: str | None
    ssh_user: str | None
    roles: tuple[str, ...]

    @property
    def is_client(self) -> bool:
        return "client" in self.roles

    @property
    def is_sut(self) -> bool:
        return "sut" in self.roles

    @property
    def is_reserve(self) -> bool:
        return "reserve" in self.roles


@dataclass(frozen=True, slots=True)
class Inventory:
    cluster_name: str
    nodes: tuple[NodeInfo, ...]

    @property
    def client_nodes(self) -> tuple[NodeInfo, ...]:
        return tuple(n for n in self.nodes if n.is_client)

    @property
    def sut_nodes(self) -> tuple[NodeInfo, ...]:
        return tuple(n for n in self.nodes if n.is_sut)

    @property
    def reserve_nodes(self) -> tuple[NodeInfo, ...]:
        return tuple(n for n in self.nodes if n.is_reserve)

    def by_name(self, name: str) -> NodeInfo:
        for n in self.nodes:
            if n.name == name:
                return n
        raise KeyError(f"unknown node {name!r}")

    def all_hostnames(self) -> tuple[str, ...]:
        return tuple(n.hostname for n in self.nodes)


def load_inventory(path: Path) -> Inventory:
    """Parse a YAML inventory file into an `Inventory`."""
    with Path(path).open("r", encoding="utf-8") as fp:
        data = yaml.safe_load(fp)
    if not isinstance(data, dict):
        raise ValueError(f"{path}: expected a top-level mapping")
    cluster_name = str(data.get("cluster_name", "unnamed-cluster"))
    raw_nodes = data.get("nodes", {})
    if not isinstance(raw_nodes, dict):
        raise ValueError(f"{path}: 'nodes' must be a mapping")

    parsed: list[NodeInfo] = []
    for name, n in raw_nodes.items():
        n = n or {}
        roles = tuple(n.get("roles") or ())
        parsed.append(
            NodeInfo(
                name=name,
                hostname=str(n.get("hostname", name)),
                ip=str(n.get("ip")) if n.get("ip") is not None else None,
                ssh_user=str(n.get("ssh_user")) if n.get("ssh_user") is not None else None,
                roles=roles,
            )
        )
    return Inventory(cluster_name=cluster_name, nodes=tuple(parsed))
