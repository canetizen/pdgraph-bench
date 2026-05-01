# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""Deployment plan value types.

A `Deployer` (one per SUT) produces a `DeploymentPlan` from an `Inventory`.
The plan is a per-node list of `ServiceSpec`s; each `ServiceSpec` is an
abstract description of one container that will run on that node.

The runner translates `ServiceSpec`s into a per-node `docker-compose.yaml`
file and pushes them via SSH for execution. Going through Compose (rather
than raw `docker run`) keeps the deployment introspectable with `docker
compose ps`, gives us free health-check evaluation, and makes restarts /
teardowns idempotent.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol

from graph_bench.orchestration.inventory import Inventory


@dataclass(frozen=True, slots=True)
class HealthCheck:
    test: list[str]
    interval_s: int = 10
    timeout_s: int = 5
    retries: int = 6
    start_period_s: int = 30

    def to_compose(self) -> dict[str, object]:
        return {
            "test": list(self.test),
            "interval": f"{self.interval_s}s",
            "timeout": f"{self.timeout_s}s",
            "retries": self.retries,
            "start_period": f"{self.start_period_s}s",
        }


@dataclass(frozen=True, slots=True)
class ServiceSpec:
    """One container to run on one cluster node."""

    name: str
    """Compose service name (unique within the per-node compose project)."""

    container_name: str
    """Globally unique container name (so `docker ps` shows what it is)."""

    image: str

    command: tuple[str, ...] = ()

    env: dict[str, str] = field(default_factory=dict)

    network_mode: str = "host"
    """`host` is the default because peer nodes are addressed by their real
    hostname (resolved via /etc/hosts or DNS on the host); a bridge network
    would shadow those names."""

    volumes: tuple[tuple[str, str], tuple] = field(default_factory=tuple)
    """Tuples of (host_path, container_path)."""

    restart: str = "on-failure"

    healthcheck: HealthCheck | None = None

    extra_hosts: dict[str, str] = field(default_factory=dict)
    """Hostname → IP entries injected into /etc/hosts inside the container,
    used as a fallback when the host's own /etc/hosts does not resolve all
    peer hostnames (e.g., bridge-network deployments)."""

    def to_compose_service(self) -> dict[str, object]:
        body: dict[str, object] = {
            "image": self.image,
            "container_name": self.container_name,
            "restart": self.restart,
        }
        if self.command:
            body["command"] = list(self.command)
        if self.env:
            body["environment"] = dict(self.env)
        if self.volumes:
            body["volumes"] = [f"{h}:{c}" for h, c in self.volumes]
        if self.network_mode != "default":
            body["network_mode"] = self.network_mode
        if self.healthcheck is not None:
            body["healthcheck"] = self.healthcheck.to_compose()
        if self.extra_hosts:
            body["extra_hosts"] = [f"{h}:{ip}" for h, ip in self.extra_hosts.items()]
        return body


@dataclass(frozen=True, slots=True)
class DeploymentPlan:
    sut: str
    services_per_node: dict[str, list[ServiceSpec]]
    """Keyed by node hostname (NOT logical name) so the runner can ssh directly."""

    def nodes(self) -> tuple[str, ...]:
        return tuple(self.services_per_node.keys())


class Deployer(Protocol):
    """Per-SUT topology + scale-out logic."""

    sut_name: str

    def plan_initial(self, inventory: Inventory) -> DeploymentPlan:
        """Plan for bringing up the initial cluster (only `sut`-role nodes)."""
        ...

    def plan_scaleout(self, inventory: Inventory, target: str) -> tuple[str, ServiceSpec]:
        """Return (host_hostname, service) for the new instance to start.

        `target` is the logical node name (e.g., `node5`); the deployer maps it
        to the inventory's hostname and emits the `ServiceSpec` that the
        Fabric `scale-out` task will start on that node.
        """
        ...
