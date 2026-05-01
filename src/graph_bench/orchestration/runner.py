# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""SSH-based execution of `DeploymentPlan`s.

`SSHRunner` translates each per-node service list into a Docker-Compose
project, pushes it to that node via SCP, and runs `docker compose up -d`.
Local mode (where `hostname` is `localhost` or `127.0.0.1`) is supported by
falling back to a local subprocess invocation, so that the same code path
exercises both the local docker-compose simulation and any remote multi-node
cluster — the only difference between the two is the inventory.
"""

from __future__ import annotations

import io
import shlex
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING, Iterator

import yaml

from graph_bench.orchestration.inventory import Inventory, NodeInfo
from graph_bench.orchestration.plan import DeploymentPlan, ServiceSpec
from graph_bench.utils.logging import get_logger

if TYPE_CHECKING:
    from fabric import Connection

_log = get_logger(__name__)


_LOCAL_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})


def _is_local(node: NodeInfo) -> bool:
    return node.hostname in _LOCAL_HOSTS or node.ip in _LOCAL_HOSTS


def render_compose(sut: str, services: list[ServiceSpec]) -> str:
    """Render a list of `ServiceSpec`s into a docker-compose YAML string."""
    body: dict[str, object] = {
        "name": f"gb-{sut}",
        "services": {svc.name: svc.to_compose_service() for svc in services},
    }
    return yaml.safe_dump(body, sort_keys=False)


class SSHRunner:
    """Executes a `DeploymentPlan` via SSH (or locally for the dev cluster)."""

    def __init__(self, inventory: Inventory, *, deploy_root: str = "~/.gb-deploy") -> None:
        self._inventory = inventory
        self._deploy_root = deploy_root

    # ------------------------------------------------------------------ helpers
    def _node_for_hostname(self, hostname: str) -> NodeInfo:
        for n in self._inventory.nodes:
            if n.hostname == hostname or n.name == hostname:
                return n
        raise KeyError(f"hostname {hostname!r} not in inventory")

    def _connection(self, node: NodeInfo) -> "Connection":
        # Lazy import so that `pip install graph-bench[deploy]` is required
        # only when SSH execution is needed.
        from fabric import Connection
        kwargs: dict[str, object] = {}
        if node.ssh_user:
            kwargs["user"] = node.ssh_user
        return Connection(host=node.hostname, **kwargs)

    def _project_dir(self, sut: str) -> str:
        return f"{self._deploy_root}/{sut}"

    # ------------------------------------------------------------------ actions
    def bring_up(self, plan: DeploymentPlan) -> None:
        """Push compose + start services on every node in the plan."""
        for hostname, services in plan.services_per_node.items():
            node = self._node_for_hostname(hostname)
            compose_yaml = render_compose(plan.sut, services)
            _log.info("orchestration_bring_up", node=node.name, services=[s.name for s in services])
            self._push_and_up(node, plan.sut, compose_yaml)

    def tear_down(self, plan: DeploymentPlan) -> None:
        for hostname in plan.services_per_node:
            node = self._node_for_hostname(hostname)
            _log.info("orchestration_tear_down", node=node.name)
            self._compose_command(node, plan.sut, "down -v --remove-orphans")

    def add_service(self, plan_node: str, sut: str, service: ServiceSpec) -> None:
        """Bring a single service up on `plan_node` (used for scale-out)."""
        node = self._node_for_hostname(plan_node)
        compose_yaml = render_compose(sut, [service])
        _log.info(
            "orchestration_scale_out",
            node=node.name,
            service=service.name,
        )
        self._push_and_up(node, f"{sut}-scaleout", compose_yaml)

    # ------------------------------------------------------------------ I/O
    def _push_and_up(self, node: NodeInfo, sut: str, compose_yaml: str) -> None:
        project_dir = self._project_dir(sut)
        if _is_local(node):
            local_dir = Path(project_dir.replace("~", str(Path.home())))
            local_dir.mkdir(parents=True, exist_ok=True)
            (local_dir / "compose.yaml").write_text(compose_yaml)
            subprocess.run(
                ["docker", "compose", "-f", str(local_dir / "compose.yaml"), "up", "-d"],
                check=True,
            )
            return
        with self._connection(node) as conn:
            conn.run(f"mkdir -p {shlex.quote(project_dir)}", hide=True)
            conn.put(io.StringIO(compose_yaml), remote=f"{project_dir}/compose.yaml")
            conn.run(
                f"cd {shlex.quote(project_dir)} && docker compose -f compose.yaml up -d",
            )

    def _compose_command(self, node: NodeInfo, sut: str, args: str) -> None:
        project_dir = self._project_dir(sut)
        if _is_local(node):
            local_dir = Path(project_dir.replace("~", str(Path.home())))
            if not (local_dir / "compose.yaml").exists():
                return
            subprocess.run(
                ["docker", "compose", "-f", str(local_dir / "compose.yaml"), *shlex.split(args)],
                check=False,
            )
            return
        with self._connection(node) as conn:
            conn.run(
                f"cd {shlex.quote(project_dir)} && docker compose -f compose.yaml {args}",
                warn=True,
            )

    # ------------------------------------------------------------------ misc
    def each_node(self, plan: DeploymentPlan) -> Iterator[tuple[NodeInfo, list[ServiceSpec]]]:
        for hostname, services in plan.services_per_node.items():
            yield self._node_for_hostname(hostname), services
