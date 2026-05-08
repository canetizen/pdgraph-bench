# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""SSH-based execution of `DeploymentPlan`s.

`SSHRunner` translates each per-node service list into a Docker-Compose
project, pushes it to that node via SCP, and runs `docker compose up -d`.

There is one code path for both production (multi-host) and the single-host
playground: every node — including localhost — is reached through fabric's
`Connection`. The playground requires a working sshd + authorised self-key
on the orchestrator host so that `localhost` is reachable the same way a
real cluster node would be. This keeps prod and playground bit-for-bit
equivalent at the orchestration layer.

Compose projects are namespaced per `(sut, logical-node-name)` so that two
logical nodes sharing a host (playground) get distinct project directories
and distinct `name:` fields, and `docker compose down` for one does not
disturb the other.
"""

from __future__ import annotations

import io
import shlex
from typing import TYPE_CHECKING, Iterator

import yaml

from graph_bench.orchestration.inventory import Inventory, NodeInfo
from graph_bench.orchestration.plan import DeploymentPlan, ServiceSpec
from graph_bench.utils.logging import get_logger

if TYPE_CHECKING:
    from fabric import Connection

_log = get_logger(__name__)


# Deploy root for compose projects on every node. Kept dotsuz under $HOME so
# that snap-confined Docker daemons (which only see non-dotted paths under
# the user's home) can read the rendered compose file. Using `$HOME` (not
# `~`) means `shlex.quote` does not break tilde expansion — quoted `~`
# stays literal under sh/bash, which would create a directory called `~`.
_DEFAULT_DEPLOY_ROOT = "$HOME/gb-deploy"


def render_compose(sut: str, node_name: str, services: list[ServiceSpec]) -> str:
    """Render a list of `ServiceSpec`s into a docker-compose YAML string."""
    body: dict[str, object] = {
        "name": f"gb-{sut}-{node_name}",
        "services": {svc.name: svc.to_compose_service() for svc in services},
    }
    return yaml.safe_dump(body, sort_keys=False)


class SSHRunner:
    """Executes a `DeploymentPlan` via SSH (fabric Connection per node)."""

    def __init__(self, inventory: Inventory, *, deploy_root: str = _DEFAULT_DEPLOY_ROOT) -> None:
        self._inventory = inventory
        self._deploy_root = deploy_root

    # ------------------------------------------------------------------ helpers
    def _node_by_name(self, name: str) -> NodeInfo:
        for n in self._inventory.nodes:
            if n.name == name:
                return n
        raise KeyError(f"node {name!r} not in inventory")

    def _connection(self, node: NodeInfo) -> "Connection":
        # Lazy import so that `pip install pdgraph-bench[deploy]` is required
        # only when SSH execution is needed.
        from fabric import Connection
        kwargs: dict[str, object] = {}
        if node.ssh_user:
            kwargs["user"] = node.ssh_user
        return Connection(host=node.hostname, **kwargs)

    def _project_dir(self, sut: str, node_name: str) -> str:
        return f"{self._deploy_root}/{sut}/{node_name}"

    # ------------------------------------------------------------------ actions
    def bring_up(self, plan: DeploymentPlan) -> None:
        """Push compose + start services on every node in the plan."""
        for node_name, services in plan.services_per_node.items():
            node = self._node_by_name(node_name)
            compose_yaml = render_compose(plan.sut, node_name, services)
            _log.info("orchestration_bring_up", node=node.name, services=[s.name for s in services])
            self._push_and_up(node, plan.sut, node_name, compose_yaml)

    def tear_down(self, plan: DeploymentPlan) -> None:
        # Tear down the per-(sut, node) projects from `plan_initial` …
        for node_name in plan.services_per_node:
            node = self._node_by_name(node_name)
            _log.info("orchestration_tear_down", node=node.name)
            self._compose_command(node, plan.sut, node_name, "down -v --remove-orphans")
        # … and any scale-out projects that may have been added at runtime
        # for reserve nodes. The project name is `<reserve-node>-scaleout`.
        for node in self._inventory.reserve_nodes:
            scaleout_id = f"{node.name}-scaleout"
            _log.info("orchestration_tear_down_scaleout", node=node.name)
            self._compose_command(node, plan.sut, scaleout_id, "down -v --remove-orphans")
        # Finally wipe any bind-mounted state directories the deployers laid
        # down. We do this through a throwaway alpine container because the
        # paths are root-owned (Docker bind mounts) and the SSH user usually
        # cannot rm them directly.
        for node_name in plan.services_per_node:
            node = self._node_by_name(node_name)
            self._wipe_state(node, plan.sut, node_name)
        for node in self._inventory.reserve_nodes:
            self._wipe_state(node, plan.sut, f"{node.name}-scaleout")

    def add_service(self, plan_node: str, sut: str, service: ServiceSpec) -> None:
        """Bring a single service up on `plan_node` (used for scale-out).

        `plan_node` is the logical node name (e.g. `node5`); the project name
        is suffixed with `-scaleout` so it doesn't collide with the node's
        baseline project (which a Tier-2 node may not even have).
        """
        node = self._node_by_name(plan_node)
        scaleout_node_id = f"{plan_node}-scaleout"
        compose_yaml = render_compose(sut, scaleout_node_id, [service])
        _log.info(
            "orchestration_scale_out",
            node=node.name,
            service=service.name,
        )
        self._push_and_up(node, sut, scaleout_node_id, compose_yaml)

    # ------------------------------------------------------------------ I/O
    def _push_and_up(
        self, node: NodeInfo, sut: str, node_name: str, compose_yaml: str
    ) -> None:
        project_dir = self._project_dir(sut, node_name)
        with self._connection(node) as conn:
            # No shlex.quote: project_dir contains `$HOME` which the remote
            # shell must expand. shlex.quote wraps it in single quotes and
            # `$` stays literal. Path content is benchmark-controlled
            # ([a-z0-9-_/$]) so injection risk is nil.
            conn.run(f"mkdir -p {project_dir}", hide=True)
            # SFTP put cannot expand $HOME, so resolve it once via the same
            # connection and substitute manually for the upload.
            home = conn.run("echo $HOME", hide=True).stdout.strip()
            remote_path = project_dir.replace("$HOME", home)
            conn.put(io.StringIO(compose_yaml), remote=f"{remote_path}/compose.yaml")
            conn.run(
                f"cd {project_dir} && docker compose -f compose.yaml up -d",
            )

    def _compose_command(
        self, node: NodeInfo, sut: str, node_name: str, args: str
    ) -> None:
        project_dir = self._project_dir(sut, node_name)
        with self._connection(node) as conn:
            conn.run(
                f"cd {project_dir} && docker compose -f compose.yaml {args}",
                warn=True,
                hide=True,
            )

    def _wipe_state(self, node: NodeInfo, sut: str, node_name: str) -> None:
        """Delete the per-(sut, node) bind-mount data via a one-shot alpine
        container, since the directories are root-owned by the docker daemon."""
        project_dir = self._project_dir(sut, node_name)
        with self._connection(node) as conn:
            home = conn.run("echo $HOME", hide=True).stdout.strip()
            resolved = project_dir.replace("$HOME", home)
            conn.run(
                f"if [ -d {resolved}/gb-data ]; then "
                f"docker run --rm -v {resolved}:/work alpine "
                f"sh -c 'rm -rf /work/gb-data'; fi",
                warn=True,
                hide=True,
            )

    # ------------------------------------------------------------------ misc
    def each_node(self, plan: DeploymentPlan) -> Iterator[tuple[NodeInfo, list[ServiceSpec]]]:
        for node_name, services in plan.services_per_node.items():
            yield self._node_by_name(node_name), services
