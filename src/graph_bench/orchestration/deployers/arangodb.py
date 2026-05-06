# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""ArangoDB 3.11 cluster deployer.

Topology (per `sut`-role node, three SUT nodes total):
- 1 `agent` (consensus / agency)
- 1 `dbserver` (data sharding)
- 1 `coordinator` (query routing + client-facing endpoint, port 8529)

Reserve node hosts a `dbserver` only, started during scenario S5.

Co-located nodes (single-host playground): when two or more logical nodes
share a hostname, each gets a port offset of `i*10` so agent/dbserver/
coordinator ports do not collide.
"""

from __future__ import annotations

from graph_bench.orchestration.inventory import Inventory, NodeInfo
from graph_bench.orchestration.plan import DeploymentPlan, HealthCheck, ServiceSpec


_IMAGE = "arangodb/arangodb:3.11.14"

_AGENT_BASE = 5001
_DBSERVER_BASE = 6001
_COORDINATOR_BASE = 8529
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


def _agency_endpoints(inventory: Inventory, offsets: dict[str, int]) -> list[str]:
    """ArangoDB expects `--agency.endpoint` / `--cluster.agency-endpoint`
    repeated once per agent (NOT comma-separated)."""
    return [
        f"tcp://{n.hostname}:{_AGENT_BASE + offsets[n.name]}" for n in inventory.sut_nodes
    ]


def _agent_service(node: NodeInfo, agency: list[str], agency_size: int, offset: int) -> ServiceSpec:
    host = node.hostname
    port = _AGENT_BASE + offset
    args = [
        "--agency.activate=true",
        *[f"--agency.endpoint={ep}" for ep in agency],
        f"--agency.size={agency_size}",
        f"--agency.my-address=tcp://{host}:{port}",
        "--agency.supervision=true",
        f"--server.endpoint=tcp://0.0.0.0:{port}",
        "--server.statistics=true",
        "--server.authentication=false",
        "--database.directory=/var/lib/arangodb3-agent",
        "--log.file=/var/log/arangodb3/agent.log",
    ]
    return ServiceSpec(
        name="arangodb-agent",
        container_name=f"gb-{node.name}-agent",
        image=_IMAGE,
        # Bypass the image's entrypoint.sh which rewrites/ignores cluster CLI
        # args. We invoke arangod directly with the agent/dbserver/coordinator
        # role flags the deployer assembled.
        entrypoint=("arangod",),
        command=tuple(args),
        env={"ARANGO_NO_AUTH": "1"},
        volumes=(
            (f"./gb-data/{node.name}/agent", "/var/lib/arangodb3-agent"),
            (f"./gb-data/{node.name}/agent-logs", "/var/log/arangodb3"),
        ),
        # No healthcheck: official ArangoDB image lacks curl/wget; readiness
        # is verified by the loader's _wait_for_coordinator probe instead.
    )


def _dbserver_service(node: NodeInfo, agency: list[str], offset: int) -> ServiceSpec:
    host = node.hostname
    port = _DBSERVER_BASE + offset
    args = [
        *[f"--cluster.agency-endpoint={ep}" for ep in agency],
        "--cluster.my-role=DBSERVER",
        f"--cluster.my-address=tcp://{host}:{port}",
        f"--server.endpoint=tcp://0.0.0.0:{port}",
        "--server.statistics=true",
        "--server.authentication=false",
        "--database.directory=/var/lib/arangodb3-dbserver",
        "--log.file=/var/log/arangodb3/dbserver.log",
    ]
    return ServiceSpec(
        name="arangodb-dbserver",
        container_name=f"gb-{node.name}-dbserver",
        image=_IMAGE,
        # Bypass the image's entrypoint.sh which rewrites/ignores cluster CLI
        # args. We invoke arangod directly with the agent/dbserver/coordinator
        # role flags the deployer assembled.
        entrypoint=("arangod",),
        command=tuple(args),
        env={"ARANGO_NO_AUTH": "1"},
        volumes=(
            (f"./gb-data/{node.name}/dbserver", "/var/lib/arangodb3-dbserver"),
            (f"./gb-data/{node.name}/dbserver-logs", "/var/log/arangodb3"),
        ),
    )


def _coordinator_service(node: NodeInfo, agency: list[str], offset: int) -> ServiceSpec:
    host = node.hostname
    port = _COORDINATOR_BASE + offset
    args = [
        *[f"--cluster.agency-endpoint={ep}" for ep in agency],
        "--cluster.my-role=COORDINATOR",
        f"--cluster.my-address=tcp://{host}:{port}",
        f"--server.endpoint=tcp://0.0.0.0:{port}",
        "--server.statistics=true",
        "--server.authentication=false",
        "--database.directory=/var/lib/arangodb3-coordinator",
        "--log.file=/var/log/arangodb3/coordinator.log",
    ]
    return ServiceSpec(
        name="arangodb-coordinator",
        container_name=f"gb-{node.name}-coordinator",
        image=_IMAGE,
        # Bypass the image's entrypoint.sh which rewrites/ignores cluster CLI
        # args. We invoke arangod directly with the agent/dbserver/coordinator
        # role flags the deployer assembled.
        entrypoint=("arangod",),
        command=tuple(args),
        env={"ARANGO_NO_AUTH": "1"},
        volumes=(
            (f"./gb-data/{node.name}/coordinator", "/var/lib/arangodb3-coordinator"),
            (f"./gb-data/{node.name}/coordinator-logs", "/var/log/arangodb3"),
        ),
        # See `_agent_service` re: healthcheck.
    )


class ArangoDBDeployer:
    sut_name = "arangodb"

    def plan_initial(self, inventory: Inventory) -> DeploymentPlan:
        offsets = _port_offsets(inventory)
        agency = _agency_endpoints(inventory, offsets)
        agency_size = len(inventory.sut_nodes)
        services_per_node: dict[str, list[ServiceSpec]] = {}
        for node in inventory.sut_nodes:
            offset = offsets[node.name]
            services_per_node[node.name] = [
                _agent_service(node, agency, agency_size, offset),
                _dbserver_service(node, agency, offset),
                _coordinator_service(node, agency, offset),
            ]
        return DeploymentPlan(sut=self.sut_name, services_per_node=services_per_node)

    def plan_scaleout(self, inventory: Inventory, target: str) -> tuple[str, ServiceSpec]:
        node = inventory.by_name(target)
        offsets = _port_offsets(inventory)
        agency = _agency_endpoints(inventory, offsets)
        return node.name, _dbserver_service(node, agency, offsets[node.name])

    def scale_out_endpoint(self, inventory: Inventory, target: str) -> str:
        node = inventory.by_name(target)
        offsets = _port_offsets(inventory)
        return f"{node.hostname}:{_DBSERVER_BASE + offsets[node.name]}"
