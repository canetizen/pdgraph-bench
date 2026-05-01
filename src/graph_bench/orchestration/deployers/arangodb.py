# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""ArangoDB 3.11 cluster deployer.

Topology (per `sut`-role node, three SUT nodes total):
- 1 `agent` (consensus / agency)
- 1 `dbserver` (data sharding)
- 1 `coordinator` (query routing + client-facing endpoint, port 8529)

Reserve node hosts a `dbserver` only, started during scenario S5.
"""

from __future__ import annotations

from graph_bench.orchestration.inventory import Inventory
from graph_bench.orchestration.plan import DeploymentPlan, HealthCheck, ServiceSpec


_IMAGE = "arangodb/arangodb:3.11.14"


def _agency_endpoints(inventory: Inventory) -> str:
    """Comma-separated list of agency endpoints (one per SUT node)."""
    return ",".join(f"tcp://{n.hostname}:5001" for n in inventory.sut_nodes)


def _agent_service(host: str, agency: str, all_hosts: list[str]) -> ServiceSpec:
    args = [
        "--agency.activate=true",
        "--agency.endpoint=" + agency,
        "--agency.size=" + str(len(all_hosts)),
        f"--agency.my-address=tcp://{host}:5001",
        "--agency.supervision=true",
        "--server.endpoint=tcp://0.0.0.0:5001",
        "--server.statistics=true",
        "--database.directory=/var/lib/arangodb3-agent",
        "--log.file=/var/log/arangodb3/agent.log",
    ]
    return ServiceSpec(
        name="arangodb-agent",
        container_name=f"gb-{host}-agent",
        image=_IMAGE,
        command=tuple(args),
        env={"ARANGO_NO_AUTH": "1"},
        volumes=(
            (f"./gb-data/{host}/agent", "/var/lib/arangodb3-agent"),
            (f"./gb-data/{host}/agent-logs", "/var/log/arangodb3"),
        ),
        healthcheck=HealthCheck(
            test=["CMD", "curl", "-f", f"http://{host}:5001/_api/version"],
        ),
    )


def _dbserver_service(host: str, agency: str) -> ServiceSpec:
    args = [
        "--cluster.agency-endpoint=" + agency,
        "--cluster.my-role=DBSERVER",
        f"--cluster.my-address=tcp://{host}:6001",
        "--server.endpoint=tcp://0.0.0.0:6001",
        "--server.statistics=true",
        "--database.directory=/var/lib/arangodb3-dbserver",
        "--log.file=/var/log/arangodb3/dbserver.log",
    ]
    return ServiceSpec(
        name="arangodb-dbserver",
        container_name=f"gb-{host}-dbserver",
        image=_IMAGE,
        command=tuple(args),
        env={"ARANGO_NO_AUTH": "1"},
        volumes=(
            (f"./gb-data/{host}/dbserver", "/var/lib/arangodb3-dbserver"),
            (f"./gb-data/{host}/dbserver-logs", "/var/log/arangodb3"),
        ),
    )


def _coordinator_service(host: str, agency: str) -> ServiceSpec:
    args = [
        "--cluster.agency-endpoint=" + agency,
        "--cluster.my-role=COORDINATOR",
        f"--cluster.my-address=tcp://{host}:8529",
        "--server.endpoint=tcp://0.0.0.0:8529",
        "--server.statistics=true",
        "--database.directory=/var/lib/arangodb3-coordinator",
        "--log.file=/var/log/arangodb3/coordinator.log",
    ]
    return ServiceSpec(
        name="arangodb-coordinator",
        container_name=f"gb-{host}-coordinator",
        image=_IMAGE,
        command=tuple(args),
        env={"ARANGO_NO_AUTH": "1"},
        volumes=(
            (f"./gb-data/{host}/coordinator", "/var/lib/arangodb3-coordinator"),
            (f"./gb-data/{host}/coordinator-logs", "/var/log/arangodb3"),
        ),
        healthcheck=HealthCheck(
            test=["CMD", "curl", "-f", f"http://{host}:8529/_api/version"],
        ),
    )


class ArangoDBDeployer:
    sut_name = "arangodb"

    def plan_initial(self, inventory: Inventory) -> DeploymentPlan:
        agency = _agency_endpoints(inventory)
        sut_hosts = [n.hostname for n in inventory.sut_nodes]
        services_per_node: dict[str, list[ServiceSpec]] = {}
        for node in inventory.sut_nodes:
            host = node.hostname
            services_per_node[host] = [
                _agent_service(host, agency, sut_hosts),
                _dbserver_service(host, agency),
                _coordinator_service(host, agency),
            ]
        return DeploymentPlan(sut=self.sut_name, services_per_node=services_per_node)

    def plan_scaleout(self, inventory: Inventory, target: str) -> tuple[str, ServiceSpec]:
        node = inventory.by_name(target)
        return node.hostname, _dbserver_service(node.hostname, _agency_endpoints(inventory))
