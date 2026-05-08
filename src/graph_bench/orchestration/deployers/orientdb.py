# Created by: Mustafa Can Caliskan
# Date: 2026-05-08

"""OrientDB deployer — distributed Hazelcast cluster.

OrientDB 3.x ships with a built-in Hazelcast-based multi-master cluster.
Setting `ORIENTDB_OPTS_MEMORY` plus invoking `dserver.sh` launches the
distributed daemon; the nodes auto-discover each other on the host
network. We deploy one OrientDB process per `sut`-role node — three
nodes in production, three co-located on the playground via per-node
port offsets.

Default ports:
- 2424: binary (clients, replication)
- 2480: HTTP REST + Studio UI
- 5701: Hazelcast cluster heartbeat (range 5701-5703 for 3-node)

Co-located nodes (single-host playground): each Hazelcast port +1, REST
+ binary +10 per offset slot.
"""

from __future__ import annotations

from graph_bench.orchestration.inventory import Inventory, NodeInfo
from graph_bench.orchestration.plan import DeploymentPlan, HealthCheck, ServiceSpec


_IMAGE = "orientdb:3.2"

_BINARY_BASE = 2424
_HTTP_BASE = 2480
_HAZELCAST_BASE = 5701
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


def _orientdb_service(node: NodeInfo, offset: int) -> ServiceSpec:
    host = node.hostname
    binary_port = _BINARY_BASE + offset
    http_port = _HTTP_BASE + offset
    # Hazelcast picks the next free port within (5701, 5701+portCount); we
    # bump the base so co-located peers don't fight over 5701-5703.
    hazelcast_port = _HAZELCAST_BASE + (offset // _OFFSET_STEP) * 10
    return ServiceSpec(
        name="orientdb",
        container_name=f"gb-{node.name}-orientdb",
        image=_IMAGE,
        # `dserver.sh` enables the distributed daemon (vs. `server.sh`).
        # ORIENTDB_NODE_NAME must be unique per cluster member.
        env={
            "ORIENTDB_ROOT_PASSWORD": "rootpwd",
            "ORIENTDB_NODE_NAME": node.name,
            "ORIENTDB_OPTS_MEMORY": "-Xms512m -Xmx2g",
            # Bind to all interfaces; the deployer's hostname-aware port
            # offsets keep the ports unique on a co-located host.
            "ORIENTDB_NETWORK_BINARY_PORT": str(binary_port),
            "ORIENTDB_NETWORK_HTTP_PORT": str(http_port),
            "ORIENTDB_DISTRIBUTED": "true",
        },
        # Override default `server.sh` with the distributed entrypoint.
        entrypoint=("/orientdb/bin/dserver.sh",),
        # NOTE: do NOT mount /orientdb/config — the image's default config
        # (orientdb-server-config.xml + hazelcast.xml) lives there and a host
        # bind mount would shadow it, leaving the HTTP listener undefined.
        # We only persist data + log.
        volumes=(
            (f"./gb-data/{node.name}/orientdb-databases", "/orientdb/databases"),
            (f"./gb-data/{node.name}/orientdb-log", "/orientdb/log"),
        ),
        user="0:0",
        healthcheck=HealthCheck(
            test=["CMD", "bash", "-c", f"exec 3<>/dev/tcp/localhost/{http_port}"],
            interval_s=10,
            timeout_s=5,
            retries=20,
            start_period_s=60,
        ),
    )


class OrientDBDeployer:
    sut_name = "orientdb"

    def plan_initial(self, inventory: Inventory) -> DeploymentPlan:
        offsets = _port_offsets(inventory)
        services_per_node: dict[str, list[ServiceSpec]] = {}
        for node in inventory.sut_nodes:
            services_per_node[node.name] = [_orientdb_service(node, offsets[node.name])]
        return DeploymentPlan(sut=self.sut_name, services_per_node=services_per_node)

    def plan_scaleout(self, inventory: Inventory, target: str) -> tuple[str, ServiceSpec]:
        node = inventory.by_name(target)
        offsets = _port_offsets(inventory)
        return node.name, _orientdb_service(node, offsets[node.name])

    def scale_out_endpoint(self, inventory: Inventory, target: str) -> str:
        node = inventory.by_name(target)
        offsets = _port_offsets(inventory)
        return f"{node.hostname}:{_HTTP_BASE + offsets[node.name]}"
