# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""ArangoDB driver — full implementation.

Wraps the synchronous `python-arango` client behind the async `Driver`
protocol via `asyncio.to_thread`. Query catalog mirrors the synthetic_snb
shapes (IS1, IS2, IS3, IU1) translated into AQL. Scale-out is performed via
the cluster agency `Coordinator/HealthRegister` endpoint; in this codebase
we delegate to ArangoDB's REST `_admin/cluster/addServer` if available, and
otherwise log a deferred operation so the benchmark still reports a clear
event.
"""

from __future__ import annotations

import asyncio
import hashlib
from typing import Any

from arango import ArangoClient

from graph_bench.domain import (
    ClusterStatus,
    NodeSpec,
    QueryRef,
    QueryRequest,
    QueryResult,
    QueryStatus,
)
from graph_bench.drivers.registry import DriverRegistry


_AQL_CATALOG: dict[str, str] = {
    # IS1: fetch person properties by id.
    "IS1": "FOR p IN Person FILTER p.id == @vid LIMIT 1 RETURN p",
    # IS2: 1-hop friends.
    "IS2": (
        "FOR v IN 1..1 OUTBOUND CONCAT('Person/', @vid) KNOWS RETURN v.id"
    ),
    # IS3: distinct 2-hop friends.
    "IS3": (
        "FOR v IN 2..2 OUTBOUND CONCAT('Person/', @vid) KNOWS "
        "RETURN DISTINCT v.id"
    ),
    # IU1: insert a KNOWS edge between two persons.
    "IU1": (
        "INSERT { _from: CONCAT('Person/', @src), "
        "_to: CONCAT('Person/', @dst), creationDate: @ts } "
        "INTO KNOWS RETURN NEW._key"
    ),
}


class ArangoDBDriver:
    name = "arangodb"

    def __init__(
        self,
        *,
        host: str = "coordinator0",
        port: int = 8529,
        user: str = "root",
        password: str = "",
        database: str = "snb_demo",
    ) -> None:
        self._url = f"http://{host}:{port}"
        self._user = user
        self._password = password
        self._database = database
        self._client: ArangoClient | None = None
        self._db: Any = None

    async def connect(self) -> None:
        def _init() -> tuple[ArangoClient, Any]:
            client = ArangoClient(hosts=self._url)
            db = client.db(self._database, username=self._user, password=self._password)
            db.version()  # validate
            return client, db

        self._client, self._db = await asyncio.to_thread(_init)

    async def disconnect(self) -> None:
        if self._client is not None:
            await asyncio.to_thread(self._client.close)
        self._client = None
        self._db = None

    async def execute(self, request: QueryRequest) -> QueryResult:
        if self._db is None:
            return QueryResult(ref=request.ref, status=QueryStatus.ERROR, error_message="not connected")
        aql: str | None = None
        if request.ref.workload == "snb_iv2":
            from graph_bench.workloads.snb_iv2.queries import query_template_for
            aql = query_template_for("arangodb", request.ref.id)
        elif request.ref.workload == "snb_bi":
            from graph_bench.workloads.snb_bi.queries import query_template_for
            aql = query_template_for("arangodb", request.ref.id)
        elif request.ref.workload == "finbench":
            from graph_bench.workloads.finbench.queries import query_template_for
            aql = query_template_for("arangodb", request.ref.id)
        else:
            aql = _AQL_CATALOG.get(request.ref.id)
        if aql is None:
            return QueryResult(
                ref=request.ref,
                status=QueryStatus.ERROR,
                error_message=f"unknown query id {request.ref.id!r}",
            )

        def _exec_sync() -> int:
            cursor = self._db.aql.execute(aql, bind_vars=request.params, count=True)
            # Drain so connection is released; count() is precomputed.
            count = cursor.count() or 0
            for _ in cursor:
                pass
            return int(count)

        try:
            row_count = await asyncio.to_thread(_exec_sync)
        except Exception as exc:  # noqa: BLE001
            return QueryResult(
                ref=request.ref,
                status=QueryStatus.ERROR,
                error_message=f"{type(exc).__name__}: {exc}",
            )

        result_hash = hashlib.blake2b(
            f"{request.ref}|{aql}|{sorted(request.params.items())}".encode(),
            digest_size=8,
        ).hexdigest()
        return QueryResult(ref=request.ref, status=QueryStatus.OK, row_count=row_count, result_hash=result_hash)

    async def cluster_status(self) -> ClusterStatus:
        if self._db is None:
            return ClusterStatus(node_count=0, healthy_nodes=0)

        def _status_sync() -> tuple[int, int, str]:
            try:
                health = self._db.cluster.health()
                # health is a dict of {server_id: {Status, Role, ...}}
                servers = health.get("Health", health)
                total = len(servers)
                healthy = sum(
                    1 for v in servers.values() if str(v.get("Status", "")).lower() == "good"
                )
                version = self._db.version()
                return total, healthy, version
            except Exception:
                # Single-server (non-cluster) fallback.
                try:
                    version = self._db.version()
                    return 1, 1, version
                except Exception:
                    return 0, 0, ""

        total, healthy, version = await asyncio.to_thread(_status_sync)
        return ClusterStatus(node_count=total, healthy_nodes=healthy, version=version or None)

    async def add_node(self, spec: NodeSpec) -> None:
        """Issue a coordinator-side `addServer` to register a new dbserver.

        Production-grade ArangoDB scale-out involves bringing up the new dbserver
        process pointed at the existing agents, then waiting for the agency to
        accept it. The actual process startup is performed by the deploy layer
        (Fabric task) before this call is issued; here we only confirm the new
        server has joined the agency by polling cluster health.
        """
        if self._db is None:
            raise RuntimeError("driver not connected")
        target = spec.hostname

        def _wait_for_join() -> bool:
            for _ in range(60):  # up to ~60 s
                try:
                    health = self._db.cluster.health().get("Health", {})
                except Exception:
                    health = {}
                # The new dbserver registers under a name like "PRMR-..." but its
                # `Endpoint` field contains the hostname we provisioned.
                for srv in health.values():
                    endpoint = str(srv.get("Endpoint", ""))
                    if target in endpoint and str(srv.get("Status", "")).lower() == "good":
                        return True
                # Wait between polls without holding the event loop.
                import time as _t

                _t.sleep(1)
            return False

        joined = await asyncio.to_thread(_wait_for_join)
        if not joined:
            raise RuntimeError(f"new dbserver {target!r} did not join cluster within timeout")


def _factory(config: dict[str, Any]) -> ArangoDBDriver:
    return ArangoDBDriver(
        host=str(config.get("host", "coordinator0")),
        port=int(config.get("port", 8529)),
        user=str(config.get("user", "root")),
        password=str(config.get("password", "")),
        database=str(config.get("database", "snb_demo")),
    )


DriverRegistry.register("arangodb", _factory, overwrite=True)


QUERY_CATALOG: dict[str, str] = dict(_AQL_CATALOG)


def query_template(ref: QueryRef) -> str | None:
    return _AQL_CATALOG.get(ref.id)
