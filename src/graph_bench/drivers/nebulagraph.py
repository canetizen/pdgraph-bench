# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""NebulaGraph driver — first real-SUT implementation.

Wraps the synchronous `nebula3-python` client behind the async `Driver` protocol
via `asyncio.to_thread`. The query catalog is intentionally small for the
demo cluster — just enough to exercise the harness end-to-end against a real
distributed database. The full LDBC SNB Interactive v2 query subset will be
added once the synthetic schema is replaced with the official LDBC datagen
output.

Cluster topology (per Progress Report):
- node2-node4 each host one metad, one storaged, one graphd.
- node5 hosts a reserve storaged that joins the cluster on the S5 trigger.
"""

from __future__ import annotations

import asyncio
import hashlib
from typing import Any

from nebula3.Config import Config
from nebula3.gclient.net import ConnectionPool

from graph_bench.domain import (
    ClusterStatus,
    NodeSpec,
    QueryRef,
    QueryRequest,
    QueryResult,
    QueryStatus,
)
from graph_bench.drivers.registry import DriverRegistry


# Demo query catalog. Refs match the synthetic_snb workload; these are NOT the
# official LDBC IS1..IS7 queries. They have the same shape (1-hop / 2-hop
# parametric reads + an insert) and validate the harness end-to-end.
_QUERY_CATALOG: dict[str, str] = {
    # IS1: fetch person properties by vertex id
    "IS1": "USE {space}; FETCH PROP ON Person {vid} YIELD properties(vertex) AS props",
    # IS2: 1-hop friends
    "IS2": "USE {space}; GO FROM {vid} OVER KNOWS YIELD dst(edge) AS friend",
    # IS3: 2-hop friends-of-friends, distinct
    "IS3": (
        "USE {space}; "
        "GO 2 STEPS FROM {vid} OVER KNOWS "
        "YIELD DISTINCT dst(edge) AS fof"
    ),
    # IU1: insert a KNOWS edge between two persons
    "IU1": (
        "USE {space}; "
        "INSERT EDGE KNOWS(creationDate) "
        "VALUES {src}->{dst}:({ts})"
    ),
}


class NebulaGraphDriver:
    name = "nebulagraph"

    def __init__(
        self,
        *,
        host: str = "graphd0",
        port: int = 9669,
        user: str = "root",
        password: str = "nebula",
        space: str = "snb_demo",
        meta_host: str = "metad0",
        meta_port: int = 9559,
        pool_size: int = 32,
    ) -> None:
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._space = space
        self._meta_host = meta_host
        self._meta_port = meta_port
        self._pool_size = pool_size
        self._pool: ConnectionPool | None = None

    async def connect(self) -> None:
        def _init() -> ConnectionPool:
            cfg = Config()
            cfg.max_connection_pool_size = self._pool_size
            pool = ConnectionPool()
            ok = pool.init([(self._host, self._port)], cfg)
            if not ok:
                raise RuntimeError(f"NebulaGraph pool init failed for {self._host}:{self._port}")
            return pool

        self._pool = await asyncio.to_thread(_init)

    async def disconnect(self) -> None:
        if self._pool is not None:
            await asyncio.to_thread(self._pool.close)
            self._pool = None

    async def execute(self, request: QueryRequest) -> QueryResult:
        if self._pool is None:
            return QueryResult(ref=request.ref, status=QueryStatus.ERROR, error_message="not connected")

        template: str | None = None
        if request.ref.workload == "snb_iv2":
            from graph_bench.workloads.snb_iv2.queries import query_template_for
            template = query_template_for("nebulagraph", request.ref.id)
        elif request.ref.workload == "snb_bi":
            from graph_bench.workloads.snb_bi.queries import query_template_for
            template = query_template_for("nebulagraph", request.ref.id)
        elif request.ref.workload == "finbench":
            from graph_bench.workloads.finbench.queries import query_template_for
            template = query_template_for("nebulagraph", request.ref.id)
        else:
            template = _QUERY_CATALOG.get(request.ref.id)
        if template is None:
            return QueryResult(
                ref=request.ref,
                status=QueryStatus.ERROR,
                error_message=f"unknown query id {request.ref.id!r}",
            )

        try:
            ngql = template.format(space=self._space, **request.params)
        except KeyError as exc:
            return QueryResult(
                ref=request.ref,
                status=QueryStatus.ERROR,
                error_message=f"missing parameter {exc.args[0]!r}",
            )

        def _exec_sync() -> tuple[bool, int, str]:
            session = self._pool.get_session(self._user, self._password)
            try:
                result = session.execute(ngql)
                if not result.is_succeeded():
                    return False, 0, result.error_msg() or "unknown error"
                rows = result.row_size() if hasattr(result, "row_size") else 0
                return True, rows, ""
            finally:
                session.release()

        try:
            ok, rows, err = await asyncio.to_thread(_exec_sync)
        except Exception as exc:  # noqa: BLE001
            return QueryResult(
                ref=request.ref,
                status=QueryStatus.ERROR,
                error_message=f"{type(exc).__name__}: {exc}",
            )

        if not ok:
            return QueryResult(
                ref=request.ref,
                status=QueryStatus.ERROR,
                error_message=err,
            )

        result_hash = hashlib.blake2b(
            f"{request.ref}|{ngql}".encode(), digest_size=8
        ).hexdigest()
        return QueryResult(
            ref=request.ref,
            status=QueryStatus.OK,
            row_count=int(rows),
            result_hash=result_hash,
        )

    async def cluster_status(self) -> ClusterStatus:
        if self._pool is None:
            return ClusterStatus(node_count=0, healthy_nodes=0)

        def _status_sync() -> tuple[int, int]:
            session = self._pool.get_session(self._user, self._password)
            try:
                # SHOW HOSTS reports storage hosts. Online == healthy.
                result = session.execute("SHOW HOSTS")
                if not result.is_succeeded():
                    return 0, 0
                rows = result.rows()
                healthy = sum(
                    1
                    for row in rows
                    if row.values and len(row.values) > 2 and "online" in str(row.values[2]).lower()
                )
                return len(rows), healthy
            finally:
                session.release()

        try:
            total, healthy = await asyncio.to_thread(_status_sync)
        except Exception:  # noqa: BLE001
            return ClusterStatus(node_count=0, healthy_nodes=0)
        return ClusterStatus(node_count=total, healthy_nodes=healthy, version="nebulagraph")

    async def add_node(self, spec: NodeSpec) -> None:
        """Register a new storaged host with the running cluster.

        `spec.hostname` is expected in the form `<host>:<port>` (e.g.,
        `storaged-reserve:9779`); a bare hostname defaults to port 9779.
        """
        if self._pool is None:
            raise RuntimeError("driver not connected")
        host_part = spec.hostname if ":" in spec.hostname else f"{spec.hostname}:9779"
        ngql = f'ADD HOSTS "{host_part.split(":", 1)[0]}":{host_part.split(":", 1)[1]}'

        def _add_sync() -> tuple[bool, str]:
            session = self._pool.get_session(self._user, self._password)
            try:
                result = session.execute(ngql)
                return result.is_succeeded(), result.error_msg() or ""
            finally:
                session.release()

        ok, err = await asyncio.to_thread(_add_sync)
        if not ok:
            raise RuntimeError(f"ADD HOSTS failed: {err}")


def _factory(config: dict[str, Any]) -> NebulaGraphDriver:
    return NebulaGraphDriver(
        host=str(config.get("host", "graphd0")),
        port=int(config.get("port", 9669)),
        user=str(config.get("user", "root")),
        password=str(config.get("password", "nebula")),
        space=str(config.get("space", "snb_demo")),
        meta_host=str(config.get("meta_host", "metad0")),
        meta_port=int(config.get("meta_port", 9559)),
        pool_size=int(config.get("pool_size", 32)),
    )


# Re-register: replace the stub entry with the real implementation.
DriverRegistry.register("nebulagraph", _factory, overwrite=True)


# Dummy reference to the catalog so it is exposed for inspection from outside.
QUERY_CATALOG: dict[str, str] = dict(_QUERY_CATALOG)


def query_template(ref: QueryRef) -> str | None:
    """Return the nGQL template for a given QueryRef, or None if unknown."""
    return _QUERY_CATALOG.get(ref.id)
