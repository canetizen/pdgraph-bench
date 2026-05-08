# Created by: Mustafa Can Caliskan
# Date: 2026-05-08

"""Memgraph driver — Bolt protocol via the official `neo4j` Python driver.

Memgraph 2.x speaks the same Bolt wire format as Neo4j 4/5, so the upstream
`neo4j` driver works unchanged. Cypher catalog is identical to Neo4j's;
queries use the SNB Iv2 conventions (Person/id property).
"""

from __future__ import annotations

import asyncio
import hashlib
from typing import Any

from neo4j import GraphDatabase

from graph_bench.domain import (
    ClusterStatus,
    NodeSpec,
    QueryRef,
    QueryRequest,
    QueryResult,
    QueryStatus,
)
from graph_bench.drivers.registry import DriverRegistry


_CYPHER_CATALOG: dict[str, str] = {
    "IS1": (
        "MATCH (p:Person {id: $vid}) "
        "RETURN p.firstName AS firstName, p.lastName AS lastName, "
        "p.age AS age, p.country AS country"
    ),
    "IS2": "MATCH (p:Person {id: $vid})-[:KNOWS]->(f:Person) RETURN f.id AS id",
    "IS3": (
        "MATCH (p:Person {id: $vid})-[:KNOWS]->()-[:KNOWS]->(f:Person) "
        "RETURN DISTINCT f.id AS id"
    ),
    "IU1": (
        "MATCH (s:Person {id: $srcId}), (d:Person {id: $dstId}) "
        "CREATE (s)-[:KNOWS {creationDate: $ts}]->(d)"
    ),
}


class MemgraphDriver:
    name = "memgraph"

    def __init__(
        self,
        *,
        host: str = "memgraph0",
        port: int = 7687,
        user: str = "",
        password: str = "",
    ) -> None:
        self._uri = f"bolt://{host}:{port}"
        self._auth = (user, password) if user else None
        self._driver: Any = None

    async def connect(self) -> None:
        def _init():
            return GraphDatabase.driver(self._uri, auth=self._auth)
        self._driver = await asyncio.to_thread(_init)
        # Eagerly verify the connection so a misconfigured URI fails fast.
        await asyncio.to_thread(self._driver.verify_connectivity)

    async def disconnect(self) -> None:
        if self._driver is not None:
            await asyncio.to_thread(self._driver.close)
        self._driver = None

    async def execute(self, request: QueryRequest) -> QueryResult:
        if self._driver is None:
            return QueryResult(ref=request.ref, status=QueryStatus.ERROR, error_message="not connected")
        cypher = _CYPHER_CATALOG.get(request.ref.id)
        if cypher is None:
            return QueryResult(
                ref=request.ref,
                status=QueryStatus.ERROR,
                error_message=f"unknown query id {request.ref.id!r}",
            )
        params: dict[str, Any] = {}
        if "vid" in request.params:
            params["vid"] = int(request.params["vid"])
        if "src" in request.params:
            params["srcId"] = int(request.params["src"])
        if "dst" in request.params:
            params["dstId"] = int(request.params["dst"])
        if "ts" in request.params:
            params["ts"] = int(request.params["ts"])

        def _exec_sync() -> int:
            with self._driver.session() as s:
                rs = s.run(cypher, **params)
                return sum(1 for _ in rs)

        try:
            row_count = await asyncio.to_thread(_exec_sync)
        except Exception as exc:  # noqa: BLE001
            return QueryResult(
                ref=request.ref,
                status=QueryStatus.ERROR,
                error_message=f"{type(exc).__name__}: {exc}",
            )
        return QueryResult(
            ref=request.ref,
            status=QueryStatus.OK,
            row_count=row_count,
            result_hash=hashlib.blake2b(
                f"{request.ref}|{params}".encode(), digest_size=8
            ).hexdigest(),
        )

    async def cluster_status(self) -> ClusterStatus:
        if self._driver is None:
            return ClusterStatus(node_count=0, healthy_nodes=0)

        def _status_sync() -> tuple[int, int, str]:
            with self._driver.session() as s:
                version = s.run("CALL mg.version() YIELD version RETURN version").single()
                return 1, 1, version["version"] if version else ""

        try:
            n, h, v = await asyncio.to_thread(_status_sync)
            return ClusterStatus(node_count=n, healthy_nodes=h, version=str(v) or None)
        except Exception:  # noqa: BLE001
            return ClusterStatus(node_count=0, healthy_nodes=0)

    async def add_node(self, spec: NodeSpec) -> None:
        # Memgraph S5 scale-out is out of scope for Tier-2; the harness should
        # not call this in any registered Tier-2 scenario.
        raise NotImplementedError("Memgraph scale-out is not part of the Tier-2 scenario set")


def _factory(config: dict[str, Any]) -> MemgraphDriver:
    return MemgraphDriver(
        host=str(config.get("host", "memgraph0")),
        port=int(config.get("port", 7687)),
        user=str(config.get("user", "")),
        password=str(config.get("password", "")),
    )


DriverRegistry.register("memgraph", _factory, overwrite=True)
