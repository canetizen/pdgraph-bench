# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""Apache HugeGraph driver — full implementation.

Talks to a HugeGraph server via its REST/Gremlin endpoint using `httpx`. The
benchmark uses HugeGraph's Gremlin endpoint for queries (fewer round trips,
parameterised payloads). Tier 2 in this study: scenario S1 only; does not
participate in scale-out, so `add_node` raises.
"""

from __future__ import annotations

import asyncio
import hashlib
from typing import Any

import httpx

from graph_bench.domain import (
    ClusterStatus,
    NodeSpec,
    QueryRef,
    QueryRequest,
    QueryResult,
    QueryStatus,
)
from graph_bench.drivers.registry import DriverRegistry


# Same Gremlin shapes as the JanusGraph driver — HugeGraph speaks Gremlin too,
# only the endpoint and authentication differ.
_GREMLIN_CATALOG: dict[str, str] = {
    "IS1": "g.V().has('Person', 'id', vid).valueMap('firstName','lastName','age','country')",
    "IS2": "g.V().has('Person', 'id', vid).out('knows').values('id')",
    "IS3": "g.V().has('Person', 'id', vid).out('knows').out('knows').dedup().values('id')",
    "IU1": (
        "g.V().has('Person','id',srcId).next();"
        "g.V().has('Person','id',dstId).next();"
        "g.V().has('Person','id',srcId).addE('knows').to(g.V().has('Person','id',dstId)).property('creationDate', ts).next()"
    ),
}


class HugeGraphDriver:
    name = "hugegraph"

    def __init__(
        self,
        *,
        host: str = "hugegraph0",
        port: int = 8080,
        graph: str = "hugegraph",
        timeout_s: float = 30.0,
    ) -> None:
        self._base_url = f"http://{host}:{port}"
        self._graph = graph
        self._timeout = timeout_s
        self._client: httpx.AsyncClient | None = None

    async def connect(self) -> None:
        self._client = httpx.AsyncClient(
            base_url=self._base_url, timeout=self._timeout
        )
        # Validate graph existence by hitting the schema endpoint.
        resp = await self._client.get(f"/apis/graphs/{self._graph}")
        resp.raise_for_status()

    async def disconnect(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def execute(self, request: QueryRequest) -> QueryResult:
        if self._client is None:
            return QueryResult(ref=request.ref, status=QueryStatus.ERROR, error_message="not connected")
        gremlin = _GREMLIN_CATALOG.get(request.ref.id)
        if gremlin is None:
            return QueryResult(
                ref=request.ref,
                status=QueryStatus.ERROR,
                error_message=f"unknown query id {request.ref.id!r}",
            )

        bindings: dict[str, Any] = {}
        if "vid" in request.params:
            bindings["vid"] = int(request.params["vid"])
        if "src" in request.params:
            bindings["srcId"] = int(request.params["src"])
        if "dst" in request.params:
            bindings["dstId"] = int(request.params["dst"])
        if "ts" in request.params:
            bindings["ts"] = int(request.params["ts"])

        payload = {
            "gremlin": gremlin,
            "bindings": bindings,
            "language": "gremlin-groovy",
            "aliases": {"graph": self._graph, "g": f"__g_{self._graph}"},
        }

        try:
            resp = await self._client.post("/apis/gremlin", json=payload)
        except httpx.HTTPError as exc:
            return QueryResult(ref=request.ref, status=QueryStatus.ERROR, error_message=str(exc))

        if resp.status_code >= 400:
            return QueryResult(
                ref=request.ref,
                status=QueryStatus.ERROR,
                error_message=f"HTTP {resp.status_code}: {resp.text[:200]}",
            )

        body = resp.json()
        data = body.get("result", {}).get("data", [])
        return QueryResult(
            ref=request.ref,
            status=QueryStatus.OK,
            row_count=len(data) if isinstance(data, list) else 0,
            result_hash=hashlib.blake2b(
                f"{request.ref}|{bindings}".encode(), digest_size=8
            ).hexdigest(),
        )

    async def cluster_status(self) -> ClusterStatus:
        if self._client is None:
            return ClusterStatus(node_count=0, healthy_nodes=0)
        try:
            resp = await self._client.get("/apis/version")
            version = resp.json().get("versions", {}).get("server") if resp.status_code == 200 else None
        except httpx.HTTPError:
            version = None
        return ClusterStatus(node_count=1, healthy_nodes=1, version=version)

    async def add_node(self, spec: NodeSpec) -> None:
        """Confirm that a new Cassandra peer (the HugeGraph storage backend) is reachable.

        Identical model to the JanusGraph driver: the deploy layer starts a
        Cassandra container on the reserve node; the driver polls until the
        new peer accepts CQL connections on port 9042.
        """
        target_host = spec.hostname.split(":", 1)[0]

        def _wait_ring_join() -> bool:
            import socket
            import time as _t
            deadline = _t.time() + 180
            while _t.time() < deadline:
                try:
                    with socket.create_connection((target_host, 9042), timeout=3) as _s:
                        return True
                except OSError:
                    _t.sleep(2)
            return False

        joined = await asyncio.to_thread(_wait_ring_join)
        if not joined:
            raise RuntimeError(f"Cassandra peer {target_host!r} did not become reachable on 9042")


def _factory(config: dict[str, Any]) -> HugeGraphDriver:
    return HugeGraphDriver(
        host=str(config.get("host", "hugegraph0")),
        port=int(config.get("port", 8080)),
        graph=str(config.get("graph", "hugegraph")),
        timeout_s=float(config.get("timeout_s", 30.0)),
    )


DriverRegistry.register("hugegraph", _factory, overwrite=True)
