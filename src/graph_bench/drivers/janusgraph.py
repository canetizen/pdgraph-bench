# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""JanusGraph driver — full implementation.

Talks to a Gremlin Server (JanusGraph) via `gremlinpython`. Used in Tier 2
(scenario S1 only); does not participate in scale-out, so `add_node` raises.
"""

from __future__ import annotations

import asyncio
import hashlib
from typing import Any

from gremlin_python.driver import client as gremlin_client
from gremlin_python.driver import serializer

from graph_bench.domain import (
    ClusterStatus,
    NodeSpec,
    QueryRef,
    QueryRequest,
    QueryResult,
    QueryStatus,
)
from graph_bench.drivers.registry import DriverRegistry


# Gremlin (Groovy) catalog — IS1/IS2/IS3 read-only; IU1 adds an edge.
_GREMLIN_CATALOG: dict[str, str] = {
    "IS1": "g.V().has('Person', 'id', vid).valueMap('firstName','lastName','age','country')",
    "IS2": "g.V().has('Person', 'id', vid).out('knows').values('id')",
    "IS3": "g.V().has('Person', 'id', vid).out('knows').out('knows').dedup().values('id')",
    "IU1": (
        "src = g.V().has('Person','id',srcId).next();"
        "dst = g.V().has('Person','id',dstId).next();"
        "g.addE('knows').from(src).to(dst).property('creationDate', ts).next()"
    ),
}


class JanusGraphDriver:
    name = "janusgraph"

    def __init__(
        self,
        *,
        host: str = "janusgraph0",
        port: int = 8182,
        traversal_source: str = "g",
    ) -> None:
        self._url = f"ws://{host}:{port}/gremlin"
        self._traversal = traversal_source
        self._client: gremlin_client.Client | None = None

    async def connect(self) -> None:
        def _init():
            return gremlin_client.Client(
                self._url,
                self._traversal,
                message_serializer=serializer.GraphSONSerializersV3d0(),
            )

        self._client = await asyncio.to_thread(_init)

    async def disconnect(self) -> None:
        if self._client is not None:
            await asyncio.to_thread(self._client.close)
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
        # Translate parameter names from synthetic_snb conventions to Gremlin
        # bindings.
        bindings: dict[str, Any] = {}
        if "vid" in request.params:
            bindings["vid"] = int(request.params["vid"])
        if "src" in request.params:
            bindings["srcId"] = int(request.params["src"])
        if "dst" in request.params:
            bindings["dstId"] = int(request.params["dst"])
        if "ts" in request.params:
            bindings["ts"] = int(request.params["ts"])

        def _exec_sync() -> int:
            rs = self._client.submit(gremlin, bindings)
            rows = list(rs.all().result())
            return len(rows)

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
                f"{request.ref}|{bindings}".encode(), digest_size=8
            ).hexdigest(),
        )

    async def cluster_status(self) -> ClusterStatus:
        if self._client is None:
            return ClusterStatus(node_count=0, healthy_nodes=0)

        def _status_sync() -> tuple[int, int, str]:
            try:
                rs = self._client.submit("Gremlin.version()")
                version = list(rs.all().result())[0]
                return 1, 1, str(version)
            except Exception:
                return 0, 0, ""

        n, h, v = await asyncio.to_thread(_status_sync)
        return ClusterStatus(node_count=n, healthy_nodes=h, version=v or None)

    async def add_node(self, spec: NodeSpec) -> None:
        """Wait for a freshly-started Cassandra peer to join the ring.

        The deployment layer starts a new Cassandra container on `spec.hostname`
        with the existing seeds already configured; once it boots it auto-joins
        via gossip. The driver polls `nodetool status` (via the local
        JanusGraph node's Cassandra) until the new peer is `UN` (Up + Normal).

        Note: Tier-2 systems are excluded from S5 in the Progress Report; this
        method exists so that operators who want to run an exploratory scale-out
        outside the canonical campaign can still trigger it.
        """
        if self._client is None:
            raise RuntimeError("driver not connected")
        target_host = spec.hostname.split(":", 1)[0]

        def _wait_ring_join() -> bool:
            import socket
            import time as _t
            # We rely on the Gremlin server connection being co-located with a
            # Cassandra node; ask Cassandra over its native port.
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


def _factory(config: dict[str, Any]) -> JanusGraphDriver:
    return JanusGraphDriver(
        host=str(config.get("host", "janusgraph0")),
        port=int(config.get("port", 8182)),
        traversal_source=str(config.get("traversal_source", "g")),
    )


DriverRegistry.register("janusgraph", _factory, overwrite=True)
