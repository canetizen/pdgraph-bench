# Created by: Mustafa Can Caliskan
# Date: 2026-05-08

"""OrientDB driver — REST API via httpx.

OrientDB's HTTP API at `/command/<db>/sql` evaluates an OrientSQL command
against the named database. We use it for the SNB Iv2 query catalog
(IS1/IS2/IS3 reads + IU1 edge create). `pyorient` is unmaintained and
Python 3.12-incompatible, so going through HTTP keeps the dep tree clean.
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


# OrientSQL templates. `:vid`, `:srcId`, `:dstId`, `:ts` are positional
# parameters substituted at request time (sent as JSON `parameters`).
_OSQL_CATALOG: dict[str, str] = {
    "IS1": (
        "SELECT firstName, lastName, age, country FROM Person WHERE id = :vid"
    ),
    "IS2": (
        "SELECT EXPAND(out('KNOWS')) FROM Person WHERE id = :vid"
    ),
    "IS3": (
        "SELECT EXPAND(out('KNOWS').out('KNOWS')).id "
        "FROM Person WHERE id = :vid"
    ),
    "IU1": (
        "CREATE EDGE KNOWS "
        "FROM (SELECT FROM Person WHERE id = :srcId) "
        "TO (SELECT FROM Person WHERE id = :dstId) "
        "SET creationDate = :ts"
    ),
}


class OrientDBDriver:
    name = "orientdb"

    def __init__(
        self,
        *,
        host: str = "orientdb0",
        port: int = 2480,
        database: str = "snb",
        user: str = "root",
        password: str = "rootpwd",
    ) -> None:
        self._base = f"http://{host}:{port}"
        self._database = database
        self._auth = (user, password)
        self._client: httpx.AsyncClient | None = None

    async def connect(self) -> None:
        self._client = httpx.AsyncClient(
            base_url=self._base, auth=self._auth, timeout=60.0
        )

    async def disconnect(self) -> None:
        if self._client is not None:
            await self._client.aclose()
        self._client = None

    async def execute(self, request: QueryRequest) -> QueryResult:
        if self._client is None:
            return QueryResult(ref=request.ref, status=QueryStatus.ERROR, error_message="not connected")
        sql = _OSQL_CATALOG.get(request.ref.id)
        if sql is None:
            return QueryResult(
                ref=request.ref, status=QueryStatus.ERROR,
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

        try:
            resp = await self._client.post(
                f"/command/{self._database}/sql",
                json={"command": sql, "parameters": params},
            )
        except Exception as exc:  # noqa: BLE001
            return QueryResult(
                ref=request.ref, status=QueryStatus.ERROR,
                error_message=f"{type(exc).__name__}: {exc}",
            )
        if resp.status_code >= 400:
            return QueryResult(
                ref=request.ref, status=QueryStatus.ERROR,
                error_message=f"HTTP {resp.status_code}: {resp.text[:200]}",
            )
        try:
            data = resp.json().get("result", [])
            row_count = len(data) if isinstance(data, list) else 1
        except Exception:  # noqa: BLE001
            row_count = 0
        return QueryResult(
            ref=request.ref, status=QueryStatus.OK, row_count=row_count,
            result_hash=hashlib.blake2b(
                f"{request.ref}|{params}".encode(), digest_size=8
            ).hexdigest(),
        )

    async def cluster_status(self) -> ClusterStatus:
        if self._client is None:
            return ClusterStatus(node_count=0, healthy_nodes=0)
        try:
            resp = await self._client.get(f"/server")
            if resp.status_code != 200:
                return ClusterStatus(node_count=0, healthy_nodes=0)
            info = resp.json()
            members = info.get("distributedCfg", {}).get("members", [])
            return ClusterStatus(
                node_count=len(members) or 1, healthy_nodes=len(members) or 1,
                version=info.get("version", "")
            )
        except Exception:  # noqa: BLE001
            return ClusterStatus(node_count=0, healthy_nodes=0)

    async def add_node(self, spec: NodeSpec) -> None:
        # OrientDB Hazelcast cluster auto-joins; we only confirm reachability.
        import socket
        target_host = spec.hostname.split(":", 1)[0]
        target_port = int(spec.hostname.rsplit(":", 1)[1]) if ":" in spec.hostname else 2480

        def _probe() -> bool:
            import time as _t
            deadline = _t.time() + 180
            while _t.time() < deadline:
                try:
                    with socket.create_connection((target_host, target_port), timeout=3):
                        return True
                except OSError:
                    _t.sleep(2)
            return False

        joined = await asyncio.to_thread(_probe)
        if not joined:
            raise RuntimeError(f"OrientDB peer {target_host}:{target_port} not reachable")


def _factory(config: dict[str, Any]) -> OrientDBDriver:
    return OrientDBDriver(
        host=str(config.get("host", "orientdb0")),
        port=int(config.get("port", 2480)),
        database=str(config.get("database", "snb")),
        user=str(config.get("user", "root")),
        password=str(config.get("password", "rootpwd")),
    )


DriverRegistry.register("orientdb", _factory, overwrite=True)
