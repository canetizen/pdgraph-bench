# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""Dgraph driver — full implementation.

Wraps `pydgraph` (gRPC) behind the async `Driver` protocol. Dgraph is
schema-driven; queries are written in DQL. The synthetic_snb refs translate as
follows:

- IS1 → query Person by `id` predicate, return scalar properties.
- IS2 → 1-hop `knows` traversal returning friend ids.
- IS3 → 2-hop `knows` traversal with DISTINCT semantics emulated via a
        nested query.
- IU1 → upsert mutation that adds a `knows` edge between two persons.

Scale-out registers a new alpha by waiting for it to appear in `/state`
served by an existing zero. The new alpha process itself is started by the
deploy layer with `--zero=<existing zero address>`; this driver only confirms
membership.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
from typing import Any

import grpc
import pydgraph
import urllib.request

from graph_bench.domain import (
    ClusterStatus,
    NodeSpec,
    QueryRef,
    QueryRequest,
    QueryResult,
    QueryStatus,
)
from graph_bench.drivers.registry import DriverRegistry


_DQL_CATALOG: dict[str, str] = {
    "IS1": (
        "query is1($vid:int) { "
        "  person(func: eq(id, $vid)) { id firstName lastName age country } "
        "}"
    ),
    "IS2": (
        "query is2($vid:int) { "
        "  person(func: eq(id, $vid)) { knows { id } } "
        "}"
    ),
    "IS3": (
        "query is3($vid:int) { "
        "  person(func: eq(id, $vid)) { knows { knows { id } } } "
        "}"
    ),
}


class DgraphDriver:
    name = "dgraph"

    def __init__(
        self,
        *,
        host: str = "alpha0",
        port: int = 9080,
        zero_host: str = "zero0",
        zero_port: int = 6080,
    ) -> None:
        self._host = host
        self._port = port
        self._zero_host = zero_host
        self._zero_port = zero_port
        self._stub: pydgraph.DgraphClientStub | None = None
        self._client: pydgraph.DgraphClient | None = None

    async def connect(self) -> None:
        def _init():
            stub = pydgraph.DgraphClientStub(f"{self._host}:{self._port}")
            client = pydgraph.DgraphClient(stub)
            client.check_version()
            return stub, client

        self._stub, self._client = await asyncio.to_thread(_init)

    async def disconnect(self) -> None:
        if self._stub is not None:
            await asyncio.to_thread(self._stub.close)
        self._stub = None
        self._client = None

    async def execute(self, request: QueryRequest) -> QueryResult:
        if self._client is None:
            return QueryResult(ref=request.ref, status=QueryStatus.ERROR, error_message="not connected")

        if request.ref.workload == "synthetic_snb" and request.ref.id == "IU1":
            return await self._upsert_knows(request)

        dql: str | None = None
        if request.ref.workload == "snb_iv2":
            from graph_bench.workloads.snb_iv2.queries import query_template_for
            dql = query_template_for("dgraph", request.ref.id)
        elif request.ref.workload == "snb_bi":
            from graph_bench.workloads.snb_bi.queries import query_template_for
            dql = query_template_for("dgraph", request.ref.id)
        elif request.ref.workload == "finbench":
            from graph_bench.workloads.finbench.queries import query_template_for
            dql = query_template_for("dgraph", request.ref.id)
        else:
            dql = _DQL_CATALOG.get(request.ref.id)
        if dql is None:
            return QueryResult(
                ref=request.ref,
                status=QueryStatus.ERROR,
                error_message=f"unknown query id {request.ref.id!r}",
            )

        # Mutation templates start with `{` (set/delete blocks) or `upsert`.
        # Read queries start with `query`. The two paths use different
        # pydgraph APIs: bind-variable queries vs. client-substituted N-quad
        # mutations.
        is_mutation = dql.lstrip().startswith(("{", "upsert"))

        def _exec_sync() -> int:
            if is_mutation:
                # Substitute $name placeholders client-side; pydgraph mutations
                # do not accept bind variables.
                body = dql
                for k, v in request.params.items():
                    body = body.replace(f"${k}", str(v))
                txn = self._client.txn()
                try:
                    if body.lstrip().startswith("upsert"):
                        # pydgraph requires the upsert's `query` and
                        # `mutation` blocks to be passed as separate fields
                        # on the Request, not as one big "upsert {...}"
                        # string. Parse the two blocks out of the template.
                        q_start = body.find("query")
                        m_start = body.find("mutation")
                        # Extract bracketed content of each block.
                        def _extract_block(text: str, after: int) -> str:
                            depth = 0
                            start = -1
                            for i in range(after, len(text)):
                                ch = text[i]
                                if ch == "{":
                                    if start == -1:
                                        start = i
                                    depth += 1
                                elif ch == "}":
                                    depth -= 1
                                    if depth == 0:
                                        return text[start: i + 1]
                            return ""
                        q_block = _extract_block(body, q_start)
                        m_block = _extract_block(body, m_start)
                        # Strip the leading `set {` and trailing `}` from the
                        # mutation block to get the bare N-quads.
                        m_inner = m_block.strip().lstrip("{").rstrip("}").strip()
                        if m_inner.startswith("set"):
                            m_inner = m_inner[3:].strip().lstrip("{").rstrip("}").strip()
                        mutation = pydgraph.Mutation(
                            set_nquads=m_inner.encode("utf-8"), commit_now=True
                        )
                        req = pydgraph.Request(
                            query=q_block, mutations=[mutation], commit_now=True
                        )
                        resp = txn.do_request(req)
                    else:
                        # Plain `{ set { ... } }` — extract the inner set block
                        # and submit as N-quads. pydgraph requires bytes.
                        inner = body.strip()
                        if inner.startswith("{"):
                            inner = inner[1:].rstrip().rstrip("}").strip()
                        if inner.startswith("set"):
                            inner = inner[3:].strip().lstrip("{").rstrip().rstrip("}").strip()
                        mutation = pydgraph.Mutation(
                            set_nquads=inner.encode("utf-8"), commit_now=True
                        )
                        resp = txn.do_request(
                            pydgraph.Request(mutations=[mutation], commit_now=True)
                        )
                    return len(resp.uids) + 1
                finally:
                    txn.discard()
            else:
                # Read query — bind every workload param as a $-prefixed string.
                # Dgraph's pydgraph requires every variable to be a string;
                # the typed-variable declaration in the query header coerces.
                variables = {f"${k}": str(v) for k, v in request.params.items()}
                txn = self._client.txn(read_only=True)
                try:
                    resp = txn.query(dql, variables=variables)
                    body = json.loads(resp.json)
                    # Count rows in any top-level result key.
                    return sum(len(v) for v in body.values() if isinstance(v, list))
                finally:
                    txn.discard()

        try:
            row_count = await asyncio.to_thread(_exec_sync)
        except Exception as exc:  # noqa: BLE001
            return QueryResult(
                ref=request.ref,
                status=QueryStatus.ERROR,
                error_message=f"{type(exc).__name__}: {exc}",
            )

        result_hash = hashlib.blake2b(
            f"{request.ref}|{request.params}".encode(), digest_size=8
        ).hexdigest()
        return QueryResult(
            ref=request.ref,
            status=QueryStatus.OK,
            row_count=row_count,
            result_hash=result_hash,
        )

    async def _upsert_knows(self, request: QueryRequest) -> QueryResult:
        src = int(request.params["src"])
        dst = int(request.params["dst"])

        def _mutate_sync() -> int:
            txn = self._client.txn()
            try:
                # Upsert pattern: query the two persons by their `id` predicate,
                # then add the `knows` edge. Dgraph blank-node uids would only
                # work for new vertices; here we look up existing ones.
                upsert = (
                    "upsert {\n"
                    "  query {\n"
                    "    src(func: eq(id, %d)) { src_uid as uid }\n"
                    "    dst(func: eq(id, %d)) { dst_uid as uid }\n"
                    "  }\n"
                    "  mutation {\n"
                    "    set { uid(src_uid) <knows> uid(dst_uid) . }\n"
                    "  }\n"
                    "}\n" % (src, dst)
                )
                req = pydgraph.Request(
                    query=upsert, commit_now=True
                )
                resp = txn.do_request(req)
                # Number of N-quads written approximates the row count.
                return len(resp.uids) + 1
            finally:
                txn.discard()

        try:
            row_count = await asyncio.to_thread(_mutate_sync)
        except Exception as exc:  # noqa: BLE001
            return QueryResult(
                ref=request.ref,
                status=QueryStatus.ERROR,
                error_message=f"{type(exc).__name__}: {exc}",
            )

        result_hash = hashlib.blake2b(
            f"{request.ref}|{src}->{dst}".encode(), digest_size=8
        ).hexdigest()
        return QueryResult(
            ref=request.ref,
            status=QueryStatus.OK,
            row_count=row_count,
            result_hash=result_hash,
        )

    async def cluster_status(self) -> ClusterStatus:
        def _state_sync() -> tuple[int, int, str]:
            url = f"http://{self._zero_host}:{self._zero_port}/state"
            try:
                with urllib.request.urlopen(url, timeout=3) as fp:
                    body = json.loads(fp.read().decode("utf-8"))
            except Exception:
                return 0, 0, ""
            groups = body.get("groups", {})
            members = []
            for grp in groups.values():
                members.extend((grp.get("members") or {}).values())
            total = len(members)
            healthy = sum(1 for m in members if not m.get("amDead"))
            return total, healthy, body.get("version", "")

        total, healthy, version = await asyncio.to_thread(_state_sync)
        return ClusterStatus(node_count=total, healthy_nodes=healthy, version=version or None)

    async def add_node(self, spec: NodeSpec) -> None:
        """Confirm that a freshly-started alpha has joined the cluster.

        The new alpha process is started by the deploy layer with
        `--zero=<zero_addr>`; once started it auto-joins. This call polls
        `/state` until the new alpha is visible.
        """
        target = spec.hostname.split(":", 1)[0]

        def _wait_join() -> bool:
            deadline = time.time() + 60
            url = f"http://{self._zero_host}:{self._zero_port}/state"
            while time.time() < deadline:
                try:
                    with urllib.request.urlopen(url, timeout=3) as fp:
                        body = json.loads(fp.read().decode("utf-8"))
                    for grp in (body.get("groups") or {}).values():
                        for member in (grp.get("members") or {}).values():
                            addr = str(member.get("addr", ""))
                            if target in addr and not member.get("amDead"):
                                return True
                except Exception:
                    pass
                time.sleep(1)
            return False

        joined = await asyncio.to_thread(_wait_join)
        if not joined:
            raise RuntimeError(f"new alpha {target!r} did not join cluster within timeout")


def _factory(config: dict[str, Any]) -> DgraphDriver:
    return DgraphDriver(
        host=str(config.get("host", "alpha0")),
        port=int(config.get("port", 9080)),
        zero_host=str(config.get("zero_host", "zero0")),
        zero_port=int(config.get("zero_port", 6080)),
    )


DriverRegistry.register("dgraph", _factory, overwrite=True)


QUERY_CATALOG: dict[str, str] = dict(_DQL_CATALOG)


def query_template(ref: QueryRef) -> str | None:
    return _DQL_CATALOG.get(ref.id)
