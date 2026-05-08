# Created by: Mustafa Can Caliskan
# Date: 2026-05-08

"""LDBC SNB Interactive v2 loader for OrientDB (composite-merged-fk layout).

Bootstraps the `snb` graph DB, creates one V-class per VertexType + one
E-class per edge label, then bulk-inserts via the `/batch/<db>` JSON API
(transactional, `transactionLog` ON).
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Iterable

import httpx

from data.loaders._ldbc_snb_v2 import (
    LdbcDatasetLayout,
    iter_edge_rows,
    iter_fk_edge_rows,
    iter_vertex_rows,
)
from graph_bench.workloads.snb_iv2.schema import EDGES, VERTICES


_OSQL_TYPE = {
    "id": "LONG",
    "int": "INTEGER",
    "long": "LONG",
    "bigint": "LONG",
    "string": "STRING",
    "datetime": "LONG",
    "date": "LONG",
    "float": "FLOAT",
}


def _wait(client: httpx.Client, timeout_s: int = 240) -> None:
    deadline = time.time() + timeout_s
    last_err: Exception | None = None
    while time.time() < deadline:
        try:
            r = client.get("/listDatabases")
            if r.status_code in (200, 401):  # 401 = auth challenge → server up
                return
        except Exception as exc:  # noqa: BLE001
            last_err = exc
        time.sleep(3)
    raise RuntimeError(f"OrientDB not reachable: {last_err}")


def _exec_sql(client: httpx.Client, db: str, sql: str) -> None:
    r = client.post(f"/command/{db}/sql", json={"command": sql})
    if r.status_code >= 400 and "already exists" not in r.text.lower():
        raise RuntimeError(f"SQL failed [{r.status_code}]: {sql[:160]}\n{r.text[:240]}")


def _ensure_database(client: httpx.Client, db: str) -> None:
    """Create the `snb` graph DB if it doesn't already exist."""
    r = client.get(f"/database/{db}")
    if r.status_code == 200:
        return
    r = client.post(f"/database/{db}/plocal/graph")
    if r.status_code >= 400:
        raise RuntimeError(f"DB create failed [{r.status_code}]: {r.text[:240]}")


def _ensure_schema(client: httpx.Client, db: str) -> None:
    # Vertex classes
    for v in VERTICES:
        _exec_sql(client, db, f"CREATE CLASS {v.name} EXTENDS V")
        _exec_sql(client, db, f"CREATE PROPERTY {v.name}.id LONG")
        _exec_sql(client, db, f"CREATE INDEX {v.name}.id ON {v.name} (id) UNIQUE")
        for p in v.properties:
            if p.name == "id":
                continue
            _exec_sql(
                client, db,
                f"CREATE PROPERTY {v.name}.{p.name} {_OSQL_TYPE[p.dtype]}",
            )
    # Edge classes (standalone + FK-derived)
    edge_names: set[str] = {e.name for e in EDGES}
    for v in VERTICES:
        for fk in v.foreign_keys:
            edge_names.add(fk.edge_label)
    for name in sorted(edge_names):
        _exec_sql(client, db, f"CREATE CLASS {name} EXTENDS E")
    # Edge property types
    for e in EDGES:
        for p in e.properties:
            _exec_sql(
                client, db,
                f"CREATE PROPERTY {e.name}.{p.name} {_OSQL_TYPE[p.dtype]}",
            )


def _batched(it, size: int):
    chunk: list = []
    for x in it:
        chunk.append(x)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def _coerce(val):
    if val in (None, ""):
        return None
    return val


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Load LDBC SNB Iv2 dataset into OrientDB.")
    p.add_argument("--host", default="orientdb0")
    p.add_argument("--port", type=int, default=2480)
    p.add_argument("--user", default="root")
    p.add_argument("--password", default="rootpwd")
    p.add_argument("--database", default="snb")
    p.add_argument("--dataset", type=Path, required=True)
    p.add_argument("--mode", default="interactive", help="interactive | bi")
    p.add_argument("--batch", type=int, default=500)
    args = p.parse_args(argv)

    layout = LdbcDatasetLayout(base=args.dataset, mode=args.mode)
    if not layout.root.exists():
        raise SystemExit(f"dataset root {layout.root} does not exist")

    base_url = f"http://{args.host}:{args.port}"
    print(f"[loader] connecting to {base_url}")
    client = httpx.Client(base_url=base_url, auth=(args.user, args.password), timeout=120.0)
    _wait(client)

    print(f"[loader] (re)creating database '{args.database}'")
    # Drop + recreate for a clean slate.
    try:
        client.delete(f"/database/{args.database}")
    except Exception:
        pass
    _ensure_database(client, args.database)
    _ensure_schema(client, args.database)

    total_v = 0
    for v in VERTICES:
        n = 0
        for batch in _batched(
            ({"id": vid, **{k: _coerce(val) for k, val in props.items()}}
             for vid, props in iter_vertex_rows(v, layout)),
            args.batch,
        ):
            ops = [
                {"type": "c", "record": {"@class": v.name, **{k: x[k] for k in x if x[k] is not None}}}
                for x in batch
            ]
            r = client.post(
                f"/batch/{args.database}",
                json={"transaction": True, "operations": ops},
            )
            if r.status_code >= 400:
                raise RuntimeError(f"vertex batch failed: {r.status_code} {r.text[:200]}")
            n += len(batch)
        if n:
            print(f"[loader]   vertex {v.name}: {n}")
        total_v += n

    # Build a lookup from (label, id) → @rid for fast edge creation.
    print("[loader] building rid lookup")
    rid_lookup: dict[tuple[str, int], str] = {}
    for v in VERTICES:
        offset = 0
        while True:
            r = client.get(
                f"/query/{args.database}/sql/"
                f"SELECT @rid AS rid, id FROM {v.name} SKIP {offset} LIMIT 5000"
            )
            rows = r.json().get("result", [])
            if not rows:
                break
            for row in rows:
                rid = row.get("rid") or row.get("@rid")
                if rid:
                    rid_lookup[(v.name, int(row["id"]))] = rid
            offset += len(rows)
            if len(rows) < 5000:
                break

    total_e = 0
    for v in VERTICES:
        for fk in v.foreign_keys:
            if fk.direction == "in":
                src_label, dst_label = fk.target_label, v.name
            else:
                src_label, dst_label = v.name, fk.target_label
            n = 0
            for batch in _batched(iter_fk_edge_rows(v, fk, layout), args.batch):
                ops = []
                for src, dst, _ in batch:
                    a = rid_lookup.get((src_label, src))
                    b = rid_lookup.get((dst_label, dst))
                    if a and b:
                        ops.append({"type": "cmd", "language": "sql",
                                    "command": f"CREATE EDGE {fk.edge_label} FROM {a} TO {b}"})
                if ops:
                    r = client.post(
                        f"/batch/{args.database}",
                        json={"transaction": True, "operations": ops},
                    )
                    if r.status_code >= 400:
                        raise RuntimeError(f"edge batch failed: {r.status_code} {r.text[:200]}")
                    n += len(ops)
            if n:
                print(f"[loader]   edge   {fk.edge_label}: {n}  (FK {v.name}.{fk.column})")
            total_e += n

    for e in EDGES:
        prop_keys = [p.name for p in e.properties]
        n = 0
        for batch in _batched(iter_edge_rows(e, layout), args.batch):
            ops = []
            for src, dst, props in batch:
                a = rid_lookup.get((e.src_label, src))
                b = rid_lookup.get((e.dst_label, dst))
                if a and b:
                    set_clause = ""
                    if prop_keys:
                        kv = [f"{k} = {json.dumps(_coerce(props.get(k)))}"
                              for k in prop_keys if _coerce(props.get(k)) is not None]
                        if kv:
                            set_clause = " SET " + ", ".join(kv)
                    ops.append({"type": "cmd", "language": "sql",
                                "command": f"CREATE EDGE {e.name} FROM {a} TO {b}{set_clause}"})
            if ops:
                r = client.post(
                    f"/batch/{args.database}",
                    json={"transaction": True, "operations": ops},
                )
                if r.status_code >= 400:
                    raise RuntimeError(f"edge batch failed: {r.status_code} {r.text[:200]}")
                n += len(ops)
        if n:
            print(f"[loader]   edge   {e.name}: {n}")
        total_e += n

    client.close()
    print(f"[loader] vertices={total_v} edges={total_e}; OrientDB SNB Iv2 is benchmark-ready")
    return 0


if __name__ == "__main__":
    sys.exit(main())
