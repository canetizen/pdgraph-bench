# Created by: Mustafa Can Caliskan
# Date: 2026-05-08

"""LDBC SNB Interactive v2 loader for Memgraph (composite-merged-fk layout).

Bulk-loads via Cypher `UNWIND $batch AS row CREATE (...)` over Bolt.
Memgraph 2.x is in-memory; even SF=1 fits comfortably with default heap.
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from typing import Any, Iterable

from neo4j import GraphDatabase

from data.loaders._ldbc_snb_v2 import (
    LdbcDatasetLayout,
    iter_edge_rows,
    iter_fk_edge_rows,
    iter_vertex_rows,
)
from graph_bench.workloads.snb_iv2.schema import EDGES, VERTICES


def _wait_for_bolt(uri: str, auth, timeout_s: int = 180):
    deadline = time.time() + timeout_s
    last_err: Exception | None = None
    while time.time() < deadline:
        try:
            d = GraphDatabase.driver(uri, auth=auth)
            d.verify_connectivity()
            return d
        except Exception as exc:  # noqa: BLE001
            last_err = exc
            time.sleep(2)
    raise RuntimeError(f"Memgraph Bolt not reachable: {last_err}")


def _ensure_indexes(driver, vertex_labels: Iterable[str]) -> None:
    """One composite index per (label, id) so MATCH lookups are O(log n)."""
    with driver.session() as s:
        for label in vertex_labels:
            try:
                s.run(f"CREATE INDEX ON :{label}(id)")
            except Exception as exc:  # noqa: BLE001
                # `INDEX ALREADY EXISTS` is fine; anything else we surface.
                if "already exists" not in str(exc).lower():
                    print(f"[loader] (index warning) {label}.id: {exc}")


def _batched(it, size: int):
    chunk: list = []
    for x in it:
        chunk.append(x)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Load LDBC SNB Iv2 dataset into Memgraph.")
    p.add_argument("--host", default="memgraph0")
    p.add_argument("--port", type=int, default=7687)
    p.add_argument("--user", default="")
    p.add_argument("--password", default="")
    p.add_argument("--dataset", type=Path, required=True)
    p.add_argument("--mode", default="interactive", help="interactive | bi")
    p.add_argument("--batch", type=int, default=2000)
    args = p.parse_args(argv)

    layout = LdbcDatasetLayout(base=args.dataset, mode=args.mode)
    if not layout.root.exists():
        raise SystemExit(f"dataset root {layout.root} does not exist")

    uri = f"bolt://{args.host}:{args.port}"
    auth = (args.user, args.password) if args.user else None
    print(f"[loader] connecting to {uri}")
    driver = _wait_for_bolt(uri, auth)

    print("[loader] dropping existing graph + applying constraints")
    with driver.session() as s:
        s.run("MATCH (n) DETACH DELETE n")
    _ensure_indexes(driver, [v.name for v in VERTICES])

    total_v = 0
    with driver.session() as s:
        for v in VERTICES:
            cypher = f"UNWIND $rows AS row CREATE (n:{v.name}) SET n = row"
            n = 0
            for batch in _batched(
                ({"id": vid, **{k: _coerce(val) for k, val in props.items()}}
                 for vid, props in iter_vertex_rows(v, layout)),
                args.batch,
            ):
                s.run(cypher, rows=batch)
                n += len(batch)
            if n:
                print(f"[loader]   vertex {v.name}: {n}")
            total_v += n

    total_e = 0
    with driver.session() as s:
        for v in VERTICES:
            for fk in v.foreign_keys:
                if fk.direction == "in":
                    src_label, dst_label = fk.target_label, v.name
                else:
                    src_label, dst_label = v.name, fk.target_label
                cypher = (
                    f"UNWIND $rows AS row "
                    f"MATCH (a:{src_label} {{id: row.src}}), (b:{dst_label} {{id: row.dst}}) "
                    f"CREATE (a)-[:{fk.edge_label}]->(b)"
                )
                n = 0
                for batch in _batched(
                    ({"src": src, "dst": dst}
                     for src, dst, _ in iter_fk_edge_rows(v, fk, layout)),
                    args.batch,
                ):
                    s.run(cypher, rows=batch)
                    n += len(batch)
                if n:
                    print(f"[loader]   edge   {fk.edge_label}: {n}  (FK {v.name}.{fk.column})")
                total_e += n

        for e in EDGES:
            prop_keys = [p.name for p in e.properties]
            prop_set = (
                "{" + ", ".join(f"{k}: row.{k}" for k in prop_keys) + "}"
                if prop_keys else ""
            )
            cypher = (
                f"UNWIND $rows AS row "
                f"MATCH (a:{e.src_label} {{id: row.src}}), (b:{e.dst_label} {{id: row.dst}}) "
                f"CREATE (a)-[:{e.name} {prop_set}]->(b)"
            )
            n = 0
            for batch in _batched(
                ({"src": src, "dst": dst,
                  **{k: _coerce(props.get(k)) for k in prop_keys}}
                 for src, dst, props in iter_edge_rows(e, layout)),
                args.batch,
            ):
                s.run(cypher, rows=batch)
                n += len(batch)
            if n:
                print(f"[loader]   edge   {e.name}: {n}")
            total_e += n

    driver.close()
    print(f"[loader] vertices={total_v} edges={total_e}; Memgraph SNB Iv2 is benchmark-ready")
    return 0


def _coerce(val):
    """Memgraph rejects None for typed properties; keep them as native types
    when possible and stringify the rest. Numbers and bools pass through."""
    if val is None or val == "":
        return None
    if isinstance(val, (int, float, bool)):
        return val
    return val


if __name__ == "__main__":
    sys.exit(main())
