# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""LDBC SNB Interactive v2 loader for ArangoDB (composite-merged-fk layout)."""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from arango import ArangoClient

from data.loaders._ldbc_snb_v2 import (
    LdbcDatasetLayout,
    batched,
    iter_edge_rows,
    iter_fk_edge_rows,
    iter_vertex_rows,
)
from graph_bench.workloads.snb_iv2.ddl import arangodb_collections
from graph_bench.workloads.snb_iv2.schema import EDGES, VERTICES


def _wait_for_coordinator(client: ArangoClient, user: str, password: str, timeout_s: int = 240):
    deadline = time.time() + timeout_s
    last_err = None
    while time.time() < deadline:
        try:
            sys_db = client.db("_system", username=user, password=password)
            sys_db.version()
            return sys_db
        except Exception as exc:  # noqa: BLE001
            last_err = exc
            time.sleep(2)
    raise RuntimeError(f"ArangoDB coordinator not ready: {last_err}")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Load LDBC SNB Iv2 dataset into ArangoDB.")
    p.add_argument("--host", default="coordinator0")
    p.add_argument("--port", type=int, default=8529)
    p.add_argument("--user", default="root")
    p.add_argument("--password", default="")
    p.add_argument("--database", default="snb")
    p.add_argument("--dataset", type=Path, required=True)
    p.add_argument("--mode", default="interactive", help="interactive | bi")
    p.add_argument("--batch", type=int, default=500)
    args = p.parse_args(argv)

    layout = LdbcDatasetLayout(base=args.dataset, mode=args.mode)
    if not layout.root.exists():
        raise SystemExit(f"dataset root {layout.root} does not exist")

    client = ArangoClient(hosts=f"http://{args.host}:{args.port}")
    print(f"[loader] connecting to {args.host}:{args.port}")
    sys_db = _wait_for_coordinator(client, args.user, args.password)

    if sys_db.has_database(args.database):
        sys_db.delete_database(args.database)
    sys_db.create_database(args.database)
    db = client.db(args.database, username=args.user, password=args.password)

    vertex_names, edge_defs = arangodb_collections()
    print(
        f"[loader] creating {len(vertex_names)} vertex collections "
        f"+ {len(edge_defs)} edge collections"
    )
    for v in vertex_names:
        db.create_collection(v)
    for e_name, _, _ in edge_defs:
        db.create_collection(e_name, edge=True)
    db.create_graph(
        "snb",
        edge_definitions=[
            {"edge_collection": e, "from_vertex_collections": [src], "to_vertex_collections": [dst]}
            for e, src, dst in edge_defs
        ],
    )

    total_v = 0
    for v in VERTICES:
        coll = db.collection(v.name)
        n = 0
        for chunk in batched(iter_vertex_rows(v, layout), args.batch):
            docs = [{"_key": str(vid), **props} for vid, props in chunk]
            coll.import_bulk(docs, on_duplicate="error")
            n += len(docs)
        if n:
            print(f"[loader]   vertex {v.name}: {n}")
        total_v += n

    total_e = 0
    for v in VERTICES:
        for fk in v.foreign_keys:
            coll = db.collection(fk.edge_label)
            if fk.direction == "in":
                src_label, dst_label = fk.target_label, v.name
            else:
                src_label, dst_label = v.name, fk.target_label
            n = 0
            for chunk in batched(iter_fk_edge_rows(v, fk, layout), args.batch):
                docs = [
                    {"_from": f"{src_label}/{src}", "_to": f"{dst_label}/{dst}"}
                    for src, dst, _ in chunk
                ]
                coll.import_bulk(docs, on_duplicate="error")
                n += len(docs)
            if n:
                print(f"[loader]   edge   {fk.edge_label}: {n}  (FK {v.name}.{fk.column})")
            total_e += n

    for e in EDGES:
        coll = db.collection(e.name)
        n = 0
        for chunk in batched(iter_edge_rows(e, layout), args.batch):
            docs = [
                {"_from": f"{e.src_label}/{src}", "_to": f"{e.dst_label}/{dst}", **props}
                for src, dst, props in chunk
            ]
            coll.import_bulk(docs, on_duplicate="error")
            n += len(docs)
        if n:
            print(f"[loader]   edge   {e.name}: {n}")
        total_e += n

    print(f"[loader] vertices={total_v} edges={total_e}; ArangoDB SNB Iv2 is benchmark-ready")
    return 0


if __name__ == "__main__":
    sys.exit(main())
