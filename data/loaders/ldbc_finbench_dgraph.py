# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""LDBC FinBench loader for Dgraph (raw/<entity>/part-*.csv layout)."""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import pydgraph

from data.loaders._finbench_v2 import (
    FinbenchDatasetLayout,
    batched,
    iter_edge_rows,
    iter_vertex_rows,
)
from graph_bench.workloads.finbench.ddl import dgraph_schema
from graph_bench.workloads.finbench.schema import EDGES, VERTICES


def _wait(host, port, timeout_s=180):
    deadline = time.time() + timeout_s
    last_err = None
    while time.time() < deadline:
        try:
            stub = pydgraph.DgraphClientStub(f"{host}:{port}")
            client = pydgraph.DgraphClient(stub)
            client.check_version()
            return client, stub
        except Exception as exc:  # noqa: BLE001
            last_err = exc
            time.sleep(2)
    raise RuntimeError(f"Dgraph not ready: {last_err}")


def _commit(client, batch, uid_map):
    txn = client.txn()
    try:
        mu = pydgraph.Mutation(set_json=json.dumps(batch).encode("utf-8"))
        resp = txn.mutate(mu, commit_now=True)
        if uid_map is not None:
            for row in batch:
                blank = row["uid"][2:]
                tname, _, raw_id = blank.partition("_")
                if not raw_id:
                    continue
                uid = resp.uids.get(blank)
                if uid is not None:
                    uid_map[(tname, int(raw_id))] = uid
    finally:
        txn.discard()


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Load LDBC FinBench dataset into Dgraph.")
    p.add_argument("--host", default="alpha0")
    p.add_argument("--port", type=int, default=9080)
    p.add_argument("--dataset", type=Path, required=True)
    p.add_argument("--batch", type=int, default=500)
    args = p.parse_args(argv)

    layout = FinbenchDatasetLayout(base=args.dataset)
    if not layout.root.exists():
        raise SystemExit(f"dataset root {layout.root} does not exist")

    client, stub = _wait(args.host, args.port)
    client.alter(pydgraph.Operation(drop_all=True))
    client.alter(pydgraph.Operation(schema=dgraph_schema()))

    uid_map: dict = {}

    total_v = 0
    for v in VERTICES:
        n = 0
        for chunk in batched(iter_vertex_rows(v, layout), args.batch):
            docs = []
            for vid, props in chunk:
                doc = {"uid": f"_:{v.name}_{vid}", "dgraph.type": v.name, "id": vid}
                doc.update(props)
                docs.append(doc)
            _commit(client, docs, uid_map)
            n += len(docs)
        if n:
            print(f"[loader]   vertex {v.name}: {n}")
        total_v += n

    total_e = 0
    for e in EDGES:
        n = 0
        for chunk in batched(iter_edge_rows(e, layout), args.batch):
            docs = []
            for src, dst, props in chunk:
                src_uid = uid_map.get((e.src_label, src))
                dst_uid = uid_map.get((e.dst_label, dst))
                if src_uid is None or dst_uid is None:
                    continue
                doc = {"uid": src_uid, e.name: [{"uid": dst_uid}]}
                doc.update(props)
                docs.append(doc)
            if docs:
                _commit(client, docs, None)
                n += len(docs)
        if n:
            print(f"[loader]   edge   {e.name}: {n}")
        total_e += n

    stub.close()
    print(f"[loader] vertices={total_v} edges={total_e}; Dgraph FinBench is benchmark-ready")
    return 0


if __name__ == "__main__":
    sys.exit(main())
