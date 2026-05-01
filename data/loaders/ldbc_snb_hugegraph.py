# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""LDBC SNB Interactive v2 loader for HugeGraph (composite-merged-fk layout)."""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import httpx

from data.loaders._gremlin_common import (
    gremlin_edge_script,
    gremlin_vertex_script,
    submit_batched,
)
from data.loaders._ldbc_snb_v2 import (
    LdbcDatasetLayout,
    iter_edge_rows,
    iter_fk_edge_rows,
    iter_vertex_rows,
)
from graph_bench.workloads.snb_iv2.schema import EDGES, VERTICES, fk_edge_labels


def _wait(client: httpx.Client, graph: str, timeout_s: int = 240) -> None:
    deadline = time.time() + timeout_s
    last_err: Exception | None = None
    while time.time() < deadline:
        try:
            r = client.get(f"/apis/graphs/{graph}")
            if r.status_code == 200:
                return
        except Exception as exc:  # noqa: BLE001
            last_err = exc
        time.sleep(3)
    raise RuntimeError(f"HugeGraph not ready: {last_err}")


def _ensure_schema(client: httpx.Client, graph: str) -> None:
    """Pre-declare the property keys, vertex labels, and edge labels we touch."""
    base = f"/apis/graphs/{graph}/schema"

    # Property keys: union over all vertex + edge properties + the universal `id`.
    pk_specs = [("id", "LONG", "SINGLE")]
    seen_pks = {"id"}
    for v in VERTICES:
        for p in v.properties:
            if p.name in seen_pks:
                continue
            seen_pks.add(p.name)
            dt = "LONG" if p.dtype in {"id", "int", "long", "bigint", "datetime", "date"} else (
                "INT" if p.dtype == "int" else "TEXT"
            )
            pk_specs.append((p.name, dt, "SINGLE"))
    for e in EDGES:
        for p in e.properties:
            if p.name in seen_pks:
                continue
            seen_pks.add(p.name)
            dt = "LONG" if p.dtype in {"id", "long", "bigint", "datetime", "date"} else (
                "INT" if p.dtype == "int" else "TEXT"
            )
            pk_specs.append((p.name, dt, "SINGLE"))

    for name, dt, card in pk_specs:
        body = {"name": name, "data_type": dt, "cardinality": card}
        r = client.post(f"{base}/propertykeys", json=body)
        if r.status_code not in (200, 201, 400):
            print(f"[loader] (schema warning) propertykey {name}: {r.status_code} {r.text[:120]}")

    for v in VERTICES:
        body = {
            "name": v.name,
            "id_strategy": "PRIMARY_KEY",
            "primary_keys": ["id"],
            "properties": ["id"] + [p.name for p in v.properties],
        }
        r = client.post(f"{base}/vertexlabels", json=body)
        if r.status_code not in (200, 201, 400):
            print(f"[loader] (schema warning) vertexlabel {v.name}: {r.status_code} {r.text[:120]}")

    edges_to_create: list[tuple[str, str, str, list[str]]] = []
    for e in EDGES:
        edges_to_create.append((e.name, e.src_label, e.dst_label, [p.name for p in e.properties]))
    for v in VERTICES:
        for fk in v.foreign_keys:
            if fk.direction == "in":
                src, dst = fk.target_label, v.name
            else:
                src, dst = v.name, fk.target_label
            edges_to_create.append((fk.edge_label, src, dst, []))

    for name, src, dst, props in edges_to_create:
        body = {
            "name": name,
            "source_label": src,
            "target_label": dst,
            "frequency": "MULTIPLE",
            "properties": props,
        }
        r = client.post(f"{base}/edgelabels", json=body)
        if r.status_code not in (200, 201, 400):
            print(f"[loader] (schema warning) edgelabel {name}: {r.status_code} {r.text[:120]}")


def _submit_factory(client: httpx.Client, graph: str):
    aliases = {"graph": graph, "g": f"__g_{graph}"}

    def _submit(script: str) -> None:
        r = client.post(
            "/apis/gremlin",
            json={"gremlin": script, "language": "gremlin-groovy", "aliases": aliases},
        )
        if r.status_code >= 400:
            raise RuntimeError(f"gremlin POST failed: {r.status_code} {r.text[:240]}")

    return _submit


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Load LDBC SNB Iv2 dataset into HugeGraph.")
    p.add_argument("--host", default="hugegraph0")
    p.add_argument("--port", type=int, default=8080)
    p.add_argument("--graph", default="hugegraph")
    p.add_argument("--dataset", type=Path, required=True)
    p.add_argument("--mode", default="interactive", help="interactive | bi")
    p.add_argument("--batch", type=int, default=20)
    args = p.parse_args(argv)

    layout = LdbcDatasetLayout(base=args.dataset, mode=args.mode)
    if not layout.root.exists():
        raise SystemExit(f"dataset root {layout.root} does not exist")

    base_url = f"http://{args.host}:{args.port}"
    client = httpx.Client(base_url=base_url, timeout=30.0)
    print(f"[loader] connecting to {base_url}")
    _wait(client, args.graph)
    _ensure_schema(client, args.graph)
    submit = _submit_factory(client, args.graph)

    total_v = 0
    for v in VERTICES:
        gen = (
            gremlin_vertex_script(v.name, vid, props)
            for vid, props in iter_vertex_rows(v, layout)
        )
        n = submit_batched(submit, gen, batch_size=args.batch)
        if n:
            print(f"[loader]   vertex {v.name}: {n}")
        total_v += n

    total_e = 0
    for v in VERTICES:
        for fk in v.foreign_keys:
            if fk.direction == "in":
                src_label, dst_label = fk.target_label, v.name
            else:
                src_label, dst_label = v.name, fk.target_label
            gen = (
                gremlin_edge_script(fk.edge_label, src_label, src, dst_label, dst, {})
                for src, dst, _ in iter_fk_edge_rows(v, fk, layout)
            )
            n = submit_batched(submit, gen, batch_size=args.batch)
            if n:
                print(f"[loader]   edge   {fk.edge_label}: {n}  (FK {v.name}.{fk.column})")
            total_e += n

    for e in EDGES:
        gen = (
            gremlin_edge_script(e.name, e.src_label, src, e.dst_label, dst, props)
            for src, dst, props in iter_edge_rows(e, layout)
        )
        n = submit_batched(submit, gen, batch_size=args.batch)
        if n:
            print(f"[loader]   edge   {e.name}: {n}")
        total_e += n

    client.close()
    print(f"[loader] vertices={total_v} edges={total_e}; HugeGraph SNB Iv2 is benchmark-ready")
    return 0


if __name__ == "__main__":
    sys.exit(main())
