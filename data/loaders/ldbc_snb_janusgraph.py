# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""LDBC SNB Interactive v2 loader for JanusGraph (composite-merged-fk layout)."""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from gremlin_python.driver import client as gremlin_client
from gremlin_python.driver import serializer

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
from graph_bench.workloads.snb_iv2.schema import EDGES, VERTICES


def _wait(host: str, port: int, timeout_s: int = 240) -> gremlin_client.Client:
    deadline = time.time() + timeout_s
    last_err: Exception | None = None
    url = f"ws://{host}:{port}/gremlin"
    while time.time() < deadline:
        try:
            c = gremlin_client.Client(
                url, "g", message_serializer=serializer.GraphSONSerializersV3d0()
            )
            c.submit("g.V().limit(1).count()").all().result()
            return c
        except Exception as exc:  # noqa: BLE001
            last_err = exc
            time.sleep(3)
    raise RuntimeError(f"JanusGraph Gremlin server not ready: {last_err}")


def _ensure_schema(c: gremlin_client.Client) -> None:
    """Create a composite index on the universal `id` property."""
    script = (
        "mgmt = graph.openManagement(); "
        "if (!mgmt.containsPropertyKey('id')) { "
        "  mgmt.makePropertyKey('id').dataType(Long.class)"
        ".cardinality(org.janusgraph.core.Cardinality.SINGLE).make(); "
        "} "
        "if (!mgmt.containsGraphIndex('byId')) { "
        "  k = mgmt.getPropertyKey('id'); "
        "  mgmt.buildIndex('byId', Vertex.class).addKey(k).buildCompositeIndex(); "
        "} "
        "mgmt.commit();"
    )
    try:
        c.submit(script).all().result()
    except Exception as exc:  # noqa: BLE001
        print(f"[loader] (schema warning, continuing) {exc}")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Load LDBC SNB Iv2 dataset into JanusGraph.")
    p.add_argument("--host", default="janusgraph0")
    p.add_argument("--port", type=int, default=8182)
    p.add_argument("--dataset", type=Path, required=True)
    p.add_argument("--mode", default="interactive", help="interactive | bi")
    p.add_argument("--batch", type=int, default=50)
    args = p.parse_args(argv)

    layout = LdbcDatasetLayout(base=args.dataset, mode=args.mode)
    if not layout.root.exists():
        raise SystemExit(f"dataset root {layout.root} does not exist")

    print(f"[loader] connecting to {args.host}:{args.port}")
    c = _wait(args.host, args.port)
    _ensure_schema(c)

    def _submit(script: str) -> None:
        c.submit(script).all().result()

    total_v = 0
    for v in VERTICES:
        gen = (
            gremlin_vertex_script(v.name, vid, props)
            for vid, props in iter_vertex_rows(v, layout)
        )
        n = submit_batched(_submit, gen, batch_size=args.batch)
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
            n = submit_batched(_submit, gen, batch_size=args.batch)
            if n:
                print(f"[loader]   edge   {fk.edge_label}: {n}  (FK {v.name}.{fk.column})")
            total_e += n

    for e in EDGES:
        gen = (
            gremlin_edge_script(e.name, e.src_label, src, e.dst_label, dst, props)
            for src, dst, props in iter_edge_rows(e, layout)
        )
        n = submit_batched(_submit, gen, batch_size=args.batch)
        if n:
            print(f"[loader]   edge   {e.name}: {n}")
        total_e += n

    c.close()
    print(f"[loader] vertices={total_v} edges={total_e}; JanusGraph SNB Iv2 is benchmark-ready")
    return 0


if __name__ == "__main__":
    sys.exit(main())
