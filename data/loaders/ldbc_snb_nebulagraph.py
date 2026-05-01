# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""LDBC SNB Interactive v2 loader for NebulaGraph (composite-merged-fk layout)."""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from nebula3.Config import Config
from nebula3.gclient.net import ConnectionPool

from data.loaders._ldbc_snb_v2 import (
    LdbcDatasetLayout,
    batched,
    iter_edge_rows,
    iter_fk_edge_rows,
    iter_vertex_rows,
)
from graph_bench.workloads.snb_iv2.ddl import nebulagraph_ddl
from graph_bench.workloads.snb_iv2.schema import EDGES, VERTICES


def _exec(pool, user: str, password: str, ngql: str) -> None:
    session = pool.get_session(user, password)
    try:
        result = session.execute(ngql)
        if not result.is_succeeded():
            raise RuntimeError(f"nGQL failed: {ngql[:160]!r}: {result.error_msg()}")
    finally:
        session.release()


def _wait_for_graphd(host: str, port: int, user: str, password: str, timeout_s: int = 180) -> ConnectionPool:
    cfg = Config()
    cfg.max_connection_pool_size = 8
    deadline = time.time() + timeout_s
    last_err: Exception | None = None
    while time.time() < deadline:
        pool = ConnectionPool()
        try:
            if pool.init([(host, port)], cfg):
                session = pool.get_session(user, password)
                try:
                    if session.execute("SHOW HOSTS").is_succeeded():
                        return pool
                finally:
                    session.release()
        except Exception as exc:  # noqa: BLE001
            last_err = exc
        time.sleep(2)
    raise RuntimeError(f"graphd not ready after {timeout_s}s ({last_err})")


def _literal(v) -> str:
    if v is None:
        return "''"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return str(v)
    s = str(v).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{s}'"


def _insert_vertices_batch(pool, user, password, space, vertex, batch) -> None:
    prop_names = [p.name for p in vertex.properties]
    cols = ", ".join(f"`{p}`" for p in prop_names) if prop_names else "gb_p"
    if not prop_names:
        rows = ", ".join(f"{vid}:(0)" for vid, _ in batch)
    else:
        rows = ", ".join(
            f"{vid}:(" + ", ".join(_literal(props.get(p)) for p in prop_names) + ")"
            for vid, props in batch
        )
    ngql = f"USE {space}; INSERT VERTEX `{vertex.name}`({cols}) VALUES {rows}"
    _exec(pool, user, password, ngql)


def _insert_edges_batch(pool, user, password, space, label, prop_names, batch) -> None:
    cols = ", ".join(f"`{p}`" for p in prop_names) if prop_names else "gb_p"
    if prop_names:
        rows = ", ".join(
            f"{src}->{dst}:(" + ", ".join(_literal(props.get(p)) for p in prop_names) + ")"
            for src, dst, props in batch
        )
    else:
        rows = ", ".join(f"{src}->{dst}:(0)" for src, dst, _ in batch)
    ngql = f"USE {space}; INSERT EDGE `{label}`({cols}) VALUES {rows}"
    _exec(pool, user, password, ngql)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Load LDBC SNB Iv2 dataset into NebulaGraph.")
    p.add_argument("--host", default="graphd0")
    p.add_argument("--port", type=int, default=9669)
    p.add_argument("--user", default="root")
    p.add_argument("--password", default="nebula")
    p.add_argument("--space", default="snb")
    p.add_argument("--dataset", type=Path, required=True)
    p.add_argument("--mode", default="interactive", help="interactive | bi")
    p.add_argument("--partition-num", type=int, default=30)
    p.add_argument("--replica-factor", type=int, default=1)
    p.add_argument("--batch", type=int, default=200)
    p.add_argument(
        "--storage-hosts",
        default="storaged0:9779,storaged1:9779,storaged2:9779",
        help="Comma-separated host:port list of storaged instances; ignored if already registered.",
    )
    args = p.parse_args(argv)

    layout = LdbcDatasetLayout(base=args.dataset, mode=args.mode)
    if not layout.root.exists():
        raise SystemExit(f"dataset root {layout.root} does not exist")

    print(f"[loader] connecting to {args.host}:{args.port}")
    pool = _wait_for_graphd(args.host, args.port, args.user, args.password)

    storage_hosts = [tuple(h.strip().split(":")) for h in args.storage_hosts.split(",") if h.strip()]
    addr_list = ", ".join(f'"{h}":{p}' for h, p in storage_hosts)
    session = pool.get_session(args.user, args.password)
    try:
        res = session.execute(f"ADD HOSTS {addr_list}")
        if not res.is_succeeded():
            err = (res.error_msg() or "").lower()
            if "already exist" not in err and "existed" not in err:
                raise RuntimeError(f"ADD HOSTS failed: {res.error_msg()}")
    finally:
        session.release()
    deadline = time.time() + 240
    while time.time() < deadline:
        session = pool.get_session(args.user, args.password)
        try:
            res = session.execute("SHOW HOSTS")
            if res.is_succeeded():
                online = sum(
                    1 for row in res.rows()
                    if row.values and len(row.values) > 2 and "online" in str(row.values[2]).lower()
                )
                if online >= len(storage_hosts):
                    break
        finally:
            session.release()
        time.sleep(2)

    print(f"[loader] applying schema to space '{args.space}'")
    ddl = nebulagraph_ddl(args.space, args.partition_num, args.replica_factor)
    # NebulaGraph processes DROP/CREATE SPACE asynchronously: subsequent USE on
    # the new space races with the metad's view propagation. Issue the
    # space-shaping statements first, then poll SHOW SPACES until the new
    # space is visible from this graphd, then run the tag/edge DDL.
    space_stmts = [s for s in ddl if "SPACE" in s]
    rest = [s for s in ddl if "SPACE" not in s]
    for s in space_stmts:
        _exec(pool, args.user, args.password, s)
        time.sleep(0.5)

    deadline = time.time() + 60
    while time.time() < deadline:
        session = pool.get_session(args.user, args.password)
        try:
            res = session.execute("SHOW SPACES")
            if res.is_succeeded():
                names = {str(row.values[0]).strip("'\"") for row in res.rows() if row.values}
                if args.space in {n.replace('"', "") for n in names}:
                    break
        finally:
            session.release()
        time.sleep(1)
    time.sleep(8)  # graphd schema cache settle

    for stmt in rest:
        _exec(pool, args.user, args.password, stmt)
        time.sleep(0.3)
    time.sleep(8)  # allow tag/edge schema to propagate to storaged

    total_v = 0
    for v in VERTICES:
        n = 0
        for chunk in batched(iter_vertex_rows(v, layout), args.batch):
            _insert_vertices_batch(pool, args.user, args.password, args.space, v, chunk)
            n += len(chunk)
        if n:
            print(f"[loader]   vertex {v.name}: {n}")
        total_v += n

    total_e = 0
    for v in VERTICES:
        for fk in v.foreign_keys:
            n = 0
            for chunk in batched(iter_fk_edge_rows(v, fk, layout), args.batch):
                _insert_edges_batch(pool, args.user, args.password, args.space, fk.edge_label, [], chunk)
                n += len(chunk)
            if n:
                print(f"[loader]   edge   {fk.edge_label}: {n}  (FK {v.name}.{fk.column})")
            total_e += n

    for e in EDGES:
        prop_names = [p.name for p in e.properties]
        n = 0
        for chunk in batched(iter_edge_rows(e, layout), args.batch):
            _insert_edges_batch(pool, args.user, args.password, args.space, e.name, prop_names, chunk)
            n += len(chunk)
        if n:
            print(f"[loader]   edge   {e.name}: {n}")
        total_e += n

    pool.close()
    print(f"[loader] vertices={total_v} edges={total_e}; NebulaGraph SNB Iv2 is benchmark-ready")
    return 0


if __name__ == "__main__":
    sys.exit(main())
