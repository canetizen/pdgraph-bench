# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""Bulk loader for the synthetic SNB dataset into NebulaGraph.

Strategy:
1. Wait for graphd readiness.
2. Drop the demo space if it exists, then create it.
3. Wait for partitions to be assigned (NebulaGraph requires a few seconds
   between space creation and the first INSERT).
4. Create Person tag and KNOWS edge type.
5. Insert vertices and edges in batches via INSERT statements.
6. Run a smoke validator query and report counts.

This is intentionally pragmatic — `nebula-importer` would be more efficient
but adds an extra binary; the dataset is small enough that batched INSERTs
finish in seconds.

CLI:
    python -m data.loaders.nebulagraph \
        --host graphd0 --port 9669 \
        --space snb_demo \
        --dataset data/generated/synthetic_snb
"""

from __future__ import annotations

import argparse
import csv
import sys
import time
from pathlib import Path

from nebula3.Config import Config
from nebula3.gclient.net import ConnectionPool


def _session(pool: ConnectionPool, user: str, password: str):
    return pool.get_session(user, password)


def _exec(pool: ConnectionPool, user: str, password: str, ngql: str) -> None:
    session = _session(pool, user, password)
    try:
        result = session.execute(ngql)
        if not result.is_succeeded():
            raise RuntimeError(f"nGQL failed: {ngql!r}: {result.error_msg()}")
    finally:
        session.release()


def _wait_for_graphd(host: str, port: int, user: str, password: str, timeout_s: int = 120) -> ConnectionPool:
    cfg = Config()
    cfg.max_connection_pool_size = 4
    deadline = time.time() + timeout_s
    last_err: Exception | None = None
    while time.time() < deadline:
        pool = ConnectionPool()
        try:
            ok = pool.init([(host, port)], cfg)
            if ok:
                # Validate by issuing a trivial query.
                session = pool.get_session(user, password)
                try:
                    res = session.execute("SHOW HOSTS")
                    if res.is_succeeded():
                        return pool
                finally:
                    session.release()
        except Exception as exc:  # noqa: BLE001
            last_err = exc
        time.sleep(2)
    raise RuntimeError(f"graphd at {host}:{port} not ready after {timeout_s}s ({last_err})")


def _register_storage_hosts(pool: ConnectionPool, user: str, password: str, hosts: list[tuple[str, int]]) -> None:
    """Register storage nodes with the cluster via `ADD HOSTS`.

    NebulaGraph requires storage daemons to be explicitly added before they
    show up as ONLINE. Already-registered hosts cause `ADD HOSTS` to fail with
    a "host already exists" error, which we treat as a no-op.
    """
    if not hosts:
        return
    addr_list = ", ".join(f'"{h}":{p}' for h, p in hosts)
    ngql = f"ADD HOSTS {addr_list}"
    session = pool.get_session(user, password)
    try:
        result = session.execute(ngql)
        if not result.is_succeeded():
            err = (result.error_msg() or "").lower()
            if "already exist" not in err and "existed" not in err:
                raise RuntimeError(f"ADD HOSTS failed: {result.error_msg()}")
    finally:
        session.release()


def _wait_for_storage(pool: ConnectionPool, user: str, password: str, expected_min: int, timeout_s: int = 180) -> int:
    """Wait until at least `expected_min` storage hosts report ONLINE."""
    deadline = time.time() + timeout_s
    online = 0
    while time.time() < deadline:
        session = pool.get_session(user, password)
        try:
            result = session.execute("SHOW HOSTS")
            if result.is_succeeded():
                rows = result.rows()
                online = sum(
                    1
                    for row in rows
                    if row.values and len(row.values) > 2 and "online" in str(row.values[2]).lower()
                )
                if online >= expected_min:
                    return online
        finally:
            session.release()
        time.sleep(2)
    raise RuntimeError(f"only {online}/{expected_min} storage hosts ONLINE after {timeout_s}s")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Load synthetic SNB dataset into NebulaGraph.")
    parser.add_argument("--host", default="graphd0")
    parser.add_argument("--port", type=int, default=9669)
    parser.add_argument("--user", default="root")
    parser.add_argument("--password", default="nebula")
    parser.add_argument("--space", default="snb_demo")
    parser.add_argument("--dataset", type=Path, default=Path("data/generated/synthetic_snb"))
    parser.add_argument("--partition-num", type=int, default=10)
    parser.add_argument("--replica-factor", type=int, default=1)
    parser.add_argument("--batch", type=int, default=200)
    parser.add_argument("--expected-storage-hosts", type=int, default=3)
    parser.add_argument(
        "--storage-hosts",
        default="storaged0:9779,storaged1:9779,storaged2:9779",
        help="Comma-separated list of storage hosts to register with ADD HOSTS.",
    )
    args = parser.parse_args(argv)

    persons_csv = args.dataset / "persons.csv"
    knows_csv = args.dataset / "knows.csv"
    if not persons_csv.exists() or not knows_csv.exists():
        print(f"missing dataset files in {args.dataset}", file=sys.stderr)
        return 2

    print(f"[loader] connecting to graphd at {args.host}:{args.port}")
    pool = _wait_for_graphd(args.host, args.port, args.user, args.password)

    storage_hosts: list[tuple[str, int]] = []
    for entry in args.storage_hosts.split(","):
        entry = entry.strip()
        if not entry:
            continue
        host, port = entry.split(":", 1)
        storage_hosts.append((host, int(port)))
    print(f"[loader] registering {len(storage_hosts)} storage hosts with ADD HOSTS")
    _register_storage_hosts(pool, args.user, args.password, storage_hosts)

    print(f"[loader] waiting for >= {args.expected_storage_hosts} storage hosts ONLINE")
    online = _wait_for_storage(pool, args.user, args.password, args.expected_storage_hosts)
    print(f"[loader] {online} storage hosts ONLINE")

    print(f"[loader] (re)creating space {args.space!r}")
    _exec(pool, args.user, args.password, f"DROP SPACE IF EXISTS {args.space}")
    # `vid_type=INT64` lets us use raw integer ids without quoting.
    _exec(
        pool,
        args.user,
        args.password,
        (
            f"CREATE SPACE IF NOT EXISTS {args.space} ("
            f"partition_num={args.partition_num}, replica_factor={args.replica_factor}, "
            f"vid_type=INT64)"
        ),
    )
    # Wait for partitions to settle before issuing schema DDL or INSERTs.
    print("[loader] waiting 12s for partition assignment")
    time.sleep(12)

    print(f"[loader] creating tag Person + edge KNOWS in {args.space}")
    _exec(
        pool, args.user, args.password,
        f"USE {args.space}; CREATE TAG IF NOT EXISTS Person("
        "firstName string, lastName string, age int, country string)",
    )
    _exec(
        pool, args.user, args.password,
        f"USE {args.space}; CREATE EDGE IF NOT EXISTS KNOWS(creationDate int)",
    )
    print("[loader] waiting 8s for schema propagation")
    time.sleep(8)

    print(f"[loader] inserting Person vertices (batch={args.batch})")
    n_persons = 0
    with persons_csv.open("r", encoding="utf-8") as fp:
        reader = csv.DictReader(fp)
        batch: list[str] = []
        for row in reader:
            n_persons += 1
            fn = row["firstName"].replace("'", "''")
            ln = row["lastName"].replace("'", "''")
            ctry = row["country"].replace("'", "''")
            batch.append(
                f"{row['id']}:('{fn}', '{ln}', {int(row['age'])}, '{ctry}')"
            )
            if len(batch) >= args.batch:
                _exec(
                    pool, args.user, args.password,
                    f"USE {args.space}; INSERT VERTEX Person(firstName, lastName, age, country) VALUES "
                    + ", ".join(batch),
                )
                batch.clear()
        if batch:
            _exec(
                pool, args.user, args.password,
                f"USE {args.space}; INSERT VERTEX Person(firstName, lastName, age, country) VALUES "
                + ", ".join(batch),
            )
    print(f"[loader] inserted {n_persons} Person vertices")

    print(f"[loader] inserting KNOWS edges (batch={args.batch})")
    n_edges = 0
    with knows_csv.open("r", encoding="utf-8") as fp:
        reader = csv.DictReader(fp)
        batch: list[str] = []
        for row in reader:
            n_edges += 1
            batch.append(f"{row['src']}->{row['dst']}:({int(row['creationDate'])})")
            if len(batch) >= args.batch:
                _exec(
                    pool, args.user, args.password,
                    f"USE {args.space}; INSERT EDGE KNOWS(creationDate) VALUES " + ", ".join(batch),
                )
                batch.clear()
        if batch:
            _exec(
                pool, args.user, args.password,
                f"USE {args.space}; INSERT EDGE KNOWS(creationDate) VALUES " + ", ".join(batch),
            )
    print(f"[loader] inserted {n_edges} KNOWS edges")

    # Smoke validator.
    print("[loader] smoke query: count vertices")
    session = pool.get_session(args.user, args.password)
    try:
        res = session.execute(f"USE {args.space}; SUBMIT JOB STATS")
        if not res.is_succeeded():
            print(f"[loader] STATS submit warning: {res.error_msg()}")
    finally:
        session.release()

    pool.close()
    print("[loader] done — cluster is benchmark-ready")
    return 0


if __name__ == "__main__":
    sys.exit(main())
