# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""Bulk loader for the synthetic SNB dataset into Dgraph.

Strategy:
1. Wait for the alpha to be reachable.
2. Drop existing data and apply the schema (Person + knows predicates).
3. Issue parameterized mutations in batches via gRPC.
4. Smoke-validate by counting Persons and edges.

NOTE: For production-scale loads, `dgraph live` (RDF/JSON over gRPC) or
`dgraph bulk` (offline, fastest) are preferable. For the demo dataset
(~1k vertices, ~5k edges) the in-process gRPC mutation is plenty.

CLI:
    python -m data.loaders.dgraph \\
        --host alpha0 --port 9080 \\
        --dataset data/generated/synthetic_snb
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from pathlib import Path

import grpc
import pydgraph


_SCHEMA = """
id:          int   @index(int)  .
firstName:   string @index(exact) .
lastName:    string @index(exact) .
age:         int    .
country:     string @index(exact) .
creationDate:int    .
knows:       [uid]  @count @reverse .
type Person {
  id
  firstName
  lastName
  age
  country
  knows
}
"""


def _wait_for_alpha(host: str, port: int, timeout_s: int = 120) -> pydgraph.DgraphClient:
    deadline = time.time() + timeout_s
    last_err: Exception | None = None
    while time.time() < deadline:
        try:
            stub = pydgraph.DgraphClientStub(f"{host}:{port}")
            client = pydgraph.DgraphClient(stub)
            client.check_version()
            return client
        except Exception as exc:  # noqa: BLE001
            last_err = exc
            time.sleep(2)
    raise RuntimeError(f"Dgraph alpha at {host}:{port} not ready: {last_err}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Load synthetic SNB into Dgraph.")
    parser.add_argument("--host", default="alpha0")
    parser.add_argument("--port", type=int, default=9080)
    parser.add_argument("--dataset", type=Path, default=Path("data/generated/synthetic_snb"))
    parser.add_argument("--batch", type=int, default=500)
    args = parser.parse_args(argv)

    persons_csv = args.dataset / "persons.csv"
    knows_csv = args.dataset / "knows.csv"
    if not persons_csv.exists() or not knows_csv.exists():
        print(f"missing dataset files in {args.dataset}", file=sys.stderr)
        return 2

    print(f"[loader] connecting to alpha at {args.host}:{args.port}")
    client = _wait_for_alpha(args.host, args.port)

    print("[loader] dropping all data and applying schema")
    op = pydgraph.Operation(drop_all=True)
    client.alter(op)
    op = pydgraph.Operation(schema=_SCHEMA)
    client.alter(op)

    # Phase 1: insert Person nodes; remember person_id -> assigned uid.
    print(f"[loader] inserting Person vertices (batch={args.batch})")
    person_uids: dict[int, str] = {}
    n_persons = 0
    batch: list[dict] = []
    with persons_csv.open("r", encoding="utf-8") as fp:
        for row in csv.DictReader(fp):
            n_persons += 1
            blank = f"_:p{row['id']}"
            batch.append(
                {
                    "uid": blank,
                    "dgraph.type": "Person",
                    "id": int(row["id"]),
                    "firstName": row["firstName"],
                    "lastName": row["lastName"],
                    "age": int(row["age"]),
                    "country": row["country"],
                }
            )
            if len(batch) >= args.batch:
                _commit(client, batch, person_uids)
                batch.clear()
        if batch:
            _commit(client, batch, person_uids)
    print(f"[loader] inserted {n_persons} Person vertices")

    # Phase 2: edges. Map src/dst person ids to assigned uids and emit knows
    # mutations.
    print(f"[loader] inserting KNOWS edges (batch={args.batch})")
    n_edges = 0
    batch.clear()
    with knows_csv.open("r", encoding="utf-8") as fp:
        for row in csv.DictReader(fp):
            src_uid = person_uids.get(int(row["src"]))
            dst_uid = person_uids.get(int(row["dst"]))
            if src_uid is None or dst_uid is None:
                continue
            n_edges += 1
            batch.append({"uid": src_uid, "knows": [{"uid": dst_uid}]})
            if len(batch) >= args.batch:
                _commit(client, batch, None)
                batch.clear()
        if batch:
            _commit(client, batch, None)
    print(f"[loader] inserted {n_edges} KNOWS edges")

    # Smoke validate: count Persons.
    txn = client.txn(read_only=True)
    try:
        res = txn.query("{ persons(func: type(Person)) { count(uid) } }")
        body = json.loads(res.json)
        n = (body.get("persons") or [{}])[0].get("count", 0)
        print(f"[loader] count(Person) = {n}")
        if n != n_persons:
            print(f"[loader] WARNING: count mismatch (expected {n_persons})", file=sys.stderr)
    finally:
        txn.discard()

    print("[loader] done — Dgraph ready")
    return 0


def _commit(client: pydgraph.DgraphClient, batch: list[dict], uid_map: dict[int, str] | None) -> None:
    txn = client.txn()
    try:
        mu = pydgraph.Mutation(set_json=json.dumps(batch).encode("utf-8"))
        resp = txn.mutate(mu, commit_now=True)
        if uid_map is not None:
            # Map blank-node label `pN` -> assigned uid for the Person rows.
            for row in batch:
                blank = row["uid"][2:]  # strip "_:" prefix
                uid = resp.uids.get(blank)
                if uid is not None:
                    uid_map[int(row["id"])] = uid
    finally:
        txn.discard()


if __name__ == "__main__":
    sys.exit(main())
