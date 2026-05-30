# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""Bulk loader for the synthetic SNB dataset into ArangoDB 3.11.

Strategy:
1. Wait for the coordinator to be reachable.
2. Drop the demo database if it exists, then create it.
3. Create vertex collection `Person` and edge collection `KNOWS` with a
   `Persons` graph definition so traversal queries can use `OUTBOUND`.
4. Bulk-import vertices and edges via the python-arango `import_bulk` API
   (chunked).
5. Smoke-validate by counting documents per collection.

CLI:
    python -m data.loaders.arangodb \\
        --host coordinator0 --port 8529 \\
        --user root --password '' \\
        --database snb_demo \\
        --dataset data/generated/synthetic_snb
"""

from __future__ import annotations

import argparse
import csv
import sys
import time
from pathlib import Path

from arango import ArangoClient
from arango.exceptions import ArangoClientError, ArangoServerError


def _wait_for_coordinator(client: ArangoClient, user: str, password: str, timeout_s: int = 120):
    deadline = time.time() + timeout_s
    last_err: Exception | None = None
    while time.time() < deadline:
        try:
            sys_db = client.db("_system", username=user, password=password)
            sys_db.version()
            return sys_db
        except (ArangoClientError, ArangoServerError) as exc:
            last_err = exc
            time.sleep(2)
    raise RuntimeError(f"ArangoDB coordinator not ready: {last_err}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Load synthetic SNB into ArangoDB.")
    parser.add_argument("--host", default="coordinator0")
    parser.add_argument("--port", type=int, default=8529)
    parser.add_argument("--user", default="root")
    parser.add_argument("--password", default="")
    parser.add_argument("--database", default="snb_demo")
    parser.add_argument("--dataset", type=Path, default=Path("data/generated/synthetic_snb"))
    parser.add_argument("--batch", type=int, default=500)
    args = parser.parse_args(argv)

    persons_csv = args.dataset / "persons.csv"
    knows_csv = args.dataset / "knows.csv"
    if not persons_csv.exists() or not knows_csv.exists():
        print(f"missing dataset files in {args.dataset}", file=sys.stderr)
        return 2

    client = ArangoClient(hosts=f"http://{args.host}:{args.port}")
    print(f"[loader] waiting for ArangoDB at {args.host}:{args.port}")
    sys_db = _wait_for_coordinator(client, args.user, args.password)

    if sys_db.has_database(args.database):
        print(f"[loader] dropping existing database {args.database!r}")
        sys_db.delete_database(args.database)
    print(f"[loader] creating database {args.database!r}")
    sys_db.create_database(args.database)

    db = client.db(args.database, username=args.user, password=args.password)

    print("[loader] creating Person vertex + KNOWS edge collections")
    persons = db.create_collection("Person")
    knows = db.create_collection("KNOWS", edge=True)

    if not db.has_graph("snb"):
        db.create_graph(
            "snb",
            edge_definitions=[
                {
                    "edge_collection": "KNOWS",
                    "from_vertex_collections": ["Person"],
                    "to_vertex_collections": ["Person"],
                }
            ],
        )

    print(f"[loader] inserting Person vertices (batch={args.batch})")
    n_persons = 0
    batch: list[dict] = []
    with persons_csv.open("r", encoding="utf-8") as fp:
        for row in csv.DictReader(fp):
            n_persons += 1
            batch.append(
                {
                    "_key": str(row["id"]),
                    "id": int(row["id"]),
                    "firstName": row["firstName"],
                    "lastName": row["lastName"],
                    "age": int(row["age"]),
                    "country": row["country"],
                }
            )
            if len(batch) >= args.batch:
                persons.import_bulk(batch, on_duplicate="error")
                batch.clear()
        if batch:
            persons.import_bulk(batch, on_duplicate="error")
    print(f"[loader] inserted {n_persons} Person vertices")

    print(f"[loader] inserting KNOWS edges (batch={args.batch})")
    n_edges = 0
    batch.clear()
    with knows_csv.open("r", encoding="utf-8") as fp:
        for row in csv.DictReader(fp):
            n_edges += 1
            batch.append(
                {
                    "_from": f"Person/{row['src']}",
                    "_to": f"Person/{row['dst']}",
                    "creationDate": int(row["creationDate"]),
                }
            )
            if len(batch) >= args.batch:
                knows.import_bulk(batch, on_duplicate="error")
                batch.clear()
        if batch:
            knows.import_bulk(batch, on_duplicate="error")
    print(f"[loader] inserted {n_edges} KNOWS edges")

    cnt_p = persons.count()
    cnt_e = knows.count()
    print(f"[loader] counts: Person={cnt_p} KNOWS={cnt_e}")
    if cnt_p != n_persons or cnt_e != n_edges:
        print("[loader] WARNING: counts do not match input rows", file=sys.stderr)
        return 1
    print("[loader] done — ArangoDB ready")
    return 0


if __name__ == "__main__":
    sys.exit(main())
