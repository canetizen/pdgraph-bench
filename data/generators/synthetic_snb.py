# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""Generate a small synthetic SNB-shaped dataset for the demo cluster.

Produces three artifacts under the chosen output directory:

- `persons.csv`  — Person vertices (id, firstName, lastName, age, country)
- `knows.csv`    — KNOWS edges (src, dst, creationDate)
- `parameters.json` — list of materialised person ids, used by the
                      synthetic_snb workload's parameter source

This is **not** an LDBC dataset. The schema is a deliberately small subset of
SNB so that the harness can be exercised end-to-end against a real distributed
graph DB without depending on the LDBC SNB Datagen Spark job.

CLI:
    python -m data.generators.synthetic_snb --persons 1000 --avg-degree 5 --out data/generated/synthetic_snb
"""

from __future__ import annotations

import argparse
import csv
import json
import random
import sys
from pathlib import Path


_FIRST_NAMES = [
    "Ada", "Alan", "Barbara", "Brian", "Carol", "Carlos", "Dilek", "Deniz",
    "Emine", "Erkan", "Fatma", "Furkan", "Gizem", "Halil", "Hande", "Ibrahim",
    "Ines", "Joon", "Karim", "Lea", "Mehmet", "Maria", "Naz", "Omar",
    "Pelin", "Rita", "Selim", "Tolga", "Umut", "Yara", "Zeynep",
]
_LAST_NAMES = [
    "Aydin", "Bayar", "Caliskan", "Demir", "Erol", "Fidan", "Gokce", "Hilmi",
    "Ilgaz", "Karaca", "Lale", "Mert", "Nuri", "Ozkan", "Polat", "Rumi",
    "Saglam", "Tekin", "Ucar", "Vural", "Yildiz",
]
_COUNTRIES = [
    "TR", "DE", "US", "FR", "GB", "IT", "ES", "NL", "JP", "KR", "BR", "IN",
]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Synthetic SNB-shaped dataset generator.")
    parser.add_argument("--persons", type=int, default=1000, help="Number of Person vertices.")
    parser.add_argument(
        "--avg-degree", type=int, default=5, help="Average out-degree of KNOWS edges per person."
    )
    parser.add_argument("--seed", type=int, default=42, help="RNG seed for reproducibility.")
    parser.add_argument(
        "--out", type=Path, default=Path("data/generated/synthetic_snb"),
        help="Output directory.",
    )
    args = parser.parse_args(argv)

    out: Path = args.out
    out.mkdir(parents=True, exist_ok=True)

    rng = random.Random(args.seed)
    person_ids = list(range(1, args.persons + 1))

    persons_path = out / "persons.csv"
    with persons_path.open("w", newline="", encoding="utf-8") as fp:
        writer = csv.writer(fp)
        writer.writerow(["id", "firstName", "lastName", "age", "country"])
        for pid in person_ids:
            writer.writerow(
                [
                    pid,
                    rng.choice(_FIRST_NAMES),
                    rng.choice(_LAST_NAMES),
                    rng.randint(18, 80),
                    rng.choice(_COUNTRIES),
                ]
            )

    knows_path = out / "knows.csv"
    edges_seen: set[tuple[int, int]] = set()
    with knows_path.open("w", newline="", encoding="utf-8") as fp:
        writer = csv.writer(fp)
        writer.writerow(["src", "dst", "creationDate"])
        for src in person_ids:
            for _ in range(args.avg_degree):
                dst = rng.choice(person_ids)
                if dst == src or (src, dst) in edges_seen:
                    continue
                edges_seen.add((src, dst))
                writer.writerow([src, dst, rng.randint(1_700_000_000, 1_800_000_000)])

    params_path = out / "parameters.json"
    with params_path.open("w", encoding="utf-8") as fp:
        json.dump(
            {
                "schema": "synthetic_snb",
                "version": 1,
                "person_ids": person_ids,
            },
            fp,
        )

    print(f"persons:    {len(person_ids):>6}  -> {persons_path}")
    print(f"knows:      {len(edges_seen):>6}  -> {knows_path}")
    print(f"parameters:           -> {params_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
