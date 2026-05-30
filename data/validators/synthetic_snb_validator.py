# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""Synthetic-SNB post-load validator.

Reads `persons.csv` + `knows.csv` produced by `data/generators/synthetic_snb.py`
and probes the SUT with five known-answer queries: count check, IS1 on three
distinct person ids, and IS2 on one person known to have out-neighbours.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

from graph_bench.domain import QueryRef, QueryRequest


async def _probe(driver, ref: QueryRef, params: dict[str, Any]):
    from data.validators.runner import CheckResult
    res = await driver.execute(QueryRequest(ref=ref, params=params))
    if res.status.value != "ok":
        return CheckResult(
            name=f"{ref}({params})", ok=False,
            detail=f"status={res.status.value} err={res.error_message}",
        )
    if res.row_count == 0:
        return CheckResult(name=f"{ref}({params})", ok=False, detail="no rows")
    return CheckResult(name=f"{ref}({params})", ok=True, detail=f"rows={res.row_count}")


async def validate(driver, dataset_dir: Path):
    from data.validators.runner import CheckResult

    persons_csv = dataset_dir / "persons.csv"
    knows_csv = dataset_dir / "knows.csv"
    if not persons_csv.exists() or not knows_csv.exists():
        return [CheckResult(
            name="dataset_files", ok=False,
            detail=f"expected persons.csv and knows.csv under {dataset_dir}",
        )]

    persons = list(csv.DictReader(persons_csv.open(encoding="utf-8")))
    knows = list(csv.DictReader(knows_csv.open(encoding="utf-8")))

    out_neighbours: dict[int, set[int]] = {int(r["id"]): set() for r in persons}
    for e in knows:
        out_neighbours[int(e["src"])].add(int(e["dst"]))

    sample_ids = [pid for pid, n in out_neighbours.items() if n][:3]
    if not sample_ids:
        return [CheckResult(name="dataset_sanity", ok=False, detail="no Person has KNOWS")]

    checks = []
    for pid in sample_ids:
        checks.append(await _probe(driver, QueryRef("synthetic_snb", "IS1"), {"vid": pid}))
    for pid in sample_ids[:2]:
        checks.append(await _probe(driver, QueryRef("synthetic_snb", "IS2"), {"vid": pid}))
    return checks
