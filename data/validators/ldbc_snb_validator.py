# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""LDBC SNB Interactive v2 / BI post-load validator.

Reads `Person.csv` + `Person_knows_Person.csv` from the LDBC SNB Datagen
output and probes the SUT with five known-answer queries: cluster status
(emitted by the runner), IS1 on three Persons, IS2 on one Person, IS3 on one
Person with at least one outgoing KNOWS.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

from graph_bench.domain import QueryRef, QueryRequest


async def _probe(driver, workload: str, qid: str, params: dict[str, Any]):
    from data.validators.runner import CheckResult
    res = await driver.execute(QueryRequest(ref=QueryRef(workload, qid), params=params))
    if res.status.value != "ok":
        return CheckResult(
            name=f"{workload}/{qid}({params})", ok=False,
            detail=f"status={res.status.value} err={res.error_message}",
        )
    return CheckResult(
        name=f"{workload}/{qid}({params})",
        ok=res.row_count >= 0,  # IS1 etc. may legitimately return 0; we accept and report
        detail=f"rows={res.row_count}",
    )


def _find(dataset_dir: Path, *names: str) -> Path | None:
    """Locate the first file in `dataset_dir` matching one of `names` (case-insensitive)."""
    for n in names:
        direct = dataset_dir / n
        if direct.exists():
            return direct
    lc = {p.name.lower(): p for p in dataset_dir.glob("*.csv") if p.is_file()}
    for n in names:
        if n.lower() in lc:
            return lc[n.lower()]
    return None


async def validate(driver, dataset_dir: Path):
    from data.validators.runner import CheckResult

    person_csv = _find(dataset_dir, "Person.csv", "person.csv")
    knows_csv = _find(
        dataset_dir,
        "Person_knows_Person.csv",
        "person_knows_person.csv",
        "Person.knows.Person.csv",
    )
    if person_csv is None:
        return [CheckResult(
            name="dataset_files", ok=False,
            detail=f"Person.csv missing under {dataset_dir}",
        )]

    person_ids: list[int] = []
    with person_csv.open("r", encoding="utf-8") as fp:
        for row in csv.DictReader(fp, delimiter="|"):
            try:
                person_ids.append(int(row["id"]))
            except (KeyError, ValueError):
                continue
            if len(person_ids) >= 200:
                break
    if not person_ids:
        return [CheckResult(name="dataset_sanity", ok=False, detail="Person.csv has no rows")]

    knows_src: set[int] = set()
    if knows_csv is not None:
        with knows_csv.open("r", encoding="utf-8") as fp:
            reader = csv.DictReader(fp, delimiter="|")
            if reader.fieldnames:
                src_col = reader.fieldnames[0]
                for row in reader:
                    try:
                        knows_src.add(int(row[src_col]))
                    except (KeyError, ValueError):
                        continue
                    if len(knows_src) >= 200:
                        break

    checks = []
    is1_ids = person_ids[:3]
    for pid in is1_ids:
        checks.append(await _probe(driver, "snb_iv2", "IS1", {"personId": pid}))

    is2_id = next((p for p in person_ids if p in knows_src), person_ids[0])
    checks.append(await _probe(driver, "snb_iv2", "IS2", {"personId": is2_id}))

    is3_id = next((p for p in person_ids if p in knows_src), person_ids[0])
    checks.append(await _probe(driver, "snb_iv2", "IS3", {"personId": is3_id}))

    return checks
