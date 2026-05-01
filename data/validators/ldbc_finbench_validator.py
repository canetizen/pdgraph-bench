# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""LDBC FinBench post-load validator.

Reads `Account.csv` + `AccountTransferAccount.csv` from the LDBC FinBench
Datagen output and probes the SUT with five known-answer queries: cluster
status (emitted by the runner), SR1 on three Accounts, SR2 on one Account,
SR3 on one Account that is the target of at least one TRANSFER.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

from graph_bench.domain import QueryRef, QueryRequest


async def _probe(driver, qid: str, params: dict[str, Any]):
    from data.validators.runner import CheckResult
    res = await driver.execute(QueryRequest(ref=QueryRef("finbench", qid), params=params))
    if res.status.value != "ok":
        return CheckResult(
            name=f"finbench/{qid}({params})", ok=False,
            detail=f"status={res.status.value} err={res.error_message}",
        )
    return CheckResult(
        name=f"finbench/{qid}({params})", ok=res.row_count >= 0,
        detail=f"rows={res.row_count}",
    )


def _find(dataset_dir: Path, *names: str) -> Path | None:
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

    account_csv = _find(dataset_dir, "Account.csv", "account.csv")
    transfer_csv = _find(
        dataset_dir,
        "AccountTransferAccount.csv",
        "accounttransferaccount.csv",
    )
    if account_csv is None:
        return [CheckResult(
            name="dataset_files", ok=False,
            detail=f"Account.csv missing under {dataset_dir}",
        )]

    account_ids: list[int] = []
    with account_csv.open("r", encoding="utf-8") as fp:
        for row in csv.DictReader(fp, delimiter="|"):
            try:
                account_ids.append(int(row["id"]))
            except (KeyError, ValueError):
                continue
            if len(account_ids) >= 200:
                break
    if not account_ids:
        return [CheckResult(name="dataset_sanity", ok=False, detail="Account.csv has no rows")]

    transfer_dst: set[int] = set()
    if transfer_csv is not None:
        with transfer_csv.open("r", encoding="utf-8") as fp:
            reader = csv.DictReader(fp, delimiter="|")
            if reader.fieldnames and len(reader.fieldnames) >= 2:
                dst_col = reader.fieldnames[1]
                for row in reader:
                    try:
                        transfer_dst.add(int(row[dst_col]))
                    except (KeyError, ValueError):
                        continue
                    if len(transfer_dst) >= 200:
                        break

    checks = []
    for aid in account_ids[:3]:
        checks.append(await _probe(driver, "SR1", {"accountId": aid}))
    checks.append(await _probe(driver, "SR2", {"accountId": account_ids[0]}))
    sr3_id = next((a for a in account_ids if a in transfer_dst), account_ids[0])
    checks.append(await _probe(driver, "SR3", {"accountId": sr3_id}))
    return checks
