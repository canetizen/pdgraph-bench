# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""LDBC FinBench workload.

Two parameter sources are supported:

- ``parameter_dir`` — LDBC FinBench parameter-curation output (preferred when
  available; canonical parameter distributions).
- ``dataset_dir`` — raw LDBC FinBench Datagen output. The workload synthesises
  a small parameter pool per ref by sampling Account / Person / Loan vertex
  IDs out of the loaded ``raw/<entity>/`` CSVs. This path lets the protocol
  run without LDBC's parameter-curation step at the cost of a non-canonical
  parameter distribution. The bulk-load uses the same raw CSVs, so every
  sampled ID is guaranteed to exist in the loaded graph.
"""

from __future__ import annotations

import csv
import random
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from graph_bench.domain import QueryRef, QueryRequest, WorkloadMix
from graph_bench.workloads.ldbc_params import SeededParamCycler, find_param_file, read_param_file
from graph_bench.workloads.registry import WorkloadRegistry


_SR_REFS = [QueryRef("finbench", f"SR{i}") for i in range(1, 7)]
_W_REFS = [QueryRef("finbench", f"W{i}") for i in range(1, 4)]


def _raw_csv_glob(dataset_dir: Path, entity: str) -> list[Path]:
    base = dataset_dir / "raw" / entity
    if not base.exists():
        return []
    return sorted(base.glob("part-*.csv"))


def _read_id_column(part_files: list[Path], col: str = "id", limit: int = 5000) -> list[int]:
    out: list[int] = []
    for path in part_files:
        with path.open("r", encoding="utf-8") as fp:
            for row in csv.DictReader(fp, delimiter="|"):
                try:
                    out.append(int(row[col]))
                except (KeyError, ValueError):
                    continue
                if len(out) >= limit:
                    return out
    return out


class FinBenchWorkload:
    name = "finbench"

    def __init__(
        self,
        *,
        parameter_dir: Path | None = None,
        dataset_dir: Path | None = None,
    ) -> None:
        weights: dict[QueryRef, float] = {}
        for ref in _SR_REFS:
            weights[ref] = 80.0 / len(_SR_REFS)
        for ref in _W_REFS:
            weights[ref] = 20.0 / len(_W_REFS)
        self.mix = WorkloadMix(weights=weights)
        self._parameter_dir = parameter_dir
        self._dataset_dir = dataset_dir

    def iter_requests(self, seed: int) -> Iterator[QueryRequest]:
        if self._parameter_dir is not None and self._parameter_dir.exists():
            yield from self._iter_from_curation(seed)
            return
        if self._dataset_dir is not None and self._dataset_dir.exists():
            yield from self._iter_from_dataset(seed)
            return
        raise RuntimeError(
            "finbench: provide workload.options.parameter_dir (curated) OR "
            "workload.options.dataset_dir (raw FinBench Datagen output). Got: "
            f"parameter_dir={self._parameter_dir} dataset_dir={self._dataset_dir}"
        )

    def _iter_from_curation(self, seed: int) -> Iterator[QueryRequest]:
        cyclers: dict[QueryRef, SeededParamCycler] = {}
        for offset, ref in enumerate(self.mix.weights):
            f = find_param_file(self._parameter_dir, ref.id.lower())
            if f is None:
                continue
            cyclers[ref] = SeededParamCycler(read_param_file(f), seed=seed + offset)
        if not cyclers:
            raise RuntimeError(f"no FinBench parameter files under {self._parameter_dir}")

        rng = random.Random(seed)
        refs = list(cyclers.keys())
        weights = [self.mix.weights[r] for r in refs]
        while True:
            ref = rng.choices(refs, weights=weights, k=1)[0]
            yield QueryRequest(ref=ref, params=next(cyclers[ref]))

    def _iter_from_dataset(self, seed: int) -> Iterator[QueryRequest]:
        rng = random.Random(seed)
        account_ids = _read_id_column(_raw_csv_glob(self._dataset_dir, "account"))
        loan_ids = _read_id_column(_raw_csv_glob(self._dataset_dir, "loan"))
        if not account_ids:
            raise RuntimeError(f"no Account rows under {self._dataset_dir}/raw/account")
        if not loan_ids:
            # Degrade gracefully: W2 (deposit) edges then reuse account IDs.
            loan_ids = account_ids

        def sr_params() -> dict[str, Any]:
            return {"accountId": rng.choice(account_ids)}

        def w_params(ref: QueryRef) -> dict[str, Any]:
            now = 1700000000000  # 2023-11-15 ms; well past the dataset's deleteTime.
            if ref.id == "W1":
                # Insert TRANSFER edge (account -> account).
                return {
                    "srcId": rng.choice(account_ids),
                    "dstId": rng.choice(account_ids),
                    "amount": round(rng.uniform(1, 10000), 2),
                    "createTime": now + rng.randint(0, 86_400_000),
                    "orderNum": rng.randint(0, 1_000_000),
                    "comment": "bench",
                    "payType": "wire",
                    "goodsType": "service",
                }
            if ref.id == "W2":
                # Insert DEPOSIT edge (loan -> account).
                return {
                    "loanId": rng.choice(loan_ids),
                    "accountId": rng.choice(account_ids),
                    "amount": round(rng.uniform(1, 10000), 2),
                    "createTime": now + rng.randint(0, 86_400_000),
                }
            if ref.id == "W3":
                # Insert WITHDRAW edge (account -> account).
                return {
                    "srcId": rng.choice(account_ids),
                    "dstId": rng.choice(account_ids),
                    "amount": round(rng.uniform(1, 10000), 2),
                    "createTime": now + rng.randint(0, 86_400_000),
                }
            return {}

        refs = list(self.mix.weights)
        weights = [self.mix.weights[r] for r in refs]
        while True:
            ref = rng.choices(refs, weights=weights, k=1)[0]
            params = sr_params() if ref in _SR_REFS else w_params(ref)
            yield QueryRequest(ref=ref, params=params)


def _factory(config: dict[str, Any]) -> FinBenchWorkload:
    pd = config.get("parameter_dir")
    dd = config.get("dataset_dir")
    return FinBenchWorkload(
        parameter_dir=Path(pd) if pd else None,
        dataset_dir=Path(dd) if dd else None,
    )


WorkloadRegistry.register("finbench", _factory, overwrite=True)
