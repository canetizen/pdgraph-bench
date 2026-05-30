# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""Composite analytical workload for scenario S2.

Per the Progress Report Section 5 (Workload Subset and Query Translation), the
analytical-only scenario draws complex reads IC1/IC3/IC5/IC6 from LDBC SNB
Interactive v2 *and* power-class reads Q1/Q2/Q6/Q11 from LDBC SNB BI. These
live in two distinct parameter-curation file trees produced by separate
generators. The composite workload wraps both: each `QueryRequest` carries the
originating workload tag in its `QueryRef`, so the per-driver template lookup
routes to `snb_iv2.queries` or `snb_bi.queries` accordingly.

Two parameter sources are supported:

- ``iv2_parameter_dir`` + ``bi_parameter_dir`` — LDBC SNB Driver curated files
  (preferred when available; gives canonical parameter distributions).
- ``iv2_dataset_dir`` + ``bi_dataset_dir`` — raw LDBC SNB Datagen output for
  each workload. The workload synthesises a small parameter pool per ref by
  sampling Person/Tag/Forum vertex IDs out of the loaded CSVs. This path lets
  the protocol run without requiring the LDBC SNB Driver's parameter-curation
  step, at the cost of a non-canonical parameter distribution.
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


_IC_REFS = [QueryRef("snb_iv2", f"IC{i}") for i in (1, 3, 5, 6)]
_BI_REFS = [QueryRef("snb_bi", f"Q{i}") for i in (1, 2, 6, 11)]


def _iv2_csv_glob(dataset_dir: Path, subdir: str) -> list[Path]:
    base = dataset_dir / "graphs" / "csv" / "interactive" / "composite-merged-fk" / subdir
    if not base.exists():
        return []
    return sorted(base.glob("part-*.csv"))


def _bi_csv_glob(dataset_dir: Path, subdir: str) -> list[Path]:
    base = (
        dataset_dir / "graphs" / "csv" / "bi" / "composite-merged-fk"
        / "initial_snapshot" / subdir
    )
    if not base.exists():
        return []
    return sorted(base.glob("part-*.csv"))


def _read_ids(part_files: list[Path], limit: int = 5000) -> list[int]:
    out: list[int] = []
    for path in part_files:
        with path.open("r", encoding="utf-8") as fp:
            for row in csv.DictReader(fp, delimiter="|"):
                try:
                    out.append(int(row["id"]))
                except (KeyError, ValueError):
                    continue
                if len(out) >= limit:
                    return out
    return out


def _read_persons_first_names(iv2_dir: Path, limit: int = 2000) -> list[str]:
    names: list[str] = []
    for path in _iv2_csv_glob(iv2_dir, "dynamic/Person"):
        with path.open("r", encoding="utf-8") as fp:
            for row in csv.DictReader(fp, delimiter="|"):
                fn = row.get("firstName")
                if fn:
                    names.append(fn)
                if len(names) >= limit:
                    return names
    return names


class SnbAnalyticalWorkload:
    name = "snb_analytical"

    def __init__(
        self,
        *,
        iv2_parameter_dir: Path | None = None,
        bi_parameter_dir: Path | None = None,
        iv2_dataset_dir: Path | None = None,
        bi_dataset_dir: Path | None = None,
    ) -> None:
        # Equal weighting across the eight refs (per Progress Report).
        weights: dict[QueryRef, float] = {ref: 1.0 for ref in _IC_REFS + _BI_REFS}
        self.mix = WorkloadMix(weights=weights)
        self._iv2_param_dir = iv2_parameter_dir
        self._bi_param_dir = bi_parameter_dir
        self._iv2_dataset_dir = iv2_dataset_dir
        self._bi_dataset_dir = bi_dataset_dir

    def iter_requests(self, seed: int) -> Iterator[QueryRequest]:
        # Prefer curated parameters when both dirs are present and exist.
        if (
            self._iv2_param_dir is not None and self._iv2_param_dir.exists()
            and self._bi_param_dir is not None and self._bi_param_dir.exists()
        ):
            yield from self._iter_from_curation(seed)
            return
        # Fall back to dataset-derived synthesis when both dataset roots exist.
        if (
            self._iv2_dataset_dir is not None and self._iv2_dataset_dir.exists()
            and self._bi_dataset_dir is not None and self._bi_dataset_dir.exists()
        ):
            yield from self._iter_from_dataset(seed)
            return
        raise RuntimeError(
            "snb_analytical: provide BOTH iv2_parameter_dir + bi_parameter_dir "
            "(curated) OR BOTH iv2_dataset_dir + bi_dataset_dir (raw datagen) "
            "via workload.options. Got: "
            f"iv2_param={self._iv2_param_dir} bi_param={self._bi_param_dir} "
            f"iv2_data={self._iv2_dataset_dir} bi_data={self._bi_dataset_dir}"
        )

    def _iter_from_curation(self, seed: int) -> Iterator[QueryRequest]:
        cyclers: dict[QueryRef, SeededParamCycler] = {}
        for offset, ref in enumerate(_IC_REFS):
            f = find_param_file(self._iv2_param_dir, ref.id.lower())
            if f is None:
                continue
            cyclers[ref] = SeededParamCycler(read_param_file(f), seed=seed + offset)
        for offset, ref in enumerate(_BI_REFS, start=len(_IC_REFS)):
            f = find_param_file(self._bi_param_dir, ref.id.lower())
            if f is None:
                continue
            cyclers[ref] = SeededParamCycler(read_param_file(f), seed=seed + offset)

        if not cyclers:
            raise RuntimeError(
                f"snb_analytical: no parameter files matched any ref under "
                f"{self._iv2_param_dir} or {self._bi_param_dir}"
            )

        rng = random.Random(seed)
        refs = list(cyclers.keys())
        weights = [self.mix.weights[r] for r in refs]
        while True:
            ref = rng.choices(refs, weights=weights, k=1)[0]
            yield QueryRequest(ref=ref, params=next(cyclers[ref]))

    def _iter_from_dataset(self, seed: int) -> Iterator[QueryRequest]:
        rng = random.Random(seed)
        iv2_person_ids = _read_ids(_iv2_csv_glob(self._iv2_dataset_dir, "dynamic/Person"))
        if not iv2_person_ids:
            raise RuntimeError(f"no Person rows under {self._iv2_dataset_dir}")
        first_names = _read_persons_first_names(self._iv2_dataset_dir) or ["Anon"]
        bi_tag_ids = _read_ids(_bi_csv_glob(self._bi_dataset_dir, "static/Tag"))
        bi_forum_ids = _read_ids(_bi_csv_glob(self._bi_dataset_dir, "dynamic/Forum"))
        bi_person_ids = (
            _read_ids(_bi_csv_glob(self._bi_dataset_dir, "dynamic/Person"))
            or iv2_person_ids
        )
        if not bi_tag_ids:
            bi_tag_ids = [0]
        if not bi_forum_ids:
            bi_forum_ids = [0]

        # IC params reuse iv2 Person IDs and (for IC1) firstName.
        def ic_params(ref: QueryRef) -> dict[str, Any]:
            row: dict[str, Any] = {"personId": rng.choice(iv2_person_ids)}
            if ref.id == "IC1":
                row["firstName"] = rng.choice(first_names)
            return row

        # BI Q params per ref.
        def q_params(ref: QueryRef) -> dict[str, Any]:
            if ref.id == "Q1":
                return {"minLength": rng.randint(0, 50)}
            if ref.id == "Q2":
                return {"tagId": rng.choice(bi_tag_ids)}
            if ref.id == "Q6":
                return {"forumId": rng.choice(bi_forum_ids)}
            if ref.id == "Q11":
                return {"personId": rng.choice(bi_person_ids)}
            return {}

        refs = _IC_REFS + _BI_REFS
        weights = [self.mix.weights[r] for r in refs]
        while True:
            ref = rng.choices(refs, weights=weights, k=1)[0]
            params = ic_params(ref) if ref in _IC_REFS else q_params(ref)
            yield QueryRequest(ref=ref, params=params)


def _factory(config: dict[str, Any]) -> SnbAnalyticalWorkload:
    iv2 = config.get("iv2_parameter_dir")
    bi = config.get("bi_parameter_dir")
    iv2_d = config.get("iv2_dataset_dir")
    bi_d = config.get("bi_dataset_dir")
    return SnbAnalyticalWorkload(
        iv2_parameter_dir=Path(iv2) if iv2 else None,
        bi_parameter_dir=Path(bi) if bi else None,
        iv2_dataset_dir=Path(iv2_d) if iv2_d else None,
        bi_dataset_dir=Path(bi_d) if bi_d else None,
    )


WorkloadRegistry.register("snb_analytical", _factory, overwrite=True)
