# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""LDBC SNB Interactive v2 workload.

Two parameter sources, picked at construction time:

- ``parameter_dir`` — pointer to an LDBC SNB parameter-curation directory
  (one ``<query_id>.csv`` per query class). Used in production once the LDBC
  SNB Driver's parameter-curation pass has been run.
- ``dataset_dir`` — pointer to the LDBC SNB Datagen output (composite-merged-fk
  layout). When given without ``parameter_dir``, the workload derives a
  parameter pool from the dataset's vertex CSVs so the harness can run
  end-to-end without the curation pass. This is the playground default.
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


_IS_REFS = [QueryRef("snb_iv2", f"IS{i}") for i in range(1, 8)]
_IC_REFS = [QueryRef("snb_iv2", f"IC{i}") for i in (1, 3, 5, 6)]
_IU_REFS = [QueryRef("snb_iv2", f"IU{i}") for i in (1, 2, 6)]


# Map each ref to which dataset-derived pool name and which parameter-name
# key the loader should emit.
_DATASET_PARAM_SPEC: dict[str, tuple[str, ...]] = {
    "IS1": ("personId",),
    "IS2": ("personId",),
    "IS3": ("personId",),
    "IS4": ("messageId",),
    "IS5": ("messageId",),
    "IS6": ("messageId",),
    "IS7": ("messageId",),
    "IC1": ("personId", "firstName"),
    "IC3": ("personId",),
    "IC5": ("personId",),
    "IC6": ("personId",),
    "IU1": ("personId", "firstName", "lastName", "gender", "birthday",
            "creationDate", "locationIP", "browserUsed"),
    "IU2": ("personId", "postId", "creationDate"),
    "IU6": ("commentId", "creationDate", "locationIP", "browserUsed",
            "content", "length"),
}


def _composite_csv_glob(dataset_dir: Path, subdir: str) -> list[Path]:
    base = dataset_dir / "graphs" / "csv" / "interactive" / "composite-merged-fk" / subdir
    if not base.exists():
        return []
    return sorted(base.glob("part-*.csv"))


def _read_ids(dataset_dir: Path, subdir: str, limit: int = 5000) -> list[int]:
    ids: list[int] = []
    for path in _composite_csv_glob(dataset_dir, subdir):
        with path.open("r", encoding="utf-8") as fp:
            for row in csv.DictReader(fp, delimiter="|"):
                try:
                    ids.append(int(row["id"]))
                except (KeyError, ValueError):
                    continue
                if len(ids) >= limit:
                    return ids
    return ids


def _read_persons(dataset_dir: Path, limit: int = 5000) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for path in _composite_csv_glob(dataset_dir, "dynamic/Person"):
        with path.open("r", encoding="utf-8") as fp:
            for r in csv.DictReader(fp, delimiter="|"):
                rows.append(r)
                if len(rows) >= limit:
                    return rows
    return rows


def _build_dataset_params(dataset_dir: Path, seed: int) -> dict[QueryRef, list[dict[str, Any]]]:
    """Synthesise a small parameter pool per ref from the loaded vertex CSVs."""
    rng = random.Random(seed)
    persons = _read_persons(dataset_dir)
    person_ids = [int(r["id"]) for r in persons]
    post_ids = _read_ids(dataset_dir, "dynamic/Post")
    comment_ids = _read_ids(dataset_dir, "dynamic/Comment")
    message_ids = post_ids + comment_ids
    if not person_ids:
        raise RuntimeError(f"no Person rows under {dataset_dir}")
    if not message_ids:
        message_ids = person_ids  # degrade gracefully
    first_names = [r.get("firstName", "Anon") for r in persons if r.get("firstName")]
    if not first_names:
        first_names = ["Anon"]

    pools: dict[QueryRef, list[dict[str, Any]]] = {}
    for ref in _IS_REFS + _IC_REFS + _IU_REFS:
        spec = _DATASET_PARAM_SPEC.get(ref.id, ("personId",))
        pool: list[dict[str, Any]] = []
        for _ in range(min(500, len(person_ids))):
            row: dict[str, Any] = {}
            for key in spec:
                if key == "personId":
                    row[key] = rng.choice(person_ids)
                elif key == "messageId":
                    row[key] = rng.choice(message_ids)
                elif key == "postId":
                    row[key] = rng.choice(post_ids) if post_ids else rng.choice(person_ids)
                elif key == "commentId":
                    row[key] = rng.choice(comment_ids) if comment_ids else rng.choice(person_ids)
                elif key == "firstName":
                    row[key] = rng.choice(first_names)
                elif key == "lastName":
                    row[key] = rng.choice([r.get("lastName", "Anon") for r in persons]) or "Anon"
                elif key == "gender":
                    row[key] = "male"
                elif key == "birthday":
                    row[key] = 0
                elif key == "creationDate":
                    row[key] = 0
                elif key == "locationIP":
                    row[key] = "127.0.0.1"
                elif key == "browserUsed":
                    row[key] = "Firefox"
                elif key == "content":
                    row[key] = "ok"
                elif key == "length":
                    row[key] = 2
            pool.append(row)
        pools[ref] = pool
    return pools


class SnbInteractiveV2Workload:
    name = "snb_iv2"

    def __init__(
        self,
        *,
        parameter_dir: Path | None = None,
        dataset_dir: Path | None = None,
        mix_weights: dict[str, float] | None = None,
    ) -> None:
        weights: dict[QueryRef, float] = {}
        for ref in _IS_REFS:
            weights[ref] = (mix_weights or {}).get(ref.id, 70.0 / len(_IS_REFS))
        for ref in _IC_REFS:
            weights[ref] = (mix_weights or {}).get(ref.id, 20.0 / len(_IC_REFS))
        for ref in _IU_REFS:
            weights[ref] = (mix_weights or {}).get(ref.id, 10.0 / len(_IU_REFS))
        self.mix = WorkloadMix(weights=weights)
        self._parameter_dir = parameter_dir
        self._dataset_dir = dataset_dir

    def iter_requests(self, seed: int) -> Iterator[QueryRequest]:
        if self._parameter_dir is not None and self._parameter_dir.exists():
            yield from self._iter_from_curation(seed)
        elif self._dataset_dir is not None and self._dataset_dir.exists():
            yield from self._iter_from_dataset(seed)
        else:
            raise RuntimeError(
                "snb_iv2: provide workload.options.parameter_dir (LDBC SNB Driver "
                "parameter-curation output) OR workload.options.dataset_dir (raw "
                "datagen output for in-process parameter synthesis)."
            )

    def _iter_from_curation(self, seed: int) -> Iterator[QueryRequest]:
        cyclers: dict[QueryRef, SeededParamCycler] = {}
        for offset, ref in enumerate(self.mix.weights):
            f = find_param_file(self._parameter_dir, ref.id.lower())
            if f is None:
                continue
            cyclers[ref] = SeededParamCycler(read_param_file(f), seed=seed + offset)
        if not cyclers:
            raise RuntimeError(
                f"no LDBC parameter files matched any query id under {self._parameter_dir}"
            )
        rng = random.Random(seed)
        refs = list(cyclers.keys())
        weights = [self.mix.weights[r] for r in refs]
        while True:
            ref = rng.choices(refs, weights=weights, k=1)[0]
            yield QueryRequest(ref=ref, params=next(cyclers[ref]))

    def _iter_from_dataset(self, seed: int) -> Iterator[QueryRequest]:
        pools = _build_dataset_params(self._dataset_dir, seed)
        cyclers = {ref: SeededParamCycler(rows, seed=seed + i) for i, (ref, rows) in enumerate(pools.items())}
        rng = random.Random(seed)
        refs = list(cyclers.keys())
        weights = [self.mix.weights[r] for r in refs]
        while True:
            ref = rng.choices(refs, weights=weights, k=1)[0]
            yield QueryRequest(ref=ref, params=next(cyclers[ref]))


def _factory(config: dict[str, Any]) -> SnbInteractiveV2Workload:
    pd = config.get("parameter_dir")
    dd = config.get("dataset_dir")
    return SnbInteractiveV2Workload(
        parameter_dir=Path(pd) if pd else None,
        dataset_dir=Path(dd) if dd else None,
        mix_weights=config.get("mix"),
    )


WorkloadRegistry.register("snb_iv2", _factory, overwrite=True)
