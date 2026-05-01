# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""Helpers to read LDBC parameter-curation files.

The LDBC SNB Datagen emits per-query parameter files in CSV with `|` as the
field separator (e.g., `interactive-1.csv` carries the curated parameters for
IS1 / IC1 / etc., depending on the profile). The schema is heterogeneous
across queries; the helpers here treat each row as an unvalidated dict so that
the workload code stays one layer above the file format.
"""

from __future__ import annotations

import csv
import random
from collections.abc import Iterator
from pathlib import Path


def read_param_file(path: Path) -> list[dict[str, str]]:
    """Read one LDBC parameter file (CSV with `|` separator) into a list of dicts."""
    with path.open("r", encoding="utf-8") as fp:
        reader = csv.DictReader(fp, delimiter="|")
        return [dict(row) for row in reader]


def find_param_file(base: Path, query_id: str) -> Path | None:
    """Locate the parameter file for a given query id under `base`.

    Looks for `<query_id>.csv`, `<query_id>.parquet`, or any file whose stem
    contains the query id (case-insensitive). Returns `None` if nothing matches.
    """
    if not base.exists():
        return None
    direct = base / f"{query_id}.csv"
    if direct.exists():
        return direct
    parquet = base / f"{query_id}.parquet"
    if parquet.exists():
        return parquet
    qid = query_id.lower()
    for candidate in base.iterdir():
        if candidate.is_file() and qid in candidate.stem.lower():
            return candidate
    return None


class SeededParamCycler:
    """Cycle through parameter rows with a deterministic, seeded order.

    LDBC parameter-curation files are finite; for long-running benchmarks the
    iterator wraps around. To avoid repeating the same parameter sequence on
    every wrap, the order is shuffled per-cycle with the seeded RNG.
    """

    def __init__(self, rows: list[dict[str, str]], seed: int) -> None:
        if not rows:
            raise ValueError("parameter file is empty")
        self._rows = rows
        self._rng = random.Random(seed)
        self._order: list[int] = []
        self._idx = 0
        self._reshuffle()

    def _reshuffle(self) -> None:
        n = len(self._rows)
        self._order = list(range(n))
        self._rng.shuffle(self._order)
        self._idx = 0

    def __iter__(self) -> Iterator[dict[str, str]]:
        return self

    def __next__(self) -> dict[str, str]:
        if self._idx >= len(self._order):
            self._reshuffle()
        row = self._rows[self._order[self._idx]]
        self._idx += 1
        return row
