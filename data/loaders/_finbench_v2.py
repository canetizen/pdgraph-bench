# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""Shared LDBC FinBench loader helpers.

The FinBench Datagen emits one Spark-partitioned subdirectory per entity:

    <dataset>/raw/<entity>/part-*.csv

Helpers exported:

- ``FinbenchDatasetLayout`` resolves the per-entity directory + glob
- ``iter_vertex_rows``  yields ``(id, scalar_props)``
- ``iter_edge_rows``    yields ``(src_id, dst_id, props)``

Soft-delete columns on edge CSVs (``deleteTime``, ``isExplicitDeleted``) are
read as ordinary properties; we do not enforce the temporal-deletion semantics.
"""

from __future__ import annotations

import csv
import datetime as dt
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from graph_bench.workloads.finbench.schema import EdgeType, VertexType


@dataclass(frozen=True, slots=True)
class FinbenchDatasetLayout:
    base: Path

    @property
    def root(self) -> Path:
        return self.base / "raw"

    def part_files(self, csv_subdir: str) -> list[Path]:
        d = self.root / csv_subdir
        if not d.exists():
            return []
        return sorted(d.glob("part-*.csv"))


def _coerce(value: str | None, dtype: str) -> Any:
    if value is None or value == "":
        return None
    if dtype in {"id", "int", "long", "bigint"}:
        try:
            return int(value)
        except ValueError:
            return None
    if dtype == "float":
        try:
            return float(value)
        except ValueError:
            return None
    if dtype == "datetime":
        try:
            return int(value)
        except ValueError:
            try:
                return int(dt.datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp())
            except ValueError:
                return int(dt.datetime.strptime(value[:19], "%Y-%m-%dT%H:%M:%S").timestamp())
    return value


def _iter_csv(part_files: list[Path]) -> Iterator[dict[str, str]]:
    for path in part_files:
        with path.open("r", encoding="utf-8") as fp:
            for row in csv.DictReader(fp, delimiter="|"):
                yield row


def iter_vertex_rows(
    vertex: VertexType, layout: FinbenchDatasetLayout
) -> Iterator[tuple[int, dict[str, Any]]]:
    parts = layout.part_files(vertex.csv_subdir)
    if not parts:
        return
    for row in _iter_csv(parts):
        try:
            vid = int(row["id"])
        except (KeyError, ValueError):
            continue
        props: dict[str, Any] = {}
        for p in vertex.properties:
            v = _coerce(row.get(p.name, ""), p.dtype)
            if v is not None:
                props[p.name] = v
        yield vid, props


def iter_edge_rows(
    edge: EdgeType, layout: FinbenchDatasetLayout
) -> Iterator[tuple[int, int, dict[str, Any]]]:
    parts = layout.part_files(edge.csv_subdir)
    if not parts:
        return
    for row in _iter_csv(parts):
        try:
            src = int(row[edge.src_column])
            dst = int(row[edge.dst_column])
        except (KeyError, ValueError):
            continue
        props: dict[str, Any] = {}
        for p in edge.properties:
            v = _coerce(row.get(p.name, ""), p.dtype)
            if v is not None:
                props[p.name] = v
        yield src, dst, props


def batched(iterator: Iterator, size: int) -> Iterator[list]:
    batch: list = []
    for item in iterator:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch
