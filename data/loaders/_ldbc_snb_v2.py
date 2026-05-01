# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""Shared LDBC SNB Datagen loader helpers (composite-merged-fk layout).

The official ``ldbc/datagen-standalone`` image emits a Spark-partitioned tree:

    <dataset>/graphs/csv/<mode>/composite-merged-fk/{dynamic,static}/<Entity>/part-*.csv

These helpers expose four primitives that every per-SUT loader uses:

- ``LdbcDatasetLayout`` resolves the per-entity directory + glob
- ``iter_vertex_rows(vertex, layout)`` yields ``(id, scalar_props)`` tuples
- ``iter_fk_edge_rows(vertex, fk, layout)`` yields ``(src_id, dst_id, props)``
  for edges synthesised from a vertex foreign key
- ``iter_edge_rows(edge, layout)`` yields ``(src_id, dst_id, props)`` for
  standalone edge CSVs

The output is workload-agnostic: per-SUT loaders pass each tuple through the
SUT's bulk-insert API.
"""

from __future__ import annotations

import csv
import datetime as dt
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from graph_bench.workloads.snb_iv2.schema import EdgeType, ForeignKey, Property, VertexType


@dataclass(frozen=True, slots=True)
class LdbcDatasetLayout:
    """Resolves a `csv_subdir` to a list of part files.

    `mode` is `"interactive"` or `"bi"`; it selects between
    ``graphs/csv/interactive/composite-merged-fk/`` and
    ``graphs/csv/bi/composite-merged-fk/``. The two trees are otherwise
    identical for the entities Tier-1 queries touch.
    """

    base: Path
    mode: str = "interactive"

    @property
    def root(self) -> Path:
        return self.base / "graphs" / "csv" / self.mode / "composite-merged-fk"

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
        return float(value)
    if dtype == "datetime":
        # Datagen emits epoch millis as longs in `--epoch-millis` mode and ISO
        # strings otherwise. Tolerate both.
        try:
            return int(value)
        except ValueError:
            try:
                return int(dt.datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp())
            except ValueError:
                return int(dt.datetime.strptime(value[:19], "%Y-%m-%dT%H:%M:%S").timestamp())
    if dtype == "date":
        try:
            return int(value)
        except ValueError:
            return int(dt.datetime.fromisoformat(value).timestamp())
    return value


def _iter_csv(part_files: list[Path]) -> Iterator[dict[str, str]]:
    for path in part_files:
        with path.open("r", encoding="utf-8") as fp:
            for row in csv.DictReader(fp, delimiter="|"):
                yield row


def iter_vertex_rows(
    vertex: VertexType, layout: LdbcDatasetLayout
) -> Iterator[tuple[int, dict[str, Any]]]:
    """Yield ``(id, scalar_properties)`` for each vertex row.

    Foreign key columns are intentionally NOT included in the scalar dict;
    they are recovered separately via `iter_fk_edge_rows`. This keeps the
    SUT-side vertex insert call independent of edge synthesis.
    """
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


def iter_fk_edge_rows(
    vertex: VertexType,
    fk: ForeignKey,
    layout: LdbcDatasetLayout,
) -> Iterator[tuple[int, int, dict[str, Any]]]:
    """Yield ``(src_id, dst_id, props)`` for one FK on a vertex.

    The owning vertex's id and the FK column form the (src, dst) pair, with
    the order swapped when ``fk.direction == "in"``. No edge properties are
    emitted (FK-derived edges in the LDBC SNB schema carry none).
    """
    parts = layout.part_files(vertex.csv_subdir)
    if not parts:
        return
    for row in _iter_csv(parts):
        try:
            self_id = int(row["id"])
        except (KeyError, ValueError):
            continue
        raw_target = row.get(fk.column, "")
        if raw_target is None or raw_target == "":
            continue
        try:
            target = int(raw_target)
        except ValueError:
            continue
        if fk.direction == "in":
            src, dst = target, self_id
        else:
            src, dst = self_id, target
        yield src, dst, {}


def iter_edge_rows(
    edge: EdgeType, layout: LdbcDatasetLayout
) -> Iterator[tuple[int, int, dict[str, Any]]]:
    """Yield ``(src_id, dst_id, properties)`` for each row of an edge CSV."""
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
    """Yield successive `size`-element batches from `iterator`."""
    batch: list = []
    for item in iterator:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch
