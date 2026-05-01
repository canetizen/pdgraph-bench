# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""Shared Gremlin-side loading helpers used by JanusGraph + HugeGraph loaders.

Both Tier-2 systems speak Gremlin; the only differences worth abstracting are
the connection mechanics (WebSocket for JanusGraph, REST for HugeGraph) and
schema-creation primitives. CSV row reading and parameter coercion mirror the
LDBC SNB / FinBench helpers in `_ldbc_common`.
"""

from __future__ import annotations

import csv
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any, Protocol


class _PropLike(Protocol):
    name: str
    dtype: str


class _VertexLike(Protocol):
    name: str
    csv_filename: str
    properties: tuple[_PropLike, ...]


class _EdgeLike(Protocol):
    name: str
    src: str
    dst: str
    csv_filename: str
    properties: tuple[_PropLike, ...]


def _coerce(value: str | None, dtype: str) -> Any:
    """Coerce one CSV cell into a Python value of the right Python type."""
    import datetime as dt

    if value is None or value == "":
        return None
    if dtype in {"int", "id", "bigint"}:
        return int(value)
    if dtype == "float":
        return float(value)
    if dtype == "datetime":
        try:
            return int(dt.datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp())
        except ValueError:
            return int(dt.datetime.strptime(value[:19], "%Y-%m-%dT%H:%M:%S").timestamp())
    if dtype == "date":
        return int(dt.datetime.fromisoformat(value).timestamp())
    return value


def iter_vertex_rows(vertex: _VertexLike, csv_path: Path) -> Iterator[tuple[int, dict[str, Any]]]:
    """Yield (id, properties) for each vertex row in `csv_path`."""
    if not csv_path.exists():
        return
    with csv_path.open("r", encoding="utf-8") as fp:
        for row in csv.DictReader(fp, delimiter="|"):
            vid = int(row["id"])
            props: dict[str, Any] = {}
            for p in vertex.properties:
                if p.name == "id":
                    continue
                v = _coerce(row.get(p.name, ""), p.dtype)
                if v is not None:
                    props[p.name] = v
            yield vid, props


def iter_edge_rows(edge: _EdgeLike, csv_path: Path) -> Iterator[tuple[int, int, dict[str, Any]]]:
    """Yield (src_id, dst_id, properties) for each edge row in `csv_path`."""
    if not csv_path.exists():
        return
    with csv_path.open("r", encoding="utf-8") as fp:
        reader = csv.DictReader(fp, delimiter="|")
        if reader.fieldnames is None:
            return
        src_col, dst_col = reader.fieldnames[0], reader.fieldnames[1]
        for row in reader:
            src = int(row[src_col])
            dst = int(row[dst_col])
            props: dict[str, Any] = {}
            for p in edge.properties:
                v = _coerce(row.get(p.name, ""), p.dtype)
                if v is not None:
                    props[p.name] = v
            yield src, dst, props


def batched(iterable: Iterator, size: int) -> Iterator[list]:
    """Yield successive `size`-element batches from `iterable`."""
    batch: list = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def gremlin_vertex_script(label: str, vid: int, props: dict[str, Any]) -> str:
    """Build a Gremlin `addV` snippet for one vertex."""
    parts = [f"g.addV('{label}').property('id', {vid}L)"]
    for k, v in props.items():
        if isinstance(v, str):
            esc = v.replace("\\", "\\\\").replace("'", "\\'")
            parts.append(f".property('{k}', '{esc}')")
        elif isinstance(v, bool):
            parts.append(f".property('{k}', {str(v).lower()})")
        elif isinstance(v, int):
            parts.append(f".property('{k}', {v}L)")
        else:
            parts.append(f".property('{k}', {v})")
    return "".join(parts) + ".iterate()"


def gremlin_edge_script(label: str, src_label: str, src: int, dst_label: str, dst: int, props: dict[str, Any]) -> str:
    """Build a Gremlin `addE` snippet locating endpoints by `id` property."""
    parts = [
        f"g.V().has('{src_label}','id',{src}L).as('a')"
        f".V().has('{dst_label}','id',{dst}L).as('b')"
        f".addE('{label}').from('a').to('b')"
    ]
    for k, v in props.items():
        if isinstance(v, str):
            esc = v.replace("\\", "\\\\").replace("'", "\\'")
            parts.append(f".property('{k}', '{esc}')")
        elif isinstance(v, bool):
            parts.append(f".property('{k}', {str(v).lower()})")
        elif isinstance(v, int):
            parts.append(f".property('{k}', {v}L)")
        else:
            parts.append(f".property('{k}', {v})")
    return "".join(parts) + ".iterate()"


def submit_batched(
    submit: Callable[[str], None],
    scripts: Iterator[str],
    batch_size: int = 50,
) -> int:
    """Submit Gremlin script lines in `;`-joined batches; return total count."""
    n = 0
    for chunk in batched(scripts, batch_size):
        submit("; ".join(chunk))
        n += len(chunk)
    return n
