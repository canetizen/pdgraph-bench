# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""Per-request log persisted as Apache Parquet.

One row per query execution. The schema is wide enough to allow any downstream
analysis (latency percentiles, failed-query rate, per-worker fairness, query
class distribution) without re-running the benchmark.

Rows are buffered in memory and flushed in batches to keep write amortized;
`close()` drains the remaining buffer. Typical throughputs (low thousands of
ops/s across 16 workers) produce a few million rows per 15-minute window, which
fits comfortably in a single Parquet file per run.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq


@dataclass(frozen=True, slots=True)
class RequestRecord:
    """Per-request outcome, timed by the harness around `Driver.execute`."""

    issued_ns: int
    """Wall-clock UTC nanoseconds when the request was handed to the driver."""

    completed_ns: int
    """Wall-clock UTC nanoseconds when the driver returned."""

    latency_us: int
    """Monotonic elapsed microseconds (preferred over `completed_ns - issued_ns`)."""

    worker_id: int
    query_class: str
    status: str
    row_count: int
    result_hash: str | None
    error_message: str | None


_SCHEMA = pa.schema(
    [
        ("issued_ns", pa.int64()),
        ("completed_ns", pa.int64()),
        ("latency_us", pa.int64()),
        ("worker_id", pa.int32()),
        ("query_class", pa.string()),
        ("status", pa.string()),
        ("row_count", pa.int64()),
        ("result_hash", pa.string()),
        ("error_message", pa.string()),
    ]
)


class RequestLogSink:
    """Buffered Parquet sink for `RequestRecord`s."""

    def __init__(self, path: Path, batch_size: int = 10_000) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._path = path
        self._batch_size = batch_size
        self._buffer: list[dict[str, Any]] = []
        self._writer: pq.ParquetWriter | None = None

    def record(self, row: RequestRecord) -> None:
        self._buffer.append(asdict(row))
        if len(self._buffer) >= self._batch_size:
            self._flush()

    def _flush(self) -> None:
        if not self._buffer:
            return
        table = pa.Table.from_pylist(self._buffer, schema=_SCHEMA)
        if self._writer is None:
            self._writer = pq.ParquetWriter(self._path, _SCHEMA, compression="zstd")
        self._writer.write_table(table)
        self._buffer.clear()

    def close(self) -> None:
        self._flush()
        if self._writer is not None:
            self._writer.close()
            self._writer = None

    def __enter__(self) -> "RequestLogSink":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
