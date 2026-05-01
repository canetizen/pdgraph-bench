# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""One-second rolling throughput counters, persisted as Parquet."""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from graph_bench.utils.clock import wall_ns


_NS_PER_S = 1_000_000_000


_SCHEMA = pa.schema(
    [
        ("bucket_ns", pa.int64()),
        ("query_class", pa.string()),
        ("ok_count", pa.int64()),
        ("error_count", pa.int64()),
        ("timeout_count", pa.int64()),
    ]
)


class ThroughputSink:
    """Count per-class request outcomes into 1-second buckets."""

    def __init__(self, path: Path, t0_ns: int | None = None) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._path = path
        self._t0_ns = t0_ns if t0_ns is not None else wall_ns()
        # bucket_index -> query_class -> {ok, error, timeout}
        self._buckets: dict[int, dict[str, dict[str, int]]] = defaultdict(
            lambda: defaultdict(lambda: {"ok": 0, "error": 0, "timeout": 0})
        )

    def record(self, query_class: str, status: str, completed_ns: int) -> None:
        bucket = (completed_ns - self._t0_ns) // _NS_PER_S
        counters = self._buckets[bucket][query_class]
        if status in counters:
            counters[status] += 1
        else:
            # Unknown statuses become errors; silent dropping is banned in this codebase.
            counters["error"] += 1

    def close(self) -> None:
        rows: list[dict[str, object]] = []
        for bucket_idx in sorted(self._buckets):
            bucket_start_ns = self._t0_ns + bucket_idx * _NS_PER_S
            for qclass, counts in self._buckets[bucket_idx].items():
                rows.append(
                    {
                        "bucket_ns": bucket_start_ns,
                        "query_class": qclass,
                        "ok_count": counts["ok"],
                        "error_count": counts["error"],
                        "timeout_count": counts["timeout"],
                    }
                )
        table = pa.Table.from_pylist(rows, schema=_SCHEMA)
        pq.write_table(table, self._path, compression="zstd")

    def __enter__(self) -> "ThroughputSink":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
