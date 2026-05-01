# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""Per-node CPU / memory / disk / network sampler.

Uses `psutil` to sample the process's host every N seconds. Samples are written
directly to Parquet; aggregation is done in the analysis layer.

When running on a real cluster, each SUT node runs its own `ResourceSampler`
inside the benchmark harness container and results are rsync-collected back to
the client node. When running in-process tests (mock SUT), a single sampler
observes the test runner itself.
"""

from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass
from pathlib import Path

import psutil
import pyarrow as pa
import pyarrow.parquet as pq

from graph_bench.utils.clock import wall_ns


@dataclass(frozen=True, slots=True)
class ResourceSample:
    ts_ns: int
    host: str
    cpu_percent: float
    mem_used_bytes: int
    mem_total_bytes: int
    disk_read_bytes: int
    disk_write_bytes: int
    net_recv_bytes: int
    net_sent_bytes: int


_SCHEMA = pa.schema(
    [
        ("ts_ns", pa.int64()),
        ("host", pa.string()),
        ("cpu_percent", pa.float64()),
        ("mem_used_bytes", pa.int64()),
        ("mem_total_bytes", pa.int64()),
        ("disk_read_bytes", pa.int64()),
        ("disk_write_bytes", pa.int64()),
        ("net_recv_bytes", pa.int64()),
        ("net_sent_bytes", pa.int64()),
    ]
)


class ResourceSampler:
    """Periodic system-resource sampler writing to a Parquet file."""

    def __init__(self, path: Path, host: str, interval_s: float = 5.0) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._path = path
        self._host = host
        self._interval_s = interval_s
        self._samples: list[ResourceSample] = []
        self._task: asyncio.Task[None] | None = None
        self._stopped = asyncio.Event()

    def _take_sample(self) -> ResourceSample:
        mem = psutil.virtual_memory()
        disk = psutil.disk_io_counters()
        net = psutil.net_io_counters()
        return ResourceSample(
            ts_ns=wall_ns(),
            host=self._host,
            cpu_percent=psutil.cpu_percent(interval=None),
            mem_used_bytes=mem.used,
            mem_total_bytes=mem.total,
            disk_read_bytes=getattr(disk, "read_bytes", 0),
            disk_write_bytes=getattr(disk, "write_bytes", 0),
            net_recv_bytes=net.bytes_recv,
            net_sent_bytes=net.bytes_sent,
        )

    async def _run(self) -> None:
        # Prime psutil's CPU percent calculation.
        psutil.cpu_percent(interval=None)
        while not self._stopped.is_set():
            self._samples.append(self._take_sample())
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self._stopped.wait(), timeout=self._interval_s)

    async def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="resource-sampler")

    async def stop(self) -> None:
        self._stopped.set()
        if self._task is not None:
            await self._task
            self._task = None
        self._flush()

    def _flush(self) -> None:
        if not self._samples:
            # Write an empty file so downstream analysis can distinguish "no
            # samples" from "sampler never ran".
            table = pa.Table.from_pylist([], schema=_SCHEMA)
        else:
            table = pa.Table.from_pylist(
                [
                    {
                        "ts_ns": s.ts_ns,
                        "host": s.host,
                        "cpu_percent": s.cpu_percent,
                        "mem_used_bytes": s.mem_used_bytes,
                        "mem_total_bytes": s.mem_total_bytes,
                        "disk_read_bytes": s.disk_read_bytes,
                        "disk_write_bytes": s.disk_write_bytes,
                        "net_recv_bytes": s.net_recv_bytes,
                        "net_sent_bytes": s.net_sent_bytes,
                    }
                    for s in self._samples
                ],
                schema=_SCHEMA,
            )
        pq.write_table(table, self._path, compression="zstd")
