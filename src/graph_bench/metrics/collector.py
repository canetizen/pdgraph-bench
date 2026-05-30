# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""`MetricsCollector` — wires a run's output artifacts together.

Responsibilities:
- Own the paths for `results/<run_id>/` artifacts.
- Provide a single `record_request` entry point that fans out to all sinks.
- Own the `EventLog` and the `ResourceSampler` lifecycles.

The harness holds one `MetricsCollector` instance per scenario-repetition. The
collector is created *after* warmup ends so that warmup samples do not pollute
measurement statistics; emissions before `start_measurement()` are dropped.
"""

from __future__ import annotations

import asyncio
import socket
from pathlib import Path

from graph_bench.domain import QueryRef, QueryStatus
from graph_bench.metrics.events import EventLog
from graph_bench.metrics.latency import LatencySink
from graph_bench.metrics.remote_resource import RemoteEndpoint, RemoteResourceSampler
from graph_bench.metrics.requests import RequestLogSink, RequestRecord
from graph_bench.metrics.resource import ResourceSampler
from graph_bench.metrics.throughput import ThroughputSink


class MetricsCollector:
    """Fan-out collector that owns all per-run sinks."""

    def __init__(
        self,
        results_dir: Path,
        host: str | None = None,
        *,
        remote_endpoints: list[RemoteEndpoint] | None = None,
    ) -> None:
        self._dir = results_dir
        self._dir.mkdir(parents=True, exist_ok=True)
        self._host = host or socket.gethostname()
        self._measuring = False

        self.events = EventLog(self._dir / "events.jsonl")
        self.latency = LatencySink()
        self.requests = RequestLogSink(self._dir / "requests.parquet")
        self.throughput = ThroughputSink(self._dir / "throughput.parquet")
        self.resource = ResourceSampler(
            self._dir / "system_metrics.parquet", host=self._host
        )
        self.remote_resource: RemoteResourceSampler | None = None
        if remote_endpoints:
            self.remote_resource = RemoteResourceSampler(
                self._dir / "system_metrics.parquet", endpoints=remote_endpoints
            )

    @property
    def results_dir(self) -> Path:
        return self._dir

    async def start_measurement(self) -> None:
        """Begin recording. Before this is called, `record_request` drops samples."""
        self._measuring = True
        await self.resource.start()
        if self.remote_resource is not None:
            await self.remote_resource.start()
        self.events.emit("measurement_start", host=self._host)

    async def stop_measurement(self) -> None:
        """Stop recording, flush all sinks, and close files.

        Local sampler is stopped first so its Parquet write completes before
        the remote sampler appends; the remote sampler's `_flush` reads the
        file back, concatenates, and rewrites in one shot.
        """
        self.events.emit("measurement_stop", host=self._host)
        self._measuring = False
        await self.resource.stop()
        if self.remote_resource is not None:
            await self.remote_resource.stop()
        self.requests.close()
        self.throughput.close()
        self.latency.export(self._dir)
        self.events.close()

    def record_request(
        self,
        ref: QueryRef,
        status: QueryStatus,
        worker_id: int,
        issued_ns: int,
        completed_ns: int,
        latency_us: int,
        row_count: int = 0,
        result_hash: str | None = None,
        error_message: str | None = None,
    ) -> None:
        """Record one per-request outcome to all relevant sinks."""
        if not self._measuring:
            return

        self.requests.record(
            RequestRecord(
                issued_ns=issued_ns,
                completed_ns=completed_ns,
                latency_us=latency_us,
                worker_id=worker_id,
                query_class=str(ref),
                status=status.value,
                row_count=row_count,
                result_hash=result_hash,
                error_message=error_message,
            )
        )
        self.throughput.record(str(ref), status.value, completed_ns)
        if status is QueryStatus.OK:
            self.latency.record(ref, latency_us)

    async def __aenter__(self) -> "MetricsCollector":
        return self

    async def __aexit__(self, *_: object) -> None:
        if self._measuring:
            await self.stop_measurement()
