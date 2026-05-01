# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""Per-node resource sampler for remote SUT hosts.

For runs against a real cluster (production multi-host deployment) the
benchmark client lives on a different machine from each SUT node. The local
`ResourceSampler` only captures the client host; per-node CPU/mem/disk/net for
each SUT node must be pulled separately. This module exposes
`RemoteResourceSampler`, which keeps one Fabric SSH connection open per
cluster node and reads `/proc/stat`, `/proc/meminfo`, `/proc/diskstats`,
`/proc/net/dev` on each tick. Output is appended to the same Parquet schema as
the local sampler, with the `host` column distinguishing samples.

Falls back gracefully when the `deploy` extra (Fabric) is not installed:
attempting to start a remote sampler raises `ImportError` at start-time only,
so test runs that only need the local sampler do not pull Fabric in.
"""

from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from graph_bench.utils.clock import wall_ns
from graph_bench.utils.logging import get_logger

_log = get_logger(__name__)


# Same row schema as the local sampler (graph_bench.metrics.resource._SCHEMA).
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


# Compact shell snippet that emits one CSV line per invocation. Reads from
# /proc which is available on every Linux SUT node without extra packages.
_PROBE_SH = (
    "awk '/^cpu /{u=$2+$4;t=$2+$3+$4+$5+$6+$7+$8;print u\",\"t}' /proc/stat; "
    "awk '/^MemTotal:/{t=$2}/^MemAvailable:/{a=$2}END{print (t-a)*1024\",\"t*1024}' /proc/meminfo; "
    "awk 'BEGIN{r=0;w=0} $3 !~ /loop|ram|dm-/ {r+=$6;w+=$10} END{print r*512\",\"w*512}' /proc/diskstats; "
    "awk 'NR>2 && $1 !~ /lo:/ {gsub(/:/,\"\");rx+=$2;tx+=$10} END{print rx\",\"tx}' /proc/net/dev"
)


@dataclass(frozen=True, slots=True)
class _CpuSnapshot:
    used: float
    total: float


@dataclass(frozen=True, slots=True)
class RemoteEndpoint:
    """Identifies one remote host to sample (hostname + optional SSH user)."""

    name: str
    hostname: str
    ssh_user: str | None = None


class RemoteResourceSampler:
    """Periodically samples /proc on one or more remote hosts via SSH.

    Samples are buffered in memory and flushed to a single Parquet file on
    `stop()`. The host column lets analysis distinguish per-node series.
    """

    def __init__(
        self,
        path: Path,
        endpoints: list[RemoteEndpoint],
        *,
        interval_s: float = 5.0,
    ) -> None:
        if not endpoints:
            raise ValueError("RemoteResourceSampler needs at least one endpoint")
        path.parent.mkdir(parents=True, exist_ok=True)
        self._path = path
        self._endpoints = endpoints
        self._interval_s = interval_s
        self._samples: list[dict] = []
        self._task: asyncio.Task[None] | None = None
        self._stopped = asyncio.Event()
        # Keep one connection per endpoint open across the run.
        self._connections: dict[str, object] = {}
        # Previous cpu snapshot per host for delta-based percent.
        self._prev_cpu: dict[str, _CpuSnapshot] = {}

    def _open_connections(self) -> None:
        from fabric import Connection  # lazy: deploy extra is optional
        for ep in self._endpoints:
            kwargs: dict = {}
            if ep.ssh_user:
                kwargs["user"] = ep.ssh_user
            self._connections[ep.name] = Connection(host=ep.hostname, **kwargs)

    def _close_connections(self) -> None:
        for conn in self._connections.values():
            with contextlib.suppress(Exception):
                conn.close()  # type: ignore[attr-defined]
        self._connections.clear()

    def _probe_one(self, ep: RemoteEndpoint) -> dict | None:
        conn = self._connections[ep.name]
        try:
            r = conn.run(_PROBE_SH, hide=True, warn=True, pty=False)  # type: ignore[attr-defined]
        except Exception as exc:  # noqa: BLE001
            _log.warning("remote_resource_probe_failed", host=ep.hostname, error=str(exc))
            return None
        out = (r.stdout or "").strip().splitlines()
        if len(out) < 4:
            return None
        try:
            cu, ct = (float(x) for x in out[0].split(","))
            mu, mt = (int(x) for x in out[1].split(","))
            dr, dw = (int(x) for x in out[2].split(","))
            nr, nt = (int(x) for x in out[3].split(","))
        except ValueError:
            return None

        cpu_percent = 0.0
        prev = self._prev_cpu.get(ep.name)
        cur = _CpuSnapshot(used=cu, total=ct)
        if prev is not None and ct > prev.total:
            cpu_percent = max(0.0, min(100.0, (cu - prev.used) / (ct - prev.total) * 100.0))
        self._prev_cpu[ep.name] = cur

        return {
            "ts_ns": wall_ns(),
            "host": ep.name,
            "cpu_percent": cpu_percent,
            "mem_used_bytes": mu,
            "mem_total_bytes": mt,
            "disk_read_bytes": dr,
            "disk_write_bytes": dw,
            "net_recv_bytes": nr,
            "net_sent_bytes": nt,
        }

    async def _run(self) -> None:
        # Prime CPU snapshot so the first reported percent is meaningful.
        for ep in self._endpoints:
            await asyncio.to_thread(self._probe_one, ep)
        while not self._stopped.is_set():
            for ep in self._endpoints:
                sample = await asyncio.to_thread(self._probe_one, ep)
                if sample is not None:
                    self._samples.append(sample)
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self._stopped.wait(), timeout=self._interval_s)

    async def start(self) -> None:
        await asyncio.to_thread(self._open_connections)
        self._task = asyncio.create_task(self._run(), name="remote-resource-sampler")

    async def stop(self) -> None:
        self._stopped.set()
        if self._task is not None:
            await self._task
            self._task = None
        await asyncio.to_thread(self._close_connections)
        self._flush()

    def _flush(self) -> None:
        table = pa.Table.from_pylist(self._samples or [], schema=_SCHEMA)
        # Append-friendly: if a local sampler already wrote system_metrics.parquet
        # at this path, merge by reading + concat'ing.
        if self._path.exists():
            existing = pq.read_table(self._path)
            table = pa.concat_tables([existing, table], promote_options="default")
        pq.write_table(table, self._path, compression="zstd")
