# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""HDR histogram aggregation per query class.

Latency is sampled in microseconds with a maximum of 60 seconds (matching the
per-query timeout). The histogram range is `[1 us, 60 s]` with three significant
digits, which yields sub-millisecond precision across the useful range.

A separate histogram is maintained per logical query (e.g., IS1, IC3) so that
per-class percentiles can be reported in the analysis stage. An "all" aggregate
is maintained as well.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from hdrh.histogram import HdrHistogram

if TYPE_CHECKING:
    from graph_bench.domain import QueryRef


_LOWEST_US = 1
_HIGHEST_US = 60 * 1_000_000  # 60 s in microseconds
_SIGFIG = 3


class LatencySink:
    """Aggregate per-request latencies into HDR histograms keyed by query class."""

    def __init__(self) -> None:
        self._per_class: dict[str, HdrHistogram] = {}
        self._all = self._make_histogram()

    @staticmethod
    def _make_histogram() -> HdrHistogram:
        return HdrHistogram(_LOWEST_US, _HIGHEST_US, _SIGFIG)

    def record(self, ref: QueryRef, latency_us: int) -> None:
        """Record a single successful latency sample.

        Timeouts and errors must not be passed here; they are recorded separately
        via the failed-query rate in the request log.
        """
        key = str(ref)
        hist = self._per_class.get(key)
        if hist is None:
            hist = self._make_histogram()
            self._per_class[key] = hist
        # HdrHistogram clamps values outside its range; cap explicitly so the
        # clamping is visible rather than silent.
        v = max(_LOWEST_US, min(_HIGHEST_US, latency_us))
        hist.record_value(v)
        self._all.record_value(v)

    def export(self, directory: Path) -> None:
        """Write each histogram to an `.hgrm`-like binary file under `directory`."""
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "latency_all.hgrm").write_bytes(self._all.encode())
        for key, hist in self._per_class.items():
            safe = key.replace(":", "__").replace("/", "_")
            (directory / f"latency_{safe}.hgrm").write_bytes(hist.encode())
