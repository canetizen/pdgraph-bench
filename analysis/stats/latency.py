# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""Latency percentiles from serialized HDR histograms.

Histograms are aggregated across repetitions by summing counts (NEVER by
averaging percentiles); per-class percentiles are then reported from the
aggregated histogram.
"""

from __future__ import annotations

from pathlib import Path

from hdrh.histogram import HdrHistogram


_LOWEST_US = 1
_HIGHEST_US = 60 * 1_000_000
_SIGFIG = 3


def _new_histogram() -> HdrHistogram:
    return HdrHistogram(_LOWEST_US, _HIGHEST_US, _SIGFIG)


def _decode(path: Path) -> HdrHistogram:
    return HdrHistogram.decode(path.read_bytes())


def percentiles(
    hgrm_path: Path, ps: tuple[float, ...] = (50.0, 95.0, 99.0, 99.9)
) -> dict[float, float]:
    """Return requested percentiles (in microseconds) from one HDR histogram file."""
    hist = _decode(hgrm_path)
    return {p: float(hist.get_value_at_percentile(p)) for p in ps}


def aggregate(hgrm_paths: list[Path]) -> HdrHistogram:
    """Sum a list of HDR histogram files into one aggregated histogram.

    This is the correct way to combine percentiles across repetitions: aggregate
    raw counts, then read percentiles. Averaging per-repetition percentiles is
    a well-known statistical mistake.
    """
    agg = _new_histogram()
    for p in hgrm_paths:
        agg.add(_decode(p))
    return agg


def aggregate_percentiles(
    hgrm_paths: list[Path], ps: tuple[float, ...] = (50.0, 95.0, 99.0, 99.9)
) -> dict[float, float]:
    """Sum histograms across repetitions, then report percentiles."""
    agg = aggregate(hgrm_paths)
    return {p: float(agg.get_value_at_percentile(p)) for p in ps}


def histogram_for_class(run_dir: Path, query_class: str) -> Path | None:
    """Locate the per-class HDR file inside one run directory.

    Filenames are written by `LatencySink.export` with `:` replaced by `__`.
    """
    safe = query_class.replace(":", "__").replace("/", "_")
    candidate = run_dir / f"latency_{safe}.hgrm"
    return candidate if candidate.exists() else None
