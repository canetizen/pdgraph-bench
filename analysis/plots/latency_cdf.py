# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""Latency CDF plot from one or more HDR histogram files."""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt

from analysis.plots.style import apply_paper_style
from analysis.stats.latency import _decode  # noqa: PLC2701 — internal helper reuse


def render(hgrm_files: dict[str, Path], out_path: Path, max_us: int | None = None) -> None:
    """Render a latency CDF with one curve per `(label, hgrm path)` entry."""
    apply_paper_style()
    fig, ax = plt.subplots()
    for label, path in hgrm_files.items():
        hist = _decode(path)
        # Sample the CDF at log-spaced percentiles for a smooth curve.
        percentiles = [0.1, 1, 5, 10, 25, 50, 75, 90, 95, 99, 99.9, 99.99]
        xs = [hist.get_value_at_percentile(p) for p in percentiles]
        ys = [p / 100.0 for p in percentiles]
        ax.plot(xs, ys, label=label, marker="o")
    ax.set_xscale("log")
    ax.set_xlabel("latency ($\\mu$s)")
    ax.set_ylabel("CDF")
    if max_us is not None:
        ax.set_xlim(right=max_us)
    ax.set_ylim(0, 1.02)
    ax.legend(frameon=False)
    fig.savefig(out_path)
    plt.close(fig)
