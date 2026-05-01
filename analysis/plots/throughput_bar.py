# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""Bar chart of overall throughput per (system, scenario)."""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from analysis.plots.style import apply_paper_style


def render(summary: pd.DataFrame, out_path: Path) -> None:
    """Render `summary` (cols: system, scenario, mean_ops_per_s) into a bar chart.

    Bars are grouped by scenario, coloured by system. Saved as PDF (paper-ready).
    """
    apply_paper_style()
    fig, ax = plt.subplots()
    pivot = summary.pivot(index="scenario", columns="system", values="mean_ops_per_s").fillna(0)
    pivot.plot(kind="bar", ax=ax, edgecolor="black", linewidth=0.5)
    ax.set_xlabel("scenario")
    ax.set_ylabel("throughput (ops/s)")
    ax.tick_params(axis="x", rotation=0)
    ax.legend(title="system", frameon=False)
    fig.savefig(out_path)
    plt.close(fig)
