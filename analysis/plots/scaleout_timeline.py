# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""Throughput-vs-time plot with a vertical marker at the scale-out trigger."""

from __future__ import annotations

import json
from pathlib import Path

import matplotlib.pyplot as plt

from analysis.plots.style import apply_paper_style
from analysis.stats.throughput import per_second_series


def render(run_dir: Path, out_path: Path) -> None:
    apply_paper_style()
    series = per_second_series(run_dir)
    if series.empty:
        return

    series = series.copy()
    series["t_s"] = (series["ts"].astype("int64") - series["ts"].astype("int64").iloc[0]) / 1e9

    fig, ax = plt.subplots()
    ax.plot(series["t_s"], series["ok"], label="ok")
    ax.plot(series["t_s"], series["error"] + series["timeout"], label="failed", linestyle="--")
    ax.set_xlabel("time (s)")
    ax.set_ylabel("throughput (ops/s)")

    # Mark scale-out events from the events log.
    events_path = run_dir / "events.jsonl"
    if events_path.exists():
        run_t0 = series["ts"].astype("int64").iloc[0]
        for line in events_path.read_text().splitlines():
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            if event.get("type") == "scale_out_trigger":
                offset_s = (int(event["ts_ns"]) - run_t0) / 1e9
                ax.axvline(offset_s, color="red", linestyle=":", linewidth=1)
                ax.text(
                    offset_s,
                    ax.get_ylim()[1] * 0.95,
                    "scale-out",
                    color="red",
                    fontsize=8,
                    rotation=90,
                    va="top",
                    ha="right",
                )
    ax.legend(frameon=False)
    fig.savefig(out_path)
    plt.close(fig)
