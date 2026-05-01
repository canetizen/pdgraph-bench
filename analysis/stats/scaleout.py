# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""Scale-out stabilization metrics."""

from __future__ import annotations

import json
from pathlib import Path

from analysis.stats.throughput import per_second_series


def _scale_out_event_ts_ns(run_dir: Path) -> int | None:
    """Return the wall-clock ns of the `scale_out_trigger` event, or None."""
    events_path = run_dir / "events.jsonl"
    if not events_path.exists():
        return None
    for line in events_path.read_text().splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if event.get("type") == "scale_out_trigger":
            return int(event["ts_ns"])
    return None


def stabilization_time(run_dir: Path, tol: float = 0.05, hold_s: int = 60) -> float:
    """Seconds from scale-out trigger until throughput stabilizes within `tol`.

    Stabilization is the first instant at which the rolling-average throughput
    re-enters `[1-tol, 1+tol] * T_pre-event` and remains within that band for
    at least `hold_s` consecutive seconds.

    Returns `float("inf")` if stabilization is not reached within the
    available throughput series; raises if no scale-out event is logged.
    """
    event_ns = _scale_out_event_ts_ns(run_dir)
    if event_ns is None:
        raise ValueError(f"no scale_out_trigger event in {run_dir}")

    series = per_second_series(run_dir)
    if series.empty:
        return float("inf")

    series["ts_ns"] = series["ts"].astype("int64")  # ns since epoch
    pre = series[series["ts_ns"] < event_ns]
    post = series[series["ts_ns"] >= event_ns]
    if pre.empty or post.empty:
        return float("inf")

    baseline = float(pre["ok"].mean())
    if baseline <= 0:
        return float("inf")
    lo, hi = baseline * (1 - tol), baseline * (1 + tol)

    consecutive = 0
    stable_start_ns: int | None = None
    for _, row in post.iterrows():
        ok = float(row["ok"])
        if lo <= ok <= hi:
            if stable_start_ns is None:
                stable_start_ns = int(row["ts_ns"])
            consecutive += 1
            if consecutive >= hold_s:
                return (stable_start_ns - event_ns) / 1e9
        else:
            consecutive = 0
            stable_start_ns = None
    return float("inf")


def throughput_dip(run_dir: Path) -> float:
    """Maximum relative throughput reduction during the rebalance window.

    Defined as `1 - min(post_event_throughput) / pre_event_baseline`.
    Returns `0.0` if no dip is observed; positive values indicate a dip.
    """
    event_ns = _scale_out_event_ts_ns(run_dir)
    if event_ns is None:
        raise ValueError(f"no scale_out_trigger event in {run_dir}")

    series = per_second_series(run_dir)
    if series.empty:
        return 0.0

    series["ts_ns"] = series["ts"].astype("int64")
    pre = series[series["ts_ns"] < event_ns]
    post = series[series["ts_ns"] >= event_ns]
    if pre.empty or post.empty:
        return 0.0

    baseline = float(pre["ok"].mean())
    if baseline <= 0:
        return 0.0
    min_post = float(post["ok"].min())
    return max(0.0, 1.0 - (min_post / baseline))
