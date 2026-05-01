# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""Throughput statistics from `throughput.parquet`."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq


def _load(run_dir: Path) -> pd.DataFrame:
    """Load the throughput Parquet for one run as a DataFrame.

    Schema: bucket_ns (int64), query_class (str), ok_count, error_count,
    timeout_count.
    """
    path = run_dir / "throughput.parquet"
    if not path.exists():
        raise FileNotFoundError(path)
    return pq.read_table(path).to_pandas()


def per_second_series(run_dir: Path) -> pd.DataFrame:
    """Return a DataFrame indexed by 1 s buckets with total ops/s.

    Output columns: ts (UTC datetime), ok, error, timeout, total.
    """
    df = _load(run_dir)
    grouped = df.groupby("bucket_ns", as_index=False).agg(
        ok=("ok_count", "sum"),
        error=("error_count", "sum"),
        timeout=("timeout_count", "sum"),
    )
    grouped["total"] = grouped["ok"] + grouped["error"] + grouped["timeout"]
    grouped["ts"] = pd.to_datetime(grouped["bucket_ns"], unit="ns", utc=True)
    return grouped[["ts", "ok", "error", "timeout", "total"]].sort_values("ts").reset_index(drop=True)


def per_class_mean(run_dir: Path) -> pd.DataFrame:
    """Mean throughput (ops/s) per query class, averaged across measurement window."""
    df = _load(run_dir)
    n_buckets = df["bucket_ns"].nunique()
    if n_buckets == 0:
        return pd.DataFrame(columns=["query_class", "mean_ops_per_s", "ok", "error", "timeout"])
    grouped = df.groupby("query_class", as_index=False).agg(
        ok=("ok_count", "sum"),
        error=("error_count", "sum"),
        timeout=("timeout_count", "sum"),
    )
    grouped["total"] = grouped["ok"] + grouped["error"] + grouped["timeout"]
    grouped["mean_ops_per_s"] = grouped["total"] / n_buckets
    return grouped[["query_class", "mean_ops_per_s", "ok", "error", "timeout"]].sort_values("query_class").reset_index(drop=True)


def overall_throughput(run_dir: Path) -> float:
    """Total ok ops/s across the whole window, all query classes combined."""
    df = _load(run_dir)
    if df.empty:
        return 0.0
    n_buckets = df["bucket_ns"].nunique()
    return float(df["ok_count"].sum()) / max(n_buckets, 1)
