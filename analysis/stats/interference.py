# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""Mixed-workload interference score per the Progress Report definition.

    I = 1 - T_mixed / sum_c (w_c * T_iso_c)

A positive `I` means the mixed workload under-performs the weighted sum of
isolated baselines (interference present); `I = 0` means the workload classes
do not interfere; a small negative `I` is observable when the mix benefits
from buffer-cache or scheduling effects, and is reported as-is.
"""

from __future__ import annotations

from pathlib import Path

from analysis.stats.throughput import overall_throughput, per_class_mean


def interference(
    mixed_run: Path,
    isolated_runs: dict[str, Path],
    mix_weights: dict[str, float],
) -> float:
    """Compute the interference score from one mixed run and matching isolated runs.

    `mix_weights` maps query-class identifiers (e.g., `"snb_iv2:IS1"`) to
    their relative weight in the mix; weights need not sum to 1 (they are
    normalised here). `isolated_runs` maps the same identifiers to a result
    directory whose `throughput.parquet` contains the isolated baseline.
    """
    if not mix_weights:
        raise ValueError("mix_weights must not be empty")
    total_w = sum(mix_weights.values())
    if total_w <= 0:
        raise ValueError("mix_weights must sum to a positive value")

    weighted_isolated = 0.0
    for qclass, weight in mix_weights.items():
        run = isolated_runs.get(qclass)
        if run is None:
            raise KeyError(f"missing isolated run for class {qclass!r}")
        df = per_class_mean(run)
        rows = df[df["query_class"] == qclass]
        if rows.empty:
            raise ValueError(f"isolated run {run} has no data for {qclass!r}")
        iso_throughput = float(rows.iloc[0]["mean_ops_per_s"])
        weighted_isolated += (weight / total_w) * iso_throughput

    if weighted_isolated <= 0:
        raise ValueError("weighted isolated throughput is zero; cannot compute interference")

    mixed_throughput = overall_throughput(mixed_run)
    return 1.0 - (mixed_throughput / weighted_isolated)
