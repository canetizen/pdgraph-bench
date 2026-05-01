# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""End-to-end report generator.

CLI entry point: `python -m analysis.reports.generate_report --runs 'results/*'`

The generator:
1. Scans the matched run directories.
2. Groups runs by (system, scenario) using the `config.yaml` produced by
   the harness; falls back to parsing the directory name when missing.
3. Computes the statistics defined in `analysis.stats.*`.
4. Emits the plot set defined in `analysis.plots.*` next to the report.
5. Renders the summary as a Markdown report (with embedded image references)
   and an HTML report.
"""

from __future__ import annotations

import argparse
import glob
import json
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from analysis.plots import latency_cdf, scaleout_timeline, throughput_bar
from analysis.stats.latency import aggregate_percentiles, histogram_for_class
from analysis.stats.scaleout import stabilization_time, throughput_dip
from analysis.stats.throughput import overall_throughput, per_class_mean


@dataclass
class RunMeta:
    run_id: str
    system: str
    scenario: str
    repetition: int
    path: Path


def _parse_run_meta(run_dir: Path) -> RunMeta | None:
    """Extract (system, scenario, repetition) from one run directory.

    Reads the `events.jsonl`'s `run_start` event when present; falls back to
    parsing the directory name (the harness creates names like
    `<system>-<scenario>-<rep>-<hash>`).
    """
    events_path = run_dir / "events.jsonl"
    if events_path.exists():
        for line in events_path.read_text().splitlines():
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            if event.get("type") == "run_start":
                return RunMeta(
                    run_id=run_dir.name,
                    system=str(event.get("system", "?")),
                    scenario=str(event.get("scenario", "?")),
                    repetition=int(event.get("repetition", 0)),
                    path=run_dir,
                )

    parts = run_dir.name.split("-")
    if len(parts) >= 3:
        try:
            return RunMeta(
                run_id=run_dir.name,
                system=parts[0],
                scenario=parts[1],
                repetition=int(parts[2]),
                path=run_dir,
            )
        except ValueError:
            return None
    return None


def _summarise(metas: list[RunMeta]) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, RunMeta]]:
    """Compute overall + per-class summaries across all runs."""
    overall_rows: list[dict] = []
    perclass_rows: list[dict] = []
    s5_runs: dict[str, RunMeta] = {}
    for m in metas:
        try:
            tput = overall_throughput(m.path)
        except FileNotFoundError:
            continue
        overall_rows.append(
            {"system": m.system, "scenario": m.scenario, "repetition": m.repetition, "ops_per_s": tput}
        )
        try:
            pc = per_class_mean(m.path)
        except FileNotFoundError:
            continue
        for _, row in pc.iterrows():
            perclass_rows.append(
                {
                    "system": m.system,
                    "scenario": m.scenario,
                    "repetition": m.repetition,
                    "query_class": row["query_class"],
                    "mean_ops_per_s": row["mean_ops_per_s"],
                }
            )
        if m.scenario.lower() in {"s5", "demo"}:
            s5_runs[f"{m.system}/{m.scenario}/{m.repetition}"] = m
    overall_df = pd.DataFrame(overall_rows)
    perclass_df = pd.DataFrame(perclass_rows)
    return overall_df, perclass_df, s5_runs


def _aggregate_overall(overall: pd.DataFrame) -> pd.DataFrame:
    """Mean + std of overall ops/s across repetitions."""
    if overall.empty:
        return overall
    return (
        overall.groupby(["system", "scenario"], as_index=False)
        .agg(mean_ops_per_s=("ops_per_s", "mean"), std_ops_per_s=("ops_per_s", "std"), reps=("ops_per_s", "size"))
        .fillna({"std_ops_per_s": 0.0})
    )


def _latency_table(metas: list[RunMeta]) -> pd.DataFrame:
    """Aggregate HDR histograms across repetitions; report p50/p95/p99/p999."""
    by_group: dict[tuple[str, str], list[Path]] = defaultdict(list)
    for m in metas:
        all_hgrm = m.path / "latency_all.hgrm"
        if all_hgrm.exists():
            by_group[(m.system, m.scenario)].append(all_hgrm)
    rows: list[dict] = []
    for (system, scenario), files in by_group.items():
        ps = aggregate_percentiles(files)
        rows.append(
            {
                "system": system,
                "scenario": scenario,
                "p50_us": ps[50.0],
                "p95_us": ps[95.0],
                "p99_us": ps[99.0],
                "p99_9_us": ps[99.9],
                "files": len(files),
            }
        )
    return pd.DataFrame(rows).sort_values(["scenario", "system"]).reset_index(drop=True)


def generate(runs_glob: str, output: Path) -> int:
    paths = [Path(p) for p in sorted(glob.glob(runs_glob)) if Path(p).is_dir()]
    metas = [m for m in (_parse_run_meta(p) for p in paths) if m is not None]
    if not metas:
        print(f"no parseable runs matched {runs_glob!r}", file=sys.stderr)
        return 1

    out_dir = output.parent if output.suffix else output
    out_dir.mkdir(parents=True, exist_ok=True)

    overall, perclass, s5_runs = _summarise(metas)
    overall_agg = _aggregate_overall(overall)
    latency = _latency_table(metas)

    plots_dir = out_dir / "figures"
    plots_dir.mkdir(parents=True, exist_ok=True)

    # Throughput bar chart.
    if not overall_agg.empty:
        throughput_bar.render(
            overall_agg.rename(columns={"mean_ops_per_s": "mean_ops_per_s"}),
            plots_dir / "throughput.pdf",
        )

    # Latency CDF — one curve per (system, scenario) at the all-class level.
    cdf_files: dict[str, Path] = {}
    for m in metas:
        path = m.path / "latency_all.hgrm"
        if path.exists():
            label = f"{m.system}/{m.scenario}"
            if label not in cdf_files:  # use first rep's histogram for the figure
                cdf_files[label] = path
    if cdf_files:
        latency_cdf.render(cdf_files, plots_dir / "latency_cdf.pdf")

    # Scale-out timelines (one figure per S5 run).
    scaleout_rows: list[dict] = []
    for label, m in s5_runs.items():
        timeline_path = plots_dir / f"scaleout_{m.system}_{m.scenario}_{m.repetition}.pdf"
        try:
            scaleout_timeline.render(m.path, timeline_path)
            t = stabilization_time(m.path)
            d = throughput_dip(m.path)
            scaleout_rows.append(
                {
                    "system": m.system,
                    "scenario": m.scenario,
                    "repetition": m.repetition,
                    "stabilization_s": t,
                    "throughput_dip": d,
                    "figure": timeline_path.name,
                }
            )
        except (FileNotFoundError, ValueError) as exc:
            print(f"[report] scale-out summary skipped for {label}: {exc}", file=sys.stderr)

    scaleout_df = pd.DataFrame(scaleout_rows)

    # Markdown report.
    md_path = out_dir / "report.md"
    md_lines: list[str] = []
    md_lines.append("# pdgraph-bench report")
    md_lines.append("")
    md_lines.append(f"runs: {len(metas)} runs across "
                    f"{overall_agg['system'].nunique() if not overall_agg.empty else 0} systems")
    md_lines.append("")
    if not overall_agg.empty:
        md_lines.append("## Overall throughput")
        md_lines.append("")
        md_lines.append(overall_agg.to_markdown(index=False))
        md_lines.append("")
        md_lines.append("![throughput](figures/throughput.pdf)")
        md_lines.append("")
    if not latency.empty:
        md_lines.append("## Latency percentiles (μs)")
        md_lines.append("")
        md_lines.append(latency.to_markdown(index=False))
        md_lines.append("")
        md_lines.append("![latency CDF](figures/latency_cdf.pdf)")
        md_lines.append("")
    if not scaleout_df.empty:
        md_lines.append("## Scale-out behavior (S5)")
        md_lines.append("")
        md_lines.append(scaleout_df.to_markdown(index=False))
        md_lines.append("")
    md_path.write_text("\n".join(md_lines), encoding="utf-8")

    # CSV side-cars for downstream paper generation.
    overall_agg.to_csv(out_dir / "overall_throughput.csv", index=False)
    latency.to_csv(out_dir / "latency_percentiles.csv", index=False)
    if not scaleout_df.empty:
        scaleout_df.to_csv(out_dir / "scaleout.csv", index=False)
    if not perclass.empty:
        perclass.to_csv(out_dir / "throughput_per_class.csv", index=False)

    print(f"[report] wrote {md_path}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate the benchmark report.")
    parser.add_argument("--runs", default="results/*", help="Glob matching run directories.")
    parser.add_argument("--output", type=Path, default=Path("results/_report"))
    args = parser.parse_args(argv)
    return generate(args.runs, args.output)


if __name__ == "__main__":
    sys.exit(main())
