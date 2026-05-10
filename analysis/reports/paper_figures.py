# Created by: Mustafa Can Caliskan
# Date: 2026-05-10

"""Generate paper-ready figures + LaTeX tables from a results directory.

Reads a glob of run directories produced by the harness (each containing
`requests.parquet`, `latency_*.hgrm`, `events.jsonl`) and writes:

- docs/term_paper/figures/*.pdf  — paper-ready figures (serif, fixed
  width ~8 cm column / ~17 cm spread, no chartjunk)
- docs/term_paper/tables/*.tex   — LaTeX `booktabs` tables (input via
  `\input{tables/...}`)
- docs/term_paper/results_summary.md — human-readable headline numbers

Usage:
    uv run python -m analysis.reports.paper_figures \\
        --runs 'results-smoke-rep3/*' \\
        --out  docs/term_paper
"""

from __future__ import annotations

import argparse
import glob
import json
from collections import defaultdict
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from analysis.plots.style import apply_paper_style as apply_style


COLUMN_WIDTH_IN = 3.4   # ~8.6 cm IEEE single column
PAGE_WIDTH_IN = 7.0     # ~17.8 cm IEEE two-column spread


def _scan(runs_glob: str) -> pd.DataFrame:
    """Walk run dirs, return one row per (system, scenario, repetition).

    Columns:
        system, scenario, repetition, run_id, n_requests, ok_rate,
        ops_per_s_total, ops_per_s_per_class (dict),
        p50_us, p95_us, p99_us
    """
    rows: list[dict] = []
    for path in sorted(glob.glob(runs_glob)):
        run_dir = Path(path)
        if not run_dir.is_dir() or run_dir.name.startswith("_"):
            continue
        rp = run_dir / "requests.parquet"
        if not rp.exists():
            continue
        # Parse meta from events.jsonl (run_start) or from the dir name.
        meta_system = meta_scenario = None
        rep = 0
        events_path = run_dir / "events.jsonl"
        if events_path.exists():
            for line in events_path.read_text().splitlines():
                try:
                    ev = json.loads(line)
                except Exception:
                    continue
                if ev.get("type") == "run_start":
                    meta_system = ev.get("system")
                    meta_scenario = ev.get("scenario")
                    rep = int(ev.get("repetition", 0))
                    break
        if meta_system is None:
            # Fall back to dir-name parsing: <system>-<scenario>-rep<N>
            parts = run_dir.name.split("-")
            if len(parts) >= 3:
                meta_system, meta_scenario = parts[0], parts[1]
                if parts[2].startswith("rep"):
                    rep = int(parts[2][3:])

        t = pq.read_table(rp).to_pandas()
        # Measurement window in seconds — derived from issued_ns extremes.
        if len(t) >= 2:
            window_s = (t["issued_ns"].max() - t["issued_ns"].min()) / 1e9
        else:
            window_s = 1.0
        ops_total = len(t) / max(window_s, 1e-3)
        per_class = {
            cls: len(sub) / max(window_s, 1e-3)
            for cls, sub in t.groupby("query_class")
        }
        ok = (t["status"] == "ok").mean() if len(t) else 0.0
        if (t["status"] == "ok").any():
            ok_lat = t.loc[t["status"] == "ok", "latency_us"]
            p50 = float(np.percentile(ok_lat, 50))
            p95 = float(np.percentile(ok_lat, 95))
            p99 = float(np.percentile(ok_lat, 99))
        else:
            p50 = p95 = p99 = float("nan")
        per_class_lat = {}
        for cls, sub in t.groupby("query_class"):
            ok_sub = sub.loc[sub["status"] == "ok", "latency_us"]
            if len(ok_sub):
                per_class_lat[cls] = {
                    "p50": float(np.percentile(ok_sub, 50)),
                    "p95": float(np.percentile(ok_sub, 95)),
                    "p99": float(np.percentile(ok_sub, 99)),
                }
        rows.append({
            "system": meta_system,
            "scenario": meta_scenario,
            "repetition": rep,
            "run_id": run_dir.name,
            "n_requests": len(t),
            "ok_rate": ok,
            "window_s": window_s,
            "ops_per_s_total": ops_total,
            "per_class_throughput": per_class,
            "per_class_latency": per_class_lat,
            "p50_us": p50,
            "p95_us": p95,
            "p99_us": p99,
        })
    return pd.DataFrame(rows)


# ------------------------------------------------------------ aggregations

def _agg_overall(df: pd.DataFrame) -> pd.DataFrame:
    """Per-(system, scenario) mean ± std across reps."""
    grp = df.groupby(["system", "scenario"]).agg(
        ops_mean=("ops_per_s_total", "mean"),
        ops_std=("ops_per_s_total", "std"),
        p50_mean=("p50_us", "mean"),
        p95_mean=("p95_us", "mean"),
        p99_mean=("p99_us", "mean"),
        ok_mean=("ok_rate", "mean"),
        reps=("ops_per_s_total", "count"),
    ).reset_index()
    return grp


def _agg_per_class(df: pd.DataFrame) -> pd.DataFrame:
    """Per-(system, scenario, class) mean throughput + latency p50/p95/p99."""
    rows: list[dict] = []
    for _, row in df.iterrows():
        for cls, ops in row["per_class_throughput"].items():
            lat = row["per_class_latency"].get(cls, {})
            rows.append({
                "system": row["system"],
                "scenario": row["scenario"],
                "repetition": row["repetition"],
                "query_class": cls,
                "ops_per_s": ops,
                "p50_us": lat.get("p50", float("nan")),
                "p95_us": lat.get("p95", float("nan")),
                "p99_us": lat.get("p99", float("nan")),
            })
    long = pd.DataFrame(rows)
    return long.groupby(["system", "scenario", "query_class"]).agg(
        ops_mean=("ops_per_s", "mean"),
        ops_std=("ops_per_s", "std"),
        p50_mean=("p50_us", "mean"),
        p95_mean=("p95_us", "mean"),
        p99_mean=("p99_us", "mean"),
    ).reset_index()


# ------------------------------------------------------------ figures

_SUT_ORDER = ["nebulagraph", "arangodb", "dgraph", "memgraph", "orientdb"]
_SUT_LABEL = {
    "nebulagraph": "NebulaGraph",
    "arangodb": "ArangoDB",
    "dgraph": "Dgraph",
    "memgraph": "Memgraph",
    "orientdb": "OrientDB",
}
_SCN_ORDER = ["s1", "s2", "s3", "s4", "s5"]
_SCN_LABEL = {
    "s1": "S1\nTx-only",
    "s2": "S2\nAnalytical",
    "s3": "S3\nMixed",
    "s4": "S4\nFinBench",
    "s5": "S5\nMixed*",  # asterisk = scale-out did not fire (30s window)
}


def _fig_s1_throughput(overall: pd.DataFrame, out: Path) -> None:
    """Cross-tier throughput on S1 — single column bar chart with std-dev."""
    s1 = overall[overall["scenario"] == "s1"].set_index("system").reindex(_SUT_ORDER)
    fig, ax = plt.subplots(figsize=(COLUMN_WIDTH_IN, 2.4))
    xs = np.arange(len(s1))
    ax.bar(xs, s1["ops_mean"], yerr=s1["ops_std"], capsize=3,
           color="#4477AA", edgecolor="black", linewidth=0.5)
    ax.set_xticks(xs)
    ax.set_xticklabels([_SUT_LABEL[s] for s in s1.index], rotation=20, ha="right")
    ax.set_ylabel("Throughput (ops/s)")
    ax.set_title("S1 transactional baseline")
    fig.tight_layout()
    fig.savefig(out / "fig_s1_throughput.pdf", bbox_inches="tight")
    plt.close(fig)


def _fig_s1_latency_percentiles(overall: pd.DataFrame, out: Path) -> None:
    """Cross-tier S1 latency (p50/p95/p99) — single column grouped bar."""
    s1 = overall[overall["scenario"] == "s1"].set_index("system").reindex(_SUT_ORDER)
    fig, ax = plt.subplots(figsize=(COLUMN_WIDTH_IN, 2.6))
    xs = np.arange(len(s1))
    width = 0.27
    for i, (col, label, color) in enumerate(
        [("p50_mean", "p50", "#88CCEE"),
         ("p95_mean", "p95", "#4477AA"),
         ("p99_mean", "p99", "#114477")]
    ):
        ax.bar(xs + (i - 1) * width, s1[col] / 1000.0, width=width,
               label=label, color=color, edgecolor="black", linewidth=0.4)
    ax.set_xticks(xs)
    ax.set_xticklabels([_SUT_LABEL[s] for s in s1.index], rotation=20, ha="right")
    ax.set_ylabel("Latency (ms)")
    ax.set_title("S1 latency percentiles")
    ax.set_yscale("log")
    ax.legend(loc="upper left", frameon=False, fontsize=7)
    fig.tight_layout()
    fig.savefig(out / "fig_s1_latency.pdf", bbox_inches="tight")
    plt.close(fig)


def _fig_tier1_scenario_throughput(overall: pd.DataFrame, out: Path) -> None:
    """Tier-1 cross-scenario throughput — wide grouped bar (3 SUT × 5 scn)."""
    tier1 = ["nebulagraph", "arangodb", "dgraph"]
    sub = overall[overall["system"].isin(tier1)].copy()
    pivot_mean = sub.pivot(index="scenario", columns="system", values="ops_mean").reindex(_SCN_ORDER)
    pivot_std = sub.pivot(index="scenario", columns="system", values="ops_std").reindex(_SCN_ORDER)
    pivot_mean = pivot_mean[tier1]
    pivot_std = pivot_std[tier1]
    fig, ax = plt.subplots(figsize=(PAGE_WIDTH_IN, 2.6))
    xs = np.arange(len(pivot_mean))
    width = 0.26
    colors = ["#114477", "#4477AA", "#88CCEE"]
    for i, sut in enumerate(tier1):
        ax.bar(xs + (i - 1) * width, pivot_mean[sut],
               yerr=pivot_std[sut], capsize=3, width=width,
               label=_SUT_LABEL[sut], color=colors[i],
               edgecolor="black", linewidth=0.4)
    ax.set_xticks(xs)
    ax.set_xticklabels([_SCN_LABEL[s] for s in pivot_mean.index])
    ax.set_ylabel("Throughput (ops/s)")
    ax.set_title("Tier-1 systems across all five scenarios")
    ax.legend(frameon=False, ncol=3, loc="upper center", fontsize=8)
    fig.tight_layout()
    fig.savefig(out / "fig_tier1_throughput.pdf", bbox_inches="tight")
    plt.close(fig)


def _fig_tier1_scenario_latency(overall: pd.DataFrame, out: Path) -> None:
    """Tier-1 cross-scenario p99 latency."""
    tier1 = ["nebulagraph", "arangodb", "dgraph"]
    sub = overall[overall["system"].isin(tier1)].copy()
    pivot = sub.pivot(index="scenario", columns="system", values="p99_mean").reindex(_SCN_ORDER)
    pivot = pivot[tier1]
    fig, ax = plt.subplots(figsize=(PAGE_WIDTH_IN, 2.4))
    xs = np.arange(len(pivot))
    width = 0.26
    colors = ["#114477", "#4477AA", "#88CCEE"]
    for i, sut in enumerate(tier1):
        ax.bar(xs + (i - 1) * width, pivot[sut] / 1000.0, width=width,
               label=_SUT_LABEL[sut], color=colors[i],
               edgecolor="black", linewidth=0.4)
    ax.set_xticks(xs)
    ax.set_xticklabels([_SCN_LABEL[s] for s in pivot.index])
    ax.set_ylabel("p99 latency (ms)")
    ax.set_title("Tier-1 p99 latency across scenarios")
    ax.legend(frameon=False, ncol=3, loc="upper center", fontsize=8)
    ax.set_yscale("log")
    fig.tight_layout()
    fig.savefig(out / "fig_tier1_p99.pdf", bbox_inches="tight")
    plt.close(fig)


def _fig_per_class_s1(per_class: pd.DataFrame, out: Path) -> None:
    """Per-class p95 latency on S1 — heatmap (5 SUTs × 14 classes)."""
    s1 = per_class[per_class["scenario"] == "s1"]
    pivot = s1.pivot(index="system", columns="query_class", values="p95_mean").reindex(_SUT_ORDER)
    # Drop columns that are all NaN.
    pivot = pivot.dropna(axis=1, how="all")
    # Convert to ms.
    data = pivot.values / 1000.0
    fig, ax = plt.subplots(figsize=(PAGE_WIDTH_IN, 2.2))
    im = ax.imshow(data, aspect="auto", cmap="viridis_r")
    ax.set_yticks(np.arange(len(pivot.index)))
    ax.set_yticklabels([_SUT_LABEL[s] for s in pivot.index])
    cls_labels = [c.replace("snb_iv2:", "") for c in pivot.columns]
    ax.set_xticks(np.arange(len(cls_labels)))
    ax.set_xticklabels(cls_labels, rotation=45, ha="right")
    ax.set_title("S1 per-class p95 latency (ms; lower is better)")
    cbar = fig.colorbar(im, ax=ax, fraction=0.025, pad=0.02)
    cbar.set_label("ms", rotation=0, labelpad=8)
    # Annotate cells with the numeric value.
    for i in range(data.shape[0]):
        for j in range(data.shape[1]):
            v = data[i, j]
            if not np.isnan(v):
                ax.text(j, i, f"{int(v)}", ha="center", va="center",
                        fontsize=6, color="white" if v > np.nanmean(data) else "black")
    fig.tight_layout()
    fig.savefig(out / "fig_s1_per_class.pdf", bbox_inches="tight")
    plt.close(fig)


# ------------------------------------------------------------ tables

def _emit_table(rows: list[list[str]], header: list[str], col_format: str,
                caption: str, label: str) -> str:
    """Tiny hand-rolled booktabs writer — sidesteps the jinja2 dep."""
    body = []
    body.append("\\begin{table}[t]")
    body.append("\\centering")
    body.append(f"\\caption{{{caption}}}")
    body.append(f"\\label{{{label}}}")
    body.append(f"\\begin{{tabular}}{{{col_format}}}")
    body.append("\\toprule")
    body.append(" & ".join(header) + " \\\\")
    body.append("\\midrule")
    for r in rows:
        body.append(" & ".join(r) + " \\\\")
    body.append("\\bottomrule")
    body.append("\\end{tabular}")
    body.append("\\end{table}")
    return "\n".join(body) + "\n"


def _table_overall(overall: pd.DataFrame, out: Path) -> None:
    """LaTeX `booktabs` table — per-(system, scenario) summary."""
    rows: list[list[str]] = []
    for _, r in overall.iterrows():
        rows.append([
            _SUT_LABEL[r["system"]],
            r["scenario"].upper(),
            f"{r['ops_mean']:.1f}",
            f"{r['ops_std']:.1f}",
            f"{r['p50_mean']/1000:.1f}",
            f"{r['p95_mean']/1000:.1f}",
            f"{r['p99_mean']/1000:.1f}",
            f"{r['ok_mean']*100:.1f}",
            f"{int(r['reps'])}",
        ])
    tex = _emit_table(
        rows,
        header=["System", "Scn.", "Thrpt", "$\\sigma$",
                "p50", "p95", "p99", "OK\\%", "n"],
        col_format="llrrrrrrr",
        caption=("Per-(system, scenario) summary at LDBC SF=0.003 "
                 "(SNB family) and SF=0.01 (FinBench). Throughput in "
                 "ops/s, latency percentiles in milliseconds. "
                 "Mean $\\pm$ std across $n$ repetitions."),
        label="tab:results",
    )
    (out / "table_overall.tex").write_text(tex)


def _table_per_class_s1(per_class: pd.DataFrame, out: Path) -> None:
    """Per-class p95 latency for S1 across 5 SUTs."""
    s1 = per_class[per_class["scenario"] == "s1"].copy()
    s1["query_class"] = s1["query_class"].str.replace("snb_iv2:", "")
    pivot = s1.pivot(index="query_class", columns="system", values="p95_mean")
    pivot = pivot.reindex(columns=_SUT_ORDER)
    pivot = pivot.dropna(how="all", axis=1)
    pivot = (pivot / 1000.0).round(1)
    rows: list[list[str]] = []
    for cls in pivot.index:
        row = [cls]
        for sut in pivot.columns:
            v = pivot.loc[cls, sut]
            row.append("--" if pd.isna(v) else f"{v:.1f}")
        rows.append(row)
    header = ["Class"] + [_SUT_LABEL[s] for s in pivot.columns]
    tex = _emit_table(
        rows, header=header,
        col_format="l" + "r" * len(pivot.columns),
        caption=("S1 per-class p95 latency in ms across the five SUTs "
                 "at SF=0.003. Lower is better."),
        label="tab:s1_per_class",
    )
    (out / "table_s1_per_class.tex").write_text(tex)


# ------------------------------------------------------------ summary

def _write_summary(overall: pd.DataFrame, df: pd.DataFrame, out: Path) -> None:
    lines: list[str] = ["# Results summary (paper-ready)\n"]
    lines.append(f"- **Total runs:** {len(df)} (5 SUTs × scenarios × 3 reps)")
    lines.append(f"- **Total recorded requests:** {int(df['n_requests'].sum())}")
    lines.append(f"- **Overall OK%:** {df['ok_rate'].mean()*100:.2f}")
    lines.append("")
    lines.append("## Headline numbers")
    lines.append("")
    s1 = overall[overall["scenario"] == "s1"].sort_values("ops_mean", ascending=False)
    lines.append("### S1 transactional baseline (all 5 SUTs)")
    lines.append("")
    lines.append("| System | Throughput (ops/s) | p50 (ms) | p99 (ms) |")
    lines.append("|---|---:|---:|---:|")
    for _, r in s1.iterrows():
        lines.append(f"| {_SUT_LABEL[r['system']]} | {r['ops_mean']:.0f} ± {r['ops_std']:.0f} | "
                     f"{r['p50_mean']/1000:.1f} | {r['p99_mean']/1000:.1f} |")
    lines.append("")
    lines.append("### Tier-1 cross-scenario throughput (mean ops/s)")
    lines.append("")
    tier1 = overall[overall["system"].isin(["nebulagraph", "arangodb", "dgraph"])]
    pivot = tier1.pivot(index="scenario", columns="system", values="ops_mean").reindex(_SCN_ORDER)
    lines.append("| Scenario | NebulaGraph | ArangoDB | Dgraph |")
    lines.append("|---|---:|---:|---:|")
    for scn, row in pivot.iterrows():
        lines.append(f"| {scn.upper()} | {row.get('nebulagraph', float('nan')):.0f} | "
                     f"{row.get('arangodb', float('nan')):.0f} | "
                     f"{row.get('dgraph', float('nan')):.0f} |")
    lines.append("")
    lines.append("## Notes / limitations")
    lines.append("")
    lines.append("- **S5 scale-out**: the main 30s-window campaign does "
                 "not fire the scale-out (trigger at t=10s now, but the "
                 "main campaign was launched before that fix); a separate "
                 "60s-window S5-only re-run captures the scale-out events "
                 "and is reported in `summary_s5_scaleout.csv` and "
                 "Section~III-E.")
    lines.append("- **Stabilization-time** is computed with a 5-second "
                 "hold window (vs. the Progress Report's 60 s) because "
                 "our minimum-viable measurement window is 60 s total. "
                 "Runs marked unstable did not return to a $\\pm$5% band "
                 "within the post-event remainder.")
    lines.append("- **OrientDB cluster** runs as standalone (Hazelcast "
                 "distributed mode triggered RID collisions on cluster "
                 "writes); Tier-2 only runs S1 so HA is out of scope.")
    lines.append("- All reported numbers are at LDBC SNB SF=0.003 (snb_iv2, "
                 "snb_bi) and FinBench SF=0.01 (datagen lower bound). "
                 "Throughput values are absolute and not normalized.")
    (out.parent / "results_summary.md").write_text("\n".join(lines))


# ------------------------------------------------------------ S5 scale-out

def _s5_scaleout_metrics(scaleout_glob: str) -> pd.DataFrame:
    """Return per-run scale-out metrics from a glob of S5 run dirs."""
    from analysis.stats.scaleout import stabilization_time, throughput_dip
    rows = []
    for path in sorted(glob.glob(scaleout_glob)):
        d = Path(path)
        if not d.is_dir() or d.name.startswith("_"):
            continue
        try:
            parsed = [json.loads(l) for l in (d / "events.jsonl").read_text().splitlines() if l]
        except Exception:
            continue
        trig = next((e for e in parsed if e.get("type") == "scale_out_trigger"), None)
        comp = next((e for e in parsed if e.get("type") == "scale_out_complete"), None)
        prov_s = (comp["ts_ns"] - trig["ts_ns"]) / 1e9 if trig and comp else float("nan")
        try:
            stab = stabilization_time(d, hold_s=5)
        except Exception:
            stab = float("nan")
        try:
            dip = throughput_dip(d)
        except Exception:
            dip = float("nan")
        # Parse system + repetition from dir name (e.g. nebulagraph-s5-rep0).
        parts = d.name.split("-")
        sys = parts[0]
        rep = int(parts[2][3:]) if len(parts) >= 3 and parts[2].startswith("rep") else 0
        rows.append({
            "system": sys, "repetition": rep, "run_id": d.name,
            "prov_s": prov_s, "stab_s": stab, "dip_pct": dip * 100.0 if not np.isnan(dip) else float("nan"),
        })
    return pd.DataFrame(rows)


def _fig_s5_timeline(scaleout_glob: str, out: Path) -> None:
    """Throughput vs time around the scale-out event for each Tier-1 SUT.

    Three panels (one per SUT), each showing the rep-0 throughput series
    with the scale-out trigger marked as a vertical dashed line.
    """
    from analysis.stats.throughput import per_second_series
    fig, axes = plt.subplots(1, 3, figsize=(PAGE_WIDTH_IN, 2.4), sharey=False)
    tier1 = ["nebulagraph", "arangodb", "dgraph"]
    for ax, sut in zip(axes, tier1):
        # Use rep0 as the representative trace.
        d = Path(scaleout_glob.replace("*", f"{sut}-s5-rep0"))
        if not d.exists():
            ax.set_title(f"{_SUT_LABEL[sut]} (no data)")
            continue
        parsed = [json.loads(l) for l in (d / "events.jsonl").read_text().splitlines() if l]
        meas_start = next((e for e in parsed if e.get("type") == "measurement_start"), None)
        trig = next((e for e in parsed if e.get("type") == "scale_out_trigger"), None)
        comp = next((e for e in parsed if e.get("type") == "scale_out_complete"), None)
        if meas_start is None:
            continue
        series = per_second_series(d)
        if series.empty:
            continue
        # Convert ts ns to seconds-since-measurement-start.
        t_offset = (series["ts"].astype("int64") - meas_start["ts_ns"]) / 1e9
        ok_per_s = series["ok"]
        ax.plot(t_offset, ok_per_s, color="#114477", linewidth=1.2)
        if trig:
            t_trig = (trig["ts_ns"] - meas_start["ts_ns"]) / 1e9
            ax.axvline(t_trig, color="#CC0000", linestyle="--", linewidth=1, label="trigger")
        if comp:
            t_comp = (comp["ts_ns"] - meas_start["ts_ns"]) / 1e9
            ax.axvline(t_comp, color="#FF8800", linestyle=":", linewidth=1, label="complete")
        ax.set_title(_SUT_LABEL[sut], fontsize=9)
        ax.set_xlabel("time since measurement start (s)", fontsize=8)
        ax.tick_params(labelsize=7)
        ax.set_ylim(bottom=0)
        ax.legend(loc="upper right", fontsize=7, frameon=False)
    axes[0].set_ylabel("throughput (ops/s)")
    fig.suptitle("S5 scale-out: throughput timeline (rep 0)", fontsize=10)
    fig.tight_layout()
    fig.savefig(out / "fig_s5_timeline.pdf", bbox_inches="tight")
    plt.close(fig)


def _table_s5_scaleout(metrics: pd.DataFrame, out: Path) -> None:
    """Per-system mean ± std of provisioning time, stabilization time, dip."""
    g = metrics.groupby("system").agg(
        prov_mean=("prov_s", "mean"),
        prov_std=("prov_s", "std"),
        stab_finite=("stab_s", lambda s: s[s != float("inf")].mean() if (s != float("inf")).any() else float("nan")),
        stab_inf=("stab_s", lambda s: int((s == float("inf")).sum())),
        dip_mean=("dip_pct", "mean"),
        dip_std=("dip_pct", "std"),
        n=("run_id", "count"),
    ).reset_index()
    rows: list[list[str]] = []
    for _, r in g.iterrows():
        stab_str = f"{r['stab_finite']:.1f}" if not pd.isna(r['stab_finite']) else "--"
        rows.append([
            _SUT_LABEL[r["system"]],
            f"{r['prov_mean']:.2f} $\\pm$ {r['prov_std']:.2f}",
            f"{stab_str} ({int(r['stab_inf'])}/{int(r['n'])} unstable)",
            f"{r['dip_mean']:.1f} $\\pm$ {r['dip_std']:.1f}",
            f"{int(r['n'])}",
        ])
    tex = _emit_table(
        rows,
        header=["System", "Provisioning (s)", "Stab. time (s)",
                "Max dip (\\%)", "n"],
        col_format="lrrrr",
        caption=("S5 scale-out behaviour (Tier-1 only). Provisioning is "
                 "the wall-clock interval between the harness's "
                 "\\texttt{scale\\_out\\_trigger} and "
                 "\\texttt{scale\\_out\\_complete} events. Stabilization "
                 "time is the seconds-since-trigger at which the "
                 "throughput rolling average re-enters $[0.95, 1.05]"
                 "\\cdot T_{\\mathrm{pre}}$ for at least 5 consecutive "
                 "seconds; runs that never re-enter the band within the "
                 "60\\,s measurement window are counted as ``unstable.'' "
                 "Maximum dip is "
                 "$1 - \\min(T_{\\mathrm{post}})/T_{\\mathrm{pre}}$ "
                 "expressed as a percentage."),
        label="tab:s5_scaleout",
    )
    (out / "table_s5_scaleout.tex").write_text(tex)


# ------------------------------------------------------------ main

def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--runs", required=True, help="Glob of run directories.")
    p.add_argument("--scaleout-runs", default=None,
                   help="Glob of S5 scale-out run directories (separate "
                        "campaign with longer measurement window).")
    p.add_argument("--out", required=True,
                   help="Paper directory; figures/ and tables/ subdirs created.")
    args = p.parse_args(argv)

    apply_style()
    out_root = Path(args.out)
    fig_dir = out_root / "figures"
    tbl_dir = out_root / "tables"
    fig_dir.mkdir(parents=True, exist_ok=True)
    tbl_dir.mkdir(parents=True, exist_ok=True)

    df = _scan(args.runs)
    if df.empty:
        print("[paper-figs] no runs found", flush=True)
        return 1

    overall = _agg_overall(df)
    per_class = _agg_per_class(df)

    overall.to_csv(out_root / "summary_overall.csv", index=False)
    per_class.to_csv(out_root / "summary_per_class.csv", index=False)

    _fig_s1_throughput(overall, fig_dir)
    _fig_s1_latency_percentiles(overall, fig_dir)
    _fig_tier1_scenario_throughput(overall, fig_dir)
    _fig_tier1_scenario_latency(overall, fig_dir)
    _fig_per_class_s1(per_class, fig_dir)

    _table_overall(overall, tbl_dir)
    _table_per_class_s1(per_class, tbl_dir)

    if args.scaleout_runs:
        s5_metrics = _s5_scaleout_metrics(args.scaleout_runs)
        if not s5_metrics.empty:
            s5_metrics.to_csv(out_root / "summary_s5_scaleout.csv", index=False)
            _fig_s5_timeline(args.scaleout_runs, fig_dir)
            _table_s5_scaleout(s5_metrics, tbl_dir)

    _write_summary(overall, df, fig_dir)

    print(f"[paper-figs] wrote {fig_dir} + {tbl_dir} + {out_root}/results_summary.md",
          flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
