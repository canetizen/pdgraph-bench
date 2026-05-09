#!/usr/bin/env bash
# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

# End-to-end benchmark campaign — one command, all SUTs, all scenarios.
#
# Usage:
#   bash scripts/run-full-campaign.sh [<sf>] [<results-root>]
#       sf            scale factor for the LDBC datagens (default 1)
#       results-root  parent directory for per-run results (default ./results)
#
# Pre-conditions on the orchestrator host:
#   - configs/inventory/cluster.yaml is populated (production cluster) or
#     copied from configs/inventory/cluster.yaml.playground (single-host).
#   - Passwordless SSH from the orchestrator into every node listed in the
#     inventory (the playground self-loop included).
#   - Docker engine running on every sut/reserve node.
#   - `make dev-install` has been run; `uv sync --extra deploy --extra dev
#     --extra drivers` resolved.
#
# Layout:
#   Tier-1 SUTs (nebulagraph, arangodb, dgraph) run S1..S5 with three
#   repetitions each (15 runs per SUT × 3 SUTs = 45).
#   Tier-2 SUTs (janusgraph, hugegraph) run S1 only (3 runs each = 6).
#   Total: 51 runs.
#
# Idempotency:
#   - Per-SUT bring-up is preceded by an unconditional tear-down so a partial
#     previous attempt cannot poison state.
#   - Datasets are generated once per (sf, workload); subsequent SUTs in the
#     same campaign reuse them.
#   - On any single-run failure the campaign records the failure to
#     `<results-root>/_failures.log` and continues with the next run.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

SF="${1:-1}"
RESULTS_ROOT="${2:-results}"

# ── tunables (override via env) ──────────────────────────────────────────────
# Default to the scenario YAML's own values (handed by leaving these empty).
# For a quick pipeline dry-run set:
#   WARMUP=5 MEASUREMENT=15 REPETITIONS=1 WORKERS=4 \
#       bash scripts/run-full-campaign.sh 0.003 results-dryrun
WARMUP="${WARMUP:-}"            # seconds, blank = use scenario YAML
MEASUREMENT="${MEASUREMENT:-}"  # seconds, blank = use scenario YAML
WORKERS="${WORKERS:-}"          # int, blank = use scenario YAML
REPETITIONS="${REPETITIONS:-3}" # int (always set; default 3 reps per scenario)

# Workload-specific SF override: the upstream LDBC datagens publish different
# scale-factor sets, so a single global `--sf` would fail for one of them.
# `WORKLOAD_SF[name]` overrides `SF` for that workload only.
declare -A WORKLOAD_SF
case "$SF" in
    0.001|0.003|0.01)
        # Smallest practical sizes: SNB family runs at 0.001 / 0.003,
        # FinBench's datagen refuses anything below 0.01 so it is pinned.
        WORKLOAD_SF[snb_iv2]="$SF"
        WORKLOAD_SF[snb_bi]="$SF"
        WORKLOAD_SF[finbench]=0.01
        ;;
    *)
        WORKLOAD_SF[snb_iv2]="$SF"
        WORKLOAD_SF[snb_bi]="$SF"
        WORKLOAD_SF[finbench]="$SF"
        ;;
esac

INVENTORY="configs/inventory/cluster.yaml"
if [ ! -f "$INVENTORY" ]; then
    echo "ERROR: $INVENTORY missing. Copy cluster.yaml.playground or cluster.yaml.example." >&2
    exit 2
fi

mkdir -p "$RESULTS_ROOT"
FAILURE_LOG="$RESULTS_ROOT/_failures.log"
: > "$FAILURE_LOG"
CAMPAIGN_LOG="$RESULTS_ROOT/_campaign.log"
: > "$CAMPAIGN_LOG"

note()  { printf '\n\033[1;34m▸ %s\033[0m\n' "$*" | tee -a "$CAMPAIGN_LOG"; }
ok()    { printf '\033[1;32m✓\033[0m %s\n' "$*" | tee -a "$CAMPAIGN_LOG"; }
warn()  { printf '\033[1;33m⚠\033[0m %s\n' "$*" | tee -a "$CAMPAIGN_LOG"; }
fail()  { printf '\033[1;31m✗\033[0m %s\n' "$*" | tee -a "$CAMPAIGN_LOG" "$FAILURE_LOG"; }

TIER1_SUTS=(nebulagraph arangodb dgraph)
TIER2_SUTS=(memgraph orientdb)

# Optional SUT filter — `SUTS=janusgraph,hugegraph bash run-full-campaign.sh ...`
# trims the campaign down to those SUTs only. Empty = run everything.
if [[ -n "${SUTS:-}" ]]; then
    IFS=',' read -ra _filter <<< "$SUTS"
    _filtered_t1=(); _filtered_t2=()
    for f in "${_filter[@]}"; do
        for s in "${TIER1_SUTS[@]}"; do [[ "$s" == "$f" ]] && _filtered_t1+=("$s"); done
        for s in "${TIER2_SUTS[@]}"; do [[ "$s" == "$f" ]] && _filtered_t2+=("$s"); done
    done
    TIER1_SUTS=("${_filtered_t1[@]}")
    TIER2_SUTS=("${_filtered_t2[@]}")
fi

# Scenarios per tier. Tier-2 only S1 because of backend complexity.
TIER1_SCENARIOS=(s1 s2 s3 s4 s5)
TIER2_SCENARIOS=(s1)

# Optional scenario filter — `SCENARIOS=s1,s3,s5 bash run-full-campaign.sh ...`
# trims each tier's scenario list to the intersection. Empty = run all.
if [[ -n "${SCENARIOS:-}" ]]; then
    IFS=',' read -ra _scen_filter <<< "$SCENARIOS"
    _filtered_t1_sc=(); _filtered_t2_sc=()
    for f in "${_scen_filter[@]}"; do
        for s in "${TIER1_SCENARIOS[@]}"; do [[ "$s" == "$f" ]] && _filtered_t1_sc+=("$s"); done
        for s in "${TIER2_SCENARIOS[@]}"; do [[ "$s" == "$f" ]] && _filtered_t2_sc+=("$s"); done
    done
    TIER1_SCENARIOS=("${_filtered_t1_sc[@]}")
    TIER2_SCENARIOS=("${_filtered_t2_sc[@]}")
fi

# Workloads needed across the campaign. Generated once per (sf, workload),
# reused across SUTs to avoid recomputing the LDBC datagen output.
WORKLOADS=(snb_iv2 snb_bi finbench)
declare -A DATASET_PATH
DATASET_PATH[snb_iv2]="data/generated/snb_iv2_sf${WORKLOAD_SF[snb_iv2]}"
DATASET_PATH[snb_bi]="data/generated/snb_bi_sf${WORKLOAD_SF[snb_bi]}"
DATASET_PATH[finbench]="data/generated/finbench_sf${WORKLOAD_SF[finbench]}"

scenario_workload() {
    case "$1" in
        s1|s3|s5) echo snb_iv2 ;;
        s2)       echo snb_bi ;;
        s4)       echo finbench ;;
        *)        echo "" ;;
    esac
}

scenario_workload_yaml() {
    case "$1" in
        s1|s3|s5) echo configs/workloads/snb_iv2.yaml ;;
        s2)       echo configs/workloads/snb_analytical.yaml ;;
        s4)       echo configs/workloads/finbench.yaml ;;
    esac
}

run_one() {
    local sut="$1" scenario="$2" rep="$3"
    local sys_yaml="configs/systems/${sut}.yaml"
    local scen_yaml="configs/scenarios/${scenario}.yaml"
    local wl_yaml; wl_yaml="$(scenario_workload_yaml "$scenario")"
    local workload; workload="$(scenario_workload "$scenario")"
    local dataset_dir="${DATASET_PATH[$workload]}"
    local rid="${sut}-${scenario}-rep${rep}"
    local extra=()
    [ -n "$WARMUP" ]      && extra+=(--warmup-seconds "$WARMUP")
    [ -n "$MEASUREMENT" ] && extra+=(--measurement-seconds "$MEASUREMENT")
    [ -n "$WORKERS" ]     && extra+=(--worker-count "$WORKERS")
    [ -n "$dataset_dir" ] && extra+=(--dataset-dir "$dataset_dir")
    # snb_analytical (S2) is a composite workload over IV2 + BI: pass both
    # dataset roots so the workload can synthesise IC* + Q* parameters.
    if [ "$scenario" = "s2" ]; then
        extra+=(--iv2-dataset-dir "${DATASET_PATH[snb_iv2]}")
        extra+=(--bi-dataset-dir  "${DATASET_PATH[snb_bi]}")
    fi
    note "run ${rid}"
    if uv run pdgraph-bench run "$scen_yaml" \
        --system "$sys_yaml" \
        --workload "$wl_yaml" \
        --inventory "$INVENTORY" \
        --system-name "$sut" \
        --results "$RESULTS_ROOT" \
        --repetition "$rep" \
        --run-id "$rid" \
        "${extra[@]}" \
        2>&1 | tee -a "$CAMPAIGN_LOG"; then
        ok "$rid"
        return 0
    else
        fail "$rid"
        return 1
    fi
}

ensure_dataset() {
    local workload="$1"
    local target="${DATASET_PATH[$workload]}"
    if [ -d "$target" ] && [ "$(find "$target" -maxdepth 2 -type f | head -1)" ]; then
        ok "dataset present: $target"
        return 0
    fi
    local sf="${WORKLOAD_SF[$workload]:-$SF}"
    note "generating dataset: workload=$workload sf=$sf -> $target"
    if uv run fab --search-root deploy/fabric generate-data \
        --workload="$workload" --sf="$sf" --out="$target" \
        2>&1 | tee -a "$CAMPAIGN_LOG"; then
        ok "dataset generated: $target"
    else
        fail "dataset gen failed: $workload sf=$sf"
        return 1
    fi
}

per_sut() {
    local sut="$1"; shift
    local scenarios=("$@")

    note "===== SUT: $sut ====="

    note "tear-down any prior $sut deployment"
    uv run fab --search-root deploy/fabric tear-down --system="$sut" --inventory="$INVENTORY" 2>&1 | tail -3 || true

    note "bring-up $sut"
    if ! uv run fab --search-root deploy/fabric bring-up --system="$sut" --inventory="$INVENTORY" 2>&1 | tee -a "$CAMPAIGN_LOG"; then
        fail "$sut bring-up failed; skipping"
        return 1
    fi
    ok "$sut bring-up requested"

    # Tier-2 SUTs (JanusGraph/HugeGraph) sit on Cassandra which needs longer
    # to come up + run its schema migration before the application daemon
    # can talk to it. Tier-1 SUTs are ready in ~60s.
    case "$sut" in
        # OrientDB Hazelcast cluster takes ~60-90s to gossip + elect a leader.
        orientdb) wait_s=120 ;;
        memgraph) wait_s=30  ;;
        *)        wait_s=60  ;;
    esac
    note "wait ${wait_s} s for $sut cluster initialisation"
    sleep "$wait_s"

    # Group scenarios by workload so each workload's scenarios run while
    # *its* dataset is loaded. All SUTs use a single space that gets wiped
    # by each new load, so loading every workload up-front would leave only
    # the last-loaded one queryable. Instead: load workload → run all of
    # its scenarios → load next workload → run those, etc.
    local workload_order=()
    declare -A workload_scenarios=()
    for scen in "${scenarios[@]}"; do
        local wl; wl="$(scenario_workload "$scen")"
        if [[ ! " ${workload_order[*]} " =~ " $wl " ]]; then
            workload_order+=("$wl")
            workload_scenarios[$wl]=""
        fi
        workload_scenarios[$wl]+="$scen "
    done

    for wl in "${workload_order[@]}"; do
        ensure_dataset "$wl" || continue

        note "load $wl into $sut"
        if ! uv run fab --search-root deploy/fabric load-data \
            --system="$sut" --workload="$wl" \
            --dataset-path="${DATASET_PATH[$wl]}" \
            --inventory="$INVENTORY" 2>&1 | tee -a "$CAMPAIGN_LOG"; then
            fail "$sut/$wl load failed; skipping scenarios using this workload"
            continue
        fi
        ok "$sut/$wl loaded"

        note "validate $sut/$wl"
        uv run fab --search-root deploy/fabric validate \
            --system="$sut" --workload="$wl" \
            --dataset-path="${DATASET_PATH[$wl]}" \
            --inventory="$INVENTORY" 2>&1 | tee -a "$CAMPAIGN_LOG" || warn "$sut/$wl validator failed (non-fatal, scenarios still attempted)"

        # Run scenarios bound to this workload while its data is live.
        for scen in ${workload_scenarios[$wl]}; do
            for ((rep=0; rep<REPETITIONS; rep++)); do
                run_one "$sut" "$scen" "$rep" || true
            done
        done
    done

    note "tear-down $sut"
    uv run fab --search-root deploy/fabric tear-down --system="$sut" --inventory="$INVENTORY" 2>&1 | tail -3 || true
    ok "$sut done"
}

# ============================ MAIN ===========================================

note "campaign starting: sf=$SF results=$RESULTS_ROOT"
note "inventory:"
uv run python - <<'PY' | tee -a "$CAMPAIGN_LOG"
from pathlib import Path
from graph_bench.orchestration import load_inventory
inv = load_inventory(Path("configs/inventory/cluster.yaml"))
print(f"  cluster: {inv.cluster_name}  ({len(inv.nodes)} nodes)")
for n in inv.nodes:
    print(f"  - {n.name:<8s} {n.hostname:<24s} {','.join(n.roles)}")
PY

for sut in "${TIER1_SUTS[@]}"; do
    per_sut "$sut" "${TIER1_SCENARIOS[@]}" || true
done

for sut in "${TIER2_SUTS[@]}"; do
    per_sut "$sut" "${TIER2_SCENARIOS[@]}" || true
done

note "campaign summary"
TOTAL_RUNS=$(find "$RESULTS_ROOT" -mindepth 1 -maxdepth 1 -type d -not -name "_*" | wc -l)
FAILED_RUNS=$(wc -l < "$FAILURE_LOG" || echo 0)
ok "completed runs: $TOTAL_RUNS"
if [ "$FAILED_RUNS" -gt 0 ]; then
    warn "failed runs: $FAILED_RUNS (see $FAILURE_LOG)"
else
    ok "no failures"
fi

cat <<EOF | tee -a "$CAMPAIGN_LOG"

╭────────────────────────────────────────────────────────────────╮
│  pdgraph-bench: campaign complete
│  results: $RESULTS_ROOT/
│  log:     $CAMPAIGN_LOG
╰────────────────────────────────────────────────────────────────╯

Generate the report:
    uv run python -m analysis.reports.generate_report --runs '$RESULTS_ROOT/*'
EOF
