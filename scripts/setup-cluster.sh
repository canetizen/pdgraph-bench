#!/usr/bin/env bash
# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

# End-to-end cluster setup driven entirely by configs/inventory/cluster.yaml.
#
# Pre-conditions on every node listed in the inventory:
#   - Linux with passwordless sudo for the SSH user.
#   - Docker engine (>=24) running.
#   - Network: every node resolves every other node's hostname (the deployer
#     uses `network_mode: host`, so peer hostnames must be reachable on the
#     host's /etc/hosts or via DNS).
#   - The deploy root (default ~/.gb-deploy) is writable.
#
# Pre-conditions on the operator's machine (where you run this script):
#   - Python 3.11+, `uv` installed.
#   - SSH agent loaded with a key authorised on every cluster node.
#   - This repository checked out, `make dev-install` already run.
#
# Usage:
#   bash scripts/setup-cluster.sh <system> <workload> [<sf>]
#       system   = nebulagraph | arangodb | dgraph | janusgraph | hugegraph
#       workload = snb_iv2 | snb_bi | finbench | synthetic_snb
#       sf       = scale factor (default 1; ignored for synthetic_snb)
#
# Steps performed:
#   1. Load inventory; print every node so the operator can sanity-check.
#   2. Bring up the SUT cluster across all `sut`-role nodes via Fabric/SSH.
#   3. Generate the requested dataset on the client node.
#   4. Load the dataset into the SUT.
#   5. Run the post-load validator.
#   6. Print a benchmark-ready summary.
#
# Idempotent: re-running tears down any previously-deployed instance of the
# same SUT before bringing it up.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

SYSTEM="${1:?usage: setup-cluster.sh <system> <workload> [<sf>]}"
WORKLOAD="${2:?usage: setup-cluster.sh <system> <workload> [<sf>]}"
SF="${3:-1}"

INVENTORY="configs/inventory/cluster.yaml"
if [ ! -f "$INVENTORY" ]; then
    echo "ERROR: $INVENTORY does not exist." >&2
    echo "       Copy ${INVENTORY}.example and fill in your cluster's node info." >&2
    exit 2
fi

note() { printf '\n\033[1;34m▸ %s\033[0m\n' "$*"; }
ok()   { printf '\033[1;32m✓\033[0m %s\n' "$*"; }

note "loading inventory"
uv run python - <<'PY'
from pathlib import Path
from graph_bench.orchestration import load_inventory
inv = load_inventory(Path("configs/inventory/cluster.yaml"))
print(f"  cluster: {inv.cluster_name}  ({len(inv.nodes)} nodes)")
for n in inv.nodes:
    print(f"  - {n.name:<8s} hostname={n.hostname:<24s} roles={','.join(n.roles)}")
PY
ok "inventory parsed"

note "verifying node-pair connectivity (each node resolves and reaches every other)"
fail=0
nodes=$(uv run python -c "
from pathlib import Path
from graph_bench.orchestration import load_inventory
inv = load_inventory(Path('configs/inventory/cluster.yaml'))
for n in inv.nodes:
    print(n.name, n.hostname, n.ssh_user or '')
")
mapfile -t node_lines < <(printf '%s\n' "$nodes")

# Build the host-list table the deploy layer expects to be resolvable on every
# peer. Distribute it as /etc/gb-hosts on each node so admins can append it to
# /etc/hosts manually if DNS is not in place.
HOSTS_FILE=$(mktemp)
for line in "${node_lines[@]}"; do
    [ -z "$line" ] && continue
    name=$(echo "$line" | awk '{print $1}')
    hostname=$(echo "$line" | awk '{print $2}')
    # Resolve hostname → IP from the orchestrator's perspective. If DNS fails,
    # the host's /etc/hosts must already carry the mapping.
    ip=$(getent hosts "$hostname" | awk '{print $1}' | head -1)
    if [ -z "$ip" ]; then
        echo "  ✗ orchestrator cannot resolve $hostname (node $name)" >&2
        fail=1
    else
        printf '%s\t%s\n' "$ip" "$hostname" >> "$HOSTS_FILE"
        echo "  ✓ $name → $hostname → $ip"
    fi
done
if [ "$fail" -ne 0 ]; then
    echo "" >&2
    echo "FAIL: at least one cluster hostname does not resolve from this orchestrator." >&2
    echo "      Either point DNS at the cluster or append the following to /etc/hosts" >&2
    echo "      on every node (and on this orchestrator):" >&2
    cat "$HOSTS_FILE" >&2
    rm -f "$HOSTS_FILE"
    exit 3
fi

# Pair-wise reachability check: each non-local node must be able to ssh +
# resolve every other node's hostname. We piggyback on the existing ssh-agent.
for line in "${node_lines[@]}"; do
    [ -z "$line" ] && continue
    name=$(echo "$line" | awk '{print $1}')
    hostname=$(echo "$line" | awk '{print $2}')
    ssh_user=$(echo "$line" | awk '{print $3}')
    if [ "$hostname" = "localhost" ] || [ "$hostname" = "127.0.0.1" ]; then
        continue
    fi
    target="${ssh_user:+${ssh_user}@}${hostname}"
    for line2 in "${node_lines[@]}"; do
        [ -z "$line2" ] && continue
        peer=$(echo "$line2" | awk '{print $2}')
        [ "$peer" = "$hostname" ] && continue
        if ! ssh -o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=no \
             "$target" "getent hosts $peer >/dev/null" 2>/dev/null; then
            echo "  ✗ $name cannot resolve peer hostname $peer" >&2
            fail=1
        fi
    done
    [ "$fail" -eq 0 ] && echo "  ✓ $name reaches all peers"
done
if [ "$fail" -ne 0 ]; then
    echo "" >&2
    echo "FAIL: at least one node cannot resolve a peer hostname over SSH." >&2
    echo "      Distribute the /etc/hosts entries above to every node and retry." >&2
    rm -f "$HOSTS_FILE"
    exit 3
fi
rm -f "$HOSTS_FILE"
ok "all nodes resolve and reach each other"

note "tearing down any prior $SYSTEM deployment"
uv run fab --search-root deploy/fabric tear-down --system="$SYSTEM" --inventory="$INVENTORY" || true

note "bringing up $SYSTEM cluster across all SUT nodes"
uv run fab --search-root deploy/fabric bring-up --system="$SYSTEM" --inventory="$INVENTORY"
ok "cluster deployment requested"

note "waiting 60 s for cluster initialisation"
sleep 60

note "generating $WORKLOAD dataset (sf=$SF)"
uv run fab --search-root deploy/fabric generate-data --workload="$WORKLOAD" --sf="$SF"
ok "dataset generated"

note "loading $WORKLOAD into $SYSTEM"
DATASET_PATH="data/generated/${WORKLOAD}_sf${SF}"
[ "$WORKLOAD" = "synthetic_snb" ] && DATASET_PATH="data/generated/synthetic_snb"
uv run fab --search-root deploy/fabric load-data --system="$SYSTEM" --workload="$WORKLOAD" --dataset-path="$DATASET_PATH"
ok "data loaded"

note "running post-load validator"
uv run fab --search-root deploy/fabric validate --system="$SYSTEM" --dataset-path="$DATASET_PATH" --workload="$WORKLOAD"
ok "validator passed"

note "summary"
cat <<EOF

╭────────────────────────────────────────────────────────────────╮
│  pdgraph-bench: $SYSTEM × $WORKLOAD is BENCHMARK-READY
╰────────────────────────────────────────────────────────────────╯

To run a scenario, invoke:

    uv run fab --search-root deploy/fabric run-scenario \\
        --system=$SYSTEM --scenario=s3 --workload=$WORKLOAD

For S5 (mixed + scale-out) the harness fires \`add_node\` at t=300 s into the
measurement window (configured in configs/scenarios/s5.yaml):

    uv run fab --search-root deploy/fabric run-scenario \\
        --system=$SYSTEM --scenario=s5 --workload=$WORKLOAD

Collect a run's artifacts back to this machine:

    uv run fab --search-root deploy/fabric collect-results --run-id=<id>

Tear the cluster down:

    uv run fab --search-root deploy/fabric tear-down --system=$SYSTEM

EOF
