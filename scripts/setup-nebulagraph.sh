#!/usr/bin/env bash
# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

# Bring the simulated 5-node cluster up, generate a small synthetic SNB
# dataset, and load it into NebulaGraph. Leaves the cluster running and ready
# for benchmark runs.
#
# Idempotent: re-running this script tears down any prior cluster, rebuilds the
# client image if its sources changed, and re-creates the demo space from
# scratch.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

COMPOSE="docker compose -f docker-compose.cluster.yaml"
DATASET_DIR="data/generated/synthetic_snb"
PERSONS=${PERSONS:-1000}
AVG_DEGREE=${AVG_DEGREE:-5}

note() { printf '\n\033[1;34m▸ %s\033[0m\n' "$*"; }
ok()   { printf '\033[1;32m✓\033[0m %s\n' "$*"; }

note "tearing down any prior cluster"
$COMPOSE down --remove-orphans -v >/dev/null 2>&1 || true

note "building gb-node1 image (pdgraph-bench client)"
$COMPOSE build node1

note "pulling NebulaGraph images (one-time, ~1.5 GB total)"
$COMPOSE pull metad0 storaged0 graphd0 >/dev/null

note "starting cluster (11 containers across 5 logical nodes)"
$COMPOSE up -d
ok "cluster started"

note "waiting for graphd healthchecks to pass"
deadline=$(( $(date +%s) + 300 ))
while true; do
    pending=$($COMPOSE ps --format json 2>/dev/null \
        | grep -E '"Name":"gb-graphd[0-2]"' \
        | grep -v 'starting\|healthy' || true)
    healthy=$($COMPOSE ps --format json 2>/dev/null \
        | grep -E '"Name":"gb-graphd[0-2]"' \
        | grep -c '"Health":"healthy"' || true)
    if [ "${healthy:-0}" -ge 3 ]; then
        ok "graphd0/1/2 healthy"
        break
    fi
    if [ $(date +%s) -ge "$deadline" ]; then
        echo "timeout waiting for graphd health" >&2
        $COMPOSE ps
        exit 1
    fi
    printf '.'
    sleep 3
done

note "generating synthetic SNB dataset (persons=$PERSONS, avg_degree=$AVG_DEGREE)"
docker exec gb-node1 python -m data.generators.synthetic_snb \
    --persons "$PERSONS" --avg-degree "$AVG_DEGREE" \
    --out /app/data/generated/synthetic_snb

note "loading dataset into NebulaGraph (space=snb_demo)"
docker exec gb-node1 python -m data.loaders.nebulagraph \
    --host graphd0 --port 9669 \
    --space snb_demo \
    --dataset /app/data/generated/synthetic_snb

note "smoke validation (pdgraph-bench list-systems / driver connect)"
docker exec gb-node1 pdgraph-bench list-systems
docker exec gb-node1 pdgraph-bench list-workloads

note "summary"
$COMPOSE ps --format 'table {{.Name}}\t{{.Status}}\t{{.Image}}'
echo
ok "cluster is benchmark-ready"
echo
echo "Run a scenario from inside gb-node1 with:"
echo "  docker exec gb-node1 pdgraph-bench run \\"
echo "      configs/scenarios/demo-cluster.yaml \\"
echo "      --system configs/systems/nebulagraph-demo.yaml \\"
echo "      --workload configs/workloads/synthetic_snb.yaml"
echo
echo "Tear the cluster down with:"
echo "  $COMPOSE down -v"
