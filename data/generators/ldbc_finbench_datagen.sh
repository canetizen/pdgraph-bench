#!/usr/bin/env bash
# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

# Wrapper around the LDBC FinBench Datagen.
#
# Usage: ldbc_finbench_datagen.sh <sf> <output_dir>
#   sf:         scale factor (e.g., 0.003, 0.1, 1, 3)
#   output_dir: destination directory on the host (will be created)
#
# Strategy:
#   - LDBC FinBench Datagen has no official Docker image. We build one locally
#     from data/generators/Dockerfile.finbench (multi-stage Maven + Spark) the
#     first time the script runs and tag it as pdgraph-bench/finbench-datagen.
#   - The container writes as root → `alpine chown` reclaims ownership so the
#     host user can manage the output directory.

set -euo pipefail

if [ "$#" -lt 2 ]; then
    echo "usage: $0 <sf> <output_dir>" >&2
    exit 2
fi

SF="$1"
OUT_DIR="$2"

mkdir -p "$OUT_DIR"
ABS_OUT="$(cd "$OUT_DIR" && pwd)"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

IMAGE="${LDBC_FINBENCH_DATAGEN_IMAGE:-pdgraph-bench/finbench-datagen:0.1.0}"

if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
    echo "[finbench-datagen] building $IMAGE (one-time, ~10 min)"
    docker build -t "$IMAGE" -f "$SCRIPT_DIR/Dockerfile.finbench" "$SCRIPT_DIR"
fi

echo "[finbench-datagen] running FinBench Datagen (sf=$SF) → $ABS_OUT"
docker run --rm \
    -v "$ABS_OUT:/work" \
    "$IMAGE" \
    --scale-factor "$SF" \
    --output-dir /work

echo "[finbench-datagen] reclaiming ownership"
docker run --rm -v "$ABS_OUT:/work" alpine \
    chown -R "$(id -u):$(id -g)" /work

echo "[finbench-datagen] done — output at $ABS_OUT"
