#!/usr/bin/env bash
# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

# Wrapper around the official LDBC SNB Datagen (Spark edition).
#
# Usage: ldbc_snb_datagen.sh <profile> <sf> <output_dir>
#   profile:    interactive | bi
#   sf:         scale factor (e.g., 0.003, 0.1, 1, 3, 10)
#   output_dir: destination directory on the host (will be created); MUST live
#               under a path the docker daemon can mount (snap docker on
#               Ubuntu only allows /home/* by default)
#
# Strategy:
#   1. Run `ldbc/datagen-standalone` (Spark + jar packaged) as root inside a
#      container; mount the output dir as /work.
#   2. The image's entrypoint is run.py — a Spark wrapper. Datagen flags go
#      after `--`.
#   3. Output schema is the `composite-merged-fk` Spark-partitioned tree:
#         <out>/graphs/csv/<profile>/composite-merged-fk/{dynamic,static}/<Entity>/part-*.csv
#      The loaders (data/loaders/ldbc_snb_*.py) consume this layout via
#      `data/loaders/_ldbc_snb_v2.py::LdbcDatasetLayout`.
#   4. The container writes as root → host user can't delete/edit. After the
#      datagen finishes we run a quick `alpine chown` container against the
#      same mount to give ownership back to the invoking user.

set -euo pipefail

if [ "$#" -lt 3 ]; then
    echo "usage: $0 <interactive|bi> <sf> <output_dir>" >&2
    exit 2
fi

PROFILE="$1"
SF="$2"
OUT_DIR="$3"

case "$PROFILE" in
    interactive|bi) ;;
    *) echo "profile must be one of: interactive bi" >&2; exit 2 ;;
esac

mkdir -p "$OUT_DIR"
ABS_OUT="$(cd "$OUT_DIR" && pwd)"

# 0.5.1-2.12_spark3.2 is the latest stable tag at the time of writing; bumping
# it requires re-running the post-load validator across all SUTs because the
# datagen has historically renamed CSV columns between minor releases.
IMAGE="${LDBC_SNB_DATAGEN_IMAGE:-ldbc/datagen-standalone:0.5.1-2.12_spark3.2}"

echo "[datagen] pulling $IMAGE (one-time)"
docker pull "$IMAGE" >/dev/null

echo "[datagen] running SNB Datagen (profile=$PROFILE sf=$SF) → $ABS_OUT"
# Datagen flags go after `--` (the run.py wrapper consumes everything before).
docker run --rm \
    -v "$ABS_OUT:/work" \
    "$IMAGE" \
    -- \
    --format csv \
    --scale-factor "$SF" \
    --mode "$PROFILE" \
    --output-dir /work

# Files inside the volume are owned by root because the container ran as root.
# Use a throwaway alpine container to give ownership back to the host user, so
# subsequent `rm`/`mv` on the host doesn't need sudo.
echo "[datagen] reclaiming ownership"
docker run --rm -v "$ABS_OUT:/work" alpine \
    chown -R "$(id -u):$(id -g)" /work

echo "[datagen] done — output at $ABS_OUT"
echo "[datagen] entity tree:"
find "$ABS_OUT/graphs/csv/$PROFILE/composite-merged-fk" -mindepth 2 -maxdepth 2 -type d 2>/dev/null \
    | sort | sed 's|^|  |'
