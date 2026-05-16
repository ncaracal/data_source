#!/bin/bash
set -e

DIR=/ndata/trade/data-source/data_source
BIN="$DIR/target/release"
STAMP="$DIR/service/option/.last_run"
REPORT_DIR="$DIR/report/option"

# On boot: skip if last run was less than 24h ago
if [ "$1" = "--boot" ] && [ -f "$STAMP" ]; then
    last=$(cat "$STAMP")
    now=$(date +%s)
    elapsed=$(( now - last ))
    if [ "$elapsed" -lt 86400 ]; then
        echo "Last run was ${elapsed}s ago (< 24h), skipping."
        exit 0
    fi
fi

cd "$DIR"
mkdir -p "$REPORT_DIR"
DAY="$(date -u +%F)"
# Mirror all stdout/stderr to a per-day run log (also visible in journal)
exec > >(tee -a "$REPORT_DIR/$DAY.run.log") 2>&1

echo "=== $(date) Starting option jobs ==="

echo "--- BVOLIndex ETHBVOLUSDT ---"
"$BIN/data_source" -s ETHBVOLUSDT -m option -d BVOLIndex

date +%s > "$STAMP"
echo "=== $(date) Done ==="
