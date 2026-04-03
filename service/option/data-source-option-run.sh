#!/bin/bash
set -e

DIR=/ndata/trade/data-source/data_source
BIN="$DIR/target/release"
STAMP="$DIR/service/option/.last_run"

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

echo "=== $(date) Starting data-source jobs ==="

echo "--- aggTrades BTCUSDT ETHUSDT ---"
"$BIN/data_source" -s BTCUSDT ETHUSDT -m future --market-sub um -d aggTrades

echo "--- BVOLIndex ETHBVOLUSDT ---"
"$BIN/data_source" -s ETHBVOLUSDT -m option -d BVOLIndex

echo "--- agg_kline ---"
"$BIN/agg_kline" -m um -s BTCUSDT -s ETHUSDT --kline-1s

# Record last successful run
date +%s > "$STAMP"
echo "=== $(date) Done ==="
