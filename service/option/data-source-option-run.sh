#!/bin/bash
set -e

DIR=/ndata/trade/data-source/data_source
BIN="$DIR/target/release"
PY="/ndata/trade/py312/bin/python3"
STAMP="$DIR/service/option/.last_run"
REPORT_DIR="$DIR/report/service"

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
REPORT="$REPORT_DIR/$(date -u +%F).report"

echo "=== $(date) Starting data-source jobs ==="

echo "--- aggTrades future/um BTCUSDT ETHUSDT ---"
"$BIN/data_source" -s BTCUSDT ETHUSDT -m future --market-sub um -d aggTrades

echo "--- aggTrades spot BTCUSDT ETHUSDT ---"
"$BIN/data_source" -s BTCUSDT ETHUSDT -m spot -d aggTrades

echo "--- BVOLIndex ETHBVOLUSDT ---"
"$BIN/data_source" -s ETHBVOLUSDT -m option -d BVOLIndex

echo "--- agg_kline future/um ---"
"$BIN/agg_kline" -m um -s BTCUSDT -s ETHUSDT --kline-1s

echo "--- agg_kline spot ---"
"$BIN/agg_kline" -m spot -s BTCUSDT -s ETHUSDT --kline-1s

echo "--- verify (informational; non-fatal) ---"
{
    echo "=== verify report $(date -u +%F) (UTC) ==="
    echo
    echo "----- spot BTCUSDT ETHUSDT -----"
    "$PY" python/verify_with_binance_kline_api_last_day.py -s BTCUSDT ETHUSDT -m spot 2>&1 || true
    echo
    echo "----- future/um BTCUSDT ETHUSDT -----"
    "$PY" python/verify_with_binance_kline_api_last_day.py -s BTCUSDT ETHUSDT -m future --market-sub um 2>&1 || true
} >> "$REPORT"
echo "verify output appended to $REPORT"

date +%s > "$STAMP"
echo "=== $(date) Done ==="
