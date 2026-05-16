#!/bin/bash
set -e

DIR=/ndata/trade/data-source/data_source
BIN="$DIR/target/release"
PY="/ndata/trade/py312/bin/python3"
STAMP="$DIR/service/btceth/.last_run"
REPORT_DIR="$DIR/report/btceth"

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
REPORT="$REPORT_DIR/$DAY.report"
# Mirror all stdout/stderr to a per-day run log (also visible in journal)
exec > >(tee -a "$REPORT_DIR/$DAY.run.log") 2>&1

echo "=== $(date) Starting btceth jobs ==="

echo "--- aggTrades spot BTCUSDT ETHUSDT ---"
"$BIN/data_source" -s BTCUSDT ETHUSDT -m spot -d aggTrades

echo "--- agg_kline spot BTCUSDT ETHUSDT ---"
"$BIN/agg_kline" -m spot -s BTCUSDT -s ETHUSDT --kline-1s

echo "--- verify (informational; non-fatal) ---"
{
    echo "=== verify report $DAY (UTC) ==="
    echo
    echo "----- spot BTCUSDT ETHUSDT -----"
    "$PY" python/verify_with_binance_kline_api_last_day.py -s BTCUSDT ETHUSDT -m spot 2>&1 || true
} >> "$REPORT"
echo "verify output appended to $REPORT"

date +%s > "$STAMP"
echo "=== $(date) Done ==="
