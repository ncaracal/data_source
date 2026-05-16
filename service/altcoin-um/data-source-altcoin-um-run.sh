#!/bin/bash
set -e

DIR=/ndata/trade/data-source/data_source
PY="/ndata/trade/py312/bin/python3"
STAMP="$DIR/service/altcoin-um/.last_run"
REPORT_DIR="$DIR/report/altcoin-um"

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
# Python scripts write their logs/reports under this base (running_log/,
# verify_all_symbols_id/) — keeps all output in report/altcoin-um/.
export DATA_SOURCE_REPORT_DIR="$REPORT_DIR"
# Mirror all stdout/stderr to a per-day run log (also visible in journal)
exec > >(tee -a "$REPORT_DIR/$DAY.run.log") 2>&1

echo "=== $(date) Starting altcoin-um jobs ==="

echo "--- altcoin download + verify um (non-fatal) ---"
"$PY" python/altcoin_symbols_download_and_verify.py --exchange binance --market um || \
    echo "WARN: altcoin um download/verify returned non-zero — continuing"

echo "--- altcoin -> kline um (non-fatal) ---"
"$PY" python/altcoin_symbols_to_kline.py --exchange binance --market um || \
    echo "WARN: altcoin um kline returned non-zero — continuing"

date +%s > "$STAMP"
echo "=== $(date) Done ==="
