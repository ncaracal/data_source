#!/bin/bash
set -e

# Usage: ./server/run_full_year.sh <SYMBOL> <YEAR>
# Example: ./server/run_full_year.sh ETHUSDT 2024
#
# Downloads and converts all data types for a symbol for an entire year:
#   - aggTrades (future/um + spot)
#   - metrics (future/um only)
#   - fundingRate (future/um only)
#   - agg_kline (future/um + spot, 1s klines only for ETHUSDT/BTCUSDT)

SYMBOL="${1:?Usage: $0 <SYMBOL> <YEAR>}"
YEAR="${2:?Usage: $0 <SYMBOL> <YEAR>}"

DIR="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$DIR/target/release"
TRADE_DATA="${TRADE_DATA:-/ndata/trade/data}"

START_DATE="${YEAR}-01-01"

# Build if needed
if [ ! -f "$BIN/data_source" ] || [ ! -f "$BIN/agg_kline" ]; then
    echo "Building release binaries..."
    cd "$DIR" && cargo build --release
fi

echo "=== $(date) Starting full-year download: ${SYMBOL} ${YEAR} ==="
echo "Trade data: ${TRADE_DATA}"

echo "--- [1/4] aggTrades future/um ${SYMBOL} ---"
"$BIN/data_source" -s "$SYMBOL" -m future --market-sub um -d aggTrades \
    --start-date "$START_DATE" --trade-data "$TRADE_DATA"

echo "--- [2/4] aggTrades spot ${SYMBOL} ---"
"$BIN/data_source" -s "$SYMBOL" -m spot -d aggTrades \
    --start-date "$START_DATE" --trade-data "$TRADE_DATA"

echo "--- [3/4] metrics future/um ${SYMBOL} ---"
"$BIN/data_source" -s "$SYMBOL" -m future -d metrics \
    --start-date "$START_DATE" --trade-data "$TRADE_DATA"

echo "--- [4/4] fundingRate future/um ${SYMBOL} ---"
"$BIN/data_source" -s "$SYMBOL" -m future -d fundingRate \
    --start-date "$START_DATE" --trade-data "$TRADE_DATA"

# Only ETHUSDT and BTCUSDT get 1s klines
KLINE_1S_FLAG=""
if [ "$SYMBOL" = "ETHUSDT" ] || [ "$SYMBOL" = "BTCUSDT" ]; then
    KLINE_1S_FLAG="--kline-1s"
fi

echo "--- agg_kline future/um ${SYMBOL} ---"
"$BIN/agg_kline" -m um -s "$SYMBOL" $KLINE_1S_FLAG --data-dir "$TRADE_DATA"

echo "--- agg_kline spot ${SYMBOL} ---"
"$BIN/agg_kline" -m spot -s "$SYMBOL" $KLINE_1S_FLAG --data-dir "$TRADE_DATA"

echo "=== $(date) Done: ${SYMBOL} ${YEAR} ==="
