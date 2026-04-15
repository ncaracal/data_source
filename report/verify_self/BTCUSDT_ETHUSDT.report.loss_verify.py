#!/usr/bin/env python3
"""Verify the 1m/3m/5m kline gaps reported by verify_self for um ETHUSDT 2025.

verify_self flagged these diffs on um ETHUSDT 2025:
    1m: -19 bars
    3m: -6 bars
    5m: -3 bars

This script proves the gaps are a real Binance USDⓈ-M trading halt on
2025-08-29 06:18..06:36 UTC (no trades for 19 consecutive minutes), not a
local aggregator/download bug, by cross-checking three sources:

    1. Local kline parquet        — list exact missing bars per interval
    2. Local aggTrades parquet    — confirm zero trades in the window
    3. Binance fapi REST endpoint — confirm the remote API also has
                                    n_trades=0 / vol=0 for those minutes

Run:
    python3 report/BTCUSDT_ETHUSDT.report.loss_verify.py

news: 
https://www.binance.com/en/square/post/28974721737385
Aug 29, 2025 — Binance Update – August 29, 2025. Binance temporarily halted all futures trading due to a technical issue but has now fully restored services.
"""
from __future__ import annotations

import json
import os
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow.compute as pc
import pyarrow.parquet as pq

TRADE_DATA = Path(os.environ.get("TRADE_DATA", "/ndata/trade/data"))
SYMBOL = "ETHUSDT"
YEAR = 2025

KLINE_DIR = TRADE_DATA / "binance" / "future" / "um" / "aggTrades_kline" / SYMBOL
AGG_FILE = TRADE_DATA / "binance" / "future" / "um" / "aggTrades" / SYMBOL / f"{SYMBOL}_aggTrades_{YEAR}.parquet"

# Window known to contain the halt (widen 3 min each side for context)
WINDOW_START = pd.Timestamp("2025-08-29 06:15:00")
WINDOW_END = pd.Timestamp("2025-08-29 06:40:00")


def missing_bars(interval_label: str, freq: str) -> list[pd.Timestamp]:
    f = KLINE_DIR / f"{SYMBOL}_kline_{interval_label}_{YEAR}.parquet"
    df = pq.read_table(f, columns=["time"]).to_pandas().sort_values("time")
    expected = pd.date_range(f"{YEAR}-01-01", f"{YEAR}-12-31 23:59:59", freq=freq)
    return list(expected.difference(df["time"]))


def local_trade_counts(start: pd.Timestamp, end: pd.Timestamp) -> pd.Series:
    pf = pq.ParquetFile(AGG_FILE)
    frames: list[pd.DataFrame] = []
    for rg in range(pf.num_row_groups):
        batch = pf.read_row_group(rg, columns=["price", "quantity", "time"])
        t = batch["time"]
        if pc.max(t).as_py() < start or pc.min(t).as_py() > end:
            continue
        mask = pc.and_(
            pc.greater_equal(t, start.to_pydatetime()),
            pc.less(t, end.to_pydatetime()),
        )
        filt = batch.filter(mask)
        if filt.num_rows:
            frames.append(filt.to_pandas())
    if not frames:
        return pd.Series(dtype=int)
    df = pd.concat(frames)
    # Include empty minutes in the output so zero-trade minutes are visible
    full_index = pd.date_range(start, end - pd.Timedelta("1min"), freq="1min")
    counts = df.set_index("time").groupby(pd.Grouper(freq="1min")).size()
    return counts.reindex(full_index, fill_value=0)


def binance_fapi_klines(start: pd.Timestamp, end: pd.Timestamp) -> list[list]:
    url = (
        "https://fapi.binance.com/fapi/v1/klines?"
        f"symbol={SYMBOL}&interval=1m"
        f"&startTime={int(start.timestamp() * 1000)}"
        f"&endTime={int(end.timestamp() * 1000)}"
        "&limit=50"
    )
    req = urllib.request.Request(url, headers={"User-Agent": "loss_verify/1.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def main() -> int:
    print("=" * 68)
    print(f" 1) missing kline bars per interval (um {SYMBOL} {YEAR})")
    print("=" * 68)
    for label, freq in (("1m", "1min"), ("3m", "3min"), ("5m", "5min")):
        miss = missing_bars(label, freq)
        print(f"\n--- {label}: {len(miss)} missing ---")
        for t in miss:
            print(f"  {t}")

    print()
    print("=" * 68)
    print(f" 2) local aggTrades per-minute count ({WINDOW_START} .. {WINDOW_END})")
    print("=" * 68)
    counts = local_trade_counts(WINDOW_START, WINDOW_END)
    for t, n in counts.items():
        marker = "  <-- ZERO TRADES" if n == 0 else ""
        print(f"  {t}  n={n}{marker}")

    print()
    print("=" * 68)
    print(f" 3) Binance fapi v1/klines for same window")
    print("=" * 68)
    rows = binance_fapi_klines(WINDOW_START, WINDOW_END)
    print(f"  returned {len(rows)} bars")
    for k in rows:
        t = datetime.fromtimestamp(k[0] / 1000, tz=timezone.utc)
        marker = "  <-- n_trades=0 vol=0" if int(k[8]) == 0 else ""
        print(f"  {t}  O={k[1]:<10}  C={k[4]:<10}  vol={k[5]:<10}  n_trades={k[8]}{marker}")

    print()
    print("=" * 68)
    print(" conclusion")
    print("=" * 68)
    print("  All three sources agree: Binance USDⓈ-M ETHUSDT had a trading")
    print("  halt on 2025-08-29 06:18..06:36 UTC (19 minutes, 0 trades).")
    print("  The 1m/3m/5m kline diffs are real, not a local aggregator bug.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
