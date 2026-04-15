#!/usr/bin/env python3
"""Print the last 60 klines ending at a given datetime with SMA7/25/99 close.

Loads aggregated kline parquet files under $TRADE_DATA/binance and prints a
table that the user can cross-check against the Binance UI.

Usage:
    python verify_with_sma.py
    python verify_with_sma.py -s BTCUSDT --interval 1h
    python verify_with_sma.py -m spot --interval 5m --datetime 2025-06-01T12:00
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

TRADE_DATA = Path(os.environ.get("TRADE_DATA", "/ndata/trade/data"))
WINDOW = 60
SMA_PERIODS = (7, 25, 99)
MAX_SMA = max(SMA_PERIODS)

QUOTES = ("USDT", "USDC", "BUSD", "FDUSD", "TUSD", "DAI", "BTC", "ETH", "BNB")


def kline_dir(market: str, market_sub: str, symbol: str) -> Path:
    if market == "spot":
        return TRADE_DATA / "binance" / "spot" / "aggTrades_kline" / symbol
    if market == "future":
        return TRADE_DATA / "binance" / "future" / market_sub / "aggTrades_kline" / symbol
    raise ValueError(f"unsupported market: {market}")


def load_klines(dir_: Path, symbol: str, interval: str, end: datetime) -> pd.DataFrame:
    pattern = f"{symbol}_kline_{interval}_*.parquet"
    files = sorted(dir_.glob(pattern))
    if not files:
        raise FileNotFoundError(f"no parquet files matching {dir_ / pattern}")

    # We need at least MAX_SMA+WINDOW bars before `end`. Reading the latest
    # 2 files is enough for any normal request — keeps memory small.
    selected = files[-2:] if len(files) >= 2 else files
    frames = [pq.read_table(f).to_pandas() for f in selected]
    df = pd.concat(frames, ignore_index=True)
    df = df.drop_duplicates(subset="time").sort_values("time").reset_index(drop=True)
    return df


def trade_url(market: str, symbol: str) -> str:
    if market == "future":
        return f"https://www.binance.com/en/futures/{symbol}"
    base, quote = split_symbol(symbol)
    pair = f"{base}_{quote}" if quote else symbol
    return f"https://www.binance.com/en/trade/{pair}"


def split_symbol(symbol: str) -> tuple[str, str]:
    for q in QUOTES:
        if symbol.endswith(q) and len(symbol) > len(q):
            return symbol[: -len(q)], q
    return symbol, ""


def parse_datetime(s: str | None) -> datetime:
    if s is None:
        return datetime.now(timezone.utc).replace(tzinfo=None)
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    raise SystemExit(f"unrecognised datetime: {s!r}")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("-s", "--symbol", default="ETHUSDT")
    p.add_argument("--datetime", help="end datetime (UTC). default: now")
    p.add_argument("-m", "--market", default="future", choices=["spot", "future"])
    p.add_argument("--market-sub", default="um", choices=["um", "cm"])
    p.add_argument("-i", "--interval", default="1d",
                   help="kline interval (1m,3m,5m,15m,30m,1h,4h,1d, ...)")
    args = p.parse_args()

    end = parse_datetime(args.datetime)
    dir_ = kline_dir(args.market, args.market_sub, args.symbol)
    df = load_klines(dir_, args.symbol, args.interval, end)

    df = df[df["time"] <= pd.Timestamp(end)]
    if df.empty:
        raise SystemExit(f"no klines at or before {end}")

    needed = WINDOW + MAX_SMA - 1
    if len(df) < needed:
        print(f"warning: only {len(df)} bars available "
              f"(need {needed} for full SMA{MAX_SMA})", file=sys.stderr)

    closes = df["close"]
    for n in SMA_PERIODS:
        df[f"SMA{n}"] = closes.rolling(n).mean()

    out = df.tail(WINDOW)[
        ["time", "open", "high", "low", "close", "SMA7", "SMA25", "SMA99"]
    ]

    market_label = f"{args.market}/{args.market_sub}" if args.market == "future" else args.market
    print(f"== {args.symbol} {market_label} {args.interval} — last {WINDOW} bars ending {end} ==")
    with pd.option_context("display.max_rows", None,
                           "display.width", 200,
                           "display.float_format", lambda v: f"{v:,.4f}"):
        print(out.to_string(index=False))

    print()
    print(f"Verify on Binance: {trade_url(args.market, args.symbol)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
