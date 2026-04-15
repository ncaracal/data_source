#!/usr/bin/env python3
"""Verify last-day kline data against Binance API for all intervals.

Runs verify_with_binance_kline_api for the most recent complete UTC day
(yesterday UTC) across every supported interval and writes a report to
report/verify_with_binance_kline_api/.

With --detail, additionally prints a per-bar OHLC + diff + sum_abs_diff
table for every interval, sorted by sum_abs_diff descending (biggest
mismatches first). Useful for drilling into WARN/FAIL rows.

Usage:
    python verify_with_binance_kline_api_last_day.py
    python verify_with_binance_kline_api_last_day.py -s ETHUSDT BTCUSDT
    python verify_with_binance_kline_api_last_day.py -m spot --detail
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from verify_with_binance_kline_api import (  # noqa: E402
    ALL_INTERVALS,
    date_range_ms,
    filter_supported_intervals,
    format_detail_table,
    format_table,
    report_path,
    verify_one,
    write_report,
)


def main() -> int:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("-s", "--symbol", nargs="+", default=["ETHUSDT"])
    p.add_argument("-m", "--market", default="future", choices=["spot", "future"])
    p.add_argument("--market-sub", default="um", choices=["um", "cm"])
    p.add_argument("--detail", action="store_true",
                   help="print per-bar OHLC diff table for each interval")
    p.add_argument("--detail-max-rows", type=int, default=50,
                   help="max rows to show per detail table (default 50)")
    args = p.parse_args()

    last_day = datetime.now(timezone.utc).date() - timedelta(days=1)
    intervals = filter_supported_intervals(args.market, ALL_INTERVALS)
    dropped = [i for i in ALL_INTERVALS if i not in intervals]
    if dropped:
        print(f"[info] dropping {','.join(dropped)} — not supported by "
              f"{args.market} API", file=sys.stderr)
    start_ms, end_ms = date_range_ms(last_day, last_day)

    results = []
    frames: list[tuple[str, str, "object", "object"]] = []
    for symbol in args.symbol:
        for interval in intervals:
            r, local_df, api_df = verify_one(
                symbol, interval, args.market, args.market_sub,
                start_ms, end_ms,
            )
            results.append(r)
            if args.detail:
                frames.append((symbol, interval, local_df, api_df))

    path = report_path(args.symbol, last_day, last_day, args.market, args.market_sub)
    write_report(path, results, args.symbol, args.market, args.market_sub,
                 intervals, last_day, last_day)
    print(f"wrote {path}")
    print()
    print(format_table(results))

    if args.detail:
        print()
        print("=" * 80)
        print("DETAIL (per-bar OHLC diffs)")
        print("=" * 80)
        for symbol, interval, local_df, api_df in frames:
            print()
            print(format_detail_table(symbol, interval, local_df, api_df,
                                      max_rows=args.detail_max_rows))

    return 1 if any(r.status == "FAIL" for r in results) else 0


if __name__ == "__main__":
    sys.exit(main())
