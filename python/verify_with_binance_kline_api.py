#!/usr/bin/env python3
"""Verify local kline parquet data against the Binance REST kline API.

For each (symbol, interval) combination, fetch klines from Binance for the
requested date range and compare OHLC + volume against the local aggregated
parquet. Writes a summary table under report/verify_with_binance_kline_api/.

APIs used:
    spot : https://api.binance.com/api/v3/klines
    um   : https://fapi.binance.com/fapi/v1/klines
    cm   : https://dapi.binance.com/dapi/v1/klines

Usage:
    python verify_with_binance_kline_api.py
    python verify_with_binance_kline_api.py -s ETHUSDT BTCUSDT \\
        --start-date 2026-04-01 --end-date 2026-04-14 -i 1d 1h
    python verify_with_binance_kline_api.py -m spot -s ETHUSDT -i 1d
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Sequence

import pandas as pd
import pyarrow.parquet as pq

TRADE_DATA = Path(os.environ.get("TRADE_DATA", "/ndata/trade/data"))
REPO_ROOT = Path(__file__).resolve().parent.parent
REPORT_DIR = REPO_ROOT / "report" / "verify_with_binance_kline_api"

ALL_INTERVALS: tuple[str, ...] = (
    "1s", "1m", "3m", "5m", "15m", "30m", "1h", "4h", "8h", "12h", "1d",
)

INTERVAL_MS: dict[str, int] = {
    "1s": 1_000,
    "1m": 60_000,
    "3m": 3 * 60_000,
    "5m": 5 * 60_000,
    "15m": 15 * 60_000,
    "30m": 30 * 60_000,
    "1h": 3_600_000,
    "2h": 2 * 3_600_000,
    "4h": 4 * 3_600_000,
    "6h": 6 * 3_600_000,
    "8h": 8 * 3_600_000,
    "12h": 12 * 3_600_000,
    "1d": 86_400_000,
}

# Binance futures fapi/dapi do not expose 1s klines (spot API does).
# Filter 1s out of futures runs to avoid a guaranteed -1120 "Invalid interval".
_UNSUPPORTED_API_INTERVALS: dict[str, frozenset[str]] = {
    "spot": frozenset(),
    "future": frozenset({"1s"}),
}


def filter_supported_intervals(market: str, intervals: Iterable[str]) -> list[str]:
    blocked = _UNSUPPORTED_API_INTERVALS.get(market, frozenset())
    return [i for i in intervals if i not in blocked]


_API_BASE: dict[tuple[str, str | None], tuple[str, int]] = {
    ("spot", None): ("https://api.binance.com/api/v3/klines", 1000),
    ("future", "um"): ("https://fapi.binance.com/fapi/v1/klines", 1500),
    ("future", "cm"): ("https://dapi.binance.com/dapi/v1/klines", 1500),
}


def kline_dir(market: str, market_sub: str, symbol: str) -> Path:
    if market == "spot":
        return TRADE_DATA / "binance" / "spot" / "aggTrades_kline" / symbol
    return TRADE_DATA / "binance" / "future" / market_sub / "aggTrades_kline" / symbol


def load_local_klines(market: str, market_sub: str, symbol: str, interval: str,
                      start_ms: int, end_ms: int) -> pd.DataFrame:
    dir_ = kline_dir(market, market_sub, symbol)
    cols = ["time", "open", "high", "low", "close", "qty", "qty_usd"]
    files = sorted(dir_.glob(f"{symbol}_kline_{interval}_*.parquet"))
    if not files:
        return pd.DataFrame(columns=cols)

    start_dt = pd.Timestamp(start_ms, unit="ms")
    end_dt = pd.Timestamp(end_ms, unit="ms")
    frames: list[pd.DataFrame] = []
    for f in files:
        try:
            pf = pq.ParquetFile(f)
            col_idx = pf.schema_arrow.get_field_index("time")
            meta = pf.metadata
            if col_idx >= 0 and meta is not None and meta.num_row_groups > 0:
                s_first = meta.row_group(0).column(col_idx).statistics
                s_last = meta.row_group(meta.num_row_groups - 1).column(col_idx).statistics
                if s_first and s_last and s_first.has_min_max and s_last.has_min_max:
                    fmin = pd.Timestamp(s_first.min)
                    fmax = pd.Timestamp(s_last.max)
                    if fmax < start_dt or fmin > end_dt:
                        continue
        except Exception:
            pass
        frames.append(pq.read_table(f, columns=cols).to_pandas())

    if not frames:
        return pd.DataFrame(columns=cols)
    df = pd.concat(frames, ignore_index=True)
    df = df.drop_duplicates(subset="time").sort_values("time").reset_index(drop=True)
    df = df[(df["time"] >= start_dt) & (df["time"] <= end_dt)]
    return df.reset_index(drop=True)


def _http_get_json(url: str, max_retries: int = 6) -> tuple[object, dict[str, str]]:
    """GET a JSON URL with retry/backoff for Binance rate limits.

    Handles 429 (rate limit) and 418 (IP ban) by honoring Retry-After, and
    transient network errors with exponential backoff. Returns (payload, headers).
    """
    req = urllib.request.Request(url, headers={"User-Agent": "verify_kline/1.0"})
    backoff = 1.0
    for attempt in range(max_retries):
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                body = resp.read()
                return json.loads(body), dict(resp.headers)
        except urllib.error.HTTPError as e:
            code = e.code
            body_bytes = b""
            try:
                body_bytes = e.read()
            except Exception:
                pass
            body_text = body_bytes[:300].decode("utf-8", "replace")
            if code in (429, 418):
                retry_after = e.headers.get("Retry-After") if e.headers else None
                try:
                    wait = float(retry_after) if retry_after else backoff * 4
                except ValueError:
                    wait = backoff * 4
                wait = max(wait, 1.0)
                print(f"[rate-limit] HTTP {code} on {url}; sleeping {wait:.1f}s "
                      f"(attempt {attempt + 1}/{max_retries})", file=sys.stderr)
                time.sleep(wait)
                backoff = min(backoff * 2, 60.0)
                continue
            if 500 <= code < 600 and attempt + 1 < max_retries:
                print(f"[retry] HTTP {code} on {url}; sleeping {backoff:.1f}s",
                      file=sys.stderr)
                time.sleep(backoff)
                backoff = min(backoff * 2, 30.0)
                continue
            raise RuntimeError(f"HTTP {code}: {body_text}") from e
        except (urllib.error.URLError, TimeoutError) as e:
            if attempt + 1 >= max_retries:
                raise RuntimeError(f"network error: {e}") from e
            print(f"[retry] network error {e}; sleeping {backoff:.1f}s",
                  file=sys.stderr)
            time.sleep(backoff)
            backoff = min(backoff * 2, 30.0)
    raise RuntimeError(f"max retries exhausted for {url}")


# Proactive weight throttle. Binance caps used weight per minute at ~1200 (spot)
# and ~2400 (fapi/dapi). If the server reports we're approaching the cap, sleep.
_WEIGHT_CAP = {"spot": 1100, "future": 2200}


def _maybe_throttle_weight(market: str, headers: dict[str, str]) -> None:
    cap = _WEIGHT_CAP.get(market, 1100)
    used = 0
    for k, v in headers.items():
        kl = k.lower()
        if kl.startswith("x-mbx-used-weight-1m") or kl == "x-mbx-used-weight":
            try:
                used = max(used, int(v))
            except ValueError:
                pass
    if used >= cap:
        print(f"[throttle] used weight={used} >= cap {cap}; sleeping 10s",
              file=sys.stderr)
        time.sleep(10.0)


def fetch_binance_klines(market: str, market_sub: str, symbol: str, interval: str,
                         start_ms: int, end_ms: int,
                         throttle_s: float = 0.25) -> pd.DataFrame:
    key: tuple[str, str | None] = ("spot", None) if market == "spot" else ("future", market_sub)
    base, max_limit = _API_BASE[key]

    rows: list[list] = []
    cursor = start_ms
    while cursor <= end_ms:
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": cursor,
            "endTime": end_ms,
            "limit": max_limit,
        }
        url = f"{base}?{urllib.parse.urlencode(params)}"
        batch, headers = _http_get_json(url)
        _maybe_throttle_weight(market, headers)
        if not batch:
            break
        rows.extend(batch)
        last_open = int(batch[-1][0])
        if len(batch) < max_limit:
            break
        next_cursor = last_open + INTERVAL_MS[interval]
        if next_cursor <= cursor:
            break
        cursor = next_cursor
        time.sleep(throttle_s)

    cols = ["time", "open", "high", "low", "close", "qty", "qty_usd"]
    if not rows:
        return pd.DataFrame(columns=cols)
    df = pd.DataFrame(rows, columns=[
        "open_time", "open", "high", "low", "close", "volume", "close_time",
        "quote_volume", "trades", "taker_buy_base", "taker_buy_quote", "ignore",
    ])
    df["time"] = pd.to_datetime(df["open_time"], unit="ms")
    for c in ("open", "high", "low", "close", "volume", "quote_volume"):
        df[c] = df[c].astype(float)
    df = df.rename(columns={"volume": "qty", "quote_volume": "qty_usd"})
    df = df[cols]
    return df.drop_duplicates(subset="time").sort_values("time").reset_index(drop=True)


@dataclass
class CompareResult:
    symbol: str
    interval: str
    local_bars: int
    api_bars: int
    matched: int
    missing_local: int
    missing_api: int
    max_abs_close: float
    max_rel_close: float
    max_abs_qty: float
    max_rel_qty: float
    status: str


def compare(symbol: str, interval: str,
            local_df: pd.DataFrame, api_df: pd.DataFrame,
            rel_tol_close: float = 1e-4,
            rel_tol_qty: float = 5e-2) -> CompareResult:
    local_bars = len(local_df)
    api_bars = len(api_df)
    if local_bars == 0 and api_bars == 0:
        return CompareResult(symbol, interval, 0, 0, 0, 0, 0, 0.0, 0.0, 0.0, 0.0, "SKIP")

    merged = pd.merge(local_df, api_df, on="time", how="outer",
                      suffixes=("_l", "_a"), indicator=True)
    matched = int((merged["_merge"] == "both").sum())
    # Binance emits phantom zero-qty bars for intervals with no trades
    # (observed on 1s). Our local aggregator only emits bars when trades
    # exist, so don't count zero-qty API bars as "missing_local".
    right_only = merged["_merge"] == "right_only"
    api_qty = merged["qty_a"].fillna(0.0)
    missing_local = int((right_only & (api_qty > 0)).sum())
    missing_api = int((merged["_merge"] == "left_only").sum())

    if matched == 0:
        return CompareResult(symbol, interval, local_bars, api_bars, 0,
                             missing_local, missing_api, 0.0, 0.0, 0.0, 0.0, "FAIL")

    both = merged[merged["_merge"] == "both"]
    close_l = both["close_l"].astype(float)
    close_a = both["close_a"].astype(float)
    close_diff = (close_l - close_a).abs()
    max_abs_close = float(close_diff.max())
    max_rel_close = float((close_diff / close_a.abs().clip(lower=1e-12)).max())

    qty_l = both["qty_l"].astype(float)
    qty_a = both["qty_a"].astype(float)
    qty_diff = (qty_l - qty_a).abs()
    max_abs_qty = float(qty_diff.max())
    max_rel_qty = float((qty_diff / qty_a.abs().clip(lower=1e-12)).max())

    if missing_local or missing_api:
        status = "FAIL"
    elif max_rel_close <= rel_tol_close and max_rel_qty <= rel_tol_qty:
        status = "PASS"
    elif max_rel_close <= 1e-3:
        status = "WARN"
    else:
        status = "FAIL"
    return CompareResult(symbol, interval, local_bars, api_bars, matched,
                         missing_local, missing_api,
                         max_abs_close, max_rel_close, max_abs_qty, max_rel_qty,
                         status)


def date_range_ms(start: date, end: date) -> tuple[int, int]:
    start_dt = datetime(start.year, start.month, start.day, tzinfo=timezone.utc)
    end_dt = datetime(end.year, end.month, end.day, 23, 59, 59, 999_000, tzinfo=timezone.utc)
    return int(start_dt.timestamp() * 1000), int(end_dt.timestamp() * 1000)


def _empty_df() -> pd.DataFrame:
    return pd.DataFrame(columns=["time", "open", "high", "low", "close", "qty", "qty_usd"])


def verify_one(symbol: str, interval: str, market: str, market_sub: str,
               start_ms: int, end_ms: int,
               throttle_s: float = 0.25
               ) -> tuple[CompareResult, pd.DataFrame, pd.DataFrame]:
    """Fetch API + load local + compare for a single (symbol, interval).

    Returns (result, local_df, api_df). On API failure returns SKIP status
    with empty dataframes.
    """
    try:
        api_df = fetch_binance_klines(market, market_sub, symbol, interval,
                                      start_ms, end_ms, throttle_s)
    except Exception as e:
        print(f"[warn] {symbol} {interval}: API fetch failed: {e}", file=sys.stderr)
        return (
            CompareResult(symbol, interval, 0, 0, 0, 0, 0,
                          0.0, 0.0, 0.0, 0.0, "SKIP"),
            _empty_df(),
            _empty_df(),
        )
    local_df = load_local_klines(market, market_sub, symbol, interval,
                                 start_ms, end_ms)
    return compare(symbol, interval, local_df, api_df), local_df, api_df


def run(symbols: Sequence[str], market: str, market_sub: str,
        intervals: Iterable[str], start: date, end: date,
        throttle_s: float = 0.25) -> list[CompareResult]:
    start_ms, end_ms = date_range_ms(start, end)
    intervals = filter_supported_intervals(market, intervals)
    results: list[CompareResult] = []
    for symbol in symbols:
        for interval in intervals:
            r, _, _ = verify_one(symbol, interval, market, market_sub,
                                 start_ms, end_ms, throttle_s)
            results.append(r)
    return results


def format_detail_table(symbol: str, interval: str,
                        local_df: pd.DataFrame, api_df: pd.DataFrame,
                        max_rows: int = 50) -> str:
    """Per-bar OHLC diff table. Rows are ordered by sum_abs_diff descending
    (biggest mismatches first), capped at ``max_rows``.
    """
    title = f"== {symbol} {interval} =="
    if local_df.empty and api_df.empty:
        return f"{title}\n(no data on either side)"

    merged = pd.merge(local_df, api_df, on="time", how="outer",
                      suffixes=("_p", "_b"))
    merged = merged.sort_values("time").reset_index(drop=True)

    price_cols = ("open", "high", "low", "close")
    for c in price_cols:
        merged[f"{c}_diff"] = merged[f"{c}_p"] - merged[f"{c}_b"]

    diff_cols = [f"{c}_diff" for c in price_cols]
    merged["sum_abs_diff"] = merged[diff_cols].abs().sum(axis=1, min_count=1)

    total_rows = len(merged)
    missing_rows = int(merged["sum_abs_diff"].isna().sum())
    diff_rows = int((merged["sum_abs_diff"].fillna(0.0) > 0).sum())
    mismatched = missing_rows + diff_rows
    total_sum_abs = float(merged[diff_cols].abs().sum().sum())

    # Sort by magnitude of real OHLC diff first (descending); NaN rows
    # (missing on one side) are treated as 0 so they sort to the bottom
    # of the priority list — the caller cares more about real mismatches.
    sort_key = merged["sum_abs_diff"].fillna(0.0)
    shown = merged.assign(_sk=sort_key).sort_values(
        ["_sk", "time"], ascending=[False, True]
    ).head(max_rows)

    display_cols = ["time"]
    for c in price_cols:
        display_cols.extend([f"{c}_p", f"{c}_b", f"{c}_diff"])
    display_cols.append("sum_abs_diff")

    header = [
        "time", "open_p", "open_b", "open_diff",
        "high_p", "high_b", "high_diff",
        "low_p", "low_b", "low_diff",
        "close_p", "close_b", "close_diff", "sum_abs_diff",
    ]

    def fmt_cell(col: str, value) -> str:
        if pd.isna(value):
            return "--"
        if col == "time":
            return pd.Timestamp(value).strftime("%Y-%m-%d %H:%M:%S")
        return f"{float(value):,.4f}"

    rows: list[list[str]] = [header]
    for _, row in shown.iterrows():
        rows.append([fmt_cell(col, row[col]) for col in display_cols])

    widths = [max(len(r[i]) for r in rows) for i in range(len(header))]
    sep = "  ".join("-" * w for w in widths)
    def render(r: list[str]) -> str:
        return "  ".join(v.ljust(w) for v, w in zip(r, widths))

    lines = [title, render(rows[0]), sep]
    lines.extend(render(r) for r in rows[1:])
    lines.append(
        f"shown {len(rows) - 1}/{total_rows} bars | "
        f"mismatched (or missing) rows: {mismatched} | "
        f"total sum_abs_diff: {total_sum_abs:,.4f}"
    )
    return "\n".join(lines)


def _fmt_row(values: tuple, widths: list[int]) -> str:
    return "  ".join(str(v).ljust(w) for v, w in zip(values, widths))


def format_table(results: list[CompareResult]) -> str:
    header = ("symbol", "interval", "local", "api", "matched", "miss_local",
              "miss_api", "max_abs_close", "max_rel_close", "max_abs_qty",
              "max_rel_qty", "status")
    body = [(
        r.symbol, r.interval,
        str(r.local_bars), str(r.api_bars), str(r.matched),
        str(r.missing_local), str(r.missing_api),
        f"{r.max_abs_close:.4f}", f"{r.max_rel_close:.2e}",
        f"{r.max_abs_qty:.4f}", f"{r.max_rel_qty:.2e}",
        r.status,
    ) for r in results]
    rows = [header] + body
    widths = [max(len(row[c]) for row in rows) for c in range(len(header))]
    lines = [_fmt_row(header, widths), "  ".join("-" * w for w in widths)]
    lines.extend(_fmt_row(row, widths) for row in body)
    return "\n".join(lines)


def report_path(symbols: Sequence[str], start: date, end: date,
                market: str = "", market_sub: str = "") -> Path:
    syms_token = symbols[0] if len(symbols) < 2 else f"{len(symbols)}symbols"
    date_token = start.isoformat() if start == end else f"{start.isoformat()}_{end.isoformat()}"
    if market == "spot":
        market_token = "spot"
    elif market == "future":
        market_token = market_sub or "um"
    else:
        market_token = ""
    parts = [syms_token]
    if market_token:
        parts.append(market_token)
    parts.append(date_token)
    return REPORT_DIR / f"{'_'.join(parts)}.report"


def write_report(path: Path, results: list[CompareResult],
                 symbols: Sequence[str], market: str, market_sub: str,
                 intervals: Sequence[str], start: date, end: date) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    market_label = f"{market}/{market_sub}" if market == "future" else market
    date_line = start.isoformat() if start == end else f"{start.isoformat()} .. {end.isoformat()}"
    passes = sum(1 for r in results if r.status == "PASS")
    warns = sum(1 for r in results if r.status == "WARN")
    fails = sum(1 for r in results if r.status == "FAIL")
    skips = sum(1 for r in results if r.status == "SKIP")
    header = [
        "Binance kline API verification",
        f"Generated : {datetime.now(timezone.utc).isoformat(timespec='seconds')}",
        f"Market    : {market_label}",
        f"Date      : {date_line}",
        f"Symbols ({len(symbols)}): {', '.join(symbols)}",
        f"Intervals ({len(intervals)}): {', '.join(intervals)}",
        f"Summary   : PASS={passes} WARN={warns} FAIL={fails} SKIP={skips}",
        "",
    ]
    path.write_text("\n".join(header) + format_table(results) + "\n")


def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _default_date() -> date:
    return datetime.now(timezone.utc).date() - timedelta(days=1)


def main() -> int:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("-s", "--symbol", nargs="+", default=["ETHUSDT"])
    p.add_argument("--start-date", type=_parse_date,
                   help="UTC start date YYYY-MM-DD (default: yesterday UTC)")
    p.add_argument("--end-date", type=_parse_date,
                   help="UTC end date YYYY-MM-DD (default: start-date)")
    p.add_argument("-m", "--market", default="future", choices=["spot", "future"])
    p.add_argument("--market-sub", default="um", choices=["um", "cm"])
    p.add_argument("-i", "--interval", nargs="+", default=list(ALL_INTERVALS),
                   help=f"kline intervals (default: {' '.join(ALL_INTERVALS)})")
    p.add_argument("--detail", action="store_true",
                   help="print per-bar OHLC diff table for each interval")
    p.add_argument("--detail-max-rows", type=int, default=50,
                   help="max rows per detail table (default 50)")
    args = p.parse_args()

    start = args.start_date or _default_date()
    end = args.end_date or start
    if start > end:
        raise SystemExit("start-date must be <= end-date")

    intervals = filter_supported_intervals(args.market, args.interval)
    dropped = [i for i in args.interval if i not in intervals]
    if dropped:
        print(f"[info] dropping {','.join(dropped)} — not supported by "
              f"{args.market} API", file=sys.stderr)

    start_ms, end_ms = date_range_ms(start, end)
    results: list[CompareResult] = []
    frames: list[tuple[str, str, pd.DataFrame, pd.DataFrame]] = []
    for symbol in args.symbol:
        for interval in intervals:
            r, local_df, api_df = verify_one(
                symbol, interval, args.market, args.market_sub,
                start_ms, end_ms,
            )
            results.append(r)
            if args.detail:
                frames.append((symbol, interval, local_df, api_df))

    path = report_path(args.symbol, start, end, args.market, args.market_sub)
    write_report(path, results, args.symbol, args.market, args.market_sub,
                 intervals, start, end)
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
