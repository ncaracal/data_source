#!/usr/bin/env python3
"""Verify local Gate spot kline parquet data against the Gate REST kline API.

For each (symbol, interval) combination, fetch klines from Gate for the
requested date range and compare OHLC + volume against the local aggregated
parquet. Writes a summary table under report/verify_with_gate_kline_api/.

API used:
    spot : https://api.gateio.ws/api/v4/spot/candlesticks

Usage:
    python verify_with_gate_kline_api.py
    python verify_with_gate_kline_api.py -s BUSDT \\
        --start-date 2026-04-01 --end-date 2026-04-14 -i 1d 1h
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
REPORT_DIR = REPO_ROOT / "report" / "verify_with_gate_kline_api"

# Intervals supported by Gate's spot candlesticks endpoint.
# Notably MISSING from Gate (vs Binance): 1s, 3m, 12h. Gate uniquely supports 10s.
GATE_INTERVALS: tuple[str, ...] = (
    "1m", "5m", "15m", "30m", "1h", "4h", "8h", "1d",
)

ALL_INTERVALS: tuple[str, ...] = (
    "1s", "1m", "3m", "5m", "15m", "30m", "1h", "4h", "8h", "12h", "1d",
)

INTERVAL_SECONDS: dict[str, int] = {
    "1s": 1,
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
    "4h": 14400,
    "8h": 28800,
    "1d": 86400,
}

API_URL = "https://api.gateio.ws/api/v4/spot/candlesticks"
# Gate caps `(to - from) / interval` at 1000 *inclusive*, i.e. up to 1001 bars.
# We page in chunks of 999 so a single off-by-one rounding never trips it.
API_PAGE_LIMIT = 999

# Gate uses USDT-quoted pairs as `BASE_USDT`. The CLI accepts the binance-style
# `BUSDT` form for consistency; convert here.
_KNOWN_QUOTES = ("USDT", "USDC", "TUSD", "BUSD", "FDUSD", "DAI",
                 "USD", "BTC", "ETH", "BNB", "TRY", "EUR")


def to_gate_pair(symbol: str) -> str:
    if "_" in symbol:
        return symbol
    for q in _KNOWN_QUOTES:
        if symbol.endswith(q) and len(symbol) > len(q):
            return f"{symbol[:-len(q)]}_{q}"
    return symbol


def filter_supported_intervals(intervals: Iterable[str]) -> list[str]:
    """Drop intervals the Gate API doesn't expose (1s, 3m, 12h)."""
    supported = set(GATE_INTERVALS)
    return [i for i in intervals if i in supported]


def kline_dir(symbol: str) -> Path:
    return TRADE_DATA / "gate" / "spot" / "aggTrades_kline" / symbol


def load_local_klines(symbol: str, interval: str,
                      start_ms: int, end_ms: int) -> pd.DataFrame:
    dir_ = kline_dir(symbol)
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


def _http_get_json(url: str, max_retries: int = 6) -> object:
    """GET a JSON URL with retry/backoff. Returns the parsed payload."""
    req = urllib.request.Request(url, headers={"User-Agent": "verify_kline/1.0"})
    backoff = 1.0
    for attempt in range(max_retries):
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as e:
            code = e.code
            body_bytes = b""
            try:
                body_bytes = e.read()
            except Exception:
                pass
            body_text = body_bytes[:300].decode("utf-8", "replace")
            if code == 429:
                wait = max(backoff * 4, 1.0)
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


def fetch_gate_klines(symbol: str, interval: str,
                      start_ms: int, end_ms: int,
                      throttle_s: float = 0.25) -> tuple[pd.DataFrame, int]:
    """Fetch klines from Gate in `[start_ms, end_ms]` (inclusive), paginated.

    Gate response row layout (each element is a string):
        [open_time_sec, quote_volume, close, high, low, open, base_volume, window_closed]

    Gate caps lookback at 10000 bars from now (server-side). For short
    intervals (1m, 5m), this can exclude the requested start; we clamp
    start to `now - 10000*interval` and emit a single warning.
    """
    pair = to_gate_pair(symbol)
    interval_s = INTERVAL_SECONDS[interval]
    page_span_s = API_PAGE_LIMIT * interval_s

    now_s = int(datetime.now(timezone.utc).timestamp())
    # Use 9990 (not 10000) bars to leave ~10-bar slack for clock drift
    # between client and server; otherwise the boundary call rejects.
    earliest_supported_s = now_s - 9990 * interval_s
    requested_start_s = start_ms // 1000
    if requested_start_s < earliest_supported_s:
        clamped_start = datetime.fromtimestamp(earliest_supported_s, tz=timezone.utc)
        print(
            f"[warn] {symbol} {interval}: Gate API caps lookback at 10000 "
            f"bars; clamping start to {clamped_start.isoformat()}",
            file=sys.stderr,
        )
        start_ms = earliest_supported_s * 1000

    rows: list[list[str]] = []
    cursor_s = start_ms // 1000
    end_s = end_ms // 1000

    while cursor_s <= end_s:
        # Gate expects `to` <= now and `from < to`. Clamp `to` per page so
        # each call returns at most API_PAGE_LIMIT bars.
        page_end_s = min(cursor_s + page_span_s - 1, end_s)
        params = {
            "currency_pair": pair,
            "interval": interval,
            "from": cursor_s,
            "to": page_end_s,
            "limit": API_PAGE_LIMIT,
        }
        url = f"{API_URL}?{urllib.parse.urlencode(params)}"
        batch = _http_get_json(url)
        if not batch:
            cursor_s = page_end_s + 1
            continue
        rows.extend(batch)
        last_open_s = int(batch[-1][0])
        next_cursor = last_open_s + interval_s
        if next_cursor <= cursor_s:
            break
        cursor_s = next_cursor
        time.sleep(throttle_s)

    cols = ["time", "open", "high", "low", "close", "qty", "qty_usd"]
    if not rows:
        return pd.DataFrame(columns=cols), start_ms

    df = pd.DataFrame(rows, columns=[
        "open_time_s", "quote_volume", "close", "high", "low", "open",
        "base_volume", "window_closed",
    ])
    df["time"] = pd.to_datetime(df["open_time_s"].astype("int64"), unit="s")
    for c in ("open", "high", "low", "close", "base_volume", "quote_volume"):
        df[c] = df[c].astype(float)
    df = df.rename(columns={"base_volume": "qty", "quote_volume": "qty_usd"})
    df = df[cols]
    df = df.drop_duplicates(subset="time").sort_values("time").reset_index(drop=True)
    return df, start_ms


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
    # Some exchanges emit phantom zero-qty bars where no trades occurred —
    # don't count those as missing on the local side.
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


def verify_one(symbol: str, interval: str,
               start_ms: int, end_ms: int,
               throttle_s: float = 0.25
               ) -> tuple[CompareResult, pd.DataFrame, pd.DataFrame]:
    try:
        api_df, effective_start_ms = fetch_gate_klines(
            symbol, interval, start_ms, end_ms, throttle_s
        )
    except Exception as e:
        print(f"[warn] {symbol} {interval}: API fetch failed: {e}", file=sys.stderr)
        return (
            CompareResult(symbol, interval, 0, 0, 0, 0, 0,
                          0.0, 0.0, 0.0, 0.0, "SKIP"),
            _empty_df(),
            _empty_df(),
        )
    # Use the same window the API actually returned so local bars older
    # than `effective_start_ms` (e.g. when the 10000-bar lookback cap kicks
    # in) don't show up as "missing on API".
    local_df = load_local_klines(symbol, interval, effective_start_ms, end_ms)
    return compare(symbol, interval, local_df, api_df), local_df, api_df


def format_detail_table(symbol: str, interval: str,
                        local_df: pd.DataFrame, api_df: pd.DataFrame,
                        max_rows: int = 50) -> str:
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


def report_path(symbols: Sequence[str], start: date, end: date) -> Path:
    syms_token = symbols[0] if len(symbols) < 2 else f"{len(symbols)}symbols"
    date_token = start.isoformat() if start == end else f"{start.isoformat()}_{end.isoformat()}"
    return REPORT_DIR / f"{syms_token}_spot_{date_token}.report"


def write_report(path: Path, results: list[CompareResult],
                 symbols: Sequence[str],
                 intervals: Sequence[str], start: date, end: date) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    date_line = start.isoformat() if start == end else f"{start.isoformat()} .. {end.isoformat()}"
    passes = sum(1 for r in results if r.status == "PASS")
    warns = sum(1 for r in results if r.status == "WARN")
    fails = sum(1 for r in results if r.status == "FAIL")
    skips = sum(1 for r in results if r.status == "SKIP")
    header = [
        "Gate kline API verification",
        f"Generated : {datetime.now(timezone.utc).isoformat(timespec='seconds')}",
        f"Market    : spot",
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
    p.add_argument("-s", "--symbol", nargs="+", default=["BUSDT"])
    p.add_argument("--start-date", type=_parse_date,
                   help="UTC start date YYYY-MM-DD (default: yesterday UTC)")
    p.add_argument("--end-date", type=_parse_date,
                   help="UTC end date YYYY-MM-DD (default: start-date)")
    p.add_argument("-i", "--interval", nargs="+", default=list(GATE_INTERVALS),
                   help=f"kline intervals (default: {' '.join(GATE_INTERVALS)})")
    p.add_argument("--detail", action="store_true",
                   help="print per-bar OHLC diff table for each interval")
    p.add_argument("--detail-max-rows", type=int, default=50,
                   help="max rows per detail table (default 50)")
    args = p.parse_args()

    start = args.start_date or _default_date()
    end = args.end_date or start
    if start > end:
        raise SystemExit("start-date must be <= end-date")

    intervals = filter_supported_intervals(args.interval)
    dropped = [i for i in args.interval if i not in intervals]
    if dropped:
        print(f"[info] dropping {','.join(dropped)} — Gate spot kline API "
              f"only supports {','.join(GATE_INTERVALS)}", file=sys.stderr)

    start_ms, end_ms = date_range_ms(start, end)
    results: list[CompareResult] = []
    frames: list[tuple[str, str, pd.DataFrame, pd.DataFrame]] = []
    for symbol in args.symbol:
        for interval in intervals:
            r, local_df, api_df = verify_one(
                symbol, interval, start_ms, end_ms,
            )
            results.append(r)
            if args.detail:
                frames.append((symbol, interval, local_df, api_df))

    path = report_path(args.symbol, start, end)
    write_report(path, results, args.symbol, intervals, start, end)
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
