#!/usr/bin/env python3
"""verify_self.py - Self-verify symbol data under $TRADE_DATA.

For each symbol, check:
  1. kline files: row count matches the expected value for the interval and
     the covered date range.
       - full year        (e.g. BTCUSDT_kline_1h_2025.parquet)
           expected = days_in_year * 86400 / interval_seconds
       - partial year     (e.g. BTCUSDT_kline_1d_2026-04-13.parquet)
           expected = covered_days * 86400 / interval_seconds
  2. aggTrades files:
       - row count == (last_agg_id - first_agg_id + 1) for each file
       - across adjacent files: last_agg_id[i] + 1 == first_agg_id[i+1]

A report table is written to <repo>/report/verify_self/{symbols}.report.
When len(symbols) <= 3, the file name is the symbols joined by underscore
(e.g. BTCUSDT_ETHUSDT.report). Otherwise it is "{N}symbols.report".

Usage:
    python verify_self.py                    # verify all discovered symbols
    python verify_self.py -s BTCUSDT
    python verify_self.py --symbols BTCUSDT ETHUSDT
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from calendar import isleap
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq


TRADE_DATA = Path(os.environ.get("TRADE_DATA", "/ndata/trade/data"))
REPO_ROOT = Path(__file__).resolve().parent.parent
REPORT_DIR = REPO_ROOT / "report" / "verify_self"

# Markets to scan: (label, path under $TRADE_DATA that holds
# "aggTrades/{sym}/" and "aggTrades_kline/{sym}/" subtrees).
MARKETS: list[tuple[str, Path]] = [
    ("future_um", Path("binance/future/um")),
    ("spot", Path("binance/spot")),
]

INTERVAL_SECONDS: dict[str, int] = {
    "1s": 1,
    "1m": 60,
    "3m": 180,
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
    "2h": 7200,
    "4h": 14400,
    "6h": 21600,
    "8h": 28800,
    "12h": 43200,
    "1d": 86400,
}

_KLINE_RE = re.compile(
    r"_kline_(?P<interval>[0-9]+[smhd])_(?P<year>\d{4})"
    r"(?:-(?P<md>\d{2}-\d{2}))?\.parquet$"
)


@dataclass
class KlineRow:
    name: str
    interval: str
    exp: int
    count: int
    diff: int
    note: str = ""


@dataclass
class AggRow:
    name: str
    start_id: int
    end_id: int
    gap_count: int
    gaps: list[tuple[int, int, int]] = field(default_factory=list)
    note: str = ""


@dataclass
class SymbolResult:
    symbol: str
    market: str
    folders: list[str] = field(default_factory=list)
    agg_rows: list[AggRow] = field(default_factory=list)
    kline_rows: list[KlineRow] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


def expected_kline_rows(year: int, md: str | None, interval: str) -> int:
    if md is None:
        days = 366 if isleap(year) else 365
    else:
        mm, dd = (int(x) for x in md.split("-"))
        days = (date(year, mm, dd) - date(year, 1, 1)).days + 1
    return days * (86400 // INTERVAL_SECONDS[interval])


def _label(path: Path) -> str:
    return path.name


def _folder(path: Path) -> str:
    try:
        return str(path.parent.relative_to(TRADE_DATA)) + "/"
    except ValueError:
        return str(path.parent) + "/"


def check_kline(path: Path) -> KlineRow:
    m = _KLINE_RE.search(path.name)
    if m is None:
        return KlineRow(_label(path), "?", 0, 0, 0, "cannot parse name")
    interval = m.group("interval")
    if interval not in INTERVAL_SECONDS:
        return KlineRow(_label(path), interval, 0, 0, 0, "unknown interval")
    year = int(m.group("year"))
    md = m.group("md")
    try:
        count = pq.ParquetFile(path).metadata.num_rows
    except Exception as e:
        return KlineRow(_label(path), interval, 0, 0, 0,
                        f"read error: {type(e).__name__}")
    exp = expected_kline_rows(year, md, interval)
    diff = count - exp
    note = ""
    # 1s kline is derived from aggTrades; seconds with no trade produce no
    # row, so fewer rows than the theoretical max is expected and not an
    # error.
    if interval == "1s" and diff < 0:
        note = "sparse (no-trade seconds)"
    return KlineRow(_label(path), interval, exp, count, diff, note)


def check_aggtrades(path: Path) -> AggRow:
    try:
        pf = pq.ParquetFile(path)
        meta = pf.metadata
        count = meta.num_rows
    except Exception as e:
        return AggRow(_label(path), 0, 0, 0,
                      note=f"read error: {type(e).__name__}")

    col_idx = pf.schema_arrow.get_field_index("agg_trade_id")
    if col_idx < 0:
        return AggRow(_label(path), 0, 0, 0, note="no agg_trade_id column")

    first_id: int | None = None
    last_id: int | None = None
    for rg in range(meta.num_row_groups):
        stats = meta.row_group(rg).column(col_idx).statistics
        if stats is None or not stats.has_min_max:
            return AggRow(_label(path), 0, 0, 0,
                          note="row-group stats missing")
        rg_min = int(stats.min)
        rg_max = int(stats.max)
        if first_id is None or rg_min < first_id:
            first_id = rg_min
        if last_id is None or rg_max > last_id:
            last_id = rg_max

    if first_id is None or last_id is None:
        return AggRow(_label(path), 0, 0, 0, note="empty file")

    gap_count = (last_id - first_id + 1) - count
    if gap_count == 0:
        return AggRow(_label(path), first_id, last_id, 0)

    # Gaps exist: locate them. aggTrades are not guaranteed to be strictly
    # monotonic in storage order (futures interleave multiple streams),
    # so we build a bitmap indexed by (agg_id - first_id) and scan for
    # unset bits afterwards. Skip enumeration if the span would cost too
    # much memory (> ~2 GiB).
    span = last_id - first_id + 1
    if span > (2 << 30):
        return AggRow(_label(path), first_id, last_id, gap_count,
                      note=f"span too large to enumerate ({span})")

    present = np.zeros(span, dtype=np.bool_)
    for rg in range(meta.num_row_groups):
        arr = (pf.read_row_group(rg, columns=["agg_trade_id"])
                 .column("agg_trade_id")
                 .to_numpy(zero_copy_only=False))
        if arr.size == 0:
            continue
        present[arr - first_id] = True

    missing = np.where(~present)[0]
    gaps: list[tuple[int, int, int]] = []
    if missing.size:
        breaks = np.where(np.diff(missing) > 1)[0]
        starts = np.concatenate([[0], breaks + 1])
        ends = np.concatenate([breaks, [missing.size - 1]])
        for s, e in zip(starts, ends):
            lo = int(missing[s] + first_id)
            hi = int(missing[e] + first_id)
            miss = hi - lo + 1
            gaps.append((lo - 1, hi + 1, miss))

    return AggRow(_label(path), first_id, last_id, gap_count, gaps)


def check_symbol(market: str, market_root: Path, symbol: str) -> SymbolResult:
    res = SymbolResult(symbol=symbol, market=market)

    agg_dir = market_root / "aggTrades" / symbol
    if agg_dir.is_dir():
        agg_files = sorted(agg_dir.glob(f"{symbol}_aggTrades_*.parquet"))
        if agg_files:
            res.folders.append(_folder(agg_files[0]))
        for f in agg_files:
            res.agg_rows.append(check_aggtrades(f))

        for i in range(1, len(res.agg_rows)):
            prev = res.agg_rows[i - 1]
            cur = res.agg_rows[i]
            if prev.note or cur.note:
                continue
            if cur.start_id != prev.end_id + 1:
                delta = cur.start_id - prev.end_id - 1
                res.errors.append(
                    f"agg_id gap between {prev.name} (end={prev.end_id}) "
                    f"and {cur.name} (start={cur.start_id}): missing={delta}"
                )

    kline_dir = market_root / "aggTrades_kline" / symbol
    if kline_dir.is_dir():
        kline_files = sorted(kline_dir.glob(f"{symbol}_kline_*.parquet"))
        if kline_files:
            res.folders.append(_folder(kline_files[0]))
        klines = [check_kline(f) for f in kline_files]
        klines.sort(key=lambda r: (
            INTERVAL_SECONDS.get(r.interval, 1 << 31),
            r.name,
        ))
        res.kline_rows.extend(klines)

    return res


def discover_symbols() -> dict[str, set[str]]:
    out: dict[str, set[str]] = {}
    for market, sub in MARKETS:
        root = TRADE_DATA / sub
        syms: set[str] = set()
        for kind in ("aggTrades", "aggTrades_kline"):
            d = root / kind
            if not d.is_dir():
                continue
            for p in d.iterdir():
                if p.is_dir() and not p.name.startswith("_"):
                    syms.add(p.name)
        out[market] = syms
    return out


def _render_agg_table(rows: list[AggRow]) -> list[str]:
    if not rows:
        return []
    name_w = max(len("parquet name"), max(len(r.name) for r in rows))
    header = (f"{'parquet name':<{name_w}}  {'start id':>14}  "
              f"{'end id':>14}  {'gap count':>12}")
    sep = "-" * len(header)
    out = ["[aggTrades]", header, sep]
    for r in rows:
        if r.note:
            out.append(f"{r.name:<{name_w}}  {'':>14}  {'':>14}  "
                       f"{'':>12}  {r.note}")
            continue
        out.append(
            f"{r.name:<{name_w}}  {r.start_id:>14}  "
            f"{r.end_id:>14}  {r.gap_count:>12}"
        )
        for prev, nxt, miss in r.gaps:
            out.append(f"    gap: {prev} -> {nxt} (missing {miss})")
    return out


def _render_kline_table(rows: list[KlineRow]) -> list[str]:
    if not rows:
        return []
    name_w = max(len("parquet name"), max(len(r.name) for r in rows))
    header = (f"{'parquet name':<{name_w}}  {'interval':>8}  "
              f"{'expect':>12}  {'count':>12}  {'diff':>12}")
    sep = "-" * len(header)
    out = ["[kline]", header, sep]
    for r in rows:
        note = f"  {r.note}" if r.note else ""
        out.append(
            f"{r.name:<{name_w}}  {r.interval:>8}  "
            f"{r.exp:>12}  {r.count:>12}  {r.diff:>12}{note}"
        )
    return out


def _report_filename(symbols: list[str]) -> str:
    if len(symbols) <= 3:
        return "_".join(symbols) + ".report"
    return f"{len(symbols)}symbols.report"


def write_report(symbols: list[str], results: list[SymbolResult]) -> Path:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    path = REPORT_DIR / _report_filename(symbols)

    total_files = sum(
        len(r.agg_rows) + len(r.kline_rows) for r in results
    )
    bad_agg = sum(
        1 for r in results for row in r.agg_rows
        if row.gap_count != 0 or row.note
    )
    bad_kline = sum(
        1 for r in results for row in r.kline_rows
        if (row.diff != 0 or row.note)
        and not row.note.startswith("sparse")
    )
    bad_files = bad_agg + bad_kline
    total_errors = sum(len(r.errors) for r in results)

    lines: list[str] = []
    lines.append("verify_self report")
    lines.append(f"trade_data : {TRADE_DATA}")
    if len(symbols) <= 3:
        lines.append(f"symbols    : {', '.join(symbols)}")
    else:
        lines.append(f"symbols    : {len(symbols)} symbols")
    lines.append(
        f"files      : {total_files} checked, {bad_files} bad, "
        f"{total_errors} cross-file error(s)"
    )
    lines.append("")

    for res in results:
        if not res.agg_rows and not res.kline_rows and not res.errors:
            continue
        lines.append(f"## {res.symbol} [{res.market}]")
        for folder in res.folders:
            lines.append(f"  {folder}")
        lines.append("")
        if res.agg_rows:
            lines.extend(_render_agg_table(res.agg_rows))
            lines.append("")
        if res.kline_rows:
            lines.extend(_render_kline_table(res.kline_rows))
            lines.append("")
        for err in res.errors:
            lines.append(f"  !! {err}")
        if res.errors:
            lines.append("")

    path.write_text("\n".join(lines) + "\n")
    return path


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("-s", "--symbols", nargs="+",
                    help="symbols to verify (default: all)")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    by_market = discover_symbols()
    if not any(by_market.values()):
        print(f"no data found under {TRADE_DATA}", file=sys.stderr)
        return 2

    if args.symbols:
        requested = set(args.symbols)
        by_market = {m: s & requested for m, s in by_market.items()}

    all_syms = sorted({s for syms in by_market.values() for s in syms})
    if not all_syms:
        print("no matching symbols", file=sys.stderr)
        return 2

    results: list[SymbolResult] = []
    for sym in all_syms:
        for market, sub in MARKETS:
            if sym in by_market.get(market, set()):
                if args.verbose:
                    print(f"checking {sym} [{market}]", file=sys.stderr)
                results.append(check_symbol(market, TRADE_DATA / sub, sym))

    out = write_report(all_syms, results)
    print(f"wrote {out}")

    bad = (
        any(row.gap_count != 0 or row.note
            for r in results for row in r.agg_rows)
        or any((row.diff != 0 or row.note)
               and not row.note.startswith("sparse")
               for r in results for row in r.kline_rows)
        or any(r.errors for r in results)
    )
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
