#!/usr/bin/env python3
"""verify_all_symbols_id.py - Verify aggTrades id-continuity for every symbol.

For each parquet under ``$TRADE_DATA/<exchange>/<market-path>/aggTrades/<sym>/``,
compute:
  start_id       = min(agg_trade_id)
  end_id         = max(agg_trade_id)
  gap_count      = (end_id - start_id + 1) - row_count
  start_datetime = min(time)
  end_datetime   = max(time)

A non-zero ``gap_count`` means at least one ``agg_trade_id`` between
``start_id`` and ``end_id`` is missing from the file. Note for Gate markets
the global id stream resets historically (see verify_self.py); this script
still computes the per-file gap so the operator can see the raw numbers.

Every symbol under the chosen (exchange, market) is verified. Symbols are
grouped in the report by the year of their earliest aggTrades datetime
(e.g. a symbol whose first parquet starts on 2024-01-01 is a "2024 new
symbol"); within each year they are sorted by that earliest datetime.

The report is written to
``report/verify_all_symbols_id/{exchange}_{market}.{UTC-timestamp}.report``
so each run produces a fresh artifact.

Usage:
    python verify_all_symbols_id.py --exchange binance --market spot
    python verify_all_symbols_id.py --exchange gate    --market um
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pyarrow.parquet as pq


TRADE_DATA = Path(os.environ.get("TRADE_DATA", "/ndata/trade/data"))
REPO_ROOT = Path(__file__).resolve().parent.parent
REPORT_DIR = REPO_ROOT / "report" / "verify_all_symbols_id"

# (exchange, market) -> sub-path under $TRADE_DATA
MARKET_PATHS: dict[tuple[str, str], Path] = {
    ("binance", "spot"): Path("binance/spot"),
    ("binance", "um"):   Path("binance/future/um"),
    ("gate",    "spot"): Path("gate/spot"),
    ("gate",    "um"):   Path("gate/future/um"),
}


@dataclass
class FileRow:
    name: str
    count: int = 0  # row count (len)
    start_id: int = 0
    end_id: int = 0
    gap_count: int = 0
    start_dt: datetime | None = None
    end_dt: datetime | None = None
    note: str = ""


@dataclass
class SymbolResult:
    symbol: str
    rows: list[FileRow] = field(default_factory=list)


def _stat_min_max(meta, col_idx: int) -> tuple[object, object] | None:
    """Reduce min/max across all row groups for one column."""
    lo = hi = None
    for rg in range(meta.num_row_groups):
        s = meta.row_group(rg).column(col_idx).statistics
        if s is None or not s.has_min_max:
            return None
        rg_min, rg_max = s.min, s.max
        if lo is None or rg_min < lo:
            lo = rg_min
        if hi is None or rg_max > hi:
            hi = rg_max
    return (lo, hi) if lo is not None else None


def check_file(path: Path) -> FileRow:
    try:
        pf = pq.ParquetFile(path)
        meta = pf.metadata
        count = meta.num_rows
    except Exception as e:
        return FileRow(path.name, note=f"read error: {type(e).__name__}")

    sch = pf.schema_arrow
    id_idx = sch.get_field_index("agg_trade_id")
    if id_idx < 0:
        return FileRow(path.name, note="no agg_trade_id column")
    if count == 0:
        return FileRow(path.name, note="empty file")

    id_mm = _stat_min_max(meta, id_idx)
    if id_mm is None:
        return FileRow(path.name, note="agg_trade_id stats missing")
    start_id, end_id = int(id_mm[0]), int(id_mm[1])
    gap_count = (end_id - start_id + 1) - count

    start_dt = end_dt = None
    time_idx = sch.get_field_index("time")
    if time_idx >= 0:
        t_mm = _stat_min_max(meta, time_idx)
        if t_mm is not None:
            start_dt, end_dt = t_mm  # already datetime objects

    return FileRow(
        name=path.name,
        count=count,
        start_id=start_id,
        end_id=end_id,
        gap_count=gap_count,
        start_dt=start_dt,
        end_dt=end_dt,
    )


def discover_symbols(market_root: Path) -> list[str]:
    agg_root = market_root / "aggTrades"
    if not agg_root.is_dir():
        return []
    syms = [
        p.name for p in agg_root.iterdir()
        if p.is_dir() and not p.name.startswith("_")
    ]
    return sorted(syms)


def check_symbol(market_root: Path, symbol: str) -> SymbolResult:
    res = SymbolResult(symbol=symbol)
    agg_dir = market_root / "aggTrades" / symbol
    if not agg_dir.is_dir():
        return res
    files = sorted(agg_dir.glob(f"{symbol}_aggTrades_*.parquet"))
    for f in files:
        res.rows.append(check_file(f))
    return res


def _fmt_dt(dt: datetime | None) -> str:
    if dt is None:
        return "-"
    # datetime from parquet stats may carry microseconds; trim to seconds.
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--exchange", required=True,
                    choices=sorted({e for e, _ in MARKET_PATHS}))
    ap.add_argument("--market", required=True,
                    choices=sorted({m for _, m in MARKET_PATHS}))
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()
    return run(args.exchange, args.market, args.verbose)


def run(exchange: str, market: str, verbose: bool = False) -> int:
    """Programmatic entry point. Returns the same exit code as ``main()``."""
    key = (exchange, market)
    if key not in MARKET_PATHS:
        print(f"unsupported (exchange, market): {key}", file=sys.stderr)
        return 2
    market_root = TRADE_DATA / MARKET_PATHS[key]
    if not market_root.is_dir():
        print(f"market root not found: {market_root}", file=sys.stderr)
        return 2

    symbols = discover_symbols(market_root)
    if not symbols:
        print(f"no symbols under {market_root / 'aggTrades'}", file=sys.stderr)
        return 2

    results: list[SymbolResult] = []
    for sym in symbols:
        if verbose:
            print(f"checking {sym}", file=sys.stderr)
        results.append(check_symbol(market_root, sym))

    total_files = sum(len(r.rows) for r in results)
    bad_entries: list[tuple[str, FileRow]] = [
        (r.symbol, row)
        for r in results for row in r.rows
        if row.gap_count != 0 or row.note
    ]

    lines: list[str] = []
    lines.append("verify_all_symbols_id report")
    lines.append(f"trade_data : {TRADE_DATA}")
    lines.append(f"exchange   : {exchange}")
    lines.append(f"market     : {market}")
    lines.append(f"market root: {market_root}")
    lines.append(f"symbols    : {len(symbols)}")
    lines.append(f"files      : {total_files} checked, {len(bad_entries)} bad")
    lines.append("")

    # Summary: every parquet whose gap_count != 0 (or carries a fatal note).
    lines.append("# summary (gap_count != 0)")
    if not bad_entries:
        lines.append("  (none)")
    else:
        sym_w = max(len("symbol"),
                    max(len(s) for s, _ in bad_entries))
        name_w = max(len("parquet name"),
                     max(len(r.name) for _, r in bad_entries))
        head = (f"{'symbol':<{sym_w}}  {'parquet name':<{name_w}}  "
                f"{'start id':>14}  {'end id':>14}  {'gap count':>12}")
        lines.append(head)
        lines.append("-" * len(head))
        for sym, r in bad_entries:
            note = f"  {r.note}" if r.note else ""
            if r.note and r.start_id == 0 and r.end_id == 0:
                lines.append(f"{sym:<{sym_w}}  {r.name:<{name_w}}  {r.note}")
            else:
                lines.append(
                    f"{sym:<{sym_w}}  {r.name:<{name_w}}  "
                    f"{r.start_id:>14}  {r.end_id:>14}  "
                    f"{r.gap_count:>12}{note}"
                )
    lines.append("")

    # Helpers for the year-bucket and per-symbol summaries below.
    def _earliest_row(res: SymbolResult) -> FileRow | None:
        candidates = [r for r in res.rows if r.start_dt is not None]
        return min(candidates, key=lambda r: r.start_dt) if candidates else None

    earliest_by_symbol: dict[str, FileRow | None] = {
        res.symbol: _earliest_row(res) for res in results
    }

    # Group symbols by the year of their earliest start datetime; within each
    # year sort by that earliest datetime ascending. Years rendered newest
    # first. Placed up here, right after the gap_count summary, as a quick
    # at-a-glance roster of when each symbol's data starts.
    by_year: dict[int | None, list[tuple[datetime | None, SymbolResult]]] = {}
    for res in results:
        er = earliest_by_symbol[res.symbol]
        edt = er.start_dt if er is not None else None
        year = edt.year if edt is not None else None
        by_year.setdefault(year, []).append((edt, res))

    def _year_key(y: int | None) -> tuple[int, int]:
        # None (unknown start) last; real years descending so newest first.
        return (1, 0) if y is None else (0, -y)

    for year in sorted(by_year, key=_year_key):
        bucket = sorted(
            by_year[year],
            key=lambda pair: (pair[0] is None, pair[0] or datetime.max,
                              pair[1].symbol),
        )
        title = (f"# {year} new symbols ({len(bucket)})"
                 if year is not None
                 else f"# unknown start ({len(bucket)})")
        lines.append(title)
        lines.append("-" * len(title))
        lines.append(", ".join(res.symbol for _edt, res in bucket))
        lines.append("")

    # Summary: symbols whose first parquet's start_id != 0. For binance the
    # agg_trade_id is per-symbol, so non-zero here means we did not collect
    # from the first trade of the symbol. (Gate ids are not contiguous, so
    # this column is informational rather than diagnostic for that exchange.)
    not_zero = [
        (res.symbol, earliest_by_symbol[res.symbol])
        for res in results
        if earliest_by_symbol[res.symbol] is not None
        and earliest_by_symbol[res.symbol].start_id != 0
    ]
    lines.append("# summary - start id not 0 symbols")
    if not not_zero:
        lines.append("  (none)")
    else:
        sym_w = max(len("symbol"), max(len(s) for s, _ in not_zero))
        name_w = max(len("parquet name"),
                     max(len(r.name) for _, r in not_zero))
        head = (f"{'symbol':<{sym_w}}  {'parquet name':<{name_w}}  "
                f"{'start id':>14}  {'start datetime':>19}")
        lines.append(head)
        lines.append("-" * len(head))
        for sym, r in sorted(not_zero, key=lambda p: p[0]):
            lines.append(
                f"{sym:<{sym_w}}  {r.name:<{name_w}}  "
                f"{r.start_id:>14}  {_fmt_dt(r.start_dt):>19}"
            )
    lines.append("")

    # Per-parquet length buckets. "len" is the parquet's row count
    # (meta.num_rows). Buckets in the requested order: <=10M, 10M..100M,
    # 100M..1B, >1B. Each section lists every parquet in that bucket sorted
    # by len descending so the heaviest files are easy to spot.
    M = 1_000_000
    B = 1_000_000_000
    buckets: list[tuple[str, callable]] = [
        ("len <= 10M",         lambda c: c <= 10 * M),
        ("10M < len <= 100M",  lambda c: 10 * M  < c <= 100 * M),
        ("100M < len <= 1B",   lambda c: 100 * M < c <= B),
        ("len > 1B",           lambda c: c > B),
    ]
    all_pairs: list[tuple[str, FileRow]] = [
        (res.symbol, r)
        for res in results for r in res.rows
        if r.note == "" or r.count > 0  # skip pure read-error sentinels
    ]

    def _fmt_len(n: int) -> str:
        return f"{n:,}"

    for label, pred in buckets:
        bucket_rows = [(s, r) for s, r in all_pairs if pred(r.count)]
        title = f"# summary - {label} ({len(bucket_rows)})"
        lines.append(title)
        if not bucket_rows:
            lines.append("  (none)")
            lines.append("")
            continue
        bucket_rows.sort(key=lambda p: p[1].count, reverse=True)
        sym_w = max(len("symbol"), max(len(s) for s, _ in bucket_rows))
        name_w = max(len("parquet name"),
                     max(len(r.name) for _, r in bucket_rows))
        len_w = max(len("len"),
                    max(len(_fmt_len(r.count)) for _, r in bucket_rows))
        head = (f"{'symbol':<{sym_w}}  {'parquet name':<{name_w}}  "
                f"{'len':>{len_w}}  "
                f"{'start datetime':>19}  {'end datetime':>19}")
        lines.append(head)
        lines.append("-" * len(head))
        for sym, r in bucket_rows:
            lines.append(
                f"{sym:<{sym_w}}  {r.name:<{name_w}}  "
                f"{_fmt_len(r.count):>{len_w}}  "
                f"{_fmt_dt(r.start_dt):>19}  {_fmt_dt(r.end_dt):>19}"
            )
        lines.append("")

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(ZoneInfo("UTC")).strftime("%Y%m%d_%H%M%S")
    out_path = REPORT_DIR / f"{exchange}_{market}.{stamp}.report"
    out_path.write_text("\n".join(lines) + "\n")

    print(f"wrote {out_path}")
    print()
    print(out_path.read_text(), end="")
    return 1 if bad_entries else 0


if __name__ == "__main__":
    sys.exit(main())
