#!/usr/bin/env python3
"""verify_depth.py - Verify depth-stream integrity for every symbol.

For each parquet under
``<depth-root>/<symbol>/<symbol>_binance_depth_<YYYY-MM-DD>(.<suffix>)?.parquet``
this script reads parquet metadata (no full data scan) to compute:

  rows           = meta.num_rows
  first_uid_min  = min(first_update_id)
  final_uid_max  = max(final_update_id)
  time_min       = min(time)
  time_max       = max(time)

The Binance depth-diff stream advertises a global update-id chain per symbol:
each event carries ``[U, u] = [first_update_id, final_update_id]`` and the
next event must satisfy ``U_next == u_prev + 1``. Files within a symbol are
sorted by (filename-date, suffix) so that the date-only "daily" parquet is
last for each date (it is the most recently flushed chunk). The verifier then
walks the per-symbol file chain and flags any pair where
``next.first_uid_min != prev.final_uid_max + 1`` — that gap_uid is the count
of update-ids that fall between two adjacent files but are not stored
anywhere in this dataset (or are stored out-of-order).

Per-file sanity checks reported:
  - read errors / missing columns / empty files
  - inverted ranges (first_uid_min > final_uid_max, time_min > time_max)

The report is written to
``report/verify_depth/{exchange}_{market}.{UTC-timestamp}.report``
so each run produces a fresh artifact.

Usage:
    python verify_depth.py
    python verify_depth.py --depth-root /mnt/sharpe-data/binance/spot/depth
    python verify_depth.py --symbol AAVEUSDT --workers 8
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import os
import re
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pyarrow.parquet as pq


REPO_ROOT = Path(__file__).resolve().parent.parent.parent
REPORT_DIR = REPO_ROOT / "report" / "verify_depth"

DEFAULT_DEPTH_ROOT = Path("/mnt/sharpe-data/binance/spot/depth")

# Filename: <SYMBOL>_<exchange>_depth_<YYYY-MM-DD>(.<suffix>)?.parquet
# SYMBOL may contain non-ASCII chars (e.g. 币安人生USDT) so we accept anything
# that does not contain an underscore.
FILENAME_RE = re.compile(
    r"^(?P<symbol>[^_]+)_(?P<exchange>[^_]+)_depth_"
    r"(?P<date>\d{4}-\d{2}-\d{2})(?:\.(?P<suffix>\d+))?\.parquet$"
)


@dataclass
class FileRow:
    path: Path
    name: str
    file_date: date | None = None
    suffix: int | None = None  # None means the "daily" (date-only) file
    rows: int = 0
    first_uid_min: int = 0
    final_uid_max: int = 0
    time_min: datetime | None = None
    time_max: datetime | None = None
    note: str = ""

    @property
    def sort_key(self) -> tuple[date, int]:
        # Within a date, hourly files (with smaller numeric suffix) come
        # first; the date-only "daily" parquet is the final flushed chunk
        # so it sorts last (suffix=inf).
        d = self.file_date or date.min
        s = self.suffix if self.suffix is not None else (1 << 62)
        return (d, s)


@dataclass
class SymbolResult:
    symbol: str
    rows: list[FileRow] = field(default_factory=list)
    gaps: list[tuple[FileRow, FileRow, int]] = field(default_factory=list)


def _stat_min_max(meta, col_idx: int):
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
    name = path.name
    m = FILENAME_RE.match(name)
    file_date = None
    suffix = None
    if m:
        try:
            file_date = date.fromisoformat(m.group("date"))
        except ValueError:
            file_date = None
        if m.group("suffix") is not None:
            try:
                suffix = int(m.group("suffix"))
            except ValueError:
                suffix = None

    try:
        pf = pq.ParquetFile(path)
        meta = pf.metadata
        rows = meta.num_rows
    except Exception as e:
        return FileRow(path=path, name=name, file_date=file_date,
                       suffix=suffix,
                       note=f"read error: {type(e).__name__}")

    sch = pf.schema_arrow
    first_idx = sch.get_field_index("first_update_id")
    final_idx = sch.get_field_index("final_update_id")
    time_idx = sch.get_field_index("time")
    missing = [c for c, i in [
        ("first_update_id", first_idx),
        ("final_update_id", final_idx),
        ("time", time_idx)] if i < 0]
    if missing:
        return FileRow(path=path, name=name, file_date=file_date,
                       suffix=suffix, rows=rows,
                       note=f"missing column(s): {','.join(missing)}")
    if rows == 0:
        return FileRow(path=path, name=name, file_date=file_date,
                       suffix=suffix, note="empty file")

    first_mm = _stat_min_max(meta, first_idx)
    final_mm = _stat_min_max(meta, final_idx)
    time_mm = _stat_min_max(meta, time_idx)
    if first_mm is None or final_mm is None:
        return FileRow(path=path, name=name, file_date=file_date,
                       suffix=suffix, rows=rows,
                       note="update_id stats missing")

    fr = FileRow(
        path=path,
        name=name,
        file_date=file_date,
        suffix=suffix,
        rows=rows,
        first_uid_min=int(first_mm[0]),
        final_uid_max=int(final_mm[1]),
        time_min=time_mm[0] if time_mm else None,
        time_max=time_mm[1] if time_mm else None,
    )
    notes = []
    if fr.first_uid_min > fr.final_uid_max:
        notes.append("first_uid_min > final_uid_max")
    if (fr.time_min is not None and fr.time_max is not None
            and fr.time_min > fr.time_max):
        notes.append("time_min > time_max")
    if notes:
        fr.note = "; ".join(notes)
    return fr


def discover_symbols(depth_root: Path) -> list[str]:
    if not depth_root.is_dir():
        return []
    return sorted(
        p.name for p in depth_root.iterdir()
        if p.is_dir() and not p.name.startswith("_")
    )


def check_symbol(depth_root: Path, symbol: str,
                 file_workers: int = 8) -> SymbolResult:
    res = SymbolResult(symbol=symbol)
    sym_dir = depth_root / symbol
    if not sym_dir.is_dir():
        return res
    files = [p for p in sym_dir.iterdir() if p.is_file()
             and p.name.endswith(".parquet")]
    if not files:
        return res
    # Parallel metadata reads inside the symbol — NFS-friendly.
    with cf.ThreadPoolExecutor(max_workers=file_workers) as ex:
        rows = list(ex.map(check_file, files))
    rows.sort(key=lambda r: r.sort_key)
    res.rows = rows

    # Walk the chain and record gaps.
    prev = None
    for r in rows:
        if r.note or r.rows == 0:
            prev = None  # restart chain past unreadable / empty files
            continue
        if prev is not None:
            expected = prev.final_uid_max + 1
            if r.first_uid_min != expected:
                gap = r.first_uid_min - expected
                res.gaps.append((prev, r, gap))
        prev = r
    return res


def _fmt_dt(dt: datetime | None) -> str:
    if dt is None:
        return "-"
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _fmt_d(d: date | None) -> str:
    return "-" if d is None else d.isoformat()


def _fmt_n(n: int) -> str:
    return f"{n:,}"


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--depth-root", type=Path, default=DEFAULT_DEPTH_ROOT,
                    help=f"depth root (default: {DEFAULT_DEPTH_ROOT})")
    ap.add_argument("--exchange", default="binance",
                    help="label used in the report filename (default: binance)")
    ap.add_argument("--market", default="spot",
                    help="label used in the report filename (default: spot)")
    ap.add_argument("--symbol", default=None,
                    help="restrict to a single symbol (debug)")
    ap.add_argument("--workers", type=int, default=8,
                    help="symbol-level parallelism (default: 8)")
    ap.add_argument("--file-workers", type=int, default=8,
                    help="file-level parallelism inside a symbol (default: 8)")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    depth_root = args.depth_root
    if not depth_root.is_dir():
        print(f"depth root not found: {depth_root}", file=sys.stderr)
        return 2

    if args.symbol:
        symbols = [args.symbol]
    else:
        symbols = discover_symbols(depth_root)
    if not symbols:
        print(f"no symbols under {depth_root}", file=sys.stderr)
        return 2

    results: list[SymbolResult] = []
    done = 0

    def _work(sym: str) -> SymbolResult:
        return check_symbol(depth_root, sym, file_workers=args.file_workers)

    with cf.ThreadPoolExecutor(max_workers=args.workers) as ex:
        for res in ex.map(_work, symbols):
            results.append(res)
            done += 1
            if args.verbose:
                print(f"[{done}/{len(symbols)}] {res.symbol}: "
                      f"files={len(res.rows)} gaps={len(res.gaps)}",
                      file=sys.stderr)

    results.sort(key=lambda r: r.symbol)

    total_files = sum(len(r.rows) for r in results)
    total_rows_scanned = sum(row.rows for r in results for row in r.rows)
    bad_file_entries: list[tuple[str, FileRow]] = [
        (r.symbol, row)
        for r in results for row in r.rows if row.note
    ]
    total_gaps = sum(len(r.gaps) for r in results)
    symbols_with_gaps = [r for r in results if r.gaps]

    lines: list[str] = []
    lines.append("verify_depth report")
    lines.append(f"depth root : {depth_root}")
    lines.append(f"exchange   : {args.exchange}")
    lines.append(f"market     : {args.market}")
    lines.append(f"symbols    : {len(symbols)}")
    lines.append(f"files      : {total_files} checked, "
                 f"{len(bad_file_entries)} with notes")
    lines.append(f"rows       : {_fmt_n(total_rows_scanned)} (sum of "
                 f"meta.num_rows across all parquets)")
    lines.append(f"gaps       : {total_gaps} cross-file update_id gaps in "
                 f"{len(symbols_with_gaps)} symbol(s)")
    lines.append("")

    # -- gaps -----------------------------------------------------------------
    lines.append("# update_id gaps between adjacent files")
    if not symbols_with_gaps:
        lines.append("  (none — every symbol's chain is contiguous)")
    else:
        sym_w = max(len("symbol"),
                    max(len(r.symbol) for r in symbols_with_gaps))
        name_w = max(len("prev parquet"),
                     max(len(g[0].name)
                         for r in symbols_with_gaps for g in r.gaps),
                     max(len(g[1].name)
                         for r in symbols_with_gaps for g in r.gaps))
        head = (f"{'symbol':<{sym_w}}  {'prev parquet':<{name_w}}  "
                f"{'next parquet':<{name_w}}  "
                f"{'prev final_uid':>16}  {'next first_uid':>16}  "
                f"{'gap':>14}")
        lines.append(head)
        lines.append("-" * len(head))
        for r in symbols_with_gaps:
            for prev, nxt, gap in r.gaps:
                lines.append(
                    f"{r.symbol:<{sym_w}}  {prev.name:<{name_w}}  "
                    f"{nxt.name:<{name_w}}  "
                    f"{prev.final_uid_max:>16}  {nxt.first_uid_min:>16}  "
                    f"{gap:>14}"
                )
    lines.append("")

    # -- per-file notes (read errors, missing cols, empty files, inverted) --
    lines.append("# per-file notes (read errors / missing cols / empty / "
                 "inverted ranges)")
    if not bad_file_entries:
        lines.append("  (none)")
    else:
        sym_w = max(len("symbol"),
                    max(len(s) for s, _ in bad_file_entries))
        name_w = max(len("parquet name"),
                     max(len(r.name) for _, r in bad_file_entries))
        head = f"{'symbol':<{sym_w}}  {'parquet name':<{name_w}}  note"
        lines.append(head)
        lines.append("-" * len(head))
        for sym, r in bad_file_entries:
            lines.append(f"{sym:<{sym_w}}  {r.name:<{name_w}}  {r.note}")
    lines.append("")

    # -- per-symbol coverage --------------------------------------------------
    def _coverage(res: SymbolResult):
        good = [r for r in res.rows if not r.note and r.rows > 0]
        if not good:
            return None
        first_dt = min((r.time_min for r in good if r.time_min), default=None)
        last_dt = max((r.time_max for r in good if r.time_max), default=None)
        first_uid = min(r.first_uid_min for r in good)
        last_uid = max(r.final_uid_max for r in good)
        dates = {r.file_date for r in good if r.file_date}
        rows = sum(r.rows for r in good)
        missing_dates = []
        if dates:
            d0, d1 = min(dates), max(dates)
            cur = d0
            while cur <= d1:
                if cur not in dates:
                    missing_dates.append(cur)
                cur += timedelta(days=1)
        return {
            "files": len(res.rows),
            "good_files": len(good),
            "rows": rows,
            "first_dt": first_dt,
            "last_dt": last_dt,
            "first_uid": first_uid,
            "last_uid": last_uid,
            "dates": dates,
            "missing_dates": missing_dates,
            "gaps": len(res.gaps),
        }

    coverage = {r.symbol: _coverage(r) for r in results}

    lines.append("# per-symbol coverage")
    sym_w = max(len("symbol"), max(len(s) for s in coverage))
    head = (f"{'symbol':<{sym_w}}  {'files':>6}  {'good':>6}  "
            f"{'rows':>15}  {'gaps':>5}  {'days':>5}  "
            f"{'missing':>7}  {'first date':<10}  {'last date':<10}")
    lines.append(head)
    lines.append("-" * len(head))
    for sym in sorted(coverage):
        c = coverage[sym]
        if c is None:
            lines.append(
                f"{sym:<{sym_w}}  "
                f"{len(next(r for r in results if r.symbol == sym).rows):>6}  "
                f"{'-':>6}  {'-':>15}  {'-':>5}  {'-':>5}  {'-':>7}  "
                f"{'-':<10}  {'-':<10}"
            )
            continue
        lines.append(
            f"{sym:<{sym_w}}  {c['files']:>6}  {c['good_files']:>6}  "
            f"{_fmt_n(c['rows']):>15}  {c['gaps']:>5}  "
            f"{len(c['dates']):>5}  {len(c['missing_dates']):>7}  "
            f"{_fmt_d(min(c['dates'])):<10}  {_fmt_d(max(c['dates'])):<10}"
        )
    lines.append("")

    # -- missing dates per symbol ---------------------------------------------
    sym_missing = [(s, c) for s, c in coverage.items()
                   if c and c["missing_dates"]]
    lines.append("# symbols with missing date(s) inside [first_date, "
                 "last_date]")
    if not sym_missing:
        lines.append("  (none)")
    else:
        sym_w = max(len("symbol"), max(len(s) for s, _ in sym_missing))
        head = f"{'symbol':<{sym_w}}  missing dates"
        lines.append(head)
        lines.append("-" * len(head))
        for s, c in sorted(sym_missing):
            md_list = ", ".join(d.isoformat() for d in c["missing_dates"])
            lines.append(f"{s:<{sym_w}}  {md_list}")
    lines.append("")

    # -- new-symbol roster by year (mirrors verify_all_symbols_id.py) ---------
    by_year: dict[int | None, list[tuple[datetime | None, str]]] = {}
    for sym, c in coverage.items():
        edt = c["first_dt"] if c else None
        year = edt.year if edt else None
        by_year.setdefault(year, []).append((edt, sym))

    def _year_key(y):
        return (1, 0) if y is None else (0, -y)

    for year in sorted(by_year, key=_year_key):
        bucket = sorted(by_year[year],
                        key=lambda p: (p[0] is None, p[0] or datetime.max,
                                       p[1]))
        title = (f"# {year} new symbols ({len(bucket)})"
                 if year is not None
                 else f"# unknown start ({len(bucket)})")
        lines.append(title)
        lines.append("-" * len(title))
        lines.append(", ".join(s for _, s in bucket))
        lines.append("")

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(ZoneInfo("UTC")).strftime("%Y%m%d_%H%M%S")
    out_path = REPORT_DIR / f"{args.exchange}_{args.market}.{stamp}.report"
    out_path.write_text("\n".join(lines) + "\n")

    print(f"wrote {out_path}")
    print(f"symbols={len(symbols)} files={total_files} "
          f"rows={_fmt_n(total_rows_scanned)} "
          f"gaps={total_gaps} bad_files={len(bad_file_entries)}")
    return 1 if (total_gaps or bad_file_entries) else 0


if __name__ == "__main__":
    sys.exit(main())
