#!/usr/bin/env python3
"""Verify integrity of Parquet data under $TRADE_DATA/binance.

Scans every data type under the standard layout and reports:

  * parquet files that fail to open
  * year files whose filename-year disagrees with the actual data
  * duplicate year files for the same symbol+year (e.g. both
    BTCUSDT_aggTrades_2025.parquet and BTCUSDT_aggTrades_2025-12-14.parquet)
  * incomplete-year files whose filename date suffix doesn't match the
    actual max-time in the file
  * day-level gaps inside a year file (any calendar day in the expected
    range with zero rows)
  * cadence anomalies for fixed-cadence data types (metrics, fundingRate)

Usage:
    python verify_data.py                 # scan everything
    python verify_data.py --data-type aggTrades
    python verify_data.py --symbol ETHUSDT
    python verify_data.py --fail-fast     # stop on first issue
    python verify_data.py --json report.json
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable, Iterator

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

TRADE_DATA = Path(os.environ.get("TRADE_DATA", "/ndata/trade/data"))
TODAY = date.today()


# ---------- data type configuration ----------

@dataclass(frozen=True)
class DataTypeSpec:
    name: str
    roots: tuple[Path, ...]       # directories containing {symbol}/ subdirs
    file_glob: str                # glob for symbol parquet files
    time_col: str = "time"
    # Expected tick interval; used for cadence checks. None skips the check.
    expected_interval: timedelta | None = None
    # Maximum tolerated gap between consecutive rows before reporting.
    # None skips the per-row gap check.
    max_row_gap: timedelta | None = None
    # If True, expect 1 row per calendar day at minimum.
    require_daily_coverage: bool = True


def _fut_um(sub: str) -> Path:
    return TRADE_DATA / "binance" / "future" / "um" / sub


def _spot(sub: str) -> Path:
    return TRADE_DATA / "binance" / "spot" / sub


def build_specs() -> list[DataTypeSpec]:
    return [
        DataTypeSpec(
            name="future_um_aggTrades",
            roots=(_fut_um("aggTrades"),),
            file_glob="{sym}_aggTrades_*.parquet",
            max_row_gap=timedelta(hours=6),
        ),
        DataTypeSpec(
            name="future_um_aggTrades_kline",
            roots=(_fut_um("aggTrades_kline"),),
            # kline files include the interval: {sym}_kline_{interval}_YYYY[...]
            file_glob="{sym}_kline_*_*.parquet",
        ),
        DataTypeSpec(
            name="future_um_metrics",
            roots=(_fut_um("metrics"),),
            file_glob="{sym}_metrics_*.parquet",
            expected_interval=timedelta(minutes=5),
            # metrics cadence is 5m; allow small jitter + a few missed ticks
            max_row_gap=timedelta(minutes=30),
        ),
        DataTypeSpec(
            name="future_um_fundingRate",
            roots=(_fut_um("fundingRate"),),
            file_glob="{sym}_fundingRate_*.parquet",
            expected_interval=timedelta(hours=8),
            max_row_gap=timedelta(hours=9),
        ),
        DataTypeSpec(
            name="future_option_BVOLIndex",
            roots=(TRADE_DATA / "binance" / "future" / "option" / "BVOLIndex",),
            file_glob="{sym}_BVOLIndex_*.parquet",
            # BVOLIndex is roughly 1s cadence but with jitter; skip strict checks
            max_row_gap=timedelta(hours=6),
        ),
        DataTypeSpec(
            name="spot_aggTrades",
            roots=(_spot("aggTrades"),),
            file_glob="{sym}_aggTrades_*.parquet",
            max_row_gap=timedelta(hours=6),
        ),
        DataTypeSpec(
            name="spot_aggTrades_kline",
            roots=(_spot("aggTrades_kline"),),
            file_glob="{sym}_kline_*_*.parquet",
        ),
    ]


# ---------- issue reporting ----------

@dataclass
class Issue:
    kind: str
    spec: str
    symbol: str
    path: str
    detail: str

    def render(self) -> str:
        return f"[{self.kind}] {self.spec}/{self.symbol} {Path(self.path).name}: {self.detail}"


# ---------- filename parsing ----------

# Matches the trailing year / year-MM-DD token of a data file, e.g.
#   ETHUSDT_aggTrades_2025.parquet           -> ('2025', None)
#   ETHUSDT_aggTrades_2026-04-08.parquet     -> ('2026', '04-08')
#   BTCUSDT_kline_1m_2025.parquet            -> ('2025', None)
#   DUSDT_metrics_2025-01-09_2026-01-28.parquet  -> ('2026', '01-28')  (take last token)
_YEAR_RE = re.compile(r"(?P<year>\d{4})(?:-(?P<md>\d{2}-\d{2}))?\.parquet$")


def parse_file_year(path: Path) -> tuple[int, str | None] | None:
    m = _YEAR_RE.search(path.name)
    if not m:
        return None
    return int(m.group("year")), m.group("md")


# ---------- core checks ----------

@dataclass
class ScanResult:
    tmin: datetime
    tmax: datetime
    row_count: int
    days_with_data: set[date]
    sorted_: bool
    # gap tracking
    max_row_gap: timedelta
    row_gap_samples: list[tuple[datetime, datetime, timedelta]]
    # cadence tracking
    cadence_bad: int
    cadence_total: int
    cadence_min: timedelta | None
    cadence_max: timedelta | None


def _scan_time_column(path: Path, time_col: str,
                      max_row_gap: timedelta | None,
                      expected_interval: timedelta | None) -> ScanResult:
    """Stream the time column via row-group batches.

    Avoids materializing the whole column in pandas for multi-hundred-million
    row files. Sorting is assumed when the file's row-group min/max are
    monotonically non-decreasing; otherwise we fall back to a full sort.
    """
    pf = pq.ParquetFile(path)
    schema = pf.schema_arrow
    if time_col not in schema.names:
        raise ValueError(f"missing column {time_col}")

    tmin: datetime | None = None
    tmax: datetime | None = None
    row_count = 0
    days_with_data: set[date] = set()
    sorted_ = True
    last_ts: np.datetime64 | None = None
    max_gap = timedelta(0)
    row_gap_samples: list[tuple[datetime, datetime, timedelta]] = []

    cadence_bad = 0
    cadence_total = 0
    cadence_min: np.timedelta64 | None = None
    cadence_max: np.timedelta64 | None = None

    max_gap_td64 = np.timedelta64(int(max_row_gap.total_seconds() * 1_000_000), "us") if max_row_gap else None
    expected_td64 = None
    tol_td64 = None
    if expected_interval is not None:
        exp_us = int(expected_interval.total_seconds() * 1_000_000)
        expected_td64 = np.timedelta64(exp_us, "us")
        tol_td64 = np.timedelta64(max(exp_us // 10, 1), "us")

    # Fast path: if cadence check is not required and every row group has
    # usable min/max statistics for the time column, build the result from
    # statistics alone. This avoids reading hundreds of millions of rows.
    if expected_td64 is None:
        fast = _try_stats_only(pf, time_col, max_gap_td64)
        if fast is not None:
            return fast

    # Read row groups one at a time.
    for rg_idx in range(pf.num_row_groups):
        batch = pf.read_row_group(rg_idx, columns=[time_col]).column(time_col)
        # Cast to us precision for uniform arithmetic.
        batch = pc.cast(batch, pa.timestamp("us"))
        arr = batch.to_numpy(zero_copy_only=False)
        if arr.size == 0:
            continue

        row_count += arr.size

        # Track min/max.
        bmin = arr.min()
        bmax = arr.max()
        if tmin is None or bmin < np.datetime64(tmin):
            tmin = _dt64_to_datetime(bmin)
        if tmax is None or bmax > np.datetime64(tmax):
            tmax = _dt64_to_datetime(bmax)

        # Monotonic check within batch.
        if arr.size > 1:
            diffs = np.diff(arr)
            if (diffs < np.timedelta64(0, "us")).any():
                sorted_ = False
            # Gap tracking (only meaningful if sorted).
            if max_gap_td64 is not None:
                big_mask = diffs > max_gap_td64
                if big_mask.any():
                    bg = diffs[big_mask].max()
                    if bg > np.timedelta64(int(max_gap.total_seconds() * 1_000_000), "us"):
                        max_gap = timedelta(microseconds=int(bg / np.timedelta64(1, "us")))
                    if len(row_gap_samples) < 5:
                        idxs = np.where(big_mask)[0][: 5 - len(row_gap_samples)]
                        for i in idxs:
                            row_gap_samples.append((
                                _dt64_to_datetime(arr[i]),
                                _dt64_to_datetime(arr[i + 1]),
                                timedelta(microseconds=int(diffs[i] / np.timedelta64(1, "us"))),
                            ))
            if expected_td64 is not None:
                cadence_total += len(diffs)
                bad_mask = (diffs < expected_td64 - tol_td64) | (diffs > expected_td64 + tol_td64)
                cadence_bad += int(bad_mask.sum())
                dmin = diffs.min()
                dmax = diffs.max()
                if cadence_min is None or dmin < cadence_min:
                    cadence_min = dmin
                if cadence_max is None or dmax > cadence_max:
                    cadence_max = dmax

        # Gap between batches (if sorted so far).
        if last_ts is not None and arr.size > 0:
            inter = arr[0] - last_ts
            if inter < np.timedelta64(0, "us"):
                sorted_ = False
            if max_gap_td64 is not None and inter > max_gap_td64:
                if len(row_gap_samples) < 5:
                    row_gap_samples.append((
                        _dt64_to_datetime(last_ts),
                        _dt64_to_datetime(arr[0]),
                        timedelta(microseconds=int(inter / np.timedelta64(1, "us"))),
                    ))

        last_ts = arr[-1]

        # Unique days in this batch.
        days = np.unique(arr.astype("datetime64[D]"))
        for d in days:
            days_with_data.add(d.astype(object))

    if row_count == 0:
        raise ValueError("empty")

    assert tmin is not None and tmax is not None
    return ScanResult(
        tmin=tmin,
        tmax=tmax,
        row_count=row_count,
        days_with_data=days_with_data,
        sorted_=sorted_,
        max_row_gap=max_gap,
        row_gap_samples=row_gap_samples,
        cadence_bad=cadence_bad,
        cadence_total=cadence_total,
        cadence_min=(
            timedelta(microseconds=int(cadence_min / np.timedelta64(1, "us")))
            if cadence_min is not None else None
        ),
        cadence_max=(
            timedelta(microseconds=int(cadence_max / np.timedelta64(1, "us")))
            if cadence_max is not None else None
        ),
    )


def _dt64_to_datetime(v: np.datetime64) -> datetime:
    us = int(np.datetime64(v, "us").astype("int64"))
    return datetime(1970, 1, 1) + timedelta(microseconds=us)


def _try_stats_only(pf: pq.ParquetFile, time_col: str,
                    max_gap_td64: np.timedelta64 | None) -> ScanResult | None:
    """Build a ScanResult from row-group statistics, without reading data.

    Returns None if any row group lacks min/max statistics for `time_col`.
    Day coverage is derived from the union of per-row-group [min_day, max_day]
    intervals — this is correct as long as row groups are internally contiguous
    at day granularity, which is the case for our ingester.
    """
    col_idx = pf.schema_arrow.get_field_index(time_col)
    if col_idx < 0:
        return None
    meta = pf.metadata
    if meta is None:
        return None

    # Determine the pyarrow timestamp type so we can reinterpret the raw stat
    # values correctly (stats come back as python ints for timestamp columns).
    ts_type = pf.schema_arrow.field(time_col).type
    if not pa.types.is_timestamp(ts_type):
        return None
    unit = ts_type.unit  # 's' | 'ms' | 'us' | 'ns'
    unit_map = {
        "s": ("s", 1_000_000),
        "ms": ("ms", 1_000),
        "us": ("us", 1),
        "ns": ("ns", 1),  # we'll convert ns→us by dividing
    }
    if unit not in unit_map:
        return None

    row_count = 0
    tmin_us: int | None = None
    tmax_us: int | None = None
    days_with_data: set[date] = set()
    row_gap_samples: list[tuple[datetime, datetime, timedelta]] = []
    prev_max_us: int | None = None
    sorted_ = True
    max_gap = timedelta(0)

    for rg in range(meta.num_row_groups):
        rg_meta = meta.row_group(rg)
        col = rg_meta.column(col_idx)
        stats = col.statistics
        if stats is None or not stats.has_min_max:
            return None
        rg_min = stats.min
        rg_max = stats.max
        # pyarrow returns stats as python datetime for timestamp columns
        if isinstance(rg_min, datetime):
            mn_us = int(rg_min.timestamp() * 1_000_000) + rg_min.microsecond % 1_000_000 - int(rg_min.timestamp()) * 0
            # simpler: convert via numpy
            mn_us = int((np.datetime64(rg_min) - np.datetime64("1970-01-01")) / np.timedelta64(1, "us"))
            mx_us = int((np.datetime64(rg_max) - np.datetime64("1970-01-01")) / np.timedelta64(1, "us"))
        else:
            # raw int in column's native unit
            scale_up = unit_map[unit][1]
            if unit == "ns":
                mn_us = int(rg_min) // 1000
                mx_us = int(rg_max) // 1000
            else:
                mn_us = int(rg_min) * scale_up
                mx_us = int(rg_max) * scale_up

        row_count += rg_meta.num_rows
        if tmin_us is None or mn_us < tmin_us:
            tmin_us = mn_us
        if tmax_us is None or mx_us > tmax_us:
            tmax_us = mx_us

        if prev_max_us is not None:
            if mn_us < prev_max_us:
                sorted_ = False
            elif max_gap_td64 is not None:
                gap_us = mn_us - prev_max_us
                if gap_us > int(max_gap_td64 / np.timedelta64(1, "us")):
                    if len(row_gap_samples) < 5:
                        row_gap_samples.append((
                            _us_to_datetime(prev_max_us),
                            _us_to_datetime(mn_us),
                            timedelta(microseconds=gap_us),
                        ))
                    if gap_us > int(max_gap.total_seconds() * 1_000_000):
                        max_gap = timedelta(microseconds=gap_us)
        prev_max_us = mx_us

        # Day coverage: union of [min_day, max_day] for this row group. For
        # row groups whose span exceeds 2 days we can't assume contiguity —
        # read the actual column to get real days.
        d_start = _us_to_datetime(mn_us).date()
        d_end = _us_to_datetime(mx_us).date()
        if (d_end - d_start).days > 1:
            batch = pf.read_row_group(rg, columns=[time_col]).column(time_col)
            batch = pc.cast(batch, pa.timestamp("us"))
            arr = batch.to_numpy(zero_copy_only=False)
            for d in np.unique(arr.astype("datetime64[D]")):
                days_with_data.add(d.astype(object))
        else:
            d = d_start
            while d <= d_end:
                days_with_data.add(d)
                d += timedelta(days=1)

    if row_count == 0 or tmin_us is None or tmax_us is None:
        return None

    return ScanResult(
        tmin=_us_to_datetime(tmin_us),
        tmax=_us_to_datetime(tmax_us),
        row_count=row_count,
        days_with_data=days_with_data,
        sorted_=sorted_,
        max_row_gap=max_gap,
        row_gap_samples=row_gap_samples,
        cadence_bad=0,
        cadence_total=0,
        cadence_min=None,
        cadence_max=None,
    )


def _us_to_datetime(us: int) -> datetime:
    return datetime(1970, 1, 1) + timedelta(microseconds=us)


def check_parquet(spec: DataTypeSpec, symbol: str, path: Path) -> list[Issue]:
    issues: list[Issue] = []
    try:
        res = _scan_time_column(path, spec.time_col, spec.max_row_gap, spec.expected_interval)
    except ValueError as e:
        if str(e) == "empty":
            return [Issue("empty", spec.name, symbol, str(path), "zero rows")]
        return [Issue("unreadable", spec.name, symbol, str(path), str(e))]
    except Exception as e:
        return [Issue("unreadable", spec.name, symbol, str(path), f"{type(e).__name__}: {e}")]

    parsed = parse_file_year(path)
    if parsed is None:
        issues.append(Issue("filename", spec.name, symbol, str(path),
                            "cannot parse year from filename"))
        return issues
    year, md_suffix = parsed

    if res.tmin.year != year or res.tmax.year != year:
        issues.append(Issue("year-mismatch", spec.name, symbol, str(path),
                            f"filename year={year} but data spans {res.tmin.date()}..{res.tmax.date()}"))

    if md_suffix is not None:
        expected_md = f"{res.tmax.month:02d}-{res.tmax.day:02d}"
        if expected_md != md_suffix:
            issues.append(Issue("suffix-mismatch", spec.name, symbol, str(path),
                                f"filename suffix={md_suffix} but data max day is {expected_md}"))

    if not res.sorted_:
        issues.append(Issue("unsorted", spec.name, symbol, str(path),
                            "time column not monotonically non-decreasing"))

    if spec.require_daily_coverage:
        first_day = date(year, 1, 1) if md_suffix is None else res.tmin.date()
        if md_suffix is None:
            last_day = date(year, 12, 31)
        else:
            mm, dd = (int(x) for x in md_suffix.split("-"))
            last_day = date(year, mm, dd)
        missing: list[date] = []
        d = first_day
        while d <= last_day:
            if d not in res.days_with_data:
                missing.append(d)
            d += timedelta(days=1)
        if missing:
            issues.append(Issue("day-gap", spec.name, symbol, str(path),
                                _summarize_day_gaps(missing)))

    if spec.max_row_gap is not None and res.row_gap_samples:
        samples = "; ".join(f"{a}->{b} ({d})" for a, b, d in res.row_gap_samples[:5])
        issues.append(Issue("row-gap", spec.name, symbol, str(path),
                            f">{spec.max_row_gap}: {samples}"))

    if spec.expected_interval is not None and res.cadence_bad > 0:
        issues.append(Issue("cadence", spec.name, symbol, str(path),
                            f"{res.cadence_bad}/{res.cadence_total} diffs outside "
                            f"{spec.expected_interval}±10% "
                            f"(min={res.cadence_min}, max={res.cadence_max})"))

    return issues


def _summarize_day_gaps(missing: list[date]) -> str:
    # Collapse consecutive missing days into ranges.
    if not missing:
        return "none"
    ranges: list[tuple[date, date]] = []
    cur_start = cur_end = missing[0]
    for d in missing[1:]:
        if d == cur_end + timedelta(days=1):
            cur_end = d
        else:
            ranges.append((cur_start, cur_end))
            cur_start = cur_end = d
    ranges.append((cur_start, cur_end))
    parts = []
    for a, b in ranges:
        if a == b:
            parts.append(str(a))
        else:
            parts.append(f"{a}..{b}")
    total = sum((b - a).days + 1 for a, b in ranges)
    return f"{total} missing day(s) in {len(ranges)} range(s): {', '.join(parts[:10])}" + (
        "…" if len(parts) > 10 else ""
    )


# ---------- scan orchestration ----------

def iter_symbol_dirs(spec: DataTypeSpec, symbol_filter: set[str] | None) -> Iterator[tuple[str, Path]]:
    for root in spec.roots:
        if not root.exists():
            continue
        for sym_dir in sorted(root.iterdir()):
            if not sym_dir.is_dir() or sym_dir.name.startswith("_"):
                continue
            if symbol_filter and sym_dir.name not in symbol_filter:
                continue
            yield sym_dir.name, sym_dir


def check_symbol(spec: DataTypeSpec, symbol: str, sym_dir: Path) -> list[Issue]:
    issues: list[Issue] = []
    glob = spec.file_glob.replace("{sym}", symbol)
    files = sorted(sym_dir.glob(glob))
    if not files:
        return issues

    # For kline files, group by interval so the year-duplicate check is per
    # (symbol, interval, year). For other types, group by (year,).
    def group_key(p: Path) -> tuple:
        stem = p.stem
        if "_kline_" in stem:
            # {sym}_kline_{interval}_{year...}
            parts = stem.split("_kline_", 1)[1].split("_", 1)
            interval = parts[0]
            rest = parts[1] if len(parts) > 1 else ""
            year = rest[:4]
            return (interval, year)
        parsed = parse_file_year(p)
        return ("", str(parsed[0]) if parsed else "")

    by_group: dict[tuple, list[Path]] = {}
    for f in files:
        by_group.setdefault(group_key(f), []).append(f)

    for key, paths in by_group.items():
        if len(paths) > 1:
            issues.append(Issue(
                "duplicate-year", spec.name, symbol, str(paths[0].parent),
                f"{key} has {len(paths)} files: {[p.name for p in paths]}",
            ))

    for f in files:
        issues.extend(check_parquet(spec, symbol, f))

    return issues


def _check_symbol_worker(args: tuple[DataTypeSpec, str, Path]) -> list[Issue]:
    spec, sym, sym_dir = args
    return check_symbol(spec, sym, sym_dir)


def run(specs: Iterable[DataTypeSpec], symbol_filter: set[str] | None,
        fail_fast: bool, verbose: bool, workers: int) -> list[Issue]:
    all_issues: list[Issue] = []
    jobs: list[tuple[DataTypeSpec, str, Path]] = []
    for spec in specs:
        sym_iter = list(iter_symbol_dirs(spec, symbol_filter))
        if verbose:
            print(f"== {spec.name}: {len(sym_iter)} symbols ==", file=sys.stderr)
        for sym, sym_dir in sym_iter:
            jobs.append((spec, sym, sym_dir))

    if workers <= 1 or len(jobs) <= 1:
        for job in jobs:
            issues = _check_symbol_worker(job)
            for iss in issues:
                print(iss.render(), flush=True)
                if fail_fast:
                    return all_issues + issues
            all_issues.extend(issues)
        return all_issues

    from multiprocessing import Pool
    with Pool(workers) as pool:
        for i, issues in enumerate(pool.imap_unordered(_check_symbol_worker, jobs, chunksize=1)):
            if verbose and (i + 1) % 50 == 0:
                print(f"  [{i+1}/{len(jobs)}]", file=sys.stderr)
            for iss in issues:
                print(iss.render(), flush=True)
                if fail_fast:
                    pool.terminate()
                    return all_issues + issues
            all_issues.extend(issues)
    return all_issues


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--data-type", action="append",
                   help="restrict to spec name (repeatable), e.g. future_um_aggTrades")
    p.add_argument("--symbol", action="append", help="restrict to symbol (repeatable)")
    p.add_argument("--fail-fast", action="store_true", help="stop on first issue")
    p.add_argument("-v", "--verbose", action="store_true")
    p.add_argument("--json", metavar="PATH", help="also write issues as JSON to PATH")
    p.add_argument("-j", "--workers", type=int, default=max(1, (os.cpu_count() or 4) // 2),
                   help="parallel worker processes (default half of CPUs)")
    args = p.parse_args()

    specs = build_specs()
    if args.data_type:
        wanted = set(args.data_type)
        specs = [s for s in specs if s.name in wanted]
        if not specs:
            print(f"no specs match {args.data_type}", file=sys.stderr)
            return 2

    sym_filter = set(args.symbol) if args.symbol else None
    issues = run(specs, sym_filter, args.fail_fast, args.verbose, args.workers)

    kinds: dict[str, int] = {}
    for iss in issues:
        kinds[iss.kind] = kinds.get(iss.kind, 0) + 1
    print(f"\n=== SUMMARY: {len(issues)} issue(s) ===")
    for k, n in sorted(kinds.items(), key=lambda kv: -kv[1]):
        print(f"  {k}: {n}")

    if args.json:
        Path(args.json).write_text(json.dumps([asdict(i) for i in issues], indent=2))
        print(f"wrote {args.json}")

    return 1 if issues else 0


if __name__ == "__main__":
    sys.exit(main())
