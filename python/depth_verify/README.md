# depth_verify

Integrity checker for the Binance spot depth-diff parquet archive at
`/mnt/sharpe-data/binance/spot/depth/`.

## Mount the NFS share first

```
sudo mount -t nfs 192.168.1.214:/edata/binance/sharpe-trader/data /mnt/sharpe-data
```

## What lives on disk

```
/mnt/sharpe-data/binance/spot/depth/
└── <SYMBOL>/
    ├── <SYMBOL>_binance_depth_<YYYY-MM-DD>.<13-digit-ms>.parquet   # hourly chunks
    └── <SYMBOL>_binance_depth_<YYYY-MM-DD>.parquet                 # tail chunk of that date
```

Each parquet has one row per `(event_time, price, qty, is_bid)` diff entry:

| column            | type              | meaning                                                       |
|-------------------|-------------------|---------------------------------------------------------------|
| `event_time`      | `int64`           | Binance event time, ms                                        |
| `time`            | `timestamp[ms]`   | same time but as a typed timestamp                            |
| `first_update_id` | `int64`           | Binance `U` — first update id in this event                   |
| `final_update_id` | `int64`           | Binance `u` — final update id in this event                   |
| `price`           | `double`          | price level being updated                                     |
| `qty`             | `double`          | new quantity at that level (`0` = remove)                     |
| `is_bid`          | `bool`            | `True` for bid side, `False` for ask                          |

Many rows can share one `(event_time, first_update_id, final_update_id)`
because a single event can touch several price levels.

## What "integrity" means here

Binance documents the depth-diff stream as a strictly contiguous
update-id chain per symbol: for two consecutive events,
`U_next == u_prev + 1`. The verifier exploits that to check
**cross-file contiguity** without scanning row data — only parquet
metadata stats are read.

For each parquet the verifier reads from metadata:

- `meta.num_rows`
- `min(first_update_id)`, `max(final_update_id)`
- `min(time)`, `max(time)`

For each symbol the parquets are then sorted by
`(filename-date, suffix)` so that, within a date, the hourly files come
first (smaller numeric suffix) and the date-only "daily" parquet — the
tail chunk that was being written when the next hourly rotation
happened — comes last. The chain is walked once:

- **cross-file gap**: if
  `next.min(first_update_id) != prev.max(final_update_id) + 1`,
  the difference is recorded as `gap_uid` update ids missing or
  reordered between the two files. This is the headline integrity
  signal: every non-zero gap is data the collector did not capture
  (typically a crash / reconnect window).

The verifier also flags per-file anomalies:

- read errors (corrupt / truncated parquet)
- missing required columns (`first_update_id`, `final_update_id`, `time`)
- empty files
- inverted ranges (`first_uid_min > final_uid_max`,
  `time_min > time_max`)

## What it does **not** check

- **Within-file** gaps. If a single file contains two disjoint
  update-id segments (e.g. a mid-file reconnect), the headline scan
  will not see it because we only look at file-level min/max. The
  default mode is metadata-only so we can sweep ~140 k files in a
  minute; a deeper scan would need to read both update_id columns
  per file.
- **Semantic** correctness of `(price, qty, is_bid)` — values are not
  cross-checked against any reference book.
- **Date-range coverage** is reported (per symbol: first date, last
  date, missing dates inside that range) but the verifier does not
  enforce a particular start date.

## Run

```bash
# full sweep (defaults: /mnt/sharpe-data/binance/spot/depth, 8 worker threads)
python python/depth_verify/verify_depth.py

# higher parallelism on warm NFS
python python/depth_verify/verify_depth.py --workers 16 --file-workers 8 -v

# single symbol for debugging
python python/depth_verify/verify_depth.py --symbol AAVEUSDT

# different depth root / exchange labelling
python python/depth_verify/verify_depth.py \
    --depth-root /mnt/sharpe-data/binance/spot/depth \
    --exchange binance --market spot
```

Flags:

| flag             | default                                       | purpose                                                   |
|------------------|-----------------------------------------------|-----------------------------------------------------------|
| `--depth-root`   | `/mnt/sharpe-data/binance/spot/depth`         | root containing one directory per symbol                  |
| `--exchange`     | `binance`                                     | only used to name the report file                         |
| `--market`       | `spot`                                        | only used to name the report file                         |
| `--symbol`       | (all)                                         | limit the sweep to a single symbol                        |
| `--workers`      | `8`                                           | symbol-level threads                                      |
| `--file-workers` | `8`                                           | parquet-level threads inside one symbol                   |
| `-v / --verbose` | off                                           | per-symbol progress to stderr                             |

Exit code is `1` if any cross-file gap or per-file note was found,
else `0`.

## Output

A fresh, timestamped text report is written to
`report/verify_depth/{exchange}_{market}.{UTC-timestamp}.report`
(relative to the repo root). Sections:

1. **summary header** — totals (symbols, files, rows scanned, gaps).
2. **update_id gaps between adjacent files** — every offending
   `(prev, next)` pair with `prev.final_uid_max`, `next.first_uid_min`
   and the gap size. Empty means the chain is contiguous everywhere.
3. **per-file notes** — read errors / missing cols / empty / inverted
   ranges.
4. **per-symbol coverage** — files, good files, rows, gap count, days
   with data, missing days, first/last date.
5. **symbols with missing date(s)** — gaps in daily coverage inside
   `[first_date, last_date]`.
6. **new-symbol roster by year** — same shape as
   `verify_all_symbols_id.py`, grouped by the year the symbol's
   earliest depth datapoint falls in.

The cross-file gap section is the primary thing to read; everything
else is contextual.
