# 1000000BOBUSDT — aggTrades id-gap investigation

_Investigated 7 monthly archive(s) from `https://data.binance.vision/data/futures/um/monthly/aggTrades/1000000BOBUSDT/`, each verified against Binance's own published `.CHECKSUM` (SHA256)._

## Per-archive analysis (RAW Binance data)

| archive | sha256 vs Binance | rows | unique | dup | min id | max id | missing ids |
|---|---|--:|--:|--:|--:|--:|--:|
| `1000000BOBUSDT-aggTrades-2025-06.zip` | ✅ match | 6,740,128 | 6,740,128 | 0 | 1 | 6,740,128 | **0** |
| `1000000BOBUSDT-aggTrades-2025-07.zip` | ✅ match | 4,853,506 | 4,853,506 | 0 | 6,740,129 | 11,594,758 | **1,124** |
| `1000000BOBUSDT-aggTrades-2025-08.zip` | ✅ match | 1,505,790 | 1,505,790 | 0 | 11,594,759 | 13,100,548 | **0** |
| `1000000BOBUSDT-aggTrades-2025-09.zip` | ✅ match | 1,183,259 | 1,183,259 | 0 | 13,100,549 | 14,283,807 | **0** |
| `1000000BOBUSDT-aggTrades-2025-10.zip` | ✅ match | 1,061,178 | 1,061,178 | 0 | 14,283,808 | 15,344,985 | **0** |
| `1000000BOBUSDT-aggTrades-2025-11.zip` | ✅ match | 1,507,199 | 1,507,199 | 0 | 15,344,986 | 16,852,184 | **0** |
| `1000000BOBUSDT-aggTrades-2025-12.zip` | ✅ match | 455,851 | 455,851 | 0 | 16,852,185 | 17,308,035 | **0** |

## Cross-archive boundary continuity

Expected `max_id(prev) + 1 == min_id(next)`.

| boundary | prev max | next min | delta | status |
|---|--:|--:|--:|---|
| `1000000BOBUSDT-aggTrades-2025-06`→`1000000BOBUSDT-aggTrades-2025-07` | 6,740,128 | 6,740,129 | 0 | ✅ contiguous |
| `1000000BOBUSDT-aggTrades-2025-07`→`1000000BOBUSDT-aggTrades-2025-08` | 11,594,758 | 11,594,759 | 0 | ✅ contiguous |
| `1000000BOBUSDT-aggTrades-2025-08`→`1000000BOBUSDT-aggTrades-2025-09` | 13,100,548 | 13,100,549 | 0 | ✅ contiguous |
| `1000000BOBUSDT-aggTrades-2025-09`→`1000000BOBUSDT-aggTrades-2025-10` | 14,283,807 | 14,283,808 | 0 | ✅ contiguous |
| `1000000BOBUSDT-aggTrades-2025-10`→`1000000BOBUSDT-aggTrades-2025-11` | 15,344,985 | 15,344,986 | 0 | ✅ contiguous |
| `1000000BOBUSDT-aggTrades-2025-11`→`1000000BOBUSDT-aggTrades-2025-12` | 16,852,184 | 16,852,185 | 0 | ✅ contiguous |

## Which archive(s) carry the issue

### `1000000BOBUSDT-aggTrades-2025-07.zip` — 1,124 missing id(s) in 2 range(s)

```
  ids 10,435,864 .. 10,435,883  (20 missing)
  ids 10,437,147 .. 10,438,250  (1,104 missing)
```

## Cross-check vs our local parquet

- **2025** local `1000000BOBUSDT_aggTrades_2025.parquet`: rows=17,306,911 unique=17,306,911 dup=0 missing_ids=**1,124** | raw archive rows this year (pre-dedup) = 17,306,911

## Verdict

- ✅ **CONFIRMED BINANCE-SIDE ISSUE.** 1 checksum-verified Binance archive(s) are *themselves* internally missing `agg_trade_id`s — the gap is in Binance's own published data, not our pipeline.


## Daily-archive cross-check (is it recoverable from Binance?)

The monthly-missing ids were traced to their calendar day via surrounding
trades, then checked against Binance's **daily** archive (also SHA256-verified):

| missing id range | UTC window (2025-07-16) | in monthly? | in daily 2025-07-16? |
|---|---|---|---|
| 10,435,864 .. 10,435,883 (20) | 16:42:36 – 16:43:01 | ❌ no | ❌ no |
| 10,437,147 .. 10,438,250 (1,104) | 17:00:25 – 17:05:48 | ❌ no | ❌ no |

Daily `1000000BOBUSDT-aggTrades-2025-07-16.zip` (checksum OK, 120,108 rows,
id 10,346,985..10,468,216) recovers **0 / 1,124** of the missing ids.

## Final conclusion

**Confirmed Binance-side data loss, irrecoverable.** The 1,124 missing
`agg_trade_id`s are absent from *both* Binance's checksum-verified **monthly**
(`2025-07.zip`) and **daily** (`2025-07-16.zip`) published archives. The loss
is localized to 2025-07-16 ~16:42 and ~17:00–17:05 UTC — the *identical*
window as 1000000MOGUSDT, indicating a multi-symbol Binance data incident on
2025-07-16, not a per-symbol or pipeline problem. Nothing to fix on our side;
gap cannot be backfilled from Binance.
