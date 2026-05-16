# 1000000MOGUSDT — aggTrades id-gap investigation

_Investigated 14 monthly archive(s) from `https://data.binance.vision/data/futures/um/monthly/aggTrades/1000000MOGUSDT/`, each verified against Binance's own published `.CHECKSUM` (SHA256)._

## Per-archive analysis (RAW Binance data)

| archive | sha256 vs Binance | rows | unique | dup | min id | max id | missing ids |
|---|---|--:|--:|--:|--:|--:|--:|
| `1000000MOGUSDT-aggTrades-2024-11.zip` | ✅ match | 5,851,347 | 5,851,347 | 0 | 1 | 5,851,347 | **0** |
| `1000000MOGUSDT-aggTrades-2024-12.zip` | ✅ match | 7,224,786 | 7,224,786 | 0 | 5,851,348 | 13,076,133 | **0** |
| `1000000MOGUSDT-aggTrades-2025-01.zip` | ✅ match | 4,751,054 | 4,751,054 | 0 | 13,076,134 | 17,827,187 | **0** |
| `1000000MOGUSDT-aggTrades-2025-02.zip` | ✅ match | 3,246,287 | 3,246,287 | 0 | 17,827,188 | 21,073,474 | **0** |
| `1000000MOGUSDT-aggTrades-2025-03.zip` | ✅ match | 1,497,670 | 1,497,670 | 0 | 21,073,475 | 22,571,144 | **0** |
| `1000000MOGUSDT-aggTrades-2025-04.zip` | ✅ match | 1,543,354 | 1,543,354 | 0 | 22,571,145 | 24,114,498 | **0** |
| `1000000MOGUSDT-aggTrades-2025-05.zip` | ✅ match | 4,103,780 | 4,103,780 | 0 | 24,114,499 | 28,218,278 | **0** |
| `1000000MOGUSDT-aggTrades-2025-06.zip` | ✅ match | 2,954,575 | 2,954,575 | 0 | 28,218,279 | 31,172,853 | **0** |
| `1000000MOGUSDT-aggTrades-2025-07.zip` | ✅ match | 4,544,003 | 4,544,003 | 0 | 31,172,854 | 35,717,768 | **912** |
| `1000000MOGUSDT-aggTrades-2025-08.zip` | ✅ match | 2,907,698 | 2,907,698 | 0 | 35,717,769 | 38,625,466 | **0** |
| `1000000MOGUSDT-aggTrades-2025-09.zip` | ✅ match | 1,277,151 | 1,277,151 | 0 | 38,625,467 | 39,902,617 | **0** |
| `1000000MOGUSDT-aggTrades-2025-10.zip` | ✅ match | 1,402,321 | 1,402,321 | 0 | 39,902,618 | 41,304,938 | **0** |
| `1000000MOGUSDT-aggTrades-2025-11.zip` | ✅ match | 1,686,685 | 1,686,685 | 0 | 41,304,939 | 42,991,623 | **0** |
| `1000000MOGUSDT-aggTrades-2025-12.zip` | ✅ match | 613,587 | 613,587 | 0 | 42,991,624 | 43,605,210 | **0** |

## Cross-archive boundary continuity

Expected `max_id(prev) + 1 == min_id(next)`.

| boundary | prev max | next min | delta | status |
|---|--:|--:|--:|---|
| `1000000MOGUSDT-aggTrades-2024-11`→`1000000MOGUSDT-aggTrades-2024-12` | 5,851,347 | 5,851,348 | 0 | ✅ contiguous |
| `1000000MOGUSDT-aggTrades-2024-12`→`1000000MOGUSDT-aggTrades-2025-01` | 13,076,133 | 13,076,134 | 0 | ✅ contiguous |
| `1000000MOGUSDT-aggTrades-2025-01`→`1000000MOGUSDT-aggTrades-2025-02` | 17,827,187 | 17,827,188 | 0 | ✅ contiguous |
| `1000000MOGUSDT-aggTrades-2025-02`→`1000000MOGUSDT-aggTrades-2025-03` | 21,073,474 | 21,073,475 | 0 | ✅ contiguous |
| `1000000MOGUSDT-aggTrades-2025-03`→`1000000MOGUSDT-aggTrades-2025-04` | 22,571,144 | 22,571,145 | 0 | ✅ contiguous |
| `1000000MOGUSDT-aggTrades-2025-04`→`1000000MOGUSDT-aggTrades-2025-05` | 24,114,498 | 24,114,499 | 0 | ✅ contiguous |
| `1000000MOGUSDT-aggTrades-2025-05`→`1000000MOGUSDT-aggTrades-2025-06` | 28,218,278 | 28,218,279 | 0 | ✅ contiguous |
| `1000000MOGUSDT-aggTrades-2025-06`→`1000000MOGUSDT-aggTrades-2025-07` | 31,172,853 | 31,172,854 | 0 | ✅ contiguous |
| `1000000MOGUSDT-aggTrades-2025-07`→`1000000MOGUSDT-aggTrades-2025-08` | 35,717,768 | 35,717,769 | 0 | ✅ contiguous |
| `1000000MOGUSDT-aggTrades-2025-08`→`1000000MOGUSDT-aggTrades-2025-09` | 38,625,466 | 38,625,467 | 0 | ✅ contiguous |
| `1000000MOGUSDT-aggTrades-2025-09`→`1000000MOGUSDT-aggTrades-2025-10` | 39,902,617 | 39,902,618 | 0 | ✅ contiguous |
| `1000000MOGUSDT-aggTrades-2025-10`→`1000000MOGUSDT-aggTrades-2025-11` | 41,304,938 | 41,304,939 | 0 | ✅ contiguous |
| `1000000MOGUSDT-aggTrades-2025-11`→`1000000MOGUSDT-aggTrades-2025-12` | 42,991,623 | 42,991,624 | 0 | ✅ contiguous |

## Which archive(s) carry the issue

### `1000000MOGUSDT-aggTrades-2025-07.zip` — 912 missing id(s) in 2 range(s)

```
  ids 33,597,069 .. 33,597,175  (107 missing)
  ids 33,600,711 .. 33,601,515  (805 missing)
```

## Cross-check vs our local parquet

- **2024** local `1000000MOGUSDT_aggTrades_2024.parquet`: rows=13,076,133 unique=13,076,133 dup=0 missing_ids=**0** | raw archive rows this year (pre-dedup) = 13,076,133
- **2025** local `1000000MOGUSDT_aggTrades_2025.parquet`: rows=30,528,165 unique=30,528,165 dup=0 missing_ids=**912** | raw archive rows this year (pre-dedup) = 30,528,165

## Verdict

- ✅ **CONFIRMED BINANCE-SIDE ISSUE.** 1 checksum-verified Binance archive(s) are *themselves* internally missing `agg_trade_id`s — the gap is in Binance's own published data, not our pipeline.


## Daily-archive cross-check (is it recoverable from Binance?)

The monthly-missing ids were traced to their calendar day via surrounding
trades, then checked against Binance's **daily** archive (also SHA256-verified):

| missing id range | UTC window (2025-07-16) | in monthly? | in daily 2025-07-16? |
|---|---|---|---|
| 33,597,069 .. 33,597,175 (107) | 16:42:37 – 16:43:03 | ❌ no | ❌ no |
| 33,600,711 .. 33,601,515 (805) | 17:00:26 – 17:05:47 | ❌ no | ❌ no |

Daily `1000000MOGUSDT-aggTrades-2025-07-16.zip` (checksum OK, 287,520 rows,
id 33,392,469..33,680,900) recovers **0 / 912** of the missing ids.

## Final conclusion

**Confirmed Binance-side data loss, irrecoverable.** The 912 missing
`agg_trade_id`s are absent from *both* Binance's checksum-verified **monthly**
(`2025-07.zip`) and **daily** (`2025-07-16.zip`) published archives. The loss
is localized to 2025-07-16 ~16:42 and ~17:00–17:05 UTC. Our pipeline
reproduced Binance's data faithfully; nothing to fix on our side, and the gap
cannot be backfilled from Binance archives. (Same incident & timestamps seen
on 1000000BOBUSDT — a multi-symbol Binance incident on 2025-07-16.)
