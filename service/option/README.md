# data-source-option

Scoped to **BVOLIndex** only (`data_source -s ETHBVOLUSDT -m option -d BVOLIndex`).
Runs daily at **09:00 UTC** via `data-source-option.timer`, plus on boot if
the last successful run was > 24h ago.

Full run output is mirrored to `report/option/{UTC-date}.run.log`
(also in the journal).

This used to be the single combined job; BTC/ETH and altcoin work have
moved to `data-source-btceth` (07:30) and `data-source-altcoin` (08:00).
See `../README.md`.
