# data-source services

Four independent systemd services, each with its own timer and on-boot
catch-up unit (runs on boot if the last successful run was > 24h ago).
All times are UTC.

| Service | Time | Jobs |
|---|---|---|
| `data-source-btceth`       | 07:30 | aggTrades spot BTC/ETH, agg_kline spot BTC/ETH (`--kline-1s`), verify report |
| `data-source-altcoin-spot` | 08:00 | altcoin download+verify spot, altcoin→kline spot (non-fatal) |
| `data-source-altcoin-um`   | 08:20 | altcoin download+verify um, altcoin→kline um (non-fatal) |
| `data-source-option`       | 09:00 | BVOLIndex ETHBVOLUSDT |

The altcoin steps refresh `exchange_info_{spot,um}.json`, then download
every TRADING USDT symbol (minus the majors in
`python/altcoin_symbols_except.json`) and run `verify_all_symbols_id`,
then `agg_kline` over the same set.

## Reports / logs

Each service writes into its own independent folder `report/<service>/`
(in addition to the systemd journal):

| Service | Output |
|---|---|
| btceth       | `report/btceth/{UTC-date}.run.log` (full run) + `report/btceth/{UTC-date}.report` (verify, appended) |
| altcoin-spot | `report/altcoin-spot/{UTC-date}.run.log` + `report/altcoin-spot/running_log/*.log` + `report/altcoin-spot/verify_all_symbols_id/binance_spot.*.report` |
| altcoin-um   | `report/altcoin-um/{UTC-date}.run.log` + `report/altcoin-um/running_log/*.log` + `report/altcoin-um/verify_all_symbols_id/binance_um.*.report` |
| option       | `report/option/{UTC-date}.run.log` (full run) |

The altcoin Python scripts honor `DATA_SOURCE_REPORT_DIR` (set by each
`*-run.sh` to its `report/<service>` dir); unset, they fall back to the
old shared `report/running_log` + `report/verify_all_symbols_id`.

## Layout

```
service/
  install.sh                              # installs/enables all 4 services
  btceth/        data-source-btceth-{run.sh,.service,.timer,-boot.service}
  altcoin-spot/  data-source-altcoin-spot-{run.sh,.service,.timer,-boot.service}
  altcoin-um/    data-source-altcoin-um-{run.sh,.service,.timer,-boot.service}
  option/        data-source-option-{run.sh,.service,.timer,-boot.service}
  run_full_year.sh                        # manual per-symbol/year backfill (not a unit)
```

Each `*-run.sh` records a `.last_run` epoch stamp in its own service dir;
the `-boot.service` passes `--boot` so it skips when the last run was
< 24h ago.

## Install / cutover

```bash
bash service/install.sh
```

`install.sh` retires the old combined `data-source-altcoin` units (now
split into `altcoin-spot` + `altcoin-um`) without stopping an in-progress
run, re-uses the `data-source-option` unit names (scoped to BVOLIndex
only), and installs `data-source-btceth`.

## Commands

```bash
# All timers + next runs
systemctl list-timers 'data-source-*.timer'

# Logs (one service)
journalctl -u data-source-altcoin-um.service -f

# Manual run
sudo systemctl start data-source-altcoin-spot.service

# Uninstall everything
for s in btceth altcoin-spot altcoin-um option; do
  sudo systemctl disable --now data-source-$s.timer
  sudo systemctl disable data-source-$s-boot.service
done
sudo rm /etc/systemd/system/data-source-{btceth,altcoin-spot,altcoin-um,option}.*
sudo systemctl daemon-reload
```
