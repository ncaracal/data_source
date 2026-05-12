# data-source-option service

Systemd service that downloads and aggregates Binance trade data daily at 16:00.

## Jobs

Runs sequentially:

1. **aggTrades future/um** - `data_source -s BTCUSDT ETHUSDT -m future --market-sub um -d aggTrades`
2. **aggTrades spot** - `data_source -s BTCUSDT ETHUSDT -m spot -d aggTrades`
3. **BVOLIndex** - `data_source -s ETHBVOLUSDT -m option -d BVOLIndex`
4. **agg_kline future/um** - `agg_kline -m um -s BTCUSDT -s ETHUSDT --kline-1s`
5. **agg_kline spot** - `agg_kline -m spot -s BTCUSDT -s ETHUSDT --kline-1s`
6. **altcoin download+verify spot** - `python/altcoin_symbols_download_and_verify.py --exchange binance --market spot` (non-fatal)
7. **altcoin download+verify um** - `python/altcoin_symbols_download_and_verify.py --exchange binance --market um` (non-fatal)
8. **altcoin → kline spot** - `python/altcoin_symbols_to_kline.py --exchange binance --market spot` (non-fatal)
9. **altcoin → kline um** - `python/altcoin_symbols_to_kline.py --exchange binance --market um` (non-fatal)
10. **verify** - `python/verify_with_binance_kline_api_last_day.py` for BTC/ETH spot+um (non-fatal)

Steps 6–7 refresh `exchange_info_*.json`, then download every TRADING USDT
symbol (minus the majors in `python/altcoin_symbols_except.json`) and run
`verify_all_symbols_id`. Steps 8–9 then run `agg_kline` over the same
symbol set to populate `aggTrades_kline/`. Output goes to
`report/running_log/altcoin_symbols_download_and_verify.{UTC}.log`,
`report/running_log/altcoin_symbols_to_kline.{UTC}.log`, and
`report/verify_all_symbols_id/binance_{spot,um}.{UTC}.report`.

## Files

| File | Purpose |
|---|---|
| `data-source-option-run.sh` | Wrapper script that runs all 3 jobs and records last run timestamp |
| `data-source-option.service` | Oneshot systemd service |
| `data-source-option.timer` | Systemd timer - triggers daily at 08:00 UTC (`Persistent=true`) |
| `data-source-option-boot.service` | Runs on boot if last successful run was > 24h ago |
| `install.sh` | Installs and enables all units |

## Install

```bash
bash service/option/install.sh
```

## Commands

```bash
# Check timer status
systemctl status data-source-option.timer

# See next scheduled run
systemctl list-timers data-source-option.timer

# View logs
journalctl -u data-source-option.service

# Manual run
sudo systemctl start data-source-option.service

# Uninstall
sudo systemctl disable --now data-source-option.timer
sudo systemctl disable data-source-option-boot.service
sudo rm /etc/systemd/system/data-source-option.*
sudo systemctl daemon-reload
```


```
Status:  systemctl status data-source-option.timer
Next:    systemctl list-timers data-source-option.timer
Logs:    journalctl -u data-source-option.service
Run now: sudo systemctl start data-source-option.service
```