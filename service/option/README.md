# data-source-option service

Systemd service that downloads and aggregates Binance trade data daily at 15:00.

## Jobs

Runs sequentially:

1. **aggTrades** - `data_source -s BTCUSDT ETHUSDT -m future --market-sub um -d aggTrades`
2. **BVOLIndex** - `data_source -s ETHBVOLUSDT -m option -d BVOLIndex`
3. **agg_kline** - `agg_kline -m um -s BTCUSDT -s ETHUSDT`

## Files

| File | Purpose |
|---|---|
| `data-source-option-run.sh` | Wrapper script that runs all 3 jobs and records last run timestamp |
| `data-source-option.service` | Oneshot systemd service |
| `data-source-option.timer` | Systemd timer - triggers daily at 15:00 (`Persistent=true`) |
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