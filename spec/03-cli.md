# CLI Interface

## Usage

```bash
data_source [OPTIONS] --market <MARKET> --data-type <DATA_TYPE> --symbols <SYMBOLS>...

Options:
  -e, --exchange <EXCHANGE>        Exchange name [default: binance]
  -s, --symbols <SYMBOLS>...       Symbols to download (e.g., BTCUSDT ETHUSDT) [required]
  -m, --market <MARKET>            Market type: spot, future [required]
      --market-sub <MARKET_SUB>    Market sub type: um, cm [default: um]
  -d, --data-type <DATA_TYPE>      Data type: aggTrades, trades, klines [required]
  -i, --interval <INTERVAL>        Kline interval (for klines only): 1m, 5m, 1h, 1d
      --start-date <DATE>          Start date (YYYY-MM-DD) [auto-detect from existing parquet]
      --download-only              Only download, skip parquet conversion
      --convert-only               Only convert existing ZIPs to parquet
      --concurrency <N>            Max concurrent downloads [default: 4]
  -v, --verbose                    Enable verbose logging
  -h, --help                       Print help
  -V, --version                    Print version
  --trade-data                     trade data folder (or from env TRADE_DATA), default at ./data/
```

## Start Date Logic

```
if --start-date provided:
    use provided date
else if parquet exists in $TRADE_DATA/{exchange}/{market}/aggTrades/{SYMBOL}/:
    start from last_date + 1 day
else:
    start from 2020-01-01
```

## Examples

```bash
# Download and convert BTCUSDT spot aggTrades (auto-detect start date)
data_source -s BTCUSDT -m spot -d aggTrades

# Download multiple symbols
data_source -s BTCUSDT ETHUSDT -m spot -d aggTrades

# Specify exchange explicitly
data_source -e binance -s BTCUSDT -m spot -d aggTrades

# Download USDT-M futures
data_source -s BTCUSDT -m future --market-sub um -d aggTrades

# Download COIN-M futures
data_source -s BTCUSD_PERP -m future --market-sub cm -d aggTrades

# Force start from specific date
data_source -s BTCUSDT -m spot -d aggTrades --start-date 2024-01-01

# Download only (no conversion)
data_source -s BTCUSDT -m spot -d aggTrades --download-only

# Convert existing raw data only
data_source -s BTCUSDT -m spot -d aggTrades --convert-only

# High concurrency download
data_source -s BTCUSDT -m spot -d aggTrades --concurrency 8
```
