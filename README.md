# data_source

A high-performance CLI tool for downloading and converting Binance market data to Parquet format.

## Features

- Download historical trading data from Binance (spot and futures markets)
- Support for multiple data types: `aggTrades`, `trades`, `metrics`
- Convert CSV data to Apache Parquet with ZSTD compression
- Smart download strategy: prioritizes monthly archives, falls back to daily
- Resume incomplete downloads automatically
- Merge and deduplicate data intelligently
- Parallel processing with configurable concurrency

## Building

```bash
cargo build --release
```

The binary will be available at `target/release/data_source`.

## Usage

```
cargo run --release -- [OPTIONS] -m <MARKET> -d <DATA_TYPE>
```

### Required Arguments

| Argument | Description |
|----------|-------------|
| `-m, --market <MARKET>` | Market type: `spot` or `future` |
| `-d, --data-type <TYPE>` | Data type: `aggTrades`, `trades`, or `metrics` (Note: `metrics` is only supported for futures market) |

### Symbol Selection (one required)

| Argument | Description |
|----------|-------------|
| `-s, --symbols <SYMBOL>...` | One or more symbols (e.g., `BTCUSDT ETHUSDT`) |
| `--symbols-json <PATH>` | Path to JSON file containing symbol array |

### Optional Arguments

| Argument | Description | Default |
|----------|-------------|---------|
| `-e, --exchange <EXCHANGE>` | Exchange name | `binance` |
| `--market-sub <SUB>` | Futures sub-market: `um` (USDT-M) or `cm` (COIN-M) | `um` |
| `--trade-data <PATH>` | Output directory | `./data` or `$TRADE_DATA` |
| `--start-date <YYYY-MM-DD>` | Start date for download | Auto-detected |
| `--concurrency <NUM>` | Max concurrent downloads | `4` |
| `--download-only` | Skip parquet conversion | - |
| `--convert-only` | Skip download, process existing ZIPs | - |
| `-v, --verbose` | Enable debug logging | - |

## Examples

### Download spot market data

```bash
# Single symbol
cargo run --release -- -s BTCUSDT -m spot -d aggTrades

# Multiple symbols
cargo run --release -- -s BTCUSDT ETHUSDT BNBUSDT -m spot -d aggTrades
```

### Download futures market data

```bash
# USDT-Margined futures (default)
cargo run --release -- -s BTCUSDT -m future -d aggTrades

# COIN-Margined futures
cargo run --release -- -s BTCUSD_PERP -m future --market-sub cm -d aggTrades

# Download metrics data (futures only)
cargo run --release -- -s BTCUSDT -m future -d metrics
# include TRADE_DATA
TRADE_DATA=./data  cargo run --release -- -s DUSDT -m future -d metrics --start-date 2025-01-01
```

### Use symbols from JSON file

```bash
# symbols.json: ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
cargo run --release -- --symbols-json symbols.json -m spot -d aggTrades
```

### Custom output directory

```bash
cargo run --release -- -s BTCUSDT -m spot -d aggTrades --trade-data /data/trade
```

### Resume from specific date

```bash
cargo run --release -- -s BTCUSDT -m spot -d aggTrades --start-date 2024-01-01
```

### Download only (skip conversion)

```bash
cargo run --release -- -s BTCUSDT -m spot -d aggTrades --download-only
```

### Convert only (process existing ZIPs)

```bash
cargo run --release -- -s BTCUSDT -m spot -d aggTrades --convert-only
```

### Higher concurrency

```bash
cargo run --release -- -s BTCUSDT -m spot -d aggTrades --concurrency 8
```

## Output Structure

```
$TRADE_DATA/
└── binance/
    ├── spot/
    │   └── aggTrades/
    │       └── BTCUSDT/
    │           ├── BTCUSDT_aggTrades_2023.parquet
    │           ├── BTCUSDT_aggTrades_2024.parquet
    │           └── BTCUSDT_aggTrades_2025.parquet
    └── futures/
        ├── um/
        │   └── aggTrades/
        │       └── BTCUSDT/
        └── cm/
            └── aggTrades/
                └── BTCUSD_PERP/
```

Data is organized by year in Parquet files with ZSTD compression.

## Environment Variables

| Variable | Description |
|----------|-------------|
| `TRADE_DATA` | Default output directory (overridden by `--trade-data`) |