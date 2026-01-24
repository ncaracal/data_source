# Directory Structure

## Layout

```
$TRADE_DATA/
└── {exchange}/                      # binance
    └── {market}/                    # spot, future
        └── {market_sub}/            # um, cm (for futures only)
            └── {aggTrades, metrics}/
                ├── _download/       # Downloaded ZIP files
                │   └── {SYMBOL}/
                │       ├── {SYMBOL}-aggTrades-2024-01.zip
                │       ├── {SYMBOL}-aggTrades-2024-02.zip
                │       └── ...
                └── {SYMBOL}/        # Output Parquet files
                    ├── {SYMBOL}_aggTrades_2023.parquet
                    ├── {SYMBOL}_aggTrades_2024.parquet
                    └── {SYMBOL}_aggTrades_2025-01-24.parquet
```

## File Naming Convention

| Type | Pattern | Example |
|------|---------|---------|
| Completed Year | `{SYMBOL}_aggTrades_{YYYY}.parquet` | `BTCUSDT_aggTrades_2024.parquet` |
| Current/Incomplete | `{SYMBOL}_aggTrades_{YYYY-MM-DD}.parquet` | `BTCUSDT_aggTrades_2025-01-24.parquet` |

## Environment Variables (.env)

```env
# Required
TRADE_DATA=/path/to/data

# Optional
LOG_LEVEL=info
MAX_CONCURRENT_DOWNLOADS=4
```
