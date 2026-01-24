# Binance Data API

## Base URL

```
https://data.binance.vision/
```

## Endpoints

### Spot Market

| Period | URL Pattern |
|--------|-------------|
| Monthly | `https://data.binance.vision/data/spot/monthly/aggTrades/{SYMBOL}/{SYMBOL}-aggTrades-{YYYY}-{MM}.zip` |
| Daily | `https://data.binance.vision/data/spot/daily/aggTrades/{SYMBOL}/{SYMBOL}-aggTrades-{YYYY}-{MM}-{DD}.zip` |

**Examples:**
```
https://data.binance.vision/data/spot/monthly/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2024-12.zip
https://data.binance.vision/data/spot/daily/aggTrades/0GUSDT/0GUSDT-aggTrades-2026-01-23.zip
```

### USDT-M Futures

| Period | URL Pattern |
|--------|-------------|
| Monthly | `https://data.binance.vision/data/futures/um/monthly/aggTrades/{SYMBOL}/{SYMBOL}-aggTrades-{YYYY}-{MM}.zip` |
| Daily | `https://data.binance.vision/data/futures/um/daily/aggTrades/{SYMBOL}/{SYMBOL}-aggTrades-{YYYY}-{MM}-{DD}.zip` |

**Examples:**
```
https://data.binance.vision/data/futures/um/monthly/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2024-12.zip
https://data.binance.vision/data/futures/um/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2026-01-23.zip
```

### COIN-M Futures

| Period | URL Pattern |
|--------|-------------|
| Monthly | `https://data.binance.vision/data/futures/cm/monthly/aggTrades/{SYMBOL}/{SYMBOL}-aggTrades-{YYYY}-{MM}.zip` |
| Daily | `https://data.binance.vision/data/futures/cm/daily/aggTrades/{SYMBOL}/{SYMBOL}-aggTrades-{YYYY}-{MM}-{DD}.zip` |

**Examples:**
```
https://data.binance.vision/data/futures/cm/monthly/aggTrades/BTCUSD_PERP/BTCUSD_PERP-aggTrades-2024-12.zip
https://data.binance.vision/data/futures/cm/daily/aggTrades/BTCUSD_PERP/BTCUSD_PERP-aggTrades-2026-01-23.zip
```

## File Format

### ZIP Contents

Each ZIP file contains a single CSV file with the same base name:
```
BTCUSDT-aggTrades-2024-12.zip
└── BTCUSDT-aggTrades-2024-12.csv
```

### CSV Schema (aggTrades)

| Column | Type | Description |
|--------|------|-------------|
| agg_trade_id | i64 | Aggregated trade ID |
| price | f64 | Trade price |
| quantity | f64 | Trade quantity |
| first_trade_id | i64 | First trade ID in aggregation |
| last_trade_id | i64 | Last trade ID in aggregation |
| transact_time | i64 | Transaction timestamp (milliseconds or microseconds) |
| is_buyer_maker | bool | True if buyer is market maker |
| is_best_match | bool | (spot only) Best price match |

**Note:** Futures CSV has header row, Spot CSV has no header.

## Download Strategy

### Monthly vs Daily

- **Monthly files**: Available for completed months (more efficient, single file per month)
- **Daily files**: Available for current month and recent data

### Recommended Approach

1. Download monthly files for all completed months
2. Download daily files only for current month

## Symbol List API

To get all available symbols:

| Market | API Endpoint |
|--------|--------------|
| Spot | `https://api.binance.com/api/v3/exchangeInfo` |
| USDT-M | `https://fapi.binance.com/fapi/v1/exchangeInfo` |
| COIN-M | `https://dapi.binance.com/dapi/v1/exchangeInfo` |

Response: JSON with `symbols` array containing symbol info.
