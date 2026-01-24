# Data Source Downloader - Overview

A high-performance Rust CLI tool to download exchange market data (aggTrades) and convert to Parquet format using async I/O and columnar storage.

## Supported Exchanges

| Exchange | Status |
|----------|--------|
| Binance | Supported |

## Workflow

*Folder* 

Target_Folder:  $TRADE_DATA/{exchange}/{market}/{market_sub}/{aggTrades,metrics}/{symbol}/

Download_Folder: $TRADE_DATA/{exchange}/{market}/{market_sub}/{aggTrades,metrics}/_download/{symbol}/


### Step 1. Download Phase

```
┌─────────────────────────────────────────────────────────────┐
│                    Download Workflow                         │
├─────────────────────────────────────────────────────────────┤
│ 1. Parse CLI args & load .env                               │
│ 2. Determine start date:                                    │
│    - If --start-date provided: use it                       │
│    - Else check existing parquet in {target_folder}         │
│    - If no parquet found: default to 2020-01-01             │
│ 3. Skip if existing in  {download_folder}   (resume support)│
│ 4. Build download URL list                                  │
│    (skip daily ZIPs when monthly exists)                    │
│   ex: 
│    Monthly: https://data.binance.vision/data/spot/monthly/  │
│             aggTrades/{SYMBOL}/{SYMBOL}-aggTrades-YYYY-MM.zip│
│    Daily:   https://data.binance.vision/data/spot/daily/    │
│             aggTrades/{SYMBOL}/{SYMBOL}-aggTrades-YYYY-MM-DD.zip│
│    NOTE: if the downoad URL not existed, just ignore        │
│ 5. Download concurrently                                    │
│ 6. Save to: {Download_Folder}/{original_file_name}          │
└─────────────────────────────────────────────────────────────┘
```

### Step 2. Convert Phase

```
┌─────────────────────────────────────────────────────────────┐
│                    Convert Workflow                         │
├─────────────────────────────────────────────────────────────┤
│ 1. Scan {Download_Folder} for ZIP files                     │
│ 2. Check existing parquet files in {Target_Folder} (incremental update)        │
│ 3. Filter: skip daily ZIPs when monthly exists              │
│ 4. Process ZIPs in chronological order                      │
│ 5. Extract CSV → Parse with Polars (lazy/streaming)         │
│ 6. Merge with existing parquet data                         │
│ 7. if monthly zip exist, delete THE Month data in parquet, use the monthly data                     │
│  ex: {Download_Folder}/BTCUSDC-aggTrades-2025-12.zip        │
│    {Target_Folder}/BTCUSDC_aggTrades_2025-12-29.parquet     │
│    remove whole December data in parquet                    │
│ 8. Write yearly parquet files                               │
│ 9. Current year: include date in filename                   │
│    ex: BTCUSDC_aggTrades_2021.parquet  BTCUSDC_aggTrades_2023.parquet  BTCUSDC_aggTrades_2025-12-29.parquet                          │
└─────────────────────────────────────────────────────────────┘
```

NOTE: compression method ZSTD

```
  Example (RVNUSDT_aggTrades_2024):                                                             
  - SNAPPY: 226 MB                                                                              
  - ZSTD: 110 MB (~51% smaller)                                                                 
```                                        


## Features

- **Async Downloads**: Concurrent downloads with configurable parallelism
- **Resume Support**: Skip already downloaded ZIP files
- **Incremental Updates**: Merge new data with existing parquet files
- **Auto Start Date**: Detect last date from existing parquet, continue from there
- **Yearly Partitioning**: Organize output by year for efficient queries
- **Memory Efficient**: Streaming/lazy processing for large datasets