# Module Structure

## Project Layout

```
src/
├── main.rs                 # Entry point
├── error.rs                # Custom error types
├── exchange/
│   ├── mod.rs              # Exchange trait
│   └── binance/
│       ├── mod.rs
│       ├── client.rs       # Binance HTTP client
│       └── urls.rs         # URL construction
├── downloader/
│   ├── mod.rs
│   ├── client.rs           # Generic download logic
│   └── types.rs            # Download-related types
├── converter/
│   ├── mod.rs
│   ├── parser.rs           # CSV parsing logic
│   └── parquet.rs          # Polars DataFrame & Parquet operations
├── models/
│   ├── mod.rs
│   ├── agg_trade.rs        # AggTrade struct
│   └── market.rs           # Market type enums
└── utils/
    ├── mod.rs
    ├── date.rs             # Date utilities
    └── path.rs             # Path construction
```

## Technology Stack

| Component | Crate | Purpose |
|-----------|-------|---------|
| Async Runtime | `tokio` | Async I/O, concurrent downloads |
| CLI Arguments | `clap` | Command-line argument parsing |
| Environment | `dotenvy` | Load `.env` configuration |
| Logging | `tracing` + `tracing-subscriber` | Structured logging |
| Parquet | `polars` | DataFrame operations & Parquet I/O |
| HTTP Client | `reqwest` | Async HTTP downloads |
| Date/Time | `chrono` | Date manipulation |
| Compression | `zip` | Extract ZIP archives |
