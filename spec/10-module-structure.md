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

## Cargo.toml

```toml
[package]
name = "data_source"
version = "0.1.0"
edition = "2021"

[dependencies]
tokio = { version = "1", features = ["full"] }
clap = { version = "4", features = ["derive"] }
dotenvy = "0.15"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
polars = { version = "0.46", features = ["lazy", "parquet", "csv", "dtype-datetime"] }
reqwest = { version = "0.12", features = ["stream"] }
chrono = { version = "0.4", features = ["serde"] }
zip = "2.2"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
anyhow = "1"
thiserror = "2"
futures = "0.3"
```
