# Core Types

## CLI Arguments (src/cli.rs)

```rust
use clap::{Parser, ValueEnum};

#[derive(Parser, Debug)]
#[command(name = "data_source")]
#[command(about = "Download exchange market data and convert to Parquet")]
pub struct Args {
    /// Symbols to download (e.g., BTCUSDT ETHUSDT)
    #[arg(short, long, num_args = 1..)]
    symbols: Option<Vec<String>>,

    /// Path to JSON file containing symbols array
    #[arg(long)]
    symbols_json: Option<String>,

    /// Exchange name
    #[arg(short, long, default_value = "binance")]
    pub exchange: String,

    /// Market type, spot or future
    #[arg(short, long, value_enum)]
    pub market: Market,

    /// Market sub type (for futures)
    #[arg(long, value_enum, default_value = "um")]
    pub market_sub: MarketSub,

    /// Data type
    #[arg(short, long, value_enum)]
    pub data_type: DataType,

    /// Start date (YYYY-MM-DD), auto-detect from existing parquet if not provided
    #[arg(long)]
    pub start_date: Option<String>,

    /// Max concurrent downloads
    #[arg(long, default_value = "4")]
    pub concurrency: usize,

    /// Only download, skip parquet conversion
    #[arg(long)]
    pub download_only: bool,

    /// Only convert existing ZIPs to parquet
    #[arg(long)]
    pub convert_only: bool,
}

impl Args {
    /// Get symbols from either --symbols or --symbols-json
    pub fn get_symbols(&self) -> Result<Vec<String>, String>;
}

#[derive(ValueEnum, Clone, Debug)]
pub enum Market {
    Spot,
    Future,
}

#[derive(ValueEnum, Clone, Debug)]
pub enum MarketSub {
    Um,  // USDT-M Futures
    Cm,  // COIN-M Futures
}

#[derive(ValueEnum, Clone, Debug)]
pub enum DataType {
    AggTrades,
    Trades,
    Metrics,
}
```


## Error Types (src/error.rs)

```rust
use thiserror::Error;

#[derive(Error, Debug)]
pub enum AppError {
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Parquet error: {0}")]
    Polars(#[from] polars::error::PolarsError),

    #[error("ZIP extraction failed: {0}")]
    Zip(#[from] zip::result::ZipError),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("File not found on server: {0}")]
    NotFound(String),

    #[error("Unsupported exchange: {0}")]
    UnsupportedExchange(String),

    #[error("Parse error: {0}")]
    Parse(String),
}

pub type Result<T> = std::result::Result<T, AppError>;
```
