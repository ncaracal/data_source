use clap::{Parser, ValueEnum};

#[derive(Parser, Debug)]
#[command(name = "data_source")]
#[command(about = "Download exchange market data and convert to Parquet")]
pub struct Args {
    /// Symbols to download (e.g., BTCUSDT ETHUSDT)
    #[arg(short, long, num_args = 1.., required = true)]
    pub symbols: Vec<String>,

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

    /// Enable verbose logging
    #[arg(short, long)]
    pub verbose: bool,

    /// Trade data folder (or from env TRADE_DATA), default at ./data/
    #[arg(long)]
    pub trade_data: Option<String>,
}

#[derive(ValueEnum, Clone, Debug, Copy, PartialEq, Eq)]
pub enum Market {
    Spot,
    Future,
}

impl std::fmt::Display for Market {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Market::Spot => write!(f, "spot"),
            Market::Future => write!(f, "futures"),
        }
    }
}

#[derive(ValueEnum, Clone, Debug, Copy, PartialEq, Eq)]
pub enum MarketSub {
    Um,
    Cm,
}

impl std::fmt::Display for MarketSub {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MarketSub::Um => write!(f, "um"),
            MarketSub::Cm => write!(f, "cm"),
        }
    }
}

#[derive(ValueEnum, Clone, Debug, Copy, PartialEq, Eq)]
pub enum DataType {
    #[value(name = "aggTrades")]
    AggTrades,
    #[value(name = "trades")]
    Trades,
    #[value(name = "metrics")]
    Metrics,
}

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::AggTrades => write!(f, "aggTrades"),
            DataType::Trades => write!(f, "trades"),
            DataType::Metrics => write!(f, "metrics"),
        }
    }
}
