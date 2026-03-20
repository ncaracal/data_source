use clap::Parser;
pub use clap::ValueEnum;
use std::path::Path;

#[derive(Parser, Debug)]
#[command(name = "data_source")]
#[command(about = "Download exchange market data and convert to Parquet")]
pub struct Args {
    /// Symbols to download (e.g., BTCUSDT ETHUSDT)
    #[arg(short, long, num_args = 1..)]
    symbols: Option<Vec<String>>,

    /// Path to JSON file containing symbols array (e.g., ["BTCUSDT", "ETHUSDT"])
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
    #[value(name = "metrics", alias = "metric")]
    Metrics,
    #[value(name = "fundingRate")]
    FundingRate,
}

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::AggTrades => write!(f, "aggTrades"),
            DataType::Trades => write!(f, "trades"),
            DataType::Metrics => write!(f, "metrics"),
            DataType::FundingRate => write!(f, "fundingRate"),
        }
    }
}

impl Args {
    /// Get symbols from either --symbols or --symbols-json
    pub fn get_symbols(&self) -> Result<Vec<String>, String> {
        let mut symbols = self.symbols.clone().unwrap_or_default();

        // Read from JSON file if provided
        if let Some(ref json_path) = self.symbols_json {
            let path = Path::new(json_path);
            if !path.exists() {
                return Err(format!("Symbols JSON file not found: {}", json_path));
            }

            let content = std::fs::read_to_string(path)
                .map_err(|e| format!("Failed to read symbols JSON: {}", e))?;

            let json_symbols: Vec<String> = serde_json::from_str(&content)
                .map_err(|e| format!("Failed to parse symbols JSON: {}", e))?;

            symbols.extend(json_symbols);
        }

        if symbols.is_empty() {
            return Err("No symbols provided. Use --symbols or --symbols-json".to_string());
        }

        Ok(symbols)
    }
}
