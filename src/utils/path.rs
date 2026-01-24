use std::path::PathBuf;
use crate::cli::{DataType, Market, MarketSub};

/// Build the target folder path for parquet files
/// $TRADE_DATA/{exchange}/{market}/{market_sub}/{data_type}/{symbol}/
pub fn build_target_folder(
    trade_data: &str,
    exchange: &str,
    market: Market,
    market_sub: MarketSub,
    data_type: DataType,
    symbol: &str,
) -> PathBuf {
    let mut path = PathBuf::from(trade_data);
    path.push(exchange);

    match market {
        Market::Spot => {
            path.push("spot");
            path.push(data_type.to_string());
        }
        Market::Future => {
            path.push("futures");
            path.push(market_sub.to_string());
            path.push(data_type.to_string());
        }
    }

    path.push(symbol);
    path
}

/// Build the download folder path for ZIP files
/// $TRADE_DATA/{exchange}/{market}/{market_sub}/{data_type}/_download/{symbol}/
pub fn build_download_folder(
    trade_data: &str,
    exchange: &str,
    market: Market,
    market_sub: MarketSub,
    data_type: DataType,
    symbol: &str,
) -> PathBuf {
    let mut path = PathBuf::from(trade_data);
    path.push(exchange);

    match market {
        Market::Spot => {
            path.push("spot");
            path.push(data_type.to_string());
        }
        Market::Future => {
            path.push("futures");
            path.push(market_sub.to_string());
            path.push(data_type.to_string());
        }
    }

    path.push("_download");
    path.push(symbol);
    path
}

/// Build parquet filename
/// Completed year: {SYMBOL}_aggTrades_{YYYY}.parquet
/// Current/Incomplete: {SYMBOL}_aggTrades_{YYYY-MM-DD}.parquet
pub fn build_parquet_filename(symbol: &str, data_type: DataType, year: i32, date_suffix: Option<&str>) -> String {
    match date_suffix {
        Some(suffix) => format!("{}_{}_{}_{}.parquet", symbol, data_type, year, suffix),
        None => format!("{}_{}_{}.parquet", symbol, data_type, year),
    }
}

#[allow(dead_code)]
/// Extract year from parquet filename
pub fn extract_year_from_parquet_filename(filename: &str) -> Option<i32> {
    // Pattern: {SYMBOL}_aggTrades_{YYYY}.parquet or {SYMBOL}_aggTrades_{YYYY-MM-DD}.parquet
    let parts: Vec<&str> = filename.trim_end_matches(".parquet").split('_').collect();
    if parts.len() >= 3 {
        // The year is in the third part (index 2) for {SYMBOL}_aggTrades_{YYYY}
        // Or extract from {YYYY-MM-DD}
        let date_part = parts[2];
        if date_part.contains('-') {
            // YYYY-MM-DD format
            date_part.split('-').next()?.parse().ok()
        } else {
            // Just YYYY
            date_part.parse().ok()
        }
    } else {
        None
    }
}
