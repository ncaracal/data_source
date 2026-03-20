use crate::cli::{DataType, Market, MarketSub};
use chrono::NaiveDate;

const BASE_URL: &str = "https://data.binance.vision";

/// Build monthly download URL
/// Spot: https://data.binance.vision/data/spot/monthly/aggTrades/{SYMBOL}/{SYMBOL}-aggTrades-{YYYY}-{MM}.zip
/// Futures UM: https://data.binance.vision/data/futures/um/monthly/aggTrades/{SYMBOL}/{SYMBOL}-aggTrades-{YYYY}-{MM}.zip
/// Futures CM: https://data.binance.vision/data/futures/cm/monthly/aggTrades/{SYMBOL}/{SYMBOL}-aggTrades-{YYYY}-{MM}.zip
pub fn build_monthly_url(
    symbol: &str,
    market: Market,
    market_sub: MarketSub,
    data_type: DataType,
    year: i32,
    month: u32,
) -> String {
    let market_path = match market {
        Market::Spot => "spot".to_string(),
        Market::Future => format!("futures/{}", market_sub),
        Market::Option => "option".to_string(),
    };

    format!(
        "{}/data/{}/monthly/{}/{}/{}-{}-{:04}-{:02}.zip",
        BASE_URL, market_path, data_type, symbol, symbol, data_type, year, month
    )
}

/// Build daily download URL
pub fn build_daily_url(
    symbol: &str,
    market: Market,
    market_sub: MarketSub,
    data_type: DataType,
    date: NaiveDate,
) -> String {
    let market_path = match market {
        Market::Spot => "spot".to_string(),
        Market::Future => format!("futures/{}", market_sub),
        Market::Option => "option".to_string(),
    };

    format!(
        "{}/data/{}/daily/{}/{}/{}-{}-{:04}-{:02}-{:02}.zip",
        BASE_URL,
        market_path,
        data_type,
        symbol,
        symbol,
        data_type,
        date.year(),
        date.month(),
        date.day()
    )
}

use chrono::Datelike;

/// Build ZIP filename for monthly file
pub fn build_monthly_zip_filename(symbol: &str, data_type: DataType, year: i32, month: u32) -> String {
    format!("{}-{}-{:04}-{:02}.zip", symbol, data_type, year, month)
}

/// Build ZIP filename for daily file
pub fn build_daily_zip_filename(symbol: &str, data_type: DataType, date: NaiveDate) -> String {
    format!(
        "{}-{}-{:04}-{:02}-{:02}.zip",
        symbol,
        data_type,
        date.year(),
        date.month(),
        date.day()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spot_monthly_url() {
        let url = build_monthly_url("BTCUSDT", Market::Spot, MarketSub::Um, DataType::AggTrades, 2024, 12);
        assert_eq!(
            url,
            "https://data.binance.vision/data/spot/monthly/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2024-12.zip"
        );
    }

    #[test]
    fn test_futures_um_monthly_url() {
        let url = build_monthly_url("BTCUSDT", Market::Future, MarketSub::Um, DataType::AggTrades, 2024, 12);
        assert_eq!(
            url,
            "https://data.binance.vision/data/futures/um/monthly/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2024-12.zip"
        );
    }

    #[test]
    fn test_spot_daily_url() {
        let date = NaiveDate::from_ymd_opt(2024, 12, 25).unwrap();
        let url = build_daily_url("BTCUSDT", Market::Spot, MarketSub::Um, DataType::AggTrades, date);
        assert_eq!(
            url,
            "https://data.binance.vision/data/spot/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2024-12-25.zip"
        );
    }
}
