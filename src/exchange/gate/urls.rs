use crate::cli::{Market, MarketSub};
use chrono::{Datelike, NaiveDate};

const HISTORICAL_BASE: &str = "https://download.gatedata.org";

/// Path segment under `download.gatedata.org/` for a given (market, sub).
/// Returns `None` for combinations gate doesn't publish historicals for.
fn historical_path(market: Market, market_sub: MarketSub) -> Option<&'static str> {
    match (market, market_sub) {
        (Market::Spot, _) => Some("spot/deals"),
        (Market::Future, MarketSub::Um) => Some("futures_usdt/trades"),
        // CM (USD-margined coin futures) and option not implemented.
        _ => None,
    }
}

/// Known quote-currency suffixes used on Gate spot.
/// Sorted by length (longest first) so that e.g. `USDC` matches before `USD`.
const KNOWN_QUOTES: &[&str] = &[
    "USDT", "USDC", "TUSD", "BUSD", "FDUSD", "DAI", "USD", "BTC", "ETH", "BNB", "TRY", "EUR",
];

/// Convert a Binance-style symbol (e.g. `BUSDT`) to a Gate-style pair (e.g. `B_USDT`).
/// If the symbol already contains `_`, return it unchanged.
pub fn to_gate_pair(symbol: &str) -> String {
    if symbol.contains('_') {
        return symbol.to_string();
    }
    for q in KNOWN_QUOTES {
        if symbol.ends_with(q) && symbol.len() > q.len() {
            let base = &symbol[..symbol.len() - q.len()];
            return format!("{}_{}", base, q);
        }
    }
    symbol.to_string()
}

/// Build URL for the monthly archive of a given (market, sub).
///
/// Examples:
///   spot       : https://download.gatedata.org/spot/deals/202604/B_USDT-202604.csv.gz
///   futures um : https://download.gatedata.org/futures_usdt/trades/202603/B_USDT-202603.csv.gz
pub fn build_monthly_url(
    symbol: &str,
    market: Market,
    market_sub: MarketSub,
    year: i32,
    month: u32,
) -> String {
    let pair = to_gate_pair(symbol);
    let path = historical_path(market, market_sub)
        .expect("gate market combination has no historical archive");
    format!(
        "{}/{}/{:04}{:02}/{}-{:04}{:02}.csv.gz",
        HISTORICAL_BASE, path, year, month, pair, year, month
    )
}

/// Build local filename for a monthly spot deals archive.
/// Format: `{PAIR}-{YYYYMM}.csv.gz` (Gate native naming).
pub fn build_monthly_filename(symbol: &str, year: i32, month: u32) -> String {
    let pair = to_gate_pair(symbol);
    format!("{}-{:04}{:02}.csv.gz", pair, year, month)
}

/// Extract (year, month) from a Gate monthly filename like `B_USDT-202604.csv.gz`.
pub fn extract_year_month(filename: &str) -> Option<(i32, u32)> {
    let stem = filename
        .trim_end_matches(".csv.gz")
        .trim_end_matches(".gz")
        .trim_end_matches(".csv");
    let dash = stem.rfind('-')?;
    let yyyymm = &stem[dash + 1..];
    if yyyymm.len() != 6 {
        return None;
    }
    let year: i32 = yyyymm[0..4].parse().ok()?;
    let month: u32 = yyyymm[4..6].parse().ok()?;
    Some((year, month))
}

/// Extract year from a Gate monthly filename.
pub fn extract_year(filename: &str) -> Option<i32> {
    extract_year_month(filename).map(|(y, _)| y)
}

/// Last day of `(year, month)`.
pub fn last_day_of_month(year: i32, month: u32) -> NaiveDate {
    let next = if month == 12 {
        NaiveDate::from_ymd_opt(year + 1, 1, 1).unwrap()
    } else {
        NaiveDate::from_ymd_opt(year, month + 1, 1).unwrap()
    };
    next.pred_opt().unwrap()
}

/// True if `date` falls in the same (year, month) as `today`.
pub fn is_current_month(date: NaiveDate, today: NaiveDate) -> bool {
    date.year() == today.year() && date.month() == today.month()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pair_conversion() {
        assert_eq!(to_gate_pair("BUSDT"), "B_USDT");
        assert_eq!(to_gate_pair("BTCUSDT"), "BTC_USDT");
        assert_eq!(to_gate_pair("ETHBTC"), "ETH_BTC");
        assert_eq!(to_gate_pair("B_USDT"), "B_USDT"); // already gate-style
    }

    #[test]
    fn monthly_url_spot() {
        let u = build_monthly_url("BUSDT", Market::Spot, MarketSub::Um, 2026, 4);
        assert_eq!(
            u,
            "https://download.gatedata.org/spot/deals/202604/B_USDT-202604.csv.gz"
        );
    }

    #[test]
    fn monthly_url_futures_um() {
        let u = build_monthly_url("BUSDT", Market::Future, MarketSub::Um, 2026, 3);
        assert_eq!(
            u,
            "https://download.gatedata.org/futures_usdt/trades/202603/B_USDT-202603.csv.gz"
        );
    }

    #[test]
    fn extract_ym() {
        assert_eq!(extract_year_month("B_USDT-202604.csv.gz"), Some((2026, 4)));
        assert_eq!(extract_year_month("BTC_USDT-202001.csv.gz"), Some((2020, 1)));
    }
}
