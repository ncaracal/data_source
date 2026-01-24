use chrono::{Datelike, NaiveDate};

/// Generate list of monthly dates from start to end (inclusive)
pub fn generate_monthly_dates(start: NaiveDate, end: NaiveDate) -> Vec<NaiveDate> {
    let mut dates = Vec::new();
    let mut current = NaiveDate::from_ymd_opt(start.year(), start.month(), 1).unwrap();

    while current <= end {
        dates.push(current);
        // Move to next month
        if current.month() == 12 {
            current = NaiveDate::from_ymd_opt(current.year() + 1, 1, 1).unwrap();
        } else {
            current = NaiveDate::from_ymd_opt(current.year(), current.month() + 1, 1).unwrap();
        }
    }

    dates
}

/// Generate list of daily dates from start to end (inclusive)
pub fn generate_daily_dates(start: NaiveDate, end: NaiveDate) -> Vec<NaiveDate> {
    let mut dates = Vec::new();
    let mut current = start;

    while current <= end {
        dates.push(current);
        current = current.succ_opt().unwrap();
    }

    dates
}

/// Check if a month is complete (all days have passed)
pub fn is_month_complete(date: NaiveDate, today: NaiveDate) -> bool {
    let next_month = if date.month() == 12 {
        NaiveDate::from_ymd_opt(date.year() + 1, 1, 1).unwrap()
    } else {
        NaiveDate::from_ymd_opt(date.year(), date.month() + 1, 1).unwrap()
    };
    next_month <= today
}

#[allow(dead_code)]
/// Get the last day of a month
pub fn last_day_of_month(year: i32, month: u32) -> NaiveDate {
    let next_month = if month == 12 {
        NaiveDate::from_ymd_opt(year + 1, 1, 1).unwrap()
    } else {
        NaiveDate::from_ymd_opt(year, month + 1, 1).unwrap()
    };
    next_month.pred_opt().unwrap()
}

/// Parse date string in YYYY-MM-DD format
pub fn parse_date(s: &str) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(s, "%Y-%m-%d").ok()
}

/// Extract year from filename (works for both daily and monthly files)
pub fn extract_year_from_filename(filename: &str) -> Option<i32> {
    let parts: Vec<&str> = filename.split('-').collect();
    if parts.len() >= 5 {
        // Daily: BTCUSDT-aggTrades-2024-12-25.zip
        parts[parts.len() - 3].parse().ok()
    } else if parts.len() >= 4 {
        // Monthly: BTCUSDT-aggTrades-2024-12.zip
        parts[parts.len() - 2].parse().ok()
    } else {
        None
    }
}

/// Extract year-month from filename like BTCUSDT-aggTrades-2024-12.zip
pub fn extract_year_month_from_filename(filename: &str) -> Option<(i32, u32)> {
    let parts: Vec<&str> = filename.split('-').collect();
    if parts.len() >= 4 {
        let year: i32 = parts[parts.len() - 2].parse().ok()?;
        let month_str = parts[parts.len() - 1].trim_end_matches(".zip").trim_end_matches(".csv");
        let month: u32 = month_str.parse().ok()?;
        Some((year, month))
    } else {
        None
    }
}

/// Extract year-month-day from filename like BTCUSDT-aggTrades-2024-12-25.zip
pub fn extract_year_month_day_from_filename(filename: &str) -> Option<NaiveDate> {
    let parts: Vec<&str> = filename.split('-').collect();
    if parts.len() >= 5 {
        let year: i32 = parts[parts.len() - 3].parse().ok()?;
        let month: u32 = parts[parts.len() - 2].parse().ok()?;
        let day_str = parts[parts.len() - 1].trim_end_matches(".zip").trim_end_matches(".csv");
        let day: u32 = day_str.parse().ok()?;
        NaiveDate::from_ymd_opt(year, month, day)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_year_month() {
        assert_eq!(
            extract_year_month_from_filename("BTCUSDT-aggTrades-2024-12.zip"),
            Some((2024, 12))
        );
    }

    #[test]
    fn test_extract_year_month_day() {
        assert_eq!(
            extract_year_month_day_from_filename("BTCUSDT-aggTrades-2024-12-25.zip"),
            Some(NaiveDate::from_ymd_opt(2024, 12, 25).unwrap())
        );
    }
}
