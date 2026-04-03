use anyhow::Result;
use chrono::{DateTime, Datelike, Utc};
use clap::Parser;
use polars::prelude::*;
use std::fs;
use std::path::{Path, PathBuf};
use tracing::{info, warn};

/// Aggregate trade ticks into OHLCV klines at ALL intervals in one pass.
///
/// Processes ALL intervals: 1m, 3m, 5m, 15m, 30m, 1h, 4h, 8h, 12h, 1d
///
/// Output: `$TRADE_DATA/binance/spot/aggTrades_kline/{symbol}/{symbol}_kline_{interval}_{year_or_date}.parquet`
/// - Complete year: `BTCUSDT_kline_1m_2024.parquet`
/// - Incomplete year: `BTCUSDT_kline_1m_2026-01-10.parquet` (uses last date)
///
/// ## Output Columns
///
/// | Column            | Description                                      |
/// |-------------------|--------------------------------------------------|
/// | symbol            | Trading symbol (e.g., BTCUSDT)                   |
/// | time              | Bar timestamp                                    |
/// | Open              | Opening price                                    |
/// | High              | Highest price                                    |
/// | Low               | Lowest price                                     |
/// | Close             | Closing price                                    |
/// | qty               | Total quantity traded                            |
/// | qty_usd           | Total USD value (quantity × price)               |
/// | buyer_qty         | Quantity from buyers (is_buyer_maker = False)    |
/// | seller_qty        | Quantity from sellers (is_buyer_maker = True)    |
/// | avg_price         | Average price (qty_usd / qty)                    |
/// | buyer_avg_price   | Buyer's average price                            |
/// | seller_avg_price  | Seller's average price                           |
///
/// ## Usage
/// ```
/// TRADE_DATA=./data cargo run --release --bin agg_kline
/// # or
/// cargo run --release --bin agg_kline -- --data-dir=./data
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, clap::ValueEnum)]
pub enum MarketEnum {
    Spot,
    #[value(alias = "um")]
    FutureUm,
    Option,
}

impl MarketEnum {
    fn data_path(&self) -> PathBuf {
        match self {
            MarketEnum::Spot => PathBuf::from("spot"),
            MarketEnum::FutureUm => PathBuf::from("future").join("um"),
            MarketEnum::Option => PathBuf::from("future").join("option"),
        }
    }
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Base data directory (defaults to TRADE_DATA env var)
    #[arg(long, env = "TRADE_DATA")]
    data_dir: Option<String>,

    /// Market type
    #[arg(short, long, value_enum, default_value = "spot")]
    market: MarketEnum,

    /// Symbols to process (if omitted, processes all symbols found)
    #[arg(short, long)]
    symbols: Vec<String>,

    /// Include 1s kline output (disabled by default due to large output size)
    #[arg(long, default_value_t = false)]
    kline_1s: bool,
}

// Valid tick intervals (1s excluded by default, enabled via --kline-1s flag)
const BASE_INTERVALS: &[&str] = &["1m", "3m", "5m", "15m", "30m", "1h", "4h", "8h", "12h", "1d"];

/// Returns the Duration for each interval
fn interval_to_duration(interval: &str) -> Duration {
    match interval {
        "1s" => Duration::parse("1s"),
        "1m" => Duration::parse("1m"),
        "3m" => Duration::parse("3m"),
        "5m" => Duration::parse("5m"),
        "15m" => Duration::parse("15m"),
        "30m" => Duration::parse("30m"),
        "1h" => Duration::parse("1h"),
        "4h" => Duration::parse("4h"),
        "8h" => Duration::parse("8h"),
        "12h" => Duration::parse("12h"),
        "1d" => Duration::parse("1d"),
        _ => Duration::parse("1m"),
    }
}

/// Find all parquet files in a directory
fn find_all_parquets(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut files: Vec<PathBuf> = fs::read_dir(dir)?
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let path = entry.path();
            if path.is_file() && path.extension().map_or(false, |e| e == "parquet") {
                Some(path)
            } else {
                None
            }
        })
        .collect();

    files.sort();
    Ok(files)
}

/// Extract year from parquet filename or return None
/// Supports: SYMBOL_aggTrades_2024.parquet or SYMBOL_aggTrades_2024-12-28.parquet
fn extract_year_from_filename(filename: &str) -> Option<i32> {
    let name = filename.trim_end_matches(".parquet");
    let parts: Vec<&str> = name.split('_').collect();

    if parts.len() >= 3 {
        // Try to parse the last part as year or date
        let last_part = parts[parts.len() - 1];
        // Check if it's YYYY format
        if last_part.len() == 4 {
            return last_part.parse().ok();
        }
        // Check if it's YYYY-MM-DD format
        if last_part.len() == 10 && last_part.contains('-') {
            return last_part[0..4].parse().ok();
        }
    }
    None
}

/// Prepare the lazy frame with derived columns (computed once, used for all intervals)
fn prepare_lazy_frame(df: DataFrame) -> LazyFrame {
    df.lazy().with_columns([
        // qty_usd = price * quantity
        (col("price") * col("quantity")).alias("qty_usd"),
        // buyer_qty = quantity when NOT is_buyer_maker
        when(col("is_buyer_maker").not())
            .then(col("quantity"))
            .otherwise(lit(0.0))
            .alias("buyer_qty"),
        // seller_qty = quantity when is_buyer_maker
        when(col("is_buyer_maker"))
            .then(col("quantity"))
            .otherwise(lit(0.0))
            .alias("seller_qty"),
        // buyer_qty_usd
        when(col("is_buyer_maker").not())
            .then(col("price") * col("quantity"))
            .otherwise(lit(0.0))
            .alias("buyer_qty_usd"),
        // seller_qty_usd
        when(col("is_buyer_maker"))
            .then(col("price") * col("quantity"))
            .otherwise(lit(0.0))
            .alias("seller_qty_usd"),
    ])
}

/// Aggregate trades to OHLCV bars for a given interval using group_by_dynamic
fn aggregate_trades_to_ohlcv(lf: &LazyFrame, interval: &str) -> Result<DataFrame> {
    let duration = interval_to_duration(interval);

    let grouped = lf
        .clone()
        .group_by_dynamic(
            col("time"),
            [],
            DynamicGroupOptions {
                every: duration.clone(),
                period: duration.clone(),
                offset: Duration::parse("0"),
                closed_window: ClosedWindow::Left,
                label: Label::Left,
                ..Default::default()
            },
        )
        .agg([
            col("price").first().alias("open"),
            col("price").max().alias("high"),
            col("price").min().alias("low"),
            col("price").last().alias("close"),
            col("quantity").sum().alias("qty"),
            col("qty_usd").sum().alias("qty_usd"),
            col("buyer_qty").sum().alias("buyer_qty"),
            col("seller_qty").sum().alias("seller_qty"),
            col("buyer_qty_usd").sum().alias("buyer_qty_usd"),
            col("seller_qty_usd").sum().alias("seller_qty_usd"),
        ])
        .with_columns([
            // avg_price = qty_usd / qty
            (col("qty_usd") / col("qty")).alias("avg_price"),
            // buyer_avg_price = buyer_qty_usd / buyer_qty (null if buyer_qty == 0)
            when(col("buyer_qty").gt(lit(0.0)))
                .then(col("buyer_qty_usd") / col("buyer_qty"))
                .otherwise(lit(NULL))
                .alias("buyer_avg_price"),
            // seller_avg_price = seller_qty_usd / seller_qty (null if seller_qty == 0)
            when(col("seller_qty").gt(lit(0.0)))
                .then(col("seller_qty_usd") / col("seller_qty"))
                .otherwise(lit(NULL))
                .alias("seller_avg_price"),
        ])
        .select([
            col("time"),
            col("open"),
            col("high"),
            col("low"),
            col("close"),
            col("qty"),
            col("qty_usd"),
            col("buyer_qty"),
            col("seller_qty"),
            col("avg_price"),
            col("buyer_avg_price"),
            col("seller_avg_price"),
        ])
        .collect()?;

    Ok(grouped)
}

/// Get year from timestamp in microseconds
fn get_year_from_timestamp_us(ts_us: i64) -> i32 {
    let secs = ts_us / 1_000_000;
    DateTime::<Utc>::from_timestamp(secs, 0)
        .map(|dt| dt.year())
        .unwrap_or(0)
}

/// Check if timestamp is Dec 31 (year complete)
fn is_year_complete(ts_us: i64) -> bool {
    let secs = ts_us / 1_000_000;
    DateTime::<Utc>::from_timestamp(secs, 0)
        .map(|dt| dt.month() == 12 && dt.day() == 31)
        .unwrap_or(false)
}

/// Generate year suffix for filename: "2024" for complete year, "2026-01-10" for incomplete
fn get_year_suffix(year: i32, max_time_us: i64) -> String {
    if is_year_complete(max_time_us) {
        year.to_string()
    } else {
        format_timestamp_us(max_time_us)
    }
}

/// Format timestamp in microseconds to date string
fn format_timestamp_us(ts_us: i64) -> String {
    let secs = ts_us / 1_000_000;
    if let Some(dt) = DateTime::<Utc>::from_timestamp(secs, 0) {
        dt.format("%Y-%m-%d").to_string()
    } else {
        "unknown".to_string()
    }
}

/// Write DataFrame to parquet with ZSTD compression
fn write_parquet_zstd(df: &mut DataFrame, path: &Path) -> Result<()> {
    let file = std::fs::File::create(path)?;
    ParquetWriter::new(file)
        .with_compression(ParquetCompression::Zstd(None))
        .finish(df)?;
    Ok(())
}

/// Process a single parquet file for all intervals, outputting directly to final location
fn process_parquet_file(
    parquet_path: &Path,
    symbol: &str,
    output_symbol_dir: &Path,
    intervals: &[&str],
) -> Result<Vec<PathBuf>> {
    let filename = parquet_path.file_name().unwrap().to_string_lossy();

    // Try to get year from filename first
    let file_year = extract_year_from_filename(&filename);

    // Read the parquet file
    let file = std::fs::File::open(parquet_path)?;
    let df = ParquetReader::new(file).finish()?;

    info!("  {} ({} rows)", filename, df.height());

    // Get the year from data if not in filename
    let time_col = df.column("time")?;
    let first_time = time_col.get(0)?.try_extract::<i64>()?;
    let data_year = get_year_from_timestamp_us(first_time);
    let year = file_year.unwrap_or(data_year);

    // Get max time from source data to determine if we need to update existing files
    let last_time = time_col.get(time_col.len() - 1)?.try_extract::<i64>()?;
    let source_max_date = format_timestamp_us(last_time);

    // Check existing files and determine if update is needed
    let mut files_to_delete: Vec<PathBuf> = Vec::new();
    let mut needs_update = false;

    for interval in intervals {
        // Check for year format (complete year) - if exists, no update needed for this interval
        let year_filename = format!("{}_kline_{}_{}.parquet", symbol, interval, year);
        let year_path = output_symbol_dir.join(&year_filename);
        if year_path.exists() {
            continue;
        }

        // Check for date format (incomplete year) - compare dates
        let pattern = format!("{}_kline_{}_{}-", symbol, interval, year);
        if let Ok(entries) = fs::read_dir(output_symbol_dir) {
            for entry in entries.filter_map(|e| e.ok()) {
                let name = entry.file_name().to_string_lossy().to_string();
                if name.starts_with(&pattern) && name.ends_with(".parquet") {
                    // Extract existing date from filename: {symbol}_kline_{interval}_{YYYY-MM-DD}.parquet
                    let existing_date = name
                        .trim_end_matches(".parquet")
                        .rsplit('_')
                        .next()
                        .unwrap_or("");

                    // Compare dates (string comparison works for YYYY-MM-DD format)
                    if source_max_date.as_str() > existing_date {
                        info!(
                            "    Found newer data for {}-{}: {} > {}",
                            interval, year, source_max_date, existing_date
                        );
                        files_to_delete.push(entry.path());
                        needs_update = true;
                    }
                    break;
                }
            }
        } else {
            needs_update = true;
        }
    }

    // Check if any interval is missing entirely
    for interval in intervals {
        let year_filename = format!("{}_kline_{}_{}.parquet", symbol, interval, year);
        let year_path = output_symbol_dir.join(&year_filename);
        if year_path.exists() {
            continue;
        }

        let pattern = format!("{}_kline_{}_{}-", symbol, interval, year);
        let mut found = false;
        if let Ok(entries) = fs::read_dir(output_symbol_dir) {
            for entry in entries.filter_map(|e| e.ok()) {
                let name = entry.file_name().to_string_lossy().to_string();
                if name.starts_with(&pattern) && name.ends_with(".parquet") {
                    found = true;
                    break;
                }
            }
        }
        if !found {
            needs_update = true;
            break;
        }
    }

    if !needs_update {
        info!("    Skipping (all interval files for {} are up to date)", year);
        return Ok(Vec::new());
    }

    // Delete old files that need to be replaced
    for path in &files_to_delete {
        if let Err(e) = fs::remove_file(path) {
            warn!("    Warning: Could not remove old file {:?}: {}", path, e);
        } else {
            info!("    Removed old file: {:?}", path.file_name().unwrap());
        }
    }

    // Prepare lazy frame with derived columns
    let lf = prepare_lazy_frame(df);

    let mut results = Vec::new();

    // Process all intervals sequentially
    for interval in intervals {
        match aggregate_trades_to_ohlcv(&lf, interval) {
            Ok(ohlcv) => {
                if ohlcv.height() == 0 {
                    warn!(
                        "    Warning: No data after aggregation for {} at {}",
                        interval, year
                    );
                    continue;
                }

                // Add symbol column
                let symbol_col =
                    Column::new("symbol".into(), vec![symbol.to_string(); ohlcv.height()]);
                let mut ohlcv_with_symbol = ohlcv;
                ohlcv_with_symbol.insert_column(0, symbol_col)?;

                // Get date range for logging and filename
                let time_col = ohlcv_with_symbol.column("time")?;
                let min_time = time_col.min_reduce()?.value().try_extract::<i64>()?;
                let max_time = time_col.max_reduce()?.value().try_extract::<i64>()?;
                let start_date = format_timestamp_us(min_time);
                let end_date = format_timestamp_us(max_time);

                // Use year for complete years, date for incomplete years
                let year_suffix = get_year_suffix(year, max_time);

                // Output directly to final location
                let output_filename = format!("{}_kline_{}_{}.parquet", symbol, interval, year_suffix);
                let output_path = output_symbol_dir.join(&output_filename);

                write_parquet_zstd(&mut ohlcv_with_symbol, &output_path)?;

                info!(
                    "    {}-{}: {} bars ({} to {})",
                    interval,
                    year_suffix,
                    ohlcv_with_symbol.height(),
                    start_date,
                    end_date
                );

                results.push(output_path);
            }
            Err(e) => {
                warn!("    Error aggregating {}-{}: {:?}", interval, year, e);
            }
        }
    }

    Ok(results)
}

/// Process a single symbol (all parquet files)
fn process_symbol(
    symbol: &str,
    source_dir: &Path,
    output_base_dir: &Path,
    intervals: &[&str],
) -> Result<Vec<PathBuf>> {
    info!("Processing {}...", symbol);

    let symbol_dir = source_dir.join(symbol);
    let parquet_files = find_all_parquets(&symbol_dir)?;

    if parquet_files.is_empty() {
        warn!("  No parquet files found");
        return Ok(Vec::new());
    }

    // Create output directory for this symbol
    let output_symbol_dir = output_base_dir.join(symbol);
    fs::create_dir_all(&output_symbol_dir)?;

    let mut results = Vec::new();

    // Process each parquet file
    for parquet_path in &parquet_files {
        match process_parquet_file(parquet_path, symbol, &output_symbol_dir, intervals) {
            Ok(file_results) => {
                results.extend(file_results);
            }
            Err(e) => {
                warn!(
                    "  Error processing {:?}: {:?}",
                    parquet_path.file_name().unwrap(),
                    e
                );
            }
        }
    }

    Ok(results)
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let args = Args::parse();

    let base_dir = args
        .data_dir
        .or_else(|| std::env::var("TRADE_DATA").ok())
        .unwrap_or_else(|| ".".to_string());

    let base_path = PathBuf::from(&base_dir);

    let intervals: Vec<&str> = if args.kline_1s {
        let mut v = vec!["1s"];
        v.extend_from_slice(BASE_INTERVALS);
        v
    } else {
        BASE_INTERVALS.to_vec()
    };

    info!("Aggregating trades to ALL intervals in one pass...");
    info!("Market: {:?}, Intervals: {}", args.market, intervals.join(", "));

    let market_path = args.market.data_path();

    // Source directory with aggTrades data
    let source_dir = base_path.join("binance").join(&market_path).join("aggTrades");

    if !source_dir.exists() {
        anyhow::bail!("Source folder not found: {:?}", source_dir);
    }

    // Output directory
    let output_dir = base_path
        .join("binance")
        .join(&market_path)
        .join("aggTrades_kline");
    fs::create_dir_all(&output_dir)?;

    // Find symbol folders (filter by --symbols if provided)
    let mut symbols: Vec<String> = if args.symbols.is_empty() {
        fs::read_dir(&source_dir)?
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let path = entry.path();
                if path.is_dir() {
                    Some(path.file_name()?.to_string_lossy().to_string())
                } else {
                    None
                }
            })
            .collect()
    } else {
        args.symbols
    };

    symbols.sort();

    if symbols.is_empty() {
        anyhow::bail!("No symbol folders found in {:?}", source_dir);
    }

    info!("Found {} symbols to process", symbols.len());
    info!("Output directory: {:?}", output_dir);

    let mut all_output_files = Vec::new();

    // Process each symbol
    for symbol in &symbols {
        match process_symbol(symbol, &source_dir, &output_dir, &intervals) {
            Ok(results) => {
                all_output_files.extend(results);
            }
            Err(e) => {
                warn!("Error processing {}: {:?}", symbol, e);
            }
        }
    }

    info!("============================================================");
    info!(
        "Completed all intervals. Total output files: {}",
        all_output_files.len()
    );

    Ok(())
}
