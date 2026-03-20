use crate::cli::{DataType as CliDataType, Market};
use crate::error::{AppError, Result};
use polars::prelude::*;
use std::io::{Cursor, Read};
use std::path::Path;
use tracing::{debug, info};

/// Extract CSV from ZIP and parse into DataFrame
pub fn parse_zip_to_dataframe(zip_path: &Path, market: Market, data_type: CliDataType) -> Result<DataFrame> {
    debug!("Parsing ZIP: {:?}", zip_path);

    let file = std::fs::File::open(zip_path)?;
    let mut archive = ::zip::ZipArchive::new(file)?;

    if archive.len() == 0 {
        return Err(AppError::Parse("Empty ZIP archive".to_string()));
    }

    // Get the first (and usually only) CSV file
    let mut csv_file = archive.by_index(0)?;

    // Pre-allocate buffer based on uncompressed size for efficiency
    let uncompressed_size = csv_file.size() as usize;
    let mut csv_data = Vec::with_capacity(uncompressed_size);
    csv_file.read_to_end(&mut csv_data)?;

    let cursor = Cursor::new(csv_data);

    // Parse CSV based on market type and data type
    let df = match data_type {
        CliDataType::Metrics => parse_metrics_csv(cursor)?,
        CliDataType::FundingRate => parse_funding_rate_csv(cursor)?,
        CliDataType::BVOLIndex => parse_bvol_index_csv(cursor)?,
        CliDataType::AggTrades | CliDataType::Trades => match market {
            Market::Future => parse_futures_csv(cursor)?,
            Market::Spot | Market::Option => parse_spot_csv(cursor)?,
        },
    };

    info!(
        "Parsed {} rows from {:?}",
        df.height(),
        zip_path.file_name().unwrap_or_default()
    );

    Ok(df)
}

/// Parse Futures CSV (has header row)
fn parse_futures_csv(cursor: Cursor<Vec<u8>>) -> Result<DataFrame> {
    let df = CsvReadOptions::default()
        .with_has_header(true)
        .into_reader_with_file_handle(cursor)
        .finish()?;

    normalize_dataframe(df)
}

/// Parse Metrics CSV (has header row)
fn parse_metrics_csv(cursor: Cursor<Vec<u8>>) -> Result<DataFrame> {
    let df = CsvReadOptions::default()
        .with_has_header(true)
        .into_reader_with_file_handle(cursor)
        .finish()?;

    normalize_metrics_dataframe(df)
}

/// Parse FundingRate CSV (has header row)
/// Columns: calc_time, funding_interval_hours, last_funding_rate
fn parse_funding_rate_csv(cursor: Cursor<Vec<u8>>) -> Result<DataFrame> {
    let df = CsvReadOptions::default()
        .with_has_header(true)
        .into_reader_with_file_handle(cursor)
        .finish()?;

    normalize_funding_rate_dataframe(df)
}

/// Parse BVOLIndex CSV (has header row)
/// Columns: calc_time, symbol, base_asset, quote_asset, index_value
fn parse_bvol_index_csv(cursor: Cursor<Vec<u8>>) -> Result<DataFrame> {
    let df = CsvReadOptions::default()
        .with_has_header(true)
        .into_reader_with_file_handle(cursor)
        .finish()?;

    normalize_bvol_index_dataframe(df)
}

/// Parse Spot CSV (no header row)
fn parse_spot_csv(cursor: Cursor<Vec<u8>>) -> Result<DataFrame> {
    // Column names for spot aggTrades (no header in file)
    let schema = Schema::from_iter([
        Field::new("agg_trade_id".into(), DataType::Int64),
        Field::new("price".into(), DataType::Float64),
        Field::new("quantity".into(), DataType::Float64),
        Field::new("first_trade_id".into(), DataType::Int64),
        Field::new("last_trade_id".into(), DataType::Int64),
        Field::new("transact_time".into(), DataType::Int64),
        Field::new("is_buyer_maker".into(), DataType::Boolean),
        Field::new("is_best_match".into(), DataType::Boolean),
    ]);

    let df = CsvReadOptions::default()
        .with_has_header(false)
        .with_schema(Some(Arc::new(schema)))
        .into_reader_with_file_handle(cursor)
        .finish()?;

    normalize_dataframe(df)
}

/// Normalize FundingRate DataFrame columns (convert calc_time milliseconds to datetime)
fn normalize_funding_rate_dataframe(df: DataFrame) -> Result<DataFrame> {
    let mut lf = df.lazy();

    // calc_time is milliseconds since epoch, convert to datetime
    lf = lf.with_column(
        col("calc_time")
            .cast(DataType::Datetime(TimeUnit::Milliseconds, None))
            .alias("time"),
    );

    // Select columns in order: time, funding_interval_hours, last_funding_rate
    lf = lf.select([
        col("time"),
        col("funding_interval_hours"),
        col("last_funding_rate"),
    ]);

    // Sort by time
    lf = lf.sort(["time"], Default::default());

    let result = lf.collect()?;
    Ok(result)
}

/// Normalize Metrics DataFrame columns (parse create_time datetime string to time)
fn normalize_metrics_dataframe(df: DataFrame) -> Result<DataFrame> {
    let columns: Vec<String> = df.get_column_names().iter().map(|s| s.to_string()).collect();

    let mut lf = df.lazy();

    // Parse create_time datetime string to proper datetime column
    if columns.contains(&"create_time".to_string()) {
        // create_time is like "2025-12-31 00:05:00", parse as datetime using strptime
        lf = lf.with_column(
            col("create_time")
                .str()
                .strptime(
                    DataType::Datetime(TimeUnit::Milliseconds, None),
                    StrptimeOptions {
                        format: Some("%Y-%m-%d %H:%M:%S".into()),
                        ..Default::default()
                    },
                    lit("raise"),
                )
                .alias("time"),
        );
        lf = lf.drop(["create_time"]);
    }

    // Reorder columns: time first, then others
    lf = lf.select([
        col("time"),
        col("symbol"),
        col("sum_open_interest"),
        col("sum_open_interest_value"),
        col("count_toptrader_long_short_ratio"),
        col("sum_toptrader_long_short_ratio"),
        col("count_long_short_ratio"),
        col("sum_taker_long_short_vol_ratio"),
    ]);

    // Sort by time
    lf = lf.sort(["time"], Default::default());

    let result = lf.collect()?;
    Ok(result)
}

/// Normalize BVOLIndex DataFrame columns (convert calc_time milliseconds to datetime)
fn normalize_bvol_index_dataframe(df: DataFrame) -> Result<DataFrame> {
    let mut lf = df.lazy();

    // calc_time is milliseconds since epoch, convert to datetime
    lf = lf.with_column(
        col("calc_time")
            .cast(DataType::Datetime(TimeUnit::Milliseconds, None))
            .alias("time"),
    );

    // Select columns in order: time, symbol, base_asset, quote_asset, index_value
    lf = lf.select([
        col("time"),
        col("symbol"),
        col("base_asset"),
        col("quote_asset"),
        col("index_value"),
    ]);

    // Sort by time
    lf = lf.sort(["time"], Default::default());

    let result = lf.collect()?;
    Ok(result)
}

/// Normalize DataFrame columns and add derived columns
fn normalize_dataframe(df: DataFrame) -> Result<DataFrame> {
    // Get column names directly from DataFrame schema (no collect needed)
    let columns: Vec<String> = df.get_column_names().iter().map(|s| s.to_string()).collect();

    let mut lf = df.lazy();

    // Ensure we have the expected columns - handle different naming conventions
    if !columns.contains(&"transact_time".to_string()) {
        // Find and rename the time column
        for col_name in &columns {
            if col_name.contains("time") && col_name != "transact_time" {
                lf = lf.rename([col_name.as_str()], ["transact_time"], true);
                break;
            }
        }
    }

    // Auto-detect time unit: if transact_time > 1e14, it's microseconds; otherwise milliseconds
    // Milliseconds for 2020-2030: ~1.5e12 to ~1.9e12 (13 digits)
    // Microseconds for 2020-2030: ~1.5e15 to ~1.9e15 (16 digits)
    let ts_microseconds = when(col("transact_time").gt(lit(1_000_000_000_000_000i64)))
        .then(col("transact_time")) // already microseconds
        .otherwise(col("transact_time") * lit(1000i64)); // convert ms to us

    // Add time column (datetime in microseconds)
    lf = lf.with_column(
        ts_microseconds
            .clone()
            .cast(DataType::Datetime(TimeUnit::Microseconds, None))
            .alias("time"),
    );

    // Add ts column (timestamp in microseconds)
    lf = lf.with_column(ts_microseconds.alias("ts"));

    // Select and order columns
    let result = lf
        .select([
            col("agg_trade_id"),
            col("time"),
            col("price"),
            col("quantity"),
            col("first_trade_id"),
            col("last_trade_id"),
            col("is_buyer_maker"),
            col("is_best_match"),
            col("ts"),
        ])
        .collect()?;

    Ok(result)
}
