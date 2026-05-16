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
        CliDataType::AggTrades => match market {
            Market::Future => parse_futures_csv(cursor)?,
            Market::Spot | Market::Option => parse_spot_csv(cursor)?,
        },
        CliDataType::Trades => match market {
            Market::Future => parse_futures_trades_csv(cursor)?,
            Market::Spot | Market::Option => parse_spot_trades_csv(cursor)?,
        },
        CliDataType::MarginInterestRate => {
            return Err(AppError::Parse(
                "marginInterestRate is fetched via signed API, not ZIP conversion".to_string(),
            ))
        }
    };

    info!(
        "Parsed {} rows from {:?}",
        df.height(),
        zip_path.file_name().unwrap_or_default()
    );

    Ok(df)
}

/// Parse Futures CSV.
///
/// Pre-April-2022 monthly archives have no header and 7 columns
/// (agg_trade_id, price, quantity, first_trade_id, last_trade_id, transact_time, is_buyer_maker).
/// April-2022+ archives have an 8-col header including `is_best_match`... actually
/// the futures header has 7 cols (no is_best_match); normalize_dataframe fills it in.
/// We detect by peeking at the first byte: digit → headerless, letter → headered.
fn parse_futures_csv(mut cursor: Cursor<Vec<u8>>) -> Result<DataFrame> {
    let first_byte = cursor.get_ref().first().copied().unwrap_or(b'\n');
    let has_header = first_byte.is_ascii_alphabetic();

    cursor.set_position(0);

    let df = if has_header {
        CsvReadOptions::default()
            .with_has_header(true)
            .into_reader_with_file_handle(cursor)
            .finish()?
    } else {
        let schema = Schema::from_iter([
            Field::new("agg_trade_id".into(), DataType::Int64),
            Field::new("price".into(), DataType::Float64),
            Field::new("quantity".into(), DataType::Float64),
            Field::new("first_trade_id".into(), DataType::Int64),
            Field::new("last_trade_id".into(), DataType::Int64),
            Field::new("transact_time".into(), DataType::Int64),
            Field::new("is_buyer_maker".into(), DataType::Boolean),
        ]);
        CsvReadOptions::default()
            .with_has_header(false)
            .with_schema(Some(Arc::new(schema)))
            .into_reader_with_file_handle(cursor)
            .finish()?
    };

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

/// Parse Spot `trades` CSV. Spot trade archives are always headerless and
/// carry 7 columns: id, price, qty, quote_qty, time, is_buyer_maker,
/// is_best_match. The `time` column is milliseconds in older files
/// (pre-2025) and microseconds in newer ones — normalize_trades_dataframe
/// handles both.
fn parse_spot_trades_csv(cursor: Cursor<Vec<u8>>) -> Result<DataFrame> {
    let schema = Schema::from_iter([
        Field::new("id".into(), DataType::Int64),
        Field::new("price".into(), DataType::Float64),
        Field::new("qty".into(), DataType::Float64),
        Field::new("quote_qty".into(), DataType::Float64),
        Field::new("time".into(), DataType::Int64),
        Field::new("is_buyer_maker".into(), DataType::Boolean),
        Field::new("is_best_match".into(), DataType::Boolean),
    ]);

    let df = CsvReadOptions::default()
        .with_has_header(false)
        .with_schema(Some(Arc::new(schema)))
        .into_reader_with_file_handle(cursor)
        .finish()?;

    normalize_trades_dataframe(df)
}

/// Parse Futures `trades` CSV. Futures archives always include a header row
/// with 6 columns (no `is_best_match`): id, price, qty, quote_qty, time,
/// is_buyer_maker. `time` is milliseconds. is_best_match is filled with
/// `false` for schema parity with spot.
fn parse_futures_trades_csv(mut cursor: Cursor<Vec<u8>>) -> Result<DataFrame> {
    let first_byte = cursor.get_ref().first().copied().unwrap_or(b'\n');
    let has_header = first_byte.is_ascii_alphabetic();

    cursor.set_position(0);

    let df = if has_header {
        CsvReadOptions::default()
            .with_has_header(true)
            .into_reader_with_file_handle(cursor)
            .finish()?
    } else {
        let schema = Schema::from_iter([
            Field::new("id".into(), DataType::Int64),
            Field::new("price".into(), DataType::Float64),
            Field::new("qty".into(), DataType::Float64),
            Field::new("quote_qty".into(), DataType::Float64),
            Field::new("time".into(), DataType::Int64),
            Field::new("is_buyer_maker".into(), DataType::Boolean),
        ]);
        CsvReadOptions::default()
            .with_has_header(false)
            .with_schema(Some(Arc::new(schema)))
            .into_reader_with_file_handle(cursor)
            .finish()?
    };

    normalize_trades_dataframe(df)
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

    // Reorder columns and cast numeric columns to Float64.
    // CSV type inference can pick String for these ratio columns when a file
    // has empty/non-numeric values at the top — force Float64 so all ZIPs
    // produce the same schema and concat works.
    lf = lf.select([
        col("time"),
        col("symbol"),
        col("sum_open_interest").cast(DataType::Float64),
        col("sum_open_interest_value").cast(DataType::Float64),
        col("count_toptrader_long_short_ratio").cast(DataType::Float64),
        col("sum_toptrader_long_short_ratio").cast(DataType::Float64),
        col("count_long_short_ratio").cast(DataType::Float64),
        col("sum_taker_long_short_vol_ratio").cast(DataType::Float64),
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

/// Normalize a Binance `trades` DataFrame into the canonical output schema:
/// `trade_id, time, price, quantity, quote_qty, is_buyer_maker, is_best_match, ts`.
///
/// Handles both the spot variant (has `is_best_match`, time may be ms or us
/// depending on the era) and the futures variant (no `is_best_match`, ms).
fn normalize_trades_dataframe(df: DataFrame) -> Result<DataFrame> {
    let columns: Vec<String> = df.get_column_names().iter().map(|s| s.to_string()).collect();

    let mut lf = df.lazy();

    // Futures lacks is_best_match; fill it with false so the schema lines
    // up with spot output.
    if !columns.contains(&"is_best_match".to_string()) {
        lf = lf.with_column(lit(false).alias("is_best_match"));
    }

    // Auto-detect time unit: > 1e14 → already microseconds; otherwise ms.
    let ts_microseconds = when(col("time").gt(lit(1_000_000_000_000_000i64)))
        .then(col("time"))
        .otherwise(col("time") * lit(1000i64));

    lf = lf.with_column(ts_microseconds.clone().alias("ts"));
    lf = lf.with_column(
        ts_microseconds
            .cast(DataType::Datetime(TimeUnit::Microseconds, None))
            .alias("time_dt"),
    );

    let result = lf
        .select([
            col("id").alias("trade_id"),
            col("time_dt").alias("time"),
            col("price"),
            col("qty").alias("quantity"),
            col("quote_qty"),
            col("is_buyer_maker"),
            col("is_best_match"),
            col("ts"),
        ])
        .collect()?;

    Ok(result)
}

/// Normalize DataFrame columns and add derived columns
fn normalize_dataframe(df: DataFrame) -> Result<DataFrame> {
    // Get column names directly from DataFrame schema (no collect needed)
    let columns: Vec<String> = df.get_column_names().iter().map(|s| s.to_string()).collect();

    let mut lf = df.lazy();

    // Add is_best_match column if missing (futures data doesn't have it, spot only)
    if !columns.contains(&"is_best_match".to_string()) {
        lf = lf.with_column(lit(false).alias("is_best_match"));
    }

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

    // Select and order columns. Force numeric dtypes explicitly: headered
    // archives rely on CSV type inference, which types `quantity`/`price` as
    // Int64 in months where every value is a whole number and Float64
    // otherwise. Without these casts, concatenating such years fails with
    // "'concat' inputs should all have the same schema".
    let result = lf
        .select([
            col("agg_trade_id").cast(DataType::Int64),
            col("time"),
            col("price").cast(DataType::Float64),
            col("quantity").cast(DataType::Float64),
            col("first_trade_id").cast(DataType::Int64),
            col("last_trade_id").cast(DataType::Int64),
            col("is_buyer_maker"),
            col("is_best_match"),
            col("ts"),
        ])
        .collect()?;

    Ok(result)
}
