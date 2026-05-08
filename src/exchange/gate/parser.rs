use crate::cli::Market;
use crate::error::Result;
use flate2::read::GzDecoder;
use polars::prelude::*;
use std::io::{Cursor, Read};
use std::path::Path;
use tracing::debug;

/// Parse a Gate `.csv.gz` archive into the AggTrade-compatible DataFrame
/// shared with the Binance flow. Dispatches on `market`:
///
/// **Spot** (`Market::Spot`) — 5 cols, no header:
///   1. `transact_time`  — float seconds since epoch (e.g. `1775001604.898908`)
///   2. `agg_trade_id`   — integer trade id
///   3. `price`          — float
///   4. `quantity`       — float (base asset amount, unsigned)
///   5. `side`           — `1` (buyer aggressor) or `2` (seller aggressor)
///
/// **Futures USDT** (`Market::Future`) — 4 cols, no header:
///   1. `transact_time`
///   2. `agg_trade_id`
///   3. `price`
///   4. `size`           — *signed* base contracts; sign is the aggressor side
///                         (positive → buy aggressor, negative → sell aggressor).
///
/// Output schema is the same in both cases:
///   `agg_trade_id, time (Datetime[us]), price, quantity, first_trade_id,
///    last_trade_id, is_buyer_maker, is_best_match, ts (i64 us)`
///
/// `is_buyer_maker` mapping (the standard taker-side convention):
///   - spot: `side == 2`
///   - futures: `size < 0` (sell aggressor → buyer is maker)
pub fn parse_gz_to_dataframe(gz_path: &Path, market: Market) -> Result<DataFrame> {
    debug!("Parsing Gate gz ({:?}): {:?}", market, gz_path);

    let file = std::fs::File::open(gz_path)?;
    let mut decoder = GzDecoder::new(file);
    let mut csv_data = Vec::new();
    decoder.read_to_end(&mut csv_data)?;

    let cursor = Cursor::new(csv_data);

    match market {
        Market::Spot => parse_spot(cursor),
        Market::Future => parse_futures(cursor),
        Market::Option => Err(crate::error::AppError::Parse(
            "gate option market not supported".into(),
        )),
    }
}

fn parse_spot(cursor: Cursor<Vec<u8>>) -> Result<DataFrame> {
    let schema = Schema::from_iter([
        Field::new("ts_sec".into(), DataType::Float64),
        Field::new("agg_trade_id".into(), DataType::Int64),
        Field::new("price".into(), DataType::Float64),
        Field::new("quantity".into(), DataType::Float64),
        Field::new("side".into(), DataType::Int32),
    ]);

    let df = CsvReadOptions::default()
        .with_has_header(false)
        .with_schema(Some(Arc::new(schema)))
        .into_reader_with_file_handle(cursor)
        .finish()?;

    let lf = df
        .lazy()
        .with_column(
            (col("ts_sec") * lit(1_000_000.0))
                .cast(DataType::Int64)
                .alias("ts"),
        )
        .with_column(col("side").eq(lit(2)).alias("is_buyer_maker"))
        .drop(["ts_sec", "side"]);

    finalize_with_ts_us(lf.collect()?)
}

fn parse_futures(cursor: Cursor<Vec<u8>>) -> Result<DataFrame> {
    let schema = Schema::from_iter([
        Field::new("ts_sec".into(), DataType::Float64),
        Field::new("agg_trade_id".into(), DataType::Int64),
        Field::new("price".into(), DataType::Float64),
        Field::new("signed_size".into(), DataType::Float64),
    ]);

    let df = CsvReadOptions::default()
        .with_has_header(false)
        .with_schema(Some(Arc::new(schema)))
        .into_reader_with_file_handle(cursor)
        .finish()?;

    let lf = df
        .lazy()
        .with_column(
            (col("ts_sec") * lit(1_000_000.0))
                .cast(DataType::Int64)
                .alias("ts"),
        )
        // sign of size = aggressor side: negative → sell aggressor → buyer is maker
        .with_column(col("signed_size").lt(lit(0.0)).alias("is_buyer_maker"))
        .with_column(
            when(col("signed_size").lt(lit(0.0)))
                .then(lit(0.0) - col("signed_size"))
                .otherwise(col("signed_size"))
                .alias("quantity"),
        )
        .drop(["ts_sec", "signed_size"]);

    finalize_with_ts_us(lf.collect()?)
}

/// Build a DataFrame from API trade rows already collected in memory.
/// Each row: `(id, ts_us, price, quantity, side_is_sell_aggressor)`
/// where `side_is_sell_aggressor` ⟺ `is_buyer_maker == true`.
pub fn dataframe_from_api_rows(rows: Vec<(i64, i64, f64, f64, bool)>) -> Result<DataFrame> {
    let n = rows.len();
    let mut ids = Vec::with_capacity(n);
    let mut tss = Vec::with_capacity(n);
    let mut prices = Vec::with_capacity(n);
    let mut qtys = Vec::with_capacity(n);
    let mut buyer_maker = Vec::with_capacity(n);
    for (id, ts, p, q, sell_agg) in rows {
        ids.push(id);
        tss.push(ts);
        prices.push(p);
        qtys.push(q);
        buyer_maker.push(sell_agg);
    }

    let df = df! {
        "agg_trade_id" => ids,
        "ts" => tss.clone(),
        "price" => prices,
        "quantity" => qtys,
        "is_buyer_maker" => buyer_maker,
    }?;

    finalize_with_ts_us(df)
}

/// Add the derived `time` and placeholder columns and select in canonical order.
/// Input must have: `agg_trade_id, ts (i64 us), price, quantity, is_buyer_maker`.
fn finalize_with_ts_us(df: DataFrame) -> Result<DataFrame> {
    let result = df
        .lazy()
        .with_columns([
            col("ts")
                .cast(DataType::Datetime(TimeUnit::Microseconds, None))
                .alias("time"),
            col("agg_trade_id").alias("first_trade_id"),
            col("agg_trade_id").alias("last_trade_id"),
            lit(false).alias("is_best_match"),
        ])
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
