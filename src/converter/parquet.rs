use crate::cli::DataType as CliDataType;
use crate::error::Result;
use crate::utils::path::build_parquet_filename;
use chrono::{Datelike, NaiveDate};
use polars::prelude::*;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tracing::{debug, info};

#[allow(dead_code)]
/// Read existing parquet file if it exists
pub fn read_parquet_if_exists(path: &Path) -> Result<Option<DataFrame>> {
    if path.exists() {
        debug!("Reading existing parquet: {:?}", path);
        let df = LazyFrame::scan_parquet(path, Default::default())?
            .collect()?;
        Ok(Some(df))
    } else {
        Ok(None)
    }
}

/// Find all parquet files for a symbol in the target folder
pub fn find_existing_parquets(target_folder: &Path, symbol: &str) -> Vec<PathBuf> {
    let mut files = Vec::new();
    if let Ok(entries) = std::fs::read_dir(target_folder) {
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if name.starts_with(symbol) && name.ends_with(".parquet") {
                    files.push(path);
                }
            }
        }
    }
    files.sort();
    files
}

/// Get the last date from existing parquet files
pub fn get_last_date_from_parquets(parquet_files: &[PathBuf]) -> Option<NaiveDate> {
    let mut max_date: Option<NaiveDate> = None;

    for path in parquet_files {
        if let Ok(df) = LazyFrame::scan_parquet(path, Default::default())
            .and_then(|lf| lf.select([col("time").max()]).collect())
        {
            if let Ok(series) = df.column("time") {
                if let Ok(datetime) = series.datetime() {
                    if let Some(ts) = datetime.get(0) {
                        // ts is in microseconds
                        let secs = ts / 1_000_000;
                        let nsecs = ((ts % 1_000_000) * 1000) as u32;
                        if let Some(dt) = chrono::DateTime::from_timestamp(secs, nsecs) {
                            let date = dt.date_naive();
                            max_date = Some(max_date.map_or(date, |d| d.max(date)));
                        }
                    }
                }
            }
        }
    }

    max_date
}

#[allow(dead_code)]
/// Merge new data with existing parquet data
/// Groups by year and writes yearly parquet files
pub fn merge_and_write_parquets(
    target_folder: &Path,
    symbol: &str,
    data_type: CliDataType,
    new_data: DataFrame,
    today: NaiveDate,
) -> Result<()> {
    if new_data.height() == 0 {
        info!("{} No new data to write", symbol);
        return Ok(());
    }

    println!("target_folder: {}", target_folder.display());

    std::fs::create_dir_all(target_folder)?;

    // Group new data by year
    let new_data_by_year = group_by_year(new_data)?;

    for (year, year_df) in new_data_by_year {
        // Find existing parquet for this year
        let existing_pattern = format!("{}_{}_{}", symbol, data_type, year);
        let mut existing_path = None;

        for entry in std::fs::read_dir(target_folder)?.flatten() {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if name.starts_with(&existing_pattern) && name.ends_with(".parquet") {
                    existing_path = Some(path);
                    break;
                }
            }
        }

        // Merge with existing data
        let merged_df = if let Some(ref existing) = existing_path {
            let existing_df = LazyFrame::scan_parquet(existing, Default::default())?
                .collect()?;
            merge_dataframes(existing_df, year_df, data_type)?
        } else {
            year_df
        };

        // Get the last date from the data
        let last_date = merged_df
            .column("time")
            .ok()
            .and_then(|s| s.datetime().ok())
            .and_then(|dt| dt.max())
            .and_then(|ts| {
                let secs = ts / 1_000_000;
                chrono::DateTime::from_timestamp(secs, 0).map(|dt| dt.date_naive())
            })
            .unwrap_or(today);

        // Only add date suffix if year is incomplete (not ending on Dec 31)
        let is_year_complete = last_date.month() == 12 && last_date.day() == 31;
        let date_suffix = if is_year_complete {
            None
        } else {
            Some(format!("{:02}-{:02}", last_date.month(), last_date.day()))
        };

        let filename = build_parquet_filename(symbol, data_type, year, date_suffix.as_deref());
        let new_path = target_folder.join(&filename);

        // Remove old file if exists and different name
        if let Some(ref old_path) = existing_path {
            if old_path != &new_path {
                std::fs::remove_file(old_path)?;
                debug!("Removed old parquet: {:?}", old_path);
            }
        }

        // Write new parquet
        write_parquet(&new_path, merged_df)?;
        info!("Wrote parquet: {:?}", new_path);
    }

    Ok(())
}

/// Merge and write parquet for a single year (avoids expensive group_by_year)
pub fn merge_and_write_year_parquet(
    target_folder: &Path,
    symbol: &str,
    data_type: CliDataType,
    year: i32,
    dfs: Vec<DataFrame>,
    today: NaiveDate,
) -> Result<()> {
    if dfs.is_empty() {
        return Ok(());
    }

    std::fs::create_dir_all(target_folder)?;

    // Use concat for efficient DataFrame combination (much faster than multiple vstack_mut)
    let combined = if dfs.len() == 1 {
        dfs.into_iter().next().unwrap()
    } else {
        // Convert to lazy frames and use concat
        let lazy_frames: Vec<LazyFrame> = dfs.into_iter().map(|df| df.lazy()).collect();
        concat(lazy_frames, UnionArgs::default())?.collect()?
    };

    info!("{} year {} has {} rows", symbol, year, combined.height());

    // Find existing parquet for this year
    let existing_pattern = format!("{}_{}_{}", symbol, data_type, year);
    let mut existing_path = None;

    for entry in std::fs::read_dir(target_folder)?.flatten() {
        let path = entry.path();
        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
            if name.starts_with(&existing_pattern) && name.ends_with(".parquet") {
                existing_path = Some(path);
                break;
            }
        }
    }

    // Merge with existing data (only deduplicate when merging with existing)
    let merged_df = if let Some(ref existing) = existing_path {
        let existing_df = LazyFrame::scan_parquet(existing, Default::default())?
            .collect()?;
        merge_dataframes(existing_df, combined, data_type)?
    } else {
        // Fresh data: just sort by time (no duplicates expected from different ZIP files)
        combined.lazy()
            .sort(["time"], Default::default())
            .collect()?
    };

    // Get the last date from the data within the target year
    // Filter to only dates in this year to handle midnight boundary cases
    let last_date = merged_df
        .clone()
        .lazy()
        .filter(col("time").dt().year().eq(lit(year)))
        .select([col("time").max()])
        .collect()
        .ok()
        .and_then(|df| df.column("time").ok().cloned())
        .and_then(|s| s.datetime().ok().cloned())
        .and_then(|dt| dt.get(0))
        .and_then(|ts| {
            // For metrics: milliseconds, for others: microseconds
            let secs = if data_type == CliDataType::Metrics {
                ts / 1_000
            } else {
                ts / 1_000_000
            };
            chrono::DateTime::from_timestamp(secs, 0).map(|dt| dt.date_naive())
        })
        .unwrap_or(today);

    // Only add date suffix if year is incomplete (not ending on Dec 31)
    let is_year_complete = last_date.year() == year && last_date.month() == 12 && last_date.day() == 31;
    let date_suffix = if is_year_complete {
        None
    } else {
        Some(format!("{:02}-{:02}", last_date.month(), last_date.day()))
    };

    let filename = build_parquet_filename(symbol, data_type, year, date_suffix.as_deref());
    let new_path = target_folder.join(&filename);

    // Remove old file if exists and different name
    if let Some(ref old_path) = existing_path {
        if old_path != &new_path {
            std::fs::remove_file(old_path)?;
            debug!("Removed old parquet: {:?}", old_path);
        }
    }

    // Write new parquet
    write_parquet(&new_path, merged_df)?;
    info!("Wrote parquet: {:?}", new_path);

    Ok(())
}

#[allow(dead_code)]
/// Group DataFrame by year based on time column
fn group_by_year(df: DataFrame) -> Result<HashMap<i32, DataFrame>> {
    let mut result = HashMap::new();

    // Extract year from time column
    let df_with_year = df
        .lazy()
        .with_column(col("time").dt().year().alias("_year"))
        .collect()?;

    // Get unique years
    let years: Vec<i32> = df_with_year
        .column("_year")?
        .i32()?
        .into_iter()
        .flatten()
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();

    for year in years {
        let year_df = df_with_year
            .clone()
            .lazy()
            .filter(col("_year").eq(lit(year)))
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

        result.insert(year, year_df);
    }

    Ok(result)
}

/// Merge two DataFrames, removing duplicates based on unique key
fn merge_dataframes(existing: DataFrame, new: DataFrame, data_type: CliDataType) -> Result<DataFrame> {
    let combined = existing.vstack(&new)?;

    // Use different dedup key based on data type
    let unique_key = match data_type {
        CliDataType::Metrics => "time",
        CliDataType::AggTrades | CliDataType::Trades => "agg_trade_id",
    };

    // Remove duplicates and sort by time
    let result = combined
        .lazy()
        .unique(Some(vec![unique_key.into()]), UniqueKeepStrategy::Last)
        .sort(["time"], Default::default())
        .collect()?;

    Ok(result)
}

/// Write DataFrame to parquet file
fn write_parquet(path: &Path, df: DataFrame) -> Result<()> {
    let file = std::fs::File::create(path)?;
    ParquetWriter::new(file)
        .with_compression(ParquetCompression::Zstd(None))
        .finish(&mut df.clone())?;
    Ok(())
}

#[allow(dead_code)]
/// Remove data for a specific month from DataFrame
/// Used when processing monthly ZIP that should replace daily data
pub fn remove_month_data(df: DataFrame, year: i32, month: u32) -> Result<DataFrame> {
    let result = df
        .lazy()
        .filter(
            col("time")
                .dt()
                .year()
                .neq(lit(year))
                .or(col("time").dt().month().neq(lit(month as i32))),
        )
        .collect()?;

    Ok(result)
}
