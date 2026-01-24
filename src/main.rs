mod cli;
mod converter;
mod downloader;
mod error;
mod exchange;
mod models;
mod utils;

use chrono::{Datelike, NaiveDate, Utc};
use clap::Parser;
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Mutex;
use tracing::{debug, error, info, warn, Level};
use tracing_subscriber::EnvFilter;

use cli::Args;
use converter::{find_existing_parquets, get_last_date_from_parquets, merge_and_write_year_parquet, parse_zip_to_dataframe};
use downloader::{DownloadResult, DownloadTask, Downloader};
use exchange::binance::{build_daily_url, build_daily_zip_filename, build_monthly_url, build_monthly_zip_filename};
use utils::date::{extract_year_from_filename, extract_year_month_from_filename, generate_daily_dates, generate_monthly_dates, is_month_complete};
use utils::path::{build_download_folder, build_target_folder};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load .env file if exists
    let _ = dotenvy::dotenv();

    // Parse CLI arguments
    let args = Args::parse();

    // Initialize logging
    let log_level = if args.verbose { Level::DEBUG } else { Level::INFO };
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env()
                .add_directive(format!("data_source={}", log_level).parse().unwrap()),
        )
        .init();

    // Get trade data folder
    let trade_data = args
        .trade_data
        .clone()
        .or_else(|| std::env::var("TRADE_DATA").ok())
        .unwrap_or_else(|| "./data".to_string());

    // Get symbols from CLI args or JSON file
    let symbols = args.get_symbols().map_err(|e| anyhow::anyhow!(e))?;

    info!("Trade data folder: {}", trade_data);
    info!("Exchange: {}", args.exchange);
    info!("Market: {:?}", args.market);
    info!("Data type: {:?}", args.data_type);
    info!("Symbols: {:?}", symbols);

    // Process each symbol
    for symbol in &symbols {
        info!("--------- Processing symbol: {} ---------", symbol);

        if let Err(e) = process_symbol(&args, &trade_data, symbol).await {
            error!("Failed to process {}: {}", symbol, e);
        }
    }

    info!("Done!");
    Ok(())
}

async fn process_symbol(args: &Args, trade_data: &str, symbol: &str) -> anyhow::Result<()> {
    let target_folder = build_target_folder(
        trade_data,
        &args.exchange,
        args.market,
        args.market_sub,
        args.data_type,
        symbol,
    );

    let download_folder = build_download_folder(
        trade_data,
        &args.exchange,
        args.market,
        args.market_sub,
        args.data_type,
        symbol,
    );

    debug!("Target folder: {:?}", target_folder);
    debug!("Download folder: {:?}", download_folder);

    let today = Utc::now().date_naive();

    // Determine start date
    let start_date = determine_start_date(args, &target_folder, symbol)?;
    info!("Start date: {}", start_date);

    // Download phase
    if !args.convert_only {
        download_phase(args, &download_folder, symbol, start_date, today).await?;
    }

    // Convert phase
    if !args.download_only {
        convert_phase(args, &download_folder, &target_folder, symbol, start_date, today)?;
    }

    Ok(())
}

fn determine_start_date(args: &Args, target_folder: &PathBuf, symbol: &str) -> anyhow::Result<NaiveDate> {
    // If --start-date provided, use it
    if let Some(ref date_str) = args.start_date {
        return utils::date::parse_date(date_str)
            .ok_or_else(|| anyhow::anyhow!("Invalid start date format: {}", date_str));
    }

    // Check existing parquet files
    let parquet_files = find_existing_parquets(target_folder, symbol);
    if !parquet_files.is_empty() {
        if let Some(last_date) = get_last_date_from_parquets(&parquet_files) {
            // Start from last_date + 1 day
            let next_date = last_date.succ_opt().unwrap_or(last_date);
            info!("Found existing data up to {}, starting from {}", last_date, next_date);
            return Ok(next_date);
        }
    }

    // Default to 2020-01-01
    Ok(NaiveDate::from_ymd_opt(2020, 1, 1).unwrap())
}

async fn download_phase(
    args: &Args,
    download_folder: &PathBuf,
    symbol: &str,
    start_date: NaiveDate,
    today: NaiveDate,
) -> anyhow::Result<()> {
    info!("=== Download Phase ===");

    std::fs::create_dir_all(download_folder)?;

    let mut tasks = Vec::new();

    // Collect existing ZIP files for resume support
    let existing_files: HashSet<String> = std::fs::read_dir(download_folder)
        .ok()
        .map(|entries| {
            entries
                .flatten()
                .filter_map(|e| e.file_name().into_string().ok())
                .collect()
        })
        .unwrap_or_default();

    // Generate monthly download tasks
    let monthly_dates = generate_monthly_dates(start_date, today);
    let mut monthly_months: HashSet<(i32, u32)> = HashSet::new();

    for date in monthly_dates {
        let year = date.year();
        let month = date.month();

        // Only download monthly if month is complete
        if is_month_complete(date, today) {
            let filename = build_monthly_zip_filename(symbol, args.data_type, year, month);

            // Check if already downloaded (resume support)
            if existing_files.contains(&filename) {
                debug!("Skipping existing: {}", filename);
                monthly_months.insert((year, month));
                continue;
            }

            let url = build_monthly_url(symbol, args.market, args.market_sub, args.data_type, year, month);
            let output_path = download_folder.join(&filename);

            tasks.push(DownloadTask {
                url,
                output_path,
                filename: filename.clone(),
            });

            monthly_months.insert((year, month));
        }
    }

    // Generate daily download tasks (for current month or months without monthly ZIP)
    let daily_dates = generate_daily_dates(start_date, today.pred_opt().unwrap_or(today));
    for date in daily_dates {
        let year = date.year();
        let month = date.month();

        // Skip if monthly ZIP exists or will be downloaded
        if monthly_months.contains(&(year, month)) {
            continue;
        }

        let filename = build_daily_zip_filename(symbol, args.data_type, date);

        if existing_files.contains(&filename) {
            debug!("Skipping existing: {}", filename);
            continue;
        }

        let url = build_daily_url(symbol, args.market, args.market_sub, args.data_type, date);
        let output_path = download_folder.join(&filename);

        tasks.push(DownloadTask {
            url,
            output_path,
            filename,
        });
    }

    info!("Downloading {} files with concurrency {}", tasks.len(), args.concurrency);

    // Download concurrently
    let downloader = Downloader::new(args.concurrency);
    let results = downloader.download_all(tasks).await;

    // Log results
    let mut success = 0;
    let mut skipped = 0;
    let mut not_found = 0;
    let mut errors = 0;

    for result in results {
        match result {
            DownloadResult::Success(_) => success += 1,
            DownloadResult::Skipped(_) => skipped += 1,
            DownloadResult::NotFound(_) => not_found += 1,
            DownloadResult::Error(e) => {
                error!("Download error: {}", e);
                errors += 1;
            }
        }
    }

    info!(
        "Download complete: {} success, {} skipped, {} not found, {} errors",
        success, skipped, not_found, errors
    );

    Ok(())
}

fn convert_phase(
    args: &Args,
    download_folder: &PathBuf,
    target_folder: &PathBuf,
    symbol: &str,
    start_date: NaiveDate,
    today: NaiveDate,
) -> anyhow::Result<()> {
    info!("=== Convert Phase ===");

    // Collect ZIP files
    let mut zip_files: Vec<PathBuf> = std::fs::read_dir(download_folder)
        .ok()
        .map(|entries| {
            entries
                .flatten()
                .map(|e| e.path())
                .filter(|p| {
                    p.extension()
                        .map(|e| e == "zip")
                        .unwrap_or(false)
                })
                .collect()
        })
        .unwrap_or_default();

    if zip_files.is_empty() {
        info!("No ZIP files to convert");
        return Ok(());
    }

    // Sort chronologically
    zip_files.sort();

    info!("Found {} ZIP files to process", zip_files.len());

    // Identify monthly ZIPs to handle daily replacement
    let monthly_files: HashSet<(i32, u32)> = zip_files
        .iter()
        .filter_map(|p| {
            let name = p.file_name()?.to_str()?;
            // Monthly files have format: SYMBOL-aggTrades-YYYY-MM.zip (4 parts with -)
            let parts: Vec<&str> = name.trim_end_matches(".zip").split('-').collect();
            if parts.len() == 4 {
                extract_year_month_from_filename(name)
            } else {
                None
            }
        })
        .collect();

    // Filter ZIP files to process (exclude files before start_date and daily files when monthly exists)
    let zips_to_process: Vec<(PathBuf, i32)> = zip_files
        .iter()
        .filter_map(|zip_path| {
            let filename = zip_path.file_name().and_then(|n| n.to_str()).unwrap_or("");

            // Check daily files
            if let Some(date) = utils::date::extract_year_month_day_from_filename(filename) {
                // Skip if before start_date
                if date < start_date {
                    debug!("Skipping {} (before start_date {})", filename, start_date);
                    return None;
                }
                // Skip if monthly file exists for this month
                if monthly_files.contains(&(date.year(), date.month())) {
                    debug!("Skipping daily {} (monthly exists)", filename);
                    return None;
                }
            } else if let Some((year, month)) = extract_year_month_from_filename(filename) {
                // Check monthly files - skip if the entire month is before start_date
                // A month is before start_date if the last day of that month < start_date
                let last_day = if month == 12 {
                    NaiveDate::from_ymd_opt(year + 1, 1, 1).unwrap().pred_opt().unwrap()
                } else {
                    NaiveDate::from_ymd_opt(year, month + 1, 1).unwrap().pred_opt().unwrap()
                };
                if last_day < start_date {
                    debug!("Skipping monthly {} (before start_date {})", filename, start_date);
                    return None;
                }
            }

            // Extract year from filename
            let year = extract_year_from_filename(filename)?;
            Some((zip_path.clone(), year))
        })
        .collect();

    if zips_to_process.is_empty() {
        info!("No ZIP files to process after filtering");
        return Ok(());
    }

    info!("Processing {} ZIP files in parallel", zips_to_process.len());

    // Process ZIPs in parallel using rayon
    let market = args.market;
    let data_by_year: Mutex<HashMap<i32, Vec<polars::frame::DataFrame>>> = Mutex::new(HashMap::new());

    zips_to_process.par_iter().for_each(|(zip_path, year)| {
        match parse_zip_to_dataframe(zip_path, market) {
            Ok(df) => {
                let mut map = data_by_year.lock().unwrap();
                map.entry(*year).or_default().push(df);
            }
            Err(e) => {
                warn!("Failed to parse {:?}: {}", zip_path, e);
            }
        }
    });

    let data_by_year = data_by_year.into_inner().unwrap();

    if data_by_year.is_empty() {
        info!("No data to write");
        return Ok(());
    }

    let total_rows: usize = data_by_year.values().flat_map(|v| v.iter()).map(|df| df.height()).sum();
    info!("Total rows to write: {} across {} years", total_rows, data_by_year.len());

    // Process each year
    for (year, dfs) in data_by_year {
        merge_and_write_year_parquet(target_folder, symbol, args.data_type, year, dfs, today)?;
    }

    info!("Conversion complete");

    Ok(())
}
