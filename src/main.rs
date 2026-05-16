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

use cli::{Args, DataType, Market};
use converter::{find_existing_parquets, get_last_date_from_parquets, merge_and_write_year_parquet, parse_zip_to_dataframe};
use downloader::{DownloadResult, DownloadTask, Downloader};
use exchange::binance::{build_daily_url, build_daily_zip_filename, build_monthly_url, build_monthly_zip_filename};
use exchange::gate;
use utils::date::{extract_year_from_filename, extract_year_month_from_filename, generate_daily_dates, generate_monthly_dates, is_month_complete};
use utils::path::{build_download_base, build_download_folder, build_target_folder};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load .env file if exists
    let _ = dotenvy::dotenv();

    // Parse CLI arguments
    let args = Args::parse();

    // Validate: metrics is only supported for futures market
    if args.data_type == DataType::Metrics && args.market == Market::Spot {
        eprintln!("Error: metrics data type is not supported for spot market");
        std::process::exit(1);
    }

    // Validate: fundingRate is only supported for futures market
    if args.data_type == DataType::FundingRate && args.market == Market::Spot {
        eprintln!("Error: fundingRate data type is not supported for spot market");
        std::process::exit(1);
    }

    // Validate: BVOLIndex is only supported for option market
    if args.data_type == DataType::BVOLIndex && args.market != Market::Option {
        eprintln!("Error: BVOLIndex data type is only supported for option market");
        std::process::exit(1);
    }

    // Validate: option market only supports BVOLIndex
    if args.market == Market::Option && args.data_type != DataType::BVOLIndex {
        eprintln!("Error: option market only supports BVOLIndex data type");
        std::process::exit(1);
    }

    // Validate: marginInterestRate is Binance spot-only (asset-based signed API)
    if args.data_type == DataType::MarginInterestRate {
        if args.exchange.to_lowercase() != "binance" {
            eprintln!("Error: marginInterestRate is only supported for -e binance");
            std::process::exit(1);
        }
        if args.market != Market::Spot {
            eprintln!("Error: marginInterestRate is only supported for -m spot (cross-margin)");
            std::process::exit(1);
        }
    }

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

    // Check if _download folder is > 1GB, remove if so
    let download_base = build_download_base(
        &trade_data,
        &args.exchange,
        args.market,
        args.market_sub,
        args.data_type,
    );
    const ONE_GB: u64 = 1024 * 1024 * 1024;
    let folder_size = get_folder_size(&download_base);
    
    if folder_size > ONE_GB {
        warn!(
            "Download folder {:?} is {:.2} GB, removing...",
            download_base,
            folder_size as f64 / ONE_GB as f64
        );
        std::fs::remove_dir_all(&download_base)?;
    }

    info!("Trade data folder: {}", trade_data);
    info!("Exchange: {}", args.exchange);
    info!("Market: {:?}", args.market);
    info!("Data type: {:?}", args.data_type);
    info!("Symbols: {:?}", symbols);

    // Validate exchange + dispatch
    let exchange_lower = args.exchange.to_lowercase();
    if exchange_lower == "gate" {
        if args.data_type != DataType::AggTrades {
            eprintln!("Error: gate currently supports only -d aggTrades");
            std::process::exit(1);
        }
        let supported = matches!(
            (args.market, args.market_sub),
            (Market::Spot, _) | (Market::Future, cli::MarketSub::Um)
        );
        if !supported {
            eprintln!(
                "Error: gate supports only -m spot or '-m future --market-sub um'"
            );
            std::process::exit(1);
        }
    } else if exchange_lower != "binance" {
        eprintln!("Error: unsupported exchange '{}'. Use 'binance' or 'gate'.", args.exchange);
        std::process::exit(1);
    }

    // Process each symbol
    let total = symbols.len();
    for (i, symbol) in symbols.iter().enumerate() {
        info!("--------- Processing symbol: {} ({}/{}) ---------", symbol, i + 1, total);

        let result = if args.data_type == DataType::MarginInterestRate {
            process_symbol_margin_interest(&args, &trade_data, symbol).await
        } else if exchange_lower == "gate" {
            process_symbol_gate(&args, &trade_data, symbol).await
        } else {
            process_symbol(&args, &trade_data, symbol).await
        };
        if let Err(e) = result {
            error!("Failed to process {}: {}", symbol, e);
        }
    }

    info!("Done!");
    Ok(())
}

/// Calculate folder size in bytes (recursive)
fn get_folder_size(path: &PathBuf) -> u64 {
    if !path.exists() {
        return 0;
    }

    std::fs::read_dir(path)
        .ok()
        .map(|entries| {
            entries
                .flatten()
                .map(|e| {
                    let path = e.path();
                    if path.is_dir() {
                        get_folder_size(&path)
                    } else {
                        e.metadata().map(|m| m.len()).unwrap_or(0)
                    }
                })
                .sum()
        })
        .unwrap_or(0)
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

    // "cutoff" is the first day whose archive is NOT yet available.
    // - When --end-date is given: cutoff = end_date + 1 (end_date itself is historical, archive ready).
    // - Otherwise: cutoff = today (Binance releases today's daily archive the next day).
    // This is what downstream code treats as "today" for is_month_complete / daily-range checks.
    let cutoff = if let Some(ref date_str) = args.end_date {
        let end_date = utils::date::parse_date(date_str)
            .ok_or_else(|| anyhow::anyhow!("Invalid end date format: {}", date_str))?
            .min(today);
        end_date.succ_opt().unwrap_or(today)
    } else {
        today
    };

    // Determine start date
    let start_date = determine_start_date(args, &target_folder, symbol)?;
    info!("Start date: {}, End (exclusive): {}", start_date, cutoff);

    // Download phase
    if !args.convert_only {
        download_phase(args, &download_folder, symbol, start_date, cutoff).await?;
    }

    // Convert phase
    if !args.download_only {
        convert_phase(args, &download_folder, &target_folder, symbol, start_date, cutoff)?;
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

    // Generate monthly download tasks (skip for metrics - no monthly archives available)
    let mut monthly_months: HashSet<(i32, u32)> = HashSet::new();

    // For fundingRate: only monthly available (no daily)
    // For metrics: only daily available (no monthly)
    // For others: prefer monthly, fall back to daily for incomplete months
    let has_monthly = args.data_type != DataType::Metrics && args.data_type != DataType::BVOLIndex;
    let has_daily = args.data_type != DataType::FundingRate;

    if has_monthly {
        let monthly_dates = generate_monthly_dates(start_date, today);

        for date in monthly_dates {
            let year = date.year();
            let month = date.month();

            // Only download monthly if month is complete (or for fundingRate, always try)
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
    }

    // Generate daily download tasks
    // For metrics: download all daily files (no monthly available)
    // For fundingRate: skip daily (only monthly available)
    // For others: only for current month or months without monthly ZIP
    if has_daily {
        let daily_dates = generate_daily_dates(start_date, today.pred_opt().unwrap_or(today));
        for date in daily_dates {
            let year = date.year();
            let month = date.month();

            // Skip if monthly ZIP exists or will be downloaded (not applicable for metrics)
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
    let data_type = args.data_type;
    let data_by_year: Mutex<HashMap<i32, Vec<polars::frame::DataFrame>>> = Mutex::new(HashMap::new());

    zips_to_process.par_iter().for_each(|(zip_path, year)| {
        match parse_zip_to_dataframe(zip_path, market, data_type) {
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

// ============================================================================
// Binance cross-margin interest-rate history flow
// ============================================================================
//
// `symbol` here is interpreted as a margin *asset* (e.g. STORJ). Data is pulled
// from the signed `/sapi/v1/margin/interestRateHistory` endpoint and written as
// both parquet and CSV under the standard target folder:
//   $TRADE_DATA/binance/spot/marginInterestRate/{ASSET}/{ASSET}_marginInterestRate_all.{parquet,csv}

async fn process_symbol_margin_interest(
    args: &Args,
    trade_data: &str,
    asset: &str,
) -> anyhow::Result<()> {
    use polars::prelude::*;

    let target_folder = build_target_folder(
        trade_data,
        &args.exchange,
        args.market,
        args.market_sub,
        args.data_type,
        asset,
    );
    std::fs::create_dir_all(&target_folder)?;

    info!("Fetching cross-margin interest-rate history for asset: {}", asset);
    let rows = exchange::binance::margin::fetch_interest_rate_history(asset).await?;

    if rows.is_empty() {
        warn!(
            "No interest-rate history returned for `{}` (asset may not be cross-margin tradable, \
             or is outside Binance's ~6-month retention window)",
            asset
        );
        return Ok(());
    }

    // Optional date filtering via --start-date / --end-date (inclusive).
    let start_ms = args
        .start_date
        .as_deref()
        .and_then(utils::date::parse_date)
        .and_then(|d| d.and_hms_opt(0, 0, 0))
        .map(|dt| dt.and_utc().timestamp_millis());
    let end_ms = args
        .end_date
        .as_deref()
        .and_then(utils::date::parse_date)
        .and_then(|d| d.and_hms_opt(23, 59, 59))
        .map(|dt| dt.and_utc().timestamp_millis());

    let filtered: Vec<_> = rows
        .into_iter()
        .filter(|r| start_ms.map_or(true, |s| r.timestamp >= s))
        .filter(|r| end_ms.map_or(true, |e| r.timestamp <= e))
        .collect();

    if filtered.is_empty() {
        warn!("All {} samples filtered out by --start-date/--end-date", asset);
        return Ok(());
    }

    let assets: Vec<&str> = filtered.iter().map(|r| r.asset.as_str()).collect();
    let ts: Vec<i64> = filtered.iter().map(|r| r.timestamp).collect();
    let rates: Vec<f64> = filtered
        .iter()
        .map(|r| r.daily_interest_rate.parse::<f64>().unwrap_or(f64::NAN))
        .collect();
    let vip: Vec<i64> = filtered.iter().map(|r| r.vip_level).collect();
    let datetimes: Vec<String> = filtered
        .iter()
        .map(|r| {
            chrono::DateTime::from_timestamp_millis(r.timestamp)
                .map(|d| d.format("%Y-%m-%d %H:%M:%S").to_string())
                .unwrap_or_default()
        })
        .collect();

    let mut df = DataFrame::new(vec![
        Column::new("asset".into(), assets),
        Column::new("timestamp".into(), ts),
        Column::new("datetime".into(), datetimes),
        Column::new("daily_interest_rate".into(), rates),
        Column::new("vip_level".into(), vip),
    ])?;

    let parquet_path = target_folder.join(format!("{}_marginInterestRate_all.parquet", asset));
    let csv_path = target_folder.join(format!("{}_marginInterestRate_all.csv", asset));

    let pf = std::fs::File::create(&parquet_path)?;
    ParquetWriter::new(pf)
        .with_compression(ParquetCompression::Zstd(None))
        .finish(&mut df)?;
    let cf = std::fs::File::create(&csv_path)?;
    CsvWriter::new(cf).finish(&mut df)?;

    let first = filtered.first().unwrap();
    let last = filtered.last().unwrap();
    info!(
        "{}: wrote {} samples ({}  ..  {}) -> {:?}",
        asset,
        df.height(),
        chrono::DateTime::from_timestamp_millis(first.timestamp)
            .map(|d| d.format("%Y-%m-%d").to_string())
            .unwrap_or_default(),
        chrono::DateTime::from_timestamp_millis(last.timestamp)
            .map(|d| d.format("%Y-%m-%d").to_string())
            .unwrap_or_default(),
        parquet_path
    );
    info!("{}: CSV mirror at {:?}", asset, csv_path);

    Ok(())
}

// ============================================================================
// Gate.io flow
// ============================================================================
//
// Monthly `.csv.gz` archives are published at:
//   spot       → https://download.gatedata.org/spot/deals/{YYYYMM}/{PAIR}-{YYYYMM}.csv.gz
//   futures um → https://download.gatedata.org/futures_usdt/trades/{YYYYMM}/{PAIR}-{YYYYMM}.csv.gz
//
// The current (in-flight) month is not yet archived, so trades for it are
// pulled from the public REST endpoints:
//   spot     → GET /api/v4/spot/trades         (paginated by `last_id`, 30-day retention)
//   futures  → GET /api/v4/futures/usdt/trades (paginated by `from`/`to`, no retention cap)
//
// After successful conversion to parquet, the .gz archives are deleted.

async fn process_symbol_gate(
    args: &Args,
    trade_data: &str,
    symbol: &str,
) -> anyhow::Result<()> {
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
    let cutoff = if let Some(ref date_str) = args.end_date {
        let end_date = utils::date::parse_date(date_str)
            .ok_or_else(|| anyhow::anyhow!("Invalid end date format: {}", date_str))?
            .min(today);
        end_date.succ_opt().unwrap_or(today)
    } else {
        today
    };

    let start_date = determine_start_date(args, &target_folder, symbol)?;
    info!("Start date: {}, End (exclusive): {}", start_date, cutoff);

    let pair = gate::to_gate_pair(symbol);
    info!("Gate pair: {}", pair);

    if !args.convert_only {
        gate_download_phase(args, &download_folder, &target_folder, symbol, start_date, cutoff).await?;
    }
    if !args.download_only {
        gate_convert_phase(args, &download_folder, &target_folder, symbol, today)?;
    }
    Ok(())
}

async fn gate_download_phase(
    args: &Args,
    download_folder: &PathBuf,
    target_folder: &PathBuf,
    symbol: &str,
    start_date: NaiveDate,
    cutoff: NaiveDate,
) -> anyhow::Result<()> {
    info!("=== Gate Download Phase ===");
    std::fs::create_dir_all(download_folder)?;

    let existing_files: HashSet<String> = std::fs::read_dir(download_folder)
        .ok()
        .map(|entries| {
            entries
                .flatten()
                .filter_map(|e| e.file_name().into_string().ok())
                .collect()
        })
        .unwrap_or_default();

    // Iterate months from start_date up to cutoff. Past months → archive,
    // current month (or any month whose end is >= cutoff) → fetch via API.
    let monthly_dates = generate_monthly_dates(start_date, cutoff);
    let mut tasks: Vec<DownloadTask> = Vec::new();
    let mut api_months: Vec<(i32, u32)> = Vec::new();

    for date in monthly_dates {
        let year = date.year();
        let month = date.month();
        let month_end = gate::last_day_of_month(year, month);

        // If the month is fully past relative to cutoff, fetch the archive.
        // Otherwise (current/incomplete month), defer to API.
        let needs_api = month_end >= cutoff || gate::is_current_month(date, cutoff);

        if needs_api {
            api_months.push((year, month));
            continue;
        }

        let filename = gate::build_monthly_filename(symbol, year, month);
        if existing_files.contains(&filename) {
            debug!("Skipping existing: {}", filename);
            continue;
        }
        let url = gate::build_monthly_url(symbol, args.market, args.market_sub, year, month);
        let output_path = download_folder.join(&filename);
        tasks.push(DownloadTask {
            url,
            output_path,
            filename,
        });
    }

    info!("Downloading {} archive(s) with concurrency {}", tasks.len(), args.concurrency);
    let downloader = Downloader::new(args.concurrency);
    let results = downloader.download_all(tasks).await;

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
        "Archive download: {} success, {} skipped, {} not found, {} errors",
        success, skipped, not_found, errors
    );

    if !api_months.is_empty() {
        info!("Fetching {} month(s) via Gate REST API", api_months.len());
        gate_fetch_via_api(args, download_folder, target_folder, symbol, &api_months, cutoff).await?;
    }

    Ok(())
}

/// Pull current-month trades from the REST API and serialize them to a
/// parquet sidecar in `_download/` so the convert phase picks them up
/// uniformly with the .csv.gz archives.
async fn gate_fetch_via_api(
    args: &Args,
    download_folder: &PathBuf,
    target_folder: &PathBuf,
    symbol: &str,
    months: &[(i32, u32)],
    cutoff: NaiveDate,
) -> anyhow::Result<()> {
    let pair = gate::to_gate_pair(symbol);
    let cutoff_us = gate::end_of_day_us(cutoff.pred_opt().unwrap_or(cutoff));

    let rows = match args.market {
        Market::Spot => gate_fetch_spot_api(args, download_folder, target_folder, symbol, &pair, cutoff_us).await?,
        Market::Future => gate_fetch_futures_api(args, download_folder, target_folder, symbol, &pair, cutoff_us).await?,
        Market::Option => Vec::new(),
    };

    if rows.is_empty() {
        info!("API returned no new trades");
        return Ok(());
    }

    let df = gate::dataframe_from_api_rows(rows)?;
    let api_path = download_folder.join(format!(
        "{}-api-{:04}{:02}.parquet",
        pair, months[0].0, months[0].1,
    ));
    let mut df_mut = df;
    let file = std::fs::File::create(&api_path)?;
    polars::prelude::ParquetWriter::new(file)
        .with_compression(polars::prelude::ParquetCompression::Zstd(None))
        .finish(&mut df_mut)?;
    info!("API rows staged at {:?} ({} rows)", api_path, df_mut.height());

    Ok(())
}

/// Spot: paginate forward by `last_id`, resuming from the highest id either
/// in the most recent staged archive or in the existing parquet.
async fn gate_fetch_spot_api(
    args: &Args,
    download_folder: &PathBuf,
    target_folder: &PathBuf,
    symbol: &str,
    pair: &str,
    cutoff_us: i64,
) -> anyhow::Result<Vec<(i64, i64, f64, f64, bool)>> {
    let mut start_after_id: i64 = 0;
    if let Some(latest) = latest_staged_archive(download_folder) {
        if let Ok(df) = gate::parse_gz_to_dataframe(&latest, args.market) {
            if let Ok(s) = df.column("agg_trade_id") {
                if let Ok(reduced) = s.max_reduce() {
                    if let Ok(maxv) = reduced.value().try_extract::<i64>() {
                        start_after_id = maxv;
                        info!(
                            "API resume from last_id={} (from {:?})",
                            maxv,
                            latest.file_name().unwrap()
                        );
                    }
                }
            }
        }
    } else if let Some(maxv) = max_agg_trade_id_in_target(target_folder, symbol) {
        start_after_id = maxv;
        info!("API resume from last_id={} (from existing parquet)", maxv);
    }

    let rows = gate::fetch_trades(pair, start_after_id, Some(cutoff_us)).await?;
    Ok(rows)
}

/// Futures: paginate by `from`/`to` over the slice between the latest known
/// timestamp and `cutoff_us`. The latest known ts is derived from staged
/// archives or the existing parquet.
async fn gate_fetch_futures_api(
    args: &Args,
    download_folder: &PathBuf,
    target_folder: &PathBuf,
    symbol: &str,
    pair: &str,
    cutoff_us: i64,
) -> anyhow::Result<Vec<(i64, i64, f64, f64, bool)>> {
    let mut start_us: i64 = 0;
    if let Some(latest) = latest_staged_archive(download_folder) {
        if let Ok(df) = gate::parse_gz_to_dataframe(&latest, args.market) {
            if let Ok(s) = df.column("ts") {
                if let Ok(reduced) = s.max_reduce() {
                    if let Ok(maxv) = reduced.value().try_extract::<i64>() {
                        start_us = maxv + 1;
                        info!(
                            "Futures API resume from ts={}us (from {:?})",
                            start_us,
                            latest.file_name().unwrap()
                        );
                    }
                }
            }
        }
    } else if let Some(maxts) = max_ts_in_target(target_folder, symbol) {
        start_us = maxts + 1;
        info!("Futures API resume from ts={}us (from existing parquet)", start_us);
    }

    let rows = gate::fetch_futures_trades(pair, start_us, cutoff_us).await?;
    Ok(rows)
}

fn latest_staged_archive(download_folder: &PathBuf) -> Option<PathBuf> {
    std::fs::read_dir(download_folder)
        .ok()?
        .flatten()
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .map(|n| n.ends_with(".csv.gz"))
                .unwrap_or(false)
        })
        .max()
}

fn gate_convert_phase(
    args: &Args,
    download_folder: &PathBuf,
    target_folder: &PathBuf,
    symbol: &str,
    today: NaiveDate,
) -> anyhow::Result<()> {
    info!("=== Gate Convert Phase ===");

    let entries: Vec<PathBuf> = std::fs::read_dir(download_folder)
        .ok()
        .map(|it| it.flatten().map(|e| e.path()).collect())
        .unwrap_or_default();

    let gz_files: Vec<PathBuf> = entries
        .iter()
        .filter(|p| p.file_name()
            .and_then(|n| n.to_str())
            .map(|n| n.ends_with(".csv.gz"))
            .unwrap_or(false))
        .cloned()
        .collect();
    let api_parquets: Vec<PathBuf> = entries
        .iter()
        .filter(|p| p.file_name()
            .and_then(|n| n.to_str())
            .map(|n| n.contains("-api-") && n.ends_with(".parquet"))
            .unwrap_or(false))
        .cloned()
        .collect();

    if gz_files.is_empty() && api_parquets.is_empty() {
        info!("No files to convert");
        return Ok(());
    }

    info!(
        "Found {} archive(s), {} API parquet(s)",
        gz_files.len(),
        api_parquets.len()
    );

    let data_by_year: Mutex<HashMap<i32, Vec<polars::frame::DataFrame>>> = Mutex::new(HashMap::new());

    gz_files.par_iter().for_each(|gz| {
        match gate::parse_gz_to_dataframe(gz, args.market) {
            Ok(df) => {
                let filename = gz.file_name().and_then(|n| n.to_str()).unwrap_or("");
                let year = gate::extract_year(filename).unwrap_or_else(|| {
                    // Fallback: pull from any timestamp in the frame
                    today.year()
                });
                data_by_year.lock().unwrap().entry(year).or_default().push(df);
            }
            Err(e) => warn!("Failed to parse {:?}: {}", gz, e),
        }
    });

    for p in &api_parquets {
        use polars::prelude::SerReader;
        match polars::prelude::ParquetReader::new(std::fs::File::open(p)?).finish() {
            Ok(df) => {
                let year = today.year();
                data_by_year.lock().unwrap().entry(year).or_default().push(df);
            }
            Err(e) => warn!("Failed to read API parquet {:?}: {}", p, e),
        }
    }

    let data_by_year = data_by_year.into_inner().unwrap();
    if data_by_year.is_empty() {
        info!("No data after parsing");
        return Ok(());
    }

    for (year, dfs) in data_by_year {
        merge_and_write_year_parquet(target_folder, symbol, args.data_type, year, dfs, today)?;
    }

    // Cleanup .gz files and API parquet stages
    for p in gz_files.iter().chain(api_parquets.iter()) {
        if let Err(e) = std::fs::remove_file(p) {
            warn!("Failed to remove {:?}: {}", p, e);
        } else {
            debug!("Removed {:?}", p.file_name().unwrap_or_default());
        }
    }
    info!("Gate conversion complete; staged files cleaned up");
    Ok(())
}

/// Find the `agg_trade_id` of the most recently timestamped trade across all
/// parquet files for `symbol` in `target_folder`. Returns None if no files.
///
/// Note: Gate trade ids are not globally monotonic (the sequence reset
/// observably on 2025-06-05 for B_USDT), so resuming from the global `max(id)`
/// is unsafe. Resume from the id at `max(time)` instead.
fn max_agg_trade_id_in_target(target_folder: &PathBuf, symbol: &str) -> Option<i64> {
    use polars::prelude::*;
    let entries = std::fs::read_dir(target_folder).ok()?;
    let mut latest: Option<(i64, i64)> = None; // (time_us, agg_trade_id)
    for e in entries.flatten() {
        let path = e.path();
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if !name.starts_with(symbol) || !name.ends_with(".parquet") {
            continue;
        }
        let lf = match LazyFrame::scan_parquet(&path, Default::default()) {
            Ok(lf) => lf,
            Err(_) => continue,
        };
        let df = match lf
            .sort(["time"], SortMultipleOptions::default().with_order_descending(true))
            .limit(1)
            .select([col("ts"), col("agg_trade_id")])
            .collect()
        {
            Ok(df) => df,
            Err(_) => continue,
        };
        if df.height() == 0 {
            continue;
        }
        let ts = df.column("ts").ok()
            .and_then(|s| s.i64().ok().and_then(|c| c.get(0)));
        let id = df.column("agg_trade_id").ok()
            .and_then(|s| s.i64().ok().and_then(|c| c.get(0)));
        if let (Some(ts), Some(id)) = (ts, id) {
            latest = Some(match latest {
                Some((t, _)) if t >= ts => latest.unwrap(),
                _ => (ts, id),
            });
        }
    }
    latest.map(|(_, id)| id)
}

/// Read the maximum `ts` (microseconds since epoch) across all parquet files
/// for `symbol` in `target_folder`. Used by the futures API resume path.
fn max_ts_in_target(target_folder: &PathBuf, symbol: &str) -> Option<i64> {
    use polars::prelude::*;
    let entries = std::fs::read_dir(target_folder).ok()?;
    let mut max_ts: Option<i64> = None;
    for e in entries.flatten() {
        let path = e.path();
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if !name.starts_with(symbol) || !name.ends_with(".parquet") {
            continue;
        }
        let lf = match LazyFrame::scan_parquet(&path, Default::default()) {
            Ok(lf) => lf,
            Err(_) => continue,
        };
        let collected = match lf.select([col("ts").max()]).collect() {
            Ok(df) => df,
            Err(_) => continue,
        };
        if let Ok(s) = collected.column("ts") {
            if let Ok(reduced) = s.max_reduce() {
                if let Ok(v) = reduced.value().try_extract::<i64>() {
                    max_ts = Some(max_ts.map_or(v, |m| m.max(v)));
                }
            }
        }
    }
    max_ts
}
