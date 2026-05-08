use anyhow::Result;
use clap::Parser;
use polars::prelude::*;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;
use tracing::{info, warn};

/// Re-compress aggTrades parquet files at a chosen ZSTD level.
///
/// Reads each `*_aggTrades_*.parquet` under
/// `<TRADE_DATA>/<exchange>/<market>/aggTrades/<symbol>/` and rewrites it
/// with `ParquetCompression::Zstd(Some(level))`. By default the original
/// is replaced; pass `--target-folder` to write into a separate tree.
///
/// Examples
/// --------
/// Re-compress two symbols into a scratch folder at level 20:
/// ```
/// cargo run --release --bin recompression_aggtrade -- \
///     -m spot -s BTCUSDT SOLUSDT \
///     --target-folder /ndata/trade/data/tmp
/// ```
///
/// Replace originals at level 22 (CAREFUL):
/// ```
/// cargo run --release --bin recompression_aggtrade -- -m um --compression-level 22
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, clap::ValueEnum)]
pub enum MarketEnum {
    Spot,
    #[value(alias = "um")]
    FutureUm,
}

impl MarketEnum {
    fn data_path(&self) -> PathBuf {
        match self {
            MarketEnum::Spot => PathBuf::from("spot"),
            MarketEnum::FutureUm => PathBuf::from("future").join("um"),
        }
    }
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Base data directory (defaults to TRADE_DATA env var)
    #[arg(long, env = "TRADE_DATA")]
    data_dir: Option<String>,

    /// Exchange name
    #[arg(short, long, default_value = "binance")]
    exchange: String,

    /// Market type (spot | um)
    #[arg(short, long, value_enum)]
    market: MarketEnum,

    /// Symbols to process (default: every symbol under aggTrades/).
    /// Accepts multiple values after one flag: `-s BTCUSDT SOLUSDT`.
    #[arg(short, long, num_args = 1..)]
    symbols: Vec<String>,

    /// ZSTD compression level (1..=22). Default 20.
    #[arg(long, default_value_t = 20)]
    compression_level: i32,

    /// Output root. If unset, the original file is replaced in-place via a
    /// `.tmp` -> rename swap. If set, files are written to
    /// `<target>/<symbol>/<file>.parquet`.
    #[arg(long)]
    target_folder: Option<String>,
}

fn fmt_size(b: u64) -> String {
    const K: f64 = 1024.0;
    let f = b as f64;
    if f >= K * K * K {
        format!("{:.2} GB", f / (K * K * K))
    } else if f >= K * K {
        format!("{:.2} MB", f / (K * K))
    } else if f >= K {
        format!("{:.2} KB", f / K)
    } else {
        format!("{} B", b)
    }
}

fn process_file(src: &Path, dst: &Path, level: i32) -> Result<(u64, u64, std::time::Duration)> {
    let orig_size = fs::metadata(src)?.len();
    if let Some(parent) = dst.parent() {
        fs::create_dir_all(parent)?;
    }

    let started = Instant::now();
    let f_in = std::fs::File::open(src)?;
    let mut df = ParquetReader::new(f_in).finish()?;

    // Always write to a sibling .tmp first, then rename — safe whether dst
    // overlaps with src or not.
    let tmp = dst.with_extension("parquet.tmp");
    {
        let f_out = std::fs::File::create(&tmp)?;
        ParquetWriter::new(f_out)
            .with_compression(ParquetCompression::Zstd(Some(ZstdLevel::try_new(level)?)))
            .finish(&mut df)?;
    }
    fs::rename(&tmp, dst)?;
    let new_size = fs::metadata(dst)?.len();
    Ok((orig_size, new_size, started.elapsed()))
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let args = Args::parse();

    let level = args.compression_level;
    if !(1..=22).contains(&level) {
        anyhow::bail!("compression_level must be in 1..=22, got {}", level);
    }

    let base_dir = args
        .data_dir
        .clone()
        .or_else(|| std::env::var("TRADE_DATA").ok())
        .unwrap_or_else(|| ".".to_string());
    let base = PathBuf::from(&base_dir);
    let market_path = args.market.data_path();
    let source_dir = base
        .join(&args.exchange)
        .join(&market_path)
        .join("aggTrades");
    if !source_dir.is_dir() {
        anyhow::bail!("source not found: {:?}", source_dir);
    }

    // Discover symbols (filter out _* like _download).
    let mut symbols: Vec<String> = if args.symbols.is_empty() {
        fs::read_dir(&source_dir)?
            .filter_map(|e| {
                let p = e.ok()?.path();
                if !p.is_dir() {
                    return None;
                }
                let name = p.file_name()?.to_string_lossy().to_string();
                if name.starts_with('_') {
                    None
                } else {
                    Some(name)
                }
            })
            .collect()
    } else {
        args.symbols.clone()
    };
    symbols.sort();
    if symbols.is_empty() {
        anyhow::bail!("no symbols found under {:?}", source_dir);
    }

    info!(
        "exchange={} market={:?} level={} symbols={} target={:?}",
        args.exchange,
        args.market,
        level,
        symbols.len(),
        args.target_folder
    );
    info!("source: {:?}", source_dir);

    let mut total_orig: u64 = 0;
    let mut total_new: u64 = 0;
    let mut total_files: usize = 0;

    for symbol in &symbols {
        let sym_dir = source_dir.join(symbol);
        if !sym_dir.is_dir() {
            warn!("skip {}: dir missing", symbol);
            continue;
        }
        let prefix = format!("{}_aggTrades_", symbol);
        let mut files: Vec<PathBuf> = fs::read_dir(&sym_dir)?
            .filter_map(|e| {
                let p = e.ok()?.path();
                let name = p.file_name()?.to_string_lossy().to_string();
                if name.starts_with(&prefix) && name.ends_with(".parquet") {
                    Some(p)
                } else {
                    None
                }
            })
            .collect();
        files.sort();

        info!("=== {} ({} files) ===", symbol, files.len());
        for src in &files {
            let fname = src.file_name().unwrap().to_string_lossy().to_string();
            let dst = match &args.target_folder {
                Some(tf) => PathBuf::from(tf).join(symbol).join(&fname),
                None => src.clone(),
            };
            match process_file(src, &dst, level) {
                Ok((orig, new, dur)) => {
                    let pct = (new as f64 / orig as f64) * 100.0;
                    info!(
                        "  {:<46}  orig={:>10}  new={:>10}  {:6.2}%  ({:.1}s)",
                        fname,
                        fmt_size(orig),
                        fmt_size(new),
                        pct,
                        dur.as_secs_f64()
                    );
                    total_orig += orig;
                    total_new += new;
                    total_files += 1;
                }
                Err(e) => warn!("  {} -> ERROR: {:?}", fname, e),
            }
        }
    }

    let pct = if total_orig > 0 {
        (total_new as f64 / total_orig as f64) * 100.0
    } else {
        0.0
    };
    info!(
        "=== summary: files={} orig={} new={} ratio={:.2}% saved={} ===",
        total_files,
        fmt_size(total_orig),
        fmt_size(total_new),
        pct,
        fmt_size(total_orig.saturating_sub(total_new))
    );
    Ok(())
}
