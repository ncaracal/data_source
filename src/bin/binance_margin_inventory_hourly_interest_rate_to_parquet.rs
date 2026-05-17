//! Periodically snapshot Binance cross-margin **available inventory** and
//! **next-hourly interest rate** for every margin-spot asset, to parquet.
//!
//! Flow
//! ----
//! 0. `GET /sapi/v1/margin/allAssets` (MARKET_DATA, key header only) — the full
//!    cross-margin asset universe. Saved verbatim to `available_margin_spot.json`.
//! 1. Every `--interval-secs`, `GET /sapi/v1/margin/available-inventory?type=MARGIN`
//!    (SIGNED). One call returns all assets. **Weight 50 on the UID bucket.**
//! 2. Same tick, `GET /sapi/v1/margin/next-hourly-interest-rate` (SIGNED) in
//!    batches of 20 assets (Binance hard cap). **Weight 100 on the IP bucket
//!    per call** → ~22 calls per tick for ~425 assets.
//! 3. Append rows `(ts, symbol, inventory, next_hourly_interest_rate)` and
//!    (re)write the run's parquet after every tick so partial results survive.
//!
//! Rate-limit handling (steps 1.1 / 2)
//! -----------------------------------
//! - Two independent buckets are tracked from response headers:
//!   `x-sapi-used-uid-weight-1m` (inventory, limit ~180 000/min) and
//!   `x-sapi-used-ip-weight-1m` (hourly-rate, limit ~12 000/min).
//! - Before each hourly-rate batch we check the last-seen IP weight; if it is
//!   near the ceiling we sleep until the 1-minute window rolls.
//! - HTTP 429/418 → honour `Retry-After` (capped), exponential backoff on
//!   5xx/transport. A fixed inter-batch delay paces the ~22 hourly-rate calls.

use anyhow::{anyhow, Result};
use chrono::Utc;
use clap::Parser;
use hmac::{Hmac, Mac};
use polars::prelude::*;
use polars::prelude::SerReader;
use reqwest::Client;
use serde::Deserialize;
use sha2::Sha256;
use std::path::PathBuf;
use std::time::Duration;
use tracing::{info, warn};

type HmacSha256 = Hmac<Sha256>;

const BASE: &str = "https://api.binance.com";
const HOURLY_BATCH: usize = 20; // Binance hard cap: "size of assets should not larger than 20"
// Binance SAPI per-minute limits (the {usage}/{max} we report against).
const IP_WEIGHT_MAX: u64 = 12_000; // x-sapi-used-ip-weight-1m hard limit
const UID_WEIGHT_MAX: u64 = 180_000; // x-sapi-used-uid-weight-1m hard limit
const IP_WEIGHT_CEILING: u64 = 9_000; // back off well before the IP limit
const UID_WEIGHT_CEILING: u64 = 150_000; // well before the UID limit
const INTER_BATCH_DELAY: Duration = Duration::from_millis(300);

#[derive(Parser, Debug)]
#[command(about = "Snapshot Binance margin inventory + next-hourly interest rate to parquet")]
struct Args {
    /// Seconds between snapshots
    #[arg(long, default_value_t = 60)]
    interval_secs: u64,

    /// Output directory (defaults to $TRADE_DATA/binance/spot/margin_inventory_hourly_interest)
    #[arg(long)]
    out_dir: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AllAsset {
    #[serde(rename = "assetName")]
    asset_name: String,
}

#[derive(Debug, Deserialize)]
struct InventoryResp {
    assets: std::collections::HashMap<String, String>,
    #[serde(rename = "updateTime")]
    #[allow(dead_code)]
    update_time: i64,
}

#[derive(Debug, Deserialize)]
struct HourlyRate {
    asset: String,
    #[serde(rename = "nextHourlyInterestRate")]
    next_hourly_interest_rate: String,
}

fn now_ms() -> i64 {
    Utc::now().timestamp_millis()
}

/// Percent-encode per RFC 3986 (unreserved = A-Z a-z 0-9 - . _ ~), encoding
/// every other byte as %XX. Used for the `assets` value so the string we
/// HMAC-sign is byte-identical to what reqwest transmits — Binance's asset
/// list includes non-ASCII names (e.g. `币安人生`) whose UTF-8 bytes reqwest
/// would otherwise re-encode after we signed the raw form, causing -1022.
fn pct_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len() * 3);
    for &b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{:02X}", b)),
        }
    }
    out
}

fn sign(secret: &str, query: &str) -> String {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).expect("hmac key");
    mac.update(query.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

/// Read the two used-weight buckets from response headers (either may be absent).
fn read_weights(resp: &reqwest::Response) -> (Option<u64>, Option<u64>) {
    let g = |k: &str| {
        resp.headers()
            .get(k)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok())
    };
    (g("x-sapi-used-ip-weight-1m"), g("x-sapi-used-uid-weight-1m"))
}

/// One signed GET with rate-limit handling. Returns the body text on success.
async fn signed_get(
    client: &Client,
    key: &str,
    secret: &str,
    path: &str,
    extra: &str,
) -> Result<(String, Option<u64>, Option<u64>)> {
    let mut attempt = 0u32;
    loop {
        let q = format!("{}&recvWindow=60000&timestamp={}", extra, now_ms());
        let sig = sign(secret, &q);
        let url = format!("{}{}?{}&signature={}", BASE, path, q, sig);

        let resp = match client.get(&url).header("X-MBX-APIKEY", key).send().await {
            Ok(r) => r,
            Err(e) => {
                attempt += 1;
                if attempt > 6 {
                    return Err(anyhow!("transport error after retries: {e}"));
                }
                let b = Duration::from_secs(2u64.pow(attempt.min(5)));
                warn!("transport error {e}; retry {attempt}/6 in {b:?}");
                tokio::time::sleep(b).await;
                continue;
            }
        };

        let status = resp.status();
        let (ipw, uidw) = read_weights(&resp);

        if status.as_u16() == 429 || status.as_u16() == 418 {
            attempt += 1;
            let ra = resp
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(2u64.pow(attempt.min(6)));
            let wait = Duration::from_secs(ra.min(120));
            warn!("HTTP {status} rate-limited; sleeping {wait:?} (attempt {attempt}/6)");
            if attempt > 6 {
                return Err(anyhow!("rate limited (HTTP {status}) after 6 retries"));
            }
            tokio::time::sleep(wait).await;
            continue;
        }

        let body = resp.text().await?;
        if status.is_server_error() {
            attempt += 1;
            if attempt > 6 {
                return Err(anyhow!("HTTP {status} after retries: {body}"));
            }
            let b = Duration::from_secs(2u64.pow(attempt.min(5)));
            warn!("HTTP {status} (server); retry {attempt}/6 in {b:?}");
            tokio::time::sleep(b).await;
            continue;
        }
        if !status.is_success() {
            return Err(anyhow!("HTTP {status}: {body}"));
        }
        return Ok((body, ipw, uidw));
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();
    tracing_subscriber::fmt()
        .with_env_filter("binance_margin_inventory_hourly_interest_rate_to_parquet=info")
        .init();
    let args = Args::parse();

    let key = std::env::var("BINANCE_API_KEY")
        .map_err(|_| anyhow!("BINANCE_API_KEY not set (env or .env)"))?;
    let secret = std::env::var("BINANCE_API_SECRET")
        .map_err(|_| anyhow!("BINANCE_API_SECRET not set (env or .env)"))?;

    let trade_data =
        std::env::var("TRADE_DATA").unwrap_or_else(|_| "/ndata/trade/data".to_string());
    let out_dir = PathBuf::from(args.out_dir.unwrap_or_else(|| {
        format!("{trade_data}/binance/spot/margin_inventory_hourly_interest")
    }));
    std::fs::create_dir_all(&out_dir)?;

    let client = Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;

    // -- Step 0: allAssets (MARKET_DATA, key header only, no signature) --------
    let aa_body = client
        .get(format!("{BASE}/sapi/v1/margin/allAssets"))
        .header("X-MBX-APIKEY", &key)
        .send()
        .await?
        .text()
        .await?;
    let all_assets: Vec<AllAsset> = serde_json::from_str(&aa_body)
        .map_err(|e| anyhow!("allAssets parse: {e} body={aa_body}"))?;
    let json_path = out_dir.join("available_margin_spot.json");
    std::fs::write(&json_path, &aa_body)?;
    let assets: Vec<String> = all_assets.iter().map(|a| a.asset_name.clone()).collect();
    info!(
        "Step 0: {} cross-margin assets -> {:?}",
        assets.len(),
        json_path
    );

    // Runs forever. One parquet per UTC day:
    //   {out_dir}/margin_inv_hourly_{YYYY-MM-DD}.parquet
    // Each tick appends to that day's file (read existing + vstack + rewrite),
    // so a restart mid-day continues the same file, and the day rollover
    // naturally starts a fresh one. The asset universe is refreshed (and
    // available_margin_spot.json rewritten) on each day boundary.
    let mut tick = 0u64;
    let mut last_ip_w: u64 = 0;
    let mut current_day = String::new();
    let mut assets = assets;

    loop {
        tick += 1;
        let ts = now_ms();
        let day = chrono::DateTime::from_timestamp_millis(ts)
            .map(|d| d.format("%Y-%m-%d").to_string())
            .unwrap_or_else(|| "unknown".to_string());

        // Day rollover (or first tick): refresh the cross-margin asset list.
        if day != current_day {
            if !current_day.is_empty() {
                info!("Day rollover {current_day} -> {day}; refreshing asset list");
            }
            if let Ok(b) = client
                .get(format!("{BASE}/sapi/v1/margin/allAssets"))
                .header("X-MBX-APIKEY", &key)
                .send()
                .await
            {
                if let Ok(txt) = b.text().await {
                    if let Ok(v) = serde_json::from_str::<Vec<AllAsset>>(&txt) {
                        assets = v.into_iter().map(|a| a.asset_name).collect();
                        let _ = std::fs::write(&json_path, &txt);
                    }
                }
            }
            current_day = day.clone();
        }
        let parquet_path = out_dir.join(format!("margin_inv_hourly_{day}.parquet"));

        info!("--- tick {tick} ({day}) -> {:?} ---", parquet_path);

        // -- Step 1: available-inventory (UID weight 50) ---------------------
        let inv_map: std::collections::HashMap<String, String> = match signed_get(
            &client,
            &key,
            &secret,
            "/sapi/v1/margin/available-inventory",
            "type=MARGIN",
        )
        .await
        {
            Ok((body, _ipw, uidw)) => {
                if let Some(u) = uidw {
                    info!(
                        "inventory ok; UID weight {}/{} ({:.1}%)",
                        u,
                        UID_WEIGHT_MAX,
                        100.0 * u as f64 / UID_WEIGHT_MAX as f64
                    );
                    if u >= UID_WEIGHT_CEILING {
                        warn!(
                            "UID weight {}/{} >= ceiling {}; cooling 10s",
                            u, UID_WEIGHT_MAX, UID_WEIGHT_CEILING
                        );
                        tokio::time::sleep(Duration::from_secs(10)).await;
                    }
                }
                let r: InventoryResp = serde_json::from_str(&body)
                    .map_err(|e| anyhow!("inventory parse: {e} body={body}"))?;
                r.assets
            }
            Err(e) => {
                warn!("inventory fetch failed this tick: {e}");
                Default::default()
            }
        };

        // -- Step 2: next-hourly-interest-rate, 20-asset batches (IP w=100) --
        let mut rate_map: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();
        for chunk in assets.chunks(HOURLY_BATCH) {
            // 1.1 weight care: if last observed IP weight is high, wait out
            // the 1-minute window before issuing another weight-100 call.
            if last_ip_w >= IP_WEIGHT_CEILING {
                warn!(
                    "IP weight {}/{} >= ceiling {}; cooling 60s for window reset",
                    last_ip_w, IP_WEIGHT_MAX, IP_WEIGHT_CEILING
                );
                tokio::time::sleep(Duration::from_secs(60)).await;
                last_ip_w = 0;
            }
            // Fully percent-encode the comma-joined asset list so the signed
            // string equals what reqwest sends byte-for-byte (handles commas
            // AND non-ASCII asset names like `币安人生` → %E5%B8%81...).
            let extra = format!(
                "assets={}&isIsolated=FALSE",
                pct_encode(&chunk.join(","))
            );
            match signed_get(
                &client,
                &key,
                &secret,
                "/sapi/v1/margin/next-hourly-interest-rate",
                &extra,
            )
            .await
            {
                Ok((body, ipw, _uidw)) => {
                    if let Some(w) = ipw {
                        last_ip_w = w;
                        info!(
                            "hourly batch ({} assets); IP weight {}/{} ({:.1}%)",
                            chunk.len(),
                            w,
                            IP_WEIGHT_MAX,
                            100.0 * w as f64 / IP_WEIGHT_MAX as f64
                        );
                    }
                    match serde_json::from_str::<Vec<HourlyRate>>(&body) {
                        Ok(v) => {
                            for hr in v {
                                rate_map.insert(hr.asset, hr.next_hourly_interest_rate);
                            }
                        }
                        Err(_) => warn!("hourly batch parse skipped: {body}"),
                    }
                }
                Err(e) => warn!("hourly batch failed ({} assets): {e}", chunk.len()),
            }
            tokio::time::sleep(INTER_BATCH_DELAY).await;
        }

        // -- Step 3: this tick's rows (null where a side is missing) --------
        let n = assets.len();
        let row_ts: Vec<i64> = vec![ts; n];
        let row_sym: Vec<String> = assets.clone();
        let row_inv: Vec<Option<f64>> = assets
            .iter()
            .map(|a| inv_map.get(a).and_then(|s| s.parse::<f64>().ok()))
            .collect();
        let row_rate: Vec<Option<f64>> = assets
            .iter()
            .map(|a| rate_map.get(a).and_then(|s| s.parse::<f64>().ok()))
            .collect();
        let inv_nn = row_inv.iter().filter(|x| x.is_some()).count();
        let rate_nn = row_rate.iter().filter(|x| x.is_some()).count();

        let tick_df = DataFrame::new(vec![
            Column::new("ts".into(), &row_ts),
            Column::new("symbol".into(), &row_sym),
            Column::new("inventory".into(), &row_inv),
            Column::new("next_hourly_interest_rate".into(), &row_rate),
        ])?;

        // Append to the same day's parquet: read existing + vstack + rewrite.
        // (A mid-day restart resumes the same file; a new day starts fresh.)
        let mut out_df = if parquet_path.exists() {
            let existing = ParquetReader::new(std::fs::File::open(&parquet_path)?)
                .finish()
                .unwrap_or_else(|e| {
                    warn!("re-reading {:?} failed ({e}); starting fresh", parquet_path);
                    tick_df.clone()
                });
            if existing.height() > 0 && existing.get_column_names() == tick_df.get_column_names()
            {
                existing.vstack(&tick_df)?
            } else {
                tick_df.clone()
            }
        } else {
            tick_df.clone()
        };

        let write_t0 = std::time::Instant::now();
        ParquetWriter::new(std::fs::File::create(&parquet_path)?)
            .with_compression(ParquetCompression::Zstd(None))
            .finish(&mut out_df)?;
        let write_cost = write_t0.elapsed();
        let size = std::fs::metadata(&parquet_path).map(|m| m.len()).unwrap_or(0);
        info!(
            "tick {tick}: +{n} rows (inv {inv_nn} / rate {rate_nn} non-null); \
             day total {} rows; parquet write cost {:.3}s ({:.2} MB) -> {:?}",
            out_df.height(),
            write_cost.as_secs_f64(),
            size as f64 / 1_048_576.0,
            parquet_path
        );

        // Rotate when the active daily file exceeds 10 MB: move it aside to
        // margin_inv_hourly_{day}.{ts}.parquet and let the next tick start a
        // fresh margin_inv_hourly_{day}.parquet (the read-existing branch
        // simply sees no active file and begins anew).
        const ROTATE_BYTES: u64 = 10 * 1_048_576;
        if size > ROTATE_BYTES {
            let rotated = out_dir.join(format!("margin_inv_hourly_{day}.{ts}.parquet"));
            match std::fs::rename(&parquet_path, &rotated) {
                Ok(_) => info!(
                    "rotated: {:.2} MB > 10 MB -> archived {:?}; new ticks write fresh {:?}",
                    size as f64 / 1_048_576.0,
                    rotated,
                    parquet_path
                ),
                Err(e) => warn!("rotation rename failed: {e}"),
            }
        }

        tokio::time::sleep(Duration::from_secs(args.interval_secs)).await;
    }
}
