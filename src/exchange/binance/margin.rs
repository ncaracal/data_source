//! Binance cross-margin daily interest rate history.
//!
//! Endpoint: `GET /sapi/v1/margin/interestRateHistory` (SIGNED / USER_DATA).
//! Requires `BINANCE_API_KEY` + `BINANCE_API_SECRET` (read from env / .env).
//!
//! API constraints we work around:
//!  - `asset` is mandatory; this is an *asset* (e.g. `STORJ`), not a trading pair.
//!  - The window `[startTime, endTime]` may not exceed 30 days, so "all history"
//!    is fetched by walking backwards in 30-day windows.
//!  - Binance only retains the most recent ~6 months for this endpoint, so we
//!    stop after a couple of consecutive empty windows instead of looping forever.
//!
//! Rate limiting: see [`get_signed`] — we honour `Retry-After` on HTTP 429/418,
//! back off exponentially on 5xx / transport errors, proactively sleep when the
//! reported used IP weight gets high, and pace successive windows with a fixed
//! inter-request delay.

use crate::error::{AppError, Result};
use hmac::{Hmac, Mac};
use reqwest::Client;
use serde::Deserialize;
use sha2::Sha256;
use std::time::Duration;
use tracing::{info, warn};

type HmacSha256 = Hmac<Sha256>;

const BASE: &str = "https://api.binance.com";
const PATH: &str = "/sapi/v1/margin/interestRateHistory";

/// Max window the endpoint accepts (30 days, in ms). Stay 1h short to be safe.
const WINDOW_MS: i64 = 30 * 24 * 60 * 60 * 1000 - 60 * 60 * 1000;
/// Stop walking back after this many consecutive empty windows.
const MAX_EMPTY_WINDOWS: u32 = 2;
/// Hard floor on how far back we walk (safety net): ~7.5 years of 30d windows,
/// i.e. older than Binance margin itself (launched 2019) — in practice we stop
/// far earlier on empty windows or an "asset not supported" boundary.
const MAX_WINDOWS: u32 = 95;
/// Polite delay between successive windows to stay well under SAPI IP limits.
const INTER_REQUEST_DELAY: Duration = Duration::from_millis(400);
/// Sleep this long once reported used IP weight crosses the soft ceiling.
const WEIGHT_SOFT_CEILING: u64 = 8_000;

/// One interest-rate sample. `timestamp` is ms since epoch.
#[derive(Debug, Clone, Deserialize)]
pub struct InterestRate {
    pub asset: String,
    #[serde(rename = "dailyInterestRate")]
    pub daily_interest_rate: String,
    pub timestamp: i64,
    #[serde(rename = "vipLevel")]
    pub vip_level: i64,
}

/// Binance error envelope (returned with non-2xx for bad params, e.g. unknown asset).
#[derive(Debug, Deserialize)]
struct BinanceErr {
    code: i64,
    msg: String,
}

/// One entry of `GET /sapi/v1/margin/allAssets` (the currently-supported
/// cross-margin asset list). MARKET_DATA security: API key header, no signature.
#[derive(Debug, Deserialize)]
struct AllAsset {
    #[serde(rename = "assetName")]
    asset_name: String,
    #[serde(rename = "isBorrowable")]
    is_borrowable: bool,
}

/// Fetch the set of cross-margin assets Binance currently supports, mapped to
/// whether each is borrowable. Note: this reflects *current* support only — it
/// does not tell you when an asset was listed, so an asset present here can
/// still have a limited interest-rate history (older windows return -11027).
async fn fetch_supported_assets(
    client: &Client,
    api_key: &str,
) -> Result<std::collections::HashMap<String, bool>> {
    let url = format!("{}/sapi/v1/margin/allAssets", BASE);
    let resp = client
        .get(&url)
        .header("X-MBX-APIKEY", api_key)
        .send()
        .await
        .map_err(AppError::Http)?;
    let status = resp.status();
    let body = resp.text().await.map_err(AppError::Http)?;
    if !status.is_success() {
        return Err(AppError::Parse(format!("allAssets HTTP {}: {}", status, body)));
    }
    let list: Vec<AllAsset> = serde_json::from_str(&body)
        .map_err(|e| AppError::Parse(format!("allAssets JSON parse: {} body={}", e, body)))?;
    Ok(list
        .into_iter()
        .map(|a| (a.asset_name, a.is_borrowable))
        .collect())
}

/// Outcome of a single signed window request.
enum WindowResult {
    /// Rows for the window (possibly empty).
    Rows(Vec<InterestRate>),
    /// Binance reported the asset isn't margin-supported for this (older) window.
    /// Treated as the lower bound of history — stop walking back, keep prior data.
    Unsupported,
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn sign(secret: &str, query: &str) -> String {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .expect("HMAC accepts keys of any size");
    mac.update(query.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

/// Perform one signed GET, with full rate-limit / transient-error handling.
///
/// Returns the parsed body on success. On HTTP 429 (rate limited) or 418
/// (IP auto-banned) we read `Retry-After` and sleep for it (capped), then
/// retry — up to `max_retries` times. Transport errors and 5xx use capped
/// exponential backoff. A 4xx that is *not* 429/418 is a hard error (e.g.
/// invalid asset) and is returned immediately so the caller can report it.
async fn get_signed(
    client: &Client,
    api_key: &str,
    api_secret: &str,
    params: &[(&str, String)],
) -> Result<WindowResult> {
    let max_retries = 6u32;
    let mut attempt = 0u32;

    loop {
        // recvWindow + fresh timestamp on every attempt (clock-skew safe).
        let mut query = String::new();
        for (k, v) in params {
            if !query.is_empty() {
                query.push('&');
            }
            query.push_str(k);
            query.push('=');
            query.push_str(v);
        }
        query.push_str(&format!("&recvWindow=60000&timestamp={}", now_ms()));
        let signature = sign(api_secret, &query);
        let url = format!("{}{}?{}&signature={}", BASE, PATH, query, signature);

        let resp = match client
            .get(&url)
            .header("X-MBX-APIKEY", api_key)
            .send()
            .await
        {
            Ok(r) => r,
            Err(e) => {
                attempt += 1;
                if attempt > max_retries {
                    return Err(AppError::Http(e));
                }
                let backoff = Duration::from_secs(2u64.pow(attempt.min(5)));
                warn!("Transport error ({}); retry {}/{} in {:?}", e, attempt, max_retries, backoff);
                tokio::time::sleep(backoff).await;
                continue;
            }
        };

        let status = resp.status();

        // Surface how loaded the IP is, and self-throttle before we get banned.
        let used_weight = resp
            .headers()
            .get("x-mbx-used-weight-1m")
            .or_else(|| resp.headers().get("x-sapi-used-ip-weight-1m"))
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok());
        if let Some(w) = used_weight {
            if w >= WEIGHT_SOFT_CEILING {
                warn!("Used IP weight {} >= {}; cooling down 10s", w, WEIGHT_SOFT_CEILING);
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
        }

        if status.as_u16() == 429 || status.as_u16() == 418 {
            attempt += 1;
            let retry_after = resp
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(2u64.pow(attempt.min(6)));
            // Cap any single sleep at 2 minutes so we never wedge indefinitely.
            let wait = Duration::from_secs(retry_after.min(120));
            warn!(
                "HTTP {} (rate limited); honoring Retry-After: sleeping {:?} (attempt {}/{})",
                status, wait, attempt, max_retries
            );
            if attempt > max_retries {
                return Err(AppError::Parse(format!(
                    "Rate limited (HTTP {}) after {} retries",
                    status, max_retries
                )));
            }
            tokio::time::sleep(wait).await;
            continue;
        }

        let body = resp.text().await.map_err(AppError::Http)?;

        if status.is_server_error() {
            attempt += 1;
            if attempt > max_retries {
                return Err(AppError::Parse(format!("HTTP {} after retries: {}", status, body)));
            }
            let backoff = Duration::from_secs(2u64.pow(attempt.min(5)));
            warn!("HTTP {} (server); retry {}/{} in {:?}", status, attempt, max_retries, backoff);
            tokio::time::sleep(backoff).await;
            continue;
        }

        if !status.is_success() {
            // Hard client error — invalid asset, bad signature, etc.
            if let Ok(e) = serde_json::from_str::<BinanceErr>(&body) {
                // -11027 ("asset X is not supported") is returned for windows
                // entirely before the asset became cross-margin tradable. That
                // is a *history boundary*, not a real failure — signal the
                // caller to stop paging instead of discarding collected data.
                if e.code == -11027 || e.msg.contains("is not supported") {
                    return Ok(WindowResult::Unsupported);
                }
                return Err(AppError::Parse(format!(
                    "Binance API error (HTTP {}): code={} msg={}",
                    status, e.code, e.msg
                )));
            }
            return Err(AppError::Parse(format!("HTTP {}: {}", status, body)));
        }

        let rows: Vec<InterestRate> = serde_json::from_str(&body).map_err(|e| {
            AppError::Parse(format!("interestRateHistory JSON parse: {} body={}", e, body))
        })?;
        return Ok(WindowResult::Rows(rows));
    }
}

/// Fetch the full available cross-margin daily interest-rate history for `asset`
/// (e.g. `STORJ`), walking backwards from now in 30-day windows until Binance
/// stops returning data. Result is sorted ascending by timestamp, deduped.
pub async fn fetch_interest_rate_history(asset: &str) -> Result<Vec<InterestRate>> {
    let api_key = std::env::var("BINANCE_API_KEY")
        .map_err(|_| AppError::Config("BINANCE_API_KEY not set (env or .env)".into()))?;
    let api_secret = std::env::var("BINANCE_API_SECRET")
        .map_err(|_| AppError::Config("BINANCE_API_SECRET not set (env or .env)".into()))?;

    let client = Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(AppError::Http)?;

    // Pre-flight: confirm `asset` is a currently-supported cross-margin asset.
    // This cheaply rejects typos / non-margin coins (which would otherwise just
    // return empty), and warns on assets that exist but aren't borrowable.
    match fetch_supported_assets(&client, &api_key).await {
        Ok(supported) => match supported.get(asset) {
            None => {
                return Err(AppError::Parse(format!(
                    "`{}` is not a Binance cross-margin asset (not in /sapi/v1/margin/allAssets, \
                     {} supported). No interest-rate history exists.",
                    asset,
                    supported.len()
                )));
            }
            Some(false) => {
                warn!(
                    "{}: listed as a cross-margin asset but isBorrowable=false; \
                     interest-rate history may be empty or short",
                    asset
                );
            }
            Some(true) => {
                info!("{}: confirmed borrowable cross-margin asset", asset);
            }
        },
        Err(e) => {
            // Non-fatal: if the asset list call fails, proceed anyway — the
            // history call itself will still surface a real error.
            warn!("Could not verify asset list ({}); proceeding without pre-flight", e);
        }
    }

    let mut all: Vec<InterestRate> = Vec::new();
    let mut end = now_ms();
    let mut empty_streak = 0u32;
    let mut windows = 0u32;

    while empty_streak < MAX_EMPTY_WINDOWS && windows < MAX_WINDOWS {
        let start = end - WINDOW_MS;
        let params = vec![
            ("asset", asset.to_string()),
            ("startTime", start.to_string()),
            ("endTime", end.to_string()),
        ];

        let rows = match get_signed(&client, &api_key, &api_secret, &params).await? {
            WindowResult::Unsupported => {
                info!(
                    "{}: reached history boundary (asset not cross-margin before {}); stopping",
                    asset,
                    chrono::DateTime::from_timestamp_millis(end)
                        .map(|d| d.format("%Y-%m-%d").to_string())
                        .unwrap_or_default()
                );
                break;
            }
            WindowResult::Rows(r) => r,
        };
        windows += 1;

        if rows.is_empty() {
            empty_streak += 1;
        } else {
            empty_streak = 0;
            info!(
                "{}: window {} .. {} -> {} rows",
                asset,
                chrono::DateTime::from_timestamp_millis(start)
                    .map(|d| d.format("%Y-%m-%d").to_string())
                    .unwrap_or_default(),
                chrono::DateTime::from_timestamp_millis(end)
                    .map(|d| d.format("%Y-%m-%d").to_string())
                    .unwrap_or_default(),
                rows.len()
            );
            all.extend(rows);
        }

        // Next (older) window; stop if we've gone past the epoch-ish floor.
        end = start;
        if end <= 0 {
            break;
        }
        // Pace successive windows so a multi-asset run stays under IP limits.
        tokio::time::sleep(INTER_REQUEST_DELAY).await;
    }

    // Dedup by timestamp, sort ascending.
    all.sort_by_key(|r| r.timestamp);
    all.dedup_by_key(|r| r.timestamp);

    info!(
        "{}: collected {} interest-rate samples across {} window(s)",
        asset,
        all.len(),
        windows
    );
    Ok(all)
}
