use crate::error::{AppError, Result};
use chrono::NaiveDate;
use reqwest::Client;
use serde::Deserialize;
use serde_json::Value;
use tracing::{debug, info};

const SPOT_API_BASE: &str = "https://api.gateio.ws/api/v4/spot/trades";
const FUTURES_API_BASE: &str = "https://api.gateio.ws/api/v4/futures/usdt/trades";
const PAGE_LIMIT: usize = 1000;

#[derive(Debug, Deserialize)]
struct ApiTrade {
    id: String,
    create_time_ms: String,
    side: String,
    amount: String,
    price: String,
}

/// Returned trade row, normalized for the parquet pipeline.
/// Tuple: `(id, ts_us, price, quantity, is_buyer_maker)`.
pub type Row = (i64, i64, f64, f64, bool);

/// Fetch all spot trades for `pair` (Gate format, e.g. `B_USDT`) with `id > start_after_id`,
/// stopping when no more rows are returned. Optionally cap at trades whose timestamp falls
/// before `cutoff_us` (microseconds since epoch) — used to bound the current-month window.
///
/// Pagination scheme: Gate's `last_id` parameter returns trades ascending after the given id,
/// up to `PAGE_LIMIT` per call. The 30-day retention window is enforced by the server.
pub async fn fetch_trades(
    pair: &str,
    start_after_id: i64,
    cutoff_us: Option<i64>,
) -> Result<Vec<Row>> {
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .map_err(AppError::Http)?;

    let mut last_id = start_after_id;
    let mut out: Vec<Row> = Vec::new();
    let mut empty_pages = 0;

    loop {
        let url = format!(
            "{}?currency_pair={}&limit={}&last_id={}",
            SPOT_API_BASE, pair, PAGE_LIMIT, last_id
        );
        debug!("Gate API GET {}", url);

        let resp = client.get(&url).send().await.map_err(AppError::Http)?;
        if !resp.status().is_success() {
            return Err(AppError::Parse(format!(
                "Gate API HTTP {}: {}",
                resp.status(),
                url
            )));
        }
        let body = resp.text().await.map_err(AppError::Http)?;
        let trades: Vec<ApiTrade> = serde_json::from_str(&body)
            .map_err(|e| AppError::Parse(format!("Gate API JSON parse: {} body={}", e, body)))?;

        if trades.is_empty() {
            empty_pages += 1;
            if empty_pages >= 2 {
                break;
            }
            // Some empty responses can be transient — retry once
            continue;
        }
        empty_pages = 0;

        let page_count = trades.len();
        let mut max_id_in_page = last_id;
        let mut hit_cutoff = false;

        for t in trades {
            let id: i64 = t.id.parse().map_err(|e| {
                AppError::Parse(format!("Bad trade id `{}`: {}", t.id, e))
            })?;
            // create_time_ms is "1777948690403.294000" — fractional ms; parse as f64
            let ts_ms_f: f64 = t.create_time_ms.parse().unwrap_or(0.0);
            let ts_us = (ts_ms_f * 1000.0) as i64;
            let price: f64 = t.price.parse().unwrap_or(0.0);
            let qty: f64 = t.amount.parse().unwrap_or(0.0);
            let is_buyer_maker = t.side == "sell"; // sell aggressor → buyer is maker

            if let Some(cut) = cutoff_us {
                if ts_us >= cut {
                    hit_cutoff = true;
                    continue;
                }
            }

            out.push((id, ts_us, price, qty, is_buyer_maker));
            if id > max_id_in_page {
                max_id_in_page = id;
            }
        }

        if max_id_in_page == last_id {
            // No new id seen — guard against infinite loop.
            break;
        }
        last_id = max_id_in_page;

        if hit_cutoff || page_count < PAGE_LIMIT {
            break;
        }
    }

    info!("Gate API (spot): fetched {} trades for {}", out.len(), pair);
    Ok(out)
}

/// Fetch all USDT-margined futures trades for `pair` in `[start_us, cutoff_us)`.
///
/// Pagination scheme: time-window only, no `offset`.
///
/// Why no offset: empirically Gate's `/futures/usdt/trades` ignores `offset`
/// values that aren't on its internal page boundary — `offset=1000` returns
/// the same page as `offset=0`, then `offset=1001` jumps two pages forward —
/// so anything that paginates by `offset += limit` silently skips data.
///
/// Time-window-only scheme:
///  1. `to_s = cutoff` initially. Fetch up to 1000 newest trades in `[from_s, to_s]`.
///  2. Track the oldest second seen in the page (`oldest_sec`).
///  3. Set new `to_s = oldest_sec + 1` (so the next call INCLUDES the boundary
///     second; we dedup by `id` to drop the trades we already have at that second).
///  4. Repeat until response < `limit` or `to_s <= from_s`.
///
/// The `+1` matters: Gate's `to` is exclusive of `to_s` itself (i.e. `to=N`
/// returns trades whose floor-second is `< N`). Using `oldest_sec` (without the
/// `+1`) would skip every trade that shares the boundary second with the page's
/// oldest one. The price is per-call duplication of <1000 trades worth of data,
/// which dedup absorbs.
///
/// Bail safety: if any single second has ≥ `limit` trades, the page would only
/// ever cover that one second and we'd loop forever. We detect this via the
/// `n_new == 0 && page_count == limit` condition and force-shrink to break out.
pub async fn fetch_futures_trades(
    pair: &str,
    start_us: i64,
    cutoff_us: i64,
) -> Result<Vec<Row>> {
    if cutoff_us <= start_us {
        return Ok(Vec::new());
    }
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .map_err(AppError::Http)?;

    let from_s = (start_us / 1_000_000) as u64;
    let mut to_s = ((cutoff_us + 999_999) / 1_000_000) as u64;
    let mut out: Vec<Row> = Vec::new();
    let mut seen_ids: std::collections::HashSet<i64> = std::collections::HashSet::new();
    let mut call_count: u64 = 0;

    while to_s > from_s {
        let url = format!(
            "{}?contract={}&limit={}&from={}&to={}",
            FUTURES_API_BASE, pair, PAGE_LIMIT, from_s, to_s
        );
        debug!("Gate futures GET {}", url);

        let resp = client.get(&url).send().await.map_err(AppError::Http)?;
        if !resp.status().is_success() {
            return Err(AppError::Parse(format!(
                "Gate futures API HTTP {}: {}",
                resp.status(),
                url
            )));
        }
        let body = resp.text().await.map_err(AppError::Http)?;
        let trades: Vec<Value> = serde_json::from_str(&body).map_err(|e| {
            AppError::Parse(format!("Gate futures JSON parse: {} body={}", e, body))
        })?;

        call_count += 1;
        let page_count = trades.len();
        if page_count == 0 {
            break;
        }

        let mut oldest_in_page_s: u64 = to_s;
        let mut n_new = 0usize;

        for t in trades {
            let id = t
                .get("id")
                .and_then(|v| v.as_i64())
                .ok_or_else(|| AppError::Parse(format!("futures trade missing id: {}", t)))?;
            let ts_f = t
                .get("create_time_ms")
                .or_else(|| t.get("create_time"))
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);
            let ts_us: i64 = if ts_f < 1.0e12 {
                (ts_f * 1_000_000.0) as i64
            } else {
                (ts_f * 1_000.0) as i64
            };
            let size_f = t.get("size").and_then(|v| v.as_f64()).unwrap_or(0.0);
            let price: f64 = t
                .get("price")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0);
            let qty = size_f.abs();
            let is_buyer_maker = size_f < 0.0;

            if ts_us < start_us || ts_us >= cutoff_us {
                continue;
            }
            if seen_ids.insert(id) {
                out.push((id, ts_us, price, qty, is_buyer_maker));
                n_new += 1;
            }
            let ts_floor = ts_f as u64;
            if ts_floor < oldest_in_page_s {
                oldest_in_page_s = ts_floor;
            }
        }

        if page_count < PAGE_LIMIT {
            // Window drained.
            break;
        }

        // Advance `to_s`. Use `oldest_sec + 1` so the next page can include
        // any trade at `oldest_sec` we didn't capture (page filled before
        // exhausting that second); dedup will drop the repeats.
        let mut new_to = oldest_in_page_s.saturating_add(1);

        // Bail out if a single second has ≥ limit trades — the page is
        // entirely within one second and we'd never make progress otherwise.
        if n_new == 0 || new_to >= to_s {
            new_to = to_s.saturating_sub(1);
            if new_to <= from_s {
                break;
            }
        }
        to_s = new_to;
    }

    info!(
        "Gate API (futures): fetched {} unique trades for {} in {} call(s)",
        out.len(),
        pair,
        call_count
    );
    Ok(out)
}

/// `cutoff_us` for "end of `end_date`" (exclusive day-after midnight UTC).
pub fn end_of_day_us(end_date: NaiveDate) -> i64 {
    let next = end_date.succ_opt().unwrap_or(end_date);
    let dt = next.and_hms_opt(0, 0, 0).unwrap().and_utc();
    dt.timestamp_micros()
}
