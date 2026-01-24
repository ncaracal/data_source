use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct AggTrade {
    pub agg_trade_id: i64,
    pub price: f64,
    pub quantity: f64,
    pub first_trade_id: i64,
    pub last_trade_id: i64,
    pub transact_time: i64,
    pub is_buyer_maker: bool,
    pub is_best_match: bool,
}
