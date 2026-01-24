# Data Schema

## Binance AggTrades Parquet Schema

| Column | Type | Description |
|--------|------|-------------|
| agg_trade_id | i64 | Aggregated trade ID |
| time | datetime64[us] | Transaction time (converted from timestamp) |
| price | f64 | Trade price |
| quantity | f64 | Trade quantity |
| first_trade_id | i64 | First trade ID in aggregation |
| last_trade_id | i64 | Last trade ID in aggregation |
| is_buyer_maker | bool | True if buyer is market maker |
| is_best_match | bool | (spot only) Best price match |
| ts | i64 | Original timestamp (microseconds) |

