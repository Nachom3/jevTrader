//! V1 Lead-Lag Maker: features, questions, signal, quote rule.
//!
//! Spec: `docs/strategy-lead-lag-v1.md`. One Jev request carries all 8 V1
//! outputs (they evaluate in parallel over the same state). Rust owns the
//! features and the final quote decision; Jev only judges.

#[allow(unused_imports)]
pub use jevtrader::jev::response::{TickDistribution, V1Signal};
use serde::{Deserialize, Serialize};

/// Deterministic features Rust computes from external venues.
/// Jev never sees raw ticks, only this judged-ready summary.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeadLagFeatures {
    // -- Resolution --
    pub target: f64,
    pub time_remaining_secs: u64,
    pub resolution_source: String,
    // -- Underlying --
    pub spot: f64,
    pub distance_to_target_pct: f64,
    pub ret_250ms_pct: f64,
    pub ret_1s_pct: f64,
    pub ret_5s_pct: f64,
    pub ret_30s_pct: f64,
    pub ret_5m_pct: f64,
    pub realized_vol_1m_pct: f64,
    pub realized_vol_5m_pct: f64,
    pub binance_microprice: f64,
    pub coinbase_microprice: f64,
    pub perp_price: f64,
    pub perp_basis_pct: f64,
    // -- Order flow (external) --
    pub buy_vol_1s: f64,
    pub sell_vol_1s: f64,
    pub ofi_1s: f64,
    pub ofi_5s: f64,
    pub book_imbalance: f64,
    pub aggressive_buy_ratio: f64,
    // -- Cross-exchange --
    pub binance_coinbase_diff_pct: f64,
    pub spot_perp_diff_pct: f64,
}

/// Polymarket side of the state: YES book + short price history.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolySnapshot {
    pub yes_bid: f64,
    pub yes_ask: f64,
    pub bid_depth: f64,
    pub ask_depth: f64,
    pub spread: f64,
    pub book_imbalance: f64,
    pub last_trade_price: f64,
    pub price_1s_ago: f64,
    pub price_5s_ago: f64,
    pub price_30s_ago: f64,
}

/// Pure quote rule: post-only BUY one tick over the bid, or nothing.
pub fn should_quote(s: &V1Signal, thresholds: &jevtrader::config::QuoteThresholds) -> bool {
    s.underreact_up > thresholds.under_min
        && s.p_up_ge_1_tick() > thresholds.next_up_min
        && s.move_persists > thresholds.persist_min
        && s.fill_before_decay > thresholds.fill_min
        && s.fill_toxic < thresholds.toxic_max
        && s.underreact_down < thresholds.conflict_max
        && s.no_pressure_5s < thresholds.no_pressure_max
}
