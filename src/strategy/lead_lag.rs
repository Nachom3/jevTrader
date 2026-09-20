//! V1 Lead-Lag Maker: features, questions, signal, quote rule.
//!
//! Spec: `docs/strategy-lead-lag-v1.md`. One Jev request carries all 8 V1
//! outputs (they evaluate in parallel over the same state). Rust owns the
//! features and the final quote decision; Jev only judges.
//!
//! Executable prices serialize in Jev state JSON as integer micro-units.

use jevtrader::domain::PriceTicks;
#[allow(unused_imports)]
pub use jevtrader::jev::response::{TickDistribution, V1Signal};
use serde::{Deserialize, Serialize};

/// Deterministic features Rust computes from external venues.
/// Jev never sees raw ticks, only this judged-ready summary.
///
/// Horizon-extension rule (multi-market stage): new temporal fields are
/// purely additive and default to `0.0`/`0`/`""` when the caller has no data
/// for that horizon. Thresholds and the 8 Jev questions never change.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeadLagFeatures {
    // -- Resolution --
    pub target: f64,
    pub time_remaining_secs: u64,
    pub resolution_source: String,
    // -- Contract (which of the 8 markets this snapshot belongs to) --
    pub asset_symbol: String,
    pub horizon_label: String,
    pub horizon_secs: u64,
    // -- Underlying --
    pub spot: f64,
    pub distance_to_target_pct: f64,
    pub ret_250ms_pct: f64,
    pub ret_1s_pct: f64,
    pub ret_5s_pct: f64,
    pub ret_30s_pct: f64,
    pub ret_1m_pct: f64,
    pub ret_5m_pct: f64,
    pub ret_15m_pct: f64,
    pub ret_30m_pct: f64,
    pub ret_1h_pct: f64,
    pub realized_vol_1m_pct: f64,
    pub realized_vol_5m_pct: f64,
    pub realized_vol_1h_pct: f64,
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
    // -- Order flow (Polymarket tape, V2 only; 0.0 in V1 states) --
    // Tape trades are sparse, so only 5s aggregates are populated; 1s
    // equivalents would be near-always zero and are omitted by design.
    pub poly_ofi_5s: f64,
    pub poly_aggressive_buy_ratio: f64,
    pub poly_buy_vol_5s: f64,
    pub poly_sell_vol_5s: f64,
    // -- Cross-exchange --
    pub binance_coinbase_diff_pct: f64,
    pub spot_perp_diff_pct: f64,
}

/// Polymarket side of the state: YES book + short price history.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolySnapshot {
    #[serde(with = "price_ticks_serde")]
    pub yes_bid: PriceTicks,
    #[serde(with = "price_ticks_serde")]
    pub yes_ask: PriceTicks,
    pub bid_depth: f64,
    pub ask_depth: f64,
    pub spread: f64,
    pub book_imbalance: f64,
    #[serde(with = "price_ticks_serde")]
    pub last_trade_price: PriceTicks,
    #[serde(with = "price_ticks_serde")]
    pub price_1s_ago: PriceTicks,
    #[serde(with = "price_ticks_serde")]
    pub price_5s_ago: PriceTicks,
    #[serde(with = "price_ticks_serde")]
    pub price_30s_ago: PriceTicks,
}

/// Serde adapter for executable prices represented as integer micro-units.
pub mod price_ticks_serde {
    use super::PriceTicks;
    use serde::{Deserialize, Deserializer, Serializer, de};

    pub fn serialize<S>(price: &PriceTicks, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(price.as_micros())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<PriceTicks, D::Error>
    where
        D: Deserializer<'de>,
    {
        let micros = u64::deserialize(deserializer)?;
        if micros > 1_000_000 {
            return Err(de::Error::custom(
                "price micro-units must be within 0..=1000000",
            ));
        }
        Ok(PriceTicks::from_f64(micros as f64 / 1_000_000.0))
    }
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
