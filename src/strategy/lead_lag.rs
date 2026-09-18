//! V1 Lead-Lag Maker: features, questions, signal, quote rule.
//!
//! Spec: `docs/strategy-lead-lag-v1.md`. One Jev request carries all 8 V1
//! outputs (they evaluate in parallel over the same state). Rust owns the
//! features and the final quote decision; Jev only judges.

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

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

/// Full V1 state object sent as `state` in the Jev request.
pub fn v1_state(
    resolution_question: &str,
    resolution_rules: &str,
    features: &LeadLagFeatures,
    poly: &PolySnapshot,
    candidate_buy_price: f64,
) -> Value {
    json!({
        "market": {
            "question": resolution_question,
            "resolution_rules": resolution_rules,
        },
        "underlying": features,
        "polymarket": poly,
        "candidate_order": {
            "side": "BUY_YES_MAKER",
            "price": candidate_buy_price,
            "time_in_force": "POST_ONLY",
        },
    })
}

/// The 8 V1 questions. Keys are for code only; meaning lives in instructions.
pub fn v1_questions(candidate_buy_price: f64) -> Value {
    json!({
        "yes_pressure_5s": {
            "type": "noul",
            "instructions": "Does the current external market state in `underlying` imply an increase in the probability of YES over the next 5 seconds?",
        },
        "no_pressure_5s": {
            "type": "noul",
            "instructions": "Does the current external market state in `underlying` imply a decrease in the probability of YES over the next 5 seconds?",
        },
        "move_persists": {
            "type": "noul",
            "instructions": "Is the current external move in `underlying` likely to persist over the next seconds rather than immediately mean-revert?",
        },
        "underreact_up": {
            "type": "noul",
            "instructions": "Given `underlying` and the current `polymarket` state, has Polymarket underreacted to information that should increase P(YES)?",
        },
        "underreact_down": {
            "type": "noul",
            "instructions": "Given `underlying` and the current `polymarket` state, has Polymarket underreacted to information that should decrease P(YES)?",
        },
        "repricing_ticks": {
            "type": "choice",
            "instructions": "What is the most likely Polymarket YES price movement over the next 5 seconds?",
            "criteria": {
                "UP_3_PLUS_TICKS": "YES rises by 3 or more minimum price increments",
                "UP_2_TICKS": "YES rises by 2 minimum price increments",
                "UP_1_TICK": "YES rises by 1 minimum price increment",
                "FLAT": "YES stays within the current tick",
                "DOWN_1_TICK": "YES falls by 1 minimum price increment",
                "DOWN_2_TICKS": "YES falls by 2 minimum price increments",
                "DOWN_3_PLUS_TICKS": "YES falls by 3 or more minimum price increments",
            },
        },
        "fill_before_decay": {
            "type": "noul",
            "instructions": format!("Is the maker order in `candidate_order` (BUY YES at {candidate_buy_price}) likely to be filled before the current informational advantage disappears?"),
        },
        "fill_toxic": {
            "type": "noul",
            "instructions": format!("If the maker order in `candidate_order` (BUY YES at {candidate_buy_price}) gets filled, is the fill likely to occur because the market is moving adversely against that quote?"),
        },
    })
}

/// Probability distribution over repricing buckets (Choice answer).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TickDistribution {
    pub up_3_plus: f64,
    pub up_2: f64,
    pub up_1: f64,
    pub flat: f64,
    pub down_1: f64,
    pub down_2: f64,
    pub down_3_plus: f64,
}

/// Parsed V1 answer set. Noul fields are 0-1 probabilities (no confidence);
/// only the Choice-derived distribution carries confidence separately.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct V1Signal {
    pub yes_pressure_5s: f64,
    pub no_pressure_5s: f64,
    pub move_persists: f64,
    pub underreact_up: f64,
    pub underreact_down: f64,
    pub repricing: TickDistribution,
    pub repricing_confidence: f64,
    pub fill_before_decay: f64,
    pub fill_toxic: f64,
}

impl V1Signal {
    /// P(YES rises by >= 1 tick in 5s), straight from the distribution.
    pub fn p_up_ge_1_tick(&self) -> f64 {
        self.repricing.up_1 + self.repricing.up_2 + self.repricing.up_3_plus
    }
}

// Starting thresholds for `should_quote`. Calibrate with backtest markouts;
// change coefficients here, never the question wording, to shift behavior.
pub const UNDER_MIN: f64 = 0.75;
pub const NEXT_UP_MIN: f64 = 0.65;
pub const PERSIST_MIN: f64 = 0.60;
pub const FILL_MIN: f64 = 0.60;
pub const TOXIC_MAX: f64 = 0.30;
pub const CONFLICT_MAX: f64 = 0.30;

/// Pure quote rule: post-only BUY one tick over the bid, or nothing.
pub fn should_quote(s: &V1Signal) -> bool {
    s.underreact_up > UNDER_MIN
        && s.p_up_ge_1_tick() > NEXT_UP_MIN
        && s.move_persists > PERSIST_MIN
        && s.fill_before_decay > FILL_MIN
        && s.fill_toxic < TOXIC_MAX
        && s.underreact_down < CONFLICT_MAX
        && s.no_pressure_5s < CONFLICT_MAX
}
