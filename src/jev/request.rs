//! Typed System One request contracts for the Jev Lead-Lag V1 strategy.

use crate::strategy::lead_lag::{LeadLagFeatures, PolySnapshot};
use crate::state::quant_features::QuantFeatures;
use serde::Serialize;

/// System One model used by the V1 strategy.
pub const MODEL: &str = "jev-latest";

/// The market context needed to judge the resolution question.
#[derive(Debug, Clone, Serialize)]
pub struct MarketContext {
    pub question: String,
    pub resolution_rules: String,
}

/// The candidate maker order included in the Jev state.
#[derive(Debug, Clone, Serialize)]
pub struct CandidateOrder {
    pub side: OrderSide,
    pub price: f64,
    pub time_in_force: TimeInForce,
}

/// The only order side used by the V1 maker strategy.
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OrderSide {
    BuyYesMaker,
}

/// The candidate order must not cross the book.
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TimeInForce {
    PostOnly,
}

/// Full state object sent as the System One `state` field.
#[derive(Debug, Clone, Serialize)]
pub struct V1State {
    pub market: MarketContext,
    pub underlying: LeadLagFeatures,
    pub polymarket: PolySnapshot,
    /// RustQuant enrichment, or `None` when the quant flag is off. The eight
    /// Jev questions are identical in both cases by design (clean A/B):
    /// `quant: null` vs `quant: {...}` is the only state difference.
    pub quant: Option<QuantFeatures>,
    pub candidate_order: CandidateOrder,
}

impl V1State {
    /// Build the typed state without crossing the JSON serialization boundary.
    pub fn new(
        resolution_question: impl Into<String>,
        resolution_rules: impl Into<String>,
        features: LeadLagFeatures,
        poly: PolySnapshot,
        quant: Option<QuantFeatures>,
        candidate_buy_price: f64,
    ) -> Self {
        Self {
            market: MarketContext {
                question: resolution_question.into(),
                resolution_rules: resolution_rules.into(),
            },
            underlying: features,
            polymarket: poly,
            quant,
            candidate_order: CandidateOrder {
                side: OrderSide::BuyYesMaker,
                price: candidate_buy_price,
                time_in_force: TimeInForce::PostOnly,
            },
        }
    }
}

/// Typed replacement for the former strategy-layer state builder.
pub fn v1_state(
    resolution_question: &str,
    resolution_rules: &str,
    features: &LeadLagFeatures,
    poly: &PolySnapshot,
    quant: Option<QuantFeatures>,
    candidate_buy_price: f64,
) -> V1State {
    V1State::new(
        resolution_question,
        resolution_rules,
        features.clone(),
        poly.clone(),
        quant,
        candidate_buy_price,
    )
}

#[derive(Debug, Clone, Serialize)]
pub struct NoulQuestion {
    #[serde(rename = "type")]
    kind: NoulType,
    pub instructions: String,
}

impl NoulQuestion {
    fn new(instructions: impl Into<String>) -> Self {
        Self {
            kind: NoulType::Noul,
            instructions: instructions.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "lowercase")]
enum NoulType {
    Noul,
}

#[derive(Debug, Clone, Serialize)]
pub struct ChoiceQuestion {
    #[serde(rename = "type")]
    kind: ChoiceType,
    pub instructions: String,
    pub criteria: RepricingCriteria,
}

impl ChoiceQuestion {
    fn new(instructions: impl Into<String>) -> Self {
        Self {
            kind: ChoiceType::Choice,
            instructions: instructions.into(),
            criteria: RepricingCriteria::default(),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "lowercase")]
enum ChoiceType {
    Choice,
}

/// The seven operable price-movement buckets for `repricing_ticks`.
#[derive(Debug, Clone, Serialize)]
pub struct RepricingCriteria {
    #[serde(rename = "UP_3_PLUS_TICKS")]
    pub up_3_plus_ticks: &'static str,
    #[serde(rename = "UP_2_TICKS")]
    pub up_2_ticks: &'static str,
    #[serde(rename = "UP_1_TICK")]
    pub up_1_tick: &'static str,
    #[serde(rename = "FLAT")]
    pub flat: &'static str,
    #[serde(rename = "DOWN_1_TICK")]
    pub down_1_tick: &'static str,
    #[serde(rename = "DOWN_2_TICKS")]
    pub down_2_ticks: &'static str,
    #[serde(rename = "DOWN_3_PLUS_TICKS")]
    pub down_3_plus_ticks: &'static str,
}

impl Default for RepricingCriteria {
    fn default() -> Self {
        Self {
            up_3_plus_ticks: "YES rises by 3 or more minimum price increments",
            up_2_ticks: "YES rises by 2 minimum price increments",
            up_1_tick: "YES rises by 1 minimum price increment",
            flat: "YES stays within the current tick",
            down_1_tick: "YES falls by 1 minimum price increment",
            down_2_ticks: "YES falls by 2 minimum price increments",
            down_3_plus_ticks: "YES falls by 3 or more minimum price increments",
        }
    }
}

/// All eight V1 questions. Field names become the System One question IDs.
#[derive(Debug, Clone, Serialize)]
pub struct V1Questions {
    pub yes_pressure_5s: NoulQuestion,
    pub no_pressure_5s: NoulQuestion,
    pub move_persists: NoulQuestion,
    pub underreact_up: NoulQuestion,
    pub underreact_down: NoulQuestion,
    pub repricing_ticks: ChoiceQuestion,
    pub fill_before_decay: NoulQuestion,
    pub fill_toxic: NoulQuestion,
}

impl V1Questions {
    /// Build the exact V1 wording, including the candidate price in both fill questions.
    pub fn new(candidate_buy_price: f64) -> Self {
        Self {
            yes_pressure_5s: NoulQuestion::new(
                "Does the current external market state in `underlying` imply an increase in the probability of YES over the next 5 seconds?",
            ),
            no_pressure_5s: NoulQuestion::new(
                "Does the current external market state in `underlying` imply a decrease in the probability of YES over the next 5 seconds?",
            ),
            move_persists: NoulQuestion::new(
                "Is the current external move in `underlying` likely to persist over the next seconds rather than immediately mean-revert?",
            ),
            underreact_up: NoulQuestion::new(
                "Given `underlying` and the current `polymarket` state, has Polymarket underreacted to information that should increase P(YES)?",
            ),
            underreact_down: NoulQuestion::new(
                "Given `underlying` and the current `polymarket` state, has Polymarket underreacted to information that should decrease P(YES)?",
            ),
            repricing_ticks: ChoiceQuestion::new(
                "What is the most likely Polymarket YES price movement over the next 5 seconds?",
            ),
            fill_before_decay: NoulQuestion::new(format!(
                "Is the maker order in `candidate_order` (BUY YES at {candidate_buy_price}) likely to be filled before the current informational advantage disappears?"
            )),
            fill_toxic: NoulQuestion::new(format!(
                "If the maker order in `candidate_order` (BUY YES at {candidate_buy_price}) gets filled, is the fill likely to occur because the market is moving adversely against that quote?"
            )),
        }
    }
}

/// Typed replacement for the former strategy-layer question builder.
pub fn v1_questions(candidate_buy_price: f64) -> V1Questions {
    V1Questions::new(candidate_buy_price)
}

/// A generic System One request keeps the state typed until serialization.
#[derive(Debug, Clone, Serialize)]
pub struct SystemOneRequest<S> {
    pub model: &'static str,
    pub state: S,
    pub questions: V1Questions,
}

impl<S> SystemOneRequest<S> {
    pub fn new(state: S, candidate_buy_price: f64) -> Self {
        Self {
            model: MODEL,
            state,
            questions: V1Questions::new(candidate_buy_price),
        }
    }
}

/// The normal typed request used by strategy code.
pub type V1Request = SystemOneRequest<V1State>;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn serializes_the_system_one_wire_shape() {
        let request = SystemOneRequest::new(json!({"market": {"question": "test"}}), 0.44);
        let value = serde_json::to_value(request).expect("request should serialize");
        let object = value.as_object().expect("request should be an object");

        assert_eq!(object.len(), 3);
        assert!(object.contains_key("model"));
        assert!(object.contains_key("state"));
        assert!(object.contains_key("questions"));
        assert_eq!(value["model"], json!("jev-latest"));
        assert!(value["state"].is_object());
        assert_eq!(
            value["questions"].as_object().map(|object| object.len()),
            Some(8)
        );
        assert_eq!(value["questions"]["yes_pressure_5s"]["type"], json!("noul"));
        assert_eq!(
            value["questions"]["repricing_ticks"]["type"],
            json!("choice")
        );
        assert!(value["questions"]["repricing_ticks"]["criteria"].is_object());
    }

    #[test]
    fn quant_block_serializes_null_or_object() {
        use crate::state::quant_features::{QuantParams, build_quant};

        let features = LeadLagFeatures {
            target: 120_000.0,
            time_remaining_secs: 900,
            resolution_source: "Official source".to_owned(),
            spot: 119_000.0,
            distance_to_target_pct: -0.833,
            ret_250ms_pct: 0.0,
            ret_1s_pct: 0.02,
            ret_5s_pct: 0.0,
            ret_30s_pct: 0.0,
            ret_5m_pct: 0.0,
            realized_vol_1m_pct: 0.01,
            realized_vol_5m_pct: 0.005,
            binance_microprice: 119_000.0,
            coinbase_microprice: 119_000.0,
            perp_price: 119_000.0,
            perp_basis_pct: 0.0,
            buy_vol_1s: 0.0,
            sell_vol_1s: 0.0,
            ofi_1s: 0.0,
            ofi_5s: 0.0,
            book_imbalance: 0.0,
            aggressive_buy_ratio: 0.0,
            binance_coinbase_diff_pct: 0.0,
            spot_perp_diff_pct: 0.0,
        };
        let poly = PolySnapshot {
            yes_bid: 0.43,
            yes_ask: 0.45,
            bid_depth: 100.0,
            ask_depth: 100.0,
            spread: 0.02,
            book_imbalance: 0.0,
            last_trade_price: 0.44,
            price_1s_ago: 0.44,
            price_5s_ago: 0.43,
            price_30s_ago: 0.42,
        };
        let off = V1State::new(
            "Will BTC reach the target?",
            "Official rules.",
            features.clone(),
            poly.clone(),
            None,
            0.44,
        );
        let off_value = serde_json::to_value(off).expect("state should serialize");
        assert!(off_value["quant"].is_null());

        let quant = build_quant(&features, &QuantParams::default());
        let on = V1State::new(
            "Will BTC reach the target?",
            "Official rules.",
            features,
            poly,
            Some(quant),
            0.44,
        );
        let on_value = serde_json::to_value(on).expect("state should serialize");
        assert_eq!(on_value["quant"]["baseline_model"], "zero_drift_lognormal");
        assert_eq!(on_value["quant"]["z_vol_source"], "short_1m");
        assert!(on_value["quant"]["quant_baseline_p_yes"].is_number());
        assert_eq!(on_value["underlying"]["spot"], 119_000.0);
    }
}
