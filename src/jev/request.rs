//! Typed System One request contracts for the Jev Lead-Lag V1 strategy.

use crate::domain::PriceTicks;
use crate::state::quant_features::QuantFeatures;
use crate::strategy::lead_lag::{LeadLagFeatures, PolySnapshot, price_ticks_serde};
use serde::{Serialize, Serializer, ser::SerializeStruct};

#[path = "questions_md.rs"]
pub mod questions_md;

/// System One model used by the V1 strategy.
pub const MODEL: &str = "jev-latest";

/// The market context needed to judge the resolution question.
#[derive(Debug, Clone, Serialize)]
pub struct MarketContext {
    pub question: String,
    /// Empty only when the upstream market metadata did not provide a source;
    /// the request layer never invents one.
    pub resolution_source: String,
    /// Full resolution criteria/rules supplied by market metadata.
    pub resolution_rules: String,
}

/// The candidate maker order included in the Jev state.
#[derive(Debug, Clone, Serialize)]
pub struct CandidateOrder {
    pub side: OrderSide,
    #[serde(with = "price_ticks_serde")]
    pub price: PriceTicks,
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
    /// Optional quant enrichment; the eight Jev questions are identical on and
    /// off by design, and the disabled path serializes as `quant: null`.
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
        candidate_buy_price: PriceTicks,
    ) -> Self {
        let resolution_source = features.resolution_source.clone();
        Self {
            market: MarketContext {
                question: resolution_question.into(),
                resolution_source,
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
    candidate_buy_price: PriceTicks,
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
        Self::with_criteria(instructions, RepricingCriteria::default())
    }

    fn with_criteria(instructions: impl Into<String>, criteria: RepricingCriteria) -> Self {
        Self {
            kind: ChoiceType::Choice,
            instructions: instructions.into(),
            criteria,
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
    pub up_3_plus_ticks: String,
    #[serde(rename = "UP_2_TICKS")]
    pub up_2_ticks: String,
    #[serde(rename = "UP_1_TICK")]
    pub up_1_tick: String,
    #[serde(rename = "FLAT")]
    pub flat: String,
    #[serde(rename = "DOWN_1_TICK")]
    pub down_1_tick: String,
    #[serde(rename = "DOWN_2_TICKS")]
    pub down_2_ticks: String,
    #[serde(rename = "DOWN_3_PLUS_TICKS")]
    pub down_3_plus_ticks: String,
}

impl Default for RepricingCriteria {
    fn default() -> Self {
        Self {
            up_3_plus_ticks: "YES rises by 3 or more minimum price increments".to_owned(),
            up_2_ticks: "YES rises by 2 minimum price increments".to_owned(),
            up_1_tick: "YES rises by 1 minimum price increment".to_owned(),
            flat: "YES stays within the current tick".to_owned(),
            down_1_tick: "YES falls by 1 minimum price increment".to_owned(),
            down_2_ticks: "YES falls by 2 minimum price increments".to_owned(),
            down_3_plus_ticks: "YES falls by 3 or more minimum price increments".to_owned(),
        }
    }
}

/// All eight V1 questions. Field names become the System One question IDs.
#[derive(Debug, Clone)]
pub struct V1Questions {
    pub yes_pressure_5s: NoulQuestion,
    pub no_pressure_5s: NoulQuestion,
    pub move_persists: NoulQuestion,
    pub underreact_up: NoulQuestion,
    pub underreact_down: NoulQuestion,
    pub repricing_ticks: ChoiceQuestion,
    pub fill_before_decay: NoulQuestion,
    pub fill_toxic: NoulQuestion,
    load_error: Option<String>,
}

impl Serialize for V1Questions {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if let Some(error) = &self.load_error {
            return Err(serde::ser::Error::custom(error));
        }
        let mut state = serializer.serialize_struct("V1Questions", 8)?;
        state.serialize_field("yes_pressure_5s", &self.yes_pressure_5s)?;
        state.serialize_field("no_pressure_5s", &self.no_pressure_5s)?;
        state.serialize_field("move_persists", &self.move_persists)?;
        state.serialize_field("underreact_up", &self.underreact_up)?;
        state.serialize_field("underreact_down", &self.underreact_down)?;
        state.serialize_field("repricing_ticks", &self.repricing_ticks)?;
        state.serialize_field("fill_before_decay", &self.fill_before_decay)?;
        state.serialize_field("fill_toxic", &self.fill_toxic)?;
        state.end()
    }
}

impl V1Questions {
    /// Load the versioned document and substitute the candidate price.
    pub fn new(candidate_buy_price: PriceTicks) -> Self {
        match Self::try_new(candidate_buy_price) {
            Ok(questions) => questions,
            Err(error) => Self::load_failure(error),
        }
    }

    /// Fallible form used by callers that need the typed document error.
    pub fn try_new(
        candidate_buy_price: PriceTicks,
    ) -> Result<Self, questions_md::QuestionsMdError> {
        questions_md::load(candidate_buy_price)
    }

    // Eight fixed V1 question IDs; keep the constructor arity explicit.
    #[allow(clippy::too_many_arguments)]
    fn from_parts(
        yes_pressure_5s: NoulQuestion,
        no_pressure_5s: NoulQuestion,
        move_persists: NoulQuestion,
        underreact_up: NoulQuestion,
        underreact_down: NoulQuestion,
        repricing_ticks: ChoiceQuestion,
        fill_before_decay: NoulQuestion,
        fill_toxic: NoulQuestion,
    ) -> Self {
        Self {
            yes_pressure_5s,
            no_pressure_5s,
            move_persists,
            underreact_up,
            underreact_down,
            repricing_ticks,
            fill_before_decay,
            fill_toxic,
            load_error: None,
        }
    }

    fn load_failure(error: questions_md::QuestionsMdError) -> Self {
        Self {
            yes_pressure_5s: NoulQuestion::new(String::new()),
            no_pressure_5s: NoulQuestion::new(String::new()),
            move_persists: NoulQuestion::new(String::new()),
            underreact_up: NoulQuestion::new(String::new()),
            underreact_down: NoulQuestion::new(String::new()),
            repricing_ticks: ChoiceQuestion::new(String::new()),
            fill_before_decay: NoulQuestion::new(String::new()),
            fill_toxic: NoulQuestion::new(String::new()),
            load_error: Some(error.to_string()),
        }
    }
}

/// Typed replacement for the former strategy-layer question builder.
pub fn v1_questions(candidate_buy_price: PriceTicks) -> V1Questions {
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
    pub fn new(state: S, candidate_buy_price: PriceTicks) -> Self {
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
        let request = SystemOneRequest::new(
            json!({"market": {"question": "test"}}),
            PriceTicks::from_f64(0.44),
        );
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
    fn serializes_candidate_price_as_micro_units_and_resolution_metadata() {
        let state = V1State::new(
            "Will BTC reach the target?",
            "The market resolves according to the official source.",
            LeadLagFeatures {
                target: 120_000.0,
                time_remaining_secs: 900,
                resolution_source: "Official source".to_owned(),
                asset_symbol: "BTC".to_owned(),
                horizon_label: "15m".to_owned(),
                horizon_secs: 900,
                spot: 119_000.0,
                distance_to_target_pct: 0.0 - 0.833,
                ret_250ms_pct: 0.0,
                ret_1s_pct: 0.0,
                ret_5s_pct: 0.0,
                ret_30s_pct: 0.0,
                ret_1m_pct: 0.0,
                ret_5m_pct: 0.0,
                ret_15m_pct: 0.0,
                ret_30m_pct: 0.0,
                ret_1h_pct: 0.0,
                realized_vol_1m_pct: 0.0,
                realized_vol_5m_pct: 0.0,
                realized_vol_1h_pct: 0.0,
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
            },
            PolySnapshot {
                yes_bid: PriceTicks::from_f64(0.43),
                yes_ask: PriceTicks::from_f64(0.45),
                bid_depth: 100.0,
                ask_depth: 100.0,
                spread: 0.02,
                book_imbalance: 0.0,
                last_trade_price: PriceTicks::from_f64(0.44),
                price_1s_ago: PriceTicks::from_f64(0.44),
                price_5s_ago: PriceTicks::from_f64(0.43),
                price_30s_ago: PriceTicks::from_f64(0.42),
            },
            None,
            PriceTicks::from_f64(0.44),
        );
        let value = serde_json::to_value(state).expect("state should serialize");

        assert_eq!(value["market"]["resolution_source"], "Official source");
        assert_eq!(
            value["market"]["resolution_rules"],
            "The market resolves according to the official source."
        );
        assert_eq!(value["underlying"]["time_remaining_secs"], 900);
        assert!(value["quant"].is_null());
        assert_eq!(value["polymarket"]["price_1s_ago"], 440_000);
        assert_eq!(value["polymarket"]["price_5s_ago"], 430_000);
        assert_eq!(value["polymarket"]["price_30s_ago"], 420_000);
        assert_eq!(value["candidate_order"]["price"], 440_000);
    }

    #[test]
    fn serializes_the_quant_block_next_to_raw_features() {
        use crate::state::quant_features::{QuantParams, build_quant};

        let features = LeadLagFeatures {
            target: 120_000.0,
            time_remaining_secs: 900,
            resolution_source: "Official source".to_owned(),
            asset_symbol: "BTC".to_owned(),
            horizon_label: "15m".to_owned(),
            horizon_secs: 900,
            spot: 119_000.0,
            distance_to_target_pct: -0.833,
            ret_250ms_pct: 0.0,
            ret_1s_pct: 0.02,
            ret_5s_pct: 0.0,
            ret_30s_pct: 0.0,
            ret_1m_pct: 0.0,
            ret_5m_pct: 0.0,
            ret_15m_pct: 0.0,
            ret_30m_pct: 0.0,
            ret_1h_pct: 0.0,
            realized_vol_1m_pct: 0.01,
            realized_vol_5m_pct: 0.005,
            realized_vol_1h_pct: 0.0,
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
        let state = V1State::new(
            "Will BTC reach the target?",
            "The market resolves according to the official source.",
            features,
            PolySnapshot {
                yes_bid: PriceTicks::from_f64(0.43),
                yes_ask: PriceTicks::from_f64(0.45),
                bid_depth: 100.0,
                ask_depth: 100.0,
                spread: 0.02,
                book_imbalance: 0.0,
                last_trade_price: PriceTicks::from_f64(0.44),
                price_1s_ago: PriceTicks::from_f64(0.44),
                price_5s_ago: PriceTicks::from_f64(0.43),
                price_30s_ago: PriceTicks::from_f64(0.42),
            },
            None,
            PriceTicks::from_f64(0.44),
        );
        let quant = build_quant(&state.underlying, &QuantParams::default());
        let state = V1State::new(
            "Will BTC reach the target?",
            "The market resolves according to the official source.",
            state.underlying,
            state.polymarket,
            Some(quant),
            PriceTicks::from_f64(0.44),
        );
        let value = serde_json::to_value(state).expect("state should serialize");

        assert_eq!(value["quant"]["baseline_model"], "zero_drift_lognormal");
        assert_eq!(value["quant"]["z_vol_source"], "short_1m");
        assert!(value["quant"]["quant_baseline_p_yes"].is_number());
        // Raw features stay untouched next to the enrichment.
        assert_eq!(value["underlying"]["spot"], 119_000.0);
    }
}
