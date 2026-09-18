//! Definitive market types for jevTrader.
//!
//! Mirrors AGENTS.md sections 3 (MarketState) and 9 (QuestDB rows).
//! Raw Polymarket/Gamma payloads are converted into [`MarketState`] by the
//! Feature / State Builder; only the builder output ever reaches Jev.
//! Every Jev call is persisted as [`JevSignal`] with its exact state JSON.

use serde::{Deserialize, Serialize};

/// Hot in-RAM state for one market, rebuilt on every relevant WS event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketState {
    // -- Identifiers (Gamma + CLOB) --
    pub gamma_id: String,
    pub slug: String,
    pub condition_id: String,
    pub event_id: String,
    pub yes_token_id: String,
    pub no_token_id: String,
    // -- Question & resolution (rules define how it resolves, not the title) --
    pub question: String,
    pub resolution_source: String,
    pub resolution_rules: String,
    // -- Status: never trade without checking --
    pub active: bool,
    pub closed: bool,
    pub accepting_orders: bool,
    pub enable_order_book: bool,
    pub neg_risk: bool,
    // -- YES microstructure (executable side) --
    pub yes_bid: f64,
    pub yes_ask: f64,
    pub yes_mid: f64,
    pub yes_spread: f64,
    pub book_hash: String,
    pub last_trade_price: f64,
    pub last_trade_side: TradeSide,
    // -- Gamma aggregates --
    pub volume_24h: f64,
    pub liquidity: f64,
    pub one_day_price_change: f64,
    // -- Constraints (realistic execution) --
    pub tick_size: f64,
    pub min_order_size: f64,
    pub fees_enabled: bool,
    // -- Time --
    pub minutes_to_resolution: u64,
    // -- Context --
    pub external_data: ExternalData,
    pub recent_information: Vec<NewsItem>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TradeSide {
    Buy,
    Sell,
}

/// Underlying / external context. `None` distance for non-price markets.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExternalData {
    pub symbol: String,
    pub spot: f64,
    pub ret_5m_pct: f64,
    pub ret_1h_pct: f64,
    pub vol_1h_pct: f64,
    pub distance_to_target_pct: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewsItem {
    pub source: String,
    pub published_minutes_ago: u64,
    /// Always English: translate/summarize in the builder (Jev is English-first).
    pub text_en: String,
    pub dedup_hash: String,
}

/// What woke the pipeline up. Persisted as the `trigger` symbol in QuestDB.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Trigger {
    PriceMove,
    SpreadChange,
    SpotMove,
    TimeStop,
    AbnormalVolume,
    RelevantNews,
}

impl Trigger {
    pub fn as_str(self) -> &'static str {
        match self {
            Trigger::PriceMove => "price_move",
            Trigger::SpreadChange => "spread_change",
            Trigger::SpotMove => "spot_move",
            Trigger::TimeStop => "time_stop",
            Trigger::AbnormalVolume => "abnormal_volume",
            Trigger::RelevantNews => "relevant_news",
        }
    }
}

/// One persisted Jev evaluation: answers + the exact state that produced them.
/// Noul answers carry no confidence; only the Score (`resolution_risk`) does.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JevSignal {
    pub condition_id: String,
    pub state_hash: String,
    pub state_json: String,
    pub questions_json: String,
    pub likely_yes: f64,
    pub underpriced: f64,
    pub resolution_risk: f64,
    pub resolution_risk_conf: f64,
    pub latency_ms: u64,
    pub tokens_in: u64,
    pub tokens_out: u64,
    pub trigger: Trigger,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Decision {
    Skip,
    Trade,
}

/// Strategy output, persisted for paper-trading audit.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaperDecision {
    pub condition_id: String,
    /// Links back to the originating `JevSignal` timestamp.
    pub jev_ts_chrono: chrono::DateTime<chrono::Utc>,
    pub edge: f64,
    pub threshold: f64,
    pub decision: Decision,
    pub paper_price: f64,
    pub size: f64,
    pub fair_value: f64,
}
