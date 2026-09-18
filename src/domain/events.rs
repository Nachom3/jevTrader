use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TradeSide {
    Buy,
    Sell,
}

/// What woke the pipeline up. Persisted as the `trigger` symbol in QuestDB.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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
