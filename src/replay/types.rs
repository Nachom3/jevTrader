//! Shared replay vocabulary: events, specs, regimes, splits, profiles.

use serde::{Deserialize, Serialize};

/// One normalized historical observation, always event-timed.
#[derive(Debug, Clone, PartialEq)]
pub enum HistoricalEvent {
    PolyTrade {
        ts_ms: i64,
        condition_id: String,
        price: f64,
        size: f64,
        aggressor: Option<String>,
        direction_quality: String,
        source: String,
    },
    UnderlyingTick {
        ts_ms: i64,
        asset: String,
        venue: String,
        price: f64,
        bid: Option<f64>,
        ask: Option<f64>,
        source: String,
    },
    PolyTop {
        ts_ms: i64,
        condition_id: String,
        best_bid: f64,
        best_ask: f64,
        source: String,
    },
}

impl HistoricalEvent {
    #[must_use]
    pub const fn ts_ms(&self) -> i64 {
        match self {
            Self::PolyTrade { ts_ms, .. }
            | Self::UnderlyingTick { ts_ms, .. }
            | Self::PolyTop { ts_ms, .. } => *ts_ms,
        }
    }
}

/// Per-market resolution fidelity. Primary results filter to EXACT.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Fidelity {
    Exact,
    Proxy,
    Unknown,
}

impl Fidelity {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Exact => "EXACT",
            Self::Proxy => "PROXY",
            Self::Unknown => "UNKNOWN",
        }
    }
}

/// Per-market resolution contract (never a global asset mapping).
/// Primary evidence must filter this contract to `Fidelity::Exact`.
#[derive(Debug, Clone, PartialEq)]
pub struct ResolutionSpec {
    pub condition_id: String,
    pub market_id: String,
    pub asset: String,
    pub horizon: String,
    pub resolution_source: String,
    pub resolution_rule_excerpt: String,
    pub fidelity: Fidelity,
    pub resolution_at_ms: i64,
    pub reference: Option<String>,
    pub strike: Option<f64>,
    pub start_at: Option<String>,
    pub end_at: Option<String>,
}

/// Volatility x trend regime assigned from lightweight 1m data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Regime {
    pub vol: String,
    pub trend: String,
}

impl Regime {
    #[must_use]
    pub fn name(&self) -> String {
        format!("{}-{}", self.vol, self.trend)
    }
}

/// Temporal split. OOS is never used for threshold tuning.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Split {
    Exploration,
    Validation,
    OutOfSample,
}

impl Split {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Exploration => "EXPLORATION",
            Self::Validation => "VALIDATION",
            Self::OutOfSample => "OUT_OF_SAMPLE",
        }
    }
}

/// Which external feeds could be reconstructed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Coverage {
    BinanceOnly,
    BinanceCoinbaseDeribit,
    Custom(String),
}

impl Coverage {
    #[must_use]
    pub fn as_str(&self) -> String {
        match self {
            Self::BinanceOnly => "BINANCE_ONLY".to_owned(),
            Self::BinanceCoinbaseDeribit => "BINANCE_COINBASE_DERIBIT".to_owned(),
            Self::Custom(s) => s.clone(),
        }
    }
}

/// Maker fill assumption. Every fill records its model.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FillProfile {
    Optimistic,
    Base,
    Conservative,
}

impl FillProfile {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Optimistic => "OPTIMISTIC",
            Self::Base => "BASE",
            Self::Conservative => "CONSERVATIVE",
        }
    }
}

/// Latency sensitivity profile (Jev + submit + cancel).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LatencyProfile {
    Fast,
    Base,
    Slow,
}

impl LatencyProfile {
    #[must_use]
    pub const fn jev_latency_ms(self) -> u64 {
        match self {
            Self::Fast => 100,
            Self::Base => 320,
            Self::Slow => 800,
        }
    }

    #[must_use]
    pub const fn submit_latency_ms(self) -> u64 {
        match self {
            Self::Fast => 10,
            Self::Base => 50,
            Self::Slow => 150,
        }
    }
}
