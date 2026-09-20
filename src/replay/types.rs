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
        /// Trade quantity for OFI/volume flow. None for quotes/legacy rows.
        qty: Option<f64>,
        /// Aggressor side of the trade (BUY = buyer-initiated). Drives
        /// rolling OFI in V2; None for quotes and legacy rows.
        aggressor: Option<String>,
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
    Empirical,
    Slow,
}

impl LatencyProfile {
    /// Returns the fixed Jev latency for a synthetic sensitivity profile.
    ///
    /// [`Self::Empirical`] is resolved by [`LatencyDistribution`] and therefore
    /// has no fixed value; callers using that profile must not use this method
    /// for replay timing.
    #[must_use]
    pub const fn jev_latency_ms(self) -> u64 {
        match self {
            Self::Fast => 100,
            Self::Base => 320,
            Self::Empirical => 0,
            Self::Slow => 800,
        }
    }

    #[must_use]
    pub const fn submit_latency_ms(self) -> u64 {
        match self {
            Self::Fast => 10,
            Self::Base | Self::Empirical => 50,
            Self::Slow => 150,
        }
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Fast => "FAST",
            Self::Base => "BASE",
            Self::Empirical => "EMPIRICAL",
            Self::Slow => "SLOW",
        }
    }
}

/// Versioned, deterministic Jev latency samples for replay.
///
/// The JSON representation is intentionally small so a corpus run can pin the
/// exact sample set and seed used as evidence. `seed + sequence` selects one
/// sample without depending on process-global RNG state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LatencyDistribution {
    pub version: u32,
    #[serde(alias = "samples")]
    pub samples_ms: Vec<u64>,
    pub seed: u64,
}

impl LatencyDistribution {
    pub const CURRENT_VERSION: u32 = 1;

    /// Creates a validated distribution from observed millisecond samples.
    pub fn from_samples(samples_ms: Vec<u64>, seed: u64) -> Result<Self, String> {
        let distribution = Self {
            version: Self::CURRENT_VERSION,
            samples_ms,
            seed,
        };
        distribution.validate()?;
        Ok(distribution)
    }

    /// Loads a versioned distribution from JSON.
    pub fn from_json(json: &str) -> Result<Self, String> {
        let distribution: Self = serde_json::from_str(json)
            .map_err(|error| format!("invalid Jev latency distribution JSON: {error}"))?;
        distribution.validate()?;
        Ok(distribution)
    }

    /// Returns the deterministic sample selected for an evaluation sequence.
    #[must_use]
    pub fn sample_ms(&self, sequence: u64) -> u64 {
        debug_assert!(!self.samples_ms.is_empty());
        let mixed = splitmix64(self.seed.wrapping_add(sequence));
        self.samples_ms[(mixed as usize) % self.samples_ms.len()]
    }

    fn validate(&self) -> Result<(), String> {
        if self.version != Self::CURRENT_VERSION {
            return Err(format!(
                "unsupported Jev latency distribution version {} (expected {})",
                self.version,
                Self::CURRENT_VERSION
            ));
        }
        if self.samples_ms.is_empty() {
            return Err("Jev latency distribution must contain at least one sample".to_owned());
        }
        if self.samples_ms.contains(&0) {
            return Err("Jev latency samples must be greater than zero".to_owned());
        }
        Ok(())
    }
}

/// Pilot-range placeholder until the large run writes the complete observed
/// sample file. These are the reported bounds, not a fabricated distribution.
pub const PILOT_LATENCY_SAMPLES_JSON: &str = r#"{"version":1,"samples_ms":[307,1019],"seed":42}"#;

impl Default for LatencyDistribution {
    fn default() -> Self {
        Self::from_json(PILOT_LATENCY_SAMPLES_JSON)
            .expect("pilot latency placeholder is a valid distribution")
    }
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9E37_79B9_7F4A_7C15);
    value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    value ^ (value >> 31)
}

#[cfg(test)]
mod latency_distribution_tests {
    use super::*;

    #[test]
    fn sampling_is_deterministic_and_covering() {
        let dist =
            LatencyDistribution::from_samples(vec![100, 200, 300], 7).expect("valid samples");
        let first: Vec<u64> = (0..30).map(|seq| dist.sample_ms(seq)).collect();
        let second: Vec<u64> = (0..30).map(|seq| dist.sample_ms(seq)).collect();
        assert_eq!(first, second);
        for sample in [100, 200, 300] {
            assert!(first.contains(&sample), "sample {sample} never selected");
        }
    }

    #[test]
    fn validation_rejects_bad_distributions() {
        assert!(LatencyDistribution::from_samples(vec![], 1).is_err());
        assert!(LatencyDistribution::from_samples(vec![0, 100], 1).is_err());
        assert!(
            LatencyDistribution::from_json(r#"{"version":999,"samples_ms":[100],"seed":1}"#)
                .is_err()
        );
        assert!(LatencyDistribution::from_json("not json").is_err());
    }

    #[test]
    fn json_roundtrip_preserves_samples_and_seed() {
        let dist =
            LatencyDistribution::from_samples(vec![307, 500, 1019], 42).expect("valid samples");
        let json = serde_json::to_string(&dist).expect("serializes");
        let back = LatencyDistribution::from_json(&json).expect("deserializes");
        assert_eq!(dist, back);
    }

    #[test]
    fn pilot_placeholder_is_two_reported_bounds() {
        let dist = LatencyDistribution::default();
        assert_eq!(dist.samples_ms, vec![307, 1019]);
        assert_eq!(dist.seed, 42);
    }
}
