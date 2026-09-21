//! Point-in-time regime classification for replay.
//!
//! A quantile calculated once over an entire dataset is look-ahead: its cutoffs
//! include observations that were not available at the event being classified.
//! This module instead builds quantiles from the rolling observations supplied
//! through [`RollingRegimeClassifier::observe`]. Callers must feed observations
//! in event-time order and observe only values whose timestamp is at or before
//! the snapshot being classified. The classifier never performs I/O or learns
//! thresholds from future data.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Fixed prior for trend classification, not calibrated on out-of-sample data.
pub const TREND_THRESHOLD: f64 = 0.1;
/// Fixed prior for order-flow classification, not calibrated on out-of-sample data.
pub const OFI_THRESHOLD: f64 = 0.3;
/// Fixed prior for basis classification, not calibrated on out-of-sample data.
pub const BASIS_THRESHOLD: f64 = 0.0005;
/// A snapshot is far from resolution strictly above this many minutes.
pub const FAR_MINUTES: u64 = 240;
/// The lower bound of the mid-resolution bucket.
pub const MID_MINUTES: u64 = 60;
/// The lower bound of the near-resolution bucket.
pub const NEAR_MINUTES: u64 = 10;
/// Fixed sigma cutoffs, used as structural priors rather than OOS calibration.
pub const SIGMA_NEG_2: f64 = -2.0;
pub const SIGMA_NEG_1: f64 = -1.0;
pub const SIGMA_ZERO: f64 = 0.0;
pub const SIGMA_POS_1: f64 = 1.0;
pub const SIGMA_POS_2: f64 = 2.0;

/// Directional trend bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Trend {
    Bull,
    Bear,
    Sideways,
}

/// Rolling volatility bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Volatility {
    Low,
    Normal,
    High,
    Extreme,
}

/// Distance from resolution bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeBucket {
    Far,
    Mid,
    Near,
    VeryNear,
}

/// Standard-deviation distance bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SigmaBucket {
    NegBeyond2,
    Neg2To1,
    Neg1To0,
    Pos0To1,
    Pos1To2,
    PosBeyond2,
}

/// Order-flow imbalance bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FlowBucket {
    StrongSell,
    Neutral,
    StrongBuy,
}

/// Perpetual-versus-spot basis bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BasisBucket {
    Negative,
    Neutral,
    Positive,
}

/// Numeric snapshot persisted beside a regime label.
///
/// Keeping the original values makes it possible to re-bucket historical
/// labels offline without asking Jev to reproduce the classification.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RegimeFeatures {
    pub trend_score: f64,
    pub volatility: f64,
    pub minutes_to_resolution: u64,
    pub distance_sigma: f64,
    pub ofi: f64,
    pub basis: f64,
    pub asset: String,
    pub horizon: String,
}

/// Full point-in-time regime label and the numeric snapshot that produced it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RegimeLabel {
    pub trend: Trend,
    pub volatility: Volatility,
    pub time: TimeBucket,
    pub sigma: SigmaBucket,
    pub flow: FlowBucket,
    pub basis: BasisBucket,
    pub asset: String,
    pub horizon: String,
    pub features: RegimeFeatures,
}

/// Classifies snapshots from observations available in event-time order.
///
/// `warmup` is the minimum number of finite volatility observations required
/// before rolling volatility quantiles are used. Until then, volatility is
/// [`Volatility::Normal`]. Trend, time, sigma, flow, and basis use fixed
/// structural priors and do not depend on the warmup history.
#[derive(Debug)]
pub struct RollingRegimeClassifier {
    warmup: usize,
    histories: HashMap<(String, String), RegimeHistory>,
    classified: HashMap<FeatureKey, RegimeLabel>,
}

#[derive(Debug, Default)]
struct RegimeHistory {
    volatilities: Vec<f64>,
    trend_scores: Vec<f64>,
    observed_snapshots: Vec<ObservedSnapshot>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ObservedSnapshot {
    volatility: u64,
    trend_score: u64,
}

/// A bitwise key lets us retain an already-classified point-in-time snapshot
/// without pretending that `f64` itself has a total `Eq`/`Hash` implementation.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct FeatureKey {
    trend_score: u64,
    volatility: u64,
    minutes_to_resolution: u64,
    distance_sigma: u64,
    ofi: u64,
    basis: u64,
    asset: String,
    horizon: String,
}

impl From<&RegimeFeatures> for FeatureKey {
    fn from(features: &RegimeFeatures) -> Self {
        Self {
            trend_score: features.trend_score.to_bits(),
            volatility: features.volatility.to_bits(),
            minutes_to_resolution: features.minutes_to_resolution,
            distance_sigma: features.distance_sigma.to_bits(),
            ofi: features.ofi.to_bits(),
            basis: features.basis.to_bits(),
            asset: features.asset.clone(),
            horizon: features.horizon.clone(),
        }
    }
}

impl RollingRegimeClassifier {
    /// Creates an empty classifier with the requested rolling warmup size.
    #[must_use]
    pub fn new(warmup: usize) -> Self {
        Self {
            warmup,
            histories: HashMap::new(),
            classified: HashMap::new(),
        }
    }

    /// Records one observation for an asset and horizon.
    ///
    /// This method has no timestamp parameter by design: replay callers must
    /// invoke it in event-time order and must not push a value from after the
    /// snapshot currently being classified. Non-finite values are retained in
    /// the raw history for lineage but are excluded from rolling quantiles.
    pub fn observe(
        &mut self,
        asset: impl Into<String>,
        horizon: impl Into<String>,
        volatility: f64,
        trend_score: f64,
    ) {
        let key = (asset.into(), horizon.into());
        let history = self.histories.entry(key).or_default();
        history.volatilities.push(volatility);
        history.trend_scores.push(trend_score);
        if volatility.is_finite() && trend_score.is_finite() {
            history.observed_snapshots.push(ObservedSnapshot {
                volatility: volatility.to_bits(),
                trend_score: trend_score.to_bits(),
            });
        }
    }

    /// Classifies a snapshot using only the history available at this call.
    ///
    /// Labels for snapshots that were observed before classification are
    /// retained. That makes re-reading a past snapshot after later observations
    /// stable: a future observation cannot rewrite a previously emitted label.
    /// A snapshot not yet observed is classified against the current history and
    /// becomes cacheable once its own observation is supplied.
    #[must_use]
    pub fn classify(&mut self, features: &RegimeFeatures) -> RegimeLabel {
        let key = FeatureKey::from(features);
        let observed = self.snapshot_was_observed(features);
        if observed && let Some(label) = self.classified.get(&key) {
            return label.clone();
        }

        let label = RegimeLabel {
            trend: classify_trend(features.trend_score),
            volatility: self.classify_volatility(features),
            time: classify_time(features.minutes_to_resolution),
            sigma: classify_sigma(features.distance_sigma),
            flow: classify_flow(features.ofi),
            basis: classify_basis(features.basis),
            asset: features.asset.clone(),
            horizon: features.horizon.clone(),
            features: features.clone(),
        };

        if observed {
            self.classified.insert(key, label.clone());
        }
        label
    }

    fn snapshot_was_observed(&self, features: &RegimeFeatures) -> bool {
        let Some(history) = self
            .histories
            .get(&(features.asset.clone(), features.horizon.clone()))
        else {
            return false;
        };
        let snapshot = ObservedSnapshot {
            volatility: features.volatility.to_bits(),
            trend_score: features.trend_score.to_bits(),
        };
        history.observed_snapshots.contains(&snapshot)
    }

    fn classify_volatility(&self, features: &RegimeFeatures) -> Volatility {
        if !features.volatility.is_finite() {
            return Volatility::Normal;
        }

        let Some(history) = self
            .histories
            .get(&(features.asset.clone(), features.horizon.clone()))
        else {
            return Volatility::Normal;
        };

        let mut samples: Vec<f64> = history
            .volatilities
            .iter()
            .copied()
            .filter(|value| value.is_finite())
            .collect();
        if samples.len() < self.warmup || samples.is_empty() {
            return Volatility::Normal;
        }

        samples.sort_by(f64::total_cmp);
        let p25 = percentile(&samples, 0.25);
        let p50 = percentile(&samples, 0.50);
        let p75 = percentile(&samples, 0.75);
        if features.volatility <= p25 {
            Volatility::Low
        } else if features.volatility <= p50 {
            Volatility::Normal
        } else if features.volatility <= p75 {
            Volatility::High
        } else {
            Volatility::Extreme
        }
    }
}

fn percentile(sorted: &[f64], quantile: f64) -> f64 {
    debug_assert!(!sorted.is_empty());
    let position = quantile * (sorted.len() - 1) as f64;
    let lower = position.floor() as usize;
    let upper = position.ceil() as usize;
    if lower == upper {
        sorted[lower]
    } else {
        let weight = position - lower as f64;
        sorted[lower] + (sorted[upper] - sorted[lower]) * weight
    }
}

fn classify_trend(score: f64) -> Trend {
    if !score.is_finite() {
        Trend::Sideways
    } else if score > TREND_THRESHOLD {
        Trend::Bull
    } else if score < -TREND_THRESHOLD {
        Trend::Bear
    } else {
        Trend::Sideways
    }
}

fn classify_time(minutes: u64) -> TimeBucket {
    if minutes > FAR_MINUTES {
        TimeBucket::Far
    } else if minutes >= MID_MINUTES {
        TimeBucket::Mid
    } else if minutes >= NEAR_MINUTES {
        TimeBucket::Near
    } else {
        TimeBucket::VeryNear
    }
}

fn classify_sigma(distance: f64) -> SigmaBucket {
    if !distance.is_finite() {
        return SigmaBucket::Pos0To1;
    }
    if distance < SIGMA_NEG_2 {
        SigmaBucket::NegBeyond2
    } else if distance < SIGMA_NEG_1 {
        SigmaBucket::Neg2To1
    } else if distance < SIGMA_ZERO {
        SigmaBucket::Neg1To0
    } else if distance < SIGMA_POS_1 {
        SigmaBucket::Pos0To1
    } else if distance <= SIGMA_POS_2 {
        SigmaBucket::Pos1To2
    } else {
        SigmaBucket::PosBeyond2
    }
}

fn classify_flow(ofi: f64) -> FlowBucket {
    if !ofi.is_finite() {
        FlowBucket::Neutral
    } else if ofi > OFI_THRESHOLD {
        FlowBucket::StrongBuy
    } else if ofi < -OFI_THRESHOLD {
        FlowBucket::StrongSell
    } else {
        FlowBucket::Neutral
    }
}

fn classify_basis(basis: f64) -> BasisBucket {
    if !basis.is_finite() {
        BasisBucket::Neutral
    } else if basis > BASIS_THRESHOLD {
        BasisBucket::Positive
    } else if basis < -BASIS_THRESHOLD {
        BasisBucket::Negative
    } else {
        BasisBucket::Neutral
    }
}
