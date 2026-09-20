//! Pure quantitative enrichment for the Jev V1 state.
//!
//! This module is deterministic: it reads only caller-owned features and
//! parameters, with no clock, I/O, or hidden mutable state. Returns, realized
//! volatilities, and distances are expressed in percentage points; the
//! lognormal baseline converts its volatility to a decimal before evaluating
//! the dimensionless `d2` formula. Horizon scaling uses seconds and `sqrt(T)`.
//! Invalid ratios and distances are neutralized to `0.0`, invalid baseline
//! probabilities to `0.5`, and invalid volatility-regime ratios to `1.0`.
//!
//! RustQuant replaces only the standard-normal CDF used by the simplified
//! zero-drift lognormal baseline. It does not replace the existing rolling
//! `rolling.rs` O(1) window or the streaming realized-volatility loop in
//! `feature_builder.rs`.

use RustQuant_math::distributions::{Distribution, Gaussian};
use serde::{Deserialize, Serialize};

use crate::strategy::lead_lag::LeadLagFeatures;

/// Default floor for realized volatility expressed in percentage points.
pub const DEFAULT_MIN_VOL_PCT: f64 = 1e-6;

/// Label for the diagnostic baseline probability model.
pub const BASELINE_MODEL: &str = "zero_drift_lognormal";

/// Realized-volatility window used by the z-score and baseline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum VolSource {
    #[serde(rename = "short_1m")]
    Short,
    #[serde(rename = "long_5m")]
    Long,
}

impl VolSource {
    /// Parses the accepted case-insensitive environment spellings.
    #[must_use]
    pub fn parse_env(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "short_1m" | "short" | "1m" => Some(Self::Short),
            "long_5m" | "long" | "5m" => Some(Self::Long),
            _ => None,
        }
    }
}

/// Parameters for the pure quant enrichment.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct QuantParams {
    pub min_vol_pct: f64,
    pub vol_source: VolSource,
    pub horizon_scaling: bool,
}

impl Default for QuantParams {
    fn default() -> Self {
        Self {
            min_vol_pct: DEFAULT_MIN_VOL_PCT,
            vol_source: VolSource::Short,
            horizon_scaling: true,
        }
    }
}

impl QuantParams {
    /// Uses the documented floor when a direct caller supplies an invalid one.
    fn effective_min_vol_pct(self) -> f64 {
        if self.min_vol_pct.is_finite() && self.min_vol_pct > 0.0 {
            self.min_vol_pct
        } else {
            DEFAULT_MIN_VOL_PCT
        }
    }
}

/// Quantitative state enrichment sent alongside the raw V1 features.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuantFeatures {
    pub move_zscore_1s: f64,
    pub z_vol_pct: f64,
    pub z_vol_source: VolSource,
    pub distance_sigma: f64,
    pub sigma_horizon_secs: u64,
    pub quant_baseline_p_yes: f64,
    pub baseline_model: String,
    pub baseline_sigma_pct: f64,
    pub baseline_time_remaining_secs: u64,
    pub vol_regime_ratio: f64,
}

/// Builds the deterministic quant enrichment without reading a clock or doing I/O.
#[must_use]
pub fn build_quant(features: &LeadLagFeatures, params: &QuantParams) -> QuantFeatures {
    let floor_pct = params.effective_min_vol_pct();
    let raw_sigma_pct = match params.vol_source {
        VolSource::Short => features.realized_vol_1m_pct,
        VolSource::Long => features.realized_vol_5m_pct,
    };
    let sigma_pct = floor_vol_pct(raw_sigma_pct, floor_pct);
    let move_zscore_1s = finite_ratio(features.ret_1s_pct, sigma_pct);
    let sigma_horizon_secs = if params.horizon_scaling {
        features.time_remaining_secs
    } else {
        1
    };
    let distance_sigma = distance_sigma(features, sigma_pct, params.horizon_scaling);
    let quant_baseline_p_yes = baseline_probability(features, sigma_pct);
    let vol_regime_ratio = volatility_regime_ratio(
        features.realized_vol_1m_pct,
        features.realized_vol_5m_pct,
        floor_pct,
    );

    QuantFeatures {
        move_zscore_1s,
        z_vol_pct: sigma_pct,
        z_vol_source: params.vol_source,
        distance_sigma,
        sigma_horizon_secs,
        quant_baseline_p_yes,
        baseline_model: BASELINE_MODEL.to_owned(),
        baseline_sigma_pct: sigma_pct,
        baseline_time_remaining_secs: features.time_remaining_secs,
        vol_regime_ratio,
    }
}

fn floor_vol_pct(value: f64, floor_pct: f64) -> f64 {
    if value.is_finite() && value > 0.0 {
        value.max(floor_pct)
    } else {
        floor_pct
    }
}

fn finite_ratio(numerator: f64, denominator: f64) -> f64 {
    if numerator.is_finite() {
        let value = numerator / denominator;
        if value.is_finite() {
            return value;
        }
    }
    0.0
}

fn distance_sigma(features: &LeadLagFeatures, sigma_pct: f64, horizon_scaling: bool) -> f64 {
    if !features.distance_to_target_pct.is_finite() {
        return 0.0;
    }

    let denominator = if horizon_scaling {
        if features.time_remaining_secs == 0 {
            return 0.0;
        }
        sigma_pct * (features.time_remaining_secs as f64).sqrt()
    } else {
        sigma_pct
    };
    finite_ratio(features.distance_to_target_pct, denominator)
}

fn baseline_probability(features: &LeadLagFeatures, sigma_pct: f64) -> f64 {
    let (spot, target) = (features.spot, features.target);
    if !(spot.is_finite() && spot > 0.0 && target.is_finite() && target > 0.0) {
        return 0.5;
    }

    let time_remaining_secs = features.time_remaining_secs;
    if time_remaining_secs == 0 {
        return if spot > target {
            1.0
        } else if spot < target {
            0.0
        } else {
            0.5
        };
    }

    // Inputs are percentage points in the state; the lognormal formula needs
    // decimal per-second sigma.
    let sigma_decimal = sigma_pct / 100.0;
    let time = time_remaining_secs as f64;
    let sigma_time = sigma_decimal * time.sqrt();
    if !(sigma_decimal.is_finite() && sigma_decimal > 0.0 && sigma_time.is_finite()) {
        return 0.5;
    }

    let d2 = ((spot / target).ln() - 0.5 * sigma_decimal * sigma_decimal * time) / sigma_time;
    if !d2.is_finite() {
        return 0.5;
    }

    let probability = Gaussian::new(0.0, 1.0).cdf(d2);
    if probability.is_finite() {
        probability.clamp(0.0, 1.0)
    } else {
        0.5
    }
}

fn volatility_regime_ratio(short_pct: f64, long_pct: f64, floor_pct: f64) -> f64 {
    if !(short_pct.is_finite() && short_pct >= 0.0 && long_pct.is_finite() && long_pct >= 0.0) {
        return 1.0;
    }

    let ratio = floor_vol_pct(short_pct, floor_pct) / floor_vol_pct(long_pct, floor_pct);
    if ratio.is_finite() { ratio } else { 1.0 }
}

#[cfg(test)]
mod tests {
    use super::{BASELINE_MODEL, DEFAULT_MIN_VOL_PCT, QuantParams, VolSource, build_quant};
    use crate::strategy::lead_lag::LeadLagFeatures;

    fn features() -> LeadLagFeatures {
        LeadLagFeatures {
            target: 100_000.0,
            time_remaining_secs: 3_600,
            resolution_source: String::new(),
            asset_symbol: String::new(),
            horizon_label: String::new(),
            horizon_secs: 0,
            spot: 100_000.0,
            distance_to_target_pct: 0.0,
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
            binance_microprice: 0.0,
            coinbase_microprice: 0.0,
            perp_price: 0.0,
            perp_basis_pct: 0.0,
            buy_vol_1s: 0.0,
            sell_vol_1s: 0.0,
            ofi_1s: 0.0,
            ofi_5s: 0.0,
            book_imbalance: 0.0,
            aggressive_buy_ratio: 0.0,
            poly_ofi_5s: 0.0,
            poly_aggressive_buy_ratio: 0.0,
            poly_buy_vol_5s: 0.0,
            poly_sell_vol_5s: 0.0,
            binance_coinbase_diff_pct: 0.0,
            spot_perp_diff_pct: 0.0,
        }
    }

    #[test]
    fn short_source_builds_one_second_zscore() {
        let quant = build_quant(&features(), &QuantParams::default());
        assert!((quant.move_zscore_1s - 2.0).abs() < f64::EPSILON);
        assert_eq!(quant.z_vol_source, VolSource::Short);
    }

    #[test]
    fn long_source_uses_five_minute_sigma() {
        let quant = build_quant(
            &features(),
            &QuantParams {
                vol_source: VolSource::Long,
                ..QuantParams::default()
            },
        );
        assert!((quant.move_zscore_1s - 4.0).abs() < f64::EPSILON);
        assert_eq!(quant.z_vol_pct, 0.005);
    }

    #[test]
    fn scaling_uses_square_root_of_time() {
        let mut input = features();
        input.distance_to_target_pct = -0.1;
        let quant = build_quant(&input, &QuantParams::default());
        assert!((quant.distance_sigma - (-0.1 / 0.6)).abs() < 1e-12);
        assert_eq!(quant.sigma_horizon_secs, 3_600);
    }

    #[test]
    fn scaling_off_records_one_second_horizon() {
        let mut input = features();
        input.distance_to_target_pct = -0.1;
        let quant = build_quant(
            &input,
            &QuantParams {
                horizon_scaling: false,
                ..QuantParams::default()
            },
        );
        assert!((quant.distance_sigma + 10.0).abs() < f64::EPSILON);
        assert_eq!(quant.sigma_horizon_secs, 1);
    }

    #[test]
    fn at_the_money_baseline_includes_zero_drift_term() {
        let mut input = features();
        input.time_remaining_secs = 10_000;
        let quant = build_quant(&input, &QuantParams::default());
        assert!((quant.quant_baseline_p_yes - 0.498005).abs() <= 1e-3);
        assert!(quant.quant_baseline_p_yes > 0.0 && quant.quant_baseline_p_yes < 0.5);
        assert_eq!(quant.baseline_model, BASELINE_MODEL);
    }

    #[test]
    fn baseline_separates_clear_itm_and_otm_cases() {
        let mut input = features();
        input.spot = 130_000.0;
        let itm = build_quant(&input, &QuantParams::default());
        input.spot = 70_000.0;
        let otm = build_quant(&input, &QuantParams::default());
        assert!(itm.quant_baseline_p_yes > 0.99);
        assert!(otm.quant_baseline_p_yes < 0.01);
    }

    #[test]
    fn zero_horizon_uses_digital_limits() {
        let mut input = features();
        input.time_remaining_secs = 0;
        input.distance_to_target_pct = 0.0;
        input.spot = 110_000.0;
        assert_eq!(
            build_quant(&input, &QuantParams::default()).quant_baseline_p_yes,
            1.0
        );
        input.spot = 90_000.0;
        assert_eq!(
            build_quant(&input, &QuantParams::default()).quant_baseline_p_yes,
            0.0
        );
        input.spot = input.target;
        let quant = build_quant(&input, &QuantParams::default());
        assert_eq!(quant.quant_baseline_p_yes, 0.5);
        assert_eq!(quant.distance_sigma, 0.0);
    }

    #[test]
    fn invalid_inputs_return_neutral_outputs() {
        let mut input = features();
        input.target = f64::NAN;
        input.spot = f64::NAN;
        input.distance_to_target_pct = f64::NAN;
        input.ret_1s_pct = f64::NAN;
        input.realized_vol_1m_pct = f64::NAN;
        input.realized_vol_5m_pct = f64::NAN;
        let quant = build_quant(&input, &QuantParams::default());
        assert_eq!(quant.move_zscore_1s, 0.0);
        assert_eq!(quant.distance_sigma, 0.0);
        assert_eq!(quant.quant_baseline_p_yes, 0.5);
        assert_eq!(quant.vol_regime_ratio, 1.0);
    }

    #[test]
    fn regime_ratio_compares_short_and_long_windows() {
        let quant = build_quant(&features(), &QuantParams::default());
        assert!((quant.vol_regime_ratio - 2.0).abs() < f64::EPSILON);
    }

    #[test]
    fn parses_all_environment_spellings_case_insensitively() {
        assert_eq!(VolSource::parse_env("short_1m"), Some(VolSource::Short));
        assert_eq!(VolSource::parse_env("SHORT"), Some(VolSource::Short));
        assert_eq!(VolSource::parse_env("1M"), Some(VolSource::Short));
        assert_eq!(VolSource::parse_env("long_5m"), Some(VolSource::Long));
        assert_eq!(VolSource::parse_env("Long"), Some(VolSource::Long));
        assert_eq!(VolSource::parse_env("5M"), Some(VolSource::Long));
        assert_eq!(VolSource::parse_env("other"), None);
        assert_eq!(DEFAULT_MIN_VOL_PCT, QuantParams::default().min_vol_pct);
    }
}
