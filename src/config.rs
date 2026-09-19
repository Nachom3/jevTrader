use std::env;

use thiserror::Error;

use crate::state::quant_features::{QuantParams, VolSource};

/// Runtime configuration loaded once at application startup.
#[derive(Clone)]
pub struct AppConfig {
    pub typesafe_api_key: String,
    pub polymarket_private_key: String,
    pub questdb_http_url: String,
    pub questdb_ilp_addr: String,
    pub quote_thresholds: QuoteThresholds,
    pub quant: QuantConfig,
}

/// Thresholds for the pure lead-lag quote rule.
///
/// These starting values are the constants formerly defined in
/// `strategy::lead_lag`; they are intended to be calibrated with backtest
/// markouts without changing the Jev question wording.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct QuoteThresholds {
    pub under_min: f64,
    pub next_up_min: f64,
    pub persist_min: f64,
    pub fill_min: f64,
    pub toxic_max: f64,
    pub conflict_max: f64,
    pub no_pressure_max: f64,
}

impl Default for QuoteThresholds {
    fn default() -> Self {
        Self {
            under_min: 0.75,
            next_up_min: 0.65,
            persist_min: 0.60,
            fill_min: 0.60,
            toxic_max: 0.30,
            conflict_max: 0.30,
            no_pressure_max: 0.30,
        }
    }
}

impl QuoteThresholds {
    fn validate(self) -> Result<(), ConfigError> {
        for (name, value) in [
            ("QUOTE_UNDER_MIN", self.under_min),
            ("QUOTE_NEXT_UP_MIN", self.next_up_min),
            ("QUOTE_PERSIST_MIN", self.persist_min),
            ("QUOTE_FILL_MIN", self.fill_min),
            ("QUOTE_TOXIC_MAX", self.toxic_max),
            ("QUOTE_CONFLICT_MAX", self.conflict_max),
            ("QUOTE_NO_PRESSURE_MAX", self.no_pressure_max),
        ] {
            if !(0.0 < value && value < 1.0) {
                return Err(ConfigError::ThresholdOutOfRange { name, value });
            }
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
/// Feature-gated quantitative enrichment settings.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct QuantConfig {
    pub enabled: bool,
    pub params: QuantParams,
}

impl QuantConfig {
    pub fn validate(self) -> Result<(), ConfigError> {
        if !(self.params.min_vol_pct.is_finite() && self.params.min_vol_pct > 0.0) {
            return Err(ConfigError::ThresholdOutOfRange {
                name: "QUANT_MIN_VOL_PCT",
                value: self.params.min_vol_pct,
            });
        }
        Ok(())
    }
}

pub enum ConfigError {
    #[error("missing required environment variable `{0}`")]
    MissingEnvironmentVariable(&'static str),
    #[error("environment variable `{0}` must not be empty")]
    EmptyEnvironmentVariable(&'static str),
    #[error("environment variable `{0}` is not valid UTF-8")]
    InvalidEnvironmentVariable(&'static str),
    #[error("invalid threshold `{name}` value `{value}`")]
    InvalidThreshold { name: &'static str, value: String },
    #[error("threshold `{name}` must be in (0, 1), got {value}")]
    ThresholdOutOfRange { name: &'static str, value: f64 },
    #[error(
        "invalid boolean flag `{name}` value `{value}` (expected 0/1, true/false, yes/no, on/off)"
    )]
    InvalidFlag { name: &'static str, value: String },
    #[error("invalid vol source `{name}` value `{value}` (expected short_1m or long_5m)")]
    InvalidVolSource { name: &'static str, value: String },
}

impl AppConfig {
    /// Loads required credentials, endpoints, and optional quote thresholds.
    ///
    /// `QUOTE_*` variables override the defaults from [`QuoteThresholds`].
    /// The four credentials/endpoints are always required.
    pub fn load() -> Result<Self, ConfigError> {
        let _ = dotenvy::dotenv();

        let defaults = QuoteThresholds::default();
        let quote_thresholds = QuoteThresholds {
            under_min: optional_threshold("QUOTE_UNDER_MIN", defaults.under_min)?,
            next_up_min: optional_threshold("QUOTE_NEXT_UP_MIN", defaults.next_up_min)?,
            persist_min: optional_threshold("QUOTE_PERSIST_MIN", defaults.persist_min)?,
            fill_min: optional_threshold("QUOTE_FILL_MIN", defaults.fill_min)?,
            toxic_max: optional_threshold("QUOTE_TOXIC_MAX", defaults.toxic_max)?,
            conflict_max: optional_threshold("QUOTE_CONFLICT_MAX", defaults.conflict_max)?,
            no_pressure_max: optional_threshold("QUOTE_NO_PRESSURE_MAX", defaults.no_pressure_max)?,
        };
        quote_thresholds.validate()?;

        let defaults = QuantConfig::default();
        let quant = QuantConfig {
            enabled: optional_bool("QUANT_FEATURES_ENABLED", defaults.enabled)?,
            params: QuantParams {
                min_vol_pct: optional_f64("QUANT_MIN_VOL_PCT", defaults.params.min_vol_pct)?,
                vol_source: optional_vol_source("QUANT_Z_VOL_SOURCE", defaults.params.vol_source)?,
                horizon_scaling: optional_bool(
                    "QUANT_HORIZON_SCALING",
                    defaults.params.horizon_scaling,
                )?,
            },
        };
        quant.validate()?;

        Ok(Self {
            typesafe_api_key: required_environment_variable("TYPESAFE_API_KEY")?,
            polymarket_private_key: required_environment_variable("POLYMARKET_PRIVATE_KEY")?,
            questdb_http_url: required_environment_variable("QUESTDB_HTTP_URL")?,
            questdb_ilp_addr: required_environment_variable("QUESTDB_ILP_ADDR")?,
            quote_thresholds,
            quant,
        })
    }
}

fn required_environment_variable(name: &'static str) -> Result<String, ConfigError> {
    match env::var(name) {
        Ok(value) if value.is_empty() => Err(ConfigError::EmptyEnvironmentVariable(name)),
        Ok(value) => Ok(value),
        Err(env::VarError::NotPresent) => Err(ConfigError::MissingEnvironmentVariable(name)),
        Err(env::VarError::NotUnicode(_)) => Err(ConfigError::InvalidEnvironmentVariable(name)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_thresholds_validate() {
        QuoteThresholds::default()
            .validate()
            .expect("documented defaults must be valid");
    }

    #[test]
    fn boundary_thresholds_rejected() {    #[test]
    fn default_quant_is_disabled_and_valid() {
        let quant = QuantConfig::default();

        assert!(!quant.enabled);
        assert!(quant.params.horizon_scaling);
        assert_eq!(quant.params.vol_source, VolSource::Short);
        assert_eq!(quant.params, QuantParams::default());
        quant
            .validate()
            .expect("documented quant defaults must be valid");

        let mut zero_floor = quant;
        zero_floor.params.min_vol_pct = 0.0;
        assert!(zero_floor.validate().is_err());
    }


        for bad in [0.0, 1.0, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let candidate = QuoteThresholds {
                under_min: bad,
                ..QuoteThresholds::default()
            };
            assert!(
                candidate.validate().is_err(),
                "threshold must be rejected: {bad}"
            );
        }
    }
}

fn optional_threshold(name: &'static str, default: f64) -> Result<f64, ConfigError> {
    match env::var(name) {
        Ok(value) => value
            .parse::<f64>()
            .map_err(|_| ConfigError::InvalidThreshold { name, value }),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(env::VarError::NotUnicode(_)) => Err(ConfigError::InvalidEnvironmentVariable(name)),
    }
}
#[allow(clippy::items_after_test_module)]
fn optional_bool(name: &'static str, default: bool) -> Result<bool, ConfigError> {
    match env::var(name) {
        Ok(value) => match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Ok(true),
            "0" | "false" | "no" | "off" => Ok(false),
            _ => Err(ConfigError::InvalidFlag { name, value }),
        },
        Err(env::VarError::NotPresent) => Ok(default),
        Err(env::VarError::NotUnicode(_)) => Err(ConfigError::InvalidEnvironmentVariable(name)),
    }
}

#[allow(clippy::items_after_test_module)]
fn optional_f64(name: &'static str, default: f64) -> Result<f64, ConfigError> {
    match env::var(name) {
        Ok(value) => value
            .parse::<f64>()
            .map_err(|_| ConfigError::InvalidThreshold { name, value }),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(env::VarError::NotUnicode(_)) => Err(ConfigError::InvalidEnvironmentVariable(name)),
    }
}

#[allow(clippy::items_after_test_module)]
fn optional_vol_source(name: &'static str, default: VolSource) -> Result<VolSource, ConfigError> {
    match env::var(name) {
        Ok(value) => {
            VolSource::parse_env(&value).ok_or(ConfigError::InvalidVolSource { name, value })
        }
        Err(env::VarError::NotPresent) => Ok(default),
        Err(env::VarError::NotUnicode(_)) => Err(ConfigError::InvalidEnvironmentVariable(name)),
    }
}
