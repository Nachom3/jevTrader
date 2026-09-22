//! Historical and current fee regimes for replay accounting.
//!
//! This module is deliberately pure: callers provide the fee regime selected
//! for a historical date and the venue context, while I/O and market metadata
//! remain outside replay accounting.

use serde::{Deserialize, Serialize};

use super::ledger::TradeEpisode;

/// Fee and maker-rebate rates represented as fractions of notional.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct FeeRegime {
    /// Fee charged to a maker fill, as a fraction of notional.
    pub maker_rate: f64,
    /// Fee charged to a taker fill, as a fraction of notional.
    pub taker_rate: f64,
    /// Maker rebate rate exposed for reporting, never applied to the ledger.
    pub maker_rebate: f64,
    /// Versioned name of the regime.
    pub label: &'static str,
}

impl FeeRegime {
    fn from_parts(
        maker_rate: f64,
        taker_rate: f64,
        maker_rebate: f64,
        label: &'static str,
    ) -> Result<Self, String> {
        let regime = Self {
            maker_rate,
            taker_rate,
            maker_rebate,
            label,
        };
        regime.validate()?;
        Ok(regime)
    }

    fn validate(&self) -> Result<(), String> {
        validate_rate("maker_rate", self.maker_rate)?;
        validate_rate("taker_rate", self.taker_rate)?;
        validate_rate("maker_rebate", self.maker_rebate)
    }
}

fn validate_rate(name: &str, value: f64) -> Result<(), String> {
    if !value.is_finite() || !(0.0..1.0).contains(&value) {
        return Err(format!("{name} must be finite and in the range [0, 1)"));
    }
    Ok(())
}

/// Current crypto fee schedule used by the replay baseline.
///
/// Source: the current Polymarket fee documentation and each market's
/// `feesEnabled` metadata. The caller decides whether a fill is maker or
/// taker and whether this regime applies; this module performs no I/O and
/// does not inspect market metadata.
#[must_use]
pub fn current_crypto_regime() -> FeeRegime {
    FeeRegime::from_parts(0.0, 0.07, 0.0, "current_crypto")
        .expect("the named current crypto regime must be valid")
}

/// Zero-fee regime for fee-free markets or sensitivity analysis.
#[must_use]
pub fn zero_regime() -> FeeRegime {
    FeeRegime::from_parts(0.0, 0.0, 0.0, "zero").expect("the zero regime must be valid")
}

/// Creates a validated historical regime.
///
/// The caller owns the versioned date table and supplies its label here. No
/// dates or historical schedules are hardcoded in this module.
pub fn historical_regime(
    maker_rate: f64,
    taker_rate: f64,
    rebate: f64,
    label: &'static str,
) -> Result<FeeRegime, String> {
    FeeRegime::from_parts(maker_rate, taker_rate, rebate, label)
}

/// Calculates the fee for one positive fill notional.
///
/// `notional_usd` is the caller's `price * quantity`; it is not recomputed or
/// fetched here. A zero notional is invalid for an individual fill. The
/// all-zero no-fill exception is handled only by [`apply_to_episode`].
pub fn fee_for_fill(notional_usd: f64, is_maker: bool, regime: &FeeRegime) -> Result<f64, String> {
    regime.validate()?;
    if !notional_usd.is_finite() || notional_usd <= 0.0 {
        return Err("fill notional must be finite and greater than zero".to_owned());
    }

    let rate = if is_maker {
        regime.maker_rate
    } else {
        regime.taker_rate
    };
    let fee = notional_usd * rate;
    if !fee.is_finite() || fee < 0.0 {
        return Err("calculated fee must be finite and non-negative".to_owned());
    }
    Ok(if rate == 0.0 { 0.0 } else { fee })
}

/// Applies historical and current fee views to one trade episode.
///
/// The episode's principal accounting fields (`fees_usd`, `rebates_usd`, and
/// `net_pnl_usd`) are always overwritten with the historical view, so portfolio
/// and cash calculations remain reproducible. The current comparison is
/// stored separately in `pnl_current_usd` and never contaminates that
/// principal. Rebate rates are intentionally not booked: use
/// [`upside_with_rebate`] for a separate upside display.
///
/// If the episode is `NoFill`, positive notionals are rejected. Both notionals
/// exactly zero are accepted only for `NoFill` and must have zero gross PnL;
/// both PnL streams are then set to `Some(0.0)`. Any invalid input returns
/// before mutating the episode. Existing accounting and PnL values are
/// deterministically overwritten on success.
pub fn apply_to_episode(
    ep: &mut TradeEpisode,
    gross: f64,
    fill_notional: f64,
    exit_notional: f64,
    is_maker: bool,
    historical: &FeeRegime,
    current: &FeeRegime,
) -> Result<(), String> {
    if !gross.is_finite() {
        return Err("gross PnL must be finite".to_owned());
    }
    validate_component_notional("fill", fill_notional)?;
    validate_component_notional("exit", exit_notional)?;
    historical.validate()?;
    current.validate()?;

    if fill_notional == 0.0 && exit_notional == 0.0 {
        if !ep.is_no_fill() {
            return Err("zero notionals are valid only for a no-fill episode".to_owned());
        }
        if gross != 0.0 {
            return Err("zero notionals require zero gross PnL".to_owned());
        }
        ep.settle_accounting(0.0, 0.0, 0.0, 0.0)?;
        ep.pnl_historical_usd = Some(0.0);
        ep.pnl_current_usd = Some(0.0);
        return Ok(());
    }
    if ep.is_no_fill() {
        return Err("positive notionals cannot be applied to a no-fill episode".to_owned());
    }

    let total_notional = fill_notional + exit_notional;
    if !total_notional.is_finite() || total_notional <= 0.0 {
        return Err(
            "combined fill and exit notional must be finite and greater than zero".to_owned(),
        );
    }

    let fee_historical = fee_for_fill(total_notional, is_maker, historical)?;
    let fee_current = fee_for_fill(total_notional, is_maker, current)?;
    let net_historical = gross - fee_historical;
    let net_current = gross - fee_current;
    if !net_historical.is_finite() || !net_current.is_finite() {
        return Err("historical and current net PnL must be finite".to_owned());
    }

    // The ledger API verifies gross - fees + rebates == net before mutation.
    // Rebate stays zero even when the selected regime exposes a rebate rate.
    ep.settle_accounting(gross, fee_historical, 0.0, net_historical)?;
    ep.pnl_historical_usd = Some(net_historical);
    ep.pnl_current_usd = Some(net_current);
    Ok(())
}

fn validate_component_notional(name: &str, notional: f64) -> Result<(), String> {
    if !notional.is_finite() || notional < 0.0 {
        return Err(format!("{name} notional must be finite and non-negative"));
    }
    Ok(())
}

/// Returns display-only upside with a rebate.
///
/// This helper does not mutate a ledger and is intentionally separate from
/// the principal accounting path, where rebates remain zero.
#[must_use]
pub fn upside_with_rebate(gross: f64, fee: f64, rebate: f64) -> f64 {
    gross - fee + rebate
}
