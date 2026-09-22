//! Deterministic order sizing for replay entries and hedges.
//!
//! `min_order_size` is expressed in shares, not USD notional. Both price and
//! share quantities are rounded down only where the venue's discrete
//! constraints require it; an order that becomes non-executable is rejected.

use serde::{Deserialize, Serialize};

/// Default USDC stake for a standard replay entry.
pub const STANDARD_STAKE_USD: f64 = 5.0;

/// Venue constraints used to quantize a replay order.
///
/// `min_order_size` is measured in shares. It is deliberately not converted
/// to notional because the venue's minimum quantity constraint is a share
/// quantity and the actual notional is reported separately.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct MarketConstraints {
    pub tick_size: f64,
    pub size_step: f64,
    pub min_order_size: f64,
}

impl MarketConstraints {
    /// Creates validated market constraints.
    pub fn new(tick_size: f64, size_step: f64, min_order_size: f64) -> Result<Self, String> {
        let constraints = Self {
            tick_size,
            size_step,
            min_order_size,
        };
        constraints.validate()?;
        Ok(constraints)
    }

    /// Validates the constraint values without changing them.
    pub fn validate(&self) -> Result<(), String> {
        if !self.tick_size.is_finite() || self.tick_size <= 0.0 {
            return Err("tick_size must be finite and greater than zero".to_owned());
        }
        if !self.size_step.is_finite() || self.size_step <= 0.0 {
            return Err("size_step must be finite and greater than zero".to_owned());
        }
        if !self.min_order_size.is_finite() || self.min_order_size < 0.0 {
            return Err("min_order_size must be finite and non-negative".to_owned());
        }
        Ok(())
    }
}

/// A venue-quantized order and the accounting impact of its rounding.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct SizedOrder {
    pub limit_price: f64,
    pub shares: f64,
    pub intended_stake_usd: f64,
    pub actual_notional_usd: f64,
    /// Intended stake minus actual notional; rounding down keeps this
    /// non-negative apart from ordinary floating-point representation error.
    pub rounding_delta_usd: f64,
}

/// Sizes a standard entry for a requested stake.
///
/// Rust has no default function arguments, so callers wanting the standard
/// $5 stake should pass [`STANDARD_STAKE_USD`] (or use
/// [`size_standard_entry`]). The raw price is validated before rounding; no
/// invalid request is silently adjusted into an executable order.
pub fn size_entry(
    limit_price_raw: f64,
    stake_usd: f64,
    c: &MarketConstraints,
) -> Result<SizedOrder, String> {
    c.validate()?;
    validate_price(limit_price_raw)?;
    validate_stake(stake_usd)?;

    let limit_price = round_price(limit_price_raw, c.tick_size)?;
    let shares_raw = stake_usd / limit_price;
    let shares = floor_to_step(shares_raw, c.size_step)?;
    validate_shares(shares, c.min_order_size)?;

    sized_order(limit_price, shares, stake_usd)
}

/// Sizes an entry using the standard $5 USDC stake.
pub fn size_standard_entry(
    limit_price_raw: f64,
    c: &MarketConstraints,
) -> Result<SizedOrder, String> {
    size_entry(limit_price_raw, STANDARD_STAKE_USD, c)
}

/// Sizes a hedge at the same share quantity as the entry, subject to the
/// venue's share step. The hedge is not resized to the standard $5 stake.
pub fn size_hedge(
    entry_shares: f64,
    opposite_price_raw: f64,
    c: &MarketConstraints,
) -> Result<SizedOrder, String> {
    c.validate()?;
    if !entry_shares.is_finite() || entry_shares <= 0.0 {
        return Err("entry_shares must be finite and greater than zero".to_owned());
    }
    validate_price(opposite_price_raw)?;

    let limit_price = round_price(opposite_price_raw, c.tick_size)?;
    let shares = floor_to_step(entry_shares, c.size_step)?;
    validate_shares(shares, c.min_order_size)?;

    let intended_stake_usd = entry_shares * limit_price;
    if !intended_stake_usd.is_finite() {
        return Err("hedge intended stake must be finite".to_owned());
    }
    sized_order(limit_price, shares, intended_stake_usd)
}

fn validate_price(price: f64) -> Result<(), String> {
    if !price.is_finite() || price <= 0.0 {
        return Err("limit price must be finite and greater than zero".to_owned());
    }
    if price >= 1.0 {
        return Err("limit price must be strictly between zero and one".to_owned());
    }
    Ok(())
}

fn validate_stake(stake_usd: f64) -> Result<(), String> {
    if !stake_usd.is_finite() || stake_usd <= 0.0 {
        return Err("stake_usd must be finite and greater than zero".to_owned());
    }
    Ok(())
}

fn round_price(price: f64, tick_size: f64) -> Result<f64, String> {
    let ticks = (price / tick_size + 0.5).floor();
    let rounded = ticks * tick_size;
    if !rounded.is_finite() || rounded <= 0.0 || rounded >= 1.0 {
        return Err("price rounded to tick must be strictly between zero and one".to_owned());
    }
    Ok(rounded)
}

fn floor_to_step(value: f64, step: f64) -> Result<f64, String> {
    let units = (value / step).floor();
    let rounded = units * step;
    if !rounded.is_finite() || rounded <= 0.0 {
        return Err("shares must remain positive after size-step rounding".to_owned());
    }
    Ok(rounded)
}

fn validate_shares(shares: f64, min_order_size: f64) -> Result<(), String> {
    if shares < min_order_size {
        return Err(format!(
            "shares {shares} are below min_order_size {min_order_size}"
        ));
    }
    Ok(())
}

fn sized_order(
    limit_price: f64,
    shares: f64,
    intended_stake_usd: f64,
) -> Result<SizedOrder, String> {
    let actual_notional_usd = shares * limit_price;
    let rounding_delta_usd = intended_stake_usd - actual_notional_usd;
    if !actual_notional_usd.is_finite() || !rounding_delta_usd.is_finite() {
        return Err("order notional and rounding delta must be finite".to_owned());
    }

    Ok(SizedOrder {
        limit_price,
        shares,
        intended_stake_usd,
        actual_notional_usd,
        rounding_delta_usd,
    })
}
