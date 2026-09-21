//! Deterministic replay exits, hedges, and complete YES/NO merges.
//!
//! Hedge and exit proposals are not executions. In particular, this module
//! deliberately does not use `src/execution/paper.rs`: that model uses a
//! touch-based `DEFAULT_FILL_RATIO_PER_TOUCH` assumption and supports BUY
//! only, while replay requires conservative trade-through evidence. The
//! caller must turn a [`HedgeQuote`] into a resting order and pass it through
//! [`crate::replay::FillSimulator`] with real [`crate::replay::fills::FillPrint`]
//! values before applying a fill. Instantaneous hedge execution is forbidden
//! (invariant 8).

use serde::{Deserialize, Serialize};

use super::ledger::{ExitType, Side, TradeEpisode};

/// Policy selected by the replay strategy for closing or hedging a position.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExitPolicy {
    Hold,
    HedgeProfit { target_profit_usd_per_share: f64 },
    HedgeDynamic,
    RiskExit,
}

/// Proposed opposite-side order for a hedge.
///
/// This value contains no fill information. The proposal becomes a trade only
/// after the caller runs it through the replay fill engine.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct HedgeQuote {
    pub side: Side,
    pub limit_price: f64,
    pub qty: f64,
    pub arrival_ts_ms: i64,
}

/// Deterministic accounting result for merging a complete YES/NO pair.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct MergeResult {
    pub yes_cost_usd: f64,
    pub no_cost_usd: f64,
    pub qty: f64,
    pub total_cost_usd: f64,
    pub terminal_value: f64,
    pub locked_pnl: f64,
}

/// Returns whether the two token prices meet the requested hedge profit.
///
/// Prices must be finite and strictly inside `(0, 1)`. A negative target is
/// not a valid profit target and is rejected as well.
#[must_use]
pub fn should_hedge_profit(entry_price: f64, opposite_price: f64, target: f64) -> bool {
    if !entry_price.is_finite()
        || !opposite_price.is_finite()
        || !target.is_finite()
        || !(0.0 < entry_price && entry_price < 1.0)
        || !(0.0 < opposite_price && opposite_price < 1.0)
        || target < 0.0
    {
        return false;
    }

    // The epsilon keeps decimal market prices on the mathematical boundary
    // despite binary floating-point rounding (for example, 0.45 + 0.52).
    entry_price + opposite_price <= 1.0 - target + 1e-12
}

/// Builds an opposite-side hedge proposal without executing it.
///
/// The quote inherits the entry's filled quantity and original arrival time.
/// It must still be represented as a new resting order and evaluated by
/// [`crate::replay::FillSimulator`] using [`crate::replay::fills::FillPrint`]
/// data; this function never creates a fill.
pub fn hedge_quote(entry: &TradeEpisode, opposite_price: f64) -> Result<HedgeQuote, String> {
    let (_, _, qty) = filled_values(entry)?;
    if !opposite_price.is_finite() || !(0.0 < opposite_price && opposite_price < 1.0) {
        return Err("opposite_price must be finite and strictly between zero and one".to_owned());
    }

    Ok(HedgeQuote {
        side: opposite_side(entry.side),
        limit_price: opposite_price,
        qty,
        arrival_ts_ms: entry.order_arrival_ts_ms,
    })
}

/// Settles both legs of a filled hedge using only the other leg's fill data.
///
/// The function has no error return in order to preserve the requested API:
/// invalid or incomplete episodes are left untouched. With two valid fills,
/// both exits use the later fill timestamp and each gross PnL is calculated
/// from the opposite leg's actual fill price.
pub fn settle_hedge_pair(entry: &mut TradeEpisode, hedge: &mut TradeEpisode) {
    let Ok((entry_fill_ts, entry_fill_price, entry_qty)) = filled_values(entry) else {
        return;
    };
    let Ok((hedge_fill_ts, hedge_fill_price, hedge_qty)) = filled_values(hedge) else {
        return;
    };

    let exit_ts_ms = entry_fill_ts.max(hedge_fill_ts);
    if entry
        .apply_exit(ExitType::Hedge, exit_ts_ms, hedge_fill_price)
        .is_err()
    {
        return;
    }
    if hedge
        .apply_exit(ExitType::Hedge, exit_ts_ms, entry_fill_price)
        .is_err()
    {
        return;
    }

    entry.gross_pnl_usd = entry_qty * (hedge_fill_price - entry_fill_price);
    hedge.gross_pnl_usd = hedge_qty * (entry_fill_price - hedge_fill_price);
}

/// Computes the deterministic value and PnL of merging a complete YES/NO pair.
///
/// A complete pair pays exactly `$1` per share at resolution, so the terminal
/// value is `qty` and locked PnL is terminal value minus both acquisition costs
/// (invariant 7).
pub fn merge_pair(yes_cost_usd: f64, no_cost_usd: f64, qty: f64) -> Result<MergeResult, String> {
    if !qty.is_finite() || qty <= 0.0 {
        return Err("qty must be finite and greater than zero".to_owned());
    }
    if !yes_cost_usd.is_finite() || !no_cost_usd.is_finite() {
        return Err("pair costs must be finite".to_owned());
    }

    let total_cost_usd = yes_cost_usd + no_cost_usd;
    let terminal_value = qty;
    let locked_pnl = terminal_value - total_cost_usd;
    if !total_cost_usd.is_finite() || !locked_pnl.is_finite() {
        return Err("merge result must be finite".to_owned());
    }

    Ok(MergeResult {
        yes_cost_usd,
        no_cost_usd,
        qty,
        total_cost_usd,
        terminal_value,
        locked_pnl,
    })
}

/// Settles an episode against the caller-provided real resolution outcome.
///
/// `payoff_win` is the winner's terminal payoff, normally `1.0`. The loser
/// always receives zero. Resolution timestamps before the fill are rejected;
/// this function never invents an outcome or a timestamp (invariant 9).
pub fn settle_resolution(
    episode: &mut TradeEpisode,
    yes_won: bool,
    resolution_ts_ms: i64,
    payoff_win: f64,
) -> Result<(), String> {
    let Ok((fill_ts_ms, fill_price, fill_qty)) = filled_values(episode) else {
        return Err("a filled episode is required before resolution".to_owned());
    };
    if resolution_ts_ms < fill_ts_ms {
        return Err(format!(
            "resolution timestamp {resolution_ts_ms} precedes fill timestamp {fill_ts_ms}"
        ));
    }
    if !payoff_win.is_finite() || !(0.0..=1.0).contains(&payoff_win) {
        return Err("payoff_win must be finite and between zero and one".to_owned());
    }

    let won = match episode.side {
        Side::BuyYes => yes_won,
        Side::BuyNo => !yes_won,
    };
    let payoff = if won { payoff_win } else { 0.0 };
    episode.apply_exit(ExitType::Resolution, resolution_ts_ms, payoff)?;
    episode.gross_pnl_usd = fill_qty * (payoff - fill_price);
    Ok(())
}

fn opposite_side(side: Side) -> Side {
    match side {
        Side::BuyYes => Side::BuyNo,
        Side::BuyNo => Side::BuyYes,
    }
}

fn filled_values(episode: &TradeEpisode) -> Result<(i64, f64, f64), String> {
    let fill_ts_ms = episode
        .fill_ts_ms
        .ok_or_else(|| "entry must have a fill timestamp".to_owned())?;
    let fill_price = episode
        .fill_price
        .ok_or_else(|| "entry must have a fill price".to_owned())?;
    let fill_qty = episode
        .fill_qty
        .ok_or_else(|| "entry must have a fill quantity".to_owned())?;

    if !fill_price.is_finite() || !(0.0 < fill_price && fill_price < 1.0) {
        return Err("fill price must be finite and strictly between zero and one".to_owned());
    }
    if !fill_qty.is_finite() || fill_qty <= 0.0 {
        return Err("fill quantity must be finite and greater than zero".to_owned());
    }

    Ok((fill_ts_ms, fill_price, fill_qty))
}
