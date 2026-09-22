//! Public strategy contract for deterministic replay and live orchestration.
//!
//! A [`Strategy`] only evaluates point-in-time (`PIT`) market state. It does
//! not decide fills, mutate a portfolio, or read future events; the replay
//! layer's [`FillSimulator`](crate::replay) owns execution. `MarketEvent`
//! carries the strategy version so actions remain compact and the replay can
//! attribute every intent to the version that observed it.
//!
//! [`Side`] is the existing [`TradeSide`](crate::domain::TradeSide) type. For
//! V1, `TradeSide::Buy` means buying YES: V1 is BUY-YES-only and never emits a
//! sell action. `Action` deliberately does not repeat `strategy_version`.
//!
//! V1 also deliberately emits `size_shares = 0.0`. This is an intent rather
//! than an executable order; replay applies the configured dollar sizing via
//! `size_entry`. V1 has no exit policy. Even when an open position's signal
//! decays, it emits `Hold { reason: "position_open" }`; exit decisions belong
//! to the replay exit policy (Task 4).

use jevtrader::config::QuoteThresholds;
use jevtrader::domain::{PriceTicks, TickSize};
use serde::{Deserialize, Serialize};

use super::lead_lag::{V1Signal, should_quote};

/// Existing venue side type used by [`Action::PlaceMaker`].
///
/// V1 maps `TradeSide::Buy` to a YES maker quote. No NO or SELL action is
/// emitted by [`V1Strategy`].
pub use jevtrader::domain::TradeSide as Side;

/// Complete point-in-time state visible to a strategy for one market event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketEvent {
    pub event_id: String,
    pub ts_ms: i64,
    pub condition_id: String,
    pub asset: String,
    pub horizon: String,
    pub signal: V1Signal,
    pub yes_bid: f64,
    pub yes_ask: f64,
    pub book_stale: bool,
    pub open_qty: f64,
    pub open_avg_price: f64,
    pub strategy_version: String,
}

/// Intent returned by a strategy.
///
/// The strategy version is carried by the [`MarketEvent`] that produced the
/// action, not duplicated in every variant. These are intents only: execution
/// and fill simulation happen outside the strategy.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Action {
    PlaceMaker {
        side: Side,
        price: f64,
        size_shares: f64,
    },
    Cancel {
        order_id: String,
    },
    Replace {
        order_id: String,
        price: f64,
        size_shares: f64,
    },
    Hedge {
        opposite_price: f64,
    },
    Merge,
    Hold {
        reason: String,
    },
}

/// Point-in-time market constraints available to a strategy.
#[derive(Debug, Clone, Copy)]
pub struct StrategyContext {
    pub thresholds: QuoteThresholds,
    pub tick_size: f64,
    pub min_order_size: f64,
}

/// Deterministic strategy interface used by replay and live adapters.
pub trait Strategy {
    fn strategy_version(&self) -> &'static str;

    fn on_market_event(&mut self, ctx: &StrategyContext, event: &MarketEvent) -> Vec<Action>;
}

/// V1 Lead-Lag Maker strategy.
///
/// V1 is intentionally quote-or-hold. It reuses the shared [`should_quote`]
/// rule, emits one post-only YES quote when the book permits it, and leaves
/// sizing and execution to replay.
#[derive(Debug, Clone, Copy, Default)]
pub struct V1Strategy;

impl V1Strategy {
    /// Stable strategy identity persisted with replay episodes.
    pub const VERSION: &'static str = "v1-lead-lag";

    fn hold(reason: &str) -> Vec<Action> {
        vec![Action::Hold {
            reason: reason.to_owned(),
        }]
    }
}

impl Strategy for V1Strategy {
    fn strategy_version(&self) -> &'static str {
        Self::VERSION
    }

    fn on_market_event(&mut self, ctx: &StrategyContext, event: &MarketEvent) -> Vec<Action> {
        if event.book_stale {
            return Self::hold("stale_book");
        }

        let Some(candidate) = candidate_price(event.yes_bid, event.yes_ask, ctx.tick_size) else {
            return Self::hold("invalid_book");
        };

        if candidate >= event.yes_ask {
            return Self::hold("crossed");
        }

        if event.open_qty > 0.0 && event.signal.underreact_up < ctx.thresholds.under_min {
            return Self::hold("position_open");
        }

        if !should_quote(&event.signal, &ctx.thresholds) {
            return Self::hold("rule_rejected");
        }

        // Sizing is intentionally not decided here. The replay applies its
        // dollar sizing policy through `size_entry`.
        vec![Action::PlaceMaker {
            side: Side::Buy,
            price: candidate,
            size_shares: 0.0,
        }]
    }
}

fn candidate_price(bid: f64, ask: f64, tick_size: f64) -> Option<f64> {
    if !bid.is_finite()
        || !ask.is_finite()
        || !(0.0..=1.0).contains(&bid)
        || !(0.0..=1.0).contains(&ask)
        || !tick_size.is_finite()
        || !(0.0..=1.0).contains(&tick_size)
        || tick_size <= 0.0
    {
        return None;
    }

    let bid = PriceTicks::from_f64(bid);
    let tick = TickSize::from_f64(tick_size);
    let tick_micros = (tick.to_f64() * 1_000_000.0).round() as u64;
    if tick_micros == 0 {
        return None;
    }

    // Round the observed bid up to a valid tick, then move one more tick over
    // it. This matches the existing post-only quote rule for non-aligned bids.
    let rounded_bid_ticks = bid.as_micros().div_ceil(tick_micros);
    let candidate_micros = rounded_bid_ticks.checked_add(1)?.checked_mul(tick_micros)?;
    (candidate_micros <= 1_000_000).then_some(candidate_micros as f64 / 1_000_000.0)
}
