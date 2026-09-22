//! Conservative tape fill models: OPTIMISTIC / BASE / CONSERVATIVE.
//!
//! The historical tape is mostly trades, not full queue snapshots, so an
//! exact fill can never be claimed. Every fill records its `fill_model`.
//!
//! Side convention: `RestingOrder::side_buy = true` is a BUY YES maker order.
//! It is filled by aggressive SELL prints that trade down through the limit.
//! `side_buy = false` is a SELL/hedge maker order. It is filled by aggressive
//! BUY prints that trade up through the limit. Unknown aggressors contribute
//! half volume in either direction.
//!
//! - OPTIMISTIC: eligible directional touch or through volume can fill.
//! - BASE: eligible directional touch or through volume fills proportionally
//!   after the queue-ahead proxy.
//! - CONSERVATIVE: requires two distinct directional through prints and
//!   sustained volume through the level after the order is resting.
//!
//! Touches contribute half volume. They can fill OPTIMISTIC and BASE, but a
//! touch alone never satisfies CONSERVATIVE's two-through-print requirement.

use super::types::FillProfile;

/// The aggressor side reported by a tape print.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Aggressor {
    Buy,
    Sell,
    Unknown,
}

/// One tape print used by the maker fill simulator.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FillPrint {
    pub ts_ms: i64,
    pub price: f64,
    pub qty: f64,
    pub aggressor: Aggressor,
}

impl FillPrint {
    /// Creates a print, normalizing invalid quantity to zero.
    #[must_use]
    pub fn new(ts_ms: i64, price: f64, qty: f64, aggressor: Aggressor) -> Self {
        Self {
            ts_ms,
            price,
            qty: valid_qty(qty),
            aggressor,
        }
    }
}

impl From<(i64, f64, f64)> for FillPrint {
    fn from((ts_ms, price, qty): (i64, f64, f64)) -> Self {
        Self::new(ts_ms, price, qty, Aggressor::Unknown)
    }
}

/// Resting maker order in replay event time.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RestingOrder {
    pub price: f64,
    pub size: f64,
    pub resting_from_ms: i64,
    pub side_buy: bool,
}

/// Execution latency already applied before this fill check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionLatency {
    pub submit_ms: u64,
}

impl ExecutionLatency {
    #[must_use]
    pub const fn new(submit_ms: u64) -> Self {
        Self { submit_ms }
    }
}

/// One fill decision with its model label and partial-fill fraction.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FillOutcome {
    pub filled: bool,
    pub fill_fraction: f64,
    pub fill_price: f64,
    pub profile: FillProfile,
    /// Event-time print that completed the simulated fill, never arrival time.
    pub fill_ts_ms: Option<i64>,
    /// Aggressive directional volume consumed by this simulated fill.
    pub used_aggressive_qty: f64,
}

impl FillOutcome {
    #[must_use]
    pub const fn no_fill(fill_price: f64, profile: FillProfile) -> Self {
        Self {
            filled: false,
            fill_fraction: 0.0,
            fill_price,
            profile,
            fill_ts_ms: None,
            used_aggressive_qty: 0.0,
        }
    }
}

/// Tape-fill simulator with explicit queue-ahead and through-volume proxies.
pub struct FillSimulator {
    pub profile: FillProfile,
    /// Shares assumed ahead of us in queue (0..1 of level volume).
    pub queue_ahead: f64,
    /// Conservative full-fill multiple. The default requires 2x effective
    /// order size; the half-fill threshold is one quarter of this multiple.
    pub through_multiple: f64,
}

impl FillSimulator {
    #[must_use]
    pub const fn new(profile: FillProfile) -> Self {
        let queue_ahead = match profile {
            FillProfile::Optimistic => 0.0,
            FillProfile::Base => 0.3,
            FillProfile::Conservative => 0.6,
        };
        Self {
            profile,
            queue_ahead,
            through_multiple: 2.0,
        }
    }

    /// Overrides queue ahead, clamped to the valid `0..=1` range.
    #[must_use]
    pub fn with_queue_ahead(mut self, queue_ahead: f64) -> Self {
        self.queue_ahead = clamp_unit(queue_ahead);
        self
    }

    /// Overrides the conservative through-volume sensitivity multiple.
    #[must_use]
    pub fn with_through_multiple(mut self, through_multiple: f64) -> Self {
        self.through_multiple = if through_multiple.is_nan() {
            2.0
        } else {
            through_multiple.max(0.0)
        };
        self
    }

    /// Decides a fill for a resting order given subsequent tape prints.
    ///
    /// Only prints with `ts_ms >= resting_from_ms + latency.submit_ms` are
    /// considered. Prints are consumed in the supplied event-time order; no
    /// later print is used to change an earlier completion timestamp.
    ///
    /// `FillPrint` is the preferred input. The legacy `(ts_ms, price, qty)`
    /// tuple is also accepted and is treated as an `Unknown` aggressor.
    pub fn check_fill<P>(
        &self,
        order: &RestingOrder,
        prints: &[P],
        latency: ExecutionLatency,
    ) -> FillOutcome
    where
        P: Copy + Into<FillPrint>,
    {
        let submit_ms = i64::try_from(latency.submit_ms).unwrap_or(i64::MAX);
        let eligible_from = order.resting_from_ms.saturating_add(submit_ms);
        let order_size = valid_qty(order.size);
        if order_size == 0.0 || !order.price.is_finite() {
            return FillOutcome::no_fill(order.price, self.profile);
        }

        let mut evidence = Vec::new();
        let mut directional_available = 0.0;
        let mut through_prints = 0usize;

        for raw_print in prints {
            let print: FillPrint = (*raw_print).into();
            if print.ts_ms < eligible_from {
                continue;
            }

            let aggressor_weight = directional_aggressor_weight(order.side_buy, print.aggressor);
            if aggressor_weight == 0.0 {
                continue;
            }

            let Some(price_weight) = price_weight(order, print.price) else {
                continue;
            };
            let contribution = valid_qty(print.qty) * aggressor_weight * price_weight;
            if contribution == 0.0 {
                continue;
            }

            let is_through = price_weight == 1.0 && print.price != order.price;
            if is_through {
                through_prints += 1;
            }
            directional_available += contribution;
            evidence.push((print.ts_ms, contribution, is_through));
        }

        let effective_available = after_queue(directional_available, self.queue_ahead);
        if effective_available <= 0.0 || effective_available.is_nan() {
            return FillOutcome::no_fill(order.price, self.profile);
        }

        match self.profile {
            FillProfile::Optimistic | FillProfile::Base => {
                let fill_qty = effective_available.min(order_size);
                let fill_ts_ms = completion_timestamp_for_fill(
                    &evidence,
                    self.queue_ahead,
                    fill_qty,
                    effective_available >= order_size,
                );
                outcome_from_available(order, self.profile, effective_available, 1.0, fill_ts_ms)
            }
            FillProfile::Conservative => {
                if through_prints < 2 {
                    return FillOutcome::no_fill(order.price, self.profile);
                }

                let full_threshold = order_size * self.through_multiple;
                let half_threshold = full_threshold * 0.25;
                let desired_fraction = if effective_available >= full_threshold {
                    1.0
                } else if effective_available >= half_threshold {
                    0.5
                } else {
                    return FillOutcome::no_fill(order.price, self.profile);
                };

                let fill_ts_ms = completion_timestamp(
                    &evidence,
                    self.queue_ahead,
                    2,
                    if desired_fraction == 1.0 {
                        full_threshold
                    } else {
                        half_threshold
                    },
                );
                outcome_from_available(
                    order,
                    self.profile,
                    effective_available,
                    desired_fraction,
                    fill_ts_ms,
                )
            }
        }
    }
}

fn valid_qty(qty: f64) -> f64 {
    if qty.is_finite() && qty > 0.0 {
        qty
    } else {
        0.0
    }
}

fn clamp_unit(value: f64) -> f64 {
    if value.is_nan() {
        0.0
    } else {
        value.clamp(0.0, 1.0)
    }
}

fn directional_aggressor_weight(side_buy: bool, aggressor: Aggressor) -> f64 {
    match (side_buy, aggressor) {
        (true, Aggressor::Sell) | (false, Aggressor::Buy) => 1.0,
        (_, Aggressor::Unknown) => 0.5,
        _ => 0.0,
    }
}

fn price_weight(order: &RestingOrder, print_price: f64) -> Option<f64> {
    if order.side_buy {
        if print_price < order.price {
            Some(1.0)
        } else if print_price == order.price {
            Some(0.5)
        } else {
            None
        }
    } else if print_price > order.price {
        Some(1.0)
    } else if print_price == order.price {
        Some(0.5)
    } else {
        None
    }
}

fn after_queue(available: f64, queue_ahead: f64) -> f64 {
    if available <= 0.0 || queue_ahead >= 1.0 {
        0.0
    } else {
        available * (1.0 - queue_ahead)
    }
}

fn completion_timestamp_for_fill(
    evidence: &[(i64, f64, bool)],
    queue_ahead: f64,
    fill_qty: f64,
    reaches_order_size: bool,
) -> Option<i64> {
    if reaches_order_size {
        completion_timestamp(evidence, queue_ahead, 0, fill_qty)
    } else {
        evidence.last().map(|(ts_ms, _, _)| *ts_ms)
    }
}

fn completion_timestamp(
    evidence: &[(i64, f64, bool)],
    queue_ahead: f64,
    required_through_prints: usize,
    threshold: f64,
) -> Option<i64> {
    let mut available = 0.0;
    let mut through_prints = 0usize;
    for (ts_ms, contribution, is_through) in evidence {
        available += contribution;
        if *is_through {
            through_prints += 1;
        }
        if through_prints >= required_through_prints
            && after_queue(available, queue_ahead) > 0.0
            && after_queue(available, queue_ahead) >= threshold
        {
            return Some(*ts_ms);
        }
    }
    None
}

fn outcome_from_available(
    order: &RestingOrder,
    profile: FillProfile,
    available: f64,
    desired_fraction: f64,
    fill_ts_ms: Option<i64>,
) -> FillOutcome {
    let order_size = valid_qty(order.size);
    let requested_qty = order_size * desired_fraction.min(1.0);
    let available = if available.is_nan() {
        0.0
    } else {
        available.max(0.0)
    };
    let fill_qty = requested_qty.min(available);
    let fill_fraction = if order_size == 0.0 {
        0.0
    } else {
        (fill_qty / order_size).clamp(0.0, 1.0)
    };
    FillOutcome {
        filled: fill_qty > 0.0,
        fill_fraction,
        fill_price: order.price,
        profile,
        fill_ts_ms: if fill_qty > 0.0 { fill_ts_ms } else { None },
        used_aggressive_qty: fill_qty,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn buy(price: f64, size: f64, from: i64) -> RestingOrder {
        RestingOrder {
            price,
            size,
            resting_from_ms: from,
            side_buy: true,
        }
    }

    #[test]
    fn single_touch_never_fills_conservative() {
        let sim = FillSimulator::new(FillProfile::Conservative);
        let out = sim.check_fill(
            &buy(0.43, 10.0, 1_000),
            &[(1_100, 0.43, 5.0)],
            ExecutionLatency::new(50),
        );
        assert!(!out.filled, "a touch must not fill conservative by itself");
        assert_eq!(out.fill_ts_ms, None);
        assert_eq!(out.used_aggressive_qty, 0.0);
    }

    #[test]
    fn optimistic_and_base_accept_directional_touch_volume() {
        let opt = FillSimulator::new(FillProfile::Optimistic);
        let out = opt.check_fill(
            &buy(0.43, 10.0, 1_000),
            &[FillPrint::new(1_100, 0.43, 1.0, Aggressor::Sell)],
            ExecutionLatency::new(50),
        );
        assert!(out.filled);
        assert_eq!(out.used_aggressive_qty, 0.5);

        let base = FillSimulator::new(FillProfile::Base);
        let out = base.check_fill(
            &buy(0.43, 10.0, 1_000),
            &[(1_100, 0.43, 1.0)],
            ExecutionLatency::new(50),
        );
        assert!(out.filled);
        assert!(out.fill_fraction < 1.0);
    }

    #[test]
    fn latency_hides_early_prints() {
        let sim = FillSimulator::new(FillProfile::Optimistic);
        let out = sim.check_fill(
            &buy(0.43, 10.0, 1_000),
            &[(1_010, 0.40, 50.0)],
            ExecutionLatency::new(50),
        );
        assert!(!out.filled, "print before resting+latency is invisible");
    }

    #[test]
    fn conservative_partial_then_full() {
        let sim = FillSimulator::new(FillProfile::Conservative);
        let partial = sim.check_fill(
            &buy(0.43, 10.0, 0),
            &[(100, 0.42, 15.0), (200, 0.42, 15.0)],
            ExecutionLatency::new(0),
        );
        assert!(partial.filled && partial.fill_fraction == 0.5);
        let full = sim.check_fill(
            &buy(0.43, 10.0, 0),
            &[(100, 0.42, 60.0), (200, 0.42, 60.0)],
            ExecutionLatency::new(0),
        );
        assert!(full.filled && full.fill_fraction == 1.0);
    }

    #[test]
    fn conservative_rejects_single_oversized_touch() {
        let sim = FillSimulator::new(FillProfile::Conservative);
        let out = sim.check_fill(
            &buy(0.43, 10.0, 0),
            &[(100, 0.40, 1_000.0)],
            ExecutionLatency::new(0),
        );
        assert!(!out.filled);
    }
}
