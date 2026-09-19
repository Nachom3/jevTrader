//! Conservative tape fill models: OPTIMISTIC / BASE / CONSERVATIVE.
//!
//! The historical tape is mostly trades, not full queue snapshots, so an
//! exact fill can never be claimed. Every fill records its `fill_model`.
//!
//! - OPTIMISTIC: any trade at or through the quote price fills.
//! - BASE: requires one trade through the level plus resting latency.
//! - CONSERVATIVE: requires sustained volume through the level AFTER the
//!   order is resting (execution latency honored), a queue-ahead proxy, and
//!   partial fills; a single `last_trade == quote` never fills.

use super::types::FillProfile;

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
}

/// Tape-fill simulator with explicit queue-ahead proxy.
pub struct FillSimulator {
    pub profile: FillProfile,
    /// Shares assumed ahead of us in queue (0..1 of level volume).
    pub queue_ahead: f64,
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
        }
    }

    /// Decides a fill for a resting BUY at `order.price` given subsequent
    /// tape prints strictly after `resting_from_ms + latency`.
    ///
    /// `prints` are `(ts_ms, price, size)` in event-time order. Only prints
    /// with `ts_ms >= eligible_from_ms` are considered (execution latency).
    /// Partial fills scale with through-volume net of the queue proxy.
    pub fn check_fill(
        &self,
        order: &RestingOrder,
        prints: &[(i64, f64, f64)],
        latency: ExecutionLatency,
    ) -> FillOutcome {
        let eligible_from = order.resting_from_ms + latency.submit_ms as i64;
        let mut through_vol = 0.0;
        let mut touched = false;
        // Distinct prints strictly through the level (below for a BUY).
        // The conservative model requires sustained evidence, never one print.
        let mut through_prints = 0usize;
        for (ts, price, size) in prints {
            if *ts < eligible_from {
                continue;
            }
            if *price <= order.price {
                touched = true;
                if *price < order.price {
                    through_vol += size.max(0.0);
                    through_prints += 1;
                } else {
                    through_vol += size.max(0.0) * 0.5;
                }
            }
        }
        match self.profile {
            FillProfile::Optimistic => {
                if touched {
                    FillOutcome {
                        filled: true,
                        fill_fraction: 1.0,
                        fill_price: order.price,
                        profile: self.profile,
                    }
                } else {
                    FillOutcome {
                        filled: false,
                        fill_fraction: 0.0,
                        fill_price: order.price,
                        profile: self.profile,
                    }
                }
            }
            FillProfile::Base => {
                if through_vol > 0.0 {
                    let frac = (through_vol / order.size.max(1e-9)).min(1.0);
                    FillOutcome {
                        filled: frac > 0.0,
                        fill_fraction: frac,
                        fill_price: order.price,
                        profile: self.profile,
                    }
                } else {
                    FillOutcome {
                        filled: false,
                        fill_fraction: 0.0,
                        fill_price: order.price,
                        profile: self.profile,
                    }
                }
            }
            FillProfile::Conservative => {
                // Queue proxy: only volume beyond the ahead-queue counts.
                // At least two distinct through-level prints are required:
                // one oversized touch is never enough evidence.
                if through_prints < 2 {
                    return FillOutcome {
                        filled: false,
                        fill_fraction: 0.0,
                        fill_price: order.price,
                        profile: self.profile,
                    };
                }
                let effective = through_vol * (1.0 - self.queue_ahead);
                // Requires at least 2x order size through the level.
                if effective >= order.size.max(1e-9) * 2.0 {
                    FillOutcome {
                        filled: true,
                        fill_fraction: 1.0,
                        fill_price: order.price,
                        profile: self.profile,
                    }
                } else if effective >= order.size.max(1e-9) * 0.5 {
                    FillOutcome {
                        filled: true,
                        fill_fraction: 0.5,
                        fill_price: order.price,
                        profile: self.profile,
                    }
                } else {
                    FillOutcome {
                        filled: false,
                        fill_fraction: 0.0,
                        fill_price: order.price,
                        profile: self.profile,
                    }
                }
            }
        }
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
        assert!(!out.filled, "last_trade == quote must not fill conserved");
    }

    #[test]
    fn optimistic_fills_on_touch_base_needs_through() {
        let opt = FillSimulator::new(FillProfile::Optimistic);
        let out = opt.check_fill(
            &buy(0.43, 10.0, 1_000),
            &[(1_100, 0.43, 1.0)],
            ExecutionLatency::new(50),
        );
        assert!(out.filled);
        let base = FillSimulator::new(FillProfile::Base);
        let out = base.check_fill(
            &buy(0.43, 10.0, 1_000),
            &[(1_100, 0.43, 1.0)],
            ExecutionLatency::new(50),
        );
        // Touch at level counts half volume -> partial fill in BASE.
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
