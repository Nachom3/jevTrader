//! Hot-path engine counters.
//!
//! [`EngineMetrics`] complements [`crate::telemetry::LatencyTracker`]: these
//! metrics count events, while the latency tracker records distributions.

use std::sync::atomic::{AtomicU64, Ordering};

/// Plain copy of the engine counter values at one instant.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EngineMetricsSnapshot {
    pub jev_calls: u64,
    pub jev_deadlines: u64,
    pub jev_stale_dropped: u64,
    pub quotes_proposed: u64,
    pub quotes_placed: u64,
    pub risk_blocks: u64,
    pub paper_fills: u64,
    pub storage_drops: u64,
}

/// Lock-free counters for hot-path engine events.
#[derive(Debug, Default)]
pub struct EngineMetrics {
    jev_calls: AtomicU64,
    jev_deadlines: AtomicU64,
    jev_stale_dropped: AtomicU64,
    quotes_proposed: AtomicU64,
    quotes_placed: AtomicU64,
    risk_blocks: AtomicU64,
    paper_fills: AtomicU64,
    storage_drops: AtomicU64,
}

impl EngineMetrics {
    /// Creates a zeroed counter set.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            jev_calls: AtomicU64::new(0),
            jev_deadlines: AtomicU64::new(0),
            jev_stale_dropped: AtomicU64::new(0),
            quotes_proposed: AtomicU64::new(0),
            quotes_placed: AtomicU64::new(0),
            risk_blocks: AtomicU64::new(0),
            paper_fills: AtomicU64::new(0),
            storage_drops: AtomicU64::new(0),
        }
    }

    pub fn incr_jev_calls(&self) {
        self.jev_calls.fetch_add(1, Ordering::Relaxed);
    }

    pub fn incr_jev_deadlines(&self) {
        self.jev_deadlines.fetch_add(1, Ordering::Relaxed);
    }

    pub fn incr_jev_stale_dropped(&self) {
        self.jev_stale_dropped.fetch_add(1, Ordering::Relaxed);
    }

    pub fn incr_quotes_proposed(&self) {
        self.quotes_proposed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn incr_quotes_placed(&self) {
        self.quotes_placed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn incr_risk_blocks(&self) {
        self.risk_blocks.fetch_add(1, Ordering::Relaxed);
    }

    pub fn incr_paper_fills(&self) {
        self.paper_fills.fetch_add(1, Ordering::Relaxed);
    }

    pub fn incr_storage_drops(&self) {
        self.storage_drops.fetch_add(1, Ordering::Relaxed);
    }

    /// Reads all counters using relaxed ordering appropriate for telemetry.
    #[must_use]
    pub fn snapshot(&self) -> EngineMetricsSnapshot {
        EngineMetricsSnapshot {
            jev_calls: self.jev_calls.load(Ordering::Relaxed),
            jev_deadlines: self.jev_deadlines.load(Ordering::Relaxed),
            jev_stale_dropped: self.jev_stale_dropped.load(Ordering::Relaxed),
            quotes_proposed: self.quotes_proposed.load(Ordering::Relaxed),
            quotes_placed: self.quotes_placed.load(Ordering::Relaxed),
            risk_blocks: self.risk_blocks.load(Ordering::Relaxed),
            paper_fills: self.paper_fills.load(Ordering::Relaxed),
            storage_drops: self.storage_drops.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn increments_are_visible_in_snapshot() {
        let metrics = EngineMetrics::new();
        metrics.incr_jev_calls();
        metrics.incr_jev_deadlines();
        metrics.incr_jev_stale_dropped();
        metrics.incr_quotes_proposed();
        metrics.incr_quotes_placed();
        metrics.incr_risk_blocks();
        metrics.incr_paper_fills();
        metrics.incr_storage_drops();

        assert_eq!(
            metrics.snapshot(),
            EngineMetricsSnapshot {
                jev_calls: 1,
                jev_deadlines: 1,
                jev_stale_dropped: 1,
                quotes_proposed: 1,
                quotes_placed: 1,
                risk_blocks: 1,
                paper_fills: 1,
                storage_drops: 1,
            }
        );
    }
}
