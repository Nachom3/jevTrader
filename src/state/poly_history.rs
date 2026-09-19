//! Pure Polymarket mid-price history for the Jev lead-lag snapshot.
//!
//! The buffer stores timestamped YES mids supplied by the caller. Historical
//! values use a last-known-at-or-before-target rule: for `as_of - horizon`, the
//! newest sample that is not newer than that target is selected. This avoids
//! inventing a price by extrapolation or by looking into the future. A target
//! before the oldest retained sample is unavailable (`None`).
//!
//! [`PolyHistory::apply_to_snapshot`] adapts that explicit availability to the
//! existing `PolySnapshot` wire shape. Its documented compatibility default is
//! price zero for a missing historical value; callers that need to distinguish
//! a real zero from missing data should retain the returned [`PolyHistoricalPrices`]
//! alongside the snapshot.

use crate::strategy::lead_lag::PolySnapshot;
use jevtrader::domain::PriceTicks;
use std::collections::VecDeque;

/// One observed Polymarket YES mid-price.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PolyMid {
    pub ts_ms: u64,
    pub mid: PriceTicks,
}

/// Historical prices requested by the V1 snapshot.
///
/// `None` means the buffer had no sample at or before the requested historical
/// timestamp. No value is extrapolated across the beginning of the buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PolyHistoricalPrices {
    pub price_1s_ago: Option<PriceTicks>,
    pub price_5s_ago: Option<PriceTicks>,
    pub price_30s_ago: Option<PriceTicks>,
}

/// Fixed-capacity, timestamp-ordered Polymarket mid-price history.
#[derive(Debug, Clone)]
pub struct PolyHistory {
    samples: VecDeque<PolyMid>,
    capacity: usize,
}

impl PolyHistory {
    /// Creates an empty history. A zero capacity is normalized to one sample so
    /// callers cannot create a buffer that silently drops every observation.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        let capacity = capacity.max(1);
        Self {
            samples: VecDeque::with_capacity(capacity),
            capacity,
        }
    }

    /// Maximum number of mids retained by this history.
    #[must_use]
    pub const fn capacity(&self) -> usize {
        self.capacity
    }

    /// Number of mids currently retained.
    #[must_use]
    pub fn len(&self) -> usize {
        self.samples.len()
    }

    /// Whether no mids have been observed.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.samples.is_empty()
    }

    /// Adds or replaces one timestamped mid.
    ///
    /// Samples are kept ordered even if a caller delivers a late observation.
    /// A repeated timestamp replaces the prior value, which prevents duplicate
    /// observations from changing the last-known lookup.
    pub fn push(&mut self, ts_ms: u64, mid: PriceTicks) {
        let sample = PolyMid { ts_ms, mid };
        let index = self
            .samples
            .iter()
            .position(|existing| existing.ts_ms >= ts_ms);

        match index {
            Some(index) if self.samples[index].ts_ms == ts_ms => self.samples[index] = sample,
            Some(index) => self.samples.insert(index, sample),
            None => self.samples.push_back(sample),
        }

        while self.samples.len() > self.capacity {
            self.samples.pop_front();
        }
    }

    /// Returns the latest observed mid at or before `target_ts_ms`.
    #[must_use]
    pub fn last_known_at_or_before(&self, target_ts_ms: u64) -> Option<PriceTicks> {
        self.samples
            .iter()
            .rev()
            .find(|sample| sample.ts_ms <= target_ts_ms)
            .map(|sample| sample.mid)
    }

    /// Resolves the three historical prices relative to an observation time.
    #[must_use]
    pub fn historical_prices(&self, as_of_ts_ms: u64) -> PolyHistoricalPrices {
        PolyHistoricalPrices {
            price_1s_ago: self.price_1s_ago(as_of_ts_ms),
            price_5s_ago: self.price_5s_ago(as_of_ts_ms),
            price_30s_ago: self.price_30s_ago(as_of_ts_ms),
        }
    }

    /// Returns the last-known mid one second before the observation.
    #[must_use]
    pub fn price_1s_ago(&self, as_of_ts_ms: u64) -> Option<PriceTicks> {
        self.price_ago(as_of_ts_ms, 1_000)
    }

    /// Returns the last-known mid five seconds before the observation.
    #[must_use]
    pub fn price_5s_ago(&self, as_of_ts_ms: u64) -> Option<PriceTicks> {
        self.price_ago(as_of_ts_ms, 5_000)
    }

    /// Returns the last-known mid thirty seconds before the observation.
    #[must_use]
    pub fn price_30s_ago(&self, as_of_ts_ms: u64) -> Option<PriceTicks> {
        self.price_ago(as_of_ts_ms, 30_000)
    }

    /// Returns the last-known price at a fixed millisecond horizon.
    #[must_use]
    pub fn price_ago(&self, as_of_ts_ms: u64, horizon_ms: u64) -> Option<PriceTicks> {
        let target_ts_ms = as_of_ts_ms.saturating_sub(horizon_ms);
        self.last_known_at_or_before(target_ts_ms)
    }

    /// Copies historical values into the existing `PolySnapshot` shape.
    ///
    /// The return value preserves availability. The snapshot itself uses
    /// `0.0` only for missing values because changing the established
    /// `PriceTicks` fields to `Option<PriceTicks>` would break existing state
    /// constructors and wire consumers. A zero historical field therefore
    /// requires the returned availability object for an unambiguous reading.
    pub fn apply_to_snapshot(
        &self,
        as_of_ts_ms: u64,
        snapshot: &mut PolySnapshot,
    ) -> PolyHistoricalPrices {
        let historical = self.historical_prices(as_of_ts_ms);
        let unavailable = PriceTicks::from_f64(0.0);
        snapshot.price_1s_ago = historical.price_1s_ago.unwrap_or(unavailable);
        snapshot.price_5s_ago = historical.price_5s_ago.unwrap_or(unavailable);
        snapshot.price_30s_ago = historical.price_30s_ago.unwrap_or(unavailable);
        historical
    }
}

impl Default for PolyHistory {
    fn default() -> Self {
        Self::new(1)
    }
}

#[cfg(test)]
mod tests {
    use super::{PolyHistoricalPrices, PolyHistory};
    use crate::strategy::lead_lag::PolySnapshot;
    use jevtrader::domain::PriceTicks;

    fn price(value: f64) -> PriceTicks {
        PriceTicks::from_f64(value)
    }

    fn snapshot() -> PolySnapshot {
        PolySnapshot {
            yes_bid: price(0.40),
            yes_ask: price(0.42),
            bid_depth: 10.0,
            ask_depth: 10.0,
            spread: 0.02,
            book_imbalance: 0.0,
            last_trade_price: price(0.41),
            price_1s_ago: price(0.99),
            price_5s_ago: price(0.99),
            price_30s_ago: price(0.99),
        }
    }

    #[test]
    fn last_known_history_feeds_requested_horizons() {
        let mut history = PolyHistory::new(16);
        history.push(1_000, price(0.40));
        history.push(2_000, price(0.41));
        history.push(5_000, price(0.45));

        let prices = history.historical_prices(5_000);

        assert_eq!(prices.price_1s_ago, Some(price(0.41)));
        assert_eq!(prices.price_5s_ago, None);
        assert_eq!(prices.price_30s_ago, None);
    }

    #[test]
    fn gaps_use_last_known_sample_without_future_extrapolation() {
        let mut history = PolyHistory::new(16);
        history.push(1_000, price(0.40));
        history.push(10_000, price(0.50));

        assert_eq!(history.price_ago(9_000, 1_000), Some(price(0.40)));
        assert_eq!(history.price_ago(500, 1_000), None);
    }

    #[test]
    fn missing_history_uses_documented_snapshot_default_and_reports_unavailable() {
        let history = PolyHistory::new(4);
        let mut snapshot = snapshot();

        let prices = history.apply_to_snapshot(30_000, &mut snapshot);

        assert_eq!(
            prices,
            PolyHistoricalPrices {
                price_1s_ago: None,
                price_5s_ago: None,
                price_30s_ago: None,
            }
        );
        assert_eq!(snapshot.price_1s_ago, price(0.0));
        assert_eq!(snapshot.price_5s_ago, price(0.0));
        assert_eq!(snapshot.price_30s_ago, price(0.0));
    }

    #[test]
    fn late_samples_are_ordered_and_duplicate_timestamps_replace() {
        let mut history = PolyHistory::new(4);
        history.push(3_000, price(0.43));
        history.push(1_000, price(0.41));
        history.push(1_000, price(0.42));

        assert_eq!(history.last_known_at_or_before(1_000), Some(price(0.42)));
        assert_eq!(history.last_known_at_or_before(2_000), Some(price(0.42)));
    }
}
