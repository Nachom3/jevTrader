//! Allocation-free-on-update rolling statistics.
//!
//! [`RollingWindow`] stores samples in a fixed-capacity `ringbuf`. A push evicts
//! the oldest value when the window is full and updates running first and second
//! moments, so mean and population variance are O(1). The variance is the
//! population variance (divide by `n`, not `n - 1`).

use ringbuf::{
    HeapRb,
    traits::{Consumer, Observer, Producer},
};

/// A fixed-capacity FIFO window of `f64` samples.
///
/// `new` requires a non-zero capacity. A newly-created window is empty: `mean`,
/// `variance`, and `last_n` return `None` or an empty vector until samples are
/// pushed. `push` never allocates after construction. `last_n` allocates its
/// returned `Vec` because a wrapped ring buffer cannot always be represented by
/// one borrowed slice; values are returned oldest-first.
pub struct RollingWindow {
    samples: HeapRb<f64>,
    sum: f64,
    sum_squares: f64,
}

impl RollingWindow {
    /// Creates an empty window with a fixed, non-zero capacity.
    ///
    /// # Panics
    ///
    /// Panics when `capacity` is zero because a zero-capacity rolling statistic
    /// cannot retain a sample.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "rolling window capacity must be non-zero");
        Self {
            samples: HeapRb::new(capacity),
            sum: 0.0,
            sum_squares: 0.0,
        }
    }

    /// Returns the configured maximum number of samples.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.samples.capacity().get()
    }

    /// Returns the number of samples currently retained.
    #[must_use]
    pub fn len(&self) -> usize {
        self.samples.occupied_len()
    }

    /// Returns whether no samples are retained.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.samples.is_empty()
    }

    /// Adds one sample, evicting the oldest sample when the window is full.
    ///
    /// The ring buffer and the running moments are updated without allocation.
    /// Callers must push finite samples: a NaN or infinite sample would poison
    /// the running moments for the whole window lifetime. This is enforced
    /// with `debug_assert` (zero release cost); upstream translators already
    /// reject non-finite venue values.
    pub fn push(&mut self, sample: f64) {
        debug_assert!(sample.is_finite(), "rolling window samples must be finite");
        if self.samples.is_full() {
            let evicted = self
                .samples
                .try_pop()
                .expect("a full rolling window must contain an oldest sample");
            self.sum -= evicted;
            self.sum_squares -= evicted * evicted;
        }

        self.samples
            .try_push(sample)
            .expect("a rolling window has room after optional eviction");
        self.sum += sample;
        self.sum_squares += sample * sample;
    }

    /// Returns the arithmetic mean, or `None` for an empty window.
    #[must_use]
    pub fn mean(&self) -> Option<f64> {
        (!self.is_empty()).then(|| self.sum / self.len() as f64)
    }

    /// Returns the population variance, or `None` for an empty window.
    ///
    /// Small floating-point round-off can make the algebraic result slightly
    /// negative for a constant window; such values are clamped to zero.
    #[must_use]
    pub fn variance(&self) -> Option<f64> {
        self.mean().map(|mean| {
            let variance = self.sum_squares / self.len() as f64 - mean * mean;
            variance.max(0.0)
        })
    }

    /// Copies up to `n` most recent samples, oldest-first.
    #[must_use]
    pub fn last_n(&self, n: usize) -> Vec<f64> {
        let skip = self.len().saturating_sub(n);
        self.samples.iter().skip(skip).copied().collect()
    }
}

/// Return calculation policy used by [`Returns`].
///
/// Returns are simple percentage returns, `(new / old - 1) * 100`, rather than
/// log returns. This matches the percentage-valued `LeadLagFeatures` fields and
/// keeps the sign and magnitude directly interpretable by the strategy layer.
#[derive(Debug, Clone, Copy, Default)]
pub struct Returns;

impl Returns {
    /// Computes the simple percentage return between two positive finite prices.
    ///
    /// A non-positive or non-finite input has no meaningful price return and
    /// yields `None` instead of panicking or producing an invalid feature.
    #[must_use]
    pub fn between(old_price: f64, new_price: f64) -> Option<f64> {
        (old_price.is_finite() && new_price.is_finite() && old_price > 0.0 && new_price > 0.0)
            .then(|| (new_price / old_price - 1.0) * 100.0)
    }

    /// Computes adjacent simple percentage returns for a price series.
    ///
    /// The returned vector is empty for fewer than two prices. This helper is a
    /// convenience for batch calculations; it is separate from
    /// [`RollingWindow::push`], which remains allocation-free on every update.
    #[must_use]
    pub fn simple(prices: &[f64]) -> Vec<f64> {
        prices
            .windows(2)
            .filter_map(|pair| Self::between(pair[0], pair[1]))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::{Returns, RollingWindow};

    fn naive_mean(values: &[f64]) -> f64 {
        values.iter().sum::<f64>() / values.len() as f64
    }

    fn naive_variance(values: &[f64]) -> f64 {
        let mean = naive_mean(values);
        values
            .iter()
            .map(|value| (value - mean).powi(2))
            .sum::<f64>()
            / values.len() as f64
    }

    #[test]
    fn mean_and_variance_match_naive_population_calculation() {
        let values = [1.0, 2.0, 4.0, 8.0];
        let mut window = RollingWindow::new(values.len());
        for value in values {
            window.push(value);
        }

        assert_eq!(window.mean(), Some(naive_mean(&values)));
        assert_eq!(window.variance(), Some(naive_variance(&values)));
    }

    #[test]
    fn full_window_evicts_oldest_sample_and_updates_moments() {
        let mut window = RollingWindow::new(3);
        for value in [1.0, 2.0, 3.0, 4.0] {
            window.push(value);
        }

        let retained = [2.0, 3.0, 4.0];
        assert_eq!(window.len(), 3);
        assert_eq!(window.last_n(10), retained);
        assert!((window.mean().unwrap() - naive_mean(&retained)).abs() < 1e-12);
        assert!((window.variance().unwrap() - naive_variance(&retained)).abs() < 1e-12);
    }

    #[test]
    fn empty_window_has_documented_defaults() {
        let window = RollingWindow::new(4);

        assert!(window.is_empty());
        assert_eq!(window.len(), 0);
        assert_eq!(window.mean(), None);
        assert_eq!(window.variance(), None);
        assert!(window.last_n(3).is_empty());
    }

    #[test]
    fn returns_are_simple_percentages_with_expected_signs() {
        assert!((Returns::between(100.0, 110.0).unwrap() - 10.0).abs() < 1e-12);
        assert!((Returns::between(100.0, 90.0).unwrap() + 10.0).abs() < 1e-12);
        let returns = Returns::simple(&[100.0, 110.0, 99.0]);
        assert!((returns[0] - 10.0).abs() < 1e-12);
        assert!((returns[1] + 10.0).abs() < 1e-12);
    }

    #[test]
    fn invalid_returns_are_ignored_by_batch_helper() {
        assert_eq!(Returns::between(0.0, 1.0), None);
        assert_eq!(Returns::simple(&[100.0, 0.0, 110.0]), Vec::<f64>::new());
    }
}
