use std::sync::Mutex;

use hdrhistogram::Histogram;

const SIGNIFICANT_DIGITS: u8 = 3;
const SPAN_COUNT: usize = 9;

/// A latency interval measured by the trading pipeline.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum LatencySpan {
    WsReceiveToState,
    StateToJevRequest,
    JevRequestToResponse,
    JevTotal,
    ResponseToDecision,
    DecisionToSubmit,
    SubmitToAck,
    CancelToAck,
    SignalToQuote,
}

impl LatencySpan {
    const fn index(self) -> usize {
        match self {
            Self::WsReceiveToState => 0,
            Self::StateToJevRequest => 1,
            Self::JevRequestToResponse => 2,
            Self::JevTotal => 3,
            Self::ResponseToDecision => 4,
            Self::DecisionToSubmit => 5,
            Self::SubmitToAck => 6,
            Self::CancelToAck => 7,
            Self::SignalToQuote => 8,
        }
    }
}

/// Percentiles for one latency span, expressed in milliseconds.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct LatencySnapshot {
    pub p50_ms: f64,
    pub p90_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub p99_9_ms: f64,
}

/// Best-effort latency histograms for the pipeline.
///
/// Each span has its own mutex because `hdrhistogram::Histogram` is mutable and
/// hdrhistogram 7.6 does not provide an atomic/concurrent histogram. `record`
/// uses `try_lock` rather than waiting: a contended telemetry sample is dropped,
/// so telemetry can never block the trading path. Snapshots are allowed to take
/// the normal lock because they run off the hot path. A sample can therefore be
/// lost only when its span is contended (or cannot be represented by the
/// histogram), and callers should treat snapshots as best-effort measurements.
pub struct LatencyTracker {
    histograms: [Mutex<Histogram<u64>>; SPAN_COUNT],
}

impl LatencyTracker {
    /// Create an empty tracker with one histogram per latency span.
    pub fn new() -> Self {
        Self {
            histograms: std::array::from_fn(|_| {
                Mutex::new(
                    Histogram::new(SIGNIFICANT_DIGITS)
                        .expect("latency histogram configuration is valid"),
                )
            }),
        }
    }

    /// Record a nanosecond latency without waiting for telemetry synchronization.
    pub fn record(&self, span: LatencySpan, nanos: u64) {
        let Ok(mut histogram) = self.histograms[span.index()].try_lock() else {
            return;
        };

        let _ = histogram.record(nanos);
    }

    /// Return latency percentiles in milliseconds, or `None` when no sample exists.
    pub fn snapshot(&self, span: LatencySpan) -> Option<LatencySnapshot> {
        let histogram = match self.histograms[span.index()].lock() {
            Ok(histogram) => histogram,
            Err(poisoned) => poisoned.into_inner(),
        };

        if histogram.is_empty() {
            return None;
        }

        Some(LatencySnapshot {
            p50_ms: nanos_to_millis(histogram.value_at_quantile(0.50)),
            p90_ms: nanos_to_millis(histogram.value_at_quantile(0.90)),
            p95_ms: nanos_to_millis(histogram.value_at_quantile(0.95)),
            p99_ms: nanos_to_millis(histogram.value_at_quantile(0.99)),
            p99_9_ms: nanos_to_millis(histogram.value_at_quantile(0.999)),
        })
    }
}

impl Default for LatencyTracker {
    fn default() -> Self {
        Self::new()
    }
}

fn nanos_to_millis(nanos: u64) -> f64 {
    nanos as f64 / 1_000_000.0
}

#[cfg(test)]
mod tests {
    use super::{LatencySpan, LatencyTracker};
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn snapshot_returns_ordered_percentiles_for_known_distribution() {
        let tracker = LatencyTracker::new();
        for millis in 1..=100 {
            tracker.record(LatencySpan::JevTotal, millis * 1_000_000);
        }

        let snapshot = tracker
            .snapshot(LatencySpan::JevTotal)
            .expect("known distribution should have a snapshot");

        assert!(snapshot.p50_ms <= snapshot.p90_ms);
        assert!(snapshot.p90_ms <= snapshot.p95_ms);
        assert!(snapshot.p95_ms <= snapshot.p99_ms);
        assert!(snapshot.p99_ms <= snapshot.p99_9_ms);
        assert!((snapshot.p50_ms - 50.0).abs() < 1.0);
        assert!((snapshot.p99_9_ms - 100.0).abs() < 1.0);
    }

    #[test]
    fn empty_span_returns_none() {
        let tracker = LatencyTracker::new();

        assert!(tracker.snapshot(LatencySpan::SignalToQuote).is_none());
    }

    #[test]
    fn concurrent_recording_is_best_effort_and_does_not_panic() {
        let tracker = Arc::new(LatencyTracker::new());
        let mut handles = Vec::new();

        for thread_id in 0..4 {
            let tracker = Arc::clone(&tracker);
            handles.push(thread::spawn(move || {
                for sample in 0..1_000 {
                    tracker.record(
                        LatencySpan::WsReceiveToState,
                        (thread_id * 1_000 + sample + 1) as u64,
                    );
                }
            }));
        }

        for handle in handles {
            handle
                .join()
                .expect("concurrent recording should not panic");
        }

        let snapshot = tracker.snapshot(LatencySpan::WsReceiveToState);
        assert!(
            snapshot.is_some(),
            "at least one uncontended sample is expected"
        );
    }
}
