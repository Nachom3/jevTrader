//! Latency histograms, logging, and engine counters.

#[allow(dead_code)]
pub mod latency;

#[allow(dead_code)]
pub mod metrics;

#[allow(dead_code, unused_imports)]
pub use latency::{LatencySnapshot, LatencySpan, LatencyTracker};

#[allow(dead_code, unused_imports)]
pub use metrics::{EngineMetrics, EngineMetricsSnapshot};
