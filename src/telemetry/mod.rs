//! Latency histograms and logging.

#[allow(dead_code)]
pub mod latency;

#[allow(dead_code, unused_imports)]
pub use latency::{LatencySnapshot, LatencySpan, LatencyTracker};
