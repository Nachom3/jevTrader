//! Walk-forward runner: window A -> next period, thresholds frozen.
//!
//! Thresholds/config freeze before each test window. Selecting the best
//! threshold by looking at the future is forbidden; this runner enforces it
//! by taking one frozen config and evaluating strictly forward windows.

use super::report::ReportRow;
use super::runner::{JevEvaluator, ReplayConfig, ReplayRunner, SyntheticItem};

/// One frozen walk-forward window over synthetic items.
#[derive(Debug, Clone)]
pub struct WalkforwardWindow {
    pub name: String,
    pub start_idx: usize,
    pub end_idx: usize,
}

/// Walk-forward evaluation across non-overlapping forward windows.
pub struct WalkforwardRunner<E: JevEvaluator + Clone> {
    pub config: ReplayConfig,
    pub evaluator: E,
}

impl<E: JevEvaluator + Clone> WalkforwardRunner<E> {
    #[must_use]
    pub fn new(config: ReplayConfig, evaluator: E) -> Self {
        Self { config, evaluator }
    }

    /// Evaluates each window with the SAME frozen config and returns
    /// per-window rows. No threshold is ever re-tuned on a later window.
    pub fn run_windows(
        &mut self,
        items: &[SyntheticItem],
        windows: &[WalkforwardWindow],
        resolution_at_ms: i64,
    ) -> Vec<(String, Vec<ReportRow>)> {
        let mut out = Vec::new();
        for w in windows {
            let slice: Vec<SyntheticItem> = items
                .iter()
                .enumerate()
                .filter(|(i, _)| *i >= w.start_idx && *i < w.end_idx)
                .map(|(_, it)| it.clone())
                .collect();
            let mut runner = ReplayRunner::new(self.config.clone(), self.evaluator.clone());
            let result = runner.run_synthetic(&slice, resolution_at_ms);
            out.push((w.name.clone(), result.rows));
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replay::runner::StubJev;

    #[test]
    fn frozen_config_across_windows() {
        let cfg = ReplayConfig::smoke("wf-test");
        let runner = WalkforwardRunner::new(cfg.clone(), StubJev::new(1));
        assert_eq!(runner.config.thresholds, cfg.thresholds);
        assert_eq!(runner.config.max_pairs, cfg.max_pairs);
    }
}
