//! Walk-forward planning and temporal leakage controls.
//!
//! [`WalkforwardWindow`] remains the index-based API used by the existing
//! replay runner. [`TemporalWindow`] is the timestamp-based planner for new
//! OOS consumers; the OOS caller must fit thresholds only from
//! [`SplitAssign::InSample`] rows and evaluate [`SplitAssign::OutOfSample`]
//! rows without feeding them back into calibration.

use serde::{Deserialize, Serialize};

use crate::config::QuoteThresholds;

use super::report::ReportRow;
use super::runner::{JevEvaluator, ReplayConfig, ReplayRunner, SyntheticItem};

/// One frozen walk-forward window over synthetic items.
#[derive(Debug, Clone)]
pub struct WalkforwardWindow {
    pub name: String,
    pub start_idx: usize,
    pub end_idx: usize,
}

/// A validated timestamp window.
///
/// Bounds are milliseconds since the Unix epoch. The train interval is
/// `[train_start_ms, train_end_ms)` and the test interval is
/// `[test_start_ms, test_end_ms)`. Integer timestamps are finite by
/// construction; all four must be positive, ordered, and satisfy
/// `train_end_ms <= test_start_ms`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TemporalWindow {
    pub name: String,
    pub train_start_ms: i64,
    pub train_end_ms: i64,
    pub test_start_ms: i64,
    pub test_end_ms: i64,
}

impl TemporalWindow {
    /// Validates an already-created window.
    pub fn validate(&self) -> Result<(), String> {
        if self.name.trim().is_empty() {
            return Err("window name must not be empty".to_owned());
        }
        if self.train_start_ms <= 0
            || self.train_end_ms <= 0
            || self.test_start_ms <= 0
            || self.test_end_ms <= 0
        {
            return Err("window timestamps must be positive".to_owned());
        }
        if self.train_start_ms > self.train_end_ms {
            return Err("train_start_ms must be <= train_end_ms".to_owned());
        }
        if self.test_start_ms > self.test_end_ms {
            return Err("test_start_ms must be <= test_end_ms".to_owned());
        }
        if self.train_end_ms > self.test_start_ms {
            return Err("train_end_ms must be <= test_start_ms".to_owned());
        }
        Ok(())
    }

    /// Creates a validated timestamp window.
    pub fn new(
        name: impl Into<String>,
        train_start_ms: i64,
        train_end_ms: i64,
        test_start_ms: i64,
        test_end_ms: i64,
    ) -> Result<Self, String> {
        let window = Self {
            name: name.into(),
            train_start_ms,
            train_end_ms,
            test_start_ms,
            test_end_ms,
        };
        window.validate().map(|()| window)
    }

    /// Alias for callers that prefer a fallible-constructor name.
    pub fn try_new(
        name: impl Into<String>,
        train_start_ms: i64,
        train_end_ms: i64,
        test_start_ms: i64,
        test_end_ms: i64,
    ) -> Result<Self, String> {
        Self::new(
            name,
            train_start_ms,
            train_end_ms,
            test_start_ms,
            test_end_ms,
        )
    }

    /// Returns which side of this window contains `ts_ms`.
    ///
    /// The gap between train and test, and timestamps outside the window, are
    /// intentionally unassigned. This keeps a caller from silently treating a
    /// gap as calibration data or OOS data.
    #[must_use]
    pub fn assign(&self, ts_ms: i64) -> Option<SplitAssign> {
        if ts_ms >= self.train_start_ms && ts_ms < self.train_end_ms {
            Some(SplitAssign::InSample)
        } else if ts_ms >= self.test_start_ms && ts_ms < self.test_end_ms {
            Some(SplitAssign::OutOfSample)
        } else {
            None
        }
    }

    /// Whether the next train start is too close to this test interval.
    #[must_use]
    pub fn requires_gap_ms(&self, next_train_start_ms: i64, embargo_ms: i64) -> bool {
        requires_gap_ms(self.test_end_ms, next_train_start_ms, embargo_ms)
    }
}

/// Assignment of a timestamp to the calibration or evaluation side.
///
/// This is deliberately separate from the older [`crate::replay::Split`]
/// enum. The temporal planner only has two responsibilities: in-sample
/// calibration and out-of-sample evaluation. A caller must not use
/// `OutOfSample` rows to fit thresholds, weights, or any other statistics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SplitAssign {
    InSample,
    OutOfSample,
}

/// Assignment of one timestamped replay item to a temporal window side.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TemporalAssignment {
    pub item_index: usize,
    pub assignment: SplitAssign,
}

/// Results from one timestamp-based out-of-sample evaluation.
#[derive(Debug, Clone)]
pub struct TemporalWindowResult {
    pub window: TemporalWindow,
    pub assignments: Vec<TemporalAssignment>,
    pub in_sample_count: usize,
    pub out_of_sample_count: usize,
    /// The config cloned for this window. Thresholds are fitted once from the
    /// in-sample items and then remain unchanged while its OOS rows run.
    pub config: ReplayConfig,
    pub rows: Vec<ReportRow>,
}

/// Plans deterministic rolling timestamp windows.
///
/// Each window starts at `first_ms + k * step_ms`, has a contiguous train/test
/// boundary, and is emitted only when its test end is at or before `last_ms`.
/// Invalid arguments are fail-closed and return an empty vector; callers that
/// need the reason should use [`try_plan_windows`].
#[must_use]
pub fn plan_windows(
    first_ms: i64,
    last_ms: i64,
    train_span_ms: i64,
    test_span_ms: i64,
    step_ms: i64,
) -> Vec<TemporalWindow> {
    try_plan_windows(first_ms, last_ms, train_span_ms, test_span_ms, step_ms).unwrap_or_default()
}

/// Fallible form of [`plan_windows`] with validation errors.
pub fn try_plan_windows(
    first_ms: i64,
    last_ms: i64,
    train_span_ms: i64,
    test_span_ms: i64,
    step_ms: i64,
) -> Result<Vec<TemporalWindow>, String> {
    if first_ms <= 0 || last_ms <= 0 {
        return Err("first_ms and last_ms must be positive".to_owned());
    }
    if first_ms > last_ms {
        return Err("first_ms must be <= last_ms".to_owned());
    }
    if train_span_ms <= 0 || test_span_ms <= 0 || step_ms <= 0 {
        return Err("train, test, and step spans must be positive".to_owned());
    }

    let mut windows = Vec::new();
    let mut train_start_ms = first_ms;
    let mut index = 0usize;
    while let Some(train_end_ms) = train_start_ms.checked_add(train_span_ms) {
        let Some(test_end_ms) = train_end_ms.checked_add(test_span_ms) else {
            break;
        };
        if test_end_ms > last_ms {
            break;
        }
        windows.push(TemporalWindow::new(
            format!("w{index:02}"),
            train_start_ms,
            train_end_ms,
            train_end_ms,
            test_end_ms,
        )?);
        index = index.saturating_add(1);
        let Some(next_start_ms) = train_start_ms.checked_add(step_ms) else {
            break;
        };
        if next_start_ms <= train_start_ms {
            break;
        }
        train_start_ms = next_start_ms;
    }
    Ok(windows)
}

/// A market's information interval, from first usable information to label
/// resolution, inclusive at both endpoints for overlap reasoning.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketSpan {
    pub condition_id: String,
    pub info_start_ms: i64,
    pub resolution_ms: i64,
}

impl MarketSpan {
    /// Creates a validated market information span.
    pub fn new(
        condition_id: impl Into<String>,
        info_start_ms: i64,
        resolution_ms: i64,
    ) -> Result<Self, String> {
        let span = Self {
            condition_id: condition_id.into(),
            info_start_ms,
            resolution_ms,
        };
        span.validate().map(|()| span)
    }

    /// Validates positive, chronological information timestamps.
    pub fn validate(&self) -> Result<(), String> {
        if self.condition_id.trim().is_empty() {
            return Err("condition_id must not be empty".to_owned());
        }
        if self.info_start_ms <= 0 || self.resolution_ms <= 0 {
            return Err("market timestamps must be positive".to_owned());
        }
        if self.info_start_ms > self.resolution_ms {
            return Err("info_start_ms must be <= resolution_ms".to_owned());
        }
        Ok(())
    }
}

/// Removes training markets whose labels arrive inside the purge zone.
///
/// The retained training rule is `resolution_ms <= train_end_ms - purge_ms`.
/// Equality is retained; only labels strictly after the cutoff are dropped.
/// For example, with `train_end_ms = 1_000` and `purge_ms = 100`, the cutoff
/// is `900`: a market resolving at `900` remains in TRAIN, while one resolving
/// at `950` is purged. This rule never filters the TEST labels themselves.
///
/// Invalid bounds fail closed by returning every input market in `dropped`.
#[must_use]
pub fn purge_train(
    markets: &[MarketSpan],
    train_end_ms: i64,
    purge_ms: i64,
) -> (Vec<MarketSpan>, Vec<MarketSpan>) {
    try_purge_train(markets, train_end_ms, purge_ms)
        .unwrap_or_else(|_| (Vec::new(), markets.to_vec()))
}

/// Fallible form of [`purge_train`].
pub fn try_purge_train(
    markets: &[MarketSpan],
    train_end_ms: i64,
    purge_ms: i64,
) -> Result<(Vec<MarketSpan>, Vec<MarketSpan>), String> {
    if train_end_ms <= 0 || purge_ms < 0 {
        return Err("train_end_ms must be positive and purge_ms non-negative".to_owned());
    }
    let cutoff = train_end_ms
        .checked_sub(purge_ms)
        .ok_or_else(|| "purge_ms underflows the timestamp range".to_owned())?;
    let mut kept = Vec::with_capacity(markets.len());
    let mut dropped = Vec::new();
    for market in markets {
        if market.validate().is_err() || market.resolution_ms > cutoff {
            dropped.push(market.clone());
        } else {
            kept.push(market.clone());
        }
    }
    Ok((kept, dropped))
}

/// Applies purge and then removes training candidates that overlap the
/// embargo zone immediately after the test window.
///
/// This compatibility helper keeps the original task-shaped API, but its
/// inputs are **training candidates**, not the test set. Purge still uses
/// `test_start_ms` as the train/test cut, so test labels are never removed by
/// this helper. The embargo zone is half-open:
/// `[test_end_ms, test_end_ms + embargo_ms)`. A candidate that starts before
/// the zone but spans into it is also removed. A zero-length embargo leaves
/// the purge result unchanged.
#[must_use]
pub fn apply_purge_embargo(
    markets: &[MarketSpan],
    test_start_ms: i64,
    test_end_ms: i64,
    purge_ms: i64,
    embargo_ms: i64,
) -> (Vec<MarketSpan>, Vec<MarketSpan>) {
    if test_start_ms <= 0
        || test_end_ms <= 0
        || test_end_ms < test_start_ms
        || purge_ms < 0
        || embargo_ms < 0
    {
        return (Vec::new(), markets.to_vec());
    }

    let (purged, mut dropped) = purge_train(markets, test_start_ms, purge_ms);
    if embargo_ms == 0 {
        return (purged, dropped);
    }
    let Some(embargo_end_ms) = test_end_ms.checked_add(embargo_ms) else {
        return (Vec::new(), markets.to_vec());
    };

    let mut kept = Vec::with_capacity(purged.len());
    for market in purged {
        if overlaps_embargo_zone(&market, test_end_ms, embargo_end_ms, embargo_ms) {
            dropped.push(market);
        } else {
            kept.push(market);
        }
    }
    (kept, dropped)
}

fn overlaps_embargo_zone(
    market: &MarketSpan,
    test_end_ms: i64,
    embargo_end_ms: i64,
    embargo_ms: i64,
) -> bool {
    if embargo_ms == 0 {
        return false;
    }

    let starts_in_zone = market.info_start_ms >= test_end_ms
        && market.info_start_ms < embargo_end_ms
        && requires_gap_ms(test_end_ms, market.info_start_ms, embargo_ms);
    let crosses_zone_start = market.info_start_ms < test_end_ms
        && market.resolution_ms >= test_end_ms
        && market.info_start_ms < embargo_end_ms;
    starts_in_zone || crosses_zone_start
}

/// Returns the actual gap between a prior test end and the next train start.
#[must_use]
pub fn gap_ms(previous_test_end_ms: i64, next_train_start_ms: i64) -> Option<i64> {
    next_train_start_ms.checked_sub(previous_test_end_ms)
}

/// Returns true when consecutive windows satisfy the requested embargo.
#[must_use]
pub fn embargo_after_test(
    previous_test_end_ms: i64,
    next_train_start_ms: i64,
    embargo_ms: i64,
) -> bool {
    if previous_test_end_ms <= 0 || next_train_start_ms <= 0 || embargo_ms < 0 {
        return false;
    }
    gap_ms(previous_test_end_ms, next_train_start_ms).is_some_and(|gap| gap >= embargo_ms)
}

/// Returns true when a caller must insert or mark an embargo gap.
///
/// In particular, adjacent windows with `next_train_start_ms -
/// previous_test_end_ms < embargo_ms` return `true`. The OOS runner should
/// not use the prior TEST rows as the next TRAIN sample until this condition
/// is false.
#[must_use]
pub fn requires_gap_ms(
    previous_test_end_ms: i64,
    next_train_start_ms: i64,
    embargo_ms: i64,
) -> bool {
    !embargo_after_test(previous_test_end_ms, next_train_start_ms, embargo_ms)
}

/// Marks each adjacent pair that violates an embargo.
#[must_use]
pub fn embargo_flags(windows: &[TemporalWindow], embargo_ms: i64) -> Vec<bool> {
    windows
        .windows(2)
        .map(|pair| pair[0].requires_gap_ms(pair[1].train_start_ms, embargo_ms))
        .collect()
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

    /// Evaluates each index-based window with the SAME frozen config and
    /// returns per-window rows. No threshold is ever re-tuned on a later
    /// window. New timestamp-based callers should use [`TemporalWindow`] and
    /// assign rows with [`TemporalWindow::assign`] before calibration.
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

    /// Evaluates timestamp-based windows without allowing OOS feedback into
    /// calibration.
    ///
    /// `fit_thresholds` receives only the items assigned to the current
    /// window's [`SplitAssign::InSample`] side. It is called once per valid
    /// window. Its returned thresholds are installed into a clone of the
    /// runner's base config, and that clone is frozen for the OOS replay. OOS
    /// items are never passed to the fitter. Items in a temporal gap or
    /// outside a window are not assigned or evaluated.
    pub fn run_temporal_windows<F>(
        &mut self,
        items: &[SyntheticItem],
        windows: &[TemporalWindow],
        resolution_at_ms: i64,
        mut fit_thresholds: F,
    ) -> Vec<TemporalWindowResult>
    where
        F: FnMut(&[SyntheticItem], &ReplayConfig) -> QuoteThresholds,
    {
        let base_config = self.config.clone();
        let mut results = Vec::new();

        for window in windows {
            if window.validate().is_err() {
                continue;
            }

            let mut assignments = Vec::new();
            let mut in_sample = Vec::new();
            let mut out_of_sample = Vec::new();
            for (item_index, item) in items.iter().enumerate() {
                let Some(assignment) = window.assign(item.ts_ms) else {
                    continue;
                };
                assignments.push(TemporalAssignment {
                    item_index,
                    assignment,
                });
                match assignment {
                    SplitAssign::InSample => in_sample.push(item.clone()),
                    SplitAssign::OutOfSample => out_of_sample.push(item.clone()),
                }
            }

            let thresholds = fit_thresholds(&in_sample, &base_config);
            let mut config = base_config.clone();
            config.thresholds = thresholds;
            let mut runner = ReplayRunner::new(config.clone(), self.evaluator.clone());
            let evaluation = runner.run_synthetic(&out_of_sample, resolution_at_ms);
            results.push(TemporalWindowResult {
                window: window.clone(),
                assignments,
                in_sample_count: in_sample.len(),
                out_of_sample_count: out_of_sample.len(),
                config,
                rows: evaluation.rows,
            });
        }
        results
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
