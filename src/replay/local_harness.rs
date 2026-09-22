//! Deterministic, offline phase-2 replay over precomputed Jev evaluations.

use crate::config::QuoteThresholds;
use crate::domain::TickSize;
use crate::jev::response::{
    JevEvaluation, SystemOneResponse, TickDistribution, V1Signal, parse_v1_signal,
};
use crate::polymarket::OrderBook;
use crate::replay::jev_cache::VersionPins;
use crate::strategy::lead_lag::should_quote;
use crate::strategy::quote::{QuoteIntent, decide_quote};
use crate::strategy::risk::{RiskGate, RiskLimits};
use arrow::array::{
    Array, BooleanArray, Float64Array, Int64Array, StringArray, UInt32Array, UInt64Array,
};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::Serialize;
use std::collections::BTreeMap;
use std::fs::File;
use std::path::Path;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvaluationReadPolicy {
    pub allow_live: bool,
    pub allow_stub: bool,
    pub expected_pins: Option<VersionPins>,
}
impl Default for EvaluationReadPolicy {
    fn default() -> Self {
        Self {
            allow_live: true,
            allow_stub: true,
            expected_pins: None,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct ReadCounts {
    pub n_rows: usize,
    pub n_usable: usize,
    pub n_skipped_error: usize,
    pub n_skipped_mode: usize,
    pub n_skipped_version: usize,
    pub n_skipped_malformed: usize,
}
impl ReadCounts {
    #[must_use]
    pub fn n_skipped(&self) -> usize {
        self.n_rows.saturating_sub(self.n_usable)
    }
}

#[derive(Debug, Clone)]
pub struct EvaluationRow {
    pub state_ts_ms: i64,
    pub state_hash: String,
    pub pins: VersionPins,
    pub evaluation: JevEvaluation,
    pub observed_latency_ms: u64,
    pub attempt_count: u32,
    pub error: Option<String>,
    pub trigger: String,
    pub split: String,
    pub fidelity: String,
    pub live: bool,
}
#[derive(Debug, Clone, Default)]
pub struct EvaluationDataset {
    pub rows: Vec<EvaluationRow>,
    pub counts: ReadCounts,
}

type Skip = Result<EvaluationRow, SkipReason>;
#[derive(Debug, Clone, Copy)]
enum SkipReason {
    Error,
    Mode,
    Version,
    Malformed,
}

pub fn read_evaluations<P: AsRef<Path>>(
    path: P,
    policy: EvaluationReadPolicy,
) -> Result<EvaluationDataset, String> {
    let path = path.as_ref();
    let reader = ParquetRecordBatchReaderBuilder::try_new(
        File::open(path).map_err(|e| format!("{}: {e}", path.display()))?,
    )
    .map_err(|e| format!("{}: {e}", path.display()))?
    .with_batch_size(8192)
    .build()
    .map_err(|e| format!("{}: {e}", path.display()))?;
    let mut out = EvaluationDataset::default();
    for batch in reader {
        let batch = batch.map_err(|e| format!("{}: {e}", path.display()))?;
        for row in 0..batch.num_rows() {
            out.counts.n_rows += 1;
            match read_row(&batch, row, &policy) {
                Ok(value) => {
                    out.counts.n_usable += 1;
                    out.rows.push(value);
                }
                Err(SkipReason::Error) => out.counts.n_skipped_error += 1,
                Err(SkipReason::Mode) => out.counts.n_skipped_mode += 1,
                Err(SkipReason::Version) => out.counts.n_skipped_version += 1,
                Err(SkipReason::Malformed) => out.counts.n_skipped_malformed += 1,
            }
        }
    }
    Ok(out)
}

fn read_row(batch: &RecordBatch, row: usize, policy: &EvaluationReadPolicy) -> Skip {
    let error = opt_text(batch, "error", row).map_err(|_| SkipReason::Malformed)?;
    if error.as_deref().is_some_and(|v| !v.trim().is_empty()) {
        return Err(SkipReason::Error);
    }
    let live = bool_at(batch, "live", row).map_err(|_| SkipReason::Malformed)?;
    if (live && !policy.allow_live) || (!live && !policy.allow_stub) {
        return Err(SkipReason::Mode);
    }
    let pins = pins_at(batch, row).map_err(|_| SkipReason::Malformed)?;
    if policy.expected_pins.as_ref().is_some_and(|p| p != &pins) {
        return Err(SkipReason::Version);
    }
    let ts = i64_at(batch, "timestamp", row).map_err(|_| SkipReason::Malformed)?;
    let latency = u64_at(batch, "observed_latency_ms", row).map_err(|_| SkipReason::Malformed)?;
    let state_hash = text_at(batch, "state_hash", row).map_err(|_| SkipReason::Malformed)?;
    let signal = signal_at(batch, row).map_err(|_| SkipReason::Malformed)?;
    let received = ts.saturating_add(i64::try_from(latency).unwrap_or(i64::MAX));
    Ok(EvaluationRow {
        state_ts_ms: ts,
        state_hash: state_hash.clone(),
        pins,
        observed_latency_ms: latency,
        attempt_count: u32_at(batch, "attempt_count", row).map_err(|_| SkipReason::Malformed)?,
        error,
        trigger: text_at(batch, "trigger", row).map_err(|_| SkipReason::Malformed)?,
        split: text_at(batch, "split", row).map_err(|_| SkipReason::Malformed)?,
        fidelity: text_at(batch, "fidelity", row).map_err(|_| SkipReason::Malformed)?,
        live,
        evaluation: JevEvaluation {
            market_id: state_hash,
            state_seq: 0,
            sent_at_ms: ts,
            received_at_ms: received,
            latency_ms: latency,
            signal,
            tokens_in: u64_at(batch, "tokens_in", row).map_err(|_| SkipReason::Malformed)?,
            tokens_out: u64_at(batch, "tokens_out", row).map_err(|_| SkipReason::Malformed)?,
        },
    })
}

fn pins_at(b: &RecordBatch, r: usize) -> Result<VersionPins, String> {
    let t = |n| text_at(b, n, r);
    Ok(VersionPins {
        model_id: t("model_id")?,
        model_version: t("model_version")?,
        strategy_version: t("strategy_version")?,
        prompt_version: t("prompt_version")?,
        question_document_sha256: t("question_document_sha256")?,
        question_schema_version: t("question_schema_version")?,
        feature_builder_version: t("feature_builder_version")?,
        normalization_version: t("normalization_version")?,
        serialization_version: t("serialization_version")?,
        variant: t("variant")?,
    })
}

fn signal_at(b: &RecordBatch, r: usize) -> Result<V1Signal, String> {
    let f = |n| f64_at(b, n, r);
    let t: TickDistribution = serde_json::from_str(&text_at(b, "repricing_ticks", r)?)
        .map_err(|e| format!("repricing_ticks: {e}"))?;
    let response: SystemOneResponse = serde_json::from_value(serde_json::json!({"answers": {
        "yes_pressure_5s": {"type":"noul", "noul":f("yes_pressure_5s")?},
        "no_pressure_5s": {"type":"noul", "noul":f("no_pressure_5s")?},
        "move_persists": {"type":"noul", "noul":f("move_persists")?},
        "underreact_up": {"type":"noul", "noul":f("underreact_up")?},
        "underreact_down": {"type":"noul", "noul":f("underreact_down")?},
        "fill_before_decay": {"type":"noul", "noul":f("fill_before_decay")?},
        "fill_toxic": {"type":"noul", "noul":f("fill_toxic")?},
        "repricing_ticks": {"type":"choice", "probabilities": {
            "UP_3_PLUS_TICKS":t.up_3_plus, "UP_2_TICKS":t.up_2, "UP_1_TICK":t.up_1,
            "FLAT":t.flat, "DOWN_1_TICK":t.down_1, "DOWN_2_TICKS":t.down_2,
            "DOWN_3_PLUS_TICKS":t.down_3_plus}, "confidence":f("repricing_confidence")?}
    }}))
    .map_err(|e| e.to_string())?;
    parse_v1_signal(&response).map_err(|e| e.to_string())
}

fn column<'a, T: 'static>(b: &'a RecordBatch, name: &str) -> Result<&'a T, String> {
    let i = b
        .schema()
        .index_of(name)
        .map_err(|_| format!("missing column `{name}`"))?;
    b.column(i)
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| format!("invalid type for `{name}`"))
}
fn text_at(b: &RecordBatch, n: &str, r: usize) -> Result<String, String> {
    let a = column::<StringArray>(b, n)?;
    (!a.is_null(r))
        .then(|| a.value(r).to_owned())
        .ok_or_else(|| format!("{n} is null"))
}
fn opt_text(b: &RecordBatch, n: &str, r: usize) -> Result<Option<String>, String> {
    let a = column::<StringArray>(b, n)?;
    Ok((!a.is_null(r)).then(|| a.value(r).to_owned()))
}
macro_rules! scalar_at {
    ($name:ident, $array:ty, $value:ty) => {
        fn $name(b: &RecordBatch, n: &str, r: usize) -> Result<$value, String> {
            let a = column::<$array>(b, n)?;
            (!a.is_null(r))
                .then(|| a.value(r))
                .ok_or_else(|| format!("{n} is null"))
        }
    };
}
scalar_at!(i64_at, Int64Array, i64);
scalar_at!(u64_at, UInt64Array, u64);
scalar_at!(u32_at, UInt32Array, u32);
scalar_at!(bool_at, BooleanArray, bool);
fn f64_at(b: &RecordBatch, n: &str, r: usize) -> Result<f64, String> {
    let a = column::<Float64Array>(b, n)?;
    (!a.is_null(r))
        .then(|| a.value(r))
        .filter(|v| v.is_finite())
        .ok_or_else(|| format!("{n} is invalid"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum LatencyPercentile {
    #[default]
    P50,
    P75,
    P90,
    P95,
    P99,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Default)]
pub struct LatencyProfile {
    pub p50_ms: u64,
    pub p75_ms: u64,
    pub p90_ms: u64,
    pub p95_ms: u64,
    pub p99_ms: u64,
}
impl LatencyProfile {
    #[must_use]
    pub const fn sampled_ms(self, p: LatencyPercentile) -> u64 {
        match p {
            LatencyPercentile::P50 => self.p50_ms,
            LatencyPercentile::P75 => self.p75_ms,
            LatencyPercentile::P90 => self.p90_ms,
            LatencyPercentile::P95 => self.p95_ms,
            LatencyPercentile::P99 => self.p99_ms,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Default)]
pub struct LatencyScenario {
    pub profile: LatencyProfile,
    pub percentile: LatencyPercentile,
    pub execution_latency_ms: u64,
}
#[must_use]
pub fn decision_time(ts: i64, s: LatencyScenario) -> i64 {
    ts.saturating_add(i64::try_from(s.profile.sampled_ms(s.percentile)).unwrap_or(i64::MAX))
        .saturating_add(i64::try_from(s.execution_latency_ms).unwrap_or(i64::MAX))
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Position {
    pub quantity: f64,
    pub entry_price: Option<f64>,
    pub mark_price: Option<f64>,
    pub opened_at_ms: i64,
}
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PortfolioState {
    pub position: Position,
    pub entry_size: f64,
    pub max_position: f64,
    pub max_exposure: f64,
    pub exposure: f64,
    pub outstanding_quotes: usize,
    pub book_stale: bool,
    pub kill_switch: bool,
}
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LocalQuote {
    pub active: bool,
    pub expires_at_ms: i64,
}
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Default)]
pub struct ExitLevels {
    pub stop_loss_pp: f64,
    pub take_profit_pp: f64,
    pub max_hold_ms: u64,
    pub toxicity_exit_min: f64,
    pub down_probability_exit_min: f64,
}

#[must_use]
pub fn decide_entry(
    e: &JevEvaluation,
    t: &QuoteThresholds,
    p: &PortfolioState,
    g: &RiskGate,
) -> bool {
    !p.kill_switch
        && p.position.quantity + p.entry_size <= p.max_position
        && p.exposure + p.entry_size <= p.max_exposure
        && g.check(e.latency_ms, p.outstanding_quotes, p.book_stale)
            .is_ok()
        && should_quote(&e.signal, t)
}
#[derive(Debug, Clone, Copy)]
pub struct EntryQuoteParams<'a> {
    pub book: &'a OrderBook,
    pub stale: bool,
    pub tick: TickSize,
    pub size: u64,
}

#[must_use]
pub fn decide_entry_quote(
    e: &JevEvaluation,
    t: &QuoteThresholds,
    p: &PortfolioState,
    g: &RiskGate,
    params: EntryQuoteParams<'_>,
) -> Option<QuoteIntent> {
    decide_entry(e, t, p, g)
        .then(|| {
            decide_quote(
                &e.signal,
                params.book,
                t,
                params.stale,
                params.tick,
                params.size,
            )
        })
        .flatten()
}
#[must_use]
pub fn decide_hold(e: &JevEvaluation, p: &Position, t: &QuoteThresholds) -> bool {
    p.quantity > 0.0 && e.signal.move_persists >= t.persist_min && e.signal.fill_toxic < t.toxic_max
}
#[must_use]
pub fn decide_cancel(e: &JevEvaluation, q: &LocalQuote, t: &QuoteThresholds, now: i64) -> bool {
    q.active
        && (now >= q.expires_at_ms
            || !should_quote(&e.signal, t)
            || e.signal.fill_toxic >= t.toxic_max)
}
#[must_use]
pub fn decide_exit(e: &JevEvaluation, p: &Position, x: ExitLevels, now: i64) -> bool {
    if p.quantity <= 0.0 {
        return false;
    }
    let timed = x.max_hold_ms > 0
        && now.saturating_sub(p.opened_at_ms) >= i64::try_from(x.max_hold_ms).unwrap_or(i64::MAX);
    let toxic = e.signal.fill_toxic >= x.toxicity_exit_min;
    let down =
        e.signal.repricing.down_1 + e.signal.repricing.down_2 + e.signal.repricing.down_3_plus
            >= x.down_probability_exit_min;
    let level = p
        .entry_price
        .zip(p.mark_price)
        .is_some_and(|(entry, mark)| {
            let move_pp = (mark - entry) * 100.0;
            move_pp <= -x.stop_loss_pp || move_pp >= x.take_profit_pp
        });
    timed || toxic || down || level
}

#[derive(Debug, Clone)]
pub struct SweepGrid {
    pub thresholds: Vec<QuoteThresholds>,
    pub fees: Vec<f64>,
    pub slippage_pp: Vec<f64>,
    pub sizing: Vec<f64>,
    pub stops: Vec<ExitLevels>,
    pub latency: Vec<LatencyScenario>,
}
#[derive(Debug, Clone, Copy, Serialize)]
pub struct ReportParams {
    pub thresholds: [f64; 7],
    pub fee_rate: f64,
    pub slippage_pp: f64,
    pub size: f64,
    pub stops: ExitLevels,
    pub latency: LatencyScenario,
}
#[derive(Debug, Clone, Default, Serialize)]
pub struct SplitReport {
    pub n_evaluated: usize,
    pub n_enter: usize,
    pub n_cancel: usize,
    pub n_exits: usize,
    pub markout_not_available: usize,
    pub pnl_not_available: usize,
}
#[derive(Debug, Clone, Serialize)]
pub struct RunReport {
    pub params: ReportParams,
    pub n_evaluated: usize,
    pub n_skipped: usize,
    pub n_enter: usize,
    pub n_exits: usize,
    pub per_split: BTreeMap<String, SplitReport>,
    pub markout_not_available: usize,
    pub pnl_not_available: usize,
}

#[must_use]
pub fn run_sweep(d: &EvaluationDataset, g: &SweepGrid) -> Vec<RunReport> {
    let mut out = Vec::new();
    for &t in &g.thresholds {
        for &fee in &g.fees {
            for &slip in &g.slippage_pp {
                for &size in &g.sizing {
                    for &stops in &g.stops {
                        for &latency in &g.latency {
                            out.push(run_one(
                                d,
                                ReportParams {
                                    thresholds: threshold_values(t),
                                    fee_rate: fee,
                                    slippage_pp: slip,
                                    size,
                                    stops,
                                    latency,
                                },
                            ));
                        }
                    }
                }
            }
        }
    }
    out
}
fn threshold_values(t: QuoteThresholds) -> [f64; 7] {
    [
        t.under_min,
        t.next_up_min,
        t.persist_min,
        t.fill_min,
        t.toxic_max,
        t.conflict_max,
        t.no_pressure_max,
    ]
}
fn thresholds(v: [f64; 7]) -> QuoteThresholds {
    QuoteThresholds {
        under_min: v[0],
        next_up_min: v[1],
        persist_min: v[2],
        fill_min: v[3],
        toxic_max: v[4],
        conflict_max: v[5],
        no_pressure_max: v[6],
    }
}
fn run_one(d: &EvaluationDataset, params: ReportParams) -> RunReport {
    let t = thresholds(params.thresholds);
    let gate = RiskGate::new(RiskLimits {
        max_outstanding_quotes: 1,
        max_latency_ms: u64::MAX,
        killed: false,
    });
    let mut state = PortfolioState {
        position: Position {
            quantity: 0.0,
            entry_price: None,
            mark_price: None,
            opened_at_ms: 0,
        },
        entry_size: params.size.max(0.0),
        max_position: params.size.max(0.0),
        max_exposure: params.size.max(0.0),
        exposure: 0.0,
        outstanding_quotes: 0,
        book_stale: false,
        kill_switch: false,
    };
    let mut quote = None;
    let mut r = RunReport {
        params,
        n_evaluated: 0,
        n_skipped: d.counts.n_skipped(),
        n_enter: 0,
        n_exits: 0,
        per_split: BTreeMap::new(),
        markout_not_available: 0,
        pnl_not_available: 0,
    };
    for row in &d.rows {
        r.n_evaluated += 1;
        let split = r.per_split.entry(row.split.clone()).or_default();
        split.n_evaluated += 1;
        let now = decision_time(row.state_ts_ms, params.latency);
        if let Some(q) = quote {
            if decide_cancel(&row.evaluation, &q, &t, now) {
                split.n_cancel += 1;
                quote = None;
                state.outstanding_quotes = 0;
            } else {
                continue;
            }
        }
        if decide_entry(&row.evaluation, &t, &state, &gate) {
            r.n_enter += 1;
            split.n_enter += 1;
            quote = Some(LocalQuote {
                active: true,
                expires_at_ms: now.saturating_add(5_000),
            });
            state.outstanding_quotes = 1;
        }
        r.markout_not_available += 1;
        r.pnl_not_available += 1;
        split.markout_not_available += 1;
        split.pnl_not_available += 1;
    }
    r
}

pub fn serialize_report(r: &RunReport) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(r)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn row(ts: i64) -> EvaluationRow {
        let signal = V1Signal {
            yes_pressure_5s: 0.8,
            no_pressure_5s: 0.1,
            move_persists: 0.8,
            underreact_up: 0.9,
            underreact_down: 0.1,
            repricing: TickDistribution {
                up_3_plus: 0.1,
                up_2: 0.2,
                up_1: 0.4,
                flat: 0.1,
                down_1: 0.1,
                down_2: 0.05,
                down_3_plus: 0.05,
            },
            repricing_confidence: 0.8,
            fill_before_decay: 0.8,
            fill_toxic: 0.2,
        };
        let id = format!("s{ts}");
        EvaluationRow {
            state_ts_ms: ts,
            state_hash: id.clone(),
            pins: VersionPins::current_v1(),
            evaluation: JevEvaluation {
                market_id: id,
                state_seq: 0,
                sent_at_ms: ts,
                received_at_ms: ts,
                latency_ms: 10,
                signal,
                tokens_in: 1,
                tokens_out: 1,
            },
            observed_latency_ms: 10,
            attempt_count: 1,
            error: None,
            trigger: "test".into(),
            split: "EXPLORATION".into(),
            fidelity: "EXACT".into(),
            live: false,
        }
    }
    fn grid() -> SweepGrid {
        SweepGrid {
            thresholds: vec![QuoteThresholds::default()],
            fees: vec![0.0],
            slippage_pp: vec![0.0],
            sizing: vec![1.0],
            stops: vec![ExitLevels::default()],
            latency: vec![LatencyScenario::default()],
        }
    }
    #[test]
    fn same_rows_and_params_have_identical_report_bytes() {
        let d = EvaluationDataset {
            rows: vec![row(1_000), row(2_000)],
            counts: ReadCounts {
                n_rows: 2,
                n_usable: 2,
                ..ReadCounts::default()
            },
        };
        assert_eq!(
            serialize_report(&run_sweep(&d, &grid())[0]).unwrap(),
            serialize_report(&run_sweep(&d, &grid())[0]).unwrap()
        );
    }
    #[test]
    fn cache_mode_policy_segregates_stub_and_live_rows() {
        let p = EvaluationReadPolicy {
            allow_live: false,
            allow_stub: true,
            expected_pins: None,
        };
        assert!(!p.allow_live && p.allow_stub);
    }
    #[test]
    fn higher_latency_profile_never_moves_decision_earlier() {
        let p = LatencyProfile {
            p50_ms: 10,
            p75_ms: 20,
            p90_ms: 30,
            p95_ms: 40,
            p99_ms: 50,
        };
        let low = LatencyScenario {
            profile: p,
            percentile: LatencyPercentile::P50,
            execution_latency_ms: 5,
        };
        let high = LatencyScenario {
            profile: p,
            percentile: LatencyPercentile::P99,
            execution_latency_ms: 5,
        };
        assert!(decision_time(1_000, high) >= decision_time(1_000, low));
    }
}
