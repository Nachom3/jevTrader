//! End-to-end shadow/paper pipeline for one market.
//!
//! The pipeline owns no feed or wall-clock work. Callers provide one coherent,
//! timestamped snapshot and the already-normalized external observations. This
//! keeps the decision and markout paths deterministic while `main` wires the
//! live adapters around them.

use crate::config::{QuantConfig, QuoteThresholds};
use crate::domain::{PriceTicks, TickSize, TradeSide, Trigger};
use crate::engine::execution_actor::ExecutionActor;
use crate::engine::market_actor::MarketSnapshot;
use crate::engine::signal_actor::SignalActor;
use crate::execution::markout;
use crate::jev::client::JevError;
use crate::jev::request::{V1Questions, V1State};
use crate::jev::response::{JevEvaluation, V1Signal};
use crate::market_spec::MarketSpec;
use crate::state::feature_builder::{
    ExternalTick, OrderFlowAggregates, ResolutionContext, VenueMicroprices, build_features,
};
use crate::state::poly_history::PolyHistory;
use crate::state::quant_features::build_quant;
use crate::storage::{QuestDbHandle, StorageEvent, Variant};
use crate::strategy::lead_lag::PolySnapshot;
use crate::strategy::quote::{QuoteIntent, decide_quote};
use crate::strategy::risk::RiskBlock;
use std::collections::hash_map::DefaultHasher;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::time::Duration;

/// Markout horizons required by the V1 storage contract.
pub const MARKOUT_HORIZONS_MS: [u64; 5] = [1_000, 5_000, 10_000, 30_000, 60_000];

/// Typed reasons for a paper decision that did not produce a quote.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkipReason {
    SizeZero,
    StaleBook,
    MissingBid,
    MissingAsk,
    InvalidCandidate,
    StrategyRejected,
    UnusableSignal,
    JevError,
    StateSerialization,
    RiskBlocked(RiskBlock),
}

/// Pure output of the deterministic quote decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Quote(QuoteIntent),
    Skip(SkipReason),
}

impl Outcome {
    #[must_use]
    pub const fn is_quote(self) -> bool {
        match self {
            Self::Quote(_) => true,
            Self::Skip(_) => false,
        }
    }

    #[must_use]
    pub const fn decision_name(self) -> &'static str {
        match self {
            Self::Quote(_) => "QUOTE",
            Self::Skip(_) => "SKIP",
        }
    }
}

/// Caller-owned inputs to [`decide`].
///
/// The input has no timestamp because the decision itself must not read or
/// derive time. Timestamps are carried by [`PipelineInput`] and
/// [`MarkoutTracker`].
pub struct DecisionInput<'a> {
    pub signal: &'a V1Signal,
    pub market: &'a MarketSnapshot,
    pub thresholds: &'a QuoteThresholds,
    pub tick_size: TickSize,
    pub size: u64,
}

/// Pure quote decision. It performs no I/O and reads no clock.
#[must_use]
pub fn decide(input: DecisionInput<'_>) -> Outcome {
    if input.size == 0 {
        return Outcome::Skip(SkipReason::SizeZero);
    }
    if input.market.stale || input.market.book.is_stale() {
        return Outcome::Skip(SkipReason::StaleBook);
    }
    if input.market.book.best_bid().is_none() {
        return Outcome::Skip(SkipReason::MissingBid);
    }
    if input.market.book.best_ask().is_none() {
        return Outcome::Skip(SkipReason::MissingAsk);
    }

    let Some(candidate) = candidate_maker_price(&input.market.book, input.tick_size) else {
        return Outcome::Skip(SkipReason::InvalidCandidate);
    };
    if input
        .market
        .book
        .best_ask()
        .is_none_or(|best_ask| candidate >= best_ask)
    {
        return Outcome::Skip(SkipReason::InvalidCandidate);
    }

    match decide_quote(
        input.signal,
        &input.market.book,
        input.thresholds,
        input.market.stale,
        input.tick_size,
        input.size,
    ) {
        Some(intent) if intent.price == candidate => Outcome::Quote(intent),
        Some(_) => Outcome::Skip(SkipReason::InvalidCandidate),
        None => Outcome::Skip(SkipReason::StrategyRejected),
    }
}

/// Computes the post-only candidate before Jev is called.
///
/// The integer arithmetic mirrors the established quote rule while avoiding a
/// floating-point price conversion at this boundary. A candidate that cannot
/// be represented as a valid price is rejected before the Jev request.
#[must_use]
pub fn candidate_maker_price(
    book: &crate::polymarket::OrderBook,
    tick_size: TickSize,
) -> Option<PriceTicks> {
    let best_bid = book.best_bid()?.as_micros();
    let tick = (tick_size.to_f64() * 1_000_000.0).round() as u64;
    if tick == 0 {
        return None;
    }

    let raw = best_bid.checked_add(tick)?;
    let increments = raw.div_ceil(tick);
    let candidate_micros = increments.checked_mul(tick)?;
    (candidate_micros <= 1_000_000).then(|| {
        // The range check above makes this conversion infallible without
        // exposing a fallible executable-price constructor just for wiring.
        PriceTicks::from_f64(candidate_micros as f64 / 1_000_000.0)
    })
}

/// Timestamped, coherent data supplied by the feed/application owner.
pub struct PipelineInput<'a> {
    pub market_id: &'a str,
    pub condition_id: &'a str,
    pub market_spec: &'a MarketSpec,
    pub resolution: ResolutionContext,
    pub snapshot: MarketSnapshot,
    pub last_trade_price: PriceTicks,
    pub tick_size: TickSize,
    pub recent_ticks: &'a [ExternalTick],
    pub venues: VenueMicroprices,
    pub order_flow: OrderFlowAggregates,
    pub size: u64,
    pub observed_at_ms: i64,
    pub mid: Option<PriceTicks>,
    pub trigger: Trigger,
}

/// Result of one pipeline step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StepResult {
    pub outcome: Outcome,
    pub jev_evaluated: bool,
    pub markouts_emitted: usize,
}

/// Small pure record completed by [`MarkoutTracker`].
#[derive(Debug, Clone, PartialEq)]
pub struct CompletedMarkout {
    pub condition_id: String,
    pub variant: Variant,
    pub jev_ts: i64,
    pub side: TradeSide,
    pub price: PriceTicks,
    pub size: u64,
    pub mids: [PriceTicks; 5],
    pub completed_at_ms: i64,
}

impl CompletedMarkout {
    fn into_storage_event(self) -> StorageEvent {
        let [mid_1s, mid_5s, mid_10s, mid_30s, mid_60s] = self.mids;
        let [pnl_1s, pnl_5s, pnl_10s, pnl_30s, pnl_60s] = [
            markout(self.price, mid_1s),
            markout(self.price, mid_5s),
            markout(self.price, mid_10s),
            markout(self.price, mid_30s),
            markout(self.price, mid_60s),
        ];
        StorageEvent::MakerMarkout {
            ts: millis_to_micros(self.completed_at_ms),
            condition_id: self.condition_id,
            variant: self.variant,
            jev_ts: self.jev_ts,
            side: match self.side {
                TradeSide::Buy => "BUY".to_owned(),
                TradeSide::Sell => "SELL".to_owned(),
            },
            price: self.price.to_f64(),
            size: self.size as f64,
            mid_1s: mid_1s.to_f64(),
            mid_5s: mid_5s.to_f64(),
            mid_10s: mid_10s.to_f64(),
            mid_30s: mid_30s.to_f64(),
            mid_60s: mid_60s.to_f64(),
            pnl_1s_pp: pnl_1s,
            pnl_5s_pp: pnl_5s,
            pnl_10s_pp: pnl_10s,
            pnl_30s_pp: pnl_30s,
            pnl_60s_pp: pnl_60s,
        }
    }
}

#[derive(Debug)]
struct PendingMarkout {
    condition_id: String,
    variant: Variant,
    jev_ts: i64,
    side: TradeSide,
    price: PriceTicks,
    size: u64,
    quoted_at_ms: i64,
    mids: [Option<PriceTicks>; 5],
}

/// One quote to track for maker markouts, carrying its A/B variant so
/// CONTROL and QUANT_V1 fills complete into separately labeled rows.
#[derive(Debug, Clone)]
pub struct MarkoutQuote {
    pub condition_id: String,
    pub jev_ts: i64,
    pub side: TradeSide,
    pub price: PriceTicks,
    pub size: u64,
    pub quoted_at_ms: i64,
    pub variant: Variant,
}

/// Pure timestamp-driven markout tracker.
///
/// A horizon is filled by the first caller-supplied mid whose timestamp is at
/// or after the quote timestamp plus that horizon. No clock or sleep is used.
#[derive(Debug, Default)]
pub struct MarkoutTracker {
    pending: Vec<PendingMarkout>,
}

impl MarkoutTracker {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_quote(
        &mut self,
        condition_id: impl Into<String>,
        jev_ts: i64,
        side: TradeSide,
        price: PriceTicks,
        size: u64,
        quoted_at_ms: i64,
    ) {
        self.add_quote_with_variant(MarkoutQuote {
            condition_id: condition_id.into(),
            jev_ts,
            side,
            price,
            size,
            quoted_at_ms,
            variant: Variant::Control,
        });
    }

    pub fn add_quote_with_variant(&mut self, quote: MarkoutQuote) {
        self.pending.push(PendingMarkout {
            condition_id: quote.condition_id,
            variant: quote.variant,
            jev_ts: quote.jev_ts,
            side: quote.side,
            price: quote.price,
            size: quote.size,
            quoted_at_ms: quote.quoted_at_ms,
            mids: [None; 5],
        });
    }

    /// Feeds one caller-timestamped mid and returns any fully completed rows.
    pub fn observe(&mut self, at_ms: i64, mid: Option<PriceTicks>) -> Vec<CompletedMarkout> {
        let Some(mid) = mid else {
            return Vec::new();
        };

        let mut completed = Vec::new();
        let mut remaining = Vec::with_capacity(self.pending.len());
        for mut pending in self.pending.drain(..) {
            for (index, horizon_ms) in MARKOUT_HORIZONS_MS.iter().enumerate() {
                let target = pending
                    .quoted_at_ms
                    .saturating_add((*horizon_ms).min(i64::MAX as u64) as i64);
                if pending.mids[index].is_none() && at_ms >= target {
                    pending.mids[index] = Some(mid);
                }
            }

            if let [
                Some(mid_1s),
                Some(mid_5s),
                Some(mid_10s),
                Some(mid_30s),
                Some(mid_60s),
            ] = pending.mids
            {
                completed.push(CompletedMarkout {
                    condition_id: pending.condition_id,
                    variant: pending.variant,
                    jev_ts: pending.jev_ts,
                    side: pending.side,
                    price: pending.price,
                    size: pending.size,
                    mids: [mid_1s, mid_5s, mid_10s, mid_30s, mid_60s],
                    completed_at_ms: at_ms,
                });
            } else {
                remaining.push(pending);
            }
        }
        self.pending = remaining;
        completed
    }

    #[must_use]
    pub fn pending(&self) -> usize {
        self.pending.len()
    }
}

/// Async boundary implemented by the real [`SignalActor`] and by tests.
pub trait SignalEvaluator {
    fn evaluate_next<'a>(
        &'a mut self,
        state: &'a V1State,
        market_id: &'a str,
        api_key: &'a str,
        deadline: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<JevEvaluation, JevError>> + Send + 'a>>;

    fn usable_signal(&self) -> Option<&V1Signal>;
}

impl SignalEvaluator for SignalActor {
    fn evaluate_next<'a>(
        &'a mut self,
        state: &'a V1State,
        market_id: &'a str,
        api_key: &'a str,
        deadline: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<JevEvaluation, JevError>> + Send + 'a>> {
        Box::pin(SignalActor::evaluate_next(
            self, state, market_id, api_key, deadline,
        ))
    }

    fn usable_signal(&self) -> Option<&V1Signal> {
        SignalActor::usable_signal(self)
    }
}

/// One-market stateful end-to-end pipeline.
pub struct Pipeline<E = SignalActor> {
    signal_actor: E,
    execution_actor: ExecutionActor,
    questdb: Option<QuestDbHandle>,
    api_key: String,
    deadline: Duration,
    thresholds: QuoteThresholds,
    quant: QuantConfig,
    poly_history: PolyHistory,
    markouts: MarkoutTracker,
    #[cfg(test)]
    persisted_events: std::cell::RefCell<Vec<StorageEvent>>,
}

impl Pipeline<SignalActor> {
    /// Creates the production pipeline. Execution remains paper-only because
    /// [`ExecutionActor`] owns only a [`crate::execution::PaperBook`].
    // Eight fixed pipeline dependencies; a params struct would only rename them.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        signal_actor: SignalActor,
        execution_actor: ExecutionActor,
        questdb: QuestDbHandle,
        api_key: impl Into<String>,
        deadline: Duration,
        thresholds: QuoteThresholds,
        quant: QuantConfig,
        poly_history_capacity: usize,
    ) -> Self {
        Self::with_evaluator(
            signal_actor,
            execution_actor,
            Some(questdb),
            api_key,
            deadline,
            thresholds,
            quant,
            poly_history_capacity,
        )
    }
}

impl<E: SignalEvaluator> Pipeline<E> {
    // Same eight dependencies as `new`; see the note there.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn with_evaluator(
        signal_actor: E,
        execution_actor: ExecutionActor,
        questdb: Option<QuestDbHandle>,
        api_key: impl Into<String>,
        deadline: Duration,
        thresholds: QuoteThresholds,
        quant: QuantConfig,
        poly_history_capacity: usize,
    ) -> Self {
        Self {
            signal_actor,
            execution_actor,
            questdb,
            api_key: api_key.into(),
            deadline,
            thresholds,
            quant,
            poly_history: PolyHistory::new(poly_history_capacity),
            markouts: MarkoutTracker::new(),
            #[cfg(test)]
            persisted_events: std::cell::RefCell::new(Vec::new()),
        }
    }

    /// Executes one complete caller-timestamped step.
    pub async fn run_step(&mut self, input: PipelineInput<'_>) -> StepResult {
        let completed_markouts = self.markouts.observe(input.observed_at_ms, input.mid);
        let markouts_emitted = completed_markouts.len();
        self.persist_markouts(completed_markouts);

        let early_outcome = if input.size == 0 {
            Some(Outcome::Skip(SkipReason::SizeZero))
        } else if input.snapshot.stale || input.snapshot.book.is_stale() {
            Some(Outcome::Skip(SkipReason::StaleBook))
        } else if candidate_maker_price(&input.snapshot.book, input.tick_size).is_none_or(
            |candidate| {
                input
                    .snapshot
                    .book
                    .best_ask()
                    .is_none_or(|best_ask| candidate >= best_ask)
            },
        ) {
            Some(Outcome::Skip(SkipReason::InvalidCandidate))
        } else {
            None
        };
        if let Some(outcome) = early_outcome {
            self.persist_decision(
                &input,
                outcome,
                input.observed_at_ms,
                None,
                Variant::Control,
            );
            return StepResult {
                outcome,
                jev_evaluated: false,
                markouts_emitted,
            };
        }

        let Some(candidate_price) = candidate_maker_price(&input.snapshot.book, input.tick_size)
        else {
            // The guard above makes this unreachable, but keeping this branch
            // explicit avoids a panic if the guard and constructor diverge.
            let outcome = Outcome::Skip(SkipReason::InvalidCandidate);
            self.persist_decision(
                &input,
                outcome,
                input.observed_at_ms,
                None,
                Variant::Control,
            );
            return StepResult {
                outcome,
                jev_evaluated: false,
                markouts_emitted,
            };
        };
        let Some(poly_snapshot) = build_poly_snapshot(
            &input.snapshot,
            input.last_trade_price,
            &mut self.poly_history,
            input.observed_at_ms,
        ) else {
            let outcome = Outcome::Skip(SkipReason::InvalidCandidate);
            self.persist_decision(
                &input,
                outcome,
                input.observed_at_ms,
                None,
                Variant::Control,
            );
            return StepResult {
                outcome,
                jev_evaluated: false,
                markouts_emitted,
            };
        };

        let features = build_features(
            input.recent_ticks,
            &poly_snapshot,
            input.resolution.clone(),
            input.venues,
            input.order_flow,
        );
        let control_state = V1State::new(
            input.market_spec.question.as_str(),
            input.market_spec.resolution_rules.as_str(),
            features.clone(),
            poly_snapshot.clone(),
            None,
            candidate_price,
        );
        let control_state_json = match serde_json::to_string(&control_state) {
            Ok(state_json) => state_json,
            Err(_error) => {
                let outcome = Outcome::Skip(SkipReason::StateSerialization);
                self.persist_decision(
                    &input,
                    outcome,
                    input.observed_at_ms,
                    Some(&poly_snapshot),
                    Variant::Control,
                );
                return StepResult {
                    outcome,
                    jev_evaluated: false,
                    markouts_emitted,
                };
            }
        };
        let questions_json = match serde_json::to_string(&V1Questions::new(candidate_price)) {
            Ok(questions_json) => questions_json,
            Err(_error) => {
                let outcome = Outcome::Skip(SkipReason::StateSerialization);
                self.persist_decision(
                    &input,
                    outcome,
                    input.observed_at_ms,
                    Some(&poly_snapshot),
                    Variant::Control,
                );
                return StepResult {
                    outcome,
                    jev_evaluated: false,
                    markouts_emitted,
                };
            }
        };
        let control_state_hash = hash_text(&control_state_json);

        if !self.quant.enabled {
            let (evaluation, signal) = self.evaluate_branch(&control_state, input.market_id).await;
            if let Ok(evaluation) = &evaluation {
                self.persist_signal(
                    &input,
                    Variant::Control,
                    &control_state_hash,
                    &control_state_json,
                    &questions_json,
                    evaluation,
                );
            }
            let outcome = self.finish_branch(
                &input,
                &poly_snapshot,
                Variant::Control,
                &evaluation,
                signal,
                true,
            );
            return StepResult {
                outcome,
                jev_evaluated: true,
                markouts_emitted,
            };
        }

        let quant_state = V1State::new(
            input.market_spec.question.as_str(),
            input.market_spec.resolution_rules.as_str(),
            features,
            poly_snapshot.clone(),
            Some(build_quant(&control_state.underlying, &self.quant.params)),
            candidate_price,
        );
        let quant_state_json = match serde_json::to_string(&quant_state) {
            Ok(state_json) => state_json,
            Err(_error) => {
                // Serialization failures remain a single control-labelled
                // early skip; neither branch has made a Jev call yet.
                let outcome = Outcome::Skip(SkipReason::StateSerialization);
                self.persist_decision(
                    &input,
                    outcome,
                    input.observed_at_ms,
                    Some(&poly_snapshot),
                    Variant::Control,
                );
                return StepResult {
                    outcome,
                    jev_evaluated: false,
                    markouts_emitted,
                };
            }
        };
        let quant_state_hash = hash_text(&quant_state_json);

        // Shadow mode evaluates the stateful actor sequentially, which costs
        // roughly 2x Jev latency. The branches still share this frozen feature
        // snapshot and never share execution decisions.
        let (control_evaluation, control_signal) =
            self.evaluate_branch(&control_state, input.market_id).await;
        if let Ok(evaluation) = &control_evaluation {
            self.persist_signal(
                &input,
                Variant::Control,
                &control_state_hash,
                &control_state_json,
                &questions_json,
                evaluation,
            );
        }

        let (quant_evaluation, quant_signal) =
            self.evaluate_branch(&quant_state, input.market_id).await;
        if let Ok(evaluation) = &quant_evaluation {
            self.persist_signal(
                &input,
                Variant::QuantV1,
                &quant_state_hash,
                &quant_state_json,
                &questions_json,
                evaluation,
            );
        }

        // CONTROL is shadow-only: it records the decision and hypothetical
        // markout, but it can never reach the execution actor.
        let _control_outcome = self.finish_branch(
            &input,
            &poly_snapshot,
            Variant::Control,
            &control_evaluation,
            control_signal,
            false,
        );
        // QUANT_V1 is primary when enabled and is the only branch allowed to
        // invoke the execution actor.
        let outcome = self.finish_branch(
            &input,
            &poly_snapshot,
            Variant::QuantV1,
            &quant_evaluation,
            quant_signal,
            true,
        );

        StepResult {
            outcome,
            jev_evaluated: true,
            markouts_emitted,
        }
    }

    async fn evaluate_branch(
        &mut self,
        state: &V1State,
        market_id: &str,
    ) -> (Result<JevEvaluation, JevError>, Option<V1Signal>) {
        let evaluation = self
            .signal_actor
            .evaluate_next(state, market_id, &self.api_key, self.deadline)
            .await;
        let signal = if evaluation.is_ok() {
            self.signal_actor.usable_signal().cloned()
        } else {
            None
        };
        (evaluation, signal)
    }

    fn persist_signal(
        &self,
        input: &PipelineInput<'_>,
        variant: Variant,
        state_hash: &str,
        state_json: &str,
        questions_json: &str,
        evaluation: &JevEvaluation,
    ) {
        self.enqueue(StorageEvent::jev_signal(
            millis_to_micros(evaluation.received_at_ms),
            input.condition_id,
            state_hash.to_owned(),
            state_json.to_owned(),
            questions_json.to_owned(),
            i64::try_from(evaluation.state_seq).unwrap_or(i64::MAX),
            i64::try_from(evaluation.latency_ms).unwrap_or(i64::MAX),
            input.trigger,
            variant,
            &evaluation.signal,
        ));
    }

    fn finish_branch(
        &mut self,
        input: &PipelineInput<'_>,
        poly_snapshot: &PolySnapshot,
        variant: Variant,
        evaluation: &Result<JevEvaluation, JevError>,
        signal: Option<V1Signal>,
        execute: bool,
    ) -> Outcome {
        let outcome = match (evaluation, signal) {
            (Err(_), _) => Outcome::Skip(SkipReason::JevError),
            (Ok(_), None) => Outcome::Skip(SkipReason::UnusableSignal),
            (Ok(evaluation), Some(signal)) => {
                let decision = decide(DecisionInput {
                    signal: &signal,
                    market: &input.snapshot,
                    thresholds: &self.thresholds,
                    tick_size: input.tick_size,
                    size: input.size,
                });
                if execute {
                    // `decide_quote` owns the strategy-side checks; `on_quote`
                    // is the sole runtime RiskGate invocation.
                    match decision {
                        Outcome::Quote(intent) => match self.execution_actor.on_quote(
                            intent,
                            input.snapshot.stale || input.snapshot.book.is_stale(),
                            evaluation.latency_ms,
                        ) {
                            Ok(_) => Outcome::Quote(intent),
                            Err(block) => Outcome::Skip(SkipReason::RiskBlocked(block)),
                        },
                        Outcome::Skip(reason) => Outcome::Skip(reason),
                    }
                } else {
                    decision
                }
            }
        };
        let jev_ts_ms = evaluation
            .as_ref()
            .map_or(input.observed_at_ms, |evaluation| evaluation.received_at_ms);
        self.persist_decision(input, outcome, jev_ts_ms, Some(poly_snapshot), variant);
        if let Outcome::Quote(intent) = outcome {
            self.markouts.add_quote_with_variant(MarkoutQuote {
                condition_id: input.condition_id.to_owned(),
                jev_ts: millis_to_micros(jev_ts_ms),
                side: intent.side,
                price: intent.price,
                size: intent.size,
                quoted_at_ms: input.observed_at_ms,
                variant,
            });
        }
        outcome
    }

    fn persist_markouts(&self, markouts: Vec<CompletedMarkout>) {
        for markout in markouts {
            self.enqueue(markout.into_storage_event());
        }
    }

    fn persist_decision(
        &self,
        input: &PipelineInput<'_>,
        outcome: Outcome,
        jev_ts_ms: i64,
        poly_snapshot: Option<&PolySnapshot>,
        variant: Variant,
    ) {
        let (paper_price, fair_value) = match (outcome, poly_snapshot) {
            (Outcome::Quote(intent), Some(poly)) => (intent.price.to_f64(), poly.yes_ask.to_f64()),
            (_, Some(poly)) => (0.0, poly.yes_ask.to_f64()),
            _ => (0.0, 0.0),
        };
        let edge = if paper_price > 0.0 {
            fair_value - paper_price
        } else {
            0.0
        };
        self.enqueue(StorageEvent::PaperDecision {
            ts: millis_to_micros(input.observed_at_ms),
            condition_id: input.condition_id.to_owned(),
            variant,
            jev_ts: millis_to_micros(jev_ts_ms),
            edge,
            threshold: 0.0,
            decision: outcome.decision_name().to_owned(),
            paper_price,
            size: input.size as f64,
            fair_value,
        });
        let _ = (input.market_id, outcome);
    }

    fn enqueue(&self, event: StorageEvent) {
        #[cfg(test)]
        self.persisted_events.borrow_mut().push(event.clone());
        if let Some(questdb) = &self.questdb {
            let _ = questdb.try_send(event);
        }
    }
}

fn build_poly_snapshot(
    market: &MarketSnapshot,
    last_trade_price: PriceTicks,
    history: &mut PolyHistory,
    observed_at_ms: i64,
) -> Option<PolySnapshot> {
    let best_bid = market.book.best_bid()?;
    let best_ask = market.book.best_ask()?;
    let mid = coherent_mid(best_bid, best_ask)?;
    let bid_depth = market
        .book
        .bids()
        .iter()
        .map(|(_, quantity)| *quantity as f64)
        .sum::<f64>();
    let ask_depth = market
        .book
        .asks()
        .iter()
        .map(|(_, quantity)| *quantity as f64)
        .sum::<f64>();
    let total_depth = bid_depth + ask_depth;
    let book_imbalance = if total_depth > 0.0 {
        (bid_depth - ask_depth) / total_depth
    } else {
        0.0
    };

    history.push(observed_at_ms.max(0) as u64, mid);
    let mut snapshot = PolySnapshot {
        yes_bid: best_bid,
        yes_ask: best_ask,
        bid_depth,
        ask_depth,
        spread: (best_ask.to_f64() - best_bid.to_f64()).max(0.0),
        book_imbalance,
        last_trade_price,
        price_1s_ago: PriceTicks::from_f64(0.0),
        price_5s_ago: PriceTicks::from_f64(0.0),
        price_30s_ago: PriceTicks::from_f64(0.0),
    };
    history.apply_to_snapshot(observed_at_ms.max(0) as u64, &mut snapshot);
    Some(snapshot)
}

fn coherent_mid(best_bid: PriceTicks, best_ask: PriceTicks) -> Option<PriceTicks> {
    (best_bid <= best_ask)
        .then(|| PriceTicks::from_f64((best_bid.to_f64() + best_ask.to_f64()) / 2.0))
}

fn hash_text(value: &str) -> String {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

fn millis_to_micros(value: i64) -> i64 {
    value.saturating_mul(1_000)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::FreshnessPolicy;
    use crate::domain::Trigger;
    use crate::jev::response::{JevParseError, TickDistribution};
    use crate::polymarket::OrderBook;
    use crate::storage::QuestDbWriter;
    use std::{future, sync::OnceLock};

    #[derive(Debug)]
    struct FakeEvaluator {
        result: Option<Result<JevEvaluation, JevError>>,
        queued_results: Vec<Result<JevEvaluation, JevError>>,
        signal: Option<V1Signal>,
        calls: usize,
    }

    impl SignalEvaluator for FakeEvaluator {
        fn evaluate_next<'a>(
            &'a mut self,
            _state: &'a V1State,
            _market_id: &'a str,
            _api_key: &'a str,
            _deadline: Duration,
        ) -> Pin<Box<dyn Future<Output = Result<JevEvaluation, JevError>> + Send + 'a>> {
            self.calls += 1;
            // Move the configured result so Parse, Status, Deadline, and other
            // JevError variants reach the pipeline unchanged. Shadow tests use
            // the queue to serve the control and quant evaluations in order.
            let result = if self.queued_results.is_empty() {
                self.result
                    .take()
                    .unwrap_or_else(|| Err(JevError::Deadline))
            } else {
                self.queued_results.remove(0)
            };
            Box::pin(future::ready(result))
        }

        fn usable_signal(&self) -> Option<&V1Signal> {
            self.signal.as_ref()
        }
    }

    fn signal() -> V1Signal {
        V1Signal {
            yes_pressure_5s: 0.8,
            no_pressure_5s: 0.1,
            move_persists: 0.7,
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
            fill_before_decay: 0.7,
            fill_toxic: 0.2,
        }
    }

    fn market(stale: bool, bid: f64, ask: f64) -> MarketSnapshot {
        let mut book = OrderBook::default();
        book.apply_snapshot(
            [(PriceTicks::from_f64(bid), 100)],
            [(PriceTicks::from_f64(ask), 100)],
        );
        if stale {
            book.mark_stale();
        }
        MarketSnapshot { book, stale }
    }

    static EMPTY_TICKS: [ExternalTick; 0] = [];

    fn empty_ticks() -> &'static [ExternalTick] {
        EMPTY_TICKS.as_slice()
    }

    fn test_market_spec() -> &'static MarketSpec {
        static SPEC: OnceLock<MarketSpec> = OnceLock::new();
        SPEC.get_or_init(|| MarketSpec {
            slug: "market-1".to_owned(),
            question: "Will BTC reach the target?".to_owned(),
            resolution_source: "Official source".to_owned(),
            resolution_rules: "Official source resolves the market.".to_owned(),
            target: 120_000.0,
            resolution_at_ms: 120_900_000,
        })
    }

    fn input<'a>(market: MarketSnapshot, size: u64, at_ms: i64) -> PipelineInput<'a> {
        // The string literals make this fixture independent from any storage or
        // network setup; the lifetime is only used for the borrowed input shape.
        PipelineInput {
            market_id: "market-1",
            condition_id: "condition-1",
            market_spec: test_market_spec(),
            resolution: ResolutionContext::new(120_000.0, 900, "Official source"),
            snapshot: market,
            last_trade_price: PriceTicks::from_f64(0.41),
            tick_size: TickSize::from_f64(0.01),
            recent_ticks: empty_ticks(),
            venues: VenueMicroprices {
                binance: 100.0,
                coinbase: 100.0,
                perp: 100.0,
                perp_basis_pct: 0.0,
            },
            order_flow: OrderFlowAggregates {
                buy_vol_1s: 0.0,
                sell_vol_1s: 0.0,
                ofi_1s: 0.0,
                ofi_5s: 0.0,
                imbalance: 0.0,
                aggressive_buy_ratio: 0.5,
            },
            size,
            observed_at_ms: at_ms,
            mid: Some(PriceTicks::from_f64(0.41)),
            trigger: Trigger::PriceMove,
        }
    }

    fn pipeline(evaluator: FakeEvaluator) -> Pipeline<FakeEvaluator> {
        Pipeline::with_evaluator(
            evaluator,
            ExecutionActor::new(crate::strategy::risk::RiskLimits::from_freshness_policy(
                2,
                FreshnessPolicy::default(),
                false,
            )),
            None,
            "test-key",
            Duration::from_millis(100),
            QuoteThresholds::default(),
            QuantConfig::default(),
            64,
        )
    }

    fn evaluation() -> JevEvaluation {
        JevEvaluation {
            market_id: "market-1".to_owned(),
            state_seq: 1,
            sent_at_ms: 1_000,
            received_at_ms: 1_100,
            latency_ms: 100,
            signal: signal(),
        }
    }

    #[tokio::test]
    async fn happy_path_quotes_without_network() {
        let evaluator = FakeEvaluator {
            result: Some(Ok(evaluation())),
            queued_results: Vec::new(),
            signal: Some(signal()),
            calls: 0,
        };
        let mut pipeline = pipeline(evaluator);

        let result = pipeline
            .run_step(input(market(false, 0.40, 0.45), 25, 1_000))
            .await;

        assert!(
            matches!(result.outcome, Outcome::Quote(intent) if intent.price == PriceTicks::from_f64(0.41))
        );
        assert!(result.jev_evaluated);
    }

    #[tokio::test]
    async fn stale_book_skips_before_calling_jev() {
        let evaluator = FakeEvaluator {
            result: Some(Ok(evaluation())),
            queued_results: Vec::new(),
            signal: Some(signal()),
            calls: 0,
        };
        let mut pipeline = pipeline(evaluator);

        let result = pipeline
            .run_step(input(market(true, 0.40, 0.45), 25, 1_000))
            .await;

        assert_eq!(result.outcome, Outcome::Skip(SkipReason::StaleBook));
        assert!(!result.jev_evaluated);
        assert_eq!(pipeline.signal_actor.calls, 0);
    }

    #[tokio::test]
    async fn jev_error_skips_without_retry() {
        let evaluator = FakeEvaluator {
            result: Some(Err(JevError::Deadline)),
            queued_results: Vec::new(),
            signal: None,
            calls: 0,
        };
        let mut pipeline = pipeline(evaluator);

        let result = pipeline
            .run_step(input(market(false, 0.40, 0.45), 25, 1_000))
            .await;

        assert_eq!(result.outcome, Outcome::Skip(SkipReason::JevError));
        assert!(result.jev_evaluated);
        assert_eq!(pipeline.signal_actor.calls, 1);
    }

    #[tokio::test]
    async fn zero_size_skips_before_calling_jev() {
        let evaluator = FakeEvaluator {
            result: Some(Ok(evaluation())),
            queued_results: Vec::new(),
            signal: Some(signal()),
            calls: 0,
        };
        let mut pipeline = pipeline(evaluator);

        let result = pipeline
            .run_step(input(market(false, 0.40, 0.45), 0, 1_000))
            .await;

        assert_eq!(result.outcome, Outcome::Skip(SkipReason::SizeZero));
        assert!(!result.jev_evaluated);
        assert_eq!(pipeline.signal_actor.calls, 0);
    }

    #[tokio::test]
    async fn parse_error_skips_persists_without_retry() {
        let evaluator = FakeEvaluator {
            result: Some(Err(JevError::Parse(JevParseError::MissingAnswer(
                "yes_pressure_5s",
            )))),
            queued_results: Vec::new(),
            signal: None,
            calls: 0,
        };
        let (questdb, _writer_task) = QuestDbWriter::spawn("", 1);
        let mut pipeline = pipeline(evaluator);
        pipeline.questdb = Some(questdb);

        let result = pipeline
            .run_step(input(market(false, 0.40, 0.45), 25, 1_000))
            .await;

        assert_eq!(result.outcome, Outcome::Skip(SkipReason::JevError));
        assert!(result.jev_evaluated);
        assert_eq!(pipeline.signal_actor.calls, 1);
        assert_eq!(pipeline.questdb.as_ref().unwrap().metrics().sent, 1);
    }

    #[tokio::test]
    async fn invalid_candidate_skips_before_calling_jev() {
        let evaluator = FakeEvaluator {
            result: Some(Ok(evaluation())),
            queued_results: Vec::new(),
            signal: Some(signal()),
            calls: 0,
        };
        let mut pipeline = pipeline(evaluator);

        let result = pipeline
            .run_step(input(market(false, 0.40, 0.405), 25, 1_000))
            .await;

        assert_eq!(result.outcome, Outcome::Skip(SkipReason::InvalidCandidate));
        assert!(!result.jev_evaluated);
        assert_eq!(pipeline.signal_actor.calls, 0);
    }

    #[tokio::test]
    async fn quant_shadow_pairs_signals_and_isolates_execution() {
        let evaluator = FakeEvaluator {
            result: None,
            queued_results: vec![Ok(evaluation()), Ok(evaluation())],
            signal: Some(signal()),
            calls: 0,
        };
        let mut pipeline = pipeline(evaluator);
        pipeline.quant.enabled = true;

        let result = pipeline
            .run_step(input(market(false, 0.40, 0.45), 25, 1_000))
            .await;

        assert!(matches!(result.outcome, Outcome::Quote(_)));
        assert_eq!(pipeline.signal_actor.calls, 2);
        assert_eq!(pipeline.execution_actor.outstanding(), 1);
        assert_eq!(pipeline.markouts.pending(), 2);

        let events = pipeline.persisted_events.borrow();
        let signals: Vec<(Variant, String, String, String)> = events
            .iter()
            .filter_map(|event| match event {
                StorageEvent::JevSignal {
                    variant,
                    state_hash,
                    state_json,
                    questions_json,
                    ..
                } => Some((
                    *variant,
                    state_hash.clone(),
                    state_json.clone(),
                    questions_json.clone(),
                )),
                _ => None,
            })
            .collect();
        assert_eq!(signals.len(), 2);
        assert_eq!(
            signals.iter().map(|signal| signal.0).collect::<Vec<_>>(),
            vec![Variant::Control, Variant::QuantV1]
        );
        assert_ne!(signals[0].1, signals[1].1);
        assert_eq!(signals[0].3, signals[1].3);
        assert!(
            serde_json::from_str::<serde_json::Value>(&signals[0].2)
                .expect("control state JSON")
                .get("quant")
                .is_some_and(serde_json::Value::is_null)
        );
        assert!(
            serde_json::from_str::<serde_json::Value>(&signals[1].2)
                .expect("quant state JSON")
                .get("quant")
                .is_some_and(|quant| !quant.is_null())
        );

        let decisions: Vec<(Variant, String)> = events
            .iter()
            .filter_map(|event| match event {
                StorageEvent::PaperDecision {
                    variant, decision, ..
                } => Some((*variant, decision.clone())),
                _ => None,
            })
            .collect();
        assert_eq!(
            decisions,
            vec![
                (Variant::Control, "QUOTE".to_owned()),
                (Variant::QuantV1, "QUOTE".to_owned()),
            ]
        );
    }

    #[test]
    fn tracker_completes_all_five_horizons_from_caller_mids() {
        let mut tracker = MarkoutTracker::new();
        tracker.add_quote(
            "condition-1",
            1_000_000,
            TradeSide::Buy,
            PriceTicks::from_f64(0.40),
            25,
            0,
        );

        assert!(
            tracker
                .observe(1_000, Some(PriceTicks::from_f64(0.41)))
                .is_empty()
        );
        let completed = tracker.observe(60_000, Some(PriceTicks::from_f64(0.46)));

        assert_eq!(completed.len(), 1);
        assert_eq!(
            completed[0].mids,
            [
                PriceTicks::from_f64(0.41),
                PriceTicks::from_f64(0.46),
                PriceTicks::from_f64(0.46),
                PriceTicks::from_f64(0.46),
                PriceTicks::from_f64(0.46),
            ]
        );
        assert_eq!(completed[0].size, 25);
        assert_eq!(tracker.pending(), 0);
    }
}
