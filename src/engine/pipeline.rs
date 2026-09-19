//! End-to-end shadow/paper pipeline for one market.
//!
//! The pipeline owns no feed or wall-clock work. Callers provide one coherent,
//! timestamped snapshot and the already-normalized external observations. This
//! keeps the decision and markout paths deterministic while `main` wires the
//! live adapters around them.

use crate::config::{QuantConfig, QuoteThresholds};
use crate::domain::{PriceTicks, TickSize, TradeSide, Trigger};
use crate::engine::execution_actor::{DualFills, ExecutionActor};
use crate::engine::market_actor::MarketSnapshot;
use crate::engine::signal_actor::SignalActor;
use crate::execution::{TopOfBookUpdate, signed_markout};
use crate::jev::client::JevError;
use crate::jev::request::{V1Questions, V1State};
use crate::jev::response::{JevEvaluation, V1Signal};
use crate::market_spec::MarketSpec;
use crate::replay::portfolio::{FillEvent, Portfolio, PortfolioRegistry, PortfolioStats};
use crate::state::feature_builder::{
    ContractContext, ExternalTick, OrderFlowAggregates, ResolutionContext, VenueMicroprices,
    build_features_full,
};
use crate::state::poly_history::PolyHistory;
use crate::state::quant_features::build_quant;
use crate::storage::{ExperimentTags, QuestDbHandle, StorageEvent, Variant};
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
///
/// `quoted` separates the two research datasets: maker rows reference the
/// resting quote price (TRADING RESEARCH), signal rows reference the eval-time
/// mid for every usable Jev evaluation including SKIP (SIGNAL RESEARCH).
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
    pub tags: ExperimentTags,
    pub quoted: bool,
}

impl CompletedMarkout {
    fn into_storage_event(self) -> StorageEvent {
        let [mid_1s, mid_5s, mid_10s, mid_30s, mid_60s] = self.mids;
        let drift = [
            signed_markout(TradeSide::Buy, self.price, mid_1s),
            signed_markout(TradeSide::Buy, self.price, mid_5s),
            signed_markout(TradeSide::Buy, self.price, mid_10s),
            signed_markout(TradeSide::Buy, self.price, mid_30s),
            signed_markout(TradeSide::Buy, self.price, mid_60s),
        ];
        if !self.quoted {
            let [mo_1s, mo_5s, mo_10s, mo_30s, mo_60s] = drift;
            return StorageEvent::SignalMarkout {
                ts: millis_to_micros(self.completed_at_ms),
                condition_id: self.condition_id,
                variant: self.variant,
                jev_ts: self.jev_ts,
                ref_price: self.price.to_f64(),
                mid_1s: mid_1s.to_f64(),
                mid_5s: mid_5s.to_f64(),
                mid_10s: mid_10s.to_f64(),
                mid_30s: mid_30s.to_f64(),
                mid_60s: mid_60s.to_f64(),
                mo_1s_pp: mo_1s,
                mo_5s_pp: mo_5s,
                mo_10s_pp: mo_10s,
                mo_30s_pp: mo_30s,
                mo_60s_pp: mo_60s,
                tags: self.tags,
            };
        }
        let [pnl_1s, pnl_5s, pnl_10s, pnl_30s, pnl_60s] = [
            signed_markout(self.side, self.price, mid_1s),
            signed_markout(self.side, self.price, mid_5s),
            signed_markout(self.side, self.price, mid_10s),
            signed_markout(self.side, self.price, mid_30s),
            signed_markout(self.side, self.price, mid_60s),
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
            tags: self.tags,
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
    tags: ExperimentTags,
    quoted: bool,
}

/// One forward observation to track, carrying its A/B variant so CONTROL and
/// QUANT_V1 complete into separately labeled rows.
///
/// Maker quotes (`quoted = true`) reference the resting quote price.
/// Signal observations (`quoted = false`) reference the eval-time mid for
/// every usable Jev evaluation, including SKIP: they power SIGNAL RESEARCH
/// (all evaluations → future drift) while maker rows power TRADING RESEARCH
/// (quotes/fills → maker markouts → PnL). Signal rows always use
/// BUY-signed drift; conditioning on e.g. `underreact_down` flips at analysis.
#[derive(Debug, Clone)]
pub struct MarkoutQuote {
    pub condition_id: String,
    pub jev_ts: i64,
    pub side: TradeSide,
    pub price: PriceTicks,
    pub size: u64,
    pub quoted_at_ms: i64,
    pub variant: Variant,
    pub tags: ExperimentTags,
    pub quoted: bool,
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
            tags: ExperimentTags::default(),
            quoted: true,
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
            tags: quote.tags,
            quoted: quote.quoted,
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
                    tags: pending.tags,
                    quoted: pending.quoted,
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
///
/// One `Pipeline` serves exactly one contract: its `SignalActor` owns that
/// market's `state_seq`, its `ExecutionActor` owns that market's two
/// variant books, and evaluated rows carry this pipeline's `run_id` plus the
/// per-step `pair_id`; unpaired early rows retain contract tags. Multi-market
/// isolation is by construction:
/// N contracts run N pipelines fed by shared asset feeds.
pub struct Pipeline<E = SignalActor> {
    run_id: String,
    signal_actor: E,
    execution_actor: ExecutionActor,
    questdb: Option<QuestDbHandle>,
    api_key: String,
    deadline: Duration,
    thresholds: QuoteThresholds,
    quant: QuantConfig,
    poly_history: PolyHistory,
    markouts: MarkoutTracker,
    portfolios: PortfolioRegistry,
    last_tags: Option<ExperimentTags>,
    last_condition_id: Option<String>,
    last_observed_at_ms: Option<i64>,
    // A no-QuestDB pipeline keeps an in-memory event log for deterministic
    // paper-run verification; production pipelines use the async writer.
    event_log: Option<std::cell::RefCell<Vec<StorageEvent>>>,
    #[cfg(test)]
    persisted_events: std::cell::RefCell<Vec<StorageEvent>>,
}

impl Pipeline<SignalActor> {
    /// Creates the production pipeline. Execution remains paper-only because
    /// [`ExecutionActor`] owns only [`crate::execution::PaperBook`]s.
    // Nine fixed pipeline dependencies; a params struct would only rename them.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        run_id: impl Into<String>,
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
            run_id,
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

    /// Returns this pipeline's run identifier.
    #[must_use]
    pub fn run_id(&self) -> &str {
        &self.run_id
    }
}

impl<E: SignalEvaluator> Pipeline<E> {
    // Same nine dependencies as `new`; see the note there.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn with_evaluator(
        run_id: impl Into<String>,
        signal_actor: E,
        execution_actor: ExecutionActor,
        questdb: Option<QuestDbHandle>,
        api_key: impl Into<String>,
        deadline: Duration,
        thresholds: QuoteThresholds,
        quant: QuantConfig,
        poly_history_capacity: usize,
    ) -> Self {
        let capture_events = questdb.is_none();
        Self {
            run_id: run_id.into(),
            signal_actor,
            execution_actor,
            questdb,
            api_key: api_key.into(),
            deadline,
            thresholds,
            quant,
            poly_history: PolyHistory::new(poly_history_capacity),
            markouts: MarkoutTracker::new(),
            portfolios: PortfolioRegistry::new(),
            last_tags: None,
            last_condition_id: None,
            last_observed_at_ms: None,
            event_log: capture_events.then(|| std::cell::RefCell::new(Vec::new())),
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
            return self.finish_non_evaluated_step(&input, outcome, None, markouts_emitted);
        }

        let Some(candidate_price) = candidate_maker_price(&input.snapshot.book, input.tick_size)
        else {
            // The guard above makes this unreachable, but keeping this branch
            // explicit avoids a panic if the guard and constructor diverge.
            let outcome = Outcome::Skip(SkipReason::InvalidCandidate);
            return self.finish_non_evaluated_step(&input, outcome, None, markouts_emitted);
        };
        let Some(poly_snapshot) = build_poly_snapshot(
            &input.snapshot,
            input.last_trade_price,
            &mut self.poly_history,
            input.observed_at_ms,
        ) else {
            let outcome = Outcome::Skip(SkipReason::InvalidCandidate);
            return self.finish_non_evaluated_step(&input, outcome, None, markouts_emitted);
        };

        let contract = contract_context(&input);
        let features = build_features_full(
            input.recent_ticks,
            &input.resolution,
            &contract,
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
                let tags = self.untagged(&input);
                self.persist_decision(
                    &input,
                    outcome,
                    input.observed_at_ms,
                    Some(&poly_snapshot),
                    Variant::Control,
                    &tags,
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
                return self.finish_non_evaluated_step(
                    &input,
                    outcome,
                    Some(&poly_snapshot),
                    markouts_emitted,
                );
            }
        };
        let control_state_hash = hash_text(&control_state_json);
        let pair_id = Self::pair_id(
            input.condition_id,
            input.observed_at_ms,
            &control_state_hash,
        );
        let tags = self.contract_tags(&input, &pair_id);

        if !self.quant.enabled {
            self.process_book_update(&input, &tags);
            let (evaluation, signal) = self.evaluate_branch(&control_state, input.market_id).await;
            if let Ok(evaluation) = &evaluation {
                self.persist_signal(
                    &input,
                    Variant::Control,
                    &control_state_hash,
                    &control_state_json,
                    &questions_json,
                    evaluation,
                    &tags,
                );
            }
            let outcome = self.finish_branch(
                &input,
                &poly_snapshot,
                Variant::Control,
                &evaluation,
                signal,
                &tags,
            );
            self.sample_equity(&input, &tags);
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
                return self.finish_non_evaluated_step(
                    &input,
                    outcome,
                    Some(&poly_snapshot),
                    markouts_emitted,
                );
            }
        };
        let quant_state_hash = hash_text(&quant_state_json);
        self.process_book_update(&input, &tags);

        // Both branches evaluate the same frozen feature snapshot; only the
        // `quant` block differs. Each branch executes into its own paper
        // book, so fills, inventory, and exposure never cross variants.
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
                &tags,
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
                &tags,
            );
        }

        let control_ok = matches!((&control_evaluation, &control_signal), (Ok(_), Some(_)));
        let quant_ok = matches!((&quant_evaluation, &quant_signal), (Ok(_), Some(_)));
        let pair_seq = control_evaluation
            .as_ref()
            .or(quant_evaluation.as_ref())
            .map(|evaluation| i64::try_from(evaluation.state_seq).unwrap_or(i64::MAX))
            .unwrap_or(0);
        self.persist_pair(&input, &tags, pair_seq, control_ok, quant_ok);

        let _control_outcome = self.finish_branch(
            &input,
            &poly_snapshot,
            Variant::Control,
            &control_evaluation,
            control_signal,
            &tags,
        );
        // QUANT_V1 outcome stays the primary step outcome when enabled.
        let outcome = self.finish_branch(
            &input,
            &poly_snapshot,
            Variant::QuantV1,
            &quant_evaluation,
            quant_signal,
            &tags,
        );
        self.sample_equity(&input, &tags);

        StepResult {
            outcome,
            jev_evaluated: true,
            markouts_emitted,
        }
    }

    /// Settles both variant portfolios at a caller-verified YES outcome.
    ///
    /// This is the binary convenience form of [`Self::settle_at`]. The
    /// pipeline does not infer resolution; see `settle_at` for the boundary.
    pub fn settle(&mut self, yes_won: bool) -> (f64, f64) {
        self.settle_at(if yes_won { 1.0 } else { 0.0 })
    }

    /// Settles both variant portfolios at a caller-verified outcome price.
    ///
    /// The pipeline does not infer resolution. The caller must obtain the
    /// outcome price from the venue's resolution source (YES `1.0`, NO `0.0`,
    /// UMA `FIFTY` `0.5`); the price is clamped to `[0, 1]`. Settlement
    /// closes the full current inventory and persists one final equity row per
    /// variant using the last contract tags observed by this pipeline.
    pub fn settle_at(&mut self, outcome_price: f64) -> (f64, f64) {
        let outcome_price = outcome_price.clamp(0.0, 1.0);
        let observed_at_ms = self.last_observed_at_ms.unwrap_or(0);
        let condition_id = self.last_condition_id.clone().unwrap_or_default();
        let tags = self
            .last_tags
            .clone()
            .unwrap_or_else(|| ExperimentTags::new(self.run_id.clone(), "", "", "", ""));
        let market_id = tags.market_id.clone();
        let mut realized = [0.0; 2];

        for (index, variant) in [Variant::Control, Variant::QuantV1].into_iter().enumerate() {
            let (stats, exposure) = {
                let portfolio = self.portfolios.portfolio(variant.as_str(), &market_id);
                let full_inventory = portfolio.inventory();
                portfolio.apply_exit(outcome_price, full_inventory, observed_at_ms);
                let stats = PortfolioStats::of(portfolio);
                (stats, portfolio_exposure(portfolio))
            };
            realized[index] = stats.realized_pp;
            self.enqueue(StorageEvent::PaperEquity {
                ts: millis_to_micros(observed_at_ms),
                condition_id: condition_id.clone(),
                variant,
                position: stats.inventory,
                realized_pnl: stats.realized_pp,
                unrealized_pnl: stats.unrealized_pp,
                total_pnl: stats.total_pp,
                exposure,
                tags: tags.clone(),
            });
        }

        (realized[0], realized[1])
    }

    /// Exposes the current replay-style stats for one variant's contract.
    ///
    /// This is intentionally read-only; fills and exits still flow through
    /// `run_step` and [`Self::settle`]. It also gives paper-run tests a public
    /// way to verify that the live pipeline and its registry agree.
    #[must_use]
    pub fn paper_stats(&self, variant: Variant) -> Option<PortfolioStats> {
        let market_id = self.last_tags.as_ref()?.market_id.as_str();
        self.portfolios
            .get(variant.as_str(), market_id)
            .map(PortfolioStats::of)
    }

    /// Returns captured events for a pipeline without a QuestDB writer.
    ///
    /// This is a test/integration seam: production constructors pass a writer
    /// and return an empty vector rather than retaining every event.
    #[must_use]
    pub fn captured_events(&self) -> Vec<StorageEvent> {
        self.event_log
            .as_ref()
            .map_or_else(Vec::new, |events| events.borrow().clone())
    }

    fn finish_non_evaluated_step(
        &mut self,
        input: &PipelineInput<'_>,
        outcome: Outcome,
        poly_snapshot: Option<&PolySnapshot>,
        markouts_emitted: usize,
    ) -> StepResult {
        let tags = self.untagged(input);
        self.process_book_update(input, &tags);
        self.persist_decision(
            input,
            outcome,
            input.observed_at_ms,
            poly_snapshot,
            Variant::Control,
            &tags,
        );
        self.sample_equity(input, &tags);
        StepResult {
            outcome,
            jev_evaluated: false,
            markouts_emitted,
        }
    }

    /// Applies the current resolved book snapshot to both paper lanes.
    ///
    /// The live loop has no independently attributed aggressor flow, so
    /// `marketable_size` stays zero. Fills here are therefore trade-through
    /// fills consuming displayed `ask_size` FIFO. Touch partials require a
    /// future aggressor-flow attribution source and are intentionally left as
    /// a follow-up rather than fabricated from a quote update.
    fn process_book_update(&mut self, input: &PipelineInput<'_>, tags: &ExperimentTags) {
        self.last_tags = Some(tags.clone());
        self.last_condition_id = Some(input.condition_id.to_owned());
        self.last_observed_at_ms = Some(input.observed_at_ms);

        let bid_size = input
            .snapshot
            .book
            .bids()
            .iter()
            .map(|(_, quantity)| *quantity)
            .sum();
        let ask_size = input
            .snapshot
            .book
            .asks()
            .iter()
            .map(|(_, quantity)| *quantity)
            .sum();
        let update = TopOfBookUpdate::new(
            input.observed_at_ms,
            input.snapshot.book.best_bid(),
            bid_size,
            input.snapshot.book.best_ask(),
            ask_size,
            0,
        );
        let fills = self.execution_actor.on_book_update(update);
        self.persist_fills(input, fills, tags);

        let mid = input.mid.or_else(|| {
            coherent_mid(
                input.snapshot.book.best_bid()?,
                input.snapshot.book.best_ask()?,
            )
        });
        if let Some(mid) = mid {
            for variant in [Variant::Control, Variant::QuantV1] {
                self.portfolios
                    .portfolio(variant.as_str(), &tags.market_id)
                    .observe_mid(mid.to_f64());
            }
        }
    }

    fn persist_fills(
        &mut self,
        input: &PipelineInput<'_>,
        fills: DualFills,
        tags: &ExperimentTags,
    ) {
        self.persist_variant_fills(input, Variant::Control, &fills.control, tags);
        self.persist_variant_fills(input, Variant::QuantV1, &fills.quant, tags);
    }

    fn persist_variant_fills(
        &mut self,
        input: &PipelineInput<'_>,
        variant: Variant,
        fills: &[crate::execution::PaperFill],
        tags: &ExperimentTags,
    ) {
        for fill in fills {
            self.portfolios
                .portfolio(variant.as_str(), &tags.market_id)
                .apply_fill(FillEvent {
                    price: fill.price.to_f64(),
                    size: fill.size as f64,
                    ts_ms: fill.filled_at_ms,
                    // Toxicity is classified from the subsequent markout,
                    // not from this book update.
                    toxic: false,
                });
            self.enqueue(StorageEvent::PaperFill {
                ts: millis_to_micros(fill.filled_at_ms),
                order_id: i64::try_from(fill.order_id).unwrap_or(i64::MAX),
                condition_id: input.condition_id.to_owned(),
                variant,
                side: "BUY".to_owned(),
                price: fill.price.to_f64(),
                size: fill.size as f64,
                filled_at_ms: fill.filled_at_ms,
                maker: i64::from(fill.maker),
                tags: tags.clone(),
            });
        }
    }

    fn sample_equity(&mut self, input: &PipelineInput<'_>, tags: &ExperimentTags) {
        for variant in [Variant::Control, Variant::QuantV1] {
            let (stats, exposure) = {
                let portfolio = self.portfolios.portfolio(variant.as_str(), &tags.market_id);
                let stats = PortfolioStats::of(portfolio);
                (stats, portfolio_exposure(portfolio))
            };
            self.enqueue(StorageEvent::PaperEquity {
                ts: millis_to_micros(input.observed_at_ms),
                condition_id: input.condition_id.to_owned(),
                variant,
                position: stats.inventory,
                realized_pnl: stats.realized_pp,
                unrealized_pnl: stats.unrealized_pp,
                total_pnl: stats.total_pp,
                exposure,
                tags: tags.clone(),
            });
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

    // Eight fixed persist arguments; a params struct would only rename them.
    #[allow(clippy::too_many_arguments)]
    fn persist_signal(
        &self,
        input: &PipelineInput<'_>,
        variant: Variant,
        state_hash: &str,
        state_json: &str,
        questions_json: &str,
        evaluation: &JevEvaluation,
        tags: &ExperimentTags,
    ) {
        self.enqueue(StorageEvent::jev_signal_tagged(
            millis_to_micros(evaluation.received_at_ms),
            input.condition_id,
            state_hash.to_owned(),
            state_json.to_owned(),
            questions_json.to_owned(),
            i64::try_from(evaluation.state_seq).unwrap_or(i64::MAX),
            i64::try_from(evaluation.latency_ms).unwrap_or(i64::MAX),
            input.trigger,
            variant,
            tags.clone(),
            &evaluation.signal,
            evaluation.tokens_in,
            evaluation.tokens_out,
        ));
    }

    /// Tags for rows that never formed an A/B pair (early skips, serialization
    /// failures): contract dimensions are real, `pair_id` is empty.
    fn untagged(&self, input: &PipelineInput<'_>) -> ExperimentTags {
        self.contract_tags(input, "")
    }

    /// Full experiment tags for one evaluated snapshot.
    fn contract_tags(&self, input: &PipelineInput<'_>, pair_id: &str) -> ExperimentTags {
        let asset = input
            .market_spec
            .asset()
            .map(|asset| asset.as_str().to_owned())
            .unwrap_or_else(|| "UNKNOWN".to_owned());
        let horizon = input
            .market_spec
            .horizon()
            .map(|horizon| horizon.as_str().to_owned())
            .unwrap_or_else(|| "UNKNOWN".to_owned());
        let market_id = input
            .market_spec
            .market_key()
            .map(|key| key.market_id())
            .unwrap_or_else(|| input.market_id.to_owned());
        ExperimentTags::new(self.run_id.clone(), pair_id, market_id, asset, horizon)
    }

    /// Explicit pair identity shared by both branches of one snapshot.
    ///
    /// Derived from the condition, the caller timestamp, and the exact
    /// CONTROL state hash — never from consecutive `state_seq` values, which
    /// are per-market and cannot join branches.
    fn pair_id(condition_id: &str, observed_at_ms: i64, state_hash: &str) -> String {
        let short: String = state_hash.chars().take(12).collect();
        format!("{condition_id}:{observed_at_ms}:{short}")
    }

    /// Persists one A/B pair lifecycle row. Called only when both branches
    /// were attempted; `status` is `complete` only when both produced a
    /// usable signal. Incomplete pairs are excluded from paired analysis.
    fn persist_pair(
        &self,
        input: &PipelineInput<'_>,
        tags: &ExperimentTags,
        state_seq: i64,
        control_ok: bool,
        quant_ok: bool,
    ) {
        self.enqueue(StorageEvent::AbPair {
            ts: millis_to_micros(input.observed_at_ms),
            run_id: tags.run_id.clone(),
            pair_id: tags.pair_id.clone(),
            market_id: tags.market_id.clone(),
            asset: tags.asset.clone(),
            horizon: tags.horizon.clone(),
            condition_id: input.condition_id.to_owned(),
            state_seq,
            observed_at_ms: input.observed_at_ms,
            status: if control_ok && quant_ok {
                "complete".to_owned()
            } else {
                "incomplete".to_owned()
            },
            control_ok: i64::from(control_ok),
            quant_ok: i64::from(quant_ok),
        });
    }

    fn finish_branch(
        &mut self,
        input: &PipelineInput<'_>,
        poly_snapshot: &PolySnapshot,
        variant: Variant,
        evaluation: &Result<JevEvaluation, JevError>,
        signal: Option<V1Signal>,
        tags: &ExperimentTags,
    ) -> Outcome {
        // SIGNAL RESEARCH tracks every usable evaluation (QUOTE or SKIP)
        // against the eval-time mid; TRADING RESEARCH tracks resting quotes
        // against their quote price. Both complete into separately labeled
        // rows joined by pair_id + variant.
        let outcome = match (evaluation, signal) {
            (Err(_), _) => Outcome::Skip(SkipReason::JevError),
            (Ok(_), None) => Outcome::Skip(SkipReason::UnusableSignal),
            (Ok(evaluation), Some(signal)) => {
                if let Some(ref_mid) = input.mid {
                    self.markouts.add_quote_with_variant(MarkoutQuote {
                        condition_id: input.condition_id.to_owned(),
                        jev_ts: millis_to_micros(evaluation.received_at_ms),
                        side: TradeSide::Buy,
                        price: ref_mid,
                        size: 0,
                        quoted_at_ms: input.observed_at_ms,
                        variant,
                        tags: tags.clone(),
                        quoted: false,
                    });
                }
                let decision = decide(DecisionInput {
                    signal: &signal,
                    market: &input.snapshot,
                    thresholds: &self.thresholds,
                    tick_size: input.tick_size,
                    size: input.size,
                });
                // Each variant executes into its own paper book: `on_quote`
                // is the sole runtime RiskGate invocation per lane.
                match decision {
                    Outcome::Quote(intent) => match self.execution_actor.on_quote(
                        variant,
                        intent,
                        input.snapshot.stale || input.snapshot.book.is_stale(),
                        evaluation.latency_ms,
                    ) {
                        Ok(_) => Outcome::Quote(intent),
                        Err(block) => Outcome::Skip(SkipReason::RiskBlocked(block)),
                    },
                    Outcome::Skip(reason) => Outcome::Skip(reason),
                }
            }
        };
        let jev_ts_ms = evaluation
            .as_ref()
            .map_or(input.observed_at_ms, |evaluation| evaluation.received_at_ms);
        self.persist_decision(
            input,
            outcome,
            jev_ts_ms,
            Some(poly_snapshot),
            variant,
            tags,
        );
        if let Outcome::Quote(intent) = outcome {
            self.markouts.add_quote_with_variant(MarkoutQuote {
                condition_id: input.condition_id.to_owned(),
                jev_ts: millis_to_micros(jev_ts_ms),
                side: intent.side,
                price: intent.price,
                size: intent.size,
                quoted_at_ms: input.observed_at_ms,
                variant,
                tags: tags.clone(),
                quoted: true,
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
        tags: &ExperimentTags,
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
            // Persist the primary underreact gate used by decide_quote, not a
            // placeholder. This is an audit value, not a new runtime rule.
            threshold: self.thresholds.under_min,
            decision: outcome.decision_name().to_owned(),
            paper_price,
            size: input.size as f64,
            fair_value,
            tags: tags.clone(),
        });
        let _ = (input.market_id, outcome);
    }

    fn enqueue(&self, event: StorageEvent) {
        if let Some(events) = &self.event_log {
            events.borrow_mut().push(event.clone());
        }
        #[cfg(test)]
        self.persisted_events.borrow_mut().push(event.clone());
        if let Some(questdb) = &self.questdb {
            let _ = questdb.try_send(event);
        }
    }
}

/// Computes open inventory notional at its average entry price.
///
/// This keeps `exposure` stable when no current mid is available; mark-to-mid
/// PnL remains the separate `unrealized_pnl` field.
fn portfolio_exposure(portfolio: &Portfolio) -> f64 {
    let inventory = portfolio.inventory();
    if inventory <= 0.0 {
        return 0.0;
    }
    let bought_size = portfolio.fills.iter().map(|fill| fill.size).sum::<f64>();
    if bought_size <= 0.0 {
        return 0.0;
    }
    let bought_notional = portfolio
        .fills
        .iter()
        .map(|fill| fill.price * fill.size)
        .sum::<f64>();
    bought_notional / bought_size * inventory
}

/// Derives the contract label for feature building from the market spec.
///
/// Unknown contracts yield empty labels and a zero window; the builder then
/// emits the documented `0.0`/`""` horizon fields. Never a threshold input.
fn contract_context(input: &PipelineInput<'_>) -> ContractContext {
    let (asset_symbol, horizon_label, horizon_secs) =
        match (input.market_spec.asset(), input.market_spec.horizon()) {
            (Some(asset), Some(horizon)) => (
                asset.as_str().to_owned(),
                horizon.as_str().to_owned(),
                horizon.seconds(),
            ),
            _ => (String::new(), String::new(), 0),
        };
    ContractContext::new(asset_symbol, horizon_label, horizon_secs)
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
            asset: None,
            horizon: None,
            reference_source: None,
            window_secs: None,
            start_ms: None,
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
            "test-run",
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
            tokens_in: 0,
            tokens_out: 0,
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
        // Each variant executes into its own book: one resting quote each.
        assert_eq!(pipeline.execution_actor.outstanding(Variant::Control), 1);
        assert_eq!(pipeline.execution_actor.outstanding(Variant::QuantV1), 1);
        // Two maker tracks plus two signal tracks (one per evaluated branch).
        assert_eq!(pipeline.markouts.pending(), 4);

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

        // Both branches share one explicit pair_id; the pair row is complete.
        let pair_tags: Vec<(String, String)> = events
            .iter()
            .filter_map(|event| match event {
                StorageEvent::JevSignal { tags, .. } => {
                    Some((tags.pair_id.clone(), tags.run_id.clone()))
                }
                _ => None,
            })
            .collect();
        assert_eq!(pair_tags.len(), 2);
        assert_eq!(pair_tags[0], pair_tags[1]);
        assert!(!pair_tags[0].0.is_empty());
        assert_eq!(pair_tags[0].1, "test-run");

        let pairs: Vec<(String, String, i64, i64)> = events
            .iter()
            .filter_map(|event| match event {
                StorageEvent::AbPair {
                    pair_id,
                    status,
                    control_ok,
                    quant_ok,
                    ..
                } => Some((pair_id.clone(), status.clone(), *control_ok, *quant_ok)),
                _ => None,
            })
            .collect();
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].0, pair_tags[0].0);
        assert_eq!(pairs[0].1, "complete");
        assert_eq!((pairs[0].2, pairs[0].3), (1, 1));
    }

    #[tokio::test]
    async fn failed_branch_marks_pair_incomplete() {
        let evaluator = FakeEvaluator {
            result: None,
            // CONTROL succeeds, QUANT_V1 fails: same frozen snapshot.
            queued_results: vec![Ok(evaluation()), Err(JevError::Deadline)],
            signal: Some(signal()),
            calls: 0,
        };
        let mut pipeline = pipeline(evaluator);
        pipeline.quant.enabled = true;

        let result = pipeline
            .run_step(input(market(false, 0.40, 0.45), 25, 1_000))
            .await;

        // QUANT outcome is a Jev-error skip; CONTROL still quoted alone.
        assert_eq!(result.outcome, Outcome::Skip(SkipReason::JevError));
        assert_eq!(pipeline.signal_actor.calls, 2);

        let events = pipeline.persisted_events.borrow();
        let statuses: Vec<(String, i64, i64)> = events
            .iter()
            .filter_map(|event| match event {
                StorageEvent::AbPair {
                    status,
                    control_ok,
                    quant_ok,
                    ..
                } => Some((status.clone(), *control_ok, *quant_ok)),
                _ => None,
            })
            .collect();
        assert_eq!(statuses, vec![("incomplete".to_owned(), 1, 0)]);

        // Only the CONTROL signal row was persisted; the pair row still joins it.
        let signal_pairs: Vec<String> = events
            .iter()
            .filter_map(|event| match event {
                StorageEvent::JevSignal { tags, .. } => Some(tags.pair_id.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(signal_pairs.len(), 1);
        let ab_pair_ids: Vec<String> = events
            .iter()
            .filter_map(|event| match event {
                StorageEvent::AbPair { pair_id, .. } => Some(pair_id.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(ab_pair_ids, signal_pairs);
    }

    #[tokio::test]
    async fn skip_evaluation_still_yields_signal_markouts() {
        // Below-threshold signal: strategy SKIPs, but SIGNAL RESEARCH still
        // tracks forward drift from the eval-time mid.
        let weak = V1Signal {
            underreact_up: 0.31,
            ..signal()
        };
        let evaluator = FakeEvaluator {
            result: None,
            queued_results: (0..6).map(|_| Ok(evaluation())).collect(),
            signal: Some(weak),
            calls: 0,
        };
        let mut pipeline = pipeline(evaluator);

        for at_ms in [1_000, 2_000, 6_000, 11_000, 31_000, 61_000] {
            let result = pipeline
                .run_step(input(market(false, 0.40, 0.45), 25, at_ms))
                .await;
            assert_eq!(result.outcome, Outcome::Skip(SkipReason::StrategyRejected));
        }

        let events = pipeline.persisted_events.borrow();
        let signal_rows: Vec<(Variant, f64)> = events
            .iter()
            .filter_map(|event| match event {
                StorageEvent::SignalMarkout {
                    variant, ref_price, ..
                } => Some((*variant, *ref_price)),
                _ => None,
            })
            .collect();
        assert_eq!(signal_rows.len(), 1);
        assert_eq!(signal_rows[0].0, Variant::Control);
        assert!((signal_rows[0].1 - 0.41).abs() < 1e-12);
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, StorageEvent::MakerMarkout { .. })),
            "SKIP steps must not emit maker rows"
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
