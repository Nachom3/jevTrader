//! ReplayRunner: rolling state -> features -> QUANT pairing -> Jev w/latency.

use super::clock::ReplayClock;
use super::fills::{ExecutionLatency, FillSimulator, RestingOrder};
use super::jev_cache::{CachedJev, JevCache, JevCacheKey};
use super::markouts::signed_markouts_pp;
use super::report::ReportRow;
use super::types::{
    Coverage, Fidelity, FillProfile, HistoricalEvent, LatencyDistribution, LatencyProfile, Split,
};
use crate::config::{QuantConfig, QuoteThresholds};
use crate::domain::{PriceTicks, TickSize};
use crate::engine::MarketSnapshot;
use crate::engine::pipeline::{DecisionInput, MarkoutTracker, candidate_maker_price, decide};
use crate::engine::signal_actor::StalenessPolicy;
use crate::jev::request::{QuestionSet, V1State};
use crate::jev::response::{
    SystemOneResponse, V1Signal, parse_fair_p_yes, parse_pressure, parse_v1_signal,
};
use crate::polymarket::OrderBook;
use crate::state::feature_builder::{
    ContractContext, ExternalTick, OrderFlowAggregates, ResolutionContext, VenueMicroprices,
    build_features_micro, build_features_with_context,
};
use crate::state::poly_history::PolyHistory;
use crate::state::quant_features::build_quant;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::Duration;

/// One Jev evaluation: transport result plus the raw response envelope.
///
/// `latency_ms` is measured wall time for live calls and the configured
/// profile assumption for stubs/cached replays. `live` is true only for a
/// real TypeSafe call. `error` carries transport/budget failures; parsing
/// is the runner's job per arm, so a populated envelope may still fail
/// arm-specific validation downstream. `envelope_json` is the verbatim
/// System One response envelope (`{"answers": {...}}`).
#[derive(Debug, Clone)]
pub struct RawOutcome {
    pub latency_ms: u64,
    pub live: bool,
    pub error: Option<String>,
    pub envelope_json: String,
}

/// How the runner obtains Jev evaluations (real client or deterministic stub).
///
/// The runner hands over a fully built [`V1State`] plus the arm's question
/// set as wire-shape JSON. Stubs may ignore both and synthesize from hashes
/// instead. `assumed_latency_ms` is the replay profile value, used only when
/// no measured latency exists.
pub trait JevEvaluator {
    fn evaluate(
        &mut self,
        state: &V1State,
        market_id: &str,
        state_seq: u64,
        variant: &str,
        questions: &serde_json::Value,
        assumed_latency_ms: u64,
    ) -> RawOutcome;
}

/// One replay observation with V2 microstructure context.
///
/// `spot`/`spot_flow` come from the spot leg, `perp` is the as-of perp price
/// (falls back to `spot` when perp coverage is missing, i.e. V1 behavior),
/// and `poly_flow` carries 5s Poly-tape aggregates (5s vols sit in the vol
/// slots; 1s slots stay 0.0 on sparse tape). Synthetic/bootstrap paths fill
/// V2 fields with neutral values (perp = spot, zero flows).
#[derive(Debug, Clone, PartialEq)]
pub struct SyntheticItem {
    pub ts_ms: i64,
    pub book_bid: f64,
    pub book_ask: f64,
    pub spot: f64,
    pub perp: f64,
    pub spot_flow: OrderFlowAggregates,
    pub poly_flow: OrderFlowAggregates,
    pub market_id: String,
    pub asset: String,
    pub horizon: String,
    pub split: Split,
    pub fidelity: Fidelity,
    pub regime: String,
}

impl SyntheticItem {
    /// Neutral V2 context for synthetic/bootstrap items (V1-equivalent).
    #[must_use]
    pub fn neutral_flow() -> OrderFlowAggregates {
        OrderFlowAggregates {
            buy_vol_1s: 0.0,
            sell_vol_1s: 0.0,
            ofi_1s: 0.0,
            ofi_5s: 0.0,
            imbalance: 0.0,
            aggressive_buy_ratio: 0.5,
        }
    }
}

/// Deterministic stub: maps state_hash -> stable pseudo-signal (no network,
/// no cost). Smoke-valid; never presented as alpha evidence.
#[derive(Debug, Clone, Copy)]
pub struct StubJev {
    pub seed: u64,
}

impl StubJev {
    #[must_use]
    pub const fn new(seed: u64) -> Self {
        Self { seed }
    }

    fn pseudo(&self, state_json: &str) -> f64 {
        let mut h = DefaultHasher::new();
        self.seed.hash(&mut h);
        state_json.hash(&mut h);
        let v = h.finish();
        (v % 10_000) as f64 / 10_000.0
    }
}

impl JevEvaluator for StubJev {
    fn evaluate(
        &mut self,
        state: &V1State,
        _market_id: &str,
        _state_seq: u64,
        _variant: &str,
        questions: &serde_json::Value,
        assumed_latency_ms: u64,
    ) -> RawOutcome {
        // Deterministic envelope synthesized per (state, question): same
        // state + same question set => same pseudo-answers. Smoke-valid;
        // never presented as alpha evidence.
        let state_json = serde_json::to_string(state).unwrap_or_default();
        let mut answers = serde_json::Map::new();
        if let Some(object) = questions.as_object() {
            for (qid, question) in object {
                let kind = question
                    .get("type")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("");
                let base = self.pseudo(&format!("{state_json}{qid}"));
                let clamp = |v: f64| v.clamp(0.0, 1.0);
                if kind == "noul" {
                    answers.insert(
                        qid.clone(),
                        serde_json::json!({
                            "type": "noul",
                            "noul": clamp(0.3 + base * 0.5),
                        }),
                    );
                } else if kind == "choice" {
                    let mut probabilities = serde_json::Map::new();
                    let mut keys: Vec<&String> = question
                        .get("criteria")
                        .and_then(serde_json::Value::as_object)
                        .map(|criteria| criteria.keys().collect())
                        .unwrap_or_default();
                    keys.sort();
                    if keys.is_empty() {
                        keys.push(qid);
                    }
                    let share = 1.0 / keys.len() as f64;
                    for (index, key) in keys.iter().enumerate() {
                        let jitter = (base + index as f64 * 0.037) % 1.0;
                        probabilities.insert(
                            (*key).clone(),
                            serde_json::json!(clamp(share * (0.5 + jitter))),
                        );
                    }
                    // Renormalize: synthesized distributions must validate.
                    let total: f64 = probabilities
                        .values()
                        .filter_map(serde_json::Value::as_f64)
                        .sum();
                    if total > 0.0 {
                        for value in probabilities.values_mut() {
                            if let Some(probability) = value.as_f64() {
                                *value = serde_json::json!(probability / total);
                            }
                        }
                    }
                    answers.insert(
                        qid.clone(),
                        serde_json::json!({
                            "type": "choice",
                            "probabilities": probabilities,
                            "confidence": 0.5,
                        }),
                    );
                }
            }
        }
        RawOutcome {
            latency_ms: assumed_latency_ms,
            live: false,
            error: None,
            envelope_json: serde_json::json!({"answers": answers}).to_string(),
        }
    }
}

/// Live TypeSafe evaluator with a hard call budget.
///
/// Each `evaluate` performs one real HTTP call and records its measured
/// latency. When `max_calls` is exhausted the evaluator returns a neutral
/// error outcome (the runner SKIPs it) instead of spending more.
pub struct RealJev {
    api_key: String,
    deadline: Duration,
    runtime: tokio::runtime::Runtime,
    pub calls: u64,
    pub max_calls: u64,
    /// Measured live latencies in ms, one per successful call, in order.
    /// The large run persists these as the empirical latency distribution.
    pub latencies_ms: Vec<u64>,
}

impl RealJev {
    /// Creates the evaluator; fails loudly without an API key.
    pub fn new(api_key: String, deadline: Duration, max_calls: u64) -> Result<Self, String> {
        if api_key.trim().is_empty() {
            return Err("TYPESAFE_API_KEY is missing or empty".to_owned());
        }
        if max_calls == 0 {
            return Err("max_calls must be greater than zero".to_owned());
        }
        let runtime = tokio::runtime::Runtime::new().map_err(|e| format!("tokio runtime: {e}"))?;
        Ok(Self {
            api_key,
            deadline,
            runtime,
            calls: 0,
            max_calls,
            latencies_ms: Vec::new(),
        })
    }
}

impl JevEvaluator for RealJev {
    fn evaluate(
        &mut self,
        state: &V1State,
        market_id: &str,
        state_seq: u64,
        variant: &str,
        questions: &serde_json::Value,
        assumed_latency_ms: u64,
    ) -> RawOutcome {
        if self.calls >= self.max_calls {
            return RawOutcome {
                latency_ms: assumed_latency_ms,
                live: false,
                error: Some(format!(
                    "Jev budget exhausted ({}/{})",
                    self.calls, self.max_calls
                )),
                envelope_json: String::new(),
            };
        }
        self.calls += 1;
        let result = self.runtime.block_on(crate::jev::client::post(
            state,
            questions,
            &self.api_key,
            self.deadline,
        ));
        // Per-call observability for budgeted live runs. Real judgments
        // are the only alpha evidence; parsed values are logged by the
        // runner after arm-specific validation.
        let verbose = std::env::var("JEV_VERBOSE").is_ok();
        match result {
            Ok((body, sent_at_ms, received_at_ms)) => {
                let latency_ms = received_at_ms.saturating_sub(sent_at_ms).max(0) as u64;
                self.latencies_ms.push(latency_ms);
                if verbose {
                    eprintln!(
                        "JEV LIVE market={market_id} seq={state_seq} variant={variant} latency_ms={latency_ms}"
                    );
                }
                RawOutcome {
                    latency_ms,
                    live: true,
                    error: None,
                    envelope_json: String::from_utf8_lossy(&body).into_owned(),
                }
            }
            Err(error) => {
                if verbose {
                    eprintln!(
                        "JEV ERROR market={market_id} seq={state_seq} variant={variant}: {error}"
                    );
                }
                RawOutcome {
                    latency_ms: assumed_latency_ms,
                    live: false,
                    error: Some(error.to_string()),
                    envelope_json: String::new(),
                }
            }
        }
    }
}

/// Replay configuration (thresholds frozen; never tuned on OOS).
#[derive(Debug, Clone)]
pub struct ReplayConfig {
    pub run_id: String,
    pub manifest_id: String,
    pub thresholds: QuoteThresholds,
    pub quant: QuantConfig,
    pub fill: FillProfile,
    pub latency: LatencyProfile,
    /// Empirical Jev latency samples; honored only when `latency` is Empirical.
    pub latency_distribution: LatencyDistribution,
    /// Pair namespace for multi-call runs (e.g. one condition per call in
    /// run_corpus). `pair_id` becomes `{run_id}-{namespace}-{seq}` so pairs
    /// never collide across calls; the caller owns namespace uniqueness
    /// (run_corpus uses condition_id). Empty preserves single-call behavior.
    pub pair_namespace: String,
    /// Arms evaluated on every snapshot (V3 runs override with V3_ARMS).
    /// Smoke default is the V1/V2 trio; row counts divide by its length.
    pub arms: Vec<Arm>,
    pub tick_size: TickSize,
    pub size: u64,
    pub max_pairs: usize,
    pub coverage: Coverage,
    pub staleness: StalenessPolicy,
}

impl ReplayConfig {
    #[must_use]
    pub fn smoke(run_id: &str) -> Self {
        Self {
            run_id: run_id.to_owned(),
            manifest_id: "bootstrap".to_owned(),
            thresholds: QuoteThresholds::default(),
            quant: QuantConfig::default(),
            fill: FillProfile::Conservative,
            latency: LatencyProfile::Base,
            latency_distribution: LatencyDistribution::default(),
            pair_namespace: String::new(),
            arms: ARMS.to_vec(),
            tick_size: TickSize::from_f64(0.01),
            size: 10,
            max_pairs: 100,
            coverage: Coverage::BinanceOnly,
            staleness: StalenessPolicy::default(),
        }
    }
}

/// One evaluated (state, signal) pair for the diagnostic sidecar.
///
/// Persisted verbatim (no thresholding, no aggregation) so offline analysis
/// can study Jev's raw answers: distributions, state-variance, QUANT-CONTROL
/// deltas, and drift correlations. `state_json` is the exact evaluated
/// V1State payload.
#[derive(Debug, Clone, serde::Serialize)]
pub struct SignalRecord {
    pub pair_id: String,
    pub variant: String,
    pub market_id: String,
    pub state_hash: String,
    pub state_json: String,
    pub signal: V1Signal,
    pub jev_latency_ms: u64,
    pub live: bool,
}

/// One replay arm: a named state variant evaluated on every snapshot.
///
/// CONTROL is the frozen V1 state (reference arm, byte-identical to v1).
/// MICRO_V2 carries real microstructure (OFI/flow, perp, fixed distance).
/// MICRO_V2_QUANT adds quant enrichment on the micro state (secondary).
/// V3 arms share the MICRO state and vary only the question set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Arm {
    pub name: &'static str,
    pub quant: bool,
    pub micro: bool,
    pub questions: QuestionSet,
}

/// The evaluated arms, in row-emission order. Row counts divide by this.
pub const ARMS: [Arm; 3] = [
    Arm {
        name: "CONTROL",
        quant: false,
        micro: false,
        questions: QuestionSet::V1,
    },
    Arm {
        name: "MICRO_V2",
        quant: false,
        micro: true,
        questions: QuestionSet::V1,
    },
    Arm {
        name: "MICRO_V2_QUANT",
        quant: true,
        micro: true,
        questions: QuestionSet::V1,
    },
];

/// Parsed per-arm judgment. V1 carries the eight validated answers;
/// Fair carries the calibrated fair P(YES); Pressure carries the expected
/// bipolar value plus the distribution confidence.
enum ParsedArm {
    V1(V1Signal),
    Fair(f64),
    Pressure { expectation: f64, confidence: f64 },
}

/// Validates one raw envelope against the arm's question set. Values pass
/// through verbatim; no renormalization, no thresholding, no decisions.
fn parse_arm_envelope(questions: QuestionSet, envelope_json: &str) -> Result<ParsedArm, String> {
    let response: SystemOneResponse =
        serde_json::from_str(envelope_json).map_err(|e| format!("envelope decode: {e}"))?;
    match questions {
        QuestionSet::V1 => parse_v1_signal(&response)
            .map(ParsedArm::V1)
            .map_err(|e| e.to_string()),
        QuestionSet::FairValue => parse_fair_p_yes(&response)
            .map(ParsedArm::Fair)
            .map_err(|e| e.to_string()),
        QuestionSet::PressureComposite => parse_pressure(&response)
            .map(|(expectation, confidence)| ParsedArm::Pressure {
                expectation,
                confidence,
            })
            .map_err(|e| e.to_string()),
    }
}

/// V3 question-form arms: same MICRO state, different question sets.
/// No quant arm: quant proved ~0 and is out of this experiment's scope.
pub const V3_ARMS: [Arm; 3] = [
    Arm {
        name: "CONTROL",
        quant: false,
        micro: true,
        questions: QuestionSet::V1,
    },
    Arm {
        name: "FAIR_VALUE",
        quant: false,
        micro: true,
        questions: QuestionSet::FairValue,
    },
    Arm {
        name: "PRESSURE_COMPOSITE",
        quant: false,
        micro: true,
        questions: QuestionSet::PressureComposite,
    },
];

/// Output of one replay run.
#[derive(Debug)]
pub struct RunnerOutput {
    pub rows: Vec<ReportRow>,
    pub signals: Vec<SignalRecord>,
    pub jev_hits: u64,
    pub jev_misses: u64,
    pub stale_skips: usize,
    pub incomplete_pairs: usize,
    pub jev_errors: usize,
}

/// Event-driven replay over a synchronized event stream.
///
/// For each eligible snapshot the runner builds ONE frozen feature set, then
/// evaluates CONTROL (`quant=None`) and QUANT_V1 (`quant=Some`) on the same
/// timestamp/state/feeds/tape/questions/thresholds. Jev latency is honored:
/// a state built at T with latency L is usable only at T+L against the then
/// current book (stale -> SKIP, recorded).
pub struct ReplayRunner<E: JevEvaluator> {
    pub config: ReplayConfig,
    pub evaluator: E,
    pub cache: JevCache,
    pub fill_sim: FillSimulator,
}

impl<E: JevEvaluator> ReplayRunner<E> {
    #[must_use]
    pub fn new(config: ReplayConfig, evaluator: E) -> Self {
        let fill = config.fill;
        Self {
            config,
            evaluator,
            cache: JevCache::new(),
            fill_sim: FillSimulator::new(fill),
        }
    }

    /// Runs the replay over pre-synchronized rich items (V2 struct form).
    #[allow(clippy::too_many_arguments)]
    /// Evaluates one frozen snapshot for one arm and returns
    /// `(outcome, state_hash, state_json)`. The state JSON is the exact
    /// evaluated payload (already serialized for hashing); the diagnostic
    /// sidecar persists it verbatim for offline analysis.
    /// Latencies are measured for live calls and assumed otherwise; the
    /// Jev cache stores complete signals keyed by state + questions +
    /// model + variant.
    #[allow(clippy::too_many_arguments)]
    fn evaluate_arm(
        &mut self,
        features: &crate::strategy::lead_lag::LeadLagFeatures,
        poly: &crate::strategy::lead_lag::PolySnapshot,
        candidate: PriceTicks,
        question: &str,
        rules: &str,
        market_id: &str,
        seq: u64,
        arm: &Arm,
        quant: Option<crate::state::quant_features::QuantFeatures>,
        questions: &serde_json::Value,
        assumed_latency_ms: u64,
    ) -> (RawOutcome, String, String) {
        let state = V1State::new(
            question,
            rules,
            features.clone(),
            poly.clone(),
            quant,
            candidate,
        );
        let state_json = serde_json::to_string(&state).unwrap_or_default();
        let questions_json = serde_json::to_string(questions).unwrap_or_default();
        let qhash = hash_str(&questions_json);
        let hash = hash_str(&state_json);
        let key = JevCacheKey {
            state_hash: hash.clone(),
            questions_hash: qhash,
            model: "jev-latest".to_owned(),
            variant: arm.name.to_owned(),
        };
        let outcome = self.cached_or_evaluate(
            &key,
            &state,
            market_id,
            seq,
            arm.name,
            questions,
            assumed_latency_ms,
        );
        (outcome, hash, state_json)
    }

    /// Cache lookup with evaluator fallback; stores raw envelopes.
    #[allow(clippy::too_many_arguments)]
    fn cached_or_evaluate(
        &mut self,
        key: &JevCacheKey,
        state: &V1State,
        market_id: &str,
        seq: u64,
        variant: &str,
        questions: &serde_json::Value,
        assumed_latency_ms: u64,
    ) -> RawOutcome {
        if let Some(cached) = self.cache.get(key) {
            return RawOutcome {
                latency_ms: cached.latency_ms,
                live: cached.live,
                error: None,
                envelope_json: cached.envelope_json.clone(),
            };
        }
        let outcome = self.evaluator.evaluate(
            state,
            market_id,
            seq,
            variant,
            questions,
            assumed_latency_ms,
        );
        self.cache.put(
            key.clone(),
            CachedJev {
                envelope_json: outcome.envelope_json.clone(),
                latency_ms: outcome.latency_ms,
                live: outcome.live,
            },
        );
        outcome
    }

    pub fn run_synthetic(
        &mut self,
        items: &[SyntheticItem],
        resolution_at_ms: i64,
    ) -> RunnerOutput {
        self.run_synthetic_with(
            items,
            resolution_at_ms,
            &std::collections::HashMap::new(),
            &[],
            None,
        )
    }

    /// Replay with per-market questions (`market_id` -> `(question, rules)`)
    /// and an optional dense underlying history for the MICRO arm.
    ///
    /// `dense_ticks` (oldest-first) feeds MICRO return/vol windows; the V1
    /// arm always uses the legacy item-paced window. Synthetic callers pass
    /// an empty slice (MICRO rows go degenerate-SKIP there by design).
    pub fn run_synthetic_with(
        &mut self,
        items: &[SyntheticItem],
        resolution_at_ms: i64,
        questions: &std::collections::HashMap<String, (String, String)>,
        dense_ticks: &[ExternalTick],
        full_mids: Option<&[(i64, f64)]>,
    ) -> RunnerOutput {
        let mut rows = Vec::new();
        let mut signals: Vec<SignalRecord> = Vec::new();
        let mut stale_skips = 0usize;
        let mut incomplete = 0usize;
        let mut jev_errors = 0usize;
        let mut clock = ReplayClock::new(items.first().map_or(0, |i| i.ts_ms));
        let mut poly_hist = PolyHistory::new(1024);
        let mut underlying: Vec<(i64, f64)> = Vec::new();
        let mut mids: Vec<(i64, f64)> = Vec::new();
        // Full mid trajectory, known upfront because the corpus is
        // historical. Fills and markouts are post-facto LABELS evaluated
        // against later prints; the quote DECISION only ever uses history
        // accumulated through T (underlying, poly_hist, mids above).
        // Label trajectory for fills/markouts/drift: the caller's full
        // poly mid series when provided (strided runs), else the items
        // themselves (legacy: synthetic path and unstrided runs). Labels
        // are post-facto and never leak into the quote decision.
        let future_mids: Vec<(i64, f64)> = items
            .iter()
            .map(|it| (it.ts_ms, (it.book_bid + it.book_ask) / 2.0))
            .collect();
        let labels: &[(i64, f64)] = full_mids.unwrap_or(&future_mids);
        let markout_tracker = MarkoutTracker::new();
        let _ = markout_tracker;
        let jev_latency = self.config.latency.jev_latency_ms();
        let exec_latency = ExecutionLatency::new(self.config.latency.submit_latency_ms());
        // Stream-open spot: the MICRO distance reference (V2). The V1 arm
        // keeps target = current spot (frozen legacy behavior).
        let stream_open = items.first().map(|i| i.spot).unwrap_or(0.0);

        for (seq, it) in items.iter().enumerate() {
            if rows.len() / self.config.arms.len().max(1) >= self.config.max_pairs {
                break;
            }
            let ts = it.ts_ms;
            let (bid, ask, spot) = (it.book_bid, it.book_ask, it.spot);
            let (market_id, asset, horizon, regime) =
                (&it.market_id, &it.asset, &it.horizon, &it.regime);
            let (split, fidelity) = (it.split, it.fidelity);
            if clock.advance_to(ts).is_err() {
                incomplete += 1;
                continue;
            }
            underlying.push((ts, spot));
            let mid = (bid + ask) / 2.0;
            mids.push((ts, mid));
            poly_hist.push(ts.max(0) as u64, PriceTicks::from_f64(mid.clamp(0.0, 1.0)));

            // Rolling external ticks: backward-only window (no look-ahead).
            let ticks: Vec<ExternalTick> = underlying
                .iter()
                .rev()
                .take(600)
                .rev()
                .map(|(t, p)| ExternalTick {
                    price: *p,
                    ts_ms: *t as u64,
                })
                .collect();
            let ctx = ResolutionContext::new(
                spot,
                clock.time_remaining_secs(resolution_at_ms),
                "binance-replay".to_owned(),
            );
            let venues = VenueMicroprices {
                binance: spot,
                coinbase: spot,
                perp: spot,
                perp_basis_pct: 0.0,
            };
            let flow = OrderFlowAggregates {
                buy_vol_1s: 0.0,
                sell_vol_1s: 0.0,
                ofi_1s: 0.0,
                ofi_5s: 0.0,
                imbalance: 0.0,
                aggressive_buy_ratio: 0.5,
            };
            let features = build_features_with_context(&ticks, &ctx, venues, flow);

            // MICRO inputs (V2): dense history window, stream-open distance
            // reference, real perp venues, real spot+poly flow, real contract.
            // V1 names above stay frozen and byte-identical to v1.
            let dense_end = dense_ticks.partition_point(|t| t.ts_ms <= ts.max(0) as u64);
            let dense_start = dense_end.saturating_sub(3600);
            let ticks_micro: &[ExternalTick] = if dense_ticks.is_empty() {
                &ticks
            } else {
                &dense_ticks[dense_start..dense_end]
            };
            let ctx_micro = ResolutionContext::new(
                stream_open,
                clock.time_remaining_secs(resolution_at_ms),
                "binance-replay".to_owned(),
            );
            let basis = if spot > 0.0 {
                (it.perp - spot) / spot * 100.0
            } else {
                0.0
            };
            let venues_micro = VenueMicroprices {
                binance: spot,
                coinbase: spot,
                perp: it.perp,
                perp_basis_pct: basis,
            };
            let contract_micro = ContractContext::new(asset, horizon, horizon_secs(horizon));
            let features_micro = build_features_micro(
                ticks_micro,
                &ctx_micro,
                &contract_micro,
                venues_micro,
                it.spot_flow,
                it.poly_flow,
            );

            // Candidate maker price from the CURRENT book (event time).
            let mut book = OrderBook::default();
            book.apply_snapshot(
                [(PriceTicks::from_f64(bid.clamp(0.0, 1.0)), 100)],
                [(PriceTicks::from_f64(ask.clamp(0.0, 1.0)), 100)],
            );
            let tick_size = self.config.tick_size;
            let Some(candidate) = candidate_maker_price(&book, tick_size) else {
                incomplete += 1;
                continue;
            };
            // Real V1 states per variant (only `quant` differs): the same
            // builder shape the live pipeline uses, never hand-made JSON.
            // Questions come from the caller map (corpus path) or an
            // explicitly synthetic placeholder (bootstrap path).
            let Some(poly) = poly_from_book(&book, mid, &mut poly_hist, ts) else {
                incomplete += 1;
                continue;
            };
            let (question, rules) = questions
                .get(market_id)
                .cloned()
                .unwrap_or((synthetic_question(market_id), SYNTHETIC_RULES.to_owned()));
            let assumed_latency_ms = match self.config.latency {
                // Empirical replays sample the observed distribution per
                // evaluation (deterministic in seq); fixed profiles keep
                // their single reference value.
                LatencyProfile::Empirical => self.config.latency_distribution.sample_ms(seq as u64),
                _ => jev_latency,
            };
            let arms = self.config.arms.clone();
            let mut arm_results = Vec::with_capacity(arms.len());
            for arm in &arms {
                let (feat, quant) = if arm.micro {
                    (
                        &features_micro,
                        arm.quant
                            .then(|| build_quant(&features_micro, &self.config.quant.params)),
                    )
                } else {
                    (
                        &features,
                        arm.quant
                            .then(|| build_quant(&features, &self.config.quant.params)),
                    )
                };
                let questions = arm.questions.build(candidate);
                let (outcome, hash, json) = self.evaluate_arm(
                    feat,
                    &poly,
                    candidate,
                    &question,
                    &rules,
                    market_id,
                    seq as u64,
                    arm,
                    quant,
                    &questions,
                    assumed_latency_ms,
                );
                jev_errors += outcome.error.is_some() as usize;
                arm_results.push((*arm, outcome, hash, json));
            }
            // Latency: the response is usable at ts+jev_latency while the
            // market keeps printing. Two staleness gates, both in event
            // time: (1) end-to-end latency over budget; (2) sequence lag:
            // snapshots printed strictly after T but at or before the
            // response time, over the tolerated lag. A stale signal can
            // never quote. LIMITATION (documented): the tape carries no
            // historical book snapshots, so the book itself cannot be
            // re-verified at T+L; the latency budget plus the conservative
            // fill queue proxy are the guards for that gap.
            // Per-variant latency: live CONTROL/QUANT calls are sequential,
            // so each has its own measured response time and staleness.
            // Both rows below share this pair ID because they use this same
            // frozen snapshot; only the existing QUANT enrichment differs.
            // The namespace (condition per corpus call) keeps pair IDs
            // globally unique across calls; see ReplayConfig::pair_namespace.
            let pair_id = if self.config.pair_namespace.is_empty() {
                format!("{}-{:06}", self.config.run_id, seq)
            } else {
                format!(
                    "{}-{}-{:06}",
                    self.config.run_id, self.config.pair_namespace, seq
                )
            };
            for (arm, outcome, sh, state_json) in arm_results {
                let lat = outcome.latency_ms;
                let live = outcome.live;
                let mut branch_error = outcome.error;
                let usable_at = ts + lat as i64;
                let seq_lag = labels
                    .iter()
                    .filter(|e| e.0 > ts && e.0 <= usable_at)
                    .count() as u64;
                let stale = lat > self.config.staleness.max_latency_ms
                    || seq_lag > self.config.staleness.max_lag;
                if stale {
                    stale_skips += 1;
                }
                // Per-arm parsing: V1 arms validate the eight answers and may
                // quote; V3 arms extract their metric and never quote.
                // Parse failures behave like transport errors (SKIP +
                // incomplete, no sidecar record).
                let mut signal: Option<V1Signal> = None;
                let mut fair_p_yes: Option<f64> = None;
                let mut pressure: Option<f64> = None;
                let mut pressure_confidence: Option<f64> = None;
                if branch_error.is_none() {
                    match parse_arm_envelope(arm.questions, &outcome.envelope_json) {
                        Ok(ParsedArm::V1(sig)) => {
                            signal = Some(sig);
                        }
                        Ok(ParsedArm::Fair(fair)) => {
                            fair_p_yes = Some(fair);
                        }
                        Ok(ParsedArm::Pressure {
                            expectation,
                            confidence,
                        }) => {
                            pressure = Some(expectation);
                            pressure_confidence = Some(confidence);
                        }
                        Err(parse_err) => {
                            branch_error = Some(parse_err);
                        }
                    }
                }
                let err = branch_error.as_ref();
                if live && std::env::var("JEV_VERBOSE").is_ok() {
                    match (&signal, fair_p_yes, pressure) {
                        (Some(sig), _, _) => {
                            eprintln!(
                                "JEV LIVE market={market_id} seq={seq} variant={} latency_ms={lat} under_up={:.3} p_up1={:.3} persist={:.3} fill={:.3} toxic={:.3}",
                                arm.name,
                                sig.underreact_up,
                                sig.p_up_ge_1_tick(),
                                sig.move_persists,
                                sig.fill_before_decay,
                                sig.fill_toxic,
                            );
                        }
                        (None, Some(fair), _) => {
                            eprintln!(
                                "JEV LIVE market={market_id} seq={seq} variant={} latency_ms={lat} fair_p_yes={fair:.3}",
                                arm.name,
                            );
                        }
                        (None, None, Some(pressure_value)) => {
                            eprintln!(
                                "JEV LIVE market={market_id} seq={seq} variant={} latency_ms={lat} pressure={pressure_value:+.3}",
                                arm.name,
                            );
                        }
                        (None, None, None) => {}
                    }
                }
                // Same strategy path as live, V1 arms only: decide on a
                // snapshot. V3 arms carry no quote rule by design.
                let snapshot = snapshot_for(&book, stale);
                let quoted = match (&signal, arm.questions) {
                    (Some(sig), QuestionSet::V1) if err.is_none() && !stale => {
                        decide(DecisionInput {
                            signal: sig,
                            market: &snapshot,
                            thresholds: &self.config.thresholds,
                            tick_size,
                            size: self.config.size,
                        })
                        .is_quote()
                    }
                    _ => false,
                };
                // Quote intent price mirrors live decide_quote.
                let quote_price = if quoted { candidate.to_f64() } else { 0.0 };
                // Fill check on FUTURE prints only (no look-ahead into the
                // decision itself; fills use prints after usable_at).
                let future_prints: Vec<(i64, f64, f64)> = labels
                    .iter()
                    .filter(|e| e.0 >= usable_at)
                    .take(30)
                    .map(|e| (e.0, e.1, 2.0))
                    .collect();
                let fill = if quoted {
                    let order = RestingOrder {
                        price: quote_price,
                        size: self.config.size as f64,
                        resting_from_ms: usable_at,
                        side_buy: true,
                    };
                    self.fill_sim
                        .check_fill(&order, &future_prints, exec_latency)
                } else {
                    crate::replay::fills::FillOutcome {
                        filled: false,
                        fill_fraction: 0.0,
                        fill_price: quote_price,
                        profile: self.config.fill,
                        fill_ts_ms: None,
                        used_aggressive_qty: 0.0,
                    }
                };
                // Markouts from fill price vs backward-sampled future mids.
                let horizons = [1_000, 5_000, 10_000, 30_000, 60_000];
                let fill_ts = usable_at;
                let mo: [Option<f64>; 5] = if fill.filled {
                    let ms: [Option<f64>; 5] = horizons.map(|h| {
                        labels
                            .iter()
                            .find(|e| e.0 >= fill_ts + h as i64)
                            .map(|e| e.1)
                    });
                    signed_markouts_pp(fill.fill_price, ms)
                } else {
                    [None, None, None, None, None]
                };
                let pnl = if fill.filled {
                    mo[1].unwrap_or(0.0) * fill.fill_fraction
                } else {
                    0.0
                };
                // SIGNAL RESEARCH: forward drift for EVERY usable evaluation
                // (QUOTE or SKIP). Reference is the first tape mid at/after
                // usable_at; horizons sample the same tape mids, so drift is
                // a pure signal label with no execution content. Branch
                // failures (JevError) or missing forward prints yield None.
                let drift: [Option<f64>; 5] = if err.is_none() {
                    match labels.iter().find(|e| e.0 >= usable_at).map(|e| e.1) {
                        Some(ref_mid) => horizons.map(|h| {
                            labels
                                .iter()
                                .find(|e| e.0 >= usable_at + h as i64)
                                .map(|e| (e.1 - ref_mid) * 100.0)
                        }),
                        None => [None, None, None, None, None],
                    }
                } else {
                    [None, None, None, None, None]
                };
                rows.push(ReportRow {
                    run_id: self.config.run_id.clone(),
                    pair_id: pair_id.clone(),
                    variant: arm.name.to_owned(),
                    state_hash: sh.clone(),
                    market_id: market_id.clone(),
                    asset: asset.clone(),
                    horizon: horizon.clone(),
                    split: split.as_str().to_owned(),
                    regime: regime.clone(),
                    fidelity: match fidelity {
                        Fidelity::Exact => "EXACT".to_owned(),
                        Fidelity::Proxy => "PROXY".to_owned(),
                        Fidelity::Unknown => "UNKNOWN".to_owned(),
                    },
                    fill_model: match self.config.fill {
                        FillProfile::Optimistic => "OPTIMISTIC".to_owned(),
                        FillProfile::Base => "BASE".to_owned(),
                        FillProfile::Conservative => "CONSERVATIVE".to_owned(),
                    },
                    latency_profile: self.config.latency.as_str().to_owned(),
                    jev_latency_ms: lat,
                    quoted,
                    filled: fill.filled,
                    fill_fraction: fill.fill_fraction,
                    markout_1s_pp: mo[0],
                    markout_5s_pp: mo[1],
                    markout_10s_pp: mo[2],
                    markout_30s_pp: mo[3],
                    markout_60s_pp: mo[4],
                    drift_1s_pp: drift[0],
                    drift_5s_pp: drift[1],
                    drift_10s_pp: drift[2],
                    drift_30s_pp: drift[3],
                    drift_60s_pp: drift[4],
                    fair_p_yes,
                    pressure,
                    pressure_confidence,
                    pnl_pp: pnl,
                    stale_skipped: stale,
                    incomplete_pair: err.is_some(),
                });
                // Sidecar covers V1 arms with successful parses only; V3
                // metrics live in the row itself.
                if let (QuestionSet::V1, Some(sig)) = (arm.questions, signal.as_ref()) {
                    signals.push(SignalRecord {
                        pair_id: pair_id.clone(),
                        variant: arm.name.to_owned(),
                        market_id: market_id.clone(),
                        state_hash: sh.clone(),
                        state_json: state_json.clone(),
                        signal: sig.clone(),
                        jev_latency_ms: lat,
                        live,
                    });
                }
            }
        }
        RunnerOutput {
            rows,
            signals,
            jev_hits: self.cache.hits,
            jev_misses: self.cache.misses,
            stale_skips,
            incomplete_pairs: incomplete,
            jev_errors,
        }
    }

    /// Real-event entry point: synchronizes streams then delegates to the
    /// synthetic path after normalizing books/underlying at each tick.
    ///
    /// Single-meta legacy path: every Poly event shares `market_meta[0]`.
    /// Multi-market callers must use [`Self::run_events_by_condition`].
    pub fn run_events(
        &mut self,
        streams: Vec<Vec<HistoricalEvent>>,
        market_meta: &[(String, String, String, Split, Fidelity, String)],
    ) -> RunnerOutput {
        self.run_events_by_condition(
            streams,
            market_meta,
            &std::collections::HashMap::new(),
            &std::collections::HashMap::new(),
            None,
        )
    }

    /// Real-event replay with per-event market resolution.
    ///
    /// `meta_by_condition` maps condition_id to its market tuple
    /// `(market_id, asset, horizon, split, fidelity, regime)`. Poly events
    /// resolve metadata through their own condition_id; underlying ticks
    /// only update per-asset spot state and never emit decision items.
    /// Unmatched Poly events are skipped and counted (never silently
    /// borrowing another market's metadata). When the map is empty and
    /// exactly one meta tuple exists, that tuple applies to all Poly
    /// events (legacy single-market behavior, documented).
    #[allow(clippy::too_many_lines)]
    pub fn run_events_by_condition(
        &mut self,
        streams: Vec<Vec<HistoricalEvent>>,
        market_meta: &[(String, String, String, Split, Fidelity, String)],
        meta_by_condition: &std::collections::HashMap<
            String,
            (String, String, String, Split, Fidelity, String),
        >,
        questions: &std::collections::HashMap<String, (String, String)>,
        label_mids: Option<&[(i64, f64)]>,
    ) -> RunnerOutput {
        // Tag events with their stream index before the merge so books
        // stay per-stream after event-time ordering.
        let mut tagged: Vec<(usize, HistoricalEvent)> = Vec::new();
        for (si, stream) in streams.iter().enumerate() {
            for ev in stream {
                tagged.push((si, ev.clone()));
            }
        }
        tagged.sort_by(|a, b| {
            a.1.ts_ms()
                .cmp(&b.1.ts_ms())
                .then_with(|| event_rank(&a.1).cmp(&event_rank(&b.1)))
        });
        let mut books: std::collections::HashMap<usize, (f64, f64)> =
            std::collections::HashMap::new();
        let mut spots: std::collections::HashMap<String, f64> = std::collections::HashMap::new();
        // Perp as-of prices keyed by BASE asset ("BTC" for "BTC-PERP" ticks).
        let mut perp_spots: std::collections::HashMap<String, f64> =
            std::collections::HashMap::new();
        // Rolling signed trade flow: (ts_ms, is_buy, qty), pruned to 60s.
        let mut spot_flow: std::collections::HashMap<String, VecDequeFlow> =
            std::collections::HashMap::new();
        let mut poly_flow: std::collections::HashMap<String, VecDequeFlow> =
            std::collections::HashMap::new();
        // Dense spot-leg history (oldest-first) for MICRO return/vol windows.
        let mut dense: Vec<ExternalTick> = Vec::new();
        let mut items: Vec<SyntheticItem> = Vec::new();
        let mut skipped_unmatched = 0usize;
        for (si, ev) in &tagged {
            match ev {
                HistoricalEvent::UnderlyingTick {
                    asset,
                    price,
                    qty,
                    aggressor,
                    ..
                } => {
                    if let Some(base) = asset.strip_suffix("-PERP") {
                        perp_spots.insert(base.to_owned(), *price);
                    } else {
                        spots.insert(asset.clone(), *price);
                        if let Some(is_buy) = trade_side(aggressor) {
                            push_flow(
                                &mut spot_flow,
                                asset,
                                ev.ts_ms(),
                                is_buy,
                                qty.unwrap_or(0.0),
                            );
                        }
                        dense.push(ExternalTick {
                            price: *price,
                            ts_ms: ev.ts_ms().max(0) as u64,
                        });
                    }
                }
                HistoricalEvent::PolyTop {
                    best_bid,
                    best_ask,
                    condition_id,
                    ..
                } => {
                    books.insert(*si, (*best_bid, *best_ask));
                    let Some(meta) = resolve_meta(condition_id, market_meta, meta_by_condition)
                    else {
                        skipped_unmatched += 1;
                        continue;
                    };
                    let (bid, ask) = books[si];
                    items.push(emit_item(
                        ev.ts_ms(),
                        bid,
                        ask,
                        &spots,
                        &perp_spots,
                        &mut spot_flow,
                        &mut poly_flow,
                        condition_id,
                        &meta,
                    ));
                }
                HistoricalEvent::PolyTrade {
                    condition_id,
                    price,
                    size,
                    aggressor,
                    ..
                } => {
                    let bid = (price - 0.01).clamp(0.0, 1.0);
                    let ask = (price + 0.01).clamp(0.0, 1.0);
                    books.insert(*si, (bid, ask));
                    let Some(meta) = resolve_meta(condition_id, market_meta, meta_by_condition)
                    else {
                        skipped_unmatched += 1;
                        continue;
                    };
                    if let Some(is_buy) = trade_side(aggressor) {
                        push_flow(&mut poly_flow, condition_id, ev.ts_ms(), is_buy, *size);
                    }
                    items.push(emit_item(
                        ev.ts_ms(),
                        bid,
                        ask,
                        &spots,
                        &perp_spots,
                        &mut spot_flow,
                        &mut poly_flow,
                        condition_id,
                        &meta,
                    ));
                }
            }
            if items.len() >= self.config.max_pairs * 4 {
                break;
            }
        }
        let _ = skipped_unmatched;
        // Resolution follows the market horizon carried by the items
        // (first item wins; per-condition runs carry a single horizon).
        let horizon_secs = items
            .first()
            .map(|i| horizon_secs(&i.horizon))
            .unwrap_or(300);
        let resolution_at_ms = items.last().map_or(0, |i| i.ts_ms) + horizon_secs as i64 * 1000;
        self.run_synthetic_with(&items, resolution_at_ms, questions, &dense, label_mids)
    }
}

/// Seconds encoded by a horizon tag (`5m`, `15m`, `1h`, `4h`).
fn horizon_secs(horizon: &str) -> u64 {
    match horizon {
        "15m" => 900,
        "1h" => 3600,
        "4h" => 14_400,
        _ => 300,
    }
}

/// Tie-break rank for event-time ordering (ticks before tops before trades).
fn event_rank(ev: &HistoricalEvent) -> u8 {
    match ev {
        HistoricalEvent::UnderlyingTick { .. } => 0,
        HistoricalEvent::PolyTop { .. } => 1,
        HistoricalEvent::PolyTrade { .. } => 2,
    }
}

/// Rolling signed trade flow entries: (ts_ms, is_buy, qty).
type VecDequeFlow = std::collections::VecDeque<(i64, bool, f64)>;

/// Maps a recorded aggressor side to a buy flag. Verified tape/underlying
/// domains are exactly BUY/SELL; anything else is skipped, never invented.
fn trade_side(aggressor: &Option<String>) -> Option<bool> {
    match aggressor.as_deref() {
        Some("BUY") => Some(true),
        Some("SELL") => Some(false),
        _ => None,
    }
}

/// Pushes one signed trade into the keyed rolling deque, pruning beyond 60s.
fn push_flow(
    flows: &mut std::collections::HashMap<String, VecDequeFlow>,
    key: &str,
    ts_ms: i64,
    is_buy: bool,
    qty: f64,
) {
    let deque = flows.entry(key.to_owned()).or_default();
    deque.push_back((ts_ms, is_buy, qty.max(0.0)));
    while deque.front().is_some_and(|(t, _, _)| *t < ts_ms - 60_000) {
        deque.pop_front();
    }
}

/// Signed volume snapshot over the trailing window: (buy_vol, sell_vol).
fn window_vols(deque: Option<&VecDequeFlow>, now_ms: i64, window_ms: i64) -> (f64, f64) {
    let Some(deque) = deque else {
        return (0.0, 0.0);
    };
    let mut buy = 0.0;
    let mut sell = 0.0;
    for (t, is_buy, qty) in deque.iter().rev() {
        if *t < now_ms - window_ms {
            break;
        }
        if *is_buy {
            buy += qty;
        } else {
            sell += qty;
        }
    }
    (buy, sell)
}

/// Builds one rich item at a poly event: as-of spot/perp plus rolling flow
/// snapshots. Perp falls back to spot (V1 behavior) when uncovered. Poly 5s
/// vols sit in the vol slots (see build_features_micro).
#[allow(clippy::too_many_arguments)]
fn emit_item(
    ts_ms: i64,
    bid: f64,
    ask: f64,
    spots: &std::collections::HashMap<String, f64>,
    perp_spots: &std::collections::HashMap<String, f64>,
    spot_flow: &mut std::collections::HashMap<String, VecDequeFlow>,
    poly_flow: &mut std::collections::HashMap<String, VecDequeFlow>,
    condition_id: &str,
    meta: &(String, String, String, Split, Fidelity, String),
) -> SyntheticItem {
    let spot = spots.get(&meta.1).copied().unwrap_or(100.0);
    let perp = perp_spots.get(&meta.1).copied().unwrap_or(spot);
    let (buy1, sell1) = window_vols(spot_flow.get(&meta.1), ts_ms, 1_000);
    let (buy5, sell5) = window_vols(spot_flow.get(&meta.1), ts_ms, 5_000);
    let total5 = buy5 + sell5;
    let spot_flow_snap = OrderFlowAggregates {
        buy_vol_1s: buy1,
        sell_vol_1s: sell1,
        ofi_1s: buy1 - sell1,
        ofi_5s: buy5 - sell5,
        imbalance: if total5 > 0.0 {
            (buy5 - sell5) / total5
        } else {
            0.0
        },
        aggressive_buy_ratio: if total5 > 0.0 { buy5 / total5 } else { 0.5 },
    };
    let (pbuy5, psell5) = window_vols(poly_flow.get(condition_id), ts_ms, 5_000);
    let ptotal = pbuy5 + psell5;
    let poly_flow_snap = OrderFlowAggregates {
        buy_vol_1s: pbuy5,
        sell_vol_1s: psell5,
        ofi_1s: 0.0,
        ofi_5s: pbuy5 - psell5,
        imbalance: 0.0,
        aggressive_buy_ratio: if ptotal > 0.0 { pbuy5 / ptotal } else { 0.5 },
    };
    SyntheticItem {
        ts_ms,
        book_bid: bid,
        book_ask: ask,
        spot,
        perp,
        spot_flow: spot_flow_snap,
        poly_flow: poly_flow_snap,
        market_id: meta.0.clone(),
        asset: meta.1.clone(),
        horizon: meta.2.clone(),
        split: meta.3,
        fidelity: meta.4,
        regime: meta.5.clone(),
    }
}

/// Resolves one market tuple by condition_id, with a documented single-meta
/// fallback. Returns `None` when the event cannot be attributed.
fn resolve_meta(
    condition_id: &str,
    market_meta: &[(String, String, String, Split, Fidelity, String)],
    meta_by_condition: &std::collections::HashMap<
        String,
        (String, String, String, Split, Fidelity, String),
    >,
) -> Option<(String, String, String, Split, Fidelity, String)> {
    if let Some(meta) = meta_by_condition.get(condition_id) {
        return Some(meta.clone());
    }
    if market_meta.len() == 1 {
        return Some(market_meta[0].clone());
    }
    None
}

fn hash_str(s: &str) -> String {
    let mut h = DefaultHasher::new();
    s.hash(&mut h);
    format!("{:016x}", h.finish())
}

fn synthetic_question(market_id: &str) -> String {
    format!("SYNTHETIC {market_id}: no on-chain market")
}

const SYNTHETIC_RULES: &str = "synthetic bootstrap market: no on-chain resolution rules";

fn poly_from_book(
    book: &OrderBook,
    mid: f64,
    hist: &mut PolyHistory,
    ts_ms: i64,
) -> Option<crate::strategy::lead_lag::PolySnapshot> {
    use crate::strategy::lead_lag::PolySnapshot;
    let best_bid = book.best_bid()?;
    let best_ask = book.best_ask()?;
    let coherent =
        PriceTicks::from_f64(((best_bid.to_f64() + best_ask.to_f64()) / 2.0).clamp(0.0, 1.0));
    hist.push(ts_ms.max(0) as u64, coherent);
    let bid_depth: f64 = book.bids().iter().map(|(_, q)| *q as f64).sum();
    let ask_depth: f64 = book.asks().iter().map(|(_, q)| *q as f64).sum();
    let total = bid_depth + ask_depth;
    let mut snapshot = PolySnapshot {
        yes_bid: best_bid,
        yes_ask: best_ask,
        bid_depth,
        ask_depth,
        spread: (best_ask.to_f64() - best_bid.to_f64()).max(0.0),
        book_imbalance: if total > 0.0 {
            (bid_depth - ask_depth) / total
        } else {
            0.0
        },
        last_trade_price: PriceTicks::from_f64(mid.clamp(0.0, 1.0)),
        price_1s_ago: PriceTicks::from_f64(0.0),
        price_5s_ago: PriceTicks::from_f64(0.0),
        price_30s_ago: PriceTicks::from_f64(0.0),
    };
    hist.apply_to_snapshot(ts_ms.max(0) as u64, &mut snapshot);
    Some(snapshot)
}

fn snapshot_for(book: &OrderBook, stale: bool) -> MarketSnapshot {
    MarketSnapshot {
        book: book.clone(),
        stale,
    }
}

// Re-export for callers that only need the decision path signature.
#[allow(unused_imports)]
pub use crate::strategy::quote::decide_quote as replay_decide_quote;
