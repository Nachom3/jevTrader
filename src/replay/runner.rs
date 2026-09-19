//! ReplayRunner: rolling state -> features -> QUANT pairing -> Jev w/latency.

use super::clock::ReplayClock;
use super::fills::{ExecutionLatency, FillSimulator, RestingOrder};
use super::jev_cache::{CachedJev, JevCache, JevCacheKey};
use super::markouts::signed_markouts_pp;
use super::report::ReportRow;
use super::types::{Coverage, Fidelity, FillProfile, HistoricalEvent, LatencyProfile, Split};
use crate::config::{QuantConfig, QuoteThresholds};
use crate::domain::{PriceTicks, TickSize};
use crate::engine::MarketSnapshot;
use crate::engine::pipeline::{DecisionInput, MarkoutTracker, candidate_maker_price, decide};
use crate::engine::signal_actor::StalenessPolicy;
use crate::jev::request::{V1Questions, V1State};
use crate::jev::response::{TickDistribution, V1Signal};
use std::time::Duration;
use crate::polymarket::OrderBook;
use crate::state::feature_builder::{
    ExternalTick, OrderFlowAggregates, ResolutionContext, VenueMicroprices,
    build_features_with_context,
};
use crate::state::poly_history::PolyHistory;
use crate::state::quant_features::build_quant;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// One Jev evaluation: the judged signal plus how it was obtained.
///
/// `latency_ms` is measured wall time for live calls and the configured
/// profile assumption for stubs/cached replays. `live` is true only for a
/// real TypeSafe call; `error` carries transport/parse/budget failures, in
/// which case `signal` is a neutral placeholder the runner must SKIP.
#[derive(Debug, Clone)]
pub struct JevOutcome {
    pub signal: V1Signal,
    pub latency_ms: u64,
    pub live: bool,
    pub error: Option<String>,
}

/// How the runner obtains a Jev signal (real client or deterministic stub).
///
/// The runner always hands over a fully built [`V1State`]; stubs may ignore
/// it and hash its serialization instead. `assumed_latency_ms` is the
/// replay profile value, used only when no measured latency exists.
pub trait JevEvaluator {
    fn evaluate(
        &mut self,
        state: &V1State,
        market_id: &str,
        state_seq: u64,
        questions_hash: &str,
        variant: &str,
        assumed_latency_ms: u64,
    ) -> JevOutcome;
}

/// One synthetic replay tick with market metadata.
pub type SyntheticItem = (
    i64,
    f64,
    f64,
    f64,
    String,
    String,
    String,
    Split,
    Fidelity,
    String,
);

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
        _questions_hash: &str,
        _variant: &str,
        assumed_latency_ms: u64,
    ) -> JevOutcome {
        // Paired outputs differ only because the state content differs
        // (QUANT_V1 carries the quant enrichment in its JSON). No
        // variant-specific shift is applied: same state => same signal.
        let state_json = serde_json::to_string(state).unwrap_or_default();
        let base = self.pseudo(&state_json);
        let clamp = |v: f64| v.clamp(0.0, 1.0);
        JevOutcome {
            signal: V1Signal {
                yes_pressure_5s: clamp(0.3 + base * 0.5),
            no_pressure_5s: clamp(0.5 - base * 0.4),
            move_persists: clamp(0.4 + base * 0.4),
            underreact_up: clamp(0.5 + base * 0.45),
            underreact_down: clamp(0.35 - base * 0.2),
            repricing: TickDistribution {
                up_3_plus: 0.05 + base * 0.1,
                up_2: 0.10 + base * 0.1,
                up_1: 0.20 + base * 0.2,
                flat: 0.25,
                down_1: 0.15,
                down_2: 0.05,
                down_3_plus: 0.05,
            },
            repricing_confidence: 0.6,
            fill_before_decay: clamp(0.5 + base * 0.3),
            fill_toxic: clamp(0.35 - base * 0.2),
            },
            latency_ms: assumed_latency_ms,
            live: false,
            error: None,
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
        let runtime =
            tokio::runtime::Runtime::new().map_err(|e| format!("tokio runtime: {e}"))?;
        Ok(Self {
            api_key,
            deadline,
            runtime,
            calls: 0,
            max_calls,
        })
    }
}

impl JevEvaluator for RealJev {
    fn evaluate(
        &mut self,
        state: &V1State,
        market_id: &str,
        state_seq: u64,
        _questions_hash: &str,
        _variant: &str,
        assumed_latency_ms: u64,
    ) -> JevOutcome {
        if self.calls >= self.max_calls {
            return JevOutcome {
                signal: neutral_signal(),
                latency_ms: assumed_latency_ms,
                live: false,
                error: Some(format!(
                    "Jev budget exhausted ({}/{})",
                    self.calls, self.max_calls
                )),
            };
        }
        self.calls += 1;
        let result = self.runtime.block_on(crate::jev::client::evaluate(
            state,
            state_seq,
            market_id,
            &self.api_key,
            self.deadline,
        ));
        // Per-call observability for budgeted live runs. Real judgments
        // are the only alpha evidence; always log what Jev actually said.
        let verbose = std::env::var("JEV_VERBOSE").is_ok();
        match result {
            Ok(evaluation) => {
                if verbose {
                    let s = &evaluation.signal;
                    eprintln!(
                        "JEV LIVE market={market_id} seq={state_seq} latency_ms={} under_up={:.3} p_up1={:.3} persist={:.3} fill={:.3} toxic={:.3}",
                        evaluation.latency_ms,
                        s.underreact_up,
                        s.p_up_ge_1_tick(),
                        s.move_persists,
                        s.fill_before_decay,
                        s.fill_toxic,
                    );
                }
                JevOutcome {
                    latency_ms: evaluation.latency_ms,
                    signal: evaluation.signal,
                    live: true,
                    error: None,
                }
            }
            Err(error) => {
                if verbose {
                    eprintln!("JEV ERROR market={market_id} seq={state_seq}: {error}");
                }
                JevOutcome {
                    signal: neutral_signal(),
                    latency_ms: assumed_latency_ms,
                    live: false,
                    error: Some(error.to_string()),
                }
            }
        }
    }
}

/// Neutral 0.5 placeholder; the runner must SKIP error outcomes, never quote.
fn neutral_signal() -> V1Signal {
    V1Signal {
        yes_pressure_5s: 0.5,
        no_pressure_5s: 0.5,
        move_persists: 0.5,
        underreact_up: 0.5,
        underreact_down: 0.5,
        repricing: TickDistribution {
            up_3_plus: 0.05,
            up_2: 0.10,
            up_1: 0.20,
            flat: 0.30,
            down_1: 0.15,
            down_2: 0.10,
            down_3_plus: 0.10,
        },
        repricing_confidence: 0.0,
        fill_before_decay: 0.5,
        fill_toxic: 0.5,
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
            tick_size: TickSize::from_f64(0.01),
            size: 10,
            max_pairs: 100,
            coverage: Coverage::BinanceOnly,
            staleness: StalenessPolicy::default(),
        }
    }
}

/// Output of one replay run.
#[derive(Debug)]
pub struct RunnerOutput {
    pub rows: Vec<ReportRow>,
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

    /// Runs the replay over pre-synchronized events with per-event market
    /// metadata carried alongside. Each item is
    /// `(ts_ms, book_bid, book_ask, spot, market_id, asset, horizon, split,
    /// fidelity, regime)`.
    #[allow(clippy::too_many_arguments)]
    /// Evaluates one frozen snapshot in both variants and returns
    /// `(control_signal, control_latency, control_error, control_hash,
    /// quant_signal, quant_latency, quant_error, quant_hash)`.
    /// Latencies are measured for live calls and assumed otherwise; the
    /// Jev cache stores complete signals keyed by state + questions +
    /// model + variant.
    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::type_complexity)]
    fn evaluate_pair(
        &mut self,
        features: &crate::strategy::lead_lag::LeadLagFeatures,
        poly: &crate::strategy::lead_lag::PolySnapshot,
        candidate: PriceTicks,
        question: &str,
        rules: &str,
        market_id: &str,
        seq: u64,
        assumed_latency_ms: u64,
    ) -> (
        V1Signal,
        u64,
        Option<String>,
        String,
        V1Signal,
        u64,
        Option<String>,
        String,
    ) {
        let quant = build_quant(features, &self.config.quant.params);
        let control_state = V1State::new(
            question,
            rules,
            features.clone(),
            poly.clone(),
            None,
            candidate,
        );
        let quant_state = V1State::new(
            question,
            rules,
            features.clone(),
            poly.clone(),
            Some(quant),
            candidate,
        );
        let control_json = serde_json::to_string(&control_state).unwrap_or_default();
        let quant_json = serde_json::to_string(&quant_state).unwrap_or_default();
        let questions_json =
            serde_json::to_string(&V1Questions::new(candidate)).unwrap_or_default();
        let qhash = hash_str(&questions_json);
        let hash_c = hash_str(&control_json);
        let hash_q = hash_str(&quant_json);
        let key_c = JevCacheKey {
            state_hash: hash_c.clone(),
            questions_hash: qhash.clone(),
            model: "jev-latest".to_owned(),
            variant: "CONTROL".to_owned(),
        };
        let key_q = JevCacheKey {
            state_hash: hash_q.clone(),
            questions_hash: qhash,
            model: "jev-latest".to_owned(),
            variant: "QUANT_V1".to_owned(),
        };
        let (sig_c, lat_c, err_c) = self.cached_or_evaluate(
            &key_c,
            &control_state,
            market_id,
            seq,
            "CONTROL",
            assumed_latency_ms,
        );
        let (sig_q, lat_q, err_q) = self.cached_or_evaluate(
            &key_q,
            &quant_state,
            market_id,
            seq,
            "QUANT_V1",
            assumed_latency_ms,
        );
        (sig_c, lat_c, err_c, hash_c, sig_q, lat_q, err_q, hash_q)
    }

    /// Cache lookup with evaluator fallback; stores complete signals.
    fn cached_or_evaluate(
        &mut self,
        key: &JevCacheKey,
        state: &V1State,
        market_id: &str,
        seq: u64,
        variant: &str,
        assumed_latency_ms: u64,
    ) -> (V1Signal, u64, Option<String>) {
        if let Some(cached) = self.cache.get(key) {
            return (
                signal_from_cache(&cached),
                cached.latency_ms,
                None,
            );
        }
        let outcome = self.evaluator.evaluate(
            state,
            market_id,
            seq,
            &key.questions_hash,
            variant,
            assumed_latency_ms,
        );
        let signal = outcome.signal.clone();
        let latency = outcome.latency_ms;
        let error = outcome.error.clone();
        self.cache.put(
            key.clone(),
            CachedJev {
                signal_json: serde_json::to_string(&signal).unwrap_or_default(),
                latency_ms: latency,
            },
        );
        (signal, latency, error)
    }

    pub fn run_synthetic(
        &mut self,
        items: &[SyntheticItem],
        resolution_at_ms: i64,
    ) -> RunnerOutput {
        self.run_synthetic_with(items, resolution_at_ms, &std::collections::HashMap::new())
    }

    /// Replay with per-market questions (`market_id` -> `(question, rules)`).
    /// Missing entries fall back to explicitly synthetic placeholders.
    pub fn run_synthetic_with(
        &mut self,
        items: &[SyntheticItem],
        resolution_at_ms: i64,
        questions: &std::collections::HashMap<String, (String, String)>,
    ) -> RunnerOutput {
        let mut rows = Vec::new();
        let mut stale_skips = 0usize;
        let mut incomplete = 0usize;
        let mut jev_errors = 0usize;
        let mut clock = ReplayClock::new(items.first().map_or(0, |i| i.0));
        let mut poly_hist = PolyHistory::new(1024);
        let mut underlying: Vec<(i64, f64)> = Vec::new();
        let mut mids: Vec<(i64, f64)> = Vec::new();
        // Full mid trajectory, known upfront because the corpus is
        // historical. Fills and markouts are post-facto LABELS evaluated
        // against later prints; the quote DECISION only ever uses history
        // accumulated through T (underlying, poly_hist, mids above).
        let future_mids: Vec<(i64, f64)> =
            items.iter().map(|it| (it.0, (it.1 + it.2) / 2.0)).collect();
        let markout_tracker = MarkoutTracker::new();
        let _ = markout_tracker;
        let jev_latency = self.config.latency.jev_latency_ms();
        let exec_latency = ExecutionLatency::new(self.config.latency.submit_latency_ms());

        for (seq, it) in items.iter().enumerate() {
            if rows.len() / 2 >= self.config.max_pairs {
                break;
            }
            let (ts, bid, ask, spot, market_id, asset, horizon, split, fidelity, regime) = it;
            let ts = *ts;
            if clock.advance_to(ts).is_err() {
                incomplete += 1;
                continue;
            }
            underlying.push((ts, *spot));
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
                *spot,
                clock.time_remaining_secs(resolution_at_ms),
                "binance-replay".to_owned(),
            );
            let venues = VenueMicroprices {
                binance: *spot,
                coinbase: *spot,
                perp: *spot,
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
            let (sig_c, lat_c, err_c, hash_c, sig_q, lat_q, err_q, hash_q) =
                self.evaluate_pair(
                    &features,
                    &poly,
                    candidate,
                    &question,
                    &rules,
                    market_id,
                    seq as u64,
                    jev_latency,
                );
            jev_errors += err_c.is_some() as usize + err_q.is_some() as usize;
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
            for (variant, sig, lat, err, sh) in [
                ("CONTROL", &sig_c, lat_c, err_c.as_ref(), &hash_c),
                ("QUANT_V1", &sig_q, lat_q, err_q.as_ref(), &hash_q),
            ] {
                let usable_at = ts + lat as i64;
                let seq_lag = future_mids
                    .iter()
                    .filter(|e| e.0 > ts && e.0 <= usable_at)
                    .count() as u64;
                let stale = lat > self.config.staleness.max_latency_ms
                    || seq_lag > self.config.staleness.max_lag;
                if stale {
                    stale_skips += 1;
                }
                // Same strategy path as live: decide on a snapshot.
                let snapshot = snapshot_for(&book, stale);
                let outcome = if err.is_some() {
                    crate::engine::pipeline::Outcome::Skip(
                        crate::engine::pipeline::SkipReason::JevError,
                    )
                } else if stale {
                    crate::engine::pipeline::Outcome::Skip(
                        crate::engine::pipeline::SkipReason::UnusableSignal,
                    )
                } else {
                    decide(DecisionInput {
                        signal: sig,
                        market: &snapshot,
                        thresholds: &self.config.thresholds,
                        tick_size,
                        size: self.config.size,
                    })
                };
                let quoted = outcome.is_quote();
                // Quote intent price mirrors live decide_quote.
                let quote_price = if quoted { candidate.to_f64() } else { 0.0 };
                // Fill check on FUTURE prints only (no look-ahead into the
                // decision itself; fills use prints after usable_at).
                let future_prints: Vec<(i64, f64, f64)> = future_mids
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
                    }
                };
                // Markouts from fill price vs backward-sampled future mids.
                let horizons = [1_000, 5_000, 10_000, 30_000, 60_000];
                let fill_ts = usable_at;
                let mo: [Option<f64>; 5] = if fill.filled {
                    let ms: [Option<f64>; 5] = horizons.map(|h| {
                        future_mids
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
                let pair_id = format!("{}-{:06}", self.config.run_id, seq);
                rows.push(ReportRow {
                    run_id: self.config.run_id.clone(),
                    pair_id: pair_id.clone(),
                    variant: variant.to_owned(),
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
                    latency_profile: match self.config.latency {
                        LatencyProfile::Fast => "FAST".to_owned(),
                        LatencyProfile::Base => "BASE".to_owned(),
                        LatencyProfile::Slow => "SLOW".to_owned(),
                    },
                    quoted,
                    filled: fill.filled,
                    fill_fraction: fill.fill_fraction,
                    markout_1s_pp: mo[0],
                    markout_5s_pp: mo[1],
                    markout_10s_pp: mo[2],
                    markout_30s_pp: mo[3],
                    markout_60s_pp: mo[4],
                    pnl_pp: pnl,
                    stale_skipped: stale,
                    incomplete_pair: !quoted && !stale,
                });
            }
        }
        RunnerOutput {
            rows,
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
        let mut items: Vec<SyntheticItem> = Vec::new();
        let mut skipped_unmatched = 0usize;
        for (si, ev) in &tagged {
            match ev {
                HistoricalEvent::UnderlyingTick { asset, price, .. } => {
                    spots.insert(asset.clone(), *price);
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
                    let spot = spots.get(&meta.1).copied().unwrap_or(100.0);
                    items.push((
                        ev.ts_ms(),
                        bid,
                        ask,
                        spot,
                        meta.0.clone(),
                        meta.1.clone(),
                        meta.2.clone(),
                        meta.3,
                        meta.4,
                        meta.5.clone(),
                    ));
                }
                HistoricalEvent::PolyTrade {
                    condition_id,
                    price,
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
                    let spot = spots.get(&meta.1).copied().unwrap_or(100.0);
                    items.push((
                        ev.ts_ms(),
                        bid,
                        ask,
                        spot,
                        meta.0.clone(),
                        meta.1.clone(),
                        meta.2.clone(),
                        meta.3,
                        meta.4,
                        meta.5.clone(),
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
        let horizon_secs = items.first().map(|i| horizon_secs(&i.6)).unwrap_or(300);
        let resolution_at_ms = items.last().map_or(0, |i| i.0) + horizon_secs as i64 * 1000;
        self.run_synthetic_with(&items, resolution_at_ms, questions)
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

fn signal_from_cache(c: &CachedJev) -> V1Signal {
    serde_json::from_str(&c.signal_json).unwrap_or_else(|_| neutral_signal())
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
    let coherent = PriceTicks::from_f64(
        ((best_bid.to_f64() + best_ask.to_f64()) / 2.0).clamp(0.0, 1.0),
    );
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
