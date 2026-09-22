//! Episode campaign replay with cache-backed live Jev judgments.
//!
//! The campaign is print-first. It never derives a depth-bearing book or a
//! resolution from a trade print. V1 receives a print-derived touch when no
//! `PolyTop` book is available; a signal without any valid print is skipped
//! before Jev is consulted.
//!
//! En replay-sobre-prints "fresh" significa touch observado de prints reales (no un book con depth); el depth es desconocido y la incertidumbre de cola la cubre el fill engine conservador; los fills exigen trade-throughs reales post-arrival; el spread de 1 tick alrededor del last print es supuesto declarado (si el spread real fuera mayor, cotizar al touch es más agresivo: sesgo documentado, mitigado por perfiles Conservative/Base/Optimistic y su comparativa). Nada oculto.

use super::arms::Arm;
use super::exits::{ExitPolicy, settle_hedge_pair, settle_resolution, should_hedge_profit};
use super::fees::{FeeRegime, apply_to_episode};
use super::fills::{Aggressor, ExecutionLatency, FillPrint, FillSimulator, RestingOrder};
use super::jev_cache::{CachedJev, JevCache, JevCacheKey};
use super::ledger::{ExitType, Side, TradeEpisode};
use super::resolution::{ResolutionOutcome, ResolvedMarket};
use super::sizing::{MarketConstraints, STANDARD_STAKE_USD, size_entry};
use super::types::{FillProfile, HistoricalEvent};
use crate::config::QuoteThresholds;
use crate::domain::{PriceTicks, TickSize};
use crate::engine::pipeline::candidate_maker_price;
use crate::jev::request::{QuestionSet, V1State};
use crate::jev::response::{SystemOneResponse, V1Signal, parse_v1_signal};
use crate::polymarket::OrderBook;
use crate::state::feature_builder::{
    ContractContext, ExternalTick, OrderFlowAggregates, ResolutionContext, VenueMicroprices,
    build_features_micro, build_features_with_context,
};
use crate::state::poly_history::PolyHistory;
use crate::state::quant_features::build_quant;
use crate::strategy::api::{Action, MarketEvent, Strategy, StrategyContext, V1Strategy};
use crate::strategy::lead_lag::{LeadLagFeatures, PolySnapshot};
use serde::Serialize;
use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::time::Duration;

const TICK_SIZE: f64 = 0.01;
const SIZE_STEP: f64 = 0.01;
const MIN_ORDER_SIZE: f64 = 1.0;
const SUBMIT_LATENCY_MS: u64 = 50;
const EXIT_SUBMIT_LATENCY_MS: u64 = 50;
const PROMPT_VERSION: &str = "v1-lead-lag";
const MODEL_VERSION: &str = "jev-latest";
const QUESTION_SCHEMA_VERSION: &str = "v1";
const DEFAULT_CACHE_DIR: &str = "research-data/cache";

/// Tape grouped by condition, as required by the episode runner.
pub type TapeByCondition = BTreeMap<String, Vec<HistoricalEvent>>;

/// The narrow Jev boundary used by the campaign.
///
/// `state_json` and `questions_json` are the exact wire payloads used for the
/// cache key and for the live POST. Tests can implement this trait without a
/// network client.
pub trait JevCaller {
    fn call(
        &mut self,
        state_json: &str,
        questions_json: &str,
    ) -> Result<(String, u64, bool), String>;

    fn call_typed(
        &mut self,
        _state: &V1State,
        state_json: &str,
        questions_json: &str,
    ) -> Result<(String, u64, bool), String> {
        self.call(state_json, questions_json)
    }
}

/// Real TypeSafe caller used by the campaign entry point.
pub struct LiveJevCaller {
    api_key: String,
    deadline: Duration,
    runtime: tokio::runtime::Runtime,
}

impl LiveJevCaller {
    pub fn new(api_key: impl Into<String>, deadline: Duration) -> Result<Self, String> {
        let api_key = api_key.into();
        if api_key.trim().is_empty() {
            return Err("TYPESAFE_API_KEY is missing or empty".to_owned());
        }
        let runtime =
            tokio::runtime::Runtime::new().map_err(|error| format!("tokio runtime: {error}"))?;
        Ok(Self {
            api_key,
            deadline,
            runtime,
        })
    }
}

impl JevCaller for LiveJevCaller {
    fn call(
        &mut self,
        _state_json: &str,
        _questions_json: &str,
    ) -> Result<(String, u64, bool), String> {
        Err("LiveJevCaller requires the typed campaign state".to_owned())
    }

    fn call_typed(
        &mut self,
        state: &V1State,
        _state_json: &str,
        questions_json: &str,
    ) -> Result<(String, u64, bool), String> {
        let questions: serde_json::Value = serde_json::from_str(questions_json)
            .map_err(|error| format!("decode campaign questions: {error}"))?;
        let result = self.runtime.block_on(crate::jev::client::post(
            state,
            &questions,
            &self.api_key,
            self.deadline,
        ));
        let (body, sent_at_ms, received_at_ms) =
            result.map_err(|error| format!("live Jev POST: {error}"))?;
        let latency_ms = received_at_ms.saturating_sub(sent_at_ms).max(0) as u64;
        Ok((
            String::from_utf8_lossy(&body).into_owned(),
            latency_ms,
            true,
        ))
    }
}

/// Complete configuration for one deterministic campaign.
#[derive(Debug, Clone)]
pub struct CampaignConfig {
    pub run_id: String,
    pub arms: Vec<Arm>,
    pub fill_profile: FillProfile,
    pub historical_regime: FeeRegime,
    pub current_regime: FeeRegime,
    /// Supported values are `HOLD`, `HEDGE_DYNAMIC`, `RISK_EXIT`, or
    /// `HEDGE_PROFIT:<usd_per_share>`. `HOLD` is the campaign default.
    pub exit_policy: String,
    pub max_jev_calls: usize,
    pub jev_deadline_ms: u64,
    pub cache_dir: String,
    pub per_condition_signals: usize,
}

impl CampaignConfig {
    /// Small deterministic configuration used by provider-free tests.
    #[must_use]
    pub fn smoke(run_id: impl Into<String>, cache_dir: impl Into<String>) -> Self {
        Self {
            run_id: run_id.into(),
            arms: vec![
                Arm::QuantOnly,
                Arm::JevOnly,
                Arm::QuantPlusJev,
                Arm::MicroPlusRegime,
            ],
            fill_profile: FillProfile::Conservative,
            historical_regime: super::fees::zero_regime(),
            current_regime: super::fees::current_crypto_regime(),
            exit_policy: "HOLD".to_owned(),
            max_jev_calls: 20,
            jev_deadline_ms: 1_500,
            cache_dir: cache_dir.into(),
            per_condition_signals: 4,
        }
    }
}

/// Campaign result and explicit skip/cache accounting.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CampaignOutput {
    pub episodes: Vec<TradeEpisode>,
    pub jev_calls: usize,
    pub jev_hits: u64,
    pub jev_misses: u64,
    pub latencies_ms: Vec<u64>,
    pub skipped_no_resolution: usize,
    pub skipped_no_fill_data: usize,
    pub skipped_budget: usize,
    pub skipped_quant_only: usize,
    /// Backward-compatible total of all conservative skips.
    pub skipped: usize,
}

impl CampaignOutput {
    fn skip_resolution(&mut self) {
        self.skipped_no_resolution = self.skipped_no_resolution.saturating_add(1);
        self.skipped = self.skipped.saturating_add(1);
    }

    fn skip_data(&mut self) {
        self.skipped_no_fill_data = self.skipped_no_fill_data.saturating_add(1);
        self.skipped = self.skipped.saturating_add(1);
    }

    fn skip_budget(&mut self) {
        self.skipped_budget = self.skipped_budget.saturating_add(1);
        self.skipped = self.skipped.saturating_add(1);
    }

    fn skip_quant_only(&mut self) {
        self.skipped_quant_only = self.skipped_quant_only.saturating_add(1);
        self.skipped = self.skipped.saturating_add(1);
    }
}

/// Runs a campaign with the real TypeSafe client from `TYPESAFE_API_KEY`.
pub fn run_episode_campaign<T: CampaignTape + ?Sized>(
    config: &CampaignConfig,
    tape_by_condition: &T,
    resolutions: &HashMap<String, ResolvedMarket>,
    specs_lookup: &HashMap<String, (String, String)>,
) -> Result<CampaignOutput, String> {
    let caller = LiveJevCaller::new(
        std::env::var("TYPESAFE_API_KEY").unwrap_or_default(),
        Duration::from_millis(config.jev_deadline_ms),
    )?;
    run_episode_campaign_with_caller(config, tape_by_condition, resolutions, specs_lookup, caller)
}

/// Testable campaign entry point. Production uses [`run_episode_campaign`],
/// while tests inject a deterministic local [`JevCaller`].
pub fn run_episode_campaign_with_caller<T, C>(
    config: &CampaignConfig,
    tape_by_condition: &T,
    resolutions: &HashMap<String, ResolvedMarket>,
    specs_lookup: &HashMap<String, (String, String)>,
    mut caller: C,
) -> Result<CampaignOutput, String>
where
    T: CampaignTape + ?Sized,
    C: JevCaller,
{
    if config.run_id.trim().is_empty() {
        return Err("campaign run_id must not be empty".to_owned());
    }
    let exit_policy = parse_exit_policy(&config.exit_policy)?;
    let mut cache = JevCache::new();
    let cache_dir = if config.cache_dir.trim().is_empty() {
        Path::new(DEFAULT_CACHE_DIR)
    } else {
        Path::new(&config.cache_dir)
    };
    cache.load_from_dir(cache_dir)?;

    let grouped = tape_by_condition.grouped();
    // `CampaignTape` attaches the shared underlying history to each
    // condition so a pre-grouped caller can remain self-contained. Read one
    // copy here; concatenating every condition would double-count flow.
    let mut underlying: Vec<HistoricalEvent> = grouped
        .values()
        .next()
        .into_iter()
        .flat_map(|events| events.iter())
        .filter(|event| matches!(event, HistoricalEvent::UnderlyingTick { .. }))
        .cloned()
        .collect();
    underlying.sort_by_key(HistoricalEvent::ts_ms);

    let mut output = CampaignOutput {
        episodes: Vec::new(),
        jev_calls: 0,
        jev_hits: 0,
        jev_misses: 0,
        latencies_ms: Vec::new(),
        skipped_no_resolution: 0,
        skipped_no_fill_data: 0,
        skipped_budget: 0,
        skipped_quant_only: 0,
        skipped: 0,
    };

    for (condition_id, stream) in grouped {
        let Some(resolved) = resolutions.get(&condition_id) else {
            output.skip_resolution();
            continue;
        };
        if resolved.spec.condition_id != condition_id || resolved.resolved_at_ms <= 0 {
            output.skip_resolution();
            continue;
        }
        let Some((question, rules)) = specs_lookup
            .get(&resolved.spec.market_id)
            .or_else(|| specs_lookup.get(&condition_id))
        else {
            output.skip_data();
            continue;
        };

        for signal in fixed_signals(&stream, config.per_condition_signals) {
            let Some(built) = build_state(&stream, &underlying, &signal, resolved) else {
                output.skip_data();
                continue;
            };
            let Some(touch) = built.touch else {
                // En replay-sobre-prints "fresh" significa touch observado de prints reales (no un book con depth); el depth es desconocido y la incertidumbre de cola la cubre el fill engine conservador; los fills exigen trade-throughs reales post-arrival; el spread de 1 tick alrededor del last print es supuesto declarado (si el spread real fuera mayor, cotizar al touch es más agresivo: sesgo documentado, mitigado por perfiles Conservative/Base/Optimistic y su comparativa). Nada oculto.
                // Keep the anti-burn budget guard: no print means no touch, no
                // Jev request, and no episode.
                output.skip_data();
                continue;
            };

            for arm in &config.arms {
                let policy = arm.policy();
                if !policy.calls_jev {
                    // QUANT_ONLY has no V1Signal source. It is deliberately
                    // skipped, and never reaches cache lookup or the caller.
                    output.skip_quant_only();
                    continue;
                }

                let features = if policy.uses_micro_regime {
                    &built.micro_features
                } else {
                    &built.features
                };
                let quant = policy
                    .uses_quant
                    .then(|| build_quant(features, &crate::config::QuantConfig::default().params));
                let state = V1State::new(
                    question.clone(),
                    rules.clone(),
                    features.clone(),
                    built.poly.clone(),
                    quant,
                    built.candidate,
                );
                let questions_json_value = QuestionSet::V1.build(built.candidate);
                let state_json = serde_json::to_string(&state)
                    .map_err(|error| format!("serialize campaign V1State: {error}"))?;
                let questions_json = serde_json::to_string(&questions_json_value)
                    .map_err(|error| format!("serialize campaign questions: {error}"))?;
                let key = JevCacheKey::new_versioned(
                    stable_hash(&state_json),
                    stable_hash(&questions_json),
                    MODEL_VERSION.to_owned(),
                    arm.as_str().to_owned(),
                    V1Strategy::VERSION.to_owned(),
                    PROMPT_VERSION.to_owned(),
                    MODEL_VERSION.to_owned(),
                    QUESTION_SCHEMA_VERSION.to_owned(),
                );

                let outcome = if let Some(cached) = cache.get(&key) {
                    RawCampaignOutcome {
                        envelope_json: cached.envelope_json,
                        latency_ms: cached.latency_ms,
                        error: None,
                    }
                } else {
                    if output.jev_calls >= config.max_jev_calls {
                        output.skip_budget();
                        continue;
                    }
                    output.jev_calls = output.jev_calls.saturating_add(1);
                    match caller.call_typed(&state, &state_json, &questions_json) {
                        Ok((envelope_json, latency_ms, live)) => {
                            cache.put(
                                key,
                                CachedJev {
                                    envelope_json: envelope_json.clone(),
                                    latency_ms,
                                    live,
                                    request_json: serde_json::json!({
                                        "model": MODEL_VERSION,
                                        "state": state,
                                        "questions": questions_json_value,
                                    })
                                    .to_string(),
                                    parsed_output_json: String::new(),
                                    jev_start_ts_ms: signal.ts_ms,
                                },
                            );
                            let _ = live;
                            RawCampaignOutcome {
                                envelope_json,
                                latency_ms,
                                error: None,
                            }
                        }
                        Err(error) => RawCampaignOutcome {
                            envelope_json: String::new(),
                            latency_ms: 0,
                            error: Some(error),
                        },
                    }
                };

                output.latencies_ms.push(outcome.latency_ms);
                if outcome.error.is_some()
                    || (config.jev_deadline_ms > 0 && outcome.latency_ms > config.jev_deadline_ms)
                {
                    output.skip_data();
                    continue;
                }
                let response: SystemOneResponse = match serde_json::from_str(&outcome.envelope_json)
                {
                    Ok(response) => response,
                    Err(_) => {
                        output.skip_data();
                        continue;
                    }
                };
                let signal_value: V1Signal = match parse_v1_signal(&response) {
                    Ok(signal_value) => signal_value,
                    Err(_) => {
                        output.skip_data();
                        continue;
                    }
                };

                let action = V1Strategy
                    .on_market_event(
                        &StrategyContext {
                            thresholds: QuoteThresholds::default(),
                            tick_size: TICK_SIZE,
                            min_order_size: MIN_ORDER_SIZE,
                        },
                        &MarketEvent {
                            event_id: format!(
                                "{}:{}:{}:{}",
                                config.run_id,
                                condition_id,
                                signal.ordinal,
                                arm.as_str()
                            ),
                            ts_ms: signal.ts_ms,
                            condition_id: condition_id.clone(),
                            asset: resolved.spec.asset.clone(),
                            horizon: resolved.spec.horizon.clone(),
                            signal: signal_value,
                            yes_bid: touch.bid,
                            yes_ask: touch.ask,
                            book_stale: false,
                            open_qty: 0.0,
                            open_avg_price: 0.0,
                            strategy_version: V1Strategy::VERSION.to_owned(),
                        },
                    )
                    .into_iter()
                    .find_map(|action| match action {
                        Action::PlaceMaker {
                            side: crate::domain::TradeSide::Buy,
                            price,
                            ..
                        } => Some(price),
                        _ => None,
                    });
                let Some(limit_price) = action else {
                    output.skip_data();
                    continue;
                };

                let constraints = MarketConstraints::new(TICK_SIZE, SIZE_STEP, MIN_ORDER_SIZE)?;
                let sized = match size_entry(limit_price, STANDARD_STAKE_USD, &constraints) {
                    Ok(sized) => sized,
                    Err(_) => {
                        output.skip_data();
                        continue;
                    }
                };
                let mut episode = TradeEpisode::new_with_exit_submit_latency(
                    format!(
                        "{}:{}:{}:{}",
                        config.run_id,
                        condition_id,
                        signal.ordinal,
                        arm.as_str()
                    ),
                    V1Strategy::VERSION,
                    resolved.spec.market_id.clone(),
                    resolved.spec.asset.clone(),
                    resolved.spec.horizon.clone(),
                    signal.ts_ms,
                    signal.ts_ms,
                    outcome.latency_ms,
                    SUBMIT_LATENCY_MS,
                    EXIT_SUBMIT_LATENCY_MS,
                    Side::BuyYes,
                    sized.limit_price,
                    STANDARD_STAKE_USD,
                )?;
                episode.apply_sizing(&sized)?;
                episode.set_fill_profile(config.fill_profile.as_str());
                episode.set_is_maker(true);
                episode.set_entry_liquidity("maker")?;
                episode.set_fee_regime(config.historical_regime.label);
                episode.set_prompt_version(PROMPT_VERSION);
                episode.set_jev_model(MODEL_VERSION);
                episode.set_arm(arm.as_str());

                let prints = subsequent_prints(&stream, signal.ts_ms, resolved.resolved_at_ms);
                let fill = FillSimulator::new(config.fill_profile).check_fill(
                    &RestingOrder {
                        price: episode.limit_price,
                        size: episode.shares,
                        resting_from_ms: episode.order_arrival_ts_ms,
                        side_buy: true,
                    },
                    &prints,
                    ExecutionLatency::new(0),
                );
                if let Some(fill_ts_ms) = fill.fill_ts_ms.filter(|_| fill.filled) {
                    episode.apply_fill(
                        fill_ts_ms,
                        fill.fill_price,
                        fill.fill_fraction * episode.shares,
                    )?;
                    let hedge = match finish_episode(
                        &mut episode,
                        exit_policy,
                        config,
                        resolved,
                        &stream,
                    ) {
                        Ok(hedge) => hedge,
                        Err(_) => {
                            output.skip_data();
                            continue;
                        }
                    };
                    output.episodes.push(episode);
                    if let Some(hedge) = hedge {
                        output.episodes.push(hedge);
                    }
                } else {
                    episode.apply_exit(
                        ExitType::NoFill,
                        episode.order_arrival_ts_ms,
                        episode.limit_price,
                    )?;
                    apply_no_fill_fees(&mut episode, config)?;
                    output.episodes.push(episode);
                }
            }
        }
    }

    output.jev_hits = cache.hits;
    output.jev_misses = cache.misses;
    cache.save_to_dir(cache_dir)?;
    Ok(output)
}

/// Input adapter so callers may pass either a flat historical event slice or
/// a pre-grouped condition tape.
pub trait CampaignTape {
    fn grouped(&self) -> TapeByCondition;
}

impl CampaignTape for [HistoricalEvent] {
    fn grouped(&self) -> TapeByCondition {
        group_events(self.iter().cloned())
    }
}

impl CampaignTape for Vec<HistoricalEvent> {
    fn grouped(&self) -> TapeByCondition {
        group_events(self.iter().cloned())
    }
}

impl CampaignTape for BTreeMap<String, Vec<HistoricalEvent>> {
    fn grouped(&self) -> TapeByCondition {
        group_pre_grouped(self.iter())
    }
}

impl CampaignTape for HashMap<String, Vec<HistoricalEvent>> {
    fn grouped(&self) -> TapeByCondition {
        group_pre_grouped(self.iter())
    }
}

fn group_events(events: impl Iterator<Item = HistoricalEvent>) -> TapeByCondition {
    let mut grouped = BTreeMap::new();
    let mut underlying = Vec::new();
    for event in events {
        let condition_id = match &event {
            HistoricalEvent::PolyTrade { condition_id, .. }
            | HistoricalEvent::PolyTop { condition_id, .. } => condition_id.clone(),
            HistoricalEvent::UnderlyingTick { .. } => {
                underlying.push(event);
                continue;
            }
        };
        grouped
            .entry(condition_id)
            .or_insert_with(Vec::new)
            .push(event);
    }
    append_underlying_and_sort(&mut grouped, &underlying);
    grouped
}

fn group_pre_grouped<'a>(
    groups: impl Iterator<Item = (&'a String, &'a Vec<HistoricalEvent>)>,
) -> TapeByCondition {
    let mut grouped = BTreeMap::new();
    let mut underlying = Vec::new();
    for (condition_id, events) in groups {
        for event in events {
            match event {
                HistoricalEvent::PolyTrade {
                    condition_id: event_condition,
                    ..
                }
                | HistoricalEvent::PolyTop {
                    condition_id: event_condition,
                    ..
                } => {
                    grouped
                        .entry(event_condition.clone())
                        .or_insert_with(Vec::new)
                        .push(event.clone());
                }
                HistoricalEvent::UnderlyingTick { .. } => {
                    if !underlying.contains(event) {
                        underlying.push(event.clone());
                    }
                }
            }
        }
        grouped.entry(condition_id.clone()).or_default();
    }
    append_underlying_and_sort(&mut grouped, &underlying);
    grouped
}

fn append_underlying_and_sort(grouped: &mut TapeByCondition, underlying: &[HistoricalEvent]) {
    for events in grouped.values_mut() {
        events.extend(underlying.iter().cloned());
        // Stable timestamp ordering preserves source order for equal-time
        // events. The runner's explicit tick/top/trade tie rank is private;
        // the campaign reports that visibility gap rather than copying it.
        events.sort_by_key(HistoricalEvent::ts_ms);
    }
}

#[derive(Debug)]
struct RawCampaignOutcome {
    envelope_json: String,
    latency_ms: u64,
    error: Option<String>,
}

#[derive(Debug, Clone)]
struct SignalPrint {
    ts_ms: i64,
    ordinal: usize,
    stream_index: usize,
}

#[derive(Debug, Clone, Copy)]
struct BookObservation {
    bid: f64,
    ask: f64,
}

/// En replay-sobre-prints "fresh" significa touch observado de prints reales (no un book con depth); el depth es desconocido y la incertidumbre de cola la cubre el fill engine conservador; los fills exigen trade-throughs reales post-arrival; el spread de 1 tick alrededor del last print es supuesto declarado (si el spread real fuera mayor, cotizar al touch es más agresivo: sesgo documentado, mitigado por perfiles Conservative/Base/Optimistic y su comparativa). Nada oculto.
#[derive(Debug, Clone, Copy)]
struct PrintTouch {
    bid: f64,
    ask: f64,
}

#[derive(Debug, Clone)]
struct BuiltState {
    features: LeadLagFeatures,
    micro_features: LeadLagFeatures,
    poly: PolySnapshot,
    candidate: PriceTicks,
    touch: Option<PrintTouch>,
}

fn finish_episode(
    episode: &mut TradeEpisode,
    exit_policy: ExitPolicy,
    config: &CampaignConfig,
    resolved: &ResolvedMarket,
    stream: &[HistoricalEvent],
) -> Result<Option<TradeEpisode>, String> {
    let hedged = match exit_policy {
        ExitPolicy::Hold | ExitPolicy::RiskExit => false,
        ExitPolicy::HedgeProfit {
            target_profit_usd_per_share,
        } => should_hedge_profit(
            episode.fill_price.unwrap_or(0.0),
            (1.0 - episode.limit_price).max(TICK_SIZE),
            target_profit_usd_per_share,
        ),
        ExitPolicy::HedgeDynamic => true,
    };

    if hedged
        && let Some(mut hedge) = hedge_episode(
            episode,
            stream,
            (1.0 - episode.limit_price - TICK_SIZE).clamp(TICK_SIZE, 1.0 - TICK_SIZE),
            resolved.resolved_at_ms,
            config,
        )?
    {
        set_resolution_lineage(episode, resolved)?;
        set_resolution_lineage(&mut hedge, resolved)?;
        apply_episode_fees(episode, config)?;
        apply_episode_fees(&mut hedge, config)?;
        return Ok(Some(hedge));
    }

    let yes_won = resolved.outcome == ResolutionOutcome::Yes;
    settle_resolution(episode, yes_won, resolved.resolved_at_ms, 1.0)?;
    set_resolution_lineage(episode, resolved)?;
    apply_episode_fees(episode, config)?;
    Ok(None)
}

fn hedge_episode(
    episode: &mut TradeEpisode,
    stream: &[HistoricalEvent],
    opposite_price: f64,
    resolution_ts_ms: i64,
    config: &CampaignConfig,
) -> Result<Option<TradeEpisode>, String> {
    let fill_ts_ms = episode.fill_ts_ms.unwrap_or(0);
    let Some(exit_arrival_ts_ms) = fill_ts_ms.checked_add(EXIT_SUBMIT_LATENCY_MS as i64) else {
        return Ok(None);
    };
    if exit_arrival_ts_ms > resolution_ts_ms {
        return Ok(None);
    }
    let hedge_order = RestingOrder {
        price: opposite_price,
        size: episode.fill_qty.unwrap_or(0.0),
        resting_from_ms: exit_arrival_ts_ms,
        side_buy: true,
    };
    let hedge_prints: Vec<FillPrint> = stream
        .iter()
        .filter_map(|event| match event {
            HistoricalEvent::PolyTrade {
                ts_ms,
                price,
                size,
                aggressor,
                ..
            } if *ts_ms >= exit_arrival_ts_ms && *ts_ms <= resolution_ts_ms => {
                Some(FillPrint::new(
                    *ts_ms,
                    1.0 - *price,
                    *size,
                    inverse_aggressor(aggressor.as_deref()),
                ))
            }
            _ => None,
        })
        .collect();
    let outcome = FillSimulator::new(config.fill_profile).check_fill(
        &hedge_order,
        &hedge_prints,
        ExecutionLatency::new(0),
    );
    let Some(fill_ts_ms) = outcome
        .fill_ts_ms
        .filter(|_| outcome.filled && outcome.fill_fraction >= 1.0)
    else {
        return Ok(None);
    };
    let fill_price = outcome.fill_price;
    let quantity = episode.fill_qty.unwrap_or(0.0);
    let mut hedge = TradeEpisode::new_with_exit_submit_latency(
        format!("{}:hedge", episode.episode_id),
        episode.strategy_version.clone(),
        episode.market.clone(),
        episode.asset.clone(),
        episode.horizon.clone(),
        fill_ts_ms,
        fill_ts_ms,
        0,
        0,
        0,
        Side::BuyNo,
        opposite_price,
        quantity * opposite_price,
    )?;
    hedge.apply_fill(fill_ts_ms, fill_price, quantity * outcome.fill_fraction)?;
    hedge.set_fill_profile(config.fill_profile.as_str());
    hedge.set_is_maker(true);
    hedge.set_entry_liquidity("maker")?;
    hedge.set_exit_liquidity("maker")?;
    hedge.set_fee_regime(config.historical_regime.label);
    hedge.set_prompt_version(episode.prompt_version.clone().unwrap_or_default());
    hedge.set_jev_model(episode.jev_model.clone().unwrap_or_default());
    hedge.set_arm(episode.arm.clone().unwrap_or_default());

    // The settlement helper requires both real legs and assigns the pair
    // lineage atomically. Zero exit-submit latency is intentional here: the
    // hedge fill has already been delayed by EXIT_SUBMIT_LATENCY_MS above.
    episode.exit_submit_latency_ms = 0;
    settle_hedge_pair(episode, &mut hedge);
    episode.set_exit_liquidity("maker")?;
    Ok(Some(hedge))
}

fn apply_episode_fees(episode: &mut TradeEpisode, config: &CampaignConfig) -> Result<(), String> {
    let fill_notional = episode
        .fill_price
        .zip(episode.fill_qty)
        .map_or(0.0, |(price, qty)| price * qty);
    let exit_notional = episode
        .exit_price
        .zip(episode.fill_qty)
        .map_or(0.0, |(price, qty)| price * qty);
    apply_to_episode(
        episode,
        episode.gross_pnl_usd,
        fill_notional,
        exit_notional,
        true,
        &config.historical_regime,
        &config.current_regime,
    )
}

fn apply_no_fill_fees(episode: &mut TradeEpisode, config: &CampaignConfig) -> Result<(), String> {
    apply_to_episode(
        episode,
        0.0,
        0.0,
        0.0,
        true,
        &config.historical_regime,
        &config.current_regime,
    )
}

fn set_resolution_lineage(
    episode: &mut TradeEpisode,
    resolved: &ResolvedMarket,
) -> Result<(), String> {
    episode.set_resolution(resolved.resolved_at_ms)?;
    episode.set_resolution_outcome(resolved.outcome.as_str())?;
    episode.set_resolution_provenance(resolved.provenance.as_str())?;
    episode.set_outcome_provenance(resolved.outcome_provenance.as_str())?;
    episode.set_resolution_time_provenance(resolved.time_provenance.as_str())
}

fn fixed_signals(stream: &[HistoricalEvent], limit: usize) -> Vec<SignalPrint> {
    let mut signals = Vec::new();
    for (stream_index, event) in stream.iter().enumerate() {
        if let HistoricalEvent::PolyTrade { ts_ms, .. } = event {
            if signals.len() >= limit {
                break;
            }
            signals.push(SignalPrint {
                ts_ms: *ts_ms,
                ordinal: signals.len(),
                stream_index,
            });
        }
    }
    signals
}

fn subsequent_prints(
    stream: &[HistoricalEvent],
    signal_ts_ms: i64,
    resolution_ts_ms: i64,
) -> Vec<FillPrint> {
    stream
        .iter()
        .filter_map(|event| match event {
            HistoricalEvent::PolyTrade {
                ts_ms,
                price,
                size,
                aggressor,
                ..
            } if *ts_ms > signal_ts_ms && *ts_ms <= resolution_ts_ms => Some(FillPrint::new(
                *ts_ms,
                *price,
                *size,
                parse_aggressor(aggressor.as_deref()),
            )),
            _ => None,
        })
        .collect()
}

fn build_state(
    stream: &[HistoricalEvent],
    underlying: &[HistoricalEvent],
    signal: &SignalPrint,
    resolved: &ResolvedMarket,
) -> Option<BuiltState> {
    let prior = &stream[..signal.stream_index];
    let touch = print_touch_at(stream, signal.ts_ms);
    let mut history = PolyHistory::new(1024);
    let mut book = None;
    for event in prior {
        match event {
            HistoricalEvent::PolyTop {
                ts_ms,
                best_bid,
                best_ask,
                ..
            } if valid_book(*best_bid, *best_ask) => {
                book = Some(BookObservation {
                    bid: *best_bid,
                    ask: *best_ask,
                });
                history.push(
                    (*ts_ms).max(0) as u64,
                    PriceTicks::from_f64(((*best_bid + *best_ask) / 2.0).clamp(0.0, 1.0)),
                );
            }
            HistoricalEvent::PolyTrade { ts_ms, .. } => {
                if let Some(book) = book {
                    history.push(
                        (*ts_ms).max(0) as u64,
                        PriceTicks::from_f64(((book.bid + book.ask) / 2.0).clamp(0.0, 1.0)),
                    );
                }
            }
            _ => {}
        }
    }

    let spot_events: Vec<&HistoricalEvent> = underlying
        .iter()
        .filter(|event| {
            event.ts_ms() <= signal.ts_ms
                && matches!(
                    event,
                    HistoricalEvent::UnderlyingTick { asset, .. }
                        if asset == &resolved.spec.asset
                )
        })
        .collect();
    let tick_start = spot_events.len().saturating_sub(600);
    let ticks: Vec<ExternalTick> = spot_events[tick_start..]
        .iter()
        .filter_map(|event| match event {
            HistoricalEvent::UnderlyingTick { ts_ms, price, .. } if valid_price(*price) => {
                Some(ExternalTick {
                    ts_ms: (*ts_ms).max(0) as u64,
                    price: *price,
                })
            }
            _ => None,
        })
        .collect();
    let spot = ticks.last().map_or(100.0, |tick| tick.price);
    let perp = underlying
        .iter()
        .filter_map(|event| match event {
            HistoricalEvent::UnderlyingTick {
                ts_ms,
                asset,
                price,
                ..
            } if *ts_ms <= signal.ts_ms
                && asset == &format!("{}-PERP", resolved.spec.asset)
                && valid_price(*price) =>
            {
                Some(*price)
            }
            _ => None,
        })
        .next_back()
        .unwrap_or(spot);
    let remaining = resolved.resolved_at_ms.saturating_sub(signal.ts_ms).max(0) as u64 / 1_000;
    let context = ResolutionContext::new(spot, remaining, "binance-replay");
    let spot_flow = flow_for_asset(underlying, &resolved.spec.asset, signal.ts_ms);
    let venues = VenueMicroprices {
        binance: spot,
        coinbase: spot,
        perp,
        perp_basis_pct: if spot > 0.0 {
            (perp - spot) / spot * 100.0
        } else {
            0.0
        },
    };
    let features = build_features_with_context(&ticks, &context, venues, spot_flow);
    let poly_flow = poly_flow_for(stream, signal.stream_index, signal.ts_ms);
    let contract = ContractContext::new(
        resolved.spec.asset.clone(),
        resolved.spec.horizon.clone(),
        super::runner::horizon_secs(&resolved.spec.horizon),
    );
    let first_poly_ts = stream
        .iter()
        .find_map(|event| match event {
            HistoricalEvent::PolyTrade { ts_ms, .. } | HistoricalEvent::PolyTop { ts_ms, .. } => {
                Some(*ts_ms)
            }
            HistoricalEvent::UnderlyingTick { .. } => None,
        })
        .unwrap_or(signal.ts_ms);
    let micro_target = underlying
        .iter()
        .filter_map(|event| match event {
            HistoricalEvent::UnderlyingTick {
                ts_ms,
                asset,
                price,
                ..
            } if *ts_ms <= first_poly_ts
                && asset == &resolved.spec.asset
                && valid_price(*price) =>
            {
                Some(*price)
            }
            _ => None,
        })
        .next_back()
        .unwrap_or(100.0);
    let micro_context = ResolutionContext::new(micro_target, remaining, "binance-replay");
    let dense_tick_start = spot_events.len().saturating_sub(3_600);
    let dense_ticks: Vec<ExternalTick> = spot_events[dense_tick_start..]
        .iter()
        .filter_map(|event| match event {
            HistoricalEvent::UnderlyingTick { ts_ms, price, .. } if valid_price(*price) => {
                Some(ExternalTick {
                    ts_ms: (*ts_ms).max(0) as u64,
                    price: *price,
                })
            }
            _ => None,
        })
        .collect();
    let micro_features = build_features_micro(
        &dense_ticks,
        &micro_context,
        &contract,
        venues,
        spot_flow,
        poly_flow,
    );

    let (candidate, poly) = if let Some(book) = book {
        history.push(
            signal.ts_ms.max(0) as u64,
            PriceTicks::from_f64(((book.bid + book.ask) / 2.0).clamp(0.0, 1.0)),
        );
        let mut order_book = OrderBook::default();
        order_book.apply_snapshot(
            [(PriceTicks::from_f64(book.bid), 100)],
            [(PriceTicks::from_f64(book.ask), 100)],
        );
        let candidate = candidate_maker_price(&order_book, TickSize::from_f64(TICK_SIZE))?;
        let mid = (book.bid + book.ask) / 2.0;
        let mut poly = PolySnapshot {
            yes_bid: PriceTicks::from_f64(book.bid),
            yes_ask: PriceTicks::from_f64(book.ask),
            bid_depth: 100.0,
            ask_depth: 100.0,
            spread: (book.ask - book.bid).max(0.0),
            book_imbalance: 0.0,
            last_trade_price: PriceTicks::from_f64(mid),
            price_1s_ago: PriceTicks::from_f64(0.0),
            price_5s_ago: PriceTicks::from_f64(0.0),
            price_30s_ago: PriceTicks::from_f64(0.0),
        };
        history.apply_to_snapshot(signal.ts_ms.max(0) as u64, &mut poly);
        (candidate, poly)
    } else {
        // Keep the last print available for state construction; the derived
        // touch is passed separately to V1 through `MarketEvent`.
        let last_price =
            stream[..=signal.stream_index]
                .iter()
                .rev()
                .find_map(|event| match event {
                    HistoricalEvent::PolyTrade { price, .. } if valid_price(*price) => Some(*price),
                    _ => None,
                })?;
        let last_price = PriceTicks::from_f64(last_price);
        let poly = PolySnapshot {
            yes_bid: PriceTicks::from_f64(0.0),
            yes_ask: PriceTicks::from_f64(0.0),
            bid_depth: 0.0,
            ask_depth: 0.0,
            spread: 0.0,
            book_imbalance: 0.0,
            last_trade_price: last_price,
            price_1s_ago: PriceTicks::from_f64(0.0),
            price_5s_ago: PriceTicks::from_f64(0.0),
            price_30s_ago: PriceTicks::from_f64(0.0),
        };
        (last_price, poly)
    };

    let candidate = touch
        .and_then(candidate_price_for_touch)
        .unwrap_or(candidate);

    Some(BuiltState {
        features,
        micro_features,
        poly,
        candidate,
        touch,
    })
}

/// Builds a PIT print-derived touch. Every print considered here has
/// `ts_ms <= signal_ts_ms`; no future tape event can enter the quote state.
fn print_touch_at(stream: &[HistoricalEvent], signal_ts_ms: i64) -> Option<PrintTouch> {
    let mut last_print = None;
    let mut touch_bid = None;
    let mut touch_ask = None;

    for event in stream {
        let HistoricalEvent::PolyTrade {
            ts_ms,
            price,
            aggressor,
            ..
        } = event
        else {
            continue;
        };
        if *ts_ms > signal_ts_ms || !valid_price(*price) {
            continue;
        }

        last_print = Some(*price);
        match parse_aggressor(aggressor.as_deref()) {
            Aggressor::Sell => touch_bid = Some(*price),
            Aggressor::Buy => touch_ask = Some(*price),
            Aggressor::Unknown => {}
        }
    }

    let last_print = last_print?;
    let bid = touch_bid.unwrap_or_else(|| {
        // Fallback, not an observation: no aggressive SELL print was available.
        (last_print - TICK_SIZE).max(0.0)
    });
    let ask = touch_ask.unwrap_or_else(|| {
        // Fallback, not an observation: no aggressive BUY print was available.
        (last_print + TICK_SIZE).min(1.0)
    });
    Some(PrintTouch { bid, ask })
}

fn candidate_price_for_touch(touch: PrintTouch) -> Option<PriceTicks> {
    // Compute the same integer post-only price as V1 without constructing a
    // synthetic depth-bearing book from prints.
    let bid = PriceTicks::from_f64(touch.bid).as_micros();
    let tick = (TickSize::from_f64(TICK_SIZE).to_f64() * 1_000_000.0).round() as u64;
    let raw = bid.checked_add(tick)?;
    let candidate_micros = raw.div_ceil(tick).checked_mul(tick)?;
    (candidate_micros <= 1_000_000)
        .then(|| PriceTicks::from_f64(candidate_micros as f64 / 1_000_000.0))
}

fn flow_for_asset(events: &[HistoricalEvent], asset: &str, now_ms: i64) -> OrderFlowAggregates {
    let mut buy_1s = 0.0;
    let mut sell_1s = 0.0;
    let mut buy_5s = 0.0;
    let mut sell_5s = 0.0;
    for event in events {
        let HistoricalEvent::UnderlyingTick {
            ts_ms,
            asset: event_asset,
            qty,
            aggressor,
            ..
        } = event
        else {
            continue;
        };
        if event_asset != asset || *ts_ms > now_ms || *ts_ms < now_ms - 5_000 {
            continue;
        }
        let Some(qty) = qty.filter(|qty| qty.is_finite() && *qty > 0.0) else {
            continue;
        };
        let Some(is_buy) = trade_side(aggressor.as_deref()) else {
            continue;
        };
        if is_buy {
            buy_5s += qty;
            if *ts_ms >= now_ms - 1_000 {
                buy_1s += qty;
            }
        } else {
            sell_5s += qty;
            if *ts_ms >= now_ms - 1_000 {
                sell_1s += qty;
            }
        }
    }
    let total_5s = buy_5s + sell_5s;
    OrderFlowAggregates {
        buy_vol_1s: buy_1s,
        sell_vol_1s: sell_1s,
        ofi_1s: buy_1s - sell_1s,
        ofi_5s: buy_5s - sell_5s,
        imbalance: if total_5s > 0.0 {
            (buy_5s - sell_5s) / total_5s
        } else {
            0.0
        },
        aggressive_buy_ratio: if total_5s > 0.0 {
            buy_5s / total_5s
        } else {
            0.5
        },
    }
}

fn poly_flow_for(
    stream: &[HistoricalEvent],
    signal_stream_index: usize,
    now_ms: i64,
) -> OrderFlowAggregates {
    let mut buy = 0.0;
    let mut sell = 0.0;
    for event in &stream[..=signal_stream_index] {
        let HistoricalEvent::PolyTrade {
            ts_ms,
            size,
            aggressor,
            ..
        } = event
        else {
            continue;
        };
        if *ts_ms < now_ms - 5_000 || *ts_ms > now_ms {
            continue;
        }
        match parse_aggressor(aggressor.as_deref()) {
            Aggressor::Buy => buy += size.max(0.0),
            Aggressor::Sell => sell += size.max(0.0),
            Aggressor::Unknown => {}
        }
    }
    let total = buy + sell;
    OrderFlowAggregates {
        buy_vol_1s: buy,
        sell_vol_1s: sell,
        ofi_1s: 0.0,
        ofi_5s: buy - sell,
        imbalance: 0.0,
        aggressive_buy_ratio: if total > 0.0 { buy / total } else { 0.5 },
    }
}

fn parse_exit_policy(value: &str) -> Result<ExitPolicy, String> {
    let normalized = value.trim().to_ascii_uppercase();
    match normalized.as_str() {
        "HOLD" | "RESOLUTION" => Ok(ExitPolicy::Hold),
        "HEDGE_DYNAMIC" | "HEDGE" => Ok(ExitPolicy::HedgeDynamic),
        "RISK_EXIT" => Ok(ExitPolicy::RiskExit),
        value if value.starts_with("HEDGE_PROFIT:") => value[13..]
            .parse::<f64>()
            .map(|target_profit_usd_per_share| ExitPolicy::HedgeProfit {
                target_profit_usd_per_share,
            })
            .map_err(|_| format!("invalid HEDGE_PROFIT target in {value}")),
        _ => Err(format!("unsupported campaign exit_policy `{value}`")),
    }
}

fn stable_hash(value: &str) -> String {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}

fn valid_price(price: f64) -> bool {
    price.is_finite() && (0.0..=1.0).contains(&price) && price > 0.0 && price < 1.0
}

fn valid_book(bid: f64, ask: f64) -> bool {
    valid_price(bid) && valid_price(ask) && bid <= ask
}

fn parse_aggressor(value: Option<&str>) -> Aggressor {
    match value.map(str::to_ascii_uppercase).as_deref() {
        Some("BUY") => Aggressor::Buy,
        Some("SELL") => Aggressor::Sell,
        _ => Aggressor::Unknown,
    }
}

fn inverse_aggressor(value: Option<&str>) -> Aggressor {
    match value.map(str::to_ascii_uppercase).as_deref() {
        Some("BUY") => Aggressor::Sell,
        Some("SELL") => Aggressor::Buy,
        _ => Aggressor::Unknown,
    }
}

fn trade_side(value: Option<&str>) -> Option<bool> {
    match value {
        Some("BUY") => Some(true),
        Some("SELL") => Some(false),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stable_hash_is_process_independent() {
        assert_eq!(stable_hash("campaign"), stable_hash("campaign"));
        assert_ne!(stable_hash("campaign"), stable_hash("different"));
    }

    #[test]
    fn quant_only_is_a_hard_no_call_arm() {
        assert!(!Arm::QuantOnly.policy().calls_jev);
        assert!(Arm::JevOnly.policy().calls_jev);
    }
}
