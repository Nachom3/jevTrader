//! Ten-point, provider-free integration dry-run over the allowed processed tape.
//!
//! This binary deliberately reads only the four small processed parquet inputs.
//! It does not read any underlying parquet file and does not invoke Jev or the
//! network. The processed tape supplies timestamps, prices, aggressors,
//! quantities, outcomes, and resolution-spec bounds. The V1 signal itself is a
//! fixed contract fixture because those four inputs do not contain Jev answer
//! columns; this limitation is reported by the test rather than hidden.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::path::{Path, PathBuf};

use arrow::array::{
    Array, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, LargeStringArray,
    StringArray, UInt32Array, UInt64Array,
};
use chrono::{DateTime, NaiveDateTime};
use jevtrader::config::QuoteThresholds;
use jevtrader::jev::{TickDistribution, V1Signal};
use jevtrader::replay::fills::{Aggressor, FillPrint};
use jevtrader::replay::jev_cache::{CachedJev, JevCacheKey};
use jevtrader::replay::{
    Arm, ArmRun, BlockBootstrap, EpisodeMetricsInput, ExitType, FillProfile, FillSimulator,
    MarketConstraints, MarketSpan, OosReport, Portfolio, ResolutionTimeFilter, RestingOrder, Side,
    TradeEpisode, apply_to_episode, oos_report_with_draws, passes, path_of, plan_windows,
    purge_train, settle_resolution, size_entry, summarize,
};
use jevtrader::strategy::api::{Action, MarketEvent, Strategy, StrategyContext, V1Strategy};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

const TRADE_FILE: &str = "research-data/processed/polymarket_trades.parquet";
const RESOLUTION_FILE: &str = "research-data/processed/resolutions.parquet";
const SPEC_FILE: &str = "research-data/processed/resolution_specs.parquet";
const REGIME_FILE: &str = "research-data/processed/market_regimes.parquet";
const TICK: f64 = 0.01;
const STAKE_USD: f64 = 5.0;
const JEV_LATENCY_MS: u64 = 320;
const SUBMIT_LATENCY_MS: u64 = 50;
const EPSILON: f64 = 1e-6;

#[derive(Debug, Clone)]
struct Row {
    cells: BTreeMap<String, String>,
}

#[derive(Debug, Clone)]
struct Trade {
    ts_s: i64,
    market_id: String,
    yes_price: f64,
    qty: f64,
    aggressor: Aggressor,
    outcome_label: Option<String>,
    original_token: Option<String>,
}

#[derive(Debug, Clone)]
struct Condition {
    condition_id: String,
    market_id: String,
    asset: String,
    horizon: String,
    trades: Vec<Trade>,
    start_at_ms: Option<i64>,
    resolution_at_ms: i64,
    yes_won: bool,
    fidelity: String,
}

#[derive(Debug, Clone, Copy)]
struct SignalCandidate {
    index: usize,
    signal_ts_ms: i64,
    bid: f64,
    ask: f64,
    action_price: f64,
    sized: jevtrader::replay::SizedOrder,
    fill: jevtrader::replay::fills::FillOutcome,
}

fn read_rows(path: &str) -> Vec<Row> {
    let file =
        File::open(path).unwrap_or_else(|error| panic!("open allowed input {path}: {error}"));
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap_or_else(|error| panic!("read parquet metadata {path}: {error}"))
        .with_batch_size(8192)
        .build()
        .unwrap_or_else(|error| panic!("build parquet reader {path}: {error}"));

    let mut rows = Vec::new();
    for batch in reader {
        let batch = batch.unwrap_or_else(|error| panic!("read parquet batch {path}: {error}"));
        for row in 0..batch.num_rows() {
            let mut cells = BTreeMap::new();
            for field in batch.schema().fields() {
                if let Some(value) = array_text(
                    batch.column(batch.schema().index_of(field.name()).unwrap()),
                    row,
                ) {
                    cells.insert(field.name().clone(), value);
                }
            }
            rows.push(Row { cells });
        }
    }
    rows
}

fn array_text(array: &dyn Array, row: usize) -> Option<String> {
    if array.is_null(row) {
        return None;
    }
    if let Some(values) = array.as_any().downcast_ref::<StringArray>() {
        return Some(values.value(row).to_owned());
    }
    if let Some(values) = array.as_any().downcast_ref::<LargeStringArray>() {
        return Some(values.value(row).to_owned());
    }
    if let Some(values) = array.as_any().downcast_ref::<Float64Array>() {
        return Some(values.value(row).to_string());
    }
    if let Some(values) = array.as_any().downcast_ref::<Float32Array>() {
        return Some((values.value(row) as f64).to_string());
    }
    if let Some(values) = array.as_any().downcast_ref::<Int64Array>() {
        return Some(values.value(row).to_string());
    }
    if let Some(values) = array.as_any().downcast_ref::<Int32Array>() {
        return Some(values.value(row).to_string());
    }
    if let Some(values) = array.as_any().downcast_ref::<UInt64Array>() {
        return Some(values.value(row).to_string());
    }
    if let Some(values) = array.as_any().downcast_ref::<UInt32Array>() {
        return Some(values.value(row).to_string());
    }
    if let Some(values) = array.as_any().downcast_ref::<BooleanArray>() {
        return Some(values.value(row).to_string());
    }
    None
}

fn text<'a>(row: &'a Row, name: &str) -> Option<&'a str> {
    row.cells.get(name).map(String::as_str)
}

fn first_text(row: &Row, names: &[&str]) -> Option<String> {
    names
        .iter()
        .find_map(|name| text(row, name).map(str::to_owned))
}

fn f64_value(row: &Row, name: &str) -> Option<f64> {
    text(row, name)?.parse().ok()
}

fn i64_value(row: &Row, name: &str) -> Option<i64> {
    text(row, name)?.parse().ok()
}

fn parse_timestamp_ms(raw: &str) -> Option<i64> {
    if let Ok(value) = raw.parse::<i64>() {
        let magnitude = value.unsigned_abs();
        return if magnitude >= 100_000_000_000_000_000 {
            value.checked_div(1_000_000)
        } else if magnitude >= 100_000_000_000_000 {
            value.checked_div(1_000)
        } else if magnitude >= 100_000_000_000 {
            Some(value)
        } else {
            value.checked_mul(1_000)
        };
    }
    if let Ok(value) = DateTime::parse_from_rfc3339(raw) {
        return Some(value.timestamp_millis());
    }
    NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S%.f")
        .ok()
        .map(|value| value.and_utc().timestamp_millis())
}

fn parse_aggressor(value: Option<&str>) -> Aggressor {
    match value.map(str::to_ascii_uppercase).as_deref() {
        Some("BUY") => Aggressor::Buy,
        Some("SELL") => Aggressor::Sell,
        _ => Aggressor::Unknown,
    }
}

fn parse_trades() -> BTreeMap<String, Vec<Trade>> {
    let rows = read_rows(TRADE_FILE);
    assert_eq!(
        rows.len(),
        6866,
        "the dry-run must use the known small tape"
    );
    let mut grouped = BTreeMap::<String, Vec<Trade>>::new();
    for row in rows {
        let (Some(ts_s), Some(market_id), Some(condition_id), Some(yes_price)) = (
            i64_value(&row, "ts"),
            text(&row, "market_id"),
            text(&row, "condition_id"),
            f64_value(&row, "yes_price"),
        ) else {
            continue;
        };
        if !yes_price.is_finite() || !(0.0..=1.0).contains(&yes_price) {
            continue;
        }
        let qty = f64_value(&row, "amount")
            .or_else(|| f64_value(&row, "usd_amount").map(|usd| usd / yes_price))
            .filter(|value| value.is_finite() && *value > 0.0)
            .unwrap_or(0.0);
        let trade = Trade {
            ts_s,
            market_id: market_id.to_owned(),
            yes_price,
            qty,
            aggressor: parse_aggressor(first_text(&row, &["aggressor"]).as_deref()),
            outcome_label: first_text(&row, &["outcome_label"]),
            original_token: first_text(&row, &["original_token"]),
        };
        grouped
            .entry(condition_id.to_owned())
            .or_default()
            .push(trade);
    }
    for trades in grouped.values_mut() {
        trades.sort_by_key(|trade| trade.ts_s);
    }
    grouped
}

fn parse_conditions(
    trades_by_condition: BTreeMap<String, Vec<Trade>>,
) -> BTreeMap<String, Condition> {
    let resolutions = read_rows(RESOLUTION_FILE);
    let specs = read_rows(SPEC_FILE);
    let _regime_rows = read_rows(REGIME_FILE);
    let resolutions: BTreeMap<String, (String, String)> = resolutions
        .into_iter()
        .filter_map(|row| {
            Some((
                text(&row, "condition_id")?.to_owned(),
                (
                    first_text(&row, &["resolution_status", "status"])?,
                    first_text(&row, &["winning_outcome", "outcome"])?,
                ),
            ))
        })
        .collect();
    let specs: BTreeMap<String, (Option<i64>, i64, String)> = specs
        .into_iter()
        .filter_map(|row| {
            let condition_id = text(&row, "condition_id")?.to_owned();
            let end_at_ms = first_text(&row, &["end_at", "resolution_at"])
                .and_then(|value| parse_timestamp_ms(&value))?;
            Some((
                condition_id,
                (
                    first_text(&row, &["start_at"]).and_then(|value| parse_timestamp_ms(&value)),
                    end_at_ms,
                    first_text(&row, &["fidelity"]).unwrap_or_else(|| "UNKNOWN".to_owned()),
                ),
            ))
        })
        .collect();

    let mut conditions = BTreeMap::new();
    for (condition_id, trades) in trades_by_condition {
        if trades.len() < 4 {
            continue;
        }
        let Some((status, winning_outcome)) = resolutions.get(&condition_id) else {
            continue;
        };
        if !status.eq_ignore_ascii_case("resolved") {
            continue;
        }
        let Some((start_at_ms, resolution_at_ms, fidelity)) = specs.get(&condition_id) else {
            continue;
        };
        let outcome = winning_outcome.to_ascii_lowercase();
        let Some(yes_won) = (match outcome.as_str() {
            "up" | "yes" => Some(true),
            "down" | "no" => Some(false),
            _ => None,
        }) else {
            continue;
        };
        if !has_unambiguous_outcome_tape(&trades) {
            continue;
        }
        let market_id = trades[0].market_id.clone();
        let (asset, horizon) = market_identity(&market_id);
        conditions.insert(
            condition_id.clone(),
            Condition {
                condition_id,
                market_id,
                asset,
                horizon,
                trades,
                start_at_ms: *start_at_ms,
                resolution_at_ms: *resolution_at_ms,
                yes_won,
                fidelity: fidelity.clone(),
            },
        );
    }
    conditions
}

fn has_unambiguous_outcome_tape(trades: &[Trade]) -> bool {
    let mut token_labels = BTreeMap::<String, BTreeSet<String>>::new();
    for trade in trades {
        let Some(label) = trade.outcome_label.as_deref() else {
            continue;
        };
        let label = label.to_ascii_lowercase();
        if label != "up" && label != "down" {
            continue;
        }
        let token = trade
            .original_token
            .clone()
            .unwrap_or_else(|| label.clone());
        token_labels.entry(token).or_default().insert(label);
    }
    let labels: BTreeSet<String> = token_labels.values().flatten().cloned().collect();
    labels == BTreeSet::from(["down".to_owned(), "up".to_owned()])
        && token_labels.values().all(|labels| labels.len() == 1)
}

fn market_identity(market_id: &str) -> (String, String) {
    let asset = market_id
        .split(['-', '_'])
        .next()
        .filter(|value| !value.is_empty())
        .map(str::to_ascii_uppercase)
        .unwrap_or_else(|| "UNKNOWN".to_owned());
    let horizon = if market_id.contains("15m") {
        "15m"
    } else if market_id.contains("1h") {
        "1h"
    } else if market_id.contains("4h") {
        "4h"
    } else {
        "5m"
    };
    (asset, horizon.to_owned())
}

fn trade_ts_ms(trade: &Trade) -> i64 {
    trade
        .ts_s
        .checked_mul(1_000)
        .expect("known epoch seconds must fit i64 milliseconds")
}

fn v1_signal_fixture() -> V1Signal {
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

fn strategy_context() -> StrategyContext {
    StrategyContext {
        thresholds: QuoteThresholds::default(),
        tick_size: TICK,
        min_order_size: 0.01,
    }
}

fn signal_candidates(condition: &Condition) -> Vec<SignalCandidate> {
    let constraints = MarketConstraints::new(TICK, 0.01, 0.01).expect("fixed venue constraints");
    let mut candidates = Vec::new();
    for index in 1..condition.trades.len() {
        let signal_trade = &condition.trades[index];
        let bid = signal_trade.yes_price;
        // The permitted corpus has no top-of-book file. Both prices are
        // therefore observed tape prices; this test never claims they are L2
        // quotes or fabricates a book snapshot.
        let Some(ask) = condition.trades[..index]
            .iter()
            .rev()
            .map(|trade| trade.yes_price)
            .find(|price| *price > bid + TICK)
        else {
            continue;
        };
        let event = MarketEvent {
            event_id: format!("{}:{index}", condition.condition_id),
            ts_ms: trade_ts_ms(signal_trade),
            condition_id: condition.condition_id.clone(),
            asset: condition.asset.clone(),
            horizon: condition.horizon.clone(),
            signal: v1_signal_fixture(),
            yes_bid: bid,
            yes_ask: ask,
            book_stale: false,
            open_qty: 0.0,
            open_avg_price: 0.0,
            strategy_version: V1Strategy::VERSION.to_owned(),
        };
        let mut strategy = V1Strategy;
        let actions = strategy.on_market_event(&strategy_context(), &event);
        let [Action::PlaceMaker { price, .. }] = actions.as_slice() else {
            continue;
        };
        let Ok(sized) = size_entry(*price, STAKE_USD, &constraints) else {
            continue;
        };
        let resting_from_ms = event
            .ts_ms
            .checked_add(JEV_LATENCY_MS as i64)
            .expect("real signal timestamp plus fixed replay latency");
        let prints: Vec<FillPrint> = condition.trades[index + 1..]
            .iter()
            .filter(|trade| trade_ts_ms(trade) <= condition.resolution_at_ms)
            .map(|trade| {
                FillPrint::new(
                    trade_ts_ms(trade),
                    trade.yes_price,
                    trade.qty,
                    trade.aggressor,
                )
            })
            .collect();
        let fill = FillSimulator::new(FillProfile::Optimistic).check_fill(
            &RestingOrder {
                price: *price,
                size: sized.shares,
                resting_from_ms,
                side_buy: true,
            },
            &prints,
            jevtrader::replay::ExecutionLatency::new(SUBMIT_LATENCY_MS),
        );
        if fill.filled && fill.fill_ts_ms.is_some() && fill.fill_fraction > 0.0 {
            candidates.push(SignalCandidate {
                index,
                signal_ts_ms: event.ts_ms,
                bid,
                ask,
                action_price: *price,
                sized,
                fill,
            });
        }
    }
    candidates
}

fn build_dry_run(condition: &Condition, candidates: &[SignalCandidate]) -> Vec<TradeEpisode> {
    candidates
        .iter()
        .enumerate()
        .map(|(ordinal, candidate)| {
            let mut episode = TradeEpisode::new_with_exit_submit_latency(
                format!("dryrun:{}:{ordinal}", condition.condition_id),
                V1Strategy::VERSION,
                condition.market_id.clone(),
                condition.asset.clone(),
                condition.horizon.clone(),
                candidate.signal_ts_ms,
                candidate.signal_ts_ms,
                JEV_LATENCY_MS,
                SUBMIT_LATENCY_MS,
                0,
                Side::BuyYes,
                candidate.action_price,
                STAKE_USD,
            )
            .expect("real tape signal must make a valid episode");
            episode
                .apply_sizing(&candidate.sized)
                .expect("fixed sizing must apply before fill");
            let fill_ts_ms = candidate.fill.fill_ts_ms.expect("candidate has a fill");
            let fill_qty = candidate.sized.shares * candidate.fill.fill_fraction;
            episode
                .apply_fill(fill_ts_ms, candidate.fill.fill_price, fill_qty)
                .expect("real tape fill must follow order arrival");
            episode.set_fill_profile(FillProfile::Optimistic.as_str());
            episode.set_is_maker(true);
            episode
                .set_entry_liquidity("maker")
                .expect("maker is a valid entry liquidity");
            episode.set_prompt_version("dryrun-fixed-signal-v1");
            episode.set_jev_model("not-called");
            settle_resolution(
                &mut episode,
                condition.yes_won,
                condition.resolution_at_ms,
                1.0,
            )
            .expect("real resolution timestamp must follow fill");
            episode
                .set_resolution_outcome(if condition.yes_won { "yes" } else { "no" })
                .expect("resolution timestamp is present");
            episode
                .set_resolution_provenance("proxy")
                .expect("resolution timestamp provenance is present");
            episode
                .set_outcome_provenance("exact")
                .expect("outcome provenance is present");
            episode
                .set_resolution_time_provenance("proxy")
                .expect("resolution time provenance is present");
            let gross_pnl_usd = episode.gross_pnl_usd;
            let exit_notional = episode.exit_price.unwrap_or(0.0) * fill_qty;
            apply_to_episode(
                &mut episode,
                gross_pnl_usd,
                candidate.fill.fill_price * fill_qty,
                exit_notional,
                true,
                &jevtrader::replay::zero_regime(),
                &jevtrader::replay::zero_regime(),
            )
            .expect("zero-fee ledger must reconcile");
            episode.set_fee_regime("zero");
            episode
        })
        .collect()
}

fn assert_close(actual: f64, expected: f64) {
    assert!(
        (actual - expected).abs() <= EPSILON,
        "actual={actual:.12}, expected={expected:.12}, delta={:.12}",
        (actual - expected).abs()
    );
}

fn assert_cached_equal(left: &CachedJev, right: &CachedJev) {
    assert_eq!(left.envelope_json, right.envelope_json);
    assert_eq!(left.latency_ms, right.latency_ms);
    assert_eq!(left.live, right.live);
    assert_eq!(left.request_json, right.request_json);
    assert_eq!(left.parsed_output_json, right.parsed_output_json);
    assert_eq!(left.jev_start_ts_ms, right.jev_start_ts_ms);
}

fn cache_key(episode: &TradeEpisode) -> JevCacheKey {
    JevCacheKey::new_versioned(
        format!("state:{}", episode.episode_id),
        "questions-v1".to_owned(),
        "jev-latest".to_owned(),
        Arm::QuantOnly.as_str().to_owned(),
        episode.strategy_version.clone(),
        "dryrun-prompt-v1".to_owned(),
        "dryrun-model-v1".to_owned(),
        "dryrun-schema-v1".to_owned(),
    )
}

fn cached_payload(episode: &TradeEpisode) -> CachedJev {
    let payload = serde_json::to_string(episode).expect("episode payload serializes");
    CachedJev {
        envelope_json: payload.clone(),
        latency_ms: episode.jev_latency_ms,
        live: false,
        request_json: format!("request:{payload}"),
        parsed_output_json: payload,
        jev_start_ts_ms: episode.jev_start_ts_ms,
    }
}

fn temp_cache_dir() -> PathBuf {
    std::env::temp_dir().join(format!("jevtrader-replay-dryrun-{}", std::process::id()))
}

fn metric_input<'a>(episode: &'a TradeEpisode) -> EpisodeMetricsInput<'a> {
    EpisodeMetricsInput {
        episode,
        regime: Some("REAL_TAPE"),
        markouts: [None; 5],
        fill_profile: episode.fill_profile.as_deref().unwrap_or("unknown"),
    }
}

#[test]
fn integrator_dryrun_covers_ten_points_without_network() {
    for path in [TRADE_FILE, RESOLUTION_FILE, SPEC_FILE, REGIME_FILE] {
        assert!(Path::new(path).is_file(), "missing allowed input {path}");
    }

    let trades_by_condition = parse_trades();
    let conditions = parse_conditions(trades_by_condition);
    assert!(
        !conditions.is_empty(),
        "real tape/resolution/spec join is empty"
    );
    let (condition, all_candidates) = conditions
        .iter()
        .find_map(|(_id, condition)| {
            let candidates = signal_candidates(condition);
            (candidates.len() >= 2).then_some((condition, candidates))
        })
        .unwrap_or_else(|| panic!("no real condition produced two fillable V1 actions"));
    let candidates: Vec<_> = all_candidates.into_iter().take(2).collect();
    assert_eq!(candidates.len(), 2);
    let first_run = build_dry_run(condition, &candidates);
    assert_eq!(first_run.len(), 2);
    eprintln!(
        "[dryrun] inputs trades=6866 joined_conditions={} selected_condition={} market={} fidelity={} signals={:?}",
        conditions.len(),
        condition.condition_id,
        condition.market_id,
        condition.fidelity,
        candidates
            .iter()
            .map(|candidate| (
                candidate.index,
                candidate.signal_ts_ms,
                candidate.bid,
                candidate.ask,
                candidate.action_price
            ))
            .collect::<Vec<_>>()
    );

    // 1. Strategy -> Action -> latency -> real tape fill -> ledger.
    for (candidate, episode) in candidates.iter().zip(&first_run) {
        assert!(candidate.action_price < candidate.ask);
        assert_eq!(episode.strategy_version, V1Strategy::VERSION);
        assert_eq!(episode.stake_usd, STAKE_USD);
        assert_eq!(episode.order_arrival_ts_ms, candidate.signal_ts_ms + 370);
        assert!(episode.fill_ts_ms.unwrap() >= episode.order_arrival_ts_ms);
        assert!(episode.exit_ts_ms.unwrap() >= episode.fill_ts_ms.unwrap());
        assert_eq!(episode.exit_type, ExitType::Resolution);
        assert!(episode.fill_qty.unwrap() > 0.0);
        assert_eq!(episode.entry_liquidity.as_deref(), Some("maker"));
        eprintln!(
            "[dryrun:1] condition={} signal_index={} action=PlaceMaker price={:.4} stake=${:.2} arrival={} fill_ts={:?} fill_qty={:?} ledger_exit={:?}",
            condition.condition_id,
            candidate.index,
            candidate.action_price,
            STAKE_USD,
            episode.order_arrival_ts_ms,
            episode.fill_ts_ms,
            episode.fill_qty,
            episode.exit_type
        );
    }

    // 2. First pass misses; persisted second instance hits with the same payload.
    let cache_dir = temp_cache_dir();
    let key = cache_key(&first_run[0]);
    let payload = cached_payload(&first_run[0]);
    let mut first_cache = jevtrader::replay::JevCache::new();
    assert!(first_cache.get(&key).is_none());
    first_cache.put(key.clone(), payload.clone());
    assert!(first_cache.misses > 0);
    assert_eq!(first_cache.save_to_dir(&cache_dir), Ok(1));
    let mut second_cache = jevtrader::replay::JevCache::new();
    assert_eq!(second_cache.load_from_dir(&cache_dir), Ok(1));
    let hit = second_cache
        .get(&key)
        .expect("persisted cache entry must hit");
    assert!(second_cache.hits > 0);
    assert_cached_equal(&payload, &hit);
    eprintln!(
        "[dryrun:2] cache key={} MISSes={} saved=1 HITs={} payload_bytes={}",
        key.state_hash,
        first_cache.misses,
        second_cache.hits,
        hit.envelope_json.len()
    );

    // 3. A/A construction is byte-identical at the ledger JSON boundary.
    let second_run = build_dry_run(condition, &candidates);
    let first_json = serde_json::to_vec(&first_run).expect("first ledger serializes");
    let second_json = serde_json::to_vec(&second_run).expect("second ledger serializes");
    assert_eq!(first_json, second_json);
    eprintln!(
        "[dryrun:3] A/A ledgers byte_identical=true bytes={} episodes={}",
        first_json.len(),
        first_run.len()
    );

    // 4. The JevEvaluator trait is intentionally not instantiated here: the
    // QUANT_ONLY Arm::policy gate is the requested non-network contract test.
    let mut evaluator_calls = 0_u64;
    let quant_policy = Arm::QuantOnly.policy();
    let quant_verdict = if quant_policy.calls_jev {
        evaluator_calls += 1;
        false
    } else {
        first_run.iter().all(|episode| episode.fill_qty.is_some())
    };
    assert!(quant_verdict);
    assert!(!quant_policy.calls_jev);
    assert_eq!(evaluator_calls, 0);
    eprintln!(
        "[dryrun:4] arm={} calls_jev={} quant_verdict={} evaluator_calls={} (policy gate; no trait/network constructed)",
        Arm::QuantOnly.as_str(),
        quant_policy.calls_jev,
        quant_verdict,
        evaluator_calls
    );

    // 5. Four arms share exactly the same fixed replay infrastructure.
    let arms = [
        Arm::QuantOnly,
        Arm::JevOnly,
        Arm::QuantPlusJev,
        Arm::MicroPlusRegime,
    ];
    let runs: Vec<ArmRun> = arms
        .into_iter()
        .map(|arm| {
            ArmRun::new(
                arm,
                FillProfile::Optimistic.as_str(),
                "zero",
                "resolution",
                "$5 fixed stake",
            )
        })
        .collect();
    let mut lineage_counts = BTreeMap::<String, usize>::new();
    for arm in arms {
        let mut arm_episodes = first_run.clone();
        for episode in &mut arm_episodes {
            episode.set_arm(arm.as_str());
        }
        for (base, episode) in first_run.iter().zip(&arm_episodes) {
            assert_eq!(episode.arm.as_deref(), Some(arm.as_str()));
            assert_eq!(episode.limit_price, base.limit_price);
            assert_eq!(episode.shares, base.shares);
            assert_eq!(episode.fill_ts_ms, base.fill_ts_ms);
            assert_eq!(episode.fill_qty, base.fill_qty);
            assert_eq!(episode.net_pnl_usd, base.net_pnl_usd);
        }
        *lineage_counts.entry(arm.as_str().to_owned()).or_default() += arm_episodes.len();
    }
    assert_eq!(
        lineage_counts.values().copied().collect::<Vec<_>>(),
        vec![2, 2, 2, 2]
    );
    assert!(runs.windows(2).all(|pair| {
        pair[0].fill_profile == pair[1].fill_profile
            && pair[0].fee_regime == pair[1].fee_regime
            && pair[0].exit_policy == pair[1].exit_policy
            && pair[0].capital_note == pair[1].capital_note
    }));
    eprintln!(
        "[dryrun:5] arm_lineage_counts={lineage_counts:?} shared_manifest=fill:{} fees:{} exit:{} capital:{}",
        runs[0].fill_profile, runs[0].fee_regime, runs[0].exit_policy, runs[0].capital_note
    );

    // 6. Real start/end bounds form one temporal window; purge removes labels
    // too near the boundary and cannot put any test condition into train.
    let spans: Vec<MarketSpan> = conditions
        .values()
        .filter_map(|condition| {
            Some(
                MarketSpan::new(
                    condition.condition_id.clone(),
                    condition.start_at_ms?,
                    condition.resolution_at_ms,
                )
                .expect("real spec interval must be valid"),
            )
        })
        .collect();
    assert!(spans.len() >= 3, "need at least three real spec intervals");
    let mut starts: Vec<i64> = spans.iter().map(|span| span.info_start_ms).collect();
    starts.sort_unstable();
    starts.dedup();
    let first_ms = starts[0];
    let boundary = starts[starts.len() / 2];
    let last_ms = spans
        .iter()
        .map(|span| span.resolution_ms)
        .max()
        .expect("real spec end bounds are present");
    assert!(first_ms < boundary && boundary < last_ms);
    let windows = plan_windows(
        first_ms,
        last_ms,
        boundary - first_ms,
        last_ms - boundary,
        last_ms - first_ms,
    );
    assert_eq!(windows.len(), 1);
    let window = &windows[0];
    let train_candidates: Vec<_> = spans
        .iter()
        .filter(|span| {
            window.assign(span.info_start_ms) == Some(jevtrader::replay::SplitAssign::InSample)
        })
        .cloned()
        .collect();
    let test_set: Vec<_> = spans
        .iter()
        .filter(|span| {
            window.assign(span.info_start_ms) == Some(jevtrader::replay::SplitAssign::OutOfSample)
        })
        .cloned()
        .collect();
    assert!(!train_candidates.is_empty() && !test_set.is_empty());
    let (train, purged) = purge_train(&train_candidates, window.train_end_ms, 300_000);
    let train_ids: BTreeSet<_> = train
        .iter()
        .map(|span| span.condition_id.as_str())
        .collect();
    let test_ids: BTreeSet<_> = test_set
        .iter()
        .map(|span| span.condition_id.as_str())
        .collect();
    assert!(train_ids.is_disjoint(&test_ids));
    eprintln!(
        "[dryrun:6] window={} train_candidates={} train_after_purge={} purged={} test={} leakage=false bounds=({}, {}, {})",
        window.name,
        train_candidates.len(),
        train.len(),
        purged.len(),
        test_set.len(),
        window.train_start_ms,
        window.train_end_ms,
        window.test_end_ms
    );

    // 7. Same seed gives identical draws and headline intervals.
    let blocks = first_run
        .iter()
        .map(|_| condition.condition_id.clone())
        .collect::<Vec<_>>();
    let bootstrap = BlockBootstrap::new(blocks.clone(), 0xD1CE_2026);
    let draws_a = bootstrap.resample(256);
    let draws_b = BlockBootstrap::new(blocks.clone(), 0xD1CE_2026).resample(256);
    assert_eq!(draws_a, draws_b);
    let oos_values: Vec<_> = first_run
        .iter()
        .map(|episode| (condition.condition_id.clone(), episode.net_pnl_usd))
        .collect();
    let headline_a = oos_report_with_draws(
        &oos_values,
        first_run.len() as u64,
        0,
        Some(
            first_run
                .iter()
                .map(|episode| episode.actual_notional_usd)
                .sum(),
        ),
        None,
        None,
        None,
        256,
        0xD1CE_2026,
    );
    let headline_b = oos_report_with_draws(
        &oos_values,
        first_run.len() as u64,
        0,
        Some(
            first_run
                .iter()
                .map(|episode| episode.actual_notional_usd)
                .sum(),
        ),
        None,
        None,
        None,
        256,
        0xD1CE_2026,
    );
    assert_eq!(headline_a.ci95_low, headline_b.ci95_low);
    assert_eq!(headline_a.ci95_high, headline_b.ci95_high);
    assert_eq!(headline_a.p_positive, headline_b.p_positive);
    eprintln!(
        "[dryrun:7] seed={} draws_identical=true CI95=[{:.9}, {:.9}] P(PnL>0)={:.6}",
        0xD1CE_2026_u64, headline_a.ci95_low, headline_a.ci95_high, headline_a.p_positive
    );

    // 8. Liquidity paths remain separate in both labels and aggregation.
    let mut maker_maker = first_run[0].clone();
    maker_maker
        .set_exit_liquidity("maker")
        .expect("maker exit is valid");
    let mut maker_taker = first_run[1].clone();
    maker_taker
        .set_exit_liquidity("taker")
        .expect("taker exit is valid");
    let path_maker_maker = path_of(&maker_maker);
    let path_maker_taker = path_of(&maker_taker);
    assert_ne!(path_maker_maker, path_maker_taker);
    let mut path_metrics = BTreeMap::<String, (usize, f64)>::new();
    for episode in [&maker_maker, &maker_taker] {
        let path = path_of(episode).as_str().to_owned();
        let entry = path_metrics.entry(path).or_default();
        entry.0 += 1;
        entry.1 += episode.net_pnl_usd;
    }
    assert_eq!(path_metrics.len(), 2);
    assert_eq!(path_metrics.values().map(|value| value.0).sum::<usize>(), 2);
    eprintln!("[dryrun:8] path_metrics={path_metrics:?} mixed=false");

    // 9. Resolution-time PROXY is excluded only by EXACT_ONLY.
    let exact_only = first_run
        .iter()
        .filter(|episode| passes(episode, ResolutionTimeFilter::ExactOnly))
        .count();
    let include_proxy = first_run
        .iter()
        .filter(|episode| passes(episode, ResolutionTimeFilter::IncludeProxy))
        .count();
    assert_eq!(exact_only, 0);
    assert_eq!(include_proxy, first_run.len());
    assert_eq!(
        first_run[0].resolution_time_provenance.as_deref(),
        Some("proxy")
    );
    eprintln!(
        "[dryrun:9] resolution_time_provenance=proxy ExactOnly_count={} IncludeProxy_count={}",
        exact_only, include_proxy
    );

    // 10. Ledger, portfolio, and metrics reconcile to 1e-6; the report split
    // is synthetic over the two real episodes and is explicitly not tuning.
    let mut portfolio = Portfolio::new();
    for episode in &first_run {
        portfolio.apply_episode(episode);
    }
    let metric_inputs: Vec<_> = first_run.iter().map(metric_input).collect();
    let metrics = summarize(&metric_inputs);
    let ledger_net: f64 = first_run.iter().map(|episode| episode.net_pnl_usd).sum();
    assert_close(ledger_net, portfolio.net_cash_pnl_usd());
    assert_close(ledger_net, metrics.historical_net_usd);
    let in_sample_inputs = vec![metric_input(&first_run[0])];
    let out_of_sample_inputs = vec![metric_input(&first_run[1])];
    let report_headline = oos_report_with_draws(
        &[(condition.condition_id.clone(), first_run[1].net_pnl_usd)],
        1,
        0,
        Some(first_run[1].actual_notional_usd),
        None,
        None,
        None,
        64,
        0xD1CE_2026,
    );
    let report = OosReport::from_metrics(&in_sample_inputs, &out_of_sample_inputs, report_headline);
    assert_eq!(report.in_sample.n_episodes, 1);
    assert_eq!(report.out_of_sample.n_episodes, 1);
    assert_ne!(
        in_sample_inputs[0].episode.episode_id,
        out_of_sample_inputs[0].episode.episode_id
    );
    eprintln!(
        "[dryrun:10] ledger_net={ledger_net:.9} portfolio_net={:.9} metrics_net={:.9} OOS={{in_sample_episodes:{}, out_of_sample_episodes:{}, net:{:.9}, CI95:[{:.9}, {:.9}], P(PnL>0):{:.6}}}",
        portfolio.net_cash_pnl_usd(),
        metrics.historical_net_usd,
        report.in_sample.n_episodes,
        report.out_of_sample.n_episodes,
        report.oos_headline.net_pnl_usd,
        report.oos_headline.ci95_low,
        report.oos_headline.ci95_high,
        report.oos_headline.p_positive
    );

    eprintln!(
        "[dryrun] LIMITS: fixed V1 signal fixture (no Jev answer columns in allowed inputs), tape-derived price/quantity/aggressor evidence, proxy resolution time, one-condition block bootstrap, no network, no underlying parquet"
    );
}
