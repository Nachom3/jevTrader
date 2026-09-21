//! Real-data replay checkpoint for the ledger/fill/hedge/fee path.
//!
//! This test intentionally reads only the four small processed inputs named in
//! the task. It does not touch any underlying parquet file: the UP/DOWN tape
//! and daily regime file are sufficient for this accounting checkpoint.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::File;

use arrow::array::{
    Array, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, LargeStringArray,
    StringArray, UInt32Array, UInt64Array,
};
use chrono::{DateTime, NaiveDateTime};
use jevtrader::replay::fills::{Aggressor, FillPrint};
use jevtrader::replay::ledger::ExitType;
use jevtrader::replay::{
    EpisodeMetricsInput, FillProfile, FillSimulator, MarketConstraints, Portfolio, RegimeFeatures,
    RollingRegimeClassifier, Side, TradeEpisode, apply_to_episode, current_crypto_regime,
    settle_hedge_pair, settle_resolution, size_entry, size_hedge, summarize, zero_regime,
};
use jevtrader::storage::StorageEvent;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

const PROCESSED: &str = "research-data/processed";
const TRADE_FILE: &str = "research-data/processed/polymarket_trades.parquet";
const RESOLUTION_FILE: &str = "research-data/processed/resolutions.parquet";
const SPEC_FILE: &str = "research-data/processed/resolution_specs.parquet";
const REGIME_FILE: &str = "research-data/processed/market_regimes.parquet";
const TICK: f64 = 0.01;
const STAKE_USD: f64 = 5.0;
const JEV_LATENCY_MS: u64 = 320;
const SUBMIT_LATENCY_MS: u64 = 50;
const EXIT_SUBMIT_LATENCY_MS: u64 = 50;
const EPSILON: f64 = 1e-6;
const SIGNAL_INDICES: [usize; 4] = [0, 1, 2, 3];
const NO_FILL_SCENARIOS: [(usize, usize); 2] = [(0, 4), (1, 4)];
const PARTIAL_SCENARIO: (usize, usize) = (0, 57);
const PARTIAL_LIMIT: f64 = 0.30;

#[derive(Debug, Clone)]
struct Row {
    cells: BTreeMap<String, String>,
}

#[derive(Debug, Clone)]
struct Trade {
    ts_s: i64,
    market_id: String,
    condition_id: String,
    yes_price: f64,
    amount: Option<f64>,
    usd_amount: Option<f64>,
    aggressor: Option<String>,
    direction_quality: String,
    outcome_label: Option<String>,
    original_token: Option<String>,
}

#[derive(Debug, Clone)]
struct Resolution {
    winning_outcome: String,
    status: String,
}

#[derive(Debug, Clone)]
struct Spec {
    fidelity: String,
    _start_at_ms: Option<i64>,
    end_at_ms: i64,
    market_type: String,
}

#[derive(Debug, Clone)]
struct RegimeObservation {
    day_ts_s: i64,
    symbol: String,
    vol_1m: f64,
    drift_pp: f64,
    vol_regime: String,
    trend_regime: String,
}

#[derive(Debug, Clone)]
struct ConditionEvidence {
    yes_is_up: bool,
    token_labels: BTreeMap<String, BTreeSet<String>>,
}

#[derive(Debug, Clone)]
struct EpisodeRecord {
    condition_id: String,
    signal_index: usize,
    profile: FillProfile,
    regime: String,
    episode: TradeEpisode,
}

fn assert_close(actual: f64, expected: f64) {
    assert!(
        (actual - expected).abs() <= EPSILON,
        "actual={actual:.12}, expected={expected:.12}, delta={:.12}",
        (actual - expected).abs()
    );
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

fn optional_f64(row: &Row, name: &str) -> Option<f64> {
    f64_value(row, name).filter(|value| value.is_finite() && *value > 0.0)
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

fn parse_trades() -> Vec<Trade> {
    let rows = read_rows(TRADE_FILE);
    assert_eq!(
        rows.len(),
        6866,
        "the checkpoint must use the known small tape"
    );
    rows.into_iter()
        .filter_map(|row| {
            let ts_s = i64_value(&row, "ts")?;
            let market_id = text(&row, "market_id")?.to_owned();
            let condition_id = text(&row, "condition_id")?.to_owned();
            let yes_price = f64_value(&row, "yes_price")?;
            if !yes_price.is_finite() || !(0.0..=1.0).contains(&yes_price) {
                return None;
            }
            Some(Trade {
                ts_s,
                market_id,
                condition_id,
                yes_price,
                amount: optional_f64(&row, "amount"),
                usd_amount: optional_f64(&row, "usd_amount"),
                aggressor: first_text(&row, &["aggressor"]),
                direction_quality: first_text(&row, &["direction_quality"])
                    .unwrap_or_else(|| "UNKNOWN".to_owned()),
                outcome_label: first_text(&row, &["outcome_label"]),
                original_token: first_text(&row, &["original_token"]),
            })
        })
        .collect()
}

fn parse_resolutions() -> HashMap<String, Resolution> {
    read_rows(RESOLUTION_FILE)
        .into_iter()
        .filter_map(|row| {
            let condition_id = text(&row, "condition_id")?.to_owned();
            let status = first_text(&row, &["resolution_status", "status"])?;
            let winning_outcome = first_text(&row, &["winning_outcome", "outcome"])?;
            Some((
                condition_id,
                Resolution {
                    winning_outcome,
                    status,
                },
            ))
        })
        .collect()
}

fn parse_specs() -> HashMap<String, Spec> {
    read_rows(SPEC_FILE)
        .into_iter()
        .filter_map(|row| {
            let condition_id = text(&row, "condition_id")?.to_owned();
            let end_at_ms = first_text(&row, &["end_at", "resolution_at"])
                .and_then(|value| parse_timestamp_ms(&value))?;
            Some((
                condition_id,
                Spec {
                    fidelity: first_text(&row, &["fidelity"])
                        .unwrap_or_else(|| "UNKNOWN".to_owned()),
                    _start_at_ms: first_text(&row, &["start_at"])
                        .and_then(|value| parse_timestamp_ms(&value)),
                    end_at_ms,
                    market_type: first_text(
                        &row,
                        &["market_type", "market_format", "outcome_type"],
                    )
                    .unwrap_or_else(|| "UPDOWN".to_owned()),
                },
            ))
        })
        .collect()
}

fn parse_regimes() -> Vec<RegimeObservation> {
    let mut rows: Vec<_> = read_rows(REGIME_FILE)
        .into_iter()
        .filter_map(|row| {
            Some(RegimeObservation {
                day_ts_s: i64_value(&row, "day_ts")?,
                symbol: text(&row, "symbol")?.to_owned(),
                vol_1m: f64_value(&row, "vol_1m")?,
                drift_pp: f64_value(&row, "drift_pp")?,
                vol_regime: text(&row, "vol_regime")?.to_owned(),
                trend_regime: text(&row, "trend_regime")?.to_owned(),
            })
        })
        .collect();
    rows.sort_by_key(|row| row.day_ts_s);
    rows
}

fn qty_for_trade(trade: &Trade) -> f64 {
    trade
        .amount
        .or_else(|| trade.usd_amount.map(|usd| usd / trade.yes_price))
        .filter(|qty| qty.is_finite() && *qty > 0.0)
        .unwrap_or(0.0)
}

fn aggressor(value: Option<&str>) -> Aggressor {
    match value.map(|value| value.to_ascii_uppercase()).as_deref() {
        Some("SELL") => Aggressor::Sell,
        Some("BUY") => Aggressor::Buy,
        _ => Aggressor::Unknown,
    }
}

fn trade_ts_ms(trade: &Trade) -> i64 {
    trade
        .ts_s
        .checked_mul(1_000)
        .expect("known epoch seconds must fit i64 milliseconds")
}

fn prior_ofi(trades: &[Trade], signal_ts_ms: i64) -> f64 {
    let (mut buys, mut sells) = (0_u64, 0_u64);
    for trade in trades {
        if trade_ts_ms(trade) >= signal_ts_ms {
            break;
        }
        match aggressor(trade.aggressor.as_deref()) {
            Aggressor::Buy => buys += 1,
            Aggressor::Sell => sells += 1,
            Aggressor::Unknown => {}
        }
    }
    let total = buys + sells;
    if total == 0 {
        0.0
    } else {
        (buys as f64 - sells as f64) / total as f64
    }
}

fn evidence_for_condition(trades: &[Trade]) -> Option<ConditionEvidence> {
    let mut token_labels: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for trade in trades {
        let Some(label) = trade.outcome_label.as_deref() else {
            continue;
        };
        let normalized = label.to_ascii_lowercase();
        if normalized != "up" && normalized != "down" {
            continue;
        }
        let token = trade
            .original_token
            .clone()
            .unwrap_or_else(|| normalized.clone());
        token_labels.entry(token).or_default().insert(normalized);
    }
    let labels: BTreeSet<_> = token_labels.values().flatten().cloned().collect();
    if labels != BTreeSet::from(["down".to_owned(), "up".to_owned()])
        || token_labels.values().any(|labels| labels.len() != 1)
    {
        return None;
    }

    // The corpus's `yes_price` is the normalized YES price. In UPDOWN
    // markets the token/outcome evidence is the audit trail for YES == Up;
    // a token mapping that changes label is ambiguous and is skipped above.
    Some(ConditionEvidence {
        yes_is_up: true,
        token_labels,
    })
}

fn regime_label_for(
    regimes: &[RegimeObservation],
    asset: &str,
    horizon: &str,
    signal_ts_ms: i64,
    minutes_to_resolution: u64,
    ofi: f64,
) -> String {
    // `market_regimes.day_ts` is a UTC day start. The backward-only join uses
    // the latest exact asset row whose day is not after the signal day; no
    // observation from a later day can enter this classifier history.
    let signal_day_ts = signal_ts_ms
        .div_euclid(1_000)
        .div_euclid(86_400)
        .saturating_mul(86_400);
    let symbol = match asset {
        "BTC" => "BTCUSDT",
        "ETH" => "ETHUSDT",
        other => panic!("unsupported regime asset {other}"),
    };
    // The source strings are retained as audit evidence below; the canonical
    // RegimeLabel mapping is the classifier over the same row's vol_1m and
    // drift_pp after all prior rows for this asset have been observed.
    let mut classifier = RollingRegimeClassifier::new(20);
    let mut latest = None;
    let mut matched_rows = 0_usize;
    for observation in regimes {
        if observation.symbol != symbol || observation.day_ts_s > signal_day_ts {
            continue;
        }
        assert!(matches!(
            observation.vol_regime.as_str(),
            "LOW_VOL" | "NORMAL_VOL" | "HIGH_VOL" | "EXTREME_VOL"
        ));
        assert!(matches!(
            observation.trend_regime.as_str(),
            "STRONG_UP" | "SIDEWAYS" | "STRONG_DOWN"
        ));
        classifier.observe(
            asset.to_owned(),
            horizon.to_owned(),
            observation.vol_1m,
            observation.drift_pp,
        );
        matched_rows += 1;
        latest = Some(observation);
    }
    let Some(observation) = latest else {
        return "UNKNOWN".to_owned();
    };
    let features = RegimeFeatures {
        trend_score: observation.drift_pp,
        volatility: observation.vol_1m,
        minutes_to_resolution,
        distance_sigma: 0.0,
        ofi,
        basis: 0.0,
        asset: asset.to_owned(),
        horizon: horizon.to_owned(),
    };
    let label = classifier.classify(&features);
    let rendered = format!(
        "{:?}-{:?}-{:?}-{:?}-{:?}-{:?}",
        label.volatility, label.trend, label.time, label.sigma, label.flow, label.basis
    );
    eprintln!(
        "[checkpoint] regime_join asset={asset} symbol={symbol} signal_day_ts={signal_day_ts} matched_rows={matched_rows} source_vol={} source_trend={} classified={rendered}",
        observation.vol_regime, observation.trend_regime
    );
    rendered
}

fn apply_fees(episode: &mut TradeEpisode) {
    let historical = zero_regime();
    let current = current_crypto_regime();
    let fill_notional = episode
        .fill_price
        .zip(episode.fill_qty)
        .map_or(0.0, |(price, qty)| price * qty);
    let exit_notional = episode
        .exit_price
        .zip(episode.fill_qty)
        .map_or(0.0, |(price, qty)| price * qty);
    // Principal ledger is historical zero-fee. The current comparison is an
    // explicit taker stress at 0.07 even though the replayed order is maker.
    apply_to_episode(
        episode,
        episode.gross_pnl_usd,
        fill_notional,
        exit_notional,
        false,
        &historical,
        &current,
    )
    .expect("fee views must reconcile");
    episode.set_is_maker(true);
    episode.set_fee_regime("historical_zero/current_taker_0.07");
}

fn apply_no_fill_fees(episode: &mut TradeEpisode) {
    let historical = zero_regime();
    let current = current_crypto_regime();
    apply_to_episode(episode, 0.0, 0.0, 0.0, false, &historical, &current)
        .expect("no-fill fee views must reconcile");
    episode.set_is_maker(true);
    episode.set_fee_regime("historical_zero/current_taker_0.07");
}

fn make_entry_at_limit(
    trade: &Trade,
    condition_id: &str,
    signal_index: usize,
    market: &str,
    asset: &str,
    horizon: &str,
    limit_price: f64,
) -> TradeEpisode {
    let signal_ts_ms = trade_ts_ms(trade);
    let sized = size_entry(
        limit_price,
        STAKE_USD,
        &MarketConstraints::new(TICK, TICK, 1.0).expect("fixed venue constraints"),
    )
    .unwrap_or_else(|error| {
        panic!("signal {condition_id}[{signal_index}] cannot be sized: {error}")
    });
    let mut episode = TradeEpisode::new_with_exit_submit_latency(
        format!("checkpoint:{condition_id}:{signal_index}:entry"),
        "checkpoint-ledger-v1",
        market,
        asset,
        horizon,
        signal_ts_ms,
        signal_ts_ms,
        JEV_LATENCY_MS,
        SUBMIT_LATENCY_MS,
        EXIT_SUBMIT_LATENCY_MS,
        Side::BuyYes,
        sized.limit_price,
        STAKE_USD,
    )
    .expect("fixed entry must be valid");
    episode
        .apply_sizing(&sized)
        .expect("entry sizing must apply");
    episode.set_fill_profile("pending");
    episode.set_prompt_version("checkpoint-no-jev");
    episode.set_jev_model("deterministic-real-tape");
    episode
}

fn apply_risk_exit(episode: &mut TradeEpisode, trades: &[Trade], no_side: bool) -> bool {
    let Some(fill_ts_ms) = episode.fill_ts_ms else {
        return false;
    };
    let signal_ts_ms = fill_ts_ms;
    if episode
        .request_exit(signal_ts_ms, EXIT_SUBMIT_LATENCY_MS)
        .is_err()
    {
        return false;
    }
    let Some(exit_arrival_ts_ms) = episode.exit_arrival_ts_ms else {
        return false;
    };
    let Some(trade) = trades
        .iter()
        .find(|trade| trade_ts_ms(trade) >= exit_arrival_ts_ms)
    else {
        return false;
    };
    let price = if no_side {
        1.0 - trade.yes_price
    } else {
        trade.yes_price
    };
    let exit_ts_ms = trade_ts_ms(trade);
    episode
        .apply_exit_fill(exit_ts_ms, price)
        .and_then(|_| episode.apply_exit(ExitType::Stop, exit_ts_ms, price))
        .is_ok()
}

struct ProfileSimulation<'a> {
    signal: &'a Trade,
    signal_index: usize,
    condition_id: &'a str,
    trades: &'a [Trade],
    resolution: &'a Resolution,
    spec: &'a Spec,
    evidence: &'a ConditionEvidence,
    signal_ordinal: usize,
    market: &'a str,
    asset: &'a str,
    horizon: &'a str,
    regimes: &'a [RegimeObservation],
}

fn settle_filled_entry(
    entry: &mut TradeEpisode,
    resolution: &Resolution,
    evidence: &ConditionEvidence,
    resolution_ts_ms: i64,
) {
    let yes_won = resolution.winning_outcome.eq_ignore_ascii_case("up");
    assert!(evidence.yes_is_up);
    settle_resolution(entry, yes_won, resolution_ts_ms, 1.0)
        .expect("proxy end_at must settle the real fill");
    entry
        .set_resolution_outcome(if yes_won { "yes" } else { "no" })
        .expect("resolution outcome follows proxy timestamp");
    entry
        .set_resolution_provenance("proxy")
        .expect("resolution provenance follows proxy timestamp");
    apply_fees(entry);
}

fn simulate_hedge(
    simulation: &ProfileSimulation<'_>,
    profile: FillProfile,
    mut entry: TradeEpisode,
    fill_ts_ms: i64,
    regime: String,
) -> (TradeEpisode, Option<TradeEpisode>, String) {
    // The atomic pair is settled at the later real leg fill, so both exit
    // requests must already be executable at that event-time timestamp.
    entry.exit_submit_latency_ms = 0;
    // The hedge quote is the real-tape NO complement of 1-limit, rounded
    // to the same venue tick. The first hedge signal is a profit target;
    // the second intentionally exercises entry+opposite > 1 as a loss.
    let hedge_adjustment = if simulation.signal_ordinal == 2 {
        -TICK
    } else {
        TICK
    };
    let opposite_raw = 1.0 - entry.limit_price + hedge_adjustment;
    let constraints = MarketConstraints::new(TICK, TICK, 1.0).expect("fixed constraints");
    let hedge_sized = size_hedge(entry.fill_qty.unwrap(), opposite_raw, &constraints)
        .unwrap_or_else(|error| {
            panic!(
                "hedge {}[{}] cannot be sized: {error}",
                simulation.condition_id, simulation.signal_index
            )
        });
    let hedge_signal_ts_ms = fill_ts_ms
        .checked_add(1)
        .expect("hedge signal timestamp must fit i64");
    let mut hedge = TradeEpisode::new_with_exit_submit_latency(
        format!(
            "checkpoint:{}:{}:hedge",
            simulation.condition_id, simulation.signal_index
        ),
        "checkpoint-ledger-v1",
        simulation.market,
        simulation.asset,
        simulation.horizon,
        hedge_signal_ts_ms,
        hedge_signal_ts_ms,
        JEV_LATENCY_MS,
        SUBMIT_LATENCY_MS,
        // Pair settlement is attributed at the real hedge fill event. The
        // production settle_hedge_pair precondition therefore requires both
        // legs to have zero exit-submit latency for this atomic checkpoint.
        0,
        Side::BuyNo,
        hedge_sized.limit_price,
        hedge_sized.intended_stake_usd,
    )
    .expect("fixed hedge must be valid");
    hedge
        .apply_sizing(&hedge_sized)
        .expect("hedge sizing must apply");
    hedge.set_fill_profile(profile.as_str());
    hedge.set_is_maker(true);
    hedge.set_prompt_version("checkpoint-no-jev");
    hedge.set_jev_model("deterministic-real-tape");

    let hedge_order = jevtrader::replay::RestingOrder {
        price: hedge.limit_price,
        size: hedge.shares,
        resting_from_ms: hedge.order_arrival_ts_ms,
        side_buy: true,
    };
    let hedge_prints: Vec<_> = simulation
        .trades
        .iter()
        .filter(|trade| trade_ts_ms(trade) >= hedge.order_arrival_ts_ms)
        .map(|trade| {
            FillPrint::new(
                trade_ts_ms(trade),
                1.0 - trade.yes_price,
                qty_for_trade(trade),
                aggressor(trade.aggressor.as_deref()),
            )
        })
        .collect();
    let hedge_outcome = FillSimulator::new(profile).check_fill(
        &hedge_order,
        &hedge_prints,
        jevtrader::replay::ExecutionLatency::new(0),
    );
    if let Some(hedge_fill_ts_ms) = hedge_outcome.fill_ts_ms.filter(|_| hedge_outcome.filled) {
        hedge
            .apply_fill(
                hedge_fill_ts_ms,
                hedge_outcome.fill_price,
                hedge_outcome.fill_fraction * hedge.shares,
            )
            .expect("real NO complement fill must be after arrival");
    }

    settle_hedge_pair(&mut entry, &mut hedge);
    if entry.exit_type != ExitType::Hedge || hedge.exit_type != ExitType::Hedge {
        eprintln!(
            "[checkpoint] hedge_pair AUSENTE condition={} signal_index={} entry_side={:?} hedge_side={:?} entry_qty={:?} hedge_qty={:?} entry_fill_ts={:?} hedge_fill_ts={:?} entry_exit_latency={} hedge_exit_latency={} reason=pair preconditions were not all satisfied",
            simulation.condition_id,
            simulation.signal_index,
            entry.side,
            hedge.side,
            entry.fill_qty,
            hedge.fill_qty,
            entry.fill_ts_ms,
            hedge.fill_ts_ms,
            entry.exit_submit_latency_ms,
            hedge.exit_submit_latency_ms
        );
        // A non-matching partial/queue fill is not promoted to an instant
        // pair. Real later tape prints provide a risk exit instead.
        if !entry.is_no_fill() && entry.exit_type == ExitType::NoFill {
            assert!(apply_risk_exit(&mut entry, simulation.trades, false));
        }
        if !hedge.is_no_fill() && hedge.exit_type == ExitType::NoFill {
            assert!(apply_risk_exit(&mut hedge, simulation.trades, true));
        }
    }
    apply_fees(&mut entry);
    if hedge.is_no_fill() {
        apply_no_fill_fees(&mut hedge);
    } else {
        apply_fees(&mut hedge);
    }
    (entry, Some(hedge), regime)
}

fn simulate_profile(
    simulation: &ProfileSimulation<'_>,
    profile: FillProfile,
) -> (TradeEpisode, Option<TradeEpisode>, String) {
    simulate_profile_at_limit(
        simulation,
        profile,
        simulation.signal.yes_price - TICK,
        simulation.signal_ordinal >= 2,
        false,
    )
}

fn simulate_profile_at_limit(
    simulation: &ProfileSimulation<'_>,
    profile: FillProfile,
    raw_limit_price: f64,
    hedge_when_filled: bool,
    require_no_fill: bool,
) -> (TradeEpisode, Option<TradeEpisode>, String) {
    assert_eq!(simulation.spec.fidelity, "EXACT");
    assert_eq!(simulation.spec.market_type, "UPDOWN");
    assert_eq!(simulation.resolution.status, "resolved");
    assert!(simulation.evidence.yes_is_up);

    let resolution_ts_ms = simulation.spec.end_at_ms;
    let minutes_to_resolution = resolution_ts_ms
        .saturating_sub(trade_ts_ms(simulation.signal))
        .unsigned_abs()
        .saturating_div(60_000);
    let regime = regime_label_for(
        simulation.regimes,
        simulation.asset,
        simulation.horizon,
        trade_ts_ms(simulation.signal),
        minutes_to_resolution,
        prior_ofi(simulation.trades, trade_ts_ms(simulation.signal)),
    );
    let mut entry = make_entry_at_limit(
        simulation.signal,
        simulation.condition_id,
        simulation.signal_index,
        simulation.market,
        simulation.asset,
        simulation.horizon,
        raw_limit_price,
    );
    if hedge_when_filled {
        entry.exit_submit_latency_ms = 0;
    }
    entry.set_fill_profile(profile.as_str());

    let order = jevtrader::replay::RestingOrder {
        price: entry.limit_price,
        size: entry.shares,
        resting_from_ms: entry.order_arrival_ts_ms,
        side_buy: true,
    };
    let prints: Vec<_> = simulation
        .trades
        .iter()
        .filter(|trade| trade_ts_ms(trade) >= entry.order_arrival_ts_ms)
        .map(|trade| {
            FillPrint::new(
                trade_ts_ms(trade),
                trade.yes_price,
                qty_for_trade(trade),
                aggressor(trade.aggressor.as_deref()),
            )
        })
        .collect();
    let outcome = FillSimulator::new(profile).check_fill(
        &order,
        &prints,
        jevtrader::replay::ExecutionLatency::new(0),
    );
    assert_eq!(outcome.profile, profile);
    assert!(outcome.fill_fraction.is_finite());
    assert!((0.0..=1.0).contains(&outcome.fill_fraction));

    if require_no_fill {
        assert!(
            !outcome.filled,
            "forced real-tape NO_FILL unexpectedly filled"
        );
        assert_eq!(outcome.fill_fraction, 0.0);
        entry
            .apply_exit(
                ExitType::NoFill,
                entry.order_arrival_ts_ms,
                entry.limit_price,
            )
            .expect("forced no-fill terminal state");
        apply_no_fill_fees(&mut entry);
        assert!(entry.is_no_fill());
        assert_eq!(entry.gross_pnl_usd, 0.0);
        assert_eq!(entry.fees_usd, 0.0);
        assert_eq!(entry.net_pnl_usd, 0.0);
        return (entry, None, regime);
    }

    if let Some(fill_ts_ms) = outcome.fill_ts_ms.filter(|_| outcome.filled) {
        let fill_qty = outcome.fill_fraction * entry.shares;
        entry
            .apply_fill(fill_ts_ms, outcome.fill_price, fill_qty)
            .expect("real tape fill must be after arrival");
        entry.set_is_maker(true);

        if hedge_when_filled {
            return simulate_hedge(simulation, profile, entry, fill_ts_ms, regime);
        }
        settle_filled_entry(
            &mut entry,
            simulation.resolution,
            simulation.evidence,
            resolution_ts_ms,
        );
        return (entry, None, regime);
    }

    entry
        .apply_exit(
            ExitType::NoFill,
            entry.order_arrival_ts_ms,
            entry.limit_price,
        )
        .expect("no-fill terminal state");
    apply_no_fill_fees(&mut entry);
    (entry, None, regime)
}

fn profile_order() -> [FillProfile; 3] {
    [
        FillProfile::Conservative,
        FillProfile::Base,
        FillProfile::Optimistic,
    ]
}

fn profile_name(profile: FillProfile) -> &'static str {
    profile.as_str()
}

fn build_checkpoint() -> (Vec<EpisodeRecord>, Vec<String>) {
    let mut by_condition: BTreeMap<String, Vec<Trade>> = BTreeMap::new();
    for trade in parse_trades() {
        by_condition
            .entry(trade.condition_id.clone())
            .or_default()
            .push(trade);
    }
    for trades in by_condition.values_mut() {
        trades.sort_by_key(|trade| trade.ts_s);
    }

    let resolutions = parse_resolutions();
    let specs = parse_specs();
    let regimes = parse_regimes();
    assert_eq!(resolutions.len(), 72, "known resolution row count");
    assert_eq!(regimes.len(), 614, "known regime row count");

    let tape_conditions: BTreeSet<String> = by_condition.keys().cloned().collect();
    let resolved_conditions: BTreeSet<String> = resolutions
        .iter()
        .filter(|(_, resolution)| resolution.status == "resolved")
        .map(|(condition_id, _)| condition_id.clone())
        .collect();
    let spec_conditions: BTreeSet<String> = specs.keys().cloned().collect();
    let joined_conditions: BTreeSet<String> = tape_conditions
        .intersection(&resolved_conditions)
        .filter(|condition_id| spec_conditions.contains(*condition_id))
        .cloned()
        .collect();
    assert!(
        !joined_conditions.is_empty(),
        "tape/resolved/spec condition intersection must not be empty"
    );
    for condition_id in &joined_conditions {
        assert!(
            specs.contains_key(condition_id),
            "joined condition {condition_id} must have a resolution spec"
        );
    }

    let specs_without_tape = spec_conditions.difference(&tape_conditions).count();
    let resolved_without_spec: Vec<String> = resolved_conditions
        .difference(&spec_conditions)
        .cloned()
        .collect();
    eprintln!(
        "[checkpoint] join tape_conditions={} resolved_conditions={} spec_conditions={} selected_intersection={} specs_without_tape={specs_without_tape} resolved_without_spec={}",
        tape_conditions.len(),
        resolved_conditions.len(),
        spec_conditions.len(),
        joined_conditions.len(),
        resolved_without_spec.len()
    );
    if !resolved_without_spec.is_empty() {
        eprintln!(
            "[checkpoint] SKIP resolved conditions without spec: {:?}",
            resolved_without_spec
        );
    }

    let mut candidates: Vec<(usize, String)> = by_condition
        .iter()
        .filter_map(|(condition_id, trades)| {
            if !joined_conditions.contains(condition_id) || trades.len() < 4 {
                return None;
            }
            let evidence = evidence_for_condition(trades)?;
            let resolution = resolutions
                .get(condition_id)
                .expect("joined resolution exists");
            if !resolution.winning_outcome.eq_ignore_ascii_case("up")
                && !resolution.winning_outcome.eq_ignore_ascii_case("down")
            {
                return None;
            }
            assert!(evidence.yes_is_up);
            Some((trades.len(), condition_id.clone()))
        })
        .collect();
    candidates.sort_by(|left, right| right.cmp(left));
    assert!(
        candidates.len() >= 4,
        "need four unambiguous real conditions"
    );
    let selected: Vec<_> = candidates.into_iter().take(4).collect();

    let selected_ids: Vec<String> = selected.iter().map(|(_, id)| id.clone()).collect();
    eprintln!(
        "[checkpoint] inputs trades={} resolutions={} specs={} regimes={} selected_conditions={:?}",
        6866,
        resolutions.len(),
        specs.len(),
        regimes.len(),
        selected_ids
    );
    eprintln!(
        "[checkpoint] resolution timestamp provenance: resolutions.resolved_ts is NaT; end_at from resolution_specs is used as PROXY, while outcome is EXACT from resolved winning_outcome"
    );
    eprintln!(
        "[checkpoint] qty conviction: amount -> usd_amount/yes_price -> 0.0; zero-volume prints remain tape evidence but never manufacture a fill"
    );

    let mut records = Vec::new();
    for (condition_rank, (_, condition_id)) in selected.iter().enumerate() {
        let trades = by_condition
            .remove(condition_id)
            .expect("selected condition must own its tape before processing");
        let resolution = resolutions.get(condition_id).expect("resolution exists");
        let spec = specs.get(condition_id).expect("spec exists");
        let evidence = evidence_for_condition(&trades).expect("selected condition is unambiguous");
        let market = trades[0].market_id.clone();
        let asset = market
            .split(['-', '_'])
            .next()
            .filter(|asset| !asset.is_empty())
            .map(str::to_ascii_uppercase)
            .unwrap_or_else(|| "UNKNOWN".to_owned());
        let horizon = if market.contains("15m") {
            "15m"
        } else if market.contains("1h") {
            "1h"
        } else if market.contains("4h") {
            "4h"
        } else {
            "5m"
        };

        let signal_index = SIGNAL_INDICES[condition_rank];
        let signal = &trades[signal_index];
        eprintln!(
            "[checkpoint] condition={} tape_rows={} market={} fixed_signal_index={} signal_ts_s={} price={:.6} direction_quality={} outcome_label={:?} original_token={:?} token_map={:?}",
            condition_id,
            trades.len(),
            market,
            signal_index,
            signal.ts_s,
            signal.yes_price,
            signal.direction_quality,
            signal.outcome_label,
            signal.original_token,
            evidence.token_labels
        );

        let simulation = ProfileSimulation {
            signal,
            signal_index,
            condition_id,
            trades: &trades,
            resolution,
            spec,
            evidence: &evidence,
            signal_ordinal: condition_rank,
            market: &market,
            asset: &asset,
            horizon,
            regimes: &regimes,
        };
        for profile in profile_order() {
            let (entry, hedge, regime) = simulate_profile(&simulation, profile);
            records.push(EpisodeRecord {
                condition_id: condition_id.clone(),
                signal_index,
                profile,
                regime: regime.clone(),
                episode: entry,
            });
            if let Some(hedge) = hedge {
                records.push(EpisodeRecord {
                    condition_id: condition_id.clone(),
                    signal_index,
                    profile,
                    regime,
                    episode: hedge,
                });
            }
        }

        for &(extra_rank, no_fill_signal_index) in &NO_FILL_SCENARIOS {
            if extra_rank != condition_rank {
                continue;
            }
            let no_fill_signal = &trades[no_fill_signal_index];
            eprintln!(
                "[checkpoint] extra NO_FILL condition={} signal_index={} signal_ts_s={} tape_price={:.6} limit=0.0500",
                condition_id, no_fill_signal_index, no_fill_signal.ts_s, no_fill_signal.yes_price
            );
            let no_fill_simulation = ProfileSimulation {
                signal: no_fill_signal,
                signal_index: no_fill_signal_index,
                condition_id,
                trades: &trades,
                resolution,
                spec,
                evidence: &evidence,
                signal_ordinal: condition_rank,
                market: &market,
                asset: &asset,
                horizon,
                regimes: &regimes,
            };
            for profile in profile_order() {
                let (entry, hedge, regime) =
                    simulate_profile_at_limit(&no_fill_simulation, profile, 0.05, false, true);
                assert!(hedge.is_none());
                records.push(EpisodeRecord {
                    condition_id: condition_id.clone(),
                    signal_index: no_fill_signal_index,
                    profile,
                    regime,
                    episode: entry,
                });
            }
        }

        if condition_rank == PARTIAL_SCENARIO.0 {
            let partial_signal_index = PARTIAL_SCENARIO.1;
            let partial_signal = &trades[partial_signal_index];
            eprintln!(
                "[checkpoint] extra partial candidate condition={} signal_index={} signal_ts_s={} tape_price={:.6} limit={PARTIAL_LIMIT:.4}",
                condition_id, partial_signal_index, partial_signal.ts_s, partial_signal.yes_price
            );
            let partial_simulation = ProfileSimulation {
                signal: partial_signal,
                signal_index: partial_signal_index,
                condition_id,
                trades: &trades,
                resolution,
                spec,
                evidence: &evidence,
                signal_ordinal: condition_rank,
                market: &market,
                asset: &asset,
                horizon,
                regimes: &regimes,
            };
            for profile in profile_order() {
                let (entry, hedge, regime) = simulate_profile_at_limit(
                    &partial_simulation,
                    profile,
                    PARTIAL_LIMIT,
                    false,
                    false,
                );
                assert!(hedge.is_none());
                records.push(EpisodeRecord {
                    condition_id: condition_id.clone(),
                    signal_index: partial_signal_index,
                    profile,
                    regime,
                    episode: entry,
                });
            }
        }
        drop(trades);
    }
    assert_eq!(
        records.len(),
        27,
        "checkpoint rows must include four baseline signals, two NO_FILL signals, and one partial signal"
    );
    (records, selected_ids)
}

fn is_filled(episode: &TradeEpisode) -> bool {
    !episode.is_no_fill()
}

fn checkpoint_test() {
    let (records, selected_ids) = build_checkpoint();
    assert_eq!(records.len(), 27);
    assert_eq!(selected_ids.len(), 4);

    let mut converted_rows = 0_usize;
    for record in &records {
        // TradeEpisodeRow lives behind the storage module's public event
        // boundary. Constructing this event exercises its From<&TradeEpisode>
        // conversion without widening the production module's API for a test.
        let event = StorageEvent::TradeEpisode {
            row: (&record.episode).into(),
        };
        assert!(matches!(event, StorageEvent::TradeEpisode { .. }));
        converted_rows += 1;
        assert_eq!(
            record.episode.gross_pnl_usd - record.episode.fees_usd + record.episode.rebates_usd,
            record.episode.net_pnl_usd,
            "ledger accounting must reconcile for {}",
            record.episode.episode_id
        );
        if let (Some(historical), Some(current)) = (
            record.episode.pnl_historical_usd,
            record.episode.pnl_current_usd,
        ) {
            assert_eq!(historical, record.episode.net_pnl_usd);
            assert!(current.is_finite());
        }
    }
    assert_eq!(converted_rows, records.len());

    let regime_labels: BTreeSet<String> =
        records.iter().map(|record| record.regime.clone()).collect();
    let unknown_regimes = regime_labels
        .iter()
        .filter(|regime| regime.as_str() == "UNKNOWN")
        .count();
    assert_eq!(
        unknown_regimes, 0,
        "all selected episodes need a real regime"
    );
    assert!(
        regime_labels.len() >= 2,
        "regime breakdown needs at least two labels, got {regime_labels:?}"
    );
    eprintln!(
        "[checkpoint] regimes covered distinct={} unknown={} labels={regime_labels:?}",
        regime_labels.len(),
        unknown_regimes
    );

    let explicit_no_fill_records: Vec<_> = records
        .iter()
        .filter(|record| {
            record.episode.exit_type == ExitType::NoFill
                && (record.episode.limit_price - 0.05).abs() <= EPSILON
        })
        .collect();
    assert_eq!(
        explicit_no_fill_records.len(),
        NO_FILL_SCENARIOS.len() * profile_order().len(),
        "both real far-away signals must be NO_FILL under every profile"
    );
    for record in &explicit_no_fill_records {
        assert!(record.episode.is_no_fill());
        assert!(record.episode.fill_ts_ms.is_none());
        assert!(record.episode.fill_qty.is_none());
        assert_eq!(record.episode.gross_pnl_usd, 0.0);
        assert_eq!(record.episode.net_pnl_usd, 0.0);
    }

    let mut portfolios: BTreeMap<String, Portfolio> = BTreeMap::new();
    for profile in profile_order() {
        let name = profile_name(profile).to_owned();
        let mut portfolio = Portfolio::new();
        for record in records.iter().filter(|record| record.profile == profile) {
            portfolio.apply_episode(&record.episode);
        }
        portfolios.insert(name, portfolio);
    }

    for profile in profile_order() {
        let name = profile_name(profile).to_owned();
        let with_explicit_no_fills = portfolios.get(&name).expect("profile portfolio exists");
        let mut without_explicit_no_fills = Portfolio::new();
        for record in records
            .iter()
            .filter(|record| record.profile == profile)
            .filter(|record| {
                !(record.episode.exit_type == ExitType::NoFill
                    && (record.episode.limit_price - 0.05).abs() <= EPSILON)
            })
        {
            without_explicit_no_fills.apply_episode(&record.episode);
        }
        assert_eq!(
            with_explicit_no_fills.fill_count(),
            without_explicit_no_fills.fill_count(),
            "NO_FILL must not add fills to {name}"
        );
        assert_close(
            with_explicit_no_fills.net_cash_pnl_usd(),
            without_explicit_no_fills.net_cash_pnl_usd(),
        );
        assert_close(
            with_explicit_no_fills.gross_cash_pnl_usd(),
            without_explicit_no_fills.gross_cash_pnl_usd(),
        );
        assert_close(
            with_explicit_no_fills.inventory(),
            without_explicit_no_fills.inventory(),
        );
        assert_close(
            with_explicit_no_fills.turnover(),
            without_explicit_no_fills.turnover(),
        );
    }

    let ledger_net: f64 = records
        .iter()
        .map(|record| record.episode.net_pnl_usd)
        .sum();
    let portfolio_net: f64 = portfolios
        .values()
        .map(Portfolio::net_cash_pnl_usd)
        .sum::<f64>()
        / profile_order().len() as f64;
    let metric_inputs: Vec<_> = records
        .iter()
        .map(|record| EpisodeMetricsInput {
            episode: &record.episode,
            regime: Some(record.regime.as_str()),
            markouts: [None; 5],
            fill_profile: record.episode.fill_profile.as_deref().unwrap_or("unknown"),
        })
        .collect();
    let metrics_historical_net =
        summarize(&metric_inputs).historical_net_usd / profile_order().len() as f64;
    assert_close(ledger_net / profile_order().len() as f64, portfolio_net);
    assert_close(
        ledger_net / profile_order().len() as f64,
        metrics_historical_net,
    );

    let mut breakdown: BTreeMap<(String, String), (usize, usize, f64)> = BTreeMap::new();
    for record in &records {
        let key = (
            record.regime.clone(),
            profile_name(record.profile).to_owned(),
        );
        let value = breakdown.entry(key).or_default();
        value.0 += 1;
        value.1 += usize::from(is_filled(&record.episode));
        value.2 += record.episode.net_pnl_usd;
    }
    eprintln!(
        "[checkpoint] reconciliation ledger_net_all_profiles={ledger_net:.9} portfolio_net_all_profiles={:.9} metrics_historical_net_all_profiles={:.9}",
        portfolio_net * profile_order().len() as f64,
        metrics_historical_net * profile_order().len() as f64
    );
    for ((regime, profile), (episodes, fills, pnl)) in &breakdown {
        eprintln!(
            "[checkpoint] breakdown regime={regime} profile={profile} episodes={episodes} fills={fills} historical_net={pnl:.9}"
        );
    }

    let mut profile_stats = BTreeMap::new();
    for profile in profile_order() {
        let name = profile_name(profile).to_owned();
        let profile_records: Vec<_> = records
            .iter()
            .filter(|record| record.profile == profile)
            .collect();
        let fills = profile_records
            .iter()
            .filter(|record| is_filled(&record.episode))
            .count();
        let nofills = profile_records.len() - fills;
        let pnl: f64 = profile_records
            .iter()
            .map(|record| record.episode.net_pnl_usd)
            .sum();
        profile_stats.insert(name.clone(), (fills, nofills, pnl));
        eprintln!(
            "[checkpoint] profile={name} episodes={} fills={fills} nofills={nofills} historical_net={pnl:.9} current_net={:.9}",
            profile_records.len(),
            profile_records
                .iter()
                .map(|record| record.episode.pnl_current_usd.unwrap_or(0.0))
                .sum::<f64>()
        );
    }

    let scenario_ids: BTreeSet<(String, String)> = records
        .iter()
        .map(|record| {
            (
                record.condition_id.clone(),
                record.episode.episode_id.clone(),
            )
        })
        .collect();
    for (condition_id, episode_id) in scenario_ids {
        let fills: Vec<_> = profile_order()
            .into_iter()
            .map(|profile| {
                records
                    .iter()
                    .find(|record| {
                        record.condition_id == condition_id
                            && record.episode.episode_id == episode_id
                            && record.profile == profile
                    })
                    .expect("each scenario must run under all profiles")
                    .episode
                    .fill_qty
                    .unwrap_or(0.0)
            })
            .collect();
        assert!(fills[0] <= fills[1] + EPSILON);
        assert!(fills[1] <= fills[2] + EPSILON);
    }

    let hedge_records: Vec<_> = records
        .iter()
        .filter(|record| record.episode.exit_type == ExitType::Hedge)
        .collect();
    assert_eq!(
        hedge_records.len(),
        12,
        "both real entry+hedge legs must settle through settle_hedge_pair"
    );
    for record in &hedge_records {
        assert!(record.episode.hedge_pair_id.is_some());
        if matches!(record.episode.side, Side::BuyNo) {
            assert_eq!(record.episode.gross_pnl_usd, 0.0);
        } else {
            assert!(record.episode.gross_pnl_usd.abs() > EPSILON);
        }
    }

    let mut wins = 0;
    let mut losses = 0;
    let mut hedge_profit = 0;
    let mut hedge_loss = 0;
    let mut risk_exit = 0;
    let mut conservative_nofill = 0;
    let mut partial_or_queue = 0;
    for record in &records {
        let episode = &record.episode;
        if episode.net_pnl_usd > EPSILON {
            wins += 1;
        }
        if episode.net_pnl_usd < -EPSILON {
            losses += 1;
        }
        if episode.exit_type == ExitType::Hedge && episode.gross_pnl_usd > EPSILON {
            hedge_profit += 1;
        }
        if episode.exit_type == ExitType::Hedge && episode.gross_pnl_usd < -EPSILON {
            hedge_loss += 1;
        }
        if episode.exit_type == ExitType::Stop {
            risk_exit += 1;
        }
        if record.profile == FillProfile::Conservative && episode.is_no_fill() {
            conservative_nofill += 1;
        }
        if let Some((qty, shares)) = episode
            .fill_qty
            .zip(Some(episode.shares))
            .filter(|(qty, shares)| *qty < *shares - EPSILON)
        {
            partial_or_queue += 1;
            eprintln!(
                "[checkpoint] partial evidence condition={} signal_index={} profile={} fill_qty={qty:.6} size={shares:.6} fill_fraction={:.6}",
                record.condition_id,
                record.signal_index,
                profile_name(record.profile),
                qty / shares
            );
        }
        eprintln!(
            "[checkpoint] episode id={} condition={} signal_index={} profile={} side={:?} limit={:.4} stake={:.6} shares={:.6} arrival={} fill_ts={:?} fill_price={:?} fill_qty={:?} exit={:?} exit_price={:?} resolution_at={:?} resolution_outcome={:?} provenance={:?} gross={:.9} fees={:.9} net={:.9} current={:.9} regime={}",
            episode.episode_id,
            record.condition_id,
            record.signal_index,
            profile_name(record.profile),
            episode.side,
            episode.limit_price,
            episode.stake_usd,
            episode.shares,
            episode.order_arrival_ts_ms,
            episode.fill_ts_ms,
            episode.fill_price,
            episode.fill_qty,
            episode.exit_type,
            episode.exit_price,
            episode.resolution_at_ms,
            episode.resolution_outcome,
            episode.resolution_provenance,
            episode.gross_pnl_usd,
            episode.fees_usd,
            episode.net_pnl_usd,
            episode.pnl_current_usd.unwrap_or(0.0),
            record.regime
        );
    }

    eprintln!(
        "[checkpoint] variety covered wins={wins} losses={losses} hedge_profit={hedge_profit} hedge_loss={hedge_loss} risk_exit={risk_exit} conservative_nofill={conservative_nofill} partial_or_queue={partial_or_queue}"
    );
    assert!(
        wins > 0,
        "real tape must retain at least one positive outcome"
    );
    assert!(
        losses > 0,
        "real tape must retain at least one negative outcome"
    );
    assert!(
        hedge_profit > 0,
        "at least one hedge must lock positive PnL"
    );
    assert!(hedge_loss > 0, "at least one hedge must lock negative PnL");
    assert!(
        partial_or_queue > 0,
        "real tape must produce a partial fill"
    );
    if wins == 0 {
        eprintln!(
            "[checkpoint] variety AUSENTE win: no selected real episode had positive historical net; evidence={wins}"
        );
    }
    if losses == 0 {
        eprintln!(
            "[checkpoint] variety AUSENTE loss: no selected real episode had negative historical net; evidence={losses}"
        );
    }
    if hedge_profit == 0 {
        eprintln!(
            "[checkpoint] variety AUSENTE hedge profit: selected NO prints did not complete a profitable pair"
        );
    }
    if hedge_loss == 0 {
        eprintln!(
            "[checkpoint] variety AUSENTE hedge loss: selected NO prints did not complete a loss pair"
        );
    }
    eprintln!(
        "[checkpoint] risk-exit residuals={risk_exit} (complete real hedge pairs should leave this at zero)"
    );
    if conservative_nofill == 0 {
        eprintln!(
            "[checkpoint] variety AUSENTE NO_FILL Conservative: real selected tape supplied conservative evidence for every entry"
        );
    }
    if partial_or_queue == 0 {
        eprintln!(
            "[checkpoint] variety AUSENTE partial/queue: selected tape produced only full fills"
        );
    }

    assert_eq!(profile_stats.len(), 3);
}

#[test]
fn real_data_checkpoint_reconciles_ledger_fills_exits_and_fees() {
    checkpoint_test();
}

#[test]
fn allowed_inputs_are_present_and_no_underlying_is_needed() {
    for path in [TRADE_FILE, RESOLUTION_FILE, SPEC_FILE, REGIME_FILE] {
        assert!(
            std::path::Path::new(path).is_file(),
            "missing allowed input {path}"
        );
    }
    assert_eq!(PROCESSED, "research-data/processed");
}
