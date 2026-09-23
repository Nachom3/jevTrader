//! Phase-1 Jev precompute: bounded, paper-only historical state evaluation.
//!
//! The default underlying input is the merged `underlying_all.parquet` because
//! `read_underlying_window` can use its row-group statistics and only read the
//! bounded per-condition window; it never loads the 97M-row corpus wholesale.

use arrow::array::{BooleanArray, Float64Array, Int64Array, StringArray, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use futures_util::stream::{self, StreamExt};
use jevtrader::domain::{PriceTicks, TickSize};
use jevtrader::engine::pipeline::candidate_maker_price;
use jevtrader::jev::client::{self, PRECOMPUTE_MAX_ATTEMPTS};
use jevtrader::jev::request::{QuestionSet, V1State};
use jevtrader::jev::response::{JevEvaluation, parse_evaluation_json};
use jevtrader::polymarket::OrderBook;
use jevtrader::replay::causality::{CausalityVerdict, audit_state};
use jevtrader::replay::jev_cache::{
    CacheEntryMetadata, CachedJev, JevCache, JevCacheKey, VersionPins, versions::sha256_hex,
};
use jevtrader::replay::runner::{JevEvaluator, StubJev};
use jevtrader::replay::source::{
    ChunkEventSource, MarketMeta, read_market_metas, read_regimes, read_resolution_specs_end,
    read_resolutions, read_underlying_window,
};
use jevtrader::replay::types::{Fidelity, HistoricalEvent};
use jevtrader::state::feature_builder::{
    ContractContext, ExternalTick, OrderFlowAggregates, ResolutionContext, VenueMicroprices,
    build_features_full,
};
use parquet::arrow::ArrowWriter;
use serde::Serialize;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::error::Error;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Semaphore;

const DEFAULT_UNDERLYING: &str = "research-data/processed/underlying_all.parquet";
const DEFAULT_OUT: &str = "research-data/precompute";
const DEFAULT_RUN_ID: &str = "jev-precompute-v1";
const MAX_PER_CONDITION_SIGNALS: usize = 8;
const FLUSH_EVERY_COMPLETED_STATES: usize = 10;
// Fallback assumption when market metadata has no tick size; affects candidate price only.
const FALLBACK_TICK_SIZE: f64 = 0.01;
// Smoke cap only. This is not the preregistered 2,000-5,000 pair target.
const DEFAULT_MAX_STATES: &str = "50";

type BoxError = Box<dyn Error + Send + Sync>;

#[derive(Debug, Clone)]
struct Args {
    underlying: String,
    tape: Option<String>,
    out: PathBuf,
    run_id: String,
    per_condition: usize,
    max_live_calls: usize,
    max_states: usize,
    exact_only: bool,
    live: bool,
    end_boundary: Option<EndBoundary>,
}

#[derive(Debug, Clone)]
struct EndBoundary {
    raw: String,
    timestamp_ms: i64,
}

#[derive(Debug)]
struct Prepared {
    timestamp: i64,
    condition_id: String,
    state: V1State,
    state_hash: String,
    questions: Value,
    questions_hash: String,
    split: String,
    fidelity: String,
}

#[derive(Debug, Clone)]
struct EvalRow {
    timestamp: i64,
    state_hash: String,
    questions_hash: String,
    pins: VersionPins,
    signal: Option<JevEvaluation>,
    observed_latency_ms: u64,
    tokens_in: u64,
    tokens_out: u64,
    attempt_count: u32,
    error: Option<String>,
    trigger: String,
    split: String,
    fidelity: String,
    live: bool,
    status: String,
}

#[derive(Debug)]
struct TaskResult {
    row: EvalRow,
}

#[derive(Debug, Default, Clone, Serialize)]
struct Counts {
    states: usize,
    eligible_conditions: usize,
    incomplete_poly_trade_without_polytop: usize,
    /// Overlapping diagnostic for any candidate without depth/imbalance evidence.
    incomplete_missing_book_geometry: usize,
    incomplete_short_history: usize,
    incomplete_null_quote: usize,
    incomplete_missing_kachoio_outcome: usize,
    incomplete_missing_underlying: usize,
    skipped_invalid_candidate: usize,
    skipped_causality_audit: usize,
    live_calls: usize,
    status: BTreeMap<String, usize>,
    split: BTreeMap<String, usize>,
    fidelity: BTreeMap<String, usize>,
    strata: BTreeMap<String, usize>,
    condition_strata: BTreeMap<String, usize>,
    empty_strata: Vec<String>,
}

#[derive(Debug, Serialize)]
struct Manifest {
    run_id: String,
    live: bool,
    exact_only: bool,
    per_condition_signals: usize,
    max_states: usize,
    max_states_semantics: &'static str,
    max_live_calls: usize,
    /// Number of evaluation rows durable in the current partial/final output.
    persisted_calls: usize,
    /// Number of rows with a parsed evaluation after every source/feature/API skip.
    n_complete_pairs: usize,
    /// Defines `n_complete_pairs` as rows retained after skips with a parsed Jev signal.
    n_complete_pairs_semantics: &'static str,
    end_boundary_utc_exclusive: Option<String>,
    concurrency: usize,
    counts: Counts,
    version: VersionPins,
    input_files: Vec<String>,
    resolution_source: Option<&'static str>,
    resolution_label_caveat: &'static str,
    flow_available: bool,
    venue_data_caveat: &'static str,
    underlying_reason: &'static str,
}

struct FlushContext<'a> {
    out: &'a Path,
    cache: &'a Arc<Mutex<JevCache>>,
    args: &'a Args,
    pins: &'a VersionPins,
    input_files: &'a [String],
    resolution_source: Option<&'static str>,
    concurrency: usize,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    if let Err(error) = run().await {
        tracing::error!(error = %error, "precompute_jev failed");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), BoxError> {
    let args = parse_args()?;
    if args.live
        && std::env::var("TYPESAFE_API_KEY")
            .unwrap_or_default()
            .trim()
            .is_empty()
    {
        return Err("--live requires a non-empty TYPESAFE_API_KEY".into());
    }
    fs::create_dir_all(&args.out)?;
    let pins = VersionPins::current_v1();
    let corpus = Path::new(&args.underlying)
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .display()
        .to_string();
    let metas = read_market_metas(&corpus);
    let specs = read_resolution_specs_end(&corpus);
    let regimes = read_regimes(&corpus);
    let market_tick_sizes = read_market_tick_sizes(&corpus);
    let end_boundary_ms = args
        .end_boundary
        .as_ref()
        .map(|boundary| boundary.timestamp_ms);

    let mut counts = Counts::default();
    for status in ["ok", "hit", "429", "5xx", "deadline", "other"] {
        counts.status.insert(status.to_owned(), 0);
    }
    let mut prepared = Vec::new();
    let mut resolution_source = None;
    let mut tape_inputs = Vec::new();
    if let Some(tape_path) = args.tape.as_deref() {
        let tape_path_buf = PathBuf::from(tape_path);
        let market_files = kachoio_market_files(&tape_path_buf);
        let outcomes = read_kachoio_outcomes(&market_files)?;
        let tape = read_kachoio_tape(tape_path)?;
        prepared = build_tape_prepared(TapeBuildParams {
            args: &args,
            metas: &metas,
            specs: &specs,
            regimes: &regimes,
            tape: &tape,
            outcomes: &outcomes,
            market_tick_sizes: &market_tick_sizes,
            underlying_path: &args.underlying,
            end_boundary_ms,
            counts: &mut counts,
        })?;
        resolution_source = Some("underlying-initial-tick");
        tape_inputs.push(tape_path.to_owned());
        tape_inputs.extend(
            market_files
                .into_iter()
                .map(|path| path.display().to_string()),
        );
    } else {
        let tape_path = format!("{corpus}/polymarket_trades.parquet");
        let resolutions = read_resolutions(&corpus);
        let tape = ChunkEventSource::new(vec![tape_path.clone()], 8192)
            .read_parquet_chunks(1_000_000)
            .map_err(|error| format!("read tape: {error}"))?;
        let mut by_condition: HashMap<String, Vec<HistoricalEvent>> = HashMap::new();
        for event in tape {
            if end_boundary_ms.is_some_and(|boundary| event.ts_ms() >= boundary) {
                continue;
            }
            let condition_id = match &event {
                HistoricalEvent::PolyTrade { condition_id, .. }
                | HistoricalEvent::PolyTop { condition_id, .. } => condition_id,
                HistoricalEvent::UnderlyingTick { .. } => continue,
            };
            by_condition
                .entry(condition_id.clone())
                .or_default()
                .push(event);
        }
        for events in by_condition.values_mut() {
            events.sort_by_key(HistoricalEvent::ts_ms);
        }
        for meta in metas
            .iter()
            .filter(|meta| eligible(meta, &specs, args.exact_only))
        {
            if prepared.len() >= args.max_states {
                break;
            }
            let Some(events) = by_condition.get(&meta.condition_id) else {
                continue;
            };
            let Some(_resolution_ms) = resolution_time(meta, &resolutions, &specs) else {
                continue;
            };
            counts.eligible_conditions += 1;
            let first_ts = events.first().map(HistoricalEvent::ts_ms).unwrap_or(0);
            let condition_regime = regime_at(&regimes, &meta.asset, first_ts);
            *counts
                .condition_strata
                .entry(format!(
                    "{}-{}-{condition_regime}",
                    meta.asset, meta.horizon
                ))
                .or_default() += 1;
            let complete = complete_signal_indices(events, &mut counts);
            if complete.is_empty() {
                continue;
            }
            let take = args.per_condition.min(complete.len());
            let selected: Vec<usize> = (0..take)
                .map(|ordinal| complete[ordinal * complete.len() / take])
                .collect();
            for _index in selected {
                if prepared.len() >= args.max_states {
                    break;
                }
                // `HistoricalEvent::PolyTop` currently carries only best bid/ask.
                // It has no depth or imbalance evidence, so this state is excluded
                // rather than populated with synthetic book geometry or flow.
                counts.incomplete_missing_book_geometry += 1;
            }
        }
        tape_inputs.push(tape_path);
    }
    for key in [
        "BTC-5m", "BTC-15m", "BTC-1h", "BTC-4h", "ETH-5m", "ETH-15m", "ETH-1h", "ETH-4h",
    ] {
        if !counts
            .condition_strata
            .keys()
            .any(|stratum| stratum.starts_with(&format!("{key}-")))
        {
            counts.empty_strata.push(key.to_owned());
        }
    }
    counts.states = prepared.len();
    let concurrency = std::env::var("JEV_CONCURRENCY")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(8usize)
        .clamp(1, 64);
    let mut input_files = vec![
        args.underlying.clone(),
        format!("{corpus}/selected_markets.parquet"),
        format!("{corpus}/resolution_specs.parquet"),
        format!("{corpus}/market_regimes.parquet"),
    ];
    if args.tape.is_none() {
        input_files.push(format!("{corpus}/resolutions.parquet"));
    }
    input_files.extend(tape_inputs);

    let cache = Arc::new(Mutex::new(JevCache::new()));
    cache
        .lock()
        .expect("cache mutex")
        .load_from_dir(&args.out)?;
    let semaphore = Arc::new(Semaphore::new(concurrency));
    let live_calls = Arc::new(AtomicUsize::new(0));
    let api_key = std::env::var("TYPESAFE_API_KEY").unwrap_or_default();
    let flush_context = FlushContext {
        out: &args.out,
        cache: &cache,
        args: &args,
        pins: &pins,
        input_files: &input_files,
        resolution_source,
        concurrency,
    };
    let mut result_stream = stream::iter(prepared.into_iter().map(|item| {
        process_one(
            item,
            pins.clone(),
            cache.clone(),
            semaphore.clone(),
            live_calls.clone(),
            api_key.clone(),
            args.live,
            args.max_live_calls,
        )
    }))
    .buffer_unordered(concurrency);
    let mut rows = Vec::new();
    while let Some(result) = result_stream.next().await {
        let row = result.row;
        *counts.status.entry(row.status.clone()).or_default() += 1;
        rows.push(row);
        if rows.len() % FLUSH_EVERY_COMPLETED_STATES == 0 {
            counts.live_calls = live_calls.load(Ordering::Relaxed);
            let _ = flush_progress(&flush_context, &counts, &mut rows)?;
        }
    }
    counts.live_calls = live_calls.load(Ordering::Relaxed);
    let manifest = flush_progress(&flush_context, &counts, &mut rows)?;
    tracing::info!(
        states = manifest.counts.states,
        live_calls = manifest.counts.live_calls,
        cache_hits = manifest.counts.status.get("hit").copied().unwrap_or(0),
        persisted_calls = manifest.persisted_calls,
        "precompute complete"
    );
    Ok(())
}

fn flush_progress(
    context: &FlushContext<'_>,
    counts: &Counts,
    rows: &mut [EvalRow],
) -> Result<Manifest, BoxError> {
    rows.sort_by(|a, b| (a.timestamp, &a.state_hash).cmp(&(b.timestamp, &b.state_hash)));
    let manifest = build_manifest(
        context.args,
        counts,
        rows,
        context.pins,
        context.input_files,
        context.resolution_source,
        context.concurrency,
    );
    context
        .cache
        .lock()
        .expect("cache mutex")
        .save_to_dir(context.out)?;
    write_evaluations(&context.out.join("evaluations.parquet"), rows)?;
    fs::write(
        context.out.join("evaluations.manifest.json"),
        serde_json::to_vec_pretty(&manifest)?,
    )?;
    sync_output_dir(context.out);
    tracing::info!(
        persisted_calls = manifest.persisted_calls,
        cache_hits = manifest.counts.status.get("hit").copied().unwrap_or(0),
        "precompute progress flushed"
    );
    Ok(manifest)
}

fn build_manifest(
    args: &Args,
    counts: &Counts,
    rows: &[EvalRow],
    pins: &VersionPins,
    input_files: &[String],
    resolution_source: Option<&'static str>,
    concurrency: usize,
) -> Manifest {
    Manifest {
        run_id: args.run_id.clone(),
        live: args.live,
        exact_only: args.exact_only,
        per_condition_signals: args.per_condition,
        max_states: args.max_states,
        max_states_semantics: "Local smoke cap only; not the preregistered 2,000-5,000 complete-pair target.",
        max_live_calls: args.max_live_calls,
        persisted_calls: rows.len(),
        n_complete_pairs: rows.iter().filter(|row| row.signal.is_some()).count(),
        n_complete_pairs_semantics: "Rows retained after source/feature skips with a parsed Jev signal; incomplete states and request/parse failures are excluded.",
        end_boundary_utc_exclusive: args
            .end_boundary
            .as_ref()
            .map(|boundary| boundary.raw.clone()),
        concurrency,
        counts: counts.clone(),
        version: pins.clone(),
        input_files: input_files.to_vec(),
        resolution_source,
        resolution_label_caveat: if args.tape.is_some() {
            "Kachoio outcome is an inferred final-tick label used only as a presence gate; it is not resolution truth or the target value."
        } else {
            "No Kachoio outcome label is present in the non-tape path."
        },
        flow_available: false,
        venue_data_caveat: if args.tape.is_some() {
            "Kachoio has no external venue or order-flow data: Coinbase mirrors spot and flow fields are zero/neutral unavailable-data placeholders."
        } else {
            "No tape states were prepared; venue and order-flow availability is not applicable."
        },
        underlying_reason: if args.tape.is_some() {
            "underlying_all is window-read per Kachoio condition for LeadLagFeatures"
        } else {
            "underlying_all is registered for the run, but rows are not loaded when the normalized book lacks depth/imbalance evidence"
        },
    }
}

fn sync_output_dir(out: &Path) {
    if let Err(error) = File::open(out).and_then(|directory| directory.sync_all()) {
        tracing::warn!(path = %out.display(), error = %error, "unable to fsync precompute output directory");
    }
}

#[derive(Debug, Clone)]
struct TapeRow {
    ts_ms: i64,
    yes_bid: Option<f64>,
    yes_ask: Option<f64>,
    bid_depth_5c: Option<f64>,
    mid: Option<f64>,
}

fn kachoio_market_files(tape_path: &Path) -> Vec<PathBuf> {
    let raw_dir = tape_path
        .parent()
        .and_then(Path::parent)
        .map(|root| root.join("raw/kaggle-kachoio"))
        .unwrap_or_else(|| PathBuf::from("research-data/raw/kaggle-kachoio"));
    ["btc_markets.parquet", "eth_markets.parquet"]
        .into_iter()
        .map(|name| raw_dir.join(name))
        .collect()
}

fn read_kachoio_tape(path: &str) -> Result<HashMap<String, Vec<TapeRow>>, BoxError> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let file = File::open(path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?
        .with_batch_size(8192)
        .build()?;
    let mut by_condition: HashMap<String, Vec<TapeRow>> = HashMap::new();
    for batch in reader {
        let batch = batch?;
        for row in 0..batch.num_rows() {
            let (Some(condition_id), Some(ts_ms)) = (
                parquet_string(&batch, "condition_id", row).filter(|value| !value.is_empty()),
                parquet_i64(&batch, "ts_ms", row),
            ) else {
                continue;
            };
            by_condition.entry(condition_id).or_default().push(TapeRow {
                ts_ms,
                yes_bid: parquet_f64(&batch, "yes_bid", row),
                yes_ask: parquet_f64(&batch, "yes_ask", row),
                bid_depth_5c: parquet_f64(&batch, "bid_depth_5c", row),
                mid: parquet_f64(&batch, "mid", row),
            });
        }
    }
    for rows in by_condition.values_mut() {
        rows.sort_by_key(|row| row.ts_ms);
    }
    Ok(by_condition)
}

fn read_kachoio_outcomes(paths: &[PathBuf]) -> Result<HashMap<String, String>, BoxError> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let mut outcomes = HashMap::new();
    let mut opened = 0usize;
    for path in paths {
        let Ok(file) = File::open(path) else {
            continue;
        };
        opened += 1;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)?
            .with_batch_size(8192)
            .build()?;
        for batch in reader {
            let batch = batch?;
            for row in 0..batch.num_rows() {
                let (Some(condition_id), Some(outcome)) = (
                    parquet_string(&batch, "condition_id", row).filter(|value| !value.is_empty()),
                    parquet_string(&batch, "outcome", row).filter(|value| !value.is_empty()),
                ) else {
                    continue;
                };
                outcomes.insert(condition_id, outcome);
            }
        }
    }
    if opened == 0 {
        return Err("kachoio markets files are missing; expected btc_markets.parquet and eth_markets.parquet".into());
    }
    if outcomes.is_empty() {
        return Err("kachoio markets files contain no condition_id/outcome rows".into());
    }
    Ok(outcomes)
}

fn read_market_tick_sizes(processed_dir: &str) -> HashMap<String, f64> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let path = format!("{processed_dir}/selected_markets.parquet");
    let Ok(file) = File::open(path) else {
        return HashMap::new();
    };
    let Ok(reader) = ParquetRecordBatchReaderBuilder::try_new(file)
        .and_then(|builder| builder.with_batch_size(8192).build())
    else {
        return HashMap::new();
    };
    let mut tick_sizes = HashMap::new();
    for batch in reader {
        let Ok(batch) = batch else {
            continue;
        };
        for row in 0..batch.num_rows() {
            let Some(condition_id) =
                parquet_string(&batch, "condition_id", row).filter(|value| !value.is_empty())
            else {
                continue;
            };
            let Some(tick_size) = parquet_positive_f64(&batch, "tick_size", row) else {
                continue;
            };
            tick_sizes.insert(condition_id, tick_size);
        }
    }
    tick_sizes
}

fn parquet_string(batch: &RecordBatch, name: &str, row: usize) -> Option<String> {
    use arrow::array::{Array as _, LargeStringArray};

    let index = batch.schema().index_of(name).ok()?;
    let column = batch.column(index);
    if let Some(values) = column.as_any().downcast_ref::<StringArray>() {
        return (!values.is_null(row)).then(|| values.value(row).to_owned());
    }
    let values = column.as_any().downcast_ref::<LargeStringArray>()?;
    (!values.is_null(row)).then(|| values.value(row).to_owned())
}

fn parquet_f64(batch: &RecordBatch, name: &str, row: usize) -> Option<f64> {
    use arrow::array::Array as _;

    let index = batch.schema().index_of(name).ok()?;
    let values = batch
        .column(index)
        .as_any()
        .downcast_ref::<Float64Array>()?;
    (!values.is_null(row)).then(|| values.value(row))
}

fn parquet_positive_f64(batch: &RecordBatch, name: &str, row: usize) -> Option<f64> {
    let value = parquet_f64(batch, name, row)
        .or_else(|| parquet_string(batch, name, row)?.parse::<f64>().ok())?;
    (value.is_finite() && value > 0.0).then_some(value)
}

fn parquet_i64(batch: &RecordBatch, name: &str, row: usize) -> Option<i64> {
    use arrow::array::Array as _;

    let index = batch.schema().index_of(name).ok()?;
    let values = batch.column(index).as_any().downcast_ref::<Int64Array>()?;
    (!values.is_null(row)).then(|| values.value(row))
}

struct TapeBuildParams<'a> {
    args: &'a Args,
    metas: &'a [MarketMeta],
    specs: &'a HashMap<String, (i64, Fidelity)>,
    regimes: &'a [(i64, String, String)],
    tape: &'a HashMap<String, Vec<TapeRow>>,
    outcomes: &'a HashMap<String, String>,
    market_tick_sizes: &'a HashMap<String, f64>,
    underlying_path: &'a str,
    end_boundary_ms: Option<i64>,
    counts: &'a mut Counts,
}

fn build_tape_prepared(params: TapeBuildParams<'_>) -> Result<Vec<Prepared>, BoxError> {
    let TapeBuildParams {
        args,
        metas,
        specs,
        regimes,
        tape,
        outcomes,
        market_tick_sizes,
        underlying_path,
        end_boundary_ms,
        counts,
    } = params;
    let mut prepared = Vec::new();
    for meta in metas
        .iter()
        .filter(|meta| eligible(meta, specs, args.exact_only))
    {
        if prepared.len() >= args.max_states {
            break;
        }
        let Some(rows) = tape.get(&meta.condition_id) else {
            continue;
        };
        if !outcomes.contains_key(&meta.condition_id) {
            counts.incomplete_missing_kachoio_outcome += 1;
            continue;
        }
        let rows: Vec<TapeRow> = rows
            .iter()
            .filter(|row| end_boundary_ms.is_none_or(|boundary| row.ts_ms < boundary))
            .cloned()
            .collect();
        if rows.is_empty() {
            continue;
        }
        counts.eligible_conditions += 1;
        let first_ts = rows[0].ts_ms;
        let last_ts = rows.last().map_or(first_ts, |row| row.ts_ms);
        let condition_regime = regime_at(regimes, &meta.asset, first_ts);
        *counts
            .condition_strata
            .entry(format!(
                "{}-{}-{condition_regime}",
                meta.asset, meta.horizon
            ))
            .or_default() += 1;

        let mut prices = HashMap::new();
        for row in &rows {
            if let Some((_, _, mid)) = tape_quote(row) {
                prices.insert(row.ts_ms, mid);
            }
        }
        let mut eligible_indices = Vec::new();
        for (index, row) in rows.iter().enumerate() {
            // R3: missing quote columns are unavailable data; skip gracefully rather than impute.
            if row.yes_bid.is_none() || row.yes_ask.is_none() {
                counts.incomplete_null_quote += 1;
                continue;
            }
            if tape_quote(row).is_none() {
                counts.incomplete_null_quote += 1;
                continue;
            }
            // R2: exact lags are intentional because the tape asserts 1s contiguity;
            // an absent exact observation is skipped rather than fabricated.
            let complete_history = [1_000_i64, 5_000, 30_000]
                .into_iter()
                .all(|lag| prices.contains_key(&(row.ts_ms - lag)));
            if !complete_history {
                counts.incomplete_short_history += 1;
                continue;
            }
            eligible_indices.push(index);
        }
        if eligible_indices.is_empty() || args.per_condition == 0 {
            continue;
        }

        let take = args.per_condition.min(eligible_indices.len());
        let selected = sample_indices(&eligible_indices, take);
        let mut underlying = match read_underlying_window(
            underlying_path,
            &meta.asset,
            first_ts.saturating_sub(2 * 3_600_000),
            last_ts,
            16,
            50_000,
        ) {
            Ok(rows) => rows,
            Err(error) => {
                tracing::warn!(asset = %meta.asset, error = %error, "underlying window unavailable");
                Vec::new()
            }
        };
        let perp_asset = format!("{}-PERP", meta.asset);
        match read_underlying_window(
            underlying_path,
            &perp_asset,
            first_ts.saturating_sub(2 * 3_600_000),
            last_ts,
            16,
            50_000,
        ) {
            Ok(rows) => underlying.extend(rows),
            Err(error) => {
                tracing::warn!(asset = %perp_asset, error = %error, "perp window unavailable")
            }
        }
        let mut base_ticks = underlying_ticks(&underlying, &meta.asset);
        base_ticks.sort_by_key(|tick| tick.ts_ms);
        if base_ticks.is_empty() {
            counts.incomplete_missing_underlying += selected.len();
            continue;
        }
        // R1: target is the first underlying tick at/after market start (earliest loaded fallback); Kachoio's inferred final-tick outcome is presence-only.
        let target_tick = base_ticks
            .iter()
            .find(|tick| tick.ts_ms >= first_ts.max(0) as u64)
            .unwrap_or(&base_ticks[0]);
        let target = target_tick.price;
        let resolution_ms = last_ts.saturating_add(1_000);

        for index in selected {
            if prepared.len() >= args.max_states {
                break;
            }
            let row = &rows[index];
            let Some((bid, ask, mid)) = tape_quote(row) else {
                counts.incomplete_null_quote += 1;
                continue;
            };
            let Some(price_1s_ago) = prices.get(&(row.ts_ms - 1_000)).copied() else {
                counts.incomplete_short_history += 1;
                continue;
            };
            let Some(price_5s_ago) = prices.get(&(row.ts_ms - 5_000)).copied() else {
                counts.incomplete_short_history += 1;
                continue;
            };
            let Some(price_30s_ago) = prices.get(&(row.ts_ms - 30_000)).copied() else {
                counts.incomplete_short_history += 1;
                continue;
            };
            let tick_size = market_tick_sizes
                .get(&meta.condition_id)
                .copied()
                .unwrap_or(FALLBACK_TICK_SIZE);
            let Some(candidate) = candidate_price(bid, ask, TickSize::from_f64(tick_size)) else {
                counts.skipped_invalid_candidate += 1;
                continue;
            };
            let recent_ticks: Vec<ExternalTick> = base_ticks
                .iter()
                .filter(|tick| tick.ts_ms <= row.ts_ms.max(0) as u64)
                .copied()
                .collect();
            let Some(spot) = recent_ticks.last().map(|tick| tick.price) else {
                counts.incomplete_missing_underlying += 1;
                continue;
            };
            let perp_tick = underlying
                .iter()
                .filter_map(|event| match event {
                    HistoricalEvent::UnderlyingTick {
                        ts_ms,
                        asset,
                        price,
                        ..
                    } if asset == &perp_asset
                        && *ts_ms <= row.ts_ms
                        && price.is_finite()
                        && *price > 0.0 =>
                    {
                        Some((*ts_ms, *price))
                    }
                    _ => None,
                })
                .next_back();
            let perp = perp_tick.map_or(spot, |(_, price)| price);
            let resolution = ResolutionContext::new(
                target,
                resolution_ms.saturating_sub(row.ts_ms) as u64 / 1_000,
                "underlying-initial-tick",
            );
            let features = build_features_full(
                &recent_ticks,
                &resolution,
                &ContractContext::new(
                    meta.asset.clone(),
                    meta.horizon.clone(),
                    horizon_seconds(&meta.horizon),
                ),
                // R4: Kachoio has no Coinbase feed; coinbase==spot is an
                // unavailable-data placeholder, not an observed venue value.
                VenueMicroprices {
                    binance: spot,
                    coinbase: spot,
                    perp,
                    perp_basis_pct: if spot > 0.0 {
                        (perp - spot) / spot * 100.0
                    } else {
                        0.0
                    },
                },
                // R4: zero flow is unavailable data, not measured neutral flow.
                OrderFlowAggregates {
                    buy_vol_1s: 0.0,
                    sell_vol_1s: 0.0,
                    ofi_1s: 0.0,
                    ofi_5s: 0.0,
                    imbalance: 0.0,
                    aggressive_buy_ratio: 0.5,
                },
            );
            let poly = jevtrader::strategy::lead_lag::PolySnapshot {
                yes_bid: PriceTicks::from_f64(bid),
                yes_ask: PriceTicks::from_f64(ask),
                bid_depth: row.bid_depth_5c.unwrap_or(0.0).max(0.0),
                // Kachoio has no ask-side depth. Keep it explicitly empty;
                // do not fabricate an imbalance from one-sided evidence.
                ask_depth: 0.0,
                spread: ask - bid,
                book_imbalance: 0.0,
                last_trade_price: PriceTicks::from_f64(mid),
                price_1s_ago: PriceTicks::from_f64(price_1s_ago),
                price_5s_ago: PriceTicks::from_f64(price_5s_ago),
                price_30s_ago: PriceTicks::from_f64(price_30s_ago),
            };
            let state = V1State::new(
                meta.question.clone(),
                meta.resolution_rules.clone(),
                features,
                poly,
                None,
                candidate,
            );
            let state_json = serde_json::to_value(&state)?;
            let mut audit_inputs = Vec::with_capacity(recent_ticks.len() + 6);
            audit_inputs.push(("polymarket_book", row.ts_ms));
            audit_inputs.extend(
                [1_000_i64, 5_000, 30_000]
                    .into_iter()
                    .map(|lag| ("polymarket_price_lag", row.ts_ms - lag)),
            );
            audit_inputs.push(("underlying_target", target_tick.ts_ms as i64));
            audit_inputs.extend(
                recent_ticks
                    .iter()
                    .map(|tick| ("underlying_tick", tick.ts_ms as i64)),
            );
            if let Some((ts_ms, _)) = perp_tick {
                audit_inputs.push(("perp_tick", ts_ms));
            }
            if let CausalityVerdict::Rejected { reason } =
                audit_state(row.ts_ms, &audit_inputs, &state_json)
            {
                counts.skipped_causality_audit += 1;
                tracing::warn!(
                    condition_id = %meta.condition_id,
                    timestamp_ms = row.ts_ms,
                    ?reason,
                    "skipping tape state rejected by causality audit"
                );
                continue;
            }
            let questions = QuestionSet::V1.build(candidate);
            let state_hash = sha256_hex(&canonical_bytes(&state)?);
            let questions_hash = sha256_hex(&canonical_bytes(&questions)?);
            prepared.push(Prepared {
                timestamp: row.ts_ms,
                condition_id: meta.condition_id.clone(),
                state,
                state_hash,
                questions,
                questions_hash,
                split: meta.split.as_str().to_owned(),
                fidelity: meta.fidelity.as_str().to_owned(),
            });
        }
    }
    Ok(prepared)
}

fn tape_quote(row: &TapeRow) -> Option<(f64, f64, f64)> {
    let bid = row.yes_bid?;
    let ask = row.yes_ask?;
    if !bid.is_finite()
        || !ask.is_finite()
        || !(0.0..=1.0).contains(&bid)
        || !(0.0..=1.0).contains(&ask)
    {
        return None;
    }
    let mid = row
        .mid
        .filter(|value| value.is_finite())
        .unwrap_or((bid + ask) / 2.0);
    (0.0..=1.0).contains(&mid).then_some((bid, ask, mid))
}

fn sample_indices(indices: &[usize], take: usize) -> Vec<usize> {
    if take == 0 {
        return Vec::new();
    }
    if take == 1 {
        return vec![indices[indices.len() / 2]];
    }
    (0..take)
        .map(|ordinal| indices[ordinal * (indices.len() - 1) / (take - 1)])
        .collect()
}

fn underlying_ticks(events: &[HistoricalEvent], asset: &str) -> Vec<ExternalTick> {
    events
        .iter()
        .filter_map(|event| match event {
            HistoricalEvent::UnderlyingTick {
                ts_ms,
                asset: event_asset,
                price,
                ..
            } if event_asset == asset && *ts_ms >= 0 && price.is_finite() && *price > 0.0 => {
                Some(ExternalTick {
                    ts_ms: *ts_ms as u64,
                    price: *price,
                })
            }
            _ => None,
        })
        .collect()
}

fn candidate_price(bid: f64, ask: f64, tick_size: TickSize) -> Option<PriceTicks> {
    if !bid.is_finite() || !ask.is_finite() || bid >= ask {
        return None;
    }
    let mut book = OrderBook::default();
    book.apply_snapshot(
        [(PriceTicks::from_f64(bid), 100)],
        [(PriceTicks::from_f64(ask), 100)],
    );
    let candidate = candidate_maker_price(&book, tick_size)?;
    (candidate < PriceTicks::from_f64(ask)).then_some(candidate)
}

fn horizon_seconds(horizon: &str) -> u64 {
    match horizon {
        "5m" => 300,
        "15m" => 900,
        "1h" => 3_600,
        "4h" => 14_400,
        _ => 0,
    }
}

fn eligible(meta: &MarketMeta, specs: &HashMap<String, (i64, Fidelity)>, exact_only: bool) -> bool {
    let spec_fidelity = specs.get(&meta.condition_id).map(|(_, fidelity)| *fidelity);
    let fidelity = spec_fidelity.unwrap_or(meta.fidelity);
    (!exact_only || (meta.fidelity == Fidelity::Exact && fidelity == Fidelity::Exact))
        && matches!(meta.asset.to_ascii_uppercase().as_str(), "BTC" | "ETH")
        && matches!(meta.horizon.as_str(), "5m" | "15m" | "1h" | "4h")
}

fn resolution_time(
    meta: &MarketMeta,
    resolutions: &[jevtrader::replay::source::ResolutionRecord],
    specs: &HashMap<String, (i64, Fidelity)>,
) -> Option<i64> {
    let record = resolutions
        .iter()
        .find(|row| row.condition_id == meta.condition_id)?;
    if !record.status.eq_ignore_ascii_case("resolved") {
        return None;
    }
    record
        .resolved_ts_ms
        .or_else(|| specs.get(&meta.condition_id).map(|(end, _)| *end))
}

fn complete_signal_indices(events: &[HistoricalEvent], counts: &mut Counts) -> Vec<usize> {
    let mut book = false;
    let mut complete = Vec::new();
    for (index, event) in events.iter().enumerate() {
        match event {
            HistoricalEvent::PolyTop {
                best_bid, best_ask, ..
            } if best_bid.is_finite() && best_ask.is_finite() && *best_bid < *best_ask => {
                book = true;
            }
            HistoricalEvent::PolyTrade { .. } if book => complete.push(index),
            HistoricalEvent::PolyTrade { .. } => {
                counts.incomplete_poly_trade_without_polytop += 1;
                counts.incomplete_missing_book_geometry += 1;
            }
            _ => {}
        }
    }
    complete
}

fn regime_at(regimes: &[(i64, String, String)], asset: &str, ts_ms: i64) -> String {
    let day = ts_ms / 1000 / 86_400 * 86_400;
    regimes
        .iter()
        .filter(|(at, symbol, _)| *at <= day && symbol.starts_with(asset))
        .map(|(_, _, regime)| regime.clone())
        .next_back()
        .unwrap_or_else(|| "UNKNOWN-UNKNOWN".to_owned())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CacheMode {
    Stub,
    Live,
}

impl CacheMode {
    const fn from_live(live: bool) -> Self {
        if live { Self::Live } else { Self::Stub }
    }

    const fn is_live(self) -> bool {
        matches!(self, Self::Live)
    }
}

/// Cache payloads are mode-segregated at both boundaries: a stub entry is
/// never served to the live path, and a live entry is never served to the stub
/// path or written through the other mode.
fn cache_entry_matches_mode(entry: &CachedJev, mode: CacheMode) -> bool {
    entry.live == mode.is_live()
}

#[allow(clippy::too_many_arguments)]
async fn process_one(
    item: Prepared,
    pins: VersionPins,
    cache: Arc<Mutex<JevCache>>,
    semaphore: Arc<Semaphore>,
    live_calls: Arc<AtomicUsize>,
    api_key: String,
    live: bool,
    max_live_calls: usize,
) -> TaskResult {
    let _permit = semaphore.acquire().await.expect("semaphore is alive");
    let mode = CacheMode::from_live(live);
    let key =
        JevCacheKey::new_precompute(item.state_hash.clone(), item.questions_hash.clone(), &pins);
    let cached = {
        let mut cache_guard = cache.lock().expect("cache mutex");
        cache_guard.get(&key)
    };
    if let Some(cached) = cached.filter(|cached| cache_entry_matches_mode(cached, mode)) {
        let metadata = cache
            .lock()
            .expect("cache mutex")
            .metadata(&key)
            .unwrap_or_default();
        let parsed = parse_cached(&cached, &item.condition_id, item.timestamp);
        return TaskResult {
            row: result_row(item, pins, cached, metadata, parsed, "hit"),
        };
    }
    let request_json =
        canonical_bytes(&serde_json::json!({"state": &item.state, "questions": &item.questions}))
            .unwrap_or_default();
    let (cached, metadata, parsed, status) = if !live {
        let mut stub = StubJev::new(0x004a_4556_5631);
        let outcome = stub.evaluate(
            &item.state,
            &item.condition_id,
            0,
            &pins.variant,
            &item.questions,
            0,
        );
        let parsed = parse_evaluation_json(
            outcome.envelope_json.as_bytes(),
            &item.condition_id,
            0,
            0,
            0,
        )
        .ok();
        let value = CachedJev {
            envelope_json: outcome.envelope_json,
            latency_ms: 0,
            live: false,
            request_json: String::from_utf8_lossy(&request_json).into_owned(),
            parsed_output_json: serde_json::to_string(&parsed).unwrap_or_default(),
            jev_start_ts_ms: 0,
        };
        let metadata = CacheEntryMetadata {
            attempt_count: 0,
            final_error: None,
            observed_latency_ms: 0,
            attempt_durations_ms: Vec::new(),
            version: pins.clone(),
        };
        (value, metadata, parsed, "ok")
    } else if reserve_live_call(&live_calls, max_live_calls) {
        let questions = item.questions.clone();
        let outcome = client::evaluate_precompute(
            &item.state,
            &questions,
            &api_key,
            Duration::from_secs(30),
            PRECOMPUTE_MAX_ATTEMPTS,
        )
        .await;
        let parsed = outcome.body.as_deref().and_then(|body| {
            parse_evaluation_json(
                body,
                &item.condition_id,
                0,
                outcome.sent_at_ms,
                outcome.received_at_ms,
            )
            .ok()
        });
        let parse_error = outcome
            .body
            .as_ref()
            .and_then(|body| {
                parse_evaluation_json(
                    body,
                    &item.condition_id,
                    0,
                    outcome.sent_at_ms,
                    outcome.received_at_ms,
                )
                .err()
            })
            .map(|error| error.to_string());
        let error = outcome.final_error.or(parse_error);
        let status = error.as_deref().map(status_for_error).unwrap_or("ok");
        let value = CachedJev {
            envelope_json: outcome
                .body
                .map(|body| String::from_utf8_lossy(&body).into_owned())
                .unwrap_or_default(),
            latency_ms: outcome.observed_latency_ms,
            live: true,
            request_json: String::from_utf8_lossy(&request_json).into_owned(),
            parsed_output_json: serde_json::to_string(&parsed).unwrap_or_default(),
            jev_start_ts_ms: outcome.sent_at_ms,
        };
        let metadata = CacheEntryMetadata {
            attempt_count: outcome.attempt_count,
            final_error: error,
            observed_latency_ms: outcome.observed_latency_ms,
            attempt_durations_ms: outcome
                .attempts
                .into_iter()
                .map(|attempt| attempt.duration_ms)
                .collect(),
            version: pins.clone(),
        };
        (value, metadata, parsed, status)
    } else {
        let value = CachedJev {
            envelope_json: String::new(),
            latency_ms: 0,
            live: true,
            request_json: String::from_utf8_lossy(&request_json).into_owned(),
            parsed_output_json: String::new(),
            jev_start_ts_ms: 0,
        };
        let metadata = CacheEntryMetadata {
            attempt_count: 0,
            final_error: Some("live_call_budget_exhausted".to_owned()),
            observed_latency_ms: 0,
            attempt_durations_ms: Vec::new(),
            version: pins.clone(),
        };
        (value, metadata, None, "other")
    };
    if cache_entry_matches_mode(&cached, mode) {
        cache
            .lock()
            .expect("cache mutex")
            .put_precompute(key, cached.clone(), metadata.clone());
    } else {
        tracing::error!(
            live,
            "refusing to write a cache entry through the wrong mode"
        );
    }
    TaskResult {
        row: result_row(item, pins, cached, metadata, parsed, status),
    }
}

fn reserve_live_call(counter: &AtomicUsize, budget: usize) -> bool {
    loop {
        let current = counter.load(Ordering::Relaxed);
        if current >= budget {
            return false;
        }
        if counter
            .compare_exchange(current, current + 1, Ordering::SeqCst, Ordering::Relaxed)
            .is_ok()
        {
            return true;
        }
    }
}

fn parse_cached(cached: &CachedJev, condition_id: &str, timestamp: i64) -> Option<JevEvaluation> {
    parse_evaluation_json(
        cached.envelope_json.as_bytes(),
        condition_id,
        0,
        timestamp,
        timestamp,
    )
    .ok()
}

fn result_row(
    item: Prepared,
    pins: VersionPins,
    cached: CachedJev,
    metadata: CacheEntryMetadata,
    parsed: Option<JevEvaluation>,
    status: &'static str,
) -> EvalRow {
    let tokens_in = parsed.as_ref().map_or(0, |evaluation| evaluation.tokens_in);
    let tokens_out = parsed
        .as_ref()
        .map_or(0, |evaluation| evaluation.tokens_out);
    EvalRow {
        timestamp: item.timestamp,
        state_hash: item.state_hash,
        questions_hash: item.questions_hash,
        pins,
        signal: parsed,
        observed_latency_ms: metadata.observed_latency_ms.max(cached.latency_ms),
        tokens_in,
        tokens_out,
        attempt_count: metadata.attempt_count,
        error: metadata.final_error,
        trigger: "precompute_v1".to_owned(),
        split: item.split,
        fidelity: item.fidelity,
        live: cached.live,
        status: status.to_owned(),
    }
}

fn status_for_error(error: &str) -> &'static str {
    if error.contains("429") {
        "429"
    } else if error.contains("HTTP status 5") {
        "5xx"
    } else if error.to_ascii_lowercase().contains("deadline")
        || error.to_ascii_lowercase().contains("timeout")
    {
        "deadline"
    } else {
        "other"
    }
}

fn canonical_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>, serde_json::Error> {
    let value = serde_json::to_value(value)?;
    let mut out = Vec::new();
    write_canonical(&value, &mut out)?;
    Ok(out)
}

fn write_canonical(value: &Value, out: &mut Vec<u8>) -> Result<(), serde_json::Error> {
    match value {
        Value::Object(object) => {
            out.push(b'{');
            let mut keys: Vec<&String> = object.keys().collect();
            keys.sort();
            for (index, key) in keys.into_iter().enumerate() {
                if index > 0 {
                    out.push(b',');
                }
                serde_json::to_writer(&mut *out, key)?;
                out.push(b':');
                write_canonical(&object[key], out)?;
            }
            out.push(b'}');
        }
        Value::Array(array) => {
            out.push(b'[');
            for (index, value) in array.iter().enumerate() {
                if index > 0 {
                    out.push(b',');
                }
                write_canonical(value, out)?;
            }
            out.push(b']');
        }
        _ => serde_json::to_writer(&mut *out, value)?,
    }
    Ok(())
}

fn write_evaluations(path: &Path, rows: &[EvalRow]) -> Result<(), BoxError> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Int64, false),
        Field::new("state_hash", DataType::Utf8, false),
        Field::new("questions_hash", DataType::Utf8, false),
        Field::new("model_id", DataType::Utf8, false),
        Field::new("model_version", DataType::Utf8, false),
        Field::new("strategy_version", DataType::Utf8, false),
        Field::new("prompt_version", DataType::Utf8, false),
        Field::new("question_document_sha256", DataType::Utf8, false),
        Field::new("question_schema_version", DataType::Utf8, false),
        Field::new("feature_builder_version", DataType::Utf8, false),
        Field::new("normalization_version", DataType::Utf8, false),
        Field::new("serialization_version", DataType::Utf8, false),
        Field::new("variant", DataType::Utf8, false),
        Field::new("yes_pressure_5s", DataType::Float64, true),
        Field::new("no_pressure_5s", DataType::Float64, true),
        Field::new("move_persists", DataType::Float64, true),
        Field::new("underreact_up", DataType::Float64, true),
        Field::new("underreact_down", DataType::Float64, true),
        Field::new("repricing_ticks", DataType::Utf8, true),
        Field::new("repricing_confidence", DataType::Float64, true),
        Field::new("fill_before_decay", DataType::Float64, true),
        Field::new("fill_toxic", DataType::Float64, true),
        Field::new("observed_latency_ms", DataType::UInt64, false),
        Field::new("tokens_in", DataType::UInt64, false),
        Field::new("tokens_out", DataType::UInt64, false),
        Field::new("attempt_count", DataType::UInt32, false),
        Field::new("error", DataType::Utf8, true),
        Field::new("trigger", DataType::Utf8, false),
        Field::new("split", DataType::Utf8, false),
        Field::new("fidelity", DataType::Utf8, false),
        Field::new("live", DataType::Boolean, false),
    ]));
    let str_col =
        |f: fn(&EvalRow) -> String| StringArray::from(rows.iter().map(f).collect::<Vec<_>>());
    let opt_float = |f: fn(&JevEvaluation) -> f64| {
        Float64Array::from(
            rows.iter()
                .map(|row| row.signal.as_ref().map(f))
                .collect::<Vec<_>>(),
        )
    };
    let repricing = StringArray::from(
        rows.iter()
            .map(|row| {
                row.signal
                    .as_ref()
                    .and_then(|e| serde_json::to_string(&e.signal.repricing).ok())
            })
            .collect::<Vec<_>>(),
    );
    let values: Vec<Arc<dyn arrow::array::Array>> = vec![
        Arc::new(Int64Array::from(
            rows.iter().map(|row| row.timestamp).collect::<Vec<_>>(),
        )),
        Arc::new(str_col(|row| row.state_hash.clone())),
        Arc::new(str_col(|row| row.questions_hash.clone())),
        Arc::new(str_col(|row| row.pins.model_id.clone())),
        Arc::new(str_col(|row| row.pins.model_version.clone())),
        Arc::new(str_col(|row| row.pins.strategy_version.clone())),
        Arc::new(str_col(|row| row.pins.prompt_version.clone())),
        Arc::new(str_col(|row| row.pins.question_document_sha256.clone())),
        Arc::new(str_col(|row| row.pins.question_schema_version.clone())),
        Arc::new(str_col(|row| row.pins.feature_builder_version.clone())),
        Arc::new(str_col(|row| row.pins.normalization_version.clone())),
        Arc::new(str_col(|row| row.pins.serialization_version.clone())),
        Arc::new(str_col(|row| row.pins.variant.clone())),
        Arc::new(opt_float(|e| e.signal.yes_pressure_5s)),
        Arc::new(opt_float(|e| e.signal.no_pressure_5s)),
        Arc::new(opt_float(|e| e.signal.move_persists)),
        Arc::new(opt_float(|e| e.signal.underreact_up)),
        Arc::new(opt_float(|e| e.signal.underreact_down)),
        Arc::new(repricing),
        Arc::new(opt_float(|e| e.signal.repricing_confidence)),
        Arc::new(opt_float(|e| e.signal.fill_before_decay)),
        Arc::new(opt_float(|e| e.signal.fill_toxic)),
        Arc::new(UInt64Array::from(
            rows.iter()
                .map(|row| row.observed_latency_ms)
                .collect::<Vec<_>>(),
        )),
        Arc::new(UInt64Array::from(
            rows.iter().map(|row| row.tokens_in).collect::<Vec<_>>(),
        )),
        Arc::new(UInt64Array::from(
            rows.iter().map(|row| row.tokens_out).collect::<Vec<_>>(),
        )),
        Arc::new(UInt32Array::from(
            rows.iter().map(|row| row.attempt_count).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter().map(|row| row.error.clone()).collect::<Vec<_>>(),
        )),
        Arc::new(str_col(|row| row.trigger.clone())),
        Arc::new(str_col(|row| row.split.clone())),
        Arc::new(str_col(|row| row.fidelity.clone())),
        Arc::new(BooleanArray::from(
            rows.iter().map(|row| row.live).collect::<Vec<_>>(),
        )),
    ];
    let batch = RecordBatch::try_new(schema.clone(), values)?;
    let mut writer = ArrowWriter::try_new(File::create(path)?, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

fn parse_args() -> Result<Args, BoxError> {
    let args: Vec<String> = std::env::args().collect();
    let value =
        |name: &str, default: &str| arg_value(&args, name).unwrap_or_else(|| default.to_owned());
    let exact_raw = value("--exact-only", "true");
    let requested_per_condition: usize = value("--per-condition-signals", "8").parse()?;
    let per_condition = requested_per_condition.min(MAX_PER_CONDITION_SIGNALS);
    if requested_per_condition > MAX_PER_CONDITION_SIGNALS {
        tracing::warn!(
            requested = requested_per_condition,
            clamped = MAX_PER_CONDITION_SIGNALS,
            "clamping --per-condition-signals to the preregistered maximum"
        );
    }
    let live = args.iter().any(|arg| arg == "--live");
    let end_boundary = match arg_value(&args, "--end-boundary") {
        Some(raw) => Some(EndBoundary {
            timestamp_ms: parse_end_boundary(&raw)?,
            raw,
        }),
        None => None,
    };
    if live && end_boundary.is_none() {
        return Err("--live requires an exclusive UTC --end-boundary (RFC3339)".into());
    }
    Ok(Args {
        underlying: value("--underlying", DEFAULT_UNDERLYING),
        tape: arg_value(&args, "--tape"),
        out: PathBuf::from(value("--out", DEFAULT_OUT)),
        run_id: value("--run-id", DEFAULT_RUN_ID),
        per_condition,
        max_live_calls: value("--max-live-calls", "0").parse()?,
        max_states: value("--max-states", DEFAULT_MAX_STATES).parse()?,
        exact_only: !matches!(exact_raw.to_ascii_lowercase().as_str(), "false" | "0"),
        live,
        end_boundary,
    })
}

fn parse_end_boundary(raw: &str) -> Result<i64, BoxError> {
    let parsed = chrono::DateTime::parse_from_rfc3339(raw)
        .map_err(|error| format!("--end-boundary must be RFC3339 UTC: {error}"))?;
    if parsed.offset().local_minus_utc() != 0 {
        return Err("--end-boundary must use UTC (Z or +00:00)".into());
    }
    Ok(parsed.timestamp_millis())
}

fn arg_value(args: &[String], name: &str) -> Option<String> {
    args.iter().enumerate().find_map(|(index, arg)| {
        arg.strip_prefix(&format!("{name}="))
            .map(str::to_owned)
            .or_else(|| {
                (arg == name)
                    .then(|| args.get(index + 1).cloned())
                    .flatten()
            })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cache_entry(live: bool) -> CachedJev {
        CachedJev {
            envelope_json: String::new(),
            latency_ms: 0,
            live,
            request_json: String::new(),
            parsed_output_json: String::new(),
            jev_start_ts_ms: 0,
        }
    }

    #[test]
    fn stub_and_live_cache_entries_are_invisible_across_modes() {
        let stub = cache_entry(false);
        let live = cache_entry(true);
        assert!(cache_entry_matches_mode(&stub, CacheMode::Stub));
        assert!(!cache_entry_matches_mode(&stub, CacheMode::Live));
        assert!(cache_entry_matches_mode(&live, CacheMode::Live));
        assert!(!cache_entry_matches_mode(&live, CacheMode::Stub));
    }

    #[test]
    fn flushed_precompute_key_round_trips_as_a_resume_hit() {
        let dir = std::env::temp_dir().join(format!(
            "jevtrader-precompute-cache-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock is after the Unix epoch")
                .as_nanos()
        ));
        let pins = VersionPins::current_v1();
        let key = JevCacheKey::new_precompute(
            "state-hash".to_owned(),
            "questions-hash".to_owned(),
            &pins,
        );
        let mut persisted = JevCache::new();
        persisted.put_precompute(
            key.clone(),
            cache_entry(true),
            CacheEntryMetadata {
                attempt_count: 1,
                final_error: None,
                observed_latency_ms: 7,
                attempt_durations_ms: vec![7],
                version: pins,
            },
        );
        assert_eq!(persisted.save_to_dir(&dir).expect("save cache"), 1);

        let mut resumed = JevCache::new();
        assert_eq!(resumed.load_from_dir(&dir).expect("load cache"), 1);
        assert!(resumed.get(&key).is_some());
        assert_eq!(resumed.metadata(&key).expect("metadata").attempt_count, 1);
        std::fs::remove_dir_all(dir).expect("remove test cache");
    }
}
