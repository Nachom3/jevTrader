//! Historical backtest binary: deterministic bootstrap smoke + Parquet corpus.
//!
//! Reads `research-data/processed/` when present, otherwise builds a
//! deterministic synthetic bootstrap (seeded, multi-asset/horizon/regime).
//! Same strategy code as live; stub Jev by default (no cost, no network).
//! Every row carries run_id, pair_id, variant, source=HISTORICAL.

use jevtrader::replay::SyntheticItem;
use jevtrader::replay::resolution::resolve_market_split;
use jevtrader::replay::source::{
    ChunkEventSource, read_market_metas, read_regimes, read_resolution_specs_end, read_resolutions,
};
use jevtrader::replay::types::{FillProfile, LatencyDistribution, LatencyProfile};
use jevtrader::replay::{
    ARMS, Arm, CampaignConfig, Fidelity, HistoricalEvent, JevEvaluator, Provenance, RealJev,
    ReplayConfig, ReplayRunner, ResolutionOutcome, ResolutionSpec, RunnerOutput, Split, StubJev,
    TapeByCondition, V3_ARMS, build_report, current_crypto_regime, read_underlying_window,
    run_episode_campaign, write_json, write_markdown, zero_regime,
};
use std::collections::HashMap;
use std::fs;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis())
}

fn parse_arg(args: &[String], name: &str, default: &str) -> String {
    let mut out = default.to_owned();
    let mut i = 0;
    while i < args.len() {
        if args[i] == name && i + 1 < args.len() {
            out = args[i + 1].clone();
        }
        i += 1;
    }
    out
}

fn parse_fill(s: &str) -> FillProfile {
    match s.to_ascii_uppercase().as_str() {
        "OPTIMISTIC" => FillProfile::Optimistic,
        "BASE" => FillProfile::Base,
        _ => FillProfile::Conservative,
    }
}

fn parse_latency(s: &str) -> LatencyProfile {
    match s.to_ascii_uppercase().as_str() {
        "FAST" => LatencyProfile::Fast,
        "SLOW" => LatencyProfile::Slow,
        "EMPIRICAL" => LatencyProfile::Empirical,
        _ => LatencyProfile::Base,
    }
}

/// Loads a versioned latency distribution for Empirical replays.
///
/// Falls back to the built-in pilot placeholder when the file is absent or
/// invalid, and says so loudly: replaying on the placeholder is documented,
/// never silent.
fn load_latency_distribution(path: &str) -> LatencyDistribution {
    match fs::read_to_string(path) {
        Ok(json) => match LatencyDistribution::from_json(&json) {
            Ok(dist) => {
                println!(
                    "latency_distribution={path} samples={}",
                    dist.samples_ms.len()
                );
                dist
            }
            Err(err) => {
                println!("latency_distribution={path} INVALID ({err}); using pilot placeholder");
                LatencyDistribution::default()
            }
        },
        Err(_) => {
            println!("latency_distribution={path} MISSING; using pilot placeholder [307,1019]ms");
            LatencyDistribution::default()
        }
    }
}

/// Deterministic multi-bucket bootstrap: BTC/ETH x 5m/15m/1h/4h x regimes.
fn bootstrap_items(n_pairs: usize) -> Vec<SyntheticItem> {
    let assets = ["BTC", "ETH"];
    let horizons = ["5m", "15m", "1h", "4h"];
    let regimes = [
        "LOW_VOL-SIDEWAYS",
        "NORMAL_VOL-STRONG_UP",
        "HIGH_VOL-STRONG_DOWN",
    ];
    let splits = [Split::Exploration, Split::Validation, Split::OutOfSample];
    let mut items = Vec::new();
    let mut t = 1_700_000_000_000i64;
    let mut i = 0usize;
    // Simple deterministic PRNG (xorshift) for reproducibility.
    let mut rng = 0x9E3779B97F4A7C15u64;
    let mut next = move || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng
    };
    while items.len() < n_pairs * 4 {
        let ai = i % 2;
        let hi = (i / 2) % 4;
        let ri = (i / 8) % 3;
        let si = (i / 24) % 3;
        let asset = assets[ai].to_owned();
        let horizon = horizons[hi].to_owned();
        let regime = regimes[ri].to_owned();
        let split = splits[si];
        let base_spot = if ai == 0 { 100_000.0 } else { 5_000.0 };
        // Regime-driven drift + vol.
        let drift = match ri {
            1 => 0.0004,
            2 => -0.0005,
            _ => 0.0,
        };
        let vol = match ri {
            0 => 0.0002,
            1 => 0.0008,
            _ => 0.002,
        };
        let r = (next() % 10_000) as f64 / 10_000.0 - 0.5;
        let spot = base_spot * (1.0 + drift + r * vol * 2.0);
        // Poly mid oscillates around 0.5 with regime noise.
        let r2 = (next() % 10_000) as f64 / 10_000.0 - 0.5;
        let mid = (0.5 + drift * 10.0 + r2 * vol * 20.0).clamp(0.05, 0.95);
        let bid = (mid - 0.01).max(0.01);
        let ask = (mid + 0.01).min(0.99);
        let market_id = format!("{asset}-{horizon}");
        let fidelity = if hi < 2 {
            Fidelity::Exact
        } else {
            Fidelity::Proxy
        };
        items.push(SyntheticItem {
            ts_ms: t,
            book_bid: bid,
            book_ask: ask,
            spot,
            perp: spot,
            spot_flow: SyntheticItem::neutral_flow(),
            poly_flow: SyntheticItem::neutral_flow(),
            market_id,
            asset,
            horizon,
            split,
            fidelity,
            regime,
        });
        t += 5_000;
        i += 1;
    }
    items
}

/// Replays the real processed corpus, one condition at a time.
///
/// Tape trades group into per-condition Poly streams; underlying ticks group
/// per asset. Each condition replays with its own resolution horizon and
/// regime; rows accumulate into one output sharing the runner Jev cache.
/// Returns `None` when the corpus is absent so the caller falls back to the
/// synthetic bootstrap. Underlying windows that do not intersect the tape
/// window fall back to a documented constant spot (coverage gap, not a fill).
fn run_corpus<E: JevEvaluator>(
    runner: &mut ReplayRunner<E>,
    corpus: &str,
    config: &ReplayConfig,
    exact_only: bool,
    per_condition_cap: usize,
    condition_filter: &[String],
    stratified: bool,
) -> Option<RunnerOutput> {
    let metas = read_market_metas(corpus);
    if metas.is_empty() {
        return None;
    }
    // SIGNAL ALPHA primary evidence filters to EXACT resolution specs.
    // Smoke runs keep the inclusive default.
    let metas: Vec<_> = if exact_only {
        let exact: Vec<_> = metas
            .into_iter()
            .filter(|m| m.fidelity == Fidelity::Exact)
            .collect();
        if exact.is_empty() {
            println!("exact_only=1 but no EXACT markets in corpus; aborting (no PROXY fallback)");
            return None;
        }
        println!("exact_only=1 markets={}", exact.len());
        exact
    } else {
        metas
    };
    if metas.is_empty() {
        return None;
    }

    // Up/down markets are binary YES/NO markets: Up means YES won, while
    // Down means NO won. Unknown labels are skipped rather than inferred.
    let resolution_specs_end = read_resolution_specs_end(corpus);
    let resolution_records = read_resolutions(corpus);
    let meta_by_resolution: HashMap<String, _> = metas
        .iter()
        .map(|meta| (meta.condition_id.clone(), meta))
        .collect();
    let mut resolutions_map = HashMap::new();
    let mut skipped_no_resolution = 0usize;
    let mut exact_time = 0usize;
    let mut proxy_time = 0usize;
    let mut seen_resolution_conditions = std::collections::HashSet::new();
    for record in resolution_records {
        if !seen_resolution_conditions.insert(record.condition_id.clone()) {
            continue;
        }
        let Some(meta) = meta_by_resolution.get(&record.condition_id) else {
            skipped_no_resolution += 1;
            continue;
        };
        if !record.status.eq_ignore_ascii_case("resolved") {
            skipped_no_resolution += 1;
            continue;
        }
        let outcome = match record.winning_outcome.trim().to_ascii_lowercase().as_str() {
            "up" | "yes" => Some(ResolutionOutcome::Yes),
            "down" | "no" => Some(ResolutionOutcome::No),
            _ => None,
        };
        let Some(outcome) = outcome else {
            skipped_no_resolution += 1;
            continue;
        };
        let (resolution_at_ms, time_provenance) =
            if let Some(resolved_at_ms) = record.resolved_ts_ms {
                (resolved_at_ms, Provenance::Exact)
            } else if let Some((end_at_ms, _)) = resolution_specs_end.get(&record.condition_id) {
                (*end_at_ms, Provenance::Proxy)
            } else {
                skipped_no_resolution += 1;
                continue;
            };
        let fidelity = resolution_specs_end
            .get(&record.condition_id)
            .map_or(meta.fidelity, |(_, fidelity)| *fidelity);
        let spec = ResolutionSpec {
            condition_id: record.condition_id.clone(),
            market_id: meta.market_id.clone(),
            asset: meta.asset.clone(),
            horizon: meta.horizon.clone(),
            resolution_source: String::new(),
            resolution_rule_excerpt: meta.resolution_rules.clone(),
            fidelity,
            resolution_at_ms,
            reference: None,
            strike: None,
            start_at: None,
            end_at: None,
        };
        match resolve_market_split(
            &spec,
            Some(outcome),
            Provenance::Exact,
            time_provenance,
            false,
            true,
        ) {
            Ok(resolved) => {
                match resolved.time_provenance {
                    Provenance::Exact => exact_time += 1,
                    Provenance::Proxy => proxy_time += 1,
                }
                resolutions_map.insert(record.condition_id, resolved);
            }
            Err(_) => {
                skipped_no_resolution += 1;
            }
        }
    }
    println!(
        "resolutions_mapped={} skipped={} exact_time={} proxy_time={}",
        resolutions_map.len(),
        skipped_no_resolution,
        exact_time,
        proxy_time
    );

    let regimes = read_regimes(corpus);
    let tape = ChunkEventSource::new(vec![format!("{corpus}/polymarket_trades.parquet")], 8192)
        .read_parquet_chunks(1_000_000)
        .unwrap_or_default();
    if tape.is_empty() {
        return None;
    }
    let underlying_path = format!("{corpus}/underlying_all.parquet");
    // Group tape per condition. Underlying (spot + perp legs) is read PER
    // CONDITION below with read_underlying_window (time-bounded, thinned):
    // the merged multi-week file holds ~100M rows, so a global head-read
    // would price April markets with December ticks and blow RAM.
    let poly_by_condition = group_poly_by_condition(tape);
    let meta_by_condition: HashMap<String, (String, String, String, Split, Fidelity, String)> =
        metas
            .iter()
            .map(|m| {
                let slug = if m.slug.is_empty() {
                    m.condition_id.clone()
                } else {
                    m.slug.clone()
                };
                (
                    m.condition_id.clone(),
                    (
                        slug,
                        m.asset.clone(),
                        m.horizon.clone(),
                        m.split,
                        m.fidelity,
                        regime_at(&regimes, &m.asset, 0),
                    ),
                )
            })
            .collect();
    // Real per-market questions for the Jev state. Empty on-chain rules are
    // passed through as an explicit caveat, never invented.
    let questions: HashMap<String, (String, String)> = metas
        .iter()
        .map(|m| {
            let slug = if m.slug.is_empty() {
                m.condition_id.clone()
            } else {
                m.slug.clone()
            };
            let question = if m.question.is_empty() {
                format!("SYNTHETIC {}: question unavailable", slug)
            } else {
                m.question.clone()
            };
            let rules = if m.resolution_rules.is_empty() {
                "no on-chain resolution rules in the SII snapshot (fidelity UNKNOWN; see manifest)"
                    .to_owned()
            } else {
                m.resolution_rules.clone()
            };
            (slug, (question, rules))
        })
        .collect();
    // Bucket-interleaved condition order (asset x horizon round-robin)
    // so a capped budget still covers every bucket with full per-market
    // trajectories (market isolation: one run per condition).
    let mut buckets: HashMap<String, Vec<String>> = HashMap::new();
    for c in poly_by_condition.keys() {
        let key = meta_by_condition
            .get(c)
            .map(|m| format!("{}-{}", m.1, m.2))
            .unwrap_or_else(|| "UNKNOWN".to_owned());
        buckets.entry(key).or_default().push(c.clone());
    }
    for v in buckets.values_mut() {
        v.sort_by_key(|c| {
            poly_by_condition
                .get(c)
                .and_then(|v| v.first().map(HistoricalEvent::ts_ms))
                .unwrap_or(i64::MAX)
        });
    }
    let mut bucket_names: Vec<String> = buckets.keys().cloned().collect();
    bucket_names.sort();
    let mut conditions = Vec::new();
    let mut round = 0usize;
    loop {
        let mut progressed = false;
        for b in &bucket_names {
            if let Some(c) = buckets.get(b).and_then(|v| v.get(round).cloned()) {
                conditions.push(c);
                progressed = true;
            }
        }
        if !progressed {
            break;
        }
        round += 1;
    }
    // Diagnostic runs restrict to a pre-registered condition list (no
    // cherry-picking at runtime). Empty filter replays everything.
    if !condition_filter.is_empty() {
        let before = conditions.len();
        conditions.retain(|c| condition_filter.contains(c));
        for wanted in condition_filter {
            if !conditions.contains(wanted) {
                println!("conditions={wanted} NOT IN TAPE (skipped)");
            }
        }
        println!(
            "condition_filter listed={} in_tape={} (of {before} interleaved)",
            condition_filter.len(),
            conditions.len()
        );
    }
    let mut all_rows = Vec::new();
    let mut all_signals = Vec::new();
    let mut skipped_no_meta = 0usize;
    let (mut jev_hits, mut jev_misses, mut stale, mut incomplete, mut jerrs) =
        (0, 0, 0usize, 0usize, 0usize);
    for condition in conditions {
        // Per-condition pair cap: spreads the budget across buckets so one
        // long trajectory cannot consume the whole run. The SIGNAL ALPHA run
        // raises the cap (pre-registered) to reach 500-1000 pairs.
        let remaining = config
            .max_pairs
            .saturating_sub(all_rows.len() / config.arms.len().max(1));
        if remaining == 0 {
            break;
        }
        runner.config.max_pairs = remaining.clamp(1, per_condition_cap.max(1));
        // Pair namespace = condition: pair_ids stay globally unique across
        // the per-condition replay calls (methodology joins on run_id+pair_id).
        runner.config.pair_namespace = condition.clone();
        let Some(poly) = poly_by_condition.get(&condition) else {
            continue;
        };
        // Never replay a condition without its own metadata (e.g. non-EXACT
        // conditions under --exact-only): borrowing another market's tuple
        // or a default would contaminate the evidence. Counted, not run.
        let Some(meta) = meta_by_condition.get(&condition).cloned() else {
            skipped_no_meta += 1;
            continue;
        };
        // Regime at this condition's first trade (asset-aware, backward-only).
        let first_ts = poly.first().map(HistoricalEvent::ts_ms).unwrap_or(0);
        let regime = regime_at(&regimes, &meta.1, first_ts);
        let mut single_meta = meta.clone();
        single_meta.5 = regime;
        let mut single_map = HashMap::new();
        single_map.insert(condition.clone(), single_meta);
        // Full per-market trajectory (capped): fills and markouts need
        // real future prints, not a 4-item window. Stratified diagnostic
        // runs spread evals evenly across the trajectory (stride =
        // len/cap) so sampled states vary in time-remaining, moves, and
        // regime; default replays the trajectory head (legacy behavior).
        let stride = if stratified {
            (poly.len() / per_condition_cap.max(1)).max(1)
        } else {
            1
        };
        let poly_stream: Vec<HistoricalEvent> =
            poly.iter().step_by(stride).take(500).cloned().collect();
        // Dense label trajectory: unstrided head (take 500) so fills,
        // markouts, and drift sample real future prints even when the
        // evaluated stream is stratified. Legacy runs (stride 1) are
        // unaffected: identical series.
        let label_mids: Vec<(i64, f64)> = poly
            .iter()
            .take(500)
            .filter_map(|ev| match ev {
                HistoricalEvent::PolyTrade { ts_ms, price, .. } => Some((*ts_ms, *price)),
                HistoricalEvent::PolyTop {
                    ts_ms,
                    best_bid,
                    best_ask,
                    ..
                } => Some((*ts_ms, (best_bid + best_ask) / 2.0)),
                _ => None,
            })
            .collect();
        // Windowed underlying for THIS condition only: [first_poly - 2h,
        // last_poly], thinned to ~1 tick/s (thin 16) and capped. No
        // look-ahead (upper bound) and no cross-week bleed (lower bound).
        let last_ts = poly_stream
            .last()
            .map(HistoricalEvent::ts_ms)
            .unwrap_or(first_ts);
        let mut und_stream: Vec<HistoricalEvent> = read_underlying_window(
            &underlying_path,
            &meta.1,
            first_ts.saturating_sub(2 * 3_600_000),
            last_ts,
            16,
            50_000,
        )
        .unwrap_or_default();
        // Perp leg for the MICRO arm (same window, thinned identically).
        // Missing perp coverage yields nothing; items then fall back to
        // perp = spot (V1 behavior), never invented prices.
        let perp_asset = format!("{}-PERP", meta.1);
        und_stream.extend(
            read_underlying_window(
                &underlying_path,
                &perp_asset,
                first_ts.saturating_sub(2 * 3_600_000),
                last_ts,
                16,
                50_000,
            )
            .unwrap_or_default(),
        );
        let out = runner.run_events_by_condition_resolved(
            vec![poly_stream, und_stream],
            &[single_map[&condition].clone()],
            &single_map,
            &questions,
            Some(&label_mids),
            &resolutions_map,
        );
        jev_hits = out.jev_hits;
        jev_misses = out.jev_misses;
        stale += out.stale_skips;
        incomplete += out.incomplete_pairs;
        jerrs += out.jev_errors;
        all_rows.extend(out.rows);
        all_signals.extend(out.signals);
    }
    println!("skipped_no_meta={skipped_no_meta}");
    Some(jevtrader::replay::RunnerOutput {
        rows: all_rows,
        signals: all_signals,
        jev_hits,
        jev_misses,
        stale_skips: stale,
        incomplete_pairs: incomplete,
        jev_errors: jerrs,
    })
}

/// Asset-aware backward-only regime lookup: newest regime day at or before
/// `ts_ms` for `asset` (`BTC` matches `BTCUSDT`). Falls back to UNKNOWN.
type CampaignInputs = (
    TapeByCondition,
    HashMap<String, jevtrader::replay::ResolvedMarket>,
    HashMap<String, (String, String)>,
);

fn load_campaign_inputs(
    corpus: &str,
    exact_only: bool,
    condition_filter: &[String],
) -> Option<CampaignInputs> {
    let metas = read_market_metas(corpus);
    if metas.is_empty() {
        return None;
    }
    let metas: Vec<_> = if exact_only {
        metas
            .into_iter()
            .filter(|meta| meta.fidelity == Fidelity::Exact)
            .collect()
    } else {
        metas
    };
    let specs = read_resolution_specs_end(corpus);
    let records = read_resolutions(corpus);
    let mut resolutions = HashMap::new();
    let mut questions = HashMap::new();
    let mut allowed_conditions = std::collections::HashSet::new();
    for meta in &metas {
        if !condition_filter.is_empty() && !condition_filter.contains(&meta.condition_id) {
            continue;
        }
        allowed_conditions.insert(meta.condition_id.clone());
        questions.insert(
            meta.market_id.clone(),
            (meta.question.clone(), meta.resolution_rules.clone()),
        );
        let Some(record) = records
            .iter()
            .find(|record| record.condition_id == meta.condition_id)
        else {
            continue;
        };
        if !record.status.eq_ignore_ascii_case("resolved") {
            continue;
        }
        let outcome = match record.winning_outcome.trim().to_ascii_lowercase().as_str() {
            "up" | "yes" => ResolutionOutcome::Yes,
            "down" | "no" => ResolutionOutcome::No,
            _ => continue,
        };
        let (resolution_at_ms, time_provenance) = if let Some(ts_ms) = record.resolved_ts_ms {
            (ts_ms, Provenance::Exact)
        } else if let Some((ts_ms, _)) = specs.get(&meta.condition_id) {
            (*ts_ms, Provenance::Proxy)
        } else {
            continue;
        };
        let fidelity = specs
            .get(&meta.condition_id)
            .map_or(meta.fidelity, |(_, fidelity)| *fidelity);
        let spec = ResolutionSpec {
            condition_id: meta.condition_id.clone(),
            market_id: meta.market_id.clone(),
            asset: meta.asset.clone(),
            horizon: meta.horizon.clone(),
            resolution_source: String::new(),
            resolution_rule_excerpt: meta.resolution_rules.clone(),
            fidelity,
            resolution_at_ms,
            reference: None,
            strike: None,
            start_at: None,
            end_at: None,
        };
        let Ok(resolved) = resolve_market_split(
            &spec,
            Some(outcome),
            Provenance::Exact,
            time_provenance,
            false,
            true,
        ) else {
            continue;
        };
        resolutions.insert(meta.condition_id.clone(), resolved);
    }
    let tape = ChunkEventSource::new(vec![format!("{corpus}/polymarket_trades.parquet")], 8192)
        .read_parquet_chunks(1_000_000)
        .ok()?;
    let mut tape_by_condition: TapeByCondition = group_poly_by_condition(tape)
        .into_iter()
        .filter(|(condition_id, _)| allowed_conditions.contains(condition_id))
        .collect();
    if tape_by_condition.is_empty() {
        return None;
    }

    // Read only the bounded per-condition windows from the merged underlying
    // source. The historical campaign never loads that file head-to-tail.
    let underlying_path = format!("{corpus}/underlying_all.parquet");
    for meta in &metas {
        let Some(events) = tape_by_condition.get_mut(&meta.condition_id) else {
            continue;
        };
        let Some(first_ts) = events.iter().map(HistoricalEvent::ts_ms).min() else {
            continue;
        };
        let Some(last_ts) = events.iter().map(HistoricalEvent::ts_ms).max() else {
            continue;
        };
        let lo_ms = first_ts.saturating_sub(2 * 3_600_000);
        if let Ok(mut rows) =
            read_underlying_window(&underlying_path, &meta.asset, lo_ms, last_ts, 16, 50_000)
        {
            events.append(&mut rows);
        }
        let perp_asset = format!("{}-PERP", meta.asset);
        if let Ok(mut rows) =
            read_underlying_window(&underlying_path, &perp_asset, lo_ms, last_ts, 16, 50_000)
        {
            events.append(&mut rows);
        }
    }
    Some((tape_by_condition, resolutions, questions))
}

fn group_poly_by_condition(
    events: impl IntoIterator<Item = HistoricalEvent>,
) -> HashMap<String, Vec<HistoricalEvent>> {
    let mut grouped = HashMap::new();
    for event in events {
        if let HistoricalEvent::PolyTrade { condition_id, .. } = &event {
            grouped
                .entry(condition_id.clone())
                .or_insert_with(Vec::new)
                .push(event);
        }
    }
    grouped
}

fn run_campaign(
    config: &CampaignConfig,
    corpus: &str,
    exact_only: bool,
    condition_filter: &[String],
) -> Result<jevtrader::replay::CampaignOutput, String> {
    let inputs_started = Instant::now();
    let inputs = load_campaign_inputs(corpus, exact_only, condition_filter);
    let Some((events, resolutions, questions)) = inputs else {
        eprintln!(
            "ts_ms={} stage=inputs_done conditions=0 resolutions=0 ms={}",
            unix_ms(),
            inputs_started.elapsed().as_millis(),
        );
        return Ok(jevtrader::replay::CampaignOutput {
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
        });
    };
    eprintln!(
        "ts_ms={} stage=inputs_done conditions={} resolutions={} ms={}",
        unix_ms(),
        events.len(),
        resolutions.len(),
        inputs_started.elapsed().as_millis(),
    );
    run_episode_campaign(config, &events, &resolutions, &questions)
}

fn regime_at(regimes: &[(i64, String, String)], asset: &str, ts_ms: i64) -> String {
    let day = ts_ms / 1000 / 86400 * 86400;
    let mut best: Option<&String> = None;
    for (d, symbol, regime) in regimes {
        if *d <= day && symbol.starts_with(asset) {
            best = Some(regime);
        } else if *d > day {
            break;
        }
    }
    best.cloned()
        .unwrap_or_else(|| "UNKNOWN-UNKNOWN".to_owned())
}

/// Runs corpus replay (or synthetic fallback) with any evaluator.
fn execute<E: JevEvaluator>(
    runner: &mut ReplayRunner<E>,
    corpus: &str,
    config: &ReplayConfig,
    exact_only: bool,
    per_condition_cap: usize,
    condition_filter: &[String],
    stratified: bool,
) -> RunnerOutput {
    match run_corpus(
        runner,
        corpus,
        config,
        exact_only,
        per_condition_cap,
        condition_filter,
        stratified,
    ) {
        Some(output) => {
            println!("corpus=HISTORICAL dir={corpus}");
            output
        }
        None => {
            println!("corpus=SYNTHETIC-BOOTSTRAP (processed corpus absent)");
            let items = bootstrap_items(config.max_pairs);
            let resolution_at_ms = items.last().map_or(0, |i| i.ts_ms) + 4 * 3_600_000;
            runner.run_synthetic(&items, resolution_at_ms)
        }
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let max_pairs: usize = parse_arg(&args, "--max-pairs", "100")
        .parse()
        .unwrap_or(100);
    let fill = parse_fill(&parse_arg(&args, "--fill-model", "CONSERVATIVE"));
    let latency = parse_latency(&parse_arg(&args, "--latency", "BASE"));
    let out = parse_arg(&args, "--out", "research-data/reports/smoke_pairs.json");
    let run_id = parse_arg(&args, "--run-id", "smoke-bootstrap");

    let mut config = ReplayConfig::smoke(&run_id);
    config.max_pairs = max_pairs.min(10_000);
    config.fill = fill;
    config.latency = latency;
    if latency == LatencyProfile::Empirical {
        let samples_path = parse_arg(
            &args,
            "--latency-samples",
            "research-data/reports/jev_latency_samples.json",
        );
        config.latency_distribution = load_latency_distribution(&samples_path);
    }

    let corpus = parse_arg(&args, "--corpus", "research-data/processed");
    // SIGNAL ALPHA run knobs (run config, never thresholds): --exact-only 1
    // restricts to EXACT resolution specs; --per-condition-cap raises pairs
    // per market to reach the pre-registered N_complete_pairs.
    let exact_only = parse_arg(&args, "--exact-only", "0") == "1";
    let per_condition_cap: usize = parse_arg(&args, "--per-condition-cap", "4")
        .parse()
        .unwrap_or(4)
        .clamp(1, 100);
    // Diagnostic runs: comma-separated condition_ids ("" = all) and
    // stratified state sampling across each trajectory (--stride 1).
    let conditions_arg = parse_arg(&args, "--conditions", "");
    let condition_filter: Vec<String> = if conditions_arg.trim().is_empty() {
        Vec::new()
    } else {
        conditions_arg
            .split(',')
            .map(|s| s.trim().to_owned())
            .filter(|s| !s.is_empty())
            .collect()
    };
    let stratified = parse_arg(&args, "--stride", "0") == "1";
    // Question-form experiments: "v3" evaluates CONTROL (V1 questions) +
    // FAIR_VALUE + PRESSURE_COMPOSITE on the same MICRO state.
    // Default keeps the V1/V2 state-variant trio.
    let arms = match parse_arg(&args, "--arms", "v1micro").as_str() {
        "v3" => V3_ARMS.to_vec(),
        _ => ARMS.to_vec(),
    };
    config.arms = arms;
    let real_jev = parse_arg(&args, "--real-jev", "0") == "1";
    let max_jev_calls: u64 = parse_arg(&args, "--max-jev-calls", "20")
        .parse()
        .unwrap_or(20);
    // Hard spend guard: live calls never exceed this in one run.
    let max_jev_calls = max_jev_calls.clamp(1, 10_000);

    if parse_arg(&args, "--episode-campaign", "0") == "1" {
        let campaign_max_calls: usize = parse_arg(&args, "--max-jev-calls", "20")
            .parse()
            .unwrap_or(20)
            .min(10_000);
        let deadline_ms: u64 = parse_arg(
            &args,
            "--jev-deadline-ms",
            &std::env::var("JEV_DEADLINE_MS").unwrap_or_else(|_| "1500".to_owned()),
        )
        .parse()
        .unwrap_or(1500);
        let per_condition_signals: usize = parse_arg(&args, "--per-condition-signals", "4")
            .parse()
            .unwrap_or(4)
            .min(1_000);
        let cache_dir = parse_arg(
            &args,
            "--cache-dir",
            &format!("research-data/cache/{run_id}"),
        );
        let campaign_config = CampaignConfig {
            run_id: run_id.clone(),
            arms: vec![
                Arm::QuantOnly,
                Arm::JevOnly,
                Arm::QuantPlusJev,
                Arm::MicroPlusRegime,
            ],
            fill_profile: fill,
            historical_regime: zero_regime(),
            current_regime: current_crypto_regime(),
            exit_policy: "HOLD".to_owned(),
            max_jev_calls: campaign_max_calls,
            jev_deadline_ms: deadline_ms,
            cache_dir: cache_dir.clone(),
            per_condition_signals,
        };
        println!("campaign_jev=LIVE");
        let campaign = run_campaign(&campaign_config, &corpus, exact_only, &condition_filter)
            .unwrap_or_else(|error| panic!("episode campaign failed: {error}"));
        eprintln!(
            "ts_ms={} stage=campaign_done episodes={} jev_calls={} hits={} misses={} skipped_total={}",
            unix_ms(),
            campaign.episodes.len(),
            campaign.jev_calls,
            campaign.jev_hits,
            campaign.jev_misses,
            campaign.skipped,
        );
        let episodes_json =
            serde_json::to_string_pretty(&campaign.episodes).expect("campaign episodes serialize");
        if let Some(parent) = std::path::Path::new(&out).parent() {
            let _ = fs::create_dir_all(parent);
        }
        fs::write(&out, episodes_json).expect("campaign episodes write");
        println!(
            "campaign_episodes={} jev_calls={} jev_hits={} jev_misses={} skipped_res={} skipped_data={} latencies_ms={} out={} cache_dir={}",
            campaign.episodes.len(),
            campaign.jev_calls,
            campaign.jev_hits,
            campaign.jev_misses,
            campaign.skipped_no_resolution,
            campaign.skipped_no_fill_data,
            campaign.latencies_ms.len(),
            out,
            cache_dir,
        );
        return;
    }

    let output = if real_jev {
        let api_key = std::env::var("TYPESAFE_API_KEY").unwrap_or_default();
        let deadline_ms: u64 = std::env::var("JEV_DEADLINE_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1500);
        let evaluator = RealJev::new(api_key, Duration::from_millis(deadline_ms), max_jev_calls)
            .expect("RealJev requires TYPESAFE_API_KEY and max-jev-calls >= 1");
        let mut runner = ReplayRunner::new(config.clone(), evaluator);
        let output = execute(
            &mut runner,
            &corpus,
            &config,
            exact_only,
            per_condition_cap,
            &condition_filter,
            stratified,
        );
        println!(
            "jev_live_calls={} budget={}",
            runner.evaluator.calls, max_jev_calls
        );
        // Persist measured live latencies as the empirical distribution for
        // future replays. Disabled by default; pass --latency-out to enable.
        let latency_out = parse_arg(&args, "--latency-out", "");
        if !latency_out.is_empty() {
            match LatencyDistribution::from_samples(runner.evaluator.latencies_ms.clone(), 42) {
                Ok(dist) => match serde_json::to_string_pretty(&dist) {
                    Ok(json) => {
                        if fs::write(&latency_out, &json).is_ok() {
                            println!(
                                "latency_observed samples={} -> {latency_out}",
                                dist.samples_ms.len()
                            );
                        } else {
                            println!("latency_observed WRITE FAILED {latency_out}");
                        }
                    }
                    Err(err) => println!("latency_observed SERIALIZE FAILED: {err}"),
                },
                Err(err) => println!("latency_observed NO SAMPLES ({err})"),
            }
        }
        output
    } else {
        let mut runner = ReplayRunner::new(config.clone(), StubJev::new(42));
        execute(
            &mut runner,
            &corpus,
            &config,
            exact_only,
            per_condition_cap,
            &condition_filter,
            stratified,
        )
    };

    let json = write_json(&output.rows).expect("json renders");
    if let Some(parent) = std::path::Path::new(&out).parent() {
        let _ = fs::create_dir_all(parent);
    }
    fs::write(&out, &json).expect("pairs json writes");
    // Diagnostic sidecar: verbatim (state, signal) per evaluation, no
    // thresholding or aggregation. Disabled by default.
    let signals_out = parse_arg(&args, "--signals-out", "");
    if !signals_out.is_empty() {
        match serde_json::to_string_pretty(&output.signals) {
            Ok(sjson) => {
                if fs::write(&signals_out, &sjson).is_ok() {
                    println!("signals_out n={} -> {signals_out}", output.signals.len());
                } else {
                    println!("signals_out WRITE FAILED {signals_out}");
                }
            }
            Err(err) => println!("signals_out SERIALIZE FAILED: {err}"),
        }
    }
    let summary = build_report(&output.rows);
    let md = write_markdown(&summary);
    let md_path = format!("{}.md", out.trim_end_matches(".json"));
    let _ = fs::write(&md_path, &md);

    // Console summary per arm (never global only). Signal drift (all
    // usable evaluations) is the alpha readout; markouts are
    // fill-conditional execution labels.
    let mut per_arm: Vec<(String, usize, Vec<f64>, Vec<f64>)> = config
        .arms
        .iter()
        .map(|a| (a.name.to_owned(), 0, Vec::new(), Vec::new()))
        .collect();
    for r in &output.rows {
        if let Some(entry) = per_arm
            .iter_mut()
            .find(|(name, _, _, _)| *name == r.variant)
        {
            entry.1 += 1;
            if let Some(m) = r.markout_5s_pp {
                entry.2.push(m);
            }
            if let Some(d) = r.drift_5s_pp {
                entry.3.push(d);
            }
        }
    }
    let mean = |v: &[f64]| {
        if v.is_empty() {
            0.0
        } else {
            v.iter().sum::<f64>() / v.len() as f64
        }
    };
    let hit = |v: &[f64]| {
        if v.is_empty() {
            0.0
        } else {
            v.iter().filter(|x| **x > 0.0).count() as f64 / v.len() as f64
        }
    };
    let arm_line = |entry: &(String, usize, Vec<f64>, Vec<f64>)| {
        format!(
            "{} n={} mean_mo5s={:.4} drift_n={} drift_mean={:.4} drift_hit={:.3}",
            entry.0,
            entry.1,
            mean(&entry.2),
            entry.3.len(),
            mean(&entry.3),
            hit(&entry.3),
        )
    };
    println!(
        "run={} rows={} {} | stale={} incomplete={} jev_hits={} jev_misses={} -> {}",
        run_id,
        output.rows.len(),
        per_arm.iter().map(arm_line).collect::<Vec<_>>().join(" | "),
        output.stale_skips,
        output.incomplete_pairs,
        output.jev_hits,
        output.jev_misses,
        out
    );
    if real_jev {
        println!(
            "NOTE: LIVE Jev judgments (budget-capped); tiny-n pilot, not alpha evidence. OOS untouched for tuning."
        );
    } else {
        println!("NOTE: stub-Jev smoke only; not alpha evidence. OOS untouched for tuning.");
    }
}
