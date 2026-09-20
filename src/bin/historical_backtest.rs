//! Historical backtest binary: deterministic bootstrap smoke + Parquet corpus.
//!
//! Reads `research-data/processed/` when present, otherwise builds a
//! deterministic synthetic bootstrap (seeded, multi-asset/horizon/regime).
//! Same strategy code as live; stub Jev by default (no cost, no network).
//! Every row carries run_id, pair_id, variant, source=HISTORICAL.

use jevtrader::replay::SyntheticItem;
use jevtrader::replay::source::{ChunkEventSource, read_market_metas, read_regimes};
use jevtrader::replay::types::{FillProfile, LatencyDistribution, LatencyProfile};
use jevtrader::replay::{
    Fidelity, HistoricalEvent, JevEvaluator, RealJev, ReplayConfig, ReplayRunner, RunnerOutput,
    Split, StubJev, build_report, read_underlying_window, write_json, write_markdown,
};
use std::collections::HashMap;
use std::fs;
use std::time::Duration;

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
        items.push((
            t, bid, ask, spot, market_id, asset, horizon, split, fidelity, regime,
        ));
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
    let regimes = read_regimes(corpus);
    let tape = ChunkEventSource::new(vec![format!("{corpus}/polymarket_trades.parquet")], 8192)
        .read_parquet_chunks(1_000_000)
        .unwrap_or_default();
    if tape.is_empty() {
        return None;
    }
    let underlying_path = format!("{corpus}/underlying_market_data.parquet");
    // Group tape per condition. Underlying is read PER CONDITION below with
    // read_underlying_window (time-bounded, thinned): the merged multi-week
    // file holds 40M rows, so a global head-read would price April markets
    // with December ticks and blow RAM.
    let mut poly_by_condition: HashMap<String, Vec<HistoricalEvent>> = HashMap::new();
    for ev in tape {
        if let HistoricalEvent::PolyTrade { condition_id, .. } = &ev {
            poly_by_condition
                .entry(condition_id.clone())
                .or_default()
                .push(ev);
        }
    }
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
        let remaining = config.max_pairs.saturating_sub(all_rows.len() / 2);
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
        // Windowed underlying for THIS condition only: [first_poly - 2h,
        // last_poly], thinned to ~1 tick/s (thin 16) and capped. No
        // look-ahead (upper bound) and no cross-week bleed (lower bound).
        let last_ts = poly_stream
            .last()
            .map(HistoricalEvent::ts_ms)
            .unwrap_or(first_ts);
        let und_stream: Vec<HistoricalEvent> = read_underlying_window(
            &underlying_path,
            &meta.1,
            first_ts.saturating_sub(2 * 3_600_000),
            last_ts,
            16,
            50_000,
        )
        .unwrap_or_default();
        let out = runner.run_events_by_condition(
            vec![poly_stream, und_stream],
            &[single_map[&condition].clone()],
            &single_map,
            &questions,
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
            let resolution_at_ms = items.last().map_or(0, |i| i.0) + 4 * 3_600_000;
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
    let real_jev = parse_arg(&args, "--real-jev", "0") == "1";
    let max_jev_calls: u64 = parse_arg(&args, "--max-jev-calls", "20")
        .parse()
        .unwrap_or(20);
    // Hard spend guard: live calls never exceed this in one run.
    let max_jev_calls = max_jev_calls.clamp(1, 10_000);
    let output = if real_jev {
        let api_key = std::env::var("TYPESAFE_API_KEY").unwrap_or_default();
        let deadline_ms: u64 = std::env::var("JEV_DEADLINE_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1500);
        let evaluator = RealJev::new(api_key, Duration::from_millis(deadline_ms), max_jev_calls)
            .expect("RealJev requires TYPESAFE_API_KEY and max-jev-calls >= 1");
        let mut runner = ReplayRunner::new(config.clone(), evaluator);
        let output = execute(&mut runner, &corpus, &config, exact_only, per_condition_cap, &condition_filter, stratified);
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
        execute(&mut runner, &corpus, &config, exact_only, per_condition_cap, &condition_filter, stratified)
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
                    println!(
                        "signals_out n={} -> {signals_out}",
                        output.signals.len()
                    );
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

    // Console summary segmented by variant/asset/horizon (never global only).
    // Signal drift (all usable evaluations) is the alpha readout; markouts
    // are fill-conditional execution labels.
    let mut n_c = 0;
    let mut n_q = 0;
    let mut mo_c = Vec::new();
    let mut mo_q = Vec::new();
    let mut drift_c = Vec::new();
    let mut drift_q = Vec::new();
    for r in &output.rows {
        if r.variant == "CONTROL" {
            n_c += 1;
            if let Some(m) = r.markout_5s_pp {
                mo_c.push(m);
            }
            if let Some(d) = r.drift_5s_pp {
                drift_c.push(d);
            }
        } else {
            n_q += 1;
            if let Some(m) = r.markout_5s_pp {
                mo_q.push(m);
            }
            if let Some(d) = r.drift_5s_pp {
                drift_q.push(d);
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
    println!(
        "run={} rows={} CONTROL n={} mean_mo5s={:.4} | QUANT_V1 n={} mean_mo5s={:.4} | stale={} incomplete={} jev_hits={} jev_misses={} -> {}",
        run_id,
        output.rows.len(),
        n_c,
        mean(&mo_c),
        n_q,
        mean(&mo_q),
        output.stale_skips,
        output.incomplete_pairs,
        output.jev_hits,
        output.jev_misses,
        out
    );
    println!(
        "signal_drift_5s CONTROL n={} mean={:.4} hit={:.3} | QUANT_V1 n={} mean={:.4} hit={:.3}",
        drift_c.len(),
        mean(&drift_c),
        hit(&drift_c),
        drift_q.len(),
        mean(&drift_q),
        hit(&drift_q),
    );
    if real_jev {
        println!(
            "NOTE: LIVE Jev judgments (budget-capped); tiny-n pilot, not alpha evidence. OOS untouched for tuning."
        );
    } else {
        println!("NOTE: stub-Jev smoke only; not alpha evidence. OOS untouched for tuning.");
    }
}
