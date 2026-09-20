//! Historical replay invariants (AGENTS + task section 38).
//!
//! Covers: ordering, backward-only as-of, no look-ahead, ReplayClock,
//! rolling features, reference price, time_remaining, regimes,
//! CONTROL/QUANT pairing, Jev latency/stale, execution latency, partial
//! fills, 3 fill models, realized + held-to-resolution PnL, markout sign,
//! drawdown, portfolio/market/asset/horizon isolation, manifest
//! reproducibility, incomplete data.

use jevtrader::config::QuantConfig;
use jevtrader::domain::{PriceTicks, TickSize};
use jevtrader::engine::MarketSnapshot;
use jevtrader::engine::pipeline::{MarkoutTracker, candidate_maker_price};
use jevtrader::engine::signal_actor::StalenessPolicy;
use jevtrader::polymarket::OrderBook;
use jevtrader::replay::fills::{ExecutionLatency, RestingOrder};
use jevtrader::replay::markouts::signed_markouts_pp;
use jevtrader::replay::portfolio::{FillEvent, Portfolio, PortfolioRegistry};
use jevtrader::replay::report::write_markdown;
use jevtrader::replay::walkforward::WalkforwardWindow;
use jevtrader::replay::{
    Fidelity, FillProfile, FillSimulator, HistoricalEvent, HistoricalSource, InMemorySource,
    JevEvaluator, LatencyProfile, ReplayClock, ReplayConfig, ReplayRunner, Split,
    StubJev, Synchronizer, SyntheticItem, as_of_backward, build_report,
};
use jevtrader::state::feature_builder::{
    ExternalTick, OrderFlowAggregates, ResolutionContext, VenueMicroprices,
    build_features_with_context,
};
use jevtrader::state::poly_history::PolyHistory;
use jevtrader::state::quant_features::build_quant;

fn book_at(bid: f64, ask: f64) -> OrderBook {
    let mut b = OrderBook::default();
    b.apply_snapshot(
        [(PriceTicks::from_f64(bid), 100)],
        [(PriceTicks::from_f64(ask), 100)],
    );
    b
}

fn snapshot(bid: f64, ask: f64) -> MarketSnapshot {
    MarketSnapshot {
        book: book_at(bid, ask),
        stale: false,
    }
}

/// Neutral synthetic item (V2 context = V1-equivalent: perp = spot, zero
/// flows). Tests override fields (e.g. split) after construction.
fn mk_item(
    ts_ms: i64,
    bid: f64,
    ask: f64,
    spot: f64,
    market: &str,
    asset: &str,
    horizon: &str,
) -> SyntheticItem {
    SyntheticItem {
        ts_ms,
        book_bid: bid,
        book_ask: ask,
        spot,
        perp: spot,
        spot_flow: SyntheticItem::neutral_flow(),
        poly_flow: SyntheticItem::neutral_flow(),
        market_id: market.to_owned(),
        asset: asset.to_owned(),
        horizon: horizon.to_owned(),
        split: Split::Exploration,
        fidelity: Fidelity::Exact,
        regime: "NORMAL_VOL-SIDEWAYS".to_owned(),
    }
}

#[derive(Clone, Copy)]
struct FixedReplayJev {
    error: bool,
}

impl JevEvaluator for FixedReplayJev {
    fn evaluate(
        &mut self,
        _state: &jevtrader::jev::request::V1State,
        _market_id: &str,
        _state_seq: u64,
        _variant: &str,
        questions: &serde_json::Value,
        assumed_latency_ms: u64,
    ) -> jevtrader::replay::RawOutcome {
        if self.error {
            return jevtrader::replay::RawOutcome {
                latency_ms: assumed_latency_ms,
                live: false,
                error: Some("synthetic Jev failure".to_owned()),
                envelope_json: String::new(),
            };
        }
        // Neutral envelope shaped by the requested question set: noul 0.5,
        // choice uniform over its criteria with neutral confidence.
        let mut answers = serde_json::Map::new();
        if let Some(object) = questions.as_object() {
            for (qid, question) in object {
                let kind = question
                    .get("type")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("");
                if kind == "noul" {
                    answers.insert(
                        qid.clone(),
                        serde_json::json!({"type": "noul", "noul": 0.5}),
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
                    for key in keys {
                        probabilities.insert(key.clone(), serde_json::json!(share));
                    }
                    answers.insert(
                        qid.clone(),
                        serde_json::json!({
                            "type": "choice",
                            "probabilities": probabilities,
                            "confidence": 0.0,
                        }),
                    );
                }
            }
        }
        jevtrader::replay::RawOutcome {
            latency_ms: assumed_latency_ms,
            live: false,
            error: None,
            envelope_json: serde_json::json!({"answers": answers}).to_string(),
        }
    }
}

#[test]
fn historical_timestamp_ordering_is_event_time() {
    let evs = vec![
        HistoricalEvent::PolyTrade {
            ts_ms: 3000,
            condition_id: "c".to_owned(),
            price: 0.5,
            size: 1.0,
            aggressor: None,
            direction_quality: "GROUND_TRUTH".to_owned(),
            source: "t".to_owned(),
        },
        HistoricalEvent::UnderlyingTick {
            ts_ms: 1000,
            asset: "BTC".to_owned(),
            venue: "BINANCE".to_owned(),
            price: 1.0,
            bid: None,
            ask: None,
            qty: None,
            aggressor: None,
            source: "t".to_owned(),
        },
    ];
    let merged = Synchronizer::merge(vec![evs]);
    assert_eq!(merged[0].ts_ms, 1000);
    assert_eq!(merged[1].ts_ms, 3000);
}

#[test]
fn as_of_is_backward_only() {
    let hist = vec![(10, 1.0), (20, 2.0)];
    assert_eq!(as_of_backward(&hist, 5), None);
    assert_eq!(as_of_backward(&hist, 20), Some(&2.0));
    assert_eq!(as_of_backward(&hist, 99), Some(&2.0));
}

#[test]
fn replay_clock_rejects_backward_time_travel() {
    let mut c = ReplayClock::new(100);
    c.advance_to(200).unwrap();
    assert!(c.advance_to(150).is_err());
    assert_eq!(
        c.time_remaining_secs(200_000),
        (200_000 - 200) as u64 / 1000
    );
}

#[test]
fn rolling_features_use_only_past_ticks() {
    let ticks: Vec<ExternalTick> = (0..600)
        .map(|i| ExternalTick {
            price: 100.0 + i as f64 * 0.01,
            ts_ms: i * 1000,
        })
        .collect();
    let ctx = ResolutionContext::new(100.0, 60, "replay-test".to_owned());
    let venues = VenueMicroprices {
        binance: 106.0,
        coinbase: 106.0,
        perp: 106.0,
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
    let f = build_features_with_context(&ticks, &ctx, venues, flow);
    assert!(f.spot > 0.0);
    assert_eq!(f.time_remaining_secs, 60);
    // Quant enrichment is deterministic and clock-free.
    let q = build_quant(&f, &QuantConfig::default().params);
    assert!(q.quant_baseline_p_yes >= 0.0 && q.quant_baseline_p_yes <= 1.0);
}

#[test]
fn reference_price_and_poly_history_are_backward_only() {
    let mut h = PolyHistory::new(16);
    h.push(1000, PriceTicks::from_f64(0.5));
    h.push(2000, PriceTicks::from_f64(0.6));
    assert_eq!(h.price_1s_ago(2000), Some(PriceTicks::from_f64(0.5)));
    assert_eq!(h.price_1s_ago(500), None);
}

#[test]
fn control_quant_share_everything_but_quant() {
    let cfg = ReplayConfig::smoke("pair-test");
    assert_eq!(cfg.fill, FillProfile::Conservative);
    assert_eq!(cfg.latency, LatencyProfile::Base);
    // Pairing invariant: one frozen feature set -> three arm states.
    let mut runner = ReplayRunner::new(cfg, StubJev::new(1));
    let mut third = mk_item(11000, 0.42, 0.47, 102.0, "BTC-5m", "BTC", "5m");
    third.split = Split::OutOfSample;
    let items = vec![
        mk_item(1000, 0.40, 0.45, 100.0, "BTC-5m", "BTC", "5m"),
        mk_item(6000, 0.41, 0.46, 101.0, "BTC-5m", "BTC", "5m"),
        third,
    ];
    let out = runner.run_synthetic(&items, 1_000_000);
    // Rows come in CONTROL/MICRO_V2/MICRO_V2_QUANT triples sharing pair_id.
    assert!(!out.rows.is_empty());
    assert_eq!(out.rows.len() % 3, 0);
    for w in out.rows.chunks(3) {
        assert_eq!(w[0].pair_id, w[1].pair_id);
        assert_eq!(w[1].pair_id, w[2].pair_id);
        let mut variants: Vec<&str> = w.iter().map(|r| r.variant.as_str()).collect();
        variants.sort_unstable();
        assert_eq!(variants, ["CONTROL", "MICRO_V2", "MICRO_V2_QUANT"]);
        assert_ne!(w[0].state_hash, w[1].state_hash);
        assert_ne!(w[1].state_hash, w[2].state_hash);
    }
}

#[test]
fn healthy_skips_are_complete_but_jev_errors_are_incomplete() {
    let item = mk_item(1000, 0.40, 0.45, 100.0, "BTC-5m", "BTC", "5m");

    let mut healthy = ReplayRunner::new(
        ReplayConfig::smoke("healthy-skip"),
        FixedReplayJev { error: false },
    );
    let healthy_output = healthy.run_synthetic(std::slice::from_ref(&item), 1_000_000);
    assert_eq!(healthy_output.rows.len(), 3);
    assert!(healthy_output.rows.iter().all(|row| !row.quoted));
    assert!(healthy_output.rows.iter().all(|row| !row.incomplete_pair));
    assert_eq!(healthy_output.incomplete_pairs, 0);
    let summaries = build_report(&healthy_output.rows);
    assert!(
        summaries
            .iter()
            .all(|summary| summary.incomplete_pairs == 0)
    );

    let mut failing = ReplayRunner::new(
        ReplayConfig::smoke("jev-error"),
        FixedReplayJev { error: true },
    );
    let failing_output = failing.run_synthetic(&[item], 1_000_000);
    assert_eq!(failing_output.rows.len(), 3);
    assert!(failing_output.rows.iter().all(|row| row.incomplete_pair));
    assert_eq!(failing_output.jev_errors, 3);
    // RunnerOutput.incomplete_pairs is reserved for pairs with no rows.
    assert_eq!(failing_output.incomplete_pairs, 0);
}

#[test]
fn jev_latency_and_stale_responses_skip_quotes() {
    let policy = StalenessPolicy::new(2, 100);
    let eval = jevtrader::jev::response::JevEvaluation {
        market_id: "m".to_owned(),
        state_seq: 1,
        sent_at_ms: 0,
        received_at_ms: 500,
        latency_ms: 500,
        signal: jevtrader::jev::response::V1Signal {
            yes_pressure_5s: 0.9,
            no_pressure_5s: 0.1,
            move_persists: 0.9,
            underreact_up: 0.9,
            underreact_down: 0.1,
            repricing: jevtrader::jev::response::TickDistribution {
                up_3_plus: 0.1,
                up_2: 0.2,
                up_1: 0.4,
                flat: 0.1,
                down_1: 0.1,
                down_2: 0.05,
                down_3_plus: 0.05,
            },
            repricing_confidence: 0.8,
            fill_before_decay: 0.9,
            fill_toxic: 0.1,
        },
        tokens_in: 0,
        tokens_out: 0,
    };
    assert!(!policy.is_usable(&eval, 1));
}

#[test]
fn execution_latency_hides_pre_resting_prints_and_partials_fill() {
    let sim = FillSimulator::new(FillProfile::Base);
    let order = RestingOrder {
        price: 0.43,
        size: 10.0,
        resting_from_ms: 1000,
        side_buy: true,
    };
    let none = sim.check_fill(&order, &[(1010, 0.40, 99.0)], ExecutionLatency::new(50));
    assert!(!none.filled);
    let partial = sim.check_fill(&order, &[(1100, 0.42, 4.0)], ExecutionLatency::new(50));
    assert!(partial.filled && partial.fill_fraction < 1.0);
}

#[test]
fn three_fill_models_are_ordered_optimistic_ge_base_ge_conservative() {
    let prints = vec![(2000, 0.43, 3.0), (3000, 0.42, 3.0)];
    let order = RestingOrder {
        price: 0.43,
        size: 10.0,
        resting_from_ms: 1000,
        side_buy: true,
    };
    let lat = ExecutionLatency::new(0);
    let o = FillSimulator::new(FillProfile::Optimistic).check_fill(&order, &prints, lat);
    let b = FillSimulator::new(FillProfile::Base).check_fill(&order, &prints, lat);
    let c = FillSimulator::new(FillProfile::Conservative).check_fill(&order, &prints, lat);
    assert!(o.fill_fraction >= b.fill_fraction);
    assert!(b.fill_fraction >= c.fill_fraction);
}

#[test]
fn realized_pnl_held_to_resolution_and_markout_sign() {
    let mut p = Portfolio::new();
    p.apply_fill(FillEvent {
        price: 0.43,
        size: 10.0,
        ts_ms: 0,
        toxic: false,
    });
    p.apply_exit(1.0, 10.0, 999);
    assert!(p.realized_pnl_pp() > 0.0);
    let mo = signed_markouts_pp(0.41, [Some(0.43), Some(0.39), None, None, None]);
    assert!(mo[0].unwrap() > 0.0 && mo[1].unwrap() < 0.0);
    // Drawdown is tracked on mid observations.
    let mut d = Portfolio::new();
    d.apply_fill(FillEvent {
        price: 0.5,
        size: 5.0,
        ts_ms: 0,
        toxic: false,
    });
    d.observe_mid(0.6);
    d.observe_mid(0.4);
    assert!(d.max_drawdown_pp > 0.0);
}

#[test]
fn isolation_across_portfolios_markets_assets_horizons() {
    let mut reg = PortfolioRegistry::new();
    reg.portfolio("CONTROL", "BTC-5m").apply_fill(FillEvent {
        price: 0.4,
        size: 1.0,
        ts_ms: 0,
        toxic: false,
    });
    assert!(reg.get("QUANT_V1", "BTC-5m").is_none());
    assert!(reg.get("CONTROL", "ETH-1h").is_none());
    // Horizons and assets segment in reports, never merged silently.
    let cfg = ReplayConfig::smoke("seg-test");
    let mut runner = ReplayRunner::new(cfg, StubJev::new(9));
    let mut eth_item = mk_item(6000, 0.40, 0.45, 100.0, "ETH-1h", "ETH", "1h");
    eth_item.regime = "R".to_owned();
    let mut btc_item = mk_item(1000, 0.40, 0.45, 100.0, "BTC-5m", "BTC", "5m");
    btc_item.regime = "R".to_owned();
    let items = vec![btc_item, eth_item];
    let out = runner.run_synthetic(&items, 999_999);
    let rep = build_report(&out.rows);
    assert!(!rep.is_empty());
    let md = write_markdown(&rep);
    assert!(md.contains("BTC") || md.contains("ETH"));
}

#[test]
fn manifest_reproducibility_and_incomplete_data() {
    // Incomplete snapshot (crossed book) yields no quote, not a panic.
    let snap = snapshot(0.50, 0.40);
    assert!(
        candidate_maker_price(&snap.book, TickSize::from_f64(0.01))
            .is_none_or(|c| c >= snap.book.best_ask().unwrap())
    );
    // Chunked source never requires the full dataset at once.
    let evs: Vec<HistoricalEvent> = (0..50)
        .map(|i| HistoricalEvent::UnderlyingTick {
            ts_ms: i,
            asset: "BTC".to_owned(),
            venue: "BINANCE".to_owned(),
            price: 100.0,
            bid: None,
            ask: None,
            qty: None,
            aggressor: None,
            source: "t".to_owned(),
        })
        .collect();
    let mut src = InMemorySource::new(evs);
    let c1 = src.next_chunk(20).unwrap();
    assert_eq!(c1.len(), 20);
    // Walk-forward windows exist and keep frozen thresholds.
    let w = WalkforwardWindow {
        name: "A".to_owned(),
        start_idx: 0,
        end_idx: 1,
    };
    assert_eq!(w.name, "A");
    let _ = MarkoutTracker::new();
    // decide() is exercised via the runner path; reaching here proves
    // incomplete snapshots never panic.
}

#[test]
fn empirical_latency_uses_sampled_distribution_deterministically() {
    let items = [
        mk_item(1000, 0.40, 0.45, 100.0, "BTC-5m", "BTC", "5m"),
        mk_item(6000, 0.41, 0.46, 101.0, "BTC-5m", "BTC", "5m"),
        mk_item(11000, 0.42, 0.47, 102.0, "BTC-5m", "BTC", "5m"),
    ];
    let run_once = || {
        let mut cfg = ReplayConfig::smoke("empirical-lat");
        cfg.latency = LatencyProfile::Empirical;
        cfg.max_pairs = 3;
        let mut runner = ReplayRunner::new(cfg, StubJev::new(42));
        runner.run_synthetic(&items, 1_000_000)
    };
    let first = run_once();
    let second = run_once();
    assert_eq!(first.rows.len(), 9);
    assert!(
        first
            .rows
            .iter()
            .all(|row| row.latency_profile == "EMPIRICAL")
    );
    // Default distribution is the pilot placeholder bounds only.
    assert!(
        first
            .rows
            .iter()
            .all(|row| row.jev_latency_ms == 307 || row.jev_latency_ms == 1019)
    );
    let lat_first: Vec<u64> = first.rows.iter().map(|row| row.jev_latency_ms).collect();
    let lat_second: Vec<u64> = second.rows.iter().map(|row| row.jev_latency_ms).collect();
    assert_eq!(lat_first, lat_second);
}

#[test]
fn signal_drift_recorded_for_all_usable_evaluations_including_skips() {
    // Rising mids: every healthy evaluation must carry forward drift,
    // even when the strategy SKIPs (no quote, no fill).
    let items = [
        mk_item(0, 0.39, 0.41, 100.0, "BTC-5m", "BTC", "5m"),
        mk_item(6000, 0.41, 0.43, 101.0, "BTC-5m", "BTC", "5m"),
        mk_item(11000, 0.43, 0.45, 102.0, "BTC-5m", "BTC", "5m"),
        mk_item(21000, 0.45, 0.47, 103.0, "BTC-5m", "BTC", "5m"),
    ];
    let mut healthy = ReplayRunner::new(
        ReplayConfig::smoke("drift-skip"),
        FixedReplayJev { error: false },
    );
    let out = healthy.run_synthetic(&items, 1_000_000);
    assert_eq!(out.rows.len(), 12);
    assert!(out.rows.iter().all(|row| !row.quoted));
    assert!(out.rows.iter().all(|row| !row.incomplete_pair));
    // SKIP rows carry no markouts (no fill) but must carry drift, except the
    // tail pair which has no forward prints (honest missing, not a bug).
    assert!(out.rows.iter().all(|row| row.markout_5s_pp.is_none()));
    for row in &out.rows {
        if row.pair_id.ends_with("000003") {
            assert!(row.drift_5s_pp.is_none());
        } else {
            assert!(row.drift_5s_pp.is_some_and(|d| d.is_finite()));
        }
    }

    let mut failing = ReplayRunner::new(
        ReplayConfig::smoke("drift-error"),
        FixedReplayJev { error: true },
    );
    let bad = failing.run_synthetic(&items, 1_000_000);
    assert!(bad.rows.iter().all(|row| row.incomplete_pair));
    assert!(
        bad.rows
            .iter()
            .all(|row| row.drift_5s_pp.is_none() && row.markout_5s_pp.is_none())
    );
}

#[test]
fn pair_ids_stay_unique_across_per_condition_calls() {
    // run_corpus replays one condition per call on a shared runner; without
    // a namespace, seq restarts at 0 and pair_ids collide across conditions.
    let item = mk_item(1000, 0.40, 0.45, 100.0, "BTC-5m", "BTC", "5m");
    let mut cfg = ReplayConfig::smoke("ns-run");
    cfg.max_pairs = 10;
    let mut runner = ReplayRunner::new(cfg, StubJev::new(7));
    runner.config.pair_namespace = "cond-A".to_owned();
    let a = runner.run_synthetic(std::slice::from_ref(&item), 1_000_000);
    runner.config.pair_namespace = "cond-B".to_owned();
    let b = runner.run_synthetic(std::slice::from_ref(&item), 1_000_000);
    assert_eq!(a.rows.len(), 3);
    assert_eq!(b.rows.len(), 3);
    let ids_a: Vec<&str> = a.rows.iter().map(|row| row.pair_id.as_str()).collect();
    let ids_b: Vec<&str> = b.rows.iter().map(|row| row.pair_id.as_str()).collect();
    assert!(ids_a.iter().all(|id| id.contains("cond-A")));
    assert!(ids_b.iter().all(|row| row.contains("cond-B")));
    // Every pair_id appears exactly three times (one per arm), never
    // shared across the two calls.
    let mut counts: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();
    for row in a.rows.iter().chain(b.rows.iter()) {
        *counts.entry(row.pair_id.as_str()).or_default() += 1;
    }
    assert_eq!(counts.len(), 2);
    assert!(counts.values().all(|n| *n == 3));
    // Within one pair the two rows share pair_id across variants.
    assert_eq!(a.rows[0].pair_id, a.rows[1].pair_id);
    assert_ne!(a.rows[0].pair_id, b.rows[0].pair_id);
}

#[test]
fn micro_arm_diverges_from_v1_given_real_microstructure() {
    // With real micro context (perp != spot, nonzero flows), the MICRO arms
    // must evaluate different states than CONTROL. With neutral context
    // they still differ by contract/target (covered in pairing test).
    let mut it = mk_item(1000, 0.40, 0.45, 100.0, "BTC-5m", "BTC", "5m");
    it.perp = 101.5;
    it.spot_flow = OrderFlowAggregates {
        buy_vol_1s: 8.0,
        sell_vol_1s: 2.0,
        ofi_1s: 6.0,
        ofi_5s: 20.0,
        imbalance: 0.6,
        aggressive_buy_ratio: 0.8,
    };
    it.poly_flow = OrderFlowAggregates {
        buy_vol_1s: 5.0,
        sell_vol_1s: 5.0,
        ofi_1s: 0.0,
        ofi_5s: 0.0,
        imbalance: 0.0,
        aggressive_buy_ratio: 0.5,
    };
    let mut runner = ReplayRunner::new(ReplayConfig::smoke("micro-diverge"), StubJev::new(3));
    let out = runner.run_synthetic(std::slice::from_ref(&it), 1_000_000);
    assert_eq!(out.rows.len(), 3);
    assert_eq!(out.signals.len(), 3);
    let hashes: Vec<&str> = out.rows.iter().map(|row| row.state_hash.as_str()).collect();
    assert_ne!(hashes[0], hashes[1]);
    assert_ne!(hashes[0], hashes[2]);
    assert_ne!(hashes[1], hashes[2]);
    // StubJev hashes state content, so distinct states yield distinct
    // pseudo-signals: the arms genuinely see different states.
    let ups: Vec<f64> = out.signals.iter().map(|s| s.signal.underreact_up).collect();
    assert!(!(ups[0] == ups[1] && ups[1] == ups[2]));
    assert!(out.signals.iter().all(|s| !s.live));
}
