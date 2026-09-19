use std::{future::Future, pin::Pin, sync::OnceLock, time::Duration};

use jevtrader::config::{FreshnessPolicy, QuantConfig, QuoteThresholds};
use jevtrader::domain::{PriceTicks, TickSize, Trigger};
use jevtrader::engine::execution_actor::ExecutionActor;
use jevtrader::engine::market_actor::MarketSnapshot;
use jevtrader::engine::pipeline::{SignalEvaluator, SkipReason};
use jevtrader::engine::{Outcome, Pipeline, PipelineInput};
use jevtrader::execution::PaperFill;
use jevtrader::jev::client::JevError;
use jevtrader::jev::request::V1State;
use jevtrader::jev::response::{JevEvaluation, TickDistribution, V1Signal};
use jevtrader::market_spec::MarketSpec;
use jevtrader::polymarket::OrderBook;
use jevtrader::state::feature_builder::{
    ExternalTick, OrderFlowAggregates, ResolutionContext, VenueMicroprices,
};
use jevtrader::storage::{ExperimentTags, StorageEvent, Variant};
use jevtrader::strategy::risk::RiskLimits;

#[derive(Debug)]
struct FakeEvaluator {
    results: Vec<Result<JevEvaluation, JevError>>,
    signal: V1Signal,
}

impl SignalEvaluator for FakeEvaluator {
    fn evaluate_next<'a>(
        &'a mut self,
        _state: &'a V1State,
        _market_id: &'a str,
        _api_key: &'a str,
        _deadline: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<JevEvaluation, JevError>> + Send + 'a>> {
        let result = if self.results.is_empty() {
            Err(JevError::Deadline)
        } else {
            self.results.remove(0)
        };
        Box::pin(std::future::ready(result))
    }

    fn usable_signal(&self) -> Option<&V1Signal> {
        Some(&self.signal)
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

fn market(bid: f64, ask: f64) -> MarketSnapshot {
    let mut book = OrderBook::default();
    book.apply_snapshot(
        [(PriceTicks::from_f64(bid), 100)],
        [(PriceTicks::from_f64(ask), 100)],
    );
    MarketSnapshot { book, stale: false }
}

fn spec() -> &'static MarketSpec {
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

static EMPTY_TICKS: [ExternalTick; 0] = [];

fn input(
    snapshot: MarketSnapshot,
    size: u64,
    observed_at_ms: i64,
    mid: f64,
) -> PipelineInput<'static> {
    PipelineInput {
        market_id: "market-1",
        condition_id: "condition-1",
        market_spec: spec(),
        resolution: ResolutionContext::new(120_000.0, 900, "Official source"),
        snapshot,
        last_trade_price: PriceTicks::from_f64(mid),
        tick_size: TickSize::from_f64(0.01),
        recent_ticks: &EMPTY_TICKS,
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
        observed_at_ms,
        mid: Some(PriceTicks::from_f64(mid)),
        trigger: Trigger::PriceMove,
    }
}

fn pipeline(
    results: Vec<Result<JevEvaluation, JevError>>,
    thresholds: QuoteThresholds,
) -> Pipeline<FakeEvaluator> {
    pipeline_with_quant(results, thresholds, false)
}

fn pipeline_with_quant(
    results: Vec<Result<JevEvaluation, JevError>>,
    thresholds: QuoteThresholds,
    quant_enabled: bool,
) -> Pipeline<FakeEvaluator> {
    Pipeline::with_evaluator(
        "paper-pnl-test",
        FakeEvaluator {
            results,
            signal: signal(),
        },
        ExecutionActor::new(RiskLimits::from_freshness_policy(
            2,
            FreshnessPolicy::default(),
            false,
        )),
        None,
        "test-key",
        Duration::from_millis(100),
        thresholds,
        QuantConfig {
            enabled: quant_enabled,
            ..QuantConfig::default()
        },
        64,
    )
}

fn fills(events: &[StorageEvent]) -> Vec<(Variant, PaperFill, ExperimentTags)> {
    events
        .iter()
        .filter_map(|event| match event {
            StorageEvent::PaperFill {
                variant,
                order_id,
                price,
                size,
                filled_at_ms,
                maker,
                tags,
                ..
            } => Some((
                *variant,
                PaperFill {
                    order_id: u64::try_from(*order_id).expect("test order id should be positive"),
                    price: PriceTicks::from_f64(*price),
                    size: *size as u64,
                    filled_at_ms: *filled_at_ms,
                    maker: *maker != 0,
                },
                tags.clone(),
            )),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn trade_through_emits_fill_and_equity_with_consistent_pnl() {
    let thresholds = QuoteThresholds {
        under_min: 0.83,
        ..QuoteThresholds::default()
    };
    let mut pipeline = pipeline(vec![Ok(evaluation())], thresholds);

    let quoted = pipeline
        .run_step(input(market(0.40, 0.45), 10, 1_000, 0.425))
        .await;
    assert!(matches!(quoted.outcome, Outcome::Quote(_)));

    // The current ask trades through the resting quote before the second
    // Jev evaluation completes, so the fill carries that step's pair tags.
    let filled = pipeline
        .run_step(input(market(0.38, 0.40), 10, 2_000, 0.39))
        .await;
    assert_eq!(filled.outcome, Outcome::Skip(SkipReason::JevError));

    let stats = pipeline
        .paper_stats(Variant::Control)
        .expect("the control portfolio should exist");
    assert_eq!(stats.fills, 1);
    assert_eq!(stats.inventory, 10.0);
    assert!((stats.unrealized_pp + 20.0).abs() < 1e-9);
    assert!((stats.total_pp + 20.0).abs() < 1e-9);

    let events = pipeline.captured_events();
    let paper_fills = fills(&events);
    assert_eq!(paper_fills.len(), 1);
    assert_eq!(paper_fills[0].0, Variant::Control);
    assert_eq!(paper_fills[0].1.size, 10);
    assert!(!paper_fills[0].2.pair_id.is_empty());

    let equity = events
        .iter()
        .filter(|event| matches!(event, StorageEvent::PaperEquity { .. }))
        .count();
    assert_eq!(equity, 4, "two variants must be sampled on every step");

    let mut thresholds = events.iter().filter_map(|event| match event {
        StorageEvent::PaperDecision { threshold, .. } => Some(*threshold),
        _ => None,
    });
    assert!(thresholds.all(|threshold| (threshold - 0.83).abs() < 1e-9));
}

#[tokio::test]
async fn settle_yes_and_no_realizes_full_inventory() {
    for (yes_won, expected) in [(true, 590.0), (false, -410.0)] {
        let mut pipeline = pipeline(vec![Ok(evaluation())], QuoteThresholds::default());
        pipeline
            .run_step(input(market(0.40, 0.45), 10, 1_000, 0.425))
            .await;
        pipeline
            .run_step(input(market(0.38, 0.39), 10, 2_000, 0.385))
            .await;

        let (control, quant) = pipeline.settle(yes_won);
        assert!((control - expected).abs() < 1e-9);
        assert_eq!(quant, 0.0);
        let stats = pipeline.paper_stats(Variant::Control).unwrap();
        assert_eq!(stats.inventory, 0.0);
        assert!((stats.total_pp - expected).abs() < 1e-9);

        let final_equity: Vec<(Variant, f64, f64)> = pipeline
            .captured_events()
            .iter()
            .rev()
            .filter_map(|event| match event {
                StorageEvent::PaperEquity {
                    variant,
                    position,
                    total_pnl,
                    ..
                } => Some((*variant, *position, *total_pnl)),
                _ => None,
            })
            .take(2)
            .collect();
        assert_eq!(final_equity.len(), 2);
        assert!(final_equity.iter().all(|(_, position, _)| *position == 0.0));
        assert!(final_equity.iter().any(|(variant, _, total)| {
            *variant == Variant::Control && (*total - expected).abs() < 1e-9
        }));
    }
}

#[tokio::test]
async fn settle_at_fifty_and_clamps_out_of_range_prices() {
    // Same inventory as the binary test (entry 0.41, size 10):
    // FIFTY pays (0.5 - 0.41) * 10 * 100 = 90.
    let mut settling = pipeline(vec![Ok(evaluation())], QuoteThresholds::default());
    settling
        .run_step(input(market(0.40, 0.45), 10, 1_000, 0.425))
        .await;
    settling
        .run_step(input(market(0.38, 0.39), 10, 2_000, 0.385))
        .await;

    let (control, quant) = settling.settle_at(0.5);
    assert!((control - 90.0).abs() < 1e-9);
    assert_eq!(quant, 0.0);
    let stats = settling.paper_stats(Variant::Control).unwrap();
    assert_eq!(stats.inventory, 0.0);

    // Out-of-range prices clamp to the binary outcomes, never invent PnL.
    for (price, expected) in [(2.0, 590.0), (-1.0, -410.0)] {
        let mut clamped = pipeline(vec![Ok(evaluation())], QuoteThresholds::default());
        clamped
            .run_step(input(market(0.40, 0.45), 10, 1_000, 0.425))
            .await;
        clamped
            .run_step(input(market(0.38, 0.39), 10, 2_000, 0.385))
            .await;

        let (control, _) = clamped.settle_at(price);
        assert!((control - expected).abs() < 1e-9);
    }
}

#[tokio::test]
async fn incomplete_pair_does_not_mix_fills_between_variants() {
    let mut pipeline = pipeline_with_quant(
        vec![Ok(evaluation()), Err(JevError::Deadline)],
        QuoteThresholds::default(),
        true,
    );

    pipeline
        .run_step(input(market(0.40, 0.45), 10, 1_000, 0.425))
        .await;
    pipeline
        .run_step(input(market(0.38, 0.39), 10, 2_000, 0.385))
        .await;

    let events = pipeline.captured_events();
    assert!(events.iter().any(|event| matches!(
        event,
        StorageEvent::AbPair { status, .. } if status == "incomplete"
    )));
    let paper_fills = fills(&events);
    assert_eq!(paper_fills.len(), 1);
    assert_eq!(paper_fills[0].0, Variant::Control);
    assert!(paper_fills[0].2.pair_id.is_empty());

    let control = pipeline.paper_stats(Variant::Control).unwrap();
    assert_eq!(control.fills, 1);
    let quant = pipeline
        .paper_stats(Variant::QuantV1)
        .expect("mid sampling creates the quant portfolio");
    assert_eq!(quant.fills, 0);
}
