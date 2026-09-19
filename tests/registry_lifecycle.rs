use jevtrader::config::QuoteThresholds;
use jevtrader::domain::{Asset, Horizon, MarketKey, PriceTicks, TickSize, TradeSide};
use jevtrader::engine::rollover_spec;
use jevtrader::engine::{
    DecisionInput, ExecutionActor, Lifecycle, MarketRegistry, MarketSnapshot, RegistryError,
    SignalActor, StalenessPolicy, decide,
};
use jevtrader::jev::{JevEvaluation, TickDistribution, V1Signal};
use jevtrader::market_spec::MarketSpec;
use jevtrader::polymarket::OrderBook;
use jevtrader::strategy::quote::QuoteIntent;
use jevtrader::strategy::risk::RiskLimits;

fn spec(key: MarketKey, resolution_at_ms: i64) -> MarketSpec {
    MarketSpec::parse(&format!(
        "slug: {}\nquestion: Will {} resolve above the target?\nresolution_source: Binance\ntarget: 100\nresolution_at_ms: {resolution_at_ms}\nasset: {}\nhorizon: {}\nresolution_rules: |\n  Resolves from the declared Binance reference.\n",
        key.market_id(),
        key.asset.as_str(),
        key.asset.as_str(),
        key.horizon.as_str(),
    ))
    .expect("registry fixture should parse")
}

#[test]
fn registers_all_eight_contracts_and_rejects_unknown_or_duplicate_keys() {
    let mut registry = MarketRegistry::new();
    for (index, key) in MarketKey::all().into_iter().enumerate() {
        registry
            .register(
                spec(key, 2_000_000 + index as i64),
                format!("condition-{index}"),
                format!("yes-{index}"),
            )
            .expect("supported contract should register");
    }

    assert_eq!(registry.len(), 8);
    assert!(!registry.is_empty());
    let keys = registry
        .keys()
        .copied()
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(keys.len(), 8);

    let duplicate = MarketKey::new(Asset::Btc, Horizon::M5);
    assert!(matches!(
        registry.register(spec(duplicate, 3_000_000), "other-condition", "other-yes"),
        Err(RegistryError::DuplicateMarket { key }) if key == duplicate
    ));

    let unknown = MarketSpec::parse(
        "slug: sol-5m\nquestion: Will SOL resolve above the target?\nresolution_source: Binance\ntarget: 100\nresolution_at_ms: 2_000_000\nresolution_rules: |\n  Resolves from Binance.\n",
    )
    .expect("unknown fixture should parse");
    assert!(matches!(
        registry.register(unknown, "sol-condition", "sol-yes"),
        Err(RegistryError::UnknownMarketKey { .. })
    ));
}

#[test]
fn lifecycle_transitions_and_tradability_are_explicit() {
    assert_eq!(Lifecycle::Discovered.advance(300), Lifecycle::Active);
    assert_eq!(Lifecycle::Discovered.advance(60), Lifecycle::NearResolution);
    assert_eq!(Lifecycle::Active.advance(61), Lifecycle::Active);
    assert_eq!(Lifecycle::Active.advance(60), Lifecycle::NearResolution);
    assert_eq!(
        Lifecycle::NearResolution.advance(1),
        Lifecycle::NearResolution
    );
    assert_eq!(Lifecycle::NearResolution.advance(0), Lifecycle::Resolved);
    assert_eq!(Lifecycle::Resolved.advance(0), Lifecycle::RolledOver);
    assert_eq!(Lifecycle::RolledOver.advance(300), Lifecycle::RolledOver);

    let key = MarketKey::new(Asset::Eth, Horizon::H1);
    let mut registry = MarketRegistry::new();
    registry
        .register(spec(key, 2_000_000), "condition", "yes")
        .expect("market should register");
    let runtime = registry.get_mut(&key).expect("registered runtime");
    assert!(!runtime.is_tradable());
    runtime.lifecycle = Lifecycle::Active;
    assert!(runtime.is_tradable());
    runtime.lifecycle = Lifecycle::NearResolution;
    assert!(runtime.is_tradable());
    runtime.lifecycle = Lifecycle::Resolved;
    assert!(!runtime.is_tradable());
}

#[test]
fn rollover_uses_exact_horizon_milliseconds() {
    let end = 1_700_000_000_123_i64;
    for (horizon, seconds) in [
        (Horizon::M5, 300_i64),
        (Horizon::M15, 900_i64),
        (Horizon::H1, 3_600_i64),
        (Horizon::H4, 14_400_i64),
    ] {
        let key = MarketKey::new(Asset::Btc, horizon);
        assert_eq!(rollover_spec(key, end), (end, end + seconds * 1_000));
    }
}

#[test]
fn note_resolution_maps_only_the_yes_token() {
    let key = MarketKey::new(Asset::Btc, Horizon::M5);
    let mut registry = MarketRegistry::new();
    registry
        .register(spec(key, 2_000_000), "condition", "yes-token-1")
        .expect("market should register");

    let runtime = registry.get(&key).expect("registered runtime");
    assert_eq!(runtime.winning_token_id, None);
    assert_eq!(runtime.yes_won(), None);

    let runtime = registry.get_mut(&key).expect("registered runtime");
    runtime.note_resolution("yes-token-1");
    assert_eq!(runtime.lifecycle, Lifecycle::Resolved);
    assert!(!runtime.is_tradable());
    assert_eq!(runtime.yes_won(), Some(true));

    // A non-YES token never invents NO: the caller must verify the outcome
    // from the resolution source and settle explicitly (e.g. settle_at(0.0)).
    let runtime = registry.get_mut(&key).expect("registered runtime");
    runtime.note_resolution("no-token-9");
    assert_eq!(runtime.yes_won(), None);
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

fn book() -> OrderBook {
    let mut book = OrderBook::default();
    book.apply_snapshot(
        [(PriceTicks::from_f64(0.40), 100)],
        [(PriceTicks::from_f64(0.50), 100)],
    );
    book
}

fn intent() -> QuoteIntent {
    QuoteIntent {
        side: TradeSide::Buy,
        price: PriceTicks::from_f64(0.41),
        size: 10,
    }
}

fn limits() -> RiskLimits {
    RiskLimits {
        max_outstanding_quotes: 2,
        max_latency_ms: 1_500,
        killed: false,
    }
}

#[test]
fn separate_signal_actors_keep_independent_state_and_results() {
    let mut first = SignalActor::new(StalenessPolicy::new(0, 1_500));
    let mut second = SignalActor::new(StalenessPolicy::new(0, 1_500));
    first.record_evaluation(JevEvaluation {
        market_id: "BTC-5m".to_owned(),
        state_seq: 1,
        sent_at_ms: 1,
        received_at_ms: 2,
        latency_ms: 1,
        signal: signal(),
        tokens_in: 0,
        tokens_out: 0,
    });
    second.record_evaluation(JevEvaluation {
        market_id: "ETH-1h".to_owned(),
        state_seq: 1,
        sent_at_ms: 1,
        received_at_ms: 2,
        latency_ms: 1,
        signal: signal(),
        tokens_in: 0,
        tokens_out: 0,
    });

    assert_eq!(first.state_seq(), 0);
    assert_eq!(second.state_seq(), 0);
    assert_eq!(first.latest_evaluation().unwrap().market_id, "BTC-5m");
    assert_eq!(second.latest_evaluation().unwrap().market_id, "ETH-1h");
}

#[test]
fn separate_execution_actors_keep_outstanding_quotes_independent() {
    let mut first = ExecutionActor::new(limits());
    let second = ExecutionActor::new(limits());
    first
        .on_quote(jevtrader::storage::Variant::Control, intent(), false, 10)
        .expect("first market quote should rest");

    assert_eq!(first.outstanding(jevtrader::storage::Variant::Control), 1);
    assert_eq!(second.outstanding(jevtrader::storage::Variant::Control), 0);
    assert_eq!(second.outstanding_total(), 0);
}

#[test]
fn stale_market_a_does_not_change_market_b_pure_decision() {
    let fresh_book = book();
    let stale_a = MarketSnapshot {
        book: fresh_book.clone(),
        stale: true,
    };
    let fresh_b = MarketSnapshot {
        book: fresh_book,
        stale: false,
    };
    let thresholds = QuoteThresholds::default();
    let signal = signal();

    let stale_decision = decide(DecisionInput {
        signal: &signal,
        market: &stale_a,
        thresholds: &thresholds,
        tick_size: TickSize::from_f64(0.01),
        size: 10,
    });
    let fresh_decision = decide(DecisionInput {
        signal: &signal,
        market: &fresh_b,
        thresholds: &thresholds,
        tick_size: TickSize::from_f64(0.01),
        size: 10,
    });

    assert_eq!(
        stale_decision,
        jevtrader::engine::Outcome::Skip(jevtrader::engine::SkipReason::StaleBook)
    );
    assert!(fresh_decision.is_quote());
}
