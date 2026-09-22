use jevtrader::config::QuoteThresholds;
use jevtrader::jev::{TickDistribution, V1Signal};
use jevtrader::strategy::api::{Action, MarketEvent, Side, Strategy, StrategyContext, V1Strategy};

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

fn rejected_signal() -> V1Signal {
    let mut signal = signal();
    signal.underreact_up = 0.7;
    signal
}

fn context() -> StrategyContext {
    StrategyContext {
        thresholds: QuoteThresholds::default(),
        tick_size: 0.01,
        min_order_size: 0.01,
    }
}

fn event(bid: f64, ask: f64, book_stale: bool, signal: V1Signal) -> MarketEvent {
    MarketEvent {
        event_id: "event-1".to_owned(),
        ts_ms: 1_000,
        condition_id: "condition-1".to_owned(),
        asset: "BTC".to_owned(),
        horizon: "5m".to_owned(),
        signal,
        yes_bid: bid,
        yes_ask: ask,
        book_stale,
        open_qty: 0.0,
        open_avg_price: 0.0,
        strategy_version: V1Strategy::VERSION.to_owned(),
    }
}

#[test]
fn identical_event_sequence_is_deterministic() {
    let events = [
        event(0.40, 0.50, false, signal()),
        event(0.40, 0.50, false, rejected_signal()),
        event(0.40, 0.50, true, signal()),
        event(0.40, 0.41, false, signal()),
    ];

    let run = |mut strategy: V1Strategy| {
        events
            .iter()
            .map(|event| {
                serde_json::to_string(&strategy.on_market_event(&context(), event))
                    .expect("actions serialize")
            })
            .collect::<Vec<_>>()
    };

    assert_eq!(run(V1Strategy), run(V1Strategy));
}

#[test]
fn stale_book_holds_without_place_maker() {
    let mut strategy = V1Strategy;
    let actions = strategy.on_market_event(&context(), &event(0.40, 0.50, true, signal()));

    assert!(matches!(
        actions.as_slice(),
        [Action::Hold { reason }] if reason == "stale_book"
    ));
    assert!(
        !actions
            .iter()
            .any(|action| matches!(action, Action::PlaceMaker { .. }))
    );
}

#[test]
fn crossed_candidate_holds() {
    let mut strategy = V1Strategy;
    let actions = strategy.on_market_event(&context(), &event(0.40, 0.41, false, signal()));

    assert!(matches!(
        actions.as_slice(),
        [Action::Hold { reason }] if reason == "crossed"
    ));
}

#[test]
fn rejected_rule_holds() {
    let mut strategy = V1Strategy;
    let actions =
        strategy.on_market_event(&context(), &event(0.40, 0.50, false, rejected_signal()));

    assert!(matches!(
        actions.as_slice(),
        [Action::Hold { reason }] if reason == "rule_rejected"
    ));
}

#[test]
fn place_maker_is_post_only_and_tick_aligned() {
    let mut strategy = V1Strategy;
    let ask = 0.50;
    let actions = strategy.on_market_event(&context(), &event(0.40, ask, false, signal()));

    let [
        Action::PlaceMaker {
            side,
            price,
            size_shares,
        },
    ] = actions.as_slice()
    else {
        panic!("passing V1 signal should emit one maker intent");
    };

    assert_eq!(*side, Side::Buy);
    assert_eq!(*size_shares, 0.0);
    assert!(*price < ask);
    let nearest_tick = (*price / 0.01).round();
    assert!((*price - nearest_tick * 0.01).abs() <= 1e-9);
}

#[test]
fn open_position_with_decayed_signal_holds_without_an_exit() {
    let mut strategy = V1Strategy;
    let mut event = event(0.40, 0.50, false, rejected_signal());
    event.open_qty = 2.0;
    event.open_avg_price = 0.40;

    let actions = strategy.on_market_event(&context(), &event);

    assert!(matches!(
        actions.as_slice(),
        [Action::Hold { reason }] if reason == "position_open"
    ));
}

#[test]
fn strategy_version_is_constant_and_present_in_market_event() {
    let strategy = V1Strategy;
    let event = event(0.40, 0.50, false, signal());

    assert_eq!(strategy.strategy_version(), "v1-lead-lag");
    assert_eq!(strategy.strategy_version(), V1Strategy::VERSION);
    assert_eq!(event.strategy_version, V1Strategy::VERSION);
    assert_eq!(
        serde_json::to_value(&event).expect("market event serializes")["strategy_version"],
        "v1-lead-lag"
    );
}

#[test]
fn strategy_api_does_not_expose_fills_or_portfolio() {
    // Compile-time API contract: this test imports only MarketEvent, the
    // context, and actions. api.rs must not import replay fills or Portfolio;
    // FillSimulator and exit policy remain replay responsibilities.
    let _: fn(&mut V1Strategy, &StrategyContext, &MarketEvent) -> Vec<Action> =
        Strategy::on_market_event;
}
