use jevtrader::config::QuoteThresholds;
use jevtrader::domain::{PriceTicks, TickSize};
use jevtrader::jev::{TickDistribution, V1Signal};
use jevtrader::polymarket::OrderBook;
use jevtrader::strategy::lead_lag::should_quote;
use jevtrader::strategy::quote::decide_quote;

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
        [(PriceTicks::from_f64(0.42), 100)],
    );
    book
}

#[test]
fn decision_and_shared_rule_agree_on_happy_path() {
    let thresholds = QuoteThresholds::default();
    let signal = signal();
    let book = book();

    assert!(should_quote(&signal, &thresholds));
    assert!(
        decide_quote(
            &signal,
            &book,
            &thresholds,
            false,
            TickSize::from_f64(0.01),
            25,
        )
        .is_some()
    );
}

#[test]
fn decision_and_shared_rule_agree_below_threshold() {
    let thresholds = QuoteThresholds::default();
    let mut signal = signal();
    signal.underreact_up = thresholds.under_min;
    let book = book();

    assert!(!should_quote(&signal, &thresholds));
    assert_eq!(
        decide_quote(
            &signal,
            &book,
            &thresholds,
            false,
            TickSize::from_f64(0.01),
            25,
        ),
        None
    );
}
