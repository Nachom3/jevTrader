use jevtrader::replay::FillProfile;
use jevtrader::replay::fills::{
    Aggressor, ExecutionLatency, FillPrint, FillSimulator, RestingOrder,
};

fn order(price: f64, size: f64, resting_from_ms: i64, side_buy: bool) -> RestingOrder {
    RestingOrder {
        price,
        size,
        resting_from_ms,
        side_buy,
    }
}

fn print(ts_ms: i64, price: f64, qty: f64, aggressor: Aggressor) -> FillPrint {
    FillPrint::new(ts_ms, price, qty, aggressor)
}

#[test]
fn touch_without_through_does_not_fill_conservative() {
    let outcome = FillSimulator::new(FillProfile::Conservative).check_fill(
        &order(0.50, 10.0, 100, true),
        &[print(150, 0.50, 100.0, Aggressor::Sell)],
        ExecutionLatency::new(0),
    );

    assert!(!outcome.filled);
    assert_eq!(outcome.fill_ts_ms, None);
    assert_eq!(outcome.used_aggressive_qty, 0.0);
}

#[test]
fn through_print_volume_is_consumed_directionally() {
    let outcome = FillSimulator::new(FillProfile::Optimistic).check_fill(
        &order(0.50, 10.0, 100, true),
        &[print(150, 0.49, 4.0, Aggressor::Sell)],
        ExecutionLatency::new(0),
    );

    assert!(outcome.filled);
    assert_eq!(outcome.fill_fraction, 0.4);
    assert_eq!(outcome.used_aggressive_qty, 4.0);
}

#[test]
fn conservative_requires_two_distinct_through_prints() {
    let simulator = FillSimulator::new(FillProfile::Conservative);
    let one_print = simulator.check_fill(
        &order(0.50, 10.0, 100, true),
        &[print(150, 0.49, 20.0, Aggressor::Sell)],
        ExecutionLatency::new(0),
    );
    let two_prints = simulator.check_fill(
        &order(0.50, 10.0, 100, true),
        &[
            print(150, 0.49, 10.0, Aggressor::Sell),
            print(200, 0.48, 10.0, Aggressor::Sell),
        ],
        ExecutionLatency::new(0),
    );

    assert!(!one_print.filled);
    assert!(two_prints.filled);
}

#[test]
fn latency_hides_early_prints() {
    let outcome = FillSimulator::new(FillProfile::Optimistic).check_fill(
        &order(0.50, 10.0, 100, true),
        &[
            print(149, 0.49, 100.0, Aggressor::Sell),
            print(150, 0.49, 2.0, Aggressor::Sell),
        ],
        ExecutionLatency::new(50),
    );

    assert!(outcome.filled);
    assert_eq!(outcome.used_aggressive_qty, 2.0);
    assert_eq!(outcome.fill_ts_ms, Some(150));
}

#[test]
fn contrary_aggressor_does_not_fill() {
    let buy_outcome = FillSimulator::new(FillProfile::Optimistic).check_fill(
        &order(0.50, 10.0, 100, true),
        &[print(150, 0.49, 100.0, Aggressor::Buy)],
        ExecutionLatency::new(0),
    );
    let sell_outcome = FillSimulator::new(FillProfile::Optimistic).check_fill(
        &order(0.50, 10.0, 100, false),
        &[print(150, 0.51, 100.0, Aggressor::Sell)],
        ExecutionLatency::new(0),
    );

    assert!(!buy_outcome.filled);
    assert!(!sell_outcome.filled);
}

#[test]
fn sell_orders_fill_from_aggressive_buys_through_upward() {
    let outcome = FillSimulator::new(FillProfile::Optimistic).check_fill(
        &order(0.50, 10.0, 100, false),
        &[print(150, 0.51, 3.0, Aggressor::Buy)],
        ExecutionLatency::new(0),
    );

    assert!(outcome.filled);
    assert_eq!(outcome.fill_fraction, 0.3);
    assert_eq!(outcome.used_aggressive_qty, 3.0);
}

#[test]
fn fill_timestamp_is_the_completing_print_after_arrival() {
    let outcome = FillSimulator::new(FillProfile::Conservative).check_fill(
        &order(0.50, 10.0, 100, true),
        &[
            print(150, 0.49, 10.0, Aggressor::Sell),
            print(200, 0.48, 10.0, Aggressor::Sell),
        ],
        ExecutionLatency::new(50),
    );

    assert!(outcome.filled);
    assert_eq!(outcome.fill_ts_ms, Some(200));
    assert!(outcome.fill_ts_ms.unwrap() >= 150);
}

#[test]
fn fill_quantity_is_bounded_by_aggressive_volume_and_order_size() {
    let outcome = FillSimulator::new(FillProfile::Optimistic).check_fill(
        &order(0.50, 10.0, 100, true),
        &[print(150, 0.49, 3.0, Aggressor::Sell)],
        ExecutionLatency::new(0),
    );

    assert_eq!(outcome.used_aggressive_qty, 3.0);
    assert!(outcome.used_aggressive_qty <= 3.0);
    assert!(outcome.used_aggressive_qty <= 10.0);
    assert_eq!(outcome.fill_fraction * 10.0, outcome.used_aggressive_qty);
}

#[test]
fn fill_fractions_are_monotone_conservative_base_optimistic() {
    let prints = [
        print(150, 0.49, 10.0, Aggressor::Sell),
        print(200, 0.48, 10.0, Aggressor::Sell),
    ];
    let resting = order(0.50, 10.0, 100, true);
    let latency = ExecutionLatency::new(0);
    let conservative = FillSimulator::new(FillProfile::Conservative)
        .check_fill(&resting, &prints, latency)
        .fill_fraction;
    let base = FillSimulator::new(FillProfile::Base)
        .check_fill(&resting, &prints, latency)
        .fill_fraction;
    let optimistic = FillSimulator::new(FillProfile::Optimistic)
        .check_fill(&resting, &prints, latency)
        .fill_fraction;

    assert!(conservative <= base);
    assert!(base <= optimistic);
}

#[test]
fn unknown_aggressor_counts_half_volume() {
    let outcome = FillSimulator::new(FillProfile::Optimistic).check_fill(
        &order(0.50, 10.0, 100, true),
        &[print(150, 0.49, 4.0, Aggressor::Unknown)],
        ExecutionLatency::new(0),
    );

    assert!(outcome.filled);
    assert_eq!(outcome.used_aggressive_qty, 2.0);
    assert_eq!(outcome.fill_fraction, 0.2);
}

#[test]
fn invalid_quantities_are_ignored_without_panicking() {
    let prints = [
        FillPrint {
            ts_ms: 150,
            price: 0.49,
            qty: -1.0,
            aggressor: Aggressor::Sell,
        },
        FillPrint {
            ts_ms: 160,
            price: 0.49,
            qty: f64::NAN,
            aggressor: Aggressor::Sell,
        },
    ];
    let outcome = FillSimulator::new(FillProfile::Optimistic).check_fill(
        &order(0.50, 10.0, 100, true),
        &prints,
        ExecutionLatency::new(0),
    );

    assert!(!outcome.filled);
    assert_eq!(outcome.used_aggressive_qty, 0.0);
}

#[test]
fn queue_and_through_multiple_settings_are_bounded() {
    assert_eq!(
        FillSimulator::new(FillProfile::Base)
            .with_queue_ahead(-1.0)
            .queue_ahead,
        0.0
    );
    assert_eq!(
        FillSimulator::new(FillProfile::Base)
            .with_queue_ahead(2.0)
            .queue_ahead,
        1.0
    );
    assert_eq!(
        FillSimulator::new(FillProfile::Conservative)
            .with_through_multiple(3.0)
            .through_multiple,
        3.0
    );
}
