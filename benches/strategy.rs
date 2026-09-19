use criterion::{Criterion, criterion_group, criterion_main};
use jevtrader::config::QuoteThresholds;
use jevtrader::domain::{PriceTicks, TickSize};
use jevtrader::jev::{TickDistribution, V1Signal};
use jevtrader::polymarket::OrderBook;
use jevtrader::strategy::quote::decide_quote;
use std::hint::black_box;

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

fn benchmark_decide_quote(c: &mut Criterion) {
    let signal = signal();
    let book = book();
    let thresholds = QuoteThresholds::default();
    let tick_size = TickSize::from_f64(0.01);

    c.bench_function("strategy/decide_quote", |b| {
        b.iter(|| {
            black_box(decide_quote(
                black_box(&signal),
                black_box(&book),
                black_box(&thresholds),
                black_box(false),
                black_box(tick_size),
                black_box(25),
            ))
        })
    });
}

criterion_group!(benches, benchmark_decide_quote);
criterion_main!(benches);
