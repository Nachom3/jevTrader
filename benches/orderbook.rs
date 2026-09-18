use criterion::{Criterion, criterion_group, criterion_main};
use jevtrader::domain::PriceTicks;
use jevtrader::polymarket::{BookSide, Level, OrderBook};
use std::hint::black_box;

fn price(value: f64) -> PriceTicks {
    PriceTicks::from_f64(value)
}

fn snapshot_levels() -> (Vec<Level>, Vec<Level>) {
    let bids = (0..32)
        .map(|index| (price(0.50 - index as f64 * 0.001), 1_000 + index))
        .collect();
    let asks = (0..32)
        .map(|index| (price(0.51 + index as f64 * 0.001), 2_000 + index))
        .collect();
    (bids, asks)
}

fn benchmark_orderbook_snapshot_and_delta(c: &mut Criterion) {
    let (bids, asks) = snapshot_levels();
    let deltas = (0..64)
        .map(|index| {
            (
                if index % 2 == 0 {
                    BookSide::Bid
                } else {
                    BookSide::Ask
                },
                price(if index % 2 == 0 {
                    0.50 - (index / 2) as f64 * 0.001
                } else {
                    0.51 + (index / 2) as f64 * 0.001
                }),
                3_000 + index as u64,
            )
        })
        .collect::<Vec<_>>();
    let mut book = OrderBook::default();

    c.bench_function("orderbook/snapshot_plus_64_deltas", |b| {
        b.iter(|| {
            book.apply_snapshot(&bids, &asks);
            for &(side, level_price, quantity) in &deltas {
                book.apply_delta(side, level_price, quantity);
            }
            black_box(book.book_hash())
        })
    });
}

criterion_group!(benches, benchmark_orderbook_snapshot_and_delta);
criterion_main!(benches);
