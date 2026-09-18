use criterion::{Criterion, criterion_group, criterion_main};
use jevtrader::state::{ExternalTick, OrderFlowAggregates, VenueMicroprices, build_features};
use jevtrader::strategy::lead_lag::PolySnapshot;
use std::hint::black_box;

fn five_minute_ticks() -> Vec<ExternalTick> {
    (0..=3_000)
        .map(|index| {
            let ts_ms = index * 100;
            ExternalTick {
                price: 100.0 + (index % 37) as f64 * 0.01,
                ts_ms,
            }
        })
        .collect()
}

fn poly_snapshot() -> PolySnapshot {
    PolySnapshot {
        yes_bid: 0.40,
        yes_ask: 0.42,
        bid_depth: 100.0,
        ask_depth: 90.0,
        spread: 0.02,
        book_imbalance: 0.05,
        last_trade_price: 0.41,
        price_1s_ago: 0.40,
        price_5s_ago: 0.39,
        price_30s_ago: 0.38,
    }
}

fn benchmark_feature_builder(c: &mut Criterion) {
    let ticks = five_minute_ticks();
    let poly = poly_snapshot();
    let venues = VenueMicroprices {
        binance: 100.1,
        coinbase: 100.0,
        perp: 100.2,
        perp_basis_pct: 0.2,
    };
    let order_flow = OrderFlowAggregates {
        buy_vol_1s: 4.0,
        sell_vol_1s: 3.0,
        ofi_1s: 1.0,
        ofi_5s: 2.0,
        imbalance: 0.1,
        aggressive_buy_ratio: 0.6,
    };

    c.bench_function("feature_builder/5m_irregular_series", |b| {
        b.iter(|| {
            black_box(build_features(
                black_box(&ticks),
                black_box(&poly),
                black_box(105.0),
                black_box(venues),
                black_box(order_flow),
            ))
        })
    });
}

criterion_group!(benches, benchmark_feature_builder);
criterion_main!(benches);
