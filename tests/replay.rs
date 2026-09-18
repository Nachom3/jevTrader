use jevtrader::domain::{PriceTicks, TradeSide};
use jevtrader::execution::{PaperBook, PaperOrder, TopOfBookUpdate, markout};

fn price(value: f64) -> PriceTicks {
    PriceTicks::from_f64(value)
}

fn update(
    at_ms: i64,
    best_bid: f64,
    best_ask: f64,
    ask_size: u64,
    marketable_size: u64,
) -> TopOfBookUpdate {
    TopOfBookUpdate::new(
        at_ms,
        Some(price(best_bid)),
        100,
        Some(price(best_ask)),
        ask_size,
        marketable_size,
    )
}

#[test]
fn maker_replay_partial_touches_then_trade_through_and_markouts() {
    let tape = [
        update(0, 0.40, 0.60, 100, 0),
        update(1_000, 0.41, 0.60, 100, 10),
        update(2_000, 0.41, 0.60, 100, 10),
        update(3_000, 0.40, 0.59, 100, 0),
        update(4_000, 0.41, 0.59, 100, 10),
        update(5_000, 0.42, 0.58, 100, 0),
        update(6_000, 0.43, 0.57, 100, 0),
        update(7_000, 0.44, 0.56, 100, 0),
        update(8_000, 0.44, 0.55, 100, 0),
        update(9_000, 0.44, 0.40, 4, 0),
        update(10_000, 0.44, 0.40, 100, 0),
    ];

    let mut book = PaperBook::new(0.2).expect("valid touch ratio");
    assert!(book.replay(&tape[..1]).is_empty());

    let order = PaperOrder::new(7, TradeSide::Buy, price(0.41), 10, 500).expect("valid buy");
    book.place(order).expect("order should rest");
    assert_eq!(book.resting_count(), 1, "posting must not match instantly");

    let fills = book.replay(&tape.as_slice()[1..]);
    assert_eq!(
        fills.iter().map(|fill| fill.size).collect::<Vec<_>>(),
        vec![2, 2, 2, 4],
        "touches are partial and the crossing update takes the remainder"
    );
    assert!(fills[..3].iter().all(|fill| fill.maker));
    assert_eq!(fills[3].filled_at_ms, 9_000);
    assert_eq!(fills[3].size, 4);
    assert_eq!(
        book.resting_count(),
        0,
        "trade-through must complete the order"
    );

    let fill_price = fills[0].price;
    assert!(markout(fill_price, price(0.42)) > 0.0);
    assert!(markout(fill_price, price(0.43)) > 0.0);
    assert!(markout(fill_price, price(0.44)) > 0.0);
}
