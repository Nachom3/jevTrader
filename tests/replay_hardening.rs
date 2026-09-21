use jevtrader::replay::{ExitType, Portfolio, Side, TradeEpisode, settle_hedge_pair};
use jevtrader::storage::StorageEvent;

fn episode(id: &str, side: Side) -> TradeEpisode {
    TradeEpisode::new(
        id,
        "strategy-v1",
        "market-1",
        "BTC",
        "5m",
        1_000,
        900,
        50,
        50,
        side,
        0.40,
        10.0,
    )
    .expect("valid episode")
}

fn filled_episode(id: &str, side: Side, fill_ts_ms: i64) -> TradeEpisode {
    let mut trade = episode(id, side);
    trade
        .apply_fill(fill_ts_ms, 0.40, 25.0)
        .expect("fill arrives after order arrival");
    trade
}

#[test]
fn apply_fill_rejects_non_positive_non_finite_pre_arrival_and_duplicate_fills() {
    for (price, qty) in [
        (f64::NAN, 1.0),
        (f64::INFINITY, 1.0),
        (0.0, 1.0),
        (-0.1, 1.0),
        (0.4, f64::NAN),
        (0.4, f64::INFINITY),
        (0.4, 0.0),
        (0.4, -1.0),
    ] {
        let mut trade = episode("invalid", Side::BuyYes);
        assert!(
            trade
                .apply_fill(trade.order_arrival_ts_ms, price, qty)
                .is_err()
        );
        assert_eq!(trade.fill_ts_ms, None);
        assert_eq!(trade.fill_price, None);
        assert_eq!(trade.fill_qty, None);
    }

    let mut early = episode("early", Side::BuyYes);
    assert!(
        early
            .apply_fill(early.order_arrival_ts_ms - 1, 0.4, 1.0)
            .is_err()
    );
    assert_eq!(early.fill_ts_ms, None);

    let mut duplicate = episode("duplicate", Side::BuyYes);
    duplicate
        .apply_fill(early.order_arrival_ts_ms, 0.4, 1.0)
        .expect("first fill should apply");
    let before_duplicate = duplicate.clone();
    assert!(
        duplicate
            .apply_fill(early.order_arrival_ts_ms, 0.4, 1.0)
            .is_err()
    );
    assert_eq!(duplicate, before_duplicate);
}

#[test]
fn hedge_with_fill_before_entry_is_a_noop() {
    let mut entry = filled_episode("entry", Side::BuyYes, 1_200);
    let mut hedge = filled_episode("hedge", Side::BuyNo, 1_100);
    let entry_before = entry.clone();
    let hedge_before = hedge.clone();

    settle_hedge_pair(&mut entry, &mut hedge);

    assert_eq!(entry, entry_before);
    assert_eq!(hedge, hedge_before);

    let mut valid_entry = filled_episode("valid-entry", Side::BuyYes, 1_200);
    let mut valid_hedge = filled_episode("valid-hedge", Side::BuyNo, 1_300);
    settle_hedge_pair(&mut valid_entry, &mut valid_hedge);
    assert_eq!(
        valid_entry.hedge_pair_id.as_deref(),
        Some("hedge:valid-entry")
    );
    assert_eq!(valid_hedge.hedge_pair_id, valid_entry.hedge_pair_id);
}

#[test]
fn portfolio_keeps_gross_and_net_cash_pnl_separate() {
    let mut trade = filled_episode("accounting", Side::BuyYes, 1_100);
    trade
        .apply_exit(ExitType::Sell, 1_200, 0.80)
        .expect("exit after fill");
    trade
        .settle_accounting(10.0, 1.25, 0.25, 9.0)
        .expect("accounting invariant");

    let mut portfolio = Portfolio::new();
    portfolio.apply_episode(&trade);

    assert_eq!(portfolio.gross_cash_pnl_usd(), 10.0);
    assert_eq!(portfolio.net_cash_pnl_usd(), 9.0);
    assert_eq!(portfolio.cash_pnl_usd(), 9.0);
    assert_eq!(portfolio.cash_pnl_usd, 9.0);
}

#[test]
fn trade_episode_storage_row_serializes_provenance_and_roundtrips_ledger_fields() {
    let mut trade = filled_episode("storage", Side::BuyYes, 1_100);
    trade.set_fill_profile("conservative");
    trade.set_is_maker(true);
    trade.set_fee_regime("historical-2026");
    trade.set_hedge_pair_id("hedge:storage");
    trade.set_prompt_version("prompt-v3");
    trade.set_jev_model("jev-latest");

    let event = StorageEvent::trade_episode(&trade);
    let row_json = match &event {
        StorageEvent::TradeEpisode { row } => serde_json::to_string(row).expect("serialize row"),
        _ => panic!("expected a trade episode event"),
    };
    let row_value: serde_json::Value =
        serde_json::from_str(&row_json).expect("row JSON should roundtrip");
    let ledger_value = serde_json::to_value(&trade).expect("serialize ledger");

    for field in [
        "episode_id",
        "strategy_version",
        "signal_ts_ms",
        "order_arrival_ts_ms",
        "side",
        "fill_ts_ms",
        "fill_price",
        "fill_qty",
        "exit_type",
        "gross_pnl_usd",
        "fees_usd",
        "rebates_usd",
        "net_pnl_usd",
    ] {
        assert_eq!(
            row_value.get(field),
            ledger_value.get(field),
            "field={field}"
        );
    }
    assert_eq!(row_value["fill_profile"], "conservative");
    assert_eq!(row_value["is_maker"], true);
    assert_eq!(row_value["fee_regime"], "historical-2026");
    assert_eq!(row_value["hedge_pair_id"], "hedge:storage");
    assert_eq!(row_value["prompt_version"], "prompt-v3");
    assert_eq!(row_value["jev_model"], "jev-latest");
}
