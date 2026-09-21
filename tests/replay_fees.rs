use jevtrader::replay::{
    FeeRegime, Side, TradeEpisode, apply_to_episode, current_crypto_regime, fee_for_fill,
    historical_regime, upside_with_rebate, zero_regime,
};

fn episode() -> TradeEpisode {
    TradeEpisode::new(
        "episode-fees",
        "lead-lag-v1",
        "market-1",
        "BTC",
        "5s",
        1_000,
        900,
        100,
        50,
        Side::BuyYes,
        0.40,
        10.0,
    )
    .expect("valid episode")
}

fn filled_episode() -> TradeEpisode {
    let mut trade = episode();
    trade
        .apply_fill(2_000, 0.40, 25.0)
        .expect("fill arrives after order arrival");
    trade
}

fn assert_close(left: f64, right: f64) {
    assert!((left - right).abs() < 1e-9, "left={left}, right={right}");
}

#[test]
fn apply_to_episode_preserves_historical_net_invariant_for_multiple_notionals() {
    let historical = historical_regime(0.01, 0.07, 0.0, "historical").expect("valid regime");
    let current = current_crypto_regime();

    for (gross, fill_notional, exit_notional) in
        [(1.0, 1.0, 1.0), (5.0, 5.0, 5.0), (20.0, 100.0, 250.0)]
    {
        let mut trade = filled_episode();
        apply_to_episode(
            &mut trade,
            gross,
            fill_notional,
            exit_notional,
            false,
            &historical,
            &current,
        )
        .expect("fee application should succeed");

        assert_close(
            trade.net_pnl_usd,
            trade.gross_pnl_usd - trade.fees_usd + trade.rebates_usd,
        );
        assert_eq!(trade.rebates_usd, 0.0);
    }
}

#[test]
fn current_crypto_maker_is_free_and_taker_costs_seven_percent() {
    let current = current_crypto_regime();

    assert_eq!(fee_for_fill(10.0, true, &current).expect("maker fee"), 0.0);
    assert_close(
        fee_for_fill(10.0, false, &current).expect("taker fee"),
        0.70,
    );
}

#[test]
fn historical_and_current_pnl_are_separate_and_historical_owns_principal() {
    let historical = historical_regime(0.01, 0.02, 0.0, "historical").expect("valid regime");
    let current = current_crypto_regime();
    let mut trade = filled_episode();

    apply_to_episode(&mut trade, 5.0, 5.0, 5.0, false, &historical, &current)
        .expect("fee application should succeed");

    assert_close(trade.pnl_historical_usd.expect("historical PnL"), 4.80);
    assert_close(trade.pnl_current_usd.expect("current PnL"), 4.30);
    assert_close(trade.fees_usd, 0.20);
    assert_eq!(trade.rebates_usd, 0.0);
    assert_close(trade.net_pnl_usd, 4.80);
}

#[test]
fn rebate_is_display_only_and_never_enters_ledger_principal() {
    let historical =
        historical_regime(0.01, 0.10, 0.25, "historical-with-rebate").expect("valid regime");
    let current = zero_regime();
    let mut trade = filled_episode();

    apply_to_episode(&mut trade, 5.0, 5.0, 5.0, false, &historical, &current)
        .expect("fee application should succeed");

    assert_close(trade.fees_usd, 1.0);
    assert_eq!(trade.rebates_usd, 0.0);
    assert_close(trade.net_pnl_usd, 4.0);
    assert_close(upside_with_rebate(5.0, 1.0, 0.25), 4.25);
}

#[test]
fn invalid_fill_notional_fails_without_mutating_episode() {
    let regime = current_crypto_regime();

    assert!(fee_for_fill(0.0, false, &regime).is_err());
    for invalid in [-1.0, f64::NAN, f64::INFINITY] {
        let mut trade = episode();
        let before = trade.clone();
        assert!(apply_to_episode(&mut trade, 1.0, invalid, 1.0, false, &regime, &regime,).is_err());
        assert_eq!(trade, before);
    }
}

#[test]
fn invalid_rates_are_rejected_by_the_historical_constructor() {
    for invalid in [1.0, -0.1, f64::NAN] {
        assert!(historical_regime(invalid, 0.0, 0.0, "invalid").is_err());
        assert!(historical_regime(0.0, invalid, 0.0, "invalid").is_err());
        assert!(historical_regime(0.0, 0.0, invalid, "invalid").is_err());
    }
}

#[test]
fn no_fill_with_zero_notionals_records_zero_pnl() {
    let regime = zero_regime();
    let mut trade = episode();
    let before = trade.clone();

    assert!(apply_to_episode(&mut trade, 0.0, 1.0, 1.0, true, &regime, &regime).is_err());
    assert_eq!(trade, before);

    apply_to_episode(&mut trade, 0.0, 0.0, 0.0, true, &regime, &regime)
        .expect("zero-notional no-fill is supported");

    assert_eq!(trade.pnl_historical_usd, Some(0.0));
    assert_eq!(trade.pnl_current_usd, Some(0.0));
    assert_eq!(trade.gross_pnl_usd, 0.0);
    assert_eq!(trade.fees_usd, 0.0);
    assert_eq!(trade.rebates_usd, 0.0);
    assert_eq!(trade.net_pnl_usd, 0.0);
}

#[test]
fn fee_regime_serializes_with_its_rates_and_label() {
    let regime = FeeRegime {
        maker_rate: 0.01,
        taker_rate: 0.07,
        maker_rebate: 0.0,
        label: "historical",
    };
    let json = serde_json::to_string(&regime).expect("fee regime serializes");

    assert!(json.contains("\"maker_rate\":0.01"));
    assert!(json.contains("\"label\":\"historical\""));
}
