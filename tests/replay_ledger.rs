use jevtrader::replay::{ExitType, Side, TradeEpisode};

fn episode() -> TradeEpisode {
    TradeEpisode::new_with_exit_submit_latency(
        "episode-1",
        "lead-lag-v1",
        "market-1",
        "BTC",
        "5s",
        1_000,
        900,
        100,
        50,
        25,
        Side::BuyYes,
        0.40,
        10.0,
    )
    .expect("valid episode")
}

#[test]
fn accounting_preserves_gross_minus_fees_plus_rebates() {
    let cases = [(10.0, 0.25, 0.05), (-2.0, 0.10, 0.20), (0.0, 0.0, 0.0)];

    for (gross, fees, rebates) in cases {
        let mut trade = episode();
        let net = gross - fees + rebates;
        trade
            .settle_accounting(gross, fees, rebates, net)
            .expect("accounting should satisfy the invariant");
        assert_eq!(trade.gross_pnl_usd, gross);
        assert_eq!(trade.fees_usd, fees);
        assert_eq!(trade.rebates_usd, rebates);
        assert_eq!(trade.net_pnl_usd, net);
    }

    let mut trade = episode();
    assert!(trade.settle_accounting(1.0, 0.1, 0.0, 0.8).is_err());
    assert_eq!(trade.net_pnl_usd, 0.0, "failed settlement must not mutate");
}

#[test]
fn no_fill_has_no_pnl_capital_time_or_fill_exit_details() {
    let mut trade = episode();
    trade
        .apply_exit(ExitType::NoFill, 2_000, 0.42)
        .expect("NoFill is valid before a fill");

    assert!(trade.is_no_fill());
    assert_eq!(trade.net_pnl_usd, 0.0);
    assert_eq!(trade.capital_seconds_usd_s, 0.0);
    assert_eq!(trade.realized_capital_seconds(), 0.0);
    assert_eq!(trade.fill_ts_ms, None);
    assert_eq!(trade.fill_price, None);
    assert_eq!(trade.fill_qty, None);
    assert_eq!(trade.exit_price, None);
    assert_eq!(trade.exit_ts_ms, None);
}

#[test]
fn fill_before_order_arrival_fails_without_mutating() {
    let mut trade = episode();
    let before = trade.clone();

    let error = trade
        .apply_fill(trade.order_arrival_ts_ms - 1, 0.40, 25.0)
        .expect_err("fill before arrival must fail");

    assert!(error.contains("precedes order arrival"));
    assert_eq!(trade, before);
}

#[test]
fn entry_arrival_uses_checked_signal_jev_and_submit_timestamps() {
    let trade = episode();
    assert_eq!(trade.order_arrival_ts_ms, 1_000 + 100 + 50);

    let overflow = TradeEpisode::new(
        "overflow",
        "lead-lag-v1",
        "market-1",
        "BTC",
        "5s",
        i64::MAX,
        i64::MAX,
        1,
        0,
        Side::BuyYes,
        0.40,
        10.0,
    );
    assert!(overflow.is_err(), "entry arrival overflow must fail");
}

#[test]
fn exit_arrival_uses_checked_signal_and_submit_timestamps() {
    let mut trade = episode();
    trade
        .request_exit(2_000, 35)
        .expect("exit request should have a checked arrival");
    assert_eq!(trade.exit_signal_ts_ms, Some(2_000));
    assert_eq!(trade.exit_arrival_ts_ms, Some(2_035));
    assert_eq!(trade.exit_submit_latency_ms, 35);

    let before = trade.clone();
    assert!(trade.request_exit(i64::MAX, 1).is_err());
    assert_eq!(trade, before, "overflow must not mutate the exit request");
}

#[test]
fn exit_fill_before_exit_arrival_fails_without_mutating() {
    let mut trade = episode();
    trade
        .apply_fill(1_200, 0.40, 25.0)
        .expect("entry fill should arrive");
    trade
        .request_exit(2_000, 50)
        .expect("exit request should arrive at 2050");
    let before = trade.clone();

    let error = trade
        .apply_exit_fill(2_049, 0.42)
        .expect_err("exit fill before arrival must fail");

    assert!(error.contains("precedes exit arrival"));
    assert_eq!(trade, before);
}

#[test]
fn resolution_before_fill_fails_without_mutating() {
    let mut trade = episode();
    trade
        .apply_fill(1_200, 0.40, 25.0)
        .expect("entry fill should arrive");
    let before = trade.clone();

    let error = trade
        .set_resolution(1_199)
        .expect_err("resolution before fill must fail");

    assert!(error.contains("precedes fill timestamp"));
    assert_eq!(trade, before);
}

#[test]
fn identical_episodes_have_identical_json() {
    let left = episode();
    let right = episode();

    let left_json = serde_json::to_string(&left).expect("serialize left episode");
    let right_json = serde_json::to_string(&right).expect("serialize right episode");

    assert_eq!(left_json, right_json);
    assert!(left_json.contains("\"buy_yes\""));
    assert!(left_json.contains("\"order_arrival_ts_ms\""));
    assert!(left_json.contains("\"exit_signal_ts_ms\""));
    assert!(left_json.contains("\"exit_arrival_ts_ms\""));
    assert!(left_json.contains("\"exit_fill_ts_ms\""));
    assert!(left_json.contains("\"resolution_at_ms\""));
    assert!(left_json.contains("\"exit_submit_latency_ms\":25"));
}

#[test]
fn shares_are_stake_divided_by_limit_and_prices_are_positive() {
    let trade = TradeEpisode::new(
        "episode-2",
        "lead-lag-v1",
        "market-1",
        "ETH",
        "1m",
        0,
        0,
        0,
        0,
        Side::BuyNo,
        0.25,
        10.0,
    )
    .expect("valid episode");
    assert_eq!(trade.shares, 40.0);

    for (limit_price, stake_usd) in [(0.0, 10.0), (-0.1, 10.0), (0.5, 0.0), (0.5, -1.0)] {
        assert!(
            TradeEpisode::new(
                "episode-invalid",
                "lead-lag-v1",
                "market-1",
                "ETH",
                "1m",
                0,
                0,
                0,
                0,
                Side::BuyYes,
                limit_price,
                stake_usd,
            )
            .is_err(),
            "limit={limit_price}, stake={stake_usd} should be rejected"
        );
    }
}

#[test]
fn exits_require_a_fill_and_nofill_cannot_follow_one() {
    let mut trade = episode();
    assert!(trade.apply_exit(ExitType::Sell, 1_200, 0.45).is_err());

    trade
        .apply_fill(1_200, 0.40, 25.0)
        .expect("fill arrives after order arrival");
    assert!(trade.apply_exit(ExitType::NoFill, 1_300, 0.40).is_err());
    assert_eq!(trade.exit_type, ExitType::NoFill);
    assert_eq!(trade.exit_ts_ms, None);
}

#[test]
fn capital_seconds_use_fill_to_exit_elapsed_milliseconds() {
    let mut trade = episode();
    trade
        .apply_fill(2_000, 0.40, 25.0)
        .expect("fill arrives after order arrival");
    trade
        .apply_exit(ExitType::Sell, 4_500, 0.42)
        .expect("exit is after fill");

    assert_eq!(trade.realized_capital_seconds(), 25.0);
    assert_eq!(trade.capital_seconds_usd_s, 25.0);
}
