use jevtrader::replay::{
    MarketConstraints, STANDARD_STAKE_USD, Side, SizedOrder, TradeEpisode, size_entry, size_hedge,
    size_standard_entry,
};

fn constraints() -> MarketConstraints {
    MarketConstraints::new(0.01, 0.01, 1.0).expect("valid constraints")
}

fn assert_close(left: f64, right: f64) {
    assert!((left - right).abs() < 1e-9, "left={left}, right={right}");
}

#[test]
fn standard_five_usdc_entry_sizes_to_twelve_and_a_half_shares() {
    let sized = size_standard_entry(0.40, &constraints()).expect("standard entry should size");

    assert_close(STANDARD_STAKE_USD, 5.0);
    assert_close(sized.limit_price, 0.40);
    assert_close(sized.shares, 12.5);
    assert_close(sized.intended_stake_usd, 5.0);
    assert_close(sized.actual_notional_usd, 5.0);
    assert_close(sized.rounding_delta_usd, 0.0);
}

#[test]
fn price_rounds_to_tick_before_shares_are_rounded_down() {
    let c = MarketConstraints::new(0.01, 0.30, 1.0).expect("valid constraints");
    let sized = size_entry(0.404, 5.0, &c).expect("entry should size");

    assert_close(sized.limit_price, 0.40);
    assert_close(sized.shares, 12.3);
    assert_close(sized.actual_notional_usd, 4.92);

    let half_up = size_entry(0.405, 5.0, &c).expect("half-up price should size");
    assert_close(half_up.limit_price, 0.41);
}

#[test]
fn share_step_is_exact_and_remainder_is_reported() {
    let c = MarketConstraints::new(0.01, 0.10, 1.0).expect("valid constraints");
    let sized = size_entry(0.41, 5.0, &c).expect("entry should size");

    assert_close(sized.shares, 12.1);
    assert!((sized.shares / c.size_step - 121.0).abs() < 1e-9);
    assert!(sized.rounding_delta_usd > 0.0);
    assert_close(sized.rounding_delta_usd, 5.0 - (12.1 * 0.41));
}

#[test]
fn below_minimum_is_rejected_without_adjustment() {
    let c = MarketConstraints::new(0.01, 0.01, 1.0).expect("valid constraints");
    let error = size_entry(0.50, 0.40, &c).expect_err("below-minimum order must fail");

    assert!(error.contains("min_order_size"), "error was: {error}");
}

#[test]
fn invalid_prices_and_stakes_are_rejected() {
    let c = constraints();

    for price in [0.0, -0.1, 1.0, f64::NAN, f64::INFINITY] {
        assert!(
            size_entry(price, 5.0, &c).is_err(),
            "price={price} should be rejected"
        );
    }
    for stake in [0.0, -1.0, f64::NAN, f64::INFINITY] {
        assert!(
            size_entry(0.40, stake, &c).is_err(),
            "stake={stake} should be rejected"
        );
    }
}

#[test]
fn hedge_preserves_entry_shares_instead_of_resizing_to_five_usdc() {
    let hedge = size_hedge(12.5, 0.52, &constraints()).expect("hedge should size");

    assert_close(hedge.limit_price, 0.52);
    assert_close(hedge.shares, 12.5);
    assert_close(hedge.intended_stake_usd, 6.50);
    assert_close(hedge.actual_notional_usd, 6.50);
}

#[test]
fn apply_sizing_updates_unfilled_episode_and_rejects_post_fill_mismatch() {
    let mut trade = TradeEpisode::new(
        "episode-sizing",
        "lead-lag-v1",
        "market-1",
        "BTC",
        "5s",
        1_000,
        900,
        100,
        50,
        Side::BuyYes,
        0.404,
        5.0,
    )
    .expect("valid episode");
    let sizing_constraints = MarketConstraints::new(0.01, 0.10, 1.0).expect("valid constraints");
    let sized = size_entry(0.404, 5.0, &sizing_constraints).expect("entry should size");

    trade
        .apply_sizing(&sized)
        .expect("unfilled episode can be sized");
    assert_close(trade.limit_price, sized.limit_price);
    assert_close(trade.stake_usd, sized.intended_stake_usd);
    assert_close(trade.shares, sized.shares);
    assert_close(trade.intended_stake_usd, sized.intended_stake_usd);
    assert_close(trade.actual_notional_usd, sized.actual_notional_usd);
    assert_close(trade.rounding_delta_usd, sized.rounding_delta_usd);

    trade
        .apply_fill(1_200, 0.40, sized.shares)
        .expect("fill arrives after order arrival");
    let mismatch = SizedOrder {
        limit_price: sized.limit_price,
        shares: sized.shares - 0.1,
        intended_stake_usd: sized.intended_stake_usd,
        actual_notional_usd: sized.actual_notional_usd,
        rounding_delta_usd: sized.rounding_delta_usd,
    };
    let before = trade.clone();
    let error = trade
        .apply_sizing(&mismatch)
        .expect_err("filled episode cannot change share sizing");

    assert!(error.contains("filled episode"), "error was: {error}");
    assert_eq!(trade, before);
}

#[test]
fn constraints_reject_non_finite_or_non_positive_values() {
    for (tick, step, min) in [
        (0.0, 0.01, 1.0),
        (f64::NAN, 0.01, 1.0),
        (0.01, 0.0, 1.0),
        (0.01, f64::INFINITY, 1.0),
        (0.01, 0.01, -1.0),
        (0.01, 0.01, f64::NAN),
    ] {
        assert!(
            MarketConstraints::new(tick, step, min).is_err(),
            "tick={tick}, step={step}, min={min} should be rejected"
        );
    }
}
