use jevtrader::replay::fills::{Aggressor, FillPrint};
use jevtrader::replay::{
    ExecutionLatency, ExitType, FillProfile, FillSimulator, Portfolio, RestingOrder, Side,
    TradeEpisode, apply_to_episode, current_crypto_regime, fee_for_fill, hedge_quote,
    historical_regime, merge_pair, settle_hedge_pair, settle_resolution, should_hedge_profit,
};

const EPSILON: f64 = 1e-9;

fn assert_close(actual: f64, expected: f64) {
    assert!(
        (actual - expected).abs() <= EPSILON,
        "actual={actual}, expected={expected}"
    );
}

fn print(ts_ms: i64, price: f64, qty: f64, aggressor: Aggressor) -> FillPrint {
    FillPrint::new(ts_ms, price, qty, aggressor)
}

fn simulate_conservative_fill(
    episode: &mut TradeEpisode,
    prints: &[FillPrint],
) -> jevtrader::replay::FillOutcome {
    let order = RestingOrder {
        price: episode.limit_price,
        size: episode.shares,
        resting_from_ms: episode.order_arrival_ts_ms,
        side_buy: true,
    };
    let available_aggressive_qty: f64 = prints
        .iter()
        .filter(|tape_print| tape_print.aggressor == Aggressor::Sell)
        .map(|tape_print| tape_print.qty)
        .sum();
    let outcome = FillSimulator::new(FillProfile::Conservative).check_fill(
        &order,
        prints,
        ExecutionLatency::new(0),
    );

    assert_eq!(outcome.profile, FillProfile::Conservative);
    assert!(outcome.filled);
    assert!(outcome.used_aggressive_qty <= available_aggressive_qty + EPSILON);
    assert!(outcome.used_aggressive_qty <= episode.shares + EPSILON);

    let fill_ts_ms = outcome
        .fill_ts_ms
        .expect("a full fill has a completing print");
    let fill_qty = outcome.fill_fraction * episode.shares;
    episode
        .apply_fill(fill_ts_ms, outcome.fill_price, fill_qty)
        .expect("simulated fill is after order arrival");
    assert_close(fill_qty, outcome.used_aggressive_qty);
    outcome
}

fn apply_historical_and_current_fees(episode: &mut TradeEpisode, is_maker: bool) {
    let historical = historical_regime(0.0, 0.0, 0.0, "historical-maker-zero")
        .expect("fixed historical fee regime");
    let current = current_crypto_regime();
    let fill_price = episode.fill_price.unwrap_or(0.0);
    let fill_qty = episode.fill_qty.unwrap_or(0.0);
    let exit_price = episode.exit_price.unwrap_or(0.0);
    let fill_notional = fill_price * fill_qty;
    let exit_notional = exit_price * fill_qty;
    let gross = episode.gross_pnl_usd;

    apply_to_episode(
        episode,
        gross,
        fill_notional,
        exit_notional,
        is_maker,
        &historical,
        &current,
    )
    .expect("fixed fee views should settle");
}

fn build_episodes() -> Vec<TradeEpisode> {
    let mut ep1 = TradeEpisode::new_with_exit_submit_latency(
        "EP1",
        "lead-lag-v1",
        "market-mini",
        "BTC",
        "5m",
        1_000,
        1_000,
        80,
        20,
        0,
        Side::BuyYes,
        0.40,
        5.0,
    )
    .expect("EP1 is valid");
    assert_eq!(ep1.order_arrival_ts_ms, 1_100);
    simulate_conservative_fill(
        &mut ep1,
        &[
            print(1_200, 0.39, 100.0, Aggressor::Sell),
            print(1_300, 0.38, 100.0, Aggressor::Sell),
        ],
    );
    settle_resolution(&mut ep1, true, 2_000, 1.0).expect("EP1 resolves YES");
    assert_close(ep1.gross_pnl_usd, 7.50);
    apply_historical_and_current_fees(&mut ep1, false);

    let mut ep2_entry = TradeEpisode::new_with_exit_submit_latency(
        "EP2-entry",
        "lead-lag-v1",
        "market-mini",
        "BTC",
        "5m",
        3_000,
        3_000,
        30,
        20,
        0,
        Side::BuyYes,
        0.45,
        4.50,
    )
    .expect("EP2 entry is valid");
    assert_eq!(ep2_entry.order_arrival_ts_ms, 3_050);
    simulate_conservative_fill(
        &mut ep2_entry,
        &[
            print(3_100, 0.44, 100.0, Aggressor::Sell),
            print(3_200, 0.43, 100.0, Aggressor::Sell),
        ],
    );

    let entry_price = ep2_entry.fill_price.expect("EP2 entry fill price");
    assert!(should_hedge_profit(entry_price, 0.52, 0.03));
    let quote = hedge_quote(&ep2_entry, 0.52).expect("EP2 hedge quote is valid");
    assert_eq!(quote.side, Side::BuyNo);
    assert_eq!(quote.limit_price, 0.52);
    assert_close(quote.qty, 10.0);
    assert!(entry_price + quote.limit_price <= 1.0 - 0.03 + EPSILON);

    let mut ep2_hedge = TradeEpisode::new_with_exit_submit_latency(
        "EP2-hedge",
        "lead-lag-v1",
        "market-mini",
        "BTC",
        "5m",
        3_300,
        3_300,
        30,
        20,
        0,
        quote.side,
        quote.limit_price,
        quote.qty * quote.limit_price,
    )
    .expect("EP2 hedge is valid");
    assert_eq!(ep2_hedge.shares, quote.qty);
    assert_eq!(ep2_hedge.order_arrival_ts_ms, 3_350);
    assert_ne!(ep2_hedge.order_arrival_ts_ms, quote.arrival_ts_ms);
    let hedge_outcome = simulate_conservative_fill(
        &mut ep2_hedge,
        &[
            print(3_400, 0.51, 100.0, Aggressor::Sell),
            print(3_500, 0.50, 100.0, Aggressor::Sell),
        ],
    );
    assert!(
        ep2_hedge.fill_ts_ms.expect("hedge fill timestamp")
            > ep2_entry.fill_ts_ms.expect("entry fill timestamp")
    );
    assert!(hedge_outcome.fill_ts_ms.unwrap() >= ep2_hedge.order_arrival_ts_ms);

    settle_hedge_pair(&mut ep2_entry, &mut ep2_hedge);
    assert_eq!(ep2_entry.exit_type, ExitType::Hedge);
    assert_eq!(ep2_hedge.exit_type, ExitType::Hedge);
    assert_eq!(ep2_entry.exit_ts_ms, ep2_hedge.exit_ts_ms);
    let hedge_exit_signal = ep2_entry
        .fill_ts_ms
        .expect("entry fill")
        .max(ep2_hedge.fill_ts_ms.expect("hedge fill"));
    for episode in [&ep2_entry, &ep2_hedge] {
        let signal = episode.exit_signal_ts_ms.expect("exit signal");
        let arrival = episode.exit_arrival_ts_ms.expect("exit arrival");
        let fill = episode.exit_fill_ts_ms.expect("exit fill");
        assert!(signal <= arrival && arrival <= fill);
    }
    assert_eq!(ep2_entry.exit_signal_ts_ms, Some(hedge_exit_signal));
    assert_eq!(ep2_hedge.exit_signal_ts_ms, Some(hedge_exit_signal));
    assert!(ep2_hedge.exit_arrival_ts_ms.unwrap() > ep2_entry.order_arrival_ts_ms);
    assert_close(ep2_entry.gross_pnl_usd, 0.30);
    assert_close(ep2_hedge.gross_pnl_usd, 0.0);

    let merge = merge_pair(0.45 * 10.0, 0.52 * 10.0, 10.0).expect("complete YES/NO pair");
    assert_close(merge.locked_pnl, 0.30);
    assert_close(
        ep2_entry.gross_pnl_usd + ep2_hedge.gross_pnl_usd,
        merge.locked_pnl,
    );
    apply_historical_and_current_fees(&mut ep2_entry, true);
    apply_historical_and_current_fees(&mut ep2_hedge, true);

    let mut ep3 = TradeEpisode::new_with_exit_submit_latency(
        "EP3",
        "lead-lag-v1",
        "market-mini",
        "BTC",
        "5m",
        4_000,
        4_000,
        20,
        10,
        0,
        Side::BuyYes,
        0.55,
        5.0,
    )
    .expect("EP3 is valid");
    assert_eq!(ep3.order_arrival_ts_ms, 4_030);
    let touch = [print(4_050, 0.55, 100.0, Aggressor::Sell)];
    let resting = RestingOrder {
        price: ep3.limit_price,
        size: ep3.shares,
        resting_from_ms: ep3.order_arrival_ts_ms,
        side_buy: true,
    };
    let conservative = FillSimulator::new(FillProfile::Conservative).check_fill(
        &resting,
        &touch,
        ExecutionLatency::new(0),
    );
    let base = FillSimulator::new(FillProfile::Base).check_fill(
        &resting,
        &touch,
        ExecutionLatency::new(0),
    );
    let optimistic = FillSimulator::new(FillProfile::Optimistic).check_fill(
        &resting,
        &touch,
        ExecutionLatency::new(0),
    );
    assert!(!conservative.filled);
    assert_eq!(conservative.fill_ts_ms, None);
    assert_eq!(conservative.used_aggressive_qty, 0.0);
    assert!(base.filled, "BASE should accept the fixed touch");
    assert!(
        optimistic.filled,
        "OPTIMISTIC should accept the fixed touch"
    );
    assert!(base.fill_fraction <= optimistic.fill_fraction);
    assert!(conservative.used_aggressive_qty <= touch[0].qty + EPSILON);

    ep3.apply_exit(ExitType::NoFill, 4_060, ep3.limit_price)
        .expect("NoFill remains a no-fill episode");
    apply_to_episode(
        &mut ep3,
        0.0,
        0.0,
        0.0,
        true,
        &historical_regime(0.0, 0.0, 0.0, "historical-maker-zero").expect("fee regime"),
        &current_crypto_regime(),
    )
    .expect("zero-notional no-fill accounting");

    vec![ep1, ep2_entry, ep2_hedge, ep3]
}

fn print_summary(episode: &TradeEpisode) {
    let current_net = episode.pnl_current_usd.unwrap_or(0.0);
    let current_fee = episode.gross_pnl_usd - current_net;
    eprintln!(
        "[replay-mini] id={} side={:?} limit={:.4} shares={:.6} arrival={} fill_ts={:?} fill_price={:?} fill_qty={:?} exit_type={:?} exit_price={:?} exit_signal_ts={:?} exit_arrival_ts={:?} exit_fill_ts={:?} exit_ts={:?} resolution_at={:?} gross={:.9} fee_historical={:.9} net_historical={:.9} fee_current={:.9} net_current={:.9}",
        episode.episode_id,
        episode.side,
        episode.limit_price,
        episode.shares,
        episode.order_arrival_ts_ms,
        episode.fill_ts_ms,
        episode.fill_price,
        episode.fill_qty,
        episode.exit_type,
        episode.exit_price,
        episode.exit_signal_ts_ms,
        episode.exit_arrival_ts_ms,
        episode.exit_fill_ts_ms,
        episode.exit_ts_ms,
        episode.resolution_at_ms,
        episode.gross_pnl_usd,
        episode.fees_usd,
        episode.net_pnl_usd,
        current_fee,
        current_net,
    );
}

#[test]
fn deterministic_mini_replay_covers_fills_hedge_resolution_fees_and_nofill() {
    let episodes = build_episodes();
    let repeated = build_episodes();
    let json = serde_json::to_string(&episodes).expect("serialize first replay");
    let repeated_json = serde_json::to_string(&repeated).expect("serialize repeated replay");
    assert_eq!(
        json, repeated_json,
        "fixed replay construction must be deterministic"
    );

    assert_eq!(episodes.len(), 4);
    let ep1 = &episodes[0];
    assert_eq!(ep1.shares, 12.5);
    assert_eq!(ep1.order_arrival_ts_ms, ep1.signal_ts_ms + 80 + 20);
    assert_close(ep1.gross_pnl_usd, 7.50);
    assert_eq!(ep1.fees_usd, 0.0);
    assert_close(ep1.net_pnl_usd, 7.50);
    assert_close(ep1.pnl_current_usd.unwrap(), 7.50 - 17.5 * 0.07);
    assert_close(
        ep1.gross_pnl_usd - ep1.fees_usd + ep1.rebates_usd,
        ep1.net_pnl_usd,
    );

    let historical = historical_regime(0.0, 0.0, 0.0, "historical-maker-zero").expect("fee regime");
    let current = current_crypto_regime();
    assert_eq!(
        fee_for_fill(17.5, true, &historical).expect("historical maker fee"),
        0.0
    );
    assert_close(
        fee_for_fill(17.5, false, &current).expect("current taker fee"),
        17.5 * 0.07,
    );

    for episode in &episodes {
        if let Some(fill_ts_ms) = episode.fill_ts_ms {
            assert!(fill_ts_ms >= episode.order_arrival_ts_ms);
            if let Some(exit_ts_ms) = episode.exit_ts_ms {
                assert!(exit_ts_ms >= fill_ts_ms);
            }
            if let (Some(signal), Some(arrival), Some(exit_fill)) = (
                episode.exit_signal_ts_ms,
                episode.exit_arrival_ts_ms,
                episode.exit_fill_ts_ms,
            ) {
                assert!(signal <= arrival && arrival <= exit_fill);
            }
            if let Some(resolution) = episode.resolution_at_ms {
                assert!(resolution >= fill_ts_ms);
            }
        } else {
            assert!(episode.is_no_fill());
            assert_eq!(episode.exit_ts_ms, None);
        }
        assert_close(
            episode.gross_pnl_usd - episode.fees_usd + episode.rebates_usd,
            episode.net_pnl_usd,
        );
    }

    let mut portfolio = Portfolio::new();
    portfolio.apply_episode(ep1);
    portfolio.apply_episode(&episodes[1]);
    portfolio.apply_episode(&episodes[2]);
    let before_nofill = (
        portfolio.cash_pnl_usd(),
        portfolio.open_qty(),
        portfolio.fill_count(),
        portfolio.exits.len(),
        portfolio.turnover(),
        portfolio.last_mid,
    );
    portfolio.apply_episode(&episodes[3]);
    assert_eq!(
        (
            portfolio.cash_pnl_usd(),
            portfolio.open_qty(),
            portfolio.fill_count(),
            portfolio.exits.len(),
            portfolio.turnover(),
            portfolio.last_mid,
        ),
        before_nofill,
        "NO_FILL must not mutate the portfolio"
    );
    assert_close(portfolio.cash_pnl_usd(), 7.80);

    for episode in &episodes {
        print_summary(episode);
    }
}
