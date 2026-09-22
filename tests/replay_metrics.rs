use jevtrader::replay::{
    EconomySummary, EpisodeMetricsInput, ExitType, Side, TradeEpisode, breakdown_key, summarize,
    summarize_by,
};

fn assert_close(left: f64, right: f64) {
    assert!((left - right).abs() < 1e-9, "left={left}, right={right}");
}

#[allow(clippy::too_many_arguments)]
fn filled_episode(
    id: &str,
    asset: &str,
    horizon: &str,
    exit_type: ExitType,
    fill_ts_ms: i64,
    exit_ts_ms: i64,
    notional_usd: f64,
    gross_pnl_usd: f64,
    historical_net_usd: f64,
    current_net_usd: f64,
) -> TradeEpisode {
    let mut episode = TradeEpisode::new(
        id,
        "strategy-v1",
        "market-1",
        asset,
        horizon,
        0,
        0,
        0,
        0,
        Side::BuyYes,
        0.5,
        notional_usd,
    )
    .expect("valid episode");
    episode
        .apply_fill(fill_ts_ms, 0.5, notional_usd / 0.5)
        .expect("valid fill");
    episode
        .apply_exit(exit_type, exit_ts_ms, 0.5)
        .expect("valid exit");
    episode
        .settle_accounting(gross_pnl_usd, 0.0, 0.0, gross_pnl_usd)
        .expect("valid accounting");
    episode.pnl_historical_usd = Some(historical_net_usd);
    episode.pnl_current_usd = Some(current_net_usd);
    episode.actual_notional_usd = notional_usd;
    episode
}

fn nofill_episode(id: &str, asset: &str, horizon: &str) -> TradeEpisode {
    let mut episode = TradeEpisode::new(
        id,
        "strategy-v1",
        "market-1",
        asset,
        horizon,
        0,
        0,
        0,
        0,
        Side::BuyYes,
        0.5,
        10.0,
    )
    .expect("valid episode");
    // Deliberately non-zero to prove no-fill rows never enter PnL aggregates.
    episode.gross_pnl_usd = 100.0;
    episode.net_pnl_usd = 100.0;
    episode.pnl_historical_usd = Some(100.0);
    episode.pnl_current_usd = Some(100.0);
    episode
}

fn input<'a>(
    episode: &'a TradeEpisode,
    regime: Option<&'a str>,
    markouts: [Option<f64>; 5],
    fill_profile: &'a str,
) -> EpisodeMetricsInput<'a> {
    EpisodeMetricsInput {
        episode,
        regime,
        markouts,
        fill_profile,
    }
}

#[test]
fn fixed_three_episode_economy_uses_filled_episodes_for_pnl() {
    let mut hold = filled_episode(
        "hold-win",
        "BTC",
        "5m",
        ExitType::Resolution,
        100,
        200,
        10.0,
        7.50,
        7.50,
        7.25,
    );
    hold.set_is_maker(true);
    let hedge = filled_episode(
        "hedge-win",
        "BTC",
        "5m",
        ExitType::Hedge,
        300,
        400,
        5.0,
        0.30,
        0.30,
        0.0,
    );
    let nofill = nofill_episode("no-fill", "BTC", "5m");

    let inputs = [
        input(
            &hold,
            Some("normal"),
            [None, Some(-2.0), None, None, None],
            "maker",
        ),
        input(
            &hedge,
            Some("normal"),
            [None, None, None, None, None],
            "maker",
        ),
        input(&nofill, Some("normal"), [None; 5], "maker"),
    ];
    let summary = summarize(&inputs);

    assert_eq!(summary.n_episodes, 3);
    assert_eq!(summary.n_filled, 2);
    assert_eq!(summary.n_nofill, 1);
    assert_close(summary.fill_rate, 2.0 / 3.0);
    assert_close(summary.nofill_rate, 1.0 / 3.0);
    assert_close(summary.gross_pnl_usd, 7.80);
    assert_close(summary.historical_net_usd, 7.80);
    assert_close(summary.current_net_usd, 7.25);
    assert_close(summary.pnl_per_trade, 3.90);
    assert_close(summary.roi_on_used_capital, 7.80 / 15.0);
    assert_eq!(summary.wins, 2);
    assert_eq!(summary.losses, 0);
    assert_eq!(summary.hold_pnl_usd, 7.50);
    assert_close(summary.hedge_pnl_usd, 0.30);
    assert_eq!(summary.turnover_usd, 15.0);
    assert_eq!(summary.adverse_selection_pp, Some(-2.0));

    // A hedge is one filled episode in this fixture, so the rate is 2/3. If
    // callers provide entry and hedge as two legs, both legs count and the
    // same rule produces 3/4 over four supplied episodes.
}

#[test]
fn roi_uses_executable_filled_notional_only() {
    let episode = filled_episode(
        "roi",
        "BTC",
        "5m",
        ExitType::Sell,
        100,
        200,
        20.0,
        8.0,
        6.0,
        6.0,
    );
    let summary = summarize(&[input(&episode, None, [None; 5], "profile")]);
    assert_close(summary.roi_on_used_capital, 6.0 / 20.0);
}

#[test]
fn profit_factor_handles_mixed_and_loss_free_samples() {
    let win = filled_episode(
        "win",
        "BTC",
        "5m",
        ExitType::Sell,
        100,
        200,
        10.0,
        3.0,
        3.0,
        3.0,
    );
    let loss = filled_episode(
        "loss",
        "BTC",
        "5m",
        ExitType::Stop,
        300,
        400,
        10.0,
        -1.0,
        -1.0,
        -1.0,
    );
    let mixed = summarize(&[
        input(&win, None, [None; 5], "profile"),
        input(&loss, None, [None; 5], "profile"),
    ]);
    assert_eq!(mixed.profit_factor, Some(3.0));

    let loss_free = summarize(&[input(&win, None, [None; 5], "profile")]);
    assert_eq!(loss_free.profit_factor, None);
}

#[test]
fn drawdown_sorts_by_exit_timestamp_and_uses_historical_net() {
    let first_win = filled_episode(
        "first-win",
        "BTC",
        "5m",
        ExitType::Sell,
        100,
        100,
        10.0,
        10.0,
        10.0,
        10.0,
    );
    let loss = filled_episode(
        "second-loss",
        "BTC",
        "5m",
        ExitType::Stop,
        200,
        200,
        10.0,
        -6.0,
        -6.0,
        -6.0,
    );
    let final_win = filled_episode(
        "third-win",
        "BTC",
        "5m",
        ExitType::Sell,
        300,
        300,
        10.0,
        4.0,
        4.0,
        4.0,
    );

    // Deliberately reverse the input order; the curve is ordered by exit_ts.
    let summary = summarize(&[
        input(&final_win, None, [None; 5], "profile"),
        input(&loss, None, [None; 5], "profile"),
        input(&first_win, None, [None; 5], "profile"),
    ]);
    assert_close(summary.max_drawdown_usd, 6.0);
}

#[test]
fn simultaneous_capital_counts_overlapping_fills() {
    let first = filled_episode(
        "first",
        "BTC",
        "5m",
        ExitType::Sell,
        100,
        300,
        10.0,
        1.0,
        1.0,
        1.0,
    );
    let second = filled_episode(
        "second",
        "BTC",
        "5m",
        ExitType::Sell,
        200,
        400,
        7.0,
        1.0,
        1.0,
        1.0,
    );
    let summary = summarize(&[
        input(&second, None, [None; 5], "profile"),
        input(&first, None, [None; 5], "profile"),
    ]);
    assert_close(summary.max_simultaneous_capital_usd, 17.0);
}

#[test]
fn breakdown_separates_asset_and_horizon_segments() {
    let btc = filled_episode(
        "btc",
        "BTC",
        "5m",
        ExitType::Resolution,
        100,
        200,
        10.0,
        1.0,
        1.0,
        1.0,
    );
    let eth = filled_episode(
        "eth",
        "ETH",
        "1h",
        ExitType::Hedge,
        100,
        200,
        10.0,
        2.0,
        2.0,
        2.0,
    );
    assert_eq!(
        breakdown_key(&btc, Some("normal")),
        "BTC|5m|normal|strategy-v1|unknown|hold"
    );

    let grouped = summarize_by(&[
        input(&btc, Some("normal"), [None; 5], "conservative"),
        input(&eth, Some("volatile"), [None; 5], "aggressive"),
    ]);
    assert_eq!(grouped.len(), 2);
    assert_eq!(
        grouped
            .get("BTC|5m|normal|strategy-v1|conservative|hold")
            .expect("BTC segment")
            .n_episodes,
        1
    );
    assert_eq!(
        grouped
            .get("ETH|1h|volatile|strategy-v1|aggressive|hedge")
            .expect("ETH segment")
            .n_episodes,
        1
    );
}

#[test]
fn empty_summary_has_no_adverse_selection_or_profit_factor() {
    let summary = summarize(&[]);
    assert_eq!(summary, EconomySummary::default());
    assert_eq!(summary.adverse_selection_pp, None);
    assert_eq!(summary.profit_factor, None);
}
