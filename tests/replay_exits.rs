use jevtrader::replay::ledger::ExitType;
use jevtrader::replay::{
    HedgeQuote, Portfolio, Side, TradeEpisode, hedge_quote, merge_pair, settle_hedge_pair,
    settle_resolution, should_hedge_profit,
};

fn episode(id: &str, side: Side, limit_price: f64) -> TradeEpisode {
    TradeEpisode::new(
        id,
        "lead-lag-v1",
        "market-1",
        "BTC",
        "5m",
        0,
        0,
        0,
        0,
        side,
        limit_price,
        10.0,
    )
    .expect("valid episode")
}

fn filled_episode(id: &str, side: Side, price: f64, ts_ms: i64) -> TradeEpisode {
    let mut trade = episode(id, side, price);
    trade
        .apply_fill(ts_ms, price, 10.0)
        .expect("fill arrives after order arrival");
    trade
}

#[test]
fn hedge_profit_uses_the_two_token_boundary() {
    assert!(should_hedge_profit(0.45, 0.52, 0.03));
    assert!(!should_hedge_profit(0.45, 0.58, 0.03));
}

#[test]
fn hedge_pair_uses_real_fills_and_closes_both_legs() {
    let mut yes = filled_episode("yes", Side::BuyYes, 0.45, 100);
    let mut no = filled_episode("no", Side::BuyNo, 0.52, 120);

    settle_hedge_pair(&mut yes, &mut no);

    assert_eq!(yes.exit_type, ExitType::Hedge);
    assert_eq!(no.exit_type, ExitType::Hedge);
    assert_eq!(yes.exit_price, Some(0.52));
    assert_eq!(no.exit_price, Some(0.45));
    assert_eq!(yes.exit_ts_ms, Some(120));
    assert_eq!(no.exit_ts_ms, Some(120));
    assert!((yes.gross_pnl_usd - 0.7).abs() < 1e-9);
    assert!((no.gross_pnl_usd + 0.7).abs() < 1e-9);
    assert!((yes.gross_pnl_usd + no.gross_pnl_usd).abs() < 1e-9);
}

#[test]
fn complete_pair_has_the_same_one_dollar_terminal_payoff_for_both_outcomes() {
    let merge = merge_pair(0.45 * 10.0, 0.52 * 10.0, 10.0).expect("valid pair");
    assert_eq!(merge.terminal_value, 10.0);

    let mut yes_won_yes = filled_episode("yes-won-yes", Side::BuyYes, 0.45, 100);
    let mut yes_won_no = filled_episode("yes-won-no", Side::BuyNo, 0.52, 100);
    settle_resolution(&mut yes_won_yes, true, 200, 1.0).expect("resolution settles");
    settle_resolution(&mut yes_won_no, true, 200, 1.0).expect("resolution settles");
    let yes_won_terminal =
        yes_won_yes.exit_price.unwrap() * 10.0 + yes_won_no.exit_price.unwrap() * 10.0;

    let mut no_won_yes = filled_episode("no-won-yes", Side::BuyYes, 0.45, 100);
    let mut no_won_no = filled_episode("no-won-no", Side::BuyNo, 0.52, 100);
    settle_resolution(&mut no_won_yes, false, 200, 1.0).expect("resolution settles");
    settle_resolution(&mut no_won_no, false, 200, 1.0).expect("resolution settles");
    let no_won_terminal =
        no_won_yes.exit_price.unwrap() * 10.0 + no_won_no.exit_price.unwrap() * 10.0;

    assert_eq!(yes_won_terminal, merge.terminal_value);
    assert_eq!(no_won_terminal, merge.terminal_value);
    assert!((yes_won_yes.gross_pnl_usd + yes_won_no.gross_pnl_usd - merge.locked_pnl).abs() < 1e-9);
    assert!((no_won_yes.gross_pnl_usd + no_won_no.gross_pnl_usd - merge.locked_pnl).abs() < 1e-9);
}

#[test]
fn merge_does_not_create_or_destroy_value() {
    let result = merge_pair(4.50, 5.20, 10.0).expect("valid merge");

    assert_eq!(result.total_cost_usd, 9.70);
    assert_eq!(result.terminal_value, 10.0);
    assert_eq!(
        result.locked_pnl,
        result.terminal_value - result.total_cost_usd
    );
}

#[test]
fn no_fill_never_mutates_portfolio() {
    let mut portfolio = Portfolio::new();
    let no_fill = episode("no-fill", Side::BuyYes, 0.45);
    let before = (
        portfolio.cash_pnl_usd(),
        portfolio.open_qty(),
        portfolio.fill_count(),
        portfolio.exits.len(),
    );

    portfolio.apply_episode(&no_fill);

    assert_eq!(
        (
            portfolio.cash_pnl_usd(),
            portfolio.open_qty(),
            portfolio.fill_count(),
            portfolio.exits.len(),
        ),
        before
    );
}

#[test]
fn hedge_quote_is_only_a_proposal_and_contains_no_fill() {
    let entry = filled_episode("entry", Side::BuyYes, 0.45, 100);
    let entry_before = entry.clone();
    let quote: HedgeQuote = hedge_quote(&entry, 0.52).expect("valid hedge quote");
    let hedge = episode("hedge", quote.side, quote.limit_price);

    assert_eq!(quote.side, Side::BuyNo);
    assert_eq!(quote.limit_price, 0.52);
    assert_eq!(quote.qty, 10.0);
    assert_eq!(quote.arrival_ts_ms, entry.order_arrival_ts_ms);
    assert_eq!(entry, entry_before);
    assert_eq!(hedge.fill_ts_ms, None);
    assert_eq!(hedge.fill_price, None);
    assert_eq!(hedge.fill_qty, None);
}

#[test]
fn resolution_uses_the_supplied_outcome_and_rejects_early_timestamps() {
    let mut yes = filled_episode("yes", Side::BuyYes, 0.40, 100);
    settle_resolution(&mut yes, true, 200, 1.0).expect("YES winner should pay");
    assert_eq!(yes.exit_price, Some(1.0));
    assert!((yes.gross_pnl_usd - 6.0).abs() < 1e-9);

    let mut no = filled_episode("no", Side::BuyNo, 0.40, 100);
    settle_resolution(&mut no, true, 200, 1.0).expect("NO loser should settle");
    assert_eq!(no.exit_price, Some(0.0));
    assert!((no.gross_pnl_usd + 4.0).abs() < 1e-9);

    let mut early = filled_episode("early", Side::BuyYes, 0.40, 100);
    let before = early.clone();
    let error = settle_resolution(&mut early, true, 99, 1.0).expect_err("early resolution fails");
    assert!(error.contains("precedes fill"));
    assert_eq!(early, before);
}

#[test]
fn exit_before_fill_is_rejected_by_the_ledger() {
    let mut trade = episode("unfilled", Side::BuyYes, 0.40);

    assert!(trade.apply_exit(ExitType::Sell, 100, 0.42).is_err());
}
