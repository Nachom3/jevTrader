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
    filled_episode_with_qty(id, side, price, ts_ms, 10.0)
}

fn filled_episode_with_qty(id: &str, side: Side, price: f64, ts_ms: i64, qty: f64) -> TradeEpisode {
    let mut trade = episode(id, side, price);
    trade
        .apply_fill(ts_ms, price, qty)
        .expect("fill arrives after order arrival");
    trade
}

#[test]
fn hedge_profit_uses_the_two_token_boundary() {
    assert!(should_hedge_profit(0.45, 0.52, 0.03));
    assert!(!should_hedge_profit(0.45, 0.58, 0.03));
}

#[test]
fn hedge_pair_attributes_locked_pnl_to_the_entry_leg() {
    let mut yes = filled_episode("yes", Side::BuyYes, 0.45, 100);
    let mut no = filled_episode("no", Side::BuyNo, 0.52, 120);

    settle_hedge_pair(&mut yes, &mut no);

    let merge = merge_pair(0.45 * 10.0, 0.52 * 10.0, 10.0).expect("complete pair");
    assert_eq!(yes.exit_type, ExitType::Hedge);
    assert_eq!(no.exit_type, ExitType::Hedge);
    assert_eq!(yes.exit_price, Some(0.48));
    assert_eq!(no.exit_price, Some(0.52));
    assert_eq!(yes.exit_ts_ms, Some(120));
    assert_eq!(no.exit_ts_ms, Some(120));
    assert!((yes.gross_pnl_usd - 0.30).abs() < 1e-9);
    assert_eq!(no.gross_pnl_usd, 0.0);
    assert!((yes.gross_pnl_usd + no.gross_pnl_usd - merge.locked_pnl).abs() < 1e-9);
    assert!((yes.gross_pnl_usd - merge.locked_pnl).abs() < 1e-9);
}

#[test]
fn hedge_pair_handles_loss_and_both_entry_directions() {
    let mut losing_yes = filled_episode("losing-yes", Side::BuyYes, 0.45, 100);
    let mut losing_no = filled_episode("losing-no", Side::BuyNo, 0.58, 120);
    settle_hedge_pair(&mut losing_yes, &mut losing_no);
    assert!((losing_yes.gross_pnl_usd + 0.30).abs() < 1e-9);
    assert_eq!(losing_no.gross_pnl_usd, 0.0);

    let mut no_entry = filled_episode("no-entry", Side::BuyNo, 0.52, 100);
    let mut yes_hedge = filled_episode("yes-hedge", Side::BuyYes, 0.45, 120);
    settle_hedge_pair(&mut no_entry, &mut yes_hedge);

    let merge = merge_pair(0.52 * 10.0, 0.45 * 10.0, 10.0).expect("complete pair");
    assert_eq!(no_entry.exit_price, Some(0.55));
    assert_eq!(yes_hedge.exit_price, Some(0.45));
    assert!((no_entry.gross_pnl_usd - 0.30).abs() < 1e-9);
    assert_eq!(yes_hedge.gross_pnl_usd, 0.0);
    assert!((no_entry.gross_pnl_usd + yes_hedge.gross_pnl_usd - merge.locked_pnl).abs() < 1e-9);
}

#[test]
fn invalid_hedge_pair_does_not_mutate_either_leg() {
    let mut unequal_entry = filled_episode_with_qty("unequal-entry", Side::BuyYes, 0.45, 100, 10.0);
    let mut unequal_hedge = filled_episode_with_qty("unequal-hedge", Side::BuyNo, 0.52, 120, 9.0);
    let unequal_entry_before = unequal_entry.clone();
    let unequal_hedge_before = unequal_hedge.clone();
    settle_hedge_pair(&mut unequal_entry, &mut unequal_hedge);
    assert_eq!(unequal_entry, unequal_entry_before);
    assert_eq!(unequal_hedge, unequal_hedge_before);

    let mut same_side_entry = filled_episode("same-side-entry", Side::BuyYes, 0.45, 100);
    let mut same_side_hedge = filled_episode("same-side-hedge", Side::BuyYes, 0.52, 120);
    let same_side_entry_before = same_side_entry.clone();
    let same_side_hedge_before = same_side_hedge.clone();
    settle_hedge_pair(&mut same_side_entry, &mut same_side_hedge);
    assert_eq!(same_side_entry, same_side_entry_before);
    assert_eq!(same_side_hedge, same_side_hedge_before);

    let mut missing_fill = episode("missing-fill", Side::BuyYes, 0.45);
    let mut filled_hedge = filled_episode("filled-hedge", Side::BuyNo, 0.52, 120);
    let missing_fill_before = missing_fill.clone();
    let filled_hedge_before = filled_hedge.clone();
    settle_hedge_pair(&mut missing_fill, &mut filled_hedge);
    assert_eq!(missing_fill, missing_fill_before);
    assert_eq!(filled_hedge, filled_hedge_before);
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
