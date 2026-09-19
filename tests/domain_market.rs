use std::collections::HashSet;

use jevtrader::domain::{Asset, Horizon, MarketKey, ReferencePrice, ResolutionMechanism};
use jevtrader::market_spec::MarketSpec;

fn spec(slug: &str, question: &str) -> MarketSpec {
    MarketSpec::parse(&format!(
        "slug: {slug}\nquestion: {question}\nresolution_source: Binance\ntarget: 100\nresolution_at_ms: 2_000_000\nresolution_rules: |\n  Resolves from Binance.\n"
    ))
    .expect("fixture should parse")
}

#[test]
fn all_eight_market_keys_are_unique() {
    let keys = MarketKey::all();
    let unique = keys.iter().copied().collect::<HashSet<_>>();

    assert_eq!(keys.len(), 8);
    assert_eq!(unique.len(), 8);
}

#[test]
fn market_distance_uses_percentage_points_and_resolution_direction() {
    let above = spec("btc-5m", "Will BTC be above 100?");
    assert!((above.distance_to_target(ReferencePrice::new(105.0)) - 5.0).abs() < 1e-12);
    assert!((above.distance_to_target_for(105.0) - 5.0).abs() < 1e-12);

    let below = spec("btc-5m", "Will BTC be below 100?");
    assert!((below.distance_to_target_for(105.0) + 5.0).abs() < 1e-12);
    assert_eq!(below.resolution_mechanism(), ResolutionMechanism::Below);
}

#[test]
fn time_remaining_is_non_negative_and_floored_to_seconds() {
    let market = spec("btc-5m", "Will BTC be above 100?");

    assert_eq!(market.time_remaining_ms(1_998_500), 1_500);
    assert_eq!(market.time_remaining_secs(1_998_500), 1);
    assert_eq!(market.time_remaining(2_001_000), 0);
}

#[test]
fn parses_legacy_six_field_spec_without_optional_metadata() {
    let market = spec("legacy-market", "Will the market resolve yes?");

    assert_eq!(market.asset, None);
    assert_eq!(market.horizon, None);
    assert_eq!(market.reference_source, None);
    assert_eq!(market.window_secs, None);
    assert_eq!(market.start_ms, None);
}

#[test]
fn infers_asset_and_horizon_from_slug_without_optional_fields() {
    let market = spec("ethereum-15-minute-window", "Will ETH be above 100?");

    assert_eq!(market.asset, Some(Asset::Eth));
    assert_eq!(market.horizon, Some(Horizon::M15));
    assert_eq!(
        market.market_key(),
        Some(MarketKey::new(Asset::Eth, Horizon::M15))
    );
}

#[test]
fn parses_explicit_extended_metadata() {
    let market = MarketSpec::parse(
        &[
            "slug: btc-4h",
            "question: Will BTC be above 100?",
            "resolution_source: Binance",
            "target: 100",
            "resolution_at_ms: 2_000_000",
            "asset: BTC",
            "horizon: 4h",
            "reference_source: BTC/USDT",
            "window_secs: 14_400",
            "start_ms: 1_985_600",
            "resolution_rules: |",
            "  Resolves from Binance.",
            "",
        ]
        .join("\n"),
    )
    .expect("extended fixture should parse");

    assert_eq!(market.asset, Some(Asset::Btc));
    assert_eq!(market.horizon, Some(Horizon::H4));
    assert_eq!(market.reference_source.as_deref(), Some("BTC/USDT"));
    assert_eq!(market.window_secs, Some(14_400));
    assert_eq!(market.start_ms, Some(1_985_600));
}
