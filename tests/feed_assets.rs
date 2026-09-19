use jevtrader::domain::Asset;
use jevtrader::feeds::binance;
use jevtrader::feeds::coinbase;
use jevtrader::feeds::deribit;
use jevtrader::feeds::{AssetFeedState, SharedFeeds, Venue, VenueTick};

fn tick(
    venue: Venue,
    symbol: &'static str,
    price: f64,
    timestamp: i64,
    trade_size: f64,
) -> VenueTick {
    VenueTick {
        venue,
        symbol,
        price_f64: price,
        best_bid_f64: f64::NAN,
        best_ask_f64: f64::NAN,
        trade_size_f64: trade_size,
        trade_side_buy: true,
        ts_exchange_ms: timestamp,
        ts_local_ms: 0,
    }
}

#[test]
fn routes_btc_and_eth_ticks_to_their_lanes() {
    let mut feeds = SharedFeeds::new();
    feeds.apply(tick(Venue::Binance, "BTCUSDT", 42_000.0, 1_000, 2.0));
    feeds.apply(tick(Venue::Coinbase, "ETH-USD", 2_200.0, 1_500, 3.0));
    feeds.apply(tick(Venue::Deribit, "XBT-PERPETUAL", 42_001.0, 1_900, 0.0));
    feeds.apply(tick(Venue::Binance, "SOLUSDT", 150.0, 2_000, 1.0));

    assert_eq!(feeds.lane(Asset::Btc).recent_ticks().len(), 1);
    assert_eq!(feeds.lane(Asset::Eth).recent_ticks().len(), 1);
    assert_eq!(feeds.lane(Asset::Btc).venues().perp, 42_001.0);
    assert_eq!(feeds.lane(Asset::Eth).venues().coinbase, 2_200.0);
    assert_eq!(feeds.lane(Asset::Btc).order_flow(2_000).buy_vol_1s, 2.0);
    assert_eq!(feeds.lane(Asset::Eth).order_flow(2_000).buy_vol_1s, 3.0);
    assert_eq!(feeds.ignored, 1);
}

#[test]
fn asset_lanes_are_isolated() {
    let mut feeds = SharedFeeds::new();
    feeds.apply(tick(Venue::Binance, "BTCUSDT", 42_000.0, 10_000, 1.0));

    assert_eq!(feeds.lane(Asset::Btc).recent_ticks()[0].price, 42_000.0);
    assert!(feeds.lane(Asset::Eth).recent_ticks().is_empty());
    assert!(feeds.lane(Asset::Eth).venues().binance.is_nan());
}

#[test]
fn retains_recent_spot_ticks_for_sixty_five_minutes() {
    let mut state = AssetFeedState::new();
    let base = 10_000_000_i64;
    state.apply(tick(Venue::Binance, "BTCUSDT", 41_000.0, base, 0.0));
    state.apply(tick(
        Venue::Coinbase,
        "BTC-USD",
        42_000.0,
        base + 65 * 60 * 1_000 + 1,
        0.0,
    ));

    assert_eq!(state.recent_ticks().len(), 1);
    assert_eq!(state.recent_ticks()[0].price, 42_000.0);
    assert_eq!(
        state.recent_ticks()[0].ts_ms,
        (base + 65 * 60 * 1_000 + 1) as u64
    );
}

#[test]
fn each_asset_feed_has_asset_specific_subscription() {
    let btc_binance = binance::BinanceFeed::new();
    let eth_binance = binance::BinanceFeed::for_asset(Asset::Eth);
    assert_eq!(btc_binance.asset(), Asset::Btc);
    assert_eq!(eth_binance.asset(), Asset::Eth);
    assert_ne!(
        binance::channels_for(Asset::Btc),
        binance::channels_for(Asset::Eth)
    );

    let btc_coinbase = coinbase::CoinbaseFeed::new();
    let eth_coinbase = coinbase::CoinbaseFeed::for_asset(Asset::Eth);
    assert_eq!(btc_coinbase.asset(), Asset::Btc);
    assert_eq!(eth_coinbase.asset(), Asset::Eth);
    let btc_coinbase_subscription: serde_json::Value =
        serde_json::from_str(&coinbase::subscription_message_for(Asset::Btc)).unwrap();
    let eth_coinbase_subscription: serde_json::Value =
        serde_json::from_str(&coinbase::subscription_message_for(Asset::Eth)).unwrap();
    assert_ne!(
        btc_coinbase_subscription["product_ids"],
        eth_coinbase_subscription["product_ids"]
    );

    let btc_deribit = deribit::DeribitFeed::new();
    let eth_deribit = deribit::DeribitFeed::for_asset(Asset::Eth);
    assert_eq!(btc_deribit.asset(), Asset::Btc);
    assert_eq!(eth_deribit.asset(), Asset::Eth);
    assert_ne!(
        deribit::channels_for(Asset::Btc),
        deribit::channels_for(Asset::Eth)
    );
}
