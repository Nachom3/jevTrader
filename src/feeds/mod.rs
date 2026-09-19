//! External exchange feeds normalized to provider-free statistical ticks.

pub mod binance;
pub mod coinbase;
pub mod deribit;
pub mod shared;
pub mod types;

pub use binance::{
    BinanceFeed, BinanceFeedConfig, DEFAULT_WS_URL as BINANCE_WS_URL,
    SUBSCRIPTION_CHANNELS as BINANCE_SUBSCRIPTION_CHANNELS, parse_message as parse_binance_message,
};
pub use coinbase::{
    CoinbaseFeed, CoinbaseFeedConfig, DEFAULT_WS_URL as COINBASE_WS_URL,
    SUBSCRIPTION_CHANNELS as COINBASE_SUBSCRIPTION_CHANNELS,
    parse_message as parse_coinbase_message,
};
pub use deribit::{
    DEFAULT_WS_URL as DERIBIT_WS_URL, DeribitFeed, DeribitFeedConfig,
    SUBSCRIPTION_CHANNELS as DERIBIT_SUBSCRIPTION_CHANNELS, parse_message as parse_deribit_message,
};
pub use shared::{AssetFeedState, SharedFeeds};
pub use types::{FeedError, Venue, VenueTick};
