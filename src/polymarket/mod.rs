//! Venue adapter.

#[allow(dead_code)]
pub mod book;
#[allow(dead_code)]
pub mod rest;
#[allow(dead_code)]
pub mod ws;

pub use book::{BASE_UNITS_PER_TOKEN, BookSide, INLINE_LEVEL_CAPACITY, Level, OrderBook};
pub use rest::{
    MarketMetadata, RestError, TopOfBookSnapshot, fetch_market_by_slug, fetch_top_of_book,
};
pub use ws::{
    DEFAULT_MARKET_WS_ENDPOINT, MarketMessageStream, MarketStream, MarketStreamClient,
    MarketStreamConfig, WsError,
};
