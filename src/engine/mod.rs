//! Actor orchestration.

#[allow(dead_code)]
pub mod market_actor;

#[allow(dead_code)]
pub mod signal_actor;

pub use market_actor::{
    BestBidAskUpdate, BookDelta, BookSnapshot, LastTradePriceUpdate, MarketActor, MarketMessage,
    MarketResolvedUpdate, MarketSnapshot, NewMarketUpdate, TickSizeUpdate,
};

pub use signal_actor::{SignalActor, StalenessPolicy, is_usable};
