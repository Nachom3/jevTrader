//! Provider-free domain types.

pub mod market;

#[allow(dead_code)]
pub mod events;
#[allow(dead_code)]
pub mod ids;
#[allow(dead_code)]
pub mod price;
#[allow(dead_code)]
pub mod signal;

pub use events::{TradeSide, Trigger};
pub use ids::{ConditionId, EventId, MarketId, TokenId};
pub use market::{
    Asset, Horizon, MarketKey, ReferencePoint, ReferencePrice, ResolutionMechanism,
    ResolutionWindow,
};
pub use price::{PriceTicks, TickSize};
pub use signal::Decision;
