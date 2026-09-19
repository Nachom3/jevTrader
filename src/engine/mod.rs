//! Actor orchestration.

#[allow(dead_code)]
pub mod execution_actor;

pub mod pipeline;
pub mod registry;

#[allow(dead_code)]
pub mod market_actor;

#[allow(dead_code)]
pub mod signal_actor;

pub use execution_actor::{DualFills, ExecutionActor};
pub use pipeline::{
    CompletedMarkout, DecisionInput, MarkoutTracker, Outcome, Pipeline, PipelineInput, SkipReason,
    StepResult, candidate_maker_price, decide,
};
pub use registry::{Lifecycle, MarketRegistry, MarketRuntime, RegistryError, rollover_spec};

pub use market_actor::{
    BestBidAskUpdate, BookDelta, BookSnapshot, LastTradePriceUpdate, MarketActor, MarketMessage,
    MarketResolvedUpdate, MarketSnapshot, NewMarketUpdate, TickSizeUpdate,
};

pub use signal_actor::{SignalActor, StalenessPolicy, is_usable};
