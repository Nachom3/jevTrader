//! HistoricalReplay: same strategy code, substituted event sources + clock.
//!
//! LIVE: WebSocket -> MarketState -> Jev -> Strategy.
//! HISTORICAL: Parquet -> ReplayClock -> MarketState -> Jev -> Strategy.
//!
//! This module never reimplements thresholds or questions. It owns only
//! event sourcing, virtual time, latency, fills, PnL, and reports. Strategy
//! decisions reuse `engine::pipeline::decide`, `state::feature_builder`,
//! `state::quant_features`, `strategy::lead_lag::should_quote`,
//! `strategy::quote::decide_quote`, `strategy::risk::RiskGate`, and
//! `execution::markout`.

pub mod clock;
pub mod fills;
pub mod jev_cache;
pub mod markouts;
pub mod portfolio;
pub mod report;
pub mod runner;
pub mod source;
pub mod synchronizer;
pub mod types;
pub mod walkforward;

pub use clock::ReplayClock;
pub use fills::{ExecutionLatency, FillOutcome, FillSimulator, RestingOrder};
pub use jev_cache::JevCache;
pub use markouts::{MarkoutHorizons, signed_markouts_pp};
pub use portfolio::{Portfolio, PortfolioStats};
pub use report::{
    ReportRow, SegmentKey, build_report, conditional_markout_5s, write_json, write_markdown,
};
pub use runner::{
    ARMS, Arm, JevEvaluator, RawOutcome, RealJev, ReplayConfig, ReplayRunner, RunnerOutput,
    SignalRecord, StubJev, SyntheticItem, V3_ARMS,
};
pub use source::{ChunkEventSource, HistoricalSource, InMemorySource, read_underlying_window};
pub use synchronizer::{SynchronizedEvent, Synchronizer, as_of_backward};
pub use types::{
    Coverage, Fidelity, FillProfile, HistoricalEvent, LatencyDistribution, LatencyProfile, Regime,
    ResolutionSpec, Split,
};
pub use walkforward::{WalkforwardRunner, WalkforwardWindow};
