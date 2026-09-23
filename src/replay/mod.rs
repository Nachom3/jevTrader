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

pub mod arms;
pub mod bootstrap;
pub mod campaign;
pub mod clock;
pub mod complement;
pub mod economics;
pub mod exits;
pub mod fees;
pub mod fills;
pub mod jev_cache;
pub mod ledger;
pub mod local_harness;
pub mod markouts;
pub mod metrics;
pub mod portfolio;
pub mod regimes;
pub mod report;
pub mod resolution;
pub mod runner;
pub mod sizing;
pub mod source;
pub mod synchronizer;
pub mod types;
pub mod walkforward;

pub use arms::{Arm, ArmPolicy, ArmRun};
pub use bootstrap::{
    Block, BlockBootstrap, BlockId, BootstrapSummary, OosHeadline, OosReport, oos_report,
    oos_report_with_draws, summarize_distribution,
};
pub use campaign::{
    CampaignConfig, CampaignOutput, CampaignTape, JevCaller, LiveJevCaller, TapeByCondition,
    run_episode_campaign, run_episode_campaign_with_caller,
};
pub use clock::ReplayClock;
pub use complement::{
    ComplementStats, NoTopOfBook, SYNTHETIC_COMPLEMENT_SOURCE, SyntheticComplement,
    complement_no_to_yes, complement_no_to_yes_with_stats,
};
pub use economics::{ExecutionPath, ResolutionTimeFilter, classify_execution, passes, path_of};
pub use exits::{
    ExitPolicy, HedgeQuote, MergeResult, hedge_quote, merge_pair, settle_hedge_pair,
    settle_resolution, should_hedge_profit,
};
pub use fees::{
    FeeRegime, apply_to_episode, current_crypto_regime, fee_for_fill, historical_regime,
    upside_with_rebate, zero_regime,
};
pub use fills::{ExecutionLatency, FillOutcome, FillSimulator, RestingOrder};
pub use jev_cache::JevCache;
pub use ledger::{ExitType, Side, TradeEpisode};
pub use markouts::{MarkoutHorizons, signed_markouts_pp};
pub use metrics::{EconomySummary, EpisodeMetricsInput, breakdown_key, summarize, summarize_by};
pub use portfolio::{Portfolio, PortfolioStats};
pub use regimes::{
    BASIS_THRESHOLD, BasisBucket, FAR_MINUTES, FlowBucket, MID_MINUTES, NEAR_MINUTES,
    OFI_THRESHOLD, RegimeFeatures, RegimeLabel, RollingRegimeClassifier, SIGMA_NEG_1, SIGMA_NEG_2,
    SIGMA_POS_1, SIGMA_POS_2, SIGMA_ZERO, SigmaBucket, TREND_THRESHOLD, TimeBucket, Trend,
    Volatility,
};
pub use report::{
    ReportRow, SegmentKey, build_report, conditional_markout_5s, write_json, write_markdown,
};
pub use resolution::{
    Provenance, ResolutionOutcome, ResolutionSkip, ResolutionSkipReason, ResolvedMarket,
    resolve_market,
};
pub use runner::{
    ARMS, JevEvaluator, RawOutcome, RealJev, ReplayConfig, ReplayRunner, RunnerOutput,
    SignalRecord, StubJev, SyntheticItem, V3_ARMS,
};
pub use sizing::{
    MarketConstraints, STANDARD_STAKE_USD, SizedOrder, size_entry, size_hedge, size_standard_entry,
};
pub use source::{ChunkEventSource, HistoricalSource, InMemorySource, read_underlying_window};
pub use synchronizer::{SynchronizedEvent, Synchronizer, as_of_backward};
pub use types::{
    Coverage, Fidelity, FillProfile, HistoricalEvent, LatencyDistribution, LatencyProfile, Regime,
    ResolutionSpec, Split,
};
pub use walkforward::{
    MarketSpan, SplitAssign, TemporalWindow, WalkforwardRunner, WalkforwardWindow,
    apply_purge_embargo, embargo_after_test, embargo_flags, gap_ms, plan_windows, purge_train,
    requires_gap_ms, try_plan_windows, try_purge_train,
};
