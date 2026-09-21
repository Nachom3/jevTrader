//! Economic segmentation for replay episodes.
//!
//! Historical and current PnL remain separate streams, as calculated by
//! `fees.rs`. The maker-to-maker path is the system headline: its rebate is
//! zero, and an episode with a taker exit is tagged explicitly rather than
//! mixed into maker PnL.

use serde::{Deserialize, Serialize};

use super::ledger::TradeEpisode;

/// Entry/exit liquidity path for a filled episode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExecutionPath {
    #[serde(rename = "MAKER_ENTRY_MAKER_EXIT")]
    MakerMaker,
    #[serde(rename = "MAKER_ENTRY_TAKER_EXIT")]
    MakerTaker,
    #[serde(rename = "TAKER_ENTRY_MAKER_EXIT")]
    TakerMaker,
    #[serde(rename = "TAKER_ENTRY_TAKER_EXIT")]
    TakerTaker,
    #[serde(rename = "UNKNOWN")]
    Unknown,
}

impl ExecutionPath {
    /// Returns the stable reporting name for this execution path.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MakerMaker => "MAKER_ENTRY_MAKER_EXIT",
            Self::MakerTaker => "MAKER_ENTRY_TAKER_EXIT",
            Self::TakerMaker => "TAKER_ENTRY_MAKER_EXIT",
            Self::TakerTaker => "TAKER_ENTRY_TAKER_EXIT",
            Self::Unknown => "UNKNOWN",
        }
    }

    /// Returns whether this path belongs to the maker-to-maker headline.
    ///
    /// Taker exits remain explicitly tagged and are never mixed into maker
    /// PnL, even when the entry was maker liquidity.
    #[must_use]
    pub const fn is_headline(self) -> bool {
        matches!(self, Self::MakerMaker)
    }
}

/// Classifies an execution path without guessing missing liquidity data.
///
/// If either leg is unknown (`None`), the result is [`ExecutionPath::Unknown`].
#[must_use]
pub const fn classify_execution(entry: Option<bool>, exit: Option<bool>) -> ExecutionPath {
    match (entry, exit) {
        (Some(true), Some(true)) => ExecutionPath::MakerMaker,
        (Some(true), Some(false)) => ExecutionPath::MakerTaker,
        (Some(false), Some(true)) => ExecutionPath::TakerMaker,
        (Some(false), Some(false)) => ExecutionPath::TakerTaker,
        _ => ExecutionPath::Unknown,
    }
}

/// Classifies an episode from its explicit entry and exit liquidity labels.
///
/// Only `maker` and `taker` map to boolean liquidity values. Missing or
/// unexpected labels map to [`ExecutionPath::Unknown`] rather than being
/// inferred from the legacy `is_maker` flag.
#[must_use]
pub fn path_of(episode: &TradeEpisode) -> ExecutionPath {
    let entry = episode
        .entry_liquidity
        .as_deref()
        .and_then(liquidity_is_maker);
    let exit = episode
        .exit_liquidity
        .as_deref()
        .and_then(liquidity_is_maker);
    classify_execution(entry, exit)
}

fn liquidity_is_maker(liquidity: &str) -> Option<bool> {
    match liquidity {
        "maker" => Some(true),
        "taker" => Some(false),
        _ => None,
    }
}

/// Controls whether proxy resolution timestamps are included in an analysis.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResolutionTimeFilter {
    #[serde(rename = "EXACT_ONLY")]
    ExactOnly,
    #[serde(rename = "INCLUDE_PROXY")]
    IncludeProxy,
}

impl ResolutionTimeFilter {
    /// Applies this filter to one episode.
    #[must_use]
    pub fn passes(self, episode: &TradeEpisode) -> bool {
        passes(episode, self)
    }
}

/// Returns whether an episode passes the selected resolution-time filter.
///
/// `ExactOnly` requires `resolution_time_provenance == "exact"`, so pending
/// or otherwise unresolved episodes do not pass. `IncludeProxy` deliberately
/// passes every episode, including one without a resolution; callers doing
/// resolution analysis must additionally require `resolution_outcome`.
#[must_use]
pub fn passes(episode: &TradeEpisode, filter: ResolutionTimeFilter) -> bool {
    match filter {
        ResolutionTimeFilter::ExactOnly => {
            episode.resolution_time_provenance.as_deref() == Some("exact")
        }
        ResolutionTimeFilter::IncludeProxy => true,
    }
}
