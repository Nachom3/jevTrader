//! Explicit market resolution contracts for historical replay.
//!
//! Resolution is deliberately separate from the event stream: a replay may
//! only settle against an observed outcome and an explicit timestamp. Missing
//! or unreliable evidence is a skip, never an inferred YES/NO result.

use serde::{Deserialize, Serialize};

use super::types::{Fidelity, ResolutionSpec};

/// Binary market outcome. `Yes` means the YES token won; `No` means it did
/// not. Keeping both outcomes explicit prevents callers from treating a
/// missing answer as a negative answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ResolutionOutcome {
    Yes,
    No,
}

impl ResolutionOutcome {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Yes => "yes",
            Self::No => "no",
        }
    }
}

/// Evidence lineage for a resolved market.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Provenance {
    Exact,
    Proxy,
}

impl Provenance {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Exact => "exact",
            Self::Proxy => "proxy",
        }
    }
}

/// A validated resolution that is safe to use as a replay label.
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedMarket {
    pub spec: ResolutionSpec,
    pub outcome: ResolutionOutcome,
    /// Provenance of the observed outcome.
    pub outcome_provenance: Provenance,
    /// Provenance of the timestamp used for replay settlement.
    pub time_provenance: Provenance,
    /// Legacy alias for [`Self::outcome_provenance`]. New code should use the
    /// split provenance fields instead.
    pub provenance: Provenance,
    pub resolved_at_ms: i64,
}

/// Why a market was not eligible for explicit resolution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResolutionSkipReason {
    MissingOutcome,
    MissingTimestamp,
    UnreliableFidelity,
    UnknownCondition,
    ProxyNotAllowed,
}

impl ResolutionSkipReason {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MissingOutcome => "missing_outcome",
            Self::MissingTimestamp => "missing_timestamp",
            Self::UnreliableFidelity => "unreliable_fidelity",
            Self::UnknownCondition => "unknown_condition",
            Self::ProxyNotAllowed => "proxy_not_allowed",
        }
    }
}

/// A deterministic skip record. `condition_id` is preserved even for a
/// malformed spec so callers can account for the omitted market.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolutionSkip {
    pub condition_id: String,
    pub reason: ResolutionSkipReason,
}

/// Validates outcome and timestamp evidence independently for replay.
///
/// Outcome provenance is checked against the market fidelity because an exact
/// outcome cannot be claimed from a proxy-fidelity specification. Timestamp
/// provenance is governed only by `allow_proxy_time`: a proxy settlement time
/// can therefore accompany an exact outcome.
pub fn resolve_market_split(
    spec: &ResolutionSpec,
    outcome: Option<ResolutionOutcome>,
    outcome_prov: Provenance,
    time_prov: Provenance,
    allow_proxy_outcome: bool,
    allow_proxy_time: bool,
) -> Result<ResolvedMarket, ResolutionSkip> {
    let condition_id = spec.condition_id.clone();
    if spec.condition_id.trim().is_empty() || spec.market_id.trim().is_empty() {
        return Err(ResolutionSkip {
            condition_id,
            reason: ResolutionSkipReason::UnknownCondition,
        });
    }
    if spec.resolution_at_ms <= 0 {
        return Err(ResolutionSkip {
            condition_id,
            reason: ResolutionSkipReason::MissingTimestamp,
        });
    }
    let Some(outcome) = outcome else {
        return Err(ResolutionSkip {
            condition_id,
            reason: ResolutionSkipReason::MissingOutcome,
        });
    };

    if outcome_prov == Provenance::Exact && spec.fidelity != Fidelity::Exact {
        return Err(ResolutionSkip {
            condition_id,
            reason: ResolutionSkipReason::UnreliableFidelity,
        });
    }
    if outcome_prov == Provenance::Proxy && !allow_proxy_outcome {
        return Err(ResolutionSkip {
            condition_id,
            reason: ResolutionSkipReason::ProxyNotAllowed,
        });
    }
    if time_prov == Provenance::Proxy && !allow_proxy_time {
        return Err(ResolutionSkip {
            condition_id,
            reason: ResolutionSkipReason::ProxyNotAllowed,
        });
    }

    Ok(ResolvedMarket {
        spec: spec.clone(),
        outcome,
        outcome_provenance: outcome_prov,
        time_provenance: time_prov,
        // Keep the pre-split field for callers that still consume the legacy
        // single-provenance contract.
        provenance: outcome_prov,
        resolved_at_ms: spec.resolution_at_ms,
    })
}

/// Compatibility wrapper for the pre-split resolution contract.
///
/// New code should call [`resolve_market_split`] so outcome and timestamp
/// provenance can be admitted independently. The legacy field on the result
/// remains equal to the outcome provenance.
pub fn resolve_market(
    spec: &ResolutionSpec,
    outcome: Option<ResolutionOutcome>,
    provenance: Provenance,
    allow_proxy: bool,
) -> Result<ResolvedMarket, ResolutionSkip> {
    resolve_market_split(
        spec,
        outcome,
        provenance,
        provenance,
        allow_proxy,
        allow_proxy,
    )
}
