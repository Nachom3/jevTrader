//! Ablation-arm declarations for replay lineage and routing.
//!
//! This module deliberately contains no thresholds, fill behavior, or Jev
//! calls. It only declares which inputs an arm routes to the runner and keeps
//! the shared replay infrastructure auditable.

use serde::{Deserialize, Serialize};

/// Replay ablation arm.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Arm {
    QuantOnly,
    JevOnly,
    QuantPlusJev,
    MicroPlusRegime,
}

impl Arm {
    /// Stable external identifier used in cache keys and ledger lineage.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::QuantOnly => "QUANT_ONLY",
            Self::JevOnly => "JEV_ONLY",
            Self::QuantPlusJev => "QUANT_PLUS_JEV",
            Self::MicroPlusRegime => "MICRO_PLUS_REGIME",
        }
    }

    /// Returns the inputs and evaluator routing declared by this arm.
    #[must_use]
    pub const fn policy(self) -> ArmPolicy {
        match self {
            Self::QuantOnly => ArmPolicy {
                calls_jev: false,
                uses_quant: true,
                uses_micro_regime: false,
            },
            Self::JevOnly => ArmPolicy {
                calls_jev: true,
                uses_quant: false,
                uses_micro_regime: false,
            },
            Self::QuantPlusJev => ArmPolicy {
                calls_jev: true,
                uses_quant: true,
                uses_micro_regime: false,
            },
            Self::MicroPlusRegime => ArmPolicy {
                calls_jev: true,
                uses_quant: false,
                uses_micro_regime: true,
            },
        }
    }
}

/// Routing declaration for one replay ablation arm.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArmPolicy {
    pub calls_jev: bool,
    pub uses_quant: bool,
    pub uses_micro_regime: bool,
}

/// Auditable manifest of the replay infrastructure shared by one arm.
///
/// The runner must keep `fill_profile`, `fee_regime`, `exit_policy`, and
/// `capital_note` identical across arms. The only permitted runtime difference
/// between arms is Jev latency. This type declares routing and lineage only; it
/// does not contain thresholds, fill logic, or Jev calls.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArmRun {
    pub arm: Arm,
    pub fill_profile: String,
    pub fee_regime: String,
    pub exit_policy: String,
    pub capital_note: String,
}

impl ArmRun {
    /// Builds an auditable arm manifest from shared replay configuration.
    #[must_use]
    pub fn new(
        arm: Arm,
        fill_profile: impl Into<String>,
        fee_regime: impl Into<String>,
        exit_policy: impl Into<String>,
        capital_note: impl Into<String>,
    ) -> Self {
        Self {
            arm,
            fill_profile: fill_profile.into(),
            fee_regime: fee_regime.into(),
            exit_policy: exit_policy.into(),
            capital_note: capital_note.into(),
        }
    }
}
