//! Signal evaluation state machine and its explicit freshness gate.
//!
//! The actor owns the state sequence and the latest inbound [`JevEvaluation`].
//! The async boundary is intentionally limited to [`SignalActor::evaluate_next`];
//! freshness decisions remain synchronous and can be tested without a runtime.

use crate::jev::client::{JevError, evaluate};
use crate::jev::response::JevEvaluation;
use crate::strategy::lead_lag::V1Signal;
use serde_json::Value;
use std::time::Duration;

/// Freshness limits for allowing a Jev evaluation to reach a downstream quote
/// decision.
///
/// A small sequence lag does not always mean that a signal is invalid: venue
/// state can advance while a short-lived evaluation is in flight. This policy
/// is the single gate that decides whether that tolerated lag, and the
/// evaluation latency, are still acceptable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StalenessPolicy {
    /// Number of newer states an evaluation may lag behind.
    pub max_lag: u64,
    /// Maximum acceptable end-to-end Jev latency in milliseconds.
    pub max_latency_ms: u64,
}

impl StalenessPolicy {
    /// Default sequence lag tolerance: two state updates.
    pub const DEFAULT_MAX_LAG: u64 = 2;
    /// Default latency limit: 1.5 seconds.
    pub const DEFAULT_MAX_LATENCY_MS: u64 = 1_500;

    /// Creates an explicit freshness policy.
    #[must_use]
    pub const fn new(max_lag: u64, max_latency_ms: u64) -> Self {
        Self {
            max_lag,
            max_latency_ms,
        }
    }

    /// Returns whether an evaluation is fresh enough for the current state.
    ///
    /// Both checks are required: a sequence is usable only when
    /// `evaluation.state_seq + max_lag >= current_seq`, and its measured
    /// latency must be at most `max_latency_ms`.
    #[must_use]
    pub fn is_usable(&self, evaluation: &JevEvaluation, current_seq: u64) -> bool {
        evaluation.state_seq.saturating_add(self.max_lag) >= current_seq
            && evaluation.latency_ms <= self.max_latency_ms
    }
}

impl Default for StalenessPolicy {
    /// Uses a two-state lag and 1.5-second latency budget.
    fn default() -> Self {
        Self::new(Self::DEFAULT_MAX_LAG, Self::DEFAULT_MAX_LATENCY_MS)
    }
}

/// Evaluates venue state and keeps only the latest Jev result for downstream
/// freshness checks.
#[derive(Debug)]
pub struct SignalActor {
    state_seq: u64,
    latest_evaluation: Option<JevEvaluation>,
    staleness_policy: StalenessPolicy,
}

impl SignalActor {
    /// Creates an actor with an explicit staleness policy.
    #[must_use]
    pub const fn new(staleness_policy: StalenessPolicy) -> Self {
        Self {
            state_seq: 0,
            latest_evaluation: None,
            staleness_policy,
        }
    }

    /// The sequence number of the newest state sent for evaluation.
    #[must_use]
    pub const fn state_seq(&self) -> u64 {
        self.state_seq
    }

    /// The policy used by [`Self::usable_signal`].
    #[must_use]
    pub const fn staleness_policy(&self) -> StalenessPolicy {
        self.staleness_policy
    }

    /// The most recently received evaluation, whether usable or stale.
    #[must_use]
    pub fn latest_evaluation(&self) -> Option<&JevEvaluation> {
        self.latest_evaluation.as_ref()
    }

    /// Stores an inbound evaluation from the async boundary.
    ///
    /// [`SignalActor::evaluate_next`] uses this transition in production. The
    /// method is also useful to feed deterministic synthetic evaluations to a
    /// caller that owns the async boundary.
    pub fn record_evaluation(&mut self, evaluation: JevEvaluation) {
        self.latest_evaluation = Some(evaluation);
    }

    /// Returns the latest signal only when the single staleness gate accepts it.
    ///
    /// A stale evaluation can never produce a downstream quote through this
    /// accessor. Staleness is not automatically invalidation; the configured
    /// sequence and latency tolerances are the only acceptance criteria.
    #[must_use]
    pub fn usable_signal(&self) -> Option<&V1Signal> {
        self.latest_evaluation
            .as_ref()
            .filter(|evaluation| self.staleness_policy.is_usable(evaluation, self.state_seq))
            .map(|evaluation| &evaluation.signal)
    }

    /// Sends the next state to Jev and records its inbound evaluation.
    ///
    /// The client call receives the incremented `state_seq` and `market_id`;
    /// it stamps `sent_at_ms` before the request and returns that identity
    /// together with `received_at_ms` and `latency_ms` in [`JevEvaluation`].
    /// No Tokio runtime or network work is owned by the actor itself.
    pub async fn evaluate_next(
        &mut self,
        state_value: &Value,
        market_id: &str,
        api_key: &str,
        deadline: Duration,
    ) -> Result<JevEvaluation, JevError> {
        self.state_seq = self.state_seq.saturating_add(1);
        let evaluation =
            evaluate(state_value, self.state_seq, market_id, api_key, deadline).await?;
        self.record_evaluation(evaluation.clone());
        Ok(evaluation)
    }
}

impl Default for SignalActor {
    fn default() -> Self {
        Self::new(StalenessPolicy::default())
    }
}

/// Applies a staleness policy to one evaluation.
#[must_use]
pub fn is_usable(evaluation: &JevEvaluation, current_seq: u64, policy: &StalenessPolicy) -> bool {
    policy.is_usable(evaluation, current_seq)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jev::response::TickDistribution;

    fn synthetic_signal() -> V1Signal {
        V1Signal {
            yes_pressure_5s: 0.8,
            no_pressure_5s: 0.1,
            move_persists: 0.7,
            underreact_up: 0.9,
            underreact_down: 0.1,
            repricing: TickDistribution {
                up_3_plus: 0.1,
                up_2: 0.2,
                up_1: 0.4,
                flat: 0.1,
                down_1: 0.1,
                down_2: 0.05,
                down_3_plus: 0.05,
            },
            repricing_confidence: 0.8,
            fill_before_decay: 0.7,
            fill_toxic: 0.2,
        }
    }

    fn synthetic_evaluation(state_seq: u64, latency_ms: u64) -> JevEvaluation {
        JevEvaluation {
            market_id: "market-1".to_owned(),
            state_seq,
            sent_at_ms: 1_000,
            received_at_ms: 1_000 + latency_ms as i64,
            latency_ms,
            signal: synthetic_signal(),
        }
    }

    #[test]
    fn fresh_sequence_is_usable() {
        let policy = StalenessPolicy::default();
        let evaluation = synthetic_evaluation(10, 100);

        assert!(is_usable(&evaluation, 10, &policy));
    }

    #[test]
    fn lagged_sequence_is_rejected() {
        let policy = StalenessPolicy::default();
        let evaluation = synthetic_evaluation(7, 100);

        assert!(!is_usable(&evaluation, 10, &policy));
    }

    #[test]
    fn slow_evaluation_is_rejected() {
        let policy = StalenessPolicy::default();
        let evaluation = synthetic_evaluation(10, policy.max_latency_ms + 1);

        assert!(!is_usable(&evaluation, 10, &policy));
    }

    #[test]
    fn lag_boundary_is_usable_and_next_state_is_rejected() {
        let policy = StalenessPolicy::default();
        let boundary = synthetic_evaluation(10 - policy.max_lag, 100);
        let beyond_boundary = synthetic_evaluation(10 - policy.max_lag - 1, 100);

        assert!(is_usable(&boundary, 10, &policy));
        assert!(!is_usable(&beyond_boundary, 10, &policy));
    }

    #[test]
    fn stale_evaluation_cannot_produce_a_usable_signal() {
        let policy = StalenessPolicy::default();
        let mut actor = SignalActor::new(policy);
        actor.state_seq = 10;
        actor.record_evaluation(synthetic_evaluation(7, 100));

        assert!(actor.usable_signal().is_none());
    }
}
