//! Fail-closed audit for information available to a replay state.
//!
//! The audit checks input timestamps against the state time and prevents known
//! resolution-truth fields from entering the serialized Jev state.

use serde_json::Value;

/// Fields that would expose labels or resolution timing to a state judgment.
///
/// These names are intentionally based on observed source fields, not guessed
/// aliases. `outcome` is read from the Kachoio market parquet as a label in
/// `src/bin/precompute_jev.rs:538-545`; `resolved_ts_ms` is read from resolution
/// records in `src/bin/precompute_jev.rs:974-988`. The V1 request contract at
/// `src/jev/request.rs:14-58` has no outcome or resolution timestamp. Its
/// `candidate_order.side` (`src/jev/request.rs:25-32`) is the proposed order
/// side, not a winning side, so it is not forbidden. No winning-side field is
/// present in the inspected V1 state/request contract.
const FORBIDDEN_FIELDS: &[&str] = &["outcome", "resolved_ts_ms"];

/// Why a state failed the causal audit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CausalityReason {
    /// An input became available after the state timestamp.
    FutureInput {
        name: String,
        input_ts_ms: i64,
        t_ms: i64,
    },
    /// A serialized field carries resolution truth or resolution timing.
    ForbiddenField { field: &'static str },
}

/// Result of auditing one replay state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CausalityVerdict {
    /// Every supplied input was available by `t_ms`, and no forbidden field
    /// was present in the state JSON.
    Ok,
    /// The state failed a causal invariant.
    Rejected { reason: CausalityReason },
}

/// Aggregate counts for calls to [`audit_state`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CausalityStats {
    pub n_audited: usize,
    pub n_ok: usize,
    pub n_rejected_future_input: usize,
    pub n_rejected_forbidden_field: usize,
}

impl CausalityStats {
    /// Record exactly one audit result in these aggregate counters.
    pub fn record(&mut self, verdict: &CausalityVerdict) {
        self.n_audited += 1;
        match verdict {
            CausalityVerdict::Ok => self.n_ok += 1,
            CausalityVerdict::Rejected {
                reason: CausalityReason::FutureInput { .. },
            } => self.n_rejected_future_input += 1,
            CausalityVerdict::Rejected {
                reason: CausalityReason::ForbiddenField { .. },
            } => self.n_rejected_forbidden_field += 1,
        }
    }
}

/// Audit one state against its timestamp and the timestamps of all source data.
///
/// `inputs` must include every data input used to build the state (for example,
/// the book snapshot, each underlying tick, and each feature lag). Inputs at
/// exactly `t_ms` are valid; any later input rejects the state.
#[must_use]
pub fn audit_state(t_ms: i64, inputs: &[(&str, i64)], state_json: &Value) -> CausalityVerdict {
    if let Some((name, input_ts_ms)) = inputs.iter().find(|(_, input_ts_ms)| *input_ts_ms > t_ms) {
        return CausalityVerdict::Rejected {
            reason: CausalityReason::FutureInput {
                name: (*name).to_owned(),
                input_ts_ms: *input_ts_ms,
                t_ms,
            },
        };
    }

    if let Some(field) = find_forbidden_field(state_json) {
        return CausalityVerdict::Rejected {
            reason: CausalityReason::ForbiddenField { field },
        };
    }

    CausalityVerdict::Ok
}

fn find_forbidden_field(value: &Value) -> Option<&'static str> {
    match value {
        Value::Object(object) => {
            for field in FORBIDDEN_FIELDS {
                if object.contains_key(*field) {
                    return Some(field);
                }
            }
            object.values().find_map(find_forbidden_field)
        }
        Value::Array(values) => values.iter().find_map(find_forbidden_field),
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => None,
    }
}
