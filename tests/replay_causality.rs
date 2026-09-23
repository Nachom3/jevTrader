use jevtrader::replay::causality::{
    CausalityReason, CausalityStats, CausalityVerdict, audit_state,
};
use serde_json::json;

#[test]
fn clean_inputs_pass() {
    let verdict = audit_state(
        10_000,
        &[
            ("book", 9_900),
            ("underlying", 9_800),
            ("feature_lag", 9_000),
        ],
        &json!({"market": {}, "underlying": {}}),
    );

    assert_eq!(verdict, CausalityVerdict::Ok);
}

#[test]
fn one_future_input_rejects_the_state() {
    let verdict = audit_state(
        10_000,
        &[("book", 9_900), ("underlying_tick", 10_001)],
        &json!({"market": {}}),
    );

    assert_eq!(
        verdict,
        CausalityVerdict::Rejected {
            reason: CausalityReason::FutureInput {
                name: "underlying_tick".to_owned(),
                input_ts_ms: 10_001,
                t_ms: 10_000,
            },
        }
    );
}

#[test]
fn nested_resolution_outcome_field_rejects_the_state() {
    let verdict = audit_state(
        10_000,
        &[("book", 10_000)],
        &json!({"market": {}, "metadata": {"outcome": "yes"}}),
    );

    assert_eq!(
        verdict,
        CausalityVerdict::Rejected {
            reason: CausalityReason::ForbiddenField { field: "outcome" },
        }
    );
}

#[test]
fn counters_accumulate_one_result_per_audit() {
    let state = json!({"market": {}});
    let verdicts = [
        audit_state(10_000, &[("book", 10_000)], &state),
        audit_state(10_000, &[("tick", 10_001)], &state),
        audit_state(10_000, &[], &json!({"outcome": "no"})),
        audit_state(10_000, &[("lag", 9_999)], &state),
    ];
    let mut stats = CausalityStats::default();

    for verdict in &verdicts {
        stats.record(verdict);
    }

    assert_eq!(
        stats,
        CausalityStats {
            n_audited: 4,
            n_ok: 2,
            n_rejected_future_input: 1,
            n_rejected_forbidden_field: 1,
        }
    );
}

#[test]
fn input_at_state_timestamp_is_allowed() {
    let verdict = audit_state(
        10_000,
        &[("book", 10_000), ("underlying_tick", 10_000)],
        &json!({"market": {}}),
    );

    assert_eq!(verdict, CausalityVerdict::Ok);
}
