use std::collections::HashMap;

use jevtrader::replay::{
    Fidelity, HistoricalEvent, Provenance, ReplayConfig, ReplayRunner, ResolutionOutcome,
    ResolutionSkipReason, ResolutionSpec, Side, StubJev, TradeEpisode, resolve_market,
};

fn spec(
    condition_id: &str,
    market_id: &str,
    fidelity: Fidelity,
    resolution_at_ms: i64,
) -> ResolutionSpec {
    ResolutionSpec {
        condition_id: condition_id.to_owned(),
        market_id: market_id.to_owned(),
        asset: "BTC".to_owned(),
        horizon: "5m".to_owned(),
        resolution_source: "official".to_owned(),
        resolution_rule_excerpt: "rule".to_owned(),
        fidelity,
        resolution_at_ms,
        reference: None,
        strike: None,
        start_at: None,
        end_at: None,
    }
}

#[test]
fn exact_outcome_with_valid_timestamp_resolves() {
    let resolved = resolve_market(
        &spec("condition-1", "market-1", Fidelity::Exact, 1_000),
        Some(ResolutionOutcome::Yes),
        Provenance::Exact,
        false,
    )
    .expect("exact resolution should be accepted");

    assert_eq!(resolved.outcome, ResolutionOutcome::Yes);
    assert_eq!(resolved.provenance, Provenance::Exact);
    assert_eq!(resolved.resolved_at_ms, 1_000);
}

#[test]
fn missing_outcome_is_a_skip_and_is_never_inferred() {
    let skip = resolve_market(
        &spec("condition-1", "market-1", Fidelity::Exact, 1_000),
        None,
        Provenance::Exact,
        false,
    )
    .expect_err("missing outcome must skip");

    assert_eq!(skip.reason, ResolutionSkipReason::MissingOutcome);
}

#[test]
fn invalid_timestamp_or_condition_is_a_skip() {
    let timestamp_skip = resolve_market(
        &spec("condition-1", "market-1", Fidelity::Exact, 0),
        Some(ResolutionOutcome::No),
        Provenance::Exact,
        false,
    )
    .expect_err("non-positive timestamp must skip");
    assert_eq!(
        timestamp_skip.reason,
        ResolutionSkipReason::MissingTimestamp
    );

    let condition_skip = resolve_market(
        &spec("", "market-1", Fidelity::Exact, 1_000),
        Some(ResolutionOutcome::No),
        Provenance::Exact,
        false,
    )
    .expect_err("empty condition must skip");
    assert_eq!(
        condition_skip.reason,
        ResolutionSkipReason::UnknownCondition
    );
}

#[test]
fn proxy_requires_explicit_acceptance() {
    let proxy_spec = spec("condition-1", "market-1", Fidelity::Proxy, 1_000);
    let rejected = resolve_market(
        &proxy_spec,
        Some(ResolutionOutcome::No),
        Provenance::Proxy,
        false,
    )
    .expect_err("proxy must be opt-in");
    assert_eq!(rejected.reason, ResolutionSkipReason::ProxyNotAllowed);

    let accepted = resolve_market(
        &proxy_spec,
        Some(ResolutionOutcome::No),
        Provenance::Proxy,
        true,
    )
    .expect("accepted proxy should resolve");
    assert_eq!(accepted.provenance, Provenance::Proxy);
}

#[test]
fn exact_provenance_without_exact_fidelity_is_not_a_silent_proxy() {
    let skip = resolve_market(
        &spec("condition-1", "market-1", Fidelity::Proxy, 1_000),
        Some(ResolutionOutcome::Yes),
        Provenance::Exact,
        true,
    )
    .expect_err("exact provenance cannot downgrade fidelity");
    assert_eq!(skip.reason, ResolutionSkipReason::UnreliableFidelity);

    let missing = resolve_market(
        &spec("condition-1", "market-1", Fidelity::Exact, 1_000),
        None,
        Provenance::Exact,
        true,
    )
    .expect_err("exact with no outcome must remain unresolved");
    assert_eq!(missing.reason, ResolutionSkipReason::MissingOutcome);
}

#[test]
fn trade_without_a_prior_book_is_skipped_without_fabricating_a_fill_book() {
    let resolved = resolve_market(
        &spec("condition-1", "market-1", Fidelity::Exact, 10_000),
        Some(ResolutionOutcome::Yes),
        Provenance::Exact,
        false,
    )
    .expect("valid resolution");
    let resolutions = HashMap::from([(String::from("condition-1"), resolved)]);
    let meta = [(
        String::from("market-1"),
        String::from("BTC"),
        String::from("5m"),
        jevtrader::replay::Split::OutOfSample,
        Fidelity::Exact,
        String::from("UNKNOWN-UNKNOWN"),
    )];
    let trade = HistoricalEvent::PolyTrade {
        ts_ms: 1_000,
        condition_id: String::from("condition-1"),
        price: 0.50,
        size: 1.0,
        aggressor: Some(String::from("BUY")),
        direction_quality: String::from("GROUND_TRUTH"),
        source: String::from("test"),
    };
    let mut runner = ReplayRunner::new(ReplayConfig::smoke("resolution-test"), StubJev::new(42));
    let output = runner.run_events_by_condition_resolved(
        vec![vec![trade]],
        &meta,
        &HashMap::from([(String::from("condition-1"), meta[0].clone())]),
        &HashMap::new(),
        None,
        &resolutions,
    );

    assert!(output.rows.is_empty());
    assert_eq!(
        output.incomplete_pairs, 1,
        "the unmatched trade is counted as skipped"
    );
}

#[test]
fn ledger_requires_timestamp_before_outcome_and_allows_pending_timestamp() {
    let mut episode = TradeEpisode::new(
        "episode-1",
        "lead-lag-v1",
        "market-1",
        "BTC",
        "5m",
        1_000,
        1_000,
        0,
        0,
        Side::BuyYes,
        0.40,
        10.0,
    )
    .expect("valid episode");

    assert!(episode.set_resolution_outcome("yes").is_err());
    episode
        .set_resolution(2_000)
        .expect("timestamp-only resolution is pending, not invalid");
    assert_eq!(episode.resolution_outcome, None);
    episode
        .set_resolution_outcome("yes")
        .expect("outcome now has a timestamp");
    episode
        .set_resolution_provenance("exact")
        .expect("provenance now has a timestamp");
    assert_eq!(episode.resolution_outcome.as_deref(), Some("yes"));
    assert_eq!(episode.resolution_provenance.as_deref(), Some("exact"));
}
