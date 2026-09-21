use jevtrader::replay::{
    ExecutionPath, ResolutionTimeFilter, Side, TradeEpisode, classify_execution, passes, path_of,
};

fn episode() -> TradeEpisode {
    TradeEpisode::new(
        "episode-1",
        "lead-lag-v1",
        "market-1",
        "BTC",
        "5s",
        0,
        0,
        0,
        0,
        Side::BuyYes,
        0.40,
        10.0,
    )
    .expect("valid episode")
}

fn resolved_episode(provenance: Option<&str>) -> TradeEpisode {
    let mut episode = episode();
    episode
        .set_resolution(1_000)
        .expect("resolution timestamp should be valid");
    if let Some(provenance) = provenance {
        episode
            .set_resolution_time_provenance(provenance)
            .expect("resolution-time provenance should be valid");
    }
    episode
}

#[test]
fn classify_execution_covers_all_paths_without_guessing() {
    assert_eq!(
        classify_execution(Some(true), Some(true)),
        ExecutionPath::MakerMaker
    );
    assert_eq!(
        classify_execution(Some(true), Some(false)),
        ExecutionPath::MakerTaker
    );
    assert_eq!(
        classify_execution(Some(false), Some(true)),
        ExecutionPath::TakerMaker
    );
    assert_eq!(
        classify_execution(Some(false), Some(false)),
        ExecutionPath::TakerTaker
    );
    assert_eq!(classify_execution(None, Some(true)), ExecutionPath::Unknown);
}

#[test]
fn path_of_uses_entry_and_exit_liquidity() {
    let cases = [
        ("maker", "maker", ExecutionPath::MakerMaker),
        ("maker", "taker", ExecutionPath::MakerTaker),
        ("taker", "maker", ExecutionPath::TakerMaker),
        ("taker", "taker", ExecutionPath::TakerTaker),
    ];

    for (entry, exit, expected) in cases {
        let mut episode = episode();
        episode
            .set_entry_liquidity(entry)
            .expect("entry liquidity should be valid");
        episode
            .set_exit_liquidity(exit)
            .expect("exit liquidity should be valid");
        assert_eq!(path_of(&episode), expected);
    }

    let mut invalid = episode();
    invalid.entry_liquidity = Some("mid".to_owned());
    invalid.exit_liquidity = Some("maker".to_owned());
    assert_eq!(path_of(&invalid), ExecutionPath::Unknown);
}

#[test]
fn liquidity_setter_rejects_unknown_vocabulary_without_mutating() {
    let mut episode = episode();
    episode
        .set_entry_liquidity("maker")
        .expect("maker should be valid");

    let error = episode
        .set_entry_liquidity("mid")
        .expect_err("mid is not a liquidity role");

    assert!(error.contains("maker or taker"));
    assert_eq!(episode.entry_liquidity.as_deref(), Some("maker"));
}

#[test]
fn resolution_time_filters_separate_exact_proxy_and_pending() {
    let exact = resolved_episode(Some("exact"));
    let proxy = resolved_episode(Some("proxy"));
    let pending = resolved_episode(None);
    let unresolved = episode();

    assert!(passes(&exact, ResolutionTimeFilter::ExactOnly));
    assert!(!passes(&proxy, ResolutionTimeFilter::ExactOnly));
    assert!(!passes(&pending, ResolutionTimeFilter::ExactOnly));
    assert!(!passes(&unresolved, ResolutionTimeFilter::ExactOnly));

    assert!(passes(&exact, ResolutionTimeFilter::IncludeProxy));
    assert!(passes(&proxy, ResolutionTimeFilter::IncludeProxy));
    assert!(passes(&pending, ResolutionTimeFilter::IncludeProxy));
    assert!(passes(&unresolved, ResolutionTimeFilter::IncludeProxy));
}

#[test]
fn only_maker_maker_is_the_headline_path() {
    assert!(ExecutionPath::MakerMaker.is_headline());
    assert!(!ExecutionPath::MakerTaker.is_headline());
    assert!(!ExecutionPath::TakerMaker.is_headline());
    assert!(!ExecutionPath::TakerTaker.is_headline());
    assert!(!ExecutionPath::Unknown.is_headline());
}

#[test]
fn serde_roundtrip_preserves_split_provenance_and_liquidity() {
    let mut episode = resolved_episode(Some("exact"));
    episode
        .set_resolution_outcome("yes")
        .expect("resolution outcome should be valid");
    episode
        .set_outcome_provenance("proxy")
        .expect("outcome provenance should be valid");
    episode
        .set_entry_liquidity("maker")
        .expect("entry liquidity should be valid");
    episode
        .set_exit_liquidity("taker")
        .expect("exit liquidity should be valid");

    let json = serde_json::to_string(&episode).expect("episode should serialize");
    let roundtrip: TradeEpisode = serde_json::from_str(&json).expect("episode should deserialize");

    assert_eq!(roundtrip.outcome_provenance.as_deref(), Some("proxy"));
    assert_eq!(
        roundtrip.resolution_time_provenance.as_deref(),
        Some("exact")
    );
    assert_eq!(roundtrip.entry_liquidity.as_deref(), Some("maker"));
    assert_eq!(roundtrip.exit_liquidity.as_deref(), Some("taker"));
}
