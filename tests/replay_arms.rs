use std::fs;
use std::path::{Path, PathBuf};

use jevtrader::replay::jev_cache::{CachedJev, JevCacheKey};
use jevtrader::replay::{Arm, ArmRun, JevCache, Side, TradeEpisode};

struct TempCacheDir {
    path: PathBuf,
}

impl TempCacheDir {
    fn new(test_name: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "jevtrader-replay-arms-{test_name}-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&path);
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempCacheDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn cache_key(
    variant: &str,
    prompt_version: &str,
    model_version: &str,
    question_schema_version: &str,
) -> JevCacheKey {
    JevCacheKey::new_versioned(
        "state-shared".to_owned(),
        "questions-v1".to_owned(),
        "jev-latest".to_owned(),
        variant.to_owned(),
        "strategy-v1".to_owned(),
        prompt_version.to_owned(),
        model_version.to_owned(),
        question_schema_version.to_owned(),
    )
}

fn cached_jev(response: &str, latency_ms: u64, live: bool, jev_start_ts_ms: i64) -> CachedJev {
    CachedJev {
        envelope_json: response.to_owned(),
        latency_ms,
        live,
        request_json: format!("request-{response}"),
        parsed_output_json: format!("parsed-{response}"),
        jev_start_ts_ms,
    }
}

fn assert_cached_jev(expected: &CachedJev, actual: &CachedJev) {
    assert_eq!(actual.envelope_json, expected.envelope_json);
    assert_eq!(actual.latency_ms, expected.latency_ms);
    assert_eq!(actual.live, expected.live);
    assert_eq!(actual.request_json, expected.request_json);
    assert_eq!(actual.parsed_output_json, expected.parsed_output_json);
    assert_eq!(actual.jev_start_ts_ms, expected.jev_start_ts_ms);
}

#[test]
fn cache_persistence_preserves_versioned_arm_entries_and_accounting() {
    let temp_dir = TempCacheDir::new("persistence");
    let entries = vec![
        (
            cache_key(
                Arm::QuantOnly.as_str(),
                "prompt-v1",
                "model-v1",
                "schema-v1",
            ),
            cached_jev("response-quant-v1", 101, true, 10_001),
        ),
        (
            cache_key(
                Arm::QuantOnly.as_str(),
                "prompt-v2",
                "model-v1",
                "schema-v1",
            ),
            cached_jev("response-quant-v2", 202, false, 20_002),
        ),
        (
            cache_key(Arm::JevOnly.as_str(), "prompt-v1", "model-v1", "schema-v1"),
            cached_jev("response-jev-v1", 303, true, 30_003),
        ),
    ];

    let mut original = JevCache::new();
    for (key, value) in &entries {
        original.put(key.clone(), value.clone());
    }
    assert_eq!(original.save_to_dir(temp_dir.path()), Ok(3));

    let mut loaded = JevCache::new();
    assert_eq!(loaded.load_from_dir(temp_dir.path()), Ok(3));
    assert_eq!(loaded.len(), 3);
    assert_eq!((loaded.hits, loaded.misses), (0, 0));

    for (key, expected) in &entries {
        let actual = loaded.get(key).expect("persisted cache entry should hit");
        assert_cached_jev(expected, &actual);
    }
    assert_eq!((loaded.hits, loaded.misses), (3, 0));

    let missing = cache_key(
        Arm::JevOnly.as_str(),
        "prompt-missing",
        "model-v1",
        "schema-v1",
    );
    assert!(loaded.get(&missing).is_none());
    assert_eq!((loaded.hits, loaded.misses), (3, 1));

    let len_before_second_load = loaded.len();
    assert_eq!(loaded.load_from_dir(temp_dir.path()), Ok(3));
    assert_eq!(loaded.len(), len_before_second_load);
}

#[test]
fn cache_versions_isolate_same_state_entries() {
    let temp_dir = TempCacheDir::new("version-isolation");
    let base = cache_key(
        Arm::QuantOnly.as_str(),
        "prompt-v1",
        "model-v1",
        "schema-v1",
    );
    let mut cache = JevCache::new();
    cache.put(base.clone(), cached_jev("base-response", 100, true, 1_000));

    assert!(cache.get(&base).is_some());
    assert!(
        cache
            .get(&cache_key(
                Arm::QuantOnly.as_str(),
                "prompt-v2",
                "model-v1",
                "schema-v1"
            ))
            .is_none()
    );
    assert!(
        cache
            .get(&cache_key(
                Arm::QuantOnly.as_str(),
                "prompt-v1",
                "model-v2",
                "schema-v1"
            ))
            .is_none()
    );
    assert!(
        cache
            .get(&cache_key(
                Arm::QuantOnly.as_str(),
                "prompt-v1",
                "model-v1",
                "schema-v2"
            ))
            .is_none()
    );
    assert_eq!((cache.hits, cache.misses), (1, 3));

    assert_eq!(cache.save_to_dir(temp_dir.path()), Ok(1));
}

#[test]
fn arm_policies_and_serialized_names_are_explicit() {
    let cases = [
        (Arm::QuantOnly, false, true, false),
        (Arm::JevOnly, true, false, false),
        (Arm::QuantPlusJev, true, true, false),
        (Arm::MicroPlusRegime, true, false, true),
    ];

    for (arm, calls_jev, uses_quant, uses_micro_regime) in cases {
        let policy = arm.policy();
        assert_eq!(policy.calls_jev, calls_jev);
        assert_eq!(policy.uses_quant, uses_quant);
        assert_eq!(policy.uses_micro_regime, uses_micro_regime);
        assert_eq!(
            serde_json::to_string(&arm).expect("serialize arm"),
            format!("\"{}\"", arm.as_str())
        );
    }
}

fn episode() -> TradeEpisode {
    TradeEpisode::new(
        "episode-arm",
        "strategy-v1",
        "market-1",
        "BTC",
        "5s",
        1_000,
        900,
        100,
        50,
        Side::BuyYes,
        0.40,
        10.0,
    )
    .expect("valid episode")
}

#[test]
fn identical_arm_lineage_episodes_have_identical_json() {
    let mut left = episode();
    let mut right = episode();
    left.set_arm(Arm::QuantPlusJev.as_str());
    right.set_arm(Arm::QuantPlusJev.as_str());

    let left_json = serde_json::to_string(&left).expect("serialize left episode");
    let right_json = serde_json::to_string(&right).expect("serialize right episode");

    assert_eq!(left_json, right_json);
    assert_eq!(left.arm.as_deref(), Some("QUANT_PLUS_JEV"));
    assert!(left_json.contains("\"arm\":\"QUANT_PLUS_JEV\""));
}

#[test]
fn arm_runs_share_the_same_infrastructure_manifest() {
    let arms = [
        Arm::QuantOnly,
        Arm::JevOnly,
        Arm::QuantPlusJev,
        Arm::MicroPlusRegime,
    ];
    let runs: Vec<ArmRun> = arms
        .into_iter()
        .map(|arm| {
            ArmRun::new(
                arm,
                "conservative",
                "standard",
                "resolution",
                "shared-capital",
            )
        })
        .collect();

    assert_eq!(runs.len(), arms.len());
    for (run, arm) in runs.iter().zip(arms) {
        assert_eq!(run.arm, arm);
        assert_eq!(run.fill_profile, "conservative");
        assert_eq!(run.fee_regime, "standard");
        assert_eq!(run.exit_policy, "resolution");
        assert_eq!(run.capital_note, "shared-capital");
    }
}
