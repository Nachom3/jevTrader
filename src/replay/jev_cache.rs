//! Reproducible Jev cache: state_hash + questions_hash + model + variant.
//!
//! Task 11 key rule: the full cache identity is `state_hash +
//! strategy_version + prompt_version + model_version +
//! question_schema_version` (plus legacy `questions_hash`/`model`/`variant`
//! scoping). Same input + same versions reuses the exact output; versions
//! isolate entries so a prompt/model upgrade can never masquerade as alpha.
//! Disk persistence (`save_to_dir`/`load_from_dir`) lets the cache survive
//! processes: first run MISSes, second run HITs with identical payloads.

use std::collections::HashMap;
use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};

/// Cache key: identical state + questions + model reuses the evaluation.
/// Never shares across models, questions, or differing states.
///
/// The four version fields are the Task 11 identity: the same state
/// evaluated under a different prompt/model/schema version is a different
/// entry, so version upgrades can never look like alpha.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct JevCacheKey {
    pub state_hash: String,
    pub questions_hash: String,
    pub model: String,
    pub variant: String,
    #[serde(default = "unknown_version")]
    pub strategy_version: String,
    #[serde(default = "unknown_version")]
    pub prompt_version: String,
    #[serde(default = "unknown_version")]
    pub model_version: String,
    #[serde(default = "unknown_version")]
    pub question_schema_version: String,
}

fn unknown_version() -> String {
    UNKNOWN_VERSION.to_owned()
}

/// Placeholder version used until the caller threads a real pinned version
/// (runner wiring lands in Task 10). Entries keyed with this value are
/// still correctly isolated; they are just coarse until wired.
pub const UNKNOWN_VERSION: &str = "unknown";

impl JevCacheKey {
    /// Legacy-shaped constructor: versions default to [`UNKNOWN_VERSION`]
    /// until the caller threads pinned versions (Task 10 runner wiring).
    #[must_use]
    pub fn new(state_hash: String, questions_hash: String, model: String, variant: String) -> Self {
        Self {
            state_hash,
            questions_hash,
            model,
            variant,
            strategy_version: UNKNOWN_VERSION.to_owned(),
            prompt_version: UNKNOWN_VERSION.to_owned(),
            model_version: UNKNOWN_VERSION.to_owned(),
            question_schema_version: UNKNOWN_VERSION.to_owned(),
        }
    }

    /// Fully-versioned constructor for callers with pinned versions.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new_versioned(
        state_hash: String,
        questions_hash: String,
        model: String,
        variant: String,
        strategy_version: String,
        prompt_version: String,
        model_version: String,
        question_schema_version: String,
    ) -> Self {
        Self {
            state_hash,
            questions_hash,
            model,
            variant,
            strategy_version,
            prompt_version,
            model_version,
            question_schema_version,
        }
    }
}

/// Cached Jev signal with its original request/response payloads.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedJev {
    pub envelope_json: String,
    pub latency_ms: u64,
    /// Whether the cached evaluation came from a live call (vs stub/assumed).
    pub live: bool,
    /// Exact request payload sent to Jev (audit/replay without re-querying).
    #[serde(default)]
    pub request_json: String,
    /// Parsed model output (audit without re-parsing the envelope).
    #[serde(default)]
    pub parsed_output_json: String,
    /// When the evaluated call started (ms epoch; 0 = unknown/assumed).
    #[serde(default)]
    pub jev_start_ts_ms: i64,
}

/// Reproducible in-memory Jev cache (persisted to JSON by the runner).
#[derive(Debug, Default)]
pub struct JevCache {
    map: HashMap<JevCacheKey, CachedJev>,
    pub hits: u64,
    pub misses: u64,
}

impl JevCache {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn get(&mut self, key: &JevCacheKey) -> Option<CachedJev> {
        match self.map.get(key) {
            Some(v) => {
                self.hits += 1;
                Some(v.clone())
            }
            None => {
                self.misses += 1;
                None
            }
        }
    }

    pub fn put(&mut self, key: JevCacheKey, value: CachedJev) {
        self.map.insert(key, value);
    }

    /// Persists every entry to `<dir>/jev_cache.json` (created if missing).
    /// Entries are sorted by key for byte-deterministic output. Returns the
    /// number of entries written.
    pub fn save_to_dir(&self, dir: &Path) -> Result<usize, String> {
        fs::create_dir_all(dir).map_err(|e| format!("create cache dir {}: {e}", dir.display()))?;
        let mut entries: Vec<(&JevCacheKey, &CachedJev)> = self.map.iter().collect();
        entries.sort_by(|(a, _), (b, _)| {
            (&a.state_hash, &a.strategy_version, &a.prompt_version).cmp(&(
                &b.state_hash,
                &b.strategy_version,
                &b.prompt_version,
            ))
        });
        let json = serde_json::to_string_pretty(&entries)
            .map_err(|e| format!("serialize jev cache: {e}"))?;
        fs::write(dir.join("jev_cache.json"), json).map_err(|e| format!("write jev cache: {e}"))?;
        Ok(self.map.len())
    }

    /// Rehydrates entries saved by [`Self::save_to_dir`]. Same key twice is
    /// idempotent (last wins, length unchanged). Hits/misses restart at zero:
    /// persistence preserves payloads, not counters. Missing file = empty
    /// cache (not an error), so a first run boots cleanly.
    pub fn load_from_dir(&mut self, dir: &Path) -> Result<usize, String> {
        let path = dir.join("jev_cache.json");
        let raw = match fs::read_to_string(&path) {
            Ok(raw) => raw,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
            Err(e) => return Err(format!("read jev cache {}: {e}", path.display())),
        };
        let rows: Vec<(JevCacheKey, CachedJev)> =
            serde_json::from_str(&raw).map_err(|e| format!("parse jev cache: {e}"))?;
        let mut loaded = 0usize;
        for (key, value) in rows {
            self.map.insert(key, value);
            loaded += 1;
        }
        Ok(loaded)
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.map.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(state: &str) -> JevCacheKey {
        JevCacheKey::new(
            state.to_owned(),
            "q".to_owned(),
            "jev-latest".to_owned(),
            "CONTROL".to_owned(),
        )
    }

    fn value() -> CachedJev {
        CachedJev {
            envelope_json: "{}".to_owned(),
            latency_ms: 100,
            live: false,
            request_json: String::new(),
            parsed_output_json: String::new(),
            jev_start_ts_ms: 0,
        }
    }

    #[test]
    fn hit_miss_accounting() {
        let mut c = JevCache::new();
        assert!(c.get(&key("a")).is_none());
        c.put(key("a"), value());
        assert!(c.get(&key("a")).is_some());
        assert_eq!((c.hits, c.misses), (1, 1));
    }

    #[test]
    fn never_shares_across_models_or_variants() {
        let mut c = JevCache::new();
        c.put(key("a"), value());
        let mut other_model = key("a");
        other_model.model = "other".to_owned();
        assert!(c.get(&other_model).is_none());
        let mut other_variant = key("a");
        other_variant.variant = "QUANT_V1".to_owned();
        assert!(c.get(&other_variant).is_none());
    }
}
