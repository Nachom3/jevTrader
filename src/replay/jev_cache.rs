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

#[path = "versions.rs"]
pub mod versions;

pub use versions::VersionPins;

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
    #[serde(default = "unknown_version")]
    pub question_document_sha256: String,
    #[serde(default = "unknown_version")]
    pub feature_builder_version: String,
    #[serde(default = "unknown_version")]
    pub normalization_version: String,
    #[serde(default = "unknown_version")]
    pub serialization_version: String,
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
            question_document_sha256: UNKNOWN_VERSION.to_owned(),
            feature_builder_version: UNKNOWN_VERSION.to_owned(),
            normalization_version: UNKNOWN_VERSION.to_owned(),
            serialization_version: UNKNOWN_VERSION.to_owned(),
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
            question_document_sha256: UNKNOWN_VERSION.to_owned(),
            feature_builder_version: UNKNOWN_VERSION.to_owned(),
            normalization_version: UNKNOWN_VERSION.to_owned(),
            serialization_version: UNKNOWN_VERSION.to_owned(),
        }
    }

    /// Adds the phase-1 identity fields without breaking older callers that
    /// use the eight-argument Task 11 constructor.
    #[must_use]
    pub fn with_extended_versions(
        mut self,
        question_document_sha256: String,
        feature_builder_version: String,
        normalization_version: String,
        serialization_version: String,
    ) -> Self {
        self.question_document_sha256 = question_document_sha256;
        self.feature_builder_version = feature_builder_version;
        self.normalization_version = normalization_version;
        self.serialization_version = serialization_version;
        self
    }

    /// Builds a full phase-1 key from the frozen version tuple.
    #[must_use]
    pub fn new_precompute(
        state_hash: String,
        questions_hash: String,
        pins: &VersionPins,
    ) -> Self {
        Self::new_versioned(
            state_hash,
            questions_hash,
            pins.model_id.clone(),
            pins.variant.clone(),
            pins.strategy_version.clone(),
            pins.prompt_version.clone(),
            pins.model_version.clone(),
            pins.question_schema_version.clone(),
        )
        .with_extended_versions(
            pins.question_document_sha256.clone(),
            pins.feature_builder_version.clone(),
            pins.normalization_version.clone(),
            pins.serialization_version.clone(),
        )
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

/// Phase-1 per-entry audit fields kept separate from [`CachedJev`] so the
/// older replay/campaign struct literals remain source-compatible.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheEntryMetadata {
    pub attempt_count: u32,
    pub final_error: Option<String>,
    pub observed_latency_ms: u64,
    #[serde(default)]
    pub attempt_durations_ms: Vec<u64>,
    pub version: VersionPins,
}

impl Default for CacheEntryMetadata {
    fn default() -> Self {
        Self {
            attempt_count: 0,
            final_error: None,
            observed_latency_ms: 0,
            attempt_durations_ms: Vec::new(),
            version: VersionPins::unknown(),
        }
    }
}

/// Reproducible in-memory Jev cache (persisted to JSON by the runner).
#[derive(Debug, Default)]
pub struct JevCache {
    map: HashMap<JevCacheKey, CachedJev>,
    metadata: HashMap<JevCacheKey, CacheEntryMetadata>,
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

    /// Stores a precompute entry and its complete audit metadata.
    pub fn put_precompute(
        &mut self,
        key: JevCacheKey,
        value: CachedJev,
        metadata: CacheEntryMetadata,
    ) {
        self.metadata.insert(key.clone(), metadata);
        self.map.insert(key, value);
    }

    /// Returns phase-1 audit metadata for a key, if it was persisted.
    #[must_use]
    pub fn metadata(&self, key: &JevCacheKey) -> Option<CacheEntryMetadata> {
        self.metadata.get(key).cloned()
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

        // Keep the historical pair-array file stable and write the additive
        // phase-1 audit record beside it.
        let mut metadata_entries: Vec<(&JevCacheKey, &CacheEntryMetadata)> =
            self.metadata.iter().collect();
        metadata_entries.sort_by(|(a, _), (b, _)| {
            (&a.state_hash, &a.strategy_version, &a.prompt_version).cmp(&(
                &b.state_hash,
                &b.strategy_version,
                &b.prompt_version,
            ))
        });
        let metadata_json = serde_json::to_string_pretty(&metadata_entries)
            .map_err(|e| format!("serialize jev cache metadata: {e}"))?;
        fs::write(dir.join("jev_cache_metadata.json"), metadata_json)
            .map_err(|e| format!("write jev cache metadata: {e}"))?;
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
        let metadata_path = dir.join("jev_cache_metadata.json");
        if let Ok(metadata_raw) = fs::read_to_string(&metadata_path) {
            let metadata_rows: Vec<(JevCacheKey, CacheEntryMetadata)> = serde_json::from_str(
                &metadata_raw,
            )
            .map_err(|e| format!("parse jev cache metadata: {e}"))?;
            for (key, metadata) in metadata_rows {
                self.metadata.insert(key, metadata);
            }
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
