//! Reproducible Jev cache: state_hash + questions_hash + model + variant.

use std::collections::HashMap;

/// Cache key: identical state + questions + model reuses the evaluation.
/// Never shares across models, questions, or differing states.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct JevCacheKey {
    pub state_hash: String,
    pub questions_hash: String,
    pub model: String,
    pub variant: String,
}

/// Cached Jev signal with its original request/response payloads.
#[derive(Debug, Clone)]
pub struct CachedJev {
    pub signal_json: String,
    pub latency_ms: u64,
    /// Whether the cached evaluation came from a live call (vs stub/assumed).
    pub live: bool,
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
        JevCacheKey {
            state_hash: state.to_owned(),
            questions_hash: "q".to_owned(),
            model: "jev-latest".to_owned(),
            variant: "CONTROL".to_owned(),
        }
    }

    #[test]
    fn hit_miss_accounting() {
        let mut c = JevCache::new();
        assert!(c.get(&key("a")).is_none());
        c.put(
            key("a"),
            CachedJev {
                signal_json: "{}".to_owned(),
                latency_ms: 100,
                live: false,
            },
        );
        assert!(c.get(&key("a")).is_some());
        assert_eq!((c.hits, c.misses), (1, 1));
    }

    #[test]
    fn never_shares_across_models_or_variants() {
        let mut c = JevCache::new();
        c.put(
            key("a"),
            CachedJev {
                signal_json: "{}".to_owned(),
                latency_ms: 1,
                live: false,
            },
        );
        let mut other_model = key("a");
        other_model.model = "other".to_owned();
        assert!(c.get(&other_model).is_none());
        let mut other_variant = key("a");
        other_variant.variant = "QUANT_V1".to_owned();
        assert!(c.get(&other_variant).is_none());
    }
}
