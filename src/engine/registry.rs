//! Registry and lifecycle state for the supported multi-market runtime.

use std::collections::HashMap;

use thiserror::Error;

use crate::domain::MarketKey;
use crate::market_spec::MarketSpec;

/// Runtime lifecycle for one configured market contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lifecycle {
    Discovered,
    Active,
    NearResolution,
    Resolved,
    RolledOver,
}

impl Lifecycle {
    /// Advances one lifecycle state using the caller-provided remaining time.
    ///
    /// The transition is deliberately pure: it does not read a clock or
    /// perform discovery. A resolved runtime becomes rolled over on the next
    /// explicit lifecycle advance; replacing its spec remains the caller's
    /// responsibility.
    #[must_use]
    pub const fn advance(self, time_remaining_secs: u64) -> Self {
        match self {
            Self::Discovered if time_remaining_secs <= 60 => Self::NearResolution,
            Self::Discovered => Self::Active,
            Self::Active if time_remaining_secs <= 60 => Self::NearResolution,
            Self::Active => Self::Active,
            Self::NearResolution if time_remaining_secs == 0 => Self::Resolved,
            Self::NearResolution => Self::NearResolution,
            Self::Resolved => Self::RolledOver,
            Self::RolledOver => Self::RolledOver,
        }
    }
}

/// The market metadata and lifecycle state owned by the registry.
#[derive(Debug, Clone, PartialEq)]
pub struct MarketRuntime {
    pub key: MarketKey,
    pub spec: MarketSpec,
    pub condition_id: String,
    pub yes_token_id: String,
    pub lifecycle: Lifecycle,
    /// Verified winning token once resolution is observed; `None` before.
    pub winning_token_id: Option<String>,
}

impl MarketRuntime {
    /// Returns whether this contract may reach the strategy pipeline.
    #[must_use]
    pub const fn is_tradable(&self) -> bool {
        matches!(
            self.lifecycle,
            Lifecycle::Active | Lifecycle::NearResolution
        )
    }

    /// Records the venue-verified winning token and closes the lifecycle.
    ///
    /// The token is stored verbatim; nothing is inferred about other
    /// outcomes. Use [`Self::yes_won`] to map the YES case without inventing
    /// NO/FIFTY outcomes: a non-YES token yields `None` and the caller must
    /// confirm the outcome from the resolution source before settling.
    pub fn note_resolution(&mut self, winning_token_id: impl Into<String>) {
        self.winning_token_id = Some(winning_token_id.into());
        self.lifecycle = Lifecycle::Resolved;
    }

    /// Maps a recorded resolution to the YES outcome, if possible.
    ///
    /// Returns `Some(true)` only when the winning token equals this
    /// contract's YES token. Any other recorded token returns `None`: the
    /// registry cannot distinguish NO from FIFTY/unknown from token ids
    /// alone, so the caller settles those explicitly via
    /// [`crate::engine::Pipeline::settle_at`] with a verified price.
    #[must_use]
    pub fn yes_won(&self) -> Option<bool> {
        let winning = self.winning_token_id.as_deref()?;
        if winning == self.yes_token_id {
            Some(true)
        } else {
            None
        }
    }
}

/// Errors returned while building the bounded market registry.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum RegistryError {
    #[error("market spec `{slug}` does not identify a supported asset and horizon")]
    UnknownMarketKey { slug: String },
    #[error("market key `{key}` is already registered")]
    DuplicateMarket { key: MarketKey },
}

/// Bounded registry keyed by the canonical asset/horizon pair.
#[derive(Debug, Default)]
pub struct MarketRegistry {
    markets: HashMap<MarketKey, MarketRuntime>,
}

impl MarketRegistry {
    /// Creates an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers one market, inferring its key from the market specification.
    pub fn register(
        &mut self,
        spec: MarketSpec,
        condition_id: impl Into<String>,
        yes_token_id: impl Into<String>,
    ) -> Result<(), RegistryError> {
        let key = spec
            .market_key()
            .ok_or_else(|| RegistryError::UnknownMarketKey {
                slug: spec.slug.clone(),
            })?;
        if self.markets.contains_key(&key) {
            return Err(RegistryError::DuplicateMarket { key });
        }

        self.markets.insert(
            key,
            MarketRuntime {
                key,
                spec,
                condition_id: condition_id.into(),
                yes_token_id: yes_token_id.into(),
                lifecycle: Lifecycle::Discovered,
                winning_token_id: None,
            },
        );
        Ok(())
    }

    /// Returns one registered runtime.
    #[must_use]
    pub fn get(&self, key: &MarketKey) -> Option<&MarketRuntime> {
        self.markets.get(key)
    }

    /// Returns one registered runtime mutably.
    pub fn get_mut(&mut self, key: &MarketKey) -> Option<&mut MarketRuntime> {
        self.markets.get_mut(key)
    }

    /// Marks a contract resolved after an authoritative venue event.
    ///
    /// This is a lifecycle passthrough only. It does not invent or store a
    /// winning outcome; live settlement still needs the verified Gamma or
    /// `market_resolved` event and must call the owning
    /// [`crate::engine::Pipeline::settle`] separately.
    pub fn mark_resolved(&mut self, key: &MarketKey) -> bool {
        let Some(runtime) = self.markets.get_mut(key) else {
            return false;
        };
        runtime.lifecycle = Lifecycle::Resolved;
        true
    }

    /// Lifecycle-only alias for [`Self::mark_resolved`].
    ///
    /// The registry has no portfolio or resolution oracle, so this hook only
    /// closes the runtime lifecycle. The caller supplies the verified outcome
    /// to the pipeline settlement boundary.
    pub fn settle(&mut self, key: &MarketKey) -> bool {
        self.mark_resolved(key)
    }

    /// Number of registered contracts.
    #[must_use]
    pub fn len(&self) -> usize {
        self.markets.len()
    }

    /// Whether no contracts are registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.markets.is_empty()
    }

    /// Iterates over the registered canonical keys.
    pub fn keys(&self) -> impl Iterator<Item = &MarketKey> {
        self.markets.keys()
    }
}

/// Calculates the next resolution window for one supported market family.
///
/// The input is the current contract's resolution timestamp. The returned
/// pair is `(start_ms, end_ms)` for the next window: its start is the current
/// end and its end is one horizon later.
#[must_use]
pub fn rollover_spec(key: MarketKey, resolution_at_ms: i64) -> (i64, i64) {
    let start_ms = resolution_at_ms;
    let duration_ms = key.horizon.seconds().saturating_mul(1_000);
    let end_ms = start_ms.saturating_add(duration_ms as i64);
    (start_ms, end_ms)
}
