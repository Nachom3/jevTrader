//! Synthetic YES quotes reconstructed from an observed NO top of book.
//!
//! The reconstruction assumes the binary-market no-arbitrage relationship
//! `YES + NO ~= 1.00..1.01`. It is valid for pricing, fair value, and
//! direction inputs, but it is not an observed YES book.
//!
//! The field taxonomy is enforced by [`SyntheticComplement`]: native YES
//! queue fields are **FORBIDDEN**, and native YES fill dynamics are
//! **QUARANTINED**. Neither is represented on the output type, so a caller
//! cannot accidentally treat synthetic prices as native queue or fill data.

use serde::{Deserialize, Serialize};

use super::types::Fidelity;

/// Stable source label for reconstructed YES observations.
pub const SYNTHETIC_COMPLEMENT_SOURCE: &str = "SYNTHETIC_COMPLEMENT";

/// An observed NO-side top-of-book snapshot.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NoTopOfBook {
    pub no_bid: f64,
    pub no_ask: f64,
    pub no_bid_size: f64,
    pub no_ask_size: f64,
    pub ts_ms: i64,
    pub market_id: String,
}

impl NoTopOfBook {
    /// Returns the NO midpoint implied by the two top-of-book prices.
    #[must_use]
    pub fn no_mid(&self) -> f64 {
        (self.no_bid + self.no_ask) / 2.0
    }
}

/// YES-side pricing fields reconstructed from a NO-side top of book.
///
/// This type intentionally has no native YES queue or fill fields. Native YES
/// queue evidence is **FORBIDDEN** here, while YES fill conclusions remain
/// **QUARANTINED** until an independent model exists.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SyntheticComplement {
    pub yes_bid: f64,
    pub yes_ask: f64,
    pub yes_bid_size: f64,
    pub yes_ask_size: f64,
    pub yes_mid: f64,
    pub ts_ms: i64,
    pub market_id: String,
    pub fidelity: Fidelity,
    pub source: String,
}

/// Accounting for every input presented to the complement builder.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComplementStats {
    pub n_input: usize,
    pub n_ok: usize,
    pub n_rejected_bad_spread: usize,
    pub n_rejected_range: usize,
}

impl ComplementStats {
    /// Builds one complement while updating honest acceptance/rejection counts.
    pub fn observe(&mut self, input: &NoTopOfBook) -> Option<SyntheticComplement> {
        self.n_input += 1;
        match build_complement(input) {
            Ok(complement) => {
                self.n_ok += 1;
                Some(complement)
            }
            Err(RejectReason::BadSpread) => {
                self.n_rejected_bad_spread += 1;
                None
            }
            Err(RejectReason::Range) => {
                self.n_rejected_range += 1;
                None
            }
        }
    }
}

/// Reconstructs a YES top of book from an observed NO top of book.
///
/// The complement mirrors the executable side: `YES_bid = 1 - NO_ask` and
/// `YES_ask = 1 - NO_bid`. Sizes are carried in the same units. Only finite
/// prices in `[0, 1]` and non-negative finite sizes are accepted.
#[must_use]
pub fn complement_no_to_yes(input: &NoTopOfBook) -> Option<SyntheticComplement> {
    build_complement(input).ok()
}

/// Reconstructs a complement and updates [`ComplementStats`] in one call.
#[must_use]
pub fn complement_no_to_yes_with_stats(
    input: &NoTopOfBook,
    stats: &mut ComplementStats,
) -> Option<SyntheticComplement> {
    stats.observe(input)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RejectReason {
    BadSpread,
    Range,
}

fn build_complement(input: &NoTopOfBook) -> Result<SyntheticComplement, RejectReason> {
    if !valid_probability(input.no_bid)
        || !valid_probability(input.no_ask)
        || !valid_size(input.no_bid_size)
        || !valid_size(input.no_ask_size)
    {
        return Err(RejectReason::Range);
    }

    if input.no_bid > input.no_ask {
        return Err(RejectReason::BadSpread);
    }

    let no_mid = input.no_mid();
    let yes_bid = 1.0 - input.no_ask;
    let yes_ask = 1.0 - input.no_bid;
    let yes_mid = 1.0 - no_mid;

    Ok(SyntheticComplement {
        yes_bid,
        yes_ask,
        yes_bid_size: input.no_ask_size,
        yes_ask_size: input.no_bid_size,
        yes_mid,
        ts_ms: input.ts_ms,
        market_id: input.market_id.clone(),
        fidelity: Fidelity::SyntheticComplement,
        source: SYNTHETIC_COMPLEMENT_SOURCE.to_owned(),
    })
}

fn valid_probability(value: f64) -> bool {
    value.is_finite() && (0.0..=1.0).contains(&value)
}

fn valid_size(value: f64) -> bool {
    value.is_finite() && value >= 0.0
}
