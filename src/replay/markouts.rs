//! Signed maker markouts over +1/+5/+10/+30/+60s horizons.

/// Horizons in milliseconds, matching `pipeline::MARKOUT_HORIZONS_MS`.
#[derive(Debug, Clone, Copy)]
pub struct MarkoutHorizons(pub [u64; 5]);

impl Default for MarkoutHorizons {
    fn default() -> Self {
        Self([1_000, 5_000, 10_000, 30_000, 60_000])
    }
}

/// Signed markouts in percentage points: `(mid - fill) * 100` for a BUY.
///
/// `mids` are the first mids at or after each horizon (backward-only
/// sampling by the caller). Missing horizons yield `None`.
#[must_use]
pub fn signed_markouts_pp(fill_price: f64, mids: [Option<f64>; 5]) -> [Option<f64>; 5] {
    mids.map(|m| m.map(|mid| (mid - fill_price) * 100.0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn markout_sign_favors_up_moves_for_buys() {
        let out = signed_markouts_pp(0.41, [Some(0.42), Some(0.40), None, None, None]);
        assert!(out[0].unwrap() > 0.0);
        assert!(out[1].unwrap() < 0.0);
        assert!(out[2].is_none());
    }

    #[test]
    fn horizons_match_pipeline_contract() {
        assert_eq!(
            MarkoutHorizons::default().0,
            [1_000, 5_000, 10_000, 30_000, 60_000]
        );
    }
}
