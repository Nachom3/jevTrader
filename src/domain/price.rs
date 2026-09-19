const MICRO_UNITS_PER_PRICE: u64 = 1_000_000;

/// An executable outcome price quantized to one-millionth of the 0-1 range.
///
/// `f64` remains suitable for statistics and analysis, but executable prices
/// must use `PriceTicks` so rounding and tick comparisons are deterministic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct PriceTicks(u64);

impl PriceTicks {
    /// Converts a valid outcome price to the nearest micro-unit.
    ///
    /// # Panics
    ///
    /// Panics when `price` is not finite or is outside the inclusive 0-1
    /// outcome-price range.
    pub fn from_f64(price: f64) -> Self {
        assert!(
            price.is_finite() && (0.0..=1.0).contains(&price),
            "outcome price must be finite and within 0..=1"
        );
        Self((price * MICRO_UNITS_PER_PRICE as f64).round() as u64)
    }

    /// Raw micro-units (1e-6 of the 0-1 range).
    pub fn as_micros(self) -> u64 {
        self.0
    }

    /// Converts the quantized price back to the 0-1 range.
    pub fn to_f64(self) -> f64 {
        self.0 as f64 / MICRO_UNITS_PER_PRICE as f64
    }

    /// Returns whether this price is exactly divisible by `tick` in micro-units.
    pub fn is_multiple_of(self, tick: TickSize) -> bool {
        tick.0 != 0 && self.0.is_multiple_of(tick.0)
    }
}

/// A market tick size represented in the same micro-units as [`PriceTicks`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct TickSize(u64);

impl TickSize {
    /// Converts a positive, valid outcome-price increment to micro-units.
    ///
    /// # Panics
    ///
    /// Panics when `tick` is not finite, is not positive, or exceeds one.
    pub fn from_f64(tick: f64) -> Self {
        assert!(
            tick.is_finite() && (0.0..=1.0).contains(&tick) && tick > 0.0,
            "tick size must be finite and within (0, 1]"
        );
        let micros = (tick * MICRO_UNITS_PER_PRICE as f64).round() as u64;
        assert!(micros > 0, "tick size below one micro-unit resolution");
        Self(micros)
    }

    /// Raw micro-units.
    /// Converts the quantized tick size back to a floating-point increment.
    pub fn to_f64(self) -> f64 {
        self.0 as f64 / MICRO_UNITS_PER_PRICE as f64
    }
}
