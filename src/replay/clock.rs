//! Virtual event-time clock. Wall-clock reads are forbidden in replay.

/// Monotonic virtual clock driven only by historical timestamps.
///
/// All `time_remaining`, rolling windows, staleness, markouts, order
/// lifetime, and cancel latency derive from [`ReplayClock::now_ms`]. No
/// wall-clock call exists in this module by construction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplayClock {
    now_ms: i64,
}

impl ReplayClock {
    /// Creates a clock pinned at the first historical timestamp.
    #[must_use]
    pub const fn new(start_ms: i64) -> Self {
        Self { now_ms: start_ms }
    }

    /// Current virtual event time in milliseconds.
    #[must_use]
    pub const fn now_ms(self) -> i64 {
        self.now_ms
    }

    /// Advances only forward to a new event timestamp.
    ///
    /// Returns an error when `ts_ms` moves backward, which would break
    /// no-look-ahead ordering.
    pub fn advance_to(&mut self, ts_ms: i64) -> Result<(), String> {
        if ts_ms < self.now_ms {
            return Err(format!(
                "replay clock cannot move backward: {} -> {}",
                self.now_ms, ts_ms
            ));
        }
        self.now_ms = ts_ms;
        Ok(())
    }

    /// Milliseconds remaining until `resolution_at_ms`, floored at zero.
    #[must_use]
    pub const fn time_remaining_ms(self, resolution_at_ms: i64) -> u64 {
        if resolution_at_ms <= self.now_ms {
            0
        } else {
            (resolution_at_ms - self.now_ms) as u64
        }
    }

    /// Seconds remaining until resolution, floored at zero.
    #[must_use]
    pub const fn time_remaining_secs(self, resolution_at_ms: i64) -> u64 {
        self.time_remaining_ms(resolution_at_ms) / 1000
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn advances_forward_only() {
        let mut clock = ReplayClock::new(1_000);
        assert_eq!(clock.now_ms(), 1_000);
        clock.advance_to(2_000).expect("forward advance");
        assert!(clock.advance_to(1_500).is_err());
    }

    #[test]
    fn time_remaining_floors_at_zero() {
        let clock = ReplayClock::new(5_000);
        assert_eq!(clock.time_remaining_ms(8_000), 3_000);
        assert_eq!(clock.time_remaining_secs(8_500), 3);
        assert_eq!(clock.time_remaining_ms(4_000), 0);
    }

    #[test]
    fn no_wall_clock_source_in_module() {
        // Static guard: built via concat so this test itself does not
        // contain the forbidden literals.
        let src = include_str!("clock.rs");
        let wall = ["chrono", "::", "Utc"].concat();
        let sys = ["SystemTime", "::", "now"].concat();
        assert!(!src.contains(&wall));
        assert!(!src.contains(&sys));
    }
}
