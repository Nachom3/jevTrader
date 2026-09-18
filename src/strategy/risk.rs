//! Runtime risk gates shared by quote decisions and future execution paths.

/// Hard limits checked before a strategy intent can leave the hot path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RiskLimits {
    pub max_outstanding_quotes: usize,
    pub max_latency_ms: u64,
    pub killed: bool,
}

/// The typed reason a quote was blocked by the risk gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RiskBlock {
    Killed,
    StaleBook,
    TooManyOutstandingQuotes,
    LatencyExceeded,
}

/// Mutable runtime gate for the V1 kill switch and bounded quote risk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RiskGate {
    limits: RiskLimits,
}

impl RiskGate {
    /// Creates a gate with the supplied limits and initial kill-switch state.
    #[must_use]
    pub const fn new(limits: RiskLimits) -> Self {
        Self { limits }
    }

    /// Returns the limits currently enforced by this gate.
    #[must_use]
    pub const fn limits(&self) -> RiskLimits {
        self.limits
    }

    /// Permanently blocks this gate until its owner replaces or resets it.
    ///
    /// Calling `kill` repeatedly is intentionally idempotent.
    pub fn kill(&mut self) {
        self.limits.killed = true;
    }

    /// Returns whether the kill switch is active.
    #[must_use]
    pub const fn is_killed(&self) -> bool {
        self.limits.killed
    }

    /// Check signal latency, quote count, and book freshness before quoting.
    ///
    /// The kill switch has precedence over all other reasons. Outstanding
    /// quotes are bounded with `>=`: reaching the configured maximum leaves no
    /// capacity for another quote. Latency blocks only when it is over the
    /// configured maximum.
    pub fn check(
        &self,
        signal_latency_ms: u64,
        outstanding: usize,
        book_stale: bool,
    ) -> Result<(), RiskBlock> {
        if self.limits.killed {
            return Err(RiskBlock::Killed);
        }
        if book_stale {
            return Err(RiskBlock::StaleBook);
        }
        if outstanding >= self.limits.max_outstanding_quotes {
            return Err(RiskBlock::TooManyOutstandingQuotes);
        }
        if signal_latency_ms > self.limits.max_latency_ms {
            return Err(RiskBlock::LatencyExceeded);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gate() -> RiskGate {
        RiskGate::new(RiskLimits {
            max_outstanding_quotes: 2,
            max_latency_ms: 100,
            killed: false,
        })
    }

    #[test]
    fn stale_book_blocks() {
        assert_eq!(gate().check(50, 0, true), Err(RiskBlock::StaleBook));
    }

    #[test]
    fn killed_gate_blocks() {
        let mut gate = gate();
        gate.kill();

        assert_eq!(gate.check(0, 0, false), Err(RiskBlock::Killed));
    }

    #[test]
    fn too_many_outstanding_quotes_block() {
        assert_eq!(
            gate().check(50, 2, false),
            Err(RiskBlock::TooManyOutstandingQuotes)
        );
    }

    #[test]
    fn excessive_latency_blocks() {
        assert_eq!(gate().check(101, 0, false), Err(RiskBlock::LatencyExceeded));
    }

    #[test]
    fn kill_is_idempotent() {
        let mut gate = gate();
        gate.kill();
        let killed_limits = gate.limits();
        gate.kill();

        assert!(gate.is_killed());
        assert_eq!(gate.limits(), killed_limits);
        assert_eq!(gate.check(0, 0, false), Err(RiskBlock::Killed));
    }
}
