//! Paper-only execution actor for strategy quote intents.
//!
//! The actor owns the paper book and applies the risk gate before a quote can
//! enter it. It has no venue, clock, or network responsibilities.

use crate::execution::{PaperBook, PaperFill, PaperOrder, TopOfBookUpdate};
use crate::strategy::quote::QuoteIntent;
use crate::strategy::risk::{RiskBlock, RiskGate, RiskLimits};

/// Paper execution owner for one market's outstanding maker quotes.
pub struct ExecutionActor {
    paper_book: PaperBook,
    risk_gate: RiskGate,
    next_order_id: u64,
}

impl ExecutionActor {
    /// Creates an execution actor with an empty paper book and supplied limits.
    #[must_use]
    pub fn new(risk_limits: RiskLimits) -> Self {
        Self {
            paper_book: PaperBook::with_default_fill_ratio(),
            risk_gate: RiskGate::new(risk_limits),
            next_order_id: 1,
        }
    }

    /// Applies the risk gate before placing a quote into the paper book.
    ///
    /// A successful quote is resting and therefore returns no fills. Fills are
    /// produced by later [`Self::on_book_update`] calls.
    pub fn on_quote(
        &mut self,
        intent: QuoteIntent,
        book_stale: bool,
        signal_latency_ms: u64,
    ) -> Result<Vec<PaperFill>, RiskBlock> {
        self.risk_gate.check(
            signal_latency_ms,
            self.paper_book.resting_count(),
            book_stale,
        )?;

        let order = PaperOrder::new(
            self.next_order_id,
            intent.side,
            intent.price,
            intent.size,
            0,
        )
        .map_err(|_| RiskBlock::InvalidOrder)?;
        self.paper_book
            .place(order)
            .map_err(|_| RiskBlock::InvalidOrder)?;
        self.next_order_id = self.next_order_id.saturating_add(1);

        Ok(Vec::new())
    }

    /// Forwards one synthetic market update to the owned paper book.
    pub fn on_book_update(&mut self, update: TopOfBookUpdate) -> Vec<PaperFill> {
        self.paper_book.apply_update(update)
    }

    /// Cancels one resting paper order.
    pub fn cancel(&mut self, order_id: u64) -> bool {
        self.paper_book.cancel(order_id)
    }

    /// Returns the number of currently resting paper quotes.
    #[must_use]
    pub fn outstanding(&self) -> usize {
        self.paper_book.resting_count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{PriceTicks, TradeSide};

    fn limits() -> RiskLimits {
        RiskLimits {
            max_outstanding_quotes: 2,
            max_latency_ms: 500,
            killed: false,
        }
    }

    fn actor() -> ExecutionActor {
        ExecutionActor::new(limits())
    }

    fn intent() -> QuoteIntent {
        QuoteIntent {
            side: TradeSide::Buy,
            price: PriceTicks::from_f64(0.40),
            size: 10,
        }
    }

    #[test]
    fn stale_quote_is_blocked_before_entering_the_paper_book() {
        let mut actor = actor();

        assert_eq!(
            actor.on_quote(intent(), true, 50),
            Err(RiskBlock::StaleBook)
        );
        assert_eq!(actor.outstanding(), 0);
    }

    #[test]
    fn killed_quote_is_blocked_before_entering_the_paper_book() {
        let mut actor = actor();
        actor.risk_gate.kill();

        assert_eq!(actor.on_quote(intent(), false, 50), Err(RiskBlock::Killed));
        assert_eq!(actor.outstanding(), 0);
    }

    #[test]
    fn zero_size_quote_is_rejected_without_panicking() {
        let mut actor = actor();
        let invalid = QuoteIntent {
            size: 0,
            ..intent()
        };

        assert_eq!(
            actor.on_quote(invalid, false, 50),
            Err(RiskBlock::InvalidOrder)
        );
        assert_eq!(actor.outstanding(), 0);
    }

    #[test]
    fn book_update_produces_a_fill_from_a_resting_quote() {
        let mut actor = actor();
        actor
            .on_quote(intent(), false, 50)
            .expect("quote should pass the risk gate");

        let fills = actor.on_book_update(TopOfBookUpdate::new(
            1_000,
            Some(PriceTicks::from_f64(0.40)),
            10,
            Some(PriceTicks::from_f64(0.39)),
            10,
            0,
        ));

        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].order_id, 1);
        assert_eq!(fills[0].price, PriceTicks::from_f64(0.40));
        assert_eq!(fills[0].size, 10);
        assert!(fills[0].maker);
        assert_eq!(actor.outstanding(), 0);
    }

    #[test]
    fn cancel_removes_a_resting_quote() {
        let mut actor = actor();
        actor
            .on_quote(intent(), false, 50)
            .expect("quote should pass the risk gate");

        assert!(actor.cancel(1));
        assert!(!actor.cancel(1));
        assert_eq!(actor.outstanding(), 0);
    }
}
