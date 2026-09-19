//! Paper-only execution actor for strategy quote intents.
//!
//! The actor owns one paper book per A/B variant (CONTROL and QUANT_V1) and
//! applies the risk gate before a quote can enter either. Variant books never
//! share inventory, queue, fills, or exposure: a CONTROL fill can never move
//! the QUANT_V1 position and vice versa. The actor has no venue, clock, or
//! network responsibilities.

use crate::execution::{PaperBook, PaperFill, PaperOrder, TopOfBookUpdate};
use crate::storage::Variant;
use crate::strategy::quote::QuoteIntent;
use crate::strategy::risk::{RiskBlock, RiskGate, RiskLimits};

/// Fills produced by one market update, separated by owning variant.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DualFills {
    pub control: Vec<PaperFill>,
    pub quant: Vec<PaperFill>,
}

impl DualFills {
    /// Fills owned by one variant.
    #[must_use]
    pub fn of(&self, variant: Variant) -> &[PaperFill] {
        match variant {
            Variant::Control => &self.control,
            Variant::QuantV1 => &self.quant,
        }
    }

    /// Total fills across both variants.
    #[must_use]
    pub fn total(&self) -> usize {
        self.control.len() + self.quant.len()
    }
}

/// Paper execution owner for one market's outstanding maker quotes.
///
/// One market, two independent bookkeeping lanes: CONTROL quotes rest and
/// fill in the CONTROL book, QUANT_V1 quotes in the QUANT_V1 book. Order ids
/// are drawn from one sequence so ids stay unique across lanes for audit.
pub struct ExecutionActor {
    control_book: PaperBook,
    quant_book: PaperBook,
    risk_gate: RiskGate,
    next_order_id: u64,
}

impl ExecutionActor {
    /// Creates an execution actor with two empty paper books and supplied limits.
    #[must_use]
    pub fn new(risk_limits: RiskLimits) -> Self {
        Self {
            control_book: PaperBook::with_default_fill_ratio(),
            quant_book: PaperBook::with_default_fill_ratio(),
            risk_gate: RiskGate::new(risk_limits),
            next_order_id: 1,
        }
    }

    fn book(&self, variant: Variant) -> &PaperBook {
        match variant {
            Variant::Control => &self.control_book,
            Variant::QuantV1 => &self.quant_book,
        }
    }

    fn book_mut(&mut self, variant: Variant) -> &mut PaperBook {
        match variant {
            Variant::Control => &mut self.control_book,
            Variant::QuantV1 => &mut self.quant_book,
        }
    }

    /// Applies the risk gate before placing a quote into its variant's book.
    ///
    /// A successful quote is resting and therefore returns no fills. Fills are
    /// produced by later [`Self::on_book_update`] calls, routed per variant.
    pub fn on_quote(
        &mut self,
        variant: Variant,
        intent: QuoteIntent,
        book_stale: bool,
        signal_latency_ms: u64,
    ) -> Result<Vec<PaperFill>, RiskBlock> {
        self.risk_gate.check(
            signal_latency_ms,
            self.book(variant).resting_count(),
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
        self.book_mut(variant)
            .place(order)
            .map_err(|_| RiskBlock::InvalidOrder)?;
        self.next_order_id = self.next_order_id.saturating_add(1);

        Ok(Vec::new())
    }

    /// Forwards one market update to both variant books independently.
    ///
    /// Each book matches only its own resting orders with the same explicit
    /// queue model, so a touch never double-fills across variants.
    pub fn on_book_update(&mut self, update: TopOfBookUpdate) -> DualFills {
        DualFills {
            control: self.control_book.apply_update(update),
            quant: self.quant_book.apply_update(update),
        }
    }

    /// Cancels one resting paper order in its variant's book.
    pub fn cancel(&mut self, variant: Variant, order_id: u64) -> bool {
        self.book_mut(variant).cancel(order_id)
    }

    /// Returns the number of currently resting paper quotes for a variant.
    #[must_use]
    pub fn outstanding(&self, variant: Variant) -> usize {
        self.book(variant).resting_count()
    }

    /// Returns the resting count across both variants.
    #[must_use]
    pub fn outstanding_total(&self) -> usize {
        self.control_book.resting_count() + self.quant_book.resting_count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{PriceTicks, TradeSide};
    use crate::storage::Variant;

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
            actor.on_quote(Variant::Control, intent(), true, 50),
            Err(RiskBlock::StaleBook)
        );
        assert_eq!(actor.outstanding(Variant::Control), 0);
        assert_eq!(actor.outstanding(Variant::QuantV1), 0);
    }

    #[test]
    fn killed_quote_is_blocked_before_entering_the_paper_book() {
        let mut actor = actor();
        actor.risk_gate.kill();

        assert_eq!(
            actor.on_quote(Variant::QuantV1, intent(), false, 50),
            Err(RiskBlock::Killed)
        );
        assert_eq!(actor.outstanding_total(), 0);
    }

    #[test]
    fn zero_size_quote_is_rejected_without_panicking() {
        let mut actor = actor();
        let invalid = QuoteIntent {
            size: 0,
            ..intent()
        };

        assert_eq!(
            actor.on_quote(Variant::Control, invalid, false, 50),
            Err(RiskBlock::InvalidOrder)
        );
        assert_eq!(actor.outstanding_total(), 0);
    }

    #[test]
    fn book_update_produces_a_fill_from_a_resting_quote() {
        let mut actor = actor();
        actor
            .on_quote(Variant::Control, intent(), false, 50)
            .expect("quote should pass the risk gate");

        let fills = actor.on_book_update(TopOfBookUpdate::new(
            1_000,
            Some(PriceTicks::from_f64(0.40)),
            10,
            Some(PriceTicks::from_f64(0.39)),
            10,
            0,
        ));

        assert_eq!(fills.total(), 1);
        assert!(fills.quant.is_empty());
        assert_eq!(fills.control[0].order_id, 1);
        assert_eq!(fills.control[0].price, PriceTicks::from_f64(0.40));
        assert_eq!(fills.control[0].size, 10);
        assert!(fills.control[0].maker);
        assert_eq!(actor.outstanding(Variant::Control), 0);
    }

    #[test]
    fn variants_rest_and_fill_independently() {
        let mut actor = actor();
        actor
            .on_quote(Variant::Control, intent(), false, 50)
            .expect("control quote should rest");
        actor
            .on_quote(Variant::QuantV1, intent(), false, 50)
            .expect("quant quote should rest");
        assert_eq!(actor.outstanding(Variant::Control), 1);
        assert_eq!(actor.outstanding(Variant::QuantV1), 1);

        // A touch sized for one allocation fills partially in EACH book:
        // shared market data, independent queues. The first-touch cap leaves
        // one unit resting per lane (size 10, allocation 10 -> fill 9).
        let fills = actor.on_book_update(TopOfBookUpdate::new(
            2_000,
            Some(PriceTicks::from_f64(0.40)),
            100,
            Some(PriceTicks::from_f64(0.60)),
            100,
            50,
        ));
        assert_eq!(fills.control.len(), fills.quant.len());
        assert_eq!(fills.control.iter().map(|fill| fill.size).sum::<u64>(), 9);
        assert_eq!(fills.control[0].order_id, 1);
        assert_eq!(fills.quant[0].order_id, 2);

        // Canceling the remainder in one variant never touches the other lane.
        assert!(actor.cancel(Variant::Control, 1));
        assert!(!actor.cancel(Variant::Control, 1));
        assert_eq!(actor.outstanding(Variant::Control), 0);
        assert_eq!(actor.outstanding(Variant::QuantV1), 1);
        assert_eq!(actor.outstanding_total(), 1);
    }

    #[test]
    fn cancel_removes_a_resting_quote() {
        let mut actor = actor();
        actor
            .on_quote(Variant::QuantV1, intent(), false, 50)
            .expect("quote should pass the risk gate");

        assert!(actor.cancel(Variant::QuantV1, 1));
        assert!(!actor.cancel(Variant::QuantV1, 1));
        // The other lane is unaffected.
        assert_eq!(actor.outstanding(Variant::Control), 0);
        assert_eq!(actor.outstanding_total(), 0);
    }
}
