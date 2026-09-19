//! Deterministic post-only quote construction for the V1 maker strategy.

use jevtrader::config::QuoteThresholds;
use jevtrader::domain::{PriceTicks, TickSize, TradeSide};
use jevtrader::polymarket::OrderBook;

use super::lead_lag::{V1Signal, should_quote};

/// A quote request emitted by the strategy layer.
///
/// V1 only emits BUY intents. The caller owns sizing; this type deliberately
/// carries the supplied size without applying a sizing model.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuoteIntent {
    pub side: TradeSide,
    pub price: PriceTicks,
    pub size: u64,
}

/// Decide whether to place a fresh post-only BUY quote.
///
/// The caller supplies the book freshness observation, venue tick size, and
/// order size. A quote is returned only when the shared V1 signal rule passes,
/// the book is trusted, and the rounded candidate remains strictly below the
/// best ask.
#[must_use]
pub fn decide_quote(
    signal: &V1Signal,
    book: &OrderBook,
    thresholds: &QuoteThresholds,
    stale: bool,
    tick_size: TickSize,
    size: u64,
) -> Option<QuoteIntent> {
    if size == 0 || stale || book.is_stale() {
        return None;
    }
    should_quote(signal, thresholds).then_some(())?;

    let best_bid = book.best_bid()?;
    let best_ask = book.best_ask()?;
    let price = round_up_to_tick(best_bid, tick_size)?;

    // This is the maker-only invariant: never cross or join an already crossed
    // ask. Zero-size intents were rejected before any quote construction.
    (price < best_ask).then_some(QuoteIntent {
        side: TradeSide::Buy,
        price,
        size,
    })
}

fn round_up_to_tick(best_bid: PriceTicks, tick_size: TickSize) -> Option<PriceTicks> {
    let tick = tick_size.to_f64();
    let candidate = ((best_bid.to_f64() + tick) / tick).ceil() * tick;
    if !candidate.is_finite() || !(0.0..=1.0).contains(&candidate) {
        return None;
    }

    let price = PriceTicks::from_f64(candidate);
    price.is_multiple_of(tick_size).then_some(price)
}

#[cfg(test)]
mod tests {
    use super::*;
    use jevtrader::jev::response::TickDistribution;

    fn signal() -> V1Signal {
        V1Signal {
            yes_pressure_5s: 0.8,
            no_pressure_5s: 0.1,
            move_persists: 0.7,
            underreact_up: 0.9,
            underreact_down: 0.1,
            repricing: TickDistribution {
                up_3_plus: 0.1,
                up_2: 0.2,
                up_1: 0.4,
                flat: 0.1,
                down_1: 0.1,
                down_2: 0.05,
                down_3_plus: 0.05,
            },
            repricing_confidence: 0.8,
            fill_before_decay: 0.7,
            fill_toxic: 0.2,
        }
    }

    fn book(bid: f64, ask: f64) -> OrderBook {
        let mut book = OrderBook::default();
        book.apply_snapshot(
            [(PriceTicks::from_f64(bid), 100)],
            [(PriceTicks::from_f64(ask), 100)],
        );
        book
    }

    fn decide(book: &OrderBook, stale: bool) -> Option<QuoteIntent> {
        decide_quote(
            &signal(),
            book,
            &QuoteThresholds::default(),
            stale,
            TickSize::from_f64(0.01),
            25,
        )
    }

    #[test]
    fn stale_book_returns_no_quote() {
        assert_eq!(decide(&book(0.40, 0.45), true), None);
    }

    #[test]
    fn zero_size_returns_no_quote() {
        assert_eq!(
            decide_quote(
                &signal(),
                &book(0.40, 0.45),
                &QuoteThresholds::default(),
                false,
                TickSize::from_f64(0.01),
                0,
            ),
            None
        );
    }

    #[test]
    fn crossed_book_returns_no_quote() {
        assert_eq!(decide(&book(0.41, 0.41), false), None);
    }

    #[test]
    fn happy_path_quotes_one_tick_over_bid_below_ask() {
        let intent = decide(&book(0.40, 0.42), false).expect("quote should pass");

        assert_eq!(intent.side, TradeSide::Buy);
        assert_eq!(intent.price, PriceTicks::from_f64(0.41));
        assert_eq!(intent.size, 25);
        assert!(intent.price < PriceTicks::from_f64(0.42));
    }

    #[test]
    fn candidate_rounds_up_to_a_valid_tick_multiple() {
        let intent = decide_quote(
            &signal(),
            &book(0.403, 0.43),
            &QuoteThresholds::default(),
            false,
            TickSize::from_f64(0.01),
            25,
        )
        .expect("rounded quote should pass");

        assert_eq!(intent.price, PriceTicks::from_f64(0.42));
        assert!(intent.price.is_multiple_of(TickSize::from_f64(0.01)));
    }

    #[test]
    fn under_threshold_signal_returns_no_quote() {
        let mut under_threshold = signal();
        under_threshold.underreact_up = QuoteThresholds::default().under_min;

        assert_eq!(
            decide_quote(
                &under_threshold,
                &book(0.40, 0.45),
                &QuoteThresholds::default(),
                false,
                TickSize::from_f64(0.01),
                25,
            ),
            None
        );
    }
}
