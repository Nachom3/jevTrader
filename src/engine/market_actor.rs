//! Authoritative in-memory market actor.
//!
//! [`MarketActor`] is deliberately the only owner of the local YES-side
//! [`OrderBook`]. Consumers receive a cloned book together with its stale flag;
//! they cannot accidentally treat a book that needs resynchronization as
//! trusted. A fresh REST snapshot is the caller's responsibility after seeing
//! `stale == true`.

use tokio::sync::{mpsc, watch};

use crate::domain::{ConditionId, MarketId, PriceTicks, TickSize, TokenId, TradeSide};
use crate::polymarket::{BookSide, Level, OrderBook};

/// A complete order-book snapshot received from the market stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BookSnapshot {
    /// Market condition represented by this snapshot.
    pub condition_id: ConditionId,
    /// Token whose levels are included.
    pub token_id: TokenId,
    /// Optional venue sequence. The SDK currently does not expose one.
    pub sequence: Option<u64>,
    /// Bid levels as `(price, base-unit quantity)` pairs.
    pub bids: Vec<Level>,
    /// Ask levels as `(price, base-unit quantity)` pairs.
    pub asks: Vec<Level>,
    /// Local hash expected after applying the snapshot.
    pub book_hash: Option<u64>,
    /// Opaque venue hash retained for observability.
    pub source_hash: Option<String>,
}

/// An absolute level update received from a market-stream price change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BookDelta {
    /// Market condition represented by this update.
    pub condition_id: ConditionId,
    /// Token whose level changed.
    pub token_id: TokenId,
    /// Optional venue sequence. The SDK currently does not expose one.
    pub sequence: Option<u64>,
    /// Changed side of the local book.
    pub side: BookSide,
    /// Changed price level.
    pub price: PriceTicks,
    /// New absolute quantity. `None` means the venue omitted the size.
    pub quantity: Option<u64>,
    /// Local hash expected after applying the delta, when available.
    pub book_hash: Option<u64>,
    /// Opaque venue hash retained for observability.
    pub source_hash: Option<String>,
}

/// A last-trade-price update that does not mutate the local order book.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LastTradePriceUpdate {
    pub condition_id: ConditionId,
    pub token_id: TokenId,
    pub price: PriceTicks,
    pub side: Option<TradeSide>,
    pub size: Option<u64>,
    pub timestamp_ms: i64,
}

/// A venue tick-size update that does not mutate the local order book.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TickSizeUpdate {
    pub condition_id: ConditionId,
    pub token_id: TokenId,
    pub old_tick_size: TickSize,
    pub new_tick_size: TickSize,
    pub timestamp_ms: i64,
}

/// A best-bid/ask update that does not replace the authoritative book.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BestBidAskUpdate {
    pub condition_id: ConditionId,
    pub token_id: TokenId,
    pub best_bid: PriceTicks,
    pub best_ask: PriceTicks,
    pub spread: PriceTicks,
    pub timestamp_ms: i64,
}

/// A new-market lifecycle event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewMarketUpdate {
    pub market_id: MarketId,
    pub condition_id: ConditionId,
    pub question: String,
    pub slug: String,
    pub description: String,
    pub token_ids: Vec<TokenId>,
    pub outcomes: Vec<String>,
    pub timestamp_ms: i64,
}

/// A market-resolution lifecycle event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketResolvedUpdate {
    pub market_id: MarketId,
    pub condition_id: ConditionId,
    pub token_ids: Vec<TokenId>,
    pub winning_token_id: TokenId,
    pub winning_outcome: String,
    pub timestamp_ms: i64,
}

/// Provider-free messages accepted by [`MarketActor`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MarketMessage {
    BookSnapshot(BookSnapshot),
    BookDelta(BookDelta),
    LastTradePrice(LastTradePriceUpdate),
    TickSizeChange(TickSizeUpdate),
    BestBidAsk(BestBidAskUpdate),
    NewMarket(NewMarketUpdate),
    MarketResolved(MarketResolvedUpdate),
}

/// A consumer-visible copy of the authoritative book and its trust state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketSnapshot {
    pub book: OrderBook,
    /// `true` means a fresh REST snapshot is required before trusting `book`.
    pub stale: bool,
}

/// The single owner of one market's authoritative YES-side order book.
pub struct MarketActor {
    yes_token_id: TokenId,
    receiver: mpsc::Receiver<MarketMessage>,
    book: OrderBook,
    next_sequence: Option<u64>,
    snapshot_sender: Option<watch::Sender<MarketSnapshot>>,
}

impl MarketActor {
    /// Creates an actor from a bounded venue-message receiver.
    ///
    /// The actor starts stale because an authoritative book cannot be inferred
    /// from deltas alone. Send a [`MarketMessage::BookSnapshot`] before using
    /// the first snapshot as trusted state.
    #[must_use]
    pub fn new(yes_token_id: TokenId, receiver: mpsc::Receiver<MarketMessage>) -> Self {
        let mut book = OrderBook::default();
        book.mark_stale();
        Self {
            yes_token_id,
            receiver,
            book,
            next_sequence: None,
            snapshot_sender: None,
        }
    }

    /// Creates an actor, its bounded input channel, and a read-only snapshot observer.
    ///
    /// The observer receives an updated snapshot after every applied message,
    /// allowing a caller to poll the book while [`Self::run`] owns the actor.
    #[must_use]
    pub fn channel_with_snapshot(
        yes_token_id: TokenId,
        capacity: usize,
    ) -> (
        Self,
        mpsc::Sender<MarketMessage>,
        watch::Receiver<MarketSnapshot>,
    ) {
        let (mut actor, sender) = Self::channel(yes_token_id, capacity);
        let (snapshot_sender, snapshot_receiver) = watch::channel(actor.latest_snapshot());
        actor.snapshot_sender = Some(snapshot_sender);
        (actor, sender, snapshot_receiver)
    }

    /// Creates an actor and its bounded input channel.
    ///
    /// A minimum capacity of one is used because Tokio bounded channels do not
    /// accept zero capacity.
    #[must_use]
    pub fn channel(yes_token_id: TokenId, capacity: usize) -> (Self, mpsc::Sender<MarketMessage>) {
        let (sender, receiver) = mpsc::channel(capacity.max(1));
        (Self::new(yes_token_id, receiver), sender)
    }

    /// Applies one provider-free message synchronously.
    pub fn apply_message(&mut self, message: MarketMessage) {
        match message {
            MarketMessage::BookSnapshot(snapshot) => self.apply_snapshot(snapshot),
            MarketMessage::BookDelta(delta) => self.apply_delta(delta),
            MarketMessage::LastTradePrice(_)
            | MarketMessage::TickSizeChange(_)
            | MarketMessage::BestBidAsk(_)
            | MarketMessage::NewMarket(_)
            | MarketMessage::MarketResolved(_) => {}
        }
        if let Some(sender) = &self.snapshot_sender {
            sender.send_replace(self.latest_snapshot());
        }
    }

    /// Drains the bounded input channel until its sender is dropped.
    ///
    /// The returned actor still owns the sole authoritative book, so callers
    /// can inspect it with [`Self::latest_snapshot`] after the stream ends.
    pub async fn run(mut self) -> Self {
        while let Some(message) = self.receiver.recv().await {
            self.apply_message(message);
        }
        self
    }

    /// Returns a cloned book and the stale flag as one inseparable value.
    ///
    /// Consumers must trigger a fresh REST snapshot themselves after observing
    /// `stale == true`; the actor intentionally does not perform network I/O or
    /// clear the flag in response to deltas.
    #[must_use]
    pub fn latest_snapshot(&self) -> MarketSnapshot {
        MarketSnapshot {
            book: self.book.clone(),
            stale: self.book.is_stale(),
        }
    }

    fn apply_snapshot(&mut self, snapshot: BookSnapshot) {
        if snapshot.token_id != self.yes_token_id {
            return;
        }

        self.book.apply_snapshot(snapshot.bids, snapshot.asks);
        self.next_sequence = snapshot.sequence.map(|sequence| sequence.saturating_add(1));

        if let Some(expected_hash) = snapshot.book_hash
            && self.book.book_hash() != expected_hash
        {
            self.book.mark_stale();
        }
    }

    fn apply_delta(&mut self, delta: BookDelta) {
        if delta.token_id != self.yes_token_id {
            return;
        }

        match (self.next_sequence, delta.sequence) {
            (Some(expected), Some(actual)) if expected != actual => self.book.mark_stale(),
            (Some(_), None) => self.book.mark_stale(),
            _ => {}
        }

        if let Some(quantity) = delta.quantity {
            self.book.apply_delta(delta.side, delta.price, quantity);
        }

        if let Some(expected_hash) = delta.book_hash
            && self.book.book_hash() != expected_hash
        {
            self.book.mark_stale();
        }

        if let Some(sequence) = delta.sequence {
            self.next_sequence = Some(sequence.saturating_add(1));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn yes_token() -> TokenId {
        TokenId("yes-token".to_owned())
    }

    fn condition() -> ConditionId {
        ConditionId("condition".to_owned())
    }

    fn price(value: f64) -> PriceTicks {
        PriceTicks::from_f64(value)
    }

    fn snapshot(sequence: u64, bids: Vec<Level>, asks: Vec<Level>) -> BookSnapshot {
        let mut expected = OrderBook::default();
        expected.apply_snapshot(bids.clone(), asks.clone());
        BookSnapshot {
            condition_id: condition(),
            token_id: yes_token(),
            sequence: Some(sequence),
            bids,
            asks,
            book_hash: Some(expected.book_hash()),
            source_hash: None,
        }
    }

    fn delta(
        sequence: u64,
        side: BookSide,
        price: PriceTicks,
        quantity: u64,
        before: &OrderBook,
    ) -> BookDelta {
        let mut expected = before.clone();
        expected.apply_delta(side, price, quantity);
        BookDelta {
            condition_id: condition(),
            token_id: yes_token(),
            sequence: Some(sequence),
            side,
            price,
            quantity: Some(quantity),
            book_hash: Some(expected.book_hash()),
            source_hash: None,
        }
    }

    fn actor() -> MarketActor {
        let (_sender, receiver) = mpsc::channel(8);
        MarketActor::new(yes_token(), receiver)
    }

    #[test]
    fn snapshot_channel_observer_tracks_actor_updates() {
        let (mut actor, _sender, mut observer) = MarketActor::channel_with_snapshot(yes_token(), 8);
        assert!(observer.borrow().stale);

        actor.apply_message(MarketMessage::BookSnapshot(snapshot(
            10,
            vec![(price(0.40), 10)],
            vec![(price(0.60), 20)],
        )));

        let latest = observer.borrow_and_update().clone();
        assert!(!latest.stale);
        assert_eq!(latest.book.bids(), &[(price(0.40), 10)]);
    }

    #[test]
    fn snapshot_then_deltas_converge_to_expected_book() {
        let mut actor = actor();
        let initial_bids = vec![(price(0.40), 10)];
        let initial_asks = vec![(price(0.60), 20)];
        actor.apply_message(MarketMessage::BookSnapshot(snapshot(
            10,
            initial_bids,
            initial_asks,
        )));

        let after_snapshot = actor.latest_snapshot().book;
        actor.apply_message(MarketMessage::BookDelta(delta(
            11,
            BookSide::Bid,
            price(0.41),
            15,
            &after_snapshot,
        )));

        let after_first_delta = actor.latest_snapshot().book;
        actor.apply_message(MarketMessage::BookDelta(delta(
            12,
            BookSide::Ask,
            price(0.60),
            0,
            &after_first_delta,
        )));

        let latest = actor.latest_snapshot();
        assert!(!latest.stale);
        assert_eq!(latest.book.bids(), &[(price(0.41), 15), (price(0.40), 10)]);
        assert_eq!(latest.book.asks(), &[]);
    }

    #[test]
    fn sequence_gap_marks_stale_and_deltas_keep_it_stale() {
        let mut actor = actor();
        actor.apply_message(MarketMessage::BookSnapshot(snapshot(
            10,
            vec![(price(0.40), 10)],
            vec![(price(0.60), 20)],
        )));

        actor.apply_message(MarketMessage::BookDelta(BookDelta {
            condition_id: condition(),
            token_id: yes_token(),
            sequence: Some(12),
            side: BookSide::Bid,
            price: price(0.41),
            quantity: Some(15),
            book_hash: None,
            source_hash: None,
        }));
        assert!(actor.latest_snapshot().stale);

        actor.apply_message(MarketMessage::BookDelta(BookDelta {
            condition_id: condition(),
            token_id: yes_token(),
            sequence: Some(13),
            side: BookSide::Ask,
            price: price(0.59),
            quantity: Some(5),
            book_hash: None,
            source_hash: None,
        }));
        assert!(actor.latest_snapshot().stale);
    }

    #[test]
    fn hash_mismatch_marks_stale() {
        let mut actor = actor();
        let mut mismatched = snapshot(10, vec![(price(0.40), 10)], vec![(price(0.60), 20)]);
        mismatched.book_hash = Some(0);

        actor.apply_message(MarketMessage::BookSnapshot(mismatched));

        assert!(actor.latest_snapshot().stale);
    }

    #[test]
    fn fresh_snapshot_clears_stale() {
        let mut actor = actor();
        actor.apply_message(MarketMessage::BookSnapshot(snapshot(
            10,
            vec![(price(0.40), 10)],
            vec![(price(0.60), 20)],
        )));
        actor.apply_message(MarketMessage::BookDelta(BookDelta {
            condition_id: condition(),
            token_id: yes_token(),
            sequence: Some(12),
            side: BookSide::Bid,
            price: price(0.41),
            quantity: Some(15),
            book_hash: None,
            source_hash: None,
        }));
        assert!(actor.latest_snapshot().stale);

        actor.apply_message(MarketMessage::BookSnapshot(snapshot(
            20,
            vec![(price(0.42), 12)],
            vec![(price(0.58), 18)],
        )));
        assert!(!actor.latest_snapshot().stale);
    }

    #[test]
    fn latest_snapshot_reflects_last_applied_state() {
        let mut actor = actor();
        actor.apply_message(MarketMessage::BookSnapshot(snapshot(
            1,
            vec![(price(0.40), 10)],
            vec![(price(0.60), 20)],
        )));
        let before = actor.latest_snapshot();

        actor.apply_message(MarketMessage::BookDelta(BookDelta {
            condition_id: condition(),
            token_id: yes_token(),
            sequence: Some(2),
            side: BookSide::Bid,
            price: price(0.41),
            quantity: Some(15),
            book_hash: None,
            source_hash: None,
        }));

        assert_eq!(before.book.bids(), &[(price(0.40), 10)]);
        assert_eq!(
            actor.latest_snapshot().book.bids(),
            &[(price(0.41), 15), (price(0.40), 10),]
        );
    }
}
