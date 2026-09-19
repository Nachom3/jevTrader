use std::borrow::Borrow;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;

use smallvec::SmallVec;

use crate::domain::PriceTicks;

/// Number of price/quantity levels kept inline before `SmallVec` allocates.
///
/// Most top-of-book updates contain substantially fewer than 32 levels. Keeping
/// that common depth inline avoids a heap allocation while retaining unbounded
/// depth when a caller supplies a deeper snapshot.
pub const INLINE_LEVEL_CAPACITY: usize = 32;

/// Token quantity in six-decimal base units.
///
/// REST quantities are converted to this integer representation before entering
/// the local book. Book operations themselves only manipulate the integer and do
/// not perform floating-point arithmetic.
pub const BASE_UNITS_PER_TOKEN: u64 = 1_000_000;

/// One local YES-side book level: executable price followed by base-unit quantity.
pub type Level = (PriceTicks, u64);

/// Side of the local YES-side order book.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BookSide {
    Bid,
    Ask,
}

/// Authoritative local YES-side order book.
///
/// Invariants:
/// - each price occurs at most once on a side;
/// - zero-quantity levels are absent;
/// - bids are sorted descending by price;
/// - asks are sorted ascending by price;
/// - a delta does not clear [`Self::is_stale`]; only a snapshot does.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OrderBook {
    bids: SmallVec<[Level; INLINE_LEVEL_CAPACITY]>,
    asks: SmallVec<[Level; INLINE_LEVEL_CAPACITY]>,
    stale: bool,
}

impl OrderBook {
    /// Replaces both sides with a normalized snapshot and marks the book fresh.
    ///
    /// Duplicate prices use the last quantity supplied. A zero quantity removes
    /// the level, matching the delta semantics. Both owned levels and borrowed
    /// level slices are accepted.
    pub fn apply_snapshot<B, A>(&mut self, bids: B, asks: A)
    where
        B: IntoIterator,
        B::Item: Borrow<Level>,
        A: IntoIterator,
        A::Item: Borrow<Level>,
    {
        self.bids = normalize_levels(bids, true);
        self.asks = normalize_levels(asks, false);
        self.stale = false;
    }

    /// Applies one absolute level update without changing the stale flag.
    ///
    /// `quantity == 0` removes the level. A caller must first apply a fresh
    /// snapshot after a detected sequence gap; deltas received while stale are
    /// retained but cannot make the book authoritative again.
    pub fn apply_delta(&mut self, side: BookSide, price: PriceTicks, quantity: u64) {
        let levels = match side {
            BookSide::Bid => &mut self.bids,
            BookSide::Ask => &mut self.asks,
        };
        set_level(levels, price, quantity, matches!(side, BookSide::Bid));
    }

    /// Marks the local book as unusable until the next snapshot.
    pub fn mark_stale(&mut self) {
        self.stale = true;
    }

    /// Returns whether a full snapshot is required before trusting this book.
    #[must_use]
    pub fn is_stale(&self) -> bool {
        self.stale
    }

    /// Returns bid levels in descending price order.
    #[must_use]
    pub fn bids(&self) -> &[Level] {
        &self.bids
    }

    /// Returns ask levels in ascending price order.
    #[must_use]
    pub fn asks(&self) -> &[Level] {
        &self.asks
    }

    /// Returns the highest executable bid, if present.
    #[must_use]
    pub fn best_bid(&self) -> Option<PriceTicks> {
        self.bids.first().map(|(price, _)| *price)
    }

    /// Returns the lowest executable ask, if present.
    #[must_use]
    pub fn best_ask(&self) -> Option<PriceTicks> {
        self.asks.first().map(|(price, _)| *price)
    }

    /// Returns the bid/ask difference in price micro-units.
    ///
    /// A crossed book returns `None` rather than reporting a misleading unsigned
    /// spread. The local book does not know the venue tick size, so this is a
    /// difference in price micro-units, not a count of venue tick-size steps.
    #[must_use]
    pub fn spread_micros(&self) -> Option<u64> {
        self.best_ask()?
            .as_micros()
            .checked_sub(self.best_bid()?.as_micros())
    }

    /// Returns the midpoint in price micro-units, rounded down on a half-unit.
    #[must_use]
    pub fn mid_micros(&self) -> Option<u64> {
        let bid = self.best_bid()?.as_micros();
        let ask = self.best_ask()?.as_micros();
        Some((bid + ask) / 2)
    }

    /// Returns a deterministic change-detection hash for the top five levels.
    ///
    /// This uses [`DefaultHasher`] and is intentionally not cryptographic. It is
    /// suitable for deciding whether a persisted top-of-book snapshot changed,
    /// not for authentication, identity, or integrity protection. Levels deeper
    /// than the top five are intentionally excluded.
    #[must_use]
    pub fn book_hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        hash_top_levels(&mut hasher, &self.bids);
        hash_top_levels(&mut hasher, &self.asks);
        hasher.finish()
    }
}

fn normalize_levels<I>(levels: I, descending: bool) -> SmallVec<[Level; INLINE_LEVEL_CAPACITY]>
where
    I: IntoIterator,
    I::Item: Borrow<Level>,
{
    let mut normalized = SmallVec::new();
    for level in levels {
        let (price, quantity) = *level.borrow();
        set_level(&mut normalized, price, quantity, descending);
    }
    normalized
}

fn set_level(
    levels: &mut SmallVec<[Level; INLINE_LEVEL_CAPACITY]>,
    price: PriceTicks,
    quantity: u64,
    descending: bool,
) {
    if let Some(index) = levels
        .iter()
        .position(|(level_price, _)| *level_price == price)
    {
        if quantity == 0 {
            levels.remove(index);
        } else {
            levels[index].1 = quantity;
        }
    } else if quantity != 0 {
        levels.push((price, quantity));
    }

    if descending {
        levels.sort_unstable_by_key(|level| std::cmp::Reverse(level.0));
    } else {
        levels.sort_unstable_by_key(|left| left.0);
    }
}

fn hash_top_levels(hasher: &mut DefaultHasher, levels: &[Level]) {
    hasher.write_usize(levels.len().min(5));
    for (price, quantity) in levels.iter().take(5) {
        hasher.write_u64(price.as_micros());
        hasher.write_u64(*quantity);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn price(value: f64) -> PriceTicks {
        PriceTicks::from_f64(value)
    }

    #[test]
    fn snapshot_replaces_levels_and_sorts_them() {
        let mut book = OrderBook::default();
        book.apply_snapshot(
            vec![(price(0.40), 4), (price(0.45), 5)],
            vec![(price(0.60), 6), (price(0.55), 7)],
        );

        assert_eq!(book.bids(), &[(price(0.45), 5), (price(0.40), 4)]);
        assert_eq!(book.asks(), &[(price(0.55), 7), (price(0.60), 6)]);
        assert!(!book.is_stale());
    }

    #[test]
    fn delta_updates_inserts_and_removes_zero_quantity_levels() {
        let mut book = OrderBook::default();
        book.apply_snapshot([(price(0.40), 4)], [(price(0.60), 6)]);

        book.apply_delta(BookSide::Bid, price(0.40), 9);
        book.apply_delta(BookSide::Ask, price(0.55), 3);
        book.apply_delta(BookSide::Bid, price(0.40), 0);
        book.apply_delta(BookSide::Ask, price(0.60), 0);

        assert_eq!(book.bids(), &[]);
        assert_eq!(book.asks(), &[(price(0.55), 3)]);
    }

    #[test]
    fn empty_book_has_no_best_prices() {
        let book = OrderBook::default();

        assert_eq!(book.best_bid(), None);
        assert_eq!(book.best_ask(), None);
        assert_eq!(book.spread_micros(), None);
        assert_eq!(book.mid_micros(), None);
    }

    #[test]
    fn hash_changes_for_top_five_but_not_deeper_levels() {
        let mut book = OrderBook::default();
        let bids = (1..=6)
            .map(|index| (price(0.40 + index as f64 / 100.0), index))
            .collect::<Vec<_>>();
        book.apply_snapshot(bids.clone(), [(price(0.80), 1)]);
        let initial_hash = book.book_hash();

        book.apply_delta(BookSide::Bid, price(0.46), 99);
        assert_ne!(book.book_hash(), initial_hash);
        let top_five_hash = book.book_hash();

        book.apply_delta(BookSide::Bid, price(0.41), 77);
        assert_eq!(book.book_hash(), top_five_hash);
    }

    #[test]
    fn stale_flag_requires_a_fresh_snapshot() {
        let mut book = OrderBook::default();
        book.apply_snapshot([(price(0.40), 1)], [(price(0.60), 1)]);
        assert!(!book.is_stale());

        book.mark_stale();
        assert!(book.is_stale());
        book.apply_delta(BookSide::Bid, price(0.41), 1);
        assert!(book.is_stale());

        book.apply_snapshot([(price(0.42), 1)], [(price(0.60), 1)]);
        assert!(!book.is_stale());
    }
}
