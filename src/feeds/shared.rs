//! Pure shared accumulation for one external-feed lane per asset.
//!
//! [`AssetFeedState`] mirrors the production accumulator used by the shadow
//! loop without reading a clock or performing I/O. [`SharedFeeds`] only adds
//! symbol-based routing for the supported BTC and ETH lanes.

use crate::domain::Asset;
use crate::state::feature_builder::{ExternalTick, OrderFlowAggregates, VenueMicroprices};

/// Retention window for spot ticks used by the feature builder.
pub const RECENT_TICKS_RETENTION_MS: i64 = 65 * 60 * 1_000;
const ORDER_FLOW_RETENTION_MS: i64 = 5_000;

/// Pure accumulator for one asset's external venue observations.
#[derive(Debug, Clone)]
pub struct AssetFeedState {
    recent_ticks: Vec<ExternalTick>,
    flow: Vec<(i64, f64, bool)>,
    venues: VenueMicroprices,
}

impl Default for AssetFeedState {
    fn default() -> Self {
        Self::new()
    }
}

impl AssetFeedState {
    /// Creates an empty feed state with unavailable venue prices.
    #[must_use]
    pub fn new() -> Self {
        Self {
            recent_ticks: Vec::new(),
            flow: Vec::new(),
            venues: VenueMicroprices {
                binance: f64::NAN,
                coinbase: f64::NAN,
                perp: f64::NAN,
                perp_basis_pct: f64::NAN,
            },
        }
    }

    /// Applies one normalized tick without consulting wall-clock time.
    pub fn apply(&mut self, tick: super::VenueTick) {
        let timestamp = if tick.ts_exchange_ms > 0 {
            tick.ts_exchange_ms
        } else {
            tick.ts_local_ms
        }
        .max(0);
        let price = tick.price_f64;

        if matches!(tick.venue, super::Venue::Binance | super::Venue::Coinbase)
            && price.is_finite()
            && price > 0.0
        {
            self.recent_ticks.push(ExternalTick {
                price,
                ts_ms: timestamp as u64,
            });
            self.recent_ticks.sort_unstable_by_key(|tick| tick.ts_ms);
            let oldest = timestamp.saturating_sub(RECENT_TICKS_RETENTION_MS).max(0) as u64;
            self.recent_ticks.retain(|tick| tick.ts_ms >= oldest);
        }

        if tick.trade_size_f64.is_finite() && tick.trade_size_f64 > 0.0 {
            self.flow
                .push((timestamp, tick.trade_size_f64, tick.trade_side_buy));
            let oldest = timestamp.saturating_sub(ORDER_FLOW_RETENTION_MS);
            self.flow.retain(|(at_ms, _, _)| *at_ms >= oldest);
        }

        let microprice = if tick.best_bid_f64.is_finite()
            && tick.best_ask_f64.is_finite()
            && tick.best_bid_f64 > 0.0
            && tick.best_ask_f64 > 0.0
        {
            Some((tick.best_bid_f64 + tick.best_ask_f64) / 2.0)
        } else if price.is_finite() && price > 0.0 {
            Some(price)
        } else {
            None
        };
        if let Some(microprice) = microprice {
            match tick.venue {
                super::Venue::Binance => self.venues.binance = microprice,
                super::Venue::Coinbase => self.venues.coinbase = microprice,
                super::Venue::Deribit => self.venues.perp = microprice,
            }
        }
    }

    /// Returns oldest-first retained spot ticks.
    #[must_use]
    pub fn recent_ticks(&self) -> &[ExternalTick] {
        &self.recent_ticks
    }

    /// Returns the latest microprice observed at each venue.
    #[must_use]
    pub const fn venues(&self) -> VenueMicroprices {
        self.venues
    }

    /// Calculates order-flow aggregates for the caller-provided timestamp.
    #[must_use]
    pub fn order_flow(&self, at_ms: i64) -> OrderFlowAggregates {
        let mut buy_1s = 0.0;
        let mut sell_1s = 0.0;
        let mut buy_5s = 0.0;
        let mut sell_5s = 0.0;
        for (timestamp, size, buy) in &self.flow {
            let age = at_ms.saturating_sub(*timestamp);
            if (0..=1_000).contains(&age) {
                if *buy {
                    buy_1s += *size;
                } else {
                    sell_1s += *size;
                }
            }
            if (0..=5_000).contains(&age) {
                if *buy {
                    buy_5s += *size;
                } else {
                    sell_5s += *size;
                }
            }
        }
        let total_1s = buy_1s + sell_1s;
        OrderFlowAggregates {
            buy_vol_1s: buy_1s,
            sell_vol_1s: sell_1s,
            ofi_1s: buy_1s - sell_1s,
            ofi_5s: buy_5s - sell_5s,
            imbalance: if total_1s > 0.0 {
                (buy_1s - sell_1s) / total_1s
            } else {
                0.0
            },
            aggressive_buy_ratio: if total_1s > 0.0 {
                buy_1s / total_1s
            } else {
                0.0
            },
        }
    }
}

/// Shared BTC and ETH feed state with symbol-based routing.
#[derive(Debug, Default, Clone)]
pub struct SharedFeeds {
    /// BTC lane, including symbols containing `BTC` or `XBT`.
    pub btc: AssetFeedState,
    /// ETH lane, including symbols containing `ETH`.
    pub eth: AssetFeedState,
    /// Number of ticks whose symbol did not identify a supported asset.
    pub ignored: u64,
}

impl SharedFeeds {
    /// Creates empty BTC and ETH lanes.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Routes a tick by symbol, incrementing [`Self::ignored`] when unsupported.
    pub fn apply(&mut self, tick: super::VenueTick) {
        let symbol = tick.symbol.to_ascii_uppercase();
        if symbol.contains("BTC") || symbol.contains("XBT") {
            self.btc.apply(tick);
        } else if symbol.contains("ETH") {
            self.eth.apply(tick);
        } else {
            self.ignored = self.ignored.saturating_add(1);
        }
    }

    /// Returns the immutable lane for one supported asset.
    #[must_use]
    pub const fn lane(&self, asset: Asset) -> &AssetFeedState {
        match asset {
            Asset::Btc => &self.btc,
            Asset::Eth => &self.eth,
        }
    }
}
