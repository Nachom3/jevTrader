//! Pure feature construction for the lead-lag strategy.
//!
//! The builder accepts only caller-owned values and never reads a clock or does
//! I/O. External ticks must be oldest-first. Horizon returns are simple
//! percentage returns from the latest tick to a timestamp `horizon` milliseconds
//! earlier. The earlier price is linearly interpolated between neighboring
//! irregularly-timed ticks; a horizon without a complete bracketed history
//! returns `0.0`.
//!
//! Realized volatility is the population standard deviation of the consecutive
//! one-second simple percentage returns on the same interpolated time grid. It
//! is reported as plain percentage points, not annualized. A complete one-minute
//! or five-minute grid is required; otherwise that volatility field is `0.0`.
//! The target distance is `(spot / target - 1) * 100`, and cross-exchange
//! differences use the first value relative to the second value. Invalid or
//! non-positive prices produce the documented `0.0` default for the affected
//! derived value.

use crate::state::rolling::Returns;
use crate::strategy::lead_lag::{LeadLagFeatures, PolySnapshot};

/// One external venue tick, ordered oldest-first in the builder input.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ExternalTick {
    pub price: f64,
    pub ts_ms: u64,
}

/// Microprices and basis supplied by venue/feed actors.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct VenueMicroprices {
    pub binance: f64,
    pub coinbase: f64,
    pub perp: f64,
    pub perp_basis_pct: f64,
}

/// Order-flow aggregates supplied by the feed actor.
///
/// OFI is intentionally an input here. This pure builder does not reconstruct
/// it from book events; feed-specific OFI computation belongs to a later actor.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OrderFlowAggregates {
    pub buy_vol_1s: f64,
    pub sell_vol_1s: f64,
    pub ofi_1s: f64,
    pub ofi_5s: f64,
    pub imbalance: f64,
    pub aggressive_buy_ratio: f64,
}

/// Resolution metadata supplied by the market-state caller.
///
/// The builder does not read a clock: `time_remaining_secs` must be computed
/// by the caller from its chosen observation timestamp and the market's
/// resolution timestamp. An unavailable resolution source remains empty; this
/// type never fabricates metadata.
#[derive(Debug, Clone, PartialEq)]
pub struct ResolutionContext {
    pub target: f64,
    pub time_remaining_secs: u64,
    pub resolution_source: String,
}

impl ResolutionContext {
    #[must_use]
    pub fn new(
        target: f64,
        time_remaining_secs: u64,
        resolution_source: impl Into<String>,
    ) -> Self {
        Self {
            target,
            time_remaining_secs,
            resolution_source: resolution_source.into(),
        }
    }

    /// Legacy target-only constructor. Prefer [`ResolutionContext::new`] so
    /// resolution metadata is explicit.
    #[deprecated(note = "construct an explicit ResolutionContext with ResolutionContext::new")]
    pub fn from(target: f64) -> Self {
        Self::new(target, 0, "")
    }
}

/// Compatibility conversion for existing benchmark and test callers that
/// only supplied a target. New callers must pass [`ResolutionContext`] so the
/// serialized state carries real resolution metadata; the empty source and
/// zero remaining time here explicitly mean that legacy caller did not have
/// that metadata available.
impl From<f64> for ResolutionContext {
    fn from(target: f64) -> Self {
        Self::new(target, 0, "")
    }
}

impl From<&ResolutionContext> for ResolutionContext {
    fn from(context: &ResolutionContext) -> Self {
        context.clone()
    }
}

/// Builds deterministic lead-lag features from snapshots already collected by
/// the feed actors.
///
/// The third argument is a [`ResolutionContext`] in the production path. It is
/// generic only to preserve the existing five-argument API for old benches and
/// tests, which may still pass a bare `f64` target. The context is then used by
/// [`build_features_with_context`] without reading a clock or doing I/O.
///
/// `PolySnapshot` remains in this compatibility boundary because the existing
/// callers pass it, but it is intentionally not copied into
/// [`LeadLagFeatures`]: Polymarket fields already belong to the separate
/// `V1State::polymarket` object. The canonical context-aware builder therefore
/// omits that redundant argument while this wrapper prevents old callers from
/// dropping the snapshot at their call site.
#[must_use]
pub fn build_features<R>(
    recent_ticks: &[ExternalTick],
    _poly_snapshot: &PolySnapshot,
    resolution: R,
    venues: VenueMicroprices,
    order_flow: OrderFlowAggregates,
) -> LeadLagFeatures
where
    R: Into<ResolutionContext>,
{
    let context = resolution.into();
    build_features_with_context(recent_ticks, &context, venues, order_flow)
}

/// Which of the 8 contracts a feature snapshot belongs to.
///
/// Pure metadata: it labels the state Jev sees (asset, horizon, window) but
/// never changes thresholds or questions. `Default` is the unknown contract
/// (empty labels, zero window); builders then emit `0.0`/`""` horizon fields.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ContractContext {
    pub asset_symbol: String,
    pub horizon_label: String,
    pub horizon_secs: u64,
}

impl ContractContext {
    /// Creates an explicit contract label (e.g. `("BTC", "5m", 300)`).
    #[must_use]
    pub fn new(
        asset_symbol: impl Into<String>,
        horizon_label: impl Into<String>,
        horizon_secs: u64,
    ) -> Self {
        Self {
            asset_symbol: asset_symbol.into(),
            horizon_label: horizon_label.into(),
            horizon_secs,
        }
    }

    /// Builds the label from domain types without importing them here.
    #[must_use]
    pub fn from_parts(asset: &str, horizon: &str, horizon_secs: u64) -> Self {
        Self::new(asset, horizon, horizon_secs)
    }
}

/// Canonical pure feature builder for callers that already own the resolution
/// context and do not need the legacy Polymarket argument.
///
/// Contract labeling defaults to unknown; use [`build_features_full`] when
/// the asset/horizon are known.
#[must_use]
pub fn build_features_with_context(
    recent_ticks: &[ExternalTick],
    context: &ResolutionContext,
    venues: VenueMicroprices,
    order_flow: OrderFlowAggregates,
) -> LeadLagFeatures {
    build_features_full(
        recent_ticks,
        context,
        &ContractContext::default(),
        venues,
        order_flow,
    )
}

/// Full pure feature builder: resolution context plus contract label.
///
/// Longer horizons (`15m`/`30m`/`1h`, `vol_1h`) need longer tick retention;
/// when the caller history is too short those fields are the documented
/// `0.0` rather than extrapolated values. No clock or I/O is used.
#[must_use]
pub fn build_features_full(
    recent_ticks: &[ExternalTick],
    context: &ResolutionContext,
    contract: &ContractContext,
    venues: VenueMicroprices,
    order_flow: OrderFlowAggregates,
) -> LeadLagFeatures {
    let spot = recent_ticks
        .last()
        .and_then(|tick| valid_price(tick.price))
        .unwrap_or(0.0);
    let end_ts = recent_ticks.last().map(|tick| tick.ts_ms);

    let ret = |horizon_ms| {
        end_ts
            .and_then(|end| percentage_return(recent_ticks, end, horizon_ms))
            .unwrap_or(0.0)
    };

    let realized_vol = |seconds| {
        end_ts
            .map(|end| realized_volatility(recent_ticks, end, seconds))
            .unwrap_or(0.0)
    };

    LeadLagFeatures {
        target: finite_or_zero(context.target),
        time_remaining_secs: context.time_remaining_secs,
        resolution_source: context.resolution_source.clone(),
        asset_symbol: contract.asset_symbol.clone(),
        horizon_label: contract.horizon_label.clone(),
        horizon_secs: contract.horizon_secs,
        spot,
        distance_to_target_pct: relative_difference(spot, context.target),
        ret_250ms_pct: ret(250),
        ret_1s_pct: ret(1_000),
        ret_5s_pct: ret(5_000),
        ret_30s_pct: ret(30_000),
        ret_1m_pct: ret(60_000),
        ret_5m_pct: ret(300_000),
        ret_15m_pct: ret(900_000),
        ret_30m_pct: ret(1_800_000),
        ret_1h_pct: ret(3_600_000),
        realized_vol_1m_pct: realized_vol(60),
        realized_vol_5m_pct: realized_vol(300),
        realized_vol_1h_pct: realized_vol(3_600),
        binance_microprice: finite_or_zero(venues.binance),
        coinbase_microprice: finite_or_zero(venues.coinbase),
        perp_price: finite_or_zero(venues.perp),
        perp_basis_pct: finite_or_zero(venues.perp_basis_pct),
        buy_vol_1s: finite_or_zero(order_flow.buy_vol_1s),
        sell_vol_1s: finite_or_zero(order_flow.sell_vol_1s),
        ofi_1s: finite_or_zero(order_flow.ofi_1s),
        ofi_5s: finite_or_zero(order_flow.ofi_5s),
        book_imbalance: finite_or_zero(order_flow.imbalance),
        aggressive_buy_ratio: finite_or_zero(order_flow.aggressive_buy_ratio),
        // Poly-tape flow is V2-only via build_features_micro; the full
        // builder keeps 0.0 so V1 states stay byte-identical.
        poly_ofi_5s: 0.0,
        poly_aggressive_buy_ratio: 0.0,
        poly_buy_vol_5s: 0.0,
        poly_sell_vol_5s: 0.0,
        binance_coinbase_diff_pct: relative_difference(venues.binance, venues.coinbase),
        spot_perp_diff_pct: relative_difference(spot, venues.perp),
    }
}

/// V2 microstructure builder: real spot flow + real perp venues + Poly-tape
/// flow on top of the frozen full builder.
///
/// The V1 path (build_features_full) is untouched so V1 states stay
/// byte-identical; every V2 enrichment flows through this function only.
/// `poly_flow` carries 5s Poly-tape aggregates (buy/sell vols, OFI, buy
/// ratio); its 1s fields are ignored because tape trades are sparse.
#[must_use]
pub fn build_features_micro(
    recent_ticks: &[ExternalTick],
    context: &ResolutionContext,
    contract: &ContractContext,
    venues: VenueMicroprices,
    order_flow: OrderFlowAggregates,
    poly_flow: OrderFlowAggregates,
) -> LeadLagFeatures {
    let mut features = build_features_full(recent_ticks, context, contract, venues, order_flow);
    features.poly_ofi_5s = finite_or_zero(poly_flow.ofi_5s);
    features.poly_aggressive_buy_ratio = finite_or_zero(poly_flow.aggressive_buy_ratio);
    // Caller places 5s Poly vols in the vol slots (documented at call site).
    features.poly_buy_vol_5s = finite_or_zero(poly_flow.buy_vol_1s);
    features.poly_sell_vol_5s = finite_or_zero(poly_flow.sell_vol_1s);
    features
}

fn percentage_return(ticks: &[ExternalTick], end_ts: u64, horizon_ms: u64) -> Option<f64> {
    let end_price = interpolated_price(ticks, end_ts)?;
    let start_ts = end_ts.checked_sub(horizon_ms)?;
    let start_price = interpolated_price(ticks, start_ts)?;
    Returns::between(start_price, end_price)
}

fn realized_volatility(ticks: &[ExternalTick], end_ts: u64, seconds: u64) -> f64 {
    if seconds == 0 {
        return 0.0;
    }

    let Some(oldest_ts) = ticks.first().map(|tick| tick.ts_ms) else {
        return 0.0;
    };
    let horizon_ms = seconds.saturating_mul(1_000);
    if end_ts.saturating_sub(oldest_ts) < horizon_ms {
        return 0.0;
    }

    let mut count = 0_u64;
    let mut sum = 0.0;
    let mut sum_squares = 0.0;

    for step in (1..=seconds).rev() {
        let old_ts = end_ts.saturating_sub(step.saturating_mul(1_000));
        let new_ts = end_ts.saturating_sub((step - 1).saturating_mul(1_000));
        let Some(old_price) = interpolated_price(ticks, old_ts) else {
            return 0.0;
        };
        let Some(new_price) = interpolated_price(ticks, new_ts) else {
            return 0.0;
        };
        let Some(return_pct) = Returns::between(old_price, new_price) else {
            return 0.0;
        };

        count += 1;
        sum += return_pct;
        sum_squares += return_pct * return_pct;
    }

    let mean = sum / count as f64;
    (sum_squares / count as f64 - mean * mean).max(0.0).sqrt()
}

/// Finds a price on an irregular timestamp series by exact lookup or linear
/// interpolation. Values before the oldest tick are unavailable rather than
/// extrapolated; the latest timestamp can be returned exactly.
fn interpolated_price(ticks: &[ExternalTick], target_ts: u64) -> Option<f64> {
    let right = ticks.partition_point(|tick| tick.ts_ms < target_ts);
    if right == ticks.len() {
        return ticks.last().and_then(|tick| {
            (tick.ts_ms == target_ts)
                .then_some(tick)
                .and_then(|tick| valid_price(tick.price))
        });
    }

    let right_tick = ticks.get(right)?;
    if right_tick.ts_ms == target_ts {
        return valid_price(right_tick.price);
    }
    if right == 0 {
        return None;
    }

    let left_tick = ticks.get(right - 1)?;
    let left_price = valid_price(left_tick.price)?;
    let right_price = valid_price(right_tick.price)?;
    let span = right_tick.ts_ms.checked_sub(left_tick.ts_ms)?;
    if span == 0 {
        return Some(right_price);
    }

    let fraction = (target_ts - left_tick.ts_ms) as f64 / span as f64;
    Some(left_price + (right_price - left_price) * fraction)
}

fn valid_price(price: f64) -> Option<f64> {
    (price.is_finite() && price > 0.0).then_some(price)
}

fn finite_or_zero(value: f64) -> f64 {
    if value.is_finite() { value } else { 0.0 }
}

fn relative_difference(value: f64, reference: f64) -> f64 {
    match (valid_price(value), valid_price(reference)) {
        (Some(value), Some(reference)) => (value / reference - 1.0) * 100.0,
        _ => 0.0,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ExternalTick, OrderFlowAggregates, ResolutionContext, VenueMicroprices, build_features,
    };
    use crate::strategy::lead_lag::PolySnapshot;
    use jevtrader::domain::PriceTicks;

    fn poly_snapshot() -> PolySnapshot {
        PolySnapshot {
            yes_bid: PriceTicks::from_f64(0.40),
            yes_ask: PriceTicks::from_f64(0.42),
            bid_depth: 100.0,
            ask_depth: 90.0,
            spread: 0.02,
            book_imbalance: 0.05,
            last_trade_price: PriceTicks::from_f64(0.41),
            price_1s_ago: PriceTicks::from_f64(0.40),
            price_5s_ago: PriceTicks::from_f64(0.39),
            price_30s_ago: PriceTicks::from_f64(0.38),
        }
    }

    fn venues() -> VenueMicroprices {
        VenueMicroprices {
            binance: 101.0,
            coinbase: 100.0,
            perp: 102.0,
            perp_basis_pct: 2.0,
        }
    }

    fn flow() -> OrderFlowAggregates {
        OrderFlowAggregates {
            buy_vol_1s: 4.0,
            sell_vol_1s: 3.0,
            ofi_1s: 1.0,
            ofi_5s: 2.0,
            imbalance: 0.1,
            aggressive_buy_ratio: 0.6,
        }
    }

    fn flat_ticks() -> Vec<ExternalTick> {
        (0..=300_000)
            .step_by(1_000)
            .map(|ts_ms| ExternalTick {
                price: 100.0,
                ts_ms,
            })
            .collect()
    }

    #[test]
    fn flat_series_has_zero_returns_and_volatility() {
        let features = build_features(
            &flat_ticks(),
            &poly_snapshot(),
            ResolutionContext::new(100.0, 900, "Test source"),
            venues(),
            flow(),
        );

        assert_eq!(features.spot, 100.0);
        assert!(features.ret_250ms_pct.abs() < f64::EPSILON);
        assert!(features.ret_1s_pct.abs() < f64::EPSILON);
        assert!(features.ret_5s_pct.abs() < f64::EPSILON);
        assert!(features.ret_30s_pct.abs() < f64::EPSILON);
        assert!(features.ret_5m_pct.abs() < f64::EPSILON);
        assert!(features.realized_vol_1m_pct.abs() < f64::EPSILON);
        assert!(features.realized_vol_5m_pct.abs() < f64::EPSILON);
        assert!(features.distance_to_target_pct.abs() < f64::EPSILON);
    }

    #[test]
    fn resolution_context_is_copied_without_clock_or_io() {
        let features = build_features(
            &flat_ticks(),
            &poly_snapshot(),
            ResolutionContext::new(120.0, 987, "Official source"),
            venues(),
            flow(),
        );

        assert_eq!(features.target, 120.0);
        assert_eq!(features.time_remaining_secs, 987);
        assert_eq!(features.resolution_source, "Official source");
    }

    #[test]
    fn irregular_interpolation_preserves_expected_return_signs() {
        let ticks = vec![
            ExternalTick {
                price: 100.0,
                ts_ms: 0,
            },
            ExternalTick {
                price: 100.0,
                ts_ms: 299_000,
            },
            ExternalTick {
                price: 110.0,
                ts_ms: 300_000,
            },
        ];
        let features = build_features(
            &ticks,
            &poly_snapshot(),
            ResolutionContext::new(120.0, 900, "Test source"),
            venues(),
            flow(),
        );

        assert!(features.ret_250ms_pct > 0.0);
        assert!(features.ret_1s_pct > 0.0);
        assert!(features.ret_5s_pct > 0.0);
        assert!(features.ret_30s_pct > 0.0);
        assert!(features.ret_5m_pct > 0.0);
        assert!(features.distance_to_target_pct < 0.0);
        assert!(features.spot_perp_diff_pct > 0.0);
    }

    #[test]
    fn empty_and_short_series_use_zero_derived_defaults_without_panicking() {
        let empty_ticks: [ExternalTick; 0] = [];
        let empty = build_features(
            empty_ticks.as_slice(),
            &poly_snapshot(),
            ResolutionContext::new(100.0, 900, "Test source"),
            venues(),
            flow(),
        );
        assert_eq!(empty.spot, 0.0);
        assert_eq!(empty.ret_1s_pct, 0.0);
        assert_eq!(empty.realized_vol_5m_pct, 0.0);
        assert_eq!(empty.distance_to_target_pct, 0.0);

        let short = [ExternalTick {
            price: 123.0,
            ts_ms: 10,
        }];
        let features = build_features(
            short.as_slice(),
            &poly_snapshot(),
            ResolutionContext::new(100.0, 900, "Test source"),
            venues(),
            flow(),
        );
        assert_eq!(features.spot, 123.0);
        assert_eq!(features.ret_5m_pct, 0.0);
        assert_eq!(features.realized_vol_1m_pct, 0.0);
        assert!((features.distance_to_target_pct - 23.0).abs() < f64::EPSILON);
    }

    #[test]
    fn order_flow_and_cross_exchange_inputs_are_forwarded_deterministically() {
        let features = build_features(
            &flat_ticks(),
            &poly_snapshot(),
            ResolutionContext::new(100.0, 900, "Test source"),
            venues(),
            flow(),
        );

        assert_eq!(features.buy_vol_1s, 4.0);
        assert_eq!(features.sell_vol_1s, 3.0);
        assert_eq!(features.ofi_1s, 1.0);
        assert_eq!(features.ofi_5s, 2.0);
        assert_eq!(features.book_imbalance, 0.1);
        assert_eq!(features.aggressive_buy_ratio, 0.6);
        assert!((features.binance_coinbase_diff_pct - 1.0).abs() < 1e-12);
        let expected_spot_perp_diff = (100.0 / 102.0 - 1.0) * 100.0;
        assert!((features.spot_perp_diff_pct - expected_spot_perp_diff).abs() < 1e-12);
    }

    #[test]
    fn micro_builder_maps_poly_flow_and_keeps_v1_frozen() {
        use super::{ContractContext, build_features_full, build_features_micro};
        let ticks = flat_ticks();
        let ctx = ResolutionContext::new(100.0, 900, "Test source");
        let contract = ContractContext::new("BTC", "5m", 300);
        let poly_flow = OrderFlowAggregates {
            buy_vol_1s: 7.0,
            sell_vol_1s: 3.0,
            ofi_1s: 0.0,
            ofi_5s: 4.0,
            imbalance: 0.0,
            aggressive_buy_ratio: 0.7,
        };
        let micro = build_features_micro(&ticks, &ctx, &contract, venues(), flow(), poly_flow);
        assert_eq!(micro.poly_ofi_5s, 4.0);
        assert_eq!(micro.poly_aggressive_buy_ratio, 0.7);
        assert_eq!(micro.poly_buy_vol_5s, 7.0);
        assert_eq!(micro.poly_sell_vol_5s, 3.0);
        assert_eq!(micro.asset_symbol, "BTC");
        // V1 path untouched: poly fields stay zero.
        let v1 = build_features_full(&ticks, &ctx, &contract, venues(), flow());
        assert_eq!(v1.poly_ofi_5s, 0.0);
        assert_eq!(v1.poly_aggressive_buy_ratio, 0.0);
    }

    #[test]
    fn contract_context_labels_features_without_changing_values() {
        use super::{ContractContext, build_features_full};

        let contract = ContractContext::new("ETH", "1h", 3_600);
        let features = build_features_full(
            &flat_ticks(),
            &ResolutionContext::new(100.0, 900, "Test source"),
            &contract,
            venues(),
            flow(),
        );

        assert_eq!(features.asset_symbol, "ETH");
        assert_eq!(features.horizon_label, "1h");
        assert_eq!(features.horizon_secs, 3_600);
        assert_eq!(features.spot, 100.0);
    }

    #[test]
    fn long_horizons_default_to_zero_without_enough_history() {
        // 5 minutes of ticks: 1m/5m are computable, 15m/30m/1h are not.
        let features = build_features(
            &flat_ticks(),
            &poly_snapshot(),
            ResolutionContext::new(100.0, 900, "Test source"),
            venues(),
            flow(),
        );

        assert_eq!(features.ret_15m_pct, 0.0);
        assert_eq!(features.ret_30m_pct, 0.0);
        assert_eq!(features.ret_1h_pct, 0.0);
        assert_eq!(features.realized_vol_1h_pct, 0.0);
    }

    #[test]
    fn one_hour_uptrend_is_visible_when_history_covers_it() {
        use super::{ContractContext, build_features_full};

        let ticks: Vec<ExternalTick> = (0..=3_600_000)
            .step_by(60_000)
            .map(|ts_ms| ExternalTick {
                price: 100.0 + (ts_ms as f64 / 60_000.0) * 0.1,
                ts_ms: ts_ms as u64,
            })
            .collect();
        let features = build_features_full(
            &ticks,
            &ResolutionContext::new(200.0, 3_600, "Test source"),
            &ContractContext::new("BTC", "1h", 3_600),
            venues(),
            flow(),
        );

        assert!(features.ret_1m_pct > 0.0);
        assert!(features.ret_15m_pct > 0.0);
        assert!(features.ret_30m_pct > 0.0);
        assert!(features.ret_1h_pct > 0.0);
        assert!(features.ret_1h_pct > features.ret_1m_pct);
    }
}
