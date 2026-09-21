//! Trade-episode ledger for deterministic replay accounting.
//!
//! A [`TradeEpisode`] records the complete lifecycle of one replay order,
//! including event-time latency, fill/exit state, excursions, and PnL.

use serde::{Deserialize, Serialize};

/// The token bought by a trade episode.
///
/// Values serialize as `"buy_yes"` and `"buy_no"` for QuestDB/JSON.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    BuyYes,
    BuyNo,
}

/// How a trade episode was closed, or why it never became a position.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExitType {
    Resolution,
    Hedge,
    Sell,
    Stop,
    NoFill,
}

/// Ledger record for one order episode in replay event time.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct TradeEpisode {
    pub episode_id: String,
    pub strategy_version: String,
    pub market: String,
    pub asset: String,
    pub horizon: String,
    pub signal_ts_ms: i64,
    pub jev_start_ts_ms: i64,
    pub jev_latency_ms: u64,
    pub submit_latency_ms: u64,
    pub order_arrival_ts_ms: i64,
    pub side: Side,
    pub limit_price: f64,
    pub stake_usd: f64,
    pub shares: f64,
    pub fill_ts_ms: Option<i64>,
    pub fill_price: Option<f64>,
    pub fill_qty: Option<f64>,
    pub exit_type: ExitType,
    pub exit_price: Option<f64>,
    pub exit_ts_ms: Option<i64>,
    #[serde(default)]
    pub exit_signal_ts_ms: Option<i64>,
    #[serde(default)]
    pub exit_arrival_ts_ms: Option<i64>,
    #[serde(default)]
    pub exit_fill_ts_ms: Option<i64>,
    #[serde(default)]
    pub resolution_at_ms: Option<i64>,
    #[serde(default)]
    pub exit_submit_latency_ms: u64,
    pub gross_pnl_usd: f64,
    pub fees_usd: f64,
    pub rebates_usd: f64,
    pub net_pnl_usd: f64,
    pub max_adverse_excursion_usd: f64,
    pub max_favorable_excursion_usd: f64,
    pub capital_seconds_usd_s: f64,
    /// Placeholder for the historical PnL stream owned by Task 5.
    pub pnl_historical_usd: Option<f64>,
    /// Placeholder for the current-mark PnL stream owned by Task 5.
    pub pnl_current_usd: Option<f64>,
}

impl TradeEpisode {
    /// Creates an unfilled episode with an event-time order-arrival timestamp.
    ///
    /// `side` is an explicit YES/NO buy side rather than a free-form string,
    /// which keeps the serialized ledger values normalized.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        episode_id: impl Into<String>,
        strategy_version: impl Into<String>,
        market: impl Into<String>,
        asset: impl Into<String>,
        horizon: impl Into<String>,
        signal_ts_ms: i64,
        jev_start_ts_ms: i64,
        jev_latency_ms: u64,
        submit_latency_ms: u64,
        side: Side,
        limit_price: f64,
        stake_usd: f64,
    ) -> Result<Self, String> {
        Self::new_with_exit_submit_latency(
            episode_id,
            strategy_version,
            market,
            asset,
            horizon,
            signal_ts_ms,
            jev_start_ts_ms,
            jev_latency_ms,
            submit_latency_ms,
            0,
            side,
            limit_price,
            stake_usd,
        )
    }

    /// Creates an unfilled episode with explicit entry and exit submit latency.
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_exit_submit_latency(
        episode_id: impl Into<String>,
        strategy_version: impl Into<String>,
        market: impl Into<String>,
        asset: impl Into<String>,
        horizon: impl Into<String>,
        signal_ts_ms: i64,
        jev_start_ts_ms: i64,
        jev_latency_ms: u64,
        submit_latency_ms: u64,
        exit_submit_latency_ms: u64,
        side: Side,
        limit_price: f64,
        stake_usd: f64,
    ) -> Result<Self, String> {
        if !limit_price.is_finite() || limit_price <= 0.0 {
            return Err("limit_price must be finite and greater than zero".to_owned());
        }
        if !stake_usd.is_finite() || stake_usd <= 0.0 {
            return Err("stake_usd must be finite and greater than zero".to_owned());
        }

        let jev_latency_ms_i64 = i64::try_from(jev_latency_ms)
            .map_err(|_| "jev_latency_ms overflows the i64 timestamp range".to_owned())?;
        let submit_latency_ms_i64 = i64::try_from(submit_latency_ms)
            .map_err(|_| "submit_latency_ms overflows the i64 timestamp range".to_owned())?;
        let order_arrival_ts_ms = signal_ts_ms
            .checked_add(jev_latency_ms_i64)
            .and_then(|ts| ts.checked_add(submit_latency_ms_i64))
            .ok_or_else(|| "order arrival timestamp overflows i64".to_owned())?;

        Ok(Self {
            episode_id: episode_id.into(),
            strategy_version: strategy_version.into(),
            market: market.into(),
            asset: asset.into(),
            horizon: horizon.into(),
            signal_ts_ms,
            jev_start_ts_ms,
            jev_latency_ms,
            submit_latency_ms,
            order_arrival_ts_ms,
            side,
            limit_price,
            stake_usd,
            shares: stake_usd / limit_price,
            fill_ts_ms: None,
            fill_price: None,
            fill_qty: None,
            exit_type: ExitType::NoFill,
            exit_price: None,
            exit_ts_ms: None,
            exit_signal_ts_ms: None,
            exit_arrival_ts_ms: None,
            exit_fill_ts_ms: None,
            resolution_at_ms: None,
            exit_submit_latency_ms,
            gross_pnl_usd: 0.0,
            fees_usd: 0.0,
            rebates_usd: 0.0,
            net_pnl_usd: 0.0,
            max_adverse_excursion_usd: 0.0,
            max_favorable_excursion_usd: 0.0,
            capital_seconds_usd_s: 0.0,
            pnl_historical_usd: None,
            pnl_current_usd: None,
        })
    }

    /// Records a fill after the order has arrived in replay event time.
    pub fn apply_fill(&mut self, ts_ms: i64, price: f64, qty: f64) -> Result<(), String> {
        if ts_ms < self.order_arrival_ts_ms {
            return Err(format!(
                "fill timestamp {ts_ms} precedes order arrival {}",
                self.order_arrival_ts_ms
            ));
        }

        self.fill_ts_ms = Some(ts_ms);
        self.fill_price = Some(price);
        self.fill_qty = Some(qty);
        Ok(())
    }

    /// Requests an exit and computes its event-time arrival timestamp.
    pub fn request_exit(
        &mut self,
        signal_ts_ms: i64,
        submit_latency_ms: u64,
    ) -> Result<(), String> {
        let submit_latency_ms_i64 = i64::try_from(submit_latency_ms)
            .map_err(|_| "exit submit latency overflows the i64 timestamp range".to_owned())?;
        let exit_arrival_ts_ms = signal_ts_ms
            .checked_add(submit_latency_ms_i64)
            .ok_or_else(|| "exit arrival timestamp overflows i64".to_owned())?;

        self.exit_signal_ts_ms = Some(signal_ts_ms);
        self.exit_arrival_ts_ms = Some(exit_arrival_ts_ms);
        self.exit_submit_latency_ms = submit_latency_ms;
        Ok(())
    }

    /// Records the real fill of the exit leg after its requested exit arrives.
    pub fn apply_exit_fill(&mut self, ts_ms: i64, price: f64) -> Result<(), String> {
        let Some(exit_arrival_ts_ms) = self.exit_arrival_ts_ms else {
            return Err("an exit request is required before applying an exit fill".to_owned());
        };
        if ts_ms < exit_arrival_ts_ms {
            return Err(format!(
                "exit fill timestamp {ts_ms} precedes exit arrival {exit_arrival_ts_ms}"
            ));
        }

        self.exit_fill_ts_ms = Some(ts_ms);
        self.exit_price = Some(price);
        self.exit_ts_ms = Some(ts_ms);
        self.capital_seconds_usd_s = self.realized_capital_seconds();
        Ok(())
    }

    /// Records a caller-provided resolution timestamp.
    pub fn set_resolution(&mut self, ts_ms: i64) -> Result<(), String> {
        if let Some(fill_ts_ms) = self.fill_ts_ms
            && ts_ms < fill_ts_ms
        {
            return Err(format!(
                "resolution timestamp {ts_ms} precedes fill timestamp {fill_ts_ms}"
            ));
        }
        if let Some(exit_fill_ts_ms) = self.exit_fill_ts_ms
            && ts_ms < exit_fill_ts_ms
        {
            return Err(format!(
                "resolution timestamp {ts_ms} precedes exit fill timestamp {exit_fill_ts_ms}"
            ));
        }

        self.resolution_at_ms = Some(ts_ms);
        Ok(())
    }

    /// Records a terminal exit. `NoFill` leaves fill and exit details empty.
    pub fn apply_exit(
        &mut self,
        exit_type: ExitType,
        ts_ms: i64,
        price: f64,
    ) -> Result<(), String> {
        if exit_type == ExitType::NoFill {
            if self.fill_ts_ms.is_some() || self.fill_price.is_some() || self.fill_qty.is_some() {
                return Err("NoFill cannot be applied after a fill".to_owned());
            }
            self.exit_type = ExitType::NoFill;
            self.exit_price = None;
            self.exit_ts_ms = None;
            self.exit_signal_ts_ms = None;
            self.exit_arrival_ts_ms = None;
            self.exit_fill_ts_ms = None;
            self.resolution_at_ms = None;
            return Ok(());
        }

        let Some(fill_ts_ms) = self.fill_ts_ms else {
            return Err("a filled episode is required before applying an exit".to_owned());
        };
        if ts_ms < fill_ts_ms {
            return Err(format!(
                "exit timestamp {ts_ms} precedes fill timestamp {fill_ts_ms}"
            ));
        }
        if let Some(exit_arrival_ts_ms) = self.exit_arrival_ts_ms
            && ts_ms < exit_arrival_ts_ms
        {
            return Err(format!(
                "exit timestamp {ts_ms} precedes exit arrival {exit_arrival_ts_ms}"
            ));
        }
        if let Some(exit_fill_ts_ms) = self.exit_fill_ts_ms
            && ts_ms < exit_fill_ts_ms
        {
            return Err(format!(
                "exit timestamp {ts_ms} precedes exit fill {exit_fill_ts_ms}"
            ));
        }

        self.exit_type = exit_type;
        self.exit_price = Some(price);
        self.exit_ts_ms = Some(ts_ms);
        self.capital_seconds_usd_s = self.realized_capital_seconds();
        Ok(())
    }

    /// Stores the latest adverse and favorable excursion observations.
    pub fn record_excursions(&mut self, mae: f64, mfe: f64) {
        self.max_adverse_excursion_usd = mae;
        self.max_favorable_excursion_usd = mfe;
    }

    /// Stores accounting components after verifying the net PnL invariant.
    pub fn settle_accounting(
        &mut self,
        gross_pnl_usd: f64,
        fees_usd: f64,
        rebates_usd: f64,
        net_pnl_usd: f64,
    ) -> Result<(), String> {
        let expected_net = gross_pnl_usd - fees_usd + rebates_usd;
        if !expected_net.is_finite()
            || !gross_pnl_usd.is_finite()
            || !fees_usd.is_finite()
            || !rebates_usd.is_finite()
            || !net_pnl_usd.is_finite()
            || (expected_net - net_pnl_usd).abs() > 1e-9
        {
            return Err(format!(
                "net PnL invariant failed: expected {expected_net}, received {net_pnl_usd}"
            ));
        }

        self.gross_pnl_usd = gross_pnl_usd;
        self.fees_usd = fees_usd;
        self.rebates_usd = rebates_usd;
        self.net_pnl_usd = net_pnl_usd;
        Ok(())
    }

    /// Returns whether this episode never received a fill.
    #[must_use]
    pub fn is_no_fill(&self) -> bool {
        self.exit_type == ExitType::NoFill
            && self.fill_ts_ms.is_none()
            && self.fill_price.is_none()
            && self.fill_qty.is_none()
    }

    /// Returns stake-weighted capital time in USD-seconds.
    #[must_use]
    pub fn realized_capital_seconds(&self) -> f64 {
        let (Some(fill_ts_ms), Some(exit_ts_ms)) = (self.fill_ts_ms, self.exit_ts_ms) else {
            return 0.0;
        };
        self.stake_usd * (exit_ts_ms.saturating_sub(fill_ts_ms) as f64 / 1_000.0)
    }
}
