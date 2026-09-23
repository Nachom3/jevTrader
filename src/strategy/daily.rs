//! Deterministic daily technical strategy (V2), independent of Jev and V1.
//!
//! Signals are based exclusively on closed daily candles. Execution, market
//! references, persistence, and decisions about when to trade remain with the
//! caller. PnL tracking uses raw reference-price differences and does not
//! account for fees, slippage, or funding.

use std::time::Duration;

use chrono::{DateTime, Utc};
use serde_json::Value;
use thiserror::Error;

use super::indicators::{adx_last, ema_last, rsi_last, sma_last};

const BINANCE_KLINES_URL: &str = "https://api.binance.com/api/v3/klines";
const MIN_CLOSED_CANDLES: usize = 60;

/// One UTC daily OHLC candle.
#[derive(Debug, Clone, PartialEq)]
pub struct Candle {
    pub day_utc: String,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
}

/// Closed historical candles plus the current live UTC-day candle.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct DailyFeed {
    /// Closed candles, oldest first.
    pub closed: Vec<Candle>,
    /// The candle currently being formed by live ticks.
    pub forming: Option<Candle>,
}

impl DailyFeed {
    /// Adds a valid positive spot tick to its UTC-day candle.
    ///
    /// Invalid prices and timestamps outside Chrono's representable range are
    /// ignored. On a day change, the previous forming candle becomes closed.
    pub fn push_tick(&mut self, ts_ms: i64, price: f64) {
        if !price.is_finite() || price <= 0.0 {
            return;
        }
        let Some(timestamp) = DateTime::<Utc>::from_timestamp_millis(ts_ms) else {
            return;
        };
        let day_utc = timestamp.format("%Y-%m-%d").to_string();

        match self.forming.as_mut() {
            Some(candle) if candle.day_utc == day_utc => {
                candle.high = candle.high.max(price);
                candle.low = candle.low.min(price);
                candle.close = price;
            }
            Some(_) => {
                if let Some(previous) = self.forming.take() {
                    self.closed.push(previous);
                }
                self.forming = Some(new_candle(day_utc, price));
            }
            None => self.forming = Some(new_candle(day_utc, price)),
        }
    }
}

fn new_candle(day_utc: String, price: f64) -> Candle {
    Candle {
        day_utc,
        open: price,
        high: price,
        low: price,
        close: price,
    }
}

/// Errors returned while requesting or decoding Binance daily klines.
#[derive(Debug, Error)]
pub enum DailyError {
    #[error("unsupported symbol `{0}`; expected BTC or ETH")]
    UnsupportedSymbol(String),
    #[error("Binance daily kline HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("Binance returned invalid daily kline JSON: {0}")]
    Parse(#[from] serde_json::Error),
    #[error("invalid Binance kline row {row}: {reason}")]
    InvalidKline { row: usize, reason: String },
}

/// Fetches Binance UTC 1D klines for BTC or ETH.
///
/// `days` is clamped to Binance's supported range requested by this engine,
/// 5 through 200. Rows are returned oldest-first; Binance's still-forming
/// current UTC-day kline is excluded so this result can seed `DailyFeed::closed`.
pub async fn backfill_daily(symbol: &str, days: u64) -> Result<Vec<Candle>, DailyError> {
    let symbol = match symbol.trim().to_ascii_uppercase().as_str() {
        "BTC" => "BTCUSDT",
        "ETH" => "ETHUSDT",
        _ => return Err(DailyError::UnsupportedSymbol(symbol.to_owned())),
    };
    let limit = days.clamp(5, 200).to_string();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;
    let response = client
        .get(BINANCE_KLINES_URL)
        .query(&[("symbol", symbol), ("interval", "1d"), ("limit", &limit)])
        .send()
        .await?
        .error_for_status()?;
    let body = response.bytes().await?;
    let rows: Vec<Vec<Value>> = serde_json::from_slice(&body)?;

    let today_utc = Utc::now().format("%Y-%m-%d").to_string();
    rows.iter()
        .enumerate()
        .map(|(index, row)| parse_kline(index, row))
        .collect::<Result<Vec<_>, _>>()
        .map(|candles| {
            candles
                .into_iter()
                .filter(|candle| candle.day_utc.as_str() < today_utc.as_str())
                .collect()
        })
}

fn parse_kline(index: usize, row: &[Value]) -> Result<Candle, DailyError> {
    let invalid = |reason: &str| DailyError::InvalidKline {
        row: index,
        reason: reason.to_owned(),
    };
    if row.len() < 5 {
        return Err(invalid("expected open time and four OHLC values"));
    }
    let open_time = row[0]
        .as_i64()
        .ok_or_else(|| invalid("open time must be an integer millisecond timestamp"))?;
    let timestamp = DateTime::<Utc>::from_timestamp_millis(open_time)
        .ok_or_else(|| invalid("open time is outside the UTC date range"))?;
    let parse_price = |column: usize, name: &str| -> Result<f64, DailyError> {
        let value = row[column]
            .as_str()
            .ok_or_else(|| invalid(&format!("{name} must be a string")))?;
        value
            .parse::<f64>()
            .map_err(|_| invalid(&format!("{name} is not a number")))
    };
    let open = parse_price(1, "open")?;
    let high = parse_price(2, "high")?;
    let low = parse_price(3, "low")?;
    let close = parse_price(4, "close")?;
    if [open, high, low, close]
        .iter()
        .any(|value| !value.is_finite() || *value <= 0.0)
        || high < low
        || high < open.max(close)
        || low > open.min(close)
    {
        return Err(invalid(
            "OHLC values must be finite, positive, and consistent",
        ));
    }

    Ok(Candle {
        day_utc: timestamp.format("%Y-%m-%d").to_string(),
        open,
        high,
        low,
        close,
    })
}

/// Daily indicator values and the deterministic entry predicate.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Signal {
    pub sma50: f64,
    pub ema7: f64,
    pub rsi2: f64,
    pub adx2: f64,
    pub entry: bool,
}

/// Evaluates a signal from at least 60 closed daily candles, oldest first.
pub fn evaluate(closed: &[Candle]) -> Option<Signal> {
    if closed.len() < MIN_CLOSED_CANDLES {
        return None;
    }

    let closes = closed.iter().map(|candle| candle.close).collect::<Vec<_>>();
    let highs = closed.iter().map(|candle| candle.high).collect::<Vec<_>>();
    let lows = closed.iter().map(|candle| candle.low).collect::<Vec<_>>();
    let sma50 = sma_last(&closes, 50)?;
    let ema7 = ema_last(&closes, 7)?;
    let rsi2 = rsi_last(&closes, 2)?;
    let adx2 = adx_last(&highs, &lows, &closes, 2)?;
    let close = *closes.last()?;

    Some(Signal {
        sma50,
        ema7,
        rsi2,
        adx2,
        entry: close > sma50 && close > ema7 && rsi2 > adx2,
    })
}

/// Position state for the daily LONG-Up strategy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Position {
    Flat,
    LongUp { since_day: String },
}

/// Why the strategy held instead of changing position.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HoldReason {
    InsufficientHistory,
    NoSignal,
    StillLong,
}

/// Position decision returned by [`step`].
#[derive(Debug, Clone, PartialEq)]
pub enum Decision {
    EnterLongUp(Signal),
    ExitFlat(Signal),
    Hold(HoldReason),
}

/// Applies one daily signal to the current position without performing trades.
pub fn step(position: &Position, closed: &[Candle]) -> (Position, Decision) {
    let Some(signal) = evaluate(closed) else {
        return (
            position.clone(),
            Decision::Hold(HoldReason::InsufficientHistory),
        );
    };

    match position {
        Position::Flat if signal.entry => {
            let since_day = closed
                .last()
                .map(|candle| candle.day_utc.clone())
                .unwrap_or_default();
            (
                Position::LongUp { since_day },
                Decision::EnterLongUp(signal),
            )
        }
        Position::Flat => (Position::Flat, Decision::Hold(HoldReason::NoSignal)),
        Position::LongUp { .. } if signal.rsi2 < signal.adx2 => {
            (Position::Flat, Decision::ExitFlat(signal))
        }
        Position::LongUp { .. } => (position.clone(), Decision::Hold(HoldReason::StillLong)),
    }
}

/// Paper-trading realized PnL in raw price points; fees are not deducted.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Tracker {
    pub realized_pnl_pp: f64,
}

impl Tracker {
    /// Adds the caller-supplied exit-minus-entry reference-price difference.
    /// No fees, slippage, or funding costs are modeled.
    pub fn record_exit(&mut self, entry_ref: f64, exit_ref: f64) {
        self.realized_pnl_pp += exit_ref - entry_ref;
    }
}

/// Returns unrealized price-point PnL for a long position, or zero when flat.
pub fn mtm(position: &Position, current_ref: f64, entry_ref: f64) -> f64 {
    match position {
        Position::Flat => 0.0,
        Position::LongUp { .. } => current_ref - entry_ref,
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use chrono::{DateTime, Utc};

    use super::{
        Candle, DailyError, DailyFeed, Decision, HoldReason, Position, Tracker, backfill_daily,
        evaluate, mtm, step,
    };

    fn test_day(index: usize) -> String {
        format!("2025-{:02}-{:02}", index / 28 + 1, index % 28 + 1)
    }

    fn candles_from_closes(closes: impl IntoIterator<Item = f64>) -> Vec<Candle> {
        closes
            .into_iter()
            .enumerate()
            .map(|(index, close)| Candle {
                day_utc: test_day(index),
                open: close,
                high: close + 1.0,
                low: close - 1.0,
                close,
            })
            .collect()
    }

    fn rising_candles(count: usize) -> Vec<Candle> {
        let mut close = 100.0;
        let mut candles = Vec::with_capacity(count);
        for index in 0..count {
            if index > 0 {
                close += if index % 7 == 4 { -0.1 } else { 1.0 };
            }
            candles.push(Candle {
                day_utc: test_day(index),
                open: close,
                high: close + 1.0,
                low: close - 1.0,
                close,
            });
        }
        candles
    }

    fn falling_candles(count: usize) -> Vec<Candle> {
        let closes = (0..count).map(|index| 200.0 - index as f64);
        candles_from_closes(closes)
    }

    #[test]
    fn rising_trend_enters_and_falling_closes_exit_with_positive_realized_pnl() {
        let rising = rising_candles(70);
        let signal = evaluate(&rising).expect("enough valid candles");
        assert!(signal.entry, "expected an entry signal: {signal:?}");

        let (long, entry) = step(&Position::Flat, &rising);
        assert!(matches!(entry, Decision::EnterLongUp(_)));
        assert!(matches!(long, Position::LongUp { .. }));

        let falling = falling_candles(70);
        let exit_signal = evaluate(&falling).expect("enough valid candles");
        assert!(exit_signal.rsi2 < exit_signal.adx2);
        let (flat, exit) = step(&long, &falling);
        assert_eq!(flat, Position::Flat);
        assert!(matches!(exit, Decision::ExitFlat(_)));

        let mut tracker = Tracker::default();
        tracker.record_exit(0.40, 0.46);
        assert!((tracker.realized_pnl_pp - 0.06).abs() < 1e-12);
        assert!((mtm(&long, 0.46, 0.40) - 0.06).abs() < 1e-12);
        assert_eq!(mtm(&Position::Flat, 0.46, 0.40), 0.0);
    }

    #[test]
    fn sideways_chop_holds_without_a_signal() {
        let candles =
            candles_from_closes((0..60).map(|index| if index % 2 == 0 { 100.1 } else { 100.0 }));
        let (position, decision) = step(&Position::Flat, &candles);
        assert_eq!(position, Position::Flat);
        assert_eq!(decision, Decision::Hold(HoldReason::NoSignal));
    }

    #[test]
    fn fewer_than_sixty_closed_candles_holds_for_insufficient_history() {
        let candles = rising_candles(59);
        assert!(evaluate(&candles).is_none());
        let (position, decision) = step(&Position::Flat, &candles);
        assert_eq!(position, Position::Flat);
        assert_eq!(decision, Decision::Hold(HoldReason::InsufficientHistory));
    }

    #[test]
    fn invalid_prices_are_ignored_and_day_rollover_closes_the_previous_candle() {
        let mut feed = DailyFeed::default();
        let first_day = DateTime::parse_from_rfc3339("2025-01-01T12:00:00Z")
            .expect("valid timestamp")
            .timestamp_millis();
        let next_day = DateTime::parse_from_rfc3339("2025-01-02T00:00:00Z")
            .expect("valid timestamp")
            .timestamp_millis();

        feed.push_tick(first_day, 100.0);
        feed.push_tick(first_day + 1_000, f64::NAN);
        feed.push_tick(first_day + 2_000, 105.0);
        assert_eq!(
            feed.forming.as_ref().map(|candle| candle.close),
            Some(105.0)
        );
        assert_eq!(feed.forming.as_ref().map(|candle| candle.high), Some(105.0));

        feed.push_tick(next_day, 110.0);
        assert_eq!(feed.closed.len(), 1);
        assert_eq!(feed.closed[0].day_utc, "2025-01-01");
        assert_eq!(feed.closed[0].open, 100.0);
        assert_eq!(feed.closed[0].high, 105.0);
        assert_eq!(feed.closed[0].low, 100.0);
        assert_eq!(feed.closed[0].close, 105.0);
        assert_eq!(
            feed.forming.as_ref().map(|candle| candle.day_utc.as_str()),
            Some("2025-01-02")
        );
        assert_eq!(
            feed.forming.as_ref().map(|candle| candle.close),
            Some(110.0)
        );
    }

    #[tokio::test]
    async fn backfill_daily_fetches_real_binance_klines() {
        // The Binance endpoint was reachable via a five-second probe before
        // enabling this test. Keep the request bounded for offline CI changes.
        let result = tokio::time::timeout(Duration::from_secs(5), backfill_daily("BTC", 5))
            .await
            .expect("Binance daily backfill exceeded its five-second timeout")
            .expect("Binance daily backfill should return valid klines");
        assert!(!result.is_empty());
        let today_utc = Utc::now().format("%Y-%m-%d").to_string();
        assert!(result.iter().all(|candle| candle.day_utc < today_utc));
        assert!(result.iter().all(|candle| {
            candle.open.is_finite()
                && candle.high.is_finite()
                && candle.low.is_finite()
                && candle.close.is_finite()
        }));
    }

    #[tokio::test]
    async fn backfill_rejects_symbols_outside_btc_and_eth_without_network_access() {
        assert!(matches!(
            backfill_daily("SOL", 5).await,
            Err(DailyError::UnsupportedSymbol(_))
        ));
    }
}
