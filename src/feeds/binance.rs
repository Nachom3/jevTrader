//! Binance spot WebSocket adapter for BTCUSDT.
//!
//! The adapter subscribes to the public `trade` and `bookTicker` streams. A
//! trade event has no quote attached, so its bid and ask are `f64::NAN`. A
//! book-ticker event uses the quote midpoint as `price_f64` and has no trade
//! size or trade direction. These values remain statistical until a downstream
//! boundary explicitly quantizes an executable price.

use std::time::Duration;

use futures_util::{SinkExt as _, StreamExt as _};
use serde_json::Value;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use super::types::{FeedError, Venue, VenueTick};

/// Binance combined-stream endpoint used by this feed.
pub const DEFAULT_WS_URL: &str = "wss://stream.binance.com:9443/ws";
/// Channels subscribed to by [`BinanceFeed`].
pub const SUBSCRIPTION_CHANNELS: [&str; 2] = ["btcusdt@trade", "btcusdt@bookTicker"];
const SYMBOL: &str = "BTCUSDT";

/// Runtime policy for Binance connection retries.
///
/// One initial connection plus at most `max_reconnect_attempts` retries are
/// made. The default retry backoff is one second, doubling to sixteen seconds,
/// matching the bounded discipline used by the Polymarket WebSocket adapter.
#[derive(Debug, Clone)]
pub struct BinanceFeedConfig {
    pub endpoint: String,
    pub max_reconnect_attempts: u32,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
}

impl Default for BinanceFeedConfig {
    fn default() -> Self {
        Self {
            endpoint: DEFAULT_WS_URL.to_owned(),
            max_reconnect_attempts: 5,
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(16),
        }
    }
}

/// Binance public spot feed normalized to [`VenueTick`].
#[derive(Debug, Clone)]
pub struct BinanceFeed {
    config: BinanceFeedConfig,
}

impl BinanceFeed {
    /// Creates a feed with the production Binance public endpoint and retry
    /// limits.
    #[must_use]
    pub fn new() -> Self {
        Self {
            config: BinanceFeedConfig::default(),
        }
    }

    /// Creates a feed with an explicit endpoint and bounded retry policy.
    pub fn with_config(config: BinanceFeedConfig) -> Result<Self, FeedError> {
        if config.endpoint.trim().is_empty() {
            return Err(FeedError::Connect(
                "Binance WebSocket URL is empty".to_owned(),
            ));
        }
        Ok(Self { config })
    }

    /// Runs the feed into a normalized tick channel until the retry budget is
    /// exhausted or a payload cannot be decoded.
    pub async fn run(&self, sender: mpsc::Sender<VenueTick>) -> Result<(), FeedError> {
        let mut retries = 0_u32;
        loop {
            match self.run_connection(&sender).await {
                Ok(()) => return Ok(()),
                Err(FeedError::Parse(error)) => return Err(FeedError::Parse(error)),
                Err(FeedError::Connect(_)) => {
                    retries = retries.saturating_add(1);
                    if retries > self.config.max_reconnect_attempts {
                        return Err(FeedError::Exhausted { attempts: retries });
                    }
                    tracing::warn!("Binance feed disconnected; reconnecting");
                    tokio::time::sleep(self.backoff_for(retries)).await;
                }
                Err(error @ FeedError::Exhausted { .. }) => return Err(error),
            }
        }
    }

    async fn run_connection(&self, sender: &mpsc::Sender<VenueTick>) -> Result<(), FeedError> {
        let (mut socket, _) = connect_async(&self.config.endpoint)
            .await
            .map_err(|error| FeedError::Connect(error.to_string()))?;
        socket
            .send(Message::Text(subscription_message().into()))
            .await
            .map_err(|error| FeedError::Connect(error.to_string()))?;

        while let Some(message) = socket.next().await {
            match message.map_err(|error| FeedError::Connect(error.to_string()))? {
                Message::Text(payload) => {
                    for tick in parse_message(payload.as_ref())? {
                        sender.send(tick).await.map_err(|_| {
                            FeedError::Connect("Binance tick consumer dropped".to_owned())
                        })?;
                    }
                }
                Message::Ping(payload) => socket
                    .send(Message::Pong(payload))
                    .await
                    .map_err(|error| FeedError::Connect(error.to_string()))?,
                Message::Close(_) => {
                    return Err(FeedError::Connect(
                        "Binance WebSocket closed by peer".to_owned(),
                    ));
                }
                Message::Binary(_) | Message::Pong(_) | Message::Frame(_) => {}
            }
        }

        Err(FeedError::Connect(
            "Binance WebSocket stream ended".to_owned(),
        ))
    }

    fn backoff_for(&self, retry: u32) -> Duration {
        let mut delay = self.config.initial_backoff;
        for _ in 1..retry {
            delay = delay.checked_mul(2).unwrap_or(self.config.max_backoff);
            if delay >= self.config.max_backoff {
                return self.config.max_backoff;
            }
        }
        delay.min(self.config.max_backoff)
    }
}

impl Default for BinanceFeed {
    fn default() -> Self {
        Self::new()
    }
}

fn subscription_message() -> String {
    serde_json::json!({
        "method": "SUBSCRIBE",
        "params": SUBSCRIPTION_CHANNELS,
        "id": 1,
    })
    .to_string()
}

/// Parses one synthetic or live Binance WebSocket payload.
///
/// Subscription acknowledgements and unrelated control messages produce an
/// empty vector. Relevant trade and book-ticker payloads produce one tick.
pub fn parse_message(payload: &str) -> Result<Vec<VenueTick>, FeedError> {
    let value: Value = serde_json::from_str(payload)
        .map_err(|error| FeedError::Parse(format!("invalid Binance JSON: {error}")))?;
    match value.get("e").and_then(Value::as_str) {
        Some("trade") => Ok(vec![parse_trade(&value)?]),
        Some("bookTicker") => Ok(vec![parse_book_ticker(&value)?]),
        Some(_) => Ok(Vec::new()),
        None if value.get("s").is_some()
            && value.get("b").is_some()
            && value.get("a").is_some() =>
        {
            Ok(vec![parse_book_ticker(&value)?])
        }
        None => Ok(Vec::new()),
    }
}

fn parse_trade(value: &Value) -> Result<VenueTick, FeedError> {
    ensure_symbol(value)?;
    let price = number_field(value, "p")?;
    let size = number_field(value, "q")?;
    let exchange_timestamp = integer_field(value, "T")?;
    let buyer_is_maker = value
        .get("m")
        .and_then(Value::as_bool)
        .ok_or_else(|| FeedError::Parse("Binance trade field `m` is missing".to_owned()))?;

    Ok(VenueTick {
        venue: Venue::Binance,
        symbol: SYMBOL,
        price_f64: price,
        best_bid_f64: f64::NAN,
        best_ask_f64: f64::NAN,
        trade_size_f64: size,
        trade_side_buy: !buyer_is_maker,
        ts_exchange_ms: exchange_timestamp,
        ts_local_ms: local_timestamp_ms(),
    })
}

fn parse_book_ticker(value: &Value) -> Result<VenueTick, FeedError> {
    ensure_symbol(value)?;
    let bid = number_field(value, "b")?;
    let ask = number_field(value, "a")?;
    // The raw Binance bookTicker stream omits an exchange timestamp. Keep
    // zero as the explicit unavailable sentinel and retain local arrival time;
    // event-shaped payloads use T/E when Binance includes either field.
    let exchange_timestamp = value
        .get("T")
        .or_else(|| value.get("E"))
        .map(|timestamp| integer_value(timestamp, "bookTicker timestamp"))
        .transpose()?
        .unwrap_or(0);

    Ok(VenueTick {
        venue: Venue::Binance,
        symbol: SYMBOL,
        price_f64: (bid + ask) / 2.0,
        best_bid_f64: bid,
        best_ask_f64: ask,
        trade_size_f64: 0.0,
        trade_side_buy: false,
        ts_exchange_ms: exchange_timestamp,
        ts_local_ms: local_timestamp_ms(),
    })
}

fn ensure_symbol(value: &Value) -> Result<(), FeedError> {
    let symbol = value
        .get("s")
        .and_then(Value::as_str)
        .ok_or_else(|| FeedError::Parse("Binance symbol is missing".to_owned()))?;
    if symbol != SYMBOL {
        return Err(FeedError::Parse(format!(
            "unexpected Binance symbol `{symbol}`"
        )));
    }
    Ok(())
}

fn number_field(value: &Value, field: &str) -> Result<f64, FeedError> {
    let raw = value
        .get(field)
        .ok_or_else(|| FeedError::Parse(format!("Binance field `{field}` is missing")))?;
    let number = match raw {
        Value::Number(number) => number
            .as_f64()
            .ok_or_else(|| FeedError::Parse(format!("Binance field `{field}` is not finite")))?,
        Value::String(number) => number
            .parse::<f64>()
            .map_err(|_| FeedError::Parse(format!("invalid Binance {field} value `{number}`")))?,
        _ => {
            return Err(FeedError::Parse(format!(
                "Binance field `{field}` is not numeric"
            )));
        }
    };
    if !number.is_finite() {
        return Err(FeedError::Parse(format!(
            "Binance field `{field}` is not finite"
        )));
    }
    Ok(number)
}

fn integer_field(value: &Value, field: &str) -> Result<i64, FeedError> {
    let raw = value
        .get(field)
        .ok_or_else(|| FeedError::Parse(format!("Binance field `{field}` is missing")))?;
    integer_value(raw, field)
}

fn integer_value(value: &Value, field: &str) -> Result<i64, FeedError> {
    match value {
        Value::Number(number) => number
            .as_i64()
            .ok_or_else(|| FeedError::Parse(format!("Binance {field} is not an integer"))),
        Value::String(number) => number
            .parse::<i64>()
            .map_err(|_| FeedError::Parse(format!("invalid Binance {field} `{number}`"))),
        _ => Err(FeedError::Parse(format!(
            "Binance {field} is not an integer"
        ))),
    }
}

fn local_timestamp_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::{SYMBOL, parse_message};
    use crate::feeds::types::Venue;

    #[test]
    fn parses_trade_payload() {
        let payload = r#"{
            "e":"trade","E":1700000000123,"s":"BTCUSDT","t":123,
            "p":"42000.12500000","q":"0.01000000","T":1700000000456,
            "m":false,"M":true
        }"#;

        let ticks = parse_message(payload).unwrap();
        assert_eq!(ticks.len(), 1);
        let tick = ticks[0];
        assert_eq!(tick.venue, Venue::Binance);
        assert_eq!(tick.symbol, SYMBOL);
        assert_eq!(tick.price_f64, 42_000.125);
        assert_eq!(tick.trade_size_f64, 0.01);
        assert!(tick.trade_side_buy);
        assert_eq!(tick.ts_exchange_ms, 1_700_000_000_456);
        assert!(tick.best_bid_f64.is_nan());
    }

    #[test]
    fn parses_book_ticker_payload() {
        let payload = r#"{
            "u":400900217,"s":"BTCUSDT","b":"41999.90","B":"1.2",
            "a":"42000.10","A":"0.8","T":1700000000789,"E":1700000000790
        }"#;

        let ticks = parse_message(payload).unwrap();
        let tick = ticks[0];
        assert_eq!(tick.price_f64, 42_000.0);
        assert_eq!(tick.best_bid_f64, 41_999.90);
        assert_eq!(tick.best_ask_f64, 42_000.10);
        assert_eq!(tick.trade_size_f64, 0.0);
        assert!(!tick.trade_side_buy);
        assert_eq!(tick.ts_exchange_ms, 1_700_000_000_789);
    }
}
