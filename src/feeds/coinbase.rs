//! Coinbase Exchange WebSocket adapter for BTC-USD.
//!
//! The feed subscribes to the public `ticker` and `matches` channels. Ticker
//! messages carry the best bid/ask and the latest trade, while match messages
//! carry only a trade. Missing quote fields are represented as `f64::NAN`.
//! The normalized `f64` values are statistical only; executable prices require
//! explicit quantization downstream.

use std::time::Duration;

use futures_util::{SinkExt as _, StreamExt as _};
use serde_json::Value;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use super::types::{FeedError, Venue, VenueTick, parse_rfc3339_millis};

/// Coinbase Exchange public WebSocket endpoint.
pub const DEFAULT_WS_URL: &str = "wss://ws-feed.exchange.coinbase.com";
/// Channels subscribed to by [`CoinbaseFeed`].
pub const SUBSCRIPTION_CHANNELS: [&str; 2] = ["ticker", "matches"];
const PRODUCT_ID: &str = "BTC-USD";

/// Runtime policy for Coinbase connection retries.
///
/// One initial connection plus at most `max_reconnect_attempts` retries are
/// made. The default retry backoff is one second, doubling to sixteen seconds,
/// matching the bounded discipline used by the Polymarket WebSocket adapter.
#[derive(Debug, Clone)]
pub struct CoinbaseFeedConfig {
    pub endpoint: String,
    pub max_reconnect_attempts: u32,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
}

impl Default for CoinbaseFeedConfig {
    fn default() -> Self {
        Self {
            endpoint: DEFAULT_WS_URL.to_owned(),
            max_reconnect_attempts: 5,
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(16),
        }
    }
}

/// Coinbase public feed normalized to [`VenueTick`].
#[derive(Debug, Clone)]
pub struct CoinbaseFeed {
    config: CoinbaseFeedConfig,
}

impl CoinbaseFeed {
    /// Creates a feed with the production Coinbase public endpoint and retry
    /// limits.
    #[must_use]
    pub fn new() -> Self {
        Self {
            config: CoinbaseFeedConfig::default(),
        }
    }

    /// Creates a feed with an explicit endpoint and bounded retry policy.
    pub fn with_config(config: CoinbaseFeedConfig) -> Result<Self, FeedError> {
        if config.endpoint.trim().is_empty() {
            return Err(FeedError::Connect(
                "Coinbase WebSocket URL is empty".to_owned(),
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
                    tracing::warn!("Coinbase feed disconnected; reconnecting");
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
                            FeedError::Connect("Coinbase tick consumer dropped".to_owned())
                        })?;
                    }
                }
                Message::Ping(payload) => socket
                    .send(Message::Pong(payload))
                    .await
                    .map_err(|error| FeedError::Connect(error.to_string()))?,
                Message::Close(_) => {
                    return Err(FeedError::Connect(
                        "Coinbase WebSocket closed by peer".to_owned(),
                    ));
                }
                Message::Binary(_) | Message::Pong(_) | Message::Frame(_) => {}
            }
        }

        Err(FeedError::Connect(
            "Coinbase WebSocket stream ended".to_owned(),
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

impl Default for CoinbaseFeed {
    fn default() -> Self {
        Self::new()
    }
}

fn subscription_message() -> String {
    serde_json::json!({
        "type": "subscribe",
        "product_ids": [PRODUCT_ID],
        "channels": SUBSCRIPTION_CHANNELS,
    })
    .to_string()
}

/// Parses one synthetic or live Coinbase WebSocket payload.
///
/// Subscription acknowledgements and unrelated control messages produce an
/// empty vector. A ticker or match payload produces one normalized tick.
pub fn parse_message(payload: &str) -> Result<Vec<VenueTick>, FeedError> {
    let value: Value = serde_json::from_str(payload)
        .map_err(|error| FeedError::Parse(format!("invalid Coinbase JSON: {error}")))?;
    let Some(message_type) = value.get("type").and_then(Value::as_str) else {
        return Ok(Vec::new());
    };

    match message_type {
        "ticker" => Ok(vec![parse_ticker(&value)?]),
        "match" | "last_match" => Ok(vec![parse_match(&value)?]),
        _ => Ok(Vec::new()),
    }
}

fn parse_ticker(value: &Value) -> Result<VenueTick, FeedError> {
    ensure_product(value)?;
    let price = number_field(value, "price")?;
    let bid = number_field(value, "best_bid")?;
    let ask = number_field(value, "best_ask")?;
    let trade_size = optional_number_field(value, "last_size")?.unwrap_or(0.0);
    let trade_side_buy = match value.get("side").and_then(Value::as_str) {
        Some("buy") => true,
        Some("sell") | None => false,
        Some(side) => {
            return Err(FeedError::Parse(format!(
                "unsupported Coinbase trade side `{side}`"
            )));
        }
    };
    let timestamp = timestamp_field(value)?;

    Ok(VenueTick {
        venue: Venue::Coinbase,
        symbol: PRODUCT_ID,
        price_f64: price,
        best_bid_f64: bid,
        best_ask_f64: ask,
        trade_size_f64: trade_size,
        trade_side_buy,
        ts_exchange_ms: timestamp,
        ts_local_ms: local_timestamp_ms(),
    })
}

fn parse_match(value: &Value) -> Result<VenueTick, FeedError> {
    ensure_product(value)?;
    let price = number_field(value, "price")?;
    let size = number_field(value, "size")?;
    let trade_side_buy = match value
        .get("side")
        .and_then(Value::as_str)
        .ok_or_else(|| FeedError::Parse("Coinbase match side is missing".to_owned()))?
    {
        "buy" => true,
        "sell" => false,
        side => {
            return Err(FeedError::Parse(format!(
                "unsupported Coinbase match side `{side}`"
            )));
        }
    };

    Ok(VenueTick {
        venue: Venue::Coinbase,
        symbol: PRODUCT_ID,
        price_f64: price,
        best_bid_f64: f64::NAN,
        best_ask_f64: f64::NAN,
        trade_size_f64: size,
        trade_side_buy,
        ts_exchange_ms: timestamp_field(value)?,
        ts_local_ms: local_timestamp_ms(),
    })
}

fn ensure_product(value: &Value) -> Result<(), FeedError> {
    let product = value
        .get("product_id")
        .and_then(Value::as_str)
        .ok_or_else(|| FeedError::Parse("Coinbase product_id is missing".to_owned()))?;
    if product != PRODUCT_ID {
        return Err(FeedError::Parse(format!(
            "unexpected Coinbase product `{product}`"
        )));
    }
    Ok(())
}

fn timestamp_field(value: &Value) -> Result<i64, FeedError> {
    let timestamp = value
        .get("time")
        .and_then(Value::as_str)
        .ok_or_else(|| FeedError::Parse("Coinbase timestamp is missing".to_owned()))?;
    parse_rfc3339_millis(timestamp)
}

fn number_field(value: &Value, field: &str) -> Result<f64, FeedError> {
    optional_number_field(value, field)?
        .ok_or_else(|| FeedError::Parse(format!("Coinbase field `{field}` is missing or null")))
}

fn optional_number_field(value: &Value, field: &str) -> Result<Option<f64>, FeedError> {
    let Some(raw) = value.get(field) else {
        return Ok(None);
    };
    if raw.is_null() {
        return Ok(None);
    }
    let number = match raw {
        Value::Number(number) => number
            .as_f64()
            .ok_or_else(|| FeedError::Parse(format!("Coinbase field `{field}` is not finite")))?,
        Value::String(number) => number
            .parse::<f64>()
            .map_err(|_| FeedError::Parse(format!("invalid Coinbase {field} value `{number}`")))?,
        _ => {
            return Err(FeedError::Parse(format!(
                "Coinbase field `{field}` is not numeric"
            )));
        }
    };
    if !number.is_finite() {
        return Err(FeedError::Parse(format!(
            "Coinbase field `{field}` is not finite"
        )));
    }
    Ok(Some(number))
}

fn local_timestamp_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::{PRODUCT_ID, parse_message};
    use crate::feeds::types::Venue;

    #[test]
    fn parses_ticker_payload() {
        let payload = r#"{
            "type":"ticker","sequence":123,"product_id":"BTC-USD",
            "price":"42000.25","open_24h":"41000.00","volume_24h":"12.3",
            "low_24h":"40000.00","high_24h":"43000.00","volume_30d":"100.0",
            "best_bid":"42000.20","best_ask":"42000.30","side":"buy",
            "time":"2024-01-02T03:04:05.678901Z","trade_id":456,"last_size":"0.25"
        }"#;

        let ticks = parse_message(payload).unwrap();
        assert_eq!(ticks.len(), 1);
        let tick = ticks[0];
        assert_eq!(tick.venue, Venue::Coinbase);
        assert_eq!(tick.symbol, PRODUCT_ID);
        assert_eq!(tick.price_f64, 42_000.25);
        assert_eq!(tick.best_bid_f64, 42_000.20);
        assert_eq!(tick.best_ask_f64, 42_000.30);
        assert_eq!(tick.trade_size_f64, 0.25);
        assert!(tick.trade_side_buy);
        assert_eq!(tick.ts_exchange_ms, 1_704_164_645_678);
    }

    #[test]
    fn parses_match_payload() {
        let payload = r#"{
            "type":"match","trade_id":10,"maker_order_id":"maker",
            "taker_order_id":"taker","side":"sell","size":"0.01000000",
            "price":"41999.75","product_id":"BTC-USD","sequence":11,
            "time":"2024-01-02T03:04:05.000Z"
        }"#;

        let tick = parse_message(payload).unwrap()[0];
        assert_eq!(tick.price_f64, 41_999.75);
        assert_eq!(tick.trade_size_f64, 0.01);
        assert!(!tick.trade_side_buy);
        assert_eq!(tick.ts_exchange_ms, 1_704_164_645_000);
        assert!(tick.best_bid_f64.is_nan());
    }
}
