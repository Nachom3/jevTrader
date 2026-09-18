//! Deribit public WebSocket adapter for BTC-PERPETUAL.
//!
//! The feed subscribes to `ticker.BTC-PERPETUAL` and
//! `trades.BTC-PERPETUAL`. Prices are normalized as perp prices. Any spot/perp
//! basis calculation belongs downstream and is intentionally not performed in
//! this adapter. As with the other feeds, normalized `f64` values are
//! statistical only and require explicit quantization before execution.

use std::time::Duration;

use futures_util::{SinkExt as _, StreamExt as _};
use serde_json::Value;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use super::types::{FeedError, Venue, VenueTick};

/// Deribit public WebSocket API endpoint.
pub const DEFAULT_WS_URL: &str = "wss://www.deribit.com/ws/api/v2";
/// Channels subscribed to by [`DeribitFeed`].
pub const SUBSCRIPTION_CHANNELS: [&str; 2] = ["ticker.BTC-PERPETUAL", "trades.BTC-PERPETUAL"];
const INSTRUMENT: &str = "BTC-PERPETUAL";

/// Runtime policy for Deribit connection retries.
///
/// One initial connection plus at most `max_reconnect_attempts` retries are
/// made. The default retry backoff is one second, doubling to sixteen seconds,
/// matching the bounded discipline used by the Polymarket WebSocket adapter.
#[derive(Debug, Clone)]
pub struct DeribitFeedConfig {
    pub endpoint: String,
    pub max_reconnect_attempts: u32,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
}

impl Default for DeribitFeedConfig {
    fn default() -> Self {
        Self {
            endpoint: DEFAULT_WS_URL.to_owned(),
            max_reconnect_attempts: 5,
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(16),
        }
    }
}

/// Deribit public feed normalized to [`VenueTick`].
#[derive(Debug, Clone)]
pub struct DeribitFeed {
    config: DeribitFeedConfig,
}

impl DeribitFeed {
    /// Creates a feed with the production Deribit public endpoint and retry
    /// limits.
    #[must_use]
    pub fn new() -> Self {
        Self {
            config: DeribitFeedConfig::default(),
        }
    }

    /// Creates a feed with an explicit endpoint and bounded retry policy.
    pub fn with_config(config: DeribitFeedConfig) -> Result<Self, FeedError> {
        if config.endpoint.trim().is_empty() {
            return Err(FeedError::Connect(
                "Deribit WebSocket URL is empty".to_owned(),
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
                    tracing::warn!("Deribit feed disconnected; reconnecting");
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
                            FeedError::Connect("Deribit tick consumer dropped".to_owned())
                        })?;
                    }
                }
                Message::Ping(payload) => socket
                    .send(Message::Pong(payload))
                    .await
                    .map_err(|error| FeedError::Connect(error.to_string()))?,
                Message::Close(_) => {
                    return Err(FeedError::Connect(
                        "Deribit WebSocket closed by peer".to_owned(),
                    ));
                }
                Message::Binary(_) | Message::Pong(_) | Message::Frame(_) => {}
            }
        }

        Err(FeedError::Connect(
            "Deribit WebSocket stream ended".to_owned(),
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

impl Default for DeribitFeed {
    fn default() -> Self {
        Self::new()
    }
}

fn subscription_message() -> String {
    serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "public/subscribe",
        "params": { "channels": SUBSCRIPTION_CHANNELS },
    })
    .to_string()
}

/// Parses one synthetic or live Deribit WebSocket payload.
///
/// JSON-RPC responses and unrelated control messages produce an empty vector.
/// A ticker payload produces one tick; a trade notification may produce one
/// tick per item in its `data` array.
pub fn parse_message(payload: &str) -> Result<Vec<VenueTick>, FeedError> {
    let value: Value = serde_json::from_str(payload)
        .map_err(|error| FeedError::Parse(format!("invalid Deribit JSON: {error}")))?;
    if value.get("method").and_then(Value::as_str) != Some("subscription") {
        return Ok(Vec::new());
    }

    let params = value
        .get("params")
        .ok_or_else(|| FeedError::Parse("Deribit subscription params are missing".to_owned()))?;
    let channel = params
        .get("channel")
        .and_then(Value::as_str)
        .ok_or_else(|| FeedError::Parse("Deribit subscription channel is missing".to_owned()))?;
    let data = params
        .get("data")
        .ok_or_else(|| FeedError::Parse("Deribit subscription data is missing".to_owned()))?;

    if channel == "ticker.BTC-PERPETUAL" {
        return Ok(vec![parse_ticker(data)?]);
    }
    if channel == "trades.BTC-PERPETUAL" {
        let trades = data
            .as_array()
            .ok_or_else(|| FeedError::Parse("Deribit trades data is not an array".to_owned()))?;
        return trades.iter().map(parse_trade).collect();
    }

    Ok(Vec::new())
}

fn parse_ticker(value: &Value) -> Result<VenueTick, FeedError> {
    ensure_instrument(value)?;
    let best_bid = optional_number_field(value, "best_bid_price")?.unwrap_or(f64::NAN);
    let best_ask = optional_number_field(value, "best_ask_price")?.unwrap_or(f64::NAN);
    let price = number_field(value, "last_price")?;

    Ok(VenueTick {
        venue: Venue::Deribit,
        symbol: INSTRUMENT,
        price_f64: price,
        best_bid_f64: best_bid,
        best_ask_f64: best_ask,
        trade_size_f64: 0.0,
        trade_side_buy: false,
        ts_exchange_ms: integer_field(value, "timestamp")?,
        ts_local_ms: local_timestamp_ms(),
    })
}

fn parse_trade(value: &Value) -> Result<VenueTick, FeedError> {
    ensure_instrument(value)?;
    let trade_side_buy = match value
        .get("direction")
        .and_then(Value::as_str)
        .ok_or_else(|| FeedError::Parse("Deribit trade direction is missing".to_owned()))?
    {
        "buy" => true,
        "sell" => false,
        direction => {
            return Err(FeedError::Parse(format!(
                "unsupported Deribit trade direction `{direction}`"
            )));
        }
    };

    Ok(VenueTick {
        venue: Venue::Deribit,
        symbol: INSTRUMENT,
        price_f64: number_field(value, "price")?,
        best_bid_f64: f64::NAN,
        best_ask_f64: f64::NAN,
        trade_size_f64: number_field(value, "amount")?,
        trade_side_buy,
        ts_exchange_ms: integer_field(value, "timestamp")?,
        ts_local_ms: local_timestamp_ms(),
    })
}

fn ensure_instrument(value: &Value) -> Result<(), FeedError> {
    let instrument = value
        .get("instrument_name")
        .and_then(Value::as_str)
        .ok_or_else(|| FeedError::Parse("Deribit instrument_name is missing".to_owned()))?;
    if instrument != INSTRUMENT {
        return Err(FeedError::Parse(format!(
            "unexpected Deribit instrument `{instrument}`"
        )));
    }
    Ok(())
}

fn number_field(value: &Value, field: &str) -> Result<f64, FeedError> {
    optional_number_field(value, field)?
        .ok_or_else(|| FeedError::Parse(format!("Deribit field `{field}` is missing or null")))
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
            .ok_or_else(|| FeedError::Parse(format!("Deribit field `{field}` is not finite")))?,
        Value::String(number) => number
            .parse::<f64>()
            .map_err(|_| FeedError::Parse(format!("invalid Deribit {field} value `{number}`")))?,
        _ => {
            return Err(FeedError::Parse(format!(
                "Deribit field `{field}` is not numeric"
            )));
        }
    };
    if !number.is_finite() {
        return Err(FeedError::Parse(format!(
            "Deribit field `{field}` is not finite"
        )));
    }
    Ok(Some(number))
}

fn integer_field(value: &Value, field: &str) -> Result<i64, FeedError> {
    let raw = value
        .get(field)
        .ok_or_else(|| FeedError::Parse(format!("Deribit field `{field}` is missing")))?;
    match raw {
        Value::Number(number) => number
            .as_i64()
            .ok_or_else(|| FeedError::Parse(format!("Deribit {field} is not an integer"))),
        Value::String(number) => number
            .parse::<i64>()
            .map_err(|_| FeedError::Parse(format!("invalid Deribit {field} `{number}`"))),
        _ => Err(FeedError::Parse(format!(
            "Deribit {field} is not an integer"
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
    use super::{INSTRUMENT, parse_message};
    use crate::feeds::types::Venue;

    #[test]
    fn parses_ticker_payload() {
        let payload = r#"{
            "jsonrpc":"2.0","method":"subscription","params":{
                "channel":"ticker.BTC-PERPETUAL","data":{
                    "timestamp":1700000000123,"stats":{"volume_usd":1},
                    "state":"open","last_price":42010.5,"mark_price":42011.0,
                    "index_price":42000.0,"best_bid_price":42010.0,
                    "best_ask_price":42011.0,"instrument_name":"BTC-PERPETUAL"
                }
            }
        }"#;

        let ticks = parse_message(payload).unwrap();
        assert_eq!(ticks.len(), 1);
        let tick = ticks[0];
        assert_eq!(tick.venue, Venue::Deribit);
        assert_eq!(tick.symbol, INSTRUMENT);
        assert_eq!(tick.price_f64, 42_010.5);
        assert_eq!(tick.best_bid_f64, 42_010.0);
        assert_eq!(tick.best_ask_f64, 42_011.0);
        assert_eq!(tick.ts_exchange_ms, 1_700_000_000_123);
    }

    #[test]
    fn parses_trade_payload() {
        let payload = r#"{
            "jsonrpc":"2.0","method":"subscription","params":{
                "channel":"trades.BTC-PERPETUAL","data":[{
                    "trade_seq":123,"trade_id":"abc","timestamp":1700000000456,
                    "tick_direction":0,"price":41999.75,"mark_price":42000.0,
                    "instrument_name":"BTC-PERPETUAL","amount":10.0,
                    "direction":"buy","liquidation":"none"
                }]
            }
        }"#;

        let tick = parse_message(payload).unwrap()[0];
        assert_eq!(tick.price_f64, 41_999.75);
        assert_eq!(tick.trade_size_f64, 10.0);
        assert!(tick.trade_side_buy);
        assert_eq!(tick.ts_exchange_ms, 1_700_000_000_456);
        assert!(tick.best_bid_f64.is_nan());
    }
}
