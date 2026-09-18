//! Polymarket public market-stream adapter.
//!
//! The SDK's WebSocket response types stop at this module. The stream exposed
//! here emits only [`MarketMessage`] values, so the actor and its consumers do
//! not depend on Polymarket SDK types.

use std::fmt::Display;
use std::pin::Pin;
use std::str::FromStr;
use std::task::{Context, Poll};
use std::time::Duration;

use futures_util::{Stream, StreamExt as _};
use thiserror::Error;
use tokio::sync::mpsc;

use crate::domain::{ConditionId, MarketId, PriceTicks, TickSize, TokenId, TradeSide};
use crate::engine::market_actor::{
    BestBidAskUpdate, BookDelta, BookSnapshot, LastTradePriceUpdate, MarketMessage,
    MarketResolvedUpdate, NewMarketUpdate, TickSizeUpdate,
};
use crate::polymarket::{BookSide, Level, OrderBook};

use polymarket_client_sdk_v2::clob::types::Side as SdkSide;
use polymarket_client_sdk_v2::clob::ws::{ChannelType, Client as SdkClient};
use polymarket_client_sdk_v2::types::U256;
use polymarket_client_sdk_v2::ws::config::Config as SdkWsConfig;

/// The SDK client starts the public CLOB market channel lazily at this endpoint.
pub const DEFAULT_MARKET_WS_ENDPOINT: &str = "wss://ws-subscriptions-clob.polymarket.com";

/// Errors raised while creating, decoding, or maintaining a market stream.
#[derive(Debug, Error)]
pub enum WsError {
    #[error("Polymarket WebSocket SDK error: {0}")]
    Sdk(String),
    #[error("market WebSocket endpoint must not be empty")]
    EmptyEndpoint,
    #[error("invalid token id `{0}`")]
    InvalidTokenId(String),
    #[error("YES and NO token ids must differ")]
    DuplicateTokenIds,
    #[error("failed to parse {field} value `{value}`")]
    ParseValue { field: &'static str, value: String },
    #[error("unsupported market WebSocket event: {0}")]
    UnsupportedEvent(&'static str),
    #[error("market WebSocket connection closed")]
    ConnectionClosed,
    #[error("market-message consumer dropped")]
    ConsumerDropped,
    #[error("market WebSocket reconnect budget exhausted after {attempts} retries")]
    ReconnectExhausted { attempts: u32 },
}

/// Runtime policy for the public market stream.
///
/// The SDK sends a text `PING` every `heartbeat_interval`; the default is the
/// required ten-second application heartbeat. The SDK's own reconnect attempt
/// is limited to one failure per client instance, while [`MarketStreamClient::run`]
/// owns the outer bounded retry loop. The default outer budget is five retries,
/// with exponential backoff from one second up to sixteen seconds.
#[derive(Debug, Clone)]
pub struct MarketStreamConfig {
    pub endpoint: String,
    pub heartbeat_interval: Duration,
    pub heartbeat_timeout: Duration,
    pub max_reconnect_attempts: u32,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
}

impl Default for MarketStreamConfig {
    fn default() -> Self {
        Self {
            endpoint: DEFAULT_MARKET_WS_ENDPOINT.to_owned(),
            heartbeat_interval: Duration::from_secs(10),
            heartbeat_timeout: Duration::from_secs(15),
            max_reconnect_attempts: 5,
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(16),
        }
    }
}

/// A provider-free stream of public market events for one binary market.
pub type MarketMessageStream = Pin<Box<dyn Stream<Item = Result<MarketMessage, WsError>> + Send>>;

/// Public market-stream client subscribed to the YES and NO assets.
#[derive(Debug, Clone)]
pub struct MarketStreamClient {
    token_ids: [TokenId; 2],
    asset_ids: [U256; 2],
    config: MarketStreamConfig,
}

impl MarketStreamClient {
    /// Creates a client with the default endpoint, heartbeat, and retry policy.
    pub fn new(yes_token_id: TokenId, no_token_id: TokenId) -> Result<Self, WsError> {
        Self::with_config(yes_token_id, no_token_id, MarketStreamConfig::default())
    }

    /// Creates a client with an explicit endpoint and bounded retry policy.
    pub fn with_config(
        yes_token_id: TokenId,
        no_token_id: TokenId,
        config: MarketStreamConfig,
    ) -> Result<Self, WsError> {
        if config.endpoint.trim().is_empty() {
            return Err(WsError::EmptyEndpoint);
        }

        let yes_asset_id = parse_asset_id(&yes_token_id)?;
        let no_asset_id = parse_asset_id(&no_token_id)?;
        if yes_asset_id == no_asset_id {
            return Err(WsError::DuplicateTokenIds);
        }

        Ok(Self {
            token_ids: [yes_token_id, no_token_id],
            asset_ids: [yes_asset_id, no_asset_id],
            config,
        })
    }

    /// Returns the configured YES token id.
    #[must_use]
    pub fn yes_token_id(&self) -> &TokenId {
        &self.token_ids[0]
    }

    /// Returns the configured NO token id.
    #[must_use]
    pub fn no_token_id(&self) -> &TokenId {
        &self.token_ids[1]
    }

    /// Opens one SDK-backed market stream and translates all received events.
    ///
    /// This method performs no network I/O until the returned stream is polled.
    /// Use [`Self::run`] when a bounded reconnect policy is required.
    pub fn subscribe(&self) -> Result<MarketStream, WsError> {
        let (client, translated) = self.open_sdk_subscription()?;

        // The SDK client owns the connection manager and must live as long as
        // the receiver stream. Keeping it in the stream wrapper prevents an
        // accidental disconnect when this method returns.
        Ok(MarketStream {
            client,
            inner: translated,
        })
    }

    /// Runs the market stream into a bounded actor channel.
    ///
    /// A disconnected SDK client is discarded and recreated after backoff. One
    /// initial attempt plus at most `max_reconnect_attempts` retries are made;
    /// [`WsError::ReconnectExhausted`]. The caller owns REST resynchronization
    /// after the actor reports a stale book.
    pub async fn run(&self, sender: mpsc::Sender<MarketMessage>) -> Result<(), WsError> {
        let mut retries = 0_u32;

        loop {
            let mut stream = self.subscribe()?;
            let mut connection_seen = false;
            let mut state_polls = 0_u8;
            let mut state_poll = tokio::time::interval(Duration::from_millis(100));
            // Tokio intervals tick immediately once; consume that tick so a
            // just-created SDK client has time to move out of Disconnected.
            state_poll.tick().await;

            let failure = loop {
                match tokio::time::timeout(Duration::from_millis(100), stream.next()).await {
                    Ok(Some(Ok(message))) => {
                        connection_seen = true;
                        sender
                            .send(message)
                            .await
                            .map_err(|_| WsError::ConsumerDropped)?;
                    }
                    Ok(Some(Err(error))) => break error,
                    Ok(None) => break WsError::ConnectionClosed,
                    Err(_) => {
                        state_poll.tick().await;
                        state_polls = state_polls.saturating_add(1);
                        if stream.is_connected() {
                            connection_seen = true;
                        } else if stream.is_disconnected() && (connection_seen || state_polls >= 3)
                        {
                            break WsError::ConnectionClosed;
                        }
                    }
                }
            };

            let _ = failure;
            retries = retries.saturating_add(1);
            if retries > self.config.max_reconnect_attempts {
                return Err(WsError::ReconnectExhausted { attempts: retries });
            }
            tokio::time::sleep(self.backoff_for(retries)).await;
        }
    }

    fn open_sdk_subscription(&self) -> Result<(SdkClient, MarketMessageStream), WsError> {
        let mut sdk_config = SdkWsConfig::default();
        sdk_config.heartbeat_interval = self.config.heartbeat_interval;
        sdk_config.heartbeat_timeout = self.config.heartbeat_timeout;
        sdk_config.reconnect.max_attempts = Some(1);
        sdk_config.reconnect.initial_backoff = self.config.initial_backoff;
        sdk_config.reconnect.max_backoff = self.config.max_backoff;

        let client = SdkClient::new(&self.config.endpoint, sdk_config).map_err(sdk_error)?;
        let assets = self.asset_ids.to_vec();
        let streams: Vec<MarketMessageStream> = vec![
            map_one(
                client
                    .subscribe_orderbook(assets.clone())
                    .map_err(sdk_error)?,
                translate_book_message,
            ),
            map_many(
                client.subscribe_prices(assets.clone()).map_err(sdk_error)?,
                translate_price_changes,
            ),
            map_one(
                client
                    .subscribe_last_trade_price(assets.clone())
                    .map_err(sdk_error)?,
                translate_last_trade_message,
            ),
            map_one(
                client
                    .subscribe_tick_size_change(assets.clone())
                    .map_err(sdk_error)?,
                translate_tick_size_message,
            ),
            map_one(
                client
                    .subscribe_best_bid_ask(assets.clone())
                    .map_err(sdk_error)?,
                translate_best_bid_ask_message,
            ),
            map_one(
                client
                    .subscribe_new_markets(assets.clone())
                    .map_err(sdk_error)?,
                translate_new_market_message,
            ),
            map_one(
                client
                    .subscribe_market_resolutions(assets)
                    .map_err(sdk_error)?,
                translate_market_resolved_message,
            ),
        ];

        Ok((client, Box::pin(futures_util::stream::select_all(streams))))
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

/// A translated SDK stream that keeps its client alive and exposes no SDK type.
pub struct MarketStream {
    client: SdkClient,
    inner: MarketMessageStream,
}

impl MarketStream {
    fn is_connected(&self) -> bool {
        self.client.is_connected(ChannelType::Market)
    }

    fn is_disconnected(&self) -> bool {
        self.client.connection_state(ChannelType::Market)
            == polymarket_client_sdk_v2::ws::connection::ConnectionState::Disconnected
    }
}

impl Stream for MarketStream {
    type Item = Result<MarketMessage, WsError>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(context)
    }
}

fn map_one<T, S>(
    stream: S,
    translate: fn(T) -> Result<MarketMessage, WsError>,
) -> MarketMessageStream
where
    S: Stream<Item = polymarket_client_sdk_v2::Result<T>> + Send + 'static,
    T: Send + 'static,
{
    Box::pin(stream.map(move |result| result.map_err(sdk_error).and_then(translate)))
}

fn map_many<T, S>(
    stream: S,
    translate: fn(T) -> Result<Vec<MarketMessage>, WsError>,
) -> MarketMessageStream
where
    S: Stream<Item = polymarket_client_sdk_v2::Result<T>> + Send + 'static,
    T: Send + 'static,
{
    Box::pin(stream.flat_map(move |result| {
        let messages: Vec<Result<MarketMessage, WsError>> = match result {
            Ok(value) => match translate(value) {
                Ok(messages) => messages.into_iter().map(Ok).collect(),
                Err(error) => vec![Err(error)],
            },
            Err(error) => vec![Err(sdk_error(error))],
        };
        futures_util::stream::iter(messages)
    }))
}

fn sdk_error(error: polymarket_client_sdk_v2::error::Error) -> WsError {
    WsError::Sdk(error.to_string())
}

fn translate_book_message(
    book: polymarket_client_sdk_v2::clob::ws::BookUpdate,
) -> Result<MarketMessage, WsError> {
    Ok(MarketMessage::BookSnapshot(translate_book(book)?))
}

fn translate_last_trade_message(
    last_trade: polymarket_client_sdk_v2::clob::ws::LastTradePrice,
) -> Result<MarketMessage, WsError> {
    Ok(MarketMessage::LastTradePrice(translate_last_trade(
        last_trade,
    )?))
}

fn translate_tick_size_message(
    tick_size: polymarket_client_sdk_v2::clob::ws::TickSizeChange,
) -> Result<MarketMessage, WsError> {
    Ok(MarketMessage::TickSizeChange(translate_tick_size(
        tick_size,
    )?))
}

fn translate_best_bid_ask_message(
    best_bid_ask: polymarket_client_sdk_v2::clob::ws::BestBidAsk,
) -> Result<MarketMessage, WsError> {
    Ok(MarketMessage::BestBidAsk(translate_best_bid_ask(
        best_bid_ask,
    )?))
}

fn translate_new_market_message(
    new_market: polymarket_client_sdk_v2::clob::ws::NewMarket,
) -> Result<MarketMessage, WsError> {
    Ok(MarketMessage::NewMarket(translate_new_market(new_market)))
}

fn translate_market_resolved_message(
    resolved: polymarket_client_sdk_v2::clob::ws::MarketResolved,
) -> Result<MarketMessage, WsError> {
    Ok(MarketMessage::MarketResolved(translate_market_resolved(
        resolved,
    )))
}

fn parse_asset_id(token_id: &TokenId) -> Result<U256, WsError> {
    U256::from_str(&token_id.0).map_err(|_| WsError::InvalidTokenId(token_id.0.clone()))
}

fn translate_book(
    book: polymarket_client_sdk_v2::clob::ws::BookUpdate,
) -> Result<BookSnapshot, WsError> {
    let condition_id = ConditionId(book.market.to_string());
    let token_id = TokenId(book.asset_id.to_string());
    let bids = translate_levels(book.bids, "book bid")?;
    let asks = translate_levels(book.asks, "book ask")?;

    // The local hash is the actor's provider-free integrity check. The SDK's
    // opaque venue hash is retained separately because SDK 0.8 does not expose
    // the server hash algorithm as part of BookUpdate.
    let mut expected = OrderBook::default();
    expected.apply_snapshot(bids.clone(), asks.clone());

    Ok(BookSnapshot {
        condition_id,
        token_id,
        sequence: None,
        bids,
        asks,
        book_hash: Some(expected.book_hash()),
        source_hash: book.hash,
    })
}

fn translate_levels(
    levels: Vec<polymarket_client_sdk_v2::clob::ws::types::response::OrderBookLevel>,
    field: &'static str,
) -> Result<Vec<Level>, WsError> {
    levels
        .into_iter()
        .map(|level| {
            Ok((
                decimal_to_price_ticks(level.price, field)?,
                decimal_to_base_units(level.size, field)?,
            ))
        })
        .collect()
}

fn translate_price_changes(
    price_change: polymarket_client_sdk_v2::clob::ws::PriceChange,
) -> Result<Vec<MarketMessage>, WsError> {
    let condition_id = ConditionId(price_change.market.to_string());
    price_change
        .price_changes
        .into_iter()
        .map(|change| {
            let quantity = change
                .size
                .map(|size| decimal_to_base_units(size, "price change size"))
                .transpose()?;
            Ok(MarketMessage::BookDelta(BookDelta {
                condition_id: condition_id.clone(),
                token_id: TokenId(change.asset_id.to_string()),
                sequence: None,
                side: translate_book_side(change.side)?,
                price: decimal_to_price_ticks(change.price, "price change price")?,
                quantity,
                book_hash: None,
                source_hash: change.hash,
            }))
        })
        .collect()
}

fn translate_last_trade(
    last_trade: polymarket_client_sdk_v2::clob::ws::LastTradePrice,
) -> Result<LastTradePriceUpdate, WsError> {
    Ok(LastTradePriceUpdate {
        condition_id: ConditionId(last_trade.market.to_string()),
        token_id: TokenId(last_trade.asset_id.to_string()),
        price: decimal_to_price_ticks(last_trade.price, "last trade price")?,
        side: last_trade.side.map(translate_trade_side).transpose()?,
        size: last_trade
            .size
            .map(|size| decimal_to_base_units(size, "last trade size"))
            .transpose()?,
        timestamp_ms: last_trade.timestamp,
    })
}

fn translate_tick_size(
    tick_size: polymarket_client_sdk_v2::clob::ws::TickSizeChange,
) -> Result<TickSizeUpdate, WsError> {
    Ok(TickSizeUpdate {
        condition_id: ConditionId(tick_size.market.to_string()),
        token_id: TokenId(tick_size.asset_id.to_string()),
        old_tick_size: decimal_to_tick_size(tick_size.old_tick_size, "old tick size")?,
        new_tick_size: decimal_to_tick_size(tick_size.new_tick_size, "new tick size")?,
        timestamp_ms: tick_size.timestamp,
    })
}

fn translate_best_bid_ask(
    best_bid_ask: polymarket_client_sdk_v2::clob::ws::BestBidAsk,
) -> Result<BestBidAskUpdate, WsError> {
    Ok(BestBidAskUpdate {
        condition_id: ConditionId(best_bid_ask.market.to_string()),
        token_id: TokenId(best_bid_ask.asset_id.to_string()),
        best_bid: decimal_to_price_ticks(best_bid_ask.best_bid, "best bid")?,
        best_ask: decimal_to_price_ticks(best_bid_ask.best_ask, "best ask")?,
        spread: decimal_to_price_ticks(best_bid_ask.spread, "spread")?,
        timestamp_ms: best_bid_ask.timestamp,
    })
}

fn translate_new_market(
    new_market: polymarket_client_sdk_v2::clob::ws::NewMarket,
) -> NewMarketUpdate {
    NewMarketUpdate {
        market_id: MarketId(new_market.id),
        condition_id: ConditionId(new_market.market.to_string()),
        question: new_market.question,
        slug: new_market.slug,
        description: new_market.description,
        token_ids: new_market
            .asset_ids
            .into_iter()
            .map(|asset_id| TokenId(asset_id.to_string()))
            .collect(),
        outcomes: new_market.outcomes,
        timestamp_ms: new_market.timestamp,
    }
}

fn translate_market_resolved(
    resolved: polymarket_client_sdk_v2::clob::ws::MarketResolved,
) -> MarketResolvedUpdate {
    MarketResolvedUpdate {
        market_id: MarketId(resolved.id),
        condition_id: ConditionId(resolved.market.to_string()),
        token_ids: resolved
            .asset_ids
            .into_iter()
            .map(|asset_id| TokenId(asset_id.to_string()))
            .collect(),
        winning_token_id: TokenId(resolved.winning_asset_id.to_string()),
        winning_outcome: resolved.winning_outcome,
        timestamp_ms: resolved.timestamp,
    }
}

fn translate_book_side(side: SdkSide) -> Result<BookSide, WsError> {
    match side {
        SdkSide::Buy => Ok(BookSide::Bid),
        SdkSide::Sell => Ok(BookSide::Ask),
        _ => Err(WsError::ParseValue {
            field: "price change side",
            value: "UNKNOWN".to_owned(),
        }),
    }
}

fn translate_trade_side(side: SdkSide) -> Result<TradeSide, WsError> {
    match side {
        SdkSide::Buy => Ok(TradeSide::Buy),
        SdkSide::Sell => Ok(TradeSide::Sell),
        _ => Err(WsError::ParseValue {
            field: "last trade side",
            value: "UNKNOWN".to_owned(),
        }),
    }
}

fn decimal_to_price_ticks(value: impl Display, field: &'static str) -> Result<PriceTicks, WsError> {
    let text = value.to_string();
    let number = text.parse::<f64>().map_err(|_| WsError::ParseValue {
        field,
        value: text.clone(),
    })?;
    if !number.is_finite() || !(0.0..=1.0).contains(&number) {
        return Err(WsError::ParseValue { field, value: text });
    }
    Ok(PriceTicks::from_f64(number))
}

fn decimal_to_tick_size(value: impl Display, field: &'static str) -> Result<TickSize, WsError> {
    let text = value.to_string();
    let number = text.parse::<f64>().map_err(|_| WsError::ParseValue {
        field,
        value: text.clone(),
    })?;
    if !number.is_finite() || !(0.0..=1.0).contains(&number) || number == 0.0 {
        return Err(WsError::ParseValue { field, value: text });
    }
    Ok(TickSize::from_f64(number))
}

fn decimal_to_base_units(value: impl Display, field: &'static str) -> Result<u64, WsError> {
    let text = value.to_string();
    let (whole, fraction) = text.split_once('.').unwrap_or((&text, ""));
    if whole.starts_with('-') || whole.is_empty() || !whole.chars().all(|c| c.is_ascii_digit()) {
        return Err(WsError::ParseValue { field, value: text });
    }
    if fraction.len() > 6 && fraction[6..].chars().any(|digit| digit != '0') {
        return Err(WsError::ParseValue { field, value: text });
    }
    if !fraction.chars().all(|c| c.is_ascii_digit()) {
        return Err(WsError::ParseValue { field, value: text });
    }

    let whole_units = whole
        .parse::<u64>()
        .ok()
        .and_then(|units| units.checked_mul(1_000_000));
    let fractional_text = format!("{fraction:0<6}");
    let fractional_units = fractional_text[..6].parse::<u64>().ok();
    whole_units
        .and_then(|units| fractional_units.and_then(|fraction| units.checked_add(fraction)))
        .ok_or(WsError::ParseValue { field, value: text })
}
