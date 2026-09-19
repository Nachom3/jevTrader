use std::env;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use jevtrader::config::AppConfig;
use jevtrader::domain::{ConditionId, PriceTicks, TokenId, Trigger};
use jevtrader::engine::{
    BookSnapshot, ExecutionActor, MarketActor, MarketMessage, Pipeline, PipelineInput, SignalActor,
};
use jevtrader::feeds::{BinanceFeed, CoinbaseFeed, DeribitFeed, Venue, VenueTick};
use jevtrader::market_spec::MarketSpec;
use jevtrader::polymarket::{
    BASE_UNITS_PER_TOKEN, OrderBook, TopOfBookSnapshot, fetch_market_by_slug, fetch_top_of_book,
};
use jevtrader::state::feature_builder::{
    ExternalTick, OrderFlowAggregates, ResolutionContext, VenueMicroprices,
};
use jevtrader::strategy::risk::RiskLimits;
use polymarket_client_sdk_v2::clob::{Client as ClobClient, Config as ClobConfig};
use polymarket_client_sdk_v2::gamma::Client as GammaClient;
use tokio::sync::mpsc;

fn main() -> Result<()> {
    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(start())
}

async fn start() -> Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    match env::var("JEVTRADER_MARKET_FILE") {
        Ok(path) if !path.trim().is_empty() => {
            let config = AppConfig::load().map_err(anyhow::Error::new)?;
            let deadline = jev_deadline()?;
            let market = MarketSpec::load(&path)
                .map_err(anyhow::Error::new)
                .with_context(|| format!("loading market spec `{path}`"))?;
            run_shadow_paper(config, market, deadline).await
        }
        _ => startup_check().await,
    }
}

async fn startup_check() -> Result<()> {
    eprintln!("INFO jevtrader online: market engine scaffold ready!");
    let config = AppConfig::load().map_err(anyhow::Error::new)?;
    eprintln!(
        "INFO configuration validated and quote thresholds loaded: {:?}",
        config.quote_thresholds
    );
    eprintln!(
        "INFO Tokio runtime startup check complete; set JEVTRADER_MARKET_FILE to run shadow/paper"
    );
    Ok(())
}

/// Wires the existing public feed and Polymarket adapters into one paper-only
/// market loop. No private key is passed to an order client and no live order
/// path exists here.
async fn run_shadow_paper(config: AppConfig, market: MarketSpec, deadline: Duration) -> Result<()> {
    let size = parse_shadow_env::<u64>("JEVTRADER_MARKET_SIZE")?;

    let gamma = GammaClient::default();
    let metadata = fetch_market_by_slug(&gamma, &market.slug)
        .await
        .context("fetching Polymarket market metadata")?;
    let clob = ClobClient::new("https://clob-v2.polymarket.com", ClobConfig::default())
        .context("creating the public Polymarket CLOB client")?;
    let top = fetch_top_of_book(&clob, &metadata.yes_token_id)
        .await
        .context("fetching the initial Polymarket book")?;

    let (_market_sender, market_receiver) = mpsc::channel(1);
    let mut market_actor = MarketActor::new(metadata.yes_token_id.clone(), market_receiver);
    let mut initial_book = OrderBook::default();
    if let Some(best_bid) = top.best_bid {
        initial_book.apply_delta(
            jevtrader::polymarket::BookSide::Bid,
            best_bid,
            BASE_UNITS_PER_TOKEN,
        );
    }
    if let Some(best_ask) = top.best_ask {
        initial_book.apply_delta(
            jevtrader::polymarket::BookSide::Ask,
            best_ask,
            BASE_UNITS_PER_TOKEN,
        );
    }
    let initial_snapshot = BookSnapshot {
        condition_id: metadata.condition_id.clone(),
        token_id: metadata.yes_token_id.clone(),
        sequence: None,
        bids: initial_book.bids().to_vec(),
        asks: initial_book.asks().to_vec(),
        book_hash: Some(initial_book.book_hash()),
        source_hash: None,
    };
    market_actor.apply_message(MarketMessage::BookSnapshot(initial_snapshot));

    // The existing MarketActor owns the stream receiver and intentionally does
    // not expose it. This bounded shadow path therefore refreshes the same
    // actor-owned book through the existing public REST adapter; the WS adapter
    // remains a follow-up integration surface outside this task's allowed files.

    let (feed_sender, mut feed_receiver) = mpsc::channel::<VenueTick>(8192);
    let binance_sender = feed_sender.clone();
    tokio::spawn(async move {
        let _ = BinanceFeed::new().run(binance_sender).await;
    });
    let coinbase_sender = feed_sender.clone();
    tokio::spawn(async move {
        let _ = CoinbaseFeed::new().run(coinbase_sender).await;
    });
    let deribit_sender = feed_sender.clone();
    tokio::spawn(async move {
        let _ = DeribitFeed::new().run(deribit_sender).await;
    });
    drop(feed_sender);

    let (questdb, _questdb_task) =
        jevtrader::storage::QuestDbWriter::spawn(&config.questdb_ilp_addr, 4096);
    let signal_actor = SignalActor::from_freshness_policy(config.freshness_policy);
    let execution_actor = ExecutionActor::new(RiskLimits::from_freshness_policy(
        1,
        config.freshness_policy,
        false,
    ));
    let mut pipeline = Pipeline::new(
        signal_actor,
        execution_actor,
        questdb,
        config.typesafe_api_key,
        deadline,
        config.quote_thresholds,
        config.quant,
        512,
    );

    let initial_mid = coherent_mid(top.best_bid, top.best_ask);
    let mut last_trade_price = initial_mid.unwrap_or_else(|| PriceTicks::from_f64(0.0));
    let mut tick_size = top.tick_size;
    let mut external = ExternalState::new();
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    loop {
        interval.tick().await;
        while let Ok(tick) = feed_receiver.try_recv() {
            external.apply(tick);
        }

        let observed_at_ms = unix_time_ms();
        let mut book_is_stale = false;
        match fetch_top_of_book(&clob, &metadata.yes_token_id).await {
            Ok(top) => {
                tick_size = top.tick_size;
                let snapshot =
                    book_snapshot_from_top(&metadata.condition_id, &metadata.yes_token_id, &top);
                market_actor.apply_message(MarketMessage::BookSnapshot(snapshot));
                if let Some(mid) = coherent_mid(top.best_bid, top.best_ask) {
                    last_trade_price = mid;
                }
            }
            Err(_error) => {
                book_is_stale = true;
            }
        }
        let mut snapshot = market_actor.latest_snapshot();
        if book_is_stale {
            snapshot.stale = true;
            snapshot.book.mark_stale();
        }
        let resolution = ResolutionContext::new(
            market.target,
            market
                .resolution_at_ms
                .saturating_sub(observed_at_ms)
                .max(0) as u64
                / 1_000,
            market.resolution_source.clone(),
        );
        let order_flow = external.order_flow(observed_at_ms);
        let result = pipeline
            .run_step(PipelineInput {
                market_id: &metadata.market_id.0,
                condition_id: &metadata.condition_id.0,
                market_spec: &market,
                resolution,
                snapshot: snapshot.clone(),
                last_trade_price,
                tick_size,
                recent_ticks: &external.recent_ticks,
                venues: external.venues,
                order_flow,
                size,
                observed_at_ms,
                mid: coherent_mid(snapshot.book.best_bid(), snapshot.book.best_ask()),
                trigger: Trigger::PriceMove,
            })
            .await;
        let _ = result;
    }
}

/// The current feed adapters expose normalized ticks rather than an aggregate
/// actor. This small accumulator is the minimum bridge needed by the feature
/// builder; it does not change or reimplement any venue adapter.
struct ExternalState {
    recent_ticks: Vec<ExternalTick>,
    flow: Vec<(i64, f64, bool)>,
    venues: VenueMicroprices,
}

impl ExternalState {
    fn new() -> Self {
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

    fn apply(&mut self, tick: VenueTick) {
        let timestamp = if tick.ts_exchange_ms > 0 {
            tick.ts_exchange_ms
        } else {
            tick.ts_local_ms
        };
        let timestamp = timestamp.max(0);
        let price = tick.price_f64;
        if matches!(tick.venue, Venue::Binance | Venue::Coinbase)
            && price.is_finite()
            && price > 0.0
        {
            self.recent_ticks.push(ExternalTick {
                price,
                ts_ms: timestamp as u64,
            });
            self.recent_ticks.sort_unstable_by_key(|tick| tick.ts_ms);
            let oldest = timestamp.saturating_sub(300_000) as u64;
            self.recent_ticks.retain(|tick| tick.ts_ms >= oldest);
        }

        if tick.trade_size_f64.is_finite() && tick.trade_size_f64 > 0.0 {
            self.flow
                .push((timestamp, tick.trade_size_f64, tick.trade_side_buy));
            let oldest = timestamp.saturating_sub(5_000);
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
                Venue::Binance => self.venues.binance = microprice,
                Venue::Coinbase => self.venues.coinbase = microprice,
                Venue::Deribit => self.venues.perp = microprice,
            }
        }
    }

    fn order_flow(&self, at_ms: i64) -> OrderFlowAggregates {
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

fn book_snapshot_from_top(
    condition_id: &ConditionId,
    token_id: &TokenId,
    top: &TopOfBookSnapshot,
) -> BookSnapshot {
    let mut book = OrderBook::default();
    if let Some(best_bid) = top.best_bid {
        book.apply_delta(
            jevtrader::polymarket::BookSide::Bid,
            best_bid,
            BASE_UNITS_PER_TOKEN,
        );
    }
    if let Some(best_ask) = top.best_ask {
        book.apply_delta(
            jevtrader::polymarket::BookSide::Ask,
            best_ask,
            BASE_UNITS_PER_TOKEN,
        );
    }
    BookSnapshot {
        condition_id: condition_id.clone(),
        token_id: token_id.clone(),
        sequence: None,
        bids: book.bids().to_vec(),
        asks: book.asks().to_vec(),
        book_hash: Some(book.book_hash()),
        source_hash: None,
    }
}

fn coherent_mid(best_bid: Option<PriceTicks>, best_ask: Option<PriceTicks>) -> Option<PriceTicks> {
    let (best_bid, best_ask) = (best_bid?, best_ask?);
    (best_bid <= best_ask)
        .then(|| PriceTicks::from_f64((best_bid.to_f64() + best_ask.to_f64()) / 2.0))
}

fn jev_deadline() -> Result<Duration> {
    let millis = env::var("JEV_DEADLINE_MS")
        .ok()
        .map(|value| {
            value
                .parse::<u64>()
                .map_err(|_| anyhow!("JEV_DEADLINE_MS must be an unsigned integer"))
        })
        .transpose()?
        .unwrap_or(1_500);
    if millis == 0 {
        return Err(anyhow!("JEV_DEADLINE_MS must be greater than zero"));
    }
    Ok(Duration::from_millis(millis))
}

fn required_shadow_env(name: &'static str) -> Result<String> {
    let value = env::var(name).with_context(|| format!("missing {name} for shadow/paper mode"))?;
    if value.trim().is_empty() {
        return Err(anyhow!("{name} must not be empty in shadow/paper mode"));
    }
    Ok(value)
}

fn parse_shadow_env<T>(name: &'static str) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    let value = required_shadow_env(name)?;
    value
        .parse::<T>()
        .map_err(|error| anyhow!("invalid {name} value `{value}`: {error}"))
}

fn unix_time_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}
