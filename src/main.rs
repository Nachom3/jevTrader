use std::env;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use jevtrader::config::AppConfig;
use jevtrader::domain::{Asset, ConditionId, MarketKey, PriceTicks, TickSize, TokenId, Trigger};
use jevtrader::engine::{
    BookSnapshot, ExecutionActor, MarketActor, MarketMessage, MarketRegistry, Pipeline,
    PipelineInput, SignalActor,
};
use jevtrader::feeds::{BinanceFeed, CoinbaseFeed, DeribitFeed, SharedFeeds, VenueTick};
use jevtrader::market_spec::MarketSpec;
use jevtrader::polymarket::{
    BASE_UNITS_PER_TOKEN, MarketMetadata, OrderBook, TopOfBookSnapshot, fetch_market_by_slug,
    fetch_top_of_book,
};
use jevtrader::state::feature_builder::{ContractContext, ResolutionContext};
use jevtrader::storage::{QuestDbHandle, QuestDbWriter};
use jevtrader::strategy::risk::RiskLimits;
use polymarket_client_sdk_v2::clob::{Client as ClobClient, Config as ClobConfig};
use polymarket_client_sdk_v2::gamma::Client as GammaClient;
use tokio::sync::mpsc;

fn main() -> Result<()> {
    // Both `ring` and `aws-lc-rs` end up compiled in via transitive TLS
    // features, so rustls cannot auto-pick a process CryptoProvider and the
    // first TLS use panics. Pin `ring` explicitly before any worker spawns.
    let _ = rustls::crypto::ring::default_provider().install_default();
    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(start())
}

async fn start() -> Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let paths = configured_market_paths()?;
    if paths.is_empty() {
        startup_check().await
    } else {
        let config = AppConfig::load().map_err(anyhow::Error::new)?;
        let deadline = jev_deadline()?;
        let mut specs = Vec::with_capacity(paths.len());
        for path in paths {
            let spec = MarketSpec::load(&path)
                .map_err(anyhow::Error::new)
                .with_context(|| format!("loading market spec `{path}`"))?;
            specs.push(spec);
        }
        run_multi_market(config, specs, deadline).await
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
        "INFO Tokio runtime startup check complete; set JEVTRADER_MARKET_FILE or JEVTRADER_MARKET_FILES to run shadow/paper"
    );
    Ok(())
}

/// Reads the plural market configuration first, retaining the singular
/// variable as a compatibility fallback for the original single-market mode.
fn configured_market_paths() -> Result<Vec<String>> {
    let mut paths = env::var("JEVTRADER_MARKET_FILES")
        .ok()
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|path| !path.is_empty())
                .map(str::to_owned)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    if paths.is_empty()
        && let Ok(path) = env::var("JEVTRADER_MARKET_FILE")
        && !path.trim().is_empty()
    {
        paths.push(path);
    }

    if paths.len() > 8 {
        return Err(anyhow!(
            "JEVTRADER_MARKET_FILES supports at most 8 market specs; got {}",
            paths.len()
        ));
    }
    Ok(paths)
}

/// Wires the public feeds and Polymarket adapters into one paper-only loop for
/// every configured contract. There is one stateful [`Pipeline`] per market.
async fn run_multi_market(
    config: AppConfig,
    specs: Vec<MarketSpec>,
    deadline: Duration,
) -> Result<()> {
    let gamma = GammaClient::default();
    let clob = ClobClient::new("https://clob.polymarket.com", ClobConfig::default())
        .context("creating the public Polymarket CLOB client")?;
    let (questdb, _questdb_task) = QuestDbWriter::spawn(&config.questdb_ilp_addr, 4096);
    let run_id = env::var("JEVTRADER_RUN_ID")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| format!("run-{}", unix_time_ms()));
    let size = parse_shadow_env::<u64>("JEVTRADER_MARKET_SIZE")?;

    let mut registry = MarketRegistry::new();
    let mut markets = Vec::with_capacity(specs.len());
    for spec in specs {
        let metadata = fetch_market_by_slug(&gamma, &spec.slug)
            .await
            .with_context(|| format!("fetching Polymarket market metadata for `{}`", spec.slug))?;
        let key = spec
            .market_key()
            .ok_or_else(|| anyhow!("market spec `{}` has no supported market key", spec.slug))?;
        registry
            .register(
                spec.clone(),
                metadata.condition_id.0.clone(),
                metadata.yes_token_id.0.clone(),
            )
            .map_err(anyhow::Error::new)?;

        let registered = registry
            .get(&key)
            .expect("a market is present immediately after registration");
        let runtime_spec = registered.spec.clone();
        let condition_id = ConditionId(registered.condition_id.clone());
        let yes_token_id = TokenId(registered.yes_token_id.clone());
        let market_id = runtime_spec
            .market_key()
            .map(MarketKey::market_id)
            .unwrap_or_else(|| metadata.market_id.0.clone());
        markets.push(
            build_market_runtime(
                &run_id,
                &config,
                deadline,
                questdb.clone(),
                runtime_spec,
                market_id,
                metadata,
                condition_id,
                yes_token_id,
                &clob,
            )
            .await?,
        );
    }

    let (feed_sender, mut feed_receiver) = mpsc::channel::<VenueTick>(8192);
    spawn_feeds(feed_sender.clone());
    drop(feed_sender);
    let mut shared_feeds = SharedFeeds::new();
    let mut interval = tokio::time::interval(Duration::from_secs(1));

    // This is intentionally a sequential modular monolith. Real parallelism
    // per market runtime is a follow-up once its scheduling and rate limits
    // are specified; one shared loop keeps V1 ordering deterministic today.
    loop {
        interval.tick().await;
        while let Ok(tick) = feed_receiver.try_recv() {
            shared_feeds.apply(tick);
        }

        let observed_at_ms = unix_time_ms();
        for market in &mut markets {
            let remaining_secs = market.spec.time_remaining_secs(observed_at_ms);
            let tradable = {
                let registered = registry
                    .get_mut(&market.key)
                    .expect("runtime and registry must have the same market keys");
                registered.lifecycle = registered.lifecycle.advance(remaining_secs);
                registered.is_tradable()
            };
            if !tradable {
                continue;
            }

            let fresh = refresh_market_book(market, &clob).await;
            let mut snapshot = market.actor.latest_snapshot();
            if !fresh {
                // Staleness belongs to this runtime only; no other market's
                // decision is blocked by a REST failure here.
                snapshot.stale = true;
                snapshot.book.mark_stale();
            }

            let (asset, horizon) = match (market.spec.asset(), market.spec.horizon()) {
                (Some(asset), Some(horizon)) => (asset, horizon),
                _ => continue,
            };
            let lane = shared_feeds.lane(asset);
            let resolution = ResolutionContext::new(
                market.spec.target,
                remaining_secs,
                market.spec.resolution_source.clone(),
            );
            let contract =
                ContractContext::from_parts(asset.as_str(), horizon.as_str(), horizon.seconds());
            tracing::trace!(
                market = %market.market_id,
                asset = %contract.asset_symbol,
                horizon = %contract.horizon_label,
                "building contract context"
            );
            let mid = coherent_mid(snapshot.book.best_bid(), snapshot.book.best_ask());
            market
                .pipeline
                .run_step(PipelineInput {
                    market_id: &market.market_id,
                    condition_id: &market.condition_id.0,
                    market_spec: &market.spec,
                    resolution,
                    snapshot,
                    last_trade_price: market.last_trade_price,
                    tick_size: market.tick_size,
                    recent_ticks: lane.recent_ticks(),
                    venues: lane.venues(),
                    order_flow: lane.order_flow(observed_at_ms),
                    size,
                    observed_at_ms,
                    mid,
                    trigger: Trigger::PriceMove,
                })
                .await;
        }
    }
}

/// The six public feed connections share one bounded normalized tick channel.
fn spawn_feeds(sender: mpsc::Sender<VenueTick>) {
    for asset in Asset::ALL {
        let feed_sender = sender.clone();
        tokio::spawn(async move {
            if let Err(error) = BinanceFeed::for_asset(asset).run(feed_sender).await {
                tracing::warn!(?asset, %error, "Binance feed stopped");
            }
        });

        let feed_sender = sender.clone();
        tokio::spawn(async move {
            if let Err(error) = CoinbaseFeed::for_asset(asset).run(feed_sender).await {
                tracing::warn!(?asset, %error, "Coinbase feed stopped");
            }
        });

        let feed_sender = sender.clone();
        tokio::spawn(async move {
            if let Err(error) = DeribitFeed::for_asset(asset).run(feed_sender).await {
                tracing::warn!(?asset, %error, "Deribit feed stopped");
            }
        });
    }
}

struct MarketRuntimeState {
    key: MarketKey,
    spec: MarketSpec,
    market_id: String,
    condition_id: ConditionId,
    yes_token_id: TokenId,
    actor: MarketActor,
    pipeline: Pipeline,
    last_trade_price: PriceTicks,
    tick_size: TickSize,
}

// The runtime constructor keeps the venue metadata and pipeline wiring
// explicit; grouping these stable dependencies would hide market isolation.
#[allow(clippy::too_many_arguments)]
async fn build_market_runtime(
    run_id: &str,
    config: &AppConfig,
    deadline: Duration,
    questdb: QuestDbHandle,
    spec: MarketSpec,
    market_id: String,
    metadata: MarketMetadata,
    condition_id: ConditionId,
    yes_token_id: TokenId,
    clob: &ClobClient,
) -> Result<MarketRuntimeState> {
    let top = fetch_top_of_book(clob, &yes_token_id)
        .await
        .with_context(|| format!("fetching initial Polymarket book for `{market_id}`"))?;
    let (_market_sender, market_receiver) = mpsc::channel(1);
    let mut actor = MarketActor::new(yes_token_id.clone(), market_receiver);
    actor.apply_message(MarketMessage::BookSnapshot(book_snapshot_from_top(
        &condition_id,
        &yes_token_id,
        &top,
    )));

    let key = spec
        .market_key()
        .ok_or_else(|| anyhow!("market spec `{}` has no supported market key", spec.slug))?;
    let pipeline = Pipeline::new(
        run_id.to_owned(),
        SignalActor::from_freshness_policy(config.freshness_policy),
        ExecutionActor::new(RiskLimits::from_freshness_policy(
            1,
            config.freshness_policy,
            false,
        )),
        questdb,
        config.typesafe_api_key.clone(),
        deadline,
        config.quote_thresholds,
        config.quant,
        512,
    );

    Ok(MarketRuntimeState {
        key,
        spec,
        market_id: if market_id.is_empty() {
            metadata.market_id.0
        } else {
            market_id
        },
        condition_id,
        yes_token_id,
        actor,
        pipeline,
        last_trade_price: coherent_mid(top.best_bid, top.best_ask)
            .unwrap_or_else(|| PriceTicks::from_f64(0.0)),
        tick_size: top.tick_size,
    })
}

async fn refresh_market_book(runtime: &mut MarketRuntimeState, clob: &ClobClient) -> bool {
    match fetch_top_of_book(clob, &runtime.yes_token_id).await {
        Ok(top) => {
            runtime.tick_size = top.tick_size;
            runtime
                .actor
                .apply_message(MarketMessage::BookSnapshot(book_snapshot_from_top(
                    &runtime.condition_id,
                    &runtime.yes_token_id,
                    &top,
                )));
            if let Some(mid) = coherent_mid(top.best_bid, top.best_ask) {
                runtime.last_trade_price = mid;
            }
            true
        }
        Err(error) => {
            tracing::warn!(market = %runtime.market_id, %error, "Polymarket top-of-book refresh failed");
            false
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
