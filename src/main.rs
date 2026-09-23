use std::collections::VecDeque;
use std::env;
use std::str::FromStr;
use std::time::Duration;

use chrono::{Datelike, Utc};

use anyhow::{Context, Result, anyhow};
use jevtrader::config::AppConfig;
use jevtrader::domain::{Asset, ConditionId, MarketKey, PriceTicks, TickSize, TokenId, Trigger};
use jevtrader::engine::{
    BookSnapshot, ExecutionActor, MarketActor, MarketMessage, MarketRegistry, MarketSnapshot,
    Pipeline, PipelineInput, SignalActor,
};
use jevtrader::feeds::{BinanceFeed, CoinbaseFeed, DeribitFeed, SharedFeeds, Venue, VenueTick};
use jevtrader::market_spec::MarketSpec;
use jevtrader::polymarket::{
    BASE_UNITS_PER_TOKEN, MarketMetadata, MarketStreamClient, OrderBook, TopOfBookSnapshot,
    discover_live_specs, fetch_market_by_slug, fetch_top_of_book,
};
use jevtrader::state::feature_builder::{ContractContext, ResolutionContext};
use jevtrader::storage::{QuestDbHandle, QuestDbWriter};
use jevtrader::strategy::daily::{
    self, Candle, DailyFeed, Decision as DailyDecision, HoldReason, Position, Tracker,
};
use jevtrader::strategy::risk::RiskLimits;
use polymarket_client_sdk_v2::clob::{Client as ClobClient, Config as ClobConfig};
use polymarket_client_sdk_v2::gamma::Client as GammaClient;
use serde_json::{Value, json};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

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

    let dry_run = parse_args_from(env::args().skip(1))?;
    if configured_strategy()? == "v2" {
        return run_daily_v2(dry_run).await;
    }

    let paths = configured_market_paths()?;
    let config = AppConfig::load().map_err(anyhow::Error::new)?;
    let deadline = jev_deadline()?;
    let mut specs = Vec::with_capacity(paths.len());
    if paths.is_empty() {
        let gamma = GammaClient::default();
        specs = discover_live_specs(&gamma)
            .await
            .context("discovering live Polymarket markets")?;
        for spec in &specs {
            tracing::info!(
                slug = %spec.slug,
                horizon = %spec.horizon().map_or("unknown", |horizon| horizon.as_str()),
                target = spec.target,
                ends_at = %format_end_time(spec.resolution_at_ms),
                "selected live market"
            );
        }
    } else {
        for path in paths {
            let spec = MarketSpec::load(&path)
                .map_err(anyhow::Error::new)
                .with_context(|| format!("loading market spec `{path}`"))?;
            specs.push(spec);
        }
    }

    let size = market_size()?;
    let cooldown = jev_min_interval();
    let run_id = configured_run_id();
    if dry_run {
        print_dry_run_summary(&config, &specs, &run_id, size, cooldown);
        return Ok(());
    }

    run_multi_market(config, specs, deadline, run_id, size, cooldown).await
}

fn configured_strategy() -> Result<&'static str> {
    match env::var("STRATEGY") {
        Ok(value) if value.trim().eq_ignore_ascii_case("v1") => Ok("v1"),
        Ok(value) if value.trim().eq_ignore_ascii_case("v2") => Ok("v2"),
        Ok(value) => Err(anyhow!("unsupported STRATEGY `{value}`; expected v1 or v2")),
        Err(env::VarError::NotPresent) => Ok("v2"),
        Err(env::VarError::NotUnicode(_)) => Err(anyhow!("STRATEGY is not valid UTF-8")),
    }
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
    run_id: String,
    size: u64,
    jev_min_interval: Duration,
) -> Result<()> {
    let gamma = GammaClient::default();
    let clob = ClobClient::new("https://clob.polymarket.com", ClobConfig::default())
        .context("creating the public Polymarket CLOB client")?;
    let (questdb, _questdb_task) = QuestDbWriter::spawn(&config.questdb_ilp_addr, 4096);
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
            if jev_cooldown_active(
                market.last_jev_evaluated_at_ms,
                observed_at_ms,
                jev_min_interval,
            ) {
                let elapsed_ms = market
                    .last_jev_evaluated_at_ms
                    .map(|last_eval| observed_at_ms.saturating_sub(last_eval).max(0))
                    .unwrap_or_default();
                tracing::debug!(
                    market = %market.market_id,
                    elapsed_ms,
                    cooldown_secs = jev_min_interval.as_secs(),
                    "skipping Jev evaluation due to cooldown"
                );
                continue;
            }

            let result = market
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
            if result.jev_evaluated {
                market.last_jev_evaluated_at_ms = Some(observed_at_ms);
            }
        }
    }
}

const DAILY_BACKFILL_DAYS: u64 = 70;
const MIN_DAILY_BACKFILL_ROWS: usize = 61;
const V2_EVALUATION_INTERVAL: Duration = Duration::from_secs(60);
const V2_DECISION_BUFFER_CAPACITY: usize = 10_000;

struct DiscoveredDailyMarket {
    slug: String,
    asset: Asset,
    epoch: i64,
    target: f64,
    metadata: MarketMetadata,
}

struct DailyMarketRuntime {
    slug: String,
    asset: Asset,
    condition_id: ConditionId,
    feed: DailyFeed,
    position: Position,
    entry_ref: Option<f64>,
    tracker: Tracker,
    book: watch::Receiver<MarketSnapshot>,
    _actor_task: JoinHandle<MarketActor>,
    _websocket_task: JoinHandle<Result<(), jevtrader::polymarket::WsError>>,
}

async fn run_daily_v2(dry_run: bool) -> Result<()> {
    let markets = discover_daily_markets().await?;
    let mut backfills = std::collections::HashMap::new();
    for asset in Asset::ALL {
        if !markets.iter().any(|market| market.asset == asset) {
            continue;
        }
        let candles = daily::backfill_daily(asset.as_str(), DAILY_BACKFILL_DAYS)
            .await
            .with_context(|| {
                format!("backfilling daily {} candles from Binance", asset.as_str())
            })?;
        if candles.len() < MIN_DAILY_BACKFILL_ROWS {
            return Err(anyhow!(
                "Binance returned only {} closed daily {} candles; V2 requires at least {MIN_DAILY_BACKFILL_ROWS}",
                candles.len(),
                asset.as_str()
            ));
        }
        backfills.insert(asset, candles);
    }

    if dry_run {
        print_daily_v2_plan(&markets, &backfills);
        return Ok(());
    }

    let clob = ClobClient::new("https://clob.polymarket.com", ClobConfig::default())
        .context("creating the public Polymarket CLOB client for V2")?;
    let mut runtimes = Vec::with_capacity(markets.len());
    for market in markets {
        let closed = backfills
            .remove(&market.asset)
            .ok_or_else(|| anyhow!("missing {} daily candle backfill", market.asset.as_str()))?;
        let top = fetch_top_of_book(&clob, &market.metadata.yes_token_id)
            .await
            .with_context(|| format!("seeding the V2 book for `{}`", market.slug))?;
        let (mut actor, sender, book) =
            MarketActor::channel_with_snapshot(market.metadata.yes_token_id.clone(), 8192);
        actor.apply_message(MarketMessage::BookSnapshot(book_snapshot_from_top(
            &market.metadata.condition_id,
            &market.metadata.yes_token_id,
            &top,
        )));
        let actor_task = tokio::spawn(actor.run());
        let stream = MarketStreamClient::new(
            market.metadata.yes_token_id.clone(),
            market.metadata.no_token_id.clone(),
        )
        .with_context(|| format!("creating the V2 market stream for `{}`", market.slug))?;
        let websocket_task = jevtrader::engine::ws_bridge::spawn_market_ws(stream, sender);

        tracing::info!(
            strategy = "v2",
            market = %market.slug,
            asset = %market.asset,
            target = market.target,
            closed_candles = closed.len(),
            "started deterministic daily market runtime"
        );
        runtimes.push(DailyMarketRuntime {
            slug: market.slug,
            asset: market.asset,
            condition_id: market.metadata.condition_id,
            feed: DailyFeed {
                closed,
                forming: None,
            },
            position: Position::Flat,
            entry_ref: None,
            tracker: Tracker::default(),
            book,
            _actor_task: actor_task,
            _websocket_task: websocket_task,
        });
    }

    let (feed_sender, mut feed_receiver) = mpsc::channel::<VenueTick>(8192);
    for asset in Asset::ALL {
        if runtimes.iter().any(|market| market.asset == asset) {
            let sender = feed_sender.clone();
            tokio::spawn(async move {
                if let Err(error) = BinanceFeed::for_asset(asset).run(sender).await {
                    tracing::warn!(?asset, %error, "V2 Binance daily feed stopped");
                }
            });
        }
    }
    drop(feed_sender);

    let mut interval = tokio::time::interval(V2_EVALUATION_INTERVAL);
    let mut feed_open = true;
    let mut decision_buffer = VecDeque::with_capacity(V2_DECISION_BUFFER_CAPACITY);
    loop {
        tokio::select! {
            tick = feed_receiver.recv(), if feed_open => match tick {
                Some(tick) if tick.venue == Venue::Binance => {
                    if let Some(asset) = daily_asset_for_symbol(tick.symbol) {
                        let timestamp = if tick.ts_exchange_ms > 0 {
                            tick.ts_exchange_ms
                        } else {
                            tick.ts_local_ms
                        };
                        for market in runtimes.iter_mut().filter(|market| market.asset == asset) {
                            market.feed.push_tick(timestamp, tick.price_f64);
                        }
                    }
                }
                Some(_) => {}
                None => feed_open = false,
            },
            _ = interval.tick() => {
                let observed_at_ms = unix_time_ms();
                for market in &mut runtimes {
                    evaluate_daily_market(market, observed_at_ms, &mut decision_buffer);
                }
            }
        }
    }
}

async fn discover_daily_markets() -> Result<Vec<DiscoveredDailyMarket>> {
    let gamma = GammaClient::default();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .context("creating the Gamma daily-market client")?;
    let now = Utc::now();
    let current_epoch = now.timestamp().div_euclid(86_400) * 86_400;
    let named_date = format!(
        "{}-{}-{}",
        now.format("%B").to_string().to_ascii_lowercase(),
        now.day(),
        now.year()
    );
    let mut selected: Vec<DiscoveredDailyMarket> = Vec::with_capacity(Asset::ALL.len());

    for asset in Asset::ALL {
        let named_asset_slug = match asset {
            Asset::Btc => "bitcoin",
            Asset::Eth => "ethereum",
        };
        let named_slug = format!("{named_asset_slug}-up-or-down-on-{named_date}");
        let mut candidate =
            discover_daily_candidate(&gamma, &client, &named_slug, asset, current_epoch).await?;

        if candidate.is_none() {
            let epoch_asset_slug = match asset {
                Asset::Btc => "btc",
                Asset::Eth => "eth",
            };
            for epoch in [current_epoch, current_epoch - 86_400] {
                let slug = format!("{epoch_asset_slug}-updown-daily-{epoch}");
                if let Some(epoch_candidate) =
                    discover_daily_candidate(&gamma, &client, &slug, asset, epoch).await?
                    && candidate
                        .as_ref()
                        .is_none_or(|previous| epoch_candidate.epoch > previous.epoch)
                {
                    candidate = Some(epoch_candidate);
                }
            }
        }

        if let Some(candidate) = candidate {
            selected.push(candidate);
        }
    }

    if selected.is_empty() {
        return Err(anyhow!("no active BTC/ETH daily up-down markets qualified"));
    }
    selected.sort_by_key(|market| match market.asset {
        Asset::Btc => 0,
        Asset::Eth => 1,
    });
    Ok(selected)
}

async fn discover_daily_candidate(
    gamma: &GammaClient,
    client: &reqwest::Client,
    slug: &str,
    asset: Asset,
    epoch: i64,
) -> Result<Option<DiscoveredDailyMarket>> {
    let Some(value) = fetch_daily_market_value(gamma, client, slug).await? else {
        return Ok(None);
    };
    if value.get("slug").and_then(Value::as_str) != Some(slug)
        || value.get("active").and_then(Value::as_bool) != Some(true)
        || value.get("closed").and_then(Value::as_bool) != Some(false)
        || value.get("acceptingOrders").and_then(Value::as_bool) != Some(true)
        || value.get("enableOrderBook").and_then(Value::as_bool) != Some(true)
    {
        return Ok(None);
    }
    let Some(target) = event_price_to_beat(&value) else {
        tracing::warn!(market = %slug, "skipping daily market with missing or invalid eventMetadata.priceToBeat");
        return Ok(None);
    };
    let metadata = fetch_market_by_slug(gamma, slug)
        .await
        .with_context(|| format!("fetching V2 Polymarket metadata for `{slug}`"))?;
    Ok(Some(DiscoveredDailyMarket {
        slug: slug.to_owned(),
        asset,
        epoch,
        target,
        metadata,
    }))
}

async fn fetch_daily_market_value(
    gamma: &GammaClient,
    client: &reqwest::Client,
    slug: &str,
) -> Result<Option<Value>> {
    let url = gamma
        .host()
        .join(&format!("markets/slug/{slug}"))
        .context("building the Gamma daily-market URL")?;
    let response = client
        .get(url)
        .send()
        .await
        .with_context(|| format!("requesting Gamma daily market `{slug}`"))?;
    if matches!(
        response.status(),
        reqwest::StatusCode::NOT_FOUND | reqwest::StatusCode::NO_CONTENT
    ) {
        return Ok(None);
    }
    let value: Value = response
        .error_for_status()
        .with_context(|| format!("Gamma daily-market request failed for `{slug}`"))?
        .json()
        .await
        .with_context(|| format!("decoding Gamma daily market `{slug}`"))?;
    if value.is_null()
        || value.as_array().is_some_and(Vec::is_empty)
        || value.as_object().is_some_and(serde_json::Map::is_empty)
    {
        Ok(None)
    } else {
        Ok(Some(value))
    }
}

fn event_price_to_beat(value: &Value) -> Option<f64> {
    let direct = value.get("eventMetadata").and_then(parse_price_to_beat);
    let nested = value
        .get("events")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|event| event.get("eventMetadata"))
        .find_map(parse_price_to_beat);
    direct
        .or(nested)
        .filter(|price| price.is_finite() && *price > 0.0)
}

fn parse_price_to_beat(metadata: &Value) -> Option<f64> {
    let parsed_metadata;
    let metadata = if let Some(encoded) = metadata.as_str() {
        parsed_metadata = serde_json::from_str::<Value>(encoded).ok()?;
        &parsed_metadata
    } else {
        metadata
    };
    let price = metadata.get("priceToBeat")?;
    price
        .as_f64()
        .or_else(|| price.as_str()?.parse::<f64>().ok())
}

fn daily_asset_for_symbol(symbol: &str) -> Option<Asset> {
    match symbol.to_ascii_uppercase().as_str() {
        "BTCUSDT" => Some(Asset::Btc),
        "ETHUSDT" => Some(Asset::Eth),
        _ => None,
    }
}

fn print_daily_v2_plan(
    markets: &[DiscoveredDailyMarket],
    backfills: &std::collections::HashMap<Asset, Vec<Candle>>,
) {
    println!(
        "STRATEGY=v2 dry-run: no Binance feeds, Polymarket WebSockets, or QuestDB writes started."
    );
    println!("markets: {}", markets.len());
    for market in markets {
        let closed = backfills
            .get(&market.asset)
            .map(Vec::as_slice)
            .unwrap_or_default();
        let signal = daily::evaluate(closed);
        let (_, would_be) = daily::step(&Position::Flat, closed);
        let day = closed
            .last()
            .map_or("unavailable", |candle| candle.day_utc.as_str());
        println!(
            "  - slug={} horizon=Daily asset={} target={} ends-at={} epoch={} closed_candles={} day={}",
            market.slug,
            market.asset,
            market.target,
            format_end_time((market.epoch + 86_400) * 1_000),
            market.epoch,
            closed.len(),
            day
        );
        if let Some(signal) = signal {
            let close = closed.last().map_or(0.0, |candle| candle.close);
            println!(
                "    indicators: close={close:.6} sma50={:.6} ema7={:.6} rsi2={:.6} adx2={:.6} entry={}",
                signal.sma50, signal.ema7, signal.rsi2, signal.adx2, signal.entry
            );
        } else {
            println!("    indicators: unavailable (insufficient closed daily history)");
        }
        println!("    first WOULD-BE decision: {would_be:?}");
    }
}

fn evaluate_daily_market(
    market: &mut DailyMarketRuntime,
    observed_at_ms: i64,
    decision_buffer: &mut VecDeque<String>,
) {
    let day = market
        .feed
        .closed
        .last()
        .map_or_else(|| "unavailable".to_owned(), |candle| candle.day_utc.clone());
    let signal = daily::evaluate(&market.feed.closed);
    let (proposed_position, strategy_decision) = daily::step(&market.position, &market.feed.closed);
    let snapshot = market.book.borrow().clone();
    let book_mid = if snapshot.stale {
        None
    } else {
        coherent_mid(snapshot.book.best_bid(), snapshot.book.best_ask()).map(PriceTicks::to_f64)
    };

    let decision_name = match strategy_decision {
        DailyDecision::EnterLongUp(_) => match book_mid {
            Some(reference) => {
                market.position = proposed_position;
                market.entry_ref = Some(reference);
                "ENTER_LONG_UP"
            }
            None => "HOLD_ENTRY_WITHOUT_FRESH_BOOK",
        },
        DailyDecision::ExitFlat(_) => match book_mid {
            Some(reference) => {
                if let Some(entry_ref) = market.entry_ref {
                    market.tracker.record_exit(entry_ref, reference);
                }
                market.position = proposed_position;
                market.entry_ref = None;
                "EXIT_FLAT"
            }
            None => "HOLD_EXIT_WITHOUT_FRESH_BOOK",
        },
        DailyDecision::Hold(reason) => match reason {
            HoldReason::InsufficientHistory => "HOLD_INSUFFICIENT_HISTORY",
            HoldReason::NoSignal => "HOLD_NO_SIGNAL",
            HoldReason::StillLong => "HOLD_STILL_LONG",
        },
    };
    let close = market.feed.closed.last().map(|candle| candle.close);
    let mtm_pp = book_mid.map(|reference| {
        daily::mtm(
            &market.position,
            reference,
            market.entry_ref.unwrap_or(reference),
        )
    });
    let record = json!({
        "ts": observed_at_ms,
        "condition_id": market.condition_id.0,
        "market": market.slug,
        "asset": market.asset.as_str(),
        "day_utc": day,
        "decision": decision_name,
        "position": daily_position_name(&market.position),
        "close": close,
        "sma50": signal.map(|value| value.sma50),
        "ema7": signal.map(|value| value.ema7),
        "rsi2": signal.map(|value| value.rsi2),
        "adx2": signal.map(|value| value.adx2),
        "entry": signal.map(|value| value.entry),
        "book_mid": book_mid,
        "book_stale": snapshot.stale,
        "entry_ref": market.entry_ref,
        "mtm_pp": mtm_pp,
        "realized_pnl_pp": market.tracker.realized_pnl_pp,
    });
    if decision_buffer.len() == V2_DECISION_BUFFER_CAPACITY {
        decision_buffer.pop_front();
        tracing::warn!("V2 in-memory decision buffer reached capacity; oldest record discarded");
    }
    decision_buffer.push_back(record.to_string());

    tracing::info!(
        strategy = "v2",
        market = %market.slug,
        asset = %market.asset,
        day = %day,
        close = ?close,
        sma50 = ?signal.map(|value| value.sma50),
        ema7 = ?signal.map(|value| value.ema7),
        rsi2 = ?signal.map(|value| value.rsi2),
        adx2 = ?signal.map(|value| value.adx2),
        entry_condition = ?signal.map(|value| value.entry),
        decision = decision_name,
        book_mid = ?book_mid,
        book_stale = snapshot.stale,
        position = daily_position_name(&market.position),
        entry_ref = ?market.entry_ref,
        mtm_pp = ?mtm_pp,
        realized_pnl_pp = market.tracker.realized_pnl_pp,
        buffered_evaluations = decision_buffer.len(),
        "daily strategy evaluation"
    );
}

fn daily_position_name(position: &Position) -> &'static str {
    match position {
        Position::Flat => "FLAT",
        Position::LongUp { .. } => "LONG_UP",
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
    last_jev_evaluated_at_ms: Option<i64>,
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
        last_jev_evaluated_at_ms: None,
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
    let millis = parse_defaulted_env("JEV_DEADLINE_MS", 1_500_u64)?;
    if millis == 0 {
        return Err(anyhow!("JEV_DEADLINE_MS must be greater than zero"));
    }
    Ok(Duration::from_millis(millis))
}

fn market_size() -> Result<u64> {
    parse_defaulted_env("JEVTRADER_MARKET_SIZE", 10_u64)
}

fn parse_defaulted_env<T>(name: &'static str, default: T) -> Result<T>
where
    T: FromStr + std::fmt::Display,
    T::Err: std::fmt::Display,
{
    let configured = match env::var(name) {
        Ok(value) => Some(value),
        Err(env::VarError::NotPresent) => None,
        Err(env::VarError::NotUnicode(_)) => {
            return Err(anyhow!("{name} is not valid UTF-8"));
        }
    };
    let (value, used_default) = parse_defaulted_value(name, configured.as_deref(), default)?;
    if used_default {
        tracing::info!(
            environment_variable = name,
            default = %value,
            "using default; set the environment variable to override"
        );
    }
    Ok(value)
}

fn parse_defaulted_value<T>(
    name: &'static str,
    configured: Option<&str>,
    default: T,
) -> Result<(T, bool)>
where
    T: FromStr,
    T::Err: std::fmt::Display,
{
    match configured {
        Some(value) if !value.is_empty() => value
            .parse::<T>()
            .map(|parsed| (parsed, false))
            .map_err(|error| anyhow!("invalid {name} value `{value}`: {error}")),
        _ => Ok((default, true)),
    }
}

fn parse_args_from(args: impl IntoIterator<Item = String>) -> Result<bool> {
    let mut args = args.into_iter();
    match args.next() {
        None => Ok(false),
        Some(flag) if flag == "--dry-run" => match args.next() {
            None => Ok(true),
            Some(extra) => Err(anyhow!("unexpected argument `{extra}`")),
        },
        Some(argument) => Err(anyhow!("unknown argument `{argument}`")),
    }
}

fn configured_run_id() -> String {
    env::var("JEVTRADER_RUN_ID")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| format!("run-{}", unix_time_ms()))
}

fn print_dry_run_summary(
    config: &AppConfig,
    specs: &[MarketSpec],
    run_id: &str,
    size: u64,
    cooldown: Duration,
) {
    println!("Paper-only dry run: no feeds, books, Jev calls, or QuestDB writes will start.");
    println!("run_id (would use): {run_id}");
    println!("markets: {}", specs.len());
    for spec in specs {
        println!(
            "  - slug={} horizon={} target={} ends-at={}",
            spec.slug,
            spec.horizon().map_or("unknown", |horizon| horizon.as_str()),
            spec.target,
            format_end_time(spec.resolution_at_ms)
        );
    }
    println!("market_size: {size}");
    println!("thresholds_source: {}", thresholds_source());
    println!("thresholds: {:?}", config.quote_thresholds);
    println!("cooldown_secs: {}", cooldown.as_secs());
    println!("quant: {}", if config.quant.enabled { "on" } else { "off" });
    println!("QuestDB HTTP: {}", config.questdb_http_url);
    println!("QuestDB ILP: {}", config.questdb_ilp_addr);
}

fn thresholds_source() -> &'static str {
    const VARIABLES: [&str; 7] = [
        "QUOTE_UNDER_MIN",
        "QUOTE_NEXT_UP_MIN",
        "QUOTE_PERSIST_MIN",
        "QUOTE_FILL_MIN",
        "QUOTE_TOXIC_MAX",
        "QUOTE_CONFLICT_MAX",
        "QUOTE_NO_PRESSURE_MAX",
    ];
    if VARIABLES
        .iter()
        .any(|name| env::var(name).is_ok_and(|value| !value.is_empty()))
    {
        "QUOTE_* environment overrides plus built-in defaults for unset values"
    } else {
        "built-in QuoteThresholds defaults"
    }
}

fn format_end_time(timestamp_ms: i64) -> String {
    chrono::DateTime::<chrono::Utc>::from_timestamp_millis(timestamp_ms)
        .map(|timestamp| timestamp.to_rfc3339())
        .unwrap_or_else(|| format!("{timestamp_ms} ms since Unix epoch"))
}

const DEFAULT_JEV_MIN_INTERVAL_SECS: u64 = 30;

fn jev_min_interval() -> Duration {
    match env::var("JEV_MIN_INTERVAL_SECS") {
        Ok(value) => parse_jev_min_interval(Some(&value)),
        Err(env::VarError::NotPresent) => Duration::from_secs(DEFAULT_JEV_MIN_INTERVAL_SECS),
        Err(error) => {
            tracing::warn!(
                %error,
                default_secs = DEFAULT_JEV_MIN_INTERVAL_SECS,
                "invalid JEV_MIN_INTERVAL_SECS; using default"
            );
            Duration::from_secs(DEFAULT_JEV_MIN_INTERVAL_SECS)
        }
    }
}

fn parse_jev_min_interval(value: Option<&str>) -> Duration {
    let Some(value) = value else {
        return Duration::from_secs(DEFAULT_JEV_MIN_INTERVAL_SECS);
    };

    match value.trim().parse::<u64>() {
        Ok(seconds) => Duration::from_secs(seconds),
        Err(error) => {
            tracing::warn!(
                value,
                %error,
                default_secs = DEFAULT_JEV_MIN_INTERVAL_SECS,
                "invalid JEV_MIN_INTERVAL_SECS; using default"
            );
            Duration::from_secs(DEFAULT_JEV_MIN_INTERVAL_SECS)
        }
    }
}

fn jev_cooldown_active(last_eval_at_ms: Option<i64>, now_ms: i64, interval: Duration) -> bool {
    let Some(last_eval_at_ms) = last_eval_at_ms else {
        return false;
    };
    let elapsed_ms = now_ms.saturating_sub(last_eval_at_ms).max(0) as u128;
    elapsed_ms < interval.as_millis()
}

fn unix_time_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

#[cfg(test)]
mod tests {
    use super::{
        Asset, DEFAULT_JEV_MIN_INTERVAL_SECS, Duration, daily_asset_for_symbol,
        event_price_to_beat, jev_cooldown_active, parse_args_from, parse_defaulted_value,
        parse_jev_min_interval,
    };

    #[test]
    fn market_size_and_jev_deadline_default_on_unset_or_empty_values() {
        assert_eq!(
            parse_defaulted_value("JEVTRADER_MARKET_SIZE", None, 10_u64)
                .expect("market size default"),
            (10, true)
        );
        assert_eq!(
            parse_defaulted_value("JEVTRADER_MARKET_SIZE", Some(""), 10_u64)
                .expect("empty market size default"),
            (10, true)
        );
        assert_eq!(
            parse_defaulted_value("JEV_DEADLINE_MS", None, 1_500_u64)
                .expect("Jev deadline default"),
            (1_500, true)
        );
        assert_eq!(
            parse_defaulted_value("JEV_DEADLINE_MS", Some(""), 1_500_u64)
                .expect("empty Jev deadline default"),
            (1_500, true)
        );
        assert_eq!(
            parse_defaulted_value("JEV_DEADLINE_MS", Some("2500"), 1_500_u64)
                .expect("configured Jev deadline"),
            (2_500, false)
        );
        assert!(parse_defaulted_value("JEV_DEADLINE_MS", Some("invalid"), 1_500_u64).is_err());
    }

    #[test]
    fn cli_accepts_only_the_dry_run_flag() {
        assert!(!parse_args_from(Vec::<String>::new()).expect("default mode"));
        assert!(parse_args_from(["--dry-run".to_owned()]).expect("dry-run mode"));
        assert!(parse_args_from(["--unknown".to_owned()]).is_err());
    }

    #[test]
    fn daily_market_helpers_accept_only_supported_symbols_and_valid_targets() {
        assert_eq!(daily_asset_for_symbol("BTCUSDT"), Some(Asset::Btc));
        assert_eq!(daily_asset_for_symbol("ethusdt"), Some(Asset::Eth));
        assert_eq!(daily_asset_for_symbol("SOLUSDT"), None);
        assert_eq!(
            event_price_to_beat(&serde_json::json!({"eventMetadata": {"priceToBeat": "65000.5"}})),
            Some(65_000.5)
        );
        assert_eq!(
            event_price_to_beat(
                &serde_json::json!({"events": [{"eventMetadata": r#"{"priceToBeat":65000}"#}]})
            ),
            Some(65_000.0)
        );
        assert_eq!(
            event_price_to_beat(&serde_json::json!({"eventMetadata": {"priceToBeat": 0}})),
            None
        );
    }

    #[test]
    fn jev_min_interval_parses_valid_and_falls_back_for_invalid_values() {
        assert_eq!(
            parse_jev_min_interval(None),
            Duration::from_secs(DEFAULT_JEV_MIN_INTERVAL_SECS)
        );
        assert_eq!(parse_jev_min_interval(Some("15")), Duration::from_secs(15));
        assert_eq!(
            parse_jev_min_interval(Some("")),
            Duration::from_secs(DEFAULT_JEV_MIN_INTERVAL_SECS)
        );
        assert_eq!(
            parse_jev_min_interval(Some("not-a-number")),
            Duration::from_secs(DEFAULT_JEV_MIN_INTERVAL_SECS)
        );
    }

    #[test]
    fn jev_cooldown_allows_first_and_post_interval_evaluations() {
        let interval = Duration::from_secs(30);
        assert!(!jev_cooldown_active(None, 1_000, interval));
        assert!(jev_cooldown_active(Some(1_000), 30_999, interval));
        assert!(!jev_cooldown_active(Some(1_000), 31_000, interval));
    }
}
