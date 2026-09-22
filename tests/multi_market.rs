use jevtrader::config::QuoteThresholds;
use jevtrader::domain::{
    Asset, ConditionId, Horizon, MarketKey, PriceTicks, TickSize, TokenId, TradeSide, Trigger,
};
use jevtrader::engine::{
    BookDelta, BookSnapshot, ExecutionActor, MarketActor, MarketMessage, SignalActor,
    StalenessPolicy, decide,
};
use jevtrader::feeds::{SharedFeeds, Venue, VenueTick};
use jevtrader::jev::{JevEvaluation, TickDistribution, V1Signal};
use jevtrader::market_spec::MarketSpec;
use jevtrader::polymarket::{BookSide, OrderBook};
use jevtrader::replay::portfolio::FillEvent;
use jevtrader::replay::{Portfolio, conditional_markout_5s, signed_markouts_pp};
use jevtrader::storage::{ExperimentTags, StorageEvent, Variant};
use jevtrader::strategy::quote::QuoteIntent;
use jevtrader::strategy::risk::RiskLimits;
use tokio::sync::mpsc;

fn parse_spec(asset: Asset, horizon: Horizon) -> MarketSpec {
    MarketSpec::parse(&format!(
        "slug: {}-{}-integration\nquestion: Will {} resolve above the target?\nresolution_source: Binance\ntarget: 100\nresolution_at_ms: 3_600_000\nasset: {}\nhorizon: {}\nresolution_rules: |\n  Resolves from the declared Binance reference.\n",
        asset.as_str().to_ascii_lowercase(),
        horizon.as_str(),
        asset.as_str(),
        asset.as_str(),
        horizon.as_str(),
    ))
    .expect("integration market spec should parse")
}

fn tick(
    venue: Venue,
    symbol: &'static str,
    price: f64,
    timestamp: i64,
    trade_size: f64,
) -> VenueTick {
    VenueTick {
        venue,
        symbol,
        price_f64: price,
        best_bid_f64: f64::NAN,
        best_ask_f64: f64::NAN,
        trade_size_f64: trade_size,
        trade_side_buy: true,
        ts_exchange_ms: timestamp,
        ts_local_ms: 0,
    }
}

fn signal() -> V1Signal {
    V1Signal {
        yes_pressure_5s: 0.8,
        no_pressure_5s: 0.1,
        move_persists: 0.7,
        underreact_up: 0.9,
        underreact_down: 0.1,
        repricing: TickDistribution {
            up_3_plus: 0.1,
            up_2: 0.2,
            up_1: 0.4,
            flat: 0.1,
            down_1: 0.1,
            down_2: 0.05,
            down_3_plus: 0.05,
        },
        repricing_confidence: 0.8,
        fill_before_decay: 0.7,
        fill_toxic: 0.2,
    }
}

fn evaluation(market_id: &str, state_seq: u64) -> JevEvaluation {
    JevEvaluation {
        market_id: market_id.to_owned(),
        state_seq,
        sent_at_ms: 1_000,
        received_at_ms: 1_050,
        latency_ms: 50,
        signal: signal(),
        tokens_in: 0,
        tokens_out: 0,
    }
}

fn actor(token_id: &str) -> MarketActor {
    let (_sender, receiver) = mpsc::channel(1);
    MarketActor::new(TokenId(token_id.to_owned()), receiver)
}

fn snapshot(token_id: &str, condition_id: &str, sequence: u64) -> BookSnapshot {
    let mut expected = OrderBook::default();
    expected.apply_snapshot(
        [(PriceTicks::from_f64(0.40), 100)],
        [(PriceTicks::from_f64(0.50), 100)],
    );
    BookSnapshot {
        condition_id: ConditionId(condition_id.to_owned()),
        token_id: TokenId(token_id.to_owned()),
        sequence: Some(sequence),
        bids: expected.bids().to_vec(),
        asks: expected.asks().to_vec(),
        book_hash: Some(expected.book_hash()),
        source_hash: None,
    }
}

fn intent() -> QuoteIntent {
    QuoteIntent {
        side: TradeSide::Buy,
        price: PriceTicks::from_f64(0.41),
        size: 10,
    }
}

fn limits() -> RiskLimits {
    RiskLimits {
        max_outstanding_quotes: 2,
        max_latency_ms: 1_500,
        killed: false,
    }
}

#[test]
fn parses_btc_5m_and_eth_1h_specs_without_crossing_contract_metadata() {
    let btc = parse_spec(Asset::Btc, Horizon::M5);
    let eth = parse_spec(Asset::Eth, Horizon::H1);

    assert_eq!(
        btc.market_key(),
        Some(MarketKey::new(Asset::Btc, Horizon::M5))
    );
    assert_eq!(
        eth.market_key(),
        Some(MarketKey::new(Asset::Eth, Horizon::H1))
    );
    assert_eq!(btc.market_key().unwrap().market_id(), "BTC-5m");
    assert_eq!(eth.market_key().unwrap().market_id(), "ETH-1h");
    assert_ne!(btc.question, eth.question);
    assert_eq!(btc.horizon(), Some(Horizon::M5));
    assert_eq!(eth.horizon(), Some(Horizon::H1));
}

#[test]
fn shared_feeds_route_btc_and_eth_ticks_to_only_their_asset_lanes() {
    let mut feeds = SharedFeeds::new();
    feeds.apply(tick(Venue::Binance, "BTCUSDT", 42_000.0, 1_000, 2.0));
    feeds.apply(tick(Venue::Coinbase, "ETH-USD", 2_200.0, 1_500, 3.0));
    feeds.apply(tick(Venue::Deribit, "XBT-PERPETUAL", 42_001.0, 1_900, 0.0));
    feeds.apply(tick(Venue::Binance, "SOLUSDT", 150.0, 2_000, 1.0));

    assert_eq!(feeds.lane(Asset::Btc).recent_ticks().len(), 1);
    assert_eq!(feeds.lane(Asset::Btc).recent_ticks()[0].price, 42_000.0);
    assert_eq!(feeds.lane(Asset::Eth).recent_ticks().len(), 1);
    assert_eq!(feeds.lane(Asset::Eth).recent_ticks()[0].price, 2_200.0);
    assert_eq!(feeds.lane(Asset::Btc).venues().perp, 42_001.0);
    assert_eq!(feeds.lane(Asset::Eth).venues().coinbase, 2_200.0);
    assert_eq!(feeds.lane(Asset::Btc).order_flow(2_000).buy_vol_1s, 2.0);
    assert_eq!(feeds.lane(Asset::Eth).order_flow(2_000).buy_vol_1s, 3.0);
    assert_eq!(feeds.ignored, 1);
}

#[test]
fn signal_actors_keep_market_identity_and_state_sequences_independent() {
    let mut btc = SignalActor::new(StalenessPolicy::new(0, 1_500));
    let mut eth = SignalActor::new(StalenessPolicy::new(0, 1_500));

    btc.record_evaluation(evaluation("BTC-5m", 2));
    eth.record_evaluation(evaluation("ETH-1h", 1));

    assert_eq!(btc.latest_evaluation().unwrap().market_id, "BTC-5m");
    assert_eq!(btc.latest_evaluation().unwrap().state_seq, 2);
    assert_eq!(eth.latest_evaluation().unwrap().market_id, "ETH-1h");
    assert_eq!(eth.latest_evaluation().unwrap().state_seq, 1);

    btc.record_evaluation(evaluation("BTC-5m", 3));
    assert_eq!(btc.latest_evaluation().unwrap().state_seq, 3);
    assert_eq!(eth.latest_evaluation().unwrap().state_seq, 1);
}

#[test]
fn stale_market_a_does_not_affect_market_b_decision() {
    let mut market_a = actor("btc-yes");
    let mut market_b = actor("eth-yes");
    market_a.apply_message(MarketMessage::BookSnapshot(snapshot(
        "btc-yes",
        "btc-condition",
        1,
    )));
    market_b.apply_message(MarketMessage::BookSnapshot(snapshot(
        "eth-yes",
        "eth-condition",
        1,
    )));

    market_a.apply_message(MarketMessage::BookDelta(BookDelta {
        condition_id: ConditionId("btc-condition".to_owned()),
        token_id: TokenId("btc-yes".to_owned()),
        sequence: Some(3),
        side: BookSide::Bid,
        price: PriceTicks::from_f64(0.41),
        quantity: Some(100),
        book_hash: None,
        source_hash: None,
    }));

    let thresholds = QuoteThresholds::default();
    let stale_a = market_a.latest_snapshot();
    let fresh_b = market_b.latest_snapshot();
    let stale_decision = decide(jevtrader::engine::DecisionInput {
        signal: &signal(),
        market: &stale_a,
        thresholds: &thresholds,
        tick_size: TickSize::from_f64(0.01),
        size: 10,
    });
    let fresh_decision = decide(jevtrader::engine::DecisionInput {
        signal: &signal(),
        market: &fresh_b,
        thresholds: &thresholds,
        tick_size: TickSize::from_f64(0.01),
        size: 10,
    });

    assert!(stale_a.stale);
    assert!(!fresh_b.stale);
    assert_eq!(
        stale_decision,
        jevtrader::engine::Outcome::Skip(jevtrader::engine::SkipReason::StaleBook)
    );
    assert!(fresh_decision.is_quote());
}

#[test]
fn control_and_quant_paper_fills_are_independent() {
    let mut execution = ExecutionActor::new(limits());
    execution
        .on_quote(Variant::Control, intent(), false, 50)
        .expect("control quote should rest");
    execution
        .on_quote(Variant::QuantV1, intent(), false, 50)
        .expect("quant quote should rest");

    let first_touch = execution.on_book_update(jevtrader::execution::TopOfBookUpdate::new(
        2_000,
        Some(PriceTicks::from_f64(0.41)),
        100,
        Some(PriceTicks::from_f64(0.60)),
        100,
        100,
    ));
    assert_eq!(first_touch.control.len(), 1);
    assert_eq!(first_touch.quant.len(), 1);
    assert_eq!(first_touch.control[0].size, 9);
    assert_eq!(first_touch.quant[0].size, 9);
    assert_eq!(first_touch.control[0].order_id, 1);
    assert_eq!(first_touch.quant[0].order_id, 2);

    assert!(execution.cancel(Variant::Control, 1));
    let second_touch = execution.on_book_update(jevtrader::execution::TopOfBookUpdate::new(
        3_000,
        Some(PriceTicks::from_f64(0.41)),
        100,
        Some(PriceTicks::from_f64(0.60)),
        100,
        5,
    ));
    assert!(second_touch.control.is_empty());
    assert_eq!(second_touch.quant.len(), 1);
    assert_eq!(second_touch.quant[0].size, 1);
    assert_eq!(execution.outstanding(Variant::Control), 0);
    assert_eq!(execution.outstanding(Variant::QuantV1), 0);
}

#[test]
fn replay_settlement_drawdown_and_conditional_markout_are_deterministic() {
    let mut yes = Portfolio::new();
    yes.apply_fill(FillEvent {
        price: 0.40,
        size: 10.0,
        ts_ms: 0,
        toxic: false,
    });
    yes.apply_exit(1.0, 10.0, 300_000);
    assert!((yes.realized_pnl_pp() - 600.0).abs() < 1e-9);

    let mut no = Portfolio::new();
    no.apply_fill(FillEvent {
        price: 0.40,
        size: 10.0,
        ts_ms: 0,
        toxic: false,
    });
    no.apply_exit(0.0, 10.0, 300_000);
    assert!((no.realized_pnl_pp() + 400.0).abs() < 1e-9);

    let mut drawdown = Portfolio::new();
    drawdown.apply_fill(FillEvent {
        price: 0.40,
        size: 10.0,
        ts_ms: 0,
        toxic: false,
    });
    drawdown.observe_mid(0.50);
    drawdown.observe_mid(0.30);
    assert!((drawdown.max_drawdown_pp - 200.0).abs() < 1e-9);

    let (n, mean) = conditional_markout_5s(
        &[
            (0.90, Some(3.0)),
            (0.80, Some(-1.0)),
            (0.40, Some(8.0)),
            (0.95, None),
        ],
        0.75,
    );
    assert_eq!(n, 2);
    assert!((mean - 1.0).abs() < 1e-9);
    let first_markout = signed_markouts_pp(0.40, [Some(0.41), Some(0.39), None, None, None])[0]
        .expect("the first markout is available");
    assert!((first_markout - 1.0).abs() < 1e-9);
}

fn event_tags(event: &StorageEvent) -> Option<&ExperimentTags> {
    match event {
        StorageEvent::JevSignal { tags, .. }
        | StorageEvent::PaperDecision { tags, .. }
        | StorageEvent::MakerMarkout { tags, .. }
        | StorageEvent::PaperFill { tags, .. }
        | StorageEvent::SignalMarkout { tags, .. }
        | StorageEvent::PaperEquity { tags, .. } => Some(tags),
        StorageEvent::Trade { .. }
        | StorageEvent::TopOfBook { .. }
        | StorageEvent::BookSnapshot { .. }
        | StorageEvent::MarketFeatures { .. }
        | StorageEvent::ExternalTick { .. }
        | StorageEvent::NewsItem { .. }
        | StorageEvent::Resolution { .. }
        | StorageEvent::AbPair { .. }
        | StorageEvent::TradeEpisode { .. } => None,
    }
}

fn table_name(event: &StorageEvent) -> &'static str {
    match event {
        StorageEvent::JevSignal { .. } => "jev_signals",
        StorageEvent::PaperDecision { .. } => "paper_decisions",
        StorageEvent::MakerMarkout { .. } => "maker_markouts",
        StorageEvent::AbPair { .. } => "ab_pairs",
        StorageEvent::PaperFill { .. } => "paper_fills",
        StorageEvent::PaperEquity { .. } => "paper_equity",
        StorageEvent::SignalMarkout { .. } => "signal_markouts",
        StorageEvent::Trade { .. }
        | StorageEvent::TopOfBook { .. }
        | StorageEvent::BookSnapshot { .. }
        | StorageEvent::MarketFeatures { .. }
        | StorageEvent::ExternalTick { .. }
        | StorageEvent::NewsItem { .. }
        | StorageEvent::Resolution { .. } => "other",
        StorageEvent::TradeEpisode { .. } => "trade_episodes",
    }
}

#[test]
fn storage_events_preserve_pair_tags_variant_tags_and_questdb_table_contract() {
    let tags = ExperimentTags::new("run-integration", "pair-btc-0001", "BTC-5m", "BTC", "5m");
    let control = StorageEvent::jev_signal_tagged(
        1_000_000,
        "btc-condition",
        "control-hash",
        "{}",
        "{}",
        1,
        50,
        Trigger::PriceMove,
        Variant::Control,
        tags.clone(),
        &signal(),
        100,
        50,
    );
    let quant = StorageEvent::jev_signal_tagged(
        1_000_000,
        "btc-condition",
        "quant-hash",
        "{\"quant\":{}}",
        "{}",
        1,
        50,
        Trigger::PriceMove,
        Variant::QuantV1,
        tags.clone(),
        &signal(),
        120,
        60,
    );
    let events = [
        control,
        quant,
        StorageEvent::PaperDecision {
            ts: 1_000_000,
            condition_id: "btc-condition".to_owned(),
            variant: Variant::Control,
            jev_ts: 1_000_000,
            edge: 0.02,
            threshold: 0.01,
            decision: "QUOTE".to_owned(),
            paper_price: 0.41,
            size: 10.0,
            fair_value: 0.43,
            tags: tags.clone(),
        },
        StorageEvent::MakerMarkout {
            ts: 6_000_000,
            condition_id: "btc-condition".to_owned(),
            variant: Variant::QuantV1,
            jev_ts: 1_000_000,
            side: "BUY".to_owned(),
            price: 0.41,
            size: 10.0,
            mid_1s: 0.42,
            mid_5s: 0.44,
            mid_10s: 0.44,
            mid_30s: 0.44,
            mid_60s: 0.44,
            pnl_1s_pp: 1.0,
            pnl_5s_pp: 3.0,
            pnl_10s_pp: 3.0,
            pnl_30s_pp: 3.0,
            pnl_60s_pp: 3.0,
            tags: tags.clone(),
        },
        StorageEvent::AbPair {
            ts: 1_000_000,
            run_id: tags.run_id.clone(),
            pair_id: tags.pair_id.clone(),
            market_id: tags.market_id.clone(),
            asset: tags.asset.clone(),
            horizon: tags.horizon.clone(),
            condition_id: "btc-condition".to_owned(),
            state_seq: 1,
            observed_at_ms: 1_000,
            status: "complete".to_owned(),
            control_ok: 1,
            quant_ok: 1,
        },
        StorageEvent::PaperFill {
            ts: 2_000_000,
            order_id: 1,
            condition_id: "btc-condition".to_owned(),
            variant: Variant::Control,
            side: "BUY".to_owned(),
            price: 0.41,
            size: 9.0,
            filled_at_ms: 2_000,
            maker: 1,
            tags: tags.clone(),
        },
        StorageEvent::PaperEquity {
            ts: 3_000_000,
            condition_id: "btc-condition".to_owned(),
            variant: Variant::QuantV1,
            position: 9.0,
            realized_pnl: 0.0,
            unrealized_pnl: 2.7,
            total_pnl: 2.7,
            exposure: 3.69,
            tags: tags.clone(),
        },
    ];
    let expected_tables = [
        "jev_signals",
        "jev_signals",
        "paper_decisions",
        "maker_markouts",
        "ab_pairs",
        "paper_fills",
        "paper_equity",
    ];

    assert_eq!(events.len(), expected_tables.len());
    for (event, expected_table) in events.iter().zip(expected_tables) {
        assert_eq!(table_name(event), expected_table);
        if let Some(event_tags) = event_tags(event) {
            assert_eq!(event_tags, &tags);
        }
    }

    let variants = events
        .iter()
        .filter_map(|event| match event {
            StorageEvent::JevSignal { variant, tags, .. } => Some((*variant, tags.pair_id.clone())),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        variants,
        vec![
            (Variant::Control, "pair-btc-0001".to_owned()),
            (Variant::QuantV1, "pair-btc-0001".to_owned())
        ]
    );
}
