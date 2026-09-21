use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use crate::{
    domain::Trigger,
    jev::V1Signal,
    replay::{ExitType, Side, TradeEpisode},
};
use questdb::ingress::{Buffer, ProtocolVersion, Sender, TimestampMicros};
use serde::{Deserialize, Serialize};

/// Shadow-evaluation branch persisted alongside storage events.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum Variant {
    #[serde(rename = "CONTROL")]
    Control,
    #[serde(rename = "QUANT_V1")]
    QuantV1,
}

impl Variant {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Control => "CONTROL",
            Self::QuantV1 => "QUANT_V1",
        }
    }
}

/// Dimensions that let every row group by run, A/B pair, and contract.
///
/// `run_id` identifies one shadow/paper process invocation; `pair_id` joins
/// the CONTROL and QUANT_V1 rows evaluated on the same snapshot; `market_id`
/// is the canonical contract tag (`BTC-5m`); `asset`/`horizon` repeat the
/// contract axes for direct `GROUP BY` without parsing tags. An empty
/// `pair_id` means that the row has no pair attribution: this is valid in
/// production for early skips and fills/equity samples not attributable to an
/// evaluated pair. Other empty dimensions still mean "unknown".
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ExperimentTags {
    pub run_id: String,
    pub pair_id: String,
    pub market_id: String,
    pub asset: String,
    pub horizon: String,
}

impl ExperimentTags {
    /// Creates tags for an evaluated pair or an explicitly unpaired row.
    #[must_use]
    pub fn new(
        run_id: impl Into<String>,
        pair_id: impl Into<String>,
        market_id: impl Into<String>,
        asset: impl Into<String>,
        horizon: impl Into<String>,
    ) -> Self {
        Self {
            run_id: run_id.into(),
            pair_id: pair_id.into(),
            market_id: market_id.into(),
            asset: asset.into(),
            horizon: horizon.into(),
        }
    }
}
use tokio::{sync::mpsc, task::JoinHandle};

/// Serializable replay ledger row destined for QuestDB and JSON consumers.
///
/// The row retains the complete ledger lifecycle even when the current
/// `trade_episodes` schema only ingests its stable execution/accounting
/// columns. Provenance defaults are deliberately explicit: the replay runner
/// in Task 10 is responsible for setting fill profile, maker attribution, fee
/// regime, prompt version, and Jev model before persistence.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct TradeEpisodeRow {
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
    pub intended_stake_usd: f64,
    pub actual_notional_usd: f64,
    pub rounding_delta_usd: f64,
    pub fill_ts_ms: Option<i64>,
    pub fill_price: Option<f64>,
    pub fill_qty: Option<f64>,
    pub exit_type: ExitType,
    pub exit_price: Option<f64>,
    pub exit_ts_ms: Option<i64>,
    pub exit_signal_ts_ms: Option<i64>,
    pub exit_arrival_ts_ms: Option<i64>,
    pub exit_fill_ts_ms: Option<i64>,
    pub resolution_at_ms: Option<i64>,
    pub resolution_outcome: Option<String>,
    pub resolution_provenance: Option<String>,
    pub exit_submit_latency_ms: u64,
    pub gross_pnl_usd: f64,
    pub fees_usd: f64,
    pub rebates_usd: f64,
    pub net_pnl_usd: f64,
    pub max_adverse_excursion_usd: f64,
    pub max_favorable_excursion_usd: f64,
    pub capital_seconds_usd_s: f64,
    pub pnl_historical_usd: Option<f64>,
    pub pnl_current_usd: Option<f64>,
    pub fill_profile: String,
    pub is_maker: bool,
    pub fee_regime: String,
    pub hedge_pair_id: Option<String>,
    pub prompt_version: Option<String>,
    pub jev_model: Option<String>,
}

impl From<&TradeEpisode> for TradeEpisodeRow {
    fn from(episode: &TradeEpisode) -> Self {
        Self {
            episode_id: episode.episode_id.clone(),
            strategy_version: episode.strategy_version.clone(),
            market: episode.market.clone(),
            asset: episode.asset.clone(),
            horizon: episode.horizon.clone(),
            signal_ts_ms: episode.signal_ts_ms,
            jev_start_ts_ms: episode.jev_start_ts_ms,
            jev_latency_ms: episode.jev_latency_ms,
            submit_latency_ms: episode.submit_latency_ms,
            order_arrival_ts_ms: episode.order_arrival_ts_ms,
            side: episode.side,
            limit_price: episode.limit_price,
            stake_usd: episode.stake_usd,
            shares: episode.shares,
            intended_stake_usd: episode.intended_stake_usd,
            actual_notional_usd: episode.actual_notional_usd,
            rounding_delta_usd: episode.rounding_delta_usd,
            fill_ts_ms: episode.fill_ts_ms,
            fill_price: episode.fill_price,
            fill_qty: episode.fill_qty,
            exit_type: episode.exit_type,
            exit_price: episode.exit_price,
            exit_ts_ms: episode.exit_ts_ms,
            exit_signal_ts_ms: episode.exit_signal_ts_ms,
            exit_arrival_ts_ms: episode.exit_arrival_ts_ms,
            exit_fill_ts_ms: episode.exit_fill_ts_ms,
            resolution_at_ms: episode.resolution_at_ms,
            resolution_outcome: episode.resolution_outcome.clone(),
            resolution_provenance: episode.resolution_provenance.clone(),
            exit_submit_latency_ms: episode.exit_submit_latency_ms,
            gross_pnl_usd: episode.gross_pnl_usd,
            fees_usd: episode.fees_usd,
            rebates_usd: episode.rebates_usd,
            net_pnl_usd: episode.net_pnl_usd,
            max_adverse_excursion_usd: episode.max_adverse_excursion_usd,
            max_favorable_excursion_usd: episode.max_favorable_excursion_usd,
            capital_seconds_usd_s: episode.capital_seconds_usd_s,
            pnl_historical_usd: episode.pnl_historical_usd,
            pnl_current_usd: episode.pnl_current_usd,
            // These defaults are replaced by the Task 10 runner through the
            // ledger setters before it emits the storage event.
            fill_profile: episode
                .fill_profile
                .clone()
                .unwrap_or_else(|| "unknown".to_owned()),
            is_maker: episode.is_maker.unwrap_or(false),
            fee_regime: episode
                .fee_regime
                .clone()
                .unwrap_or_else(|| "unknown".to_owned()),
            hedge_pair_id: episode.hedge_pair_id.clone(),
            prompt_version: episode.prompt_version.clone(),
            jev_model: episode.jev_model.clone(),
        }
    }
}

/// A row destined for one of the QuestDB tables defined in `questdb/schema.sql`.
///
/// Timestamps are UNIX epoch microseconds. JSON columns remain strings so the
/// storage boundary does not depend on `serde_json::Value`.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug)]
pub enum StorageEvent {
    /// One executed trade.
    Trade {
        ts: i64,
        condition_id: String,
        token_id: String,
        outcome: String,
        price: f64,
        size: f64,
        fee_bps: f64,
        tx_hash: String,
    },
    /// One complete replay order episode, including ledger and provenance.
    TradeEpisode { row: TradeEpisodeRow },
    /// The current best bid and ask for one token.
    TopOfBook {
        ts: i64,
        token_id: String,
        best_bid: f64,
        best_ask: f64,
        spread: f64,
        mid: f64,
    },
    /// A sampled order-book snapshot.
    BookSnapshot {
        ts: i64,
        token_id: String,
        book_hash: String,
        bids_json: String,
        asks_json: String,
        imbalance: f64,
        microprice: f64,
    },
    /// Derived features for one market.
    MarketFeatures {
        ts: i64,
        condition_id: String,
        mid: f64,
        spread: f64,
        momentum_5m: f64,
        momentum_1h: f64,
        volatility_1h: f64,
        volume_24h: f64,
        liquidity: f64,
        one_day_change: f64,
        minutes_to_resolution: i64,
        distance_to_target: Option<f64>,
    },
    /// One external venue tick or aggregate sample.
    ExternalTick {
        ts: i64,
        symbol: String,
        price: f64,
        ret_5m: f64,
        ret_1h: f64,
        vol_1h: f64,
    },
    /// A normalized, English news item. `condition_id` is nullable in QuestDB.
    NewsItem {
        ts: i64,
        source: String,
        condition_id: Option<String>,
        text_en: String,
        published_minutes_ago: i64,
        dedup_hash: String,
    },
    /// One V1 Jev call, including the exact state that produced its signal.
    ///
    /// The `tokens_in` and `tokens_out` fields are retained for the eventual
    /// Jev usage payload, but remain zero until the client measures usage.
    JevSignal {
        ts: i64,
        condition_id: String,
        state_seq: i64,
        state_hash: String,
        state_json: String,
        questions_json: String,
        yes_pressure_5s: f64,
        no_pressure_5s: f64,
        move_persists: f64,
        underreact_up: f64,
        underreact_down: f64,
        repricing_up_3_plus: f64,
        repricing_up_2: f64,
        repricing_up_1: f64,
        repricing_flat: f64,
        repricing_down_1: f64,
        repricing_down_2: f64,
        repricing_down_3_plus: f64,
        repricing_confidence: f64,
        fill_before_decay: f64,
        fill_toxic: f64,
        latency_ms: i64,
        tokens_in: i64,
        tokens_out: i64,
        trigger: String,
        variant: Variant,
        tags: ExperimentTags,
    },
    /// A paper-trading decision linked to its Jev signal timestamp.
    ///
    /// V1 uses `QUOTE` for a decision returned by `decide_quote` and `SKIP`
    /// otherwise; `TRADE` is not part of the V1 paper vocabulary.
    PaperDecision {
        ts: i64,
        condition_id: String,
        variant: Variant,
        jev_ts: i64,
        edge: f64,
        threshold: f64,
        decision: String,
        paper_price: f64,
        size: f64,
        fair_value: f64,
        tags: ExperimentTags,
    },
    /// The resolution label for a market.
    Resolution {
        condition_id: String,
        winning_token_id: String,
        winning_outcome: String,
        resolved_ts: i64,
    },
    /// Maker markouts at the five V1 horizons: +1/+5/+10/+30/+60s.
    MakerMarkout {
        ts: i64,
        condition_id: String,
        variant: Variant,
        jev_ts: i64,
        side: String,
        price: f64,
        size: f64,
        mid_1s: f64,
        mid_5s: f64,
        mid_10s: f64,
        mid_30s: f64,
        mid_60s: f64,
        pnl_1s_pp: f64,
        pnl_5s_pp: f64,
        pnl_10s_pp: f64,
        pnl_30s_pp: f64,
        pnl_60s_pp: f64,
        tags: ExperimentTags,
    },
    /// One A/B pair lifecycle row: joins the CONTROL and QUANT_V1 rows that
    /// share a `pair_id`. `status` is `complete` when both branches produced
    /// a usable evaluation and `incomplete` otherwise; `control_ok`/`quant_ok`
    /// are 1/0 flags. Incomplete pairs must be excluded from paired A/B
    /// analysis and reported separately.
    AbPair {
        ts: i64,
        run_id: String,
        pair_id: String,
        market_id: String,
        asset: String,
        horizon: String,
        condition_id: String,
        state_seq: i64,
        observed_at_ms: i64,
        status: String,
        control_ok: i64,
        quant_ok: i64,
    },
    /// One paper fill from a variant-owned paper book. Never a live fill.
    PaperFill {
        ts: i64,
        order_id: i64,
        condition_id: String,
        variant: Variant,
        side: String,
        price: f64,
        size: f64,
        filled_at_ms: i64,
        maker: i64,
        tags: ExperimentTags,
    },
    /// One sampled paper-equity row for a variant portfolio.
    ///
    /// `position` is open YES shares; PnL is in price-points x size, the same
    /// unit as the replay [`crate::replay::Portfolio`]. `exposure` is open
    /// inventory at its average entry cost, independent of whether a mid is
    /// available for `unrealized_pnl`.
    PaperEquity {
        ts: i64,
        condition_id: String,
        variant: Variant,
        position: f64,
        realized_pnl: f64,
        unrealized_pnl: f64,
        total_pnl: f64,
        exposure: f64,
        tags: ExperimentTags,
    },
    /// One forward drift observation for a usable Jev evaluation, QUOTE or
    /// SKIP (SIGNAL RESEARCH). `ref_price` is the eval-time mid; `mo_*_pp`
    /// are BUY-signed drift in percentage points. Maker execution labels live
    /// in `MakerMarkout`; join the two datasets by `pair_id` + `variant`.
    SignalMarkout {
        ts: i64,
        condition_id: String,
        variant: Variant,
        jev_ts: i64,
        ref_price: f64,
        mid_1s: f64,
        mid_5s: f64,
        mid_10s: f64,
        mid_30s: f64,
        mid_60s: f64,
        mo_1s_pp: f64,
        mo_5s_pp: f64,
        mo_10s_pp: f64,
        mo_30s_pp: f64,
        mo_60s_pp: f64,
        tags: ExperimentTags,
    },
}

impl From<&TradeEpisode> for StorageEvent {
    fn from(episode: &TradeEpisode) -> Self {
        Self::trade_episode(episode)
    }
}

impl StorageEvent {
    /// Build a storage event from a replay ledger episode.
    #[must_use]
    pub fn trade_episode(episode: &TradeEpisode) -> Self {
        Self::TradeEpisode {
            row: episode.into(),
        }
    }

    /// Build a V1 Jev signal row from a validated signal and its state identity.
    ///
    /// The Jev client currently does not expose usage, so this helper stores
    /// `tokens_in` and `tokens_out` as zero until the client measures them.
    /// Production callers pass explicit [`ExperimentTags`]; the legacy
    /// tag-free form below exists only for unit tests.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn jev_signal_tagged(
        ts: i64,
        condition_id: impl Into<String>,
        state_hash: impl Into<String>,
        state_json: impl Into<String>,
        questions_json: impl Into<String>,
        state_seq: i64,
        latency_ms: i64,
        trigger: Trigger,
        variant: Variant,
        tags: ExperimentTags,
        signal: &V1Signal,
        tokens_in: u64,
        tokens_out: u64,
    ) -> Self {
        Self::JevSignal {
            ts,
            condition_id: condition_id.into(),
            state_seq,
            state_hash: state_hash.into(),
            state_json: state_json.into(),
            questions_json: questions_json.into(),
            yes_pressure_5s: signal.yes_pressure_5s,
            no_pressure_5s: signal.no_pressure_5s,
            move_persists: signal.move_persists,
            underreact_up: signal.underreact_up,
            underreact_down: signal.underreact_down,
            repricing_up_3_plus: signal.repricing.up_3_plus,
            repricing_up_2: signal.repricing.up_2,
            repricing_up_1: signal.repricing.up_1,
            repricing_flat: signal.repricing.flat,
            repricing_down_1: signal.repricing.down_1,
            repricing_down_2: signal.repricing.down_2,
            repricing_down_3_plus: signal.repricing.down_3_plus,
            repricing_confidence: signal.repricing_confidence,
            fill_before_decay: signal.fill_before_decay,
            fill_toxic: signal.fill_toxic,
            latency_ms,
            tokens_in: tokens_in as i64,
            tokens_out: tokens_out as i64,
            trigger: trigger.as_str().to_owned(),
            variant,
            tags,
        }
    }

    /// Test-only Jev signal row with empty [`ExperimentTags`] and zero usage.
    ///
    /// Production code must use [`Self::jev_signal_tagged`].
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn jev_signal(
        ts: i64,
        condition_id: impl Into<String>,
        state_hash: impl Into<String>,
        state_json: impl Into<String>,
        questions_json: impl Into<String>,
        state_seq: i64,
        latency_ms: i64,
        trigger: Trigger,
        variant: Variant,
        signal: &V1Signal,
    ) -> Self {
        Self::jev_signal_tagged(
            ts,
            condition_id,
            state_hash,
            state_json,
            questions_json,
            state_seq,
            latency_ms,
            trigger,
            variant,
            ExperimentTags::default(),
            signal,
            0,
            0,
        )
    }
}

/// A snapshot of the writer counters.
///
/// `dropped_full` is both the drop counter and the surfaced lag sample: a full
/// queue is never hidden from callers. Counters use atomics so querying them is
/// safe while the background writer is active.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WriterMetrics {
    pub sent: u64,
    pub dropped_full: u64,
    pub write_errors: u64,
}

#[derive(Debug, Default)]
struct MetricsInner {
    sent: AtomicU64,
    dropped_full: AtomicU64,
    write_errors: AtomicU64,
}

/// The non-blocking producer side of the QuestDB writer.
///
/// The queue uses a newest-wins drop policy: when it is full, the incoming
/// newest event is dropped and the already queued rows are preserved. This
/// keeps the bounded channel flowing without ever making trading wait for
/// storage; `dropped_full` exposes every such lag sample to the caller.
#[derive(Clone)]
pub struct QuestDbHandle {
    sender: mpsc::Sender<StorageEvent>,
    metrics: Arc<MetricsInner>,
}

/// Result of attempting to enqueue one storage event.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StorageSendResult {
    Sent,
    Dropped,
}

impl QuestDbHandle {
    /// Enqueue an event without waiting for storage capacity.
    ///
    /// A full queue returns [`StorageSendResult::Dropped`] immediately. A
    /// closed writer is also reported as dropped because the event was not
    /// accepted; it increments `write_errors` rather than pretending the queue
    /// was full.
    pub fn try_send(&self, event: StorageEvent) -> StorageSendResult {
        match self.sender.try_send(event) {
            Ok(()) => {
                self.metrics.sent.fetch_add(1, Ordering::Relaxed);
                StorageSendResult::Sent
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.metrics.dropped_full.fetch_add(1, Ordering::Relaxed);
                tracing::debug!("QuestDB writer queue is full; dropped an event as a lag sample");
                StorageSendResult::Dropped
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.metrics.write_errors.fetch_add(1, Ordering::Relaxed);
                tracing::debug!("QuestDB writer queue is closed; dropped an event");
                StorageSendResult::Dropped
            }
        }
    }

    /// Read a consistent snapshot of the writer counters.
    #[must_use]
    pub fn metrics(&self) -> WriterMetrics {
        WriterMetrics {
            sent: self.metrics.sent.load(Ordering::Relaxed),
            dropped_full: self.metrics.dropped_full.load(Ordering::Relaxed),
            write_errors: self.metrics.write_errors.load(Ordering::Relaxed),
        }
    }
}

/// Factory for the decoupled QuestDB writer task.
pub struct QuestDbWriter;

impl QuestDbWriter {
    /// Spawn a blocking ILP writer behind a bounded, non-blocking producer queue.
    ///
    /// The QuestDB connection is created inside the blocking task, not on the
    /// caller's thread. `capacity == 0` is normalized to one so the producer
    /// API remains usable without a panic.
    pub fn spawn(ilp_addr: &str, capacity: usize) -> (QuestDbHandle, JoinHandle<()>) {
        let (sender, receiver) = mpsc::channel(capacity.max(1));
        let metrics = Arc::new(MetricsInner::default());
        let handle = QuestDbHandle {
            sender,
            metrics: Arc::clone(&metrics),
        };
        let ilp_addr = ilp_addr.to_owned();
        let task = tokio::task::spawn_blocking(move || run_writer(&ilp_addr, receiver, metrics));

        (handle, task)
    }
}

fn run_writer(
    ilp_addr: &str,
    mut receiver: mpsc::Receiver<StorageEvent>,
    metrics: Arc<MetricsInner>,
) {
    let configuration = format!("tcp::addr={ilp_addr};protocol_version=1;");
    let mut sender = match Sender::from_conf(configuration) {
        Ok(sender) => Some(sender),
        Err(error) => {
            tracing::error!(
                "QuestDB writer could not connect: {error}; storage will remain best-effort"
            );
            None
        }
    };

    while let Some(event) = receiver.blocking_recv() {
        let Some(sender) = sender.as_mut() else {
            metrics.write_errors.fetch_add(1, Ordering::Relaxed);
            continue;
        };

        let mut row = match build_row(&event) {
            Ok(row) => row,
            Err(error) => {
                metrics.write_errors.fetch_add(1, Ordering::Relaxed);
                tracing::error!("QuestDB row construction failed: {error}; continuing");
                continue;
            }
        };

        if let Err(error) = sender.flush(&mut row) {
            metrics.write_errors.fetch_add(1, Ordering::Relaxed);
            tracing::error!("QuestDB write failed: {error}; continuing without affecting trading");
        }
    }
}

/// Build one ILP row without opening a connection or sending any bytes.
fn build_row(event: &StorageEvent) -> questdb::Result<Buffer> {
    let mut buffer = Buffer::new(ProtocolVersion::V1);
    append_event(&mut buffer, event)?;
    Ok(buffer)
}

fn timestamp_micros(ts_ms: i64) -> TimestampMicros {
    TimestampMicros::new(ts_ms.saturating_mul(1_000))
}

fn append_event(buffer: &mut Buffer, event: &StorageEvent) -> questdb::Result<()> {
    match event {
        StorageEvent::Trade {
            ts,
            condition_id,
            token_id,
            outcome,
            price,
            size,
            fee_bps,
            tx_hash,
        } => {
            buffer
                .table("trades")?
                .symbol("condition_id", condition_id)?
                .symbol("token_id", token_id)?
                .symbol("outcome", outcome)?
                .column_f64("price", *price)?
                .column_f64("size", *size)?
                .column_f64("fee_bps", *fee_bps)?
                .column_str("tx_hash", tx_hash)?
                .at(TimestampMicros::new(*ts))?;
        }
        StorageEvent::TradeEpisode { row } => {
            let side = match row.side {
                Side::BuyYes => "buy_yes",
                Side::BuyNo => "buy_no",
            };
            let exit_type = match row.exit_type {
                ExitType::Resolution => "resolution",
                ExitType::Hedge => "hedge",
                ExitType::Sell => "sell",
                ExitType::Stop => "stop",
                ExitType::NoFill => "no_fill",
            };
            let ilp_row = buffer
                .table("trade_episodes")?
                .symbol("episode_id", &row.episode_id)?
                .symbol("strategy_version", &row.strategy_version)?
                .symbol("market", &row.market)?
                .symbol("asset", &row.asset)?
                .symbol("horizon", &row.horizon)?
                .symbol("side", side)?
                .symbol("exit_type", exit_type)?
                .symbol("fill_profile", &row.fill_profile)?
                .symbol("fee_regime", &row.fee_regime)?;
            if let Some(hedge_pair_id) = &row.hedge_pair_id {
                ilp_row.symbol("hedge_pair_id", hedge_pair_id)?;
            }
            if let Some(prompt_version) = &row.prompt_version {
                ilp_row.symbol("prompt_version", prompt_version)?;
            }
            if let Some(jev_model) = &row.jev_model {
                ilp_row.symbol("jev_model", jev_model)?;
            }
            ilp_row
                .column_i64("jev_start_ts_ms", row.jev_start_ts_ms)?
                .column_i64("jev_latency_ms", row.jev_latency_ms as i64)?
                .column_i64("submit_latency_ms", row.submit_latency_ms as i64)?
                .column_i64("order_arrival_ts_ms", row.order_arrival_ts_ms)?
                .column_f64("limit_price", row.limit_price)?
                .column_f64("stake_usd", row.stake_usd)?
                .column_f64("shares", row.shares)?
                .column_f64("gross_pnl_usd", row.gross_pnl_usd)?
                .column_f64("fees_usd", row.fees_usd)?
                .column_f64("rebates_usd", row.rebates_usd)?
                .column_f64("net_pnl_usd", row.net_pnl_usd)?
                .column_f64("max_adverse_excursion_usd", row.max_adverse_excursion_usd)?
                .column_f64(
                    "max_favorable_excursion_usd",
                    row.max_favorable_excursion_usd,
                )?
                .column_f64("capital_seconds_usd_s", row.capital_seconds_usd_s)?
                .column_bool("is_maker", row.is_maker)?;

            if let Some(fill_ts_ms) = row.fill_ts_ms {
                ilp_row.column_ts("fill_ts_ms", timestamp_micros(fill_ts_ms))?;
            }
            if let Some(fill_price) = row.fill_price {
                ilp_row.column_f64("fill_price", fill_price)?;
            }
            if let Some(fill_qty) = row.fill_qty {
                ilp_row.column_f64("fill_qty", fill_qty)?;
            }
            if let Some(exit_price) = row.exit_price {
                ilp_row.column_f64("exit_price", exit_price)?;
            }
            if let Some(exit_ts_ms) = row.exit_ts_ms {
                ilp_row.column_ts("exit_ts_ms", timestamp_micros(exit_ts_ms))?;
            }
            ilp_row.at(timestamp_micros(row.signal_ts_ms))?;
        }
        StorageEvent::TopOfBook {
            ts,
            token_id,
            best_bid,
            best_ask,
            spread,
            mid,
        } => {
            buffer
                .table("top_of_book")?
                .symbol("token_id", token_id)?
                .column_f64("best_bid", *best_bid)?
                .column_f64("best_ask", *best_ask)?
                .column_f64("spread", *spread)?
                .column_f64("mid", *mid)?
                .at(TimestampMicros::new(*ts))?;
        }
        StorageEvent::BookSnapshot {
            ts,
            token_id,
            book_hash,
            bids_json,
            asks_json,
            imbalance,
            microprice,
        } => {
            buffer
                .table("book_snapshots")?
                .symbol("token_id", token_id)?
                .column_str("book_hash", book_hash)?
                .column_str("bids_json", bids_json)?
                .column_str("asks_json", asks_json)?
                .column_f64("imbalance", *imbalance)?
                .column_f64("microprice", *microprice)?
                .at(TimestampMicros::new(*ts))?;
        }
        StorageEvent::MarketFeatures {
            ts,
            condition_id,
            mid,
            spread,
            momentum_5m,
            momentum_1h,
            volatility_1h,
            volume_24h,
            liquidity,
            one_day_change,
            minutes_to_resolution,
            distance_to_target,
        } => {
            let row = buffer
                .table("market_features")?
                .symbol("condition_id", condition_id)?
                .column_f64("mid", *mid)?
                .column_f64("spread", *spread)?
                .column_f64("momentum_5m", *momentum_5m)?
                .column_f64("momentum_1h", *momentum_1h)?
                .column_f64("volatility_1h", *volatility_1h)?
                .column_f64("volume_24h", *volume_24h)?
                .column_f64("liquidity", *liquidity)?
                .column_f64("one_day_change", *one_day_change)?
                .column_i64("minutes_to_resolution", *minutes_to_resolution)?;
            if let Some(distance_to_target) = distance_to_target {
                row.column_f64("distance_to_target", *distance_to_target)?;
            }
            row.at(TimestampMicros::new(*ts))?;
        }
        StorageEvent::ExternalTick {
            ts,
            symbol,
            price,
            ret_5m,
            ret_1h,
            vol_1h,
        } => {
            buffer
                .table("external_ticks")?
                .symbol("symbol", symbol)?
                .column_f64("price", *price)?
                .column_f64("ret_5m", *ret_5m)?
                .column_f64("ret_1h", *ret_1h)?
                .column_f64("vol_1h", *vol_1h)?
                .at(TimestampMicros::new(*ts))?;
        }
        StorageEvent::NewsItem {
            ts,
            source,
            condition_id,
            text_en,
            published_minutes_ago,
            dedup_hash,
        } => {
            let row = buffer.table("news_items")?.symbol("source", source)?;
            let row = if let Some(condition_id) = condition_id {
                row.symbol("condition_id", condition_id)?
            } else {
                row
            };
            row.column_str("text_en", text_en)?
                .column_i64("published_minutes_ago", *published_minutes_ago)?
                .column_str("dedup_hash", dedup_hash)?
                .at(TimestampMicros::new(*ts))?;
        }
        StorageEvent::JevSignal {
            ts,
            condition_id,
            state_seq,
            state_hash,
            state_json,
            questions_json,
            yes_pressure_5s,
            no_pressure_5s,
            move_persists,
            underreact_up,
            underreact_down,
            repricing_up_3_plus,
            repricing_up_2,
            repricing_up_1,
            repricing_flat,
            repricing_down_1,
            repricing_down_2,
            repricing_down_3_plus,
            repricing_confidence,
            fill_before_decay,
            fill_toxic,
            latency_ms,
            tokens_in,
            tokens_out,
            trigger,
            variant,
            tags,
        } => {
            buffer
                .table("jev_signals")?
                .symbol("condition_id", condition_id)?
                .symbol("trigger", trigger)?
                .symbol("variant", variant.as_str())?
                .symbol("run_id", &tags.run_id)?
                .symbol("pair_id", &tags.pair_id)?
                .symbol("market_id", &tags.market_id)?
                .symbol("asset", &tags.asset)?
                .symbol("horizon", &tags.horizon)?
                .column_i64("state_seq", *state_seq)?
                .column_str("state_hash", state_hash)?
                .column_str("state_json", state_json)?
                .column_str("questions_json", questions_json)?
                .column_f64("yes_pressure_5s", *yes_pressure_5s)?
                .column_f64("no_pressure_5s", *no_pressure_5s)?
                .column_f64("move_persists", *move_persists)?
                .column_f64("underreact_up", *underreact_up)?
                .column_f64("underreact_down", *underreact_down)?
                .column_f64("repricing_up_3_plus", *repricing_up_3_plus)?
                .column_f64("repricing_up_2", *repricing_up_2)?
                .column_f64("repricing_up_1", *repricing_up_1)?
                .column_f64("repricing_flat", *repricing_flat)?
                .column_f64("repricing_down_1", *repricing_down_1)?
                .column_f64("repricing_down_2", *repricing_down_2)?
                .column_f64("repricing_down_3_plus", *repricing_down_3_plus)?
                .column_f64("repricing_confidence", *repricing_confidence)?
                .column_f64("fill_before_decay", *fill_before_decay)?
                .column_f64("fill_toxic", *fill_toxic)?
                .column_i64("latency_ms", *latency_ms)?
                .column_i64("tokens_in", *tokens_in)?
                .column_i64("tokens_out", *tokens_out)?
                .at(TimestampMicros::new(*ts))?;
        }
        StorageEvent::PaperDecision {
            ts,
            condition_id,
            variant,
            jev_ts,
            edge,
            threshold,
            decision,
            paper_price,
            size,
            fair_value,
            tags,
        } => {
            buffer
                .table("paper_decisions")?
                .symbol("condition_id", condition_id)?
                .symbol("decision", decision)?
                .symbol("variant", variant.as_str())?
                .symbol("run_id", &tags.run_id)?
                .symbol("pair_id", &tags.pair_id)?
                .symbol("market_id", &tags.market_id)?
                .symbol("asset", &tags.asset)?
                .symbol("horizon", &tags.horizon)?
                .column_ts("jev_ts", TimestampMicros::new(*jev_ts))?
                .column_f64("edge", *edge)?
                .column_f64("threshold", *threshold)?
                .column_f64("paper_price", *paper_price)?
                .column_f64("size", *size)?
                .column_f64("fair_value", *fair_value)?
                .at(TimestampMicros::new(*ts))?;
        }
        StorageEvent::Resolution {
            condition_id,
            winning_token_id,
            winning_outcome,
            resolved_ts,
        } => {
            buffer
                .table("resolutions")?
                .symbol("condition_id", condition_id)?
                .symbol("winning_token_id", winning_token_id)?
                .symbol("winning_outcome", winning_outcome)?
                .at(TimestampMicros::new(*resolved_ts))?;
        }
        StorageEvent::MakerMarkout {
            ts,
            condition_id,
            variant,
            jev_ts,
            side,
            price,
            size,
            mid_1s,
            mid_5s,
            mid_10s,
            mid_30s,
            mid_60s,
            pnl_1s_pp,
            pnl_5s_pp,
            pnl_10s_pp,
            pnl_30s_pp,
            pnl_60s_pp,
            tags,
        } => {
            buffer
                .table("maker_markouts")?
                .symbol("condition_id", condition_id)?
                .symbol("side", side)?
                .symbol("variant", variant.as_str())?
                .symbol("run_id", &tags.run_id)?
                .symbol("pair_id", &tags.pair_id)?
                .symbol("market_id", &tags.market_id)?
                .symbol("asset", &tags.asset)?
                .symbol("horizon", &tags.horizon)?
                .column_ts("jev_ts", TimestampMicros::new(*jev_ts))?
                .column_f64("price", *price)?
                .column_f64("size", *size)?
                .column_f64("mid_1s", *mid_1s)?
                .column_f64("mid_5s", *mid_5s)?
                .column_f64("mid_10s", *mid_10s)?
                .column_f64("mid_30s", *mid_30s)?
                .column_f64("mid_60s", *mid_60s)?
                .column_f64("pnl_1s_pp", *pnl_1s_pp)?
                .column_f64("pnl_5s_pp", *pnl_5s_pp)?
                .column_f64("pnl_10s_pp", *pnl_10s_pp)?
                .column_f64("pnl_30s_pp", *pnl_30s_pp)?
                .column_f64("pnl_60s_pp", *pnl_60s_pp)?
                .at(TimestampMicros::new(*ts))?;
        }
        StorageEvent::AbPair {
            ts,
            run_id,
            pair_id,
            market_id,
            asset,
            horizon,
            condition_id,
            state_seq,
            observed_at_ms,
            status,
            control_ok,
            quant_ok,
        } => {
            buffer
                .table("ab_pairs")?
                .symbol("run_id", run_id)?
                .symbol("pair_id", pair_id)?
                .symbol("market_id", market_id)?
                .symbol("asset", asset)?
                .symbol("horizon", horizon)?
                .symbol("condition_id", condition_id)?
                .symbol("status", status)?
                .column_i64("state_seq", *state_seq)?
                .column_i64("observed_at_ms", *observed_at_ms)?
                .column_i64("control_ok", *control_ok)?
                .column_i64("quant_ok", *quant_ok)?
                .at(TimestampMicros::new(*ts))?;
        }
        StorageEvent::PaperFill {
            ts,
            order_id,
            condition_id,
            variant,
            side,
            price,
            size,
            filled_at_ms,
            maker,
            tags,
        } => {
            buffer
                .table("paper_fills")?
                .symbol("condition_id", condition_id)?
                .symbol("variant", variant.as_str())?
                .symbol("side", side)?
                .symbol("run_id", &tags.run_id)?
                .symbol("pair_id", &tags.pair_id)?
                .symbol("market_id", &tags.market_id)?
                .symbol("asset", &tags.asset)?
                .symbol("horizon", &tags.horizon)?
                .column_i64("order_id", *order_id)?
                .column_f64("price", *price)?
                .column_f64("size", *size)?
                .column_i64("filled_at_ms", *filled_at_ms)?
                .column_i64("maker", *maker)?
                .at(TimestampMicros::new(*ts))?;
        }
        StorageEvent::PaperEquity {
            ts,
            condition_id,
            variant,
            position,
            realized_pnl,
            unrealized_pnl,
            total_pnl,
            exposure,
            tags,
        } => {
            buffer
                .table("paper_equity")?
                .symbol("condition_id", condition_id)?
                .symbol("variant", variant.as_str())?
                .symbol("run_id", &tags.run_id)?
                .symbol("pair_id", &tags.pair_id)?
                .symbol("market_id", &tags.market_id)?
                .symbol("asset", &tags.asset)?
                .symbol("horizon", &tags.horizon)?
                .column_f64("position", *position)?
                .column_f64("realized_pnl", *realized_pnl)?
                .column_f64("unrealized_pnl", *unrealized_pnl)?
                .column_f64("total_pnl", *total_pnl)?
                .column_f64("exposure", *exposure)?
                .at(TimestampMicros::new(*ts))?;
        }
        StorageEvent::SignalMarkout {
            ts,
            condition_id,
            variant,
            jev_ts,
            ref_price,
            mid_1s,
            mid_5s,
            mid_10s,
            mid_30s,
            mid_60s,
            mo_1s_pp,
            mo_5s_pp,
            mo_10s_pp,
            mo_30s_pp,
            mo_60s_pp,
            tags,
        } => {
            buffer
                .table("signal_markouts")?
                .symbol("condition_id", condition_id)?
                .symbol("variant", variant.as_str())?
                .symbol("run_id", &tags.run_id)?
                .symbol("pair_id", &tags.pair_id)?
                .symbol("market_id", &tags.market_id)?
                .symbol("asset", &tags.asset)?
                .symbol("horizon", &tags.horizon)?
                .column_ts("jev_ts", TimestampMicros::new(*jev_ts))?
                .column_f64("ref_price", *ref_price)?
                .column_f64("mid_1s", *mid_1s)?
                .column_f64("mid_5s", *mid_5s)?
                .column_f64("mid_10s", *mid_10s)?
                .column_f64("mid_30s", *mid_30s)?
                .column_f64("mid_60s", *mid_60s)?
                .column_f64("mo_1s_pp", *mo_1s_pp)?
                .column_f64("mo_5s_pp", *mo_5s_pp)?
                .column_f64("mo_10s_pp", *mo_10s_pp)?
                .column_f64("mo_30s_pp", *mo_30s_pp)?
                .column_f64("mo_60s_pp", *mo_60s_pp)?
                .at(TimestampMicros::new(*ts))?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{sync::mpsc as std_mpsc, thread, time::Duration};

    use super::{
        ExperimentTags, MetricsInner, QuestDbHandle, StorageEvent, StorageSendResult, Variant,
        build_row,
    };
    use crate::{
        domain::Trigger,
        jev::{TickDistribution, V1Signal},
        replay::{Side, TradeEpisode},
    };
    use tokio::sync::mpsc;

    fn row_text(event: &StorageEvent) -> String {
        String::from_utf8(
            build_row(event)
                .expect("row should build")
                .as_bytes()
                .to_vec(),
        )
        .expect("ILP row should be UTF-8")
    }

    #[test]
    fn trades_map_all_columns_without_sending() {
        let row = row_text(&StorageEvent::Trade {
            ts: 1_000_000,
            condition_id: "condition-1".to_owned(),
            token_id: "token-1".to_owned(),
            outcome: "YES".to_owned(),
            price: 0.42,
            size: 12.5,
            fee_bps: 2.0,
            tx_hash: "0xabc".to_owned(),
        });

        assert!(row.starts_with("trades,condition_id=condition-1,token_id=token-1,outcome=YES"));
        assert!(row.contains("price=0.42"));
        assert!(row.contains("size=12.5"));
        assert!(row.contains("fee_bps=2"));
        assert!(row.contains("tx_hash=\"0xabc\""));
    }

    #[test]
    fn trade_episodes_map_ledger_and_provenance_columns_without_io() {
        let mut episode = TradeEpisode::new(
            "episode-storage",
            "strategy-v1",
            "market-1",
            "BTC",
            "5m",
            1_000,
            900,
            50,
            50,
            Side::BuyYes,
            0.40,
            10.0,
        )
        .expect("valid episode");
        episode
            .apply_fill(1_200, 0.40, 25.0)
            .expect("fill arrives after order arrival");
        episode.set_fill_profile("conservative");
        episode.set_is_maker(true);
        episode.set_fee_regime("historical-2026");
        episode.set_hedge_pair_id("hedge:episode-storage");
        episode.set_prompt_version("prompt-v3");
        episode.set_jev_model("jev-latest");

        let row = row_text(&StorageEvent::trade_episode(&episode));
        assert!(row.starts_with("trade_episodes,episode_id=episode-storage"));
        assert!(row.contains("strategy_version=strategy-v1"));
        assert!(row.contains("fill_profile=conservative"));
        assert!(row.contains("is_maker=t"));
        assert!(row.contains("fee_regime=historical-2026"));
        assert!(row.contains("hedge_pair_id=hedge:episode-storage"));
        assert!(row.contains("prompt_version=prompt-v3"));
        assert!(row.contains("jev_model=jev-latest"));
        assert!(row.contains("fill_price=0.4"));
        assert!(row.contains("fill_qty=25"));
    }

    #[test]
    fn jev_signals_map_v1_columns_and_preserve_the_full_state() {
        let signal = V1Signal {
            yes_pressure_5s: 0.81,
            no_pressure_5s: 0.12,
            move_persists: 0.73,
            underreact_up: 0.88,
            underreact_down: 0.18,
            repricing: TickDistribution {
                up_3_plus: 0.10,
                up_2: 0.20,
                up_1: 0.44,
                flat: 0.12,
                down_1: 0.07,
                down_2: 0.04,
                down_3_plus: 0.03,
            },
            repricing_confidence: 0.79,
            fill_before_decay: 0.66,
            fill_toxic: 0.21,
        };
        let row = row_text(&StorageEvent::jev_signal(
            2_000_000,
            "condition-2",
            "state-hash",
            r#"{"market":{"question":"BTC above 100k"}}"#,
            r#"{"yes_pressure_5s":{"type":"noul"}}"#,
            17,
            101,
            Trigger::PriceMove,
            Variant::QuantV1,
            &signal,
        ));

        assert!(row.starts_with("jev_signals,condition_id=condition-2"));
        assert!(row.contains("state_seq=17i"));
        assert!(row.contains("state_hash=\"state-hash\""));
        assert!(
            row.contains(
                "state_json=\"{\\\"market\\\":{\\\"question\\\":\\\"BTC above 100k\\\"}}\""
            )
        );
        assert!(
            row.contains(
                "questions_json=\"{\\\"yes_pressure_5s\\\":{\\\"type\\\":\\\"noul\\\"}}\""
            )
        );
        for field in [
            "yes_pressure_5s=0.81",
            "no_pressure_5s=0.12",
            "move_persists=0.73",
            "underreact_up=0.88",
            "underreact_down=0.18",
            "repricing_up_3_plus=0.1",
            "repricing_up_2=0.2",
            "repricing_up_1=0.44",
            "repricing_flat=0.12",
            "repricing_down_1=0.07",
            "repricing_down_2=0.04",
            "repricing_down_3_plus=0.03",
            "repricing_confidence=0.79",
            "fill_before_decay=0.66",
            "fill_toxic=0.21",
            "latency_ms=101i",
            "tokens_in=0i",
            "tokens_out=0i",
            "trigger=price_move",
        ] {
            assert!(row.contains(field), "missing {field} in {row}");
        }
        assert!(row.contains("variant=QUANT_V1"));
    }

    #[test]
    fn tagged_rows_carry_run_pair_and_contract_dimensions() {
        let signal = V1Signal {
            yes_pressure_5s: 0.5,
            no_pressure_5s: 0.5,
            move_persists: 0.5,
            underreact_up: 0.5,
            underreact_down: 0.5,
            repricing: TickDistribution {
                up_3_plus: 0.1,
                up_2: 0.1,
                up_1: 0.2,
                flat: 0.2,
                down_1: 0.2,
                down_2: 0.1,
                down_3_plus: 0.1,
            },
            repricing_confidence: 0.5,
            fill_before_decay: 0.5,
            fill_toxic: 0.5,
        };
        let tags = ExperimentTags::new("run-9", "pair-9", "ETH-1h", "ETH", "1h");
        let row = row_text(&StorageEvent::jev_signal_tagged(
            2_000_000,
            "condition-9",
            "hash",
            "{}",
            "{}",
            3,
            50,
            Trigger::SpotMove,
            Variant::Control,
            tags.clone(),
            &signal,
            1_234,
            567,
        ));
        for dimension in [
            "run_id=run-9",
            "pair_id=pair-9",
            "market_id=ETH-1h",
            "asset=ETH",
            "horizon=1h",
            "tokens_in=1234i",
            "tokens_out=567i",
        ] {
            assert!(row.contains(dimension), "missing {dimension} in {row}");
        }

        let pair_row = row_text(&StorageEvent::AbPair {
            ts: 2_000_000,
            run_id: "run-9".to_owned(),
            pair_id: "pair-9".to_owned(),
            market_id: "ETH-1h".to_owned(),
            asset: "ETH".to_owned(),
            horizon: "1h".to_owned(),
            condition_id: "condition-9".to_owned(),
            state_seq: 3,
            observed_at_ms: 2_000,
            status: "incomplete".to_owned(),
            control_ok: 1,
            quant_ok: 0,
        });
        assert!(pair_row.starts_with("ab_pairs,run_id=run-9,pair_id=pair-9"));
        assert!(pair_row.contains("status=incomplete"));
        assert!(pair_row.contains("control_ok=1i"));
        assert!(pair_row.contains("quant_ok=0i"));

        let fill_row = row_text(&StorageEvent::PaperFill {
            ts: 2_001_000,
            order_id: 7,
            condition_id: "condition-9".to_owned(),
            variant: Variant::QuantV1,
            side: "BUY".to_owned(),
            price: 0.44,
            size: 10.0,
            filled_at_ms: 2_001,
            maker: 1,
            tags: tags.clone(),
        });
        assert!(fill_row.starts_with("paper_fills,condition_id=condition-9"));
        assert!(fill_row.contains("variant=QUANT_V1"));
        assert!(fill_row.contains("order_id=7i"));
        assert!(fill_row.contains("pair_id=pair-9"));

        let equity_row = row_text(&StorageEvent::PaperEquity {
            ts: 2_002_000,
            condition_id: "condition-9".to_owned(),
            variant: Variant::QuantV1,
            position: 10.0,
            realized_pnl: 0.0,
            unrealized_pnl: 20.0,
            total_pnl: 20.0,
            exposure: 10.0,
            tags: tags.clone(),
        });
        assert!(equity_row.starts_with("paper_equity,condition_id=condition-9"));
        assert!(equity_row.contains("total_pnl=20"));
        assert!(equity_row.contains("market_id=ETH-1h"));
    }

    #[test]
    fn signal_markouts_map_drift_columns_and_dimensions() {
        let tags = ExperimentTags::new("run-9", "pair-9", "ETH-1h", "ETH", "1h");
        let row = row_text(&StorageEvent::SignalMarkout {
            ts: 2_003_000,
            condition_id: "condition-9".to_owned(),
            variant: Variant::Control,
            jev_ts: 2_000_000,
            ref_price: 0.41,
            mid_1s: 0.42,
            mid_5s: 0.43,
            mid_10s: 0.44,
            mid_30s: 0.45,
            mid_60s: 0.46,
            mo_1s_pp: 1.0,
            mo_5s_pp: 2.0,
            mo_10s_pp: 3.0,
            mo_30s_pp: 4.0,
            mo_60s_pp: 5.0,
            tags,
        });

        assert!(row.starts_with("signal_markouts,condition_id=condition-9"));
        assert!(row.contains("variant=CONTROL"));
        assert!(row.contains("ref_price=0.41"));
        assert!(row.contains("mo_5s_pp=2"));
        assert!(row.contains("pair_id=pair-9"));
        assert!(row.contains("market_id=ETH-1h"));
        assert!(row.contains("asset=ETH"));
        assert!(row.contains("horizon=1h"));
    }

    #[test]
    fn maker_markouts_map_all_horizons_and_pnls() {
        let row = row_text(&StorageEvent::MakerMarkout {
            ts: 3_000_000,
            condition_id: "condition-3".to_owned(),
            variant: Variant::Control,
            jev_ts: 2_900_000,
            side: "BUY".to_owned(),
            price: 0.4,
            size: 10.0,
            mid_1s: 0.41,
            mid_5s: 0.43,
            mid_10s: 0.44,
            mid_30s: 0.45,
            mid_60s: 0.46,
            pnl_1s_pp: 1.0,
            pnl_5s_pp: 3.0,
            pnl_10s_pp: 4.0,
            pnl_30s_pp: 5.0,
            pnl_60s_pp: 6.0,
            tags: ExperimentTags::new("run-1", "pair-1", "BTC-5m", "BTC", "5m"),
        });

        assert!(row.starts_with("maker_markouts,condition_id=condition-3,side=BUY"));
        assert!(row.contains("variant=CONTROL"));
        assert!(row.contains("run_id=run-1"));
        assert!(row.contains("pair_id=pair-1"));
        assert!(row.contains("market_id=BTC-5m"));
        assert!(row.contains("asset=BTC"));
        assert!(row.contains("horizon=5m"));
        for field in [
            "price=0.4",
            "size=10",
            "mid_1s=0.41",
            "mid_5s=0.43",
            "mid_10s=0.44",
            "mid_30s=0.45",
            "mid_60s=0.46",
            "pnl_1s_pp=1",
            "pnl_5s_pp=3",
            "pnl_10s_pp=4",
            "pnl_30s_pp=5",
            "pnl_60s_pp=6",
        ] {
            assert!(row.contains(field), "missing {field} in {row}");
        }
    }

    #[test]
    fn full_queue_drops_immediately_and_surfaces_lag() {
        let (sender, _receiver) = mpsc::channel(1);
        let metrics = std::sync::Arc::new(MetricsInner::default());
        let handle = QuestDbHandle { sender, metrics };
        let event = StorageEvent::ExternalTick {
            ts: 1,
            symbol: "BTC".to_owned(),
            price: 100.0,
            ret_5m: 0.0,
            ret_1h: 0.0,
            vol_1h: 0.0,
        };

        assert_eq!(handle.try_send(event.clone()), StorageSendResult::Sent);
        let metrics_handle = handle.clone();
        let (done_sender, done_receiver) = std_mpsc::channel();
        let producer = thread::spawn(move || {
            for _ in 0..64 {
                assert_eq!(
                    metrics_handle.try_send(event.clone()),
                    StorageSendResult::Dropped
                );
            }
            done_sender.send(()).expect("test receiver should be alive");
        });

        done_receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("try_send must not wait for a full queue");
        producer.join().expect("producer thread should finish");

        let metrics = handle.metrics();
        assert_eq!(metrics.sent, 1);
        assert_eq!(metrics.dropped_full, 64);
        assert_eq!(metrics.write_errors, 0);
    }
}
