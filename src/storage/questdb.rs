use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use crate::{domain::Trigger, jev::V1Signal};
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
use tokio::{sync::mpsc, task::JoinHandle};

/// A row destined for one of the QuestDB tables defined in `questdb/schema.sql`.
///
/// Timestamps are UNIX epoch microseconds. JSON columns remain strings so the
/// storage boundary does not depend on `serde_json::Value`.
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
    },
}

impl StorageEvent {
    /// Build a V1 Jev signal row from a validated signal and its state identity.
    ///
    /// The Jev client currently does not expose usage, so this helper stores
    /// `tokens_in` and `tokens_out` as zero until the client measures them.
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
            tokens_in: 0,
            tokens_out: 0,
            trigger: trigger.as_str().to_owned(),
            variant,
        }
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
        } => {
            buffer
                .table("jev_signals")?
                .symbol("condition_id", condition_id)?
                .symbol("trigger", trigger)?
                .symbol("variant", variant.as_str())?
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
        } => {
            buffer
                .table("paper_decisions")?
                .symbol("condition_id", condition_id)?
                .symbol("decision", decision)?
                .symbol("variant", variant.as_str())?
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
        } => {
            buffer
                .table("maker_markouts")?
                .symbol("condition_id", condition_id)?
                .symbol("side", side)?
                .symbol("variant", variant.as_str())?
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
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{sync::mpsc as std_mpsc, thread, time::Duration};

    use super::{MetricsInner, QuestDbHandle, StorageEvent, StorageSendResult, Variant, build_row};
    use crate::{
        domain::Trigger,
        jev::{TickDistribution, V1Signal},
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
        });

        assert!(row.starts_with("maker_markouts,condition_id=condition-3,side=BUY"));
        assert!(row.contains("variant=CONTROL"));
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
