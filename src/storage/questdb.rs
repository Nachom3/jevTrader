use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use questdb::ingress::{Buffer, ProtocolVersion, Sender, TimestampMicros};
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
    /// One Jev call, including the exact state that produced its signal.
    JevSignal {
        ts: i64,
        condition_id: String,
        state_hash: String,
        state_json: String,
        questions_json: String,
        likely_yes: f64,
        underpriced: f64,
        resolution_risk: f64,
        resolution_risk_conf: f64,
        latency_ms: i64,
        tokens_in: i64,
        tokens_out: i64,
        trigger: String,
    },
    /// A paper-trading decision linked to its Jev signal timestamp.
    PaperDecision {
        ts: i64,
        condition_id: String,
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
    /// Maker markouts at the three configured horizons.
    MakerMarkout {
        ts: i64,
        condition_id: String,
        jev_ts: i64,
        side: String,
        price: f64,
        size: f64,
        mid_1s: f64,
        mid_5s: f64,
        mid_30s: f64,
        pnl_1s_pp: f64,
        pnl_5s_pp: f64,
        pnl_30s_pp: f64,
    },
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
            state_hash,
            state_json,
            questions_json,
            likely_yes,
            underpriced,
            resolution_risk,
            resolution_risk_conf,
            latency_ms,
            tokens_in,
            tokens_out,
            trigger,
        } => {
            buffer
                .table("jev_signals")?
                .symbol("condition_id", condition_id)?
                .symbol("trigger", trigger)?
                .column_str("state_hash", state_hash)?
                .column_str("state_json", state_json)?
                .column_str("questions_json", questions_json)?
                .column_f64("likely_yes", *likely_yes)?
                .column_f64("underpriced", *underpriced)?
                .column_f64("resolution_risk", *resolution_risk)?
                .column_f64("resolution_risk_conf", *resolution_risk_conf)?
                .column_i64("latency_ms", *latency_ms)?
                .column_i64("tokens_in", *tokens_in)?
                .column_i64("tokens_out", *tokens_out)?
                .at(TimestampMicros::new(*ts))?;
        }
        StorageEvent::PaperDecision {
            ts,
            condition_id,
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
            jev_ts,
            side,
            price,
            size,
            mid_1s,
            mid_5s,
            mid_30s,
            pnl_1s_pp,
            pnl_5s_pp,
            pnl_30s_pp,
        } => {
            buffer
                .table("maker_markouts")?
                .symbol("condition_id", condition_id)?
                .symbol("side", side)?
                .column_ts("jev_ts", TimestampMicros::new(*jev_ts))?
                .column_f64("price", *price)?
                .column_f64("size", *size)?
                .column_f64("mid_1s", *mid_1s)?
                .column_f64("mid_5s", *mid_5s)?
                .column_f64("mid_30s", *mid_30s)?
                .column_f64("pnl_1s_pp", *pnl_1s_pp)?
                .column_f64("pnl_5s_pp", *pnl_5s_pp)?
                .column_f64("pnl_30s_pp", *pnl_30s_pp)?
                .at(TimestampMicros::new(*ts))?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{sync::mpsc as std_mpsc, thread, time::Duration};

    use super::{MetricsInner, QuestDbHandle, StorageEvent, StorageSendResult, build_row};
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
    fn jev_signals_preserve_the_full_state_and_questions_json() {
        let row = row_text(&StorageEvent::JevSignal {
            ts: 2_000_000,
            condition_id: "condition-2".to_owned(),
            state_hash: "state-hash".to_owned(),
            state_json: r#"{"market":{"question":"BTC above 100k"}}"#.to_owned(),
            questions_json: r#"{"likely_yes":{"type":"noul"}}"#.to_owned(),
            likely_yes: 0.8,
            underpriced: 0.7,
            resolution_risk: 0.1,
            resolution_risk_conf: 0.9,
            latency_ms: 101,
            tokens_in: 200,
            tokens_out: 40,
            trigger: "book_move".to_owned(),
        });

        assert!(row.starts_with("jev_signals,condition_id=condition-2"));
        assert!(row.contains("state_hash=\"state-hash\""));
        assert!(
            row.contains(
                "state_json=\"{\\\"market\\\":{\\\"question\\\":\\\"BTC above 100k\\\"}}\""
            )
        );
        assert!(
            row.contains("questions_json=\"{\\\"likely_yes\\\":{\\\"type\\\":\\\"noul\\\"}}\"")
        );
        assert!(row.contains("likely_yes=0.8"));
        assert!(row.contains("resolution_risk_conf=0.9"));
        assert!(row.contains("tokens_in=200"));
        assert!(row.contains("tokens_out=40"));
        assert!(row.contains("trigger=book_move"));
    }

    #[test]
    fn maker_markouts_map_all_horizons_and_pnls() {
        let row = row_text(&StorageEvent::MakerMarkout {
            ts: 3_000_000,
            condition_id: "condition-3".to_owned(),
            jev_ts: 2_900_000,
            side: "BUY".to_owned(),
            price: 0.4,
            size: 10.0,
            mid_1s: 0.41,
            mid_5s: 0.43,
            mid_30s: 0.45,
            pnl_1s_pp: 1.0,
            pnl_5s_pp: 3.0,
            pnl_30s_pp: 5.0,
        });

        assert!(row.starts_with("maker_markouts,condition_id=condition-3,side=BUY"));
        for field in [
            "price=0.4",
            "size=10",
            "mid_1s=0.41",
            "mid_5s=0.43",
            "mid_30s=0.45",
            "pnl_1s_pp=1",
            "pnl_5s_pp=3",
            "pnl_30s_pp=5",
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
