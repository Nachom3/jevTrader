//! Event-time ordering with backward-only as-of joins (no look-ahead).

use super::types::HistoricalEvent;

/// One event in global event-time order.
#[derive(Debug, Clone, PartialEq)]
pub struct SynchronizedEvent {
    pub ts_ms: i64,
    pub event: HistoricalEvent,
}

/// Merges pre-sorted per-feed streams into one event-time ordered stream.
///
/// Ties break deterministically: underlying ticks before Poly tops before
/// Poly trades, so a decision at T never sees a same-ms future trade first.
pub struct Synchronizer;

impl Synchronizer {
    #[must_use]
    pub fn merge(mut streams: Vec<Vec<HistoricalEvent>>) -> Vec<SynchronizedEvent> {
        let mut out = Vec::new();
        for s in &mut streams {
            s.sort_by_key(HistoricalEvent::ts_ms);
        }
        let mut idx = vec![0usize; streams.len()];
        loop {
            let mut best: Option<(i64, usize, u8)> = None;
            for (si, s) in streams.iter().enumerate() {
                if idx[si] < s.len() {
                    let rank = match &s[idx[si]] {
                        HistoricalEvent::UnderlyingTick { .. } => 0,
                        HistoricalEvent::PolyTop { .. } => 1,
                        HistoricalEvent::PolyTrade { .. } => 2,
                    };
                    let ts = s[idx[si]].ts_ms();
                    let cand = (ts, si, rank);
                    let better = match best {
                        None => true,
                        Some((bts, _, brank)) => (ts, rank) < (bts, brank),
                    };
                    if better {
                        best = Some(cand);
                    }
                }
            }
            match best {
                None => break,
                Some((ts, si, _)) => {
                    let ev = streams[si][idx[si]].clone();
                    idx[si] += 1;
                    out.push(SynchronizedEvent {
                        ts_ms: ts,
                        event: ev,
                    });
                }
            }
        }
        out
    }
}

/// Backward-only as-of lookup: newest observation with `ts <= t`.
///
/// Returns `None` when no observation exists at or before `t`. Forward or
/// nearest joins are forbidden: they would leak future information.
#[must_use]
pub fn as_of_backward<T>(history: &[(i64, T)], t: i64) -> Option<&T> {
    let mut best: Option<&T> = None;
    for (ts, v) in history {
        if *ts <= t {
            best = Some(v);
        } else {
            break;
        }
    }
    best
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tick(ts: i64, price: f64) -> HistoricalEvent {
        HistoricalEvent::UnderlyingTick {
            ts_ms: ts,
            asset: "BTC".to_owned(),
            venue: "BINANCE".to_owned(),
            price,
            bid: None,
            ask: None,
            source: "test".to_owned(),
        }
    }

    fn trade(ts: i64, price: f64) -> HistoricalEvent {
        HistoricalEvent::PolyTrade {
            ts_ms: ts,
            condition_id: "c".to_owned(),
            price,
            size: 1.0,
            aggressor: None,
            direction_quality: "GROUND_TRUTH".to_owned(),
            source: "test".to_owned(),
        }
    }

    #[test]
    fn merge_orders_by_event_time() {
        let out = Synchronizer::merge(vec![vec![trade(3, 0.5)], vec![tick(1, 1.0), tick(2, 2.0)]]);
        let ts: Vec<i64> = out.iter().map(|e| e.ts_ms).collect();
        assert_eq!(ts, vec![1, 2, 3]);
    }

    #[test]
    fn as_of_never_looks_forward() {
        let hist = vec![(10, "a"), (20, "b"), (30, "c")];
        assert_eq!(as_of_backward(&hist, 5), None);
        assert_eq!(as_of_backward(&hist, 10), Some(&"a"));
        assert_eq!(as_of_backward(&hist, 25), Some(&"b"));
        assert_eq!(as_of_backward(&hist, 30), Some(&"c"));
    }
}
