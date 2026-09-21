//! Streaming historical sources. Never loads the full dataset into RAM.

use super::types::{Fidelity, HistoricalEvent};

/// Pull-based chunk source over processed history.
pub trait HistoricalSource {
    /// Returns the next chunk (empty vec = exhausted). Implementations read
    /// Parquet row groups / CSV blocks incrementally, never the full file.
    fn next_chunk(&mut self, max_rows: usize) -> Result<Vec<HistoricalEvent>, String>;
    fn exhausted(&self) -> bool;
}

/// In-memory source for tests and the deterministic smoke bootstrap.
pub struct InMemorySource {
    events: Vec<HistoricalEvent>,
    cursor: usize,
}

impl InMemorySource {
    #[must_use]
    pub fn new(mut events: Vec<HistoricalEvent>) -> Self {
        events.sort_by_key(HistoricalEvent::ts_ms);
        Self { events, cursor: 0 }
    }
}

impl HistoricalSource for InMemorySource {
    fn next_chunk(&mut self, max_rows: usize) -> Result<Vec<HistoricalEvent>, String> {
        if self.cursor >= self.events.len() {
            return Ok(Vec::new());
        }
        let end = (self.cursor + max_rows).min(self.events.len());
        let chunk = self.events[self.cursor..end].to_vec();
        self.cursor = end;
        Ok(chunk)
    }

    fn exhausted(&self) -> bool {
        self.cursor >= self.events.len()
    }
}

/// Parquet-backed chunk metadata. The actual Parquet read path streams one
/// batch at a time via the `parquet` crate (see `read_parquet_chunks`);
/// this type carries the file list so runners never glob ad hoc.
#[derive(Debug, Clone)]
pub struct ChunkEventSource {
    pub files: Vec<String>,
    pub chunk_rows: usize,
}

impl ChunkEventSource {
    #[must_use]
    pub const fn new(files: Vec<String>, chunk_rows: usize) -> Self {
        Self { files, chunk_rows }
    }

    /// Streams up to `limit_rows` normalized events across files, batch by
    /// batch. Each file is opened once and its row groups are consumed in
    /// order, so peak RAM stays bounded by one batch.
    pub fn read_parquet_chunks(&self, limit_rows: usize) -> Result<Vec<HistoricalEvent>, String> {
        read_parquet_chunks(&self.files, self.chunk_rows, limit_rows)
    }
}

/// Minimal Parquet-to-event mapping for processed corpus files.
///
/// `polymarket_trades.parquet` contributes PolyTrade rows;
/// `underlying_market_data.parquet` contributes UnderlyingTick rows. Unknown
/// schemas contribute nothing (nullable + coverage preserved). Files that do
/// not exist are skipped so a partial bootstrap still runs.
pub fn read_parquet_chunks(
    files: &[String],
    chunk_rows: usize,
    limit_rows: usize,
) -> Result<Vec<HistoricalEvent>, String> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::fs::File;

    let mut out = Vec::new();
    let batch_size = chunk_rows.clamp(1, 8192);
    for path in files {
        if out.len() >= limit_rows {
            break;
        }
        let file = match File::open(path) {
            Ok(f) => f,
            Err(_) => continue,
        };
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| format!("{path}: {e}"))?
            .with_batch_size(batch_size)
            .build()
            .map_err(|e| format!("{path}: {e}"))?;
        for batch in reader {
            if out.len() >= limit_rows {
                break;
            }
            let batch = batch.map_err(|e| format!("{path}: {e}"))?;
            decode_batch(&batch, path, &mut out, limit_rows);
        }
    }
    Ok(out)
}

/// Windowed underlying read for one replay condition (RAM-bounded).
///
/// Scans `path` batch by batch (never the whole file) and keeps only
/// `UnderlyingTick` rows for `asset` with `lo_ms <= ts_ms <= hi_ms`,
/// uniformly thinned by `thin_every` (stride over the global row index, so
/// density stays ~1/thin across the window) up to `limit_rows`. The merged
/// multi-week underlying file is tens of millions of rows; the old
/// head-limit read silently priced April markets with December ticks.
/// Predicate: time window + asset only, no look-ahead by construction.
pub fn read_underlying_window(
    path: &str,
    asset: &str,
    lo_ms: i64,
    hi_ms: i64,
    thin_every: usize,
    limit_rows: usize,
) -> Result<Vec<HistoricalEvent>, String> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::fs::File;

    let thin = thin_every.max(1);
    let mut out = Vec::new();
    let mut kept: usize = 0;
    // Row-group skipping from ts_ms min/max statistics: the merged file has
    // hundreds of day-clustered groups, and a condition window touches only
    // a few. Groups with unknown statistics are scanned (safe fallback).
    let selection = row_group_selection(path, lo_ms, hi_ms);
    let file = match File::open(path) {
        Ok(f) => f,
        Err(_) => return Ok(Vec::new()),
    };
    let mut builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| format!("{path}: {e}"))?
        .with_batch_size(8192);
    if let Some(selection) = selection {
        builder = builder.with_row_selection(selection);
    }
    let reader = builder.build().map_err(|e| format!("{path}: {e}"))?;
    for batch in reader {
        if out.len() >= limit_rows {
            break;
        }
        let batch = batch.map_err(|e| format!("{path}: {e}"))?;
        let mut tmp = Vec::new();
        decode_batch(&batch, path, &mut tmp, usize::MAX);
        for ev in tmp {
            if out.len() >= limit_rows {
                break;
            }
            if let HistoricalEvent::UnderlyingTick {
                ts_ms, asset: a, ..
            } = &ev
            {
                if a != asset || *ts_ms < lo_ms || *ts_ms > hi_ms {
                    continue;
                }
                if kept.is_multiple_of(thin) {
                    out.push(ev);
                }
                kept += 1;
            }
        }
    }
    Ok(out)
}

/// Row selection over row groups whose ts_ms statistics overlap [lo_ms, hi_ms].
///
/// Returns `None` when statistics are unavailable (caller scans everything).
/// Uses only file-footer metadata: no batch is decoded here.
fn row_group_selection(
    path: &str,
    lo_ms: i64,
    hi_ms: i64,
) -> Option<parquet::arrow::arrow_reader::RowSelection> {
    use parquet::arrow::arrow_reader::RowSelector;
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use parquet::file::statistics::Statistics;
    use std::fs::File;

    let file = File::open(path).ok()?;
    let reader = SerializedFileReader::new(file).ok()?;
    let metadata = reader.metadata();
    if metadata.num_row_groups() == 0 {
        return None;
    }
    // Locate the ts_ms column in the first row group.
    let first = metadata.row_group(0);
    let mut ts_idx = None;
    for i in 0..first.num_columns() {
        if first.column(i).column_descr().name() == "ts_ms" {
            ts_idx = Some(i);
            break;
        }
    }
    let ts_idx = ts_idx?;
    let mut selectors = Vec::with_capacity(metadata.num_row_groups());
    for g in 0..metadata.num_row_groups() {
        let group = metadata.row_group(g);
        let n = group.num_rows() as usize;
        let overlap = match group.column(ts_idx).statistics() {
            Some(Statistics::Int64(stats)) => match (stats.min_opt(), stats.max_opt()) {
                (Some(mn), Some(mx)) => *mn <= hi_ms && *mx >= lo_ms,
                _ => true,
            },
            // Unknown or non-int64 statistics: scan rather than risk a gap.
            _ => true,
        };
        selectors.push(if overlap {
            RowSelector::select(n)
        } else {
            RowSelector::skip(n)
        });
    }
    Some(selectors.into())
}

/// Maps one Arrow batch to events by schema: tape rows carry
/// `condition_id` + `yes_price`; underlying rows carry `asset` + `ts_ms`.
fn decode_batch(
    batch: &arrow::array::RecordBatch,
    path: &str,
    out: &mut Vec<HistoricalEvent>,
    limit_rows: usize,
) {
    let schema = batch.schema();
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    if names.contains(&"condition_id") && names.contains(&"yes_price") {
        for row in 0..batch.num_rows() {
            if out.len() >= limit_rows {
                break;
            }
            let (Some(condition_id), Some(price)) = (
                batch_str(batch, "condition_id", row).filter(|s| !s.is_empty()),
                batch_f64(batch, "yes_price", row),
            ) else {
                continue;
            };
            if !price.is_finite() {
                continue;
            }
            // Tape `ts` is epoch seconds; events use epoch milliseconds.
            let ts_ms = batch_i64(batch, "ts", row).map_or(0, |s| s.saturating_mul(1000));
            out.push(HistoricalEvent::PolyTrade {
                ts_ms,
                condition_id,
                price,
                size: batch_f64(batch, "usd_amount", row).unwrap_or(0.0),
                aggressor: batch_str(batch, "aggressor", row),
                direction_quality: batch_str(batch, "direction_quality", row)
                    .unwrap_or_else(|| "UNKNOWN".to_owned()),
                source: format!("parquet:{path}"),
            });
        }
    } else if names.contains(&"asset") && names.contains(&"ts_ms") {
        for row in 0..batch.num_rows() {
            if out.len() >= limit_rows {
                break;
            }
            let (Some(asset), Some(price), Some(ts_ms)) = (
                batch_str(batch, "asset", row).filter(|s| !s.is_empty()),
                batch_f64(batch, "price", row),
                batch_i64(batch, "ts_ms", row),
            ) else {
                continue;
            };
            if !price.is_finite() {
                continue;
            }
            out.push(HistoricalEvent::UnderlyingTick {
                ts_ms,
                asset,
                venue: batch_str(batch, "venue", row).unwrap_or_else(|| "BINANCE".to_owned()),
                price,
                bid: batch_f64(batch, "bid", row),
                ask: batch_f64(batch, "ask", row),
                qty: batch_f64(batch, "qty", row),
                aggressor: batch_str(batch, "aggressor_side", row),
                source: format!("parquet:{path}"),
            });
        }
    }
}

fn batch_str(batch: &arrow::array::RecordBatch, name: &str, row: usize) -> Option<String> {
    use arrow::array::{Array as _, LargeStringArray, StringArray};
    let idx = batch.schema().index_of(name).ok()?;
    let col = batch.column(idx);
    // Polars writes UTF-8 as LargeUtf8; accept both string widths.
    if let Some(a) = col.as_any().downcast_ref::<StringArray>() {
        if a.is_null(row) {
            None
        } else {
            Some(a.value(row).to_owned())
        }
    } else {
        let a = col.as_any().downcast_ref::<LargeStringArray>()?;
        if a.is_null(row) {
            None
        } else {
            Some(a.value(row).to_owned())
        }
    }
}

fn batch_f64(batch: &arrow::array::RecordBatch, name: &str, row: usize) -> Option<f64> {
    use arrow::array::{Array as _, Float64Array};
    let idx = batch.schema().index_of(name).ok()?;
    let a = batch.column(idx).as_any().downcast_ref::<Float64Array>()?;
    if a.is_null(row) {
        None
    } else {
        Some(a.value(row))
    }
}

fn batch_i64(batch: &arrow::array::RecordBatch, name: &str, row: usize) -> Option<i64> {
    use arrow::array::{Array as _, Int64Array};
    let idx = batch.schema().index_of(name).ok()?;
    let a = batch.column(idx).as_any().downcast_ref::<Int64Array>()?;
    if a.is_null(row) {
        None
    } else {
        Some(a.value(row))
    }
}

fn parse_timestamp_ms(raw: &str) -> Option<i64> {
    let raw = raw.trim();
    if raw.is_empty() || raw.eq_ignore_ascii_case("nat") {
        return None;
    }
    if let Ok(value) = raw.parse::<i64>() {
        let magnitude = value.unsigned_abs();
        return if magnitude >= 100_000_000_000_000_000 {
            value.checked_div(1_000_000)
        } else if magnitude >= 100_000_000_000_000 {
            value.checked_div(1_000)
        } else if magnitude >= 100_000_000_000 {
            Some(value)
        } else {
            value.checked_mul(1_000)
        };
    }
    chrono::DateTime::parse_from_rfc3339(raw)
        .ok()
        .map(|value| value.timestamp_millis())
}

fn batch_timestamp_ms(batch: &arrow::array::RecordBatch, name: &str, row: usize) -> Option<i64> {
    use arrow::array::{
        Array as _, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    };

    if let Some(raw) = batch_str(batch, name, row) {
        return parse_timestamp_ms(&raw);
    }
    let idx = batch.schema().index_of(name).ok()?;
    let col = batch.column(idx);
    if let Some(a) = col.as_any().downcast_ref::<TimestampMillisecondArray>() {
        return (!a.is_null(row)).then(|| a.value(row));
    }
    if let Some(a) = col.as_any().downcast_ref::<TimestampMicrosecondArray>() {
        return (!a.is_null(row)).then(|| a.value(row).checked_div(1_000))?;
    }
    if let Some(a) = col.as_any().downcast_ref::<TimestampNanosecondArray>() {
        return (!a.is_null(row)).then(|| a.value(row).checked_div(1_000_000))?;
    }
    batch_i64(batch, name, row).and_then(|value| parse_timestamp_ms(&value.to_string()))
}

#[cfg(test)]
mod tests {
    use super::super::types::HistoricalEvent;
    use super::*;

    #[test]
    fn streams_in_chunks_without_full_load() {
        let evs: Vec<HistoricalEvent> = (0..250)
            .map(|i| HistoricalEvent::UnderlyingTick {
                ts_ms: i,
                asset: "BTC".to_owned(),
                venue: "BINANCE".to_owned(),
                price: 100.0 + i as f64,
                bid: None,
                ask: None,
                qty: None,
                aggressor: None,
                source: "test".to_owned(),
            })
            .collect();
        let mut src = InMemorySource::new(evs);
        let mut total = 0;
        while !src.exhausted() {
            let c = src.next_chunk(100).expect("chunk");
            assert!(c.len() <= 100);
            total += c.len();
        }
        assert_eq!(total, 250);
    }

    #[test]
    fn missing_files_decode_to_empty_without_panicking() {
        let out = read_parquet_chunks(
            &["research-data/processed/does-not-exist.parquet".to_owned()],
            100,
            1000,
        )
        .expect("missing files are skipped");
        assert!(out.is_empty());
    }
}

/// One market's replay metadata loaded from the processed corpus.
#[derive(Debug, Clone)]
pub struct MarketMeta {
    pub market_id: String,
    pub condition_id: String,
    pub asset: String,
    pub horizon: String,
    pub split: super::types::Split,
    pub fidelity: super::types::Fidelity,
    pub slug: String,
    pub question: String,
    pub resolution_rules: String,
}

/// One resolution row loaded from the processed corpus.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolutionRecord {
    pub condition_id: String,
    pub winning_outcome: String,
    pub resolved_ts_ms: Option<i64>,
    pub status: String,
}

/// Loads the explicit resolution labels from `resolutions.parquet`.
/// Missing files or malformed batches yield an empty vec. A null/`NaT`
/// `resolved_ts` remains `None`; the caller decides whether a spec end bound
/// is an acceptable proxy.
#[must_use]
pub fn read_resolutions(processed_dir: &str) -> Vec<ResolutionRecord> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::fs::File;

    let path = format!("{processed_dir}/resolutions.parquet");
    let file = match File::open(&path) {
        Ok(f) => f,
        Err(_) => return Vec::new(),
    };
    let builder = match ParquetRecordBatchReaderBuilder::try_new(file) {
        Ok(b) => b,
        Err(_) => return Vec::new(),
    };
    let projection = parquet::arrow::ProjectionMask::columns(
        builder.parquet_schema(),
        [
            "condition_id",
            "winning_outcome",
            "outcome",
            "resolved_ts",
            "resolved_at",
            "resolution_status",
            "status",
        ],
    );
    let reader = match builder
        .with_projection(projection)
        .with_batch_size(8192)
        .build()
    {
        Ok(r) => r,
        Err(_) => return Vec::new(),
    };
    let mut out = Vec::new();
    for batch in reader {
        let batch = match batch {
            Ok(b) => b,
            Err(_) => continue,
        };
        for row in 0..batch.num_rows() {
            let Some(condition_id) = batch_str(&batch, "condition_id", row) else {
                continue;
            };
            out.push(ResolutionRecord {
                condition_id,
                winning_outcome: batch_str(&batch, "winning_outcome", row)
                    .or_else(|| batch_str(&batch, "outcome", row))
                    .unwrap_or_default(),
                resolved_ts_ms: batch_timestamp_ms(&batch, "resolved_ts", row)
                    .or_else(|| batch_timestamp_ms(&batch, "resolved_at", row)),
                status: batch_str(&batch, "resolution_status", row)
                    .or_else(|| batch_str(&batch, "status", row))
                    .unwrap_or_default(),
            });
        }
    }
    out
}

/// Loads `(end_at_ms, fidelity)` from `resolution_specs.parquet` without
/// materializing the other, potentially large, spec columns.
/// Missing files or rows without a parseable end bound yield an empty map.
#[must_use]
pub fn read_resolution_specs_end(
    processed_dir: &str,
) -> std::collections::HashMap<String, (i64, Fidelity)> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::collections::HashMap;
    use std::fs::File;

    let path = format!("{processed_dir}/resolution_specs.parquet");
    let file = match File::open(&path) {
        Ok(f) => f,
        Err(_) => return HashMap::new(),
    };
    let builder = match ParquetRecordBatchReaderBuilder::try_new(file) {
        Ok(b) => b,
        Err(_) => return HashMap::new(),
    };
    let projection = parquet::arrow::ProjectionMask::columns(
        builder.parquet_schema(),
        ["condition_id", "end_at", "resolution_at", "fidelity"],
    );
    let reader = match builder
        .with_projection(projection)
        .with_batch_size(8192)
        .build()
    {
        Ok(r) => r,
        Err(_) => return HashMap::new(),
    };
    let mut out = HashMap::new();
    for batch in reader {
        let batch = match batch {
            Ok(b) => b,
            Err(_) => continue,
        };
        for row in 0..batch.num_rows() {
            let (Some(condition_id), Some(end_at_ms)) = (
                batch_str(&batch, "condition_id", row),
                batch_timestamp_ms(&batch, "end_at", row)
                    .or_else(|| batch_timestamp_ms(&batch, "resolution_at", row)),
            ) else {
                continue;
            };
            let fidelity = match batch_str(&batch, "fidelity", row)
                .as_deref()
                .map(str::to_ascii_uppercase)
                .as_deref()
            {
                Some("EXACT") => Fidelity::Exact,
                Some("PROXY") => Fidelity::Proxy,
                _ => Fidelity::Unknown,
            };
            out.insert(condition_id, (end_at_ms, fidelity));
        }
    }
    out
}

/// Loads `selected_markets.parquet` joined with `resolution_specs.parquet`
/// (fidelity) from a processed corpus directory. Missing files yield an
/// empty vec so the caller can fall back to the synthetic bootstrap.
#[must_use]
pub fn read_market_metas(processed_dir: &str) -> Vec<MarketMeta> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::fs::File;

    let read_strings = |path: &str| -> Vec<std::collections::HashMap<String, String>> {
        let file = match File::open(path) {
            Ok(f) => f,
            Err(_) => return Vec::new(),
        };
        let reader = match ParquetRecordBatchReaderBuilder::try_new(file) {
            Ok(b) => b,
            Err(_) => return Vec::new(),
        };
        let reader = match reader.with_batch_size(8192).build() {
            Ok(r) => r,
            Err(_) => return Vec::new(),
        };
        let mut rows = Vec::new();
        for batch in reader {
            let batch = match batch {
                Ok(b) => b,
                Err(_) => continue,
            };
            for row in 0..batch.num_rows() {
                let mut map = std::collections::HashMap::new();
                for field in batch.schema().fields() {
                    let name = field.name().clone();
                    if let Some(v) = batch_str(&batch, &name, row) {
                        map.insert(name, v);
                    }
                }
                rows.push(map);
            }
        }
        rows
    };

    let selected = read_strings(&format!("{processed_dir}/selected_markets.parquet"));
    if selected.is_empty() {
        return Vec::new();
    }
    let mut fidelity_of = std::collections::HashMap::new();
    for row in read_strings(&format!("{processed_dir}/resolution_specs.parquet")) {
        if let Some(cid) = row.get("condition_id") {
            fidelity_of.insert(
                cid.clone(),
                row.get("fidelity").cloned().unwrap_or_default(),
            );
        }
    }
    selected
        .into_iter()
        .filter_map(|row| {
            Some(MarketMeta {
                market_id: row.get("market_id").cloned().unwrap_or_default(),
                condition_id: row.get("condition_id").cloned()?,
                asset: row.get("asset").cloned().unwrap_or_default(),
                horizon: row.get("horizon").cloned().unwrap_or_default(),
                slug: row.get("slug").cloned().unwrap_or_default(),
                question: row.get("question").cloned().unwrap_or_default(),
                resolution_rules: row.get("resolution_rules").cloned().unwrap_or_default(),
                split: match row.get("split").map(String::as_str) {
                    Some("VALIDATION") => super::types::Split::Validation,
                    Some("OUT_OF_SAMPLE") => super::types::Split::OutOfSample,
                    _ => super::types::Split::Exploration,
                },
                fidelity: match fidelity_of
                    .get(row.get("condition_id")?)
                    .map(String::as_str)
                {
                    Some("EXACT") => super::types::Fidelity::Exact,
                    Some("PROXY") => super::types::Fidelity::Proxy,
                    _ => super::types::Fidelity::Unknown,
                },
            })
        })
        .filter(|m| !m.condition_id.is_empty())
        .collect()
}

/// Loads `(day_ts_seconds, symbol, regime_name)` from
/// `market_regimes.parquet`. Empty vec when the file is absent.
#[must_use]
pub fn read_regimes(processed_dir: &str) -> Vec<(i64, String, String)> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::fs::File;

    let path = format!("{processed_dir}/market_regimes.parquet");
    let file = match File::open(&path) {
        Ok(f) => f,
        Err(_) => return Vec::new(),
    };
    let reader = match ParquetRecordBatchReaderBuilder::try_new(file) {
        Ok(b) => b,
        Err(_) => return Vec::new(),
    };
    let reader = match reader.with_batch_size(8192).build() {
        Ok(r) => r,
        Err(_) => return Vec::new(),
    };
    let mut out = Vec::new();
    for batch in reader {
        let batch = match batch {
            Ok(b) => b,
            Err(_) => continue,
        };
        for row in 0..batch.num_rows() {
            let (Some(day), Some(symbol), vol, trend) = (
                batch_i64(&batch, "day_ts", row),
                batch_str(&batch, "symbol", row),
                batch_str(&batch, "vol_regime", row),
                batch_str(&batch, "trend_regime", row),
            ) else {
                continue;
            };
            out.push((
                day,
                symbol,
                format!(
                    "{}-{}",
                    vol.unwrap_or_else(|| "UNKNOWN".to_owned()),
                    trend.unwrap_or_else(|| "UNKNOWN".to_owned())
                ),
            ));
        }
    }
    out.sort();
    out
}

#[cfg(test)]
mod corpus_tests {
    use super::*;

    #[test]
    fn real_corpus_decodes_when_present() {
        // No silent skip: the processed corpus is a task deliverable and
        // must decode when present. Absence fails loudly.
        let metas = read_market_metas("research-data/processed");
        assert!(!metas.is_empty(), "processed corpus must decode metas");
        let tape = ChunkEventSource::new(
            vec!["research-data/processed/polymarket_trades.parquet".to_owned()],
            8192,
        )
        .read_parquet_chunks(100_000)
        .expect("tape reads");
        assert!(!tape.is_empty(), "tape must decode to events");
        let regimes = read_regimes("research-data/processed");
        assert!(!regimes.is_empty(), "regimes must decode");
    }
}
