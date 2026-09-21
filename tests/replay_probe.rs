use std::collections::{BTreeSet, HashMap};
use std::fs::File;

use arrow::array::{
    Array, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, LargeStringArray,
    StringArray, UInt32Array, UInt64Array,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

const TRADE_FILE: &str = "research-data/processed/polymarket_trades.parquet";
const RESOLUTION_FILE: &str = "research-data/processed/resolutions.parquet";

#[derive(Debug, Default)]
struct ConditionStats {
    count: usize,
    min_price: Option<f64>,
    max_price: Option<f64>,
    aggressors: BTreeSet<String>,
    outcome_labels: BTreeSet<String>,
}

fn read_rows(path: &str) -> Vec<HashMap<String, String>> {
    let file = File::open(path).unwrap_or_else(|error| panic!("open {path}: {error}"));
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap_or_else(|error| panic!("read parquet metadata {path}: {error}"))
        .with_batch_size(8192)
        .build()
        .unwrap_or_else(|error| panic!("build parquet reader {path}: {error}"));

    let mut rows = Vec::new();
    for batch in reader {
        let batch = batch.unwrap_or_else(|error| panic!("read parquet batch {path}: {error}"));
        for row in 0..batch.num_rows() {
            let mut values = HashMap::new();
            for field in batch.schema().fields() {
                let column = batch.column(batch.schema().index_of(field.name()).unwrap());
                let value = array_text(column.as_ref(), row).unwrap_or_else(|| "<null>".to_owned());
                values.insert(field.name().clone(), value);
            }
            rows.push(values);
        }
    }
    rows
}

fn array_text(array: &dyn Array, row: usize) -> Option<String> {
    if array.is_null(row) {
        return None;
    }
    if let Some(values) = array.as_any().downcast_ref::<StringArray>() {
        return Some(values.value(row).to_owned());
    }
    if let Some(values) = array.as_any().downcast_ref::<LargeStringArray>() {
        return Some(values.value(row).to_owned());
    }
    if let Some(values) = array.as_any().downcast_ref::<Float64Array>() {
        return Some(values.value(row).to_string());
    }
    if let Some(values) = array.as_any().downcast_ref::<Float32Array>() {
        return Some((values.value(row) as f64).to_string());
    }
    if let Some(values) = array.as_any().downcast_ref::<Int64Array>() {
        return Some(values.value(row).to_string());
    }
    if let Some(values) = array.as_any().downcast_ref::<Int32Array>() {
        return Some(values.value(row).to_string());
    }
    if let Some(values) = array.as_any().downcast_ref::<UInt64Array>() {
        return Some(values.value(row).to_string());
    }
    if let Some(values) = array.as_any().downcast_ref::<UInt32Array>() {
        return Some(values.value(row).to_string());
    }
    if let Some(values) = array.as_any().downcast_ref::<BooleanArray>() {
        return Some(values.value(row).to_string());
    }
    None
}

#[test]
fn tape_probe_lists_conditions() {
    let trade_rows = read_rows(TRADE_FILE);
    assert_eq!(trade_rows.len(), 6866, "unexpected trade tape size");

    let winning_outcomes: HashMap<_, _> = read_rows(RESOLUTION_FILE)
        .into_iter()
        .filter_map(|row| {
            Some((
                row.get("condition_id")?.clone(),
                row.get("winning_outcome")?.clone(),
            ))
        })
        .collect();

    let mut conditions: HashMap<String, ConditionStats> = HashMap::new();
    for row in &trade_rows {
        let condition_id = row
            .get("condition_id")
            .expect("trade row must have condition_id")
            .clone();
        let stats = conditions.entry(condition_id).or_default();
        stats.count += 1;
        stats.aggressors.insert(row["aggressor"].clone());
        stats.outcome_labels.insert(row["outcome_label"].clone());

        let price = row["yes_price"]
            .parse::<f64>()
            .expect("trade row yes_price must be numeric");
        stats.min_price = Some(stats.min_price.map_or(price, |min| min.min(price)));
        stats.max_price = Some(stats.max_price.map_or(price, |max| max.max(price)));
    }

    let mut top: Vec<_> = conditions.iter().collect();
    top.sort_by(|(left_id, left), (right_id, right)| {
        right
            .count
            .cmp(&left.count)
            .then_with(|| left_id.cmp(right_id))
    });

    eprintln!(
        "[replay_probe] trade_rows={} resolution_rows={} conditions={}",
        trade_rows.len(),
        winning_outcomes.len(),
        conditions.len()
    );
    for (condition_id, stats) in top.into_iter().take(6) {
        eprintln!(
            "[replay_probe] condition_id={} count={} price_min={:.6} price_max={:.6} aggressors={:?} outcome_labels={:?} winning_outcome={}",
            condition_id,
            stats.count,
            stats
                .min_price
                .expect("condition must have a minimum price"),
            stats
                .max_price
                .expect("condition must have a maximum price"),
            stats.aggressors,
            stats.outcome_labels,
            winning_outcomes
                .get(condition_id)
                .map(String::as_str)
                .unwrap_or("<missing>"),
        );
    }
}
