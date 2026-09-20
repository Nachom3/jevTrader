//! Segmented reporting: variant x asset x horizon x regime x split x fidelity.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// One paired-evaluation output row (CONTROL and QUANT_V1 rows share pair_id).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReportRow {
    pub run_id: String,
    pub pair_id: String,
    pub variant: String,
    pub state_hash: String,
    pub market_id: String,
    pub asset: String,
    pub horizon: String,
    pub split: String,
    pub regime: String,
    pub fidelity: String,
    pub fill_model: String,
    pub latency_profile: String,
    /// Latency used for this evaluation, measured live or sampled for replay.
    #[serde(default)]
    pub jev_latency_ms: u64,
    pub quoted: bool,
    pub filled: bool,
    pub fill_fraction: f64,
    pub markout_1s_pp: Option<f64>,
    pub markout_5s_pp: Option<f64>,
    pub markout_10s_pp: Option<f64>,
    pub markout_30s_pp: Option<f64>,
    pub markout_60s_pp: Option<f64>,
    pub pnl_pp: f64,
    pub stale_skipped: bool,
    pub incomplete_pair: bool,
}

/// Segmentation key for grouped metrics.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct SegmentKey {
    pub variant: String,
    pub asset: String,
    pub horizon: String,
    pub regime: String,
    pub split: String,
    pub fidelity: String,
    pub fill_model: String,
    pub latency_profile: String,
}

/// Grouped summary: counts, quote/fill rates, mean markout_5s, PnL, drawdown.
#[derive(Debug, Clone, Serialize)]
pub struct SegmentSummary {
    pub key: SegmentKey,
    pub evaluations: usize,
    pub quotes: usize,
    pub fills: usize,
    pub fill_rate: f64,
    pub mean_markout_5s_pp: f64,
    pub mean_jev_latency_ms: f64,
    pub hit_rate_5s: f64,
    pub total_pnl_pp: f64,
    pub pnl_per_trade: f64,
    pub stale_skips: usize,
    pub incomplete_pairs: usize,
}

#[must_use]
pub fn build_report(rows: &[ReportRow]) -> Vec<SegmentSummary> {
    let mut groups: BTreeMap<SegmentKey, Vec<&ReportRow>> = BTreeMap::new();
    for r in rows {
        let key = SegmentKey {
            variant: r.variant.clone(),
            asset: r.asset.clone(),
            horizon: r.horizon.clone(),
            regime: r.regime.clone(),
            split: r.split.clone(),
            fidelity: r.fidelity.clone(),
            fill_model: r.fill_model.clone(),
            latency_profile: r.latency_profile.clone(),
        };
        groups.entry(key).or_default().push(r);
    }
    groups
        .into_iter()
        .map(|(key, rs)| {
            let evaluations = rs.len();
            let quotes = rs.iter().filter(|r| r.quoted).count();
            let fills = rs.iter().filter(|r| r.filled).count();
            let mo5: Vec<f64> = rs.iter().filter_map(|r| r.markout_5s_pp).collect();
            let mean_mo5 = if mo5.is_empty() {
                0.0
            } else {
                mo5.iter().sum::<f64>() / mo5.len() as f64
            };
            let mean_jev_latency =
                rs.iter().map(|r| r.jev_latency_ms as f64).sum::<f64>() / evaluations as f64;
            let hit = if mo5.is_empty() {
                0.0
            } else {
                mo5.iter().filter(|v| **v > 0.0).count() as f64 / mo5.len() as f64
            };
            let total_pnl: f64 = rs.iter().map(|r| r.pnl_pp).sum();
            let stale_skips = rs.iter().filter(|r| r.stale_skipped).count();
            let incomplete_pairs = rs.iter().filter(|r| r.incomplete_pair).count();
            SegmentSummary {
                key,
                evaluations,
                quotes,
                fills,
                fill_rate: if quotes > 0 {
                    fills as f64 / quotes as f64
                } else {
                    0.0
                },
                mean_markout_5s_pp: mean_mo5,
                mean_jev_latency_ms: mean_jev_latency,
                hit_rate_5s: hit,
                total_pnl_pp: total_pnl,
                pnl_per_trade: if fills > 0 {
                    total_pnl / fills as f64
                } else {
                    0.0
                },
                stale_skips,
                incomplete_pairs,
            }
        })
        .collect()
}

/// Writes rows as JSON (pretty). Caller owns the path.
pub fn write_json(rows: &[ReportRow]) -> Result<String, String> {
    serde_json::to_string_pretty(rows).map_err(|e| e.to_string())
}

/// Renders a Markdown summary (never a UI).
#[must_use]
pub fn write_markdown(summary: &[SegmentSummary]) -> String {
    let mut out = String::from("# Historical replay report\n\n");
    out.push_str("| variant | asset | horizon | regime | split | fidelity | fill | latency | evals | quotes | fills | mean_jev_latency_ms | mean_mo5s | pnl | stale | incomplete |\n");
    out.push_str("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|\n");
    for s in summary {
        out.push_str(&format!(
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {:.2} | {:.4} | {:.4} | {} | {} |\n",
            s.key.variant,
            s.key.asset,
            s.key.horizon,
            s.key.regime,
            s.key.split,
            s.key.fidelity,
            s.key.fill_model,
            s.key.latency_profile,
            s.evaluations,
            s.quotes,
            s.fills,
            s.mean_jev_latency_ms,
            s.mean_markout_5s_pp,
            s.total_pnl_pp,
            s.stale_skips,
            s.incomplete_pairs
        ));
    }
    out
}

/// Conditional alpha probe over paired rows: mean signed `markout_5s_pp`
/// restricted to rows whose caller-supplied signal exceeds `threshold`
/// (e.g. `E[markout_5s | underreact_up > X]`).
///
/// The caller extracts `(signal, markout_5s)` pairs from wherever the signal
/// lives (live `jev_signals` state JSON or replay rows joined to signals);
/// this helper only filters and averages. Incomplete pairs should be excluded
/// by the caller. Returns `(n, mean)`; `n == 0` yields `0.0`, never NaN.
#[must_use]
pub fn conditional_markout_5s(pairs: &[(f64, Option<f64>)], threshold: f64) -> (usize, f64) {
    let selected: Vec<f64> = pairs
        .iter()
        .filter(|(signal, _)| signal.is_finite() && *signal > threshold)
        .filter_map(|(_, markout)| *markout)
        .filter(|markout| markout.is_finite())
        .collect();
    if selected.is_empty() {
        return (0, 0.0);
    }
    let mean = selected.iter().sum::<f64>() / selected.len() as f64;
    (selected.len(), mean)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(variant: &str, mo5: Option<f64>, pnl: f64) -> ReportRow {
        ReportRow {
            run_id: "r".to_owned(),
            pair_id: "p".to_owned(),
            variant: variant.to_owned(),
            state_hash: "h".to_owned(),
            market_id: "BTC-5m".to_owned(),
            asset: "BTC".to_owned(),
            horizon: "5m".to_owned(),
            split: "EXPLORATION".to_owned(),
            regime: "NORMAL_VOL-SIDEWAYS".to_owned(),
            fidelity: "EXACT".to_owned(),
            fill_model: "CONSERVATIVE".to_owned(),
            latency_profile: "BASE".to_owned(),
            jev_latency_ms: 320,
            quoted: true,
            filled: true,
            fill_fraction: 1.0,
            markout_1s_pp: None,
            markout_5s_pp: mo5,
            markout_10s_pp: None,
            markout_30s_pp: None,
            markout_60s_pp: None,
            pnl_pp: pnl,
            stale_skipped: false,
            incomplete_pair: false,
        }
    }

    #[test]
    fn segments_never_merge_control_quant() {
        let rows = vec![
            row("CONTROL", Some(1.0), 1.0),
            row("QUANT_V1", Some(2.0), 2.0),
        ];
        let rep = build_report(&rows);
        assert_eq!(rep.len(), 2);
    }

    #[test]
    fn conditional_markout_filters_by_signal_threshold() {
        let pairs = [
            (0.9, Some(2.0)),
            (0.8, Some(1.0)),
            (0.5, Some(-1.0)),
            (0.95, None),
        ];
        let (n, mean) = conditional_markout_5s(&pairs, 0.75);
        assert_eq!(n, 2);
        assert!((mean - 1.5).abs() < 1e-12);
        assert_eq!(conditional_markout_5s(&pairs, 0.99), (0, 0.0));
    }

    #[test]
    fn markdown_renders_all_segments() {
        let rows = vec![row("CONTROL", Some(1.0), 1.0)];
        let rep = build_report(&rows);
        let md = write_markdown(&rep);
        assert!(md.contains("CONTROL") && md.contains("mean_mo5s"));
        assert!(md.contains("mean_jev_latency_ms"));
    }
}
