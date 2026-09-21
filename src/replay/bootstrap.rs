//! Deterministic block bootstrap and OOS headline metrics.
//!
//! A block is a complete market/condition (`BlockId`), never an individual
//! trade. Trades from one condition share serial information, resolution
//! timing, and often the same underlying move; IID trade resampling would
//! manufacture independent observations and make confidence intervals too
//! narrow. Resampling therefore draws whole blocks with replacement.

use serde::{Deserialize, Serialize};

use super::metrics::{EconomySummary, EpisodeMetricsInput, summarize};

/// Stable market identifier used as a bootstrap block.
pub type BlockId = String;

/// Alias used when referring to one complete bootstrap block.
pub type Block = BlockId;

/// Number of bootstrap replicates used by [`oos_report`].
pub const DEFAULT_BOOTSTRAP_DRAWS: usize = 10_000;

/// Deterministic block sampler with no global RNG state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockBootstrap {
    /// Unique or repeated block labels supplied by the caller. Each output
    /// index references one complete entry in this vector.
    pub blocks: Vec<BlockId>,
    pub seed: u64,
}

impl BlockBootstrap {
    #[must_use]
    pub fn new(blocks: Vec<BlockId>, seed: u64) -> Self {
        Self { blocks, seed }
    }

    /// Draws `n_draws` bootstrap samples, each with one whole-block draw for
    /// every block in the original sample. The returned integers are indexes
    /// into [`Self::blocks`], not trade indexes. Same seed and blocks produce
    /// byte-for-byte identical draws.
    #[must_use]
    pub fn resample(&self, n_draws: usize) -> Vec<Vec<usize>> {
        if self.blocks.is_empty() {
            return vec![Vec::new(); n_draws];
        }

        let mut state = self.seed;
        let block_count = self.blocks.len() as u64;
        let mut draws = Vec::with_capacity(n_draws);
        for _ in 0..n_draws {
            let mut draw = Vec::with_capacity(self.blocks.len());
            for _ in 0..self.blocks.len() {
                draw.push((splitmix64_next(&mut state) % block_count) as usize);
            }
            draws.push(draw);
        }
        draws
    }
}

/// Distribution summary using finite samples only.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct BootstrapSummary {
    pub n: usize,
    pub mean: f64,
    pub ci95_low: f64,
    pub ci95_high: f64,
    pub p_positive: f64,
    pub median: f64,
}

/// Summarizes samples with linearly interpolated percentiles.
///
/// Non-finite values are ignored. For sorted values `x` of length `n`, the
/// percentile at `p` uses position `p * (n - 1)`, then linearly interpolates
/// between the surrounding values. Thus the reported 2.5%, 50%, and 97.5%
/// values are deterministic even when the sample size is small.
#[must_use]
pub fn summarize_distribution(samples: &[f64]) -> BootstrapSummary {
    let mut finite: Vec<f64> = samples
        .iter()
        .copied()
        .filter(|sample| sample.is_finite())
        .collect();
    if finite.is_empty() {
        return BootstrapSummary::default();
    }
    finite.sort_by(f64::total_cmp);
    let n = finite.len();
    let mean = finite.iter().sum::<f64>() / n as f64;
    let positive = finite.iter().filter(|sample| **sample > 0.0).count();
    BootstrapSummary {
        n,
        mean,
        ci95_low: percentile(&finite, 0.025),
        ci95_high: percentile(&finite, 0.975),
        p_positive: positive as f64 / n as f64,
        median: percentile(&finite, 0.5),
    }
}

/// OOS metrics that cannot be confused with the in-sample summary.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct OosHeadline {
    pub net_pnl_usd: f64,
    pub ci95_low: f64,
    pub ci95_high: f64,
    pub p_positive: f64,
    pub pnl_per_trade: f64,
    /// `None` means used capital was not supplied or was not positive finite.
    pub roi: Option<f64>,
    /// Maximum drawdown of the supplied cumulative equity curve. `None` when
    /// no equity curve was supplied.
    pub max_drawdown_usd: Option<f64>,
    /// Net positive PnL divided by absolute net negative PnL. `None` when no
    /// negative observations exist.
    pub profit_factor: Option<f64>,
    pub fills: u64,
    pub nofills: u64,
    /// Optional peak capital supplied by the caller, not inferred from PnL.
    pub capital_peak_usd: Option<f64>,
    /// Optional sum of capital-seconds supplied by the caller.
    pub capital_seconds_usd_s: Option<f64>,
    /// Full bootstrap distribution summary, including the median.
    pub bootstrap: BootstrapSummary,
}

/// A report namespace separating calibration and OOS economy summaries.
///
/// The two summaries are intentionally separate fields. A caller must build
/// them from disjoint episode inputs (or explicitly document why an input is
/// shared); neither field is used implicitly to tune the other. Use
/// [`OosReport::from_metrics`] to delegate point aggregation to
/// [`super::metrics::summarize`].
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct OosReport {
    pub in_sample: EconomySummary,
    pub out_of_sample: EconomySummary,
    pub oos_headline: OosHeadline,
}

impl OosReport {
    #[must_use]
    pub fn new(
        in_sample: EconomySummary,
        out_of_sample: EconomySummary,
        oos_headline: OosHeadline,
    ) -> Self {
        Self {
            in_sample,
            out_of_sample,
            oos_headline,
        }
    }

    /// Builds separated economy summaries with the shared replay metrics code.
    ///
    /// The headline is passed separately because its block bootstrap consumes
    /// net-PnL observations rather than full [`super::ledger::TradeEpisode`]
    /// records.
    #[must_use]
    pub fn from_metrics(
        in_sample: &[EpisodeMetricsInput<'_>],
        out_of_sample: &[EpisodeMetricsInput<'_>],
        oos_headline: OosHeadline,
    ) -> Self {
        Self::new(summarize(in_sample), summarize(out_of_sample), oos_headline)
    }
}

/// Creates an OOS headline with deterministic block-bootstrap intervals.
///
/// `episodes_net` contains one net-PnL value per filled episode and its
/// condition block. `fills` and `nofills` are caller-owned execution counts;
/// no-fill rows must not be inserted into `episodes_net`. `used_capital` is
/// required for ROI, while `equity_curve` is a cumulative net-equity curve
/// starting at zero and is required for max drawdown. `capital_peak_usd` and
/// `capital_seconds_usd_s` are optional caller-supplied exposure statistics.
/// The final `seed` controls only the local deterministic bootstrap PRNG.
#[allow(clippy::too_many_arguments)]
#[must_use]
pub fn oos_report(
    episodes_net: &[(BlockId, f64)],
    fills: u64,
    nofills: u64,
    used_capital: Option<f64>,
    equity_curve: Option<&[f64]>,
    capital_peak_usd: Option<f64>,
    capital_seconds_usd_s: Option<f64>,
    seed: u64,
) -> OosHeadline {
    oos_report_with_draws(
        episodes_net,
        fills,
        nofills,
        used_capital,
        equity_curve,
        capital_peak_usd,
        capital_seconds_usd_s,
        DEFAULT_BOOTSTRAP_DRAWS,
        seed,
    )
}

/// Variant of [`oos_report`] with an explicit replicate count for tests or
/// resource-budgeted research runs.
#[allow(clippy::too_many_arguments)]
#[must_use]
pub fn oos_report_with_draws(
    episodes_net: &[(BlockId, f64)],
    fills: u64,
    nofills: u64,
    used_capital: Option<f64>,
    equity_curve: Option<&[f64]>,
    capital_peak_usd: Option<f64>,
    capital_seconds_usd_s: Option<f64>,
    bootstrap_draws: usize,
    seed: u64,
) -> OosHeadline {
    let net_pnl_usd = episodes_net
        .iter()
        .map(|(_, pnl)| *pnl)
        .filter(|pnl| pnl.is_finite())
        .sum::<f64>();

    let (block_ids, block_totals) = block_totals(episodes_net);
    let bootstrap = BlockBootstrap::new(block_ids, seed);
    let samples: Vec<f64> = if block_totals.is_empty() {
        Vec::new()
    } else {
        bootstrap
            .resample(bootstrap_draws)
            .into_iter()
            .map(|draw| {
                draw.into_iter()
                    .map(|block_index| block_totals[block_index])
                    .sum()
            })
            .collect()
    };
    let distribution = summarize_distribution(&samples);

    OosHeadline {
        net_pnl_usd,
        ci95_low: distribution.ci95_low,
        ci95_high: distribution.ci95_high,
        p_positive: distribution.p_positive,
        pnl_per_trade: if fills == 0 {
            0.0
        } else {
            net_pnl_usd / fills as f64
        },
        roi: used_capital
            .filter(|capital| capital.is_finite() && *capital > 0.0)
            .map(|capital| net_pnl_usd / capital),
        max_drawdown_usd: equity_curve.map(max_drawdown),
        profit_factor: profit_factor(episodes_net),
        fills,
        nofills,
        capital_peak_usd: capital_peak_usd.filter(|value| value.is_finite()),
        capital_seconds_usd_s: capital_seconds_usd_s.filter(|value| value.is_finite()),
        bootstrap: distribution,
    }
}

fn splitmix64_next(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut value = *state;
    value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    value ^ (value >> 31)
}

fn percentile(sorted: &[f64], probability: f64) -> f64 {
    if sorted.len() == 1 {
        return sorted[0];
    }
    let position = probability * (sorted.len() - 1) as f64;
    let lower = position.floor() as usize;
    let upper = position.ceil() as usize;
    if lower == upper {
        sorted[lower]
    } else {
        let weight = position - lower as f64;
        sorted[lower] + (sorted[upper] - sorted[lower]) * weight
    }
}

fn block_totals(episodes_net: &[(BlockId, f64)]) -> (Vec<BlockId>, Vec<f64>) {
    let mut block_ids = Vec::new();
    let mut block_totals = Vec::new();
    for (block_id, pnl) in episodes_net {
        let index = match block_ids.iter().position(|known| known == block_id) {
            Some(index) => index,
            None => {
                block_ids.push(block_id.clone());
                block_totals.push(0.0);
                block_ids.len() - 1
            }
        };
        if pnl.is_finite() {
            block_totals[index] += pnl;
        }
    }
    (block_ids, block_totals)
}

fn max_drawdown(equity_curve: &[f64]) -> f64 {
    let mut peak: f64 = 0.0;
    let mut max_drawdown: f64 = 0.0;
    for equity in equity_curve
        .iter()
        .copied()
        .filter(|equity| equity.is_finite())
    {
        peak = peak.max(equity);
        max_drawdown = max_drawdown.max(peak - equity);
    }
    max_drawdown
}

fn profit_factor(episodes_net: &[(BlockId, f64)]) -> Option<f64> {
    let gains: f64 = episodes_net
        .iter()
        .map(|(_, pnl)| *pnl)
        .filter(|pnl| pnl.is_finite() && *pnl > 0.0)
        .sum();
    let losses: f64 = episodes_net
        .iter()
        .map(|(_, pnl)| *pnl)
        .filter(|pnl| pnl.is_finite() && *pnl < 0.0)
        .map(f64::abs)
        .sum();
    if losses > 0.0 {
        Some(gains / losses)
    } else {
        None
    }
}
