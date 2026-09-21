//! Deterministic USD metrics over already-settled replay episodes.
//!
//! This module deliberately does not calculate fills, markouts, fees, or
//! hedges. Callers provide those episode-level facts and this module only
//! aggregates them in event-time order where an ordering is required.

use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};

use super::ledger::{ExitType, TradeEpisode};

/// Small positive denominator used by ROI when no executable capital exists.
const ROI_EPSILON: f64 = f64::EPSILON;

/// Inputs already calculated by replay; metrics only aggregate these values.
#[derive(Debug, Clone, Copy)]
pub struct EpisodeMetricsInput<'a> {
    pub episode: &'a TradeEpisode,
    pub regime: Option<&'a str>,
    /// Signed markouts at +1, +5, +10, +30, and +60 seconds.
    ///
    /// Values are percentage points, matching [`crate::replay::signed_markouts_pp`].
    /// A caller holding USD/share markouts must convert them to percentage
    /// points before passing them here because the summary field is named
    /// `adverse_selection_pp`.
    pub markouts: [Option<f64>; 5],
    pub fill_profile: &'a str,
}

/// Aggregate economy metrics for a collection of replay episodes.
///
/// No-fill episodes count in `n_episodes` and the fill rates, but never in
/// PnL, capital, turnover, drawdown, or win/loss counts. `pnl_per_trade` and
/// `mean_capital_seconds` use filled episodes as their denominator. A win is
/// strictly positive historical net PnL, a loss is strictly negative, and a
/// flat episode counts as neither.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct EconomySummary {
    pub n_episodes: usize,
    pub n_filled: usize,
    pub n_nofill: usize,
    pub fill_rate: f64,
    pub nofill_rate: f64,
    pub gross_pnl_usd: f64,
    pub historical_net_usd: f64,
    /// Sum of the optional current-mark stream; missing current marks count as zero.
    pub current_net_usd: f64,
    pub pnl_per_trade: f64,
    /// Historical net PnL divided by the sum of executable notional of fills.
    pub roi_on_used_capital: f64,
    pub wins: usize,
    pub losses: usize,
    /// Gross positive PnL divided by absolute gross negative PnL. `None` when
    /// the collection contains no gross losses.
    pub profit_factor: Option<f64>,
    /// Maximum peak-to-trough drawdown of the historical-net equity curve.
    pub max_drawdown_usd: f64,
    /// Sum of executable notional for every filled episode. Hedge legs are
    /// represented as their own filled episodes and therefore enter once.
    pub turnover_usd: f64,
    /// Maximum open executable notional while replay events are swept by time.
    pub max_simultaneous_capital_usd: f64,
    pub total_capital_seconds: f64,
    pub mean_capital_seconds: f64,
    /// Mean signed maker +5s markout in percentage points. Negative means an
    /// adverse move under the signed BUY markout convention.
    pub adverse_selection_pp: Option<f64>,
    /// Historical net PnL for Resolution/Sell/Stop episodes.
    pub hold_pnl_usd: f64,
    /// Historical net PnL for Hedge episodes.
    pub hedge_pnl_usd: f64,
}

/// Builds the documented six-component segmentation key.
///
/// The format is `asset|horizon|regime|strategy|profile|exit_class`.
/// `None` regime and a missing episode fill profile are rendered as
/// `unknown`. `Resolution`, `Sell`, and `Stop` are the `hold` class;
/// `Hedge` is `hedge`; and `NoFill` is kept as `no_fill` so unfilled orders do
/// not get mixed into a held position segment.
#[must_use]
pub fn breakdown_key(episode: &TradeEpisode, regime_label_opt: Option<&str>) -> String {
    breakdown_key_with_profile(
        episode,
        regime_label_opt,
        episode.fill_profile.as_deref().unwrap_or("unknown"),
    )
}

/// Aggregates economy metrics without I/O, look-ahead, or hidden state.
#[must_use]
pub fn summarize(inputs: &[EpisodeMetricsInput<'_>]) -> EconomySummary {
    let n_episodes = inputs.len();
    let n_filled = inputs
        .iter()
        .filter(|input| is_filled(input.episode))
        .count();
    let n_nofill = n_episodes.saturating_sub(n_filled);
    let fill_rate = rate(n_filled, n_episodes);
    let nofill_rate = rate(n_nofill, n_episodes);

    let gross_pnl_usd = inputs
        .iter()
        .filter(|input| is_filled(input.episode))
        .map(|input| input.episode.gross_pnl_usd)
        .sum();
    let historical_net_usd = inputs
        .iter()
        .filter(|input| is_filled(input.episode))
        .map(|input| historical_net(input.episode))
        .sum();
    let current_net_usd = inputs
        .iter()
        .filter(|input| is_filled(input.episode))
        .map(|input| current_net(input.episode))
        .sum();

    let used_capital: f64 = inputs
        .iter()
        .filter(|input| is_filled(input.episode))
        .map(|input| input.episode.actual_notional_usd)
        .sum();
    let roi_denominator = if used_capital > ROI_EPSILON {
        used_capital
    } else {
        ROI_EPSILON
    };

    let wins = inputs
        .iter()
        .filter(|input| is_filled(input.episode) && historical_net(input.episode) > 0.0)
        .count();
    let losses = inputs
        .iter()
        .filter(|input| is_filled(input.episode) && historical_net(input.episode) < 0.0)
        .count();

    let gross_wins: f64 = inputs
        .iter()
        .filter(|input| is_filled(input.episode))
        .map(|input| input.episode.gross_pnl_usd)
        .filter(|pnl| *pnl > 0.0)
        .sum();
    let gross_losses_abs: f64 = inputs
        .iter()
        .filter(|input| is_filled(input.episode))
        .map(|input| input.episode.gross_pnl_usd)
        .filter(|pnl| *pnl < 0.0)
        .map(f64::abs)
        .sum();
    let profit_factor = if gross_losses_abs > 0.0 {
        Some(gross_wins / gross_losses_abs)
    } else {
        None
    };

    let total_capital_seconds: f64 = inputs
        .iter()
        .filter(|input| is_filled(input.episode))
        .map(|input| input.episode.capital_seconds_usd_s)
        .sum();

    let (hold_pnl_usd, hedge_pnl_usd) = inputs
        .iter()
        .filter(|input| is_filled(input.episode))
        .fold((0.0, 0.0), |(hold, hedge), input| {
            let pnl = historical_net(input.episode);
            match input.episode.exit_type {
                ExitType::Hedge => (hold, hedge + pnl),
                ExitType::Resolution | ExitType::Sell | ExitType::Stop => (hold + pnl, hedge),
                ExitType::NoFill => (hold, hedge),
            }
        });

    let adverse_selection_pp = mean_maker_markout_5s(inputs);

    EconomySummary {
        n_episodes,
        n_filled,
        n_nofill,
        fill_rate,
        nofill_rate,
        gross_pnl_usd,
        historical_net_usd,
        current_net_usd,
        pnl_per_trade: if n_filled == 0 {
            0.0
        } else {
            historical_net_usd / n_filled as f64
        },
        roi_on_used_capital: historical_net_usd / roi_denominator,
        wins,
        losses,
        profit_factor,
        max_drawdown_usd: max_drawdown_usd(inputs),
        turnover_usd: used_capital,
        max_simultaneous_capital_usd: max_simultaneous_capital(inputs),
        total_capital_seconds,
        mean_capital_seconds: if n_filled == 0 {
            0.0
        } else {
            total_capital_seconds / n_filled as f64
        },
        adverse_selection_pp,
        hold_pnl_usd,
        hedge_pnl_usd,
    }
}

/// Aggregates the same metrics independently for each deterministic key.
#[must_use]
pub fn summarize_by(inputs: &[EpisodeMetricsInput<'_>]) -> HashMap<String, EconomySummary> {
    let mut groups: HashMap<String, Vec<EpisodeMetricsInput<'_>>> = HashMap::new();
    for input in inputs {
        let key = breakdown_key_with_profile(input.episode, input.regime, input.fill_profile);
        groups.entry(key).or_default().push(*input);
    }

    groups
        .into_iter()
        .map(|(key, group)| (key, summarize(&group)))
        .collect()
}

fn breakdown_key_with_profile(
    episode: &TradeEpisode,
    regime: Option<&str>,
    fill_profile: &str,
) -> String {
    format!(
        "{}|{}|{}|{}|{}|{}",
        episode.asset,
        episode.horizon,
        regime.unwrap_or("unknown"),
        episode.strategy_version,
        fill_profile,
        exit_class(episode.exit_type),
    )
}

fn exit_class(exit_type: ExitType) -> &'static str {
    match exit_type {
        ExitType::Hedge => "hedge",
        ExitType::Resolution | ExitType::Sell | ExitType::Stop => "hold",
        ExitType::NoFill => "no_fill",
    }
}

fn is_filled(episode: &TradeEpisode) -> bool {
    !episode.is_no_fill()
}

fn historical_net(episode: &TradeEpisode) -> f64 {
    episode.pnl_historical_usd.unwrap_or(episode.net_pnl_usd)
}

fn current_net(episode: &TradeEpisode) -> f64 {
    // Current-mark PnL is an optional stream; missing current marks contribute
    // zero rather than silently relabeling historical principal as current.
    episode.pnl_current_usd.unwrap_or(0.0)
}

fn rate(count: usize, total: usize) -> f64 {
    if total == 0 {
        0.0
    } else {
        count as f64 / total as f64
    }
}

fn mean_maker_markout_5s(inputs: &[EpisodeMetricsInput<'_>]) -> Option<f64> {
    let mut total = 0.0;
    let mut count = 0usize;
    for input in inputs {
        if !is_filled(input.episode) || input.episode.is_maker != Some(true) {
            continue;
        }
        if let Some(markout) = input.markouts[1]
            && markout.is_finite()
        {
            total += markout;
            count += 1;
        }
    }
    (count > 0).then_some(total / count as f64)
}

fn max_drawdown_usd(inputs: &[EpisodeMetricsInput<'_>]) -> f64 {
    let mut points: Vec<(i64, &TradeEpisode)> = inputs
        .iter()
        .filter(|input| is_filled(input.episode))
        .filter_map(|input| {
            input
                .episode
                .exit_ts_ms
                .or(input.episode.fill_ts_ms)
                .map(|ts| (ts, input.episode))
        })
        .collect();
    points.sort_by(|(left_ts, left_episode), (right_ts, right_episode)| {
        left_ts
            .cmp(right_ts)
            .then_with(|| left_episode.episode_id.cmp(&right_episode.episode_id))
    });

    let mut equity = 0.0;
    let mut peak = 0.0;
    let mut max_drawdown: f64 = 0.0;
    for (_, episode) in points {
        equity += historical_net(episode);
        if equity > peak {
            peak = equity;
        }
        max_drawdown = max_drawdown.max(peak - equity);
    }
    max_drawdown
}

#[derive(Debug, Default)]
struct CapitalEvents {
    released: f64,
    added: f64,
}

fn max_simultaneous_capital(inputs: &[EpisodeMetricsInput<'_>]) -> f64 {
    let mut events: BTreeMap<i64, CapitalEvents> = BTreeMap::new();
    for input in inputs.iter().filter(|input| is_filled(input.episode)) {
        let episode = input.episode;
        let notional = episode.actual_notional_usd;
        if let Some(fill_ts_ms) = episode.fill_ts_ms {
            events.entry(fill_ts_ms).or_default().added += notional;
        }
        if let Some(exit_ts_ms) = episode.exit_ts_ms {
            events.entry(exit_ts_ms).or_default().released += notional;
        }
    }

    let mut capital = 0.0;
    let mut maximum: f64 = 0.0;
    for event in events.into_values() {
        // At one timestamp, release before adding new capital so a same-time
        // exit and fill do not create a fictitious overlap.
        capital -= event.released;
        capital += event.added;
        maximum = maximum.max(capital);
    }
    maximum
}
