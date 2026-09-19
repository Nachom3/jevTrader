# Multi-market A/B and paper PnL methodology

This document defines the experimental protocol for the multi-market paper/shadow
stage. It is an evaluation protocol, not a strategy change. The Jev questions
and wording in `src/jev/questions_md.rs`, quote thresholds, `PaperBook`, and
execution assumptions remain frozen.

## 1. Unit of observation and frozen configuration

- A **run** has one `run_id` and one immutable configuration snapshot.
- A **market contract** is identified by `market_id`, `asset`, and `horizon`:
  `BTC-5m`, `BTC-15m`, `BTC-1h`, `BTC-4h`, `ETH-5m`, `ETH-15m`, `ETH-1h`, or
  `ETH-4h`.
- A **pair** is one coherent snapshot evaluated by both `CONTROL` and
  `QUANT_V1`. Both rows carry the same `pair_id`; `state_seq` is local to the
  market runtime and is not a pairing key.
- The state and questions are frozen before either branch is evaluated. The
  only intended state difference is the existing `quant: null` versus
  `quant: {...}` enrichment.
- Persist `run_id`, `pair_id`, `market_id`, `asset`, and `horizon` on every
  branch row. Store the configuration identifier and data coverage beside the
  report, even when the current QuestDB schema carries them in the run sheet
  rather than in every row.

No result may be used to edit thresholds, Jev wording, the paper queue model,
or the replay fill model during the same run. Any later calibration is a new,
pre-registered run with a new `run_id`.

## 2. Temporal TRAIN/EXPLORATION and out-of-sample design

The historical vocabulary is the one in `replay::Split`:

| Experimental label | `replay::Split` | Permitted use |
|---|---|---|
| TRAIN / exploration | `Exploration` | Inspect data quality, describe regimes, and make a pre-registered calibration choice. No OOS result is used here. |
| Validation | `Validation` | Check that the frozen choice is not specific to the exploration window. It is not a replacement for OOS. |
| Out of sample | `OutOfSample` | Final locked evaluation. Do not tune thresholds, wording, fill assumptions, or inclusion rules from this segment. |

The split is chronological per contract and per asset. There is no random row
split, shuffle, or mixing of future observations into the feature state. The
split boundary and timezone/clock convention are recorded before the run.
Events outside the declared contract resolution window, with unknown
resolution fidelity, or with look-ahead contamination are excluded and
counted in the data-quality report.

### Walk-forward procedure

For a walk-forward experiment, choose non-overlapping forward windows in
advance. `replay::WalkforwardRunner` runs each `WalkforwardWindow` with the
same frozen `ReplayConfig`; `replay::Split` labels the resulting rows. The
configuration includes thresholds, quant settings, tick size, size, fill
profile, latency profile, coverage, staleness policy, and the manifest ID.

The next window may begin only after the previous window's configuration is
frozen. Do not select a threshold by ranking the next window, and do not use
OOS rows to select a fill model or latency profile. Report every window and
its sample size, including windows with no fills.

## 3. Pre-registered stopping rule

Before starting a run, record a target `N_complete_pairs` in the run sheet.
`N_complete_pairs` is the number of eligible, complete A/B pairs required for
the primary paired analysis. Also record a maximum observation budget and the
end timestamp. Stop when the first of these occurs:

1. `N_complete_pairs` complete pairs have been collected;
2. the pre-registered maximum observation budget is exhausted; or
3. the pre-registered time/data end boundary is reached.

Never stop because PnL, hit rate, or markout is favorable or unfavorable. If
`N_complete_pairs` is not reached, label the result **underpowered** and do
not present it as confirmation. The stopping rule is the same for CONTROL and
QUANT_V1 and is not changed after looking at outcomes.

Always report, at minimum, `n_attempted`, `n_complete_pairs`,
`n_incomplete_pairs`, `n_analyzed_pairs`, `n_quoted`, `n_filled`, and the
number of non-null observations for each metric. A zero-fill segment is a
valid observation of execution conditions; it is not silently removed.

## 4. Pair completeness and exclusions

`ab_pairs.status = 'complete'` is the authority for paired analysis. A pair is
complete only when both branches produced a usable evaluation for the same
frozen snapshot. Rows with a missing branch, Jev failure, unusable signal, or
other branch-specific failure are retained for diagnostics and counted as
`incomplete`, but are excluded from paired differences and paired confidence
summaries.

A valid SKIP is a complete observation, not a broken pair. The strategy
saying "no quote" on a healthy evaluation still yields a `complete` pair plus
forward drift labels; only branch failures mark a pair `incomplete`. Historical
`incomplete_pair` flags that conflated "did not quote" with "branch failed"
must be recomputed under this rule before any A/B claim.

Do not infer a pair from adjacent `state_seq` values, timestamps alone, or
row order. Join on `run_id + pair_id`; include `variant` when joining branch
rows. Report incomplete pairs by market, asset, horizon, and failure reason
when that information is available.

## 4b. Two datasets: SIGNAL RESEARCH vs TRADING RESEARCH

- SIGNAL RESEARCH (`jev_signals` + `signal_markouts`, joined by `run_id +
  pair_id + variant`): every usable evaluation contributes forward drift at
  +1/+5/+10/+30/+60s, QUOTE or SKIP. This dataset answers "does Jev predict
  Polymarket moves" and supports threshold-ladder analysis (`E[drift |
  underreact_up > X]` for X in 0.31/0.45/0.60/0.75) without ever trading.
- TRADING RESEARCH (`paper_decisions` + `maker_markouts` + `paper_fills` +
  `paper_equity`): the quoted/filled subset. This answers "does the alpha
  survive maker execution".

Never lower thresholds to manufacture quotes for the trading dataset. If the
signal dataset shows no drift conditional on any threshold, stop before
optimizing execution: there is no alpha to monetize.

## 5. Primary alpha metrics

Alpha is the information/timing question: did the signal anticipate a useful
repricing before execution effects are considered? Report it separately from
whether a paper order filled.

Primary alpha outputs, by `variant × market_id × asset × horizon × split`:

- **Signed markout +5s**: mean `pnl_5s_pp` with the side sign preserved. A
  positive BUY markout means the YES mid rose after the quoted price.
- **Hit rate +5s**: fraction of non-null +5s markouts strictly greater than
  zero. Report its denominator; do not count missing markouts as losses.
- **Repricing accuracy**: the fraction of evaluated predictions whose realized
  direction/bucket agrees with the later signed move, using the declared
  repricing buckets and the same horizon for every variant.
- **Conditional markout**: `E[markout_5s | underreact_up > X]` (and the
  separately declared down-side analogue when applicable), computed by joining
  `signal_markouts` to `jev_signals` by `run_id + pair_id + variant` and
  filtering complete pairs. The maker-only analogue joins `maker_markouts`
  instead and answers the execution-conditional variant of the same question.

For A/B claims, report the paired QUANT_V1 minus CONTROL delta on the same
complete pairs, followed by the unpaired descriptive tables. Keep the
threshold `X` in the pre-registration; changing it is a new analysis, not a
free sensitivity check.

## 6. Execution and PnL metrics

Execution answers a different question and must not be substituted for alpha:

- **Fill rate**: fills divided by eligible resting quotes, with quote count and
  fill fraction reported separately.
- **Adverse selection**: signed post-fill markout, especially +1s and +5s,
  conditional on an actual paper fill. State the fill model and latency
  profile.
- **Realized PnL**: PnL after exits or YES/NO resolution settlement in the
  paper portfolio.
- **Total PnL**: realized plus mark-to-mid unrealized PnL at each
  `paper_equity` observation.
- **Drawdown**: peak-to-trough decline in the total-PnL equity curve, reported
  per variant and contract as well as for any explicitly defined aggregate.

Every execution table includes `variant`, `market_id`, `asset`, `horizon`,
fill model, latency profile, and sample size. A positive alpha markout with no
fills is not a positive trading PnL result; a filled result with negative
markout is not hidden by a favorable resolution outcome.

## 7. Reporting checklist

Each report must include:

1. run ID, manifest ID, frozen configuration, and split boundaries;
2. data coverage and resolution fidelity, with excluded counts;
3. counts for attempted, complete, incomplete, quoted, filled, and non-null
   observations;
4. alpha tables and paired deltas;
5. execution/PnL tables and drawdown;
6. results by BTC/ETH and by 5m/15m/1h/4h before any aggregate;
7. the pre-registered stopping rule and whether it was reached;
8. a statement that no thresholds or Jev wording were changed for the run.

If a segment is calm and produces zero trades, retain it and state that the
absence of fills is the observed execution result. Do not manufacture trades,
merge contracts to increase N, or use exploration data to imply OOS evidence.
