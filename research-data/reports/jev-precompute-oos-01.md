# Jev precompute OOS finding: `jev-precompute-oos-01`

**Status: NOT EVALUABLE on this corpus.** This is a paper/offline data finding,
not trading advice and not a profitability or certainty claim.

## Pre-registration

- **run_id:** `jev-precompute-oos-01`.
- **Target strata:** BTC/ETH x 5m/15m/1h/4h x point-in-time regime.
  The regime label is `vol_regime-trend_regime` (`docs/precompute-contract.md:56`).
- **Target:** `N_complete_pairs = 2,000-5,000` after all skips.
- **Fidelity:** `EXACT` only. The per-condition cap is 8 states
  (`docs/precompute-contract.md:57`; `src/bin/precompute_jev.rs:38,791-797`).
- **Live-call budget:** `0`, stub-only by explicit user decision on 2026-09-22.
- **End boundary:** exclusive UTC. The source filter is `ts < boundary`, so
  events at or after the boundary are excluded (`docs/precompute-contract.md:59`;
  `src/bin/precompute_jev.rs:218-224`).
- **Frozen V1:** thresholds, question wording, and fill assumptions unchanged.
  No OOS tuning was performed.

## Corpus evidence

The contract audit reports:

- `polymarket_trades.parquet`: **6,866 rows**.
- `underlying_all.parquet`: **97,589,798 rows** (about **97.6M**).
- `resolution_specs.parquet`: **340 EXACT** and **26 PROXY** specs; PROXY was
  not promoted into the primary sample (`docs/precompute-contract.md:22`).
- The selected-market map has no **ETH-1h** cell; it is an explicit empty
  stratum, not an imputation (`docs/precompute-contract.md:22,56`).
- The corpus contains no PolyTop book events/book geometry for this run.
  A trade without prior PolyTop geometry must be skipped rather than assigned
  fabricated bid/ask values (`docs/precompute-contract.md:24`;
  `src/replay/runner.rs:1251-1255`).
- The precompute path records missing depth/imbalance geometry and skips the
  state instead of fabricating it (`src/bin/precompute_jev.rs:237-238`).

## Run results

| Result | Observed value |
|---|---:|
| `precompute_jev` stub smoke `states` | **0** |
| `live_calls` | **0** |
| `incomplete_missing_book_geometry` | **6,538** |
| `N_complete_pairs` | **0** |

The 6,538 incomplete candidates were handled by the honest-skip rule. Nothing
was fabricated, and no live judgment was requested. The zero-state result is
consistent with the implementation counters (`src/bin/precompute_jev.rs:104-105,237-238,252,285`).

The local harness gates were green: **3/3 named tests** passed:
`same_rows_and_params_have_identical_report_bytes`,
`cache_mode_policy_segregates_stub_and_live_rows`, and
`higher_latency_profile_never_moves_decision_earlier`
(`src/replay/local_harness.rs:544-635`). These are harness checks only; with
zero real rows, there is no parameter-sweep or OOS result evidence.

Prior context, not pooled with this run: `campaign-btc-eth-oos-01` reports 544
live judgments, `underreact_up` maximum 0.50 versus the 0.75 gate, and 0
quotes (`research-data/reports/campaign-btc-eth-oos-01.md`).

## Empty breakdowns

Every requested asset/horizon/regime segment is empty because there are no
evaluable states. The empty breakdown is reported explicitly rather than
omitted.

| Asset | Horizon | Regime | `N_complete_pairs` | Metrics |
|---|---|---|---:|---|
| BTC | 5m | all registered regimes | 0 | unavailable: no evaluable states |
| BTC | 15m | all registered regimes | 0 | unavailable: no evaluable states |
| BTC | 1h | all registered regimes | 0 | unavailable: no evaluable states |
| BTC | 4h | all registered regimes | 0 | unavailable: no evaluable states |
| ETH | 5m | all registered regimes | 0 | unavailable: no evaluable states |
| ETH | 15m | all registered regimes | 0 | unavailable: no evaluable states |
| ETH | 1h | all registered regimes | 0 | unavailable: missing corpus cell and no evaluable states |
| ETH | 4h | all registered regimes | 0 | unavailable: no evaluable states |

| Aggregate | N | Markouts | Hit rate | Fill rate | Adverse selection | Realized PnL | Drawdown |
|---|---:|---|---|---|---|---|---|
| All strata | 0 | n/a | n/a | n/a | n/a | n/a | n/a |

## Conclusion

This strategy is **NOT EVALUABLE on this corpus without historical book
geometry**. The observed zero-state/zero-fill output is not alpha evidence for
or against the strategy. It does not prove or disprove predictive power,
markout behavior, fill behavior, adverse selection, profitability, or risk.
No certainty claim is warranted.

Exactly what is needed for a future evaluable run:

1. Historical PolyTop/order-book history sufficient to reconstruct the required
   book geometry and fill labels; **or**
2. An explicitly labelled print-touch proxy with sensitivity analysis.

Both alternatives were declined for this run. No live spend, trading advice,
or profitability conclusion follows from this report.
