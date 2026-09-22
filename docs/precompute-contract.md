# Jev precompute contract (V1)

Status: T1 audit for `jev-precompute-backtest`. This is a paper/offline data contract, not a trading recommendation.

## 1. Corpus inventory

Counts and UTC ranges below were read from Parquet metadata/read-only scans on 2026-09-19. Parquet is binary, so those observations have no line address; code and script claims use `file:line` citations. The validator itself reports selected-market groups, fidelity, and the three primary files ([`research/scripts/validate_corpus.py:10-46`](../research/scripts/validate_corpus.py)).

| file | rows | UTC coverage observed |
|---|---:|---|
| `polymarket_trades.parquet` | 6,866 | `ts`: 2025-12-23 18:11:47 to 2026-04-28 10:54:06 |
| `underlying_all.parquet` | 97,589,798 | `ts_ms`: 2025-12-23 00:00:00.006 to 2026-04-30 23:59:59.852 |
| `underlying_market_data.parquet` | 40,114,446 | `ts_ms`: 2025-12-23 00:00:00.006 to 2026-04-30 23:59:59.852 |
| `underlying_perp.parquet` | 57,475,352 | `ts_ms`: 2025-12-23 00:00:00.012 to 2026-04-30 23:59:59.783 |
| `selected_markets.parquet` | 366 | non-empty `start_at`/`end_at`: 2025-12-24 09:00 to 2026-07-02 20:00 |
| `resolution_specs.parquet` | 366 | non-empty `start_at`/`end_at`: 2025-12-24 09:00 to 2026-07-02 20:00 |
| `resolutions.parquet` | 72 | `resolved_ts`: 2026-03-22 12:00:55 to 2026-04-29 02:30:36 |
| `market_regimes.parquet` | 614 | `day_ts`: 2024-01-01 to 2026-04-30 |

`selected_markets` is BTC/ETH x 5m/15m/1h/4h, but the observed groups are BTC-15m(60), BTC-1h(6), BTC-4h(60), BTC-5m(60), ETH-15m(60), ETH-4h(60), ETH-5m(60); the validator output shows the source groups and the missing ETH-1h bucket ([`research/scripts/validate_corpus.py:13-19`](../research/scripts/validate_corpus.py)). **Correction:** the observed file has no ETH-1h rows and only six BTC-1h rows; do not synthesize the missing cell.

Fidelity is an explicit `Fidelity::{Exact,Proxy,Unknown}` contract; primary evidence must filter to `EXACT` ([`src/replay/types.rs:50-69`](../src/replay/types.rs)). Current `resolution_specs` is EXACT=340, PROXY=26, UNKNOWN=0 (92.9%/7.1%/0.0%), as reported by the validator ([`research/scripts/validate_corpus.py:25-39`](../research/scripts/validate_corpus.py)). Preserve the label in every precompute row; `PROXY` and `UNKNOWN` are diagnostic only and must not be silently promoted ([`src/bin/historical_backtest.rs:188-204`](../src/bin/historical_backtest.rs)).

The binary reads at most 1,000,000 tape rows ([`src/bin/historical_backtest.rs:291-294`](../src/bin/historical_backtest.rs)) and defaults the per-condition cap to 4, clamped to 1..=100 ([`src/bin/historical_backtest.rs:778-785`](../src/bin/historical_backtest.rs)). Underlying windows are separately capped at 50,000 rows per asset leg ([`src/bin/historical_backtest.rs:627-650`](../src/bin/historical_backtest.rs)). A `PolyTrade` without a prior `PolyTop` for its event stream is dropped rather than given fabricated bid/ask geometry ([`src/replay/runner.rs:1246-1261`](../src/replay/runner.rs)); this is the PolyTop-missing limitation and must be counted as incomplete.

**Filename mismatch.** `historical_backtest` constructs `underlying_all.parquet` ([`src/bin/historical_backtest.rs:298-302`](../src/bin/historical_backtest.rs)); the checked-in ETL defaults write `underlying_market_data.parquet` ([`research/scripts/build_underlying.py:1,50-51`](../research/scripts/build_underlying.py), [`research/scripts/preprocess.py:1,27-30`](../research/scripts/preprocess.py)). The current processed directory contains `underlying_all`, `underlying_market_data`, and `underlying_perp`, but no `underlying_p1..p5`; no checked-in producer for those names was found. **T2 gap:** choose one manifest-driven input (or explicitly build/validate the `p1..p5` shards) before precompute.

## 2. Frozen V1State inventory

`V1State` serializes exactly five top-level fields: `market`, `underlying`, `polymarket`, optional `quant`, and `candidate_order` ([`src/jev/request.rs:48-58`](../src/jev/request.rs)). All are market-time state; portfolio state is excluded.

- **`market` (safe to precompute):** `question`, `resolution_source`, `resolution_rules` ([`src/jev/request.rs:14-23`](../src/jev/request.rs)).
- **`underlying` (safe to precompute):** `target`, `time_remaining_secs`, `resolution_source`; contract labels `asset_symbol`, `horizon_label`, `horizon_secs`; `spot`, `distance_to_target_pct`; returns `ret_250ms_pct`, `ret_1s_pct`, `ret_5s_pct`, `ret_30s_pct`, `ret_1m_pct`, `ret_5m_pct`, `ret_15m_pct`, `ret_30m_pct`, `ret_1h_pct`; realized vol `realized_vol_1m_pct`, `realized_vol_5m_pct`, `realized_vol_1h_pct`; venue values `binance_microprice`, `coinbase_microprice`, `perp_price`, `perp_basis_pct`; external flow `buy_vol_1s`, `sell_vol_1s`, `ofi_1s`, `ofi_5s`, `book_imbalance`, `aggressive_buy_ratio`; V2 tape fields `poly_ofi_5s`, `poly_aggressive_buy_ratio`, `poly_buy_vol_5s`, `poly_sell_vol_5s`; cross-venue `binance_coinbase_diff_pct`, `spot_perp_diff_pct` ([`src/strategy/lead_lag.rs:21-66`](../src/strategy/lead_lag.rs)).
- **`polymarket` (safe to precompute from the point-in-time local book/tape):** `yes_bid`, `yes_ask`, `bid_depth`, `ask_depth`, `spread`, `book_imbalance`, `last_trade_price`, `price_1s_ago`, `price_5s_ago`, `price_30s_ago` ([`src/strategy/lead_lag.rs:68-86`](../src/strategy/lead_lag.rs)). Executable prices serialize as integer micro-units ([`src/strategy/lead_lag.rs:93-112`](../src/strategy/lead_lag.rs)).
- **`quant` (safe to precompute deterministically):** optional `move_zscore_1s`, `z_vol_pct`, `z_vol_source`, `distance_sigma`, `sigma_horizon_secs`, `quant_baseline_p_yes`, `baseline_model`, `baseline_sigma_pct`, `baseline_time_remaining_secs`, `vol_regime_ratio`; `quant: null` is the disabled path ([`src/state/quant_features.rs:77-90`](../src/state/quant_features.rs), [`src/jev/request.rs:53-56`](../src/jev/request.rs)).
- **`candidate_order` (derived, not independently sampled):** fixed `side=BUY_YES_MAKER` and `time_in_force=POST_ONLY`; derive `price` from the point-in-time local best bid by rounding to the tick grid and adding exactly one tick ([`src/jev/request.rs:25-45`](../src/jev/request.rs), [`src/engine/pipeline.rs:135-156`](../src/engine/pipeline.rs)). Reject if it crosses the ask or cannot be represented; do not infer a book from an unbooked print ([`src/engine/pipeline.rs:1320-1360`](../src/engine/pipeline.rs)).

**MUST NEVER enter the state:** position/open quantity, average entry, PnL, inventory, sizing, thresholds, or risk-gate state. Strategy context exposes open quantity separately ([`src/strategy/api.rs:38-46`](../src/strategy/api.rs)); replay owns PnL/portfolio mechanics ([`src/replay/mod.rs:4-11`](../src/replay/mod.rs)). Thresholds live only in `QuoteThresholds` ([`src/config.rs:82-118`](../src/config.rs)), `should_quote` ([`src/strategy/lead_lag.rs:115-123`](../src/strategy/lead_lag.rs)), and `RiskGate` ([`src/strategy/risk.rs:50-121`](../src/strategy/risk.rs)).

## 3. Serialization and cache identity

The exact current path is typed `Serialize` on `V1State` ([`src/jev/request.rs:48-58`](../src/jev/request.rs)), then compact `serde_json::to_string(&state)`; the runner hashes those exact bytes ([`src/replay/runner.rs:529-556`](../src/replay/runner.rs)), and the campaign serializes both state and questions before hashing ([`src/replay/campaign.rs:357-370`](../src/replay/campaign.rs)).

**Frozen rule for T2:** canonicalize recursively by lexicographically sorting object keys, preserve array order, emit compact UTF-8 JSON, retain finite numeric values exactly as serde emits them, and hash the resulting bytes. Keep the `price_ticks_serde` integer representation. Any canonicalizer or numeric-normalization change increments `serialization_version` and `normalization_version`; never hash pretty JSON or a language-native map iteration order.

Freeze this full tuple in every evaluation/cache row:

`(model_id, model_version, strategy_version, prompt_version, question_document_sha256, question_schema_version, feature_builder_version, normalization_version, serialization_version, variant)`.

Current values/evidence are `model_id=jev-latest` ([`src/jev/request.rs:9-11`](../src/jev/request.rs)), `strategy_version=v1-lead-lag`, `prompt_version=v1-lead-lag`, `model_version=jev-latest`, `question_schema_version=v1` ([`src/replay/campaign.rs:43-45`](../src/replay/campaign.rs), [`src/strategy/api.rs:100-103`](../src/strategy/api.rs)); the embedded document and eight IDs are defined by `src/jev/questions_md.rs` ([`src/jev/questions_md.rs:9-21`](../src/jev/questions_md.rs)). T2 must add explicit pins for the feature builder, normalization, serialization, and question-document digest rather than treating them as implicit.

`JevCacheKey` already stores `state_hash`, `questions_hash`, `model`, `variant`, `strategy_version`, `prompt_version`, `model_version`, and `question_schema_version` ([`src/replay/jev_cache.rs:24-36`](../src/replay/jev_cache.rs)); `new_versioned` accepts those fields ([`src/replay/jev_cache.rs:65-86`](../src/replay/jev_cache.rs)). It lacks explicit feature-builder, normalization, serialization/canonicalization, and question-document-digest identity. The legacy constructor fills version fields with `unknown` ([`src/replay/jev_cache.rs:49-61`](../src/replay/jev_cache.rs)); precompute must not use that constructor.

## 4. Eval sampling prescription

- **Strata:** asset `{BTC, ETH}` x horizon `{5m, 15m, 1h, 4h}` x the point-in-time regime label from `market_regimes` (`vol_regime-trend_regime`) ([`src/replay/source.rs:660-689`](../src/replay/source.rs)). Require `EXACT`; retain `PROXY`/`UNKNOWN` only as separately labelled diagnostics ([`src/replay/types.rs:50-69`](../src/replay/types.rs)). The missing ETH-1h cell is an explicit empty stratum, not an imputation.
- **Budget:** target 2,000-5,000 complete pairs; preregister `per_condition_signals=8` as the initial cap, then take at most eight point-in-time states per eligible condition, spread across its trajectory. With 340 current EXACT conditions this is a nominal 2,720-pair ceiling before incomplete-pair exclusions. The existing runner's default cap is 4, so T2 must make the new budget explicit rather than inherit a CLI default ([`src/bin/historical_backtest.rs:778-785`](../src/bin/historical_backtest.rs)).
- **Split:** preserve each condition's existing `replay::Split` assignment (`EXPLORATION`, `VALIDATION`, or `OUT_OF_SAMPLE`) and do not randomize rows across splits ([`src/replay/types.rs:102-118`](../src/replay/types.rs), [`src/replay/source.rs:632-635`](../src/replay/source.rs)). Tune wording/thresholds only on Exploration, use Validation once for lock decisions, and evaluate OutOfSample without feedback; the split contract explicitly says OOS is never used for threshold tuning ([`src/replay/types.rs:102-103`](../src/replay/types.rs)).
- **Pre-registration record:** at minimum freeze `run_id`, `N_complete_pairs` (after all skips), live-call `budget`, and an exclusive UTC `end_boundary`; also record the stratum map, per-condition budget, fidelity filter, split counts, and the full version tuple above. The current runner exposes a hard live-call budget ([`src/bin/historical_backtest.rs:808-812`](../src/bin/historical_backtest.rs)); the boundary is required to prevent later corpus additions from changing the sample.

## T2 open gaps

1. Resolve `underlying_all` versus ETL output/shard naming and record the chosen file(s) in the corpus manifest.
2. Add explicit feature-builder, normalization, serialization, and question-document digest pins to precompute/cache identity.
3. Implement stratified sampling with the missing ETH-1h cell and PolyTop-missing counts surfaced, not silently filled.
4. Ensure precompute uses canonical state bytes and `JevCacheKey::new_versioned`.
