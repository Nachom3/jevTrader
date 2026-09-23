# Walk-forward preregistration 01

Status: preregistration frozen before the stub run. No live Jev calls are authorized by this document.

## Dataset and window construction

Source: `research-data/processed/kachoio_multiday.manifest.json`. The manifest declares UTC dates from 2026-03-24 through 2026-05-18, EXACT fidelity, and 5m-only tape scope. The final date is partial: it has 38,100 one-second ticks for each asset, corresponding to coverage through `2026-05-18T10:35:00Z`. Its manifest date coverage, not a later outcome, sets the final evaluation boundary.

All intervals are half-open `[start, end)`, UTC. Anchor the first train start at `2026-03-24T00:00:00Z`; train for 7 days; move each train start forward by 2 days; leave a 60-second embargo after `train_end`; then evaluate a 48-hour OOS interval. Thresholds are fit once per window using only that window's InSample items via `run_temporal_windows`; its OutOfSample rows are evaluation-only. The embargo interval `[train_end, test_start)` is unassigned. This is the explicit operationalization of the frozen 60,000 ms embargo.

Purge each window's training markets before fitting: retain a market only when `resolution_ms <= train_end_ms - 300,000`; drop any training market resolving after that cutoff. This removes training labels whose outcomes can extend beyond the training boundary. The purge does not discard OOS outcomes.

There are 24 complete two-day OOS windows and one final truncated window. The final window uses the available partial May 18 coverage; it is not silently extended to May 20.

| Window | InSample `[train_start, train_end)` | OutOfSample `[test_start, test_end)` | Live-call ceiling |
|---|---|---|---:|
| w00 | 2026-03-24T00:00:00Z – 2026-03-31T00:00:00Z | 2026-03-31T00:01:00Z – 2026-04-02T00:01:00Z | 8 |
| w01 | 2026-03-26T00:00:00Z – 2026-04-02T00:00:00Z | 2026-04-02T00:01:00Z – 2026-04-04T00:01:00Z | 8 |
| w02 | 2026-03-28T00:00:00Z – 2026-04-04T00:00:00Z | 2026-04-04T00:01:00Z – 2026-04-06T00:01:00Z | 8 |
| w03 | 2026-03-30T00:00:00Z – 2026-04-06T00:00:00Z | 2026-04-06T00:01:00Z – 2026-04-08T00:01:00Z | 8 |
| w04 | 2026-04-01T00:00:00Z – 2026-04-08T00:00:00Z | 2026-04-08T00:01:00Z – 2026-04-10T00:01:00Z | 8 |
| w05 | 2026-04-03T00:00:00Z – 2026-04-10T00:00:00Z | 2026-04-10T00:01:00Z – 2026-04-12T00:01:00Z | 8 |
| w06 | 2026-04-05T00:00:00Z – 2026-04-12T00:00:00Z | 2026-04-12T00:01:00Z – 2026-04-14T00:01:00Z | 8 |
| w07 | 2026-04-07T00:00:00Z – 2026-04-14T00:00:00Z | 2026-04-14T00:01:00Z – 2026-04-16T00:01:00Z | 8 |
| w08 | 2026-04-09T00:00:00Z – 2026-04-16T00:00:00Z | 2026-04-16T00:01:00Z – 2026-04-18T00:01:00Z | 7 |
| w09 | 2026-04-11T00:00:00Z – 2026-04-18T00:00:00Z | 2026-04-18T00:01:00Z – 2026-04-20T00:01:00Z | 7 |
| w10 | 2026-04-13T00:00:00Z – 2026-04-20T00:00:00Z | 2026-04-20T00:01:00Z – 2026-04-22T00:01:00Z | 7 |
| w11 | 2026-04-15T00:00:00Z – 2026-04-22T00:00:00Z | 2026-04-22T00:01:00Z – 2026-04-24T00:01:00Z | 7 |
| w12 | 2026-04-17T00:00:00Z – 2026-04-24T00:00:00Z | 2026-04-24T00:01:00Z – 2026-04-26T00:01:00Z | 7 |
| w13 | 2026-04-19T00:00:00Z – 2026-04-26T00:00:00Z | 2026-04-26T00:01:00Z – 2026-04-28T00:01:00Z | 7 |
| w14 | 2026-04-21T00:00:00Z – 2026-04-28T00:00:00Z | 2026-04-28T00:01:00Z – 2026-04-30T00:01:00Z | 7 |
| w15 | 2026-04-23T00:00:00Z – 2026-04-30T00:00:00Z | 2026-04-30T00:01:00Z – 2026-05-02T00:01:00Z | 7 |
| w16 | 2026-04-25T00:00:00Z – 2026-05-02T00:00:00Z | 2026-05-02T00:01:00Z – 2026-05-04T00:01:00Z | 7 |
| w17 | 2026-04-27T00:00:00Z – 2026-05-04T00:00:00Z | 2026-05-04T00:01:00Z – 2026-05-06T00:01:00Z | 7 |
| w18 | 2026-04-29T00:00:00Z – 2026-05-06T00:00:00Z | 2026-05-06T00:01:00Z – 2026-05-08T00:01:00Z | 7 |
| w19 | 2026-05-01T00:00:00Z – 2026-05-08T00:00:00Z | 2026-05-08T00:01:00Z – 2026-05-10T00:01:00Z | 7 |
| w20 | 2026-05-03T00:00:00Z – 2026-05-10T00:00:00Z | 2026-05-10T00:01:00Z – 2026-05-12T00:01:00Z | 7 |
| w21 | 2026-05-05T00:00:00Z – 2026-05-12T00:00:00Z | 2026-05-12T00:01:00Z – 2026-05-14T00:01:00Z | 7 |
| w22 | 2026-05-07T00:00:00Z – 2026-05-14T00:00:00Z | 2026-05-14T00:01:00Z – 2026-05-16T00:01:00Z | 7 |
| w23 | 2026-05-09T00:00:00Z – 2026-05-16T00:00:00Z | 2026-05-16T00:01:00Z – 2026-05-18T00:01:00Z | 7 |
| w24 | 2026-05-11T00:00:00Z – 2026-05-18T00:00:00Z | 2026-05-18T00:01:00Z – 2026-05-18T10:35:00Z (truncated) | 7 |

Budget allocation is fixed before the pilot: 8 calls for w00–w07 and 7 calls for w08–w24, totaling exactly 183. These are hard per-window ceilings, not targets; unused calls do not transfer between windows.

## Frozen strategy and execution parameters

Sources: `docs/strategy-lead-lag-v1.md`, the implemented `src/strategy/lead_lag.rs::should_quote`, and committed `src/config.rs` (read via `git show HEAD:src/config.rs`; the modified worktree copy was not used).

- Quote only when all implemented gates pass: `underreact_up > 0.75`; `p_up_ge_1_tick > 0.65`; `move_persists > 0.60`; `fill_before_decay > 0.60`; `fill_toxic < 0.30`; `underreact_down < 0.30`; and `no_pressure_5s < 0.30`.
- Quote action: post-only BUY YES one tick above the bid, or no quote.
- The strategy document's illustrative initial gate lists the first six predicates but omits `no_pressure_5s`; the executable `should_quote` also enforces it, and committed config supplies `no_pressure_max = 0.30`. This preregistration freezes the implemented conjunction; no gate is removed or relaxed.
- Fees: TBC. Neither cited strategy document nor committed config specifies a fee value.
- Slippage: TBC. Neither cited strategy document nor committed config specifies a slippage value.
- Order size / sizing rule: TBC. Neither cited strategy document nor committed config specifies a size.
- The threshold-fitting objective/search procedure is TBC in the cited materials. Before any live evaluation, freeze it using InSample data only; do not derive it from OOS outcomes.

## Analysis and stopping rules

- No tuning, threshold relaxation, question-wording changes, or fill-model changes after looking at any OOS result. Keep every window's fitted thresholds frozen for that window's OOS interval.
- Zero quotes and zero fills are valid results. Report them as observed; do not retune to force activity.
- Report all planned windows, including empty/insufficient-data windows and the truncated final window. Do not drop a window based on its outcomes.

## Stub precompute record

Command to run (offline stub mode; deliberately no `--live`):

```sh
target/debug/precompute_jev --tape research-data/processed/kachoio_polytop_multiday.parquet --max-live-calls 0 --max-states 200 --per-condition-signals 8 --run-id walkforward-01-stub --out research-data/cache/walkforward-01
```

Observed result: completed successfully using the existing `target/debug/precompute_jev` binary; no `--live` flag was present and `TYPESAFE_API_KEY` was unset for the process. No downloads or live provider calls occurred. Output files were written only beneath `research-data/cache/walkforward-01/`.

| Manifest metric | Observed |
|---|---:|
| states prepared / persisted complete pairs | 67 / 67 |
| eligible conditions | 97 |
| `skipped_causality_audit` | 0 |
| live calls | 0 |
| status `ok` / `hit` / API errors | 67 / 0 / 0 |
| configured state cap | 200 |

The multiday tape was the input, but the local selected-market metadata yielded 97 eligible conditions; the state cap was not reached. The manifest also recorded `incomplete_missing_kachoio_outcome=23`, versus 18 in the April 28 pilot README; this is an existing category, not a new one. Compared with the April 28 categories, the only newly present skip category is `skipped_causality_audit` (count 0). No category present in the April 28 README disappeared from the new manifest.

Persisted artifacts: `evaluations.manifest.json`, `evaluations.parquet`, `jev_cache.json`, and `jev_cache_metadata.json`, all under the authorized output directory. Manifest confirms `live=false`, `max_live_calls=0`, and `counts.live_calls=0`. The 67 stub evaluations do not represent 67 provider calls.

## Live pilot command — awaiting explicit user approval

Not executed. This is a bounded Jev live-precompute command, not a claim that the current binary itself performs the per-window fit/evaluation above:

```sh
cargo run --release --bin precompute_jev -- --tape research-data/processed/kachoio_polytop_multiday.parquet --live --max-live-calls 183 --max-states 200 --per-condition-signals 8 --end-boundary 2026-05-18T10:35:00Z --run-id walkforward-01-live-pilot --out research-data/cache/walkforward-01/live-pilot
```

The CLI has one process-wide live-call cap and no per-window start-boundary/fit interface. Do not treat that command as enforcing the 8/7 per-window allocation or as executing the walk-forward evaluation; the live stage remains pending user approval and appropriate per-window orchestration.

## Pre-execution amendment: early-window asset coverage (no results seen)

Verifier note, recorded before any live execution: ETH tape coverage begins 2026-04-05; 2026-03-24–04-04 is BTC-only (Mar-24 partial: 22 markets). Windows w00–w05 therefore train on BTC only (their tests up to Apr-12 partially so). Bounds, budgets, purge/embargo, and rules are unchanged; this records that early windows are single-asset and mixed-asset inference starts where joint coverage starts.
