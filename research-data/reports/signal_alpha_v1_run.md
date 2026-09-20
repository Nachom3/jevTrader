# SIGNAL ALPHA run: signal-alpha-v1 (pre-registered)

## Pre-registration
- run_id: `signal-alpha-v1` | date: 2026-09-19/20 UTC
- branch: `feat/replay-quality-signal-alpha` | code: commit `ad1526c`
- command: `historical_backtest -- --max-pairs 550 --exact-only 1
  --per-condition-cap 10 --latency EMPIRICAL --latency-out
  research-data/reports/jev_latency_signal_alpha_v1.json --real-jev 1
  --max-jev-calls 1200 --out research-data/reports/signal_alpha_v1.json
  --run-id signal-alpha-v1` (JEV_DEADLINE_MS default 1500, JEV_VERBOSE=1)
- stopping rule: stop at N_complete_pairs=500 EXACT pairs, or 1200-call
  budget, or tape end. Target NOT reached -> result labeled UNDERPOWERED
  per methodology-ab-pnl.md §3. No early stop on results.
- frozen for the run: thresholds, Jev wording (8 lead-lag questions),
  CONSERVATIVE fills, default staleness. No tuning on any split (0 quotes,
  nothing changed). Splits reported separately, descriptively.

## Data provenance
- Polymarket tape: 6866 trades / 72 conditions, 2025-12-23..2026-04-28
  (TimeSeventeen daily_aligned, YES-normalized, GROUND_TRUTH direction).
- Underlying: BTC/ETH aggTrades for the tape window (254 daily zips +
  Apr29-30 backfill, gaps=0), merged 40,114,446 rows; read PER CONDITION
  ([first-2h, last], thin 16 ~1 tick/s, row-group skipping on ts_ms stats).
- ResolutionSpec: 340 EXACT / 26 PROXY / 0 UNKNOWN via SII event_id ->
  Gamma /events/<id> matched on conditionId (fetch_event_specs.py).
  68 EXACT conditions intersect the tape.
- Latency: EMPIRICAL profile on pilot placeholder [307,1019]ms; this run
  wrote the real 565-sample distribution (jev_latency_signal_alpha_v1.json).

## Outcome
- 796 rows = 398 pairs total; EXACT-only: 716 rows = 358 pairs,
  of which 345 complete (13 incomplete from Jev 0.99-sum parse errors,
  ~4%; tolerance widened to 0.02 post-run, values kept verbatim).
- jev_live_calls=582/1200 budget; stale=0; quotes=0; fills=0 (thresholds
  frozen; underreact_up observed live at 0.21-0.33, never near quote).
- Forward drift +5s on complete EXACT pairs (QUOTE or SKIP):
  CONTROL n=291 mean +0.0825pp hit 0.058;
  QUANT_V1 n=291 mean +0.0825pp hit 0.058.
- Paired QUANT_V1-CONTROL delta on same pairs: 0.0000 (n=291).
- Observed Jev latency: n=565, min 296ms, max 960ms, mean 395ms
  (320ms fixed was optimistic; EMPIRICAL is now the default).
- Outlier anatomy: the +0.08 headline comes from ONE BTC-5m market
  (btc-updown-5m-1777380900, Apr 28) with seven +3.0pp steps; excluding
  the BTC-5m LOW_VOL cell, REST n=566 mean +0.0018pp hit 0.034.
  Mechanism (expiry snap vs single bad print) unresolved; untradeable
  either way with 0 quotes. No threshold-ladder signal to condition on.

## Verdict
NO alpha: Jev does not predict +5s Polymarket drift in this sample, and
QUANT enrichment adds nothing over CONTROL (delta exactly 0). Per the
standing order (no alpha -> no execution optimization): maker simulator,
threshold changes, and live specs stay OFF. Full table:
signal_alpha_v1.md (segments incl. hit_mo5s/n_drift5s/mean_drift5s).

## Post-run fixes (committed AFTER this run, for reruns)
- exact-only leak: non-EXACT conditions replayed with default UNKNOWN meta
  (80 rows in this file) -> now skipped and counted (skipped_no_meta).
- pair_id collision across per-condition calls -> pair_namespace=condition_id.
- repricing-sum tolerance 1e-6 -> 0.02 (recovers the 0.99-rounding calls).
