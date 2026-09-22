# Handoff: walk-forward validation program (next chat)

Status: plan adopted from senior review, NOT YET IMPLEMENTED. This chat
closed all open work without fabricating data. Next chat implements below.

## 1. Senior's prescribed order

1. Get/reuse books across MANY days and regimes (not more rows of one day).
2. Reconstruct the complement when only NO exists, labelled
   `SYNTHETIC_COMPLEMENT` (never as observed YES).
3. Causal Jev precompute (no signal at t may use information after t,
   even with precomputed answers).
4. WALK-FORWARD: train -> OOS -> roll -> train -> OOS (e.g. days 1-7 fit,
   8-9 test; roll forward). Purge + embargo around splits.
5. Only then: block bootstrap / resampling as robustness tests.
6. Finally: Monte Carlo for outcome distribution.
Do NOT start with Monte Carlo or resampling: with 0 trades they say
nothing, and bootstrapping one microstructure day fakes diversity.

## 2. Marvingozo reclassification (replaces "NO-only = unusable")

Old (too strong): NO-side only -> 0 YES states, dataset discarded.
New taxonomy per field:

| Field family | NO -> YES complement | Status |
|---|---|---|
| mid / fair value (`YES_mid = 1-NO_mid`) | exact by no-arbitrage (bots hold sum ~1.00-1.01) | USABLE |
| BBO (`YES_bid = 1-NO_ask`, `YES_ask = 1-NO_bid`, sizes carried) | synthetic reconstruction, same units | USABLE with label |
| economic depth | possibly reconstructible | VALIDATE FIRST |
| native YES queue / fill dynamics | NOT observed | FORBIDDEN to imply |
| `fill_before_decay` / `fill_toxic` on YES | needs extra modelling | QUARANTINED |

Rule: `SYNTHETIC_COMPLEMENT` rows may feed pricing/fair-value/direction
judgments; they must NEVER feed queue/fill conclusions or be presented as
observed YES books. marvingozo Mar-22 (333M rows, 8 BTC/ETH 4h markets) is
the prime complement candidate: dense, currently unused.

## 3. Why walk-forward is blocked today

Usable evaluated sample is one day: 2026-04-28, 115 5m markets, 34,246
tape rows, 67 live Jev states. One day split five ways is still one regime
(calm/volatile, up/down unknown). Walk-forward needs books across many
days/conditions: Apr-28/29/30 intraday + Jan-10, Mar-04/22, Apr-04, May-09
clusters already in selected_markets.parquet (340 dated markets, 14 days).

## 4. Assets ready (branch feat/book-data-telonex, nothing pushed)

Code (all gated: fmt + clippy -D warnings + tests green):
- `src/bin/precompute_jev.rs`: --tape mode, canonical hashing, versioned
  cache, hard live budget, K=10 incremental persist, 60s/state ceiling.
- `src/replay/versions.rs`, `src/jev/client.rs` (precompute retry),
  `src/replay/jev_cache.rs` (versioned keys + metadata).
- `src/replay/local_harness.rs`: deterministic sweep harness over
  evaluations.parquet.
- `research/scripts/build_kachoio_tape.py`: kachoio ETL (idempotent).
Data (gitignored): kachoio ticks/markets (~300MB), marvingozo orderbook
Mar-22 (2.2GB) + snapshots (251MB) + features/labels, Telonex D1 quotes.
Reports: `research-data/reports/kachoio-pilot-oos-01.md` (0/67 frozen
passes), `research-data/reports/jev-precompute-oos-01.md` (not-evaluable).
Caches: `research-data/cache/kachoio-pilot-01/` (67 live evals, run log).
Feature docs (gitignored): `odd/tasks/jev-precompute-backtest.md` (5 done),
`odd/tasks/book-data-acquisition.md` (done + this handoff pointer).

## 5. Budgets and secrets (as left)

- Telonex: 1 of 5 file downloads spent (D1 quotes). D2+ needs the user-run
  curl (no secure env-injection exists in agent shells). Key was pasted in
  chat: ROTATE it in Telonex dashboard.
- Jev pilot budget: 17 spent of 200 pre-registered for kachoio-pilot-01
  (+ unauditable killed-run calls, disclosed in run log). TYPESAFE_API_KEY
  lives in gitignored `.env` (dotenvy-loaded, never committed).
- Kaggle auth at `~/.kaggle/access_token` (chmod 600, outside repo). Keep
  for further downloads; revoke when program ends.

## 6. Suggested first tasks for next chat

1. Acquire multi-day YES-side books: Telonex D2+ (book_snapshot_5 Up) for
   15m/1h/4h Apr-28/29 + one earlier cluster; and/or more kachoio-style days.
2. Implement complement builder emitting `SYNTHETIC_COMPLEMENT` rows with
   the field taxonomy from section 2 enforced in code (queue/fill fields
   forced null + honest counters).
3. Extend precompute with causality audit (assert no post-t state in any
   input) + walk-forward runner (rolling train/OOS windows, purge+embargo).
4. New pre-registration per window; never tune thresholds/wording on OOS.

## 7. Standing prohibitions (carry over)

Paper-only. No live orders. No model distillation (TypeSafe ToS). No
threshold/wording/fill tuning on OOS. No push/merge without explicit user
authorization. No certainty claims: report CIs, N, regimes; zero-fill and
zero-quote are valid results.
