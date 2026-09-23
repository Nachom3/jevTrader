# OOS report: walkforward-01 w14 live pilot (Apr-28 5m YES tape)

> Pre-registered run walkforward-prereg-01.md, window w14 only
> (test 2026-04-28T00:01Z – 2026-04-30T00:01Z). Frozen V1 (thresholds,
> wording, fills unchanged; git status on src/tests verified clean of our
> edits before/after). Paper/offline only. No certainty claims. No live
> calls beyond this pilot without new explicit authorization (live frozen).

## Pre-registration

- Window w14, corpus multiday tape slice Apr-28 (EXACT, kachoio-v1).
- 67 live calls spent here (0 errors, 0 causality skips), 120,923 in /
  15,574 out tokens. HONEST OVERSPEND NOTE: w14's per-window ceiling was 7;
  the process-wide CLI cap cannot enforce per-window budgets, so w14
  consumed 67. Remaining: 116 of 183. No re-allocation decided.
- Gate (frozen): `underreact_up>.75 && p_up>=1tick>.65 && persists>.60 &&
  fill>.60 && toxic<.30 && conflict<.30 && no_pressure<.30`.

## Results (67 live evaluations)

| Signal | Min | Mean | Max | Frozen gate |
|---|---:|---:|---:|---|
| underreact_up | 0.33 | 0.40 | 0.49 | >0.75: 0/67 |
| underreact_down (conflict) | 0.38 | 0.48 | 0.65 | <0.30: 0/67 |
| move_persists | 0.34 | 0.41 | 0.57 | >0.60: 0/67 |
| fill_before_decay | 0.14 | 0.34 | 0.47 | >0.60: 0/67 |
| fill_toxic | 0.31 | 0.42 | 0.52 | <0.30: 0/67 |
| no_pressure_5s | 0.35 | 0.57 | 0.78 | <0.30: 0/67 |
| p_up_ge_1_tick (derived) | 0.07 | 0.30 | 0.82 | >0.65: 1/67 |

Conjunction (all frozen gates): **0 / 67 quotes**.

## Reading (not tuning)

Exact replication of kachoio-pilot-01 (same 67 Apr-28 states, same 0/67):
third independent 5m crypto sample at zero quotes; underreact_up ceiling
0.49 again. The strategy sees direction (p_up max 0.82) but never judges
the book underreacted enough to quote at frozen thresholds. Do NOT lower
thresholds on this sample.

Artifacts: research-data/cache/walkforward-01/live-pilot/ (gitignored).
Analysis: research/scripts/analyze_evaluations.py (p_up derived from
repricing bucket JSON: up_3_plus + up_2 + up_1).
