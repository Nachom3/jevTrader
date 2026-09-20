# Jev diagnostic report

## Per-question distribution and variance
| question | n | mean | std | min | max | std_within_cond | std_between_cond |
|---|---|---|---|---|---|---|---|
| yes_pressure_5s | 180 | 0.328 | 0.145 | 0.150 | 0.790 | 0.119 | 0.065 |
| no_pressure_5s | 180 | 0.539 | 0.182 | 0.130 | 0.810 | 0.173 | 0.034 |
| move_persists | 180 | 0.347 | 0.070 | 0.240 | 0.670 | 0.064 | 0.021 |
| underreact_up | 180 | 0.375 | 0.054 | 0.270 | 0.570 | 0.052 | 0.014 |
| underreact_down | 180 | 0.482 | 0.093 | 0.300 | 0.690 | 0.086 | 0.028 |
| p_up | 180 | 0.185 | 0.158 | 0.010 | 0.700 | 0.127 | 0.079 |
| fill_before_decay | 180 | 0.350 | 0.043 | 0.240 | 0.500 | 0.034 | 0.021 |
| fill_toxic | 180 | 0.403 | 0.063 | 0.290 | 0.540 | 0.051 | 0.036 |

## Pearson(answer, feature) — fixed feature list
| question | ret_5s_pct | ret_1m_pct | realized_vol_1m_pct | distance_to_target_pct | time_remaining_secs | move_zscore_1s | ofi_5s | spread | poly_ofi_5s | perp_basis_pct |
|---|---|---|---|---|---|---|---|---|---|---|
| yes_pressure_5s | +0.39 | +0.42 | -0.06 | +0.23 | -0.21 | +0.33 | +0.19 | +0.11 | -0.25 | -0.19 |
| no_pressure_5s | -0.46 | -0.42 | +0.46 | -0.46 | -0.07 | -0.25 | -0.11 | -0.06 | -0.10 | +0.43 |
| move_persists | -0.21 | -0.18 | +0.41 | -0.18 | -0.28 | +0.16 | +0.07 | +0.06 | -0.08 | +0.25 |
| underreact_up | +0.12 | +0.24 | +0.19 | +0.02 | -0.17 | +0.20 | +0.16 | +0.01 | -0.34 | +0.01 |
| underreact_down | -0.34 | -0.43 | +0.37 | -0.64 | -0.09 | -0.23 | -0.14 | -0.11 | -0.14 | +0.52 |
| p_up | +0.00 | +0.08 | +0.10 | +0.16 | -0.44 | +0.10 | +0.09 | -0.00 | -0.06 | -0.01 |
| fill_before_decay | -0.04 | +0.13 | +0.15 | +0.20 | -0.20 | +0.06 | +0.13 | +0.00 | +0.14 | -0.06 |
| fill_toxic | -0.33 | -0.30 | +0.40 | -0.16 | -0.42 | -0.04 | +0.01 | -0.38 | +0.07 | +0.26 |

## Monotonicity: mean drift_5s by answer bucket
- yes_pressure_5s: n=54 m=+0.000 | n=25 m=+0.000 | n=30 m=+0.000 | n=36 m=+0.000 | n=35 m=+0.000
- no_pressure_5s: n=37 m=+0.000 | n=38 m=+0.000 | n=40 m=+0.000 | n=31 m=+0.000 | n=34 m=+0.000
- move_persists: n=45 m=+0.000 | n=29 m=+0.000 | n=43 m=+0.000 | n=34 m=+0.000 | n=29 m=+0.000
- underreact_up: n=46 m=+0.000 | n=27 m=+0.000 | n=36 m=+0.000 | n=43 m=+0.000 | n=28 m=+0.000
- underreact_down: n=38 m=+0.000 | n=35 m=+0.000 | n=38 m=+0.000 | n=36 m=+0.000 | n=33 m=+0.000
- p_up: n=37 m=+0.000 | n=36 m=+0.000 | n=36 m=+0.000 | n=37 m=+0.000 | n=34 m=+0.000
- fill_before_decay: n=49 m=+0.000 | n=36 m=+0.000 | n=36 m=+0.000 | n=24 m=+0.000 | n=35 m=+0.000
- fill_toxic: n=40 m=+0.000 | n=33 m=+0.000 | n=50 m=+0.000 | n=24 m=+0.000 | n=33 m=+0.000

## MICRO_V2-CONTROL paired deltas per question
| question | n_pairs | mean_abs_delta | P(abs>=0.10) | sign_agreement |
|---|---|---|---|---|
| yes_pressure_5s | 60 | 0.147 | 0.65 | 0.71 |
| no_pressure_5s | 60 | 0.236 | 0.63 | 0.82 |
| move_persists | 60 | 0.091 | 0.40 | 0.95 |
| underreact_up | 60 | 0.074 | 0.33 | 1.00 |
| underreact_down | 60 | 0.101 | 0.45 | 0.98 |
| p_up | 60 | 0.054 | 0.15 | 0.83 |
| fill_before_decay | 60 | 0.020 | 0.00 | 1.00 |
| fill_toxic | 60 | 0.037 | 0.02 | 1.00 |

## Sensitivity: probe market vs rest (mean answer)
| question | probe_mean | rest_mean | probe_std | rest_std |
|---|---|---|---|---|
| yes_pressure_5s | 0.365 (n=30) | 0.321 (n=150) | 0.132 | 0.146 |
| no_pressure_5s | 0.575 (n=30) | 0.532 (n=150) | 0.088 | 0.195 |
| move_persists | 0.343 (n=30) | 0.348 (n=150) | 0.045 | 0.073 |
| underreact_up | 0.386 (n=30) | 0.373 (n=150) | 0.061 | 0.053 |
| underreact_down | 0.501 (n=30) | 0.478 (n=150) | 0.064 | 0.097 |
| p_up | 0.194 (n=30) | 0.183 (n=150) | 0.187 | 0.152 |
| fill_before_decay | 0.330 (n=30) | 0.354 (n=150) | 0.022 | 0.045 |
| fill_toxic | 0.366 (n=30) | 0.410 (n=150) | 0.041 | 0.064 |

## Per-condition answer means
| market | yes_pressure_5s | no_pressure_5s | move_persists | underreact_up | underreact_down | p_up | fill_before_decay | fill_toxic |
|---|---|---|---|---|---|---|---|---|
| btc-updown-15m-1777381200 | 0.288 | 0.552 | 0.313 | 0.363 | 0.501 | 0.035 | 0.321 | 0.343 |
| btc-updown-4h-1768035600 | 0.354 | 0.485 | 0.346 | 0.375 | 0.475 | 0.263 | 0.374 | 0.411 |
| btc-updown-4h-1772557200 | 0.205 | 0.571 | 0.338 | 0.354 | 0.516 | 0.173 | 0.359 | 0.417 |
| btc-updown-5m-1777380900 | 0.365 | 0.575 | 0.343 | 0.386 | 0.501 | 0.194 | 0.330 | 0.366 |
| eth-updown-4h-1766581200 | 0.401 | 0.500 | 0.381 | 0.396 | 0.430 | 0.274 | 0.376 | 0.437 |
| eth-updown-4h-1772557200 | 0.355 | 0.553 | 0.362 | 0.376 | 0.468 | 0.172 | 0.337 | 0.441 |

## Verdict vs pre-registered cutoffs
- std<0.05 (flat): 1/8 questions
- std>=0.10 (varies with state): 3/8 questions
- mean|MICRO_V2-CONTROL|>=0.05: 6/8 questions
- H1 (ignores state): supported iff flat>=6/8
- H2 (state yes, quant no): varied>=4/8 AND q_big==0
- H3 (uses both): varied>=4/8 AND >=2/8 with mean|d|>=0.10, sign consistent
- H4 (reacts, no markout): H2/H3 signals AND |corr(answer,drift_5s)|<0.15 all

## V2 verdict (pre-reg cutoffs)
- std(underreact_up) overall: 0.054 (SENSIBLE iff >= 0.10)
- std(underreact_up | CONTROL): 0.030 (n=60)
- std(underreact_up | MICRO_V2): 0.041 (n=60)
- std(underreact_up | MICRO_V2_QUANT): 0.056 (n=60)
- corr(underreact_up, drift_5s): n/a (n=180); PREDICTIVO iff |r|>=0.30, n>=50
- monotonic spread (max-min bucket mean drift): 0.000 (PREDICTIVO iff >= 1.0pp)
- RAMA 1 (maker PnL): sensible + predictivo. RAMA 2 (mueve, no predice): problema preguntas. RAMA 3 (ni mueve): cuestionar diseno de preguntas.
