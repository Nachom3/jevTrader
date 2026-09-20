# Jev diagnostic report

## Per-question distribution and variance
| question | n | mean | std | min | max | std_within_cond | std_between_cond |
|---|---|---|---|---|---|---|---|
| yes_pressure_5s | 120 | 0.279 | 0.158 | 0.130 | 0.650 | 0.113 | 0.085 |
| no_pressure_5s | 120 | 0.403 | 0.200 | 0.130 | 0.700 | 0.174 | 0.095 |
| move_persists | 120 | 0.280 | 0.053 | 0.190 | 0.410 | 0.043 | 0.028 |
| underreact_up | 120 | 0.313 | 0.037 | 0.230 | 0.420 | 0.030 | 0.022 |
| underreact_down | 120 | 0.394 | 0.062 | 0.250 | 0.560 | 0.057 | 0.025 |
| p_up | 120 | 0.173 | 0.148 | 0.010 | 0.500 | 0.119 | 0.072 |
| fill_before_decay | 120 | 0.346 | 0.035 | 0.260 | 0.420 | 0.026 | 0.022 |
| fill_toxic | 120 | 0.388 | 0.061 | 0.280 | 0.530 | 0.046 | 0.037 |

## Pearson(answer, feature) — fixed feature list
| question | ret_5s_pct | ret_1m_pct | realized_vol_1m_pct | distance_to_target_pct | time_remaining_secs | move_zscore_1s | ofi_5s | spread |
|---|---|---|---|---|---|---|---|---|
| yes_pressure_5s | +0.30 | +0.30 | -0.05 | n/a | -0.17 | +0.30 | n/a | -0.06 |
| no_pressure_5s | -0.37 | -0.37 | +0.22 | n/a | -0.21 | -0.32 | n/a | +0.08 |
| move_persists | +0.16 | +0.16 | +0.01 | n/a | -0.17 | +0.22 | n/a | -0.04 |
| underreact_up | +0.03 | +0.03 | +0.16 | n/a | -0.33 | +0.16 | n/a | -0.24 |
| underreact_down | -0.16 | -0.16 | +0.03 | n/a | -0.27 | -0.22 | n/a | -0.04 |
| p_up | -0.11 | -0.11 | +0.20 | n/a | -0.48 | -0.02 | n/a | -0.08 |
| fill_before_decay | +0.05 | +0.05 | +0.05 | n/a | -0.20 | +0.11 | n/a | +0.04 |
| fill_toxic | -0.04 | -0.04 | +0.08 | n/a | -0.42 | -0.02 | n/a | -0.43 |

## Monotonicity: mean drift_5s by answer bucket
- yes_pressure_5s: n=39 m=+0.000 | n=25 m=+0.000 | n=9 m=+0.000 | n=26 m=+0.000 | n=21 m=+0.000
- no_pressure_5s: n=25 m=+0.000 | n=25 m=+0.000 | n=27 m=+0.000 | n=20 m=+0.000 | n=23 m=+0.000
- move_persists: n=27 m=+0.000 | n=26 m=+0.000 | n=29 m=+0.000 | n=17 m=+0.000 | n=21 m=+0.000
- underreact_up: n=25 m=+0.000 | n=27 m=+0.000 | n=24 m=+0.000 | n=26 m=+0.000 | n=18 m=+0.000
- underreact_down: n=36 m=+0.000 | n=21 m=+0.000 | n=16 m=+0.000 | n=26 m=+0.000 | n=21 m=+0.000
- p_up: n=33 m=+0.000 | n=17 m=+0.000 | n=23 m=+0.000 | n=24 m=+0.000 | n=23 m=+0.000
- fill_before_decay: n=35 m=+0.000 | n=27 m=+0.000 | n=19 m=+0.000 | n=19 m=+0.000 | n=20 m=+0.000
- fill_toxic: n=27 m=+0.000 | n=26 m=+0.000 | n=20 m=+0.000 | n=28 m=+0.000 | n=19 m=+0.000

## QUANT-CONTROL paired deltas per question
| question | n_pairs | mean_abs_delta | P(abs>=0.10) | sign_agreement |
|---|---|---|---|---|
| yes_pressure_5s | 60 | 0.025 | 0.02 | 1.00 |
| no_pressure_5s | 60 | 0.044 | 0.08 | 1.00 |
| move_persists | 60 | 0.045 | 0.00 | 1.00 |
| underreact_up | 60 | 0.029 | 0.00 | 1.00 |
| underreact_down | 60 | 0.024 | 0.00 | 0.71 |
| p_up | 60 | 0.026 | 0.00 | 0.86 |
| fill_before_decay | 60 | 0.017 | 0.00 | n/a |
| fill_toxic | 60 | 0.017 | 0.00 | 1.00 |

## Sensitivity: probe market vs rest (mean answer)
| question | probe_mean | rest_mean | probe_std | rest_std |
|---|---|---|---|---|
| yes_pressure_5s | 0.183 (n=20) | 0.298 (n=100) | 0.019 | 0.167 |
| no_pressure_5s | 0.564 (n=20) | 0.371 (n=100) | 0.141 | 0.194 |
| move_persists | 0.252 (n=20) | 0.285 (n=100) | 0.029 | 0.055 |
| underreact_up | 0.282 (n=20) | 0.319 (n=100) | 0.019 | 0.037 |
| underreact_down | 0.441 (n=20) | 0.384 (n=100) | 0.046 | 0.061 |
| p_up | 0.175 (n=20) | 0.173 (n=100) | 0.193 | 0.138 |
| fill_before_decay | 0.324 (n=20) | 0.351 (n=100) | 0.017 | 0.036 |
| fill_toxic | 0.350 (n=20) | 0.395 (n=100) | 0.040 | 0.061 |

## Per-condition answer means
| market | yes_pressure_5s | no_pressure_5s | move_persists | underreact_up | underreact_down | p_up | fill_before_decay | fill_toxic |
|---|---|---|---|---|---|---|---|---|
| btc-updown-15m-1777381200 | 0.259 | 0.436 | 0.271 | 0.290 | 0.397 | 0.024 | 0.319 | 0.331 |
| btc-updown-4h-1768035600 | 0.309 | 0.404 | 0.297 | 0.325 | 0.396 | 0.247 | 0.373 | 0.408 |
| btc-updown-4h-1772557200 | 0.167 | 0.242 | 0.238 | 0.305 | 0.357 | 0.189 | 0.365 | 0.386 |
| btc-updown-5m-1777380900 | 0.183 | 0.564 | 0.252 | 0.282 | 0.441 | 0.175 | 0.324 | 0.350 |
| eth-updown-4h-1766581200 | 0.389 | 0.367 | 0.308 | 0.336 | 0.383 | 0.228 | 0.364 | 0.422 |
| eth-updown-4h-1772557200 | 0.367 | 0.404 | 0.312 | 0.338 | 0.390 | 0.177 | 0.334 | 0.431 |

## Verdict vs pre-registered cutoffs
- std<0.05 (flat): 2/8 questions
- std>=0.10 (varies with state): 3/8 questions
- mean|QUANT-CONTROL|>=0.05: 0/8 questions
- H1 (ignores state): supported iff flat>=6/8
- H2 (state yes, quant no): varied>=4/8 AND q_big==0
- H3 (uses both): varied>=4/8 AND >=2/8 with mean|d|>=0.10, sign consistent
- H4 (reacts, no markout): H2/H3 signals AND |corr(answer,drift_5s)|<0.15 all
