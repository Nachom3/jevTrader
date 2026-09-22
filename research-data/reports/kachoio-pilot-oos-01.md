# OOS report: kachoio-pilot-01 (5m up/down YES-side tape)

> Pre-registered SIGNAL-research run on real YES-side books. Frozen V1
> (thresholds, wording, fills unchanged — protected paths verified clean
> before/after). Paper/offline only. No certainty claims.

## Pre-registration

- run_id: `kachoio-pilot-01`
- Corpus: `research-data/processed/kachoio_polytop.parquet` (115 BTC/ETH 5m
  markets, 2026-04-28, 34,246 per-second YES top-of-book rows, kachoio-v1)
- Budget: max 200 live calls (spent 17; 183 remain). Boundary (exclusive):
  2026-04-29T00:00:00Z. Fidelity: EXACT. Splits preserved.
- Gate (frozen): `underreact_up>.75 && p_up>=1tick>.65 && persists>.60 &&
  fill>.60 && toxic<.30 && conflict<.30 && no_pressure<.30`.

## Results (67 live evaluations, 0 API errors)

| Signal | Min | Mean | Max | Frozen gate |
|---|---:|---:|---:|---|
| underreact_up | 0.33 | 0.40 | 0.49 | >0.75: 0/67 |
| underreact_down (conflict) | 0.38 | 0.49 | 0.61 | <0.30: 0/67 |
| move_persists | 0.33 | 0.41 | 0.58 | >0.60: 0/67 |
| fill_before_decay | 0.15 | 0.34 | 0.47 | >0.60: 0/67 |
| fill_toxic | 0.30 | 0.42 | 0.53 | <0.30: 0/67 |
| no_pressure_5s | 0.33 | 0.57 | 0.78 | <0.30: 0/67 |
| p_up_ge_1_tick | 0.06 | 0.29 | 0.84 | >0.65: 2/67 |
| yes_pressure_5s | 0.19 | 0.44 | 0.73 | n/a |

Conjunction (all frozen gates): **0 / 67 quotes**.

Splits: EXPLORATION 36, VALIDATION 4, OUT_OF_SAMPLE 27 (OOS max
underreact_up 0.49). Latency 289/548/1271ms. Tokens 120,923 in / 15,580 out.

## Reading (not tuning)

Jev DOES see directional moves (yes_pressure max 0.73, p_up max 0.84) but
does not judge Polymarket as underreacting (underreact_up max 0.49) and
rates these fills as toxic/conflicted at every threshold. Every gate
component binds, not just one. Consistent with campaign-btc-eth-oos-01
(544 live judgments on 15m/1h/4h tape, underreact_up max 0.50, 0 quotes):
two independent samples, same ceiling.

## Conclusion (evidence AGAINST quoting at frozen V1, not alpha proof)

Frozen Lead-Lag V1 does not fire on crypto up/down tape with real books:
0/67 here, 0/544 before. Zero-fill/zero-quote is the observed execution
result. Do NOT lower thresholds on this sample.

NOT proven: other horizons with books (15m/1h/4h untested), other question
designs, trigger selection, or execution PnL. Next decisions belong to
strategy research, not re-measurement.

Artifacts: research-data/cache/kachoio-pilot-01/ (evaluations.parquet,
manifest, jev_cache.json, README run log).
