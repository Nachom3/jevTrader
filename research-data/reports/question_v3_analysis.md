# V3 question-form report

- std(fair_p_yes): 0.078 (n=60; SENSIBLE iff >= 0.10)
  range [0.290, 0.640] mean 0.492
- std(pressure): 0.412 (n=60; SENSIBLE iff >= 0.10)
  range [-0.960, 0.905] mean -0.292

- edge=fair-mid: n=60 mean=-0.0673 corr(edge,drift_5s)=+0.02

## Pearson(metric, feature)
| metric | ofi_5s | poly_ofi_5s | perp_basis_pct | distance_to_target_pct | ret_5s_pct | realized_vol_1m_pct | move_zscore_1s |
|---|---|---|---|---|---|---|---|
| fair_p_yes | +0.22 | -0.22 | -0.31 | +0.42 | +0.36 | -0.21 | n/a |
| pressure | +0.14 | +0.12 | -0.16 | +0.29 | +0.19 | -0.07 | n/a |

## Incremental info vs CONTROL
- CONTROL rows: n=60 (underreact band from diagnostic: 0.23-0.42)
- FAIR_VALUE fair range above; PRESSURE range above (vs underreact band)

## Prediction: signal buckets vs future drift
- fair_p_yes x drift_1s: n=14 m=+0.000 | n=11 m=+0.000 | n=12 m=+0.000 | n=14 m=+0.000 | n=9 m=+0.000
- fair_p_yes x drift_5s: n=14 m=+0.857 | n=11 m=+0.000 | n=12 m=-0.083 | n=14 m=+1.714 | n=9 m=+0.444
- fair_p_yes x drift_30s: n=14 m=+2.857 | n=11 m=+1.636 | n=12 m=-0.333 | n=14 m=+1.764 | n=9 m=-0.556
- corr(fair_p_yes, drift_5s) = +0.06 (n=60)
- pressure x drift_1s: n=14 m=+0.000 | n=11 m=+0.000 | n=12 m=+0.000 | n=12 m=+0.000 | n=11 m=+0.000
- pressure x drift_5s: n=14 m=+0.857 | n=11 m=+0.455 | n=12 m=+1.000 | n=12 m=+0.500 | n=11 m=+0.364
- pressure x drift_30s: n=14 m=+3.571 | n=11 m=+0.182 | n=12 m=+1.167 | n=12 m=+0.417 | n=11 m=+0.245
- corr(pressure, drift_5s) = -0.02 (n=60)

## Pressure user buckets vs drift_5s
- [-1.0,-0.6): n=15 m=+0.800
- [-0.6,-0.2): n=21 m=+0.810
- [-0.2,+0.2]: n=18 m=+0.333
- (+0.2,+0.6]: n=3 m=+0.000
- (+0.6,+1.0]: n=3 m=+1.333

## Verdict (rama 1/2/3)
- RAMA 1 (quote engine): sensible + predictivo por cutoffs.
- RAMA 2 (mueve, no predice): problema preguntas/interpretacion.
- RAMA 3 (ni mueve): cuestionar Jev en loop direccional vs regimen/contexto.
