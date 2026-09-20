# signal-alpha-v2 run (PRE-REGISTRO en odd/tasks/signal-alpha-v2.md)

## Pre-reg recap (congelado antes del run)
- 6 condiciones del diagnostico, 10 estados stratified (--stride 1),
  3 brazos (CONTROL=V1 frozen, MICRO_V2, MICRO_V2_QUANT), ~180 evals.
- 8 preguntas + thresholds (.75) CONGELADOS. Primary markout +5s.
- Cutoffs: SENSIBLE std(underreact_up)>=0.10; PREDICTIVO |corr|>=0.30 n>=50
  o monotonic spread>=1.0pp. Budget 250 calls.
- Comando: --max-pairs 60 --exact-only 1 --per-condition-cap 10
  --conditions <6 CIDs> --stride 1 --latency EMPIRICAL
  --latency-samples jev_latency_signal_alpha_v1.json
  --real-jev 1 --max-jev-calls 250
  --signals-out signal_alpha_v2_signals.json
  --latency-out jev_latency_v2.json
  --out signal_alpha_v2.json --run-id signal-alpha-v2

## Outcome (174 live calls, 0 errores, 180 rows = 60 pares x 3)
- Quotes: 0 en los 3 brazos. Drift +5s: 0.000 en TODOS los buckets
  (muestra sin varianza de drift: predictividad no testeable aqui).
- SENSIBLE: NO. std(underreact_up)=0.054 overall
  (CONTROL 0.030, MICRO_V2 0.041, MICRO_V2_QUANT 0.056) < 0.10.
- Pero MICRO SI mueve respuestas (vs quant=0 exacto en V1):
  paired MICRO_V2-CONTROL mean|d|: yes_pressure 0.147 (P>=.10: .65),
  no_pressure 0.236 (.63), underreact_down 0.101 (.45),
  underreact_up 0.074 (.33, sign agreement 1.00).
- underreact_down reacciona mas que up: std .093, corr distance -.64,
  ret -.43, basis +.52, vol +.37. underreact_up: corr poly_ofi -.34,
  ret_1m +.24, move_z +.20.
- distance_to_target fix VALIDADO como feature viva (correlaciones fuertes
  en down-side). OFI/basis son leidos por Jev (signos coherentes).
- Nota sampling: v1 (head) vio +3.0 en un mercado; diag+v2 (stratified)
  ven drift 0. El drift depende de DONDE en la trayectoria se evalua.

## Veredicto (arbol pre-registrado)
- RAMA 1 (maker PnL): NO (no sensible + no predictivo testeable).
- RAMA 2 (mueve, no predice): PARCIAL (mueve deltas, predecir no testeable).
- RAMA 3 (ni mueve underreact): la letra del cutoff (0.054<0.10) apunta aca.
- Conclusion precisa: el estado micro SI entra a Jev (deltas sistematicos,
  distance/OFI/basis con signo), pero underreact_up — el gate del quote —
  vive en 0.27-0.57 y no sale de ahi. El problema se aisla a las PREGUNTAS
  de decision, no al state builder ni a thresholds. Recomendacion: rama 3,
  rediseno de preguntas (p.ej. juicio continuo de fair-value o composite
  sobre pressure, que SI se mueve), antes de mas features o execution.
