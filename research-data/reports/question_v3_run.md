# question-v3 run (PRE-REG en odd/tasks/question-v3.md)

## Pre-reg recap
- 6 mercados + snapshots stratified del diagnostico, state MICRO_V2
  identico en los 3 brazos (aisla forma-de-pregunta).
- Brazos: CONTROL (8 V1, baseline) / FAIR_VALUE (1 Noul fair_p_yes) /
  PRESSURE_COMPOSITE (1 Choice-5 -> E en [-1,1]).
- 8 preguntas V1 + thresholds congelados. Sin features nuevas. Sin PnL primario.
- N: 60 pares x 3 = 180 evals. Budget 250.
- Cutoffs: sensibilidad std>=0.10; monotonicidad signos esperados;
  incremental (brazos difieren); prediccion corr>=0.30 n>=50 o spread>=1.0pp.
- Wordings aprobados por usuario antes del run.
- Comando: --max-pairs 60 --exact-only 1 --per-condition-cap 10
  --conditions <6 CIDs> --stride 1 --arms v3 --latency EMPIRICAL
  --latency-samples jev_latency_signal_alpha_v1.json
  --real-jev 1 --max-jev-calls 250
  --signals-out question_v3_signals.json
  --out question_v3.json --run-id question-v3

## Metodo: mismo artifact de labels que V2 (ver signal_alpha_v2_run.md)
- Filas congeladas intactas; drift denso via question_v3_drift_fix.json
  (fix_strided_drift.py, algoritmo exacto con latencias LIVE).

## Outcome (174 live, 0 errores, 60 pares x 3, 0 quotes)
- SENSIBLE: pressure std 0.412 rango [-0.96,+0.91] (SI, 4x cutoff).
  fair std 0.078 rango [0.29,0.64] (NO por cutoff; 2.1x underreact_up 0.037,
  centrado en 0.49 = prior sensato para up/down).
- Monotonicidad interna: fair<->distance +0.42, <->ret_5s +0.36,
  <->basis -0.31; pressure<->distance +0.29, <->ret +0.19, <->ofi +0.14/+0.12.
  Signos coherentes: el estado entra a las preguntas nuevas.
- Incremental: fair [0.29,0.64] y pressure [-0.96,0.91] vs banda
  underreact 0.23-0.42. Brazos difieren materialmente.
- PREDICCION: corr(fair,drift_5s)=+0.06, corr(pressure,drift_5s)=-0.02
  (n=60). Buckets fair: +0.86/0.00/-0.08/+1.71/+0.44 (no monotono;
  bucket4 dominado por 1-2 prints: maxabs drift 15-22pp, fragil).
  Buckets usuario pressure: +0.80/+0.81/+0.33/0.00/+1.33 (n=3 arriba).
  edge=fair-mid media -0.067, corr(edge,drift)=+0.02.
- Drift gordo de cola (15-22pp): medias por bucket fragiles; hit rates
  darian robustez pero el corr ~0 ya decide.

## Veredicto (arbol pre-registrado)
- RAMA 1 (quote engine): NO (sensible pressure SI, predictivo NO).
- RAMA 2 (mueve, no predice): EVIDENCIA A FAVOR con matiz — pressure
  MUEVE (std .41, rango total, corrs coherentes) pero su magnitud NO
  ORDENA retornos futuros (corr -.02, buckets revueltos). Fair se mueve
  poco (std .078) y tampoco predice (+.06).
- RAMA 3 (ni mueve): NO para pressure (se mueve mucho). Pero la pregunta
  fundamental ("¿magnitud que ordene retornos?") responde NO en muestra.
- Conclusion: Jev produce variables continuas sensibles al estado/
  regimen (pressure trackea vol/distance/ret), pero su salida direccional
  de 5s no contiene markout aprovechable aqui. Recomendacion: Jev a
  regimen/contexto; micro-alpha direccional a modelo determinista.
  Nada de execution, nada de thresholds, nada mas de features.
