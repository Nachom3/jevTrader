# Jev diagnostic run: jev-diagnostic-v1 (pre-registered, single run)

## Pre-registration (odd/tasks/jev-diagnostic-v1.md)
- Objetivo: ¿Jev reacciona al estado? ¿QUANT cambia sus juicios?
- 6 condiciones EXACT fijas por criterios exogenos (asset x horizon x semana)
  + sonda +3.0 pre-declarada (lista en el feature doc antes del run).
- 10 estados stratified evenly-spaced por condicion (--stride 1), 2 variantes
  = ~120 calls, budget 150. Thresholds/questions/features CONGELADOS.
- Cutoffs de evidencia: H1 flat>=6/8 (std<0.05); H2 varied>=4/8 (std>=0.10)
  y q_big==0; H3 varied>=4/8 y >=2/8 con mean|d|>=0.10; H4 |corr|<0.15.
- Comando: --max-pairs 60 --exact-only 1 --per-condition-cap 10 --stride 1
  --conditions <6 CIDs> --latency EMPIRICAL
  --latency-samples jev_latency_signal_alpha_v1.json
  --real-jev 1 --max-jev-calls 150
  --signals-out jev_diagnostic_v1_signals.json
  --latency-out jev_latency_diag_v1.json
  --out jev_diagnostic_v1.json --run-id jev-diagnostic-v1
- Desviacion: primer intento con CID#1 mal transcripto (5/6 en tape),
  abortado tras ~88 calls sin escribir artefactos; rerun con 6 verificados.

## Outcome
- 120 rows = 60 pairs, 108 live calls (+12 cache hits), 0 stale,
  0 incomplete, 6/6 condiciones. Latencias reales guardadas (diag file).
- Drift +5s en esta muestra: 0.000 en TODOS los buckets (sin varianza
  para correlacionar; H4 no evaluable aqui, ver veredicto combinado abajo).
- Veredicto por pregunta (detalle: jev_diagnostic_v1_analysis.md):
  H1 RECHAZADA (pressure std 0.16-0.20, corr ±0.3 con returns, sonda difiere).
  H2 la mas cercana pero bajo cutoff (3/8 variadas, necesitaba 4/8);
  decision questions casi planas (underreact_up std 0.037, rango 0.23-0.42).
  H3 RECHAZADA (max mean|QUANT-CONTROL| = 0.045, P>=0.10 ~ 0).
  H4 no evaluable en esta muestra (drift sin varianza); v1 ya mostro
  drift ~0 con senales en banda angosta.
- Hallazgo clave: underreact_up vive en 0.23-0.42 en 120 estados diversos:
  el threshold .75 esta fuera de rango Y la respuesta es plana, asi que
  bajarlo fabricaria quotes sin alpha (v1: drift~0 condicional a nada).
- Caveat de diseno: ofi_5s y distance_to_target tienen varianza CERO en
  replay (flow hardcodeado 0.0, distance degenerado) -> el diagnostico no
  puede hablar de esas features. El hot state de replay esta empobrecido:
  exactamente el argumento para microestructura real en V2.
- ret_5s y ret_1m dan correlaciones identicas (ventanas redundantes en
  replay disperso); anotado, no interpretado.
