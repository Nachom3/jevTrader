# Feature: lead-lag-v1 (Jev como nucleo del alpha)

Goal: estrategia Lead-Lag Maker como V1. Tesis: detectar informacion externa
ya implicita en probabilidad pero no incorporada por Polymarket; entrar maker
post-only antes del repricing. Capturar repricing, no resolucion.

## Tasks

- [x] Validar diseno vs docs TypeSafe (paralelo, latencia, Noul sin confidence)
- [x] Spec: `docs/strategy-lead-lag-v1.md`
- [x] Codigo: `src/strategy/lead_lag.rs` (features, 8 preguntas, V1Signal, should_quote)
- [x] QuestDB: tabla `maker_markouts` (labels a +1s/+5s/+30s)
- [x] AGENTS.md: seccion 15 + markouts en 9.2
- [x] Siguiente: cliente Jev (`src/jev/`) + actor de ingesta WS (cerrado 2026-09-19 como stale: ambos existen — `client.rs` + `ws.rs`)

## Decisions (2026-09-18)

- 8 outputs V1 en UN solo request (paralelo, sin costo extra de latencia).
- Thresholds iniciales en codigo (UNDER .75, NEXT_UP .65, PERSIST .60,
  FILL .60, TOXIC .30, CONFLICT .30); calibrar con backtest, no con prompts.
- Variable objetivo principal: markout maker a +1s/+5s/+30s, no outcome final.

## Evidence

- TypeSafe how-to-build (preguntas atomicas en paralelo), api.md (Choice con
  distribucion + confidence; Noul 0-1 sin confidence), confidence.md
  (thresholds por riesgo), cookbook parallel_questions (batch 13q = 10x).
