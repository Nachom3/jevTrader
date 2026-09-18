# Estrategia V1: Jev Lead-Lag Maker (nucleo del alpha)

> Status: especificacion adoptada 2026-09-18. Codigo: `src/strategy/lead_lag.rs`.
> Tesis: no predecir el outcome. Detectar que la informacion externa ya implica
> un movimiento de probabilidad que Polymarket todavia no incorporo, y entrar
> maker (post-only) antes del repricing. Capturar repricing, no resolucion.

## Flujo

```text
Binance / Coinbase / Deribit (spot, orderbook, trades, perp)
           |
           v
       Rust Engine (features deterministicas)
           |
           v
          JEV (~100ms tipico, 1 request con 8 outputs)
           |
      LEADING SIGNAL
           |
           v
Polymarket book local --> ya repricio? no --> POST-ONLY ORDER
```

Rust detecta el evento (no polling ciego). Triggers: spot > 2 bps, OFI spike,
microprice shift, liquidation burst, divergencia cross-exchange, movimiento del
book Polymarket, distance-to-strike critica, volatility spike.

## State: Rust calcula, Jev juzga

Nunca mandar ticks crudos. Ejemplo para `BTC above $120,000 at 16:00?`:

- RESOLUTION: target 120000, time_remaining 23m14s, resolution_source.
- UNDERLYING: spot 119842, distance_to_target -0.1317%, returns 250ms/1s/5s/30s/5m,
  realized_vol_1m/5m, microprices Binance/Coinbase, perp price + basis.
- ORDER FLOW: buy/sell volume 1s, OFI_1s/OFI_5s, book_imbalance, aggressive_buy_ratio.
- CROSS: binance_coinbase_diff, spot_perp_diff.
- POLYMARKET: YES bid/ask (.43/.45), depth bid/ask, spread, recent trades,
  book imbalance, price 1s/5s/30s ago.
- CANDIDATE_ORDER: la orden maker propuesta (ej. BUY YES @ .44).

Tipos: `LeadLagFeatures` + `PolySnapshot` en `src/strategy/lead_lag.rs`.

## Los 8 outputs V1 (un solo request, en paralelo)

Familia impulso (Noul):
1. `yes_pressure_5s` — el estado externo implica suba de P(YES) en 5s?
2. `no_pressure_5s` — inversa (detectar conflicto).
3. `move_persists` — el movimiento persiste vs revierte?

Familia leading = el corazon (Noul):
4. `underreact_up` — Polymarket underreacciono a info que deberia subir P(YES)?
5. `underreact_down` — inversa.

Magnitud esperada (Choice, distribucion operable):
6. `repricing_ticks` — movimiento YES mas probable en 5s:
   UP_3_PLUS / UP_2 / UP_1 / FLAT / DOWN_1 / DOWN_2 / DOWN_3_PLUS.
   Ej: P(up >= 1 tick) = 74% sale directo de la distribucion.

Viabilidad maker (Noul, con la candidate order en el state):
7. `fill_before_decay` — se llena antes de que muera el alpha?
8. `fill_toxic` — el fill seria adverso (el mercado se mueve en contra)?

Regla de quote inicial (punto de partida, calibra con backtest):
`underreact_up > .75 && p_up_ge_1_tick > .65 && persists > .60 &&
fill_before_decay > .60 && fill_toxic < .30 && underreact_down < .30`
=> BUY YES un tick sobre el bid, POST-ONLY (ej. .44 entre .43/.45).

## Salida: seguir corriendo Jev, no esperar resolucion

Si despues `underreact_up .88 -> .42`, `next_up .74 -> .39`,
mean_reversion `.18 -> .69`, con YES comprado @ .44 y book .46/.48:
=> SELL .47 POST-ONLY. Profit +3c/share por repricing.

## Backtest: la variable objetivo es el markout

Guardar cada respuesta en QuestDB (`jev_signals` + `maker_markouts`).
Pregunta experimental:

> Cuando `underreact_up > .80 && next_up > .70 && toxic < .30`,
> que hizo Polymarket 1s / 5s / 10s / 30s despues?

Labels: markout de la orden maker a +1s/+5s/+30s (mid-based), NO solo si
gano YES/NO. Ahi sabemos si Jev es realmente leading. Si aparece edge,
recien ahi sofisticar con 20 preguntas mas (half-life, fair_shift en pp, etc.).

## Notas de diseno (verificadas vs docs TypeSafe)

- 8 preguntas en UN request: corren en paralelo, costo/latencia casi igual
  que una sola (docs: batching 13 preguntas = 10x mas rapido, sin cambio).
- Latencia tipica ~100ms: valida para edge de segundos en Polymarket 1h/4h,
  no para microsegundos. No llamar 100 veces/s sin motivo: event-driven.
- Noul no trae confidence: los gates sobre outputs 1-5/7-8 usan la
  probabilidad misma. Solo el Choice (6) trae `probabilities` + `confidence`.
- Composicion y thresholds en codigo (`should_quote`), nunca en prompts.
