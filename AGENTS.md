# jevTrader - AGENTS.md

> Fuente de verdad operativa para agentes que trabajen en este repo.
> Stack: Rust como core concurrente + Jev (TypeSafe System One) como juicios tipados + Polymarket como venue.
> Storage: QuestDB para todo lo temporal (ticks, books, features, senales Jev) + Postgres solo para relacional no-temporal. Estado caliente en RAM.

## 1. Decision de stack: Rust donde aporta

Rust para ingesta en tiempo real, normalizacion y ejecucion. No por fanatismo, sino donde aporta:

- Muchos WebSockets + cientos de mercados + eventos simultaneos
- Concurrencia + baja latencia + estado en memoria
- Core robusto y predecible con Tokio

No necesitamos microsegundos: Jev tarda ~100ms por request (docs: "Most queries complete in about 100 ms"). El beneficio real es concurrencia y robustez.

### 1.1 Dependencias core Rust

tokio, reqwest, tokio-tungstenite, serde / serde_json, tracing, polymarket_client_sdk_v2 = "0.6".

SDK oficial Rust: https://github.com/Polymarket/rs-clob-client-v2
Features utiles: `clob`, `ws`, `rtds`, `data`, `gamma`, `tracing`.

### 1.2 Almacenamiento (decision 2026-09-18: QuestDB primero)

- QuestDB = primera eleccion para market data e historico. Encaja con ticks, trades, order books y features temporales, y seguimos usando SQL.
- Postgres SOLO para informacion relacional no time-series: configuracion, usuarios, estrategias, metadata de mercados.
- Redis NO inicialmente: estado caliente en memoria en Rust + persistencia en QuestDB. Si se escala a varios workers: ahi si Redis/NATS.
- En QuestDB se guarda no solo precios sino cada senal de Jev + el estado exacto que la origino (state_hash + state JSON). Eso permite comprobar si Jev encontro edge real o solo lo parecia (backtesting con labels de resolucion).

## 2. Arquitectura

```text
Polymarket WS / feeds externos
            |
        Rust + Tokio
            |
   estado caliente en RAM
       /             \
    Jev             QuestDB
 senales        historico temporal
                    |
               backtesting
```

Flujo de decision:

- Rust Market Engine recibe Prices + News + Other
- Construye MarketState
- Jev devuelve probabilities
- Strategy Engine calcula fair_value - ask
- SKIP o TRADE (paper primero)

## 3. MarketState y Feature Builder

Jev necesita informacion semanticamente util, no 400 numeros crudos.

Capa crucial: RAW DATA -> Feature / State Builder -> JEV STATE.

Struct interno (version revisada contra docs: incluye identificadores, status y constraints que el original omitia):

```rust
struct MarketState {
    // Identificadores (Gamma + CLOB)
    gamma_id: String,      // market.id (Gamma)
    slug: String,          // market.slug
    condition_id: String,  // 0x... (CLOB/CTF)
    event_id: String,      // evento padre (para grupos negRisk)
    yes_token_id: String,  // ERC1155 YES
    no_token_id: String,   // ERC1155 NO
    // Pregunta y resolucion (las rules definen como resuelve, no el titulo)
    question: String,
    resolution_source: String,
    resolution_rules: String,
    // Status (no operar sin chequear)
    active: bool,
    closed: bool,
    accepting_orders: bool,
    enable_order_book: bool,
    neg_risk: bool,
    // Microestructura YES (ejecutable)
    yes_bid: f64,          // best bid YES
    yes_ask: f64,          // best ask YES = precio de compra
    yes_mid: f64,
    yes_spread: f64,
    book_hash: String,     // hash del book para detectar cambios
    last_trade_price: f64,
    last_trade_side: String, // BUY | SELL
    // Agregados Gamma
    volume_24h: f64,
    liquidity: f64,
    one_day_price_change: f64,
    // Constraints (para ejecucion realista)
    tick_size: f64,
    min_order_size: f64,
    fees_enabled: bool,
    // Tiempo
    minutes_to_resolution: u64,
    // Contexto
    external_data: ExternalData,       // subyacente: spot, returns, vol
    recent_information: Vec<NewsItem>, // noticias clasificadas por mercado
}
```

Ejemplo crypto: Polymarket YES bid 0.41 / ask 0.43, volume 2341223, price 5m ago 0.39. Binance BTC 118431, +0.6% 5m, +2.1% 1h. Vol 1h 1.8%, IV 54%. Rust calcula mid 0.42, spread 0.02, momentum_5m +10.3%, distance_to_strike -1.31%, time_remaining 3h14m. Recien ahi se arma el state JSON con `market` (question, resolution, time_remaining_minutes), `polymarket` (yes_bid, yes_ask, spread, volume_24h) y `underlying` (btc_spot, distance_to_target_pct, return_5m_pct, return_1h_pct, volatility_1h_pct).

## 4. Jev (TypeSafe System One)

Jev tiene SDK oficial Python y JavaScript, pero su API es HTTP POST /v1/systemone, asi que desde Rust se llama con reqwest.

Request: POST https://api.typesafe.ai/v1/systemone con `model: "jev-latest"`, `state` y `questions` (`likely_yes` noul, `market_underpriced` noul, `resolution_risk` score con criteria Unambiguous / Minor ambiguity / Significant ambiguity / Highly ambiguous). Respuesta trae `answers` bajo los mismos IDs + `usage` (input/output tokens). Errores 429/529 => retry con backoff exponencial (los SDKs lo hacen solo; en reqwest implementarlo a mano).

Reglas TypeSafe (verificadas contra docs):

- State como objeto con campos nombrados; un request = un state + N questions que lo ven todo y se evaluan independientes en paralelo.
- Preguntas atomicas y angostas; IDs para codigo (el modelo no los ve), significado completo en instructions; referenciar anidados con paths `market.question`.
- Noul devuelve probabilidad 0-1 SIN confidence separada (0.5 = empate yes/no, no "intensidad media"). Choice/Score devuelven `probabilities` + `confidence` (concentracion de la distribucion).
- Composicion en codigo: reglas deterministas o sumas ponderadas (composite scoring); thresholds de confidence calibrados con datos propios, por riesgo de cada accion.
- Preguntas extra casi no cambian latencia: hacer fan-out especulativo en el mismo call (ej. `news_relevant`, `data_sufficient`) en vez de otro round-trip.
- State es solo texto; idioma primario English (noticias en otro idioma => resumir/traducir a English en el builder).

## 5. Ingesta event-driven (no cron)

No llamar a Jev con cada tick. Triggers: precio >2%, noticia relevante, spread significativo, spot >X, faltan X minutos, volumen anormal.

Market stream (WS `wss://ws-subscriptions-clob.polymarket.com/ws/market`): eventos `book`, `price_change`, `last_trade_price`, `tick_size_change`; con `custom_feature_enabled: true` sumar `best_bid_ask`, `new_market`, `market_resolved`. Suscribir por token IDs (YES+NO de cada mercado) y enviar `PING` cada 10s (heartbeat a nivel app, responde `PONG`). Comparar `hash` del book para saber si cambio entre lecturas.

Flujo: WS -> Rust actor -> MarketState en RAM -> si cambio importante -> build_state() -> Jev -> guardar senal + state en QuestDB.

Backfill: price history CLOB (`listPriceHistory`, interval/bucketSeconds o rango absoluto) para features temporales al arrancar.

## 6. Pipeline de noticias

RSS + APIs + X/fuentes oficiales + SEC/empresas/deportes -> Rust -> deduplicacion -> clasificacion por mercado -> MarketState -> Jev. Ejemplo SpaceX Flight 12 -> mercado "launch before October" -> state.recent_information con source, published_minutes_ago y text (en English para Jev).

## 7. Lo que NO hacemos de entrada

No meter Polymarket -> Vector DB -> RAG -> Jev. Es overengineering. Jev recibe estado directo.

## 8. Layout inicial

src/polymarket (rest, websocket), src/feeds (crypto, news, external), src/state (market_state, feature_builder), src/jev (client), src/strategy (edge, filters, risk), src/storage (questdb, postgres), src/execution (paper, polymarket).

## 9. Que guardamos por mercado (decision pendiente de detalle final)

### 9.1 Estatico Gamma (Postgres, tabla markets; refrescar por polling/evento new_market)

gamma_id, slug, condition_id, event_id, question, description, resolution_source + resolution_rules (texto completo, OBLIGATORIO en cada state Jev), outcomes + yes/no_token_id, active/closed/archived/accepting_orders/enable_order_book/neg_risk (+ augmented negRisk del evento), start/end dates, game_start_time (sports), seconds_delay, tick_size, min_order_size, fees (enabled, rate, exponent, taker_only, rebate), rewards (min_size, max_spread). Nota sports: ordenes limite se cancelan al empezar el partido; vigilar game_start_time.

### 9.2 Temporal QuestDB (designated timestamp; particion por dia)

- `trades`: ts, condition_id, token_id, side(YES/NO), price, size, fee_bps, tx_hash. Fuente: last_trade_price.
- `top_of_book`: ts, token_id, best_bid, best_ask, spread, mid. Fuente: best_bid_ask / price_change / poll.
- `book_snapshots`: ts, token_id, book_hash, bids_top5, asks_top5, imbalance, microprice. Solo en trigger o muestreo (no cada tick).
- `market_features`: ts, condition_id, mid, spread, momentum_5m/1h, volatility_1h, volume_24h, liquidity, one_day_change, minutes_to_resolution, distance_to_target (nullable).
- `external_ticks`: ts, symbol, price, ret_5m, ret_1h, vol_1h (BTC etc. via RTDS/Binance).
- `news_items`: ts, source, condition_id (nullable), text_en, published_minutes_ago, dedup_hash.
- `jev_signals` (CLAVE backtesting): ts, condition_id, state_hash, state_json, questions_json, likely_yes, underpriced, resolution_risk (+ confidences), latency_ms, tokens_in/out, trigger. Cada llamada Jev = una fila + state exacto.
- `paper_decisions`: ts, condition_id, jev_ts, edge, threshold, decision (SKIP/TRADE), paper_price, size, fair_value.
- `resolutions`: condition_id, winning_token_id, winning_outcome, resolved_ts. Label para medir edge real.
- `maker_markouts` (Lead-Lag V1): ts, condition_id, jev_ts, side, price, size, mid_1s/5s/30s, pnl_1s/5s/30s_pp. Labels de markout maker: la variable objetivo principal.

### 9.3 Postgres relacional (no temporal)

users, strategies (versiones + pesos + thresholds), configs, markets metadata (9.1), market_subscriptions (que mercados seguimos y por que).

## 10. Revision arquitectura vs docs (2026-09-18)

Verificado con citas contra docs vivas (TypeSafe state/api/noul/confidence/how-to-build/composite-scoring; Polymarket markets-events/market-details/prices-order-books/realtime-data/resolution):

1. Diagrama general VALIDO con 2 cambios: (a) QuestDB reemplaza a Postgres para temporal; (b) el state Jev debe incluir resolution_source + rules completas (docs: el titulo describe, las rules definen; UMA vota sobre rules + clarifications onchain).
2. MarketState original INCOMPLETO: faltaban identificadores (condition_id, token IDs, event_id), status (active/closed/accepting_orders/enable_order_book), constraints (tick_size, min_order_size, fees) y book_hash/last_trade_side. Sin eso no hay ejecucion ni trazabilidad. Corregido en seccion 3.
3. Request Jev original VALIDO pero incompleto: agregar `model` requerido, `usage` en respuesta, retry 429/529, criteria noul opcional {true,false}, score con array ordenado >=2 niveles. Preguntas propuestas (likely_yes, market_underpriced, resolution_risk) son atomicas y combinables en codigo: OK.
4. WS: correcto usar price/orderbook/trade, pero suscribirse con token IDs + custom_feature_enabled para lifecycle (new_market/market_resolved = labels gratis) y PING 10s. Comparar hash antes de persistir books completos.
5. Riesgos anotados: negRisk (mercados mutuamente excluyentes, solo uno resuelve YES: el state debe incluir contexto del grupo); Jev English-first (traducir noticias); confidence != permiso (thresholds por riesgo); tick 0.0025 solo en ciertos mercados de World Cup (leer siempre del mercado).

## 11. Paper trading primero

Solo paper trading al inicio. Objetivo: responder "cuando Jev detecta discrepancia del 10%, tiene valor predictivo real?" usando `jev_signals` + `resolutions` en QuestDB. Si es si, recien ahi ejecucion real.

## 12. Documentacion oficial

- TypeSafe intro: https://docs.typesafe.ai/introduction
- TypeSafe llms index: https://docs.typesafe.ai/llms.txt
- TypeSafe API: https://docs.typesafe.ai/api.md
- TypeSafe primitives: https://docs.typesafe.ai/primitives.md
- TypeSafe state: https://docs.typesafe.ai/concepts/state.md
- TypeSafe confidence: https://docs.typesafe.ai/confidence.md
- TypeSafe how-to-build: https://docs.typesafe.ai/concepts/how-to-build-with-system-one.md
- TypeSafe composite scoring: https://docs.typesafe.ai/patterns/composite-scoring.md
- Polymarket docs: https://docs.polymarket.com/
- Polymarket llms: https://docs.polymarket.com/llms.txt
- Polymarket markets-events: https://docs.polymarket.com/concepts/markets-events.md
- Polymarket market-details: https://docs.polymarket.com/market-data/market-details.md
- Polymarket prices-order-books: https://docs.polymarket.com/market-data/prices-order-books.md
- Polymarket realtime-data: https://docs.polymarket.com/market-data/realtime-data.md
- Polymarket resolution: https://docs.polymarket.com/concepts/resolution.md
- Polymarket Rust SDK: https://github.com/Polymarket/rs-clob-client-v2

Si hay conflicto entre este archivo y la doc viva, manda la doc oficial.

## 13. Skills instaladas

### typesafe-ai (instalada 2026-09-18)

- Origen: typesafe-ai/skills, skill typesafe-ai
- Comando: `npx -y skills add typesafe-ai/skills --skill typesafe-ai --agent pi`
- Ubicacion: `.pi/skills/typesafe-ai/SKILL.md`
- Alternativa Claude Code: `claude plugin marketplace add typesafe-ai/skills` + `claude plugin install typesafe@typesafe-ai`
- Raw: https://raw.githubusercontent.com/typesafe-ai/skills/main/skills/typesafe-ai/SKILL.md
- Uso: leer SKILL.md + docs de seccion 12 antes de integrar Jev.

## 14. Convenciones

- Rust edition 2024, `cargo fmt` + `cargo clippy -D warnings` antes de cada commit.
- `tracing` para logs, nunca `println!` en hot path.
- Secretos por env (POLYMARKET_PRIVATE_KEY, TYPESAFE_API_KEY), nunca hardcodear.
- Estado caliente en memoria; temporal en QuestDB, relacional en Postgres.
- Cada llamada a Jev persiste en `jev_signals`: market_id, state_hash + state JSON, questions, latencia, respuesta, usage.
- Pesos y thresholds en codigo, no en prompts.

## 15. Estrategia V1: Jev Lead-Lag Maker (nucleo del alpha)

Spec completa: `docs/strategy-lead-lag-v1.md`. Codigo: `src/strategy/lead_lag.rs`.
Tesis: detectar que la info externa ya implica un movimiento de probabilidad
que Polymarket no incorporo; entrar maker post-only antes del repricing.
Capturar repricing, no resolucion.

8 outputs en UN request (paralelo): `yes_pressure_5s`, `no_pressure_5s`,
`move_persists`, `underreact_up`, `underreact_down` (Noul) +
`repricing_ticks` (Choice UP_3+/UP_2/UP_1/FLAT/DOWN_1/DOWN_2/DOWN_3+) +
`fill_before_decay`, `fill_toxic` (Noul con candidate order en el state).
Regla inicial `should_quote`: under_up>.75, p_up>=1tick>.65, persist>.60,
fill>.60, toxic<.30, sin conflicto. Labels: markouts maker +1s/+5s/+30s.
