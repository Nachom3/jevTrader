# Feature: questdb-schema (per-market data contract)

Goal: cerrar el contrato de datos por mercado (AGENTS.md seccion 9) en dos
artefactos versionables: DDL QuestDB + structs Rust definitivos.

## Tasks

- [x] Revisar arquitectura vs docs vivas (TypeSafe + Polymarket + QuestDB CREATE TABLE)
- [x] DDL QuestDB: `questdb/schema.sql` (9 tablas, PARTITION BY DAY / MONTH)
- [x] Structs Rust: `src/state/market_state.rs` (MarketState + JevSignal + PaperDecision)
- [ ] Siguiente feature: `Cargo.toml` + crate scaffold segun layout AGENTS.md seccion 8

## Decisions (2026-09-18)

- QuestDB primero para todo lo temporal; Postgres solo relacional no-temporal.
- `jev_signals` guarda state JSON exacto + state_hash por cada llamada (backtesting).
- `resolutions` es la tabla de labels (winning_outcome incluye FIFTY para UMA 50/50).
- `book_snapshots` solo en trigger/muestreo, nunca por tick.
- Noticias siempre en English (`text_en`) porque Jev es English-first.

## Evidence

- TypeSafe: state/api/noul/confidence/how-to-build/composite-scoring (docs.typesafe.ai)
- Polymarket: markets-events/market-details/prices-order-books/realtime-data/resolution
- QuestDB: CREATE TABLE (TIMESTAMP designated, PARTITION BY, SYMBOL CAPACITY, DEDUP)
- SDK Rust: github.com/Polymarket/rs-clob-client-v2
