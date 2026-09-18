# Build plan

The repository is deliberately closing the foundation before any live venue
execution. The completed steps are the contracts needed for deterministic
replay, paper trading, and a later actor wiring pass.

## Completed foundation

0. **Crate and dependency scaffold** - `Cargo.toml`, `src/lib.rs`, and
   `src/main.rs` define the Rust 2024 crate and the application edge.
1. **Provider-free domain contracts** - `src/domain/` defines identifiers,
   `TradeSide`, `PriceTicks`, `TickSize`, and shared signal types.
2. **Polymarket book boundary** - `src/polymarket/book.rs` normalizes levels,
   tracks best prices, hashes top levels, and preserves stale state.
3. **Market venue adapters** - `src/polymarket/rest.rs` and
   `src/polymarket/ws.rs` keep provider messages at the adapter boundary.
4. **External feed normalization** - `src/feeds/` parses Binance, Coinbase,
   and Deribit events into provider-free feed types.
5. **State and features** - `src/state/market_state.rs`,
   `src/state/rolling.rs`, and `src/state/feature_builder.rs` build
   deterministic, replayable strategy inputs.
6. **Actor boundaries** - `src/engine/market_actor.rs` owns the book and
   `src/engine/signal_actor.rs` owns state sequencing, signal freshness, and
   the Jev async boundary.
7. **Jev contract and deadline** - `src/jev/request.rs`,
   `src/jev/response.rs`, and `src/jev/client.rs` validate V1 answers and use
   one bounded request with no retry.
8. **Paper execution and storage seams** - `src/execution/paper.rs` provides
   deterministic matching and markouts; `src/storage/questdb.rs` writes via a
   bounded off-hot-path queue.
9. **V1 strategy inputs and configuration** - `src/strategy/lead_lag.rs` and
   `src/config.rs` define the eight-answer V1 signal and calibrated quote
   thresholds.
10. **Foundation closure** - `src/strategy/quote.rs` enforces stale-book and
    maker-only quote rules; `src/strategy/risk.rs` adds typed risk blocks and a
    runtime kill switch; `src/main.rs` validates configuration through the
    Tokio startup path; `docs/architecture.md` records the invariants; and
    `.github/workflows/ci.yml` runs Rust formatting, Clippy, and tests beside
    gitleaks.

## What remains

- **Live actor wiring and execution:** connect venue/feed streams to the
  actors, schedule event-driven Jev evaluations, route intents to a live
  execution adapter, and add cancellation/reconciliation. No live order
  placement belongs in the foundation closure.
- **Postgres, if needed:** add a relational store only for non-temporal
  metadata such as users, strategies, configuration, or market subscriptions.
  Keep time-series data in QuestDB.
- **News:** add ingestion, deduplication, market classification, and
  English-first normalization before attaching news to `MarketState`.
- **Operational controls:** add production credentials/permissions, health
  reporting, restart/recovery behavior, and venue-specific rate-limit
  handling only when the live boundary is explicitly approved.

## Backtest-first order of work

1. Replay normalized external and Polymarket tapes through the pure rolling
   and feature builders.
2. Replay Jev fixtures through `SignalActor`, quote thresholds, `RiskGate`, and
   `decide_quote`; verify that stale books never produce intents.
3. Feed generated intents into `PaperBook` and measure fill rate, adverse
   selection, and +1s/+5s/+30s maker markouts.
4. Persist signal state, quote intent, and markout labels in QuestDB; calibrate
   thresholds from observed data rather than prompt changes.
5. Add news and any required relational metadata, then rerun the same replay
   and risk checks.
6. Only after paper evidence and operational controls are satisfactory, design
   a separately reviewed live execution adapter. Live wiring is intentionally
   out of scope for this closure.
