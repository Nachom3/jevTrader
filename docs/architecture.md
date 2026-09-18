# Architecture

`jevtrader` is a Rust market-data and paper-trading engine. The runtime is
actor-oriented, but live loops and live order placement are intentionally not
wired yet.

## Module map

- `src/main.rs` - Tokio startup/configuration validation edge. It exits
  non-zero when required configuration is missing or invalid.
- `src/config.rs` - environment loading and quote thresholds.
- `src/domain/` - provider-free identifiers, prices, sides, and signal types.
- `src/polymarket/` - venue REST/stream adapters and the normalized
  `OrderBook`.
- `src/feeds/` - external venue adapters for normalized ticks.
- `src/engine/` - market and signal actors. `MarketActor` is the authoritative
  owner of one market's local book.
- `src/state/` - rolling histories, deterministic feature building, and
  market-state assembly.
- `src/jev/` - the System One request/response boundary and single-flight
  evaluation client.
- `src/strategy/` - the V1 lead-lag signal rule, post-only quote construction,
  and runtime risk gate.
- `src/execution/` - deterministic paper-order matching only.
- `src/storage/` - the asynchronous QuestDB writer boundary.
- `src/telemetry/` - best-effort latency measurements.

## Hot-path data flow

```text
Polymarket stream + external feeds
                |
                v
       MarketActor owns the book
                |
       fresh MarketSnapshot only
                |
                v
       deterministic feature builder
                |
                v
       SignalActor -> Jev (one request)
                |
          deadline, no retry
                |
                v
       RiskGate -> should_quote -> decide_quote
                |
                v
       BUY post-only QuoteIntent -> paper execution
```

QuestDB receives copies through a bounded, non-blocking queue. It is an
observability and backtest sink, never a dependency of the quote decision.
The current startup path validates configuration and exits; it does not start
feed, Jev, storage, or execution loops.

## Ownership and boundary rules

1. `MarketActor` is the sole owner of the authoritative in-memory order book.
   Consumers receive a cloned `MarketSnapshot` with its freshness state and
   cannot make a stale book authoritative.
2. A book snapshot is the only operation that clears the book's stale state.
   Sequence gaps and book-hash mismatches mark it stale; deltas do not repair
   that state.
3. Rust owns normalized prices, features, quote thresholds, risk checks, and
   the final deterministic quote decision. Jev judges the supplied state; it
   does not place or size orders.
4. V1 quote sizing is supplied by the caller. The strategy does not invent a
   sizing model.
5. Storage work is off the hot path. A full QuestDB queue is surfaced as lag
   telemetry rather than making a quote wait on I/O.
6. Paper execution is the only execution boundary currently available. No
   module in this closure places a live order.

## Safety invariants

- **No trading while stale:** a stale `MarketSnapshot` fails both the strategy
  quote decision and the runtime `RiskGate`. The quote path returns `None`.
- **Maker-only:** a BUY candidate is one tick above the best bid, rounded up to
  a valid venue tick multiple, and is accepted only when it is strictly below
  the best ask. A crossed or empty book produces no intent.
- **Risk limits are explicit:** killed gates, stale books, the outstanding
  quote limit, and Jev latency above the configured maximum each block a quote
  with a typed reason.
- **Jev deadline/no retry:** the complete Jev request has a deadline. A timeout
  or late signal is discarded; it is never retried into a newer market state.
- **Paper first:** markouts and replay results must be measured before any
  future live-execution work is considered.
