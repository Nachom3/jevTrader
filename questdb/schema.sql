-- jevTrader QuestDB schema v1
-- Source of truth for time-series storage. Decisions: AGENTS.md sections 1.2, 9.
-- Docs: https://questdb.com/docs/reference/sql/create-table/
--
-- Conventions:
--   * Every event table has a designated TIMESTAMP(ts) PARTITION BY DAY (WAL default).
--   * IDs meant for WHERE filtering are SYMBOL NOCACHE (high cardinality: markets, tokens).
--   * Low-cardinality enums (outcome, decision, trigger) are cached SYMBOLs.
--   * Full JSON payloads (state, questions, book depth) go in STRING columns.
--   * Relational / static metadata (Gamma snapshot, strategies, users) lives in Postgres, NOT here.
--   * `resolutions` is the label table for backtesting jev_signals.

CREATE TABLE IF NOT EXISTS trades (
  ts TIMESTAMP,
  condition_id SYMBOL CAPACITY 4096 NOCACHE,
  token_id SYMBOL CAPACITY 8192 NOCACHE,
  outcome SYMBOL CAPACITY 4,
  price DOUBLE,
  size DOUBLE,
  fee_bps DOUBLE,
  tx_hash STRING
) TIMESTAMP(ts) PARTITION BY DAY;

-- outcome: YES | NO (side of the traded token)

CREATE TABLE IF NOT EXISTS top_of_book (
  ts TIMESTAMP,
  token_id SYMBOL CAPACITY 8192 NOCACHE,
  best_bid DOUBLE,
  best_ask DOUBLE,
  spread DOUBLE,
  mid DOUBLE
) TIMESTAMP(ts) PARTITION BY DAY;

CREATE TABLE IF NOT EXISTS book_snapshots (
  ts TIMESTAMP,
  token_id SYMBOL CAPACITY 8192 NOCACHE,
  book_hash STRING,
  bids_json STRING,
  asks_json STRING,
  imbalance DOUBLE,
  microprice DOUBLE
) TIMESTAMP(ts) PARTITION BY DAY;

-- bids_json / asks_json: top-5 levels as [{"price":..,"size":..}].
-- Write only on trigger or sampling, NOT on every tick.
-- imbalance: (bidVol - askVol) / (bidVol + askVol) over stored levels.

CREATE TABLE IF NOT EXISTS market_features (
  ts TIMESTAMP,
  condition_id SYMBOL CAPACITY 4096 NOCACHE,
  mid DOUBLE,
  spread DOUBLE,
  momentum_5m DOUBLE,
  momentum_1h DOUBLE,
  volatility_1h DOUBLE,
  volume_24h DOUBLE,
  liquidity DOUBLE,
  one_day_change DOUBLE,
  minutes_to_resolution LONG,
  distance_to_target DOUBLE
) TIMESTAMP(ts) PARTITION BY DAY;

-- distance_to_target: NULL for non-price markets (e.g. news / elections).

CREATE TABLE IF NOT EXISTS external_ticks (
  ts TIMESTAMP,
  symbol SYMBOL CAPACITY 256,
  price DOUBLE,
  ret_5m DOUBLE,
  ret_1h DOUBLE,
  vol_1h DOUBLE
) TIMESTAMP(ts) PARTITION BY DAY;

CREATE TABLE IF NOT EXISTS news_items (
  ts TIMESTAMP,
  source SYMBOL CAPACITY 256,
  condition_id SYMBOL CAPACITY 4096 NOCACHE,
  text_en STRING,
  published_minutes_ago LONG,
  dedup_hash STRING
) TIMESTAMP(ts) PARTITION BY DAY;

-- text_en: always English (Jev is English-first). Translate/summarize in the builder.
-- condition_id NULLABLE in practice: NULL means "unclassified / market-agnostic".

CREATE TABLE IF NOT EXISTS jev_signals (
  ts TIMESTAMP,
  condition_id SYMBOL CAPACITY 4096 NOCACHE,
  state_hash STRING,
  state_json STRING,
  questions_json STRING,
  likely_yes DOUBLE,
  underpriced DOUBLE,
  resolution_risk DOUBLE,
  resolution_risk_conf DOUBLE,
  latency_ms LONG,
  tokens_in LONG,
  tokens_out LONG,
  trigger SYMBOL CAPACITY 64
) TIMESTAMP(ts) PARTITION BY DAY;

-- One row per Jev call, with the EXACT state that produced it (backtesting).
-- Noul answers (likely_yes, underpriced) carry no confidence; only the Score does.

CREATE TABLE IF NOT EXISTS paper_decisions (
  ts TIMESTAMP,
  condition_id SYMBOL CAPACITY 4096 NOCACHE,
  jev_ts TIMESTAMP,
  edge DOUBLE,
  threshold DOUBLE,
  decision SYMBOL CAPACITY 8,
  paper_price DOUBLE,
  size DOUBLE,
  fair_value DOUBLE
) TIMESTAMP(ts) PARTITION BY DAY;

-- decision: SKIP | TRADE. jev_ts links back to jev_signals.ts.

CREATE TABLE IF NOT EXISTS resolutions (
  condition_id SYMBOL CAPACITY 4096 NOCACHE,
  winning_token_id SYMBOL CAPACITY 8192 NOCACHE,
  winning_outcome SYMBOL CAPACITY 8,
  resolved_ts TIMESTAMP
) TIMESTAMP(resolved_ts) PARTITION BY MONTH;

-- winning_outcome: YES | NO | FIFTY (UMA 50/50). Label for measuring real edge:
--   SELECT s.*, r.winning_outcome FROM jev_signals s
--   LEFT JOIN resolutions r ON s.condition_id = r.condition_id

CREATE TABLE IF NOT EXISTS maker_markouts (
  ts TIMESTAMP,
  condition_id SYMBOL CAPACITY 4096 NOCACHE,
  jev_ts TIMESTAMP,
  side SYMBOL CAPACITY 8,
  price DOUBLE,
  size DOUBLE,
  mid_1s DOUBLE,
  mid_5s DOUBLE,
  mid_30s DOUBLE,
  pnl_1s_pp DOUBLE,
  pnl_5s_pp DOUBLE,
  pnl_30s_pp DOUBLE
) TIMESTAMP(ts) PARTITION BY DAY;
