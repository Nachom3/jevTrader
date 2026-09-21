-- jevTrader QuestDB schema v2
-- Source of truth for time-series storage. Decisions: AGENTS.md sections 1.2, 9.
-- Docs: https://questdb.com/docs/reference/sql/create-table/
--
-- Conventions:
--   * Every event table has a designated TIMESTAMP(ts) PARTITION BY DAY (WAL default).
--   * IDs meant for WHERE filtering are SYMBOL NOCACHE (high cardinality: markets, tokens).
--   * Low-cardinality enums (outcome, decision, trigger, variant) are cached SYMBOLs.
--   * Full JSON payloads (state, questions, book depth) go in STRING columns.
--   * Relational / static metadata (Gamma snapshot, strategies, users) lives outside
--     QuestDB. V1 has no Postgres: there is no relational store in scope.
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

-- V1 shape note: these three tables replace the pre-V1 storage contract.
-- `CREATE TABLE IF NOT EXISTS` never alters existing QuestDB tables. An
-- existing database with the old `likely_yes`/`underpriced`/`resolution_risk`
-- columns, old markout horizons, or no `variant` column needs an
-- operator-managed migration before using this schema; see
-- `questdb/migration-v1-to-v2.md` for the v2 columns below.
CREATE TABLE IF NOT EXISTS jev_signals (
  ts TIMESTAMP,
  condition_id SYMBOL CAPACITY 4096 NOCACHE,
  state_seq LONG,
  state_hash STRING,
  state_json STRING,
  questions_json STRING,
  yes_pressure_5s DOUBLE,
  no_pressure_5s DOUBLE,
  move_persists DOUBLE,
  underreact_up DOUBLE,
  underreact_down DOUBLE,
  repricing_up_3_plus DOUBLE,
  repricing_up_2 DOUBLE,
  repricing_up_1 DOUBLE,
  repricing_flat DOUBLE,
  repricing_down_1 DOUBLE,
  repricing_down_2 DOUBLE,
  repricing_down_3_plus DOUBLE,
  repricing_confidence DOUBLE,
  fill_before_decay DOUBLE,
  fill_toxic DOUBLE,
  latency_ms LONG,
  -- Retained for schema compatibility; both are 0 until Jev usage is measured.
  tokens_in LONG,
  tokens_out LONG,
  trigger SYMBOL CAPACITY 64,
  variant SYMBOL CAPACITY 64,
  -- v2 experiment dimensions: every row groups by run / A-B pair / contract.
  run_id SYMBOL CAPACITY 1024 NOCACHE,
  pair_id SYMBOL CAPACITY 4096 NOCACHE,
  market_id SYMBOL CAPACITY 256,
  asset SYMBOL CAPACITY 16,
  horizon SYMBOL CAPACITY 16
) TIMESTAMP(ts) PARTITION BY DAY;

-- One row per V1 Jev call, with the EXACT state that produced it (backtesting).
-- The five Noul outputs are stored directly; repricing_* are the seven explicit
-- Choice buckets, and repricing_confidence is the Choice confidence.

CREATE TABLE IF NOT EXISTS paper_decisions (
  ts TIMESTAMP,
  condition_id SYMBOL CAPACITY 4096 NOCACHE,
  jev_ts TIMESTAMP,
  edge DOUBLE,
  threshold DOUBLE,
  decision SYMBOL CAPACITY 8,
  variant SYMBOL CAPACITY 64,
  paper_price DOUBLE,
  size DOUBLE,
  fair_value DOUBLE,
  run_id SYMBOL CAPACITY 1024 NOCACHE,
  pair_id SYMBOL CAPACITY 4096 NOCACHE,
  market_id SYMBOL CAPACITY 256,
  asset SYMBOL CAPACITY 16,
  horizon SYMBOL CAPACITY 16
) TIMESTAMP(ts) PARTITION BY DAY;

-- decision: QUOTE | SKIP. QUOTE means decide_quote returned a quote;
-- SKIP means it did not. `jev_ts` links back to jev_signals.ts.

CREATE TABLE IF NOT EXISTS resolutions (
  condition_id SYMBOL CAPACITY 4096 NOCACHE,
  winning_token_id SYMBOL CAPACITY 8192 NOCACHE,
  winning_outcome SYMBOL CAPACITY 8,
  resolved_ts TIMESTAMP
) TIMESTAMP(resolved_ts) PARTITION BY MONTH;

-- winning_outcome: YES | NO | FIFTY (UMA 50/50). Label for measuring real edge:
--   SELECT s.*, r.winning_outcome FROM jev_signals s
--   LEFT JOIN resolutions r ON s.condition_id = r.condition_id

-- Markout labels are recorded at all five V1 horizons, not only at resolution.
CREATE TABLE IF NOT EXISTS maker_markouts (
  ts TIMESTAMP,
  condition_id SYMBOL CAPACITY 4096 NOCACHE,
  jev_ts TIMESTAMP,
  side SYMBOL CAPACITY 8,
  price DOUBLE,
  size DOUBLE,
  mid_1s DOUBLE,
  mid_5s DOUBLE,
  mid_10s DOUBLE,
  mid_30s DOUBLE,
  mid_60s DOUBLE,
  variant SYMBOL CAPACITY 64,
  pnl_1s_pp DOUBLE,
  pnl_5s_pp DOUBLE,
  pnl_10s_pp DOUBLE,
  pnl_30s_pp DOUBLE,
  pnl_60s_pp DOUBLE,
  run_id SYMBOL CAPACITY 1024 NOCACHE,
  pair_id SYMBOL CAPACITY 4096 NOCACHE,
  market_id SYMBOL CAPACITY 256,
  asset SYMBOL CAPACITY 16,
  horizon SYMBOL CAPACITY 16
) TIMESTAMP(ts) PARTITION BY DAY;

-- One row per evaluated snapshot: joins the CONTROL and QUANT_V1 rows that
-- share a pair_id. status is complete (both branches usable) or incomplete
-- (one branch failed); control_ok/quant_ok are 1/0 flags. Exclude incomplete
-- pairs from paired A/B analysis; report them separately.
CREATE TABLE IF NOT EXISTS ab_pairs (
  ts TIMESTAMP,
  run_id SYMBOL CAPACITY 1024 NOCACHE,
  pair_id SYMBOL CAPACITY 4096 NOCACHE,
  market_id SYMBOL CAPACITY 256,
  asset SYMBOL CAPACITY 16,
  horizon SYMBOL CAPACITY 16,
  condition_id SYMBOL CAPACITY 4096 NOCACHE,
  status SYMBOL CAPACITY 16,
  state_seq LONG,
  observed_at_ms LONG,
  control_ok LONG,
  quant_ok LONG
) TIMESTAMP(ts) PARTITION BY DAY;

-- One row per paper fill from a variant-owned book. Never a live fill.
CREATE TABLE IF NOT EXISTS paper_fills (
  ts TIMESTAMP,
  condition_id SYMBOL CAPACITY 4096 NOCACHE,
  variant SYMBOL CAPACITY 64,
  side SYMBOL CAPACITY 8,
  order_id LONG,
  price DOUBLE,
  size DOUBLE,
  filled_at_ms LONG,
  maker LONG,
  run_id SYMBOL CAPACITY 1024 NOCACHE,
  pair_id SYMBOL CAPACITY 4096 NOCACHE,
  market_id SYMBOL CAPACITY 256,
  asset SYMBOL CAPACITY 16,
  horizon SYMBOL CAPACITY 16
) TIMESTAMP(ts) PARTITION BY DAY;

-- Sampled paper-equity rows per variant portfolio: position, realized,
-- unrealized (marked at the YES mid), total PnL, and exposure. Rebuilds
-- equity curves, cumulative PnL, and drawdowns without replaying fills.
CREATE TABLE IF NOT EXISTS paper_equity (
  ts TIMESTAMP,
  condition_id SYMBOL CAPACITY 4096 NOCACHE,
  variant SYMBOL CAPACITY 64,
  position DOUBLE,
  realized_pnl DOUBLE,
  unrealized_pnl DOUBLE,
  total_pnl DOUBLE,
  exposure DOUBLE,
  run_id SYMBOL CAPACITY 1024 NOCACHE,
  pair_id SYMBOL CAPACITY 4096 NOCACHE,
  market_id SYMBOL CAPACITY 256,
  asset SYMBOL CAPACITY 16,
  horizon SYMBOL CAPACITY 16
) TIMESTAMP(ts) PARTITION BY DAY;

-- SIGNAL RESEARCH: one forward drift row per usable Jev evaluation, QUOTE or
-- SKIP. ref_price is the eval-time mid; mo_*_pp are BUY-signed drift in
-- percentage points. Maker execution labels live in maker_markouts; join the
-- two datasets by pair_id + variant and condition signal levels from
-- jev_signals (e.g. underreact_up) for E[drift | signal > X].
CREATE TABLE IF NOT EXISTS signal_markouts (
  ts TIMESTAMP,
  condition_id SYMBOL CAPACITY 4096 NOCACHE,
  variant SYMBOL CAPACITY 64,
  jev_ts TIMESTAMP,
  ref_price DOUBLE,
  mid_1s DOUBLE,
  mid_5s DOUBLE,
  mid_10s DOUBLE,
  mid_30s DOUBLE,
  mid_60s DOUBLE,
  mo_1s_pp DOUBLE,
  mo_5s_pp DOUBLE,
  mo_10s_pp DOUBLE,
  mo_30s_pp DOUBLE,
  mo_60s_pp DOUBLE,
  run_id SYMBOL CAPACITY 1024 NOCACHE,
  pair_id SYMBOL CAPACITY 4096 NOCACHE,
  market_id SYMBOL CAPACITY 256,
  asset SYMBOL CAPACITY 16,
  horizon SYMBOL CAPACITY 16
) TIMESTAMP(ts) PARTITION BY DAY;

-- One row per replay order episode. signal_ts_ms is designated so unfilled
-- episodes remain queryable even when they never produce a fill timestamp.
CREATE TABLE IF NOT EXISTS trade_episodes (
  signal_ts_ms TIMESTAMP,
  episode_id SYMBOL CAPACITY 4096 NOCACHE,
  strategy_version SYMBOL CAPACITY 256,
  market SYMBOL CAPACITY 4096 NOCACHE,
  asset SYMBOL CAPACITY 256,
  horizon SYMBOL CAPACITY 64,
  jev_start_ts_ms LONG,
  jev_latency_ms LONG,
  submit_latency_ms LONG,
  order_arrival_ts_ms LONG,
  side SYMBOL CAPACITY 8,
  limit_price DOUBLE,
  stake_usd DOUBLE,
  shares DOUBLE,
  fill_ts_ms TIMESTAMP,
  fill_price DOUBLE,
  fill_qty DOUBLE,
  exit_type SYMBOL CAPACITY 16,
  exit_price DOUBLE,
  exit_ts_ms TIMESTAMP,
  gross_pnl_usd DOUBLE,
  fees_usd DOUBLE,
  rebates_usd DOUBLE,
  net_pnl_usd DOUBLE,
  max_adverse_excursion_usd DOUBLE,
  max_favorable_excursion_usd DOUBLE,
  capital_seconds_usd_s DOUBLE,
  pnl_historical_usd DOUBLE,
  pnl_current_usd DOUBLE
) TIMESTAMP(signal_ts_ms) PARTITION BY DAY;
