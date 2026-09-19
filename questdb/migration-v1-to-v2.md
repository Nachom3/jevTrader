# QuestDB migration v1 → v2 (multi-market paper stage)

`CREATE TABLE IF NOT EXISTS` never alters an existing QuestDB table, so a
database created with the v1 schema keeps serving the old shape while new code
writes the v2 shape. Apply this migration explicitly; never let old and new
rows mix silently.

## What v2 adds

- `jev_signals`, `paper_decisions`, `maker_markouts`: five experiment
  dimensions each — `run_id`, `pair_id`, `market_id`, `asset`, `horizon`
  (all `SYMBOL`; ids are `NOCACHE`).
- New tables: `ab_pairs` (one row per evaluated snapshot, `complete` vs
  `incomplete`), `paper_fills` (variant-owned paper fills), `paper_equity`
  (sampled variant equity: position, realized/unrealized/total PnL, exposure),
  `signal_markouts` (one forward drift row per usable Jev evaluation, QUOTE
  or SKIP; see SIGNAL vs TRADING RESEARCH below).

## Two research datasets

- SIGNAL RESEARCH (`jev_signals` + `signal_markouts`, joined by
  `pair_id` + `variant`): every usable evaluation contributes forward drift
  at +1/+5/+10/+30/+60s, whether the strategy quoted or skipped. This is the
  dataset that answers "does Jev predict Polymarket moves".
- TRADING RESEARCH (`paper_decisions` + `maker_markouts` + `paper_fills` +
  `paper_equity`): only quotes/fills. This answers "does the alpha survive
  maker execution".

A valid SKIP is a complete signal observation, not a broken pair. Only rows
in `ab_pairs` with `status = 'incomplete'` (a branch failed: Jev error, no
usable signal) are excluded from paired A/B analysis.

## Option A — fresh database (recommended for research)

```sql
DROP TABLE IF EXISTS jev_signals;
DROP TABLE IF EXISTS paper_decisions;
DROP TABLE IF EXISTS maker_markouts;
```

Then apply `questdb/schema.sql` from scratch. Old data is discarded
deliberately; v1 rows lack `pair_id` and can never join the A/B analysis.

## Option B — in-place ALTER (keeps v1 history readable)

QuestDB supports `ALTER TABLE <name> ADD COLUMN <def>` on partitioned tables.
Run once per table, in this order:

```sql
ALTER TABLE jev_signals ADD COLUMN run_id SYMBOL CAPACITY 1024 NOCACHE;
ALTER TABLE jev_signals ADD COLUMN pair_id SYMBOL CAPACITY 4096 NOCACHE;
ALTER TABLE jev_signals ADD COLUMN market_id SYMBOL CAPACITY 256;
ALTER TABLE jev_signals ADD COLUMN asset SYMBOL CAPACITY 16;
ALTER TABLE jev_signals ADD COLUMN horizon SYMBOL CAPACITY 16;

ALTER TABLE paper_decisions ADD COLUMN run_id SYMBOL CAPACITY 1024 NOCACHE;
ALTER TABLE paper_decisions ADD COLUMN pair_id SYMBOL CAPACITY 4096 NOCACHE;
ALTER TABLE paper_decisions ADD COLUMN market_id SYMBOL CAPACITY 256;
ALTER TABLE paper_decisions ADD COLUMN asset SYMBOL CAPACITY 16;
ALTER TABLE paper_decisions ADD COLUMN horizon SYMBOL CAPACITY 16;

ALTER TABLE maker_markouts ADD COLUMN run_id SYMBOL CAPACITY 1024 NOCACHE;
ALTER TABLE maker_markouts ADD COLUMN pair_id SYMBOL CAPACITY 4096 NOCACHE;
ALTER TABLE maker_markouts ADD COLUMN market_id SYMBOL CAPACITY 256;
ALTER TABLE maker_markouts ADD COLUMN asset SYMBOL CAPACITY 16;
ALTER TABLE maker_markouts ADD COLUMN horizon SYMBOL CAPACITY 16;
```

Then create the three new tables by applying the corresponding
`CREATE TABLE IF NOT EXISTS` blocks from `questdb/schema.sql`.

## Reading mixed data safely

- v1 rows have `NULL` in the five new columns. Filter paired A/B queries
  with `pair_id != '' AND pair_id IS NOT NULL` (QuestDB stores a missing
  symbol as null). The v2 writer sends real contract tags, but `pair_id` may
  be empty for production early skips and for paper fills/equity samples that
  cannot be attributed to an evaluated pair.
- Join CONTROL vs QUANT_V1 strictly through `ab_pairs` with
  `status = 'complete'`. Never infer pairs from consecutive `state_seq`.
- `paper_fills` / `paper_equity` start at v2; there is no v1 equivalent, so
  no backfill is possible or attempted.
