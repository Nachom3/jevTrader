# Complete multi-market paper/shadow runbook

This runbook starts the existing paper-only engine. It does not place live
orders. `ExecutionActor` owns `PaperBook` instances for `CONTROL` and
`QUANT_V1`; the private key is currently required by application configuration
but is not a permission to submit orders.

## 1. Start QuestDB

From the repository root:

```bash
docker compose up -d
```

The local ports are:

- QuestDB console/HTTP: `http://localhost:9002`
- QuestDB ILP TCP: `localhost:9009`
- QuestDB PostgreSQL wire protocol: `localhost:8812`

Do not use `docker compose down -v`: the `questdb_data` volume contains the
experiment history.

## 2. Apply the v2 schema/migration

Open `http://localhost:9002`, paste `questdb/schema.sql` into the SQL editor,
and execute it. On an existing v1 database, apply the explicit ALTER statements
and the `ab_pairs`, `paper_fills`, and `paper_equity` table definitions from
`questdb/migration-v1-to-v2.md` before starting the process. `CREATE TABLE IF
NOT EXISTS` does not alter old tables.

Verify the v2 contract before the run:

```sql
SELECT table_name
FROM tables()
WHERE table_name IN (
  'jev_signals', 'paper_decisions', 'maker_markouts',
  'ab_pairs', 'paper_fills', 'paper_equity'
);
```

For a database containing v1 rows, do not use rows with null/empty v2 tags in
A/B queries. The migration document explains the in-place option and why v1
rows cannot be backfilled into paired analysis.

## 3. Set the run environment

Use real credentials only through the shell environment or a local `.env` that
is not committed. The application requires all four credential/endpoint
variables even though the execution path is paper-only:

```bash
export TYPESAFE_API_KEY='replace-me'
export POLYMARKET_PRIVATE_KEY='replace-me'
export QUESTDB_HTTP_URL='http://localhost:9002'
export QUESTDB_ILP_ADDR='localhost:9009'

export JEVTRADER_RUN_ID='paper-2026-09-19-exp01'
export JEVTRADER_MARKET_FILES='markets/btc-5m.md,markets/btc-15m.md,markets/btc-1h.md,markets/btc-4h.md,markets/eth-5m.md,markets/eth-15m.md,markets/eth-1h.md,markets/eth-4h.md'
export JEVTRADER_MARKET_SIZE='10'
export JEV_DEADLINE_MS='1500'
export QUANT_FEATURES_ENABLED='true'
```

`JEVTRADER_MARKET_FILES` is a comma-separated list of at most eight
specification paths. Use the actual versioned files selected for the run; do
not invent a spec or mix contracts from different manifests. `JEVTRADER_RUN_ID`
must be unique for every run. `QUANT_FEATURES_ENABLED=false` runs CONTROL only;
`true` enables the existing QUANT_V1 shadow branch. Neither setting changes the
quote thresholds.

If only the checked-in fixture is available, run a one-market smoke check by
setting `JEVTRADER_MARKET_FILES` to that fixture. It validates wiring, not
multi-market evidence.

## 4. Start the paper/shadow process

Run from the repository root:

```bash
RUST_LOG=jevtrader=info cargo run --release
```

For contract routing diagnostics, use `RUST_LOG=jevtrader=trace`. The process
loads every configured spec, obtains the public Polymarket metadata and initial
book, starts the shared BTC/ETH feeds, then loops over each active market. It
runs until interrupted.

## 5. What to verify while it runs

### Logs

Check that:

- configuration validation succeeds and the process does not report a missing
  required environment variable;
- each configured market obtains metadata and an initial book;
- with trace logging, `market`, `asset`, and `horizon` identify the expected
  contract (`BTC-5m`, `ETH-1h`, etc.);
- no repeated `Polymarket top-of-book refresh failed` warning persists for a
  market; a failure marks that market stale and must not block the other
  markets;
- feed reconnect warnings are investigated, and QuestDB writer errors or a
  full writer queue are recorded as storage-quality incidents;
- no live-order or order-submission log appears. The runtime is paper-only.

The engine does not log every row at info level. Use the SQL checks below to
verify persistence instead of treating a quiet log as a missing signal.

### QuestDB rows

Replace the run ID in `questdb/reports.md` queries and confirm that rows carry
one common `run_id`, the expected `market_id`/`asset`/`horizon`, and matching
CONTROL/QUANT_V1 `pair_id` values. Check `ab_pairs.status` and keep incomplete
pairs visible. Inspect `paper_fills` and `paper_equity` independently from
`jev_signals` and `paper_decisions`.

Zero paper trades during a calm interval is valid. It can mean that no quote
was eligible, no quote was touched, or the conservative queue conditions did
not produce a fill. Retain the signals, decisions, zero-fill counts, and
sample size; never force a fill to make the run look active.

## 6. Stop and preserve the run

Stop the application with `Ctrl-C`. The current loop has no live execution
shutdown path because it owns no live orders. Then keep the QuestDB container
and volume available for reporting:

```bash
docker compose stop
```

Use `docker compose start` to resume the same local database. Only remove the
container when needed, and never remove `questdb_data` before exporting the
run. After stopping, run the report queries and archive the run ID, exact spec
files, split/configuration record, and row-count checks.
