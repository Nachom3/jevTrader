# Historical research (Parquet-first, budget-guarded)

Source of truth for data: `research-data/manifest.json`.
Raw Parquet stays in `research-data/raw/` (gitignored).
Processed corpus in `research-data/processed/`.
Reports in `research-data/reports/`.

## Budget rule

`RESEARCH_MAX_DOWNLOAD_GB` in `research/config/corpus.yaml`
(default 8GB). Every downloader checks size, reuses files,
records checksums, and aborts before exceeding the budget.

## Never download full

- `markets.parquet` (~294MB): full allowed.
- `trades.parquet` (37.5GB), `quant.parquet` (36.6GB): selective only.
- `orderfilled.parquet` (110GB), `users.parquet` (47.7GB): forbidden.

## Pipeline

1. `inspect_sources.py` — sizes only, no large downloads.
2. `download_metadata.py` — markets.parquet.
3. `discover_markets.py` — BTC/ETH 5m/15m/1h/4h + splits + specs.
4. `classify_regimes.py` — Binance 1m -> LOW/NORMAL/HIGH x UP/DOWN/SIDEWAYS.
5. `download_polymarket.py` — ONLY intersecting TimeSeventeen months.
6. `download_underlying.py` — Binance klines + aggTrades sample; coverage flag.
7. `preprocess.py` — ETL only, never strategy.
8. `validate_corpus.py` — counts, EXACT/PROXY/UNKNOWN, coverage.
9. `run_backtest.py` — Rust `historical_backtest` + `make_reports.py`.

## Replay (Rust, same strategy code)

`src/replay/` reuses `feature_builder`, `quant_features`,
`lead_lag::should_quote`, `quote::decide_quote`, `RiskGate`,
`PaperBook::markout`. `ReplayClock` replaces `Utc::now()`;
backward-only as-of joins; three fill models; Jev latency honored.

## Commands (exact)

Bootstrap once (metadata + discovery + regimes + selective tapes):

```bash
python3 research/scripts/download_metadata.py \
&& python3 research/scripts/discover_markets.py \
&& python3 research/scripts/classify_regimes.py \
&& python3 research/scripts/download_polymarket.py --months 2024_01,2024_06 \
&& python3 research/scripts/download_underlying.py --kinds klines_1m,aggTrades \
&& python3 research/scripts/build_underlying.py --start 2024-01-01 --days 3 \
&& python3 research/scripts/build_tape.py \
&& python3 research/scripts/validate_corpus.py
```

Smoke backtest, 100 paired evaluations (item 36):

```bash
python3 research/scripts/run_backtest.py -- --max-pairs 100 \
  --fill-model CONSERVATIVE --latency BASE \
  --out research-data/reports/smoke_pairs.json
```

Full backtest CONTROL vs QUANT_V1 (item 25):

```bash
cargo run --release --bin historical_backtest -- \
  --max-pairs 10000 --fill-model CONSERVATIVE --latency BASE \
  --run-id full-bootstrap --out research-data/reports/full_pairs.json \
&& python3 research/scripts/make_reports.py \
  --input research-data/reports/full_pairs.json \
  --outdir research-data/reports/full
```

Live-Jev pilot (real judgments, hard budget; requires TYPESAFE_API_KEY):

```bash
JEV_VERBOSE=1 cargo run --release --bin historical_backtest -- \
  --max-pairs 10 --fill-model CONSERVATIVE --latency BASE \
  --run-id pilot-real10 --out research-data/reports/pilot_real10.json \
  --real-jev 1 --max-jev-calls 20
```

Fill/latency sensitivity (item 25):

```bash
for F in OPTIMISTIC BASE CONSERVATIVE; do
  cargo run --release --bin historical_backtest -- \
    --max-pairs 10000 --fill-model $F --latency BASE \
    --run-id sens-$F --out research-data/reports/sens_$F.json
done
```

Expand the dataset without code changes (item 26, same tooling):

```bash
# 1. Widen months intersecting new periods (budget-guarded):
python3 research/scripts/download_polymarket.py --months 2025_12,2026_01
# 2. Widen underlying window/venues in research/config/corpus.yaml,
#    then re-run download_underlying.py + build_underlying.py.
# 3. Rebuild tape + validate + re-run the full backtest command above.
```
