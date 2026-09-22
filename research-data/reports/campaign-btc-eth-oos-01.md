# Campaign OOS report: campaign-btc-eth-oos-01

> Pre-registered run. No thresholds, signals, or Jev wording changed.
> Headline economics: maker→maker, rebate 0. OOS only; no tuning on this data.

## Pre-registration

- run_id: `campaign-btc-eth-oos-01`
- Corpus: `research-data/processed` (6866 trades / 72 conditions / 72 resolutions)
- Conditions: all 68 mapped (41 exact-time + 27 proxy-time via `end_at`; 4 without spec SKIPPED explicitly)
- Arms: QUANT_ONLY, JEV_ONLY, QUANT_PLUS_JEV, MICRO_PLUS_REGIME (identical dataset, clock, sizing, latency model, fills, fees, exits)
- Signals: 4 stratified per condition (quantile spread, deterministic)
- Fill: CONSERVATIVE. Sizing: $5 USDC stake. Exits: HOLD to real resolution.
- Budget: max 850 live Jev calls, 1500ms deadline. Strategy: V1 frozen (`underreact_up > 0.75` gate).

## Execution

- Live Jev calls: **544** (all `live=true`), cache hits within run: 26 (identical states deduped)
- Latency measured: min 262ms, mean 360ms, max 1378ms
- Episodes: **0** (`research-data/reports/campaign-btc-eth-oos-01-episodes.json` is `[]`)
- Cache: `research-data/cache/campaign-btc-eth-oos-01/jev_cache.json` (544 versioned entries, reproducible)

## Jev verdict distribution (544 live judgments)

- `underreact_up`: min 0.28, mean 0.36, **max 0.50**. Zero above 0.50; gate needs > 0.75.
- `yes_pressure_5s` max 0.65 (Jev sees moves; it does not judge Poly as underreacting).

## OOS headline

| Metric | Value |
|---|---|
| n_episodes (OOS) | 0 |
| fills / NO_FILL | 0 / 0 (no quotes issued) |
| net PnL (historical / current) | n/a (no trades) |
| CI95 PnL | n/a (no trades) |
| P(PnL > 0) | n/a (no trades) |
| max drawdown / profit factor / ROI | n/a (no trades) |

Walk-forward windows and block bootstrap were not run over empty input:
there is no distribution to estimate. The tradable-set result stands on its own.

## Breakdowns

BTC/ETH, 5m/15m/1h/4h, regimes, HOLD/HEDGE, maker→maker: all empty —
V1 issued no maker intention on any segment.

## Conclusion (not alpha evidence FOR; evidence AGAINST quoting)

V1 with frozen thresholds does not quote on BTC/ETH up/down tape:
max `underreact_up` 0.50 across 544 stratified live judgments vs gate 0.75.
The tradable outcome is zero trades, hence zero PnL and zero risk —
this is a strategy finding, not an instrument failure (the ledger, fills,
and cache paths are proven by the deterministic checkpoints on fixed signals).

Do NOT tune thresholds on this sample. Next decisions belong to strategy
research (question design, trigger selection, or a different edge), not to
re-measurement.
