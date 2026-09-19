# Reproducible QuestDB reports for multi-market paper runs

These queries use the QuestDB v2 schema. Replace `run-paper-001` with the
pre-registered `JEVTRADER_RUN_ID`. Every A/B query filters
`ab_pairs.status = 'complete'`; incomplete pairs remain a separate data-quality
result and are never silently dropped from the counts.

The dimension filters intentionally retain both assets and all supported
horizons so a broad result cannot hide a contract-specific failure.

## 1. CONTROL versus QUANT_V1: signed markout and fill-side summary

```sql
SELECT
  m.asset,
  m.horizon,
  m.market_id,
  m.variant,
  count() AS n_markouts,
  avg(m.pnl_1s_pp) AS mean_markout_1s_pp,
  avg(m.pnl_5s_pp) AS mean_markout_5s_pp,
  avg(m.pnl_30s_pp) AS mean_markout_30s_pp,
  sum(CASE WHEN m.pnl_5s_pp > 0 THEN 1 ELSE 0 END) / count() AS hit_rate_5s
FROM maker_markouts m
JOIN ab_pairs p
  ON m.run_id = p.run_id
 AND m.pair_id = p.pair_id
WHERE m.run_id = 'run-paper-001'
  AND p.status = 'complete'
  AND m.asset IN ('BTC', 'ETH')
  AND m.horizon IN ('5m', '15m', '1h', '4h')
  AND m.variant IN ('CONTROL', 'QUANT_V1')
GROUP BY m.asset, m.horizon, m.market_id, m.variant
ORDER BY m.asset, m.horizon, m.market_id, m.variant;
```

`n_markouts` is the denominator for markout metrics. Missing horizons are not
losses and are absent from the corresponding aggregate.

## 2. Paired CONTROL/QUANT delta on the same pair

This view makes the paired sample explicit before calculating the branch delta.
It is useful for the primary A/B comparison and avoids comparing different
market snapshots.

```sql
SELECT
  c.asset,
  c.horizon,
  c.market_id,
  count() AS n_complete_pairs,
  avg(q.pnl_5s_pp - c.pnl_5s_pp) AS quant_minus_control_markout_5s_pp,
  avg(q.pnl_1s_pp - c.pnl_1s_pp) AS quant_minus_control_markout_1s_pp
FROM maker_markouts c
JOIN maker_markouts q
  ON c.run_id = q.run_id
 AND c.pair_id = q.pair_id
 AND c.variant = 'CONTROL'
 AND q.variant = 'QUANT_V1'
JOIN ab_pairs p
  ON c.run_id = p.run_id
 AND c.pair_id = p.pair_id
WHERE c.run_id = 'run-paper-001'
  AND p.status = 'complete'
  AND c.asset IN ('BTC', 'ETH')
  AND c.horizon IN ('5m', '15m', '1h', '4h')
GROUP BY c.asset, c.horizon, c.market_id
ORDER BY c.asset, c.horizon, c.market_id;
```

## 3. Conditional alpha: `E[markout_5s | underreact_up > X]`

The signal and markout are joined by `run_id + pair_id + variant`. The same
complete-pair filter is applied to both branches. `0.75` is an analysis
placeholder for the pre-registered `X`; changing it creates a new analysis and
does not change the runtime quote threshold.

```sql
SELECT
  m.asset,
  m.horizon,
  m.market_id,
  m.variant,
  count() AS n,
  avg(m.pnl_5s_pp) AS e_markout_5s_given_underreact_up_gt_x,
  sum(CASE WHEN m.pnl_5s_pp > 0 THEN 1 ELSE 0 END) / count() AS conditional_hit_rate_5s
FROM maker_markouts m
JOIN jev_signals s
  ON m.run_id = s.run_id
 AND m.pair_id = s.pair_id
 AND m.variant = s.variant
JOIN ab_pairs p
  ON m.run_id = p.run_id
 AND m.pair_id = p.pair_id
WHERE m.run_id = 'run-paper-001'
  AND p.status = 'complete'
  AND s.underreact_up > 0.75
  AND m.asset IN ('BTC', 'ETH')
  AND m.horizon IN ('5m', '15m', '1h', '4h')
GROUP BY m.asset, m.horizon, m.market_id, m.variant
ORDER BY m.asset, m.horizon, m.market_id, m.variant;
```

## 4. Quote and fill counts by CONTROL/QUANT, asset, and horizon

This is the execution denominator. It reports quote decisions separately from
actual paper fills; a fill must never be inferred from a `QUOTE` decision.

```sql
SELECT
  d.asset,
  d.horizon,
  d.market_id,
  d.variant,
  count() AS n_decisions,
  sum(CASE WHEN d.decision = 'QUOTE' THEN 1 ELSE 0 END) AS n_quotes
FROM paper_decisions d
WHERE d.run_id = 'run-paper-001'
  AND d.asset IN ('BTC', 'ETH')
  AND d.horizon IN ('5m', '15m', '1h', '4h')
GROUP BY d.asset, d.horizon, d.market_id, d.variant
ORDER BY d.asset, d.horizon, d.market_id, d.variant;

SELECT
  f.asset,
  f.horizon,
  f.market_id,
  f.variant,
  count() AS n_fills,
  sum(f.size) AS filled_size
FROM paper_fills f
WHERE f.run_id = 'run-paper-001'
  AND f.asset IN ('BTC', 'ETH')
  AND f.horizon IN ('5m', '15m', '1h', '4h')
GROUP BY f.asset, f.horizon, f.market_id, f.variant
ORDER BY f.asset, f.horizon, f.market_id, f.variant;
```

Divide `n_fills` by the declared eligible quote denominator only after deciding
whether the unit is orders or fill events. Report both `n_fills` and
`filled_size` when partial fills are possible.

`paper_decisions.threshold` records the primary `under_min` gate used by the
runtime quote rule. It is an audit field; it does not replace the full
strategy decision or introduce a second threshold.

## 5. Equity curve and drawdown from `paper_equity`

The windowed high-water mark is calculated separately for every run, contract,
horizon, and variant. `total_pnl` is already realized plus unrealized PnL at
the sampled mid.

```sql
SELECT
  ts,
  run_id,
  asset,
  horizon,
  market_id,
  variant,
  position,
  realized_pnl,
  unrealized_pnl,
  total_pnl,
  max(total_pnl) OVER (
    PARTITION BY run_id, market_id, asset, horizon, variant
    ORDER BY ts
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS high_watermark,
  max(total_pnl) OVER (
    PARTITION BY run_id, market_id, asset, horizon, variant
    ORDER BY ts
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) - total_pnl AS drawdown_pp
FROM paper_equity
WHERE run_id = 'run-paper-001'
  AND asset IN ('BTC', 'ETH')
  AND horizon IN ('5m', '15m', '1h', '4h')
ORDER BY asset, horizon, market_id, variant, ts;
```

The `drawdown_pp` column is the running `MAX(total_pnl) - total_pnl`
calculation per variant and contract; its maximum is the segment drawdown.
This query intentionally keeps rows with an empty `pair_id`, because early
skips and fills/equity samples can be valid but not attributable to a pair. Do
not compare a combined drawdown with per-market drawdowns unless the
aggregation rule and capital/exposure weighting were pre-registered.

## 6. PnL by day, market, horizon, and variant

`paper_equity` is sampled during the run, so the daily query uses the first and
last observed total PnL in each calendar bucket. A day with no equity sample is
not imputed as zero.

```sql
SELECT
  ts,
  run_id,
  asset,
  horizon,
  market_id,
  variant,
  first(total_pnl) AS first_total_pnl,
  last(total_pnl) AS last_total_pnl,
  last(total_pnl) - first(total_pnl) AS day_pnl_pp,
  max(total_pnl) AS intraday_peak_total_pnl,
  min(total_pnl) AS intraday_trough_total_pnl
FROM paper_equity
WHERE run_id = 'run-paper-001'
  AND asset IN ('BTC', 'ETH')
  AND horizon IN ('5m', '15m', '1h', '4h')
SAMPLE BY 1d ALIGN TO CALENDAR;
```

If the report needs realized-only PnL, use `first(realized_pnl)` and
`last(realized_pnl)` in the same query. Keep `total_pnl`, realized PnL, and
unrealized PnL as distinct columns in the report.

## 7. Incomplete-pair and sample-size audit

```sql
SELECT
  asset,
  horizon,
  market_id,
  status,
  count() AS n_pairs,
  sum(control_ok) AS control_ok_rows,
  sum(quant_ok) AS quant_ok_rows
FROM ab_pairs
WHERE run_id = 'run-paper-001'
  AND asset IN ('BTC', 'ETH')
  AND horizon IN ('5m', '15m', '1h', '4h')
GROUP BY asset, horizon, market_id, status
ORDER BY asset, horizon, market_id, status;
```

This audit is part of every report. The `complete` count is the paired sample
size; `incomplete` rows stay visible but do not enter paired A/B deltas.

## 8. SIGNAL RESEARCH: E[drift | signal level] over ALL evaluations

`signal_markouts` covers every usable evaluation, QUOTE or SKIP, so signal
levels that never quote (e.g. `underreact_up = 0.31`) still contribute forward
drift. Conditioning levels come from `jev_signals` (same `pair_id` + `variant`).
This is the primary alpha table; TRADING RESEARCH (section 1) is the execution
follow-up on the quoted subset only.

```sql
SELECT
  s.asset,
  s.horizon,
  s.market_id,
  s.variant,
  count() AS n,
  avg(s.mo_5s_pp) AS mean_drift_5s_pp,
  sum(CASE WHEN s.mo_5s_pp > 0 THEN 1 ELSE 0 END) / count() AS hit_rate_5s,
  avg(CASE WHEN j.underreact_up > 0.75 THEN s.mo_5s_pp ELSE NULL END)
    AS mean_drift_5s_up075_pp,
  avg(CASE WHEN j.underreact_up > 0.60 THEN s.mo_5s_pp ELSE NULL END)
    AS mean_drift_5s_up060_pp,
  avg(CASE WHEN j.underreact_up > 0.45 THEN s.mo_5s_pp ELSE NULL END)
    AS mean_drift_5s_up045_pp,
  avg(CASE WHEN j.underreact_up > 0.31 THEN s.mo_5s_pp ELSE NULL END)
    AS mean_drift_5s_up031_pp
FROM signal_markouts s
JOIN jev_signals j
  ON s.run_id = j.run_id
 AND s.pair_id = j.pair_id
 AND s.variant = j.variant
JOIN ab_pairs p
  ON s.run_id = p.run_id
 AND s.pair_id = p.pair_id
WHERE s.run_id = 'run-paper-001'
  AND p.status = 'complete'
  AND s.asset IN ('BTC', 'ETH')
  AND s.horizon IN ('5m', '15m', '1h', '4h')
  AND s.variant IN ('CONTROL', 'QUANT_V1')
GROUP BY s.asset, s.horizon, s.market_id, s.variant
ORDER BY s.asset, s.horizon, s.market_id, s.variant;
```

Read the threshold ladder top-down: if `up075` shows no drift over `up031`,
raising thresholds cannot manufacture alpha. Thresholds stay frozen; this query
only measures. Segment further by volatility, distance-to-strike, and time
remaining from the `jev_signals.state_json` payload when a segment looks
promising.
