# PnL report (optimistic/base/conservative)

# Historical backtest reports

## CONTROL vs QUANT_V1 (n=412)

- CONTROL: n=206 mean_markout_5s=-1.0000pp sum_pnl=-2.0000pp
- QUANT_V1: n=206 mean_markout_5s=-2.5000pp sum_pnl=-2.5000pp

## By asset
- BTC: n=258 mean_mo5s=-0.6667pp
- ETH: n=154 mean_mo5s=-2.3333pp

## By horizon
- 15m: n=108 mean_mo5s=-0.0000pp
- 4h: n=254 mean_mo5s=-1.8000pp
- 5m: n=50 mean_mo5s=0.0000pp

## By regime
- HIGH_VOL-STRONG_DOWN: n=92 mean_mo5s=-3.0000pp
- LOW_VOL-SIDEWAYS: n=232 mean_mo5s=-0.0000pp
- NORMAL_VOL-SIDEWAYS: n=88 mean_mo5s=-1.0000pp

## By split
- EXPLORATION: n=256 mean_mo5s=-0.6667pp
- OUT_OF_SAMPLE: n=88 mean_mo5s=0.0000pp
- VALIDATION: n=68 mean_mo5s=-2.3333pp

## By resolution
- UNKNOWN: n=412 mean_mo5s=-1.5000pp

## By fill-model
- CONSERVATIVE: n=412 mean_mo5s=-1.5000pp

## Robustness OOS only (n=88)

- CONTROL: n=44 mean_markout_5s=0.0000pp sum_pnl=0.0000pp
- QUANT_V1: n=44 mean_markout_5s=0.0000pp sum_pnl=0.0000pp

