# Robustness (walk-forward/OOS)

# Historical backtest reports

## CONTROL vs QUANT_V1 (n=200)

- CONTROL: n=100 mean_markout_5s=-0.6667pp sum_pnl=-1.0000pp
- QUANT_V1: n=100 mean_markout_5s=-1.0000pp sum_pnl=-0.5000pp

## By asset
- BTC: n=120 mean_mo5s=-0.6667pp
- ETH: n=80 mean_mo5s=-1.0000pp

## By horizon
- 15m: n=64 mean_mo5s=-0.0000pp
- 4h: n=96 mean_mo5s=-1.0000pp
- 5m: n=40 mean_mo5s=0.0000pp

## By regime
- LOW_VOL-SIDEWAYS: n=112 mean_mo5s=-0.0000pp
- NORMAL_VOL-SIDEWAYS: n=88 mean_mo5s=-1.0000pp

## By split
- EXPLORATION: n=120 mean_mo5s=-0.6667pp
- OUT_OF_SAMPLE: n=54 mean_mo5s=0.0000pp
- VALIDATION: n=26 mean_mo5s=-1.0000pp

## By resolution
- UNKNOWN: n=200 mean_mo5s=-0.7500pp

## By fill-model
- CONSERVATIVE: n=200 mean_mo5s=-0.7500pp

## Robustness OOS only (n=54)

- CONTROL: n=27 mean_markout_5s=0.0000pp sum_pnl=0.0000pp
- QUANT_V1: n=27 mean_markout_5s=0.0000pp sum_pnl=0.0000pp

