# Alpha report (markouts +1/+5/+10/+30/+60)

# Historical backtest reports

## CONTROL vs QUANT_V1 (n=200)

- CONTROL: n=100 mean_markout_5s=-0.3333pp sum_pnl=-3.3000pp
- QUANT_V1: n=100 mean_markout_5s=0.2000pp sum_pnl=0.3000pp

## By asset
- BTC: n=120 mean_mo5s=-0.6667pp
- ETH: n=80 mean_mo5s=0.2500pp

## By horizon
- 15m: n=64 mean_mo5s=-0.0000pp
- 4h: n=96 mean_mo5s=-0.4000pp
- 5m: n=40 mean_mo5s=1.0000pp

## By regime
- LOW_VOL-SIDEWAYS: n=112 mean_mo5s=0.3333pp
- NORMAL_VOL-SIDEWAYS: n=88 mean_mo5s=-0.5000pp

## By split
- EXPLORATION: n=120 mean_mo5s=-0.6667pp
- OUT_OF_SAMPLE: n=54 mean_mo5s=0.2000pp
- VALIDATION: n=26 mean_mo5s=0.3333pp

## By resolution
- UNKNOWN: n=200 mean_mo5s=-0.1429pp

## By fill-model
- BASE: n=200 mean_mo5s=-0.1429pp

## Robustness OOS only (n=54)

- CONTROL: n=27 mean_markout_5s=0.0000pp sum_pnl=-0.1000pp
- QUANT_V1: n=27 mean_markout_5s=0.5000pp sum_pnl=0.5000pp

