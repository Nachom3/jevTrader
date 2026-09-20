# Microstructure feasibility probe (2026-09-20, no downloads promised)

Question: what historical microstructure can we REALLY reconstruct for the
corpus weeks (Dec-25..Apr-26)? Method: HEAD checks + 1 tiny sample.
Raw budget: ~5.5/8GB used. Nothing below downloads more than stated.

## Binance Vision bulk (HEAD verified)
- spot aggTrades daily: OK (already downloaded, 254 files, gaps=0).
  buyer_maker per aggregate -> real OFI/aggressive flow, CODE ONLY.
- spot trades daily (2026-04-28): 200, 16.7MB. Marginal over aggTrades.
- spot bookTicker daily/monthly: 404 (does NOT exist as bulk).
- spot/futures bookDepth daily: futures um EXISTS (529KB/day) BUT schema is
  `timestamp,percentage,depth,notional` (daily depth LADDER, not L2 snaps).
- futures um aggTrades daily (2026-04-28): 200, 13.2MB. Scoped EXACT weeks
  (~18 days x BTC+ETH) ~= 230MB: inside budget. Enables REAL cross
  spot<->perp lead/lag + basis.
- Sample fetched (then deleted): bookDepth ladder confirmed, useless for
  OBI/microprice dynamics; optional as daily liquidity-regime only.

## Coinbase
- /trades: live only (today). /candles: history OK (tested Mar-2026 daily).
  No free bulk L2 years back. Candles redundant with Binance 1m we have.
  Useful for LIVE shadow, not historical microstructure. Verdict: OUT.

## Deribit
- Not probed (no free bulk route known; presumed paid). OUT unless the paid
  route (tardis/databento-class) is explicitly chosen. V2 does not need it:
  Binance spot<->perp already gives a genuine second venue.

## Polymarket L2
- No public historical L2 (SII orderfilled 37.5GB stays forbidden).
  Poly books stay synthetic; Poly-side flow from tape taker_direction
  (aggressor + GROUND_TRUTH quality, already in polymarket_trades) is real.

## V2-buildable list (honest)
IN (no new bulk except scoped perp): OFI/aggressive flow BOTH legs,
  aggressive ratios, spot-perp basis + perp lead/lag, fixed distance_to_target,
  de-duplicated return windows, optional depth-ladder regime.
OUT (say loudly): live OBI-top, microprice deviation, spread dynamics on
  spot (no bookTicker bulk); Poly L2 depth (nonexistent publicly).
  The user's "OBI/microprice" wishes are half-blocked: OFI+basis is the
  feasible microstructure core. Design V2 around what exists.
