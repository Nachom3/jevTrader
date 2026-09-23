# Market specification

slug: bitcoin-up-or-down-september-22-2026-6pm-et
question: Bitcoin Up or Down - September 22, 6PM ET
resolution_source: Binance BTC/USDT 1h candle (open vs close)
target: 86184.00
resolution_at_ms: 1790118000000
asset: BTC
horizon: 1h
resolution_rules: |
  This market will resolve to "Up" if the close price is greater than or equal to the open price for the BTC/USDT 1 hour candle that begins on the time and date specified in the title. Otherwise, this market will resolve to "Down".

  The resolution source for this market is information from Binance, specifically the BTC/USDT pair (https://www.binance.com/en/trade/BTC_USDT). The close « C » and open « O » displayed at the top of the graph for the relevant "1H" candle will be used once the data for that candle is finalized.

  Please note that this market is about the price according to Binance BTC/USDT, not according to other exchanges or trading pairs.
