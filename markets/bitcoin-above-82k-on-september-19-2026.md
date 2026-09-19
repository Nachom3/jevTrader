# Market specification

slug: bitcoin-above-82k-on-september-19-2026
question: Will the price of Bitcoin be above $82,000 on September 19?
resolution_source: Binance BTC/USDT 1m close
target: 82000
resolution_at_ms: 1789833600000
resolution_rules: |
  This market will resolve to "Yes" if the Binance 1 minute candle for BTC/USDT 12:00 in the ET timezone (noon) on the date specified in the title has a final "Close" price higher than the price specified in the title. Otherwise, this market will resolve to "No".

  The resolution source for this market is Binance, specifically the BTC/USDT "Close" prices currently available at https://www.binance.com/en/trade/BTC_USDT with "1m" and "Candles" selected on the top bar.

  Please note that this market is about the price according to Binance BTC/USDT, not according to other exchanges or trading pairs.

  Price precision is determined by the number of decimal places in the source.
