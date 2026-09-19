# Sources inspection

## SII-WANGZJ/Polymarket_data
- markets.parquet: 0.00GB (996 bytes) [ALLOW-FULL]
- trades.parquet: 0.00GB (990 bytes) [SELECTIVE-ONLY]
- quant.parquet: 0.00GB (982 bytes) [SELECTIVE-ONLY]
- orderfilled.parquet: 0.00GB (1351 bytes) [FORBIDDEN]
- users.parquet: 0.00GB (982 bytes) [FORBIDDEN]

## TimeSeventeen/Polymarket-v1
Monthly OrderFilled + daily daily_aligned. Only intersecting months.
- OrderFilled/2024_01.parquet: 1000 bytes
- OrderFilled/2024_06.parquet: 996 bytes
- OrderFilled/2025_01.parquet: 994 bytes
- OrderFilled/2025_12.parquet: 998 bytes
- OrderFilled/2026_04.parquet: 998 bytes

## Binance vision (data.binance.vision)
- klines 1m ONLY for regimes/vol.
- aggTrades/trades sub-second for replay.
- bookTicker/depth where available.
- probe klines: 2169570 bytes

## Coinbase / Deribit
- Adapters prepared in research + src/feeds.
- No creds bundled: coverage BINANCE_ONLY.
