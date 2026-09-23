#!/usr/bin/env python3
"""Coverage map of underlying_all from row-group ts_ms stats (footer only)."""
from datetime import datetime, timezone

import pyarrow.parquet as pq

pf = pq.ParquetFile("research-data/processed/underlying_all.parquet")
names = pf.schema_arrow.names
ts_idx = names.index("ts_ms")
prev_hi = None
gaps = []
for i in range(pf.metadata.num_row_groups):
    col = pf.metadata.row_group(i).column(ts_idx)
    stats = col.statistics
    if stats is None or not stats.has_min_max:
        print(f"rg{i}: no stats")
        continue
    lo, hi = stats.min, stats.max
    if i % 100 == 0 or (prev_hi is not None and lo - prev_hi > 86_400_000):
        lod = datetime.fromtimestamp(lo / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M")
        hid = datetime.fromtimestamp(hi / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M")
        print(f"rg{i}: {lod} .. {hid}")
    if prev_hi is not None and lo - prev_hi > 86_400_000:
        lod = datetime.fromtimestamp(prev_hi / 1000, timezone.utc).strftime("%Y-%m-%d")
        hid = datetime.fromtimestamp(lo / 1000, timezone.utc).strftime("%Y-%m-%d")
        gaps.append((lod, hid, (lo - prev_hi) // 86_400_000))
    prev_hi = hi
print("GAPS >1d:")
for lod, hid, days in gaps:
    print(f"  {lod} -> {hid} (~{days}d)")
