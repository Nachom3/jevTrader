#!/usr/bin/env python3
"""Count underlying_all rows for an asset in [lo_ms, hi_ms) (diagnostic)."""
import sys

import pyarrow.parquet as pq

lo_ms = int(sys.argv[1])
hi_ms = int(sys.argv[2])
asset = sys.argv[3]
path = "research-data/processed/underlying_all.parquet"
table = pq.read_table(
    path,
    columns=["ts_ms", "asset"],
    filters=[
        ("ts_ms", ">=", lo_ms),
        ("ts_ms", "<", hi_ms),
        ("asset", "==", asset),
    ],
)
print(f"rows={table.num_rows}")
if table.num_rows:
    ts = table.column("ts_ms").to_pylist()
    print(f"min={min(ts)} max={max(ts)}")
