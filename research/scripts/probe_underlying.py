#!/usr/bin/env python3
"""Probe underlying_all schema and date/asset coverage (diagnostic)."""
import pyarrow.parquet as pq

path = "research-data/processed/underlying_all.parquet"
pf = pq.ParquetFile(path)
print("schema:", pf.schema_arrow.names)
print("rows:", pf.metadata.num_rows)
print("row_groups:", pf.metadata.num_row_groups)
for i in range(min(pf.metadata.num_row_groups, 4)):
    rg = pf.metadata.row_group(i)
    print(f"rg{i}: rows={rg.num_rows}")
    for j in range(rg.num_columns):
        col = rg.column(j)
        stats = col.statistics
        lo, hi = None, None
        if stats is not None and stats.has_min_max:
            lo, hi = stats.min, stats.max
        print(f"  {col.path_in_schema}: min={lo} max={hi}")
