#!/usr/bin/env python3
"""Frozen-gate OOS summary over a precompute evaluations.parquet.

Reads live evaluations, reports per-signal min/mean/max plus frozen
Lead-Lag V1 gate pass counts and their conjunction. No thresholds are
tuned here; the gate values are frozen inputs. Read-only on inputs;
prints a Markdown table to stdout.
"""

import argparse
import json
import sys

SIGNALS = [
    "underreact_up",
    "underreact_down",
    "move_persists",
    "fill_before_decay",
    "fill_toxic",
    "no_pressure_5s",
    "p_up_ge_1_tick",
    "yes_pressure_5s",
]

# Frozen V1 gate: (column, predicate-label, lambda). Mirrors kachoio-pilot-oos-01.
GATE = [
    ("underreact_up", ">0.75", lambda v: v > 0.75),
    ("underreact_down", "<0.30", lambda v: v < 0.30),
    ("move_persists", ">0.60", lambda v: v > 0.60),
    ("fill_before_decay", ">0.60", lambda v: v > 0.60),
    ("fill_toxic", "<0.30", lambda v: v < 0.30),
    ("no_pressure_5s", "<0.30", lambda v: v < 0.30),
    ("p_up_ge_1_tick", ">0.65", lambda v: v > 0.65),
]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("evaluations_parquet")
    args = ap.parse_args()

    import pyarrow.parquet as pq  # noqa: PLC0415 - local analysis only

    table = pq.read_table(args.evaluations_parquet)
    print(f"columns: {table.schema.names}", file=sys.stderr)
    if "repricing_ticks" in table.schema.names and len(table) > 0:
        print(f"sample repricing_ticks: {table.column('repricing_ticks')[0].as_py()}",
              file=sys.stderr)
        print(f"sample split/fidelity/live: {table.column('split')[0].as_py()} / "
              f"{table.column('fidelity')[0].as_py()} / {table.column('live')[0].as_py()}",
              file=sys.stderr)
    rows = table.to_pylist()
    print(f"rows: {len(rows)}", file=sys.stderr)
    for row in rows:  # derive p_up_ge_1_tick from the repricing bucket JSON
        if isinstance(row, dict) and row.get("p_up_ge_1_tick") is None:
            try:
                buckets = json.loads(row.get("repricing_ticks") or "{}")
                row["p_up_ge_1_tick"] = float(buckets.get("up_3_plus", 0.0)) \
                    + float(buckets.get("up_2", 0.0)) + float(buckets.get("up_1", 0.0))
            except (ValueError, TypeError, AttributeError):
                pass

    print("| Signal | Min | Mean | Max | Frozen gate |")
    print("|---|---:|---:|---:|---|")
    gate_cols = [c for c, _, _ in GATE]
    series: dict[str, list[float]] = {c: [] for c in gate_cols}
    for row in rows:
        for col in gate_cols:
            try:
                val = row.get(col)
            except AttributeError:
                val = None
            if isinstance(val, bool):
                continue
            if isinstance(val, (int, float)):
                series[col].append(float(val))
    conjunction = 0
    for row in rows:
        ok = True
        for col, _, pred in GATE:
            val = row.get(col) if isinstance(row, dict) else None
            if isinstance(val, bool) or not isinstance(val, (int, float)) or not pred(float(val)):
                ok = False
                break
        if ok:
            conjunction += 1
    for col, label, pred in GATE:
        vals = series[col]
        n = len(vals)
        passed = sum(1 for v in vals if pred(v))
        if vals:
            print(f"| {col} | {min(vals):.2f} | {sum(vals) / n:.2f} | {max(vals):.2f} | {label}: {passed}/{n} |")
        else:
            print(f"| {col} | n/a | n/a | n/a | {label}: no usable values |")
    print(f"| CONJUNCTION (all gates) | | | | {conjunction}/{len(rows)} |")

    for sig in SIGNALS:
        if sig not in table.schema.names:
            print(f"note: column {sig} absent from parquet", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
