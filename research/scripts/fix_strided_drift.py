"""Recompute drift labels against the DENSE tape trajectory (offline).

Method bug fixed in code after the V2/V3 runs: strided decision items were
also used as the label trajectory, collapsing every horizon onto one print
(drift identically 0.0). The fixed runner samples labels from the full
unstrided take(500) head. This script applies EXACTLY that algorithm offline
to frozen rows, so no Jev re-spend is needed:
  seq -> strided tape index (stride = len//cap, cap from run) ->
  ts_eval -> usable_at = ts_eval + row.jev_latency_ms (LIVE latency) ->
  ref/horizon mids from full tape prints -> (mid-ref)*100.
Only rows with incomplete_pair=false are recomputed; errors keep None.
Output: {(pair_id, variant): {drift_1s..60s}} + method note. Frozen run
files are never modified; analysis merges the overlay explicitly.
"""

import argparse
import json
import sys

import polars as pl

HORIZONS_MS = [1_000, 5_000, 10_000, 30_000, 60_000]
SUFFIX = {1_000: "1s", 5_000: "5s", 10_000: "10s", 30_000: "30s", 60_000: "60s"}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", required=True)
    ap.add_argument(
        "--tape", default="research-data/processed/polymarket_trades.parquet"
    )
    ap.add_argument(
        "--selected", default="research-data/processed/selected_markets.parquet"
    )
    ap.add_argument("--cap", default="10")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    try:
        cap = int(args.cap)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"bad cap: {exc}") from exc
    try:
        rows = json.load(open(args.rows))
        tape = pl.scan_parquet(args.tape).collect().sort(["condition_id", "ts"])
        sel = pl.scan_parquet(args.selected).collect()
    except Exception as exc:
        raise RuntimeError(f"input read failed: {exc}") from exc
    try:
        slug2cid = dict(
            zip(sel["slug"].to_list(), sel["condition_id"].to_list(), strict=True)
        )
    except Exception as exc:
        raise RuntimeError(f"slug join failed: {exc}") from exc

    by_cond = {}
    for cid in tape["condition_id"].unique().to_list():
        d = tape.filter(pl.col("condition_id") == cid).sort("ts")
        by_cond[cid] = list(
            zip(
                [int(t) * 1000 for t in d["ts"].to_list()],
                [float(p) for p in d["yes_price"].to_list()],
            )
        )
    overlay = {}
    n_hit, n_rows = 0, 0
    for r in rows:
        if r.get("incomplete_pair"):
            continue
        cid = slug2cid.get(r.get("market_id", ""))
        prints = by_cond.get(cid)
        if not prints:
            continue
        stride = max(1, len(prints) // cap)
        try:
            seq = int(r["pair_id"].rsplit("-", 1)[1])
        except (ValueError, IndexError, KeyError):
            continue
        idx = seq * stride
        if idx >= len(prints):
            continue
        ts_eval = prints[idx][0]
        usable = ts_eval + int(r.get("jev_latency_ms", 0))
        ref = next((m for t, m in prints if t >= usable), None)
        if ref is None:
            continue
        entry = {}
        for h in HORIZONS_MS:
            m = next((mm for t, mm in prints if t >= usable + h), None)
            entry[f"drift_{SUFFIX[h]}_pp"] = (
                (m - ref) * 100.0 if m is not None else None
            )
        overlay[f"{r['pair_id']}\u0000{r['variant']}"] = entry
        n_hit += 1
        n_rows += 1
    print(f"rows_recomputed={n_hit}")
    try:
        with open(args.out, "w") as f:
            json.dump(
                {
                    "method": "dense-label overlay (see script docstring)",
                    "drift": overlay,
                },
                f,
            )
        print(f"wrote {args.out}")
    except OSError as exc:
        raise RuntimeError(f"out write failed: {exc}") from exc


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
