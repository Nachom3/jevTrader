"""Validate corpus: counts, splits, fidelity, coverage."""

import argparse
import sys

import polars as pl

from common import PROCESSED


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--processed", default=str(PROCESSED))
    args = ap.parse_args()
    base = args.processed
    out_lines = []
    try:
        sel = pl.scan_parquet(f"{base}/selected_markets.parquet").collect()
        out_lines.append(f"selected_markets rows={len(sel)}")
        out_lines.append(
            str(sel.group_by(["asset", "horizon"]).len().sort(["asset", "horizon"]))
        )
        if "split" in sel.columns:
            out_lines.append(str(sel.group_by("split").len()))
    except Exception as exc:
        out_lines.append(f"selected_markets MISSING: {exc}")
    try:
        specs = pl.scan_parquet(f"{base}/resolution_specs.parquet").collect()
        out_lines.append(str(specs.group_by("fidelity").len()))
        n = len(specs)
        for fid in ["EXACT", "PROXY", "UNKNOWN"]:
            try:
                c = len(specs.filter(pl.col("fidelity") == fid))
                pct = 100.0 * c / n if n else 0.0
                out_lines.append(f"{fid}: {c}/{n} ({pct:.1f}%)")
            except Exception as exc:
                out_lines.append(f"{fid} count failed: {exc}")
                continue
    except Exception as exc:
        out_lines.append(f"resolution_specs MISSING: {exc}")
    for name in [
        "polymarket_trades.parquet",
        "underlying_market_data.parquet",
        "market_regimes.parquet",
    ]:
        try:
            df = pl.scan_parquet(f"{base}/{name}").collect()
            out_lines.append(f"{name} rows={len(df)}")
        except Exception as exc:
            out_lines.append(f"{name} MISSING: {exc}")
    print("\n".join(out_lines))


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
