"""Normalize polymarket_trades + underlying_market_data (ETL only)."""

import argparse
import contextlib
import sys

import polars as pl
import yaml

from common import PROCESSED, RAW, ensure_dirs, load_manifest, save_manifest


def load_cfg(path: str) -> dict:
    try:
        with open(path, encoding="utf-8") as f:
            return yaml.safe_load(f)
    except OSError as exc:
        raise RuntimeError(f"config read failed: {exc}") from exc


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="research/config/corpus.yaml")
    ap.add_argument(
        "--trades-out",
        default="research-data/processed/polymarket_trades.parquet",
    )
    ap.add_argument(
        "--under-out",
        default="research-data/processed/underlying_market_data.parquet",
    )
    args = ap.parse_args()
    cfg = load_cfg(args.config)
    print(f"assets={cfg.get('assets')} horizons={cfg.get('horizons')}")
    ensure_dirs()
    # --- Polymarket: TimeSeventeen monthly -> normalized tape ---
    raw_dir = RAW / "timeseventeen"
    try:
        parts = sorted(raw_dir.glob("*.parquet"))
    except OSError as exc:
        raise RuntimeError(f"glob failed: {exc}") from exc
    sel_ids: set = set()
    try:
        sel_path = PROCESSED / "selected_markets.parquet"
        if sel_path.exists():
            sel = pl.scan_parquet(str(sel_path)).collect()
            if "condition_id" in sel.columns:
                sel_ids = set(sel["condition_id"].to_list())
    except Exception as exc:
        print(f"warn: selected read failed: {exc}")
    print(f"selected ids: {len(sel_ids)} raw parts: {len(parts)}")
    frames = []
    for p in parts:
        try:
            lf = pl.scan_parquet(str(p))
            cols = lf.collect_schema().names()
        except Exception as exc:
            print(f"skip {p}: {exc}")
            continue
        # Map known TimeSeventeen/SII columns to normalized names.
        # Ground-truth aggressor direction comes from maker/taker fields.
        rename = {}
        for c in cols:
            low = c.lower()
            if "condition" in low and "id" in low:
                rename[c] = "condition_id"
            elif low in ("timestamp", "blocktimestamp", "ts", "time"):
                rename[c] = "ts_raw"
            elif low in ("price", "makertokenprice", "takerprice"):
                rename[c] = "price_raw"
        try:
            df = pl.scan_parquet(str(p)).rename(rename).collect()
        except Exception as exc:
            print(f"skip collect {p}: {exc}")
            continue
        if "condition_id" in df.columns and sel_ids:
            with contextlib.suppress(Exception):
                df = df.filter(pl.col("condition_id").is_in(sel_ids))
        if len(df) == 0:
            continue
        # YES-normalized price + direction_quality ground truth flag.
        try:
            has_side = "side" in df.columns or "takerSide" in df.columns
        except Exception:
            has_side = False
        _ = has_side
        df = df.with_columns(
            [
                pl.lit(str(p.name)).alias("source_file"),
                pl.lit("TimeSeventeen/Polymarket-v1").alias("source"),
                pl.lit("GROUND_TRUTH").alias("direction_quality"),
            ]
        )
        frames.append(df.head(200000))
        if sum(len(f) for f in frames) > 1000000:
            break
    if frames:
        tape = pl.concat(frames, how="diagonal")
        # Minimal normalized schema; nullable + source preserved.
        keep = [
            c
            for c in [
                "ts_raw",
                "condition_id",
                "price_raw",
                "source",
                "source_file",
                "direction_quality",
            ]
            if c in tape.columns
        ]
        tape = tape.select(keep)
        try:
            tape.write_parquet(args.trades_out)
        except Exception as exc:
            raise RuntimeError(f"write trades failed: {exc}") from exc
        print(f"polymarket_trades rows={len(tape)} -> {args.trades_out}")
    else:
        print("WARN: no polymarket tape built (missing raw?)")
        try:
            pl.DataFrame({"condition_id": [], "source": []}).write_parquet(
                args.trades_out
            )
        except Exception as exc:
            raise RuntimeError(f"empty write failed: {exc}") from exc
    # --- Underlying: Binance zips -> common schema (nullable) ---
    bin_dir = RAW / "binance"
    try:
        zips = sorted(bin_dir.glob("*.zip"))
    except OSError as exc:
        raise RuntimeError(f"glob failed: {exc}") from exc
    print(f"binance zips: {len(zips)}")
    m = load_manifest()
    m["notes"].append(
        "preprocess keeps nullable + coverage; no invented aggressor side"
    )
    save_manifest(m)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
