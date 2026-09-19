"""Build underlying_market_data.parquet from Binance aggTrades."""

import argparse
import datetime
import sys
import zipfile

import polars as pl

from common import (
    PROCESSED,
    RAW,
    ensure_dirs,
    load_manifest,
    record_file,
    save_manifest,
)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2024-01-01")
    ap.add_argument("--days", default="3")
    ap.add_argument("--out", default=str(PROCESSED / "underlying_market_data.parquet"))
    return ap.parse_args()


def day_bounds(start: str, days: str) -> tuple:
    try:
        n = int(days)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"bad days: {exc}") from exc
    try:
        d0 = datetime.date.fromisoformat(start)
    except ValueError as exc:
        raise RuntimeError(f"bad start: {exc}") from exc
    try:
        lo = int(datetime.datetime(d0.year, d0.month, d0.day).timestamp())
    except (OverflowError, OSError, ValueError) as exc:
        raise RuntimeError(f"bad timestamp: {exc}") from exc
    return lo * 1000, (lo + n * 86400) * 1000


def to_ms(raw: int) -> int:
    try:
        v = int(raw)
    except (TypeError, ValueError):
        return -1
    if v > 1_000_000_000_000_000:
        return v // 1000
    return v


def read_agg(path: str, lo_ms: int, hi_ms: int, asset: str) -> pl.DataFrame:
    try:
        zf = zipfile.ZipFile(path)
    except Exception as exc:
        print(f"skip {path}: {exc}")
        return pl.DataFrame()
    try:
        name = zf.namelist()[0]
        with zf.open(name) as f:
            df = pl.read_csv(
                f,
                has_header=False,
                new_columns=[
                    "agg_id",
                    "price",
                    "qty",
                    "first_id",
                    "last_id",
                    "ts_raw",
                    "buyer_maker",
                    "best_match",
                ],
            )
    except Exception as exc:
        print(f"skip collect {path}: {exc}")
        return pl.DataFrame()
    finally:
        try:
            zf.close()
        except Exception as exc:
            print(f"warn close {path}: {exc}")
    try:
        df = df.with_columns(
            [
                pl.col("ts_raw")
                .map_elements(to_ms, return_dtype=pl.Int64)
                .alias("ts_ms"),
                pl.col("price").cast(pl.Float64),
                pl.col("qty").cast(pl.Float64),
            ]
        ).filter((pl.col("ts_ms") >= lo_ms) & (pl.col("ts_ms") < hi_ms))
        if len(df) == 0:
            return df
        return df.select(
            [
                pl.col("ts_ms"),
                pl.lit(asset).alias("asset"),
                pl.lit("BINANCE").alias("venue"),
                pl.lit("spot").alias("instrument"),
                pl.lit("aggTrade").alias("event_type"),
                pl.col("price"),
                pl.col("qty"),
                pl.when(pl.col("buyer_maker") == "True")
                .then(pl.lit("SELL"))
                .otherwise(pl.lit("BUY"))
                .alias("aggressor_side"),
                pl.lit(None, dtype=pl.Float64).alias("bid"),
                pl.lit(None, dtype=pl.Float64).alias("ask"),
                pl.lit("binance-vision").alias("source"),
            ]
        )
    except Exception as exc:
        print(f"skip normalize {path}: {exc}")
        return pl.DataFrame()


def main() -> None:
    args = parse_args()
    ensure_dirs()
    lo_ms, hi_ms = day_bounds(args.start, args.days)
    frames = []
    for asset, sym in [("BTC", "BTCUSDT"), ("ETH", "ETHUSDT")]:
        path = str(RAW / "binance" / f"{sym}-aggTrades-2024-01.zip")
        df = read_agg(path, lo_ms, hi_ms, asset)
        print(f"{sym}: {len(df)} ticks in window")
        if len(df) > 0:
            frames.append(df)
    if frames:
        out = pl.concat(frames).sort(["ts_ms"])
    else:
        out = pl.DataFrame(
            {
                "ts_ms": [],
                "asset": [],
                "venue": [],
                "instrument": [],
                "event_type": [],
                "price": [],
                "qty": [],
                "aggressor_side": [],
                "bid": [],
                "ask": [],
                "source": [],
            }
        )
    try:
        out.write_parquet(args.out)
    except Exception as exc:
        raise RuntimeError(f"write failed: {exc}") from exc
    print(f"underlying rows={len(out)} -> {args.out}")
    m = load_manifest()
    record_file(
        m,
        "binance-vision",
        "underlying_market_data.parquet",
        args.out,
        [],
        [],
        {"mode": "aggtrades-subsecond-window"},
        rows_before=None,
        rows_after=len(out),
    )
    save_manifest(m)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
