"""Classify VOL + TREND regimes from Binance 1m (lightweight)."""

import argparse
import pathlib
import subprocess
import sys
import zipfile

import polars as pl
import yaml

from common import (
    RAW,
    budget_guard,
    ensure_dirs,
    load_manifest,
    record_file,
    save_manifest,
)

BIN_VISION = "https://data.binance.vision/data/spot/monthly/klines"


def load_cfg(path: str) -> dict:
    try:
        with open(path, encoding="utf-8") as f:
            return yaml.safe_load(f)
    except OSError as exc:
        raise RuntimeError(f"config read failed: {exc}") from exc


def parse_budget(cfg: dict) -> float:
    try:
        return float(cfg.get("research_max_download_gb", 8.0))
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"bad budget: {exc}") from exc


def fetch_zip(url: str, dest: pathlib.Path) -> None:
    if not url.startswith("https://"):
        raise ValueError("only https allowed")
    if dest.exists():
        try:
            if dest.stat().st_size > 0:
                return
        except OSError as exc:
            raise RuntimeError(f"stat failed: {exc}") from exc
    try:
        dest.parent.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise RuntimeError(f"mkdir failed: {exc}") from exc
    try:
        proc = subprocess.run(
            ["curl", "-fL", "--max-time", "300", url, "-o", str(dest)],
            capture_output=True,
            text=True,
            check=False,
        )
    except Exception as exc:
        raise RuntimeError(f"curl failed: {exc}") from exc
    if proc.returncode != 0:
        raise RuntimeError(f"curl error: {proc.stderr[-1000:]}")


def months_between(start: str, end: str):
    try:
        y0, m0 = int(start[:4]), int(start[5:7])
        y1, m1 = int(end[:4]), int(end[5:7])
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"bad dates: {exc}") from exc
    out = []
    y, m = y0, m0
    while (y, m) <= (y1, m1):
        out.append(f"{y:04d}-{m:02d}")
        m += 1
        if m > 12:
            m, y = 1, y + 1
    return out


def read_klines(zpath: pathlib.Path, symbol: str) -> pl.DataFrame:
    try:
        with zipfile.ZipFile(zpath) as zf:
            name = zf.namelist()[0]
            with zf.open(name) as f:
                df = pl.read_csv(
                    f,
                    has_header=False,
                    new_columns=[
                        "open_t",
                        "open",
                        "high",
                        "low",
                        "close",
                        "vol",
                        "close_t",
                        "qvol",
                        "n",
                        "tb",
                        "tq",
                        "ign",
                    ],
                )
    except Exception as exc:
        raise RuntimeError(f"zip read failed {zpath}: {exc}") from exc
    try:
        raw = df.select(
            [
                pl.col("close_t").cast(pl.Int64).alias("close_t"),
                pl.col("close").cast(pl.Float64).alias("close"),
            ]
        )
        # Binance vision unit drift: ms historically, us in recent files.
        unit = raw.select(pl.col("close_t").max().alias("m")).to_dicts()[0]["m"]
        div = 1_000_000 if unit > 1_000_000_000_000_000 else 1_000
        return raw.select(
            [
                (pl.col("close_t") // div).alias("ts_s"),
                pl.col("close"),
            ]
        ).with_columns(pl.lit(symbol).alias("symbol"))
    except Exception as exc:
        raise RuntimeError(f"frame failed: {exc}") from exc


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="research/config/corpus.yaml")
    ap.add_argument("--out", default="research-data/processed/market_regimes.parquet")
    args = ap.parse_args()
    cfg = load_cfg(args.config)
    ensure_dirs()
    budget_guard(parse_budget(cfg))
    symbols = cfg.get("underlying", {}).get("symbols", ["BTCUSDT", "ETHUSDT"])
    months = months_between(
        cfg.get("start_date", "2024-01-01"), cfg.get("end_date", "2026-04-28")
    )
    # Bootstrap keeps downloads small: sample quarterly months.
    sample = months[::3][:10]
    frames = []
    for sym in symbols:
        for ym in sample:
            url = f"{BIN_VISION}/{sym}/1m/{sym}-1m-{ym}.zip"
            dest = RAW / "binance" / f"{sym}-1m-{ym}.zip"
            try:
                fetch_zip(url, dest)
            except RuntimeError as exc:
                print(f"skip {ym} {sym}: {exc}")
                continue
            frames.append(read_klines(dest, sym))
    budget_guard(parse_budget(cfg))
    if not frames:
        raise RuntimeError("no klines fetched")
    allk = pl.concat(frames).sort(["symbol", "ts_s"])
    # Daily buckets: realized vol + drift from 1m closes.
    try:
        allk = allk.with_columns(
            (pl.col("close") / pl.col("close").shift(1).over("symbol") - 1.0).alias(
                "ret_1m"
            )
        )
    except Exception as exc:
        raise RuntimeError(f"returns failed: {exc}") from exc
    out_rows = []
    try:
        days = (
            allk.with_columns((pl.col("ts_s") // 86400 * 86400).alias("day"))
            .group_by(["symbol", "day"])
            .agg(
                [
                    pl.col("close").last().alias("close"),
                    pl.col("ret_1m").std().alias("vol_1m"),
                    (pl.col("close").last() / pl.col("close").first() - 1.0).alias(
                        "drift"
                    ),
                ]
            )
            .sort(["symbol", "day"])
        )
    except Exception as exc:
        raise RuntimeError(f"daily failed: {exc}") from exc
    for sym in symbols:
        sub = days.filter(pl.col("symbol") == sym)
        if len(sub) == 0:
            continue
        try:
            q = sub.select(
                pl.col("vol_1m").quantile(0.33).alias("q33"),
                pl.col("vol_1m").quantile(0.66).alias("q66"),
            ).to_dicts()[0]
        except Exception as exc:
            raise RuntimeError(f"quantile failed: {exc}") from exc
        q33, q66 = q["q33"], q["q66"]
        for r in sub.to_dicts():
            v = r["vol_1m"]
            d = (r["drift"] or 0.0) * 100.0
            if v is None:
                vol = "UNKNOWN"
            elif v <= q33:
                vol = "LOW_VOL"
            elif v <= q66:
                vol = "NORMAL_VOL"
            else:
                vol = "HIGH_VOL"
            if d >= 3.0:
                trend = "STRONG_UP"
            elif d <= -3.0:
                trend = "STRONG_DOWN"
            else:
                trend = "SIDEWAYS"
            out_rows.append(
                {
                    "symbol": sym,
                    "day_ts": r["day"],
                    "close": r["close"],
                    "vol_1m": v,
                    "drift_pp": d,
                    "vol_regime": vol,
                    "trend_regime": trend,
                }
            )
    try:
        pl.DataFrame(out_rows).write_parquet(args.out)
    except Exception as exc:
        raise RuntimeError(f"write failed: {exc}") from exc
    print(f"regimes={len(out_rows)} -> {args.out}")
    m = load_manifest()
    record_file(
        m,
        "binance-vision",
        "market_regimes.parquet",
        args.out,
        [],
        [],
        {"mode": "vol-trend-regimes-1m"},
        rows_before=None,
        rows_after=len(out_rows),
    )
    save_manifest(m)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
