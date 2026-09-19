"""Binance (+adapter stubs) underlying downloads."""

import argparse
import subprocess
import sys

import yaml

from common import (
    RAW,
    budget_guard,
    ensure_dirs,
    load_manifest,
    record_file,
    save_manifest,
)


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


def fetch_file(url: str, dest: str) -> None:
    if not url.startswith("https://"):
        raise ValueError("only https allowed")
    try:
        proc = subprocess.run(
            ["curl", "-fL", "--max-time", "900", url, "-o", dest],
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


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="research/config/corpus.yaml")
    ap.add_argument("--kinds", default="klines_1m,aggTrades")
    args = ap.parse_args()
    cfg = load_cfg(args.config)
    ensure_dirs()
    max_gb = parse_budget(cfg)
    budget_guard(max_gb)
    und = cfg.get("underlying", {})
    symbols = und.get("symbols", ["BTCUSDT", "ETHUSDT"])
    kinds = [k.strip() for k in args.kinds.split(",")]
    months = months_between(
        cfg.get("start_date", "2024-01-01"), cfg.get("end_date", "2026-04-28")
    )
    sample = months[::3][:6]
    base = "https://data.binance.vision/data/spot"
    got = []
    for sym in symbols:
        for ym in sample:
            if "klines_1m" in kinds:
                url = f"{base}/monthly/klines/{sym}/1m/{sym}-1m-{ym}.zip"
                dest = str(RAW / "binance" / f"{sym}-1m-{ym}.zip")
                try:
                    exists = RAW.joinpath("binance", f"{sym}-1m-{ym}.zip").exists()
                except OSError as exc:
                    raise RuntimeError(f"stat: {exc}") from exc
                if not exists:
                    try:
                        fetch_file(url, dest)
                        got.append(dest)
                    except RuntimeError as exc:
                        print(f"skip {url}: {exc}")
                else:
                    got.append(dest)
                budget_guard(max_gb)
            if "aggTrades" in kinds:
                url = f"{base}/monthly/aggTrades/{sym}/{sym}-aggTrades-{ym}.zip"
                dest = str(RAW / "binance" / f"{sym}-aggTrades-{ym}.zip")
                # Probe size first via curl headers is enough
                # for bootstrap; fetch only one sample month.
                if ym == sample[0]:
                    try:
                        exists = RAW.joinpath(
                            "binance", f"{sym}-aggTrades-{ym}.zip"
                        ).exists()
                    except OSError as exc:
                        raise RuntimeError(f"stat: {exc}") from exc
                    if not exists:
                        try:
                            fetch_file(url, dest)
                            got.append(dest)
                        except RuntimeError as exc:
                            print(f"skip agg {sym} {ym}: {exc}")
                    else:
                        got.append(dest)
                    budget_guard(max_gb)
    cov = "BINANCE_ONLY"
    venues = und.get("venues", ["BINANCE"])
    if venues != ["BINANCE"]:
        cov = "+".join(venues)
    m = load_manifest()
    m["external_source_coverage"] = cov
    m["notes"].append(
        "klines 1m for regimes only; replay needs aggTrades/sub-second, not 1m"
    )
    m["notes"].append("coinbase/deribit adapters prepared; no invented creds")
    for g in got:
        record_file(
            m,
            "binance-vision",
            g.split("/")[-1],
            g,
            sample,
            sample,
            {"kinds": kinds},
            None,
            None,
        )
    save_manifest(m)
    print(f"coverage={cov} files={len(got)}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
