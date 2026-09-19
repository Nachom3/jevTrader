"""Selective Polymarket extraction (never full trades/quant)."""

import argparse
import subprocess
import sys

import polars as pl
import yaml

from common import (
    MANIFEST,
    PROCESSED,
    RAW,
    budget_guard,
    ensure_dirs,
    load_manifest,
    record_file,
    save_manifest,
)

TS_BASE = "https://huggingface.co/datasets/TimeSeventeen/Polymarket-v1/resolve/main/"


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


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="research/config/corpus.yaml")
    ap.add_argument("--months", default="2024_01,2024_06")
    args = ap.parse_args()
    cfg = load_cfg(args.config)
    ensure_dirs()
    max_gb = parse_budget(cfg)
    budget_guard(max_gb)
    try:
        max_parts = int(cfg.get("timeseventeen", {}).get("max_monthly_partitions", 6))
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"bad partitions: {exc}") from exc
    months = [m.strip() for m in args.months.split(",") if m.strip()]
    months = months[:max_parts]
    print(f"months (intersect only): {months}")
    print(
        "policy: TimeSeventeen monthly partitions first; "
        "SII trades.parquet only via predicate pushdown"
    )
    got = []
    for m in months:
        url = f"{TS_BASE}OrderFilled/{m}.parquet"
        dest = str(RAW / "timeseventeen" / f"{m}.parquet")
        try:
            exists = RAW.joinpath("timeseventeen", f"{m}.parquet").exists()
        except OSError as exc:
            raise RuntimeError(f"stat failed: {exc}") from exc
        if exists:
            print(f"reuse {dest}")
            got.append(dest)
            continue
        print(f"fetch {url}")
        try:
            fetch_file(url, dest)
        except RuntimeError as exc:
            print(f"skip {m}: {exc}")
            continue
        got.append(dest)
        budget_guard(max_gb)
    # Probe selected-market overlap without loading full tape.
    try:
        sel_path = PROCESSED / "selected_markets.parquet"
        if sel_path.exists():
            sel = pl.scan_parquet(str(sel_path)).select(["condition_id"]).collect()
            print(f"selected markets: {len(sel)}")
    except Exception as exc:
        print(f"warn: overlap probe failed: {exc}")
    m = load_manifest()
    for g in got:
        try:
            import pyarrow.parquet as pq

            rows = pq.ParquetFile(g).metadata.num_rows
        except Exception:
            rows = None
        record_file(
            m,
            "TimeSeventeen/Polymarket-v1",
            g.split("/")[-1],
            g,
            [cfg.get("start_date"), cfg.get("end_date")],
            months,
            {"mode": "monthly-intersect-only"},
            rows_before=rows,
            rows_after=None,
        )
    m["notes"].append("primary source per record required; no silent SII+TS mixing")
    save_manifest(m)
    print(f"done files={len(got)} manifest->{MANIFEST}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
