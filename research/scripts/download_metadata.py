"""Download small metadata only (markets.parquet allowed)."""

import argparse
import subprocess
import sys

import yaml

from common import (
    MANIFEST,
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


def fetch_small(url: str, dest: str) -> None:
    if not url.startswith("https://"):
        raise ValueError("only https allowed")
    try:
        proc = subprocess.run(
            ["curl", "-fL", "--max-time", "600", url, "-o", dest],
            capture_output=True,
            text=True,
            check=False,
        )
    except Exception as exc:
        raise RuntimeError(f"curl failed: {exc}") from exc
    if proc.returncode != 0:
        raise RuntimeError(f"curl error: {proc.stderr[-2000:]}")


def parse_budget(cfg: dict) -> float:
    try:
        return float(cfg.get("research_max_download_gb", 8.0))
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"bad budget value: {exc}") from exc


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="research/config/corpus.yaml")
    args = ap.parse_args()
    cfg = load_cfg(args.config)
    ensure_dirs()
    max_gb = parse_budget(cfg)
    budget_guard(max_gb)
    dest = RAW / "sii" / "markets.parquet"
    url = (
        "https://huggingface.co/datasets/"
        "SII-WANGZJ/Polymarket_data/resolve/main/"
        "markets.parquet"
    )
    try:
        exists = dest.exists() and dest.stat().st_size > 0
    except OSError as exc:
        raise RuntimeError(f"stat failed: {exc}") from exc
    if exists:
        try:
            size = dest.stat().st_size
        except OSError as exc:
            raise RuntimeError(f"stat failed: {exc}") from exc
        print(f"reuse {dest} ({size} bytes)")
    else:
        print(f"fetch {url} -> {dest} (~294MB)")
        fetch_small(url, str(dest))
    budget_guard(max_gb)
    rows = None
    try:
        import pyarrow.parquet as pq

        pf = pq.ParquetFile(str(dest))
        print(f"rows={pf.metadata.num_rows} cols={pf.metadata.num_columns}")
        rows = pf.metadata.num_rows
    except Exception as exc:
        print(f"warn: parquet probe failed: {exc}")
    m = load_manifest()
    record_file(
        m,
        "SII-WANGZJ/Polymarket_data",
        "markets.parquet",
        str(dest),
        [cfg.get("start_date"), cfg.get("end_date")],
        [cfg.get("start_date"), cfg.get("end_date")],
        {"mode": "metadata-full-allowed"},
        rows_before=rows,
        rows_after=rows,
    )
    m["notes"].append("markets.parquet full allowed; trades/quant selective only")
    save_manifest(m)
    print(f"manifest -> {MANIFEST}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
