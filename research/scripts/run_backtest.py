"""Thin wrapper: build corpus (optional) then run Rust replay binary."""

import argparse
import subprocess
import sys


def run(cmd: list, step: str) -> None:
    print(f"+ {' '.join(cmd)}")
    try:
        proc = subprocess.run(cmd, capture_output=False, text=True)
    except Exception as exc:
        raise RuntimeError(f"{step} launch failed: {exc}") from exc
    if proc.returncode != 0:
        raise RuntimeError(f"{step} failed rc={proc.returncode}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--skip-etl", action="store_true")
    ap.add_argument("--max-pairs", default="100")
    ap.add_argument("--fill-model", default="CONSERVATIVE")
    ap.add_argument("--latency", default="BASE")
    ap.add_argument("--out", default="research-data/reports/smoke_pairs.json")
    args = ap.parse_args()
    if not args.skip_etl:
        run(
            [sys.executable, "research/scripts/download_metadata.py"],
            "download_metadata",
        )
    run(
        [
            "cargo",
            "run",
            "--quiet",
            "--bin",
            "historical_backtest",
            "--",
            "--max-pairs",
            args.max_pairs,
            "--fill-model",
            args.fill_model,
            "--latency",
            args.latency,
            "--out",
            args.out,
        ],
        "historical_backtest",
    )
    run(
        [
            sys.executable,
            "research/scripts/make_reports.py",
            "--input",
            args.out,
            "--outdir",
            "research-data/reports",
        ],
        "make_reports",
    )
    print("backtest + reports done")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
