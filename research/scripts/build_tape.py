"""Build polymarket_trades + resolutions from daily_aligned (primary)."""

import argparse
import sys

import polars as pl

from common import PROCESSED, ensure_dirs, load_manifest, record_file
from common import save_manifest

YES_LABELS = {"yes", "up"}
NO_LABELS = {"no", "down"}


def yes_price(price: object, label: object) -> tuple:
    try:
        clean = float(price or 0.0)  # type: ignore[truthy-function]
    except (TypeError, ValueError):
        clean = 0.0
    try:
        low = str(label).lower()
    except Exception:
        return clean, False
    if low in YES_LABELS:
        return clean, True
    if low in NO_LABELS:
        return 1.0 - clean, True
    return clean, False


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pattern", default="daily_*.parquet")
    args = ap.parse_args()
    ensure_dirs()
    try:
        sel = pl.scan_parquet(str(PROCESSED / "selected_markets.parquet")).collect()
        wanted = set(sel["condition_id"].to_list())
        slug_of = dict(
            zip(
                sel["condition_id"].to_list(),
                sel["slug"].to_list(),
                strict=True,
            )
        )
    except Exception as exc:
        raise RuntimeError(f"selected read failed: {exc}") from exc
    import pathlib

    files = sorted(
        str(p)
        for p in pathlib.Path("research-data/raw/timeseventeen").glob(args.pattern)
    )
    print(f"daily files: {files}, selectedormats={len(wanted)}")
    frames, resolutions = [], {}
    total_before, total_after = 0, 0
    for path in files:
        try:
            df = pl.scan_parquet(path).collect()
        except Exception as exc:
            print(f"skip {path}: {exc}")
            continue
        total_before += len(df)
        try:
            df = df.filter(pl.col("condition_id").is_in(wanted))
        except Exception as exc:
            print(f"skip filter {path}: {exc}")
            continue
        if len(df) == 0:
            continue
        rows = []
        for r in df.to_dicts():
            try:
                raw_price = float(r.get("price") or 0.0)
            except (TypeError, ValueError):
                raw_price = 0.0
            yp, ok = yes_price(raw_price, r.get("outcome_label"))
            rows.append(
                {
                    "ts": r.get("block_timestamp"),
                    "market_id": slug_of.get(r.get("condition_id"), ""),
                    "condition_id": r.get("condition_id"),
                    "yes_price": yp,
                    "yes_normalized": ok,
                    "original_token": str(r.get("asset_id", "")),
                    "outcome_label": str(r.get("outcome_label", "")),
                    "amount": None,
                    "usd_amount": r.get("usdc_amount"),
                    "maker": str(r.get("maker", "")),
                    "taker": str(r.get("taker", "")),
                    "aggressor": r.get("taker_direction"),
                    "direction_quality": "GROUND_TRUTH",
                    "tx_hash": None,
                    "log_index": None,
                    "source": "TimeSeventeen/Polymarket-v1:" + path.split("/")[-1],
                }
            )
            win = r.get("winning_outcome_label")
            if r.get("resolution_status") == "resolved" and win:
                resolutions[r["condition_id"]] = {
                    "condition_id": r["condition_id"],
                    "winning_outcome": str(win),
                    "resolved_ts": r.get("resolved_at"),
                    "resolution_status": "resolved",
                }
        frames.append(pl.DataFrame(rows))
        total_after += len(rows)
    tape = pl.concat(frames).sort(["ts"]) if frames else pl.DataFrame()
    try:
        tape.write_parquet(str(PROCESSED / "polymarket_trades.parquet"))
        pl.DataFrame(list(resolutions.values())).write_parquet(
            str(PROCESSED / "resolutions.parquet")
        )
    except Exception as exc:
        raise RuntimeError(f"write failed: {exc}") from exc
    print(f"tape rows={len(tape)} resolutions={len(resolutions)}")
    print(tape.group_by(["direction_quality"]).len() if len(tape) else "empty tape")
    m = load_manifest()
    record_file(
        m,
        "TimeSeventeen/Polymarket-v1",
        "daily_aligned selected-days",
        str(PROCESSED / "polymarket_trades.parquet"),
        ["2024-01-01", "2026-04-28"],
        files,
        {"primary": "daily_aligned", "monthly_OrderFilled": "validation-only"},
        rows_before=total_before,
        rows_after=total_after,
        market_count=tape["condition_id"].n_unique() if len(tape) else 0,
    )
    save_manifest(m)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
