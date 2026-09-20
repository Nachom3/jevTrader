"""Download Binance underlying data aligned to the processed Polymarket tape."""

import argparse
import datetime as dt
import subprocess
import sys
import zipfile
from pathlib import Path

import polars as pl
import yaml
from common import (
    PROCESSED,
    RAW,
    budget_guard,
    ensure_dirs,
    load_manifest,
    record_file,
    save_manifest,
)

DAY = dt.timedelta(days=1)


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
            [
                "curl",
                "-fL",
                "--remove-on-error",
                "--max-time",
                "900",
                url,
                "-o",
                dest,
            ],
            capture_output=True,
            text=True,
            check=False,
        )
    except Exception as exc:
        raise RuntimeError(f"curl failed: {exc}") from exc
    if proc.returncode != 0:
        raise RuntimeError(f"curl error: {proc.stderr[-1000:]}")


def months_between(start: str, end: str):
    """Return inclusive YYYY-MM values, retained for CLI/test compatibility."""
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


def date_months(start: dt.date, end: dt.date) -> list[str]:
    return months_between(start.isoformat(), end.isoformat())


def iter_days(start: dt.date, end: dt.date):
    day = start
    while day <= end:
        yield day
        day += DAY


def tape_window(path: Path) -> dict:
    """Read the exact epoch-second window from the normalized Polymarket tape."""
    if not path.exists():
        raise RuntimeError(
            f"tape not found: {path}; refusing to use config dates for underlying"
        )
    try:
        bounds = (
            pl.scan_parquet(str(path))
            .select(
                [
                    pl.col("ts").min().alias("min_ts"),
                    pl.col("ts").max().alias("max_ts"),
                ]
            )
            .collect()
        )
    except Exception as exc:
        raise RuntimeError(f"tape bounds read failed: {path}: {exc}") from exc
    if len(bounds) != 1 or bounds["min_ts"][0] is None or bounds["max_ts"][0] is None:
        raise RuntimeError(f"tape has no usable ts range: {path}")
    try:
        min_ts = int(bounds["min_ts"][0])
        max_ts = int(bounds["max_ts"][0])
    except (TypeError, ValueError) as exc:
        raise RuntimeError(
            f"tape ts range is not integer epoch seconds: {exc}"
        ) from exc
    if max_ts < min_ts:
        raise RuntimeError(f"tape ts range is inverted: {min_ts}..{max_ts}")
    # The normalized tape stores seconds. Accept millisecond input only when it is
    # unambiguous, so a bad unit cannot silently select the wrong years.
    if min_ts > 10**12:
        min_ts //= 1000
        max_ts //= 1000
    try:
        start = dt.datetime.fromtimestamp(min_ts, tz=dt.timezone.utc).date()
        end = dt.datetime.fromtimestamp(max_ts, tz=dt.timezone.utc).date()
    except (OverflowError, OSError, ValueError) as exc:
        raise RuntimeError(f"tape ts range cannot become UTC dates: {exc}") from exc
    return {
        "source": str(path),
        "min_ts": min_ts,
        "max_ts": max_ts,
        "start": start,
        "end": end,
    }


def archive_exists(path: Path) -> bool:
    try:
        return path.is_file() and path.stat().st_size > 0 and zipfile.is_zipfile(path)
    except OSError as exc:
        raise RuntimeError(f"stat failed for {path}: {exc}") from exc


def ensure_archive(url: str, dest: Path, max_gb: float) -> tuple[bool, str]:
    """Ensure one archive exists and return (available, downloaded|existing|missing)."""
    if archive_exists(dest):
        return True, "existing"
    try:
        fetch_file(url, str(dest))
    except RuntimeError as exc:
        print(f"skip {url}: {exc}")
        return False, "missing"
    budget_guard(max_gb)
    return True, "downloaded"


def iso_day(day: dt.date) -> str:
    return day.isoformat()


def month_start(month: str) -> dt.date:
    year, number = (int(part) for part in month.split("-"))
    return dt.date(year, number, 1)


def month_end(month: str) -> dt.date:
    start = month_start(month)
    if start.month == 12:
        return dt.date(start.year + 1, 1, 1) - DAY
    return dt.date(start.year, start.month + 1, 1) - DAY


def append_note(m: dict, note: str) -> None:
    notes = m.setdefault("notes", [])
    if note not in notes:
        notes.append(note)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="research/config/corpus.yaml")
    ap.add_argument("--tape", default=str(PROCESSED / "polymarket_trades.parquet"))
    ap.add_argument("--kinds", default="klines_1m,aggTrades")
    args = ap.parse_args()
    cfg = load_cfg(args.config)
    ensure_dirs()
    max_gb = parse_budget(cfg)
    budget_guard(max_gb)
    und = cfg.get("underlying", {})
    symbols = und.get("symbols", ["BTCUSDT", "ETHUSDT"])
    kinds = [k.strip() for k in args.kinds.split(",") if k.strip()]
    window = tape_window(Path(args.tape))
    days = list(iter_days(window["start"], window["end"]))
    months = date_months(window["start"], window["end"])
    base = "https://data.binance.vision/data/spot"
    binance_dir = RAW / "binance"
    got: list[dict] = []
    registered: set[Path] = set()
    coverage: dict[str, dict] = {}

    def register(path: Path, symbol: str, kind: str, mode: str, start, end) -> None:
        if path in registered:
            return
        registered.add(path)
        got.append(
            {
                "path": path,
                "symbol": symbol,
                "kind": kind,
                "mode": mode,
                "start": start,
                "end": end,
            }
        )

    # Daily aggTrades are the primary source. A monthly archive is fetched only
    # for months containing a failed daily request, so gaps remain explicit.
    if "aggTrades" in kinds:
        for sym in symbols:
            daily_ok: list[str] = []
            daily_missing: list[str] = []
            fallback_months: list[str] = []
            archive_gaps: list[str] = []
            for day in days:
                day_s = iso_day(day)
                filename = f"{sym}-aggTrades-{day_s}.zip"
                dest = binance_dir / filename
                url = f"{base}/daily/aggTrades/{sym}/{filename}"
                available, _ = ensure_archive(url, dest, max_gb)
                if available:
                    daily_ok.append(day_s)
                    register(dest, sym, "aggTrades", "daily", day, day)
                else:
                    daily_missing.append(day_s)
                budget_guard(max_gb)

            missing_by_month: dict[str, list[dt.date]] = {}
            for day_s in daily_missing:
                day = dt.date.fromisoformat(day_s)
                missing_by_month.setdefault(day_s[:7], []).append(day)
            for month, missing_days in sorted(missing_by_month.items()):
                filename = f"{sym}-aggTrades-{month}.zip"
                dest = binance_dir / filename
                url = f"{base}/monthly/aggTrades/{sym}/{filename}"
                available, _ = ensure_archive(url, dest, max_gb)
                if available:
                    fallback_months.append(month)
                    register(
                        dest,
                        sym,
                        "aggTrades",
                        "monthly-fallback",
                        month_start(month),
                        month_end(month),
                    )
                else:
                    archive_gaps.extend(iso_day(day) for day in missing_days)
                budget_guard(max_gb)

            coverage[sym] = {
                "requested_days": [iso_day(day) for day in days],
                "daily_days": daily_ok,
                "daily_missing": daily_missing,
                "monthly_fallback_months": fallback_months,
                "archive_gaps": archive_gaps,
                "archive_coverage_pct": (
                    100.0 * (len(days) - len(archive_gaps)) / len(days) if days else 0.0
                ),
            }

    # 1m klines remain monthly regime/long-vol context only. They are not used
    # as the event-level underlying tape and are never fetched daily.
    if "klines_1m" in kinds:
        for sym in symbols:
            for ym in months:
                filename = f"{sym}-1m-{ym}.zip"
                dest = binance_dir / filename
                url = f"{base}/monthly/klines/{sym}/1m/{filename}"
                available, _ = ensure_archive(url, dest, max_gb)
                if available:
                    register(
                        dest,
                        sym,
                        "klines_1m",
                        "monthly-regime",
                        month_start(ym),
                        month_end(ym),
                    )
                budget_guard(max_gb)

    cov = "BINANCE_ONLY"
    venues = und.get("venues", ["BINANCE"])
    if venues != ["BINANCE"]:
        cov = "+".join(venues)
    m = load_manifest()
    m["external_source_coverage"] = cov
    m["underlying_download_window"] = {
        "source": window["source"],
        "min_ts": window["min_ts"],
        "max_ts": window["max_ts"],
        "start_utc": iso_day(window["start"]),
        "end_utc": iso_day(window["end"]),
        "date_semantics": "inclusive UTC archive dates; builder applies exact tape ts filter",
    }
    m["underlying_download_coverage"] = {
        "symbols": coverage,
        "kinds": kinds,
        "daily_aggTrades_preferred": True,
        "monthly_aggTrades_fallback_only_for_daily_gaps": True,
    }
    append_note(
        m,
        "underlying aggTrades window is derived from polymarket_trades.parquet min/max ts; daily Binance Vision archives preferred, monthly only for daily gaps",
    )
    append_note(
        m,
        "klines 1m are monthly regime/long-vol context only; replay alignment uses aggTrades",
    )
    append_note(
        m, "coverage gaps are recorded in manifest; no underlying ticks are invented"
    )
    append_note(m, "coinbase/deribit adapters prepared; no invented creds")
    for item in got:
        record_file(
            m,
            "binance-vision",
            item["path"].name,
            str(item["path"]),
            [iso_day(item["start"]), iso_day(item["end"])],
            [iso_day(window["start"]), iso_day(window["end"])],
            {
                "kind": item["kind"],
                "mode": item["mode"],
                "window_source": window["source"],
            },
            None,
            None,
        )
    save_manifest(m)
    print(
        f"coverage={cov} tape_window={window['min_ts']}..{window['max_ts']} "
        f"days={len(days)} files={len(got)} gaps="
        f"{sum(len(v['archive_gaps']) for v in coverage.values())}"
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
