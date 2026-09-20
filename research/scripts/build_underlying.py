"""Build underlying_market_data.parquet from all available Binance aggTrades."""

import argparse
import datetime as dt
import re
import sys
import zipfile
from pathlib import Path

import polars as pl
from common import (
    PROCESSED,
    RAW,
    ensure_dirs,
    load_manifest,
    record_file,
    save_manifest,
)

ARCHIVE_RE = re.compile(
    r"^(?P<symbol>[A-Z]+)-aggTrades-(?P<period>\d{4}-\d{2}(?:-\d{2})?)\.zip$"
)
OUTPUT_SCHEMA = {
    "ts_ms": pl.Int64,
    "asset": pl.String,
    "venue": pl.String,
    "instrument": pl.String,
    "event_type": pl.String,
    "price": pl.Float64,
    "qty": pl.Float64,
    "aggressor_side": pl.String,
    "bid": pl.Float64,
    "ask": pl.Float64,
    "source": pl.String,
}


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--start",
        default=None,
        help="UTC date override; when omitted, derive exact bounds from the tape",
    )
    ap.add_argument(
        "--days",
        default=None,
        help="full UTC days for --start; when omitted with --start, use the tape",
    )
    ap.add_argument("--tape", default=str(PROCESSED / "polymarket_trades.parquet"))
    ap.add_argument("--out", default=str(PROCESSED / "underlying_market_data.parquet"))
    return ap.parse_args()


def day_bounds(start: str, days: str) -> tuple:
    try:
        n = int(days)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"bad days: {exc}") from exc
    if n < 1:
        raise RuntimeError("days must be >= 1")
    try:
        d0 = dt.date.fromisoformat(start)
    except ValueError as exc:
        raise RuntimeError(f"bad start: {exc}") from exc
    try:
        lo = int(
            dt.datetime(d0.year, d0.month, d0.day, tzinfo=dt.timezone.utc).timestamp()
        )
    except (OverflowError, OSError, ValueError) as exc:
        raise RuntimeError(f"bad timestamp: {exc}") from exc
    return lo * 1000, (lo + n * 86400) * 1000


def tape_bounds(path: Path) -> tuple:
    if not path.exists():
        raise RuntimeError(f"tape not found: {path}")
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
        min_ts = bounds["min_ts"][0]
        max_ts = bounds["max_ts"][0]
    except Exception as exc:
        raise RuntimeError(f"tape bounds read failed: {path}: {exc}") from exc
    if min_ts is None or max_ts is None:
        raise RuntimeError(f"tape has no usable ts range: {path}")
    try:
        min_ts = int(min_ts)
        max_ts = int(max_ts)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(
            f"tape ts range is not integer epoch seconds: {exc}"
        ) from exc
    if max_ts < min_ts:
        raise RuntimeError(f"tape ts range is inverted: {min_ts}..{max_ts}")
    if min_ts > 10**12:
        min_ts //= 1000
        max_ts //= 1000
    # Tape ts is second precision; include every underlying millisecond in the
    # final tape second by making the upper bound exclusive.
    return min_ts * 1000, (max_ts + 1) * 1000


def resolve_window(args: argparse.Namespace) -> tuple[tuple[int, int], str]:
    if args.start is None and args.days is None:
        return tape_bounds(Path(args.tape)), f"tape:{args.tape}"
    if args.start is None or args.days is None:
        raise RuntimeError("--start and --days must be supplied together")
    return day_bounds(args.start, args.days), "cli:start-days"


def to_ms(raw: int) -> int:
    try:
        v = int(raw)
    except (TypeError, ValueError):
        return -1
    if v > 1_000_000_000_000_000:
        return v // 1000
    return v


def parse_archive_period(path: Path, symbol: str) -> tuple[dt.date, dt.date] | None:
    match = ARCHIVE_RE.fullmatch(path.name)
    if match is None or match.group("symbol") != symbol:
        return None
    period = match.group("period")
    try:
        if len(period) == 10:
            day = dt.date.fromisoformat(period)
            return day, day
        year, month = (int(part) for part in period.split("-"))
        start = dt.date(year, month, 1)
        if month == 12:
            end = dt.date(year + 1, 1, 1) - dt.timedelta(days=1)
        else:
            end = dt.date(year, month + 1, 1) - dt.timedelta(days=1)
        return start, end
    except (TypeError, ValueError):
        return None


def agg_archives(symbol: str, lo_ms: int, hi_ms: int) -> list[Path]:
    lo_day = dt.datetime.fromtimestamp(lo_ms / 1000, tz=dt.timezone.utc).date()
    hi_day = dt.datetime.fromtimestamp((hi_ms - 1) / 1000, tz=dt.timezone.utc).date()
    paths: list[Path] = []
    try:
        candidates = sorted((RAW / "binance").glob(f"{symbol}-aggTrades-*.zip"))
    except OSError as exc:
        raise RuntimeError(f"glob failed: {exc}") from exc
    for path in candidates:
        period = parse_archive_period(path, symbol)
        if period is None:
            continue
        start, end = period
        if start <= hi_day and end >= lo_day:
            paths.append(path)
    return paths


def read_agg(path: str, lo_ms: int, hi_ms: int, asset: str) -> pl.DataFrame:
    try:
        zf = zipfile.ZipFile(path)
    except Exception as exc:
        print(f"GAP archive={path}: {exc}")
        return pl.DataFrame()
    try:
        members = [name for name in zf.namelist() if not name.endswith("/")]
        if not members:
            print(f"GAP archive={path}: no files in zip")
            return pl.DataFrame()
        with zf.open(members[0]) as f:
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
        print(f"GAP collect={path}: {exc}")
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
                pl.col("agg_id").cast(pl.Int64, strict=False),
                pl.col("price").cast(pl.Float64, strict=False),
                pl.col("qty").cast(pl.Float64, strict=False),
            ]
        ).filter(
            (pl.col("ts_ms") >= lo_ms)
            & (pl.col("ts_ms") < hi_ms)
            & pl.col("agg_id").is_not_null()
            & pl.col("price").is_not_null()
            & pl.col("qty").is_not_null()
        )
        if len(df) == 0:
            return df
        buyer_maker = pl.col("buyer_maker").cast(pl.String).str.to_lowercase()
        return df.select(
            [
                pl.col("agg_id"),
                pl.col("ts_ms"),
                pl.lit(asset).alias("asset"),
                pl.lit("BINANCE").alias("venue"),
                pl.lit("spot").alias("instrument"),
                pl.lit("aggTrade").alias("event_type"),
                pl.col("price"),
                pl.col("qty"),
                pl.when(buyer_maker.is_in(["true", "1"]))
                .then(pl.lit("SELL"))
                .otherwise(pl.lit("BUY"))
                .alias("aggressor_side"),
                pl.lit(None, dtype=pl.Float64).alias("bid"),
                pl.lit(None, dtype=pl.Float64).alias("ask"),
                pl.lit("binance-vision").alias("source"),
            ]
        )
    except Exception as exc:
        print(f"GAP normalize={path}: {exc}")
        return pl.DataFrame()


def frame_days(df: pl.DataFrame) -> set[dt.date]:
    if len(df) == 0:
        return set()
    try:
        return set(
            df.select(pl.col("ts_ms").cast(pl.Datetime("ms")).dt.date().alias("day"))[
                "day"
            ].to_list()
        )
    except Exception as exc:
        print(f"warn coverage date extraction failed: {exc}")
        return set()


def empty_output() -> pl.DataFrame:
    return pl.DataFrame(schema=OUTPUT_SCHEMA)


def append_note(m: dict, note: str) -> None:
    notes = m.setdefault("notes", [])
    if note not in notes:
        notes.append(note)


def iso_utc(ms: int) -> str:
    return dt.datetime.fromtimestamp(ms / 1000, tz=dt.timezone.utc).isoformat()


def main() -> None:
    args = parse_args()
    ensure_dirs()
    (lo_ms, hi_ms), window_source = resolve_window(args)
    lo_day = dt.datetime.fromtimestamp(lo_ms / 1000, tz=dt.timezone.utc).date()
    hi_day = dt.datetime.fromtimestamp((hi_ms - 1) / 1000, tz=dt.timezone.utc).date()
    frames = []
    symbol_coverage: dict[str, dict] = {}
    for asset, sym in [("BTC", "BTCUSDT"), ("ETH", "ETHUSDT")]:
        paths = agg_archives(sym, lo_ms, hi_ms)
        symbol_frames = []
        for path in paths:
            df = read_agg(str(path), lo_ms, hi_ms, asset)
            if len(df) > 0:
                symbol_frames.append(df)
        if symbol_frames:
            symbol_df = pl.concat(symbol_frames, how="vertical_relaxed")
            # Monthly fallback and daily primary archives can overlap. Binance
            # aggTrade IDs are unique per symbol, so dedupe without inventing data.
            symbol_df = symbol_df.unique(subset=["asset", "agg_id"], keep="first")
            frames.append(symbol_df)
        else:
            symbol_df = pl.DataFrame()
        actual_days = frame_days(symbol_df)
        requested_days = {
            lo_day + dt.timedelta(days=offset)
            for offset in range((hi_day - lo_day).days + 1)
        }
        gaps = sorted(requested_days - actual_days)
        for day in gaps:
            print(f"GAP symbol={sym} day={day.isoformat()} no aggTrades rows")
        print(
            f"{sym}: archives={len(paths)} ticks={len(symbol_df)} "
            f"covered_days={len(actual_days)}/{len(requested_days)} gaps={len(gaps)}"
        )
        symbol_coverage[sym] = {
            "archives": [path.name for path in paths],
            "requested_days": [day.isoformat() for day in sorted(requested_days)],
            "covered_days": [day.isoformat() for day in sorted(actual_days)],
            "gaps": [day.isoformat() for day in gaps],
            "rows": len(symbol_df),
        }

    if frames:
        out = pl.concat(frames, how="vertical_relaxed")
        out = out.unique(subset=["asset", "agg_id"], keep="first").drop("agg_id")
        out = out.select(list(OUTPUT_SCHEMA)).sort(["ts_ms", "asset"])
    else:
        out = empty_output()
    try:
        out.write_parquet(args.out)
    except Exception as exc:
        raise RuntimeError(f"write failed: {exc}") from exc
    print(f"underlying rows={len(out)} -> {args.out}")
    m = load_manifest()
    coverage = {
        "window_source": window_source,
        "start_ts_ms": lo_ms,
        "end_ts_ms_exclusive": hi_ms,
        "start_utc": iso_utc(lo_ms),
        "end_utc_exclusive": iso_utc(hi_ms),
        "symbols": symbol_coverage,
        "rows": len(out),
        "no_synthetic_ticks": True,
    }
    m["underlying_coverage"] = coverage
    append_note(
        m,
        "underlying builder reads every aggTrades archive overlapping the requested window, filters exact timestamps, and records missing symbol/day gaps without synthetic ticks",
    )
    record_file(
        m,
        "binance-vision",
        "underlying_market_data.parquet",
        args.out,
        [lo_day.isoformat(), hi_day.isoformat()],
        [iso_utc(lo_ms), iso_utc(hi_ms)],
        {
            "mode": "aggtrades-subsecond-window",
            "window_source": window_source,
            "coverage_gaps": {
                sym: details["gaps"] for sym, details in symbol_coverage.items()
            },
        },
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
