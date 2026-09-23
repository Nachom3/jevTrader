"""Build an exact YES-side 5m tape from all local Kachoio tick dates.

The row construction below is copied and adapted from the frozen
``build_kachoio_tape.py`` pipeline. It intentionally does not infer prices,
fill missing ticks, or call remote services.
"""

import json
import math
import shutil
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[2]
RAW = ROOT / "research-data" / "raw" / "kaggle-kachoio"
PROCESSED = ROOT / "research-data" / "processed"
SELECTED = PROCESSED / "selected_markets.parquet"
BASELINE = PROCESSED / "kachoio_polytop.parquet"
OUTPUT = PROCESSED / "kachoio_polytop_multiday.parquet"
MANIFEST = PROCESSED / "kachoio_multiday.manifest.json"
MIN_FREE_BYTES = 5 * 1024**3
WINDOW_SECONDS = 5 * 60
ASSETS = ("BTC", "ETH")
TICK_COLUMNS = ["condition_id", "t", "bu", "au", "su", "sau", "sad", "du", "dd"]

# This is the frozen single-day schema with only the requested UTC date field
# appended. The source/fidelity and YES-side row construction match the original.
BASE_SCHEMA = pa.schema(
    [
        ("ts_ms", pa.int64()),
        ("condition_id", pa.string()),
        ("asset", pa.string()),
        ("horizon", pa.string()),
        ("yes_bid", pa.float64()),
        ("yes_ask", pa.float64()),
        ("bid_size", pa.float64()),
        ("ask_size", pa.float64()),
        ("bid_depth_5c", pa.float64()),
        ("ask_depth", pa.float64()),
        ("has_ask_depth", pa.bool_()),
        ("spread", pa.float64()),
        ("mid", pa.float64()),
        ("source", pa.string()),
        ("fidelity", pa.string()),
    ]
)
OUTPUT_SCHEMA = pa.schema([*BASE_SCHEMA, ("date", pa.string())])


@dataclass(frozen=True)
class Market:
    condition_id: str
    slug: str
    asset: str
    start_s: int
    end_s: int

    @property
    def market_date(self) -> str:
        return datetime.fromtimestamp(self.start_s, tz=timezone.utc).date().isoformat()


@dataclass
class MarketStats:
    seen: bytearray = field(default_factory=lambda: bytearray((WINDOW_SECONDS + 7) // 8))
    in_window_rows: int = 0
    null_quote_rows: int = 0
    out_of_window_rows: int = 0


@dataclass
class DayAssetCounts:
    raw_tick_rows: int = 0
    candidate_markets: int = 0
    kept_markets: int = 0
    dropped_markets: int = 0
    matching_market_ticks: int = 0
    dropped_out_of_window: int = 0
    dropped_null_quotes: int = 0
    dropped_incomplete_market: int = 0
    null_quotes_in_incomplete_markets: int = 0
    kept_rows: int = 0
    drop_reasons: Counter = field(default_factory=Counter)


def is_missing(value: object) -> bool:
    return value is None or (isinstance(value, float) and math.isnan(value))


def as_float(value: object) -> float | None:
    return None if is_missing(value) else float(str(value))


def first_available(primary: object, fallback: object) -> float | None:
    """Use the preferred same-row field, then its paired field if available."""
    return as_float(primary) if not is_missing(primary) else as_float(fallback)


def parse_epoch(value: object) -> int:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        raise ValueError(f"expected UTC timestamp, got {value!r}")
    if parsed.tzinfo is None:
        raise ValueError(f"timestamp is timezone-naive: {value!r}")
    return int(parsed.timestamp())


def day_counts() -> dict[str, dict[str, DayAssetCounts]]:
    return defaultdict(lambda: defaultdict(DayAssetCounts))


def survey_tick_dates() -> tuple[dict[str, Counter], dict[str, set[str]]]:
    """Survey every local tick timestamp and condition ID before conversion."""
    dates: dict[str, Counter] = {}
    condition_ids: dict[str, set[str]] = {}
    for asset in ASSETS:
        path = RAW / f"{asset.lower()}_ticks.parquet"
        dataset = ds.dataset(path, format="parquet")
        by_date: Counter = Counter()
        ids: set[str] = set()
        for batch in dataset.to_batches(columns=["ts_utc", "condition_id"], batch_size=250_000):
            timestamps = batch.column("ts_utc").to_pylist()
            by_date.update(ts.astimezone(timezone.utc).date().isoformat() for ts in timestamps)
            ids.update(str(value) for value in batch.column("condition_id").to_pylist())
        dates[asset] = by_date
        condition_ids[asset] = ids
        print(
            f"tick_survey asset={asset} rows={sum(by_date.values())} "
            f"dates={len(by_date)} first={min(by_date)} last={max(by_date)}"
        )
    return dates, condition_ids


def load_targets() -> tuple[dict[str, dict[str, Market]], dict[str, int]]:
    targets: dict[str, dict[str, Market]] = {asset: {} for asset in ASSETS}
    for asset in ASSETS:
        path = RAW / f"{asset.lower()}_markets.parquet"
        rows = pq.read_table(
            path, columns=["condition_id", "slug", "market_start", "market_end"]
        ).to_pylist()
        expected_prefix = f"{asset.lower()}-updown-5m-"
        for row in rows:
            slug = str(row["slug"])
            start_s = parse_epoch(row["market_start"])
            end_s = parse_epoch(row["market_end"])
            if not slug.startswith(expected_prefix) or end_s - start_s != WINDOW_SECONDS:
                raise RuntimeError(
                    f"unexpected local market metadata for {asset}: "
                    f"slug={slug!r}, duration={end_s - start_s}"
                )
            condition_id = str(row["condition_id"])
            if condition_id in targets[asset]:
                raise RuntimeError(f"duplicate {asset} condition_id in market metadata: {condition_id}")
            targets[asset][condition_id] = Market(
                condition_id=condition_id,
                slug=slug,
                asset=asset,
                start_s=start_s,
                end_s=end_s,
            )
    return targets, {asset: len(markets) for asset, markets in targets.items()}


def assess_15m_support(
    targets: dict[str, dict[str, Market]], tick_ids: dict[str, set[str]]
) -> dict:
    """Use explicit selected-market horizon labels; never infer a 15m market."""
    selected = pq.read_table(SELECTED, columns=["condition_id", "asset", "horizon"]).to_pylist()
    result = {}
    for asset in ASSETS:
        selected_ids = {
            str(row["condition_id"])
            for row in selected
            if row["asset"] == asset and row["horizon"] == "15m"
        }
        market_ids = set(targets[asset])
        metadata_matches = selected_ids & market_ids
        tick_matches = selected_ids & tick_ids[asset]
        result[asset] = {
            "selected_15m_markets": len(selected_ids),
            "matched_local_market_metadata": len(metadata_matches),
            "matched_local_tick_condition_ids": len(tick_matches),
            "included": False,
            "reason": (
                "not included: no explicitly labeled selected 15m condition_id exists in the "
                "local Kachoio 5m market metadata or ticks; local markets have 300-second "
                "windows, so a 15m tape cannot be built without new inference"
            ),
        }
    return result


def tick_batches(asset: str, markets: dict[str, Market], columns: list[str]):
    path = RAW / f"{asset.lower()}_ticks.parquet"
    dataset = ds.dataset(path, format="parquet")
    start = datetime.fromtimestamp(min(m.start_s for m in markets.values()), tz=timezone.utc)
    end = datetime.fromtimestamp(max(m.end_s for m in markets.values()), tz=timezone.utc)
    yield from dataset.to_batches(
        columns=columns,
        filter=(ds.field("ts_utc") >= start) & (ds.field("ts_utc") < end),
        batch_size=100_000,
    )


def add_market_counts(targets: dict[str, dict[str, Market]], days) -> None:
    for asset, markets in targets.items():
        for market in markets.values():
            days[market.market_date][asset].candidate_markets += 1


def survey_market_ticks(
    targets: dict[str, dict[str, Market]], days
) -> tuple[dict[str, set[str]], dict[str, int]]:
    """Validate exact one-second windows with compact per-market bitmaps."""
    valid_ids: dict[str, set[str]] = {}
    totals = Counter()
    for asset, markets in targets.items():
        stats = {condition_id: MarketStats() for condition_id in markets}
        for batch in tick_batches(asset, markets, [*TICK_COLUMNS, "ts_utc"]):
            table = pa.Table.from_batches([batch])
            output_columns = table.select(TICK_COLUMNS).to_pylist()
            timestamps = table["ts_utc"].to_pylist()
            for row, timestamp in zip(output_columns, timestamps, strict=True):
                condition_id = str(row["condition_id"])
                market = markets.get(condition_id)
                if market is None:
                    continue
                counts = days[timestamp.astimezone(timezone.utc).date().isoformat()][asset]
                counts.matching_market_ticks += 1
                market_stats = stats[condition_id]
                tick_s = int(row["t"])
                if not market.start_s <= tick_s < market.end_s:
                    market_stats.out_of_window_rows += 1
                    counts.dropped_out_of_window += 1
                    continue
                offset = tick_s - market.start_s
                byte_index, bit_index = divmod(offset, 8)
                mask = 1 << bit_index
                if market_stats.seen[byte_index] & mask:
                    raise RuntimeError(f"duplicate in-window timestamp for {market.slug}: {tick_s}")
                market_stats.seen[byte_index] |= mask
                market_stats.in_window_rows += 1
                if is_missing(row["bu"]) or is_missing(row["au"]):
                    market_stats.null_quote_rows += 1

        valid: set[str] = set()
        for condition_id, market in markets.items():
            market_stats = stats[condition_id]
            counts = days[market.market_date][asset]
            expected = (1 << WINDOW_SECONDS) - 1
            actual = sum(byte << (8 * index) for index, byte in enumerate(market_stats.seen))
            if market_stats.in_window_rows == 0:
                counts.dropped_markets += 1
                counts.drop_reasons["no_in_window_ticks"] += 1
                totals["markets_dropped_no_in_window_ticks"] += 1
                continue
            if actual != expected:
                counts.dropped_markets += 1
                counts.dropped_incomplete_market += market_stats.in_window_rows
                counts.null_quotes_in_incomplete_markets += market_stats.null_quote_rows
                counts.drop_reasons["incomplete_1s_tick_window"] += 1
                totals["markets_dropped_incomplete_1s_window"] += 1
                totals["rows_dropped_incomplete_market"] += market_stats.in_window_rows
                continue
            valid.add(condition_id)
            counts.kept_markets += 1
            counts.dropped_null_quotes += market_stats.null_quote_rows
            counts.drop_reasons["null_yes_bid_or_ask"] += market_stats.null_quote_rows
            totals["rows_dropped_null_quotes"] += market_stats.null_quote_rows
            totals["expected_rows_kept"] += (
                market_stats.in_window_rows - market_stats.null_quote_rows
            )
        valid_ids[asset] = valid
        totals[f"{asset.lower()}_candidate_markets"] = len(markets)
        totals[f"{asset.lower()}_kept_markets"] = len(valid)
        print(
            f"market_survey asset={asset} candidates={len(markets)} "
            f"kept={len(valid)} dropped={len(markets) - len(valid)}"
        )
    return valid_ids, totals


def write_tape(
    targets: dict[str, dict[str, Market]], valid_ids: dict[str, set[str]], days
) -> int:
    written_rows = 0
    writer = pq.ParquetWriter(OUTPUT, OUTPUT_SCHEMA, compression="zstd")
    try:
        for asset in ASSETS:
            markets = targets[asset]
            for batch in tick_batches(asset, markets, [*TICK_COLUMNS, "ts_utc"]):
                table = pa.Table.from_batches([batch])
                rows = table.select(TICK_COLUMNS).to_pylist()
                output_rows = []
                for row in rows:
                    condition_id = str(row["condition_id"])
                    if condition_id not in valid_ids[asset]:
                        continue
                    market = markets[condition_id]
                    tick_s = int(row["t"])
                    if not market.start_s <= tick_s < market.end_s:
                        continue
                    if is_missing(row["bu"]) or is_missing(row["au"]):
                        continue
                    yes_bid, yes_ask = float(row["bu"]), float(row["au"])
                    tick_date = datetime.fromtimestamp(tick_s, tz=timezone.utc).date().isoformat()
                    output_rows.append(
                        {
                            "ts_ms": tick_s * 1000,
                            "condition_id": market.condition_id,
                            "asset": asset,
                            "horizon": "5m",
                            "yes_bid": yes_bid,
                            "yes_ask": yes_ask,
                            "bid_size": as_float(row["su"]),
                            "ask_size": first_available(row["sau"], row["sad"]),
                            "bid_depth_5c": first_available(row["du"], row["dd"]),
                            "ask_depth": None,
                            "has_ask_depth": False,
                            "spread": yes_ask - yes_bid,
                            "mid": (yes_bid + yes_ask) / 2.0,
                            "source": "kachoio-v1",
                            "fidelity": "EXACT",
                            "date": tick_date,
                        }
                    )
                    days[tick_date][asset].kept_rows += 1
                if output_rows:
                    writer.write_table(pa.Table.from_pylist(output_rows, schema=OUTPUT_SCHEMA))
                    written_rows += len(output_rows)
    finally:
        writer.close()
    return written_rows


def serializable_days(days) -> dict:
    serialized = {}
    for day in sorted(days):
        assets = {}
        for asset in ASSETS:
            counts = days[day][asset]
            assets[asset] = {
                "markets": {
                    "candidate": counts.candidate_markets,
                    "kept": counts.kept_markets,
                    "dropped": counts.dropped_markets,
                    "drop_reasons": dict(sorted(counts.drop_reasons.items())),
                },
                "rows": {
                    "raw_tick_rows": counts.raw_tick_rows,
                    "matching_market_ticks": counts.matching_market_ticks,
                    "kept": counts.kept_rows,
                    "dropped_null_quotes": counts.dropped_null_quotes,
                    "dropped_out_of_window": counts.dropped_out_of_window,
                    "dropped_incomplete_market": counts.dropped_incomplete_market,
                    "null_quotes_in_incomplete_markets": counts.null_quotes_in_incomplete_markets,
                },
            }
        serialized[day] = assets
    return serialized


def main() -> None:
    required = [
        SELECTED,
        BASELINE,
        RAW / "btc_ticks.parquet",
        RAW / "eth_ticks.parquet",
        RAW / "btc_markets.parquet",
        RAW / "eth_markets.parquet",
    ]
    missing = [str(path) for path in required if not path.exists()]
    if missing:
        raise FileNotFoundError("missing inputs: " + ", ".join(missing))
    free_bytes = shutil.disk_usage(PROCESSED).free
    if free_bytes < MIN_FREE_BYTES:
        raise RuntimeError(f"refusing ETL: only {free_bytes} free bytes; need 5 GiB")
    baseline_schema = pq.read_schema(BASELINE)
    if not baseline_schema.equals(BASE_SCHEMA, check_metadata=True):
        raise RuntimeError(f"frozen tape schema changed:\n{baseline_schema}")

    # Survey local tick availability before selecting or writing any tape rows.
    tick_dates, tick_ids = survey_tick_dates()
    targets, candidate_counts = load_targets()
    unsupported_15m = assess_15m_support(targets, tick_ids)
    days = day_counts()
    for asset in ASSETS:
        for day, row_count in tick_dates[asset].items():
            days[day][asset].raw_tick_rows = row_count
    add_market_counts(targets, days)

    valid_ids, market_totals = survey_market_ticks(targets, days)
    expected_kept_rows = market_totals["expected_rows_kept"]

    # The pass above determines exactly which complete market windows are safe
    # to retain. The write pass reuses the frozen row transformation unchanged.
    actual_kept_rows = write_tape(targets, valid_ids, days)
    readback = pq.read_table(OUTPUT)
    readback_base_schema = pa.schema(list(readback.schema)[:-1])
    if not readback_base_schema.equals(BASE_SCHEMA, check_metadata=True):
        raise RuntimeError(f"output base schema differs from frozen tape:\n{readback.schema}")
    if readback.schema != OUTPUT_SCHEMA:
        raise RuntimeError(f"output schema mismatch:\n{readback.schema}")
    if readback.num_rows != actual_kept_rows:
        raise RuntimeError("output row count differs from ETL count")
    if actual_kept_rows != expected_kept_rows:
        raise RuntimeError(
            f"write count differs from validated rows: {actual_kept_rows} != {expected_kept_rows}"
        )

    per_day = serializable_days(days)
    totals = {
        "candidate_markets": sum(candidate_counts.values()),
        "kept_markets": sum(len(valid_ids[asset]) for asset in ASSETS),
        "rows_kept": actual_kept_rows,
        **dict(market_totals),
        "output_bytes": OUTPUT.stat().st_size,
    }
    manifest = {
        "source": "kachoio-v1",
        "fidelity": "EXACT",
        "horizon_scope": ["5m"],
        "output": str(OUTPUT.relative_to(ROOT)),
        "date_column": "date",
        "date_column_timezone": "UTC",
        "input_tick_date_coverage": {
            asset: dict(sorted(tick_dates[asset].items())) for asset in ASSETS
        },
        "unsupported_15m": unsupported_15m,
        "totals": totals,
        "days": per_day,
    }
    MANIFEST.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    print("date,btc_markets_kept,eth_markets_kept,markets_kept,rows_kept,null_rows_dropped,incomplete_markets_dropped")
    for day, assets in per_day.items():
        btc = assets["BTC"]
        eth = assets["ETH"]
        print(
            f"{day},{btc['markets']['kept']},{eth['markets']['kept']},"
            f"{btc['markets']['kept'] + eth['markets']['kept']},"
            f"{btc['rows']['kept'] + eth['rows']['kept']},"
            f"{btc['rows']['dropped_null_quotes'] + eth['rows']['dropped_null_quotes']},"
            f"{btc['markets']['drop_reasons'].get('incomplete_1s_tick_window', 0) + eth['markets']['drop_reasons'].get('incomplete_1s_tick_window', 0)}"
        )
    print(f"markets_kept={totals['kept_markets']}")
    print(f"rows_kept={actual_kept_rows}")
    print(f"rows_dropped_null_quotes={totals['rows_dropped_null_quotes']}")
    print(f"rows_dropped_incomplete_market={totals.get('rows_dropped_incomplete_market', 0)}")
    print(f"output={OUTPUT.relative_to(ROOT)} bytes={OUTPUT.stat().st_size}")
    print(f"manifest={MANIFEST.relative_to(ROOT)}")
    print(f"output_schema={readback.schema}")
    for asset, coverage in unsupported_15m.items():
        print(
            f"15m_support asset={asset} selected={coverage['selected_15m_markets']} "
            f"market_matches={coverage['matched_local_market_metadata']} "
            f"tick_matches={coverage['matched_local_tick_condition_ids']} included={coverage['included']}"
        )


if __name__ == "__main__":
    main()
