"""Build a PolyTop-equivalent YES tape from Kachoio one-second books."""

import math
import shutil
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
OUTPUT = PROCESSED / "kachoio_polytop.parquet"
MIN_FREE_BYTES = 5 * 1024**3
WINDOW_SECONDS = 5 * 60
ASSETS = ("BTC", "ETH")
EXCLUDED_EARLY_ETH = {
    "eth-updown-5m-1777376700",
    "eth-updown-5m-1777377000",
    "eth-updown-5m-1777377300",
    "eth-updown-5m-1777377900",
    "eth-updown-5m-1777378500",
}
TICK_COLUMNS = ["condition_id", "t", "bu", "au", "su", "sau", "sad", "du", "dd"]
OUTPUT_SCHEMA = pa.schema(
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

@dataclass(frozen=True)
class Market:
    condition_id: str
    slug: str
    asset: str
    start_s: int
    end_s: int

@dataclass
class Counts:
    rows_in: int = 0
    rows_kept: int = 0
    rows_dropped_null: int = 0
    rows_dropped_out_of_window: int = 0
    kept_market_ids: set[str] = field(default_factory=set)

def is_missing(value: object) -> bool:
    return value is None or (isinstance(value, float) and math.isnan(value))

def as_float(value: object) -> float | None:
    return None if is_missing(value) else float(str(value))


def first_available(primary: object, fallback: object) -> float | None:
    """Use the preferred same-row field, then the paired field if available."""
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

def load_targets() -> dict[str, Market]:
    columns = ["condition_id", "slug", "start_at", "asset", "horizon"]
    candidates = []
    for row in pq.read_table(SELECTED, columns=columns).to_pylist():
        if row["asset"] not in ASSETS or row["horizon"] != "5m":
            continue
        start_s = parse_epoch(row["start_at"])
        date = datetime.fromtimestamp(start_s, tz=timezone.utc).date().isoformat()
        if date == "2026-04-28":
            candidates.append(
                Market(
                    condition_id=str(row["condition_id"]),
                    slug=str(row["slug"]),
                    asset=str(row["asset"]),
                    start_s=start_s,
                    end_s=start_s + WINDOW_SECONDS,
                )
            )
    if len(candidates) != 120:
        raise RuntimeError(f"expected 120 Apr-28 5m candidates, found {len(candidates)}")
    if len({m.condition_id for m in candidates}) != len(candidates):
        raise RuntimeError("selected_markets contains duplicate target condition_id values")
    targets = [m for m in candidates if m.slug not in EXCLUDED_EARLY_ETH]
    if len(targets) != 115 or not EXCLUDED_EARLY_ETH.issubset({m.slug for m in candidates}):
        raise RuntimeError("README-documented early-ETH exclusions changed")
    return {m.condition_id: m for m in targets}


def validate_kachoio_markets(targets: dict[str, Market]) -> None:
    for asset in ASSETS:
        path = RAW / f"{asset.lower()}_markets.parquet"
        rows = pq.read_table(
            path, columns=["condition_id", "slug", "market_start", "market_end"]
        ).to_pylist()
        by_condition = {str(row["condition_id"]): row for row in rows}
        asset_targets = [m for m in targets.values() if m.asset == asset]
        missing = [m.condition_id for m in asset_targets if m.condition_id not in by_condition]
        if missing:
            raise RuntimeError(f"{asset} metadata missing {len(missing)} target markets")
        for market in asset_targets:
            raw = by_condition[market.condition_id]
            if raw["slug"] != market.slug:
                raise RuntimeError(f"slug mismatch for {market.condition_id}")
            if parse_epoch(raw["market_start"]) != market.start_s:
                raise RuntimeError(f"market_start mismatch for {market.slug}")
            if parse_epoch(raw["market_end"]) != market.end_s:
                raise RuntimeError(f"market_end mismatch for {market.slug}")


def process_asset(
    asset: str, targets: dict[str, Market], writer: pq.ParquetWriter, counts: Counts
) -> None:
    path = RAW / f"{asset.lower()}_ticks.parquet"
    target_ids = set(targets)
    seen: dict[str, set[int]] = {condition_id: set() for condition_id in targets}
    day_start = datetime.fromtimestamp(min(m.start_s for m in targets.values()), tz=timezone.utc)
    day_end = datetime.fromtimestamp(max(m.end_s for m in targets.values()), tz=timezone.utc)
    batches = ds.dataset(path, format="parquet").to_batches(
        columns=TICK_COLUMNS,
        filter=(ds.field("ts_utc") >= day_start) & (ds.field("ts_utc") < day_end),
        batch_size=100_000,
    )
    for batch in batches:
        if not batch.num_rows:
            continue
        table = pa.Table.from_batches([batch])
        mask = pa.array([value in target_ids for value in table["condition_id"].to_pylist()])
        table = table.filter(mask)
        output_rows = []
        for row in table.to_pylist():
            market = targets[str(row["condition_id"])]
            counts.rows_in += 1
            tick_s = int(row["t"])
            if not market.start_s <= tick_s < market.end_s:
                counts.rows_dropped_out_of_window += 1
                continue
            if tick_s in seen[market.condition_id]:
                raise RuntimeError(f"duplicate in-window timestamp for {market.slug}: {tick_s}")
            seen[market.condition_id].add(tick_s)
            if is_missing(row["bu"]) or is_missing(row["au"]):
                counts.rows_dropped_null += 1
                continue
            yes_bid, yes_ask = float(row["bu"]), float(row["au"])
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
                }
            )
            counts.rows_kept += 1
            counts.kept_market_ids.add(market.condition_id)
        if output_rows:
            writer.write_table(pa.Table.from_pylist(output_rows, schema=OUTPUT_SCHEMA))
    for market in targets.values():
        actual = seen[market.condition_id]
        expected = set(range(market.start_s, market.end_s))
        if actual != expected:
            missing = sorted(expected - actual)[:5]
            extra = sorted(actual - expected)[:5]
            raise RuntimeError(
                f"non-contiguous {market.slug}: {len(actual)}/300 ticks; "
                f"missing={missing} extra={extra}"
            )


def main() -> None:
    required = [
        SELECTED,
        RAW / "btc_ticks.parquet",
        RAW / "eth_ticks.parquet",
        RAW / "btc_markets.parquet",
        RAW / "eth_markets.parquet",
    ]
    missing = [str(path) for path in required if not path.exists()]
    if missing:
        raise FileNotFoundError("missing inputs: " + ", ".join(missing))
    free_bytes = shutil.disk_usage(OUTPUT.parent).free
    if free_bytes < MIN_FREE_BYTES:
        raise RuntimeError(f"refusing ETL: only {free_bytes} free bytes; need 5 GiB")
    targets = load_targets()
    validate_kachoio_markets(targets)
    counts = Counts()
    writer = pq.ParquetWriter(OUTPUT, OUTPUT_SCHEMA, compression="zstd")
    try:
        for asset in ASSETS:
            asset_targets = {cid: m for cid, m in targets.items() if m.asset == asset}
            process_asset(asset, asset_targets, writer, counts)
    finally:
        writer.close()
    if counts.kept_market_ids != set(targets):
        raise RuntimeError(
            f"markets without a retained quote: {len(set(targets) - counts.kept_market_ids)}"
        )
    readback = pq.read_table(OUTPUT)
    if not readback.schema.equals(OUTPUT_SCHEMA, check_metadata=True):
        raise RuntimeError(f"output schema mismatch:\n{readback.schema}")
    if readback.num_rows != counts.rows_kept:
        raise RuntimeError("output row count differs from ETL count")
    print(f"markets_in={len(targets)}")
    print(f"markets_kept={len(counts.kept_market_ids)}")
    print(f"rows_in={counts.rows_in}")
    print(f"rows_kept={counts.rows_kept}")
    print(f"rows_dropped_null={counts.rows_dropped_null}")
    print(f"rows_dropped_out_of_window={counts.rows_dropped_out_of_window}")
    print(f"output_bytes={OUTPUT.stat().st_size}")
    print(f"output_schema={readback.schema}")


if __name__ == "__main__":
    main()
