#!/usr/bin/env python3
"""Slice the frozen walk-forward tape windows without changing source rows."""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_TAPE = ROOT / "research-data/processed/kachoio_polytop_multiday.parquet"
DEFAULT_SOURCE_MANIFEST = ROOT / "research-data/processed/kachoio_multiday.manifest.json"
DEFAULT_PREREG = ROOT / "research-data/reports/walkforward-prereg-01.md"
DEFAULT_OUTPUT = ROOT / "research-data/cache/walkforward-01/windows"
REQUIRED_PRECOMPUTE_COLUMNS = (
    "condition_id",
    "ts_ms",
    "yes_bid",
    "yes_ask",
    "bid_depth_5c",
    "mid",
)
EXPECTED_WINDOW_COUNT = 25
EMBARGO_MS = 60_000
BATCH_SIZE = 250_000


@dataclass(frozen=True)
class Window:
    name: str
    train_start: datetime | None
    train_end: datetime | None
    test_start: datetime | None
    test_end: datetime | None
    explicit: bool = False

    @property
    def start(self) -> datetime:
        return required_bound(
            self.train_start if self.train_start is not None else self.test_start,
            "window start",
        )

    @property
    def end(self) -> datetime:
        return required_bound(self.test_end, "window end")


def required_bound(value: datetime | None, label: str) -> datetime:
    if value is None:
        raise ValueError(f"missing {label}")
    return value


def parse_utc(value: str) -> datetime:
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"invalid UTC timestamp {value!r}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise ValueError(f"timestamp must include a UTC offset: {value!r}")
    return parsed.astimezone(timezone.utc)


def epoch_ms(value: datetime) -> int:
    delta = value - datetime(1970, 1, 1, tzinfo=timezone.utc)
    if delta.microseconds % 1000:
        raise ValueError(f"timestamp is not millisecond-aligned: {value.isoformat()}")
    return delta.days * 86_400_000 + delta.seconds * 1000 + delta.microseconds // 1000


def parse_interval(value: str) -> tuple[datetime, datetime]:
    parts = re.split(r"\s+[–—]\s+", value.strip(), maxsplit=1)
    if len(parts) != 2:
        raise ValueError(f"could not parse preregistered interval: {value!r}")
    end = parts[1].split()[0]
    return parse_utc(parts[0]), parse_utc(end)


def load_preregistered_windows(path: Path) -> list[Window]:
    windows: list[Window] = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip().startswith("|"):
            continue
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if len(cells) < 3 or not re.fullmatch(r"w\d{2}", cells[0]):
            continue
        train_start, train_end = parse_interval(cells[1])
        test_start, test_end = parse_interval(cells[2])
        window = Window(cells[0], train_start, train_end, test_start, test_end)
        validate_window(window, line_number)
        windows.append(window)

    if len(windows) != EXPECTED_WINDOW_COUNT:
        raise ValueError(
            f"expected {EXPECTED_WINDOW_COUNT} preregistered windows, found {len(windows)}"
        )
    names = [window.name for window in windows]
    if len(set(names)) != len(names):
        raise ValueError("preregistration contains duplicate window IDs")
    return windows


def validate_window(window: Window, line_number: int | None = None) -> None:
    label = f" on preregistration line {line_number}" if line_number else ""
    if window.explicit:
        if window.test_start is None or window.test_end is None:
            raise ValueError(f"explicit window needs both bounds{label}")
        if window.test_start >= window.test_end:
            raise ValueError(f"explicit start must be before end{label}")
        return
    train_start = required_bound(window.train_start, "train start")
    train_end = required_bound(window.train_end, "train end")
    test_start = required_bound(window.test_start, "test start")
    test_end = required_bound(window.test_end, "test end")
    if train_start >= train_end:
        raise ValueError(f"train start must be before train end{label}")
    if train_end > test_start:
        raise ValueError(f"train and test intervals overlap{label}")
    if test_start >= test_end:
        raise ValueError(f"test start must be before test end{label}")
    embargo_ms = epoch_ms(test_start) - epoch_ms(train_end)
    if embargo_ms != EMBARGO_MS:
        raise ValueError(f"expected a {EMBARGO_MS}ms preregistered embargo, got {embargo_ms}{label}")


def explicit_window(value: list[str]) -> Window:
    name, raw_start, raw_end = value
    if not re.fullmatch(r"w\d{2}", name):
        raise ValueError("explicit window ID must use the wNN form")
    start, end = parse_utc(raw_start), parse_utc(raw_end)
    window = Window(name, None, None, start, end, explicit=True)
    validate_window(window)
    return window


def interval_mask(timestamps, start: datetime, end: datetime):
    start_ms, end_ms = epoch_ms(start), epoch_ms(end)
    return (timestamps >= start_ms) & (timestamps < end_ms)


def update_market_set(target: set[str], ids: list[str | None], mask) -> None:
    target.update(
        condition_id
        for condition_id, selected in zip(ids, mask, strict=True)
        if selected and condition_id
    )


def display_path(path: Path) -> str:
    try:
        return path.relative_to(ROOT).as_posix()
    except ValueError:
        return str(path)


def validate_output_dir(output_dir: Path) -> Path:
    allowed_root = DEFAULT_OUTPUT.resolve()
    resolved = output_dir.resolve()
    if resolved != allowed_root and allowed_root not in resolved.parents:
        raise ValueError(f"output directory must be within {allowed_root}")
    return resolved


def slice_windows(
    tape_path: Path,
    source_manifest_path: Path,
    output_dir: Path,
    windows: list[Window],
    preregistration: Path | None,
) -> dict:
    output_dir = validate_output_dir(output_dir)
    source_manifest = json.loads(source_manifest_path.read_text(encoding="utf-8"))
    if source_manifest.get("date_column") != "date":
        raise ValueError("multiday manifest must declare the date column as 'date'")
    if source_manifest.get("date_column_timezone") != "UTC":
        raise ValueError("multiday manifest must declare UTC date-column timezone")

    parquet = pq.ParquetFile(tape_path)
    schema = parquet.schema_arrow
    missing = [name for name in (*REQUIRED_PRECOMPUTE_COLUMNS, "date") if name not in schema.names]
    if missing:
        raise ValueError(f"tape is missing required columns: {', '.join(missing)}")
    if schema.field("ts_ms").type != pa.int64():
        raise ValueError("precompute_jev requires ts_ms to be int64")
    if schema.field("condition_id").type not in (pa.string(), pa.large_string()):
        raise ValueError("precompute_jev requires condition_id to be a string")
    for name in ("yes_bid", "yes_ask", "bid_depth_5c", "mid"):
        if schema.field(name).type != pa.float64():
            raise ValueError(f"precompute_jev requires {name} to be float64")
    if schema.field("date").type not in (pa.string(), pa.large_string()):
        raise ValueError("multiday tape date column must be a string")

    output_dir.mkdir(parents=True, exist_ok=True)
    writers: dict[str, pq.ParquetWriter] = {}
    outputs: dict[str, dict] = {}
    market_sets: dict[str, dict[str, set[str]]] = {}
    for window in windows:
        window_dir = output_dir / window.name
        window_dir.mkdir(parents=True, exist_ok=True)
        writers[window.name] = pq.ParquetWriter(window_dir / "tape.parquet", schema)
        counts = dict.fromkeys(("rows", "train_rows", "embargo_rows", "test_rows"), 0)
        outputs[window.name] = {
            "window": window,
            "counts": counts,
            "markets": dict.fromkeys(("all", "train", "embargo", "test"), 0),
            "tape_path": (window_dir / "tape.parquet").relative_to(ROOT).as_posix(),
        }
        market_sets[window.name] = {
            key: set() for key in ("all", "train", "embargo", "test")
        }

    timestamp_index = schema.get_field_index("ts_ms")
    condition_index = schema.get_field_index("condition_id")
    try:
        for batch in parquet.iter_batches(batch_size=BATCH_SIZE):
            timestamp_array = batch.column(timestamp_index)
            if timestamp_array.null_count:
                raise ValueError("ts_ms contains null values; cannot slice losslessly")
            timestamps = timestamp_array.to_numpy(zero_copy_only=False)
            condition_ids = batch.column(condition_index).to_pylist()
            batch_table = pa.Table.from_batches([batch], schema=schema)

            for window in windows:
                entry = outputs[window.name]
                all_mask = interval_mask(timestamps, window.start, window.end)
                row_count = int(all_mask.sum())
                entry["counts"]["rows"] += row_count
                if row_count:
                    selected = batch_table.filter(pa.array(all_mask))
                    writers[window.name].write_table(selected)
                    update_market_set(market_sets[window.name]["all"], condition_ids, all_mask)

                if window.explicit:
                    continue
                train_mask = interval_mask(timestamps, window.train_start, window.train_end)  # type: ignore[arg-type]
                embargo_mask = interval_mask(timestamps, window.train_end, window.test_start)  # type: ignore[arg-type]
                test_mask = interval_mask(timestamps, window.test_start, window.test_end)  # type: ignore[arg-type]
                for label, mask in (
                    ("train", train_mask),
                    ("embargo", embargo_mask),
                    ("test", test_mask),
                ):
                    entry["counts"][f"{label}_rows"] += int(mask.sum())
                    if mask.any():
                        update_market_set(market_sets[window.name][label], condition_ids, mask)
    finally:
        for writer in writers.values():
            writer.close()

    manifest_windows = []
    for window in windows:
        entry = outputs[window.name]
        markets = market_sets[window.name]
        entry["markets"] = {key: len(ids) for key, ids in markets.items()}
        if window.explicit:
            bounds = {
                "start_utc": window.start.isoformat().replace("+00:00", "Z"),
                "end_utc_exclusive": window.end.isoformat().replace("+00:00", "Z"),
            }
            assignment = {
                "kind": "explicit_span",
                "unassigned_rows": entry["counts"]["rows"],
                "unassigned_markets": entry["markets"]["all"],
            }
        else:
            train_start = required_bound(window.train_start, "train start")
            train_end = required_bound(window.train_end, "train end")
            test_start = required_bound(window.test_start, "test start")
            test_end = required_bound(window.test_end, "test end")
            bounds = {
                "train_start_utc": train_start.isoformat().replace("+00:00", "Z"),
                "train_end_utc_exclusive": train_end.isoformat().replace("+00:00", "Z"),
                "test_start_utc": test_start.isoformat().replace("+00:00", "Z"),
                "test_end_utc_exclusive": test_end.isoformat().replace("+00:00", "Z"),
            }
            assignment = {
                "embargo_start_utc": bounds["train_end_utc_exclusive"],
                "embargo_end_utc_exclusive": bounds["test_start_utc"],
                "embargo_ms": EMBARGO_MS,
                "embargo_rows_unassigned": entry["counts"]["embargo_rows"],
                "embargo_markets_unassigned": entry["markets"]["embargo"],
            }
        manifest_windows.append(
            {
                "window": window.name,
                "bounds": bounds,
                "slice_bounds_utc": {
                    "start_inclusive": window.start.isoformat().replace("+00:00", "Z"),
                    "end_exclusive": window.end.isoformat().replace("+00:00", "Z"),
                },
                "rows": entry["counts"],
                "markets": entry["markets"],
                "assignment": assignment,
                "tape_path": entry["tape_path"],
            }
        )

    result = {
        "format_version": 1,
        "source_tape": display_path(tape_path),
        "source_manifest": display_path(source_manifest_path),
        "source_rows": parquet.metadata.num_rows,
        "fidelity": "EXACT: source rows copied unchanged; only half-open ts_ms bounds applied",
        "timestamp_column": "ts_ms",
        "timestamp_unit": "milliseconds since Unix epoch, UTC",
        "required_precompute_tape_columns": list(REQUIRED_PRECOMPUTE_COLUMNS),
        "schema": str(schema),
        "preregistration": display_path(preregistration) if preregistration else None,
        "windows": manifest_windows,
    }
    manifest_path = output_dir / "windows.manifest.json"
    manifest_path.write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return result


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tape", type=Path, default=DEFAULT_TAPE)
    parser.add_argument("--source-manifest", type=Path, default=DEFAULT_SOURCE_MANIFEST)
    parser.add_argument("--prereg", type=Path, default=DEFAULT_PREREG)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--explicit-window",
        nargs=3,
        metavar=("ID", "START_UTC", "END_UTC"),
        help="slice one explicit [START_UTC, END_UTC) span instead of the preregistration",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        args.tape = args.tape.resolve()
        args.source_manifest = args.source_manifest.resolve()
        args.prereg = args.prereg.resolve()
        args.out_dir = args.out_dir.resolve()
        windows = (
            [explicit_window(args.explicit_window)]
            if args.explicit_window
            else load_preregistered_windows(args.prereg)
        )
        preregistration = None if args.explicit_window else args.prereg
        manifest = slice_windows(
            args.tape,
            args.source_manifest,
            args.out_dir,
            windows,
            preregistration,
        )
    except (OSError, ValueError, pa.ArrowException, json.JSONDecodeError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    for window in manifest["windows"]:
        print(
            f"{window['window']} rows={window['rows']['rows']} "
            f"markets={window['markets']['all']} "
            f"train={window['rows']['train_rows']} embargo={window['rows']['embargo_rows']} "
            f"test={window['rows']['test_rows']}"
        )
    print(f"manifest={args.out_dir / 'windows.manifest.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
