"""Backfill exact public Gamma metadata for the Kachoio multiday tape."""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from http.client import HTTPException, HTTPSConnection
from pathlib import Path
from urllib.parse import urlencode

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[2]
INPUT = ROOT / "research-data/processed/kachoio_polytop_multiday.parquet"
SELECTED_MARKETS = ROOT / "research-data/processed/selected_markets.parquet"
OUTPUT_DIR = ROOT / "research-data/cache/walkforward-01/verify-eligibility"
OUTPUT = OUTPUT_DIR / "gamma_meta.parquet"
PROGRESS = OUTPUT_DIR / "gamma_meta.progress.jsonl"
MANIFEST = OUTPUT_DIR / "gamma_meta.manifest.json"
GAMMA_MARKETS_URL = "https://gamma-api.polymarket.com/markets"
BATCH_SIZE = 50
REQUEST_DELAY_SECONDS = 0.25
MAX_RETRIES = 5
REQUEST_TIMEOUT_SECONDS = 30

OUTPUT_COLUMNS = [
    "condition_id",
    "gamma_condition_id",
    "gamma_market_id",
    "gamma_slug",
    "question",
    "description",
    "resolution_source",
    "resolution_rules",
    "resolution_rules_source_field",
    "outcomes",
    "tokens",
    "start_date",
    "end_date",
    "event_start_time",
    "created_at",
    "updated_at",
    "fetched_at_utc",
    "gamma_source_url",
    "gamma_payload_json",
]
OUTPUT_SCHEMA = pa.schema([(name, pa.string()) for name in OUTPUT_COLUMNS])


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_condition_id(value: str) -> str:
    return value.strip().lower()


def source_condition_ids(path: Path, metadata_path: Path = SELECTED_MARKETS) -> list[str]:
    parquet_file = pq.ParquetFile(path)
    if "condition_id" not in parquet_file.schema_arrow.names:
        raise ValueError(f"{path} has no condition_id column")

    values: set[str] = set()
    for batch in parquet_file.iter_batches(columns=["condition_id"], batch_size=8192):
        for value in batch.column(0).to_pylist():
            if value is not None and str(value).strip():
                values.add(str(value).strip())

    metadata_file = pq.ParquetFile(metadata_path)
    required = {"condition_id", "question", "resolution_rules"}
    if not required.issubset(metadata_file.schema_arrow.names):
        raise ValueError(f"{metadata_path} must contain condition_id, question, and resolution_rules")
    already_exact: set[str] = set()
    for batch in metadata_file.iter_batches(
        columns=["condition_id", "question", "resolution_rules"], batch_size=8192
    ):
        for row in batch.to_pylist():
            condition_id = row.get("condition_id")
            if (
                condition_id is not None
                and str(row.get("question") or "").strip()
                and str(row.get("resolution_rules") or "").strip()
            ):
                already_exact.add(normalize_condition_id(str(condition_id)))
    missing_exact_text = {
        condition_id
        for condition_id in values
        if normalize_condition_id(condition_id) not in already_exact
    }
    return sorted(missing_exact_text, key=normalize_condition_id)


def json_field(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        try:
            return json.dumps(json.loads(value), ensure_ascii=False, separators=(",", ":"))
        except json.JSONDecodeError:
            return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def first_text(market: dict, *names: str) -> tuple[str, str]:
    for name in names:
        value = market.get(name)
        if isinstance(value, str) and value.strip():
            return value, name
    return "", ""


def matches_condition_id(market: object, condition_id: str) -> bool:
    if not isinstance(market, dict):
        return False
    gamma_condition_id, _ = first_text(market, "conditionId", "condition_id")
    return bool(gamma_condition_id) and normalize_condition_id(
        gamma_condition_id
    ) == normalize_condition_id(condition_id)


def gamma_row(market: dict, requested_condition_id: str, fetched_at: str) -> dict[str, str]:
    gamma_condition_id, _ = first_text(market, "conditionId", "condition_id")
    question, _ = first_text(market, "question")
    description, _ = first_text(market, "description")
    resolution_source, _ = first_text(market, "resolutionSource", "resolution_source")
    resolution_rules, rules_source = first_text(
        market,
        "resolutionRules",
        "resolution_rules",
        "resolutionCriteria",
        "resolution_criteria",
        "rules",
    )
    if not resolution_rules and description:
        # Gamma's public market description is the verbatim resolution detail
        # when the API does not expose a separate resolution-rules field.
        resolution_rules, rules_source = description, "description"

    raw_payload = json.dumps(market, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return {
        "condition_id": requested_condition_id,
        "gamma_condition_id": gamma_condition_id,
        "gamma_market_id": str(market.get("id") or ""),
        "gamma_slug": str(market.get("slug") or ""),
        "question": question,
        "description": description,
        "resolution_source": resolution_source,
        "resolution_rules": resolution_rules,
        "resolution_rules_source_field": rules_source,
        "outcomes": json_field(market.get("outcomes")),
        "tokens": json_field(
            market.get("clobTokenIds", market.get("clob_token_ids", market.get("tokens")))
        ),
        "start_date": str(market.get("startDate") or market.get("start_date") or ""),
        "end_date": str(market.get("endDate") or market.get("end_date") or ""),
        "event_start_time": str(market.get("eventStartTime") or ""),
        "created_at": str(market.get("createdAt") or market.get("created_at") or ""),
        "updated_at": str(market.get("updatedAt") or market.get("updated_at") or ""),
        "fetched_at_utc": fetched_at,
        "gamma_source_url": GAMMA_MARKETS_URL,
        "gamma_payload_json": raw_payload,
    }


def response_markets(payload: object) -> list[dict]:
    if isinstance(payload, list):
        return [market for market in payload if isinstance(market, dict)]
    if isinstance(payload, dict):
        for key in ("markets", "data", "results"):
            value = payload.get(key)
            if isinstance(value, list):
                return [market for market in value if isinstance(market, dict)]
    raise ValueError("Gamma markets response was not a market list")


def fetch_batch(condition_ids: list[str]) -> list[dict]:
    # Historical Kachoio markets require closed=true; Gamma otherwise omits them.
    # Gamma ignores comma-joined condition_ids (returns []); repeat the param.
    query = urlencode(
        [("condition_ids", condition_id) for condition_id in condition_ids]
        + [("closed", "true")]
    )
    request_path = f"/markets?{query}"
    for attempt in range(MAX_RETRIES + 1):
        connection = HTTPSConnection(
            "gamma-api.polymarket.com", timeout=REQUEST_TIMEOUT_SECONDS
        )
        try:
            connection.request(
                "GET",
                request_path,
                headers={
                    "Accept": "application/json",
                    "User-Agent": "jevTrader-public-metadata-backfill/1.0",
                },
            )
            response = connection.getresponse()
            if response.status == 429 or 500 <= response.status < 600:
                if attempt == MAX_RETRIES:
                    raise RuntimeError(
                        f"Gamma HTTP {response.status}: {response.reason}"
                    )
                retry_after = response.getheader("Retry-After", "")
                try:
                    delay = min(60.0, max(0.0, float(retry_after)))
                except ValueError:
                    delay = min(30.0, 2.0**attempt)
                response.read()
            elif not 200 <= response.status < 300:
                raise RuntimeError(f"Gamma HTTP {response.status}: {response.reason}")
            else:
                payload = json.loads(response.read().decode("utf-8"))
                return response_markets(payload)
        except RuntimeError:
            raise
        except (HTTPException, OSError, TimeoutError, json.JSONDecodeError) as error:
            if attempt == MAX_RETRIES:
                raise RuntimeError(f"Gamma request failed: {error}") from error
            delay = min(30.0, 2.0**attempt)
        finally:
            connection.close()
        time.sleep(delay)
    raise RuntimeError("Gamma retry loop exhausted")


def read_progress() -> dict[str, dict[str, str]]:
    records: dict[str, dict[str, str]] = {}
    if PROGRESS.is_file():
        with PROGRESS.open(encoding="utf-8") as progress:
            for line in progress:
                try:
                    entry = json.loads(line)
                    market = entry["market"]
                    condition_id = str(entry["condition_id"])
                    fetched_at = str(entry["fetched_at_utc"])
                except (KeyError, TypeError, json.JSONDecodeError):
                    continue
                if not matches_condition_id(market, condition_id):
                    continue
                records[normalize_condition_id(condition_id)] = gamma_row(
                    market, condition_id, fetched_at
                )
    if OUTPUT.is_file():
        for record in pq.read_table(OUTPUT).to_pylist():
            condition_id = record.get("condition_id")
            payload = record.get("gamma_payload_json")
            if not condition_id or not payload:
                continue
            try:
                market = json.loads(payload)
            except json.JSONDecodeError:
                continue
            if not matches_condition_id(market, str(condition_id)):
                continue
            records[normalize_condition_id(condition_id)] = gamma_row(
                market,
                condition_id,
                str(record.get("fetched_at_utc") or ""),
            )
    return records


def write_manifest(
    total: int,
    fetched: int,
    missing_ids: list[str],
    failed_ids: list[str],
    complete: bool,
) -> None:
    manifest = {
        "source": GAMMA_MARKETS_URL,
        "input": str(INPUT.relative_to(ROOT)),
        "output": str(OUTPUT.relative_to(ROOT)),
        "progress": str(PROGRESS.relative_to(ROOT)),
        "updated_at_utc": utc_now(),
        "complete": complete,
        "counts": {
            "requested": total,
            "fetched": fetched,
            "missing": len(missing_ids),
            "failed": len(failed_ids),
        },
        "missing_condition_ids": missing_ids,
        "failed_condition_ids": failed_ids,
        "text_provenance": "question and resolution_rules are copied from the Gamma row; resolution_rules_source_field records the source field",
    }
    MANIFEST.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def fetched_count(condition_ids: list[str], records: dict[str, dict[str, str]]) -> int:
    return sum(normalize_condition_id(condition_id) in records for condition_id in condition_ids)


def save_output(records: dict[str, dict[str, str]], condition_ids: list[str]) -> None:
    requested = {normalize_condition_id(condition_id) for condition_id in condition_ids}
    rows = sorted(
        (row for key, row in records.items() if key in requested),
        key=lambda row: normalize_condition_id(row["condition_id"]),
    )
    table = pa.Table.from_pylist(rows, schema=OUTPUT_SCHEMA)
    pq.write_table(table, OUTPUT, compression="zstd")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--batch-size",
        type=int,
        default=BATCH_SIZE,
        help="condition IDs per polite Gamma request (default: %(default)s)",
    )
    parser.add_argument(
        "--request-delay",
        type=float,
        default=REQUEST_DELAY_SECONDS,
        help="seconds between Gamma requests (default: %(default)s)",
    )
    args = parser.parse_args()
    if not 1 <= args.batch_size <= 100:
        parser.error("--batch-size must be between 1 and 100")
    if args.request_delay < 0:
        parser.error("--request-delay must not be negative")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    condition_ids = source_condition_ids(INPUT, SELECTED_MARKETS)
    records = read_progress()
    pending = [
        condition_id
        for condition_id in condition_ids
        if normalize_condition_id(condition_id) not in records
    ]
    missing_ids: list[str] = []
    failed_ids: list[str] = []
    print(
        f"conditions={len(condition_ids)} already_fetched={fetched_count(condition_ids, records)} "
        f"pending={len(pending)}"
    )

    for batch_number, start in enumerate(range(0, len(pending), args.batch_size)):
        batch = pending[start : start + args.batch_size]
        if batch_number:
            time.sleep(args.request_delay)
        try:
            markets = fetch_batch(batch)
        except Exception as error:
            failed_ids.extend(batch)
            print(f"failed batch={batch_number + 1} ids={len(batch)} error={error}", file=sys.stderr)
            write_manifest(
                len(condition_ids),
                fetched_count(condition_ids, records),
                missing_ids,
                failed_ids,
                False,
            )
            continue

        requested = {normalize_condition_id(condition_id): condition_id for condition_id in batch}
        returned: set[str] = set()
        fetched_at = utc_now()
        with PROGRESS.open("a", encoding="utf-8") as progress:
            for market in markets:
                gamma_condition_id, _ = first_text(market, "conditionId", "condition_id")
                normalized = normalize_condition_id(gamma_condition_id)
                if normalized not in requested or normalized in returned:
                    continue
                condition_id = requested[normalized]
                returned.add(normalized)
                entry = {
                    "condition_id": condition_id,
                    "fetched_at_utc": fetched_at,
                    "market": market,
                }
                progress.write(json.dumps(entry, ensure_ascii=False, separators=(",", ":")) + "\n")
                records[normalized] = gamma_row(market, condition_id, fetched_at)
            progress.flush()

        missing_ids.extend(
            condition_id
            for normalized, condition_id in requested.items()
            if normalized not in returned
        )
        write_manifest(
            len(condition_ids),
            fetched_count(condition_ids, records),
            missing_ids,
            failed_ids,
            False,
        )
        print(
            f"batch={batch_number + 1}/{(len(pending) + args.batch_size - 1) // args.batch_size} "
            f"returned={len(returned)} missing={len(batch) - len(returned)} "
            f"fetched_total={fetched_count(condition_ids, records)}"
        )

    fetched = fetched_count(condition_ids, records)
    save_output(records, condition_ids)
    write_manifest(len(condition_ids), fetched, missing_ids, failed_ids, not failed_ids)
    print(
        f"fetched={fetched} missing={len(missing_ids)} failed={len(failed_ids)} "
        f"output={OUTPUT.relative_to(ROOT)} manifest={MANIFEST.relative_to(ROOT)}"
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
