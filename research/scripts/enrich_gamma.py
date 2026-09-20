"""Enrich selected markets with per-market Gamma resolution contracts."""

import argparse
import json
import math
import re
import subprocess
import sys
import time
import urllib.parse

import polars as pl
from common import (
    PROCESSED,
    ensure_dirs,
    load_manifest,
    record_file,
    save_manifest,
)

SEARCH = "https://gamma-api.polymarket.com/public-search"
VENUES = ("binance", "coinbase", "chainlink")
NUMBER_RE = re.compile(
    r"(?P<currency>[$€£])?\s*"
    r"(?P<number>\d{1,3}(?:,\d{3})+|\d+(?:\.\d+)?)"
    r"\s*(?P<unit>k|thousand|m|million|b|billion)?\b",
    re.IGNORECASE,
)
ASSET_RE = {
    "BTC": re.compile(r"(?<![a-z0-9])(?:btc|bitcoin)(?![a-z0-9])", re.IGNORECASE),
    "ETH": re.compile(r"(?<![a-z0-9])(?:eth|ethereum)(?![a-z0-9])", re.IGNORECASE),
}
OPERATOR_RE = re.compile(
    r"\b(?:above|below|over|under|greater|less|reach|hit|at least|higher|lower)\b",
    re.IGNORECASE,
)


def curl_json(url: str) -> dict:
    if not url.startswith("https://"):
        raise ValueError("only https allowed")
    try:
        proc = subprocess.run(
            ["curl", "-sfL", "--max-time", "30", url],
            capture_output=True,
            text=True,
            check=False,
        )
    except Exception as exc:
        raise RuntimeError(f"curl failed: {exc}") from exc
    if proc.returncode != 0:
        raise RuntimeError(f"gamma error: {proc.stderr[-500:]}")
    try:
        data = json.loads(proc.stdout)
    except ValueError as exc:
        raise RuntimeError(f"gamma json failed: {exc}") from exc
    return data if isinstance(data, dict) else {}


def norm(value) -> str:
    try:
        if value is None:
            return ""
        if isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=False, sort_keys=True)
        return str(value)
    except Exception:
        return ""


def _same(value, expected: str) -> bool:
    return bool(expected) and norm(value).strip().lower() == expected.strip().lower()


def _as_list(value) -> list:
    if isinstance(value, list):
        return value
    if isinstance(value, dict):
        return [value]
    return []


def _market_entries(data: dict):
    for market in _as_list(data.get("markets")):
        if isinstance(market, dict):
            yield market, {}
    for event in _as_list(data.get("events")):
        if not isinstance(event, dict):
            continue
        for market in _as_list(event.get("markets")):
            if isinstance(market, dict):
                yield market, event


def _event_matches(
    event: dict, event_id: str, event_slug: str, event_title: str
) -> bool:
    if event_id and any(
        _same(event.get(key), event_id) for key in ("id", "eventId", "event_id")
    ):
        return True
    if event_slug and any(
        _same(event.get(key), event_slug) for key in ("slug", "eventSlug", "event_slug")
    ):
        return True
    return bool(
        event_title
        and any(
            _same(event.get(key), event_title)
            for key in ("title", "name", "eventTitle", "event_title")
        )
    )


def find_market(
    data: dict,
    slug: str,
    condition_id: str,
    event_id: str,
    event_slug: str,
    event_title: str,
) -> dict:
    """Match exact slug first, then condition id, then an unambiguous event."""
    entries = list(_market_entries(data))
    for market, _event in entries:
        if _same(market.get("slug"), slug):
            return market
    for market, _event in entries:
        if condition_id and any(
            _same(market.get(key), condition_id)
            for key in ("conditionId", "condition_id")
        ):
            return market

    event_entries = [
        (market, event)
        for market, event in entries
        if _event_matches(event, event_id, event_slug, event_title)
        or any(_same(market.get(key), event_id) for key in ("eventId", "event_id"))
    ]
    if condition_id:
        for market, _event in event_entries:
            if any(
                _same(market.get(key), condition_id)
                for key in ("conditionId", "condition_id")
            ):
                return market
    if len(event_entries) == 1:
        return event_entries[0][0]
    return {}


def search_gamma(query: str) -> dict:
    """Run one query and retry it once when Gamma/curl returns an error."""
    encoded = urllib.parse.quote(query)
    last_error = None
    for attempt in range(2):
        try:
            return curl_json(f"{SEARCH}?q={encoded}")
        except RuntimeError as exc:
            last_error = exc
            if attempt == 0:
                time.sleep(1)
    raise RuntimeError(f"{last_error} (after one retry)")


def fetch_market(
    slug: str,
    condition_id: str = "",
    event_id: str = "",
    event_slug: str = "",
    event_title: str = "",
) -> dict:
    """Query Gamma by slug and use condition/event identifiers as fallbacks."""
    queries = []
    for label, query in (
        ("slug", slug),
        ("condition_id", condition_id),
        ("event_slug", event_slug),
        ("event_id", event_id),
        ("event_title", event_title),
    ):
        query = norm(query).strip()
        if query and query not in [q for _, q in queries]:
            queries.append((label, query))

    errors = []
    for _label, query in queries:
        try:
            data = search_gamma(query)
        except RuntimeError as exc:
            errors.append(f"{query}: {exc}")
            continue
        market = find_market(
            data,
            slug=slug,
            condition_id=condition_id,
            event_id=event_id,
            event_slug=event_slug,
            event_title=event_title,
        )
        if market:
            return market
    if errors and len(errors) == len(queries):
        raise RuntimeError("; ".join(errors[-2:]))
    return {}


def fetch_slug(
    slug: str,
    condition_id: str = "",
    event_id: str = "",
    event_slug: str = "",
    event_title: str = "",
) -> dict:
    """Backward-compatible wrapper for callers that used the old name."""
    return fetch_market(slug, condition_id, event_id, event_slug, event_title)


def fetch_batch(markets: list[dict]) -> list[dict]:
    """Fetch a batch of selected rows without silently returning an empty stub."""
    return [
        fetch_market(
            norm(row.get("slug")),
            condition_id=norm(row.get("condition_id")),
            event_id=norm(row.get("event_id")),
            event_slug=norm(row.get("event_slug")),
            event_title=norm(row.get("event_title")),
        )
        for row in markets
    ]


def _first_text(record: dict, *keys: str) -> str:
    for key in keys:
        value = norm(record.get(key)).strip()
        if value:
            return value
    return ""


def extract_gamma(market: dict) -> dict:
    return {
        "rules": _first_text(
            market, "resolutionRules", "resolution_rules", "rules", "description"
        )[:4000],
        "source": _first_text(
            market, "resolutionSource", "resolution_source", "resolutionSourceUrl"
        ),
        "start": _first_text(market, "startDate", "startTime", "start_at", "startAt"),
        "end": _first_text(market, "endDate", "endTime", "end_at", "endAt"),
    }


def _parse_number(raw: str, unit: str) -> float:
    try:
        value = float(raw.replace(",", ""))
    except (AttributeError, TypeError, ValueError) as exc:
        raise ValueError(f"invalid numeric token: {raw!r}") from exc
    multiplier = {
        "k": 1_000.0,
        "thousand": 1_000.0,
        "m": 1_000_000.0,
        "million": 1_000_000.0,
        "b": 1_000_000_000.0,
        "billion": 1_000_000_000.0,
    }.get(unit.lower(), 1.0)
    return value * multiplier


def extract_reference_strike(
    question: str, rules: str, asset: str = ""
) -> tuple[str, float | None]:
    """Extract a per-market reference asset and numeric strike, never inventing zero."""
    text = f"{norm(question)} {norm(rules)}".strip()
    reference = ""
    for candidate, pattern in ASSET_RE.items():
        if pattern.search(text):
            reference = candidate
            break

    candidates = []
    for match in NUMBER_RE.finditer(text):
        raw = match.group("number")
        unit = (match.group("unit") or "").lower()
        currency = bool(match.group("currency"))
        grouped = "," in raw
        try:
            value = _parse_number(raw, unit)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(value) or value <= 0:
            continue
        window = text[max(0, match.start() - 40) : match.end() + 40]
        has_operator = bool(OPERATOR_RE.search(window))
        # Bare small numbers and minute-style tokens are not reliable strikes.
        if (
            not currency
            and not grouped
            and not unit
            and (value < 100 or not has_operator)
        ):
            continue
        if unit in ("m", "million") and value < 100_000:
            continue
        score = 0
        score += 4 if currency else 0
        score += 3 if grouped else 0
        score += 2 if unit else 0
        score += 1 if has_operator else 0
        if reference and ASSET_RE[reference].search(window):
            score += 2
        candidates.append((score, match.start(), value))

    if not candidates:
        return reference or norm(asset).upper(), None
    _score, _position, strike = max(candidates, key=lambda item: (item[0], -item[1]))
    if not reference and norm(asset).upper() in ASSET_RE:
        reference = norm(asset).upper()
    return reference or norm(asset).upper(), strike


def _resolution_columns(df: pl.DataFrame) -> pl.DataFrame:
    types = {
        "reference": pl.Utf8,
        "strike": pl.Float64,
        "start_at": pl.Utf8,
        "end_at": pl.Utf8,
    }
    for name, dtype in types.items():
        if name not in df.columns:
            df = df.with_columns(pl.lit(None, dtype=dtype).alias(name))
        else:
            df = df.with_columns(pl.col(name).cast(dtype, strict=False).alias(name))
    return df


def _clear_resolution(row: dict) -> None:
    row["resolution_rules"] = ""
    row["resolution_source"] = ""
    row["reference"] = ""
    row["strike"] = None
    row["start_at"] = ""
    row["end_at"] = ""


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--batch", default="50")
    args = ap.parse_args()
    ensure_dirs()
    try:
        batch = int(args.batch)
        if batch <= 0:
            raise ValueError("must be positive")
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"bad batch: {exc}") from exc
    sel_path = PROCESSED / "selected_markets.parquet"
    try:
        sel = pl.scan_parquet(str(sel_path)).collect()
    except Exception as exc:
        raise RuntimeError(f"selected read failed: {exc}") from exc

    selected_rows = sel.to_dicts()
    print(f"enriching {len(selected_rows)} markets via Gamma public-search")
    gamma_by_cid: dict[str, dict] = {}
    matched = 0
    errors = 0
    no_match = 0
    for i, row in enumerate(selected_rows):
        slug = norm(row.get("slug"))
        cid = norm(row.get("condition_id"))
        identifier = slug if slug else cid
        try:
            market = fetch_slug(
                slug,
                condition_id=cid,
                event_id=norm(row.get("event_id")),
                event_slug=norm(row.get("event_slug")),
                event_title=norm(row.get("event_title")),
            )
        except RuntimeError as exc:
            errors += 1
            print(f"warn {identifier}: {exc}")
            market = {}
        if market:
            matched += 1
            gamma_by_cid[cid] = extract_gamma(market)
        else:
            no_match += 1
        if (i + 1) % batch == 0:
            print(f"  {i + 1}/{len(selected_rows)} matched={matched}")
            if i + 1 < len(selected_rows):
                time.sleep(1)

    print(
        f"gamma matched {matched}/{len(selected_rows)} "
        f"(errors={errors}, no_match={no_match})"
    )
    if selected_rows and matched == 0:
        cause = (
            "request errors/rate-limit"
            if errors
            else "identifiers returned no matching slugs"
        )
        print(f"gamma matched 0/{len(selected_rows)}; likely cause: {cause}")
        print(
            "plan B (not implemented): use a Gamma market/event endpoint or a cached "
            "market snapshot keyed by condition_id."
        )

    rows = selected_rows
    for row in rows:
        cid = norm(row.get("condition_id"))
        gamma = gamma_by_cid.get(cid)
        if not gamma:
            # Existing SII text is not promoted when Gamma did not match.
            _clear_resolution(row)
            continue
        row["resolution_rules"] = gamma["rules"]
        row["resolution_source"] = gamma["source"]
        row["start_at"] = gamma["start"]
        row["end_at"] = gamma["end"]
        reference, strike = extract_reference_strike(
            row.get("question", ""), gamma["rules"], norm(row.get("asset"))
        )
        row["reference"] = reference
        row["strike"] = strike

    specs = []
    for row in rows:
        src = norm(row.get("resolution_source")).lower()
        rules = norm(row.get("resolution_rules"))
        reference = norm(row.get("reference"))
        strike = row.get("strike")
        try:
            strike = float(strike) if strike is not None and norm(strike) else None
        except (TypeError, ValueError):
            strike = None
        has_source_venue = any(venue in src for venue in VENUES)
        has_strike = strike is not None and math.isfinite(strike) and strike > 0
        if has_source_venue and len(rules) > 50 and has_strike:
            fidelity = "EXACT"
        elif rules or src:
            fidelity = "PROXY"
        else:
            fidelity = "UNKNOWN"
        specs.append(
            {
                "condition_id": row["condition_id"],
                "market_id": row["market_id"],
                "asset": row["asset"],
                "horizon": row["horizon"],
                "resolution_source": row.get("resolution_source", ""),
                "rule_excerpt": rules[:1000],
                "reference": reference,
                "strike": strike,
                "start_at": row.get("start_at", ""),
                "end_at": row.get("end_at", ""),
                "fidelity": fidelity,
            }
        )
    try:
        _resolution_columns(pl.DataFrame(rows)).write_parquet(str(sel_path))
        _resolution_columns(pl.DataFrame(specs)).write_parquet(
            str(PROCESSED / "resolution_specs.parquet")
        )
    except Exception as exc:
        raise RuntimeError(f"write failed: {exc}") from exc
    spec_df = pl.DataFrame(specs)
    print(spec_df.group_by("fidelity").len())
    m = load_manifest()
    record_file(
        m,
        "Polymarket-Gamma",
        "gamma-enrichment",
        str(sel_path),
        [],
        [],
        {"matched": matched, "of": len(rows), "errors": errors, "no_match": no_match},
        rows_before=len(rows),
        rows_after=len(rows),
        market_count=len(rows),
    )
    save_manifest(m)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
