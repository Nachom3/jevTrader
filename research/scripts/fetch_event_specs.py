"""Per-market ResolutionSpec via SII event_id -> Gamma /events/<id> (exact join).

Why this exists: Gamma public-search only indexes live markets, so slug
search misses resolved history (0/366). SII markets.parquet carries the
numeric Gamma event_id per condition_id; /events/<id> returns the exact
market (matched by conditionId) with description + resolutionSource.
Network-light (one small JSON per event, sequential, polite pacing) and
RAM-trivial. Writes selected_markets + resolution_specs with EXACT/PROXY/
UNKNOWN per the strict gate (venue source + rules>50 + window consistent).
Up/Down markets carry reference_type=WINDOW_OPEN (no fixed strike).
"""

import argparse
import datetime
import json
import re
import subprocess
import sys
import time

import polars as pl

from common import (
    PROCESSED,
    RAW,
    ensure_dirs,
    load_manifest,
    record_file,
    save_manifest,
)

GAMMA_EVENT = "https://gamma-api.polymarket.com/events/"
HORIZON_SECS = {"5m": 300, "15m": 900, "1h": 3600, "4h": 14400}
VENUES = ("chainlink", "binance", "coinbase")
MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11,
    "december": 12,
}


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
        raise RuntimeError(f"gamma error: {proc.stderr[-300:]}")
    try:
        data = json.loads(proc.stdout)
    except ValueError as exc:
        raise RuntimeError(f"gamma json failed: {exc}") from exc
    return data if isinstance(data, dict) else {}


def norm(value) -> str:
    try:
        return "" if value is None else str(value)
    except Exception:
        return ""


def et_offset_hours(month: int, day: int) -> int:
    """US Eastern UTC offset: EDT (UTC-4) Mar 8..Nov 1 2026, else EST."""
    if month < 3 or month > 11:
        return -5
    if month > 3 and month < 11:
        return -4
    if month == 3:
        return -4 if day >= 8 else -5
    return -4 if day < 1 else -5


def to24(h: int, ap: str) -> int:
    if ap == "AM":
        return 0 if h == 12 else h
    return 12 if h == 12 else h + 12


def parse_window(question: str, year: int):
    """Parse 'April 28, 1:00PM-1:15PM ET' -> (start_utc, end_utc)."""
    try:
        m = re.search(
            r"([A-Za-z]+)\s+(\d{1,2}),\s*(\d{1,2}):(\d{2})(AM|PM)-(\d{1,2}):(\d{2})(AM|PM)",
            question,
        )
        if not m:
            return None
        month = MONTHS.get(m.group(1).lower())
        day = int(m.group(2))
        if not month:
            return None
        off = et_offset_hours(month, day)
        start = datetime.datetime(year, month, day, to24(int(m.group(3)), m.group(5)),
                                  int(m.group(4)))
        end = datetime.datetime(year, month, day, to24(int(m.group(6)), m.group(8)),
                                int(m.group(7)))
        start = (start - datetime.timedelta(hours=off)).replace(tzinfo=datetime.timezone.utc)
        end = (end - datetime.timedelta(hours=off)).replace(tzinfo=datetime.timezone.utc)
        if end <= start:
            return None
        return start, end
    except (ValueError, OverflowError):
        return None


def extract_source(source: str, rules: str) -> str:
    """Resolution source URL: Gamma field first, else the venue URL named in
    the verified description (older events leave the field empty while the
    description names e.g. the Chainlink stream). Empty when neither names
    a known venue. Never invented: only URLs present in Gamma data."""
    if source.strip():
        return source
    try:
        m = re.search(r"https?://[^\s\"']*(?:chain\.link|binance\.com|coinbase\.com)[^\s\"']*",
                      rules, re.IGNORECASE)
    except re.error:
        return ""
    return m.group(0) if m else ""


def classify(asset: str, horizon: str, question: str, end_date, source: str, rules: str):
    """Returns (fidelity, start_iso, end_iso, effective_source)."""
    src = source.lower()
    text = (src + " " + rules.lower())
    has_venue = any(k in text for k in VENUES)
    if not (has_venue and len(rules) > 50):
        return ("PROXY" if (source or rules) else "UNKNOWN"), None, None, source
    qlow = question.lower()
    if asset == "BTC" and "bitcoin" not in qlow:
        return "PROXY", None, None, source
    if asset == "ETH" and "ethereum" not in qlow:
        return "PROXY", None, None, source
    try:
        year = end_date.year if hasattr(end_date, "year") else 2026
    except Exception:
        return "PROXY", None, None, source
    parsed = parse_window(question, year)
    if not parsed:
        return "PROXY", None, None, source
    wstart, wend = parsed
    try:
        dend = end_date if end_date.tzinfo else end_date.replace(tzinfo=datetime.timezone.utc)
    except Exception:
        return "PROXY", None, None, source
    if abs((dend - wend).total_seconds()) > 60:
        return "PROXY", None, None, source
    if HORIZON_SECS.get(horizon, -1) != int((wend - wstart).total_seconds()):
        return "PROXY", None, None, source
    return "EXACT", wstart.isoformat(), wend.isoformat(), extract_source(source, rules)


def fetch_market(evid: str, cid: str):
    """Returns (source, rules) for the market matching cid in the event."""
    data = curl_json(GAMMA_EVENT + evid)
    for m in data.get("markets", []) or []:
        if norm(m.get("conditionId")) == cid:
            return norm(m.get("resolutionSource")), norm(m.get("description"))[:4000]
    return "", ""


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--batch", default="50")
    ap.add_argument("--limit", default="0")
    ap.add_argument("--sleep", default="0.3")
    args = ap.parse_args()
    ensure_dirs()
    try:
        batch = int(args.batch)
        limit = int(args.limit)
        sleep_s = float(args.sleep)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"bad args: {exc}") from exc

    try:
        sel = pl.scan_parquet(str(PROCESSED / "selected_markets.parquet")).collect()
        sii = pl.scan_parquet(str(RAW / "sii" / "markets.parquet")).select(
            ["condition_id", "event_id", "end_date"]
        ).collect()
    except Exception as exc:
        raise RuntimeError(f"parquet read failed: {exc}") from exc

    try:
        event_of = dict(zip(sii["condition_id"].to_list(), sii["event_id"].to_list(), strict=True))
        end_of = dict(zip(sii["condition_id"].to_list(), sii["end_date"].to_list(), strict=True))
    except Exception as exc:
        raise RuntimeError(f"join build failed: {exc}") from exc

    rows = sel.to_dicts()
    if limit > 0:
        rows = rows[:limit]
    print(f"fetching {len(rows)} markets via SII event_id -> Gamma /events/<id>")
    specs = []
    matched = 0
    for i, r in enumerate(rows):
        cid = r.get("condition_id", "")
        source, rules = "", ""
        evid = norm(event_of.get(cid))
        if evid:
            try:
                source, rules = fetch_market(evid, cid)
                if rules:
                    matched += 1
            except RuntimeError as exc:
                print(f"warn {cid}: {exc}")
                time.sleep(sleep_s)
                try:
                    source, rules = fetch_market(evid, cid)
                    if rules:
                        matched += 1
                except RuntimeError as exc2:
                    print(f"warn retry {cid}: {exc2}")
        if rules:
            r["resolution_rules"] = rules
        if source:
            r["resolution_source"] = source
        fid, wstart, wend, esource = classify(
            norm(r.get("asset")), norm(r.get("horizon")), norm(r.get("question")),
            end_of.get(cid), source, rules,
        )
        if esource:
            r["resolution_source"] = esource
            source = esource
        if wstart:
            r["start_at"] = wstart
        if wend:
            r["end_at"] = wend
        r["reference_type"] = "WINDOW_OPEN"
        r["strike"] = ""
        specs.append({
            "condition_id": cid,
            "market_id": r.get("market_id", ""),
            "asset": r.get("asset", ""),
            "horizon": r.get("horizon", ""),
            "resolution_source": source,
            "rule_excerpt": rules[:1000],
            "fidelity": fid,
            "start_at": wstart or "",
            "end_at": wend or "",
            "reference_type": "WINDOW_OPEN",
        })
        if (i + 1) % batch == 0:
            print(f"  {i + 1}/{len(rows)} matched={matched}")
            time.sleep(1)
    print(f"gamma matched {matched}/{len(rows)}")
    counts = {}
    for s in specs:
        counts[s["fidelity"]] = counts.get(s["fidelity"], 0) + 1
    print("fidelity:", counts)
    try:
        if limit == 0:
            pl.DataFrame(rows).write_parquet(str(PROCESSED / "selected_markets.parquet"))
            pl.DataFrame(specs).write_parquet(str(PROCESSED / "resolution_specs.parquet"))
            print("wrote selected_markets + resolution_specs")
        else:
            print(f"--limit {limit}: dry run, nothing written")
    except Exception as exc:
        raise RuntimeError(f"write failed: {exc}") from exc
    m = load_manifest()
    record_file(
        m, "Polymarket-Gamma", "event-specs-exact-join",
        str(PROCESSED / "resolution_specs.parquet"), [], [],
        {"matched": matched, "of": len(rows), "fidelity": counts,
         "limit": limit, "method": "SII event_id -> Gamma /events/<id>, conditionId match"},
        rows_before=len(rows), rows_after=len(rows), market_count=len(rows),
    )
    save_manifest(m)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
