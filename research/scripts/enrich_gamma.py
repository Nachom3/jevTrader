"""Enrich selected markets with Gamma rules (light JSON, batched)."""

import argparse
import json
import subprocess
import sys

import polars as pl

from common import (
    PROCESSED,
    ensure_dirs,
    load_manifest,
    record_file,
    save_manifest,
)

SEARCH = "https://gamma-api.polymarket.com/public-search"


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


def fetch_slug(slug: str) -> dict:
    import urllib.parse

    data = curl_json(SEARCH + "?q=" + urllib.parse.quote(slug))
    for event in data.get("events", []) or []:
        for market in event.get("markets", []) or []:
            if market.get("slug") == slug:
                return market
        if event.get("slug") == slug:
            markets = event.get("markets", []) or []
            if markets:
                return markets[0]
    return {}


def fetch_batch(ids: list) -> list:
    return []


def norm(value) -> str:
    try:
        return "" if value is None else str(value)
    except Exception:
        return ""


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--batch", default="50")
    args = ap.parse_args()
    ensure_dirs()
    try:
        batch = int(args.batch)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"bad batch: {exc}") from exc
    sel_path = PROCESSED / "selected_markets.parquet"
    try:
        sel = pl.scan_parquet(str(sel_path)).collect()
    except Exception as exc:
        raise RuntimeError(f"selected read failed: {exc}") from exc
    import time

    slugs = sel["slug"].to_list()
    cids = sel["condition_id"].to_list()
    print(f"enriching {len(slugs)} markets via Gamma public-search")
    rules: dict = {}
    for i, (slug, cid) in enumerate(zip(slugs, cids, strict=True)):
        try:
            m = fetch_slug(norm(slug))
            if m:
                rules[cid] = {
                    "rules": norm(m.get("description"))[:4000],
                    "source": norm(m.get("resolutionSource")),
                    "start": norm(m.get("startDate")),
                    "end": norm(m.get("endDate")),
                }
        except RuntimeError as exc:
            print(f"warn {slug}: {exc}")
        if (i + 1) % batch == 0:
            print(f"  {i + 1}/{len(slugs)} matched={len(rules)}")
            time.sleep(1)
    print(f"gamma matched {len(rules)}/{len(slugs)}")
    rows = sel.to_dicts()
    for r in rows:
        g = rules.get(r["condition_id"], {})
        if g.get("rules"):
            r["resolution_rules"] = g["rules"]
        if g.get("source"):
            r["resolution_source"] = g["source"]
        if g.get("start"):
            r["start_at"] = g["start"]
    specs = []
    for r in rows:
        src = str(r.get("resolution_source", "")).lower()
        rl = str(r.get("resolution_rules", ""))
        has_src = any(k in src for k in ("binance", "coinbase", "chainlink"))
        if (has_src or "reference" in rl.lower()) and len(rl) > 50:
            fid = "EXACT"
        elif rl or src:
            fid = "PROXY"
        else:
            fid = "UNKNOWN"
        specs.append(
            {
                "condition_id": r["condition_id"],
                "market_id": r["market_id"],
                "asset": r["asset"],
                "horizon": r["horizon"],
                "resolution_source": r.get("resolution_source", ""),
                "rule_excerpt": rl[:1000],
                "fidelity": fid,
            }
        )
    try:
        pl.DataFrame(rows).write_parquet(str(sel_path))
        pl.DataFrame(specs).write_parquet(str(PROCESSED / "resolution_specs.parquet"))
    except Exception as exc:
        raise RuntimeError(f"write failed: {exc}") from exc
    print(pl.DataFrame(specs).group_by("fidelity").len())
    m = load_manifest()
    record_file(
        m,
        "Polymarket-Gamma",
        "gamma-enrichment",
        str(sel_path),
        [],
        [],
        {"matched": len(rules), "of": len(rows)},
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
