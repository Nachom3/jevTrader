"""BTC/ETH 5m/15m/1h/4h discovery from markets.parquet."""

import argparse
import contextlib
import re
import sys

import polars as pl
import yaml

from common import (
    budget_guard,
    ensure_dirs,
    load_manifest,
    record_file,
    save_manifest,
)


def load_cfg(path: str) -> dict:
    try:
        with open(path, encoding="utf-8") as f:
            return yaml.safe_load(f)
    except OSError as exc:
        raise RuntimeError(f"config read failed: {exc}") from exc


def norm_text(value) -> str:
    try:
        if value is None:
            return ""
        return str(value).lower()
    except Exception:
        return ""


def guess_asset(*texts: str) -> str:
    blob = " ".join(norm_text(t) for t in texts)
    has_btc = "btc" in blob or "bitcoin" in blob
    has_eth = "eth" in blob or "ethereum" in blob
    if has_btc and not has_eth:
        return "BTC"
    if has_eth and not has_btc:
        return "ETH"
    return ""


def horizon_table() -> dict:
    return {
        "M5": ("5m", "5min", "5mins", "5minute", "5minutes"),
        "M15": ("15m", "15min", "15mins", "15minute", "15minutes"),
        "H1": ("1h", "1hr", "1hour", "1hours"),
        "H4": ("4h", "4hr", "4hour", "4hours"),
    }


def token_horizon(blob: str) -> str:
    names = {"M5": "5m", "M15": "15m", "H1": "1h", "H4": "4h"}
    try:
        tokens = [x for x in re.split(r"[^a-z0-9]+", blob) if x]
    except re.error:
        return ""
    table = horizon_table()
    for tok in tokens:
        for tag, keys in table.items():
            if tok in keys:
                return names[tag]
    for i in range(len(tokens) - 1):
        joined = tokens[i] + tokens[i + 1]
        for tag, keys in table.items():
            if joined in keys:
                return names[tag]
    return ""


def guess_horizon(*texts: str, duration_s=None) -> str:
    blob = " ".join(norm_text(t) for t in texts)
    hit = token_horizon(blob)
    if hit:
        return hit
    try:
        if duration_s is not None:
            secs = float(duration_s)
            for tag, target in [
                ("5m", 300),
                ("15m", 900),
                ("1h", 3600),
                ("4h", 14400),
            ]:
                if abs(secs - target) / target < 0.05:
                    return tag
    except (TypeError, ValueError):
        pass
    return ""


def guess_kind(*texts: str) -> str:
    blob = " ".join(norm_text(t) for t in texts)
    if "up or down" in blob or "updown" in blob or "up-down" in blob:
        return "UPDOWN"
    if "below" in blob or "under" in blob or "less than" in blob:
        return "BELOW"
    return "ABOVE"


def parse_budget(cfg: dict) -> float:
    try:
        return float(cfg.get("research_max_download_gb", 8.0))
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"bad budget: {exc}") from exc


def parse_count(value, name: str) -> int:
    try:
        out = int(value)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"bad {name}: {exc}") from exc
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="research/config/corpus.yaml")
    ap.add_argument("--markets", default="research-data/raw/sii/markets.parquet")
    ap.add_argument("--out", default="research-data/processed/selected_markets.parquet")
    ap.add_argument(
        "--specs-out", default="research-data/processed/resolution_specs.parquet"
    )
    args = ap.parse_args()
    cfg = load_cfg(args.config)
    ensure_dirs()
    budget_guard(parse_budget(cfg))
    src = args.markets
    try:
        lf = pl.scan_parquet(src)
        cols = lf.collect_schema().names()
    except Exception as exc:
        raise RuntimeError(f"scan failed {src}: {exc}") from exc
    print(f"markets cols: {cols}")
    want = [
        "question",
        "slug",
        "eventTitle",
        "eventSlug",
        "conditionId",
        "condition_id",
        "marketId",
        "market_id",
        "yesTokenId",
        "noTokenId",
        "startTime",
        "endTime",
        "resolutionSource",
        "resolution_source",
        "rules",
        "description",
        "outcomes",
        "volume",
        "liquidity",
    ]
    pick = [c for c in want if c in cols]
    try:
        df = pl.scan_parquet(src).select(pick).collect()
    except Exception as exc:
        raise RuntimeError(f"collect failed: {exc}") from exc
    print(f"markets rows: {len(df)}")
    ren = {}
    for c in df.columns:
        low = c.lower()
        if "condition" in low and "id" in low:
            ren[c] = "condition_id"
        elif low in ("marketid", "market_id", "id"):
            ren[c] = "market_id"
    with contextlib.suppress(Exception):
        df = df.rename(ren)
    rows = []
    for r in df.to_dicts():
        q = r.get("question", "")
        slug = r.get("slug", "")
        evt = r.get("eventTitle", "") or r.get("eventSlug", "")
        asset = guess_asset(q, slug, evt)
        if asset not in ("BTC", "ETH"):
            continue
        dur = None
        try:
            s = r.get("startTime")
            e = r.get("endTime")
            if s is not None and e is not None:
                dur = float(e) - float(s)
                if dur > 1e12:
                    dur = dur / 1000.0
        except (TypeError, ValueError):
            dur = None
        horizon = guess_horizon(q, slug, evt, duration_s=dur)
        if horizon not in ("5m", "15m", "1h", "4h"):
            continue
        cid = str(r.get("condition_id", ""))
        if not cid:
            continue
        reason = f"asset={asset} horizon={horizon} via text+dur"
        rows.append(
            {
                "market_id": str(r.get("market_id", cid)),
                "condition_id": cid,
                "event_id": str(r.get("eventId", r.get("event_id", ""))),
                "yes_token_id": str(r.get("yesTokenId", r.get("yes_token_id", ""))),
                "no_token_id": str(r.get("noTokenId", r.get("no_token_id", ""))),
                "asset": asset,
                "horizon": horizon,
                "question": str(q),
                "slug": str(slug),
                "resolution_rules": str(r.get("rules", r.get("description", "")))[
                    :4000
                ],
                "resolution_source": str(
                    r.get("resolutionSource", r.get("resolution_source", ""))
                ),
                "duration_s": dur,
                "source_dataset": "SII-WANGZJ/Polymarket_data",
                "selection_reason": reason,
                "market_kind": guess_kind(q, slug, evt),
            }
        )
    if not rows:
        print("WARN: no BTC/ETH buckets found")
    sel = pl.DataFrame(rows) if rows else pl.DataFrame()
    try:
        cap = int(cfg.get("max_markets_per_bucket", 60))
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"bad cap: {exc}") from exc
    if len(sel) > 0:
        parts = []
        for _keys, grp in sel.group_by(["asset", "horizon"]):
            parts.append(grp.head(cap))
        sel = pl.concat(parts)
    with contextlib.suppress(Exception):
        sel = sel.sort(["asset", "horizon", "market_id"])
    splits = cfg.get("splits", {})
    try:
        f_exp = float(splits.get("EXPLORATION", 0.5))
        f_val = float(splits.get("VALIDATION", 0.25))
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"bad splits: {exc}") from exc
    n = len(sel)
    n_exp = parse_count(n * f_exp, "n_exp")
    n_val = parse_count(n * f_val, "n_val")
    labels = (
        ["EXPLORATION"] * n_exp
        + ["VALIDATION"] * n_val
        + ["OUT_OF_SAMPLE"] * max(0, n - n_exp - n_val)
    )
    if n > 0:
        sel = sel.with_columns(pl.Series("split", labels[:n]))
    specs = []
    if len(sel) > 0:
        for r in sel.to_dicts():
            src_txt = norm_text(r.get("resolution_source", ""))
            rules = norm_text(r.get("resolution_rules", ""))
            has_src = (
                "binance" in src_txt or "coinbase" in src_txt or "chainlink" in src_txt
            )
            if has_src or "reference" in rules:
                label = "EXACT" if len(rules) > 50 else "PROXY"
            else:
                label = "UNKNOWN"
            specs.append(
                {
                    "condition_id": r["condition_id"],
                    "market_id": r["market_id"],
                    "asset": r["asset"],
                    "horizon": r["horizon"],
                    "resolution_source": r["resolution_source"],
                    "rule_excerpt": r["resolution_rules"][:1000],
                    "fidelity": label,
                }
            )
    try:
        sel.write_parquet(args.out)
        pl.DataFrame(specs).write_parquet(args.specs_out)
    except Exception as exc:
        raise RuntimeError(f"write failed: {exc}") from exc
    print(f"selected={len(sel)} -> {args.out}")
    m = load_manifest()
    record_file(
        m,
        "SII-WANGZJ/Polymarket_data",
        "selected_markets.parquet",
        args.out,
        [],
        [],
        {"mode": "discovery-btc-eth-horizons"},
        rows_before=None,
        rows_after=len(sel),
        market_count=len(sel),
    )
    record_file(
        m,
        "SII-WANGZJ/Polymarket_data",
        "resolution_specs.parquet",
        args.specs_out,
        [],
        [],
        {"mode": "fidelity-exact-proxy-unknown"},
        rows_before=None,
        rows_after=len(specs),
        market_count=len(specs),
    )
    save_manifest(m)
    if len(sel) > 0:
        print(sel.group_by(["asset", "horizon", "split"]).len())
        print(pl.DataFrame(specs).group_by("fidelity").len())


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
