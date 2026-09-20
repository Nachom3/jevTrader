"""V3 question-form analysis: fair_p_yes and pressure vs drift and state.

Inputs: rows JSON (per-arm metrics + drift, no PnL criterion) + CONTROL
sidecar (states for features + market mid). Thresholds/questions frozen;
this only READS run outputs. Cutoffs in odd/tasks/question-v3.md.
"""

import argparse
import json
import math
import statistics
import sys

PRESSURE_BINS = [
    ("[-1.0,-0.6)", -1.0, -0.6),
    ("[-0.6,-0.2)", -0.6, -0.2),
    ("[-0.2,+0.2]", -0.2, 0.2),
    ("(+0.2,+0.6]", 0.2, 0.6),
    ("(+0.6,+1.0]", 0.6, 1.0),
]
FEATS = [
    "ofi_5s",
    "poly_ofi_5s",
    "perp_basis_pct",
    "distance_to_target_pct",
    "ret_5s_pct",
    "realized_vol_1m_pct",
    "move_zscore_1s",
]


def pearson(xs, ys):
    try:
        n = len(xs)
        if n < 3:
            return float("nan")
        mx, my = sum(xs) / n, sum(ys) / n
        cov = sum((x - mx) * (y - my) for x, y in zip(xs, ys, strict=True))
        vx = sum((x - mx) ** 2 for x in xs)
        vy = sum((y - my) ** 2 for y in ys)
        if vx <= 0 or vy <= 0:
            return float("nan")
        return cov / math.sqrt(vx * vy)
    except (ValueError, OverflowError):
        return float("nan")


def safe_float(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def quintiles(vals):
    try:
        if not vals:
            return []
        ordered = sorted(vals)
        return [ordered[int(len(ordered) * p)] for p in (0.2, 0.4, 0.6, 0.8)]
    except (TypeError, ValueError, IndexError):
        return []


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", required=True)
    ap.add_argument("--signals", default="")
    ap.add_argument("--out-md", required=True)
    ap.add_argument("--drift-overlay", default="")
    args = ap.parse_args()
    try:
        with open(args.rows) as f:
            rows = json.load(f)
    except (OSError, ValueError) as exc:
        raise RuntimeError(f"rows read failed: {exc}") from exc
    overlay = {}
    if args.drift_overlay:
        # Dense-label drift recomputation (see fix_strided_drift.py): wins
        # over the frozen rows' strided labels wherever present.
        try:
            with open(args.drift_overlay) as f:
                overlay = json.load(f).get("drift", {})
            print(f"drift_overlay entries={len(overlay)}")
        except (OSError, ValueError) as exc:
            raise RuntimeError(f"overlay read failed: {exc}") from exc
    states = {}
    if args.signals:
        try:
            with open(args.signals) as f:
                for rec in json.load(f):
                    if rec.get("variant") == "CONTROL":
                        states[rec["pair_id"]] = json.loads(rec["state_json"])
        except (OSError, ValueError) as exc:
            raise RuntimeError(f"signals read failed: {exc}") from exc

    fair, pres = [], []
    for r in rows:
        if r["variant"] == "FAIR_VALUE" and r.get("fair_p_yes") is not None:
            st = states.get(r["pair_id"], {})
            u = st.get("underlying", {})
            p = st.get("polymarket", {})
            mid = None
            if p.get("yes_bid") is not None and p.get("yes_ask") is not None:
                mid = (p["yes_bid"] + p["yes_ask"]) / 2e6
            feats = {}
            for f in FEATS:
                if f == "move_zscore_1s":
                    feats[f] = safe_float((st.get("quant") or {}).get(f, 0.0))
                else:
                    feats[f] = safe_float(u.get(f, 0.0))
            fair.append(
                {
                    "pair": r["pair_id"],
                    "fair": r["fair_p_yes"],
                    "mid": mid,
                    "drift": {
                        f"drift_{h}_pp": r.get(f"drift_{h}_pp")
                        for h in ["1s", "5s", "10s", "30s", "60s"]
                    },
                    "feats": feats,
                }
            )
        if r["variant"] == "PRESSURE_COMPOSITE" and r.get("pressure") is not None:
            st = states.get(r["pair_id"], {})
            u = st.get("underlying", {})
            feats = {}
            for f in FEATS:
                if f == "move_zscore_1s":
                    feats[f] = safe_float((st.get("quant") or {}).get(f, 0.0))
                else:
                    feats[f] = safe_float(u.get(f, 0.0))
            pres.append(
                {
                    "pair": r["pair_id"],
                    "pressure": r["pressure"],
                    "conf": r.get("pressure_confidence"),
                    "drift": {
                        f"drift_{h}_pp": r.get(f"drift_{h}_pp")
                        for h in ["1s", "5s", "10s", "30s", "60s"]
                    },
                    "feats": feats,
                }
            )
    print(f"fair={len(fair)} pressure={len(pres)}")
    if overlay:
        # Overlay wins wherever present (same (pair, variant) keys).
        keys = [f"drift_{h}_pp" for h in ["1s", "5s", "10s", "30s", "60s"]]
        applied = 0
        for e, variant in [(e, "FAIR_VALUE") for e in fair] + [
            (e, "PRESSURE_COMPOSITE") for e in pres
        ]:
            ov = overlay.get(e["pair"] + "\x00" + variant)
            if ov:
                e["drift"] = {k: ov.get(k) for k in keys}
                applied += 1
        print(f"drift_overlay applied={applied}")
    lines = ["# V3 question-form report", ""]
    # 1. sensitivity
    for label, vals in [
        ("fair_p_yes", [e["fair"] for e in fair]),
        ("pressure", [e["pressure"] for e in pres]),
    ]:
        std = statistics.pstdev(vals) if len(vals) > 1 else 0.0
        lines.append(f"- std({label}): {std:.3f} (n={len(vals)}; SENSIBLE iff >= 0.10)")
        if vals:
            lines.append(
                f"  range [{min(vals):.3f}, {max(vals):.3f}] "
                f"mean {statistics.mean(vals):.3f}"
            )
    lines.append("")
    # edge analysis for fair
    edges = [
        (e["fair"] - e["mid"], e["drift"].get("drift_5s_pp"))
        for e in fair
        if e["mid"] is not None
    ]
    edges = [(a, d) for a, d in edges if d is not None]
    if edges:
        lines.append(
            f"- edge=fair-mid: n={len(edges)} mean={sum(a for a, _ in edges) / len(edges):+.4f} "
            f"corr(edge,drift_5s)={pearson([a for a, _ in edges], [d for _, d in edges]):+.2f}"
        )
    else:
        lines.append("- edge: n/a (no mids or no drift)")
    lines.append("")
    # 2. internal monotonicity: metric vs features
    lines.append("## Pearson(metric, feature)")
    lines.append("| metric | " + " | ".join(FEATS) + " |")
    lines.append("|---|" + "|".join(["---"] * len(FEATS)) + "|")
    for label, vals, key in [
        ("fair_p_yes", [e["fair"] for e in fair], "fair"),
        ("pressure", [e["pressure"] for e in pres], "pressure"),
    ]:
        cells = []
        src = fair if key == "fair" else pres
        for f in FEATS:
            xs = [e[key] for e in src]
            ys = [e["feats"][f] for e in src]
            c = pearson(xs, ys)
            cells.append("n/a" if math.isnan(c) else f"{c:+.2f}")
        lines.append(f"| {label} | " + " | ".join(cells) + " |")
    lines.append("")
    # 3. incremental: arm output ranges vs CONTROL underreact band
    lines.append("## Incremental info vs CONTROL")
    lines.append(
        f"- CONTROL rows: n={len([r for r in rows if r['variant'] == 'CONTROL'])} "
        "(underreact band from diagnostic: 0.23-0.42)"
    )
    lines.append(
        "- FAIR_VALUE fair range above; PRESSURE range above (vs underreact band)"
    )
    lines.append("")
    # 4. prediction: quintile buckets + user pressure bins vs drift horizons
    lines.append("## Prediction: signal buckets vs future drift")
    for label, key in [("fair_p_yes", "fair"), ("pressure", "pressure")]:
        src = fair if key == "fair" else pres
        vals = sorted(e[key] for e in src)
        if not vals:
            lines.append(f"- {label}: n=0")
            continue
        qs = quintiles(vals)
        for h in ["1s", "5s", "30s"]:
            cells = []
            for b in range(5):
                ds = [
                    e["drift"].get(f"drift_{h}_pp")
                    for e in src
                    if e["drift"].get(f"drift_{h}_pp") is not None
                    and sum(1 for t in qs if e[key] > t) == b
                ]
                cells.append(f"n={len(ds)} m={sum(ds) / len(ds):+.3f}" if ds else "n=0")
            lines.append(f"- {label} x drift_{h}: " + " | ".join(cells))
        alld = [(e[key], e["drift"].get("drift_5s_pp")) for e in src]
        alld = [(a, d) for a, d in alld if d is not None]
        c = (
            pearson([a for a, _ in alld], [d for _, d in alld])
            if len(alld) >= 3
            else math.nan
        )
        lines.append(
            f"- corr({label}, drift_5s) = "
            + ("n/a" if math.isnan(c) else f"{c:+.2f}")
            + f" (n={len(alld)})"
        )
    lines.append("")
    # user pressure bins
    pb = [(e["pressure"], e["drift"].get("drift_5s_pp")) for e in pres]
    pb = [(a, d) for a, d in pb if d is not None]
    lines.append("## Pressure user buckets vs drift_5s")
    for name, lo, hi in PRESSURE_BINS:
        ds = [d for a, d in pb if (a >= lo and a < hi) or (hi == 1.0 and a == hi)]
        lines.append(
            f"- {name}: " + (f"n={len(ds)} m={sum(ds) / len(ds):+.3f}" if ds else "n=0")
        )
    lines.append("")
    lines.append("## Verdict (rama 1/2/3)")
    lines.append("- RAMA 1 (quote engine): sensible + predictivo por cutoffs.")
    lines.append("- RAMA 2 (mueve, no predice): problema preguntas/interpretacion.")
    lines.append(
        "- RAMA 3 (ni mueve): cuestionar Jev en loop direccional vs regimen/contexto."
    )
    try:
        with open(args.out_md, "w") as f:
            f.write("\n".join(lines) + "\n")
        print(f"wrote {args.out_md}")
    except OSError as exc:
        raise RuntimeError(f"md write failed: {exc}") from exc


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
