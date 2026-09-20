"""Offline diagnosis of Jev answers: distribution, state-variance, C-vs-Q, drift links.

Inputs: signals sidecar (verbatim state+signal per eval) + rows JSON (drift).
No thresholds/questions/features are touched; this only READS run outputs.
Pre-registered cutoffs live in odd/tasks/jev-diagnostic-v1.md; this script
applies them and prints which hypothesis survives.

Questions (8): 5 noul + repricing collapsed to p_up_ge_1 + 2 fill noul.
Features (fixed list, no fishing): ret_5s/1m, realized_vol_1m,
distance_to_target, time_remaining, move_zscore_1s (quant), ofi_5s, spread.
"""

import argparse
import json
import math
import statistics
import sys
from collections import defaultdict

QUESTIONS = [
    "yes_pressure_5s",
    "no_pressure_5s",
    "move_persists",
    "underreact_up",
    "underreact_down",
    "p_up",
    "fill_before_decay",
    "fill_toxic",
]
FEATURES = [
    "ret_5s_pct",
    "ret_1m_pct",
    "realized_vol_1m_pct",
    "distance_to_target_pct",
    "time_remaining_secs",
    "move_zscore_1s",
    "ofi_5s",
    "spread",
]
DRIFTS = ["drift_1s_pp", "drift_5s_pp", "drift_10s_pp", "drift_30s_pp", "drift_60s_pp"]


def pearson(xs, ys):
    try:
        n = len(xs)
        if n < 3:
            return float("nan")
        mx, my = sum(xs) / n, sum(ys) / n
        cov = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
        vx = sum((x - mx) ** 2 for x in xs)
        vy = sum((y - my) ** 2 for y in ys)
        if vx <= 0 or vy <= 0:
            return float("nan")
        return cov / math.sqrt(vx * vy)
    except (ValueError, OverflowError):
        return float("nan")


def answers_of(rec):
    try:
        s = rec["signal"]
        r = s["repricing"]
        return {
            "yes_pressure_5s": float(s["yes_pressure_5s"]),
            "no_pressure_5s": float(s["no_pressure_5s"]),
            "move_persists": float(s["move_persists"]),
            "underreact_up": float(s["underreact_up"]),
            "underreact_down": float(s["underreact_down"]),
            "p_up": float(r["up_1"]) + float(r["up_2"]) + float(r["up_3_plus"]),
            "fill_before_decay": float(s["fill_before_decay"]),
            "fill_toxic": float(s["fill_toxic"]),
        }
    except (KeyError, TypeError, ValueError):
        return {}


def features_of(rec):
    try:
        st = json.loads(rec["state_json"])
        u = st.get("underlying", {})
        p = st.get("polymarket", {})
        q = st.get("quant") or {}
        spread = p.get("spread")
        if (
            spread is None
            and p.get("yes_bid") is not None
            and p.get("yes_ask") is not None
        ):
            spread = (p["yes_ask"] - p["yes_bid"]) / 1e6
        return {
            "ret_5s_pct": float(u.get("ret_5s_pct", 0.0)),
            "ret_1m_pct": float(u.get("ret_1m_pct", 0.0)),
            "realized_vol_1m_pct": float(u.get("realized_vol_1m_pct", 0.0)),
            "distance_to_target_pct": float(u.get("distance_to_target_pct", 0.0)),
            "time_remaining_secs": float(u.get("time_remaining_secs", 0.0)),
            "move_zscore_1s": float(q.get("move_zscore_1s", 0.0)),
            "ofi_5s": float(u.get("ofi_5s", 0.0)),
            "spread": float(spread or 0.0),
        }
    except (KeyError, TypeError, ValueError):
        return {}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--signals", required=True)
    ap.add_argument("--rows", required=True)
    ap.add_argument("--out-md", required=True)
    ap.add_argument(
        "--probe",
        default="0x1539ce8dfb340c1fc8e473628e8b0380da5fa99fd60471e8a27596923484e91f",
    )
    args = ap.parse_args()

    try:
        sigs = json.load(open(args.signals))
        rows = json.load(open(args.rows))
    except (OSError, ValueError) as exc:
        raise RuntimeError(f"input read failed: {exc}") from exc

    drift_of = {(r["pair_id"], r["variant"]): r for r in rows}
    evals = []
    for rec in sigs:
        if not rec.get("live"):
            continue
        ans = answers_of(rec)
        feats = features_of(rec)
        if len(ans) != 8 or len(feats) != 8:
            continue
        row = drift_of.get((rec["pair_id"], rec["variant"]), {})
        evals.append(
            {
                "pair_id": rec["pair_id"],
                "variant": rec["variant"],
                "market_id": rec.get("market_id", ""),
                "answers": ans,
                "feats": feats,
                "drift": {h: row.get(h) for h in DRIFTS},
            }
        )
    print(f"evals={len(evals)} (live, complete)")
    if not evals:
        raise RuntimeError("no usable evaluations")

    lines = ["# Jev diagnostic report", ""]
    # 1-2. distribution + variance (overall, within/between conditions)
    lines.append("## Per-question distribution and variance")
    lines.append(
        "| question | n | mean | std | min | max | std_within_cond | std_between_cond |"
    )
    lines.append("|---|---|---|---|---|---|---|---|")
    stds = {}
    for q in QUESTIONS:
        vals = [e["answers"][q] for e in evals]
        by_cond = defaultdict(list)
        for e in evals:
            by_cond[e["market_id"]].append(e["answers"][q])
        within = statistics.mean(
            [statistics.pstdev(v) for v in by_cond.values() if len(v) > 1] or [0.0]
        )
        between = (
            statistics.pstdev([statistics.mean(v) for v in by_cond.values()])
            if len(by_cond) > 1
            else 0.0
        )
        stds[q] = statistics.pstdev(vals) if len(vals) > 1 else 0.0
        lines.append(
            f"| {q} | {len(vals)} | {statistics.mean(vals):.3f} | {stds[q]:.3f} | "
            f"{min(vals):.3f} | {max(vals):.3f} | {within:.3f} | {between:.3f} |"
        )
    lines.append("")
    # 3. correlation with features
    lines.append("## Pearson(answer, feature) — fixed feature list")
    lines.append("| question | " + " | ".join(FEATURES) + " |")
    lines.append("|---|" + "|".join(["---"] * len(FEATURES)) + "|")
    for q in QUESTIONS:
        cells = []
        for f in FEATURES:
            xs = [e["answers"][q] for e in evals]
            ys = [e["feats"][f] for e in evals]
            c = pearson(xs, ys)
            cells.append("n/a" if math.isnan(c) else f"{c:+.2f}")
        lines.append(f"| {q} | " + " | ".join(cells) + " |")
    lines.append("")
    # 4. monotonicity: answer quintile buckets -> mean drift_5s
    lines.append("## Monotonicity: mean drift_5s by answer bucket")
    for q in QUESTIONS:
        vals = sorted(e["answers"][q] for e in evals)
        qs = [vals[int(len(vals) * p)] for p in (0.2, 0.4, 0.6, 0.8)]
        buckets = []
        for e in evals:
            a, d = e["answers"][q], e["drift"].get("drift_5s_pp")
            if d is None:
                continue
            b = sum(1 for t in qs if a > t)
            buckets.append((b, d))
        cells = []
        for b in range(5):
            ds = [d for bb, d in buckets if bb == b]
            cells.append(f"n={len(ds)} m={sum(ds) / len(ds):+.3f}" if ds else "n=0")
        lines.append(f"- {q}: " + " | ".join(cells))
    lines.append("")
    # 5. QUANT-CONTROL paired deltas per question
    lines.append("## QUANT-CONTROL paired deltas per question")
    lines.append(
        "| question | n_pairs | mean_abs_delta | P(abs>=0.10) | sign_agreement |"
    )
    lines.append("|---|---|---|---|---|")
    by_pair = defaultdict(dict)
    for e in evals:
        by_pair[e["pair_id"]][e["variant"]] = e
    dq = {}
    for q in QUESTIONS:
        deltas = []
        for _, v in by_pair.items():
            if "CONTROL" in v and "QUANT_V1" in v:
                deltas.append(v["QUANT_V1"]["answers"][q] - v["CONTROL"]["answers"][q])
        mad = sum(abs(d) for d in deltas) / len(deltas) if deltas else 0.0
        big = [d for d in deltas if abs(d) >= 0.05]
        agree = (sum(1 for d in big if d > 0) / len(big)) if big else float("nan")
        agree_s = "n/a" if math.isnan(agree) else f"{max(agree, 1 - agree):.2f}"
        pbig = sum(1 for d in deltas if abs(d) >= 0.10) / len(deltas) if deltas else 0.0
        dq[q] = (mad, pbig, agree_s)
        lines.append(f"| {q} | {len(deltas)} | {mad:.3f} | {pbig:.2f} | {agree_s} |")
    lines.append("")
    # 6. sensitivity: probe market vs rest
    lines.append("## Sensitivity: probe market vs rest (mean answer)")
    lines.append("| question | probe_mean | rest_mean | probe_std | rest_std |")
    lines.append("|---|---|---|---|---|")
    for q in QUESTIONS:
        pa = [e["answers"][q] for e in evals if args.probe in e["pair_id"]]
        ra = [e["answers"][q] for e in evals if args.probe not in e["pair_id"]]
        pm = statistics.mean(pa) if pa else float("nan")
        rm = statistics.mean(ra) if ra else float("nan")
        ps = statistics.pstdev(pa) if len(pa) > 1 else 0.0
        rs = statistics.pstdev(ra) if len(ra) > 1 else 0.0
        lines.append(
            f"| {q} | {pm:.3f} (n={len(pa)}) | {rm:.3f} (n={len(ra)}) | {ps:.3f} | {rs:.3f} |"
        )
    lines.append("")
    # 7. per-condition means (similarity across very different states)
    lines.append("## Per-condition answer means")
    conds = sorted(set(e["market_id"] for e in evals))
    lines.append("| market | " + " | ".join(QUESTIONS) + " |")
    lines.append("|---|" + "|".join(["---"] * len(QUESTIONS)) + "|")
    for c in conds:
        cells = []
        for q in QUESTIONS:
            v = [e["answers"][q] for e in evals if e["market_id"] == c]
            cells.append(f"{statistics.mean(v):.3f}" if v else "n/a")
        lines.append(f"| {c[:28]} | " + " | ".join(cells) + " |")
    lines.append("")
    # Verdict vs pre-registered cutoffs
    flat = sum(1 for q in QUESTIONS if stds[q] < 0.05)
    varied = sum(1 for q in QUESTIONS if stds[q] >= 0.10)
    q_big = sum(1 for q in QUESTIONS if dq[q][0] >= 0.05)
    lines.append("## Verdict vs pre-registered cutoffs")
    lines.append(f"- std<0.05 (flat): {flat}/8 questions")
    lines.append(f"- std>=0.10 (varies with state): {varied}/8 questions")
    lines.append(f"- mean|QUANT-CONTROL|>=0.05: {q_big}/8 questions")
    lines.append("- H1 (ignores state): supported iff flat>=6/8")
    lines.append("- H2 (state yes, quant no): varied>=4/8 AND q_big==0")
    lines.append(
        "- H3 (uses both): varied>=4/8 AND >=2/8 with mean|d|>=0.10, sign consistent"
    )
    lines.append(
        "- H4 (reacts, no markout): H2/H3 signals AND |corr(answer,drift_5s)|<0.15 all"
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
