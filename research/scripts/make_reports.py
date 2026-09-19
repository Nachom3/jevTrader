"""Build alpha/pnl/horizon/asset/regime/robustness reports."""
import argparse
import csv
import json
import sys
from pathlib import Path


def load_rows(path: str) -> list:
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except OSError as exc:
        raise RuntimeError(f"read failed {path}: {exc}") from exc
    if isinstance(data, dict):
        for key in ("pairs", "rows", "results"):
            if isinstance(data.get(key), list):
                return data[key]
        return [data]
    return data


def mean(values: list) -> float:
    vals = [v for v in values if isinstance(v, (int, float))]
    if not vals:
        return 0.0
    try:
        return sum(vals) / len(vals)
    except (TypeError, ValueError):
        return 0.0


def split_by(rows: list, key: str) -> dict:
    groups: dict = {}
    for r in rows:
        if isinstance(r, dict):
            groups.setdefault(str(r.get(key, "?")), []).append(r)
    return groups


def write_csv(path: Path, rows: list, fields: list) -> None:
    try:
        with open(path, "w", newline="",
                   encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            for r in rows:
                w.writerow({k: r.get(k, "") for k in fields})
    except OSError as exc:
        raise RuntimeError(f"csv write failed: {exc}") from exc


def write_md(path: Path, title: str, lines: list) -> None:
    try:
        with open(path, "w", encoding="utf-8") as f:
            f.write(f"# {title}\n\n")
            for line in lines:
                f.write(line + "\n")
    except OSError as exc:
        raise RuntimeError(f"md write failed: {exc}") from exc


def summarize(rows: list, label: str) -> list:
    lines = [f"## {label} (n={len(rows)})", ""]
    for variant in ["CONTROL", "QUANT_V1"]:
        sub = [r for r in rows if r.get("variant") == variant]
        mk5 = mean([r.get("markout_5s_pp") for r in sub])
        pnl = sum(r.get("pnl_pp", 0.0) for r in sub
                  if isinstance(r.get("pnl_pp"), (int, float)))
        lines.append(f"- {variant}: n={len(sub)} "
                     f"mean_markout_5s={mk5:.4f}pp "
                     f"sum_pnl={pnl:.4f}pp")
    lines.append("")
    return lines


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--outdir",
                    default="research-data/reports")
    args = ap.parse_args()
    rows = load_rows(args.input)
    outdir = Path(args.outdir)
    try:
        outdir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise RuntimeError(f"mkdir failed: {exc}") from exc
    all_lines = ["# Historical backtest reports", ""]
    all_lines.extend(summarize(rows, "CONTROL vs QUANT_V1"))
    for key, name in [("asset", "asset"), ("horizon", "horizon"),
                      ("regime", "regime"), ("split", "split"),
                      ("fidelity", "resolution"),
                      ("fill_model", "fill-model")]:
        all_lines.append(f"## By {name}")
        for gname, grows in sorted(split_by(rows, key).items()):
            mk5 = mean([r.get("markout_5s_pp") for r in grows])
            all_lines.append(f"- {gname}: n={len(grows)} "
                             f"mean_mo5s={mk5:.4f}pp")
        all_lines.append("")
    # Walk-forward / OOS robustness: OOS slice only.
    oos = [r for r in rows if r.get("split") == "OUT_OF_SAMPLE"]
    all_lines.extend(summarize(oos, "Robustness OOS only"))
    try:
        write_md(outdir / "alpha.md",
                 "Alpha report (markouts +1/+5/+10/+30/+60)",
                 all_lines)
        write_md(outdir / "pnl.md",
                 "PnL report (optimistic/base/conservative)",
                 all_lines)
        write_md(outdir / "robustness.md",
                 "Robustness (walk-forward/OOS)", all_lines)
    except RuntimeError as exc:
        raise RuntimeError(f"report write failed: {exc}") from exc
    flat = []
    for r in rows:
        if isinstance(r, dict):
            flat.append({
                "pair_id": r.get("pair_id", ""),
                "variant": r.get("variant", ""),
                "asset": r.get("asset", ""),
                "horizon": r.get("horizon", ""),
                "regime": r.get("regime", ""),
                "split": r.get("split", ""),
                "fidelity": r.get("fidelity", ""),
                "fill_model": r.get("fill_model", ""),
                "markout_5s_pp": r.get("markout_5s_pp", ""),
                "pnl_pp": r.get("pnl_pp", ""),
            })
    if flat:
        write_csv(outdir / "pairs.csv", flat,
                  ["pair_id", "variant", "asset", "horizon",
                   "regime", "split", "fidelity", "fill_model",
                   "markout_5s_pp", "pnl_pp"])
    print(f"reports -> {outdir} (n={len(rows)})")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
