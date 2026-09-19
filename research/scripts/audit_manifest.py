"""Audit + repair manifest lineage: every raw file gets an entry."""

import argparse
import sys

from common import RAW, load_manifest, record_file, save_manifest

DATASET_OF = [
    ("sii/markets.parquet", "SII-WANGZJ/Polymarket_data"),
    ("sii/", "SII-WANGZJ/Polymarket_data"),
    ("timeseventeen/", "TimeSeventeen/Polymarket-v1"),
    ("binance/", "binance-vision"),
    ("coinbase/", "coinbase"),
    ("deribit/", "deribit"),
]


def dataset_for(rel: str) -> str:
    for prefix, name in DATASET_OF:
        if prefix in rel:
            return name
    return "unknown"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repair", action="store_true")
    args = ap.parse_args()
    m = load_manifest()
    try:
        raw_files = sorted(p for p in RAW.rglob("*") if p.is_file())
    except OSError as exc:
        raise RuntimeError(f"glob failed: {exc}") from exc
    known = set()
    for entry in m.get("files", []):
        try:
            known.add(entry.get("local_path", ""))
        except AttributeError:
            continue
    missing = []
    base = RAW.parent.parent
    for p in raw_files:
        try:
            rel = str(p.relative_to(base))
        except ValueError:
            rel = str(p)
        if rel not in known:
            missing.append((rel, str(p)))
    print(f"raw files={len(raw_files)} manifest entries={len(m.get('files', []))}")
    print(f"unmanifested={len(missing)}")
    for rel, _ in missing[:20]:
        print(f"  MISSING {rel}")
    if args.repair and missing:
        for rel, full in missing:
            record_file(
                m,
                dataset_for(rel),
                rel.split("/")[-1],
                full,
                [],
                [],
                {"mode": "audit-repair", "note": "added by audit_manifest"},
            )
        # Drop exact-duplicate entries (same local_path + checksum).
        seen, deduped, dupes = set(), [], 0
        for entry in m.get("files", []):
            try:
                key = (entry.get("local_path"), entry.get("checksum_sha256"))
            except AttributeError:
                continue
            if key in seen:
                dupes += 1
                continue
            seen.add(key)
            deduped.append(entry)
        m["files"] = deduped
        save_manifest(m)
        print(f"repaired: added={len(missing)} dupes_removed={dupes}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
