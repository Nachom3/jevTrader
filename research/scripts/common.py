"""Shared helpers: budget guard, manifest, checksums, paths."""

import contextlib
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

RESEARCH = Path(__file__).resolve().parents[1]
ROOT = RESEARCH.parent
RAW = ROOT / "research-data" / "raw"
PROCESSED = ROOT / "research-data" / "processed"
REPORTS = ROOT / "research-data" / "reports"
MANIFEST = ROOT / "research-data" / "manifest.json"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path: Path) -> str:
    try:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(8 << 20), b""):
                h.update(chunk)
        return h.hexdigest()
    except (OSError, ValueError) as exc:
        raise RuntimeError(f"checksum failed for {path}: {exc}") from exc


def raw_bytes() -> int:
    total = 0
    try:
        if RAW.exists():
            for p in RAW.rglob("*"):
                try:
                    if p.is_file():
                        total += p.stat().st_size
                except OSError:
                    continue
    except OSError:
        return total
    return total


def budget_guard(max_gb: float) -> None:
    used_gb = raw_bytes() / (1024**3)
    if used_gb > max_gb:
        sys.exit(
            f"BUDGET EXCEEDED: raw/ uses {used_gb:.2f}GB "
            f"> RESEARCH_MAX_DOWNLOAD_GB={max_gb}"
        )


def load_manifest() -> dict:
    try:
        if MANIFEST.exists():
            return json.loads(MANIFEST.read_text())
    except (OSError, ValueError) as exc:
        print(f"warn: manifest unreadable, starting fresh: {exc}")
    return {
        "created_at": utc_now(),
        "files": [],
        "notes": [],
    }


def save_manifest(m: dict) -> None:
    try:
        m["updated_at"] = utc_now()
        MANIFEST.write_text(json.dumps(m, indent=2))
    except OSError as exc:
        raise RuntimeError(f"manifest write failed: {exc}") from exc


def record_file(
    m: dict,
    dataset: str,
    original_file: str,
    local_path: str,
    date_range: list,
    selected_range: list,
    filters: dict,
    rows_before=None,
    rows_after=None,
    market_count=None,
) -> dict:
    p = Path(local_path)
    try:
        checksum = sha256_file(p) if p.exists() else None
    except RuntimeError:
        checksum = None
    try:
        size = p.stat().st_size if p.exists() else None
    except OSError:
        size = None
    rel = str(p)
    with contextlib.suppress(ValueError):
        rel = str(p.relative_to(ROOT))
    entry = {
        "dataset": dataset,
        "original_file": original_file,
        "local_path": rel,
        "downloaded_at": utc_now(),
        "checksum_sha256": checksum,
        "file_size_bytes": size,
        "date_range": date_range,
        "selected_range": selected_range,
        "filters": filters,
        "rows_before": rows_before,
        "rows_after": rows_after,
        "market_count": market_count,
    }
    m.setdefault("files", []).append(entry)
    return entry


def ensure_dirs() -> None:
    for d in [
        RAW / "sii",
        RAW / "timeseventeen",
        RAW / "binance",
        RAW / "coinbase",
        RAW / "deribit",
        PROCESSED,
        REPORTS,
    ]:
        try:
            d.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise RuntimeError(f"mkdir failed {d}: {exc}") from exc
