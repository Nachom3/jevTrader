"""Inspect mandatory sources WITHOUT large downloads."""

import subprocess

from common import REPORTS

ALLOWED_HOSTS = {
    "huggingface.co",
    "data.binance.vision",
}

BASE_SII = "https://huggingface.co/datasets/SII-WANGZJ/Polymarket_data/resolve/main/"
BASE_TS = "https://huggingface.co/datasets/TimeSeventeen/Polymarket-v1/resolve/main/"
PROBE_BINANCE = (
    "https://data.binance.vision/data/spot/"
    "monthly/klines/BTCUSDT/1m/"
    "BTCUSDT-1m-2024-01.zip"
)


def _host_ok(url: str) -> bool:
    try:
        host = url.split("://", 1)[1].split("/", 1)[0]
    except IndexError:
        return False
    return host in ALLOWED_HOSTS


def head_size(url: str):
    if not url.startswith("https://") or not _host_ok(url):
        return "ERR host not allowed"
    try:
        proc = subprocess.run(
            ["curl", "-sI", "--max-time", "15", url],
            capture_output=True,
            text=True,
            check=False,
        )
        for line in proc.stdout.splitlines():
            if line.lower().startswith("content-length:"):
                return line.split(":", 1)[1].strip()
        return "unknown"
    except Exception as e:
        return f"ERR {e}"


def main():
    REPORTS.mkdir(parents=True, exist_ok=True)
    lines = ["# Sources inspection", ""]
    lines.append("## SII-WANGZJ/Polymarket_data")
    for f in [
        "markets.parquet",
        "trades.parquet",
        "quant.parquet",
        "orderfilled.parquet",
        "users.parquet",
    ]:
        size = head_size(BASE_SII + f)
        try:
            gb = int(size) / 1e9 if str(size).isdigit() else size
            if isinstance(gb, float):
                gb = f"{gb:.2f}GB ({size} bytes)"
        except (ValueError, TypeError):
            gb = size
        if f == "markets.parquet":
            flag = "ALLOW-FULL"
        elif f in ("orderfilled.parquet", "users.parquet"):
            flag = "FORBIDDEN"
        else:
            flag = "SELECTIVE-ONLY"
        lines.append(f"- {f}: {gb} [{flag}]")
    lines.append("")
    lines.append("## TimeSeventeen/Polymarket-v1")
    lines.append("Monthly OrderFilled + daily daily_aligned. Only intersecting months.")
    for m in ["2024_01", "2024_06", "2025_01", "2025_12", "2026_04"]:
        path = f"OrderFilled/{m}.parquet"
        lines.append(f"- {path}: {head_size(BASE_TS + path)} bytes")
    lines.append("")
    lines.append("## Binance vision (data.binance.vision)")
    lines.append("- klines 1m ONLY for regimes/vol.")
    lines.append("- aggTrades/trades sub-second for replay.")
    lines.append("- bookTicker/depth where available.")
    lines.append(f"- probe klines: {head_size(PROBE_BINANCE)} bytes")
    lines.append("")
    lines.append("## Coinbase / Deribit")
    lines.append("- Adapters prepared in research + src/feeds.")
    lines.append("- No creds bundled: coverage BINANCE_ONLY.")
    out = REPORTS / "sources.md"
    if out.resolve().parent != REPORTS.resolve():
        raise RuntimeError("unexpected output path")
    try:
        out.write_text("\n".join(lines) + "\n")
    except OSError as exc:
        raise RuntimeError(f"write failed: {exc}") from exc
    print("\n".join(lines))
    print(f"\nwrote {out}")


if __name__ == "__main__":
    main()
