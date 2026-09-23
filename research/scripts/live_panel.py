#!/usr/bin/env python3
"""Read-only terminal panel for a live jevTrader paper run."""

import argparse
import http.client
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from urllib.parse import urlencode, urlsplit


STALE_AFTER_SECONDS = 30.0
QUERY_TIMEOUT_SECONDS = 0.2
MAX_SIGNAL_CONDITIONS = 1000
MAX_VARIANTS = 128


class QuestDBError(Exception):
    """A QuestDB request or SQL query failed."""


class QuestDB:
    def __init__(self, base_url):
        parsed = urlsplit(base_url)
        if parsed.scheme not in ("http", "https") or not parsed.hostname:
            raise ValueError("QuestDB URL must use http:// or https:// and include a host")
        if parsed.query or parsed.fragment or parsed.username or parsed.password:
            raise ValueError("QuestDB base URL must not contain credentials, query, or fragment")
        try:
            self.port = parsed.port
        except ValueError as exc:
            raise ValueError(f"Invalid QuestDB port: {exc}") from exc
        self.scheme = parsed.scheme
        self.host = parsed.hostname
        self.path = parsed.path.rstrip("/") + "/exec"
        self.reachable = False

    def query(self, sql):
        normalized = sql.lstrip().upper()
        if not normalized.startswith("SELECT ") or ";" in sql:
            raise QuestDBError("refusing non-SELECT or multi-statement query")
        if " LIMIT " not in normalized:
            raise QuestDBError("refusing query without LIMIT")

        request_path = f"{self.path}?{urlencode({'query': sql})}"
        connection_type = (
            http.client.HTTPSConnection
            if self.scheme == "https"
            else http.client.HTTPConnection
        )
        connection = connection_type(
            self.host, self.port, timeout=QUERY_TIMEOUT_SECONDS
        )
        try:
            connection.request("GET", request_path)
            response = connection.getresponse()
            response_body = response.read()
            self.reachable = True
        except (http.client.HTTPException, TimeoutError, OSError) as exc:
            raise QuestDBError(str(exc)) from exc
        finally:
            connection.close()

        if response.status >= 400:
            details = response_body.decode("utf-8", errors="replace")
            raise QuestDBError(f"HTTP {response.status}: {details[:240]}")
        try:
            payload = json.loads(response_body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise QuestDBError(f"invalid /exec response: {exc}") from exc

        if isinstance(payload, dict) and payload.get("error"):
            message = str(payload["error"])
            position = payload.get("position")
            if position is not None:
                message += f" (position {position})"
            raise QuestDBError(message)
        if not isinstance(payload, dict) or not isinstance(payload.get("dataset"), list):
            raise QuestDBError("unexpected /exec response shape")

        columns = [column.get("name") for column in payload.get("columns", [])]
        return [dict(zip(columns, row, strict=True)) for row in payload["dataset"]]


def quote_sql(value):
    """Escape a SQL string literal; table and column names remain fixed."""
    return "'" + str(value).replace("'", "''") + "'"


def parse_timestamp(value):
    """Accept QuestDB ISO timestamps and epoch-microsecond values."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        numeric = float(value)
        if abs(numeric) >= 100_000_000_000_000:
            numeric /= 1_000_000
        elif abs(numeric) >= 100_000_000_000:
            numeric /= 1_000
        try:
            return datetime.fromtimestamp(numeric, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None

    text = str(value).strip()
    if not text:
        return None
    if text.lstrip("+-").isdigit():
        try:
            return parse_timestamp(int(text))
        except ValueError:
            return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        result = datetime.fromisoformat(text)
    except ValueError:
        return None
    if result.tzinfo is None:
        result = result.replace(tzinfo=timezone.utc)
    return result.astimezone(timezone.utc)


def as_float(value):
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def as_int(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def short_id(value, fallback="?"):
    if value is None or str(value).strip() == "":
        return fallback
    text = str(value)
    return text if len(text) <= 18 else text[:8] + "…" + text[-7:]


def fmt_number(value, digits=3):
    number = as_float(value)
    return "—" if number is None else f"{number:.{digits}f}"


def fmt_time(value):
    parsed = parse_timestamp(value)
    return parsed.strftime("%H:%M:%S") if parsed else "—"


def age_seconds(value, now=None):
    parsed = parse_timestamp(value)
    if parsed is None:
        return None
    current = now or datetime.now(timezone.utc)
    return max(0.0, (current - parsed).total_seconds())


def render_table(title, headers, rows):
    print(f"\n{title}")
    if not rows:
        print("  no data yet")
        return
    text_rows = [[str(cell) for cell in row] for row in rows]
    widths = [len(header) for header in headers]
    for row in text_rows:
        for index, cell in enumerate(row):
            widths[index] = min(max(widths[index], len(cell)), 32)
    header_line = "  " + "  ".join(
        header[:widths[index]].ljust(widths[index])
        for index, header in enumerate(headers)
    )
    print(header_line)
    print("  " + "  ".join("-" * width for width in widths))
    for row in text_rows:
        print(
            "  "
            + "  ".join(
                cell[:widths[index]].ljust(widths[index])
                for index, cell in enumerate(row)
            )
        )


def latest_run_query(db):
    rows = db.query(
        "SELECT run_id, ts FROM paper_decisions "
        "WHERE run_id IS NOT NULL ORDER BY ts DESC LIMIT 1"
    )
    if not rows:
        return None, None
    return rows[0].get("run_id"), rows[0].get("ts")


def run_queries(db, run_id):
    run_literal = quote_sql(run_id)
    queries = {
        "decision_counts": (
            f"SELECT decision, count() AS decision_count, max(ts) AS latest_ts "  # noqa: S608
            f"FROM paper_decisions WHERE run_id = {run_literal} "
            "GROUP BY decision LIMIT 64"
        ),
        "decisions": (
            "SELECT ts, condition_id, market_id, decision FROM paper_decisions "
            f"WHERE run_id = {run_literal} ORDER BY ts DESC LIMIT 8"
        ),
        "signals": (
            "SELECT ts, condition_id, underreact_up, underreact_down, move_persists, "  # noqa: S608
            "fill_before_decay, fill_toxic, latency_ms FROM jev_signals "
            f"WHERE run_id = {run_literal} LATEST ON ts PARTITION BY condition_id "
            f"LIMIT {MAX_SIGNAL_CONDITIONS}"
        ),
        "max_underreact": (
            "SELECT max(underreact_up) AS max_underreact_up FROM jev_signals "  # noqa: S608
            f"WHERE run_id = {run_literal} LIMIT 1"
        ),
        "fill_count": (
            "SELECT count() AS fill_count FROM paper_fills "  # noqa: S608
            f"WHERE run_id = {run_literal} LIMIT 1"
        ),
        "fills": (
            "SELECT ts, condition_id, market_id, price, size, maker "  # noqa: S608
            f"FROM paper_fills WHERE run_id = {run_literal} ORDER BY ts DESC LIMIT 8"
        ),
        "equity": (
            "SELECT ts, variant, total_pnl, exposure FROM paper_equity "  # noqa: S608
            f"WHERE run_id = {run_literal} LATEST ON ts PARTITION BY variant "
            f"LIMIT {MAX_VARIANTS}"
        ),
    }

    results = {}
    errors = {}
    with ThreadPoolExecutor(max_workers=len(queries)) as pool:
        pending = {
            pool.submit(db.query, sql): name for name, sql in queries.items()
        }
        for future in as_completed(pending):
            name = pending[future]
            try:
                results[name] = future.result()
            except QuestDBError as exc:
                errors[name] = str(exc)
    return results, errors


def render_snapshot(db, run_id, selected_ts, interval):
    print("\033[2J\033[H", end="")
    now = datetime.now(timezone.utc)
    counts = []
    errors = {}
    latest_decision_ts = selected_ts
    results = {}

    if run_id is not None:
        results, errors = run_queries(db, run_id)
        counts = results.get("decision_counts", [])
        observed = [parse_timestamp(row.get("latest_ts")) for row in counts]
        observed = [timestamp for timestamp in observed if timestamp is not None]
        if observed:
            latest_decision_ts = max(observed)

    age = age_seconds(latest_decision_ts, now)
    if age is None:
        liveness = "NO DATA"
        age_text = "—"
    else:
        liveness = "ALIVE" if age <= STALE_AFTER_SECONDS else "STALE"
        age_text = f"{age:.1f}s"
    reachability = "REACHABLE" if db.reachable else "UNREACHABLE"
    print("jevTrader live paper panel")
    print(
        f"run_id: {run_id or 'no run yet'} | QuestDB: {reachability} | "
        f"engine hint: {liveness} (last decision {age_text}, "
        f"stale >{int(STALE_AFTER_SECONDS)}s)"
    )
    if run_id is None:
        print("No paper_decisions run_id found yet; waiting for the first run.")

    decision_rows = [
        [row.get("decision", "?"), as_int(row.get("decision_count"))]
        for row in counts
    ]
    decision_rows.sort(key=lambda row: str(row[0]))
    render_table("DECISIONS · counts by decision", ["decision", "count"], decision_rows)

    recent_decisions = results.get("decisions", [])
    render_table(
        "DECISIONS · last 8",
        ["time", "market / condition", "decision"],
        [
            [
                fmt_time(row.get("ts")),
                short_id(row.get("market_id") or row.get("condition_id")),
                row.get("decision") or "—",
            ]
            for row in recent_decisions
        ],
    )

    signals = results.get("signals", [])
    signals.sort(
        key=lambda row: parse_timestamp(row.get("ts")) or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )
    indicator_rows = []
    for row in signals:
        signal_age = age_seconds(row.get("ts"), now)
        indicator_rows.append(
            [
                short_id(row.get("condition_id")),
                fmt_number(row.get("underreact_up")),
                fmt_number(row.get("underreact_down")),
                fmt_number(row.get("move_persists")),
                fmt_number(row.get("fill_before_decay")),
                fmt_number(row.get("fill_toxic")),
                f"{row['latency_ms']}ms" if row.get("latency_ms") is not None else "—",
                f"{signal_age:.1f}s" if signal_age is not None else "—",
            ]
        )
    render_table(
        "INDICATORS · latest Jev evaluation per condition",
        ["condition", "up", "down", "persists", "fill<decay", "toxic", "latency", "age"],
        indicator_rows,
    )
    max_rows = results.get("max_underreact", [])
    max_up = as_float(max_rows[0].get("max_underreact_up")) if max_rows else None
    if max_up is None and signals:
        values = [as_float(row.get("underreact_up")) for row in signals]
        values = [value for value in values if value is not None]
        max_up = max(values) if values else None
    if max_up is None:
        print("  run-level max underreact_up: no data yet (quote gate 0.750)")
    else:
        gap = 0.75 - max_up
        print(
            f"  run-level max underreact_up: {max_up:.3f} | "
            f"gap to 0.750 quote gate: {gap:+.3f}"
        )

    fill_count_rows = results.get("fill_count", [])
    fill_count = (
        as_int(fill_count_rows[0].get("fill_count")) if fill_count_rows else None
    )
    fills = results.get("fills", [])
    count_label = fill_count if fill_count is not None else "unavailable"
    print(f"\nTRADES / FILLS · count: {count_label}")
    render_table(
        "Last 8 paper fills",
        ["time", "market / condition", "price", "size", "maker"],
        [
            [
                fmt_time(row.get("ts")),
                short_id(row.get("market_id") or row.get("condition_id")),
                fmt_number(row.get("price")),
                fmt_number(row.get("size")),
                "yes" if str(row.get("maker", "0")) in ("1", "true", "True") else "no",
            ]
            for row in fills
        ],
    )

    equity_rows = results.get("equity", [])
    equity_rows.sort(key=lambda row: str(row.get("variant") or ""))
    render_table(
        "PNL · latest paper_equity row per variant",
        ["variant", "time", "total_pnl", "exposure"],
        [
            [
                row.get("variant") or "?",
                fmt_time(row.get("ts")),
                fmt_number(row.get("total_pnl")),
                fmt_number(row.get("exposure")),
            ]
            for row in equity_rows
        ],
    )
    if equity_rows:
        pnls = [as_float(row.get("total_pnl")) for row in equity_rows]
        exposures = [as_float(row.get("exposure")) for row in equity_rows]
        pnl_total = sum(value for value in pnls if value is not None)
        exposure_total = sum(value for value in exposures if value is not None)
        print(
            "  session totals (latest snapshots summed across variants): "
            f"total_pnl={pnl_total:.3f}, exposure={exposure_total:.3f}"
        )
    else:
        print("  session totals: no data yet")
    print("  equity column: unavailable (paper_equity schema has no equity column).")

    if errors:
        print("\nQuery errors:")
        for name in sorted(errors):
            print(f"  {name}: {errors[name]}")
    if interval:
        print(f"\nRefresh: {interval:.1f}s · Ctrl-C to exit")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Read-only live terminal panel for a jevTrader paper run."
    )
    parser.add_argument(
        "--questdb", default="http://localhost:9002",
        help="QuestDB HTTP API base URL (default: http://localhost:9002)",
    )
    parser.add_argument(
        "--run-id", default=None,
        help="paper run to watch (default: latest run_id in paper_decisions)",
    )
    parser.add_argument(
        "--interval", type=float, default=2.0,
        help="refresh interval in seconds (default: 2.0)",
    )
    parser.add_argument(
        "--once", action="store_true",
        help="render one snapshot and exit",
    )
    args = parser.parse_args(argv)
    if args.interval <= 0:
        parser.error("--interval must be greater than zero")
    return args


def main(argv=None):
    args = parse_args(argv)
    db = QuestDB(args.questdb)
    run_id = args.run_id
    selected_ts = None

    try:
        while True:
            if run_id is None:
                try:
                    run_id, selected_ts = latest_run_query(db)
                except QuestDBError as exc:
                    print("\033[2J\033[H", end="")
                    print("jevTrader live paper panel")
                    reachability = "REACHABLE" if db.reachable else "UNREACHABLE"
                    print(f"run_id: unresolved | QuestDB: {reachability}")
                    print(f"Could not resolve latest run: {exc}")
                    if args.once:
                        return 0
                    time.sleep(args.interval)
                    continue

            render_snapshot(db, run_id, selected_ts, None if args.once else args.interval)
            if args.once:
                return 0
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nExiting.")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
