from __future__ import annotations

import sqlite3
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "data" / "db" / "tradingbot.sqlite3"

QUERY_TIMEOUT_SECONDS = 10.0


class QueryTimedOut(RuntimeError):
    pass


def open_ro() -> sqlite3.Connection:
    uri = DB_PATH.resolve().as_uri() + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=1.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only = ON")
    conn.execute("PRAGMA busy_timeout = 1000")
    return conn


def timed_query(conn: sqlite3.Connection, sql: str, params=()):
    started = time.monotonic()

    def progress():
        if time.monotonic() - started >= QUERY_TIMEOUT_SECONDS:
            return 1
        return 0

    conn.set_progress_handler(progress, 10_000)
    try:
        row = conn.execute(sql, params).fetchone()
        elapsed = time.monotonic() - started
        return "OK", elapsed, row
    except sqlite3.OperationalError as exc:
        elapsed = time.monotonic() - started
        if "interrupted" in str(exc).lower() and elapsed >= QUERY_TIMEOUT_SECONDS * 0.9:
            return "TIMEOUT", elapsed, None
        return f"ERROR:{exc}", elapsed, None
    finally:
        conn.set_progress_handler(None, 0)


def print_plan(conn: sqlite3.Connection, sql: str):
    rows = conn.execute("EXPLAIN QUERY PLAN " + sql).fetchall()
    for row in rows:
        print("   ", tuple(row))


def main() -> int:
    print("=" * 110)
    print("PHASE 4.3B1B - COLLECTOR STARTUP DB PROBE v0.1")
    print("=" * 110)
    print(f"Project root             : {PROJECT_ROOT}")
    print(f"Production DB            : {DB_PATH}")
    print("DB mode                  : QUERY-ONLY / READ-ONLY")
    print("Collector executed       : NO")
    print("Network / RPC            : NO")
    print("Wallet / orders          : NO")
    print(f"Per-query hard timeout   : {QUERY_TIMEOUT_SECONDS:.0f}s")
    print()

    conn = open_ro()
    try:
        print("-" * 110)
        print("A. DATABASE TAIL / LATEST COLLECTOR EVENTS")
        print("-" * 110)

        tail = conn.execute(
            "SELECT rowid, decoded_at_utc FROM pump_events ORDER BY rowid DESC LIMIT 1"
        ).fetchone()
        print("pump_events max rowid     :", None if tail is None else tail["rowid"])
        print("latest pump observed      :", None if tail is None else tail["decoded_at_utc"])

        events = conn.execute(
            """
            SELECT id, event_type, happened_at_utc, details_json
            FROM collector_events
            ORDER BY id DESC
            LIMIT 20
            """
        ).fetchall()
        print()
        print("Latest collector_events:")
        for row in reversed(events):
            details = row["details_json"] or ""
            if len(details) > 500:
                details = details[:500] + "...<TRUNCATED>"
            print(
                f"  id={row['id']} time={row['happened_at_utc']} "
                f"type={row['event_type']} details={details}"
            )

        print()
        print("-" * 110)
        print("B. STARTUP QUEUE STATUS COUNTS")
        print("-" * 110)

        for table in ("confirmation_jobs", "deep_enrichment_jobs", "gap_jobs"):
            try:
                rows = conn.execute(
                    f"SELECT status, COUNT(*) AS n FROM {table} GROUP BY status ORDER BY status"
                ).fetchall()
                print(table, ":", {r["status"]: int(r["n"]) for r in rows})
            except Exception as exc:
                print(table, ": ERROR", type(exc).__name__, exc)

        q1 = """
        SELECT COUNT(DISTINCT signature) AS n
        FROM pump_events
        WHERE COALESCE(confirmation_status, 'processed')
              NOT IN ('confirmed', 'finalized')
        """

        q2 = """
        SELECT COUNT(DISTINCT signature) AS n
        FROM pump_events
        WHERE event_type='LAUNCH'
          AND signature IN (
              SELECT signature
              FROM transactions
              WHERE raw_get_transaction_json IS NULL
          )
        """

        print()
        print("-" * 110)
        print("C. EXACT READ-ONLY EQUIVALENT OF recover_jobs CONFIRMATION BACKFILL SELECT")
        print("-" * 110)
        print("Query plan:")
        print_plan(conn, q1)
        status1, elapsed1, row1 = timed_query(conn, q1)
        print(f"Status                   : {status1}")
        print(f"Elapsed                  : {elapsed1:.3f}s")
        if row1 is not None:
            print(f"Qualifying signatures    : {row1['n']}")

        print()
        print("-" * 110)
        print("D. EXACT READ-ONLY EQUIVALENT OF recover_jobs DEEP BACKFILL SELECT")
        print("-" * 110)
        print("Query plan:")
        print_plan(conn, q2)
        status2, elapsed2, row2 = timed_query(conn, q2)
        print(f"Status                   : {status2}")
        print(f"Elapsed                  : {elapsed2:.3f}s")
        if row2 is not None:
            print(f"Qualifying signatures    : {row2['n']}")

    finally:
        conn.close()

    slow = (
        status1 == "TIMEOUT"
        or status2 == "TIMEOUT"
        or elapsed1 >= 2.0
        or elapsed2 >= 2.0
    )

    print()
    print("-" * 110)
    print("INTERPRETATION")
    print("-" * 110)

    if slow:
        print("STARTUP_BACKFILL_SCALING_ISSUE : CONFIRMED / STRONGLY SUPPORTED")
        print(
            "At least one historical recover_jobs backfill SELECT is slow enough "
            "to be inappropriate before the live WebSocket subscription."
        )
        print("RESULT: PASS_DIAGNOSIS")
        return 0

    print("STARTUP_BACKFILL_SCALING_ISSUE : NOT CONFIRMED BY QUERY TIMING")
    print(
        "The historical SELECTs are fast on this DB; inspect connect/ensure_schema "
        "or write-lock acquisition next."
    )
    print("RESULT: CHECK")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
