from __future__ import annotations

from pathlib import Path
import argparse
import sqlite3
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from phase2.data_coverage_root_cause_v0_1_1 import (
    nearest_observation_before_us,
    read_control_events,
    reconstruct_collector_coverage,
    us_to_iso,
)
from phase2.phase1_readonly_adapter_v0_1 import Phase1ReadOnlyAdapterV01

QUEUE_TABLES = ("confirmation_jobs", "deep_enrichment_jobs", "gap_jobs")
TERMINAL = {"DONE", "FAILED", "CANCELLED", "CANCELED", "SKIPPED", "COMPLETE", "COMPLETED"}


def table_exists(conn, name: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone() is not None


def status_column(conn, table: str) -> str | None:
    cols = [str(r[1]) for r in conn.execute(f"PRAGMA table_info({table})")]
    for candidate in ("status", "state", "job_status"):
        if candidate in cols:
            return candidate
    return None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--production-db",
        default=str(PROJECT_ROOT / "data/db/tradingbot.sqlite3"),
    )
    args = ap.parse_args()
    db = Path(args.production_db).resolve()
    if not db.exists():
        raise FileNotFoundError(db)

    conn = Phase1ReadOnlyAdapterV01.open_readonly(db)
    try:
        controls = read_control_events(conn)
        intervals, sessions, gaps, summary = reconstruct_collector_coverage(
            controls,
            nearest_before=lambda cutoff_us, after_us: nearest_observation_before_us(
                conn, cutoff_us, after_us=after_us
            ),
        )
        if not sessions:
            raise RuntimeError("no reconstructed sessions")
        latest = max(sessions, key=lambda s: int(s["session_id"]))
        sid = int(latest["session_id"])
        ivs = [i for i in intervals if i.session_id == sid]
        definite = [i for i in ivs if not i.uncertain_close]
        uncertain = [i for i in ivs if i.uncertain_close]
        safe_end_us = max((i.end_us for i in definite), default=None)

        print("EXP-0007 INTERRUPTED SHUTDOWN DIAGNOSTIC v0.1")
        print("=" * 76)
        print(f"Latest session id            : {sid}")
        print(f"Latest session state         : {latest['stop_kind']}")
        print(f"Latest session start         : {latest['start_at_utc']}")
        print(f"Observed through             : {latest['stop_at_utc']}")
        print(f"Definite clean intervals     : {len(definite)}")
        print(f"Uncertain clean intervals    : {len(uncertain)}")
        if safe_end_us is not None:
            print(f"Last definitely closed point : {us_to_iso(safe_end_us)}")
        else:
            print("Last definitely closed point : NONE")

        print("-" * 76)
        active_total = 0
        for table in QUEUE_TABLES:
            if not table_exists(conn, table):
                print(f"{table:28s}: TABLE MISSING")
                continue
            col = status_column(conn, table)
            if col is None:
                total = int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
                print(f"{table:28s}: {total} rows; no status column detected")
                continue
            rows = conn.execute(
                f"SELECT {col}, COUNT(*) FROM {table} GROUP BY {col} ORDER BY {col}"
            ).fetchall()
            statuses = {str(r[0]): int(r[1]) for r in rows}
            active = sum(v for k, v in statuses.items() if k.upper() not in TERMINAL)
            active_total += active
            details = ", ".join(f"{k}={v}" for k, v in statuses.items()) or "EMPTY"
            print(f"{table:28s}: ACTIVE={active} | {details}")

        print("-" * 76)
        print(f"Total non-terminal queue jobs: {active_total}")
        print("Production DB opened         : READ-ONLY")
        print("Production DB mutated        : NO")
        print("Freeze created               : NO")
        print("=" * 76)
        print("RESULT: PASS")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
