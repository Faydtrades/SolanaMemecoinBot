from __future__ import annotations

import argparse
import collections
import importlib
import json
import os
import signal
import sqlite3
import subprocess
import sys
import time
import types
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PHASE2_ROOT = PROJECT_ROOT / "src" / "phase2"
COLLECTOR = PROJECT_ROOT / "scripts" / "live_pump_collector_v0_3_4.py"
PRODUCTION_DB = PROJECT_ROOT / "data" / "db" / "tradingbot.sqlite3"
AUDIT_DIR = PROJECT_ROOT / "data" / "paper" / "smoke"

NS = "tradingbot_local_phase2_v034_live_source_smoke_v02"

if NS not in sys.modules:
    pkg = types.ModuleType(NS)
    pkg.__path__ = [str(PHASE2_ROOT)]
    pkg.__package__ = NS
    sys.modules[NS] = pkg


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def local_import(module_name: str):
    mod = importlib.import_module(f"{NS}.{module_name}")
    loaded = Path(getattr(mod, "__file__", "")).resolve()
    expected = (PHASE2_ROOT / f"{module_name}.py").resolve()
    if loaded != expected:
        raise RuntimeError(
            f"Local module binding mismatch for {module_name}: "
            f"loaded={loaded} expected={expected}"
        )
    return mod


def choose_class(mod: Any, preferred: str, tokens: tuple[str, ...]):
    obj = getattr(mod, preferred, None)
    if obj is not None:
        return obj

    matches = []
    for name, value in vars(mod).items():
        if isinstance(value, type):
            low = name.lower()
            if all(token in low for token in tokens):
                matches.append((name, value))

    if len(matches) == 1:
        return matches[0][1]

    raise AttributeError(f"Cannot uniquely bind {preferred!r}")


def open_ro() -> sqlite3.Connection:
    uri = PRODUCTION_DB.resolve().as_uri() + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=2.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only = ON")
    conn.execute("PRAGMA busy_timeout = 1000")
    return conn


def max_rowid(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT rowid FROM pump_events ORDER BY rowid DESC LIMIT 1"
    ).fetchone()
    return 0 if row is None else int(row["rowid"])


def fetch_after(conn: sqlite3.Connection, cursor: int, limit: int):
    return conn.execute(
        """
        SELECT rowid AS _paper_source_rowid, *
        FROM pump_events
        WHERE rowid > ?
        ORDER BY rowid
        LIMIT ?
        """,
        (cursor, limit),
    ).fetchall()


def row_mint(row: sqlite3.Row) -> str | None:
    return row["mint"]


def row_event_type(row: sqlite3.Row) -> str:
    return str(row["event_type"]).upper()


def stop_process(proc: subprocess.Popen, grace_seconds: int = 15):
    if proc.poll() is not None:
        return "EXITED_NATURALLY", proc.returncode

    if os.name == "nt":
        try:
            proc.send_signal(signal.CTRL_BREAK_EVENT)
            deadline = time.monotonic() + grace_seconds
            while time.monotonic() < deadline:
                if proc.poll() is not None:
                    return "CTRL_BREAK_GRACEFUL", proc.returncode
                time.sleep(0.25)
        except Exception:
            pass

    try:
        proc.terminate()
        deadline = time.monotonic() + min(grace_seconds, 10)
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                return "TERMINATE", proc.returncode
            time.sleep(0.25)
    except Exception:
        pass

    try:
        proc.kill()
        proc.wait(timeout=10)
        return "KILL", proc.returncode
    except Exception:
        return "STOP_FAILED", proc.poll()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-seconds", type=int, default=120)
    parser.add_argument("--max-events", type=int, default=250)
    parser.add_argument("--poll-ms", type=int, default=100)
    parser.add_argument("--batch-size", type=int, default=500)
    args = parser.parse_args()

    adapter_mod = local_import("phase1_readonly_adapter_v0_1")
    feature_mod = local_import("feature_engine_v0_2")

    Adapter = choose_class(
        adapter_mod,
        "Phase1ReadonlyAdapterV01",
        ("phase1", "adapter"),
    )
    FeatureEngine = choose_class(
        feature_mod,
        "FeatureEngineV02",
        ("feature", "engine"),
    )

    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = utc_now().strftime("%Y%m%dT%H%M%SZ")
    collector_log = AUDIT_DIR / f"phase4_3b1b_v034_collector_v02_{stamp}.log"
    audit_path = AUDIT_DIR / f"phase4_3b1b_v034_live_source_smoke_v02_{stamp}.json"

    conn = open_ro()
    try:
        start_cursor = max_rowid(conn)
    finally:
        conn.close()

    print("=" * 114)
    print("PHASE 4.3B1B - v0.3.4 CONTROLLED LIVE SOURCE SIDECAR SMOKE v0.2")
    print("=" * 114)
    print(f"Project root                     : {PROJECT_ROOT}")
    print(f"Collector                        : {COLLECTOR}")
    print(f"Production DB                    : {PRODUCTION_DB}")
    print("Adapter contract                 : normalize_rows -> (events, skipped)")
    print("Production DB sidecar access     : QUERY-ONLY / READ-ONLY")
    print("Collector lifecycle              : AUTOMATIC")
    print("Wallet/signing/live orders       : NO")
    print("Paper orders                     : NO")
    print("Strategy evaluation              : NO")
    print("Parameter tuning/reselection     : NO")
    print(f"Start cursor rowid               : {start_cursor}")
    print(f"Max duration                     : {args.max_seconds}s")
    print(f"Max usable normalized events     : {args.max_events}")
    print()

    creationflags = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"

    collector_fh = collector_log.open("w", encoding="utf-8", buffering=1)

    proc = subprocess.Popen(
        [
            sys.executable,
            "-u",
            str(COLLECTOR),
            "--max-pump-events",
            "0",
            "--drain-seconds",
            "20",
        ],
        cwd=str(PROJECT_ROOT),
        stdout=collector_fh,
        stderr=subprocess.STDOUT,
        env=env,
        creationflags=creationflags,
    )

    engine = FeatureEngine()

    cursor = start_cursor
    eligible_mints: set[str] = set()
    rows_seen = 0
    launch_rows_seen = 0
    usable_events = 0
    feature_states = 0
    feature_none = 0
    feature_errors: list[str] = []
    skipped_total: collections.Counter[str] = collections.Counter()
    batches = 0
    db_busy_retries = 0

    started = time.monotonic()
    next_health = started
    stop_mode = "NOT_STOPPED"
    collector_returncode = None

    conn = open_ro()

    try:
        while True:
            now = time.monotonic()
            elapsed = now - started

            if proc.poll() is not None:
                stop_mode = "EXITED_NATURALLY"
                collector_returncode = proc.returncode
                break

            if elapsed >= args.max_seconds or usable_events >= args.max_events:
                break

            try:
                rows = fetch_after(conn, cursor, args.batch_size)
            except sqlite3.OperationalError as exc:
                if "locked" in str(exc).lower() or "busy" in str(exc).lower():
                    db_busy_retries += 1
                    time.sleep(args.poll_ms / 1000)
                    continue
                raise

            if rows:
                batches += 1
                rows_seen += len(rows)
                cursor = max(int(row["_paper_source_rowid"]) for row in rows)

                new_launch_mints = {
                    row_mint(row)
                    for row in rows
                    if row_event_type(row) == "LAUNCH" and row_mint(row)
                }
                eligible_mints.update(new_launch_mints)
                launch_rows_seen += sum(
                    1 for row in rows if row_event_type(row) == "LAUNCH"
                )

                causal_rows = [
                    row for row in rows
                    if row_mint(row) in eligible_mints
                ]

                if causal_rows:
                    # IMPORTANT v0.2 FIX:
                    # normalize_rows returns (events, skipped), not row-result objects.
                    events, skipped = Adapter.normalize_rows(causal_rows)

                    if isinstance(skipped, dict):
                        skipped_total.update(
                            {str(k): int(v) for k, v in skipped.items()}
                        )

                    for event in events:
                        mint = getattr(event, "mint", None)
                        if mint not in eligible_mints:
                            continue

                        usable_events += 1

                        try:
                            state = engine.process(event)
                        except Exception as exc:
                            feature_errors.append(
                                f"{type(exc).__name__}: {exc}"
                            )
                            if len(feature_errors) <= 5:
                                print(
                                    f"[ERROR] FeatureEngineV02 mint={mint}: "
                                    f"{feature_errors[-1]}"
                                )
                            continue

                        if state is None:
                            feature_none += 1
                        else:
                            feature_states += 1

                        if usable_events >= args.max_events:
                            break

            if now >= next_health:
                print(
                    f"[HEALTH] elapsed={int(elapsed)}s "
                    f"rowid={cursor} rows={rows_seen} launches={launch_rows_seen} "
                    f"eligible_mints={len(eligible_mints)} usable={usable_events} "
                    f"states={feature_states} skips={sum(skipped_total.values())} "
                    f"errors={len(feature_errors)}"
                )
                next_health = now + 5.0

            time.sleep(args.poll_ms / 1000)

    finally:
        conn.close()

        if proc.poll() is None:
            stop_mode, collector_returncode = stop_process(proc, 15)

        collector_fh.close()

    elapsed = time.monotonic() - started
    new_pump_rows = cursor - start_cursor

    pass_result = (
        new_pump_rows > 0
        and launch_rows_seen > 0
        and len(eligible_mints) > 0
        and usable_events > 0
        and feature_states > 0
        and len(feature_errors) == 0
    )

    payload = {
        "schema_version": "phase4_3b1b_v034_live_source_smoke_v0.2",
        "started_at_utc": datetime.fromtimestamp(
            time.time() - elapsed, tz=timezone.utc
        ).isoformat(),
        "finished_at_utc": utc_now().isoformat(),
        "elapsed_seconds": round(elapsed, 3),
        "start_cursor_rowid": start_cursor,
        "end_cursor_rowid": cursor,
        "new_pump_rows": new_pump_rows,
        "rows_seen": rows_seen,
        "launch_rows_seen_after_start": launch_rows_seen,
        "eligible_new_mints": len(eligible_mints),
        "usable_events": usable_events,
        "feature_states": feature_states,
        "feature_none": feature_none,
        "feature_errors": feature_errors,
        "skipped": dict(skipped_total),
        "db_busy_retries": db_busy_retries,
        "batches": batches,
        "collector_stop_mode": stop_mode,
        "collector_returncode": collector_returncode,
        "collector_log": str(collector_log),
        "result": "PASS" if pass_result else "CHECK",
    }

    audit_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )

    print()
    print("-" * 114)
    print("VALIDATION")
    print("-" * 114)
    print(f"v0.3.4 collector produced new Pump rows : {'PASS' if new_pump_rows > 0 else 'CHECK'}")
    print(f"New LAUNCH observed after start cursor  : {'PASS' if launch_rows_seen > 0 else 'CHECK'}")
    print(f"At least one causally new mint          : {'PASS' if eligible_mints else 'CHECK'}")
    print(f"Adapter contract unpacked correctly     : PASS")
    print(f"Phase1 adapter produced usable events   : {'PASS' if usable_events > 0 else 'CHECK'}")
    print(f"FeatureEngineV02 emitted state          : {'PASS' if feature_states > 0 else 'CHECK'}")
    print(f"FeatureEngineV02 runtime errors         : {'PASS' if not feature_errors else 'FAIL'}")
    print("Sidecar production DB writes            : NO")
    print("Wallet/signing/live orders              : NO")
    print("Paper orders                            : NO")
    print("Parameter search/reselection            : NO")
    print()
    print(f"New pump rows                           : {new_pump_rows}")
    print(f"New launch rows                         : {launch_rows_seen}")
    print(f"Eligible new mints                      : {len(eligible_mints)}")
    print(f"Usable normalized events                : {usable_events}")
    print(f"Feature states                          : {feature_states}")
    print(f"Skipped by adapter                      : {dict(skipped_total)}")
    print(f"Collector stop mode                     : {stop_mode}")
    print(f"Audit                                   : {audit_path}")
    print()
    print(f"RESULT: {'PASS' if pass_result else 'CHECK'}")

    if pass_result:
        print(
            "NEXT: Phase 4.3B1C - exact locked FirstPullback CandidateSignal "
            "evaluation -> simulated entry routing."
        )
        return 0

    print("Do not advance to CandidateSignal routing yet.")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
