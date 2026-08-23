from __future__ import annotations

import argparse
import json
import os
import signal
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
COLLECTOR = PROJECT_ROOT / "scripts" / "live_pump_collector_v0_3_3.py"
PRODUCTION_DB = PROJECT_ROOT / "data" / "db" / "tradingbot.sqlite3"
AUDIT_DIR = PROJECT_ROOT / "data" / "paper" / "smoke"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def open_ro() -> sqlite3.Connection:
    uri = PRODUCTION_DB.resolve().as_uri() + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=2.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only = ON")
    conn.execute("PRAGMA busy_timeout = 1000")
    return conn


def db_snapshot() -> dict:
    conn = open_ro()
    try:
        row = conn.execute(
            "SELECT rowid, decoded_at_utc FROM pump_events ORDER BY rowid DESC LIMIT 1"
        ).fetchone()
        return {
            "max_rowid": 0 if row is None else int(row["rowid"]),
            "latest_observed": None if row is None else row["decoded_at_utc"],
        }
    finally:
        conn.close()


def parse_output(text: str) -> dict:
    lower = text.lower()
    return {
        "saw_websocket_or_health": any(
            token in lower for token in ("websocket", "health", "notification")
        ),
        "saw_pump_event_text": any(
            token in lower
            for token in ("[buy]", "[sell]", "[launch]", " buy ", " sell ", " launch ")
        ),
        "saw_reconnect_text": "reconnect" in lower,
        "saw_gap_text": "gap" in lower,
        "saw_429": "429" in lower,
        "saw_traceback": "traceback" in lower,
        "saw_error_text": "error" in lower or "exception" in lower,
    }


def stop_process(proc: subprocess.Popen, grace_seconds: int) -> tuple[str, int | None]:
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
    parser = argparse.ArgumentParser(
        description="Nonblocking controlled collector diagnostic for Phase 4.3B1B."
    )
    parser.add_argument("--run-seconds", type=int, default=90)
    parser.add_argument("--grace-seconds", type=int, default=15)
    parser.add_argument("--heartbeat-seconds", type=int, default=5)
    args = parser.parse_args()

    if not 30 <= args.run_seconds <= 300:
        raise SystemExit("--run-seconds must be between 30 and 300")
    if not 5 <= args.grace_seconds <= 60:
        raise SystemExit("--grace-seconds must be between 5 and 60")
    if not 2 <= args.heartbeat_seconds <= 30:
        raise SystemExit("--heartbeat-seconds must be between 2 and 30")

    if not COLLECTOR.exists():
        raise SystemExit(f"Collector missing: {COLLECTOR}")
    if not PRODUCTION_DB.exists():
        raise SystemExit(f"Production DB missing: {PRODUCTION_DB}")

    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    child_log = AUDIT_DIR / f"phase4_3b1b_collector_child_{stamp}.log"
    audit_path = AUDIT_DIR / f"phase4_3b1b_collector_live_diagnostic_{stamp}.json"

    print("=" * 112)
    print("PHASE 4.3B1B - CONTROLLED COLLECTOR LIVE DIAGNOSTIC v0.2")
    print("=" * 112)
    print(f"Project root                  : {PROJECT_ROOT}")
    print(f"Collector                     : {COLLECTOR}")
    print(f"Production DB                 : {PRODUCTION_DB}")
    print(f"Collector output log          : {child_log}")
    print(f"Run seconds                   : {args.run_seconds}")
    print(f"Heartbeat                     : every {args.heartbeat_seconds}s")
    print("Child stdout handling         : FILE (NONBLOCKING WRAPPER)")
    print("Wallet / signing / live order : NO")
    print("Strategy / paper order        : NO")
    print("Parameter tuning/reselection  : NO")
    print()

    before = db_snapshot()
    print(f"DB max rowid BEFORE           : {before['max_rowid']}")
    print(f"DB latest observed BEFORE     : {before['latest_observed']}")
    print()

    cmd = [
        sys.executable,
        "-u",
        str(COLLECTOR),
        "--max-pump-events",
        "0",
        "--drain-seconds",
        "30",
    ]

    creationflags = 0
    if os.name == "nt":
        creationflags = subprocess.CREATE_NEW_PROCESS_GROUP

    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"

    started_at = utc_now_iso()
    start_mono = time.monotonic()
    next_heartbeat = start_mono

    with child_log.open("w", encoding="utf-8", errors="replace", buffering=1) as log_fh:
        proc = subprocess.Popen(
            cmd,
            cwd=str(PROJECT_ROOT),
            stdout=log_fh,
            stderr=subprocess.STDOUT,
            text=True,
            env=env,
            creationflags=creationflags,
        )

        print(f"[INFO] Collector child PID: {proc.pid}")
        print("[INFO] Wrapper is nonblocking and will stop automatically.")
        print()

        latest = before
        stop_mode = "NOT_STOPPED"
        returncode = None

        try:
            while True:
                now = time.monotonic()
                elapsed = now - start_mono

                if proc.poll() is not None:
                    returncode = proc.returncode
                    stop_mode = "EXITED_NATURALLY"
                    print(
                        f"[INFO] Collector exited naturally after {elapsed:.1f}s "
                        f"with return code {returncode}."
                    )
                    break

                if elapsed >= args.run_seconds:
                    print(
                        f"[INFO] {args.run_seconds}s reached; stopping collector automatically."
                    )
                    stop_mode, returncode = stop_process(proc, args.grace_seconds)
                    break

                if now >= next_heartbeat:
                    try:
                        latest = db_snapshot()
                        delta = latest["max_rowid"] - before["max_rowid"]
                        print(
                            f"[HEALTH] elapsed={int(elapsed)}s "
                            f"child_alive=YES "
                            f"db_rowid={latest['max_rowid']} "
                            f"new_pump_rows={delta}"
                        )
                    except Exception as exc:
                        print(
                            f"[HEALTH] elapsed={int(elapsed)}s "
                            f"child_alive=YES db_probe_error={type(exc).__name__}:{exc}"
                        )
                    next_heartbeat = now + args.heartbeat_seconds

                time.sleep(0.25)

        except KeyboardInterrupt:
            print()
            print("[INFO] Wrapper interrupted; stopping child automatically.")
            stop_mode, returncode = stop_process(proc, args.grace_seconds)

        finally:
            if proc.poll() is None:
                stop_mode, returncode = stop_process(proc, args.grace_seconds)

    elapsed = round(time.monotonic() - start_mono, 3)
    after = db_snapshot()
    row_delta = after["max_rowid"] - before["max_rowid"]

    try:
        output_text = child_log.read_text(encoding="utf-8", errors="replace")
    except Exception:
        output_text = ""

    flags = parse_output(output_text)
    result = "PASS" if row_delta > 0 else "CHECK"

    payload = {
        "schema_version": "phase4_3b1b_collector_live_diagnostic_v0.2",
        "started_at_utc": started_at,
        "finished_at_utc": utc_now_iso(),
        "elapsed_seconds": elapsed,
        "collector": str(COLLECTOR),
        "collector_child_log": str(child_log),
        "db_before": before,
        "db_after": after,
        "pump_event_row_delta": row_delta,
        "stop_mode": stop_mode,
        "collector_returncode": returncode,
        "output_flags": flags,
        "wallet_signing_live_orders": False,
        "strategy_paper_orders": False,
        "parameter_tuning_reselection": False,
        "result": result,
    }

    audit_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )

    print()
    print("-" * 112)
    print("DIAGNOSTIC SUMMARY")
    print("-" * 112)
    print(f"Elapsed                        : {elapsed}s")
    print(f"Stop mode                      : {stop_mode}")
    print(f"Collector return code          : {returncode}")
    print(f"DB max rowid AFTER             : {after['max_rowid']}")
    print(f"DB latest observed AFTER       : {after['latest_observed']}")
    print(f"New pump_events rows           : {row_delta}")
    print(f"Saw websocket/health text      : {flags['saw_websocket_or_health']}")
    print(f"Saw Pump BUY/SELL/LAUNCH text  : {flags['saw_pump_event_text']}")
    print(f"Saw reconnect text             : {flags['saw_reconnect_text']}")
    print(f"Saw gap text                   : {flags['saw_gap_text']}")
    print(f"Saw HTTP 429                   : {flags['saw_429']}")
    print(f"Saw traceback                  : {flags['saw_traceback']}")
    print(f"Saw error/exception text       : {flags['saw_error_text']}")
    print(f"Audit                          : {audit_path}")
    print(f"Collector child log            : {child_log}")
    print()
    print(f"RESULT: {result}")

    if result == "PASS":
        print(
            "NEXT: repeat the live Adapter -> FeatureEngine sidecar smoke "
            "with automatic collector lifecycle."
        )
        return 0

    print(
        "No new pump_events reached production SQLite in this controlled window. "
        "Return this summary; the child log is available for targeted diagnosis."
    )
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
