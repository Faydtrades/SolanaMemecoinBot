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
    keys = {
        "saw_websocket_or_health": any(
            token in lower for token in ("websocket", "health", "notification")
        ),
        "saw_pump_event_text": any(
            token in lower for token in (" buy ", " sell ", " launch ", "[buy]", "[sell]", "[launch]")
        ),
        "saw_reconnect_text": "reconnect" in lower,
        "saw_gap_text": "gap" in lower,
        "saw_429": "429" in lower,
        "saw_traceback": "traceback" in lower,
        "saw_exception": "exception" in lower or "error" in lower,
    }
    return keys


def stop_process(proc: subprocess.Popen, grace_seconds: int) -> tuple[str, int | None]:
    if proc.poll() is not None:
        return "EXITED_NATURALLY", proc.returncode

    # Windows: send CTRL_BREAK to the dedicated process group.
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

    # Cross-platform fallback: terminate.
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
        description="Controlled collector live diagnostic for Phase 4.3B1B."
    )
    parser.add_argument("--run-seconds", type=int, default=90)
    parser.add_argument("--grace-seconds", type=int, default=20)
    args = parser.parse_args()

    if args.run_seconds < 30 or args.run_seconds > 300:
        raise SystemExit("--run-seconds must be between 30 and 300")
    if args.grace_seconds < 5 or args.grace_seconds > 60:
        raise SystemExit("--grace-seconds must be between 5 and 60")

    if not COLLECTOR.exists():
        raise SystemExit(f"Collector missing: {COLLECTOR}")
    if not PRODUCTION_DB.exists():
        raise SystemExit(f"Production DB missing: {PRODUCTION_DB}")

    print("=" * 110)
    print("PHASE 4.3B1B - CONTROLLED COLLECTOR LIVE DIAGNOSTIC v0.1")
    print("=" * 110)
    print(f"Project root                 : {PROJECT_ROOT}")
    print(f"Collector                    : {COLLECTOR}")
    print(f"Production DB                : {PRODUCTION_DB}")
    print(f"Run seconds                  : {args.run_seconds}")
    print(f"Graceful-stop window         : {args.grace_seconds}")
    print("Wallet / signing / live order: NO")
    print("Strategy / paper order       : NO")
    print("Parameter tuning/reselection : NO")
    print()

    before = db_snapshot()
    print(f"DB max rowid BEFORE          : {before['max_rowid']}")
    print(f"DB latest observed BEFORE    : {before['latest_observed']}")
    print()

    cmd = [
        sys.executable,
        str(COLLECTOR),
        "--max-pump-events",
        "0",
        "--drain-seconds",
        "30",
    ]

    creationflags = 0
    if os.name == "nt":
        creationflags = subprocess.CREATE_NEW_PROCESS_GROUP

    started_at = utc_now_iso()
    start_mono = time.monotonic()

    proc = subprocess.Popen(
        cmd,
        cwd=str(PROJECT_ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        creationflags=creationflags,
    )

    lines: list[str] = []
    print("[INFO] Collector started under controlled wrapper.")
    print("[INFO] Live output follows; wrapper will stop it automatically.")
    print()

    deadline = start_mono + args.run_seconds

    try:
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                break

            line = proc.stdout.readline() if proc.stdout is not None else ""
            if line:
                lines.append(line)
                print(line, end="")
            else:
                time.sleep(0.05)
    except KeyboardInterrupt:
        print("\n[INFO] Wrapper interrupted by user; stopping child process.")
    finally:
        stop_mode, returncode = stop_process(proc, args.grace_seconds)

        # Drain remaining buffered output.
        if proc.stdout is not None:
            try:
                remainder = proc.stdout.read()
                if remainder:
                    lines.append(remainder)
                    print(remainder, end="")
            except Exception:
                pass

    elapsed = round(time.monotonic() - start_mono, 3)
    after = db_snapshot()

    output_text = "".join(lines)
    output_flags = parse_output(output_text)

    row_delta = after["max_rowid"] - before["max_rowid"]

    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    audit_path = AUDIT_DIR / f"phase4_3b1b_collector_live_diagnostic_{stamp}.json"
    stdout_path = AUDIT_DIR / f"phase4_3b1b_collector_live_diagnostic_{stamp}.log"

    stdout_path.write_text(output_text, encoding="utf-8", errors="replace")

    result = "PASS" if row_delta > 0 else "CHECK"

    payload = {
        "schema_version": "phase4_3b1b_collector_live_diagnostic_v0.1",
        "started_at_utc": started_at,
        "finished_at_utc": utc_now_iso(),
        "elapsed_seconds": elapsed,
        "collector": str(COLLECTOR),
        "db_before": before,
        "db_after": after,
        "pump_event_row_delta": row_delta,
        "stop_mode": stop_mode,
        "collector_returncode": returncode,
        "output_flags": output_flags,
        "wallet_signing_live_orders": False,
        "strategy_paper_orders": False,
        "parameter_tuning_reselection": False,
        "result": result,
        "stdout_log": str(stdout_path),
    }

    audit_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )

    print()
    print("-" * 110)
    print("DIAGNOSTIC SUMMARY")
    print("-" * 110)
    print(f"Elapsed                       : {elapsed}s")
    print(f"Stop mode                     : {stop_mode}")
    print(f"Collector return code         : {returncode}")
    print(f"DB max rowid AFTER            : {after['max_rowid']}")
    print(f"DB latest observed AFTER      : {after['latest_observed']}")
    print(f"New pump_events rows          : {row_delta}")
    print(f"Saw websocket/health text     : {output_flags['saw_websocket_or_health']}")
    print(f"Saw Pump BUY/SELL/LAUNCH text : {output_flags['saw_pump_event_text']}")
    print(f"Saw reconnect text            : {output_flags['saw_reconnect_text']}")
    print(f"Saw gap text                  : {output_flags['saw_gap_text']}")
    print(f"Saw HTTP 429                  : {output_flags['saw_429']}")
    print(f"Saw traceback                 : {output_flags['saw_traceback']}")
    print(f"Audit                         : {audit_path}")
    print(f"Captured collector log        : {stdout_path}")
    print()
    print(f"RESULT: {result}")

    if row_delta > 0:
        print(
            "NEXT: rerun Phase 4.3B1B live source sidecar against a live collector "
            "using an automatic wrapper, then continue to CandidateSignal routing."
        )
        return 0

    print(
        "No new pump_events reached the production DB during the controlled window. "
        "Return this summary/output; do not advance to CandidateSignal routing."
    )
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
