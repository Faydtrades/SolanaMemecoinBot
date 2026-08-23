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
COLLECTOR = PROJECT_ROOT / "scripts" / "live_pump_collector_v0_3_4.py"
DB = PROJECT_ROOT / "data" / "db" / "tradingbot.sqlite3"
OUT_DIR = PROJECT_ROOT / "data" / "paper" / "smoke"


def db_tail():
    uri = DB.resolve().as_uri() + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=2.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    try:
        row = conn.execute(
            "SELECT rowid, decoded_at_utc FROM pump_events ORDER BY rowid DESC LIMIT 1"
        ).fetchone()
        return {
            "rowid": 0 if row is None else int(row["rowid"]),
            "observed": None if row is None else row["decoded_at_utc"],
        }
    finally:
        conn.close()


def stop(proc, grace=15):
    if proc.poll() is not None:
        return "EXITED_NATURALLY"

    if os.name == "nt":
        try:
            proc.send_signal(signal.CTRL_BREAK_EVENT)
            end = time.monotonic() + grace
            while time.monotonic() < end:
                if proc.poll() is not None:
                    return "CTRL_BREAK_GRACEFUL"
                time.sleep(0.25)
        except Exception:
            pass

    try:
        proc.terminate()
        proc.wait(timeout=10)
        return "TERMINATE"
    except Exception:
        proc.kill()
        proc.wait(timeout=10)
        return "KILL"


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--run-seconds", type=int, default=90)
    args = p.parse_args()

    if not COLLECTOR.exists():
        print("RESULT: CHECK")
        print("Run the v0.3.4 builder first.")
        return 2

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    child_log = OUT_DIR / f"collector_v034_live_smoke_{stamp}.log"
    audit = OUT_DIR / f"collector_v034_live_smoke_{stamp}.json"

    before = db_tail()

    print("=" * 104)
    print("live_pump_collector_v0_3_4 — CONTROLLED LIVE SMOKE")
    print("=" * 104)
    print(f"Collector       : {COLLECTOR}")
    print(f"Run seconds     : {args.run_seconds}")
    print(f"DB rowid BEFORE : {before['rowid']}")
    print(f"Child log       : {child_log}")
    print("Auto-stop       : YES")
    print("Wallet/orders   : NO")
    print()

    flags = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"

    with child_log.open("w", encoding="utf-8", buffering=1) as fh:
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
            stdout=fh,
            stderr=subprocess.STDOUT,
            env=env,
            creationflags=flags,
        )

        start = time.monotonic()
        next_health = start

        while True:
            now = time.monotonic()
            elapsed = now - start

            if proc.poll() is not None:
                stop_mode = "EXITED_NATURALLY"
                break

            if elapsed >= args.run_seconds:
                stop_mode = stop(proc)
                break

            if now >= next_health:
                tail = db_tail()
                print(
                    f"[HEALTH] elapsed={int(elapsed)}s child_alive=YES "
                    f"rowid={tail['rowid']} new_pump_rows={tail['rowid']-before['rowid']}"
                )
                next_health = now + 5

            time.sleep(0.25)

    after = db_tail()
    delta = after["rowid"] - before["rowid"]
    text = child_log.read_text(encoding="utf-8", errors="replace")

    traceback = "Traceback" in text
    result = delta > 0 and not traceback

    payload = {
        "schema_version": "collector_v034_live_smoke_v0.1",
        "before": before,
        "after": after,
        "new_pump_rows": delta,
        "traceback": traceback,
        "stop_mode": stop_mode,
        "child_log": str(child_log),
        "result": "PASS" if result else "CHECK",
    }
    audit.write_text(
        json.dumps(payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    print()
    print("-" * 104)
    print("SUMMARY")
    print("-" * 104)
    print(f"DB rowid AFTER       : {after['rowid']}")
    print(f"New pump_events rows : {delta}")
    print(f"Traceback            : {traceback}")
    print(f"Stop mode            : {stop_mode}")
    print(f"Audit                : {audit}")
    print(f"Child log            : {child_log}")
    print()
    print(f"RESULT: {'PASS' if result else 'CHECK'}")
    return 0 if result else 2


if __name__ == "__main__":
    raise SystemExit(main())
