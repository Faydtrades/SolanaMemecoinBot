from __future__ import annotations

import argparse
import dataclasses
import importlib
import json
import sqlite3
import sys
import time
import types
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PHASE2_ROOT = PROJECT_ROOT / "src" / "phase2"
PRODUCTION_DB = PROJECT_ROOT / "data" / "db" / "tradingbot.sqlite3"
AUDIT_DIR = PROJECT_ROOT / "data" / "paper" / "smoke"

NS = "tradingbot_local_phase2_live_smoke"

if NS not in sys.modules:
    pkg = types.ModuleType(NS)
    pkg.__path__ = [str(PHASE2_ROOT)]
    pkg.__package__ = NS
    sys.modules[NS] = pkg


def local_import(module_name: str):
    mod = importlib.import_module(f"{NS}.{module_name}")
    loaded = Path(getattr(mod, "__file__", "")).resolve()
    expected = (PHASE2_ROOT / f"{module_name}.py").resolve()
    if loaded != expected:
        raise RuntimeError(
            f"local module binding mismatch: {module_name}: "
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
    raise AttributeError(
        f"cannot uniquely bind {preferred!r}; "
        f"available={[name for name, value in vars(mod).items() if isinstance(value, type)]}"
    )


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def dt_text(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def open_readonly_db() -> sqlite3.Connection:
    if not PRODUCTION_DB.exists():
        raise FileNotFoundError(PRODUCTION_DB)
    uri = PRODUCTION_DB.resolve().as_uri() + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=1.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only = ON")
    conn.execute("PRAGMA busy_timeout = 1000")
    return conn


def max_rowid(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT rowid FROM pump_events ORDER BY rowid DESC LIMIT 1"
    ).fetchone()
    return 0 if row is None else int(row["rowid"])


def fetch_after(
    conn: sqlite3.Connection,
    cursor: int,
    limit: int,
) -> list[sqlite3.Row]:
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


def extract_event(adapter_item: Any) -> tuple[Any | None, str | None]:
    event = getattr(adapter_item, "event", adapter_item)
    skip_reason = getattr(adapter_item, "skip_reason", None)
    return event, skip_reason


def event_mint(event: Any) -> str | None:
    return getattr(event, "mint", None)


def row_mint(row: sqlite3.Row) -> str | None:
    try:
        return row["mint"]
    except Exception:
        return None


def row_event_type(row: sqlite3.Row) -> str | None:
    try:
        value = row["event_type"]
        return None if value is None else str(value).upper()
    except Exception:
        return None


def write_audit(payload: dict[str, Any]) -> Path:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = utc_now().strftime("%Y%m%dT%H%M%SZ")
    path = AUDIT_DIR / f"phase4_3b1b_live_source_smoke_{stamp}.json"
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    return path


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Phase 4.3B1B live source sidecar smoke (no orders)."
    )
    parser.add_argument("--max-seconds", type=int, default=120)
    parser.add_argument("--max-events", type=int, default=250)
    parser.add_argument("--poll-ms", type=int, default=100)
    parser.add_argument("--batch-size", type=int, default=500)
    args = parser.parse_args()

    if args.max_seconds <= 0:
        raise SystemExit("--max-seconds must be > 0")
    if args.max_events <= 0:
        raise SystemExit("--max-events must be > 0")
    if not 25 <= args.poll_ms <= 5000:
        raise SystemExit("--poll-ms must be in [25, 5000]")
    if not 1 <= args.batch_size <= 5000:
        raise SystemExit("--batch-size must be in [1, 5000]")

    print("=" * 112)
    print("PHASE 4.3B1B - LIVE SOURCE SIDECAR SMOKE v0.1")
    print("=" * 112)
    print(f"Project root                    : {PROJECT_ROOT}")
    print(f"Production DB                   : {PRODUCTION_DB}")
    print("Production DB access            : QUERY-ONLY / READ-ONLY")
    print(f"Exact Phase2 source             : {PHASE2_ROOT}")
    print("Network/RPC in sidecar          : NO")
    print("Collector expected separately   : YES")
    print("Wallet/signing/live orders      : NO")
    print("Paper orders                    : NO")
    print("Strategy evaluation             : NO (next bounded sub-step)")
    print("Parameter tuning/reselection    : NO")
    print(f"Max duration                    : {args.max_seconds}s")
    print(f"Max usable normalized events    : {args.max_events}")
    print()

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

    conn = open_readonly_db()
    try:
        start_cursor = max_rowid(conn)
    finally:
        conn.close()

    print(f"Start cursor rowid              : {start_cursor}")
    print("Causal eligibility              : only mints with LAUNCH observed after start cursor")
    print()
    print("[WAIT] Tailing new production pump_events ...")

    engine = FeatureEngine()

    cursor = start_cursor
    eligible_mints: set[str] = set()
    rows_seen = 0
    launch_rows_seen = 0
    adapter_results = 0
    adapter_skips = 0
    normalized_events = 0
    usable_events = 0
    feature_states = 0
    feature_none = 0
    feature_errors: list[str] = []
    db_busy_retries = 0
    batches = 0

    started = time.monotonic()
    last_heartbeat = started

    conn = open_readonly_db()

    try:
        while True:
            elapsed = time.monotonic() - started
            if elapsed >= args.max_seconds:
                break
            if usable_events >= args.max_events:
                break

            try:
                rows = fetch_after(conn, cursor, args.batch_size)
            except sqlite3.OperationalError as exc:
                if "locked" in str(exc).lower() or "busy" in str(exc).lower():
                    db_busy_retries += 1
                    time.sleep(args.poll_ms / 1000)
                    continue
                raise

            if not rows:
                now = time.monotonic()
                if now - last_heartbeat >= 5.0:
                    print(
                        f"[HEALTH] elapsed={int(now-started)}s "
                        f"rowid={cursor} rows={rows_seen} launches={launch_rows_seen} "
                        f"eligible_mints={len(eligible_mints)} usable={usable_events} "
                        f"states={feature_states} errors={len(feature_errors)}"
                    )
                    last_heartbeat = now
                time.sleep(args.poll_ms / 1000)
                continue

            batches += 1
            rows_seen += len(rows)
            cursor = max(int(row["_paper_source_rowid"]) for row in rows)

            # Determine all newly launched mints in the batch before filtering,
            # so a same-transaction trade cannot be lost merely because of row order.
            batch_launch_mints = {
                row_mint(row)
                for row in rows
                if row_event_type(row) == "LAUNCH" and row_mint(row)
            }
            eligible_mints.update(batch_launch_mints)
            launch_rows_seen += sum(
                1 for row in rows if row_event_type(row) == "LAUNCH"
            )

            causal_rows = [
                row
                for row in rows
                if row_mint(row) in eligible_mints
            ]

            if not causal_rows:
                continue

            normalized_batch = Adapter.normalize_rows(causal_rows)
            for item in normalized_batch:
                adapter_results += 1
                event, skip_reason = extract_event(item)

                if skip_reason:
                    adapter_skips += 1

                if event is None:
                    continue

                mint = event_mint(event)
                if mint not in eligible_mints:
                    continue

                normalized_events += 1
                usable_events += 1

                try:
                    state = engine.process(event)
                except Exception as exc:
                    feature_errors.append(
                        f"{type(exc).__name__}: {exc}"
                    )
                    if len(feature_errors) <= 5:
                        print(
                            f"[ERROR] FeatureEngineV02 event mint={mint}: "
                            f"{feature_errors[-1]}"
                        )
                    continue

                if state is None:
                    feature_none += 1
                else:
                    feature_states += 1

                if usable_events >= args.max_events:
                    break

            now = time.monotonic()
            if now - last_heartbeat >= 5.0:
                print(
                    f"[HEALTH] elapsed={int(now-started)}s "
                    f"rowid={cursor} rows={rows_seen} launches={launch_rows_seen} "
                    f"eligible_mints={len(eligible_mints)} usable={usable_events} "
                    f"states={feature_states} errors={len(feature_errors)}"
                )
                last_heartbeat = now

    except KeyboardInterrupt:
        print()
        print("[INFO] Sidecar smoke interrupted by user.")
    finally:
        conn.close()

    elapsed = time.monotonic() - started

    pass_result = (
        launch_rows_seen > 0
        and len(eligible_mints) > 0
        and usable_events > 0
        and feature_states > 0
        and len(feature_errors) == 0
    )

    summary = {
        "schema_version": "phase4_3b1b_live_source_smoke_v0.1",
        "started_at_utc": dt_text(
            datetime.fromtimestamp(
                time.time() - elapsed,
                tz=timezone.utc,
            )
        ),
        "finished_at_utc": dt_text(utc_now()),
        "project_root": str(PROJECT_ROOT),
        "production_db": str(PRODUCTION_DB),
        "production_db_access": "QUERY_ONLY_READ_ONLY",
        "start_cursor_rowid": start_cursor,
        "end_cursor_rowid": cursor,
        "elapsed_seconds": round(elapsed, 3),
        "rows_seen_after_start": rows_seen,
        "launch_rows_seen_after_start": launch_rows_seen,
        "eligible_new_mints": len(eligible_mints),
        "adapter_results": adapter_results,
        "adapter_skips": adapter_skips,
        "normalized_events": normalized_events,
        "usable_events": usable_events,
        "feature_states": feature_states,
        "feature_none": feature_none,
        "feature_errors": feature_errors,
        "db_busy_retries": db_busy_retries,
        "batches": batches,
        "network_rpc_in_sidecar": False,
        "wallet_signing_live_orders": False,
        "paper_orders": False,
        "strategy_evaluation": False,
        "parameter_tuning_reselection": False,
        "result": "PASS" if pass_result else "CHECK",
    }

    audit_path = write_audit(summary)

    print()
    print("-" * 112)
    print("VALIDATION")
    print("-" * 112)
    print(f"New LAUNCH observed after cursor       : {'PASS' if launch_rows_seen > 0 else 'CHECK'}")
    print(f"At least one causally new mint         : {'PASS' if eligible_mints else 'CHECK'}")
    print(f"Phase1 adapter produced usable events  : {'PASS' if usable_events > 0 else 'CHECK'}")
    print(f"FeatureEngineV02 emitted state         : {'PASS' if feature_states > 0 else 'CHECK'}")
    print(f"FeatureEngineV02 runtime errors        : {'PASS' if not feature_errors else 'FAIL'}")
    print("Production DB writes by sidecar        : NO")
    print("Network/RPC in sidecar                 : NO")
    print("Wallet/signing/live orders             : NO")
    print("Paper orders                           : NO")
    print("Parameter search/reselection           : NO")
    print()
    print(f"Rows seen                              : {rows_seen}")
    print(f"New launch rows                        : {launch_rows_seen}")
    print(f"Eligible new mints                     : {len(eligible_mints)}")
    print(f"Usable normalized events               : {usable_events}")
    print(f"Feature states                         : {feature_states}")
    print(f"DB busy retries                        : {db_busy_retries}")
    print(f"Audit                                  : {audit_path}")
    print()
    print(f"RESULT: {'PASS' if pass_result else 'CHECK'}")

    if pass_result:
        print(
            "NEXT: Phase 4.3B1C - add the exact locked FirstPullback "
            "CandidateSignal evaluation and route a controlled live signal "
            "into the simulated-entry engine."
        )
        return 0

    print(
        "Do not advance to CandidateSignal routing yet. "
        "Return this summary/output for diagnosis."
    )
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
