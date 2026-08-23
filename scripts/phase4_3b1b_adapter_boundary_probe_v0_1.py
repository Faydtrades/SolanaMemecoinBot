from __future__ import annotations

import ast
import collections
import dataclasses
import importlib
import json
import sqlite3
import sys
import types
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PHASE2_ROOT = PROJECT_ROOT / "src" / "phase2"
ADAPTER_PATH = PHASE2_ROOT / "phase1_readonly_adapter_v0_1.py"
PRODUCTION_DB = PROJECT_ROOT / "data" / "db" / "tradingbot.sqlite3"
SMOKE_DIR = PROJECT_ROOT / "data" / "paper" / "smoke"

NS = "tradingbot_local_phase2_adapter_boundary_probe"

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
            f"Local module binding mismatch for {module_name}: "
            f"loaded={loaded} expected={expected}"
        )
    return mod


def choose_adapter(mod: Any):
    preferred = getattr(mod, "Phase1ReadonlyAdapterV01", None)
    if preferred is not None:
        return preferred

    matches = []
    for name, value in vars(mod).items():
        if isinstance(value, type):
            low = name.lower()
            if "phase1" in low and "adapter" in low:
                matches.append(value)

    if len(matches) == 1:
        return matches[0]

    raise AttributeError("Could not uniquely bind Phase1ReadonlyAdapterV01")


def latest_v034_smoke_audit() -> Path:
    matches = sorted(
        SMOKE_DIR.glob("phase4_3b1b_v034_live_source_smoke_*.json"),
        key=lambda p: p.stat().st_mtime_ns,
        reverse=True,
    )
    if not matches:
        raise FileNotFoundError(
            f"No phase4_3b1b_v034_live_source_smoke_*.json found in {SMOKE_DIR}"
        )
    return matches[0]


def open_ro() -> sqlite3.Connection:
    uri = PRODUCTION_DB.resolve().as_uri() + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=2.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only = ON")
    conn.execute("PRAGMA busy_timeout = 1000")
    return conn


def source_windows() -> None:
    text = ADAPTER_PATH.read_text(encoding="utf-8", errors="replace")
    lines = text.splitlines()
    tree = ast.parse(text)

    print()
    print("-" * 116)
    print("ADAPTER SOURCE — SKIP / VALIDATION / SOURCE-ID LINES")
    print("-" * 116)

    tokens = (
        "skip_reason",
        "source_decoded_file",
        "schema",
        "gap_recovery",
        "live_websocket",
        "confirmation_status",
        "event_type",
        "quote_mint",
        "return AdapterRowResult",
        "return adapterrowresult",
    )

    hits = []
    for i, line in enumerate(lines, 1):
        low = line.lower()
        if any(tok.lower() in low for tok in tokens):
            hits.append(i)

    printed_ranges = []
    for center in hits:
        lo = max(1, center - 4)
        hi = min(len(lines), center + 6)

        if any(not (hi < old_lo or lo > old_hi) for old_lo, old_hi in printed_ranges):
            continue

        printed_ranges.append((lo, hi))
        print()
        print(f"[lines {lo}-{hi}]")
        for n in range(lo, hi + 1):
            mark = ">>" if n == center else "  "
            print(f"{mark} {n:5d}: {lines[n-1]}")


def normalize_payload(item: Any) -> dict[str, Any]:
    if dataclasses.is_dataclass(item):
        try:
            return dataclasses.asdict(item)
        except Exception:
            pass

    if isinstance(item, sqlite3.Row):
        return dict(item)

    if isinstance(item, dict):
        return item

    return {"repr": repr(item)}


def main() -> int:
    print("=" * 116)
    print("PHASE 4.3B1B - ADAPTER BOUNDARY PROBE v0.1")
    print("=" * 116)
    print(f"Project root                   : {PROJECT_ROOT}")
    print(f"Adapter                        : {ADAPTER_PATH}")
    print(f"Production DB                  : {PRODUCTION_DB}")
    print("Production DB access           : QUERY-ONLY / READ-ONLY")
    print("Collector started              : NO")
    print("Network/RPC                    : NO")
    print("Wallet/live orders             : NO")
    print("Paper orders                   : NO")
    print("Parameter tuning/reselection   : NO")

    audit_path = latest_v034_smoke_audit()
    audit = json.loads(audit_path.read_text(encoding="utf-8"))

    start_rowid = int(audit["start_cursor_rowid"])
    end_rowid = int(audit["end_cursor_rowid"])

    print(f"Smoke audit                    : {audit_path}")
    print(f"Smoke rowid window             : ({start_rowid}, {end_rowid}]")
    print(f"Recorded new Pump rows         : {audit.get('new_pump_rows')}")
    print(f"Recorded new LAUNCH rows       : {audit.get('launch_rows_seen_after_start')}")
    print(f"Recorded eligible new mints    : {audit.get('eligible_new_mints')}")
    print()

    adapter_mod = local_import("phase1_readonly_adapter_v0_1")
    Adapter = choose_adapter(adapter_mod)

    conn = open_ro()
    try:
        rows = conn.execute(
            """
            SELECT rowid AS _probe_rowid, *
            FROM pump_events
            WHERE rowid > ? AND rowid <= ?
            ORDER BY rowid
            """,
            (start_rowid, end_rowid),
        ).fetchall()
    finally:
        conn.close()

    launch_mints = {
        row["mint"]
        for row in rows
        if str(row["event_type"]).upper() == "LAUNCH" and row["mint"]
    }

    causal_rows = [
        row for row in rows if row["mint"] in launch_mints
    ]

    print("-" * 116)
    print("REPRODUCTION")
    print("-" * 116)
    print(f"Rows in exact smoke window     : {len(rows)}")
    print(f"LAUNCH mints in window         : {len(launch_mints)}")
    print(f"Causal rows for those mints    : {len(causal_rows)}")
    print()

    raw_results = list(Adapter.normalize_rows(causal_rows))

    skip_counts: collections.Counter[str] = collections.Counter()
    usable = []
    samples_by_reason: dict[str, list[dict[str, Any]]] = collections.defaultdict(list)

    for result in raw_results:
        event = getattr(result, "event", result)
        skip_reason = getattr(result, "skip_reason", None)

        if skip_reason:
            reason = str(skip_reason)
            skip_counts[reason] += 1

            if len(samples_by_reason[reason]) < 3:
                # Recover the corresponding source row via output ordering when possible.
                idx = len(usable) + sum(skip_counts.values()) - 1
                row_payload = {}
                if 0 <= idx < len(causal_rows):
                    row_payload = dict(causal_rows[idx])

                interesting = {
                    k: row_payload.get(k)
                    for k in (
                        "_probe_rowid",
                        "event_key",
                        "event_type",
                        "mint",
                        "signature",
                        "source_decoded_file",
                        "confirmation_status",
                        "decoded_at_utc",
                        "quote_mint",
                    )
                    if k in row_payload
                }
                samples_by_reason[reason].append(interesting)

        if event is not None:
            usable.append(event)

    print("-" * 116)
    print("ADAPTER RESULT")
    print("-" * 116)
    print(f"Adapter result objects         : {len(raw_results)}")
    print(f"Usable normalized events       : {len(usable)}")
    print(f"Skipped results                : {sum(skip_counts.values())}")
    print()

    if skip_counts:
        print("Skip reason distribution:")
        for reason, count in skip_counts.most_common():
            print(f"  {count:5d}  {reason}")

        print()
        print("Representative source rows by skip reason:")
        for reason, samples in samples_by_reason.items():
            print()
            print(f"[{reason}]")
            for sample in samples:
                print("  " + json.dumps(sample, sort_keys=True, default=str))
    else:
        print("No skip reasons were returned.")

    source_windows()

    print()
    print("-" * 116)
    print("INTERPRETATION")
    print("-" * 116)

    if usable:
        print("RESULT: CHECK_UNEXPECTED")
        print(
            "The exact audit window now normalizes successfully; compare timing/state "
            "with the live sidecar implementation before changing the adapter."
        )
        return 2

    if skip_counts:
        print("RESULT: PASS_DIAGNOSIS")
        print(
            "The adapter rejection is deterministically reproduced offline. "
            "Use the skip reason(s) and source rules above for a bounded compatibility fix."
        )
        return 0

    print("RESULT: CHECK")
    print(
        "No usable events and no skip reasons were exposed. "
        "Inspect Adapter.normalize_rows return contract next."
    )
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
