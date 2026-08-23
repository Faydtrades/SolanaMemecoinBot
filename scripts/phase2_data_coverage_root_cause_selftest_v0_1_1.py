from __future__ import annotations

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.phase2.data_coverage_root_cause_v0_1_1 import (
    ActiveInterval,
    ControlEvent,
    build_coverage_bins,
    build_root_cause_payload,
    diagnose_unpriceable_rows,
    reserve_path_diagnosis,
    reconstruct_collector_coverage,
    stable_sha256,
)


failures = []


def check(label: str, ok: bool) -> None:
    status = "PASS" if ok else "FAIL"
    print(f"{label:<68} {status}")
    if not ok:
        failures.append(label)


def iso(s: str) -> str:
    return s


def us(s: str) -> int:
    from src.phase2.data_coverage_root_cause_v0_1_1 import iso_to_us
    return iso_to_us(s)


print("PHASE 2 DATA COVERAGE / ROOT-CAUSE SELF-TEST v0.1.1")
print("Purpose                    : QUOTE-RESERVE DIAGNOSIS + COLLECTOR UPTIME MECHANICS")
print("Synthetic thresholds only  : YES")
print("PnL/future returns          : NOT USED")
print("Production DB touched       : NO")
print()

base = {
    "mint": "M1",
    "event_key": "e1",
    "signature": "s1",
    "event_type": "BUY",
    "decoded_at_utc": "2026-08-18T14:00:01+00:00",
    "source_decoded_file": "LIVE_WEBSOCKET_EVENT_V0_3_3",
    "virtual_token_reserves": "1000000",
    "event_json": json.dumps({"full_current_tail_decoded": True, "ix_name": "buy"}),
}

sol_row = dict(base, virtual_sol_reserves="100", virtual_quote_reserves="0")
quote_row = dict(base, event_key="e2", virtual_sol_reserves="0", virtual_quote_reserves="500", quote_mint="QuoteMint")
dead_row = dict(base, event_key="e3", virtual_sol_reserves="0", virtual_quote_reserves="0", quote_mint="QuoteMint")

check("Test 1a - positive SOL reserve path remains SOL-priceable", reserve_path_diagnosis(sol_row) == "SOL_RESERVE_PRICEABLE")
check("Test 1b - zero SOL + positive quote reserve is diagnostic candidate", reserve_path_diagnosis(quote_row) == "QUOTE_RESERVE_FALLBACK_CANDIDATE")
check("Test 1c - zero SOL + zero quote is unresolved, not auto-priced", reserve_path_diagnosis(dead_row) == "SOL_AND_QUOTE_NONPOSITIVE")

events, summary = diagnose_unpriceable_rows(
    [quote_row, dict(quote_row, event_key="e4", decoded_at_utc="2026-08-18T14:00:02+00:00")],
    t0_observed_at_us=us("2026-08-18T14:00:01+00:00"),
)
check("Test 1d - all-positive quote fallback token classified consistently", summary["token_root_class"] == "ALL_TRADES_QUOTE_RESERVE_FALLBACK_CANDIDATE")
check("Test 1e - quote mint retained for root-cause analysis", summary["distinct_quote_mints"] == {"QuoteMint": 2})
check("Test 1f - diagnosis does not silently change strategy price semantics", all(e["diagnosis"] == "QUOTE_RESERVE_FALLBACK_CANDIDATE" for e in events))

# Regression: production fetches return sqlite3.Row, which does not implement .get().
import sqlite3
mem = sqlite3.connect(":memory:")
mem.row_factory = sqlite3.Row
mem.execute("""
CREATE TABLE t(
    mint TEXT,event_key TEXT,signature TEXT,event_type TEXT,decoded_at_utc TEXT,
    source_decoded_file TEXT,virtual_token_reserves TEXT,virtual_sol_reserves TEXT,
    virtual_quote_reserves TEXT,quote_mint TEXT,event_json TEXT,
    sol_amount_lamports TEXT,quote_amount_raw TEXT,real_sol_reserves TEXT,
    real_token_reserves TEXT,real_quote_reserves TEXT
)
""")
mem.execute(
    "INSERT INTO t VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
    ("Msqlite","esql","ssql","BUY","2026-08-18T14:00:01+00:00",
     "LIVE_WEBSOCKET_EVENT_V0_3_3","1000000","0","500","QuoteMint",
     json.dumps({"full_current_tail_decoded": True, "ix_name": "buy"}),
     "100","500",None,None,None),
)
sqlite_rows = mem.execute("SELECT * FROM t").fetchall()
sqlite_events, sqlite_summary = diagnose_unpriceable_rows(
    sqlite_rows,
    t0_observed_at_us=us("2026-08-18T14:00:01+00:00"),
)
mem.close()
check("Test 1g - production-style sqlite3.Row input is accepted", len(sqlite_events) == 1 and sqlite_summary["quote_candidate_events"] == 1)

control = [
    ControlEvent(1, "COLLECTOR_V0_3_START", us("2026-08-18T14:00:00+00:00"), {"raw_path": "x_v0_3_3_20260818.jsonl"}),
    ControlEvent(2, "PUMP_SUBSCRIPTION_ACTIVE", us("2026-08-18T14:00:02+00:00"), {}),
    ControlEvent(3, "WS_DISCONNECTED", us("2026-08-18T14:10:00+00:00"), {}),
    ControlEvent(4, "PUMP_SUBSCRIPTION_ACTIVE", us("2026-08-18T14:10:05+00:00"), {}),
    ControlEvent(5, "DATA_GAP_CLOSED", us("2026-08-18T14:10:06+00:00"), {
        "gap_started_at_utc": "2026-08-18T14:10:00+00:00",
        "gap_ended_at_utc": "2026-08-18T14:10:06+00:00",
    }),
    ControlEvent(6, "LIVE_COLLECTION_STOPPED", us("2026-08-18T14:20:00+00:00"), {}),
    ControlEvent(7, "COLLECTOR_V0_3_START", us("2026-08-18T14:30:00+00:00"), {"raw_path": "x_v0_3_2_20260818.jsonl"}),
    ControlEvent(8, "PUMP_SUBSCRIPTION_ACTIVE", us("2026-08-18T14:30:02+00:00"), {}),
    # Abrupt session: next start arrives without a stop.
    ControlEvent(9, "COLLECTOR_V0_3_START", us("2026-08-18T14:40:00+00:00"), {"raw_path": "x_v0_3_3_20260818.jsonl"}),
    ControlEvent(10, "PUMP_SUBSCRIPTION_ACTIVE", us("2026-08-18T14:40:02+00:00"), {}),
    ControlEvent(11, "LIVE_COLLECTION_STOPPED", us("2026-08-18T14:50:00+00:00"), {}),
]

last_obs = {
    us("2026-08-18T14:40:00+00:00"): us("2026-08-18T14:35:00+00:00"),
}


def nearest_before(cutoff: int, after: int | None):
    if cutoff in last_obs:
        value = last_obs[cutoff]
        if after is None or value >= after:
            return value
    return None

intervals, sessions, gaps, coverage = reconstruct_collector_coverage(control, nearest_before=nearest_before)
check("Test 2a - three collector sessions reconstructed", len(sessions) == 3)
check("Test 2b - reconnect splits active coverage into separate intervals", len(intervals) == 4)
check("Test 2c - explicit disconnect gap retained", len(gaps) == 1 and abs(float(gaps[0]["gap_seconds"]) - 6.0) < 1e-9)
check("Test 2d - abrupt session closes at last observation, not next start", any(i.uncertain_close and i.end_us == us("2026-08-18T14:35:00+00:00") for i in intervals))
check("Test 2e - collector version provenance retained", coverage["session_versions"] == {"v0.3.2": 1, "v0.3.3": 2})

t0s = [
    us("2026-08-18T14:05:00+00:00"),
    us("2026-08-18T14:15:00+00:00"),
    us("2026-08-18T14:32:00+00:00"),
    us("2026-08-18T14:45:00+00:00"),
]
bins = build_coverage_bins(intervals, gaps, t0s, bin_minutes=30)
check("Test 3a - fixed 30m bins created deterministically", len(bins) == 2)
check("Test 3b - token counts assigned by wall-clock bin", [b["tokens"] for b in bins] == [2, 2])
check("Test 3c - coverage distinguishes collector uptime from market count", all(0 < float(b["active_coverage_pct"]) < 100 for b in bins))
check("Test 3d - explicit gap overlap exposed separately", float(bins[0]["explicit_gap_seconds"]) == 6.0)

payload_a = build_root_cause_payload(
    unpriceable_summary={"tokens": 2, "class": "synthetic"},
    coverage_summary=coverage,
    coverage_bins=bins,
    session_rows=sessions,
)
payload_b = build_root_cause_payload(
    unpriceable_summary={"tokens": 2, "class": "synthetic"},
    coverage_summary=coverage,
    coverage_bins=bins,
    session_rows=sessions,
)
check("Test 4a - deterministic diagnostic payload", stable_sha256(payload_a) == stable_sha256(payload_b))
check("Test 4b - PnL and parameter optimization explicitly absent", payload_a["performance_pnl_included"] is False and payload_a["parameter_optimization_performed"] is False)

print()
summary_rows = [
    ("Quote-reserve root-cause classification", not any(x.startswith("Test 1") for x in failures)),
    ("Collector active-interval reconstruction", not any(x.startswith("Test 2") for x in failures)),
    ("Fixed-clock uptime coverage bins", not any(x.startswith("Test 3") for x in failures)),
    ("Deterministic diagnostic payload", not any(x.startswith("Test 4") for x in failures)),
]
for label, ok in summary_rows:
    print(f"{label:<43}: {'PASS' if ok else 'FAIL'}")
print(f"{'PnL/future-return performance used':<43}: NO")
print(f"{'Production DB touched':<43}: NO")
print(f"{'RESULT':<43}: {'PASS' if not failures else 'FAIL'}")

if failures:
    raise SystemExit(1)
