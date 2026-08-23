from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.phase2.audit_orchestrator_v0_1 import (
    Phase2AuditOrchestratorV01,
    Phase2AuditStoreV01,
)
from src.phase2.models_v0_1 import (
    FirstPullbackParameterSet,
    FirstPullbackState,
    IngestionSource,
)
from src.phase2.phase1_readonly_adapter_v0_1 import Phase1ReadOnlyAdapterV01


DEFAULT_PROD_DB = ROOT / "data" / "db" / "tradingbot.sqlite3"
DEFAULT_AUDIT_DB = ROOT / "data" / "selftest" / "phase2_realdata_integration_selftest.sqlite3"


def check(name: str, condition: bool, detail: str = "") -> None:
    if not condition:
        raise AssertionError(f"{name}: FAIL{': ' + detail if detail else ''}")
    print(f"{name:<58} PASS")


def integration_params() -> FirstPullbackParameterSet:
    # PLUMBING/INTEGRATION VALUES ONLY.
    # These values are deliberately NOT a research result, production strategy,
    # or locked First Pullback threshold set. They merely exercise the already-
    # validated strategy code on real Phase-1 event shapes.
    return FirstPullbackParameterSet(
        parameter_set_id="FP1-INTEGRATION-PLUMBING-0001",
        strategy_version="v1.0",
        max_entry_age_ms=300_000,  # The 0-5 minute window itself is already locked.
        impulse_parameters={
            "min_return_bps": 3000,
            "min_trades_since_t0": 3,
            "min_unique_buyers_since_t0": 3,
        },
        pullback_parameters={
            "min_depth_bps": 2500,
            "max_depth_bps": 5500,
        },
        buyer_response_parameters={
            "window_id": "3s",
            "min_rebound_bps": 500,
            "min_buys": 1,
            "min_net_flow_lamports": 1,
        },
        reclaim_parameters={"min_extension_from_response_bps": 500},
        runaway_entry_parameters={"max_extension_from_response_bps": 2000},
    )


def stable_audit_export(store: Phase2AuditStoreV01) -> str:
    payload = {
        "strategy_runs": store.export_table("strategy_runs"),
        "state_transitions": store.export_table("state_transitions"),
        "decision_records": store.export_table("decision_records"),
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def run_pipeline(events, params, audit_store: Phase2AuditStoreV01):
    orch = Phase2AuditOrchestratorV01(params, audit_store)
    processed = 0
    ignored_after_lifecycle = 0
    candidate_mints: set[str] = set()
    terminal_mints: set[str] = set()
    gap_events = 0

    for event in events:
        existing = orch.runs.get(event.mint)
        if existing is not None and (
            existing.finished or existing.current_state == FirstPullbackState.CANDIDATE_SIGNAL
        ):
            # v1 has exactly one setup lifecycle per token. Continuing to evaluate a
            # finished lifecycle adds no strategy information and can create repetitive
            # terminal audit rows in the v0.1 store, so the integration runner stops
            # strategy evaluation for that mint after its material outcome.
            ignored_after_lifecycle += 1
            continue

        result = orch.process(event)
        processed += 1
        if event.source == IngestionSource.GAP_RECOVERY:
            gap_events += 1
        if result.evaluation.candidate_signal is not None:
            candidate_mints.add(event.mint)
        if result.strategy_run.finished:
            terminal_mints.add(event.mint)

    return {
        "processed": processed,
        "ignored_after_lifecycle": ignored_after_lifecycle,
        "candidate_mints": len(candidate_mints),
        "terminal_mints": len(terminal_mints),
        "gap_events_processed": gap_events,
        "runs": audit_store.count("strategy_runs"),
        "transitions": audit_store.count("state_transitions"),
        "decisions": audit_store.count("decision_records"),
    }


def main(db_path: Path, audit_path: Path, token_limit: int, events_per_token: int) -> None:
    print("PHASE 2 REAL-DATA READ-ONLY INTEGRATION SELF-TEST v0.1")
    print("Phase-1 source DB       :", db_path)
    print("Phase-2 audit DB        :", audit_path)
    print("Network/RPC             : DISABLED")
    print("Production DB mode      : READ ONLY")
    print("Wallet/execution        : DISABLED")
    print("Risk Manager            : NOT USED")
    print("Strategy thresholds     : INTEGRATION PLUMBING ONLY")
    print("Performance conclusions : FORBIDDEN FROM THIS TEST")
    print()

    before_stat = db_path.stat()
    conn = Phase1ReadOnlyAdapterV01.open_readonly(db_path)
    try:
        query_only = int(conn.execute("PRAGMA query_only").fetchone()[0])
        check("Test 1a - production SQLite opened query_only", query_only == 1)

        Phase1ReadOnlyAdapterV01.validate_schema(conn)
        check("Test 1b - Phase-1 pump_events schema compatible", True)

        source_summary = Phase1ReadOnlyAdapterV01.source_summary(conn)
        bot_truth_rows = sum(
            n
            for source, n in source_summary
            if Phase1ReadOnlyAdapterV01.is_bot_truth_source(source)
        )
        check("Test 1c - BOT_TRUTH v0.3.x source rows exist", bot_truth_rows > 0)

        cohort = Phase1ReadOnlyAdapterV01.select_cohort(
            conn, token_limit=token_limit, min_trades=3
        )
        check("Test 2a - real token cohort selected", len(cohort.mints) > 0)

        rows = Phase1ReadOnlyAdapterV01.fetch_rows_for_mints(
            conn,
            cohort.mints,
            max_events_per_token=events_per_token,
        )
        check("Test 2b - real Phase-1 rows fetched", len(rows) > 0)

        events, skipped = Phase1ReadOnlyAdapterV01.normalize_rows(rows)
        check("Test 2c - rows normalize to NME-0.1", len(events) > 0)
        check(
            "Test 2d - normalized events preserve deterministic order",
            all(
                (events[i - 1].observed_at_us, events[i - 1].ingest_seq)
                < (events[i].observed_at_us, events[i].ingest_seq)
                for i in range(1, len(events))
            ),
        )
        check(
            "Test 2e - only authoritative LAUNCH/BUY/SELL passed",
            all(e.event_type.value in {"LAUNCH", "BUY", "SELL"} for e in events),
        )
        check(
            "Test 2f - source provenance mapped to LIVE/GAP",
            all(e.source in {IngestionSource.LIVE_WS, IngestionSource.GAP_RECOVERY} for e in events),
        )
        check(
            "Test 2g - event_at and observed_at both populated",
            all(e.event_at_us > 0 and e.observed_at_us > 0 for e in events),
        )

        # Release the production DB before any Phase-2 writable SQLite is opened.
    finally:
        conn.close()

    after_read_stat = db_path.stat()
    check(
        "Test 3a - production DB size unchanged after read",
        before_stat.st_size == after_read_stat.st_size,
    )
    check(
        "Test 3b - production DB mtime unchanged after read",
        before_stat.st_mtime_ns == after_read_stat.st_mtime_ns,
    )

    audit_path.parent.mkdir(parents=True, exist_ok=True)
    if audit_path.exists():
        audit_path.unlink()

    params = integration_params()
    store_a = Phase2AuditStoreV01(audit_path)
    try:
        summary_a = run_pipeline(events, params, store_a)
        export_a = stable_audit_export(store_a)
        check("Test 4a - real events traverse full Phase-2 pipeline", summary_a["processed"] > 0)
        check("Test 4b - every processed mint has a strategy run", summary_a["runs"] > 0)
        check("Test 4c - state transitions persist to isolated audit DB", summary_a["transitions"] > 0)
        # Decision count may legitimately be zero for a tiny cohort with arbitrary
        # plumbing thresholds, so it is reported rather than used as a PASS gate.
    finally:
        store_a.close()

    # Deterministic replay of the same real event list into an in-memory audit store.
    store_b = Phase2AuditStoreV01(":memory:")
    try:
        summary_b = run_pipeline(events, params, store_b)
        export_b = stable_audit_export(store_b)
        check("Test 5a - deterministic real-data pipeline summary", summary_a == summary_b)
        check("Test 5b - deterministic real-data audit replay", export_a == export_b)
    finally:
        store_b.close()

    # Reopen persisted audit DB to prove durability and integrity.
    reopened = Phase2AuditStoreV01(audit_path)
    try:
        quick = reopened.conn.execute("PRAGMA quick_check").fetchone()[0]
        fk = reopened.conn.execute("PRAGMA foreign_key_check").fetchall()
        check("Test 6a - isolated audit SQLite reopens", reopened.count("strategy_runs") == summary_a["runs"])
        check("Test 6b - isolated audit SQLite quick_check", quick == "ok")
        check("Test 6c - isolated audit foreign keys", len(fk) == 0)
    finally:
        reopened.close()

    final_stat = db_path.stat()
    check(
        "Test 7a - production DB remains unchanged through full test",
        before_stat.st_size == final_stat.st_size
        and before_stat.st_mtime_ns == final_stat.st_mtime_ns,
    )

    gap_count = sum(1 for e in events if e.source == IngestionSource.GAP_RECOVERY)
    live_count = len(events) - gap_count

    print()
    print("-" * 78)
    print("REAL-DATA INTEGRATION SUMMARY")
    print("-" * 78)
    print(f"BOT_TRUTH source rows available        : {bot_truth_rows}")
    print(f"Cohort tokens                          : {len(cohort.mints)}")
    print(f"Requested min trades/token             : {cohort.requested_min_trades}")
    print(f"Effective min trades/token             : {cohort.effective_min_trades}")
    print(f"Phase-1 rows selected                  : {len(rows)}")
    print(f"Normalized NME events                  : {len(events)}")
    print(f"Adapter rows skipped                   : {sum(skipped.values())}")
    if skipped:
        print("Skip reasons                           : " + json.dumps(skipped, sort_keys=True))
    print(f"LIVE_WS events                         : {live_count}")
    print(f"GAP_RECOVERY events                    : {gap_count}")
    print(f"Events strategy-evaluated              : {summary_a['processed']}")
    print(f"Events ignored after lifecycle outcome : {summary_a['ignored_after_lifecycle']}")
    print(f"Strategy runs                          : {summary_a['runs']}")
    print(f"State transitions                      : {summary_a['transitions']}")
    print(f"Decision records                       : {summary_a['decisions']}")
    print(f"Candidate mints (PLUMBING ONLY)        : {summary_a['candidate_mints']}")
    print(f"Terminal mints                         : {summary_a['terminal_mints']}")
    print()
    print("IMPORTANT: candidate/decision counts above are NOT a strategy-performance result.")
    print("The parameter values are integration-only and must not be used for trading conclusions.")
    print()
    print("Production DB opened read-only         : YES")
    print("Production DB touched by test          : NO")
    print("Separate Phase-2 audit DB written      : YES")
    print("Network/RPC used                       : NO")
    print("Wallet/order/execution used            : NO")
    print("Risk Manager used                      : NO")
    print("RESULT                                 : PASS")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Read-only Phase-1 -> Phase-2 real-data integration self-test"
    )
    parser.add_argument("--db", type=Path, default=DEFAULT_PROD_DB)
    parser.add_argument("--audit-db", type=Path, default=DEFAULT_AUDIT_DB)
    parser.add_argument("--tokens", type=int, default=12)
    parser.add_argument("--events-per-token", type=int, default=120)
    args = parser.parse_args()

    try:
        main(args.db, args.audit_db, args.tokens, args.events_per_token)
    except Exception as exc:
        print()
        print(f"RESULT                                 : FAIL")
        print(f"ERROR                                  : {type(exc).__name__}: {exc}")
        raise
