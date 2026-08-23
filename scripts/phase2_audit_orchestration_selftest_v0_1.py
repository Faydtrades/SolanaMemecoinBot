from __future__ import annotations

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
    EventType,
    FirstPullbackParameterSet,
    FirstPullbackState,
    IngestionSource,
    NormalizedMarketEvent,
)


def check(name: str, condition: bool, detail: str = "") -> None:
    if not condition:
        raise AssertionError(f"{name}: FAIL{': ' + detail if detail else ''}")
    print(f"{name:<56} PASS")


def stat_signature(path: Path):
    if not path.exists():
        return (False, None, None)
    st = path.stat()
    return (True, st.st_size, st.st_mtime_ns)


def test_params() -> FirstPullbackParameterSet:
    # SYNTHETIC SELF-TEST VALUES ONLY. Not production strategy thresholds.
    return FirstPullbackParameterSet(
        parameter_set_id="FP1-AUDIT-SELFTEST-0001",
        strategy_version="v1.0",
        max_entry_age_ms=300_000,
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


def event(
    mint: str,
    seq: int,
    event_type: EventType,
    event_s: float,
    price: int | None,
    sol_lamports: int = 0,
    user: str | None = None,
    *,
    observed_s: float | None = None,
) -> NormalizedMarketEvent:
    if observed_s is None:
        observed_s = event_s
    return NormalizedMarketEvent(
        schema_version="NME-0.1",
        event_key=f"{mint}-evt-{seq:04d}",
        ingest_seq=seq,
        mint=mint,
        event_type=event_type,
        event_at_us=int(event_s * 1_000_000),
        observed_at_us=int(observed_s * 1_000_000),
        slot=3_000_000 + seq,
        signature=f"{mint}-sig-{seq:04d}",
        event_index=0,
        user=user,
        creator=f"creator-{mint}" if event_type == EventType.LAUNCH else None,
        sol_amount_lamports=sol_lamports if event_type != EventType.LAUNCH else None,
        token_amount_raw=1_000_000 if event_type != EventType.LAUNCH else None,
        virtual_sol_reserve_lamports=(price * 1_000_000 if price is not None else None),
        virtual_token_reserve_raw=(1_000_000 if price is not None else None),
        source=IngestionSource.SYNTHETIC_TEST,
    )


def candidate_path(mint: str):
    return [
        event(mint, 1, EventType.BUY, 0.0, 100, 1_000_000_000, "A"),
        event(mint, 2, EventType.BUY, 0.5, 120, 500_000_000, "B"),
        event(mint, 3, EventType.BUY, 1.0, 150, 500_000_000, "C"),
        event(mint, 4, EventType.SELL, 1.1, 145, 200_000_000, "D"),
        event(mint, 5, EventType.SELL, 1.5, 110, 500_000_000, "E"),
        event(mint, 6, EventType.BUY, 1.6, 111, 100_000_000, "F"),
        event(mint, 7, EventType.BUY, 1.9, 118, 700_000_000, "G"),
        event(mint, 8, EventType.BUY, 2.2, 125, 400_000_000, "H"),
    ]


def invalidated_path(mint: str):
    return [
        event(mint, 1, EventType.BUY, 0.0, 100, 1_000_000_000, "A"),
        event(mint, 2, EventType.BUY, 0.5, 120, 500_000_000, "B"),
        event(mint, 3, EventType.BUY, 1.0, 150, 500_000_000, "C"),
        event(mint, 4, EventType.SELL, 1.1, 145, 200_000_000, "D"),
        event(mint, 5, EventType.SELL, 1.5, 60, 500_000_000, "E"),
    ]


def expired_path(mint: str):
    return [
        event(mint, 1, EventType.BUY, 0.0, 100, 1_000_000_000, "A"),
        event(mint, 2, EventType.BUY, 300.0, 105, 100_000_000, "B"),
        event(mint, 3, EventType.BUY, 300.001, 106, 100_000_000, "C"),
    ]


def rejected_runaway_path(mint: str):
    base = candidate_path(mint)[:-1]
    return base + [event(mint, 8, EventType.BUY, 2.2, 160, 400_000_000, "H")]


def run_dataset(store: Phase2AuditStoreV01, params: FirstPullbackParameterSet):
    orch = Phase2AuditOrchestratorV01(params, store)
    paths = {
        "AuditCandidateMint": candidate_path("AuditCandidateMint"),
        "AuditInvalidMint": invalidated_path("AuditInvalidMint"),
        "AuditExpiredMint": expired_path("AuditExpiredMint"),
        "AuditRejectMint": rejected_runaway_path("AuditRejectMint"),
    }
    results = {}
    for mint, events in paths.items():
        per_token = []
        for evt in events:
            per_token.append(orch.process(evt))
        results[mint] = per_token
    return results


def snapshot_store(store: Phase2AuditStoreV01) -> str:
    payload = {
        "strategy_runs": store.export_table("strategy_runs"),
        "state_transitions": store.export_table("state_transitions"),
        "decision_records": store.export_table("decision_records"),
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def main() -> None:
    print("PHASE 2 AUDIT + ORCHESTRATION SELF-TEST v0.1")
    print("Synthetic thresholds only: YES")
    print("Execution/Risk Manager used: NO")
    print("Production DB opened by test: NO")
    print()

    params = test_params()
    selftest_dir = ROOT / "data" / "selftest"
    selftest_dir.mkdir(parents=True, exist_ok=True)
    audit_db = selftest_dir / "phase2_audit_selftest.sqlite3"
    if audit_db.exists():
        audit_db.unlink()

    production_db = ROOT / "data" / "db" / "tradingbot.sqlite3"
    prod_before = stat_signature(production_db)

    store = Phase2AuditStoreV01(audit_db)
    results = run_dataset(store, params)

    # 1. Schema / Strategy Runs: every evaluated token is represented.
    check("Test 1a - isolated audit DB created", audit_db.exists())
    check("Test 1b - all evaluated tokens have strategy runs", store.count("strategy_runs") == 4)
    mints = {
        row["mint"]
        for row in store.conn.execute("SELECT mint FROM strategy_runs").fetchall()
    }
    check(
        "Test 1c - candidate/skips all represented",
        mints == {"AuditCandidateMint", "AuditInvalidMint", "AuditExpiredMint", "AuditRejectMint"},
        str(sorted(mints)),
    )

    # 2. Candidate is audited honestly as pending downstream risk, never fake TRADE.
    candidate_decision = store.fetch_one(
        "SELECT * FROM decision_records WHERE mint=?",
        ("AuditCandidateMint",),
    )
    candidate_run = store.fetch_one(
        "SELECT * FROM strategy_runs WHERE mint=?",
        ("AuditCandidateMint",),
    )
    check("Test 2a - candidate decision persisted", candidate_decision is not None)
    check("Test 2b - candidate signal is ENTRY_LONG", candidate_decision["signal"] == "ENTRY_LONG")
    check("Test 2c - candidate is PENDING_RISK", candidate_decision["trade_or_skip"] == "PENDING_RISK")
    check("Test 2d - no fake risk approval", candidate_decision["risk_decision"] == "NOT_EVALUATED_PHASE2")
    check("Test 2e - candidate is not mislabeled TRADE", candidate_run["current_state"] == "CANDIDATE_SIGNAL" and candidate_run["finished"] == 0 and candidate_run["final_outcome"] is None)

    # 3. Terminal strategy outcomes are structured SKIP decisions.
    expected_terminal = {
        "AuditInvalidMint": ("INVALIDATED", "PULLBACK_TOO_DEEP"),
        "AuditExpiredMint": ("EXPIRED", "ENTRY_WINDOW_EXPIRED"),
        "AuditRejectMint": ("REJECT", "PRICE_RAN_AWAY"),
    }
    for idx, (mint, (outcome, reason)) in enumerate(expected_terminal.items(), start=3):
        run_row = store.fetch_one("SELECT * FROM strategy_runs WHERE mint=?", (mint,))
        decision_row = store.fetch_one("SELECT * FROM decision_records WHERE mint=?", (mint,))
        check(f"Test {idx}a - {mint} terminal outcome", run_row["finished"] == 1 and run_row["final_outcome"] == outcome)
        check(f"Test {idx}b - {mint} stored as SKIP", decision_row["trade_or_skip"] == "SKIP")
        check(f"Test {idx}c - {mint} structured skip reason", decision_row["skip_reason"] == reason and decision_row["signal_reason"] == reason)

    # 6. Transitions preserve causal event/observation times and relevant snapshots.
    transitions = store.conn.execute(
        "SELECT * FROM state_transitions ORDER BY observed_at_us, transition_id"
    ).fetchall()
    check("Test 6a - state transitions persisted", len(transitions) > 0)
    check("Test 6b - transitions carry from/to state", all(r["from_state"] and r["to_state"] for r in transitions))
    check("Test 6c - transitions carry structured reasons", all(r["reason_code"] for r in transitions))
    check("Test 6d - event_at and observed_at persisted", all(r["event_at_us"] <= r["observed_at_us"] for r in transitions))
    parsed_snapshots = [json.loads(r["feature_snapshot_json"]) for r in transitions]
    check("Test 6e - relevant feature snapshots are readable", all("market_state_version" in s and "state_seq" in s for s in parsed_snapshots))
    check("Test 6f - MarketState/Feature versions traceable", all(r["market_state_version"] == "MS-0.1" and r["feature_version"] == "FE-0.1" for r in transitions))

    # 7. Decision records contain filter and version traceability.
    decisions = store.conn.execute("SELECT * FROM decision_records").fetchall()
    check("Test 7a - one material decision per test token", len(decisions) == 4, str(len(decisions)))
    check("Test 7b - strategy version traceable", all(r["strategy_version"] == "v1.0" for r in decisions))
    check("Test 7c - parameter set traceable", all(r["parameter_set_id"] == params.parameter_set_id for r in decisions))
    check("Test 7d - filters persisted as structured JSON", all(isinstance(json.loads(r["filters_passed_json"]), list) and isinstance(json.loads(r["filters_failed_json"]), list) for r in decisions))
    check("Test 7e - candidate score explicitly absent", all(r["candidate_score"] is None for r in decisions))

    # 8. Replaying the exact same dataset into the same audit DB is idempotent.
    counts_before = tuple(store.count(t) for t in ("strategy_runs", "state_transitions", "decision_records"))
    run_dataset(store, params)
    counts_after = tuple(store.count(t) for t in ("strategy_runs", "state_transitions", "decision_records"))
    check("Test 8a - replay does not duplicate audit rows", counts_before == counts_after, f"{counts_before} -> {counts_after}")

    # 9. Independent replay produces identical audit content.
    canonical_a = snapshot_store(store)
    store_b = Phase2AuditStoreV01(":memory:")
    run_dataset(store_b, params)
    canonical_b = snapshot_store(store_b)
    check("Test 9a - deterministic audit replay", canonical_a == canonical_b)
    store_b.close()

    # 10. SQLite persistence survives close/reopen of the isolated DB.
    persisted_counts = counts_after
    store.close()
    reopened = Phase2AuditStoreV01(audit_db)
    reopened_counts = tuple(reopened.count(t) for t in ("strategy_runs", "state_transitions", "decision_records"))
    check("Test 10a - audit DB survives reopen", reopened_counts == persisted_counts)
    check("Test 10b - SQLite quick_check PASS", reopened.conn.execute("PRAGMA quick_check").fetchone()[0] == "ok")
    check("Test 10c - foreign_key_check clean", reopened.conn.execute("PRAGMA foreign_key_check").fetchall() == [])
    reopened.close()

    prod_after = stat_signature(production_db)
    check("Test 11a - production DB untouched", prod_before == prod_after)

    # Directly verify the live-free boundary from the orchestration outputs.
    candidate_last = results["AuditCandidateMint"][-1]
    check("Test 11b - orchestration returns MarketState", candidate_last.market_state.schema_version == "MS-0.1")
    check("Test 11c - orchestration returns StrategyEvaluation", candidate_last.evaluation.signal_type == "ENTRY_LONG")
    check("Test 11d - no execution/risk fields invented", not hasattr(candidate_last.evaluation.candidate_signal, "position_size"))

    print()
    print("All evaluated tokens audited              : PASS")
    print("State transitions + reason codes          : PASS")
    print("Signal/skip decision records              : PASS")
    print("Candidate remains PENDING_RISK            : PASS")
    print("No fake TRADE / risk approval             : PASS")
    print("Version/parameter traceability            : PASS")
    print("Idempotent audit replay                   : PASS")
    print("Deterministic audit replay                : PASS")
    print("Isolated SQLite persistence               : PASS")
    print("Production DB touched                     : NO")
    print("Execution/Risk Manager used               : NO")
    print("RESULT                                    : PASS")


if __name__ == "__main__":
    main()
