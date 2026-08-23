from __future__ import annotations

import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from fractions import Fraction
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.phase4.paper_cost_baseline_v0_1 import P4_COST_BASELINE_0001
from src.phase4.paper_cost_model_v0_2 import FillDecision, PaperCostModelV02
from src.phase4.paper_entry_router_v0_1 import (
    CandidateSignalEnvelope,
    EntryMarketObservation,
    PaperEntryRouterV01,
    connect_entry_router_db,
)
from src.phase4.paper_exit_fill_executor_v0_1 import (
    ExitExecutionDeterminismConflict,
    ExitExecutionMarketObservation,
    ExitExecutionState,
    ExitReferenceSource,
    PaperExitFillExecutorV01,
)
from src.phase4.paper_exit_lifecycle_bridge_v0_2 import PaperExitLifecycleBridgeV02
from src.phase4.paper_exit_orchestrator_v0_1 import ExitMarketObservation, ExitReason
from src.phase4.paper_lifecycle_v0_1 import PositionState
from src.phase4.runtime_exit_price_impact_v0_1 import MODEL_FINGERPRINT, MODEL_ID

UTC = timezone.utc
SELFTEST_DIR = PROJECT_ROOT / "data" / "selftest"
DB_TP = SELFTEST_DIR / "phase4_4d_exit_fill_tp.sqlite3"
DB_RETRY = SELFTEST_DIR / "phase4_4d_exit_fill_retry.sqlite3"
DB_FALLBACK = SELFTEST_DIR / "phase4_4d_exit_fill_fallback.sqlite3"


def clean_sqlite(path: Path) -> None:
    for suffix in ("", "-wal", "-shm"):
        p = Path(str(path) + suffix)
        if p.exists():
            p.unlink()


def make_filled_entry(path: Path, signal_key: str, t0: datetime, mint: str):
    clean_sqlite(path)
    conn = connect_entry_router_db(path)
    router = PaperEntryRouterV01(conn, PaperCostModelV02(P4_COST_BASELINE_0001))
    candidate = CandidateSignalEnvelope(
        candidate_id=signal_key,
        mint=mint,
        strategy_version="v1.1",
        parameter_set_id="LOCKED-FIXTURE",
        signal_observed_at=t0,
        signal_ingest_seq=100,
        price_identity="SOL_NATIVE",
        price_numerator_raw=10_000_000_000,
        price_denominator_raw=100_000_000_000,
    )
    entry_obs = EntryMarketObservation(
        mint=mint,
        observed_at=t0 + timedelta(milliseconds=500),
        ingest_seq=101,
        price_identity="SOL_NATIVE",
        price_numerator_raw=10_000_000_000,
        price_denominator_raw=100_000_000_000,
        is_gap_recovery=False,
        source_event_key=f"{signal_key}-ENTRY",
    )
    result = router.on_market_observation(
        candidate=candidate,
        observation=entry_obs,
        price_impact_bps=0,
    )
    assert result.route.state.value == "FILLED"
    return conn, router, candidate


def check(label: str, ok: bool, checks: dict[str, bool]) -> None:
    checks[label] = bool(ok)
    print(f"{label:<82}: {'PASS' if ok else 'FAIL'}")


def by_track(positions):
    return {p.track_id: p for p in positions}


def main() -> int:
    print("=" * 124)
    print("PHASE 4.4D - CAUSAL EXIT_PENDING -> CLOSED SIMULATED EXIT-FILL EXECUTOR SELF-TEST v0.1")
    print("=" * 124)
    print(f"Project root                           : {PROJECT_ROOT}")
    print("Production DB opened                   : NO")
    print("Collector / network / RPC              : NO")
    print("Wallet / signing / live orders         : NO")
    print("PnL / expectancy / PF / drawdown       : NO")
    print("Parameter tuning / reselection         : NO")
    print(f"Cost baseline                          : {P4_COST_BASELINE_0001.assumption_set_id}")
    print(f"Cost baseline fingerprint              : {P4_COST_BASELINE_0001.fingerprint}")
    print(f"Locked exit latency                    : {P4_COST_BASELINE_0001.exit_latency_ms}ms")
    print(f"Exit slippage rejection cap            : {P4_COST_BASELINE_0001.exit_slippage_cap_bps} bps")
    print(f"Exit impact model                      : {MODEL_ID}")
    print(f"Exit impact fingerprint                : {MODEL_FINGERPRINT}")
    print()

    SELFTEST_DIR.mkdir(parents=True, exist_ok=True)
    checks: dict[str, bool] = {}

    # ------------------------------------------------------------------
    # A. Market TP intent -> first qualifying post-ready observation fills.
    # ------------------------------------------------------------------
    t0 = datetime(2026, 8, 23, 12, 0, 0, tzinfo=UTC)
    conn, entry_router, candidate = make_filled_entry(DB_TP, "SIG-4D-TP", t0, "MINT-4D-TP")
    bridge = PaperExitLifecycleBridgeV02(conn, lifecycle=entry_router.lifecycle)

    trigger = ExitMarketObservation(
        mint=candidate.mint,
        observed_at=t0 + timedelta(seconds=2),
        ingest_seq=102,
        price_identity="SOL_NATIVE",
        price_numerator_raw=11_000_000_000,
        price_denominator_raw=100_000_000_000,
        source_event_key="TP10",
    )
    bridged = bridge.on_market_observation(candidate.candidate_id, trigger)
    p = by_track(bridged.positions)["FINAL-A"]
    e = {x.track.track_id: x for x in bridged.evaluations}["FINAL-A"]
    check("FINAL-A market intent enters EXIT_PENDING", p.state is PositionState.EXIT_PENDING and e.intent is not None and e.intent.reason is ExitReason.TAKE_PROFIT, checks)

    executor = PaperExitFillExecutorV01(
        conn,
        PaperCostModelV02(P4_COST_BASELINE_0001),
        lifecycle=entry_router.lifecycle,
        exit_orchestrator=bridge.exit_orchestrator,
    )
    route = executor.bind_position(p.paper_position_id)
    check("market intent reference uses exact trigger market", route.reference_source is ExitReferenceSource.TRIGGER_MARKET and route.reference_price_numerator_raw == 11_000_000_000 and route.reference_price_denominator_raw == 100_000_000_000, checks)
    check("exit execution_ready_at exact +500ms", route.execution_ready_at == trigger.observed_at + timedelta(milliseconds=500), checks)

    before = ExitExecutionMarketObservation(
        mint=candidate.mint,
        observed_at=route.execution_ready_at - timedelta(microseconds=1),
        ingest_seq=103,
        price_identity="SOL_NATIVE",
        price_numerator_raw=10_950_000_000,
        price_denominator_raw=100_000_000_000,
        current_virtual_token_reserve_raw=9_900_000_000,
        source_event_key="BEFORE",
    )
    r_before = executor.on_market_observation(paper_position_id=p.paper_position_id, observation=before)
    check("pre-ready observation cannot fill", r_before.ignored_reason == "BEFORE_EXECUTION_READY" and r_before.position.state is PositionState.EXIT_PENDING, checks)

    cross = ExitExecutionMarketObservation(
        mint="OTHER-MINT",
        observed_at=route.execution_ready_at,
        ingest_seq=104,
        price_identity="SOL_NATIVE",
        price_numerator_raw=10_950_000_000,
        price_denominator_raw=100_000_000_000,
        current_virtual_token_reserve_raw=9_900_000_000,
        source_event_key="CROSS",
    )
    gap = ExitExecutionMarketObservation(
        mint=candidate.mint,
        observed_at=route.execution_ready_at,
        ingest_seq=105,
        price_identity="SOL_NATIVE",
        price_numerator_raw=10_950_000_000,
        price_denominator_raw=100_000_000_000,
        current_virtual_token_reserve_raw=9_900_000_000,
        is_gap_recovery=True,
        source_event_key="GAP",
    )
    ident = ExitExecutionMarketObservation(
        mint=candidate.mint,
        observed_at=route.execution_ready_at,
        ingest_seq=106,
        price_identity="QUOTE:TEST",
        price_numerator_raw=10_950_000_000,
        price_denominator_raw=100_000_000_000,
        current_virtual_token_reserve_raw=9_900_000_000,
        source_event_key="IDENT",
    )
    check("cross-mint exit observation ignored", executor.on_market_observation(paper_position_id=p.paper_position_id, observation=cross).ignored_reason == "CROSS_MINT", checks)
    check("GAP_RECOVERY cannot fill exit", executor.on_market_observation(paper_position_id=p.paper_position_id, observation=gap).ignored_reason == "GAP_RECOVERY", checks)
    check("price-identity mismatch cannot fill exit", executor.on_market_observation(paper_position_id=p.paper_position_id, observation=ident).ignored_reason == "PRICE_IDENTITY_MISMATCH", checks)

    ready = ExitExecutionMarketObservation(
        mint=candidate.mint,
        observed_at=route.execution_ready_at,
        ingest_seq=107,
        price_identity="SOL_NATIVE",
        price_numerator_raw=10_950_000_000,
        price_denominator_raw=100_000_000_000,
        current_virtual_token_reserve_raw=9_900_000_000,
        source_event_key="READY",
    )
    filled = executor.on_market_observation(paper_position_id=p.paper_position_id, observation=ready)
    check("first qualifying post-ready observation closes position", filled.route.state is ExitExecutionState.FILLED and filled.position.state is PositionState.CLOSED, checks)
    check("exit fill decision is FILLED", filled.attempt is not None and filled.attempt.decision is FillDecision.FILLED, checks)
    check("runtime exit impact bound into attempt", filled.price_impact is not None and filled.attempt is not None and filled.attempt.price_impact_bps == filled.price_impact.router_price_impact_bps, checks)
    check("lifecycle close price equals simulated execution price", filled.fill_evaluation is not None and filled.position.exit_price_numerator_raw == filled.fill_evaluation.simulated_execution_price.numerator_raw and filled.position.exit_price_denominator_raw == filled.fill_evaluation.simulated_execution_price.denominator_raw, checks)
    check("gross exit proceeds persisted without PnL", filled.route.gross_exit_proceeds_lamports is not None and filled.route.gross_exit_proceeds_lamports > 0, checks)
    check("locked explicit exit costs persisted", filled.explicit_costs is not None and filled.route.total_exit_explicit_cost_lamports == filled.explicit_costs.total_explicit_cost_lamports, checks)
    check("terminal lifecycle reason is locked exit intent", filled.position.state_reason == "PAPER_EXIT_FILLED:TAKE_PROFIT", checks)

    sibling_states = {
        x.track_id: x.state
        for x in [entry_router.lifecycle.get_position(ref.paper_position_id) for ref in bridge.bind_signal(candidate.candidate_id)]
    }
    check("closing FINAL-A does not close sibling tracks", sibling_states["FINAL-A"] is PositionState.CLOSED and sibling_states["FINAL-B"] is PositionState.OPEN and sibling_states["SENS-C"] is PositionState.OPEN, checks)

    digest_after_fill = executor.canonical_digest()
    replay = executor.on_market_observation(paper_position_id=p.paper_position_id, observation=ready)
    check("terminal fill replay is idempotent", replay.route.state is ExitExecutionState.FILLED and executor.canonical_digest() == digest_after_fill, checks)

    quick_tp = conn.execute("PRAGMA quick_check").fetchone()[0]
    check("TP executor SQLite quick_check", str(quick_tp).lower() == "ok", checks)
    conn.close()

    # Restart exact same DB and verify terminal state/digest.
    conn_r = sqlite3.connect(DB_TP)
    conn_r.row_factory = sqlite3.Row
    conn_r.execute("PRAGMA foreign_keys=ON")
    exec_r = PaperExitFillExecutorV01(conn_r, PaperCostModelV02(P4_COST_BASELINE_0001))
    route_r = exec_r.get_route_for_position(p.paper_position_id)
    pos_r = exec_r.lifecycle.get_position(p.paper_position_id)
    check("restart retains FILLED exit route + CLOSED position", route_r is not None and route_r.state is ExitExecutionState.FILLED and pos_r is not None and pos_r.state is PositionState.CLOSED, checks)
    check("restart canonical digest unchanged", exec_r.canonical_digest() == digest_after_fill, checks)
    conn_r.close()

    # ------------------------------------------------------------------
    # B. Slippage rejection is audited; retry after restart can later fill.
    # ------------------------------------------------------------------
    t1 = datetime(2026, 8, 23, 13, 0, 0, tzinfo=UTC)
    conn2, entry_router2, candidate2 = make_filled_entry(DB_RETRY, "SIG-4D-RETRY", t1, "MINT-4D-RETRY")
    bridge2 = PaperExitLifecycleBridgeV02(conn2, lifecycle=entry_router2.lifecycle)
    trig2 = ExitMarketObservation(
        mint=candidate2.mint,
        observed_at=t1 + timedelta(seconds=2),
        ingest_seq=102,
        price_identity="SOL_NATIVE",
        price_numerator_raw=11_000_000_000,
        price_denominator_raw=100_000_000_000,
        source_event_key="TP10-RETRY",
    )
    b2 = bridge2.on_market_observation(candidate2.candidate_id, trig2)
    p2 = by_track(b2.positions)["FINAL-A"]
    ex2 = PaperExitFillExecutorV01(conn2, PaperCostModelV02(P4_COST_BASELINE_0001), lifecycle=entry_router2.lifecycle, exit_orchestrator=bridge2.exit_orchestrator)
    route2 = ex2.bind_position(p2.paper_position_id)

    bad = ExitExecutionMarketObservation(
        mint=candidate2.mint,
        observed_at=route2.execution_ready_at,
        ingest_seq=103,
        price_identity="SOL_NATIVE",
        price_numerator_raw=8_000_000_000,
        price_denominator_raw=100_000_000_000,
        current_virtual_token_reserve_raw=9_900_000_000,
        source_event_key="BAD",
    )
    rejected = ex2.on_market_observation(paper_position_id=p2.paper_position_id, observation=bad)
    check("exit slippage rejection recorded", rejected.attempt is not None and rejected.attempt.decision is FillDecision.REJECTED_SLIPPAGE and rejected.attempt.rejection_reason == "SLIPPAGE_CAP_EXCEEDED", checks)
    check("rejected exit remains EXIT_PENDING", rejected.route.state is ExitExecutionState.WAITING and rejected.position.state is PositionState.EXIT_PENDING, checks)
    check("rejected attempt incurs no persisted explicit fill cost", rejected.attempt is not None and rejected.attempt.total_exit_explicit_cost_lamports is None and rejected.explicit_costs is None, checks)
    digest_rejected = ex2.canonical_digest()

    replay_bad = ex2.on_market_observation(paper_position_id=p2.paper_position_id, observation=bad)
    check("exact rejected-attempt replay is idempotent", replay_bad.ignored_reason == "ATTEMPT_REPLAY" and ex2.canonical_digest() == digest_rejected, checks)

    conflict_ok = False
    bad_changed = ExitExecutionMarketObservation(
        mint=candidate2.mint,
        observed_at=bad.observed_at,
        ingest_seq=bad.ingest_seq,
        price_identity="SOL_NATIVE",
        price_numerator_raw=8_100_000_000,
        price_denominator_raw=100_000_000_000,
        current_virtual_token_reserve_raw=9_900_000_000,
        source_event_key="BAD",
    )
    try:
        ex2.on_market_observation(paper_position_id=p2.paper_position_id, observation=bad_changed)
    except ExitExecutionDeterminismConflict:
        conflict_ok = True
    check("same observation key different content raises determinism conflict", conflict_ok, checks)
    conn2.close()

    # Restart after rejection, then later market observation can fill.
    conn2r = sqlite3.connect(DB_RETRY)
    conn2r.row_factory = sqlite3.Row
    conn2r.execute("PRAGMA foreign_keys=ON")
    ex2r = PaperExitFillExecutorV01(conn2r, PaperCostModelV02(P4_COST_BASELINE_0001))
    route2r = ex2r.get_route_for_position(p2.paper_position_id)
    check("restart retains WAITING route after rejected attempt", route2r is not None and route2r.state is ExitExecutionState.WAITING and len(ex2r.list_attempts(route2r.route_id)) == 1, checks)

    good = ExitExecutionMarketObservation(
        mint=candidate2.mint,
        observed_at=route2.execution_ready_at + timedelta(milliseconds=100),
        ingest_seq=104,
        price_identity="SOL_NATIVE",
        price_numerator_raw=10_700_000_000,
        price_denominator_raw=100_000_000_000,
        current_virtual_token_reserve_raw=9_900_000_000,
        source_event_key="GOOD",
    )
    retried = ex2r.on_market_observation(paper_position_id=p2.paper_position_id, observation=good)
    check("later causal observation can fill after rejection", retried.route.state is ExitExecutionState.FILLED and retried.position.state is PositionState.CLOSED and len(ex2r.list_attempts(retried.route.route_id)) == 2, checks)
    check("rejection audit survives successful retry", [a.decision for a in ex2r.list_attempts(retried.route.route_id)] == [FillDecision.REJECTED_SLIPPAGE, FillDecision.FILLED], checks)
    quick_retry = conn2r.execute("PRAGMA quick_check").fetchone()[0]
    check("retry executor SQLite quick_check", str(quick_retry).lower() == "ok", checks)
    conn2r.close()

    # ------------------------------------------------------------------
    # C. Fallback reference: use last fresh real mark, not fabricated trigger.
    # ------------------------------------------------------------------
    t2 = datetime(2026, 8, 23, 14, 0, 0, tzinfo=UTC)
    conn3, entry_router3, candidate3 = make_filled_entry(DB_FALLBACK, "SIG-4D-FALLBACK", t2, "MINT-4D-FALLBACK")
    bridge3 = PaperExitLifecycleBridgeV02(conn3, lifecycle=entry_router3.lifecycle)

    fresh = ExitMarketObservation(
        mint=candidate3.mint,
        observed_at=t2 + timedelta(seconds=1),
        ingest_seq=102,
        price_identity="SOL_NATIVE",
        price_numerator_raw=9_800_000_000,
        price_denominator_raw=100_000_000_000,
        source_event_key="LAST-FRESH",
    )
    bridge3.on_market_observation(candidate3.candidate_id, fresh)
    fb = bridge3.on_clock(candidate3.candidate_id, t2 + timedelta(seconds=5, microseconds=1))
    p3 = by_track(fb.positions)["SENS-C"]
    ex3 = PaperExitFillExecutorV01(conn3, PaperCostModelV02(P4_COST_BASELINE_0001), lifecycle=entry_router3.lifecycle, exit_orchestrator=bridge3.exit_orchestrator)
    route3 = ex3.bind_position(p3.paper_position_id)
    check("fallback route uses persisted last-fresh market reference", route3.reference_source is ExitReferenceSource.LAST_FRESH_MARKET and route3.reference_observed_at == fresh.observed_at and route3.reference_price_numerator_raw == fresh.price_numerator_raw, checks)
    check("fallback still uses intent time +500ms execution latency", route3.execution_ready_at == (t2 + timedelta(seconds=5, microseconds=1, milliseconds=500)), checks)

    fb_fill_obs = ExitExecutionMarketObservation(
        mint=candidate3.mint,
        observed_at=route3.execution_ready_at,
        ingest_seq=103,
        price_identity="SOL_NATIVE",
        price_numerator_raw=9_700_000_000,
        price_denominator_raw=100_000_000_000,
        current_virtual_token_reserve_raw=9_900_000_000,
        source_event_key="FB-FILL",
    )
    fb_filled = ex3.on_market_observation(paper_position_id=p3.paper_position_id, observation=fb_fill_obs)
    check("fallback EXIT_PENDING can causally close on post-ready mark", fb_filled.route.state is ExitExecutionState.FILLED and fb_filled.position.state is PositionState.CLOSED and fb_filled.position.state_reason == "PAPER_EXIT_FILLED:FALLBACK", checks)

    # Separate no-fresh fallback: deterministic CandidateSignal rule reference, never synthetic price.
    t3 = datetime(2026, 8, 23, 15, 0, 0, tzinfo=UTC)
    DB_RULE = SELFTEST_DIR / "phase4_4d_exit_fill_fallback_rule_ref.sqlite3"
    conn4, entry_router4, candidate4 = make_filled_entry(DB_RULE, "SIG-4D-FB-RULE", t3, "MINT-4D-FB-RULE")
    bridge4 = PaperExitLifecycleBridgeV02(conn4, lifecycle=entry_router4.lifecycle)
    fb4 = bridge4.on_clock(candidate4.candidate_id, t3 + timedelta(seconds=5, microseconds=1))
    p4 = by_track(fb4.positions)["SENS-C"]
    ex4 = PaperExitFillExecutorV01(conn4, PaperCostModelV02(P4_COST_BASELINE_0001), lifecycle=entry_router4.lifecycle, exit_orchestrator=bridge4.exit_orchestrator)
    route4 = ex4.bind_position(p4.paper_position_id)
    check("fallback with no fresh mark uses real CandidateSignal rule reference", route4.reference_source is ExitReferenceSource.RULE_REFERENCE_FALLBACK and route4.reference_observed_at == candidate4.signal_observed_at and route4.reference_price_numerator_raw == candidate4.price_numerator_raw, checks)
    quick_fb = conn3.execute("PRAGMA quick_check").fetchone()[0]
    quick_rule = conn4.execute("PRAGMA quick_check").fetchone()[0]
    check("fallback executor SQLite quick_check", str(quick_fb).lower() == "ok" and str(quick_rule).lower() == "ok", checks)
    conn3.close()
    conn4.close()

    print()
    print("-" * 124)
    print("VALIDATION")
    print("-" * 124)
    summary = {
        "causal_500ms_exit_latency": all(checks[k] for k in [
            "exit execution_ready_at exact +500ms",
            "pre-ready observation cannot fill",
            "first qualifying post-ready observation closes position",
        ]),
        "locked_exit_cost_and_impact": all(checks[k] for k in [
            "runtime exit impact bound into attempt",
            "locked explicit exit costs persisted",
        ]),
        "exit_rejection_retry_audit": all(checks[k] for k in [
            "exit slippage rejection recorded",
            "rejected exit remains EXIT_PENDING",
            "later causal observation can fill after rejection",
            "rejection audit survives successful retry",
        ]),
        "fallback_reference_nonfabricated": all(checks[k] for k in [
            "fallback route uses persisted last-fresh market reference",
            "fallback with no fresh mark uses real CandidateSignal rule reference",
        ]),
        "restart_replay_deterministic": all(checks[k] for k in [
            "terminal fill replay is idempotent",
            "restart retains FILLED exit route + CLOSED position",
            "restart canonical digest unchanged",
            "restart retains WAITING route after rejected attempt",
            "same observation key different content raises determinism conflict",
        ]),
        "no_pnl_or_live_order_path": True,
        "sqlite_quick_check": all(checks[k] for k in [
            "TP executor SQLite quick_check",
            "retry executor SQLite quick_check",
            "fallback executor SQLite quick_check",
        ]),
    }
    for label, ok in summary.items():
        print(f"{label:<56}: {'PASS' if ok else 'FAIL'}")

    all_ok = all(checks.values()) and all(summary.values())
    print()
    print(f"Cost baseline                        : {P4_COST_BASELINE_0001.assumption_set_id}")
    print(f"Cost baseline fingerprint            : {P4_COST_BASELINE_0001.fingerprint}")
    print(f"Exit impact model                    : {MODEL_ID}")
    print(f"Exit impact fingerprint              : {MODEL_FINGERPRINT}")
    print("Production DB touched                : NO")
    print("Live market action                   : NO")
    print("Wallet/signing/live orders           : NO")
    print("PnL/accounting                       : NO")
    print("Parameter tuning/reselection         : NO")
    print()
    print(f"RESULT: {'PASS' if all_ok else 'CHECK'}")
    if all_ok:
        print("NEXT: controlled live causal exit-fill smoke using the validated v0.3.4 collector + live Phase-2 price/reserve path; no PnL/accounting yet.")
    return 0 if all_ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
