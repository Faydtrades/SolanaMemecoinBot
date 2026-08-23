from __future__ import annotations

import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.phase4.paper_cost_baseline_v0_1 import P4_COST_BASELINE_0001
from src.phase4.paper_cost_model_v0_2 import PaperCostModelV02
from src.phase4.paper_entry_router_v0_1 import (
    CandidateSignalEnvelope,
    EntryMarketObservation,
    PaperEntryRouterV01,
    connect_entry_router_db,
)
from src.phase4.paper_exit_fill_executor_v0_1 import (
    ExitExecutionMarketObservation,
    ExitExecutionState,
    PaperExitFillExecutorV01,
)
from src.phase4.paper_exit_lifecycle_bridge_v0_2 import PaperExitLifecycleBridgeV02
from src.phase4.paper_exit_orchestrator_v0_1 import (
    ExitMarketObservation,
    LOCKED_EXIT_SPEC_FINGERPRINT,
    LOCKED_TRACK_ORDER,
)
from src.phase4.paper_lifecycle_v0_1 import PositionState
from src.phase4.runtime_exit_price_impact_v0_1 import (
    MODEL_FINGERPRINT as EXIT_IMPACT_FINGERPRINT,
    MODEL_ID as EXIT_IMPACT_ID,
)

UTC = timezone.utc
DB = PROJECT_ROOT / "data" / "selftest" / "phase4_4e_live_exit_pipeline_fixture.sqlite3"
EXPECTED_SPEC = "0bde5378f4ff2b63c6a96ad64140840ef8b2770c3be772a7aa71f8bdeb070129"
EXPECTED_IMPACT = "e1f1fd1c786cc679cf54f45c16b5d9a0c6aff37ed4132ac63c8abc5506aea59d"


def clean(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    for suffix in ("", "-wal", "-shm"):
        p = Path(str(path) + suffix)
        if p.exists():
            p.unlink()


def check(label: str, ok: bool, checks: dict[str, bool]) -> None:
    checks[label] = bool(ok)
    print(f"{label:<74}: {'PASS' if ok else 'FAIL'}")


def by_track(bridge, signal_key, lifecycle):
    refs = bridge.bind_signal(signal_key)
    out = {}
    for ref in refs:
        p = lifecycle.get_position(ref.paper_position_id)
        assert p is not None
        out[p.track_id] = p
    return out


def deliver_market(
    *,
    bridge,
    executor,
    lifecycle,
    signal_key: str,
    mint: str,
    observed_at: datetime,
    ingest_seq: int,
    price_num: int,
    price_den: int,
    token_reserve: int,
    source: str,
):
    bridge_obs = ExitMarketObservation(
        mint=mint,
        observed_at=observed_at,
        ingest_seq=ingest_seq,
        price_identity="SOL_NATIVE",
        price_numerator_raw=price_num,
        price_denominator_raw=price_den,
        is_gap_recovery=False,
        source_event_key=source,
    )
    bridge.on_market_observation(signal_key, bridge_obs)

    positions = by_track(bridge, signal_key, lifecycle)
    results = {}
    for track_id in LOCKED_TRACK_ORDER:
        p = positions[track_id]
        if p.state is not PositionState.EXIT_PENDING:
            continue
        executor.bind_position(p.paper_position_id)
        exec_obs = ExitExecutionMarketObservation(
            mint=mint,
            observed_at=observed_at,
            ingest_seq=ingest_seq,
            price_identity="SOL_NATIVE",
            price_numerator_raw=price_num,
            price_denominator_raw=price_den,
            current_virtual_token_reserve_raw=token_reserve,
            is_gap_recovery=False,
            source_event_key=source,
        )
        results[track_id] = executor.on_market_observation(
            paper_position_id=p.paper_position_id,
            observation=exec_obs,
        )
    return results


def main() -> int:
    print("=" * 112)
    print("PHASE 4.4E - LIVE EXIT PIPELINE FIXTURE SELF-TEST v0.1")
    print("=" * 112)
    print(f"Project root                     : {PROJECT_ROOT}")
    print("Production DB / collector / RPC  : NO")
    print("Wallet / live orders             : NO")
    print("PnL/accounting                   : NO")
    print(f"Exit spec fingerprint            : {LOCKED_EXIT_SPEC_FINGERPRINT}")
    print(f"Exit impact model                : {EXIT_IMPACT_ID}")
    print(f"Exit impact fingerprint          : {EXIT_IMPACT_FINGERPRINT}")
    print()

    checks: dict[str, bool] = {}
    clean(DB)

    t0 = datetime(2026, 8, 23, 18, 0, 0, tzinfo=UTC)
    conn = connect_entry_router_db(DB)
    cost_model = PaperCostModelV02(P4_COST_BASELINE_0001)
    router = PaperEntryRouterV01(conn, cost_model)

    candidate = CandidateSignalEnvelope(
        candidate_id="SIG-4E-FIXTURE",
        mint="MINT-4E-FIXTURE",
        strategy_version="v1.1",
        parameter_set_id="LOCKED-FIXTURE",
        signal_observed_at=t0,
        signal_ingest_seq=100,
        price_identity="SOL_NATIVE",
        price_numerator_raw=10_000_000_000,
        price_denominator_raw=100_000_000_000,
    )
    entry_obs = EntryMarketObservation(
        mint=candidate.mint,
        observed_at=t0 + timedelta(milliseconds=500),
        ingest_seq=101,
        price_identity="SOL_NATIVE",
        price_numerator_raw=10_000_000_000,
        price_denominator_raw=100_000_000_000,
        source_event_key="ENTRY",
    )
    entry = router.on_market_observation(
        candidate=candidate,
        observation=entry_obs,
        price_impact_bps=0,
    )
    check("fixture entry FILLED with three positions", entry.route.state.value == "FILLED" and len(router.lifecycle.list_open_positions()) == 3, checks)

    bridge = PaperExitLifecycleBridgeV02(conn, lifecycle=router.lifecycle)
    executor = PaperExitFillExecutorV01(
        conn,
        cost_model,
        lifecycle=router.lifecycle,
        exit_orchestrator=bridge.exit_orchestrator,
    )

    check("locked exit fingerprint exact", LOCKED_EXIT_SPEC_FINGERPRINT == EXPECTED_SPEC, checks)
    check("locked exit impact fingerprint exact", EXIT_IMPACT_FINGERPRINT == EXPECTED_IMPACT, checks)
    check("semantic locked track order exact", tuple(LOCKED_TRACK_ORDER) == ("FINAL-A", "FINAL-B", "SENS-C"), checks)

    # 1) SENS-C fallback at 5s+1us; fill at +500ms on first ready mark.
    fb_time = t0 + timedelta(seconds=5, microseconds=1)
    bridge.on_clock(candidate.candidate_id, fb_time)
    positions = by_track(bridge, candidate.candidate_id, router.lifecycle)
    check("SENS-C fallback becomes EXIT_PENDING", positions["SENS-C"].state is PositionState.EXIT_PENDING, checks)
    sens_route = executor.bind_position(positions["SENS-C"].paper_position_id)
    sens_fill_time = sens_route.execution_ready_at
    sens_results = deliver_market(
        bridge=bridge,
        executor=executor,
        lifecycle=router.lifecycle,
        signal_key=candidate.candidate_id,
        mint=candidate.mint,
        observed_at=sens_fill_time,
        ingest_seq=102,
        price_num=9_800_000_000,
        price_den=100_000_000_000,
        token_reserve=9_900_000_000,
        source="SENS-FILL",
    )
    check("SENS-C causal fallback fill CLOSED", "SENS-C" in sens_results and sens_results["SENS-C"].route.state is ExitExecutionState.FILLED and sens_results["SENS-C"].position.state is PositionState.CLOSED, checks)

    # 2) FINAL-A TP +10%; FINAL-B activation +10%.
    t10 = t0 + timedelta(seconds=10)
    deliver_market(
        bridge=bridge,
        executor=executor,
        lifecycle=router.lifecycle,
        signal_key=candidate.candidate_id,
        mint=candidate.mint,
        observed_at=t10,
        ingest_seq=103,
        price_num=11_000_000_000,
        price_den=100_000_000_000,
        token_reserve=9_900_000_000,
        source="TP10-ACT10",
    )
    positions = by_track(bridge, candidate.candidate_id, router.lifecycle)
    check("FINAL-A TP enters EXIT_PENDING while SENS-C remains CLOSED", positions["FINAL-A"].state is PositionState.EXIT_PENDING and positions["SENS-C"].state is PositionState.CLOSED, checks)
    a_route = executor.bind_position(positions["FINAL-A"].paper_position_id)

    a_results = deliver_market(
        bridge=bridge,
        executor=executor,
        lifecycle=router.lifecycle,
        signal_key=candidate.candidate_id,
        mint=candidate.mint,
        observed_at=a_route.execution_ready_at,
        ingest_seq=104,
        price_num=10_950_000_000,
        price_den=100_000_000_000,
        token_reserve=9_900_000_000,
        source="A-FILL",
    )
    check("FINAL-A causal TP fill CLOSED", "FINAL-A" in a_results and a_results["FINAL-A"].position.state is PositionState.CLOSED, checks)

    # 3) FINAL-B peak +20%, then exact 300bps giveback from peak.
    deliver_market(
        bridge=bridge,
        executor=executor,
        lifecycle=router.lifecycle,
        signal_key=candidate.candidate_id,
        mint=candidate.mint,
        observed_at=t0 + timedelta(seconds=12),
        ingest_seq=105,
        price_num=12_000_000_000,
        price_den=100_000_000_000,
        token_reserve=9_900_000_000,
        source="B-PEAK",
    )
    deliver_market(
        bridge=bridge,
        executor=executor,
        lifecycle=router.lifecycle,
        signal_key=candidate.candidate_id,
        mint=candidate.mint,
        observed_at=t0 + timedelta(seconds=13),
        ingest_seq=106,
        price_num=11_700_000_000,
        price_den=100_000_000_000,
        token_reserve=9_900_000_000,
        source="B-TRAIL",
    )
    positions = by_track(bridge, candidate.candidate_id, router.lifecycle)
    check("FINAL-B exact 300bps giveback enters EXIT_PENDING", positions["FINAL-B"].state is PositionState.EXIT_PENDING, checks)
    b_route = executor.bind_position(positions["FINAL-B"].paper_position_id)

    b_results = deliver_market(
        bridge=bridge,
        executor=executor,
        lifecycle=router.lifecycle,
        signal_key=candidate.candidate_id,
        mint=candidate.mint,
        observed_at=b_route.execution_ready_at,
        ingest_seq=107,
        price_num=11_650_000_000,
        price_den=100_000_000_000,
        token_reserve=9_900_000_000,
        source="B-FILL",
    )
    check("FINAL-B causal trail fill CLOSED", "FINAL-B" in b_results and b_results["FINAL-B"].position.state is PositionState.CLOSED, checks)

    positions = by_track(bridge, candidate.candidate_id, router.lifecycle)
    check("all three tracks CLOSED independently", all(positions[t].state is PositionState.CLOSED for t in LOCKED_TRACK_ORDER), checks)

    for track_id in LOCKED_TRACK_ORDER:
        p = positions[track_id]
        route = executor.get_route_for_position(p.paper_position_id)
        check(
            f"{track_id} persisted FILLED exit execution route",
            route is not None and route.state is ExitExecutionState.FILLED and route.gross_exit_proceeds_lamports is not None and route.total_exit_explicit_cost_lamports is not None,
            checks,
        )

    digest_before = executor.canonical_digest()
    # Terminal market replay should not mutate any closed track.
    deliver_market(
        bridge=bridge,
        executor=executor,
        lifecycle=router.lifecycle,
        signal_key=candidate.candidate_id,
        mint=candidate.mint,
        observed_at=t0 + timedelta(seconds=20),
        ingest_seq=108,
        price_num=11_500_000_000,
        price_den=100_000_000_000,
        token_reserve=9_900_000_000,
        source="TERMINAL-REPLAY",
    )
    check("terminal sibling replay canonical digest stable", executor.canonical_digest() == digest_before, checks)
    check("isolated SQLite quick_check", str(conn.execute("PRAGMA quick_check").fetchone()[0]).lower() == "ok", checks)
    conn.close()

    all_ok = all(checks.values())
    print()
    print("-" * 112)
    print("VALIDATION")
    print("-" * 112)
    print(f"all_three_parallel_exit_fills             : {'PASS' if checks.get('all three tracks CLOSED independently') else 'FAIL'}")
    print(f"partial_close_bridge_live_shape           : {'PASS' if checks.get('FINAL-A TP enters EXIT_PENDING while SENS-C remains CLOSED') else 'FAIL'}")
    print(f"terminal_replay_deterministic             : {'PASS' if checks.get('terminal sibling replay canonical digest stable') else 'FAIL'}")
    print(f"sqlite_quick_check                        : {'PASS' if checks.get('isolated SQLite quick_check') else 'FAIL'}")
    print("Production DB / live market action        : NO")
    print("Wallet/signing/live orders                : NO")
    print("PnL/accounting                            : NO")
    print()
    print(f"RESULT: {'PASS' if all_ok else 'CHECK'}")
    return 0 if all_ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
