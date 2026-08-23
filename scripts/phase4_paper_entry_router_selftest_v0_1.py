from __future__ import annotations

import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.phase4.paper_cost_baseline_v0_1 import P4_COST_BASELINE_0001  # noqa: E402
from src.phase4.paper_cost_model_v0_2 import PaperCostModelV02  # noqa: E402
from src.phase4.paper_entry_router_v0_1 import (  # noqa: E402
    CandidateSignalEnvelope,
    EntryMarketObservation,
    EntryRouteState,
    PaperEntryRouterV01,
    RouteDeterminismConflict,
    canonical_entry_router_digest,
    connect_entry_router_db,
)
from src.phase4.paper_lifecycle_v0_1 import (  # noqa: E402
    LOCKED_PHASE4_TRACKS,
    OrderState,
    PaperLifecycleStore,
    PositionState,
    run_integrity_checks,
)


SELFTEST_DIR = PROJECT_ROOT / "data" / "selftest"
DB_A = SELFTEST_DIR / "phase4_paper_entry_router_selftest_a.sqlite3"
DB_B = SELFTEST_DIR / "phase4_paper_entry_router_selftest_b.sqlite3"
PRODUCTION_DB = PROJECT_ROOT / "data" / "db" / "tradingbot.sqlite3"
T0 = datetime(2026, 8, 23, 13, 30, 0, 123456, tzinfo=timezone.utc)


def candidate(candidate_id: str = "LIVE-CAND-0001", mint: str = "MINT-A") -> CandidateSignalEnvelope:
    return CandidateSignalEnvelope(
        candidate_id=candidate_id,
        mint=mint,
        strategy_version="FirstPullback-v1.1",
        parameter_set_id="ROBUST_2",
        signal_observed_at=T0,
        signal_ingest_seq=42,
        price_identity="SOL_NATIVE",
        price_numerator_raw=100,
        price_denominator_raw=100,
    )


def obs(
    ms: int,
    seq: int,
    numerator: int,
    *,
    mint: str = "MINT-A",
    identity: str = "SOL_NATIVE",
    gap: bool = False,
) -> EntryMarketObservation:
    return EntryMarketObservation(
        mint=mint,
        observed_at=T0 + timedelta(milliseconds=ms),
        ingest_seq=seq,
        price_identity=identity,
        price_numerator_raw=numerator,
        price_denominator_raw=100,
        is_gap_recovery=gap,
        source_event_key=f"E-{mint}-{ms}-{seq}-{identity}-{int(gap)}",
    )


def reset(path: Path) -> None:
    for suffix in ("", "-wal", "-shm"):
        candidate_path = Path(str(path) + suffix)
        if candidate_path.exists():
            candidate_path.unlink()


def run_scenario(path: Path) -> tuple[str, dict[str, object]]:
    reset(path)
    conn = connect_entry_router_db(path)
    lifecycle = PaperLifecycleStore(conn)
    router = PaperEntryRouterV01(
        conn,
        PaperCostModelV02(P4_COST_BASELINE_0001),
        lifecycle,
    )

    cand = candidate()

    # 1) CandidateSignal is routed to all three locked tracks, but remains a
    # strategy-layer object. The shared entry route waits until +500ms.
    first = router.route_candidate(cand)
    assert first.route.state is EntryRouteState.ENTRY_PENDING
    assert first.route.execution_ready_at == T0 + timedelta(milliseconds=500)
    assert len(first.orders) == 3
    assert {o.track_id for o in first.orders} == set(LOCKED_PHASE4_TRACKS)
    assert all(o.state is OrderState.ENTRY_PENDING for o in first.orders)

    # 2) Cross-mint, GAP_RECOVERY, price-identity mismatch and early observations
    # cannot fill the route.
    for ignored in (
        obs(600, 1, 999, mint="OTHER"),
        obs(600, 2, 999, gap=True),
        obs(600, 3, 999, identity="QUOTE:USDC"),
        obs(499, 4, 101),
    ):
        out = router.on_market_observation(
            candidate=cand,
            observation=ignored,
            price_impact_bps=100,
        )
        assert out.route.state is EntryRouteState.ENTRY_PENDING

    # 3) Restart while route is pending. Reopen same SQLite and continue.
    conn.close()
    conn = connect_entry_router_db(path)
    lifecycle = PaperLifecycleStore(conn)
    router = PaperEntryRouterV01(
        conn,
        PaperCostModelV02(P4_COST_BASELINE_0001),
        lifecycle,
    )
    reopened = router.route_candidate(cand)
    assert reopened.route.state is EntryRouteState.ENTRY_PENDING
    assert len(lifecycle.list_open_positions()) == 0

    # 4) First eligible fresh same-identity observation at/after ready_at drives
    # one shared simulated fill for FINAL-A / FINAL-B / SENS-C.
    filled = router.on_market_observation(
        candidate=cand,
        observation=obs(500, 5, 105),
        price_impact_bps=100,
    )
    assert filled.route.state is EntryRouteState.FILLED
    assert filled.fill_evaluation is not None
    assert filled.fill_evaluation.adverse_slippage_bps == 605
    assert filled.explicit_costs is not None
    assert filled.explicit_costs.total_explicit_cost_lamports == 2_355_000
    assert all(o.state is OrderState.FILLED for o in filled.orders)

    positions = lifecycle.list_open_positions()
    assert len(positions) == 3
    assert {p.track_id for p in positions} == set(LOCKED_PHASE4_TRACKS)
    assert all(p.state is PositionState.OPEN for p in positions)
    entry_pairs = {
        (p.entry_price_numerator_raw, p.entry_price_denominator_raw)
        for p in positions
    }
    assert len(entry_pairs) == 1

    # 5) Exact replay of selected fill is idempotent; it cannot create more
    # orders/positions or alter the persisted entry result.
    replay = router.on_market_observation(
        candidate=cand,
        observation=obs(500, 5, 105),
        price_impact_bps=100,
    )
    assert replay.route.state is EntryRouteState.FILLED
    assert len(lifecycle.list_open_positions()) == 3

    # 6) Conflicting terminal replay is detected when the same selected market
    # observation is supplied with different execution context.
    conflict = False
    try:
        router.on_market_observation(
            candidate=cand,
            observation=obs(500, 5, 105),
            price_impact_bps=200,
        )
    except RouteDeterminismConflict:
        conflict = True
    assert conflict

    # 7) A second candidate with >15% adverse entry slippage rejects all three
    # tracks consistently and creates no position.
    cand2 = CandidateSignalEnvelope(
        candidate_id="LIVE-CAND-0002",
        mint="MINT-B",
        strategy_version="FirstPullback-v1.1",
        parameter_set_id="ROBUST_2",
        signal_observed_at=T0 + timedelta(seconds=10),
        signal_ingest_seq=84,
        price_identity="SOL_NATIVE",
        price_numerator_raw=100,
        price_denominator_raw=100,
    )
    rejected = router.on_market_observation(
        candidate=cand2,
        observation=EntryMarketObservation(
            mint="MINT-B",
            observed_at=cand2.signal_observed_at + timedelta(milliseconds=500),
            ingest_seq=85,
            price_identity="SOL_NATIVE",
            price_numerator_raw=115,
            price_denominator_raw=100,
            source_event_key="E-REJECT",
        ),
        price_impact_bps=100,
    )
    assert rejected.route.state is EntryRouteState.REJECTED
    assert rejected.fill_evaluation is not None
    assert rejected.fill_evaluation.adverse_slippage_bps == 1615
    assert rejected.explicit_costs is None
    assert all(o.state is OrderState.REJECTED for o in rejected.orders)
    assert len(lifecycle.list_open_positions()) == 3

    # 8) SQLite integrity + deterministic route count.
    ok, issues = run_integrity_checks(conn)
    assert ok, issues
    assert conn.execute("PRAGMA quick_check").fetchone()[0] == "ok"
    assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
    assert conn.execute("SELECT COUNT(*) FROM paper_entry_routes").fetchone()[0] == 2
    assert conn.execute("SELECT COUNT(*) FROM paper_orders").fetchone()[0] == 6
    assert conn.execute("SELECT COUNT(*) FROM paper_positions").fetchone()[0] == 3

    digest = canonical_entry_router_digest(conn)
    summary = {
        "routes": conn.execute("SELECT COUNT(*) FROM paper_entry_routes").fetchone()[0],
        "orders": conn.execute("SELECT COUNT(*) FROM paper_orders").fetchone()[0],
        "positions": conn.execute("SELECT COUNT(*) FROM paper_positions").fetchone()[0],
        "filled_route_cost_lamports": filled.route.total_entry_explicit_cost_lamports,
        "conflicting_replay_detected": conflict,
    }
    conn.close()
    return digest, summary


def main() -> int:
    print("=" * 104)
    print("PHASE 4.3A - CANDIDATESIGNAL -> SIMULATED ENTRY ROUTING FOUNDATION SELF-TEST v0.1")
    print("=" * 104)
    print(f"Project root                      : {PROJECT_ROOT}")
    print(f"Self-test DB A                    : {DB_A}")
    print(f"Self-test DB B                    : {DB_B}")
    print(f"Production DB touched             : NO")
    print("Network / RPC                     : NO")
    print("Wallet / live orders              : NO")
    print("DEV-FREEZE-0002 used/tuned        : NO")
    print(f"Cost baseline                     : {P4_COST_BASELINE_0001.assumption_set_id}")
    print(f"Cost baseline fingerprint         : {P4_COST_BASELINE_0001.fingerprint}")
    print(f"Locked tracks                     : {LOCKED_PHASE4_TRACKS}")
    print()

    digest_a, summary_a = run_scenario(DB_A)
    digest_b, summary_b = run_scenario(DB_B)
    deterministic = digest_a == digest_b and summary_a == summary_b

    print("-" * 104)
    print("VALIDATION")
    print("-" * 104)
    print("CandidateSignal remains strategy boundary      : PASS")
    print("One signal -> three locked paper tracks        : PASS")
    print("FINAL-A/B share identical entry decision       : PASS")
    print("SENS-C kept separately labeled                 : PASS")
    print("500ms causal entry-ready guard                 : PASS")
    print("Cross-mint contamination blocked               : PASS")
    print("GAP_RECOVERY cannot drive fresh fill           : PASS")
    print("Price-identity mismatch cannot drive fill      : PASS")
    print("Restart-safe pending route recovery            : PASS")
    print("Shared simulated entry fill                    : PASS")
    print("Entry explicit costs persisted                 : PASS")
    print("Slippage rejection fans out consistently       : PASS")
    print("Exact replay idempotent                        : PASS")
    print("Conflicting replay detected                    : PASS")
    print("SQLite quick_check / foreign keys              : PASS")
    print(f"Logical digest A                               : {digest_a}")
    print(f"Logical digest B                               : {digest_b}")
    print(f"Cross-run deterministic equality               : {'PASS' if deterministic else 'FAIL'}")
    print()
    print("IMPORTANT: Phase 4.3A validates the deterministic routing contract only.")
    print("           It does NOT yet start a live strategy runner or network feed.")
    print("           The next bounded sub-step is Phase 4.3B: actual live CandidateSignal")
    print("           source integration + controlled live-smoke, still with NO wallet/orders.")
    print()
    print("RESULT: PASS" if deterministic else "RESULT: FAIL")
    return 0 if deterministic else 1


if __name__ == "__main__":
    raise SystemExit(main())
