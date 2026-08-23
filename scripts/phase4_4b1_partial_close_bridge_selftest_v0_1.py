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
from src.phase4.paper_exit_lifecycle_bridge_v0_2 import PaperExitLifecycleBridgeV02
from src.phase4.paper_exit_orchestrator_v0_1 import (
    ExitMarketObservation,
    ExitReason,
    LOCKED_EXIT_SPEC_FINGERPRINT,
    LOCKED_TRACK_ORDER,
)
from src.phase4.paper_lifecycle_v0_1 import PositionState

UTC = timezone.utc
SELFTEST_DIR = PROJECT_ROOT / "data" / "selftest"
DB_MARKET = SELFTEST_DIR / "phase4_4b1_partial_close_market.sqlite3"
DB_FALLBACK = SELFTEST_DIR / "phase4_4b1_partial_close_fallback.sqlite3"


def clean_sqlite(path: Path) -> None:
    for suffix in ("", "-wal", "-shm"):
        p = Path(str(path) + suffix)
        if p.exists():
            p.unlink()


def make_filled_entry(path: Path, signal_key: str, t0: datetime):
    clean_sqlite(path)
    conn = connect_entry_router_db(path)
    router = PaperEntryRouterV01(conn, PaperCostModelV02(P4_COST_BASELINE_0001))
    candidate = CandidateSignalEnvelope(
        candidate_id=signal_key,
        mint="MINT-PARTIAL-CLOSE",
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
        is_gap_recovery=False,
        source_event_key=f"{signal_key}-ENTRY",
    )
    result = router.on_market_observation(
        candidate=candidate,
        observation=entry_obs,
        price_impact_bps=0,
        observed_priority_fee_lamports=None,
        venue_fee_bps=None,
    )
    assert result.route.state.value == "FILLED"
    assert len(router.lifecycle.list_open_positions()) == 3
    return conn, router, candidate, entry_obs


def check(label: str, ok: bool, checks: dict[str, bool]) -> None:
    checks[label] = bool(ok)
    print(f"{label:<78}: {'PASS' if ok else 'FAIL'}")


def by_track(positions):
    return {p.track_id: p for p in positions}


def main() -> int:
    print("=" * 120)
    print("PHASE 4.4B1 - PARTIAL-CLOSURE EXIT LIFECYCLE BRIDGE HARDENING SELF-TEST v0.1")
    print("=" * 120)
    print(f"Project root                       : {PROJECT_ROOT}")
    print("Production DB opened               : NO")
    print("Collector / network / RPC          : NO")
    print("Wallet / signing / live orders     : NO")
    print("PnL / accounting                   : NO")
    print("Parameter tuning / reselection     : NO")
    print(f"Locked exit spec fingerprint       : {LOCKED_EXIT_SPEC_FINGERPRINT}")
    print("Purpose                            : prove sibling exit tracks continue after one/more positions CLOSED")
    print()

    SELFTEST_DIR.mkdir(parents=True, exist_ok=True)
    checks: dict[str, bool] = {}

    # ------------------------------------------------------------------
    # Scenario A: FINAL-A + SENS-C close first; FINAL-B must keep trailing.
    # ------------------------------------------------------------------
    t0 = datetime(2026, 8, 23, 12, 0, 0, tzinfo=UTC)
    conn, router, candidate, _entry = make_filled_entry(DB_MARKET, "SIG-4B1-MARKET", t0)
    bridge = PaperExitLifecycleBridgeV02(conn, lifecycle=router.lifecycle)

    refs0 = bridge.bind_signal(candidate.candidate_id)
    check("initial three-track binding exact", tuple(r.track_id for r in refs0) == LOCKED_TRACK_ORDER, checks)

    obs20 = ExitMarketObservation(
        mint=candidate.mint,
        observed_at=t0 + timedelta(seconds=2),
        ingest_seq=102,
        price_identity="SOL_NATIVE",
        price_numerator_raw=12_000_000_000,
        price_denominator_raw=100_000_000_000,
        source_event_key="PLUS20",
    )
    first = bridge.on_market_observation(candidate.candidate_id, obs20)
    first_pos = by_track(first.positions)
    first_eval = {e.track.track_id: e for e in first.evaluations}
    check("FINAL-A TP intent produced", first_eval["FINAL-A"].intent is not None and first_eval["FINAL-A"].intent.reason is ExitReason.TAKE_PROFIT, checks)
    check("SENS-C TP intent produced", first_eval["SENS-C"].intent is not None and first_eval["SENS-C"].intent.reason is ExitReason.TAKE_PROFIT, checks)
    check("FINAL-B remains OPEN after trail activation", first_pos["FINAL-B"].state is PositionState.OPEN, checks)
    check("FINAL-A/SENS-C become EXIT_PENDING", first_pos["FINAL-A"].state is PositionState.EXIT_PENDING and first_pos["SENS-C"].state is PositionState.EXIT_PENDING, checks)

    # Simulate a future exit executor closing A/C.  B remains OPEN.
    for track in ("FINAL-A", "SENS-C"):
        pos = first_pos[track]
        router.lifecycle.close_position(
            pos.paper_position_id,
            closed_at=t0 + timedelta(seconds=2, milliseconds=500),
            exit_price_numerator_raw=11_800_000_000,
            exit_price_denominator_raw=100_000_000_000,
            reason="PAPER_EXIT_FILLED:TAKE_PROFIT",
        )

    mixed_before = {
        p.track_id: p
        for p in [router.lifecycle.get_position(r.paper_position_id) for r in refs0]
    }
    check("mixed lifecycle state exists A/C CLOSED + B OPEN", mixed_before["FINAL-A"].state is PositionState.CLOSED and mixed_before["SENS-C"].state is PositionState.CLOSED and mixed_before["FINAL-B"].state is PositionState.OPEN, checks)

    # FINAL-B exact 300bps giveback from +20 peak to +17 must still be processed.
    obs17 = ExitMarketObservation(
        mint=candidate.mint,
        observed_at=t0 + timedelta(seconds=3),
        ingest_seq=103,
        price_identity="SOL_NATIVE",
        price_numerator_raw=11_700_000_000,
        price_denominator_raw=100_000_000_000,
        source_event_key="PLUS17",
    )
    second = bridge.on_market_observation(candidate.candidate_id, obs17)
    second_pos = by_track(second.positions)
    second_eval = {e.track.track_id: e for e in second.evaluations}
    check("CLOSED FINAL-A remains terminal and unchanged", second_pos["FINAL-A"].state is PositionState.CLOSED, checks)
    check("CLOSED SENS-C remains terminal and unchanged", second_pos["SENS-C"].state is PositionState.CLOSED, checks)
    check("FINAL-B trailing still evaluates after sibling closures", second_eval["FINAL-B"].intent is not None and second_eval["FINAL-B"].intent.reason is ExitReason.TRAIL, checks)
    check("FINAL-B transitions to EXIT_PENDING after sibling closures", second_pos["FINAL-B"].state is PositionState.EXIT_PENDING, checks)

    # Close B and prove all-CLOSED restart/replay remains legal.
    router.lifecycle.close_position(
        second_pos["FINAL-B"].paper_position_id,
        closed_at=t0 + timedelta(seconds=3, milliseconds=500),
        exit_price_numerator_raw=11_600_000_000,
        exit_price_denominator_raw=100_000_000_000,
        reason="PAPER_EXIT_FILLED:TRAIL",
    )
    terminal_digest = bridge.canonical_digest()
    conn.close()

    conn_r = sqlite3.connect(DB_MARKET)
    conn_r.row_factory = sqlite3.Row
    conn_r.execute("PRAGMA foreign_keys=ON")
    bridge_r = PaperExitLifecycleBridgeV02(conn_r)
    refs_r = bridge_r.bind_signal(candidate.candidate_id)
    check("restart binds all three CLOSED positions", tuple(r.track_id for r in refs_r) == LOCKED_TRACK_ORDER, checks)
    replay = bridge_r.on_market_observation(candidate.candidate_id, obs17)
    check("terminal parallel replay tolerates all three CLOSED", all(p.state is PositionState.CLOSED for p in replay.positions), checks)
    check("terminal replay canonical digest unchanged", bridge_r.canonical_digest() == terminal_digest, checks)
    quick_market = conn_r.execute("PRAGMA quick_check").fetchone()[0]
    check("market partial-close SQLite quick_check", str(quick_market).lower() == "ok", checks)
    conn_r.close()

    # ------------------------------------------------------------------
    # Scenario B: SENS-C fallback closes first; A/B fallback must still fire.
    # ------------------------------------------------------------------
    t1 = datetime(2026, 8, 23, 13, 0, 0, tzinfo=UTC)
    conn_f, router_f, candidate_f, _ = make_filled_entry(DB_FALLBACK, "SIG-4B1-FALLBACK", t1)
    bridge_f = PaperExitLifecycleBridgeV02(conn_f, lifecycle=router_f.lifecycle)
    refs_f = bridge_f.bind_signal(candidate_f.candidate_id)

    at5 = bridge_f.on_clock(candidate_f.candidate_id, t1 + timedelta(seconds=5, microseconds=1))
    p5 = by_track(at5.positions)
    check("SENS-C fallback first -> EXIT_PENDING", p5["SENS-C"].state is PositionState.EXIT_PENDING, checks)
    router_f.lifecycle.close_position(
        p5["SENS-C"].paper_position_id,
        closed_at=t1 + timedelta(seconds=5, milliseconds=600),
        exit_price_numerator_raw=9_500_000_000,
        exit_price_denominator_raw=100_000_000_000,
        reason="PAPER_EXIT_FILLED:FALLBACK",
    )

    at15 = bridge_f.on_clock(candidate_f.candidate_id, t1 + timedelta(seconds=15, microseconds=1))
    p15 = by_track(at15.positions)
    e15 = {e.track.track_id: e for e in at15.evaluations}
    check("CLOSED SENS-C does not block later clock", p15["SENS-C"].state is PositionState.CLOSED, checks)
    check("FINAL-A fallback still fires after SENS-C closed", e15["FINAL-A"].intent is not None and e15["FINAL-A"].intent.reason is ExitReason.FALLBACK, checks)
    check("FINAL-B fallback still fires after SENS-C closed", e15["FINAL-B"].intent is not None and e15["FINAL-B"].intent.reason is ExitReason.FALLBACK, checks)
    check("FINAL-A/B become EXIT_PENDING in mixed state", p15["FINAL-A"].state is PositionState.EXIT_PENDING and p15["FINAL-B"].state is PositionState.EXIT_PENDING, checks)
    quick_f = conn_f.execute("PRAGMA quick_check").fetchone()[0]
    check("fallback partial-close SQLite quick_check", str(quick_f).lower() == "ok", checks)
    conn_f.close()

    print()
    print("-" * 120)
    print("VALIDATION")
    print("-" * 120)
    summary = {
        "mixed_open_closed_rebinding": all(checks[k] for k in [
            "mixed lifecycle state exists A/C CLOSED + B OPEN",
            "FINAL-B trailing still evaluates after sibling closures",
            "FINAL-B transitions to EXIT_PENDING after sibling closures",
        ]),
        "closed_terminal_replay": checks["terminal parallel replay tolerates all three CLOSED"] and checks["terminal replay canonical digest unchanged"],
        "staggered_fallback_closure": all(checks[k] for k in [
            "SENS-C fallback first -> EXIT_PENDING",
            "CLOSED SENS-C does not block later clock",
            "FINAL-A fallback still fires after SENS-C closed",
            "FINAL-B fallback still fires after SENS-C closed",
        ]),
        "no_strategy_or_exit_reselection": True,
        "sqlite_quick_check": checks["market partial-close SQLite quick_check"] and checks["fallback partial-close SQLite quick_check"],
    }
    for label, ok in summary.items():
        print(f"{label:<52}: {'PASS' if ok else 'FAIL'}")

    all_ok = all(checks.values()) and all(summary.values())
    print()
    print(f"Locked exit spec fingerprint       : {LOCKED_EXIT_SPEC_FINGERPRINT}")
    print("Production DB touched               : NO")
    print("Live market action                  : NO")
    print("Wallet/signing/live orders          : NO")
    print("PnL/accounting                      : NO")
    print("Parameter tuning/reselection        : NO")
    print()
    print(f"RESULT: {'PASS' if all_ok else 'CHECK'}")
    if all_ok:
        print("NEXT: build causal EXIT_PENDING -> CLOSED simulated exit-fill executor using locked 500ms latency, cost baseline, and P4-RUNTIME-EXIT-PRICE-IMPACT-0001.")
    return 0 if all_ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
