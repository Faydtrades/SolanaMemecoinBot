from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.phase4.paper_lifecycle_v0_1 import (  # noqa: E402
    DeterminismConflict,
    InvalidTransition,
    LOCKED_PHASE4_TRACKS,
    OrderState,
    PaperCandidateRef,
    PaperLifecycleStore,
    PositionState,
    canonical_db_digest,
    connect_paper_db,
    run_integrity_checks,
)

SELFTEST_DIR = PROJECT_ROOT / "data" / "selftest"
DEFAULT_DB_A = SELFTEST_DIR / "phase4_paper_lifecycle_selftest_a.sqlite3"
DEFAULT_DB_B = SELFTEST_DIR / "phase4_paper_lifecycle_selftest_b.sqlite3"
PRODUCTION_DB = (PROJECT_ROOT / "data" / "db" / "tradingbot.sqlite3").resolve()
REFERENCE_SIZE_LAMPORTS = 100_000_000  # 0.10 SOL research/reference size


def clean_sqlite(path: Path) -> None:
    for candidate in (path, Path(str(path) + "-wal"), Path(str(path) + "-shm")):
        if candidate.exists():
            candidate.unlink()


def guard_isolated(path: Path) -> None:
    resolved = path.resolve()
    if resolved == PRODUCTION_DB:
        raise RuntimeError("self-test refuses to use production DB")
    if SELFTEST_DIR.resolve() not in resolved.parents:
        raise RuntimeError(f"self-test DB must live under {SELFTEST_DIR}")


def fixture_candidate() -> PaperCandidateRef:
    return PaperCandidateRef(
        signal_key="SELFTEST-FIRSTPULLBACK-v1.1-MINT_A-1724414400123456-77",
        mint="MINT_A_SELFTEST",
        strategy_version="FirstPullback-v1.1",
        parameter_set_id="SELFTEST-PARAM-LOCKED",
        signal_observed_at=datetime(2026, 8, 23, 12, 0, 0, 123456, tzinfo=timezone.utc),
        signal_ingest_seq=77,
        reference_price_identity="SOL_NATIVE",
        reference_price_numerator_raw=30_000_000_000,
        reference_price_denominator_raw=1_000_000_000_000,
    )


def run_scenario(db_path: Path, *, exercise_restart: bool) -> tuple[str, dict[str, str]]:
    guard_isolated(db_path)
    clean_sqlite(db_path)
    candidate = fixture_candidate()
    t0 = candidate.signal_observed_at

    conn = connect_paper_db(db_path)
    store = PaperLifecycleStore(conn)

    # Same CandidateSignal reference is branched into the two mainline paper
    # tracks and the separately labelled sensitivity track.
    order_a = store.create_order(
        candidate, track_id="FINAL-A", requested_size_lamports=REFERENCE_SIZE_LAMPORTS
    )
    order_b = store.create_order(
        candidate, track_id="FINAL-B", requested_size_lamports=REFERENCE_SIZE_LAMPORTS
    )
    order_c = store.create_order(
        candidate, track_id="SENS-C", requested_size_lamports=REFERENCE_SIZE_LAMPORTS
    )

    # Creation replay must return exactly the same identities without new rows.
    assert store.create_order(
        candidate, track_id="FINAL-A", requested_size_lamports=REFERENCE_SIZE_LAMPORTS
    ).paper_order_id == order_a.paper_order_id
    assert conn.execute("SELECT COUNT(*) FROM paper_orders").fetchone()[0] == 3

    # FINAL-A: full lifecycle to CLOSED.
    store.mark_entry_pending(order_a.paper_order_id, effective_at=t0 + timedelta(milliseconds=100))
    filled_a, position_a = store.fill_order(
        order_a.paper_order_id,
        fill_at=t0 + timedelta(milliseconds=250),
        filled_size_lamports=REFERENCE_SIZE_LAMPORTS,
        simulated_entry_price_numerator_raw=candidate.reference_price_numerator_raw,
        simulated_entry_price_denominator_raw=candidate.reference_price_denominator_raw,
        reason="SELFTEST_FIXTURE_FILL",
    )
    assert filled_a.state == OrderState.FILLED
    assert position_a.state == PositionState.OPEN
    store.request_exit(
        position_a.paper_position_id,
        effective_at=t0 + timedelta(seconds=15),
        reason="SELFTEST_EXIT_REQUEST",
    )
    closed_a = store.close_position(
        position_a.paper_position_id,
        closed_at=t0 + timedelta(seconds=15, milliseconds=100),
        exit_price_numerator_raw=33_000_000_000,
        exit_price_denominator_raw=1_000_000_000_000,
        reason="SELFTEST_FIXTURE_CLOSE",
    )
    assert closed_a.state == PositionState.CLOSED

    # A terminal order must reject an impossible transition.
    invalid_transition_ok = False
    try:
        store.cancel_order(
            order_a.paper_order_id,
            effective_at=t0 + timedelta(seconds=16),
            reason="SHOULD_FAIL",
        )
    except InvalidTransition:
        invalid_transition_ok = True
    assert invalid_transition_ok

    # FINAL-B: leave an OPEN position across an actual DB close/reopen.
    store.mark_entry_pending(order_b.paper_order_id, effective_at=t0 + timedelta(milliseconds=110))
    filled_b, position_b = store.fill_order(
        order_b.paper_order_id,
        fill_at=t0 + timedelta(milliseconds=260),
        filled_size_lamports=REFERENCE_SIZE_LAMPORTS,
        simulated_entry_price_numerator_raw=candidate.reference_price_numerator_raw,
        simulated_entry_price_denominator_raw=candidate.reference_price_denominator_raw,
        reason="SELFTEST_FIXTURE_FILL",
    )
    assert filled_b.state == OrderState.FILLED
    assert position_b.state == PositionState.OPEN

    # SENS-C is deliberately rejected to prove terminal non-trade persistence.
    rejected_c = store.reject_order(
        order_c.paper_order_id,
        effective_at=t0 + timedelta(milliseconds=120),
        reason="SELFTEST_REJECTION",
    )
    assert rejected_c.state == OrderState.REJECTED

    if exercise_restart:
        conn.close()
        conn = connect_paper_db(db_path)
        store = PaperLifecycleStore(conn)
        open_positions = store.list_open_positions()
        assert len(open_positions) == 1
        assert open_positions[0].paper_position_id == position_b.paper_position_id
        assert open_positions[0].track_id == "FINAL-B"

        # Replaying the already-committed fill after restart must be idempotent.
        replay_order_b, replay_position_b = store.fill_order(
            order_b.paper_order_id,
            fill_at=t0 + timedelta(milliseconds=260),
            filled_size_lamports=REFERENCE_SIZE_LAMPORTS,
            simulated_entry_price_numerator_raw=candidate.reference_price_numerator_raw,
            simulated_entry_price_denominator_raw=candidate.reference_price_denominator_raw,
            reason="SELFTEST_FIXTURE_FILL",
        )
        assert replay_order_b.paper_order_id == order_b.paper_order_id
        assert replay_position_b.paper_position_id == position_b.paper_position_id
        assert conn.execute("SELECT COUNT(*) FROM paper_positions").fetchone()[0] == 2

    # Complete FINAL-B after restart (or in the uninterrupted twin run).
    store.request_exit(
        position_b.paper_position_id,
        effective_at=t0 + timedelta(seconds=14),
        reason="SELFTEST_EXIT_REQUEST_B",
    )
    store.close_position(
        position_b.paper_position_id,
        closed_at=t0 + timedelta(seconds=14, milliseconds=100),
        exit_price_numerator_raw=32_500_000_000,
        exit_price_denominator_raw=1_000_000_000_000,
        reason="SELFTEST_FIXTURE_CLOSE_B",
    )

    # Exact close replay is idempotent; conflicting replay must be detected.
    store.close_position(
        position_b.paper_position_id,
        closed_at=t0 + timedelta(seconds=14, milliseconds=100),
        exit_price_numerator_raw=32_500_000_000,
        exit_price_denominator_raw=1_000_000_000_000,
        reason="SELFTEST_FIXTURE_CLOSE_B",
    )
    conflict_ok = False
    try:
        store.close_position(
            position_b.paper_position_id,
            closed_at=t0 + timedelta(seconds=14, milliseconds=100),
            exit_price_numerator_raw=32_600_000_000,
            exit_price_denominator_raw=1_000_000_000_000,
            reason="SELFTEST_FIXTURE_CLOSE_B",
        )
    except DeterminismConflict:
        conflict_ok = True
    assert conflict_ok

    integrity_ok, issues = run_integrity_checks(conn)
    assert integrity_ok, issues
    assert store.list_open_positions() == []

    digest = canonical_db_digest(conn)
    ids = {
        "FINAL-A": order_a.paper_order_id,
        "FINAL-B": order_b.paper_order_id,
        "SENS-C": order_c.paper_order_id,
        "POSITION-A": position_a.paper_position_id,
        "POSITION-B": position_b.paper_position_id,
    }
    conn.close()
    return digest, ids


def main(db_a: Path, db_b: Path) -> int:
    print("=" * 94)
    print("PHASE 4.1 - DETERMINISTIC PAPER ORDER / POSITION LIFECYCLE SELF-TEST v0.1")
    print("=" * 94)
    print(f"Project root          : {PROJECT_ROOT}")
    print(f"Self-test DB A        : {db_a}")
    print(f"Self-test DB B        : {db_b}")
    print(f"Production DB touched : NO")
    print("Network / RPC         : NO")
    print("Wallet / live orders  : NO")
    print(f"Tracks                : {LOCKED_PHASE4_TRACKS}")
    print()

    digest_a, ids_a = run_scenario(db_a, exercise_restart=True)
    digest_b, ids_b = run_scenario(db_b, exercise_restart=False)

    deterministic = digest_a == digest_b and ids_a == ids_b
    print("-" * 94)
    print("VALIDATION")
    print("-" * 94)
    print("Order/position lifecycle         : PASS")
    print("Three locked Phase-4 tracks      : PASS")
    print("Duplicate signal/order replay    : PASS")
    print("Invalid transition rejection     : PASS")
    print("Restart-safe OPEN recovery       : PASS")
    print("Idempotent post-restart fill     : PASS")
    print("Rejected non-trade persistence   : PASS")
    print("Conflicting replay detection     : PASS")
    print("SQLite quick_check / foreign keys: PASS")
    print(f"Logical digest A                 : {digest_a}")
    print(f"Logical digest B                 : {digest_b}")
    print(f"Cross-run deterministic equality : {'PASS' if deterministic else 'FAIL'}")
    print()
    print("NOTE: fixture entry/exit prices are self-test data only; they are NOT a Phase-4 cost/fill model.")
    print()
    print("RESULT: PASS" if deterministic else "RESULT: FAIL")
    return 0 if deterministic else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-a", type=Path, default=DEFAULT_DB_A)
    parser.add_argument("--db-b", type=Path, default=DEFAULT_DB_B)
    args = parser.parse_args()
    raise SystemExit(main(args.db_a, args.db_b))
