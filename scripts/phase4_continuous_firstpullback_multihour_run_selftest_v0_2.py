from __future__ import annotations

import sqlite3
import shutil
import sys
import tempfile
from datetime import timedelta
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
for root in (PROJECT_ROOT / "src", PROJECT_ROOT / "scripts"):
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))

import phase4_continuous_firstpullback_multihour_run_v0_2 as harness  # noqa: E402
import phase4_continuous_paper_runner_selftest_v0_1 as runner_fixtures  # noqa: E402
from phase4.paper_continuous_firstpullback_binding_v0_2 import (  # noqa: E402
    ContinuousFirstPullbackBindingV02,
)
from phase4.paper_continuous_market_source_v0_1 import (  # noqa: E402
    ContinuousMarketSourceV01,
)
from phase4.paper_continuous_runner_v0_2 import (  # noqa: E402
    ContinuousPaperRunnerV02,
)
from phase4.paper_entry_router_v0_1 import connect_entry_router_db  # noqa: E402
from phase4_continuous_firstpullback_binding_selftest_v0_1 import (  # noqa: E402
    BASE,
    SCHEMA_SQL,
    insert_row,
)


EXPECTED_FINGERPRINT = (
    "9002ed30ecd74722af5a37efec437143a17c4865f94ae37a60863fbc2320a7d7"
)


def check(name: str, condition: bool) -> None:
    if not condition:
        raise AssertionError(name)
    print(f"{name}: PASS")


def make_source(path: Path, *, with_exact_deadline_row: bool) -> ContinuousMarketSourceV01:
    conn = sqlite3.connect(path)
    try:
        conn.execute(SCHEMA_SQL)
        if with_exact_deadline_row:
            insert_row(
                conn,
                1,
                "MINT-FENCE",
                "LAUNCH",
                observed_offset=15,
                price_units=10_000,
            )
        conn.commit()
    finally:
        conn.close()
    return ContinuousMarketSourceV01(
        path,
        start_after_p1_rowid=0,
        database_identity=f"SELFTEST:T012:{path.name}",
    )


def main() -> int:
    harness._validate_locked_contracts_v02()
    check(
        "MODEL_IDENTITY",
        harness.MODEL_ID == "P4-CONTINUOUS-FIRSTPULLBACK-MULTIHOUR-RUN-0002"
        and harness.MODEL_FINGERPRINT == EXPECTED_FINGERPRINT
        and harness.MULTIHOUR_SPEC["entry_execution_deadline_fingerprint"]
        == harness.DEADLINE_POLICY_FINGERPRINT,
    )
    check(
        "DURATION_POLICY_UNCHANGED",
        harness.parse_duration_seconds(14_400) == 14_400
        and harness.run_control_reason(14_399.999, 14_400) is None
        and harness.run_control_reason(14_400, 14_400) == "DURATION_REACHED",
    )

    # Frozen end boundaries expire only deadlines that are actually due.
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    runner = ContinuousPaperRunnerV02(
        conn, source_identity="BOUNDARY", wall_clock=lambda: BASE
    )
    runner.process_source_item(runner_fixtures.candidate_item(
        0, signal="BOUNDARY", mint="MINT-BOUNDARY", role="CONTROL", at=BASE, ingest_seq=1
    ))
    runner.process_source_item(runner_fixtures.clock_item(
        1, at=BASE + timedelta(seconds=4)
    ))
    check(
        "NO_FAST_FORWARD_BEFORE_DEADLINE",
        runner.router.diagnostics()["pending_deadlines_at_end"] == 3,
    )
    runner.process_source_item(runner_fixtures.clock_item(
        2, at=BASE + timedelta(seconds=5, microseconds=1)
    ))
    diagnostics = runner.router.diagnostics()
    check(
        "FROZEN_BOUNDARY_EXPIRES_ONLY_DUE_TRACK",
        diagnostics["expiry_events"] == 1
        and diagnostics["pending_deadlines_at_end"] == 2,
    )
    conn.close()

    # Prepared timer fence: the exact +15s source row must drain before the
    # exact +15s timer, leaving A/B filled rather than falsely expired.
    with tempfile.TemporaryDirectory(prefix="p4-t012-fence-") as raw:
        root = Path(raw)
        source = make_source(root / "source.sqlite3", with_exact_deadline_row=True)
        conn = connect_entry_router_db(root / "paper.sqlite3")
        binding = ContinuousFirstPullbackBindingV02(conn, source, wall_clock=lambda: BASE)
        binding.runner.process_source_item(runner_fixtures.candidate_item(
            0, signal="FENCE", mint="MINT-FENCE", role="CONTROL", at=BASE, ingest_seq=0
        ))
        # Reserve the next durable runner cursor for the binding outbox.
        with conn:
            conn.execute(
                "UPDATE paper_fp_binding_runtime_v0_1 SET next_event_sequence=1 WHERE singleton=1"
            )
        timer = binding.prepare_clock_tick(
            BASE + timedelta(seconds=15), timer_id="T012:EXACT-15S"
        )
        pending = binding.execute_prepared_timer(timer)
        check(
            "PREPARED_TIMER_WAITS_FOR_WATERMARK",
            pending.status == "PENDING" and pending.source_watermark_p1_rowid == 1,
        )
        batch = binding.process_next_batch(batch_size=10)
        committed = binding.execute_prepared_timer(timer)
        order_states = {
            str(row["track_id"]): str(row["state"])
            for row in conn.execute("SELECT track_id,state FROM paper_orders").fetchall()
        }
        check(
            "SOURCE_FIRST_EXACT_BOUNDARY_ORDERING",
            batch.durable_p1_rowid == 1
            and committed.status == "COMMITTED"
            and order_states == {
                "FINAL-A": "FILLED", "FINAL-B": "FILLED", "SENS-C": "REJECTED"
            },
        )
        check(
            "TIMER_AND_OUTBOX_FINALIZED",
            binding.pending_outbox_count() == 0
            and binding.active_timer_fence_p1_rowid is None,
        )
        conn.close()

    # Restart a mixed track set through the full V02 binding (the accepted V01
    # bridge could not reconstruct this state).
    with tempfile.TemporaryDirectory(prefix="p4-t012-binding-restart-") as raw:
        root = Path(raw)
        source = make_source(root / "source.sqlite3", with_exact_deadline_row=False)
        paper = root / "paper.sqlite3"
        conn = connect_entry_router_db(paper)
        binding = ContinuousFirstPullbackBindingV02(conn, source, wall_clock=lambda: BASE)
        binding.runner.process_source_item(runner_fixtures.candidate_item(
            0, signal="MIXED", mint="MINT-MIXED", role="CONTROL", at=BASE, ingest_seq=1
        ))
        binding.runner.process_source_item(runner_fixtures.clock_item(
            1, at=BASE + timedelta(seconds=5, microseconds=1)
        ))
        binding.runner.process_source_item(runner_fixtures.market_item(
            2, mint="MINT-MIXED", at=BASE + timedelta(seconds=10), ingest_seq=2,
            return_bps=0,
        ))
        before = binding.canonical_digest()
        diag_before = harness.entry_deadline_diagnostics(conn)
        conn.close()
        conn = connect_entry_router_db(paper)
        reopened = ContinuousFirstPullbackBindingV02(conn, source, wall_clock=lambda: BASE)
        check(
            "MIXED_TRACK_BINDING_RESTART",
            reopened.canonical_digest() == before
            and harness.entry_deadline_diagnostics(conn) == diag_before,
        )
        check(
            "MULTIHOUR_DIAGNOSTICS",
            diag_before["partial_track_candidates"] == 1
            and diag_before["filled_tracks"] == {"FINAL-A": 1, "FINAL-B": 1}
            and diag_before["execution_expired_by_track"] == {"SENS-C": 1}
            and diag_before["pending_deadlines_at_end"] == 0,
        )
        conn.close()
        integrity = harness._integrity_audit_v02(
            paper, source=source, final_cursor=0
        )
        before_restart_sha256 = harness.accepted.file_sha256(paper)
        restart_ok, restart_digest = harness.restart_probe(paper, source)
        check(
            "V02_POSTRUN_INTEGRITY",
            bool(integrity["integrity_ok"]),
        )
        corrupt_paper = root / "missing-deadline-ledger.sqlite3"
        shutil.copy2(paper, corrupt_paper)
        corrupt_conn = sqlite3.connect(corrupt_paper)
        with corrupt_conn:
            corrupt_conn.execute(
                "DELETE FROM paper_entry_execution_deadlines_v0_1"
            )
        corrupt_conn.close()
        corrupt_integrity = harness._integrity_audit_v02(
            corrupt_paper, source=source, final_cursor=0
        )
        check(
            "V02_INTEGRITY_REJECTS_MISSING_DEADLINE_LEDGER",
            not bool(corrupt_integrity["entry_deadline_route_cardinality"])
            and not bool(corrupt_integrity["integrity_ok"]),
        )
        check(
            "V02_EXACT_STATE_RESTART_PROBE",
            restart_ok
            and restart_digest == before
            and harness.accepted.file_sha256(paper) == before_restart_sha256,
        )

        original_binding = harness.ContinuousFirstPullbackBindingV02

        class SameSizeMutatingBinding:
            def __init__(self, conn, market_source):
                conn.execute("PRAGMA user_version=1")
                conn.commit()
                self.durable_p1_rowid = market_source.start_after_p1_rowid

            def canonical_digest(self):
                return "MUTATED-SAME-SIZE"

        harness.ContinuousFirstPullbackBindingV02 = SameSizeMutatingBinding
        try:
            mutation_detected, mutation_digest = harness.restart_probe(paper, source)
        finally:
            harness.ContinuousFirstPullbackBindingV02 = original_binding
        check(
            "V02_RESTART_PROBE_REJECTS_SAME_SIZE_MUTATION",
            not mutation_detected and mutation_digest is None,
        )

    # Integrated production-regression ordering: +36s cannot open any track.
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    runner = ContinuousPaperRunnerV02(
        conn, source_identity="LATE36", wall_clock=lambda: BASE
    )
    runner.process_source_item(runner_fixtures.candidate_item(
        0, signal="LATE36", mint="MINT-LATE36", role="CONTROL", at=BASE, ingest_seq=1
    ))
    runner.process_source_item(runner_fixtures.market_item(
        1, mint="MINT-LATE36", at=BASE + timedelta(seconds=36, microseconds=512_000),
        ingest_seq=2, return_bps=0,
    ))
    route = conn.execute("SELECT state,state_reason FROM paper_entry_routes").fetchone()
    runner.process_source_item(runner_fixtures.clock_item(
        2, at=BASE + timedelta(seconds=40)
    ))
    check(
        "INTEGRATED_36S_REGRESSION",
        tuple(route) == ("REJECTED", "ALL_TRACK_ENTRY_EXECUTION_DEADLINES_EXPIRED")
        and int(conn.execute("SELECT COUNT(*) FROM paper_positions").fetchone()[0]) == 0,
    )
    check(
        "INTEGRATED_36S_RUNNER_CONTINUES",
        runner.last_committed_source_cursor == 2,
    )
    decision = conn.execute(
        "SELECT trade_or_skip,decision_reason FROM paper_terminal_trade_decisions"
    ).fetchone()
    check(
        "INTEGRATED_36S_TERMINAL_AUDIT",
        decision is not None
        and tuple(decision) == (
            "SKIP", "ALL_TRACK_ENTRY_EXECUTION_DEADLINES_EXPIRED"
        ),
    )
    check(
        "AUDIT_PROVENANCE_COMPLETE",
        int(conn.execute(
            "SELECT COUNT(*) FROM paper_entry_execution_deadlines_v0_1 "
            "WHERE reason='ENTRY_EXECUTION_DEADLINE_EXPIRED' AND terminal_at=expire_at"
        ).fetchone()[0]) == 3,
    )
    conn.close()

    print("RESULT: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
