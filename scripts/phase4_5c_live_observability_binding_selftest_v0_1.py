from __future__ import annotations

import shutil
import sqlite3
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from phase4.paper_cost_baseline_v0_1 import P4_COST_BASELINE_0001
from phase4.paper_cost_model_v0_2 import PaperCostModelV02
from phase4.paper_live_observability_binding_v0_1 import (
    LOCKED_TRACKS,
    MODEL_ID,
    PaperLiveObservabilityBindingV01,
)
from phase4.paper_runtime_observability_v0_1 import (
    MODEL_FINGERPRINT,
    PositionMarkObservation,
    build_report,
    canonical_report_digest,
)
from phase4.paper_trade_accounting_v0_1 import load_completed_trades

FIXTURE = PROJECT_ROOT / "data" / "selftest" / "phase4_4e_live_exit_pipeline_fixture.sqlite3"
UTC = timezone.utc


def dt(s: str) -> datetime:
    return datetime.fromisoformat(s).astimezone(UTC)


def main() -> int:
    print("=" * 120)
    print("PHASE 4.5C - LIVE OBSERVABILITY RUNTIME BINDING SELF-TEST v0.1")
    print("=" * 120)
    print(f"Project root                       : {PROJECT_ROOT}")
    print(f"Exact Phase-4.4E fixture           : {FIXTURE}")
    print(f"Binding model                      : {MODEL_ID}")
    print(f"Observability fingerprint          : {MODEL_FINGERPRINT}")
    print("Production DB opened               : NO")
    print("Collector / network / RPC          : NO")
    print("Wallet / signing / live orders     : NO")
    print("Original fixture mutated           : NO")
    print("Parameter tuning / reselection     : NO")
    print()

    checks = {}

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "live_binding.sqlite3"
        shutil.copy2(FIXTURE, db)
        conn = sqlite3.connect(db)
        conn.row_factory = sqlite3.Row
        binding = PaperLiveObservabilityBindingV01(
            conn,
            PaperCostModelV02(P4_COST_BASELINE_0001),
        )

        # ------------------------------------------------------------------
        # Exact Phase-2-shaped runtime facts: evaluation -> terminal SKIP.
        # ------------------------------------------------------------------
        params_skip = SimpleNamespace(
            strategy_version="v1.1",
            parameter_set_id="P-SKIP",
        )
        run_skip = SimpleNamespace(
            run_id="RUN-SKIP",
            mint="MINT-SKIP",
            finished=True,
        )
        eval_skip = SimpleNamespace(
            evaluated_at_us=100,
            current_state="EXPIRED",
            transition_occurred=True,
            reason_code="ENTRY_WINDOW_EXPIRED",
            filters_passed=(),
            filters_failed=("RECLAIM",),
            candidate_signal=None,
            audit_snapshot={"source": "selftest-skip"},
        )
        es = binding.record_strategy_evaluation(
            run=run_skip,
            role="ROBUST_2",
            params=params_skip,
            evaluation=eval_skip,
            ingest_seq=100,
        )
        ds = binding.maybe_record_terminal_skip(
            run=run_skip,
            role="ROBUST_2",
            params=params_skip,
            evaluation=eval_skip,
            source_evaluation_id=es,
        )
        ds2 = binding.maybe_record_terminal_skip(
            run=run_skip,
            role="ROBUST_2",
            params=params_skip,
            evaluation=eval_skip,
            source_evaluation_id=es,
        )

        # ------------------------------------------------------------------
        # Candidate evaluation -> accepted live paper TRADE decision.
        # ------------------------------------------------------------------
        params_trade = SimpleNamespace(
            strategy_version="v1.1",
            parameter_set_id="P-TRADE",
        )
        run_trade = SimpleNamespace(
            run_id="RUN-TRADE",
            mint="MINT-TRADE",
            finished=True,
        )
        candidate = SimpleNamespace(
            signal_id="SIG-TRADE",
            signal_type="FIRST_PULLBACK",
        )
        eval_trade = SimpleNamespace(
            evaluated_at_us=200,
            current_state="CANDIDATE_EMITTED",
            transition_occurred=True,
            reason_code="CANDIDATE_SIGNAL_EMITTED",
            filters_passed=("ALL_LOCKED_FILTERS",),
            filters_failed=(),
            candidate_signal=candidate,
            audit_snapshot={"source": "selftest-trade"},
        )
        et = binding.record_strategy_evaluation(
            run=run_trade,
            role="CONTROL",
            params=params_trade,
            evaluation=eval_trade,
            ingest_seq=200,
        )
        dt1 = binding.record_trade_fill(
            run=run_trade,
            role="CONTROL",
            params=params_trade,
            candidate=candidate,
            route_id="ROUTE-TRADE",
            decision_at_us=250,
            source_evaluation_id=et,
        )
        dt2 = binding.record_trade_fill(
            run=run_trade,
            role="CONTROL",
            params=params_trade,
            candidate=candidate,
            route_id="ROUTE-TRADE",
            decision_at_us=250,
            source_evaluation_id=et,
        )

        # ------------------------------------------------------------------
        # Simulate live state before the first sibling closure so the exact
        # fixture positions can receive one causal market mark each.
        # ------------------------------------------------------------------
        original_position_state = {
            str(r["paper_position_id"]): (str(r["state"]), r["closed_at"])
            for r in conn.execute(
                "SELECT paper_position_id,state,closed_at FROM paper_positions"
            ).fetchall()
        }
        conn.execute("UPDATE paper_positions SET state='OPEN', state_reason='SELFTEST_OPEN', closed_at=NULL")
        conn.commit()

        mark_obs = PositionMarkObservation(
            mint="MINT-4E-FIXTURE",
            observed_at=dt("2026-08-23T18:00:02+00:00"),
            ingest_seq=501,
            source_event_key="LIVE-MARK-501",
            price_identity="SOL_NATIVE",
            price_numerator_raw=95,
            price_denominator_raw=1000,
            current_virtual_token_reserve_raw=60_000_000_000,
            is_gap_recovery=False,
        )
        marks = binding.record_position_marks_for_market(mark_obs)
        eq_open = binding.record_track_equity_for_market(
            observed_at=mark_obs.observed_at,
            ingest_seq=mark_obs.ingest_seq,
            source_event_key="LIVE-EQ-501",
        )

        # Replay is idempotent.
        marks_replay = binding.record_position_marks_for_market(mark_obs)
        eq_open_replay = binding.record_track_equity_for_market(
            observed_at=mark_obs.observed_at,
            ingest_seq=mark_obs.ingest_seq,
            source_event_key="LIVE-EQ-501",
        )

        # Restore exact fixture CLOSED lifecycle, then persist final realized
        # per-track equity at a later synthetic runtime heartbeat.
        for pid, (state, closed_at) in original_position_state.items():
            conn.execute(
                "UPDATE paper_positions SET state=?, state_reason='SELFTEST_RESTORED', closed_at=? WHERE paper_position_id=?",
                (state, closed_at, pid),
            )
        conn.commit()

        eq_closed = binding.record_track_equity_for_market(
            observed_at=dt("2026-08-23T18:00:20+00:00"),
            ingest_seq=900,
            source_event_key="LIVE-EQ-FINAL",
        )

        report = build_report(conn)
        digest1 = canonical_report_digest(report)
        digest2 = canonical_report_digest(build_report(conn))
        completed = {t.track_id: t for t in load_completed_trades(conn)}
        report_tracks = {t.track_id: t for t in report.tracks}
        quick = conn.execute("PRAGMA quick_check").fetchone()[0]

        strategy_rows = conn.execute("SELECT COUNT(*) FROM paper_strategy_evaluations").fetchone()[0]
        decision_rows = conn.execute("SELECT COUNT(*) FROM paper_terminal_trade_decisions").fetchone()[0]
        mark_rows = conn.execute("SELECT COUNT(*) FROM paper_position_mtm_marks").fetchone()[0]
        equity_rows = conn.execute("SELECT COUNT(*) FROM paper_track_equity_marks").fetchone()[0]

        checks["binding_model_id_exact"] = MODEL_ID == "P4-LIVE-OBSERVABILITY-BINDING-0001"
        checks["strategy_evaluation_runtime_mapping"] = strategy_rows == 2
        checks["terminal_skip_persisted_idempotent"] = ds == ds2 and ds is not None
        checks["terminal_trade_persisted_idempotent"] = dt1 == dt2
        checks["one_live_mark_per_locked_track"] = set(marks) == set(LOCKED_TRACKS) and mark_rows == 3
        checks["mark_replay_idempotent"] = set(marks_replay) == set(LOCKED_TRACKS) and mark_rows == 3
        checks["open_track_equity_exact"] = set(eq_open) == set(LOCKED_TRACKS)
        checks["open_equity_replay_idempotent"] = set(eq_open_replay) == set(LOCKED_TRACKS)
        checks["closed_realized_equity_exact"] = set(eq_closed) == set(LOCKED_TRACKS)
        checks["strategy_trade_skip_report"] = report.strategy.trades == 1 and report.strategy.skips == 1
        checks["position_excursion_available"] = len(report.positions) == 3 and all(p.marks == 1 for p in report.positions)
        checks["mtm_drawdown_available"] = len(report.tracks) == 3 and all(t.equity_marks >= 2 for t in report.tracks)
        checks["final_equity_matches_realized_accounting"] = all(
            report_tracks[t].final_equity_pnl_lamports == completed[t].net_pnl_lamports
            for t in LOCKED_TRACKS
        )
        checks["canonical_report_repeat_equal"] = digest1 == digest2
        checks["terminal_decision_rows_exact"] = decision_rows == 2
        checks["equity_rows_expected"] = equity_rows == 6
        checks["sqlite_quick_check"] = str(quick).lower() == "ok"
        conn.close()

    print("-" * 120)
    print("VALIDATION")
    print("-" * 120)
    for key, ok in checks.items():
        print(f"{key:<58}: {'PASS' if ok else 'FAIL'}")

    all_ok = all(checks.values())
    print()
    print(f"Canonical report digest          : {digest1}")
    print("Live evaluation -> audit mapping : YES")
    print("Terminal TRADE / SKIP mapping    : YES")
    print("Live position marks              : YES")
    print("Per-track MTM equity             : YES")
    print("Cross-track portfolio sum        : NO")
    print("Production DB touched            : NO")
    print("Original fixture mutated         : NO")
    print()
    print(f"RESULT: {'PASS' if all_ok else 'CHECK'}")
    if all_ok:
        print("NEXT: run controlled live 4.5C observability smoke; user-project/live validation required.")
    return 0 if all_ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
