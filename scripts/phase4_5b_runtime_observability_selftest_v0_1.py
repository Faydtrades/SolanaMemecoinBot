from __future__ import annotations

import shutil
import sqlite3
import sys
import tempfile
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from phase4.paper_cost_baseline_v0_1 import P4_COST_BASELINE_0001
from phase4.paper_cost_model_v0_2 import PaperCostModelV02
from phase4.paper_runtime_observability_v0_1 import (
    LOCKED_TRACKS,
    MODEL_FINGERPRINT,
    MODEL_ID,
    ObservabilityDeterminismConflict,
    PaperRuntimeObservabilityV01,
    PositionMarkObservation,
    StrategyEvaluationAuditInput,
    TerminalDecisionAuditInput,
    TradeOrSkip,
    build_report,
    canonical_report_digest,
)
from phase4.paper_trade_accounting_v0_1 import load_completed_trades

FIXTURE = PROJECT_ROOT / "data" / "selftest" / "phase4_4e_live_exit_pipeline_fixture.sqlite3"
UTC = timezone.utc


def dt(s: str) -> datetime:
    return datetime.fromisoformat(s).astimezone(UTC)


def main() -> int:
    print("=" * 122)
    print("PHASE 4.5B - CAUSAL MTM / MAE-MFE / STRATEGY DECISION PERSISTENCE SELF-TEST v0.1")
    print("=" * 122)
    print(f"Project root                       : {PROJECT_ROOT}")
    print(f"Exact Phase-4.4E fixture           : {FIXTURE}")
    print(f"Observability model                : {MODEL_ID}")
    print(f"Observability fingerprint          : {MODEL_FINGERPRINT}")
    print("Production DB opened               : NO")
    print("Collector / network / RPC          : NO")
    print("Wallet / signing / live orders     : NO")
    print("Original paper fixture mutated     : NO")
    print("Parameter tuning / reselection     : NO")
    print("Cross-track equity summation       : PROHIBITED")
    print()

    if not FIXTURE.exists():
        raise FileNotFoundError(FIXTURE)

    checks: dict[str, bool] = {}

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "obs.sqlite3"
        shutil.copy2(FIXTURE, db)
        conn = sqlite3.connect(db)
        conn.row_factory = sqlite3.Row
        cost_model = PaperCostModelV02(P4_COST_BASELINE_0001)
        store = PaperRuntimeObservabilityV01(conn, cost_model)

        positions = {
            str(r["track_id"]): r
            for r in conn.execute(
                "SELECT * FROM paper_positions ORDER BY track_id"
            ).fetchall()
        }
        trades = {t.track_id: t for t in load_completed_trades(conn)}
        checks["exact_locked_fixture_tracks"] = set(positions) == set(LOCKED_TRACKS)
        checks["exact_accounting_completed_tracks"] = set(trades) == set(LOCKED_TRACKS)

        # Strategy evaluation + terminal decision audit.
        e1 = StrategyEvaluationAuditInput(
            run_id="RUN-TRADE-1", role="CONTROL", mint="MINT-TRADE",
            strategy_version="v1.1", parameter_set_id="P-TRADE", ingest_seq=10,
            evaluated_at_us=1_000_000, current_state="WAITING_FOR_RECLAIM",
            transition_occurred=True, reason_code="BUYER_RESPONSE_PASSED",
            filters_passed=("IMPULSE", "PULLBACK", "BUYER_RESPONSE"),
            filters_failed=(), signal_type=None, candidate_signal_id=None,
            audit_snapshot={"state_seq": 10, "feature": {"x": 1}},
        )
        e2 = StrategyEvaluationAuditInput(
            run_id="RUN-TRADE-1", role="CONTROL", mint="MINT-TRADE",
            strategy_version="v1.1", parameter_set_id="P-TRADE", ingest_seq=11,
            evaluated_at_us=1_100_000, current_state="CANDIDATE_EMITTED",
            transition_occurred=True, reason_code="CANDIDATE_SIGNAL_EMITTED",
            filters_passed=("RECLAIM", "RUNAWAY_ENTRY_GUARD"), filters_failed=(),
            signal_type="FIRST_PULLBACK", candidate_signal_id="SIG-TRADE-1",
            audit_snapshot={"state_seq": 11, "candidate": "SIG-TRADE-1"},
        )
        e3 = StrategyEvaluationAuditInput(
            run_id="RUN-SKIP-1", role="ROBUST_1", mint="MINT-SKIP",
            strategy_version="v1.1", parameter_set_id="P-SKIP", ingest_seq=20,
            evaluated_at_us=2_000_000, current_state="EXPIRED",
            transition_occurred=True, reason_code="ENTRY_WINDOW_EXPIRED",
            filters_passed=("IMPULSE",), filters_failed=("RECLAIM",),
            signal_type=None, candidate_signal_id=None,
            audit_snapshot={"state_seq": 20, "expired": True},
        )
        eid1 = store.record_strategy_evaluation(e1)
        eid2 = store.record_strategy_evaluation(e2)
        eid3 = store.record_strategy_evaluation(e3)
        # Exact replay must be idempotent.
        checks["strategy_eval_exact_replay_idempotent"] = store.record_strategy_evaluation(e2) == eid2
        checks["strategy_eval_rows_exact"] = conn.execute("SELECT COUNT(*) FROM paper_strategy_evaluations").fetchone()[0] == 3

        conflict = False
        try:
            store.record_strategy_evaluation(replace(e2, reason_code="DIFFERENT"))
        except ObservabilityDeterminismConflict:
            conflict = True
        checks["strategy_eval_conflict_guard"] = conflict

        d_trade = TerminalDecisionAuditInput(
            decision_key="TOKEN:MINT-TRADE:CONTROL", decision_scope="ROLE",
            run_id="RUN-TRADE-1", role="CONTROL", mint="MINT-TRADE",
            strategy_version="v1.1", parameter_set_id="P-TRADE",
            decision_at_us=1_200_000, trade_or_skip=TradeOrSkip.TRADE,
            decision_reason="ENTRY_FILLED", candidate_signal_id="SIG-TRADE-1",
            paper_entry_route_id="ROUTE-TRADE-1", source_evaluation_id=eid2,
        )
        d_skip = TerminalDecisionAuditInput(
            decision_key="TOKEN:MINT-SKIP:ROBUST_1", decision_scope="ROLE",
            run_id="RUN-SKIP-1", role="ROBUST_1", mint="MINT-SKIP",
            strategy_version="v1.1", parameter_set_id="P-SKIP",
            decision_at_us=2_100_000, trade_or_skip=TradeOrSkip.SKIP,
            decision_reason="ENTRY_WINDOW_EXPIRED", candidate_signal_id=None,
            paper_entry_route_id=None, source_evaluation_id=eid3,
        )
        did_trade = store.record_terminal_decision(d_trade)
        did_skip = store.record_terminal_decision(d_skip)
        checks["terminal_decision_exact_replay_idempotent"] = store.record_terminal_decision(d_skip) == did_skip
        checks["terminal_decision_rows_exact"] = conn.execute("SELECT COUNT(*) FROM paper_terminal_trade_decisions").fetchone()[0] == 2
        conflict = False
        try:
            store.record_terminal_decision(replace(d_skip, decision_reason="DIFFERENT_SKIP"))
        except ObservabilityDeterminismConflict:
            conflict = True
        checks["terminal_decision_conflict_guard"] = conflict

        # Causal position marks against exact persisted Phase-4.4E positions.
        mark_specs = [
            (101, "2026-08-23T18:00:01.000000+00:00", 9, 100),   # -10%
            (102, "2026-08-23T18:00:03.000000+00:00", 13, 100),  # +30%
            (103, "2026-08-23T18:00:05.000000+00:00", 8, 100),   # -20%
        ]
        per_track_marks: dict[str, list] = {t: [] for t in LOCKED_TRACKS}
        for seq, when, num, den in mark_specs:
            for track in LOCKED_TRACKS:
                pos = positions[track]
                obs = PositionMarkObservation(
                    mint=str(pos["mint"]), observed_at=dt(when), ingest_seq=seq,
                    source_event_key=f"MARK-{seq}", price_identity=str(pos["price_identity"]),
                    price_numerator_raw=num, price_denominator_raw=den,
                    current_virtual_token_reserve_raw=50_000_000_000,
                    is_gap_recovery=False,
                )
                mark = store.record_position_mark(str(pos["paper_position_id"]), obs)
                assert mark is not None
                per_track_marks[track].append(mark)

        checks["three_marks_per_track"] = all(len(v) == 3 for v in per_track_marks.values())
        checks["exact_mae_mfe_price_excursion_inputs"] = all(
            [m.market_return_bps for m in per_track_marks[t]] == [-1000, 3000, -2000]
            for t in LOCKED_TRACKS
        )
        checks["mtm_includes_entry_and_estimated_exit_costs"] = all(
            m.net_mtm_pnl_lamports
            == m.gross_liquidation_proceeds_lamports
            - m.entry_principal_lamports
            - m.entry_explicit_cost_lamports
            - m.estimated_exit_explicit_cost_lamports
            for marks in per_track_marks.values() for m in marks
        )
        checks["exit_impact_bound_into_every_mark"] = all(
            m.exit_price_impact_bps > 0 for marks in per_track_marks.values() for m in marks
        )

        # Gap/cross-mint/pre-entry/out-of-order exclusion.
        sample_pos = positions["FINAL-A"]
        gap = store.record_position_mark(
            str(sample_pos["paper_position_id"]),
            PositionMarkObservation(
                mint=str(sample_pos["mint"]), observed_at=dt("2026-08-23T18:00:05.100000+00:00"),
                ingest_seq=104, source_event_key="GAP", price_identity="SOL_NATIVE",
                price_numerator_raw=1, price_denominator_raw=10,
                current_virtual_token_reserve_raw=50_000_000_000, is_gap_recovery=True,
            ),
        )
        cross = store.record_position_mark(
            str(sample_pos["paper_position_id"]),
            PositionMarkObservation(
                mint="OTHER", observed_at=dt("2026-08-23T18:00:05.200000+00:00"),
                ingest_seq=105, source_event_key="OTHER", price_identity="SOL_NATIVE",
                price_numerator_raw=1, price_denominator_raw=10,
                current_virtual_token_reserve_raw=50_000_000_000,
            ),
        )
        before = store.record_position_mark(
            str(sample_pos["paper_position_id"]),
            PositionMarkObservation(
                mint=str(sample_pos["mint"]), observed_at=dt("2026-08-23T17:59:59.000000+00:00"),
                ingest_seq=106, source_event_key="BEFORE", price_identity="SOL_NATIVE",
                price_numerator_raw=1, price_denominator_raw=10,
                current_virtual_token_reserve_raw=50_000_000_000,
            ),
        )
        out_of_order = store.record_position_mark(
            str(sample_pos["paper_position_id"]),
            PositionMarkObservation(
                mint=str(sample_pos["mint"]), observed_at=dt("2026-08-23T18:00:04.000000+00:00"),
                ingest_seq=99, source_event_key="OLD", price_identity="SOL_NATIVE",
                price_numerator_raw=1, price_denominator_raw=10,
                current_virtual_token_reserve_raw=50_000_000_000,
            ),
        )
        checks["noncausal_marks_excluded"] = gap is None and cross is None and before is None and out_of_order is None
        checks["mark_rows_still_exact_nine"] = conn.execute("SELECT COUNT(*) FROM paper_position_mtm_marks").fetchone()[0] == 9

        # Exact replay / conflict on marks.
        replay_obs = PositionMarkObservation(
            mint=str(sample_pos["mint"]), observed_at=dt("2026-08-23T18:00:05.000000+00:00"),
            ingest_seq=103, source_event_key="MARK-103", price_identity="SOL_NATIVE",
            price_numerator_raw=8, price_denominator_raw=100,
            current_virtual_token_reserve_raw=50_000_000_000,
        )
        replay_mark = store.record_position_mark(str(sample_pos["paper_position_id"]), replay_obs)
        checks["position_mark_exact_replay_idempotent"] = replay_mark is not None and replay_mark.mark_id == per_track_marks["FINAL-A"][-1].mark_id
        conflict = False
        try:
            store.record_position_mark(
                str(sample_pos["paper_position_id"]), replace(replay_obs, price_numerator_raw=81, price_denominator_raw=1000)
            )
        except ObservabilityDeterminismConflict:
            conflict = True
        checks["position_mark_conflict_guard"] = conflict

        # Track equity streams: current open-position MTM at each mark, then exact realized close result.
        for idx, (seq, when, _num, _den) in enumerate(mark_specs):
            for track in LOCKED_TRACKS:
                mark = per_track_marks[track][idx]
                store.record_track_equity_mark(
                    track_id=track, observed_at=dt(when), ingest_seq=seq,
                    source_event_key=f"EQUITY-{seq}", realized_closed_pnl_lamports=0,
                    open_position_net_mtm=((mark.paper_position_id, mark.net_mtm_pnl_lamports),),
                )

        close_seq = {"SENS-C": 201, "FINAL-A": 202, "FINAL-B": 203}
        close_time = {
            "SENS-C": "2026-08-23T18:00:05.500001+00:00",
            "FINAL-A": "2026-08-23T18:00:10.500000+00:00",
            "FINAL-B": "2026-08-23T18:00:13.500000+00:00",
        }
        for track in LOCKED_TRACKS:
            store.record_track_equity_mark(
                track_id=track, observed_at=dt(close_time[track]), ingest_seq=close_seq[track],
                source_event_key=f"CLOSE-{track}", realized_closed_pnl_lamports=trades[track].net_pnl_lamports,
                open_position_net_mtm=(),
            )

        report1 = build_report(conn)
        digest1 = canonical_report_digest(report1)
        report2 = build_report(conn)
        digest2 = canonical_report_digest(report2)
        by_pos = {p.track_id: p for p in report1.positions}
        by_track = {t.track_id: t for t in report1.tracks}
        checks["report_mae_exact"] = all(by_pos[t].mae_bps == -2000 for t in LOCKED_TRACKS)
        checks["report_mfe_exact"] = all(by_pos[t].mfe_bps == 3000 for t in LOCKED_TRACKS)
        checks["mtm_drawdown_available_per_track"] = all(by_track[t].equity_marks == 4 and by_track[t].max_drawdown_lamports > 0 for t in LOCKED_TRACKS)
        checks["final_track_equity_matches_realized_accounting"] = all(
            by_track[t].final_equity_pnl_lamports == trades[t].net_pnl_lamports for t in LOCKED_TRACKS
        )
        checks["strategy_trade_skip_counts_exact"] = (
            report1.strategy.evaluation_rows == 3 and report1.strategy.terminal_decisions == 2
            and report1.strategy.trades == 1 and report1.strategy.skips == 1
            and report1.strategy.skip_reasons == (("ENTRY_WINDOW_EXPIRED", 1),)
        )
        checks["cross_track_aggregation_prohibited"] = report1.cross_track_aggregation_status.startswith("PROHIBITED")
        checks["canonical_report_deterministic"] = digest1 == digest2

        # Track equity exact replay + conflict.
        track_mark = store.record_track_equity_mark(
            track_id="FINAL-A", observed_at=dt(close_time["FINAL-A"]), ingest_seq=202,
            source_event_key="CLOSE-FINAL-A", realized_closed_pnl_lamports=trades["FINAL-A"].net_pnl_lamports,
            open_position_net_mtm=(),
        )
        checks["track_equity_exact_replay_idempotent"] = track_mark.equity_pnl_lamports == trades["FINAL-A"].net_pnl_lamports
        conflict = False
        try:
            store.record_track_equity_mark(
                track_id="FINAL-A", observed_at=dt(close_time["FINAL-A"]), ingest_seq=202,
                source_event_key="CLOSE-FINAL-A", realized_closed_pnl_lamports=trades["FINAL-A"].net_pnl_lamports + 1,
                open_position_net_mtm=(),
            )
        except ObservabilityDeterminismConflict:
            conflict = True
        checks["track_equity_conflict_guard"] = conflict

        quick1 = conn.execute("PRAGMA quick_check").fetchone()[0]
        conn.close()

        # Restart exact same copied DB; report/digest must survive.
        conn2 = sqlite3.connect(db)
        conn2.row_factory = sqlite3.Row
        store2 = PaperRuntimeObservabilityV01(conn2, PaperCostModelV02(P4_COST_BASELINE_0001))
        report3 = build_report(conn2)
        digest3 = canonical_report_digest(report3)
        quick2 = conn2.execute("PRAGMA quick_check").fetchone()[0]
        conn2.close()
        checks["restart_report_digest_stable"] = digest3 == digest1
        checks["restart_strategy_audit_persisted"] = report3.strategy == report1.strategy
        checks["sqlite_quick_check_before_after_restart"] = str(quick1).lower() == "ok" and str(quick2).lower() == "ok"

    print("-" * 122)
    print("VALIDATION")
    print("-" * 122)
    for name, ok in checks.items():
        print(f"{name:<66}: {'PASS' if ok else 'FAIL'}")

    all_ok = all(checks.values())
    print()
    print(f"Observability model ID           : {MODEL_ID}")
    print(f"Observability fingerprint        : {MODEL_FINGERPRINT}")
    print(f"Canonical report digest          : {digest1}")
    print("MTM definition                   : net immediate liquidation at causal mark incl. entry cost + current curve impact + est. exit cost")
    print("MAE / MFE basis                  : causal market-price return vs simulated entry execution price")
    print("MTM max drawdown                 : AVAILABLE from persisted per-track equity stream")
    print("Strategy trade/skip audit        : AVAILABLE from persisted evaluations + explicit terminal decisions")
    print("Cross-track portfolio summation  : PROHIBITED")
    print("Production DB touched            : NO")
    print("Original fixture mutated         : NO")
    print("Wallet/signing/live orders       : NO")
    print("Parameter tuning/reselection     : NO")
    print()
    print(f"RESULT: {'PASS' if all_ok else 'CHECK'}")
    if all_ok:
        print("NEXT: run exact-schema fixture integration smoke, then bind this persistence to the controlled live runtime before continuous paper trading.")
    return 0 if all_ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
