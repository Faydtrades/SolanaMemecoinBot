from __future__ import annotations

import shutil
import sqlite3
import sys
import tempfile
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
    print("=" * 120)
    print("PHASE 4.5B - EXACT PHASE-4.4E SCHEMA OBSERVABILITY INTEGRATION SMOKE v0.1")
    print("=" * 120)
    print(f"Project root                    : {PROJECT_ROOT}")
    print(f"Exact 4.4E fixture              : {FIXTURE}")
    print("Fixture mode                    : BYTE-COPY -> ISOLATED READ-WRITE")
    print("Production DB / collector       : NO")
    print("Wallet / live orders            : NO")
    print("Parameter tuning/reselection    : NO")
    print(f"Observability fingerprint       : {MODEL_FINGERPRINT}")
    print()

    checks = {}
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "fixture_obs.sqlite3"
        shutil.copy2(FIXTURE, db)
        conn = sqlite3.connect(db)
        conn.row_factory = sqlite3.Row
        store = PaperRuntimeObservabilityV01(conn, PaperCostModelV02(P4_COST_BASELINE_0001))
        positions = {
            str(r["track_id"]): r
            for r in conn.execute("SELECT * FROM paper_positions ORDER BY track_id").fetchall()
        }
        trades = {t.track_id: t for t in load_completed_trades(conn)}

        # One exact persisted trade decision plus one explicit skip decision.
        eval_trade = StrategyEvaluationAuditInput(
            run_id="FIX-RUN-T", role="CONTROL", mint="FIX-MINT-T",
            strategy_version="v1.1", parameter_set_id="FIX-P-T", ingest_seq=1,
            evaluated_at_us=1, current_state="CANDIDATE_EMITTED",
            transition_occurred=True, reason_code="CANDIDATE_SIGNAL_EMITTED",
            filters_passed=("ALL_LOCKED_FILTERS",), filters_failed=(),
            signal_type="FIRST_PULLBACK", candidate_signal_id="FIX-SIG-T",
            audit_snapshot={"fixture": "trade"},
        )
        eval_skip = StrategyEvaluationAuditInput(
            run_id="FIX-RUN-S", role="ROBUST_2", mint="FIX-MINT-S",
            strategy_version="v1.1", parameter_set_id="FIX-P-S", ingest_seq=2,
            evaluated_at_us=2, current_state="EXPIRED", transition_occurred=True,
            reason_code="ENTRY_WINDOW_EXPIRED", filters_passed=(),
            filters_failed=("RECLAIM",), signal_type=None, candidate_signal_id=None,
            audit_snapshot={"fixture": "skip"},
        )
        et = store.record_strategy_evaluation(eval_trade)
        es = store.record_strategy_evaluation(eval_skip)
        store.record_terminal_decision(TerminalDecisionAuditInput(
            decision_key="FIX-T", decision_scope="TOKEN", run_id="FIX-RUN-T",
            role="CONTROL", mint="FIX-MINT-T", strategy_version="v1.1",
            parameter_set_id="FIX-P-T", decision_at_us=3,
            trade_or_skip=TradeOrSkip.TRADE, decision_reason="ENTRY_FILLED",
            candidate_signal_id="FIX-SIG-T", paper_entry_route_id="FIX-ROUTE",
            source_evaluation_id=et,
        ))
        store.record_terminal_decision(TerminalDecisionAuditInput(
            decision_key="FIX-S", decision_scope="TOKEN", run_id="FIX-RUN-S",
            role="ROBUST_2", mint="FIX-MINT-S", strategy_version="v1.1",
            parameter_set_id="FIX-P-S", decision_at_us=4,
            trade_or_skip=TradeOrSkip.SKIP, decision_reason="ENTRY_WINDOW_EXPIRED",
            candidate_signal_id=None, paper_entry_route_id=None,
            source_evaluation_id=es,
        ))

        # Two causal marks per exact persisted track, then realized close equity.
        for seq, when, num, den in [
            (501, "2026-08-23T18:00:02+00:00", 95, 1000),
            (502, "2026-08-23T18:00:04+00:00", 115, 1000),
        ]:
            for track in LOCKED_TRACKS:
                pos = positions[track]
                mark = store.record_position_mark(
                    str(pos["paper_position_id"]),
                    PositionMarkObservation(
                        mint=str(pos["mint"]), observed_at=dt(when), ingest_seq=seq,
                        source_event_key=f"FIX-MARK-{seq}", price_identity=str(pos["price_identity"]),
                        price_numerator_raw=num, price_denominator_raw=den,
                        current_virtual_token_reserve_raw=60_000_000_000,
                    ),
                )
                assert mark is not None
                store.record_track_equity_mark(
                    track_id=track, observed_at=dt(when), ingest_seq=seq,
                    source_event_key=f"FIX-EQ-{track}-{seq}",
                    realized_closed_pnl_lamports=0,
                    open_position_net_mtm=((mark.paper_position_id, mark.net_mtm_pnl_lamports),),
                )

        close_times = {
            "SENS-C": "2026-08-23T18:00:05.500001+00:00",
            "FINAL-A": "2026-08-23T18:00:10.500000+00:00",
            "FINAL-B": "2026-08-23T18:00:13.500000+00:00",
        }
        for i, track in enumerate(LOCKED_TRACKS, start=601):
            store.record_track_equity_mark(
                track_id=track, observed_at=dt(close_times[track]), ingest_seq=i,
                source_event_key=f"FIX-CLOSE-{track}",
                realized_closed_pnl_lamports=trades[track].net_pnl_lamports,
                open_position_net_mtm=(),
            )

        report = build_report(conn)
        digest1 = canonical_report_digest(report)
        digest2 = canonical_report_digest(build_report(conn))
        pos = {p.track_id: p for p in report.positions}
        tracks = {t.track_id: t for t in report.tracks}
        quick = conn.execute("PRAGMA quick_check").fetchone()[0]

        checks["exact_fixture_track_binding"] = set(positions) == set(LOCKED_TRACKS)
        checks["all_position_marks_persisted"] = len(report.positions) == 3 and all(pos[t].marks == 2 for t in LOCKED_TRACKS)
        checks["mae_mfe_available"] = all(pos[t].mae_bps == -500 and pos[t].mfe_bps == 1500 for t in LOCKED_TRACKS)
        checks["mtm_drawdown_stream_available"] = all(tracks[t].equity_marks == 3 and tracks[t].max_drawdown_lamports >= 0 for t in LOCKED_TRACKS)
        checks["final_equity_matches_realized_pnl"] = all(tracks[t].final_equity_pnl_lamports == trades[t].net_pnl_lamports for t in LOCKED_TRACKS)
        checks["trade_skip_audit_available"] = report.strategy.trades == 1 and report.strategy.skips == 1
        checks["skip_reason_exact"] = report.strategy.skip_reasons == (("ENTRY_WINDOW_EXPIRED", 1),)
        checks["canonical_repeat_equal"] = digest1 == digest2
        checks["sqlite_quick_check"] = str(quick).lower() == "ok"
        conn.close()

    print("-" * 120)
    print("VALIDATION")
    print("-" * 120)
    for k, v in checks.items():
        print(f"{k:<54}: {'PASS' if v else 'FAIL'}")
    all_ok = all(checks.values())
    print()
    print(f"Canonical report digest         : {digest1}")
    print("MTM / MAE / MFE persisted       : YES")
    print("Strategy trade/skip audit       : YES")
    print("Cross-track portfolio sum       : NO")
    print("Production DB touched           : NO")
    print("Original fixture mutated        : NO")
    print()
    print(f"RESULT: {'PASS' if all_ok else 'CHECK'}")
    if all_ok:
        print("NEXT: bind P4-RUNTIME-OBSERVABILITY-0001 to controlled live Phase-4 runtime, then validate live persisted MTM/MAE/MFE and terminal trade/skip audit.")
    return 0 if all_ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
