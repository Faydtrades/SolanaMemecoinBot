from __future__ import annotations

import shutil
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from phase4.paper_cost_baseline_v0_1 import P4_COST_BASELINE_0001
from phase4.paper_cost_model_v0_2 import PaperCostModelV02
from phase4.paper_entry_router_v0_1 import PaperEntryRouterV01
from phase4.paper_exit_fill_executor_v0_1 import PaperExitFillExecutorV01
from phase4.paper_exit_lifecycle_bridge_v0_2 import PaperExitLifecycleBridgeV02
from phase4.paper_live_observability_binding_v0_2 import PaperLiveObservabilityBindingV01
from phase4.paper_runtime_observability_v0_1 import PositionMarkObservation
from phase4.paper_trade_accounting_v0_1 import validate_source_schema

FIXTURE = PROJECT_ROOT / "data" / "selftest" / "phase4_4e_live_exit_pipeline_fixture.sqlite3"
WORK = PROJECT_ROOT / "data" / "selftest" / "phase4_5c_entry_equity_schema_order_selftest.sqlite3"
LOCKED_TRACKS = ("FINAL-A", "FINAL-B", "SENS-C")


def dt(s: str) -> datetime:
    return datetime.fromisoformat(s)


def main() -> int:
    print("=" * 118)
    print("PHASE 4.5C - ENTRY EQUITY / EXIT-SCHEMA INITIALIZATION ORDER REGRESSION SELF-TEST v0.1")
    print("=" * 118)
    print(f"Project root                       : {PROJECT_ROOT}")
    print(f"Exact 4.4E fixture                 : {FIXTURE}")
    print("Production DB / collector / RPC    : NO")
    print("Wallet / live orders               : NO")
    print("Purpose                            : reproduce fresh-live entry mark before exit-executor schema exists")
    print()

    if not FIXTURE.exists():
        raise FileNotFoundError(FIXTURE)
    if WORK.exists():
        WORK.unlink()
    shutil.copy2(FIXTURE, WORK)

    conn = sqlite3.connect(WORK)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")

    # Recreate the exact fresh-live condition after entry fill and before the
    # exit executor has ever been constructed: three OPEN positions, but no
    # paper_exit_execution_* schema yet.
    conn.execute(
        "UPDATE paper_positions SET state='OPEN', state_reason='REGRESSION_OPEN', closed_at=NULL, exit_requested_at=NULL, exit_price_numerator_raw=NULL, exit_price_denominator_raw=NULL"
    )
    conn.execute("DROP TABLE IF EXISTS paper_exit_execution_attempts")
    conn.execute("DROP TABLE IF EXISTS paper_exit_execution_routes")
    conn.execute("DROP TABLE IF EXISTS paper_exit_execution_schema_meta")
    conn.commit()

    cost_model = PaperCostModelV02(P4_COST_BASELINE_0001)
    router = PaperEntryRouterV01(conn, cost_model)
    binding = PaperLiveObservabilityBindingV01(conn, cost_model)

    obs = PositionMarkObservation(
        mint="MINT-4E-FIXTURE",
        observed_at=dt("2026-08-23T18:00:01+00:00"),
        ingest_seq=501,
        source_event_key="ENTRY-MARK-ORDER-REGRESSION",
        price_identity="SOL_NATIVE",
        price_numerator_raw=100,
        price_denominator_raw=1000,
        current_virtual_token_reserve_raw=60_000_000_000,
        is_gap_recovery=False,
    )

    marks = binding.record_position_marks_for_market(obs)

    pre_executor_failed_closed = False
    pre_error = ""
    try:
        binding.record_track_equity_for_market(
            observed_at=obs.observed_at,
            ingest_seq=obs.ingest_seq,
            source_event_key="ENTRY-EQ-PRE-EXECUTOR",
        )
    except RuntimeError as exc:
        pre_error = str(exc)
        pre_executor_failed_closed = (
            "paper_exit_execution_routes missing required columns" in pre_error
        )

    # Correct v0.3 ordering: create exit bridge/executor and validate their
    # complete source schema BEFORE requesting the first equity mark.
    bridge = PaperExitLifecycleBridgeV02(conn, lifecycle=router.lifecycle)
    executor = PaperExitFillExecutorV01(
        conn,
        cost_model,
        lifecycle=router.lifecycle,
        exit_orchestrator=bridge.exit_orchestrator,
    )
    validate_source_schema(conn)

    equities = binding.record_track_equity_for_market(
        observed_at=obs.observed_at,
        ingest_seq=obs.ingest_seq,
        source_event_key="ENTRY-EQ-POST-EXECUTOR",
    )
    equities_replay = binding.record_track_equity_for_market(
        observed_at=obs.observed_at,
        ingest_seq=obs.ingest_seq,
        source_event_key="ENTRY-EQ-POST-EXECUTOR",
    )

    route_cols = {
        str(r[1]) for r in conn.execute("PRAGMA table_info(paper_exit_execution_routes)")
    }
    required_route_cols = {
        "route_id", "paper_position_id", "track_id", "exit_reason", "state",
        "cost_model_fingerprint", "exit_impact_model_id", "exit_impact_model_fingerprint",
        "price_impact_bps", "adverse_slippage_bps", "gross_exit_proceeds_lamports",
        "total_exit_explicit_cost_lamports",
    }

    eq_rows = int(conn.execute("SELECT COUNT(*) FROM paper_track_equity_marks").fetchone()[0])
    quick = str(conn.execute("PRAGMA quick_check").fetchone()[0]).lower()

    checks = {
        "three_open_entry_marks_persisted": set(marks) == set(LOCKED_TRACKS),
        "old_ordering_failure_reproduced_fail_closed": pre_executor_failed_closed,
        "exit_executor_creates_required_accounting_schema": required_route_cols <= route_cols,
        "post_executor_equity_all_tracks": set(equities) == set(LOCKED_TRACKS),
        "post_executor_equity_replay_idempotent": set(equities_replay) == set(LOCKED_TRACKS),
        "equity_rows_exact_three": eq_rows == 3,
        "sqlite_quick_check": quick == "ok",
    }

    print("-" * 118)
    print("DIAGNOSIS")
    print("-" * 118)
    print(f"Pre-executor error                  : {pre_error}")
    print("Root cause                          : equity accounting was called before PaperExitFillExecutorV01 created exit-execution schema")
    print("Correct ordering                    : exit bridge/executor -> schema validate -> first MTM/equity persistence")
    print()
    print("-" * 118)
    print("VALIDATION")
    print("-" * 118)
    for k, v in checks.items():
        print(f"{k:<62}: {'PASS' if v else 'FAIL'}")

    all_ok = all(checks.values())
    print()
    print(f"RESULT: {'PASS' if all_ok else 'CHECK'}")
    conn.close()
    return 0 if all_ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
