from __future__ import annotations

import importlib.util
import shutil
import sqlite3
import sys
import tempfile
from dataclasses import replace
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = PROJECT_ROOT / "src" / "phase4" / "paper_trade_accounting_v0_1.py"
FIXTURE = PROJECT_ROOT / "data" / "selftest" / "phase4_4e_live_exit_pipeline_fixture.sqlite3"


def load_module():
    spec = importlib.util.spec_from_file_location("p4_accounting_v01", MODULE_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load accounting module")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def main() -> int:
    print("=" * 118)
    print("PHASE 4.5A - DETERMINISTIC PAPER TRADE ACCOUNTING FOUNDATION SELF-TEST v0.1")
    print("=" * 118)
    print(f"Project root                       : {PROJECT_ROOT}")
    print(f"Accounting module                  : {MODULE_PATH}")
    print(f"Exact Phase-4.4E fixture           : {FIXTURE}")
    print("Production DB opened               : NO")
    print("Collector / network / RPC          : NO")
    print("Wallet / signing / live orders     : NO")
    print("Paper execution mutation           : NO")
    print("Parameter tuning / reselection     : NO")
    print("Cross-track portfolio summation    : PROHIBITED")
    print("Locked MTM max drawdown            : NOT CLAIMED IN THIS STEP")
    print()

    mod = load_module()
    if not FIXTURE.exists():
        raise FileNotFoundError(FIXTURE)

    uri = FIXTURE.resolve().as_uri() + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    try:
        report = mod.build_report(conn)
        trades = mod.load_completed_trades(conn)
        digest1 = mod.canonical_report_digest(report)
        report2 = mod.build_report(conn)
        digest2 = mod.canonical_report_digest(report2)
        quick = conn.execute("PRAGMA quick_check").fetchone()[0]
    finally:
        conn.close()

    by_track = {t.track_id: t for t in trades}
    ag = {a.track_id: a for a in report.tracks}

    checks = {}
    checks["exact_accounting_model_id"] = mod.ACCOUNTING_MODEL_ID == "P4-TRADE-ACCOUNTING-0001"
    checks["fingerprint_present"] = isinstance(mod.ACCOUNTING_FINGERPRINT, str) and len(mod.ACCOUNTING_FINGERPRINT) == 64
    checks["exact_three_fixture_completed_trades"] = len(trades) == 3
    checks["semantic_locked_track_membership"] = set(by_track) == set(mod.LOCKED_TRACKS)

    checks["final_a_exact_net_pnl"] = by_track["FINAL-A"].net_pnl_lamports == -5_255_199
    checks["final_b_exact_net_pnl"] = by_track["FINAL-B"].net_pnl_lamports == 1_022_733
    checks["sens_c_exact_net_pnl"] = by_track["SENS-C"].net_pnl_lamports == -15_568_945
    checks["gross_before_explicit_costs_exact"] = (
        by_track["FINAL-A"].gross_execution_pnl_lamports == -552_100
        and by_track["FINAL-B"].gross_execution_pnl_lamports == 5_805_300
        and by_track["SENS-C"].gross_execution_pnl_lamports == -10_996_400
    )
    checks["entry_cost_charged_per_alternative_track"] = all(
        t.entry_explicit_cost_lamports == 2_355_000 for t in trades
    )
    checks["net_formula_no_double_count"] = all(
        t.net_pnl_lamports
        == t.gross_exit_proceeds_lamports
        - t.entry_principal_lamports
        - t.entry_explicit_cost_lamports
        - t.exit_explicit_cost_lamports
        for t in trades
    )
    checks["fixture_outcomes_exact"] = (
        by_track["FINAL-A"].outcome == "LOSS"
        and by_track["FINAL-B"].outcome == "WIN"
        and by_track["SENS-C"].outcome == "LOSS"
    )
    checks["one_trade_expectancy_exact"] = all(
        ag[k].expectancy_numerator_lamports == by_track[k].net_pnl_lamports
        and ag[k].expectancy_denominator_trades == 1
        for k in mod.LOCKED_TRACKS
    )
    checks["profit_factor_single_trade_states"] = (
        ag["FINAL-A"].profit_factor.state == "ZERO_NO_WINS"
        and ag["FINAL-B"].profit_factor.state == "INFINITE_NO_LOSSES"
        and ag["SENS-C"].profit_factor.state == "ZERO_NO_WINS"
    )
    checks["breakeven_reported_separately"] = all(a.breakevens == 0 for a in report.tracks)
    checks["mtm_drawdown_explicitly_unavailable"] = (
        report.mtm_drawdown_status == "UNAVAILABLE_NO_PERSISTED_CAUSAL_MTM_STREAM"
        and all(a.mtm_max_drawdown_status == "UNAVAILABLE_NO_PERSISTED_CAUSAL_MTM_STREAM" for a in report.tracks)
    )
    checks["cross_track_portfolio_aggregation_blocked"] = report.cross_track_portfolio_aggregation_status.startswith("PROHIBITED")
    checks["strategy_skip_audit_not_fabricated"] = report.rejection_audit.strategy_skip_audit_status == "UNAVAILABLE_FROM_PAPER_EXECUTION_DB_ALONE"
    checks["fixture_rejections_zero"] = (
        report.rejection_audit.rejected_entry_routes == 0
        and report.rejection_audit.rejected_exit_attempts_total == 0
    )
    checks["canonical_report_deterministic"] = digest1 == digest2
    checks["fixture_sqlite_quick_check"] = str(quick).lower() == "ok"

    # Pure aggregate stress case: same track with win, loss, breakeven.
    base = by_track["FINAL-A"]
    synthetic = [
        replace(base, signal_key="S1", closed_at="2026-08-23T18:00:01+00:00", net_pnl_lamports=10_000_000, net_return_bps=1000, outcome="WIN", holding_time_us=1_000_000),
        replace(base, signal_key="S2", closed_at="2026-08-23T18:00:02+00:00", net_pnl_lamports=-4_000_000, net_return_bps=-400, outcome="LOSS", holding_time_us=2_000_000),
        replace(base, signal_key="S3", closed_at="2026-08-23T18:00:03+00:00", net_pnl_lamports=0, net_return_bps=0, outcome="BREAKEVEN", holding_time_us=3_000_000),
        replace(base, signal_key="S4", closed_at="2026-08-23T18:00:04+00:00", net_pnl_lamports=-8_000_000, net_return_bps=-800, outcome="LOSS", holding_time_us=4_000_000),
        replace(base, signal_key="S5", closed_at="2026-08-23T18:00:05+00:00", net_pnl_lamports=6_000_000, net_return_bps=600, outcome="WIN", holding_time_us=5_000_000),
    ]
    stress = mod.aggregate_track("FINAL-A", synthetic)
    checks["stress_counts_win_loss_breakeven"] = (stress.completed_trades, stress.wins, stress.losses, stress.breakevens) == (5, 2, 2, 1)
    checks["stress_expectancy_exact"] = (stress.expectancy_numerator_lamports, stress.expectancy_denominator_trades) == (4_000_000, 5)
    checks["stress_profit_factor_exact"] = (
        stress.profit_factor.state == "FINITE"
        and stress.profit_factor.numerator_profit_lamports == 16_000_000
        and stress.profit_factor.denominator_loss_lamports == 12_000_000
    )
    checks["stress_realized_closed_equity_dd_exact"] = stress.realized_closed_equity_max_drawdown_lamports == 12_000_000
    checks["stress_median_holding_exact"] = stress.median_holding_time_us == 3_000_000
    checks["stress_winrate_all_completed_trades"] = stress.winrate_bps == 4000

    # Rejection audit on an isolated copy: add one rejected exit attempt to a real route.
    with tempfile.TemporaryDirectory() as td:
        copy = Path(td) / "fixture.sqlite3"
        shutil.copy2(FIXTURE, copy)
        rw = sqlite3.connect(copy)
        rw.row_factory = sqlite3.Row
        route = rw.execute("SELECT route_id, paper_position_id FROM paper_exit_execution_routes ORDER BY route_id LIMIT 1").fetchone()
        rw.execute(
            """
            INSERT INTO paper_exit_execution_attempts (
                attempt_id, route_id, paper_position_id,
                market_observed_at, market_ingest_seq, source_event_key,
                observation_fingerprint, market_price_numerator_raw,
                market_price_denominator_raw, current_virtual_token_reserve_raw,
                derived_token_input_raw, price_impact_bps, signed_market_move_bps,
                adverse_slippage_bps, slippage_cap_bps, decision,
                rejection_reason, simulated_exit_price_numerator_raw,
                simulated_exit_price_denominator_raw, gross_exit_proceeds_lamports,
                venue_fee_lamports, interface_fee_lamports,
                base_network_fee_lamports, priority_fee_lamports,
                builder_tip_lamports, total_exit_explicit_cost_lamports, created_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "PXA-ACCOUNT-REJECT", str(route["route_id"]), str(route["paper_position_id"]),
                "2026-08-23T18:00:20+00:00", 999, "REJECT", "fp-account-reject",
                "1", "1", "1000", "100", 100, -2500, 2500, 2000,
                "REJECTED_SLIPPAGE", "SLIPPAGE_CAP_EXCEEDED", "1", "1", None,
                None, None, None, None, None, None, "2026-08-23T18:00:20+00:00",
            ),
        )
        rw.commit()
        rejection = mod.load_rejection_audit(rw)
        rw.close()
        checks["exit_rejection_audit_counts_real_rows"] = rejection.rejected_exit_attempts_total == 1 and rejection.exit_attempt_total == 4
        checks["exit_rejection_audit_track_bound"] = len(rejection.rejected_exit_attempts_by_track) == 1 and rejection.rejected_exit_attempts_by_track[0][1] == 1

    print("-" * 118)
    print("VALIDATION")
    print("-" * 118)
    for name, ok in checks.items():
        print(f"{name:<60}: {'PASS' if ok else 'FAIL'}")

    all_ok = all(checks.values())
    print()
    print(f"Accounting model ID              : {mod.ACCOUNTING_MODEL_ID}")
    print(f"Accounting fingerprint           : {mod.ACCOUNTING_FINGERPRINT}")
    print(f"Fixture report digest            : {digest1}")
    print("Net result includes              : entry principal + entry explicit costs + exit explicit costs + simulated execution prices")
    print("Cross-track portfolio PnL        : NOT SUMMED (alternative exit variants)")
    print("Locked MTM max drawdown          : UNAVAILABLE IN 4.5A (needs persisted causal MTM stream)")
    print("Strategy skip audit              : UNAVAILABLE FROM PAPER EXECUTION DB ALONE")
    print("Production DB touched            : NO")
    print("Paper execution state mutated    : NO")
    print("Wallet/signing/live orders       : NO")
    print("Parameter tuning/reselection     : NO")
    print()
    print(f"RESULT: {'PASS' if all_ok else 'CHECK'}")
    if all_ok:
        print("NEXT: run read-only accounting smoke on the persisted Phase 4.4E live paper DB, then add causal MTM/skip-audit persistence before continuous paper trading.")
    return 0 if all_ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
