from __future__ import annotations

import argparse
import importlib.util
import sqlite3
import sys
from fractions import Fraction
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = PROJECT_ROOT / "src" / "phase4" / "paper_trade_accounting_v0_1.py"
LIVE_SMOKE_DIR = PROJECT_ROOT / "data" / "paper" / "live_smoke"


def load_module():
    spec = importlib.util.spec_from_file_location("p4_accounting_v01_postrun", MODULE_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load accounting module")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def latest_4e_db() -> Path:
    dbs = sorted(
        LIVE_SMOKE_DIR.glob("phase4_4e_live_entry_exit_*.sqlite3"),
        key=lambda p: p.stat().st_mtime_ns,
        reverse=True,
    )
    if not dbs:
        raise FileNotFoundError(f"No Phase 4.4E live paper DB found under {LIVE_SMOKE_DIR}")
    return dbs[0]


def fmt_sol(lamports: int) -> str:
    sign = "-" if lamports < 0 else ""
    v = abs(lamports)
    return f"{sign}{v / 1_000_000_000:.9f} SOL"


def fmt_pf(pf) -> str:
    if pf.state == "FINITE":
        return f"{float(Fraction(pf.numerator_profit_lamports, pf.denominator_loss_lamports)):.6f}"
    if pf.state == "INFINITE_NO_LOSSES":
        return "INF (no losses)"
    if pf.state == "ZERO_NO_WINS":
        return "0.000000 (no wins)"
    return pf.state


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, default=None)
    args = parser.parse_args()

    mod = load_module()
    db = args.db.resolve() if args.db else latest_4e_db().resolve()
    if not db.exists():
        raise FileNotFoundError(db)

    print("=" * 120)
    print("PHASE 4.5A - READ-ONLY LIVE POSTRUN TRADE ACCOUNTING SMOKE v0.1")
    print("=" * 120)
    print(f"Project root                         : {PROJECT_ROOT}")
    print(f"Paper DB                             : {db}")
    print("Paper DB mode                        : QUERY-ONLY / READ-ONLY")
    print("Production market DB opened          : NO")
    print("Collector / network / RPC            : NO")
    print("Wallet / signing / live orders       : NO")
    print("Paper execution mutation             : NO")
    print("Parameter tuning / reselection       : NO")
    print(f"Accounting model                     : {mod.ACCOUNTING_MODEL_ID}")
    print(f"Accounting fingerprint               : {mod.ACCOUNTING_FINGERPRINT}")
    print("Cross-track PnL summation            : NO — tracks are alternative exit variants")
    print("Locked MTM max drawdown              : NOT AVAILABLE from fills alone")
    print()

    uri = db.as_uri() + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=5.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    try:
        report = mod.build_report(conn)
        trades = mod.load_completed_trades(conn)
        digest1 = mod.canonical_report_digest(report)
        digest2 = mod.canonical_report_digest(mod.build_report(conn))
        quick = conn.execute("PRAGMA quick_check").fetchone()[0]
    finally:
        conn.close()

    print("-" * 120)
    print("COMPLETED TRADE ACCOUNTING")
    print("-" * 120)
    for trade in trades:
        print(
            f"{trade.track_id:<8} outcome={trade.outcome:<9} "
            f"gross={fmt_sol(trade.gross_execution_pnl_lamports):>16} "
            f"entry_cost={fmt_sol(trade.entry_explicit_cost_lamports):>16} "
            f"exit_cost={fmt_sol(trade.exit_explicit_cost_lamports):>16} "
            f"net={fmt_sol(trade.net_pnl_lamports):>16} "
            f"net_return={trade.net_return_bps/100:.2f}% "
            f"hold={trade.holding_time_us/1_000_000:.6f}s "
            f"reason={trade.exit_reason}"
        )

    print()
    print("-" * 120)
    print("PER-TRACK METRICS")
    print("-" * 120)
    for a in report.tracks:
        expectancy = (
            None if a.expectancy_denominator_trades == 0
            else Fraction(a.expectancy_numerator_lamports, a.expectancy_denominator_trades)
        )
        exp_text = "N/A" if expectancy is None else fmt_sol(int(expectancy))
        avg_ret = (
            0.0 if a.average_net_return_bps_denominator == 0
            else a.average_net_return_bps_numerator / a.average_net_return_bps_denominator / 100.0
        )
        print(
            f"{a.track_id:<8} trades={a.completed_trades} wins={a.wins} losses={a.losses} "
            f"breakeven={a.breakevens} net={fmt_sol(a.net_pnl_lamports)} "
            f"expectancy={exp_text} avg_net_return={avg_ret:.2f}% "
            f"PF={fmt_pf(a.profit_factor)} winrate={a.winrate_bps/100:.2f}% "
            f"realized_closed_equity_DD={fmt_sol(a.realized_closed_equity_max_drawdown_lamports)}"
        )

    print()
    print("-" * 120)
    print("EXECUTION / REJECTION AUDIT")
    print("-" * 120)
    ra = report.rejection_audit
    print(f"Entry routes total                    : {ra.entry_route_total}")
    print(f"Rejected entry routes                 : {ra.rejected_entry_routes}")
    print(f"Exit attempts total                   : {ra.exit_attempt_total}")
    print(f"Filled exit attempts                  : {ra.filled_exit_attempts_total}")
    print(f"Rejected exit attempts                : {ra.rejected_exit_attempts_total}")
    print(f"Rejected exits by track               : {ra.rejected_exit_attempts_by_track}")
    print(f"Strategy skip audit                   : {ra.strategy_skip_audit_status}")

    checks = {
        "at_least_one_completed_trade": report.completed_trade_count > 0,
        "all_completed_tracks_locked": all(t.track_id in mod.LOCKED_TRACKS for t in trades),
        "accounting_digest_repeat_equal": digest1 == digest2,
        "paper_sqlite_quick_check": str(quick).lower() == "ok",
        "cross_track_portfolio_sum_blocked": report.cross_track_portfolio_aggregation_status.startswith("PROHIBITED"),
        "mtm_drawdown_not_fabricated": report.mtm_drawdown_status == "UNAVAILABLE_NO_PERSISTED_CAUSAL_MTM_STREAM",
        "strategy_skip_not_fabricated": ra.strategy_skip_audit_status == "UNAVAILABLE_FROM_PAPER_EXECUTION_DB_ALONE",
    }

    print()
    print("-" * 120)
    print("VALIDATION")
    print("-" * 120)
    for name, ok in checks.items():
        print(f"{name:<52}: {'PASS' if ok else 'FAIL'}")

    all_ok = all(checks.values())
    print()
    print(f"Canonical accounting digest           : {digest1}")
    print("Locked mark-to-market max drawdown    : UNAVAILABLE — next persistence step")
    print("MAE / MFE                             : UNAVAILABLE — next persistence step")
    print("Strategy skip audit                   : UNAVAILABLE — next persistence step")
    print("Paper DB touched                      : NO")
    print("Live market action                    : NO")
    print("Wallet/signing/live orders            : NO")
    print("Parameter tuning/reselection          : NO")
    print()
    print(f"RESULT: {'PASS' if all_ok else 'CHECK'}")
    if all_ok:
        print("NEXT: persist causal mark-to-market equity/MAE/MFE + token trade/skip audit in the continuous paper runtime; do not derive them retroactively from fills.")
    return 0 if all_ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
