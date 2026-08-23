from __future__ import annotations

import shutil
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
    connect_entry_router_db,
)
from src.phase4.paper_exit_lifecycle_bridge_v0_1 import PaperExitLifecycleBridgeV01
from src.phase4.paper_exit_orchestrator_v0_1 import (
    ExitMarketObservation,
    ExitReason,
    LOCKED_EXIT_SPEC_FINGERPRINT,
    LOCKED_TRACK_ORDER,
)
from src.phase4.paper_lifecycle_v0_1 import PositionState

UTC = timezone.utc
SELFTEST_DIR = PROJECT_ROOT / "data" / "selftest"
DB_MAIN = SELFTEST_DIR / "phase4_4b_exit_lifecycle_bridge_selftest.sqlite3"
DB_FALLBACK = SELFTEST_DIR / "phase4_4b_exit_lifecycle_bridge_fallback_selftest.sqlite3"


def clean_sqlite(path: Path) -> None:
    for suffix in ("", "-wal", "-shm"):
        p = Path(str(path) + suffix)
        if p.exists():
            p.unlink()


def make_filled_entry(path: Path, *, signal_key: str, t0: datetime):
    clean_sqlite(path)
    conn = connect_entry_router_db(path)
    model = PaperCostModelV02(P4_COST_BASELINE_0001)
    from src.phase4.paper_entry_router_v0_1 import PaperEntryRouterV01
    router = PaperEntryRouterV01(conn, model)

    candidate = CandidateSignalEnvelope(
        candidate_id=signal_key,
        mint="MINT-EXIT-BRIDGE-TEST",
        strategy_version="v1.1",
        parameter_set_id="LOCKED-FIXTURE",
        signal_observed_at=t0,
        signal_ingest_seq=100,
        price_identity="SOL_NATIVE",
        price_numerator_raw=100,
        price_denominator_raw=1000,
    )
    fill_obs = EntryMarketObservation(
        mint=candidate.mint,
        observed_at=t0 + timedelta(milliseconds=500),
        ingest_seq=101,
        price_identity="SOL_NATIVE",
        price_numerator_raw=100,
        price_denominator_raw=1000,
        is_gap_recovery=False,
        source_event_key=f"{signal_key}-ENTRY",
    )
    result = router.on_market_observation(
        candidate=candidate,
        observation=fill_obs,
        price_impact_bps=0,
        observed_priority_fee_lamports=None,
        venue_fee_bps=None,
    )
    assert result.route.state.value == "FILLED"
    assert all(o.state.value == "FILLED" for o in result.orders)
    assert len(router.lifecycle.list_open_positions()) == 3
    return conn, router, candidate, fill_obs


def check(label: str, ok: bool, checks: dict[str, bool]) -> None:
    checks[label] = bool(ok)
    print(f"{label:<72}: {'PASS' if ok else 'FAIL'}")


def main() -> int:
    print("=" * 116)
    print("PHASE 4.4B - EXIT LIFECYCLE BRIDGE SELF-TEST v0.1")
    print("=" * 116)
    print(f"Project root                       : {PROJECT_ROOT}")
    print("Production DB opened               : NO")
    print("Collector / network / RPC          : NO")
    print("Wallet / signing / live orders     : NO")
    print("Exit fill / position close         : NO")
    print("PnL fabrication                    : NO")
    print("Parameter tuning / reselection     : NO")
    print(f"Locked exit spec fingerprint       : {LOCKED_EXIT_SPEC_FINGERPRINT}")
    print()

    SELFTEST_DIR.mkdir(parents=True, exist_ok=True)
    checks: dict[str, bool] = {}

    # Scenario A: market-triggered FINAL-A + SENS-C, then FINAL-B trail.
    t0 = datetime(2026, 8, 23, 12, 0, 0, tzinfo=UTC)
    conn, router, candidate, fill_obs = make_filled_entry(DB_MAIN, signal_key="SIG-4B-MARKET", t0=t0)
    bridge = PaperExitLifecycleBridgeV01(conn, lifecycle=router.lifecycle)

    refs = bridge.bind_signal(candidate.candidate_id)
    check("bind persisted FILLED entry -> semantic FINAL-A/B/SENS-C refs", tuple(r.track_id for r in refs) == LOCKED_TRACK_ORDER, checks)
    check("all lifecycle positions initially OPEN", all(p.state is PositionState.OPEN for p in router.lifecycle.list_open_positions()), checks)
    check("rule reference remains CandidateSignal reference", all((r.rule_reference_price_numerator_raw, r.rule_reference_price_denominator_raw) == (100, 1000) for r in refs), checks)
    check("entry causality lineage retained", all((r.entry_market_observed_at, r.entry_market_ingest_seq) == (fill_obs.observed_at, fill_obs.ingest_seq) for r in refs), checks)

    # +20% from rule reference: FINAL-A TP + SENS-C TP, FINAL-B activates at +20%.
    obs20 = ExitMarketObservation(
        mint=candidate.mint,
        observed_at=t0 + timedelta(seconds=2),
        ingest_seq=102,
        price_identity="SOL_NATIVE",
        price_numerator_raw=120,
        price_denominator_raw=1000,
        source_event_key="EXIT-PLUS20",
    )
    r1 = bridge.on_market_observation(candidate.candidate_id, obs20)
    intents1 = {e.track.track_id: e.intent for e in r1.evaluations}
    states1 = {p.track_id: p.state for p in r1.positions}
    check("FINAL-A intent = TAKE_PROFIT", intents1["FINAL-A"] is not None and intents1["FINAL-A"].reason is ExitReason.TAKE_PROFIT, checks)
    check("SENS-C intent = TAKE_PROFIT", intents1["SENS-C"] is not None and intents1["SENS-C"].reason is ExitReason.TAKE_PROFIT, checks)
    check("FINAL-B +20% only activates trail", intents1["FINAL-B"] is None, checks)
    check("FINAL-A lifecycle -> EXIT_PENDING", states1["FINAL-A"] is PositionState.EXIT_PENDING, checks)
    check("SENS-C lifecycle -> EXIT_PENDING", states1["SENS-C"] is PositionState.EXIT_PENDING, checks)
    check("FINAL-B remains OPEN while trailing", states1["FINAL-B"] is PositionState.OPEN, checks)

    digest_after_first = bridge.canonical_digest()
    r1_replay = bridge.on_market_observation(candidate.candidate_id, obs20)
    check("market replay is lifecycle/orchestrator idempotent", bridge.canonical_digest() == digest_after_first, checks)

    # Drop from +20% peak to +17% = exact 300 bps giveback -> FINAL-B trail exit.
    obs17 = ExitMarketObservation(
        mint=candidate.mint,
        observed_at=t0 + timedelta(seconds=3),
        ingest_seq=103,
        price_identity="SOL_NATIVE",
        price_numerator_raw=117,
        price_denominator_raw=1000,
        source_event_key="EXIT-PLUS17",
    )
    r2 = bridge.on_market_observation(candidate.candidate_id, obs17)
    intents2 = {e.track.track_id: e.intent for e in r2.evaluations}
    states2 = {p.track_id: p.state for p in r2.positions}
    check("FINAL-B exact 300bps giveback intent = TRAIL", intents2["FINAL-B"] is not None and intents2["FINAL-B"].reason is ExitReason.TRAIL, checks)
    check("all three lifecycle positions EXIT_PENDING", all(states2[t] is PositionState.EXIT_PENDING for t in LOCKED_TRACK_ORDER), checks)
    check("no position CLOSED by bridge", all(p.closed_at is None for p in r2.positions), checks)
    check("no exit price fabricated by bridge", all(p.exit_price_numerator_raw is None and p.exit_price_denominator_raw is None for p in r2.positions), checks)

    digest_terminal = bridge.canonical_digest()
    conn.close()

    # Restart against exact persisted state.
    import sqlite3
    conn_r = sqlite3.connect(DB_MAIN)
    conn_r.row_factory = sqlite3.Row
    conn_r.execute("PRAGMA foreign_keys=ON")
    bridge_r = PaperExitLifecycleBridgeV01(conn_r)
    refs_r = bridge_r.bind_signal(candidate.candidate_id)
    check("restart rebinds exact three persisted exit tracks", tuple(r.track_id for r in refs_r) == LOCKED_TRACK_ORDER, checks)
    check("restart retains all three EXIT_PENDING positions", all(p.state is PositionState.EXIT_PENDING for p in bridge_r.lifecycle.list_open_positions()), checks)
    check("restart canonical digest unchanged", bridge_r.canonical_digest() == digest_terminal, checks)
    replay_after_restart = bridge_r.on_market_observation(candidate.candidate_id, obs17)
    check("terminal replay after restart remains idempotent", bridge_r.canonical_digest() == digest_terminal, checks)
    quick_main = conn_r.execute("PRAGMA quick_check").fetchone()[0]
    check("market-trigger scenario SQLite quick_check", str(quick_main).lower() == "ok", checks)
    conn_r.close()

    # Scenario B: fallback clocks propagate to lifecycle without synthetic fill.
    t1 = datetime(2026, 8, 23, 13, 0, 0, tzinfo=UTC)
    conn_f, router_f, candidate_f, fill_f = make_filled_entry(DB_FALLBACK, signal_key="SIG-4B-FALLBACK", t0=t1)
    bridge_f = PaperExitLifecycleBridgeV01(conn_f, lifecycle=router_f.lifecycle)
    bridge_f.bind_signal(candidate_f.candidate_id)

    at5 = bridge_f.on_clock(candidate_f.candidate_id, t1 + timedelta(seconds=5, microseconds=1))
    states5 = {p.track_id: p.state for p in at5.positions}
    eval5 = {e.track.track_id: e for e in at5.evaluations}
    check("SENS-C fallback intent at 5s+1us", eval5["SENS-C"].intent is not None and eval5["SENS-C"].intent.reason is ExitReason.FALLBACK, checks)
    check("SENS-C fallback -> EXIT_PENDING", states5["SENS-C"] is PositionState.EXIT_PENDING, checks)
    check("FINAL-A/B still OPEN before 15s fallback", states5["FINAL-A"] is PositionState.OPEN and states5["FINAL-B"] is PositionState.OPEN, checks)

    at15 = bridge_f.on_clock(candidate_f.candidate_id, t1 + timedelta(seconds=15, microseconds=1))
    states15 = {p.track_id: p.state for p in at15.positions}
    eval15 = {e.track.track_id: e for e in at15.evaluations}
    check("FINAL-A fallback intent at 15s+1us", eval15["FINAL-A"].intent is not None and eval15["FINAL-A"].intent.reason is ExitReason.FALLBACK, checks)
    check("FINAL-B fallback intent at 15s+1us", eval15["FINAL-B"].intent is not None and eval15["FINAL-B"].intent.reason is ExitReason.FALLBACK, checks)
    check("all fallback lifecycle positions EXIT_PENDING", all(states15[t] is PositionState.EXIT_PENDING for t in LOCKED_TRACK_ORDER), checks)
    check("fallback bridge never fabricates close/fill price", all(p.closed_at is None and p.exit_price_numerator_raw is None and p.exit_price_denominator_raw is None for p in at15.positions), checks)

    # Position lifecycle event reasons are exact and replayable.
    for p in at15.positions:
        events = router_f.lifecycle.list_events(entity_type="POSITION", entity_id=p.paper_position_id)
        exit_events = [e for e in events if e.to_state == "EXIT_PENDING"]
        check(f"{p.track_id} has exactly one persisted EXIT_PENDING event", len(exit_events) == 1, checks)
        check(f"{p.track_id} EXIT_PENDING reason bound to locked intent", exit_events[0].reason.startswith("LOCKED_EXIT_INTENT:"), checks)

    quick_f = conn_f.execute("PRAGMA quick_check").fetchone()[0]
    check("fallback scenario SQLite quick_check", str(quick_f).lower() == "ok", checks)
    conn_f.close()

    print()
    print("-" * 116)
    print("VALIDATION")
    print("-" * 116)
    summary = {
        "persisted_entry_lineage_binding": all(checks[k] for k in [
            "bind persisted FILLED entry -> semantic FINAL-A/B/SENS-C refs",
            "rule reference remains CandidateSignal reference",
            "entry causality lineage retained",
        ]),
        "market_intent_to_exit_pending": checks["all three lifecycle positions EXIT_PENDING"],
        "fallback_intent_to_exit_pending": checks["all fallback lifecycle positions EXIT_PENDING"],
        "no_exit_fill_fabricated": checks["no position CLOSED by bridge"] and checks["no exit price fabricated by bridge"] and checks["fallback bridge never fabricates close/fill price"],
        "restart_safe": checks["restart canonical digest unchanged"] and checks["terminal replay after restart remains idempotent"],
        "replay_idempotent": checks["market replay is lifecycle/orchestrator idempotent"],
        "sqlite_quick_check": checks["market-trigger scenario SQLite quick_check"] and checks["fallback scenario SQLite quick_check"],
    }
    for label, ok in summary.items():
        print(f"{label:<48}: {'PASS' if ok else 'FAIL'}")

    all_ok = all(checks.values()) and all(summary.values())
    print()
    print(f"Locked exit spec fingerprint       : {LOCKED_EXIT_SPEC_FINGERPRINT}")
    print("Production DB touched               : NO")
    print("Live market action                  : NO")
    print("Exit fill / PnL                     : NO")
    print("Wallet/signing/live orders          : NO")
    print("Parameter tuning/reselection        : NO")
    print()
    print(f"RESULT: {'PASS' if all_ok else 'CHECK'}")
    if all_ok:
        print("NEXT: build versioned SOL_NATIVE exit price-impact provider + causal exit-fill execution on EXIT_PENDING positions.")
    return 0 if all_ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
