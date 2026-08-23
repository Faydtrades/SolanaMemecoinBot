from __future__ import annotations

import sqlite3
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from phase4.paper_exit_orchestrator_v0_1 import (
    LOCKED_EXIT_SPECS,
    LOCKED_EXIT_SPEC_FINGERPRINT,
    LOCKED_TRACK_ORDER,
    ExitDeterminismConflict,
    ExitMarketObservation,
    ExitPositionRef,
    ExitReason,
    ExitTrackState,
    PaperExitOrchestratorV01,
    _return_bps,
)

T0 = datetime(2026, 8, 23, 17, 0, 0, tzinfo=timezone.utc)
MINT = "TEST_MINT"
IDENTITY = "SOL_NATIVE"
SIGNAL_SEQ = 100
ENTRY_AT = T0 + timedelta(milliseconds=500)
ENTRY_SEQ = 110


def ok(label: str, cond: bool) -> None:
    if not cond:
        raise AssertionError(label)
    print(f"{label:<66}: PASS")


def refs() -> tuple[ExitPositionRef, ...]:
    out = []
    # Deliberately make actual entry price 2% above the Phase-3 rule reference.
    # The exit trigger must remain anchored to the locked CandidateSignal reference.
    for i, track_id in enumerate(LOCKED_TRACK_ORDER, 1):
        spec = LOCKED_EXIT_SPECS[track_id]
        out.append(
            ExitPositionRef(
                paper_position_id=f"PP-{track_id}",
                paper_order_id=f"PO-{track_id}",
                signal_key="CAND-LOCKED-1",
                mint=MINT,
                track_id=track_id,
                exit_variant=spec.exit_variant,
                price_identity=IDENTITY,
                signal_observed_at=T0,
                signal_ingest_seq=SIGNAL_SEQ,
                rule_reference_price_numerator_raw=100,
                rule_reference_price_denominator_raw=100,
                entry_market_observed_at=ENTRY_AT,
                entry_market_ingest_seq=ENTRY_SEQ,
                open_at=ENTRY_AT,
                entry_price_numerator_raw=102,
                entry_price_denominator_raw=100,
            )
        )
    return tuple(out)


def obs(ms: int, seq: int, num: int, *, identity=IDENTITY, gap=False, mint=MINT, key=None):
    return ExitMarketObservation(
        mint=mint,
        observed_at=T0 + timedelta(milliseconds=ms),
        ingest_seq=seq,
        price_identity=identity,
        price_numerator_raw=num,
        price_denominator_raw=100,
        is_gap_recovery=gap,
        source_event_key=key or f"E-{ms}-{seq}-{num}",
    )


def fresh_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    return conn


def test_locked_specs() -> None:
    ok("locked track order exact", LOCKED_TRACK_ORDER == ("FINAL-A", "FINAL-B", "SENS-C"))
    a, b, c = [LOCKED_EXIT_SPECS[x] for x in LOCKED_TRACK_ORDER]
    ok("FINAL-A exact E3 TP10/T15 lock", a.exit_variant == "E3_TP10_T15" and a.tp_bps == 1000 and a.fallback_ms == 15000)
    ok("FINAL-B exact E4 ACT10/GB03/T15 lock", b.exit_variant == "E4_ACT10_GB03_T15" and b.trail_activation_bps == 1000 and b.trail_giveback_bps == 300 and b.fallback_ms == 15000)
    ok("SENS-C exact sensitivity TP20/T5 lock", c.exit_variant == "SENS_TP20_T5" and c.tp_bps == 2000 and c.fallback_ms == 5000 and c.sensitivity_only)
    ok("no hard-stop field exists in finalist spec", not hasattr(a, "hard_stop_bps") and not hasattr(b, "hard_stop_bps") and not hasattr(c, "hard_stop_bps"))
    print(f"Locked exit spec fingerprint{'':<37}: {LOCKED_EXIT_SPEC_FINGERPRINT}")


def test_return_math() -> None:
    ok("exact Phase-3 return math +10%", _return_bps(100, 100, 110, 100) == 1000)
    ok("exact Phase-3 return math -10%", _return_bps(100, 100, 90, 100) == -1000)
    # 100.005% => +0.5 bps, exact half-away-from-zero => +1 bps.
    ok("half-away-from-zero rounding parity", _return_bps(100000, 1000, 100005, 1000) == 1)


def test_parallel_and_tp() -> None:
    conn = fresh_db()
    eng = PaperExitOrchestratorV01(conn)
    rr = refs()
    tracks = eng.bind_parallel_positions(rr)
    ok("parallel binding returns semantic locked order", tuple(t.track_id for t in tracks) == LOCKED_TRACK_ORDER)
    ok("all tracks start MONITORING", all(t.state is ExitTrackState.MONITORING for t in tracks))

    # Entry-fill point itself cannot trigger an exit even though it is +2% vs signal.
    ev = eng.on_market_observation(rr[0], obs(500, ENTRY_SEQ, 150))
    ok("at-entry-fill observation cannot trigger", ev.intent is None and ev.ignored_reason == "AT_OR_BEFORE_ENTRY_FILL")

    # +9.99% does not TP; exact +10% does.  Trigger remains signal-ref based,
    # not actual 102 entry price (which would make 110 only +7.84%).
    ev = eng.on_market_observation(rr[0], obs(1000, 111, 109))
    ok("FINAL-A below threshold stays monitoring", ev.intent is None)
    ev = eng.on_market_observation(rr[0], obs(1100, 112, 110))
    ok("FINAL-A exact +10% observed point triggers TP", ev.intent is not None and ev.intent.reason is ExitReason.TAKE_PROFIT and ev.intent.rule_return_bps == 1000)
    intent_id = ev.intent.exit_intent_id
    replay = eng.on_market_observation(rr[0], obs(1200, 113, 150))
    ok("terminal exit intent immutable on later market replay", replay.intent is not None and replay.intent.exit_intent_id == intent_id)

    # SENS-C exact +20%.
    evc = eng.on_market_observation(rr[2], obs(1000, 111, 119))
    ok("SENS-C +19% does not TP", evc.intent is None)
    evc = eng.on_market_observation(rr[2], obs(1100, 112, 120))
    ok("SENS-C exact +20% triggers TP", evc.intent is not None and evc.intent.reason is ExitReason.TAKE_PROFIT)


def test_trailing() -> None:
    conn = fresh_db()
    eng = PaperExitOrchestratorV01(conn)
    r = refs()[1]
    eng.bind_position(r)

    ev = eng.on_market_observation(r, obs(1000, 111, 109))
    ok("FINAL-B below +10% is not activated", not ev.track.trail_activated and ev.intent is None)
    ev = eng.on_market_observation(r, obs(1100, 112, 110))
    ok("FINAL-B exact +10% activates trailing", ev.track.trail_activated and ev.track.peak_return_bps == 1000 and ev.intent is None)
    ev = eng.on_market_observation(r, obs(1200, 113, 115))
    ok("FINAL-B higher observed point updates peak", ev.track.peak_return_bps == 1500 and ev.intent is None)
    ev = eng.on_market_observation(r, obs(1300, 114, 113))
    ok("FINAL-B 200bps giveback does not exit", ev.intent is None)
    ev = eng.on_market_observation(r, obs(1400, 115, 112))
    ok("FINAL-B exact 300bps observed giveback triggers TRAIL", ev.intent is not None and ev.intent.reason is ExitReason.TRAIL and ev.intent.trail_peak_return_bps == 1500 and ev.intent.rule_return_bps == 1200)


def test_filters_and_determinism() -> None:
    conn = fresh_db()
    eng = PaperExitOrchestratorV01(conn)
    r = refs()[0]
    eng.bind_position(r)

    ev = eng.on_market_observation(r, obs(1000, 111, 150, mint="OTHER"))
    ok("cross-mint observation ignored", ev.intent is None and ev.ignored_reason == "CROSS_MINT")
    ev = eng.on_market_observation(r, obs(1000, 111, 150, gap=True))
    ok("GAP_RECOVERY cannot trigger TP", ev.intent is None and ev.ignored_reason == "GAP_RECOVERY_NOT_FRESH")
    ev = eng.on_market_observation(r, obs(1100, 112, 150, identity="QUOTE:USDC"))
    ok("price-identity mismatch cannot trigger TP", ev.intent is None and ev.ignored_reason == "PRICE_IDENTITY_MISMATCH")
    ev = eng.on_market_observation(r, obs(1200, 113, 105, key="SAME"))
    ok("fresh same-identity observation accepted", ev.observation_accepted and ev.intent is None)
    digest = eng.canonical_digest()
    ev2 = eng.on_market_observation(r, obs(1200, 113, 105, key="SAME"))
    ok("exact observed_at+ingest_seq replay is idempotent", ev2.ignored_reason == "EXACT_REPLAY" and eng.canonical_digest() == digest)
    try:
        eng.on_market_observation(r, obs(1200, 113, 106, key="DIFFERENT"))
    except ExitDeterminismConflict:
        conflict = True
    else:
        conflict = False
    ok("same key with different content raises determinism conflict", conflict)
    ev3 = eng.on_market_observation(r, obs(1150, 112, 150, key="OLD"))
    ok("out-of-order replay cannot rewind state or trigger", ev3.intent is None and ev3.ignored_reason == "OUT_OF_ORDER_REPLAY")


def test_fallback_boundary() -> None:
    conn = fresh_db()
    eng = PaperExitOrchestratorV01(conn)
    r = refs()[0]
    eng.bind_position(r)

    before = eng.on_clock(r, T0 + timedelta(seconds=15))
    ok("fallback not fired at exact deadline before +1us tie-break", before.intent is None and before.ignored_reason == "FALLBACK_NOT_DUE")
    # Exact-deadline market point remains eligible and can TP.
    ev = eng.on_market_observation(r, obs(15000, 200, 110))
    ok("exact fallback-deadline market point remains TP-eligible", ev.intent is not None and ev.intent.reason is ExitReason.TAKE_PROFIT)

    # Separate track: no TP, timer emits FALLBACK without inventing an exit fill/price.
    conn2 = fresh_db()
    eng2 = PaperExitOrchestratorV01(conn2)
    r2 = refs()[0]
    eng2.bind_position(r2)
    eng2.on_market_observation(r2, obs(14000, 190, 95))
    due = eng2.on_clock(r2, T0 + timedelta(seconds=15, microseconds=1))
    ok("FINAL-A fallback fires deterministically at 15s + 1us", due.intent is not None and due.intent.reason is ExitReason.FALLBACK)
    ok("fallback intent does not fabricate trigger fill price", due.intent.trigger_price_numerator_raw is None and due.intent.rule_return_bps is None)
    ok("fallback retains last fresh rule mark only for audit", due.intent.last_fresh_return_bps == -500)

    # Late +50% observation cannot override fallback.
    late = eng2.on_market_observation(r2, obs(15001, 191, 150))
    ok("late market point cannot retroactively defeat fallback", late.intent is not None and late.intent.reason is ExitReason.FALLBACK)

    # SENS-C has its own 5s clock.
    conn3 = fresh_db()
    eng3 = PaperExitOrchestratorV01(conn3)
    rc = refs()[2]
    eng3.bind_position(rc)
    due_c = eng3.on_clock(rc, T0 + timedelta(seconds=5, microseconds=1))
    ok("SENS-C fallback is separately locked to 5s + 1us", due_c.intent is not None and due_c.intent.reason is ExitReason.FALLBACK)


def test_restart_persistence() -> None:
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "exit_state.sqlite3"
        rr = refs()
        r = rr[1]

        conn1 = sqlite3.connect(path)
        conn1.row_factory = sqlite3.Row
        eng1 = PaperExitOrchestratorV01(conn1)
        eng1.bind_parallel_positions(rr)
        eng1.on_market_observation(r, obs(1000, 111, 110))  # activate +10
        eng1.on_market_observation(r, obs(1200, 112, 116))  # peak +16
        digest_before = eng1.canonical_digest()
        conn1.close()

        conn2 = sqlite3.connect(path)
        conn2.row_factory = sqlite3.Row
        eng2 = PaperExitOrchestratorV01(conn2)
        restored = eng2.get_track(r.paper_position_id)
        ok("restart restores trailing activation", restored is not None and restored.trail_activated)
        ok("restart restores exact trailing peak", restored is not None and restored.peak_return_bps == 1600)
        ok("restart canonical digest unchanged before new event", eng2.canonical_digest() == digest_before)
        ev = eng2.on_market_observation(r, obs(1400, 113, 113))  # +13 => exact 300 giveback
        ok("restart continues trail and triggers from persisted peak", ev.intent is not None and ev.intent.reason is ExitReason.TRAIL)
        digest_terminal = eng2.canonical_digest()
        conn2.close()

        conn3 = sqlite3.connect(path)
        conn3.row_factory = sqlite3.Row
        eng3 = PaperExitOrchestratorV01(conn3)
        intent = eng3.get_intent_for_position(r.paper_position_id)
        ok("terminal exit intent persists across second restart", intent is not None and intent.reason is ExitReason.TRAIL)
        ok("terminal restart digest stable", eng3.canonical_digest() == digest_terminal)
        qc = conn3.execute("PRAGMA quick_check").fetchone()[0]
        ok("isolated exit-state SQLite quick_check", qc == "ok")
        conn3.close()


def main() -> int:
    print("=" * 112)
    print("PHASE 4.4A - DETERMINISTIC PERSISTENT EXIT-TRACK ORCHESTRATOR SELF-TEST v0.1")
    print("=" * 112)
    print(f"Project root                       : {PROJECT_ROOT}")
    print("Production DB opened               : NO")
    print("Collector / network / RPC          : NO")
    print("Wallet / signing / live orders     : NO")
    print("Paper position mutation            : NO")
    print("Exit fill / PnL fabrication        : NO")
    print("Parameter tuning / reselection     : NO")
    print("Rule return anchor                 : locked Phase-3 CandidateSignal reference")
    print("Rule clock anchor                  : locked Phase-3 signal_observed_at")
    print("Post-entry causality gate          : observations strictly after actual entry-fill observation")
    print("Exact deadline policy              : market inclusive; fallback timer at deadline + 1us")
    print()

    test_locked_specs()
    test_return_math()
    test_parallel_and_tp()
    test_trailing()
    test_filters_and_determinism()
    test_fallback_boundary()
    test_restart_persistence()

    print()
    print("-" * 112)
    print("VALIDATION")
    print("-" * 112)
    checks = (
        "locked_finalists_exact",
        "phase3_return_math_exact",
        "candidate_reference_anchor_preserved",
        "actual_entry_fill_causality_gate",
        "final_a_tp_exact_boundary",
        "final_b_activation_peak_giveback",
        "sens_c_separate_tp_and_clock",
        "gap_identity_crossmint_guards",
        "exact_replay_idempotence",
        "determinism_conflict_guard",
        "deadline_inclusive_timer_precedence",
        "fallback_no_fabricated_fill_price",
        "parallel_track_semantic_order",
        "restart_trailing_state_persisted",
        "restart_terminal_intent_persisted",
        "isolated_sqlite_quick_check",
    )
    for name in checks:
        print(f"{name:<52}: PASS")
    print()
    print(f"Locked spec fingerprint             : {LOCKED_EXIT_SPEC_FINGERPRINT}")
    print("Production DB touched               : NO")
    print("Live market action                  : NO")
    print("Wallet/signing/live orders          : NO")
    print("Parameter tuning/reselection        : NO")
    print()
    print("RESULT: PASS")
    print("NEXT: bind persisted OPEN paper positions + lifecycle EXIT_PENDING to this engine, then build causal simulated exit-fill execution with locked cost/latency/price-impact model.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
