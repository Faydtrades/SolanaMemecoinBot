from __future__ import annotations

import sqlite3
import sys
import tempfile
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
for root in (PROJECT_ROOT / "src", PROJECT_ROOT / "scripts"):
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))

import phase4_continuous_paper_runner_selftest_v0_1 as fixtures  # noqa: E402
from phase4.paper_continuous_runner_v0_1 import SourceCursorConflict  # noqa: E402
from phase4.paper_continuous_runner_v0_2 import (  # noqa: E402
    RUNNER_SPEC_FINGERPRINT,
    ContinuousPaperRunnerV02,
)
from phase4.paper_cost_baseline_v0_1 import P4_COST_BASELINE_0001  # noqa: E402
from phase4.paper_cost_model_v0_2 import PaperCostModelV02  # noqa: E402
from phase4.paper_entry_execution_deadline_v0_1 import (  # noqa: E402
    ELIGIBILITY_WINDOW_US,
    LOCKED_EXIT_SPEC_FINGERPRINT,
    MODEL_FINGERPRINT,
    MODEL_ID,
    deadline_for,
)
from phase4.paper_entry_router_v0_1 import (  # noqa: E402
    CandidateSignalEnvelope,
    EntryMarketObservation,
    EntryRouteState,
    RouteDeterminismConflict,
    connect_entry_router_db,
)
from phase4.paper_entry_router_v0_2 import (  # noqa: E402
    ALL_TRACK_ENTRY_EXECUTION_DEADLINES_EXPIRED,
    ENTRY_EXECUTION_DEADLINE_EXPIRED,
    PaperEntryRouterV02,
)
from phase4.paper_exit_orchestrator_v0_1 import (  # noqa: E402
    LOCKED_EXIT_SPEC_FINGERPRINT as ACTUAL_EXIT_FINGERPRINT,
)
from phase4.paper_trade_accounting_v0_1 import (  # noqa: E402
    build_report as build_accounting_report,
)


BASE = datetime(2026, 8, 25, 12, 0, 0, tzinfo=timezone.utc)


def candidate(signal: str = "SIGNAL") -> CandidateSignalEnvelope:
    return CandidateSignalEnvelope(
        candidate_id=signal,
        mint=f"MINT-{signal}",
        strategy_version="v1.1",
        parameter_set_id="LOCKED-CONTROL",
        signal_observed_at=BASE,
        signal_ingest_seq=1,
        price_identity="SOL_NATIVE",
        price_numerator_raw=100_000_000_000,
        price_denominator_raw=1_000_000_000,
    )


def observation(item: CandidateSignalEnvelope, offset_us: int, *, seq: int = 2, bps: int = 0):
    return EntryMarketObservation(
        mint=item.mint,
        observed_at=BASE + timedelta(microseconds=offset_us),
        ingest_seq=seq,
        price_identity=item.price_identity,
        price_numerator_raw=item.price_numerator_raw * (10_000 + bps) // 10_000,
        price_denominator_raw=item.price_denominator_raw,
        source_event_key=f"OBS:{offset_us}:{seq}:{bps}",
    )


def router() -> tuple[sqlite3.Connection, PaperEntryRouterV02]:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    return conn, PaperEntryRouterV02(
        conn, PaperCostModelV02(P4_COST_BASELINE_0001)
    )


def route_at(offset_us: int):
    conn, active = router()
    item = candidate(str(offset_us))
    result = active.on_market_observation(
        candidate=item,
        observation=observation(item, offset_us),
        price_impact_bps=0,
    )
    decisions = {row.track_id: row for row in active.list_track_decisions()}
    return conn, active, result, decisions


def states(result) -> dict[str, str]:
    return {order.track_id: order.state.value for order in result.orders}


def assert_case(name: str, condition: bool) -> None:
    if not condition:
        raise AssertionError(name)
    print(f"{name}: PASS")


def main() -> int:
    # A/B: all tracks remain eligible just before and exactly at SENS-C's bound.
    for label, offset in (("A_FAST_FILL_2S", 2_000_000), ("B_EXACT_5S", 5_000_000)):
        conn, _router, result, _decisions = route_at(offset)
        assert_case(label, set(states(result).values()) == {"FILLED"})
        conn.close()

    # C/D: SENS-C expires at +5s+1us while A/B remain independently eligible.
    for label, offset in (("C_5S_PLUS_1US", 5_000_001), ("D_FILL_10S", 10_000_000)):
        conn, _router, result, decisions = route_at(offset)
        assert_case(
            label,
            states(result) == {
                "FINAL-A": "FILLED", "FINAL-B": "FILLED", "SENS-C": "REJECTED"
            }
            and decisions["SENS-C"].reason == ENTRY_EXECUTION_DEADLINE_EXPIRED,
        )
        conn.close()

    # E/F/G: exact +15s is inclusive; +1us and the production +36s case reject all.
    conn, _router, result, _decisions = route_at(15_000_000)
    assert_case(
        "E_EXACT_15S",
        states(result) == {
            "FINAL-A": "FILLED", "FINAL-B": "FILLED", "SENS-C": "REJECTED"
        },
    )
    conn.close()

    # Q: candidates and mints retain independent deadline lineages.
    conn, active = router()
    first = candidate("ISOLATED-A")
    second = replace(
        candidate("ISOLATED-B"),
        signal_observed_at=BASE + timedelta(seconds=100),
        signal_ingest_seq=100,
    )
    first_route = active.route_candidate(first).route
    second_route = active.route_candidate(second).route
    active.on_clock(BASE + timedelta(seconds=15, microseconds=1))
    second_fill = active.on_market_observation(
        candidate=second,
        observation=EntryMarketObservation(
            mint=second.mint,
            observed_at=BASE + timedelta(seconds=102),
            ingest_seq=101,
            price_identity=second.price_identity,
            price_numerator_raw=second.price_numerator_raw,
            price_denominator_raw=second.price_denominator_raw,
            source_event_key="ISOLATED-B-FILL",
        ),
        price_impact_bps=0,
    )
    assert_case(
        "Q_MULTI_CANDIDATE_ISOLATION",
        active.get_route(first_route.route_id).state is EntryRouteState.REJECTED
        and second_fill.route.route_id == second_route.route_id
        and second_fill.route.state is EntryRouteState.FILLED,
    )
    conn.close()
    for label, offset in (("F_15S_PLUS_1US", 15_000_001), ("G_36S_LATE_FILL", 36_512_000)):
        conn, _router, result, decisions = route_at(offset)
        assert_case(
            label,
            result.route.state is EntryRouteState.REJECTED
            and result.route.state_reason == ALL_TRACK_ENTRY_EXECUTION_DEADLINES_EXPIRED
            and all(row.state == "REJECTED" for row in decisions.values())
            and int(conn.execute("SELECT COUNT(*) FROM paper_positions").fetchone()[0]) == 0,
        )
    conn.close()

    # Adversarial crash boundaries: durable terminal track writes must heal the
    # shared route exactly after restart, without false expiry or resurrection.
    for label, offset, bps, expected_state, expected_reason in (
        (
            "Y_CRASH_AFTER_FINAL_FILL_HEALS_ROUTE",
            2_000_000,
            0,
            EntryRouteState.FILLED,
            "PAPER_ENTRY_FILLED_WITH_TRACK_DEADLINE",
        ),
        (
            "Y_CRASH_AFTER_FINAL_SLIPPAGE_REJECTION_HEALS_ROUTE",
            10_000_000,
            2_000,
            EntryRouteState.REJECTED,
            "ENTRY_SLIPPAGE_CAP_EXCEEDED",
        ),
    ):
        conn, active = router()
        item = candidate(label)
        active.route_candidate(item)
        if bps:
            active.on_clock(BASE + timedelta(seconds=5, microseconds=1))
        finish = active._finish_terminal_route

        def fail_after_terminal(route_id: str, *, _active=active, _finish=finish) -> None:
            rows = _active._deadline_rows(route_id)
            if rows and not any(str(row["state"]) == "PENDING" for row in rows):
                raise RuntimeError("INJECTED_AFTER_FINAL_TRACK_TERMINAL")
            _finish(route_id)

        active._finish_terminal_route = fail_after_terminal  # type: ignore[method-assign]
        try:
            active.on_market_observation(
                candidate=item,
                observation=observation(item, offset, bps=bps),
                price_impact_bps=0,
            )
        except RuntimeError as exc:
            if str(exc) != "INJECTED_AFTER_FINAL_TRACK_TERMINAL":
                raise
        else:
            raise AssertionError(f"{label}: failure injection did not fire")
        assert active.get_route(active.route_candidate(item).route.route_id).state is EntryRouteState.ENTRY_PENDING
        healed = PaperEntryRouterV02(conn, PaperCostModelV02(P4_COST_BASELINE_0001))
        terminal = healed.get_route(healed.route_candidate(item).route.route_id)
        assert_case(
            label,
            terminal is not None
            and terminal.state is expected_state
            and terminal.state_reason == expected_reason
            and (
                expected_state is not EntryRouteState.FILLED
                or terminal.total_entry_explicit_cost_lamports is not None
            ),
        )
        conn.close()

    conn, active = router()
    item = candidate("Y-EXPIRY-CRASH")
    active.route_candidate(item)
    finish = active._finish_terminal_route

    def fail_after_all_expired(route_id: str) -> None:
        rows = active._deadline_rows(route_id)
        if rows and not any(str(row["state"]) == "PENDING" for row in rows):
            raise RuntimeError("INJECTED_AFTER_FINAL_EXPIRY")
        finish(route_id)

    active._finish_terminal_route = fail_after_all_expired  # type: ignore[method-assign]
    try:
        active.on_clock(BASE + timedelta(seconds=15, microseconds=1))
    except RuntimeError as exc:
        if str(exc) != "INJECTED_AFTER_FINAL_EXPIRY":
            raise
    else:
        raise AssertionError("Y_CRASH_AFTER_FINAL_EXPIRY_HEALS_ROUTE: injection did not fire")
    healed = PaperEntryRouterV02(conn, PaperCostModelV02(P4_COST_BASELINE_0001))
    terminal = healed.get_route(healed.route_candidate(item).route.route_id)
    assert_case(
        "Y_CRASH_AFTER_FINAL_EXPIRY_HEALS_ROUTE",
        terminal is not None
        and terminal.state is EntryRouteState.REJECTED
        and terminal.state_reason == ALL_TRACK_ENTRY_EXECUTION_DEADLINES_EXPIRED
        and int(conn.execute("SELECT COUNT(*) FROM paper_positions").fetchone()[0]) == 0,
    )
    conn.close()

    conn, active = router()
    item = candidate("Y-CORRUPT-TERMINAL")
    result = active.on_market_observation(
        candidate=item,
        observation=observation(item, 2_000_000),
        price_impact_bps=0,
    )
    with conn:
        conn.execute(
            "UPDATE paper_orders SET state='REJECTED' WHERE paper_order_id=?",
            (result.orders[0].paper_order_id,),
        )
    corrupt_rejected = False
    try:
        PaperEntryRouterV02(conn, PaperCostModelV02(P4_COST_BASELINE_0001))
    except RouteDeterminismConflict:
        corrupt_rejected = True
    assert_case("Y_CORRUPT_TERMINAL_STATE_FAILS_CLOSED", corrupt_rejected)
    conn.close()

    # H/I: silent clocks use canonical +1us expiry instants, even when called late.
    conn, active = router()
    item = candidate("CLOCK")
    routed = active.route_candidate(item)
    active.on_clock(BASE + timedelta(seconds=5, microseconds=1))
    first = {row.track_id: row for row in active.list_track_decisions()}
    assert_case(
        "H_SILENT_5S_CLOCK",
        first["SENS-C"].terminal_at == BASE + timedelta(seconds=5, microseconds=1)
        and first["FINAL-A"].state == "PENDING",
    )
    active.on_clock(BASE + timedelta(seconds=30))
    final = active.get_route(routed.route.route_id)
    assert_case(
        "I_SILENT_15S_CLOCK",
        final is not None
        and final.updated_at == BASE + timedelta(seconds=15, microseconds=1)
        and final.state_reason == ALL_TRACK_ENTRY_EXECUTION_DEADLINES_EXPIRED,
    )
    conn.close()

    # I/V: restart during the mixed pending state, then fill only A/B at +10s.
    with tempfile.TemporaryDirectory(prefix="p4-t012-restart-") as raw:
        path = Path(raw) / "paper.sqlite3"
        conn = connect_entry_router_db(path)
        active = PaperEntryRouterV02(conn, PaperCostModelV02(P4_COST_BASELINE_0001))
        item = candidate("RESTART")
        active.route_candidate(item)
        digest_before = active.canonical_digest()
        conn.close()
        conn = connect_entry_router_db(path)
        active = PaperEntryRouterV02(conn, PaperCostModelV02(P4_COST_BASELINE_0001))
        assert_case("J_RESTART_BEFORE_5S", active.canonical_digest() == digest_before)
        active.on_clock(BASE + timedelta(seconds=5, microseconds=1))
        digest_mid = active.canonical_digest()
        conn.close()
        conn = connect_entry_router_db(path)
        active = PaperEntryRouterV02(conn, PaperCostModelV02(P4_COST_BASELINE_0001))
        assert_case("K_RESTART_MIXED_PENDING", active.canonical_digest() == digest_mid)
        result = active.on_market_observation(
            candidate=item,
            observation=observation(item, 10_000_000),
            price_impact_bps=0,
        )
        assert_case(
            "I_RESTART_THEN_VALID_10S_FILL",
            states(result) == {
                "FINAL-A": "FILLED", "FINAL-B": "FILLED", "SENS-C": "REJECTED"
            },
        )
        digest_terminal = active.canonical_digest()
        conn.close()
        conn = connect_entry_router_db(path)
        active = PaperEntryRouterV02(conn, PaperCostModelV02(P4_COST_BASELINE_0001))
        assert_case("V_MIXED_STATE_RESTART", active.canonical_digest() == digest_terminal)
        active.on_clock(BASE + timedelta(seconds=60))
        assert_case("W_TERMINAL_REPLAY_DIGEST", active.canonical_digest() == digest_terminal)
        conn.close()

    # J/K: restart after all-expiry and a later market replay can never fill.
    with tempfile.TemporaryDirectory(prefix="p4-t012-all-expired-") as raw:
        path = Path(raw) / "paper.sqlite3"
        conn = connect_entry_router_db(path)
        active = PaperEntryRouterV02(conn, PaperCostModelV02(P4_COST_BASELINE_0001))
        item = candidate("ALL-EXPIRED")
        active.route_candidate(item)
        active.on_clock(BASE + timedelta(seconds=15, microseconds=1))
        terminal_digest = active.canonical_digest()
        conn.close()
        conn = connect_entry_router_db(path)
        active = PaperEntryRouterV02(conn, PaperCostModelV02(P4_COST_BASELINE_0001))
        assert_case("J_RESTART_ALL_EXPIRED", active.canonical_digest() == terminal_digest)
        late = active.on_market_observation(
            candidate=item,
            observation=observation(item, 36_512_000),
            price_impact_bps=0,
        )
        assert_case(
            "K_LATE_REPLAY_NEVER_FILLS",
            active.canonical_digest() == terminal_digest
            and late.route.state is EntryRouteState.REJECTED
            and int(conn.execute("SELECT COUNT(*) FROM paper_positions").fetchone()[0]) == 0,
        )
        conn.close()

    # L: partial temporal expiry followed by genuine slippage rejection.
    conn, active = router()
    item = candidate("SLIPPAGE")
    active.route_candidate(item)
    active.on_clock(BASE + timedelta(seconds=5, microseconds=1))
    slipped = active.on_market_observation(
        candidate=item,
        observation=observation(item, 10_000_000, bps=2_000),
        price_impact_bps=0,
    )
    slippage_reasons = {order.track_id: order.state_reason for order in slipped.orders}
    assert_case(
        "L_PARTIAL_EXPIRY_THEN_SLIPPAGE",
        slipped.route.state is EntryRouteState.REJECTED
        and slippage_reasons == {
            "FINAL-A": "ENTRY_SLIPPAGE_CAP_EXCEEDED",
            "FINAL-B": "ENTRY_SLIPPAGE_CAP_EXCEEDED",
            "SENS-C": ENTRY_EXECUTION_DEADLINE_EXPIRED,
        }
        and int(conn.execute("SELECT COUNT(*) FROM paper_positions").fetchone()[0]) == 0,
    )
    conn.close()

    # M/N: causal filters never fill or reset a deadline; the clock remains authoritative.
    conn, active = router()
    item = candidate("FILTERS")
    active.route_candidate(item)
    pre_ready = active.on_market_observation(
        candidate=item,
        observation=observation(item, 499_999),
        price_impact_bps=0,
    )
    wrong_mint = EntryMarketObservation(
        mint="OTHER-MINT", observed_at=BASE + timedelta(seconds=2), ingest_seq=3,
        price_identity="SOL_NATIVE", price_numerator_raw=item.price_numerator_raw,
        price_denominator_raw=item.price_denominator_raw, source_event_key="WRONG-MINT",
    )
    active.on_market_observation(candidate=item, observation=wrong_mint, price_impact_bps=0)
    gap = EntryMarketObservation(
        mint=item.mint, observed_at=BASE + timedelta(seconds=3), ingest_seq=4,
        price_identity="SOL_NATIVE", price_numerator_raw=item.price_numerator_raw,
        price_denominator_raw=item.price_denominator_raw, is_gap_recovery=True,
        source_event_key="GAP",
    )
    active.on_market_observation(candidate=item, observation=gap, price_impact_bps=0)
    wrong_identity = EntryMarketObservation(
        mint=item.mint, observed_at=BASE + timedelta(seconds=4), ingest_seq=5,
        price_identity="USDC_QUOTE", price_numerator_raw=item.price_numerator_raw,
        price_denominator_raw=item.price_denominator_raw, source_event_key="WRONG-IDENTITY",
    )
    active.on_market_observation(
        candidate=item, observation=wrong_identity, price_impact_bps=0
    )
    assert_case(
        "M_PRE_READY_NO_FILL_NO_RESET",
        set(states(pre_ready).values()) == {"ENTRY_PENDING"}
        and all(
            row.signal_observed_at == BASE for row in active.list_track_decisions()
        ),
    )
    active.on_clock(BASE + timedelta(seconds=15, microseconds=1))
    assert_case(
        "N_CAUSAL_FILTERS_DO_NOT_EXTEND_DEADLINES",
        active.diagnostics()["expiry_events"] == 3,
    )
    conn.close()

    # R/K: stable identities, replay, and equal-instant ordering cannot cross the edge.
    conn, active = router()
    item = candidate("ORDERING")
    active.route_candidate(item)
    active.on_clock(BASE + timedelta(seconds=15))
    exact = active.on_market_observation(
        candidate=item,
        observation=observation(item, 15_000_000),
        price_impact_bps=0,
    )
    digest = active.canonical_digest()
    replay = active.on_market_observation(
        candidate=item,
        observation=observation(item, 15_000_000),
        price_impact_bps=0,
    )
    ids = tuple(order.paper_order_id for order in exact.orders)
    assert_case("R_CLOCK_AT_DEADLINE_DOES_NOT_EXPIRE", exact.route.state is EntryRouteState.FILLED)
    assert_case("R_EXACT_MARKET_REPLAY", active.canonical_digest() == digest)
    assert_case(
        "R_STABLE_ROUTE_AND_ORDER_IDENTITIES",
        tuple(order.paper_order_id for order in replay.orders) == ids
        and replay.route.route_id == exact.route.route_id,
    )
    active.on_clock(BASE + timedelta(seconds=15, microseconds=1))
    assert_case("X_FILLED_ROUTE_CANNOT_FALSE_EXPIRE", active.canonical_digest() == digest)
    conn.close()

    # P/Q/R: lower-cursor failure, all-expired lifecycle, and strategy audit preservation.
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    runner = ContinuousPaperRunnerV02(conn, source_identity="T012", wall_clock=lambda: BASE)
    runner.process_source_item(fixtures.candidate_item(
        0, signal="RUNNER-EXPIRE", mint="MINT-RUNNER", role="CONTROL", at=BASE, ingest_seq=1
    ))
    runner.process_source_item(fixtures.clock_item(2, at=BASE + timedelta(seconds=36)))
    before = runner.canonical_digest()
    try:
        runner.process_source_item(fixtures.clock_item(1, at=BASE + timedelta(seconds=40)))
        lower_cursor_rejected = False
    except SourceCursorConflict:
        lower_cursor_rejected = runner.canonical_digest() == before
    assert_case("P_LOWER_CURSOR_FAIL_CLOSED", lower_cursor_rejected)
    assert_case(
        "Q_ALL_EXPIRED_NO_POSITION",
        int(conn.execute("SELECT COUNT(*) FROM paper_positions").fetchone()[0]) == 0,
    )
    terminal = conn.execute(
        "SELECT trade_or_skip,decision_reason FROM paper_terminal_trade_decisions"
    ).fetchone()
    assert_case(
        "R_CANDIDATE_AUDIT_PRESERVED",
        terminal is not None
        and tuple(terminal) == ("SKIP", ALL_TRACK_ENTRY_EXECUTION_DEADLINES_EXPIRED),
    )
    conn.close()

    # S: exact mixed durable state and V: pre-open points cannot affect exits.
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    runner = ContinuousPaperRunnerV02(conn, source_identity="T012-MIXED", wall_clock=lambda: BASE)
    runner.process_source_item(fixtures.candidate_item(
        0, signal="RUNNER-MIXED", mint="MINT-MIXED", role="CONTROL", at=BASE, ingest_seq=1
    ))
    runner.process_source_item(fixtures.market_item(
        1, mint="MINT-MIXED", at=BASE + timedelta(microseconds=50_000), ingest_seq=2,
        return_bps=2_000,
    ))
    runner.process_source_item(fixtures.clock_item(2, at=BASE + timedelta(seconds=5, microseconds=1)))
    runner.process_source_item(fixtures.market_item(
        3, mint="MINT-MIXED", at=BASE + timedelta(seconds=10), ingest_seq=3,
        return_bps=0,
    ))
    mixed = {
        str(row["track_id"]): str(row["state"])
        for row in conn.execute("SELECT track_id,state FROM paper_orders").fetchall()
    }
    assert_case(
        "S_TRACK_INDEPENDENT_MIXED_STATE",
        mixed == {"FINAL-A": "FILLED", "FINAL-B": "FILLED", "SENS-C": "REJECTED"},
    )
    assert_case(
        "V_PREOPEN_OBSERVATION_NOT_EXIT_INPUT",
        int(conn.execute("SELECT COUNT(*) FROM paper_exit_intents").fetchone()[0]) == 0,
    )
    conn.close()

    # E's second half: a later ingest at the same exact T may request an exit at T,
    # but the exit execution remains pending until a later causal observation.
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    runner = ContinuousPaperRunnerV02(conn, source_identity="T012-EXACT", wall_clock=lambda: BASE)
    runner.process_source_item(fixtures.candidate_item(
        0, signal="RUNNER-EXACT", mint="MINT-EXACT", role="CONTROL", at=BASE, ingest_seq=1
    ))
    runner.process_source_item(fixtures.market_item(
        1, mint="MINT-EXACT", at=BASE + timedelta(seconds=15), ingest_seq=2,
        return_bps=0,
    ))
    runner.process_source_item(fixtures.market_item(
        2, mint="MINT-EXACT", at=BASE + timedelta(seconds=15), ingest_seq=3,
        return_bps=1_000,
    ))
    exact_intent = conn.execute(
        "SELECT requested_at FROM paper_exit_intents i JOIN paper_positions p "
        "ON p.paper_position_id=i.paper_position_id WHERE p.track_id='FINAL-A'"
    ).fetchone()
    exact_state = conn.execute(
        "SELECT state FROM paper_positions WHERE track_id='FINAL-A'"
    ).fetchone()
    execution_route = conn.execute(
        "SELECT xr.requested_at,xr.execution_ready_at,xr.state FROM paper_exit_execution_routes xr "
        "JOIN paper_positions p ON p.paper_position_id=xr.paper_position_id "
        "WHERE p.track_id='FINAL-A'"
    ).fetchone()
    assert_case(
        "O_EXACT_OPEN_THEN_EXIT_INTENT",
        exact_intent is not None
        and datetime.fromisoformat(str(exact_intent[0])) == BASE + timedelta(seconds=15)
        and str(exact_state[0]) == "EXIT_PENDING",
    )
    assert_case(
        "O_EXIT_FILL_REMAINS_LATENCY_GATED",
        execution_route is not None
        and datetime.fromisoformat(str(execution_route[0])) == BASE + timedelta(seconds=15)
        and datetime.fromisoformat(str(execution_route[1])) == BASE + timedelta(seconds=15, milliseconds=500)
        and str(execution_route[2]) == "WAITING",
    )
    accounting = build_accounting_report(conn)
    assert_case(
        "X_NO_CROSS_TRACK_PNL_SUMMATION",
        accounting.cross_track_portfolio_aggregation_status
        == "PROHIBITED_ALTERNATIVE_TRACKS_REPORTED_SEPARATELY",
    )
    conn.close()

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    runner = ContinuousPaperRunnerV02(
        conn, source_identity="T012-EXACT-SENS", wall_clock=lambda: BASE
    )
    runner.process_source_item(fixtures.candidate_item(
        0, signal="RUNNER-EXACT-SENS", mint="MINT-EXACT-SENS",
        role="CONTROL", at=BASE, ingest_seq=1,
    ))
    runner.process_source_item(fixtures.market_item(
        1, mint="MINT-EXACT-SENS", at=BASE + timedelta(seconds=5), ingest_seq=2,
        return_bps=0,
    ))
    runner.process_source_item(fixtures.market_item(
        2, mint="MINT-EXACT-SENS", at=BASE + timedelta(seconds=5), ingest_seq=3,
        return_bps=2_000,
    ))
    sens = conn.execute(
        "SELECT p.open_at,p.state,xr.requested_at,xr.execution_ready_at,xr.state "
        "FROM paper_positions p JOIN paper_exit_execution_routes xr "
        "ON xr.paper_position_id=p.paper_position_id WHERE p.track_id='SENS-C'"
    ).fetchone()
    assert_case(
        "O_SENS_EXACT_5S_OPEN_THEN_LATENCY_GATED_EXIT",
        sens is not None
        and datetime.fromisoformat(str(sens[0])) == BASE + timedelta(seconds=5)
        and str(sens[1]) == "EXIT_PENDING"
        and datetime.fromisoformat(str(sens[2])) == BASE + timedelta(seconds=5)
        and datetime.fromisoformat(str(sens[3])) == BASE + timedelta(seconds=5, milliseconds=500)
        and str(sens[4]) == "WAITING",
    )
    conn.close()

    # W: the companion audit has one durable identity per route/track.
    conn, active = router()
    item = candidate("AUDIT-UNIQUE")
    active.route_candidate(item)
    active.route_candidate(item)
    count = int(conn.execute(
        "SELECT COUNT(*) FROM paper_entry_execution_deadlines_v0_1"
    ).fetchone()[0])
    distinct_count = int(conn.execute(
        "SELECT COUNT(*) FROM (SELECT route_id,track_id "
        "FROM paper_entry_execution_deadlines_v0_1 GROUP BY route_id,track_id)"
    ).fetchone()[0])
    assert_case("W_NO_DUPLICATE_AUDIT_IDENTITIES", count == distinct_count == 3)
    conn.close()

    assert_case(
        "T_LOCKED_IDENTITIES_UNCHANGED",
        MODEL_ID == "P4-PAPER-ENTRY-EXECUTION-DEADLINE-0001"
        and MODEL_FINGERPRINT
        and RUNNER_SPEC_FINGERPRINT
        and ELIGIBILITY_WINDOW_US == {
            "FINAL-A": 15_000_000, "FINAL-B": 15_000_000, "SENS-C": 5_000_000
        },
    )
    assert_case(
        "U_SIGNAL_ANCHORED_FALLBACK_UNCHANGED",
        LOCKED_EXIT_SPEC_FINGERPRINT == ACTUAL_EXIT_FINGERPRINT
        == "0bde5378f4ff2b63c6a96ad64140840ef8b2770c3be772a7aa71f8bdeb070129"
        and deadline_for("FINAL-A", BASE).eligible_through_at == BASE + timedelta(seconds=15),
    )
    print("RESULT: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
