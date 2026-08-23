from __future__ import annotations

import json
import shutil
import sqlite3
import sys
import tempfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from phase4.paper_continuous_runner_v0_1 import (  # noqa: E402
    MODEL_ID,
    RUNNER_SPEC_FINGERPRINT,
    CandidateEvaluationEvent,
    ClockTickEvent,
    ContinuousPaperRunnerV01,
    ContinuousSourceItem,
    LOCKED_PARAMETER_SET_BY_ROLE,
    MarketObservationEvent,
    RuntimeBindingConflict,
    SourceCursorConflict,
    TerminalSkipEvaluationEvent,
)
from phase4.paper_entry_router_v0_1 import CandidateSignalEnvelope  # noqa: E402
from phase4.paper_runtime_observability_v0_1 import (  # noqa: E402
    StrategyEvaluationAuditInput,
    build_report,
    canonical_report_digest,
)
from phase4.paper_trade_accounting_v0_1 import load_completed_trades  # noqa: E402


UTC = timezone.utc
BASE = datetime(2026, 8, 23, 20, 0, 0, tzinfo=UTC)
SOURCE_IDENTITY = "SELFTEST:SYNTHETIC-CONTINUOUS-V0.1"
PRICE_DEN = 100_000_000_000
BASE_PRICE_NUM = 10_000_000_000


@dataclass(frozen=True)
class ScenarioEvidence:
    checks: dict[str, bool]
    digest: str
    report_digest: str
    quick_check: str
    final_cursor: int


class FixedClock:
    def __call__(self) -> datetime:
        return BASE - timedelta(seconds=1)


class FailOnceAfterProcessing:
    def __init__(self, cursor: int) -> None:
        self.cursor = cursor
        self.fired = False

    def __call__(self, item: ContinuousSourceItem, stage: str) -> None:
        if (
            item.cursor == self.cursor
            and stage == "after_event_processing_before_cursor_commit"
            and not self.fired
        ):
            self.fired = True
            raise RuntimeError("INJECTED_AFTER_PROCESSING_BEFORE_CURSOR_COMMIT")


def us(value: datetime) -> int:
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    delta = value - epoch
    return (
        delta.days * 86_400_000_000
        + delta.seconds * 1_000_000
        + delta.microseconds
    )


def candidate_item(
    cursor: int,
    *,
    signal: str,
    mint: str,
    role: str,
    at: datetime,
    ingest_seq: int,
    audit_snapshot: dict[str, object] | None = None,
) -> ContinuousSourceItem:
    params = LOCKED_PARAMETER_SET_BY_ROLE[role]
    evaluation = StrategyEvaluationAuditInput(
        run_id=f"RUN-{signal}",
        role=role,
        mint=mint,
        strategy_version="v1.1",
        parameter_set_id=params,
        ingest_seq=ingest_seq,
        evaluated_at_us=us(at),
        current_state="CANDIDATE_EMITTED",
        transition_occurred=True,
        reason_code="CANDIDATE_EMITTED",
        filters_passed=("IMPULSE", "PULLBACK", "BUYER_RESPONSE", "RECLAIM"),
        filters_failed=(),
        signal_type="FIRST_PULLBACK",
        candidate_signal_id=signal,
        audit_snapshot=(
            {"source": "continuous-selftest", "signal": signal}
            if audit_snapshot is None
            else audit_snapshot
        ),
    )
    candidate = CandidateSignalEnvelope(
        candidate_id=signal,
        mint=mint,
        strategy_version="v1.1",
        parameter_set_id=params,
        signal_observed_at=at,
        signal_ingest_seq=ingest_seq,
        price_identity="SOL_NATIVE",
        price_numerator_raw=BASE_PRICE_NUM,
        price_denominator_raw=PRICE_DEN,
    )
    return ContinuousSourceItem(
        cursor,
        f"SRC-{cursor}-{signal}-CANDIDATE",
        CandidateEvaluationEvent(evaluation, candidate),
    )


def skip_item(
    cursor: int,
    *,
    at: datetime,
    label: str = "C",
    ingest_seq: int = 3,
    audit_snapshot: dict[str, object] | None = None,
) -> ContinuousSourceItem:
    evaluation = StrategyEvaluationAuditInput(
        run_id=f"RUN-SKIP-{label}",
        role="ROBUST_2",
        mint=f"MINT-{label}",
        strategy_version="v1.1",
        parameter_set_id=LOCKED_PARAMETER_SET_BY_ROLE["ROBUST_2"],
        ingest_seq=ingest_seq,
        evaluated_at_us=us(at),
        current_state="EXPIRED",
        transition_occurred=True,
        reason_code="ENTRY_WINDOW_EXPIRED",
        filters_passed=(),
        filters_failed=("RECLAIM",),
        signal_type=None,
        candidate_signal_id=None,
        audit_snapshot=(
            {"source": "continuous-selftest", "terminal": "skip"}
            if audit_snapshot is None
            else audit_snapshot
        ),
    )
    return ContinuousSourceItem(
        cursor,
        f"SRC-{cursor}-SKIP-{label}",
        TerminalSkipEvaluationEvent(evaluation),
    )


def market_item(
    cursor: int,
    *,
    mint: str,
    at: datetime,
    ingest_seq: int,
    return_bps: int,
) -> ContinuousSourceItem:
    numerator = BASE_PRICE_NUM * (10_000 + return_bps) // 10_000
    return ContinuousSourceItem(
        cursor,
        f"SRC-{cursor}-{mint}-MARKET",
        MarketObservationEvent(
            mint=mint,
            observed_at=at,
            ingest_seq=ingest_seq,
            price_identity="SOL_NATIVE",
            price_numerator_raw=numerator,
            price_denominator_raw=PRICE_DEN,
            current_virtual_token_reserve_raw=PRICE_DEN,
            is_gap_recovery=False,
        ),
    )


def clock_item(cursor: int, *, at: datetime) -> ContinuousSourceItem:
    return ContinuousSourceItem(
        cursor,
        f"SRC-{cursor}-CLOCK",
        ClockTickEvent(at),
    )


def count_rows(conn: sqlite3.Connection) -> dict[str, int]:
    tables = (
        "paper_strategy_evaluations",
        "paper_terminal_trade_decisions",
        "paper_entry_routes",
        "paper_orders",
        "paper_positions",
        "paper_exit_track_states",
        "paper_exit_intents",
        "paper_exit_execution_routes",
        "paper_exit_execution_attempts",
        "paper_position_mtm_marks",
        "paper_track_equity_marks",
        "paper_continuous_signal_contexts_v0_1",
    )
    return {
        table: int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
        for table in tables
    }


def states_for_signal(conn: sqlite3.Connection, signal: str) -> dict[str, str]:
    return {
        str(row["track_id"]): str(row["state"])
        for row in conn.execute(
            "SELECT track_id,state FROM paper_positions WHERE signal_key=? ORDER BY track_id",
            (signal,),
        ).fetchall()
    }


def make_runner(
    conn: sqlite3.Connection,
    *,
    failure=None,
    wall_clock=None,
) -> ContinuousPaperRunnerV01:
    return ContinuousPaperRunnerV01(
        conn,
        source_identity=SOURCE_IDENTITY,
        wall_clock=FixedClock() if wall_clock is None else wall_clock,
        failure_injector=failure,
    )


class OneThenExplodeSource:
    source_identity = SOURCE_IDENTITY

    def __init__(self, item: ContinuousSourceItem) -> None:
        self.item = item
        self.items_requested = 0

    def __iter__(self):
        self.items_requested += 1
        yield self.item
        raise AssertionError("streaming source was consumed beyond requested result")


class MutableClock:
    def __init__(self, current: datetime) -> None:
        self.current = current

    def __call__(self) -> datetime:
        return self.current


def correction_checks(root: Path) -> dict[str, bool]:
    checks: dict[str, bool] = {}

    decimal_snapshot = {
        "nested": {
            "price": Decimal("0.0000012300"),
            "values": [Decimal("12.3400"), Decimal("0.0100")],
        }
    }
    decimal_item_a = candidate_item(
        1,
        signal="DECIMAL-SIGNAL",
        mint="DECIMAL-MINT",
        role="CONTROL",
        at=BASE,
        ingest_seq=1,
        audit_snapshot=decimal_snapshot,
    )
    decimal_item_b = candidate_item(
        1,
        signal="DECIMAL-SIGNAL",
        mint="DECIMAL-MINT",
        role="CONTROL",
        at=BASE,
        ingest_seq=1,
        audit_snapshot={
            "nested": {
                "price": Decimal("0.0000012300"),
                "values": [Decimal("12.3400"), Decimal("0.0100")],
            }
        },
    )
    decimal_without_trailing_zero = candidate_item(
        1,
        signal="DECIMAL-SIGNAL",
        mint="DECIMAL-MINT",
        role="CONTROL",
        at=BASE,
        ingest_seq=1,
        audit_snapshot={
            "nested": {
                "price": Decimal("0.00000123"),
                "values": [Decimal("12.34"), Decimal("0.01")],
            }
        },
    )
    checks["U_nested_decimal_fingerprint_stable"] = (
        decimal_item_a.content_fingerprint == decimal_item_b.content_fingerprint
    )
    checks["V_decimal_trailing_zero_text_preserved"] = (
        decimal_item_a.content_fingerprint
        != decimal_without_trailing_zero.content_fingerprint
    )

    nonfinite_failures = 0
    for label, value in (
        ("NAN", Decimal("NaN")),
        ("POSINF", Decimal("Infinity")),
        ("NEGINF", Decimal("-Infinity")),
    ):
        item = candidate_item(
            1,
            signal=f"DECIMAL-{label}",
            mint=f"DECIMAL-MINT-{label}",
            role="CONTROL",
            at=BASE,
            ingest_seq=1,
            audit_snapshot={"nested": {"value": value}},
        )
        try:
            _ = item.content_fingerprint
        except ValueError:
            nonfinite_failures += 1
    checks["W_nonfinite_decimal_fails_closed"] = nonfinite_failures == 3

    streaming_conn = sqlite3.connect(root / "streaming.sqlite3")
    streaming_conn.row_factory = sqlite3.Row
    streaming_runner = make_runner(streaming_conn)
    lazy_source = OneThenExplodeSource(
        skip_item(10, at=BASE, label="STREAM", ingest_seq=10)
    )
    result_iterator = streaming_runner.run(lazy_source)
    lazy_before_next = lazy_source.items_requested == 0
    first_result = next(result_iterator)
    checks["X_run_streams_without_prematerializing"] = (
        lazy_before_next
        and lazy_source.items_requested == 1
        and first_result.status == "COMMITTED"
        and streaming_runner.last_committed_source_cursor == 10
    )
    streaming_conn.close()

    cursor_conn = sqlite3.connect(root / "cursor_contract.sqlite3")
    cursor_conn.row_factory = sqlite3.Row
    cursor_runner = make_runner(cursor_conn)
    cursor_one = skip_item(1, at=BASE, label="CURSOR-1", ingest_seq=21)
    cursor_three = skip_item(3, at=BASE + timedelta(seconds=1), label="CURSOR-3", ingest_seq=23)
    cursor_ten = skip_item(10, at=BASE + timedelta(seconds=2), label="CURSOR-10", ingest_seq=30)
    cursor_runner.process_source_item(cursor_one)
    cursor_runner.process_source_item(cursor_three)
    exact_replay = cursor_runner.process_source_item(cursor_three)
    checks["Y_exact_last_cursor_replay_allowed"] = (
        exact_replay.status == "ALREADY_COMMITTED"
    )

    conflicting_last_failed = False
    try:
        cursor_runner.process_source_item(
            skip_item(3, at=BASE + timedelta(seconds=1), label="CONFLICT", ingest_seq=24)
        )
    except SourceCursorConflict:
        conflicting_last_failed = True
    checks["Z_conflicting_last_cursor_fails_closed"] = conflicting_last_failed

    lower_unseen_failed = False
    try:
        cursor_runner.process_source_item(
            skip_item(2, at=BASE + timedelta(milliseconds=500), label="UNSEEN-2", ingest_seq=22)
        )
    except SourceCursorConflict:
        lower_unseen_failed = True
    checks["AA_lower_unseen_cursor_fails_closed"] = lower_unseen_failed

    sparse_result = cursor_runner.process_source_item(cursor_ten)
    checks["AB_sparse_increasing_cursor_allowed"] = (
        sparse_result.status == "COMMITTED"
        and cursor_runner.last_committed_source_cursor == 10
    )
    cursor_conn.close()

    decimal_conn = sqlite3.connect(root / "decimal_persistence.sqlite3")
    decimal_conn.row_factory = sqlite3.Row
    decimal_runner = make_runner(decimal_conn)
    decimal_result = decimal_runner.process_source_item(decimal_item_a)
    checks["AC_candidate_decimal_processing_committed"] = (
        decimal_result.status == "COMMITTED"
    )
    checks["AD_candidate_decimal_cursor_advanced"] = (
        decimal_runner.last_committed_source_cursor == 1
    )
    candidate_row = decimal_conn.execute(
        "SELECT audit_snapshot_json FROM paper_strategy_evaluations WHERE run_id='RUN-DECIMAL-SIGNAL'"
    ).fetchone()
    candidate_snapshot = (
        None if candidate_row is None else json.loads(str(candidate_row["audit_snapshot_json"]))
    )
    checks["AE_candidate_decimal_json_exact"] = candidate_snapshot == {
        "nested": {
            "price": "0.0000012300",
            "values": ["12.3400", "0.0100"],
        }
    }
    eval_count_before_replay = int(
        decimal_conn.execute("SELECT COUNT(*) FROM paper_strategy_evaluations").fetchone()[0]
    )
    decimal_replay = decimal_runner.process_source_item(decimal_item_a)
    eval_count_after_replay = int(
        decimal_conn.execute("SELECT COUNT(*) FROM paper_strategy_evaluations").fetchone()[0]
    )
    checks["AF_candidate_decimal_replay_idempotent"] = (
        decimal_replay.status == "ALREADY_COMMITTED"
        and eval_count_after_replay == eval_count_before_replay == 1
    )

    decimal_skip = skip_item(
        2,
        at=BASE + timedelta(seconds=1),
        label="DECIMAL-SKIP",
        ingest_seq=2,
        audit_snapshot={
            "nested": {
                "ratio": Decimal("7.5000"),
                "thresholds": [Decimal("0.1000"), Decimal("2.000")],
            }
        },
    )
    decimal_runner.process_source_item(decimal_skip)
    skip_row = decimal_conn.execute(
        "SELECT audit_snapshot_json FROM paper_strategy_evaluations WHERE run_id='RUN-SKIP-DECIMAL-SKIP'"
    ).fetchone()
    skip_snapshot = (
        None if skip_row is None else json.loads(str(skip_row["audit_snapshot_json"]))
    )
    checks["AG_terminal_skip_decimal_json_exact"] = skip_snapshot == {
        "nested": {
            "ratio": "7.5000",
            "thresholds": ["0.1000", "2.000"],
        }
    }

    nonfinite_process_item = candidate_item(
        3,
        signal="DECIMAL-PROCESS-NAN",
        mint="DECIMAL-PROCESS-MINT",
        role="CONTROL",
        at=BASE + timedelta(seconds=2),
        ingest_seq=3,
        audit_snapshot={"nested": {"value": Decimal("NaN")}},
    )
    before_nonfinite_rows = int(
        decimal_conn.execute("SELECT COUNT(*) FROM paper_strategy_evaluations").fetchone()[0]
    )
    nonfinite_processing_failed = False
    try:
        decimal_runner.process_source_item(nonfinite_process_item)
    except ValueError:
        nonfinite_processing_failed = True
    after_nonfinite_rows = int(
        decimal_conn.execute("SELECT COUNT(*) FROM paper_strategy_evaluations").fetchone()[0]
    )
    checks["AH_nonfinite_processing_fails_closed"] = nonfinite_processing_failed
    checks["AI_nonfinite_processing_cursor_unchanged"] = (
        decimal_runner.last_committed_source_cursor == 2
    )
    checks["AJ_nonfinite_processing_no_evaluation_row"] = (
        before_nonfinite_rows == after_nonfinite_rows == 2
    )
    decimal_conn.close()

    missing_clock_failed = False
    try:
        ClockTickEvent()  # type: ignore[call-arg]
    except (TypeError, ValueError):
        missing_clock_failed = True
    checks["AK_clock_without_explicit_now_fails_closed"] = missing_clock_failed

    naive_clock_failed = False
    try:
        ClockTickEvent(datetime(2026, 8, 23, 20, 0, 0))
    except ValueError:
        naive_clock_failed = True
    checks["AL_naive_clock_fails_closed"] = naive_clock_failed

    explicit_tick = clock_item(3, at=BASE + timedelta(seconds=5))
    explicit_tick_copy = clock_item(
        3, at=BASE + timedelta(seconds=5)
    )
    checks["AM_explicit_clock_fingerprint_stable"] = (
        explicit_tick.content_fingerprint == explicit_tick_copy.content_fingerprint
    )

    clock_db = root / "clock_retry.sqlite3"
    clock_conn = sqlite3.connect(clock_db)
    clock_conn.row_factory = sqlite3.Row
    mutable_clock = MutableClock(BASE + timedelta(seconds=5))
    clock_failure = FailOnceAfterProcessing(3)
    clock_runner = make_runner(
        clock_conn, failure=clock_failure, wall_clock=mutable_clock
    )
    clock_runner.process_source_item(
        candidate_item(
            1,
            signal="CLOCK-SIGNAL",
            mint="CLOCK-MINT",
            role="CONTROL",
            at=BASE,
            ingest_seq=1,
        )
    )
    clock_runner.process_source_item(
        market_item(
            2,
            mint="CLOCK-MINT",
            at=BASE + timedelta(milliseconds=600),
            ingest_seq=41,
            return_bps=0,
        )
    )
    clock_injected = False
    try:
        clock_runner.process_source_item(explicit_tick)
    except RuntimeError as exc:
        clock_injected = str(exc) == "INJECTED_AFTER_PROCESSING_BEFORE_CURSOR_COMMIT"
    intents_after_failure = int(
        clock_conn.execute("SELECT COUNT(*) FROM paper_exit_intents").fetchone()[0]
    )
    cursor_after_clock_failure = clock_runner.last_committed_source_cursor
    clock_conn.close()

    mutable_clock.current = BASE + timedelta(seconds=7)
    clock_conn = sqlite3.connect(clock_db)
    clock_conn.row_factory = sqlite3.Row
    clock_runner = make_runner(clock_conn, wall_clock=mutable_clock)
    clock_retry = clock_runner.process_source_item(explicit_tick)
    intents_after_retry = int(
        clock_conn.execute("SELECT COUNT(*) FROM paper_exit_intents").fetchone()[0]
    )
    clock_states = states_for_signal(clock_conn, "CLOCK-SIGNAL")
    checks["AN_clock_retry_semantics_ignore_moved_wall_clock"] = (
        clock_injected
        and cursor_after_clock_failure == 2
        and intents_after_failure == intents_after_retry == 0
        and set(clock_states.values()) == {"OPEN"}
    )
    checks["AO_clock_cursor_advances_after_exact_retry"] = (
        clock_retry.status == "COMMITTED"
        and clock_runner.last_committed_source_cursor == 3
    )
    clock_conn.close()
    return checks


def execute_scenario(db_path: Path) -> ScenarioEvidence:
    checks: dict[str, bool] = {}
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    runner = make_runner(conn)

    a_candidate = candidate_item(
        1,
        signal="SIGNAL-A",
        mint="MINT-A",
        role="CONTROL",
        at=BASE,
        ingest_seq=1,
    )
    b_candidate = candidate_item(
        2,
        signal="SIGNAL-B",
        mint="MINT-B",
        role="ROBUST_1",
        at=BASE + timedelta(milliseconds=100),
        ingest_seq=2,
    )
    terminal_skip = skip_item(3, at=BASE + timedelta(milliseconds=200))
    a_fill = market_item(
        4,
        mint="MINT-A",
        at=BASE + timedelta(milliseconds=600),
        ingest_seq=101,
        return_bps=0,
    )
    b_fill = market_item(
        5,
        mint="MINT-B",
        at=BASE + timedelta(milliseconds=700),
        ingest_seq=102,
        return_bps=0,
    )
    a_activate = market_item(
        6,
        mint="MINT-A",
        at=BASE + timedelta(seconds=2),
        ingest_seq=103,
        return_bps=1200,
    )
    a_peak = market_item(
        7,
        mint="MINT-A",
        at=BASE + timedelta(seconds=3),
        ingest_seq=104,
        return_bps=2000,
    )
    a_giveback = market_item(
        8,
        mint="MINT-A",
        at=BASE + timedelta(seconds=4),
        ingest_seq=105,
        return_bps=1500,
    )
    a_finish = market_item(
        9,
        mint="MINT-A",
        at=BASE + timedelta(seconds=5),
        ingest_seq=106,
        return_bps=1500,
    )
    b_sens_clock = clock_item(10, at=BASE + timedelta(seconds=6))
    b_sens_fill = market_item(
        11,
        mint="MINT-B",
        at=BASE + timedelta(seconds=7),
        ingest_seq=107,
        return_bps=0,
    )
    b_final_clock = clock_item(12, at=BASE + timedelta(seconds=16))
    b_finish = market_item(
        13,
        mint="MINT-B",
        at=BASE + timedelta(seconds=17),
        ingest_seq=108,
        return_bps=0,
    )

    for item in (a_candidate, b_candidate, terminal_skip, a_fill, b_fill):
        runner.process_source_item(item)

    positions_after_two_entries = conn.execute(
        "SELECT signal_key,track_id,state FROM paper_positions ORDER BY signal_key,track_id"
    ).fetchall()
    checks["B_processing_continues_beyond_one_signal"] = (
        runner.last_committed_source_cursor == 5
    )
    checks["C_two_distinct_token_signal_lifecycles"] = {
        str(row["signal_key"]) for row in positions_after_two_entries
    } == {"SIGNAL-A", "SIGNAL-B"}
    checks["D_two_entries_overlap_without_cross_linkage"] = (
        len(positions_after_two_entries) == 6
        and all(str(row["state"]) == "OPEN" for row in positions_after_two_entries)
    )
    checks["E_exact_locked_tracks_per_signal"] = all(
        set(states_for_signal(conn, signal)) == {"FINAL-A", "FINAL-B", "SENS-C"}
        for signal in ("SIGNAL-A", "SIGNAL-B")
    )
    skip_rows = conn.execute(
        "SELECT trade_or_skip,decision_reason FROM paper_terminal_trade_decisions WHERE trade_or_skip='SKIP'"
    ).fetchall()
    checks["F_one_explicit_terminal_skip"] = (
        len(skip_rows) == 1
        and str(skip_rows[0]["decision_reason"]) == "ENTRY_WINDOW_EXPIRED"
    )

    runner.process_source_item(a_activate)
    a_after_activate = states_for_signal(conn, "SIGNAL-A")
    b_after_activate = states_for_signal(conn, "SIGNAL-B")
    checks["G_independent_exit_progression"] = (
        a_after_activate["FINAL-A"] == "EXIT_PENDING"
        and b_after_activate == {
            "FINAL-A": "OPEN",
            "FINAL-B": "OPEN",
            "SENS-C": "OPEN",
        }
    )

    conn.close()
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    fail_once = FailOnceAfterProcessing(7)
    runner = make_runner(conn, failure=fail_once)
    restart_states = states_for_signal(conn, "SIGNAL-A")
    checks["I_restart_with_open_and_exit_pending"] = (
        restart_states["FINAL-A"] == "EXIT_PENDING"
        and restart_states["FINAL-B"] == "OPEN"
        and restart_states["SENS-C"] == "OPEN"
    )

    injected = False
    try:
        runner.process_source_item(a_peak)
    except RuntimeError as exc:
        injected = str(exc) == "INJECTED_AFTER_PROCESSING_BEFORE_CURSOR_COMMIT"
    counts_after_failure = count_rows(conn)
    checks["M_injected_failure_cursor_not_advanced"] = (
        injected and runner.last_committed_source_cursor == 6
    )
    conn.close()

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    runner = make_runner(conn)
    runner.process_source_item(a_peak)
    counts_after_retry = count_rows(conn)
    checks["N_retry_completes_event_exactly_once"] = (
        runner.last_committed_source_cursor == 7
        and counts_after_retry == counts_after_failure
    )
    checks["J_restart_no_duplicate_entries_orders_positions"] = (
        counts_after_retry["paper_entry_routes"] == 2
        and counts_after_retry["paper_orders"] == 6
        and counts_after_retry["paper_positions"] == 6
    )
    checks["L_cursor_after_successful_processing_only"] = (
        runner.last_committed_source_cursor == 7
    )

    runner.process_source_item(a_giveback)
    a_mixed = states_for_signal(conn, "SIGNAL-A")
    checks["H_closed_siblings_do_not_block_open_siblings"] = (
        a_mixed["FINAL-A"] == "CLOSED"
        and a_mixed["SENS-C"] == "CLOSED"
        and a_mixed["FINAL-B"] == "EXIT_PENDING"
    )
    runner.process_source_item(a_finish)
    runner.process_source_item(b_sens_clock)
    runner.process_source_item(b_sens_fill)
    b_mixed = states_for_signal(conn, "SIGNAL-B")
    checks["G_independent_exit_progression"] = (
        checks["G_independent_exit_progression"]
        and b_mixed["SENS-C"] == "CLOSED"
        and b_mixed["FINAL-A"] == "OPEN"
        and b_mixed["FINAL-B"] == "OPEN"
    )
    runner.process_source_item(b_final_clock)
    runner.process_source_item(b_finish)

    counts_before_replay = count_rows(conn)
    replay = runner.process_source_item(b_finish)
    checks["K_committed_source_replay_idempotent"] = (
        replay.status == "ALREADY_COMMITTED"
        and count_rows(conn) == counts_before_replay
    )

    runtime = conn.execute(
        "SELECT * FROM paper_continuous_runtime_state_v0_1 WHERE singleton=1"
    ).fetchone()
    checks["A_exact_model_and_fingerprint_persisted"] = (
        str(runtime["runner_model_id"]) == MODEL_ID
        and str(runtime["runner_spec_fingerprint"]) == RUNNER_SPEC_FINGERPRINT
    )

    final_states = {
        str(row["state"])
        for row in conn.execute("SELECT state FROM paper_positions").fetchall()
    }
    trades = load_completed_trades(conn)
    report = build_report(conn)
    realized_by_track = {
        track: sum(t.net_pnl_lamports for t in trades if t.track_id == track)
        for track in ("FINAL-A", "FINAL-B", "SENS-C")
    }
    report_by_track = {track.track_id: track for track in report.tracks}
    checks["P_final_realized_equity_reconciles"] = (
        final_states == {"CLOSED"}
        and len(trades) == 6
        and all(
            report_by_track[track].final_equity_pnl_lamports
            == realized_by_track[track]
            for track in realized_by_track
        )
    )
    checks["Q_mtm_mae_mfe_remain_per_track"] = (
        len(report.positions) == 6
        and {(p.signal_key, p.track_id) for p in report.positions}
        == {
            (signal, track)
            for signal in ("SIGNAL-A", "SIGNAL-B")
            for track in ("FINAL-A", "FINAL-B", "SENS-C")
        }
        and all(p.marks >= 1 for p in report.positions)
    )
    checks["R_cross_track_summation_prohibited"] = (
        report.cross_track_aggregation_status
        == "PROHIBITED_ALTERNATIVE_EXIT_TRACKS_REPORTED_SEPARATELY"
    )
    quick = str(conn.execute("PRAGMA quick_check").fetchone()[0]).lower()
    checks["S_sqlite_quick_check_ok"] = quick == "ok"

    digest = runner.canonical_digest()
    report_digest = canonical_report_digest(report)
    final_cursor = runner.last_committed_source_cursor
    conn.close()

    conflict_db = db_path.with_name(db_path.stem + "_conflict.sqlite3")
    shutil.copy2(db_path, conflict_db)
    conflict_conn = sqlite3.connect(conflict_db)
    conflict_conn.execute(
        "UPDATE paper_continuous_runtime_state_v0_1 SET runner_spec_fingerprint='CONFLICT' WHERE singleton=1"
    )
    conflict_conn.commit()
    conflict_conn.close()
    conflict_conn = sqlite3.connect(conflict_db)
    conflict_conn.row_factory = sqlite3.Row
    failed_closed = False
    try:
        make_runner(conflict_conn)
    except RuntimeBindingConflict:
        failed_closed = True
    finally:
        conflict_conn.close()
    checks["O_conflicting_fingerprint_fails_closed"] = failed_closed

    return ScenarioEvidence(checks, digest, report_digest, quick, final_cursor)


def main() -> int:
    print("=" * 124)
    print("PHASE 4 - RESTART-SAFE CONTINUOUS PAPER RUNNER FOUNDATION SELF-TEST v0.1")
    print("=" * 124)
    print(f"Project root                       : {PROJECT_ROOT}")
    print(f"Runner model                       : {MODEL_ID}")
    print(f"Runner fingerprint                 : {RUNNER_SPEC_FINGERPRINT}")
    print("Production DB / collector / RPC    : NO")
    print("Wallet / signing / live orders     : NO")
    print("Research / tracked fixture writes  : NO")
    print("Cross-track portfolio summation    : PROHIBITED")
    print()

    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        first = execute_scenario(root / "continuous_a.sqlite3")
        second = execute_scenario(root / "continuous_b.sqlite3")

        checks = dict(first.checks)
        checks["T_canonical_determinism_repeated_runs"] = (
            first.digest == second.digest
            and first.report_digest == second.report_digest
            and first.final_cursor == second.final_cursor == 13
        )
        checks.update(correction_checks(root))

        print("-" * 124)
        print("VALIDATION")
        print("-" * 124)
        for name, passed in checks.items():
            print(f"{name:<72}: {'PASS' if passed else 'FAIL'}")

        all_ok = all(checks.values())
        print()
        print(f"Final source cursor                : {first.final_cursor}")
        print(f"SQLite quick_check                 : {first.quick_check}")
        print(f"Observability report digest        : {first.report_digest}")
        print(f"Canonical runner digest            : {first.digest}")
        print("Live continuous paper run          : NOT YET VALIDATED")
        print()
        print(f"RESULT: {'PASS' if all_ok else 'CHECK'}")
        return 0 if all_ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
