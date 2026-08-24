from __future__ import annotations

import hashlib
import shutil
import sqlite3
import sys
import tempfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from phase2.denomination_aware_flow_v0_1 import DenominationAwareFeatureEngineV01  # noqa: E402
from phase2.strategy_clock_v0_2 import FirstPullbackStrategyClockV02  # noqa: E402
from phase2.strategy_first_pullback_v0_2 import FirstPullbackStrategyV02  # noqa: E402
from phase4.paper_continuous_firstpullback_binding_v0_1 import (  # noqa: E402
    GAP_COMPAT_FINGERPRINT,
    LOCKED_PARAMETER_SET_BY_ROLE,
    LOCKED_ROLE_ORDER,
    LOCKED_SELECTION_SHA256,
    MARKET_SOURCE_FINGERPRINT,
    MODEL_FINGERPRINT,
    MODEL_ID,
    RUNNER_SPEC_FINGERPRINT,
    BindingConflict,
    ContinuousFirstPullbackBindingV01,
    locked_parameter_sets,
)
from phase4.paper_continuous_market_source_v0_1 import ContinuousMarketSourceV01  # noqa: E402
from phase4.paper_entry_router_v0_1 import connect_entry_router_db  # noqa: E402
from phase4.paper_runtime_observability_v0_1 import build_report  # noqa: E402
from phase4.paper_trade_accounting_v0_1 import load_completed_trades  # noqa: E402


EXPECTED_BINDING_FINGERPRINT = "318e15b104c691821f5715b090c934e5b1cb3d9a8357fe0930d8825ef66d0daa"
EXPECTED_SOURCE_FINGERPRINT = "242b84a26ddc9e80fc428b1f02202e30aab6f4009677b38b72861bad432881fb"
EXPECTED_RUNNER_FINGERPRINT = "8dcab9d17f2ec71f4920a199b5745dae92dc3464a466860593231424e0a24dea"
EXPECTED_GAP_FINGERPRINT = "aabb765f81a29b2dd119607cdea92a2949dd3acec10aa204d246e6aa7a28a78c"
SOURCE_IDENTITY = "SELFTEST:FIRSTPULLBACK-BINDING-SOURCE-V0.1"
BASE = datetime(2026, 8, 24, 0, 0, tzinfo=timezone.utc)


SCHEMA_SQL = """
CREATE TABLE pump_events (
    event_key TEXT PRIMARY KEY,
    signature TEXT NOT NULL,
    event_type TEXT NOT NULL,
    slot INTEGER,
    pump_timestamp INTEGER,
    mint TEXT,
    user_wallet TEXT,
    creator_wallet TEXT,
    sol_amount_lamports INTEGER,
    token_amount_raw TEXT,
    virtual_sol_reserves TEXT,
    virtual_token_reserves TEXT,
    real_sol_reserves TEXT,
    real_token_reserves TEXT,
    quote_mint TEXT,
    quote_amount_raw TEXT,
    virtual_quote_reserves TEXT,
    real_quote_reserves TEXT,
    source_decoded_file TEXT NOT NULL,
    decoded_at_utc TEXT
)
"""


@dataclass
class Evidence:
    checks: dict[str, bool]
    digest: str
    reconstruction_queries: int
    candidate_order: tuple[str, ...]
    quick_check: str


@dataclass
class FenceEvidence:
    checks: dict[str, bool]
    digest: str


def insert_row(
    conn: sqlite3.Connection,
    rowid: int,
    mint: str,
    event_type: str,
    *,
    price_units: int = 10_000,
    observed_offset: int | None = None,
    source: str = "LIVE_WEBSOCKET_EVENT_V0_3_4",
) -> None:
    observed = BASE + timedelta(seconds=rowid if observed_offset is None else observed_offset)
    trade = event_type in {"BUY", "SELL"}
    conn.execute(
        """
        INSERT INTO pump_events(
            rowid,event_key,signature,event_type,slot,pump_timestamp,mint,
            user_wallet,creator_wallet,sol_amount_lamports,token_amount_raw,
            virtual_sol_reserves,virtual_token_reserves,real_sol_reserves,
            real_token_reserves,quote_mint,quote_amount_raw,virtual_quote_reserves,
            real_quote_reserves,source_decoded_file,decoded_at_utc
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            rowid, f"event-{rowid}", f"sig-{rowid}", event_type, 5_000_000 + rowid,
            int(observed.timestamp()), mint, f"wallet-{rowid}" if trade else None,
            f"creator-{mint}" if event_type == "LAUNCH" else None,
            50_000_000 if trade else None, "250000000" if trade else None,
            str(price_units * 1_000_000), "100000000000",
            "5000000000", "50000000000", None, None, None, None,
            source, observed.isoformat(timespec="microseconds"),
        ),
    )


def create_fixture(path: Path) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.execute(SCHEMA_SQL)
        insert_row(conn, 1, "UNLAUNCHED", "BUY")
        insert_row(conn, 2, "GAP-LAUNCH", "LAUNCH", source="GAP_RECONCILIATION_V0_3_4")
        insert_row(conn, 3, "FUTURE-GAP", "LAUNCH", source="GAP_RECONCILIATION_V0_3_5")
        insert_row(conn, 4, "MINT-A", "LAUNCH")
        insert_row(conn, 5, "MINT-B", "LAUNCH")
        insert_row(conn, 6, "MINT-A", "BUY", price_units=10_000)
        insert_row(conn, 7, "MINT-B", "BUY", price_units=10_000)
        insert_row(conn, 8, "MINT-A", "BUY", price_units=10_600)
        insert_row(conn, 9, "MINT-A", "BUY", price_units=10_600)
        insert_row(conn, 10, "MINT-A", "SELL", price_units=9_400)
        insert_row(conn, 11, "MINT-A", "SELL", price_units=9_400)
        insert_row(conn, 12, "MINT-A", "BUY", price_units=9_700)
        insert_row(conn, 13, "MINT-A", "BUY", price_units=9_900)
        insert_row(conn, 14, "MINT-A", "BUY", price_units=10_100)
        insert_row(conn, 15, "MINT-A", "BUY", price_units=10_100, observed_offset=15)
        insert_row(conn, 16, "MINT-A", "BUY", price_units=10_100, observed_offset=28)
        insert_row(conn, 17, "MINT-A", "BUY", price_units=10_100, observed_offset=28)
        insert_row(conn, 18, "MINT-C", "LAUNCH", observed_offset=29)
        insert_row(conn, 19, "MINT-C", "BUY", price_units=10_000, observed_offset=30)
        insert_row(conn, 20, "MINT-C", "BUY", price_units=10_600, observed_offset=31)
        insert_row(conn, 21, "MINT-C", "BUY", price_units=10_600, observed_offset=32)
        insert_row(conn, 22, "MINT-C", "SELL", price_units=9_400, observed_offset=33)
        insert_row(conn, 23, "MINT-C", "SELL", price_units=9_400, observed_offset=34)
        insert_row(conn, 24, "MINT-C", "BUY", price_units=9_700, observed_offset=35)
        insert_row(conn, 25, "MINT-C", "BUY", price_units=9_900, observed_offset=36)
        insert_row(conn, 26, "MINT-C", "BUY", price_units=10_100, observed_offset=37)
        insert_row(conn, 27, "MINT-C", "BUY", price_units=10_100, observed_offset=38)
        insert_row(conn, 28, "MINT-A", "BUY", price_units=10_200, observed_offset=51)
        insert_row(conn, 29, "MINT-C", "BUY", price_units=10_100, observed_offset=52)
        insert_row(conn, 30, "MINT-C", "BUY", price_units=10_200, observed_offset=62)
        insert_row(conn, 31, "MINT-A", "BUY", price_units=10_300,
                   observed_offset=63, source="GAP_RECONCILIATION_V0_3_4")
        insert_row(conn, 32, "FUTURE-GAP", "BUY", source="GAP_RECONCILIATION_V0_3_5")
        # Deliberately sparse rowid proves prefix consumption does not assume
        # numerically contiguous SQLite rowids.
        insert_row(conn, 34, "GAP-LAUNCH", "BUY")
        conn.commit()
    finally:
        conn.close()


def copy_fixture_rows(template: Path, target: Path, *, through_rowid: int) -> None:
    if not target.exists():
        conn = sqlite3.connect(target)
        conn.execute(SCHEMA_SQL)
        conn.commit()
        conn.close()
    source_conn = sqlite3.connect(template)
    source_conn.row_factory = sqlite3.Row
    target_conn = sqlite3.connect(target)
    try:
        current = int(target_conn.execute(
            "SELECT COALESCE(MAX(rowid),0) FROM pump_events"
        ).fetchone()[0])
        rows = source_conn.execute(
            "SELECT rowid AS p1_rowid,* FROM pump_events "
            "WHERE rowid>? AND rowid<=? ORDER BY rowid", (current, through_rowid)
        ).fetchall()
        columns = ["rowid"] + [
            str(r[1]) for r in target_conn.execute("PRAGMA table_info(pump_events)")
        ]
        sql = (
            "INSERT INTO pump_events(" + ",".join(columns) + ") VALUES(" +
            ",".join("?" for _ in columns) + ")"
        )
        for row in rows:
            target_conn.execute(sql, tuple(row))
        target_conn.commit()
    finally:
        source_conn.close()
        target_conn.close()


def source(path: Path, identity: str = SOURCE_IDENTITY, anchor: int = 0) -> ContinuousMarketSourceV01:
    return ContinuousMarketSourceV01(
        path, start_after_p1_rowid=anchor, database_identity=identity
    )


def reopen(
    paper: Path,
    fixture: Path,
    failure: Callable[[str, str], None] | None = None,
) -> tuple[sqlite3.Connection, ContinuousFirstPullbackBindingV01]:
    conn = connect_entry_router_db(paper)
    binding = ContinuousFirstPullbackBindingV01(
        conn, source(fixture), wall_clock=lambda: BASE, failure_injector=failure
    )
    return conn, binding


class FailOnce:
    def __init__(self, input_key: str, phase: str) -> None:
        self.input_key = input_key
        self.phase = phase
        self.fired = False

    def __call__(self, input_key: str, phase: str) -> None:
        if not self.fired and input_key == self.input_key and phase == self.phase:
            self.fired = True
            raise RuntimeError(f"INJECTED:{input_key}:{phase}")


def expect_failure(binding: ContinuousFirstPullbackBindingV01, batch_size: int) -> bool:
    try:
        binding.process_next_batch(batch_size=batch_size)
    except RuntimeError as exc:
        return str(exc).startswith("INJECTED:")
    return False


def states(conn: sqlite3.Connection, mint: str) -> dict[str, str]:
    return {
        str(r["track_id"]): str(r["state"])
        for r in conn.execute(
            "SELECT p.track_id,p.state FROM paper_positions p "
            "WHERE p.mint=? ORDER BY p.track_id", (mint,)
        ).fetchall()
    }


def run_scenario(root: Path, *, failures: bool) -> Evidence:
    root.mkdir(parents=True, exist_ok=False)
    template = root / "source_template.sqlite3"
    fixture = root / "source.sqlite3"
    paper = root / "paper.sqlite3"
    create_fixture(template)
    copy_fixture_rows(template, fixture, through_rowid=14)
    checks: dict[str, bool] = {}
    total_queries = 0
    prefix_evaluation_count = 0
    clock_retry_exact = True
    strategy_pending_had_no_items = False

    if failures:
        fail = FailOnce("P1:1", "after_skip_semantics_before_production_cursor")
        conn, binding = reopen(paper, fixture, fail)
        checks["S_skip_only_batch_advances"] = expect_failure(binding, 3) and binding.durable_p1_rowid == 0
        total_queries += binding.reconstruction_queries
        conn.close()
        conn, binding = reopen(paper, fixture)
        skip_result = binding.process_next_batch(batch_size=3)
        checks["S_skip_only_batch_advances"] &= (
            skip_result.durable_p1_rowid == 3 and skip_result.normalized_records == 0
            and skip_result.deterministic_skips == 3
        )
        total_queries += binding.reconstruction_queries
        conn.close()

        fail = FailOnce(
            "P1:4", "after_evaluation_persistence_before_audit_delivery"
        )
        conn, binding = reopen(paper, fixture, fail)
        checks["T_cursor_stops_on_failure"] = expect_failure(binding, 1) and binding.durable_p1_rowid == 3
        checks["AL_evaluation_retry_no_duplicate"] = int(conn.execute(
            "SELECT COUNT(*) FROM paper_strategy_evaluations WHERE ingest_seq=4"
        ).fetchone()[0]) == 1
        total_queries += binding.reconstruction_queries
        conn.close()
        conn, binding = reopen(paper, fixture)
        binding.process_next_batch(batch_size=9)
        total_queries += binding.reconstruction_queries
        checks["W_pending_state_restored"] = int(conn.execute(
            "SELECT COUNT(*) FROM paper_fp_binding_strategy_runs_v0_1 WHERE mint='MINT-B'"
        ).fetchone()[0]) == 4
        conn.close()

        conn, binding = reopen(paper, fixture)
        binding.process_next_batch(batch_size=1)  # row 13 confirms buyer response
        prefix_evaluation_count = int(conn.execute(
            "SELECT COUNT(*) FROM paper_strategy_evaluations"
        ).fetchone()[0])
        total_queries += binding.reconstruction_queries

        strategy_timer_key = binding.prepare_clock_tick(
            BASE + timedelta(seconds=308), timer_id="STRATEGY-WATERMARK-TIMER"
        )
        pending_strategy = binding.execute_prepared_timer(strategy_timer_key)
        strategy_watermark = pending_strategy.source_watermark_p1_rowid
        premature_expiry = int(conn.execute(
            "SELECT COUNT(*) FROM paper_strategy_evaluations "
            "WHERE reason_code='ENTRY_WINDOW_EXPIRED'"
        ).fetchone()[0])
        strategy_pending_had_no_items = int(conn.execute(
            "SELECT COUNT(*) FROM paper_fp_binding_outbox_v0_1 "
            "WHERE input_key='TIMER-INPUT:STRATEGY-WATERMARK-TIMER'"
        ).fetchone()[0]) == 0
        conn.close()
        conn, binding = reopen(paper, fixture)
        restart_timer = conn.execute(
            "SELECT source_watermark_p1_rowid,status FROM "
            "paper_fp_binding_prepared_timers_v0_1 WHERE timer_key=?",
            (strategy_timer_key,),
        ).fetchone()
        checks["AP_strategy_clock_backlog_blocked"] = (
            pending_strategy.status == "PENDING"
            and strategy_watermark == 14
            and premature_expiry == 0
            and int(restart_timer["source_watermark_p1_rowid"]) == 14
            and str(restart_timer["status"]) == "PREPARED"
        )
        checks["AR_timer_watermark_restart"] = checks["AP_strategy_clock_backlog_blocked"]

        fail = FailOnce("P1:14", "after_runner_accept_before_outbox_delivery")
        conn.close()
        conn, binding = reopen(paper, fixture, fail)
        candidate_failed = expect_failure(binding, 1)
        candidate_cursor = binding.durable_p1_rowid
        candidate_routes = int(conn.execute("SELECT COUNT(*) FROM paper_entry_routes").fetchone()[0])
        total_queries += binding.reconstruction_queries
        conn.close()
        conn, binding = reopen(paper, fixture)
        binding.process_next_batch(batch_size=1)
        checks["U_candidate_retry_exactly_once"] = (
            candidate_failed and candidate_cursor == 13 and candidate_routes == 1
            and int(conn.execute("SELECT COUNT(*) FROM paper_entry_routes").fetchone()[0]) == 1
        )
        checks["AM_candidate_evaluation_retry_exact"] = int(conn.execute(
            "SELECT COUNT(*) FROM paper_strategy_evaluations "
            "WHERE mint='MINT-A' AND candidate_signal_id IS NOT NULL"
        ).fetchone()[0]) == 1
        total_queries += binding.reconstruction_queries

        copy_fixture_rows(template, fixture, through_rowid=15)
        fail = FailOnce(
            "TIMER-INPUT:STRATEGY-WATERMARK-TIMER",
            "after_evaluation_persistence_before_audit_delivery",
        )
        conn.close()
        conn, binding = reopen(paper, fixture, fail)
        clock_failed = False
        try:
            binding.execute_prepared_timer(strategy_timer_key)
        except RuntimeError as exc:
            clock_failed = str(exc).startswith("INJECTED:")
        conn.close()
        conn, binding = reopen(paper, fixture)
        timer = binding.execute_prepared_timer(strategy_timer_key)
        clock_evals = int(conn.execute(
            "SELECT COUNT(*) FROM paper_strategy_evaluations "
            "WHERE mint='MINT-B' AND reason_code='ENTRY_WINDOW_EXPIRED'"
        ).fetchone()[0])
        checks["AN_strategy_clock_retry_exact"] = clock_failed and clock_evals == 4
        checks["AU_new_rows_after_watermark_allowed"] = (
            timer.status == "COMMITTED" and timer.source_watermark_p1_rowid == 14
            and binding.durable_p1_rowid == 14
        )
        clock_retry_exact = checks["AN_strategy_clock_retry_exact"]
        conn.close()

        fail = FailOnce("P1:15", "after_runner_accept_before_outbox_delivery")
        conn, binding = reopen(paper, fixture, fail)
        market_failed = expect_failure(binding, 1)
        counts_failed = tuple(int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]) for table in
                              ("paper_orders", "paper_positions", "paper_position_mtm_marks"))
        total_queries += binding.reconstruction_queries
        conn.close()
        conn, binding = reopen(paper, fixture)
        binding.process_next_batch(batch_size=1)
        counts_retry = tuple(int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]) for table in
                             ("paper_orders", "paper_positions", "paper_position_mtm_marks"))
        checks["V_market_retry_exactly_once"] = market_failed and counts_failed == counts_retry
        checks["X_open_positions_restore"] = states(conn, "MINT-A") == {
            "FINAL-A": "OPEN", "FINAL-B": "OPEN", "SENS-C": "OPEN"
        }
        total_queries += binding.reconstruction_queries
    else:
        conn, binding = reopen(paper, fixture)
        binding.process_next_batch(batch_size=3)
        binding.process_next_batch(batch_size=9)
        binding.process_next_batch(batch_size=1)  # row 13 response
        prefix_evaluation_count = int(conn.execute(
            "SELECT COUNT(*) FROM paper_strategy_evaluations"
        ).fetchone()[0])
        strategy_timer_key = binding.prepare_clock_tick(
            BASE + timedelta(seconds=308), timer_id="STRATEGY-WATERMARK-TIMER"
        )
        pending_strategy = binding.execute_prepared_timer(strategy_timer_key)
        strategy_pending_had_no_items = int(conn.execute(
            "SELECT COUNT(*) FROM paper_fp_binding_outbox_v0_1 "
            "WHERE input_key='TIMER-INPUT:STRATEGY-WATERMARK-TIMER'"
        ).fetchone()[0]) == 0
        binding.process_next_batch(batch_size=1)  # row 14 candidate
        copy_fixture_rows(template, fixture, through_rowid=15)
        timer = binding.execute_prepared_timer(strategy_timer_key)
        binding.process_next_batch(batch_size=1)
        checks.update({
            "S_skip_only_batch_advances": binding.durable_p1_rowid >= 3,
            "T_cursor_stops_on_failure": True,
            "U_candidate_retry_exactly_once": True,
            "V_market_retry_exactly_once": True,
            "W_pending_state_restored": True,
            "X_open_positions_restore": True,
            "AL_evaluation_retry_no_duplicate": True,
            "AM_candidate_evaluation_retry_exact": True,
            "AN_strategy_clock_retry_exact": True,
            "AP_strategy_clock_backlog_blocked": pending_strategy.status == "PENDING",
            "AR_timer_watermark_restart": True,
            "AU_new_rows_after_watermark_allowed": timer.status == "COMMITTED",
        })
        total_queries += binding.reconstruction_queries

    # Exit-only clock captures row 16 and cannot overtake that backlog.
    copy_fixture_rows(template, fixture, through_rowid=16)
    exit_backlog_key = binding.prepare_exit_clock_tick(
        BASE + timedelta(seconds=27), timer_id="EXIT-WATERMARK-TIMER"
    )
    exit_pending = binding.execute_prepared_timer(exit_backlog_key)
    pre_drain_states = states(conn, "MINT-A")
    exit_pending_had_no_items = int(conn.execute(
        "SELECT COUNT(*) FROM paper_fp_binding_outbox_v0_1 "
        "WHERE input_key='TIMER-INPUT:EXIT-WATERMARK-TIMER'"
    ).fetchone()[0]) == 0
    binding.process_next_batch(batch_size=1)
    exit_timer = binding.execute_prepared_timer(exit_backlog_key)
    checks["AQ_exit_clock_backlog_blocked"] = (
        exit_pending.status == "PENDING"
        and exit_pending.source_watermark_p1_rowid == 16
        and exit_pending_had_no_items
        and pre_drain_states == {"FINAL-A": "OPEN", "FINAL-B": "OPEN", "SENS-C": "OPEN"}
        and exit_timer.status == "COMMITTED"
    )

    copy_fixture_rows(template, fixture, through_rowid=17)
    binding.process_next_batch(batch_size=1)  # post-tick A observation closes SENS-C
    mixed_a = states(conn, "MINT-A")
    checks["Y_closed_sibling_stays_closed"] = (
        mixed_a.get("SENS-C") == "CLOSED"
        and mixed_a.get("FINAL-A") == "OPEN"
        and mixed_a.get("FINAL-B") == "OPEN"
    )
    total_queries += binding.reconstruction_queries
    conn.close()
    conn, binding = reopen(paper, fixture)
    checks["Y_closed_sibling_stays_closed"] &= states(conn, "MINT-A") == mixed_a

    # A second mint completes the same accepted CONTROL path while A stays open.
    copy_fixture_rows(template, fixture, through_rowid=27)
    binding.process_next_batch(batch_size=10)
    before_tick = states(conn, "MINT-C")
    binding.submit_exit_clock_tick(BASE + timedelta(seconds=43))
    copy_fixture_rows(template, fixture, through_rowid=29)
    binding.process_next_batch(batch_size=2)  # A closes remaining; C closes SENS-C
    binding.submit_exit_clock_tick(BASE + timedelta(seconds=53))
    copy_fixture_rows(template, fixture, through_rowid=30)
    binding.process_next_batch(batch_size=1)  # C closes remaining
    # Gap + unknown future gap + mint whose only launch was gap recovery.
    copy_fixture_rows(template, fixture, through_rowid=34)
    tail = binding.process_next_batch(batch_size=3)
    total_queries += binding.reconstruction_queries

    runtime = conn.execute("SELECT * FROM paper_fp_binding_runtime_v0_1 WHERE singleton=1").fetchone()
    outbox = conn.execute(
        "SELECT event_sequence,input_key,event_type,event_key,delivered FROM "
        "paper_fp_binding_outbox_v0_1 ORDER BY event_sequence"
    ).fetchall()
    sequences = tuple(int(r["event_sequence"]) for r in outbox)
    candidate_rows = conn.execute(
        "SELECT role,mint FROM paper_strategy_evaluations "
        "WHERE candidate_signal_id IS NOT NULL ORDER BY evaluated_at_us,evaluation_id"
    ).fetchall()
    candidate_order = tuple(f"{r['mint']}:{r['role']}" for r in candidate_rows)
    decision_counts = {
        str(r["trade_or_skip"]): int(r["n"])
        for r in conn.execute(
            "SELECT trade_or_skip,COUNT(*) n FROM paper_terminal_trade_decisions GROUP BY trade_or_skip"
        ).fetchall()
    }
    all_states = {str(r["state"]) for r in conn.execute("SELECT state FROM paper_positions")}
    marks_by_mint = {
        str(r["mint"]): int(r["n"])
        for r in conn.execute(
            "SELECT mint,COUNT(*) n FROM paper_position_mtm_marks GROUP BY mint"
        ).fetchall()
    }
    gap_feature = conn.execute(
        "SELECT event_json FROM paper_fp_binding_feature_events_v0_1 WHERE production_p1_rowid=31"
    ).fetchone()
    gap_outbox = conn.execute(
        "SELECT event_json FROM paper_fp_binding_outbox_v0_1 WHERE input_key='P1:31'"
    ).fetchall()
    timer_terminal = int(conn.execute(
        "SELECT COUNT(*) FROM paper_terminal_trade_decisions d "
        "JOIN paper_strategy_evaluations e ON e.evaluation_id=d.source_evaluation_id "
        "WHERE d.mint='MINT-B' AND d.trade_or_skip='SKIP' AND e.reason_code='ENTRY_WINDOW_EXPIRED'"
    ).fetchone()[0])
    report = build_report(conn)
    trades = load_completed_trades(conn)
    realized_by_track = {
        track: sum(t.net_pnl_lamports for t in trades if t.track_id == track)
        for track in ("FINAL-A", "FINAL-B", "SENS-C")
    }
    report_by_track = {t.track_id: t for t in report.tracks}
    params = locked_parameter_sets()
    exact_parameters = {
        role: (
            p.parameter_set_id,
            p.max_entry_age_ms,
            p.impulse_parameters,
            p.pullback_parameters,
            p.buyer_response_parameters,
            p.reclaim_parameters,
            p.runaway_entry_parameters,
        )
        for role, p in params.items()
    }
    expected_parameters = {
        "CONTROL": (LOCKED_PARAMETER_SET_BY_ROLE["CONTROL"], 300_000,
            {"min_return_bps": 500, "min_trades_since_t0": 2, "min_unique_buyers_since_t0": 1},
            {"min_depth_bps": 1000, "max_depth_bps": 9000},
            {"window_id": "3s", "min_rebound_bps": 200, "min_buys": 1, "min_net_flow_reserve_ppm": 0},
            {"min_extension_from_response_bps": 200},
            {"max_extension_from_response_bps": 10000}),
        "ROBUST_1": (LOCKED_PARAMETER_SET_BY_ROLE["ROBUST_1"], 300_000,
            {"min_return_bps": 1500, "min_trades_since_t0": 5, "min_unique_buyers_since_t0": 9},
            {"min_depth_bps": 2500, "max_depth_bps": 6500},
            {"window_id": "3s", "min_rebound_bps": 2300, "min_buys": 2, "min_net_flow_reserve_ppm": 6000},
            {"min_extension_from_response_bps": 200},
            {"max_extension_from_response_bps": 2500}),
        "ROBUST_2": (LOCKED_PARAMETER_SET_BY_ROLE["ROBUST_2"], 300_000,
            {"min_return_bps": 2000, "min_trades_since_t0": 3, "min_unique_buyers_since_t0": 5},
            {"min_depth_bps": 2500, "max_depth_bps": 6500},
            {"window_id": "3s", "min_rebound_bps": 2300, "min_buys": 2, "min_net_flow_reserve_ppm": 6000},
            {"min_extension_from_response_bps": 200},
            {"max_extension_from_response_bps": 2500}),
        "ROBUST_3": (LOCKED_PARAMETER_SET_BY_ROLE["ROBUST_3"], 300_000,
            {"min_return_bps": 5000, "min_trades_since_t0": 3, "min_unique_buyers_since_t0": 2},
            {"min_depth_bps": 2500, "max_depth_bps": 6500},
            {"window_id": "3s", "min_rebound_bps": 2300, "min_buys": 2, "min_net_flow_reserve_ppm": 6000},
            {"min_extension_from_response_bps": 200},
            {"max_extension_from_response_bps": 2500}),
    }
    intermediate = conn.execute(
        "SELECT current_state,reason_code,filters_passed_json,filters_failed_json,"
        "audit_snapshot_json FROM paper_strategy_evaluations "
        "WHERE mint='MINT-A' AND role='CONTROL' "
        "AND reason_code='IMPULSE_CONFIRMED' LIMIT 1"
    ).fetchone()
    ledger_total = int(conn.execute(
        "SELECT COUNT(*) FROM paper_fp_binding_evaluation_audit_v0_1"
    ).fetchone()[0])
    accepted_total = int(conn.execute(
        "SELECT COUNT(*) FROM paper_strategy_evaluations"
    ).fetchone()[0])
    before_timer_replay = (
        accepted_total,
        int(conn.execute("SELECT COUNT(*) FROM paper_terminal_trade_decisions").fetchone()[0]),
        len(outbox),
    )
    replay_timer = binding.execute_prepared_timer("STRATEGY-WATERMARK-TIMER")
    after_timer_replay = (
        int(conn.execute("SELECT COUNT(*) FROM paper_strategy_evaluations").fetchone()[0]),
        int(conn.execute("SELECT COUNT(*) FROM paper_terminal_trade_decisions").fetchone()[0]),
        int(conn.execute("SELECT COUNT(*) FROM paper_fp_binding_outbox_v0_1").fetchone()[0]),
    )
    immutable_conflicts = 0
    for action in (
        lambda: binding.prepare_clock_tick(
            BASE + timedelta(seconds=309), timer_id="STRATEGY-WATERMARK-TIMER"
        ),
        lambda: binding.prepare_clock_tick(
            BASE + timedelta(seconds=308), drive_exit_clock=False,
            timer_id="STRATEGY-WATERMARK-TIMER",
        ),
        lambda: binding.prepare_exit_clock_tick(
            BASE + timedelta(seconds=308), timer_id="STRATEGY-WATERMARK-TIMER"
        ),
    ):
        try:
            action()
        except BindingConflict:
            immutable_conflicts += 1

    checks.update({
        "A_exact_binding_identity": MODEL_ID == "P4-CONTINUOUS-FIRSTPULLBACK-BINDING-0001"
            and MODEL_FINGERPRINT == EXPECTED_BINDING_FINGERPRINT
            and str(runtime["binding_fingerprint"]) == MODEL_FINGERPRINT,
        "B_market_source_bound": MARKET_SOURCE_FINGERPRINT == EXPECTED_SOURCE_FINGERPRINT,
        "C_runner_bound": RUNNER_SPEC_FINGERPRINT == EXPECTED_RUNNER_FINGERPRINT,
        "D_firstpullback_contract_bound": (
            FirstPullbackStrategyV02.strategy_version == "v1.1"
            and FirstPullbackStrategyClockV02.schema_version == "SCLOCK-0.2"
            and DenominationAwareFeatureEngineV01.schema_version == "MS-DAF-0.1"
            and LOCKED_ROLE_ORDER == ("CONTROL", "ROBUST_1", "ROBUST_2", "ROBUST_3")
            and LOCKED_SELECTION_SHA256 == "408657c1d6dc39435b61e01b368dac69796481864b7462efd150a2d4e1b0daa1"
            and exact_parameters == expected_parameters
        ),
        "E_fresh_launch_eligible": int(conn.execute(
            "SELECT COUNT(*) FROM paper_fp_binding_strategy_runs_v0_1 WHERE mint='MINT-A'"
        ).fetchone()[0]) == 4,
        "F_unlaunched_excluded": int(conn.execute(
            "SELECT COUNT(*) FROM paper_fp_binding_strategy_runs_v0_1 WHERE mint='UNLAUNCHED'"
        ).fetchone()[0]) == 0,
        "G_gap_launch_excluded": int(conn.execute(
            "SELECT COUNT(*) FROM paper_fp_binding_strategy_runs_v0_1 WHERE mint='GAP-LAUNCH'"
        ).fetchone()[0]) == 0,
        "H_v034_gap_compatibility": GAP_COMPAT_FINGERPRINT == EXPECTED_GAP_FINGERPRINT
            and gap_feature is not None and '"source":"GAP_RECOVERY"' in str(gap_feature[0]),
        "I_first_candidate_exact": candidate_order.count("MINT-A:CONTROL") == 1,
        "J_candidate_runner_event": int(conn.execute(
            "SELECT COUNT(*) FROM paper_fp_binding_outbox_v0_1 "
            "WHERE input_key='P1:14' AND event_type='CandidateEvaluationEvent'"
        ).fetchone()[0]) == 1,
        "K_exact_three_tracks": states(conn, "MINT-A").keys() == {"FINAL-A", "FINAL-B", "SENS-C"},
        "L_second_mint_terminal_skip": timer_terminal == 4,
        "M_later_candidate_same_runtime": candidate_order.count("MINT-C:CONTROL") == 1,
        "N_overlapping_mints_isolated": before_tick == {
            "FINAL-A": "OPEN", "FINAL-B": "OPEN", "SENS-C": "OPEN"
        } and len({str(r["mint"]) for r in conn.execute(
            "SELECT DISTINCT mint FROM paper_fp_binding_strategy_runs_v0_1"
        )}) == 3,
        "O_market_updates_open_positions": marks_by_mint.get("MINT-A", 0) > 0
            and marks_by_mint.get("MINT-C", 0) > 0,
        "P_same_row_strategy_before_market": tuple(
            str(r["event_type"]) for r in conn.execute(
                "SELECT event_type FROM paper_fp_binding_outbox_v0_1 "
                "WHERE input_key='P1:14' ORDER BY ordinal"
            ).fetchall()
        ) == ("CandidateEvaluationEvent", "MarketObservationEvent"),
        "Q_explicit_strategy_expiry": timer.strategy_terminal_items >= 4 and timer_terminal == 4,
        "R_explicit_exit_clock_replay_stable": int(conn.execute(
            "SELECT COUNT(*) FROM paper_fp_binding_prepared_timers_v0_1 "
            "WHERE clock_type='EXIT_CLOCK' AND status='COMPLETE'"
        ).fetchone()[0]) == 3,
        "Z_event_sequence_stable": sequences == tuple(range(len(sequences)))
            and all(int(r["delivered"]) == 1 for r in outbox),
        "AC_gap_excluded_fresh_flow": gap_feature is not None
            and all('"is_gap_recovery":true' in str(r[0]) for r in gap_outbox),
        "AD_unknown_gap_unsupported": tail.deterministic_skips == 2,
        "AE_accounting_reconciles_per_track": all_states == {"CLOSED"}
            and len(trades) == 6
            and all(report_by_track[k].final_equity_pnl_lamports == v
                    for k, v in realized_by_track.items()),
        "AF_cross_track_summation_prohibited": report.cross_track_aggregation_status
            == "PROHIBITED_ALTERNATIVE_EXIT_TRACKS_REPORTED_SEPARATELY",
        "AI_all_nonterminal_evaluations_audited": prefix_evaluation_count == 40
            and ledger_total == accepted_total,
        "AJ_exact_prefix_evaluation_count": int(conn.execute(
            "SELECT COUNT(*) FROM paper_fp_binding_evaluation_audit_v0_1 a "
            "JOIN paper_fp_binding_inputs_v0_1 i ON i.input_key=a.input_key "
            "WHERE i.production_p1_rowid<=13"
        ).fetchone()[0]) == 40,
        "AK_intermediate_transition_audit_exact": intermediate is not None
            and str(intermediate["current_state"]) == "IMPULSE_CONFIRMED"
            and str(intermediate["reason_code"]) == "IMPULSE_CONFIRMED"
            and "impulse_return" in str(intermediate["filters_passed_json"])
            and str(intermediate["filters_failed_json"]) == "[]"
            and "triggering_event_key" in str(intermediate["audit_snapshot_json"]),
        "AL_evaluation_retry_no_duplicate": checks["AL_evaluation_retry_no_duplicate"]
            and int(conn.execute(
                "SELECT COUNT(*) FROM paper_strategy_evaluations WHERE ingest_seq=4"
            ).fetchone()[0]) == 4,
        "AM_candidate_evaluation_retry_exact": checks["AM_candidate_evaluation_retry_exact"]
            and candidate_order.count("MINT-A:CONTROL") == 1,
        "AN_strategy_clock_retry_exact": clock_retry_exact and timer_terminal == 4,
        "AO_terminal_behavior_unchanged": decision_counts == {"SKIP": 4, "TRADE": 2},
        "AP_strategy_clock_backlog_blocked": checks["AP_strategy_clock_backlog_blocked"]
            and strategy_pending_had_no_items
            and candidate_order.count("MINT-A:CONTROL") == 1
            and int(conn.execute(
                "SELECT COUNT(*) FROM paper_strategy_evaluations "
                "WHERE mint='MINT-A' AND reason_code='ENTRY_WINDOW_EXPIRED'"
            ).fetchone()[0]) == 0,
        "AT_timer_meaning_conflicts_fail_closed": immutable_conflicts == 3,
        "AV_prepared_timer_replay_idempotent": replay_timer.status == "COMMITTED"
            and before_timer_replay == after_timer_replay,
    })
    quick = str(conn.execute("PRAGMA quick_check").fetchone()[0]).lower()
    checks["AG_sqlite_quick_check"] = quick == "ok"

    digest = binding.canonical_digest()
    conn.close()

    # Identity conflicts are checked against copied, otherwise valid state.
    conflict = root / "conflict.sqlite3"
    shutil.copy2(paper, conflict)
    c = sqlite3.connect(conflict)
    c.execute("UPDATE paper_fp_binding_runtime_v0_1 SET binding_fingerprint='CONFLICT'")
    c.commit(); c.close()
    c = connect_entry_router_db(conflict)
    try:
        ContinuousFirstPullbackBindingV01(c, source(fixture), wall_clock=lambda: BASE)
        checks["AA_binding_conflict_fails_closed"] = False
    except BindingConflict:
        checks["AA_binding_conflict_fails_closed"] = True
    finally:
        c.close()

    watermark_conflict = root / "watermark_conflict.sqlite3"
    shutil.copy2(paper, watermark_conflict)
    c = sqlite3.connect(watermark_conflict)
    c.execute(
        "UPDATE paper_fp_binding_prepared_timers_v0_1 "
        "SET source_watermark_p1_rowid=source_watermark_p1_rowid+1 "
        "WHERE timer_key='STRATEGY-WATERMARK-TIMER'"
    )
    c.commit(); c.close()
    c = connect_entry_router_db(watermark_conflict)
    try:
        ContinuousFirstPullbackBindingV01(c, source(fixture), wall_clock=lambda: BASE)
        checks["AS_timer_watermark_conflict"] = False
    except BindingConflict:
        checks["AS_timer_watermark_conflict"] = True
    finally:
        c.close()

    identity_conflict = root / "identity_conflict.sqlite3"
    shutil.copy2(paper, identity_conflict)
    c = connect_entry_router_db(identity_conflict)
    try:
        ContinuousFirstPullbackBindingV01(
            c, source(fixture, identity="DIFFERENT"), wall_clock=lambda: BASE
        )
        checks["AB_source_identity_anchor_conflict"] = False
    except BindingConflict:
        checks["AB_source_identity_anchor_conflict"] = True
    finally:
        c.close()

    return Evidence(checks, digest, total_queries, candidate_order, quick)


def run_timer_fence_scenario(root: Path) -> FenceEvidence:
    root.mkdir(parents=True, exist_ok=False)
    template = root / "source_template.sqlite3"
    create_fixture(template)
    checks: dict[str, bool] = {}
    component_digests: list[str] = []

    # AX/BF/BC/BD: restart with a strategy fence at 14, expose 14..16,
    # and prove a large batch can consume only the fenced prefix.
    strategy_source = root / "strategy_source.sqlite3"
    strategy_paper = root / "strategy_paper.sqlite3"
    copy_fixture_rows(template, strategy_source, through_rowid=14)
    conn, binding = reopen(strategy_paper, strategy_source)
    binding.process_next_batch(batch_size=3)
    binding.process_next_batch(batch_size=9)
    binding.process_next_batch(batch_size=1)
    strategy_key = binding.prepare_clock_tick(
        BASE + timedelta(seconds=308),
        timer_id="C2-STRATEGY-FENCE",
    )
    conn.close()
    copy_fixture_rows(template, strategy_source, through_rowid=16)
    conn, binding = reopen(strategy_paper, strategy_source)
    restored_strategy_fence = binding.active_timer_fence_p1_rowid
    strategy_batch = binding.process_next_batch(batch_size=1_000)
    strategy_fenced = binding.process_next_batch(batch_size=1_000)
    timer_row = conn.execute(
        "SELECT status FROM paper_fp_binding_prepared_timers_v0_1 "
        "WHERE timer_key=?", (strategy_key,)
    ).fetchone()
    over_fence_side_effects = (
        int(conn.execute(
            "SELECT COUNT(*) FROM paper_fp_binding_inputs_v0_1 "
            "WHERE production_p1_rowid IN (15,16)"
        ).fetchone()[0]),
        int(conn.execute(
            "SELECT COUNT(*) FROM paper_fp_binding_feature_events_v0_1 "
            "WHERE production_p1_rowid IN (15,16)"
        ).fetchone()[0]),
        int(conn.execute(
            "SELECT COUNT(*) FROM paper_fp_binding_evaluation_audit_v0_1 a "
            "JOIN paper_fp_binding_inputs_v0_1 i ON i.input_key=a.input_key "
            "WHERE i.production_p1_rowid IN (15,16)"
        ).fetchone()[0]),
        int(conn.execute(
            "SELECT COUNT(*) FROM paper_fp_binding_outbox_v0_1 o "
            "JOIN paper_fp_binding_inputs_v0_1 i ON i.input_key=o.input_key "
            "WHERE i.production_p1_rowid IN (15,16)"
        ).fetchone()[0]),
        int(conn.execute(
            "SELECT COUNT(*) FROM paper_position_mtm_marks WHERE ingest_seq IN (15,16)"
        ).fetchone()[0]),
        int(conn.execute(
            "SELECT COUNT(*) FROM paper_track_equity_marks WHERE ingest_seq IN (15,16)"
        ).fetchone()[0]),
        int(conn.execute("SELECT COUNT(*) FROM paper_positions").fetchone()[0]),
        int(conn.execute(
            "SELECT COUNT(*) FROM paper_fp_binding_strategy_runs_v0_1 "
            "WHERE last_ingest_seq>14"
        ).fetchone()[0]),
    )
    checks["AX_strategy_timer_batch_overrun_blocked"] = (
        restored_strategy_fence == 14
        and strategy_batch.durable_p1_rowid == 14
        and strategy_batch.raw_rows_fetched == 1
        and strategy_fenced.raw_rows_fetched == 0
        and binding.durable_p1_rowid == 14
        and str(timer_row["status"]) == "PREPARED"
        and binding.active_timer_fence_p1_rowid == 14
    )
    checks["BF_no_over_fence_side_effects"] = over_fence_side_effects == (0,) * 8
    checks["BD_restart_restores_active_fence"] = (
        restored_strategy_fence == 14 and strategy_fenced.durable_p1_rowid == 14
    )
    strategy_timer = binding.execute_prepared_timer(strategy_key)
    strategy_release = binding.process_next_batch(batch_size=1_000)
    checks["BC_complete_timer_releases_fence"] = (
        strategy_timer.status == "COMMITTED"
        and binding.active_timer_fence_p1_rowid is None
        and strategy_release.durable_p1_rowid == 16
        and int(conn.execute(
            "SELECT COUNT(*) FROM paper_fp_binding_inputs_v0_1 "
            "WHERE production_p1_rowid IN (15,16) AND complete=1"
        ).fetchone()[0]) == 2
    )
    component_digests.append(binding.canonical_digest())
    conn.close()

    # AY: an exit-only timer at 16 must run before row 17 can select/fill its
    # fallback exit, even when the caller requests a large batch.
    exit_source = root / "exit_source.sqlite3"
    exit_paper = root / "exit_paper.sqlite3"
    copy_fixture_rows(template, exit_source, through_rowid=15)
    conn, binding = reopen(exit_paper, exit_source)
    binding.process_next_batch(batch_size=1_000)
    open_before_exit_fence = states(conn, "MINT-A")
    exit_source_conn = sqlite3.connect(exit_source)
    insert_row(
        exit_source_conn, 16, "MINT-A", "BUY",
        price_units=10_100, observed_offset=16,
    )
    exit_source_conn.commit()
    exit_source_conn.close()
    exit_key = binding.prepare_exit_clock_tick(
        BASE + timedelta(seconds=20), timer_id="C2-EXIT-FENCE"
    )
    exit_source_conn = sqlite3.connect(exit_source)
    insert_row(
        exit_source_conn, 17, "MINT-A", "BUY",
        price_units=14_000, observed_offset=21,
    )
    exit_source_conn.commit()
    exit_source_conn.close()
    exit_batch = binding.process_next_batch(batch_size=1_000)
    exit_fenced = binding.process_next_batch(batch_size=1_000)
    row17_before_clock = int(conn.execute(
        "SELECT COUNT(*) FROM paper_fp_binding_inputs_v0_1 "
        "WHERE production_p1_rowid=17"
    ).fetchone()[0])
    states_before_clock = states(conn, "MINT-A")
    exit_timer = binding.execute_prepared_timer(exit_key)
    states_after_clock = states(conn, "MINT-A")
    row17_batch = binding.process_next_batch(batch_size=1_000)
    states_after_row17 = states(conn, "MINT-A")
    exit_routes = {
        str(row["track_id"]): (str(row["exit_reason"]), str(row["state"]))
        for row in conn.execute(
            "SELECT track_id,exit_reason,state FROM paper_exit_execution_routes "
            "WHERE mint='MINT-A' ORDER BY track_id"
        ).fetchall()
    }
    checks["AY_exit_timer_batch_overrun_blocked"] = (
        open_before_exit_fence
        == {"FINAL-A": "OPEN", "FINAL-B": "OPEN", "SENS-C": "OPEN"}
        and exit_batch.durable_p1_rowid == 16
        and exit_batch.raw_rows_fetched == 1
        and exit_fenced.raw_rows_fetched == 0
        and row17_before_clock == 0
        and states_before_clock
        == {"FINAL-A": "OPEN", "FINAL-B": "OPEN", "SENS-C": "OPEN"}
        and exit_timer.status == "COMMITTED"
        and states_after_clock
        == {"FINAL-A": "OPEN", "FINAL-B": "OPEN", "SENS-C": "EXIT_PENDING"}
        and row17_batch.durable_p1_rowid == 17
        and states_after_row17.get("SENS-C") == "CLOSED"
        and exit_routes.get("SENS-C") == ("FALLBACK", "FILLED")
    )
    component_digests.append(binding.canonical_digest())
    conn.close()

    # AZ/BA/BB/BC/BD/BE: two timers at 3 jointly hold the first fence;
    # a later timer at 6 becomes PLANNED after partial delivery and continues
    # to hold its fence across restart.
    multi_source = root / "multi_source.sqlite3"
    multi_paper = root / "multi_paper.sqlite3"
    copy_fixture_rows(template, multi_source, through_rowid=3)
    conn, binding = reopen(multi_paper, multi_source)
    same_a = binding.prepare_clock_tick(
        BASE, drive_exit_clock=False, timer_id="C2-SAME-A"
    )
    same_b = binding.prepare_exit_clock_tick(BASE, timer_id="C2-SAME-B")
    copy_fixture_rows(template, multi_source, through_rowid=6)
    later = binding.prepare_exit_clock_tick(BASE, timer_id="C2-LATER")
    copy_fixture_rows(template, multi_source, through_rowid=7)
    conn.close()

    conn, binding = reopen(multi_paper, multi_source)
    restored_same_fence = binding.active_timer_fence_p1_rowid
    through_same_fence = binding.process_next_batch(batch_size=1_000)
    binding.execute_prepared_timer(same_a)
    held_by_second_same = binding.process_next_batch(batch_size=1_000)
    row4_while_same_active = int(conn.execute(
        "SELECT COUNT(*) FROM paper_fp_binding_inputs_v0_1 "
        "WHERE production_p1_rowid=4"
    ).fetchone()[0])
    binding.execute_prepared_timer(same_b)
    through_later_fence = binding.process_next_batch(batch_size=1_000)
    row7_before_later = int(conn.execute(
        "SELECT COUNT(*) FROM paper_fp_binding_inputs_v0_1 "
        "WHERE production_p1_rowid=7"
    ).fetchone()[0])
    checks["AZ_multiple_timers_same_fence"] = (
        restored_same_fence == 3
        and through_same_fence.durable_p1_rowid == 3
        and held_by_second_same.raw_rows_fetched == 0
        and row4_while_same_active == 0
        and binding.active_timer_fence_p1_rowid == 6
    )
    checks["BA_later_timer_fence"] = (
        through_later_fence.durable_p1_rowid == 6
        and row7_before_later == 0
        and binding.active_timer_fence_p1_rowid == 6
    )
    conn.close()

    fail = FailOnce(
        "TIMER-INPUT:C2-LATER", "after_runner_accept_before_outbox_delivery"
    )
    conn, binding = reopen(multi_paper, multi_source, fail)
    planned_failed = False
    try:
        binding.execute_prepared_timer(later)
    except RuntimeError as exc:
        planned_failed = str(exc).startswith("INJECTED:")
    planned_status = str(conn.execute(
        "SELECT status FROM paper_fp_binding_prepared_timers_v0_1 "
        "WHERE timer_key=?", (later,)
    ).fetchone()[0])
    planned_fenced = binding.process_next_batch(batch_size=1_000)
    checks["BB_planned_timer_still_fences"] = (
        planned_failed
        and planned_status == "PLANNED"
        and planned_fenced.raw_rows_fetched == 0
        and binding.durable_p1_rowid == 6
    )
    conn.close()

    invalid = root / "invalid_historical.sqlite3"
    shutil.copy2(multi_paper, invalid)
    invalid_conn = sqlite3.connect(invalid)
    invalid_conn.execute(
        "UPDATE paper_fp_binding_runtime_v0_1 SET last_durable_p1_rowid=7 "
        "WHERE singleton=1"
    )
    invalid_conn.commit()
    invalid_conn.close()
    invalid_conn = connect_entry_router_db(invalid)
    try:
        ContinuousFirstPullbackBindingV01(
            invalid_conn, source(multi_source), wall_clock=lambda: BASE
        )
        checks["BE_invalid_historical_state_fails_closed"] = False
    except BindingConflict:
        checks["BE_invalid_historical_state_fails_closed"] = True
    finally:
        invalid_conn.close()

    conn, binding = reopen(multi_paper, multi_source)
    restored_planned_fence = binding.active_timer_fence_p1_rowid
    restart_still_fenced = binding.process_next_batch(batch_size=1_000)
    binding.execute_prepared_timer(later)
    released_row7 = binding.process_next_batch(batch_size=1_000)
    checks["BD_restart_restores_active_fence"] &= (
        restored_planned_fence == 6
        and restart_still_fenced.raw_rows_fetched == 0
        and released_row7.durable_p1_rowid == 7
    )
    checks["BC_complete_timer_releases_fence"] &= (
        binding.active_timer_fence_p1_rowid is None
        and int(conn.execute(
            "SELECT COUNT(*) FROM paper_fp_binding_inputs_v0_1 "
            "WHERE production_p1_rowid=7 AND complete=1"
        ).fetchone()[0]) == 1
    )
    component_digests.append(binding.canonical_digest())
    conn.close()

    digest = hashlib.sha256("|".join(component_digests).encode()).hexdigest()
    return FenceEvidence(checks, digest)


LABELS = [
    ("A", "A_exact_binding_identity"), ("B", "B_market_source_bound"),
    ("C", "C_runner_bound"), ("D", "D_firstpullback_contract_bound"),
    ("E", "E_fresh_launch_eligible"), ("F", "F_unlaunched_excluded"),
    ("G", "G_gap_launch_excluded"), ("H", "H_v034_gap_compatibility"),
    ("I", "I_first_candidate_exact"), ("J", "J_candidate_runner_event"),
    ("K", "K_exact_three_tracks"), ("L", "L_second_mint_terminal_skip"),
    ("M", "M_later_candidate_same_runtime"), ("N", "N_overlapping_mints_isolated"),
    ("O", "O_market_updates_open_positions"), ("P", "P_same_row_strategy_before_market"),
    ("Q", "Q_explicit_strategy_expiry"), ("R", "R_explicit_exit_clock_replay_stable"),
    ("S", "S_skip_only_batch_advances"), ("T", "T_cursor_stops_on_failure"),
    ("U", "U_candidate_retry_exactly_once"), ("V", "V_market_retry_exactly_once"),
    ("W", "W_pending_state_restored"), ("X", "X_open_positions_restore"),
    ("Y", "Y_closed_sibling_stays_closed"), ("Z", "Z_event_sequence_stable"),
    ("AA", "AA_binding_conflict_fails_closed"),
    ("AB", "AB_source_identity_anchor_conflict"),
    ("AC", "AC_gap_excluded_fresh_flow"), ("AD", "AD_unknown_gap_unsupported"),
    ("AE", "AE_accounting_reconciles_per_track"),
    ("AF", "AF_cross_track_summation_prohibited"),
    ("AG", "AG_sqlite_quick_check"), ("AH", "AH_repeated_digest_identical"),
    ("AI", "AI_all_nonterminal_evaluations_audited"),
    ("AJ", "AJ_exact_prefix_evaluation_count"),
    ("AK", "AK_intermediate_transition_audit_exact"),
    ("AL", "AL_evaluation_retry_no_duplicate"),
    ("AM", "AM_candidate_evaluation_retry_exact"),
    ("AN", "AN_strategy_clock_retry_exact"),
    ("AO", "AO_terminal_behavior_unchanged"),
    ("AP", "AP_strategy_clock_backlog_blocked"),
    ("AQ", "AQ_exit_clock_backlog_blocked"),
    ("AR", "AR_timer_watermark_restart"),
    ("AS", "AS_timer_watermark_conflict"),
    ("AT", "AT_timer_meaning_conflicts_fail_closed"),
    ("AU", "AU_new_rows_after_watermark_allowed"),
    ("AV", "AV_prepared_timer_replay_idempotent"),
    ("AW", "AW_complete_digest_identical"),
    ("AX", "AX_strategy_timer_batch_overrun_blocked"),
    ("AY", "AY_exit_timer_batch_overrun_blocked"),
    ("AZ", "AZ_multiple_timers_same_fence"),
    ("BA", "BA_later_timer_fence"),
    ("BB", "BB_planned_timer_still_fences"),
    ("BC", "BC_complete_timer_releases_fence"),
    ("BD", "BD_restart_restores_active_fence"),
    ("BE", "BE_invalid_historical_state_fails_closed"),
    ("BF", "BF_no_over_fence_side_effects"),
    ("BG", "BG_complete_fence_digest_identical"),
]


def main() -> int:
    root = Path(tempfile.mkdtemp(prefix="phase4_fp_binding_v01_"))
    try:
        first = run_scenario(root / "with_failures", failures=True)
        second = run_scenario(root / "clean_repeat", failures=False)
        fence_first = run_timer_fence_scenario(root / "fence_first")
        fence_second = run_timer_fence_scenario(root / "fence_repeat")
        first.checks["AH_repeated_digest_identical"] = first.digest == second.digest
        first.checks["AW_complete_digest_identical"] = first.digest == second.digest
        first.checks.update(fence_first.checks)
        first.checks["BG_complete_fence_digest_identical"] = (
            fence_first.digest == fence_second.digest
        )
        print("=" * 124)
        print("PHASE 4 - CONTINUOUS FIRSTPULLBACK BINDING FOUNDATION SELF-TEST v0.1")
        print("=" * 124)
        print(f"Binding model                       : {MODEL_ID}")
        print(f"Binding fingerprint                 : {MODEL_FINGERPRINT}")
        print(f"Market-source fingerprint bound     : {MARKET_SOURCE_FINGERPRINT}")
        print(f"Runner fingerprint bound            : {RUNNER_SPEC_FINGERPRINT}")
        print("Production DB / collector / network : NO")
        print(f"Reconstruction queries (failure run): {first.reconstruction_queries}")
        print(f"Canonical digest                    : {first.digest}")
        print(f"Timer-fence digest                  : {fence_first.digest}")
        print()
        ok = True
        for label, key in LABELS:
            passed = bool(first.checks.get(key, False))
            ok &= passed
            print(f"{label:>2}. {key:<48} : {'PASS' if passed else 'FAIL'}")
        print()
        print(f"RESULT: {'PASS' if ok else 'FAIL'}")
        return 0 if ok else 1
    finally:
        shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
