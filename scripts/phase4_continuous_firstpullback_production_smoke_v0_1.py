from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import signal
import sqlite3
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
from typing import Any, Sequence


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from phase4.paper_continuous_firstpullback_binding_v0_1 import (  # noqa: E402
    GAP_COMPAT_FINGERPRINT,
    LOCKED_ROLE_ORDER,
    LOCKED_SELECTION_SHA256,
    MARKET_SOURCE_FINGERPRINT,
    MODEL_FINGERPRINT as BINDING_FINGERPRINT,
    MODEL_ID as BINDING_MODEL_ID,
    RUNNER_SPEC_FINGERPRINT,
    BindingConflict,
    ContinuousFirstPullbackBindingV01,
)
from phase4.paper_continuous_market_source_v0_1 import (  # noqa: E402
    ContinuousMarketSourceV01,
)
from phase4.paper_cost_baseline_v0_1 import P4_COST_BASELINE_0001  # noqa: E402
from phase4.paper_entry_router_v0_1 import connect_entry_router_db  # noqa: E402
from phase4.paper_runtime_observability_v0_1 import (  # noqa: E402
    build_report as build_observability_report,
)
from phase4.paper_trade_accounting_v0_1 import (  # noqa: E402
    build_report as build_accounting_report,
    load_completed_trades,
)


MODEL_ID = "P4-CONTINUOUS-FIRSTPULLBACK-PRODUCTION-SMOKE-0001"
MIN_RUNTIME_SECONDS = 300.0
MAX_RUNTIME_SECONDS = 1_200.0
MARKET_POLL_SECONDS = 0.5
CLOCK_CADENCE_SECONDS = 1.0
BATCH_SIZE = 250
COLLECTOR_DRAIN_SECONDS = 10
COLLECTOR_CLEANUP_TIMEOUT_SECONDS = 120.0
COLLECTOR_MAX_PUMP_EVENTS = 500
PRODUCTION_DB = PROJECT_ROOT / "data" / "db" / "tradingbot.sqlite3"
RUNTIME_DIR = PROJECT_ROOT / "data" / "paper" / "live_smoke"
COST_BASELINE_ID = "P4-COST-BASELINE-0001"
COST_BASELINE_FINGERPRINT = (
    "9652aeffc792eb48d62348360c6ae804f660e43329195d1f169219b80ccab67c"
)
EXIT_TRACKS = {
    "FINAL-A": "E3_TP10_T15",
    "FINAL-B": "E4_ACT10_GB03_T15",
    "SENS-C": "SENS_TP20_T5",
}

SMOKE_SPEC = {
    "model_id": MODEL_ID,
    "binding_model_id": BINDING_MODEL_ID,
    "binding_fingerprint": BINDING_FINGERPRINT,
    "market_source_fingerprint": MARKET_SOURCE_FINGERPRINT,
    "gap_compatibility_fingerprint": GAP_COMPAT_FINGERPRINT,
    "continuous_runner_fingerprint": RUNNER_SPEC_FINGERPRINT,
    "firstpullback_selection_sha256": LOCKED_SELECTION_SHA256,
    "role_order": LOCKED_ROLE_ORDER,
    "cost_baseline_id": COST_BASELINE_ID,
    "cost_baseline_fingerprint": COST_BASELINE_FINGERPRINT,
    "exit_tracks": EXIT_TRACKS,
    "timing_policy": {
        "minimum_runtime_seconds": MIN_RUNTIME_SECONDS,
        "maximum_runtime_seconds": MAX_RUNTIME_SECONDS,
        "market_poll_seconds": MARKET_POLL_SECONDS,
        "clock_cadence_seconds": CLOCK_CADENCE_SECONDS,
        "batch_size": BATCH_SIZE,
        "collector_drain_seconds": COLLECTOR_DRAIN_SECONDS,
        "collector_cleanup_timeout_seconds": COLLECTOR_CLEANUP_TIMEOUT_SECONDS,
        "collector_max_pump_events": COLLECTOR_MAX_PUMP_EVENTS,
    },
    "source_anchor_policy": "FREEZE_CURRENT_LATEST_P1_ROWID_BEFORE_COLLECTOR_START",
    "production_db_identity_policy": "RESOLVED_ABSOLUTE_PATH_PLUS_EXACT_ANCHOR",
    "clock_order": "PREPARE_CAPTURE_WATERMARK_DRAIN_THROUGH_EXECUTE_THEN_CONTINUE",
    "collector_policy": "USE_HEALTHY_EXISTING_ELSE_MANAGED_ACCEPTED_V034_CHILD",
    "collector_stop_policy": (
        "PARENT_SENTINEL_REQUESTS_CHILD_ASYNC_CANCELLATION_AND_ACCEPTED_QUEUE_DRAIN"
    ),
    "paper_only": True,
}
MODEL_FINGERPRINT = hashlib.sha256(
    json.dumps(SMOKE_SPEC, sort_keys=True, separators=(",", ":")).encode("utf-8")
).hexdigest()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def dt_text(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_json_artifact(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(payload, handle, sort_keys=True, indent=2, ensure_ascii=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _git_head() -> str:
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=PROJECT_ROOT,
        check=True, capture_output=True, text=True,
    )
    return result.stdout.strip()


def _percentile_95(values: list[float]) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, int((len(ordered) - 1) * 0.95 + 0.999999)))
    return ordered[index]


def capture_source_anchor(db_path: Path, database_identity: str) -> int:
    probe = ContinuousMarketSourceV01(
        db_path,
        start_after_p1_rowid=0,
        database_identity=database_identity,
    )
    return probe.latest_p1_rowid()


@dataclass
class ManagedCollectorChild:
    process: subprocess.Popen[str]
    log_handle: Any
    log_path: Path
    stop_file: Path | None = None
    cleanup_method: str | None = None
    cleanup_ok: bool = False

    @classmethod
    def start(
        cls,
        command: Sequence[str],
        log_path: Path,
        *,
        stop_file: Path | None = None,
    ) -> "ManagedCollectorChild":
        log_path.parent.mkdir(parents=True, exist_ok=True)
        handle = log_path.open("w", encoding="utf-8", newline="\n")
        creationflags = (
            subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
        )
        process = subprocess.Popen(
            list(command),
            cwd=PROJECT_ROOT,
            stdin=subprocess.DEVNULL,
            stdout=handle,
            stderr=subprocess.STDOUT,
            text=True,
            creationflags=creationflags,
        )
        return cls(process, handle, log_path, stop_file)

    def stop(self, *, timeout_seconds: float = 30.0) -> bool:
        if self.process.poll() is not None:
            self.cleanup_method = "ALREADY_EXITED"
            self.cleanup_ok = True
            self.log_handle.close()
            return True
        try:
            if self.stop_file is not None:
                self.stop_file.parent.mkdir(parents=True, exist_ok=True)
                self.stop_file.write_text("STOP\n", encoding="ascii")
                self.cleanup_method = "STOP_SENTINEL"
            elif os.name == "nt":
                self.process.send_signal(signal.CTRL_BREAK_EVENT)
                self.cleanup_method = "CTRL_BREAK_EVENT"
            else:
                self.process.send_signal(signal.SIGINT)
                self.cleanup_method = "SIGINT"
            self.process.wait(timeout=timeout_seconds)
            self.cleanup_ok = True
        except (OSError, subprocess.TimeoutExpired):
            # This still targets only the process created by this object. A
            # forced fallback is recorded as a smoke failure, but prevents an
            # orphan from surviving an exceptional cleanup path.
            self.cleanup_method = "FORCED_TERMINATE_AFTER_GRACEFUL_TIMEOUT"
            self.process.terminate()
            try:
                self.process.wait(timeout=10.0)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=10.0)
            self.cleanup_ok = False
        finally:
            self.log_handle.close()
        return self.cleanup_ok


async def _collector_child_supervisor(stop_file: Path) -> None:
    import live_pump_collector_v0_3_4 as collector

    task = asyncio.create_task(
        collector.run(COLLECTOR_MAX_PUMP_EVENTS, COLLECTOR_DRAIN_SECONDS)
    )
    while not stop_file.exists():
        if task.done():
            try:
                await task
            except KeyError as exc:
                if exc.args != ("legacy_v0_2_jobs_ignored",):
                    raise
                print("[KNOWN] accepted v0.3.4 post-cleanup summary-label defect")
            return
        await asyncio.sleep(0.25)
    stop_file.unlink(missing_ok=True)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    except KeyError as exc:
        # v0.3.4 has an accepted post-cleanup summary-label typo. It occurs
        # only after listener/workers have stopped and queue drain completed.
        if exc.args != ("legacy_v0_2_jobs_ignored",):
            raise
        print("[KNOWN] accepted v0.3.4 post-cleanup summary-label defect")


def run_collector_child(stop_file: Path) -> int:
    asyncio.run(_collector_child_supervisor(stop_file))
    return 0


async def _collector_child_probe(stop_file: Path) -> None:
    while not stop_file.exists():
        await asyncio.sleep(0.05)
    stop_file.unlink(missing_ok=True)


def run_collector_child_probe(stop_file: Path) -> int:
    asyncio.run(_collector_child_probe(stop_file))
    return 0


@dataclass
class RuntimeMetrics:
    batch_latencies_ms: list[float]
    raw_rows: int = 0
    normalized_rows: int = 0
    deterministic_skips: int = 0
    semantic_events: int = 0
    source_fetches: int = 0
    launch_reconstruction_queries: int = 0
    timer_fence_wait_count: int = 0
    timer_fence_wait_seconds: float = 0.0
    timers_prepared: int = 0
    timers_executed: int = 0
    maximum_source_cursor_lag_rowids: int = 0
    sqlite_busy_locked_errors: int = 0
    conflicts: int = 0
    retries: int = 0


def _process_once(
    binding: ContinuousFirstPullbackBindingV01,
    metrics: RuntimeMetrics,
    *,
    anchor: int,
) -> Any:
    after = binding.durable_p1_rowid
    fence = binding.active_timer_fence_p1_rowid
    performs_fetch = not (fence is not None and fence == after)
    started = time.perf_counter()
    try:
        result = binding.process_next_batch(batch_size=BATCH_SIZE)
    except sqlite3.OperationalError as exc:
        if "busy" in str(exc).lower() or "locked" in str(exc).lower():
            metrics.sqlite_busy_locked_errors += 1
        raise
    metrics.batch_latencies_ms.append((time.perf_counter() - started) * 1_000.0)
    if performs_fetch:
        metrics.source_fetches += 1
        if after > anchor:
            metrics.launch_reconstruction_queries += 1
    metrics.raw_rows += result.raw_rows_fetched
    metrics.normalized_rows += result.normalized_records
    metrics.deterministic_skips += result.deterministic_skips
    metrics.semantic_events += result.runner_items_committed
    return result


def _drain_through(
    binding: ContinuousFirstPullbackBindingV01,
    metrics: RuntimeMetrics,
    *,
    anchor: int,
    watermark: int,
) -> None:
    while binding.durable_p1_rowid < watermark:
        before = binding.durable_p1_rowid
        result = _process_once(binding, metrics, anchor=anchor)
        if result.durable_p1_rowid <= before:
            raise BindingConflict(
                "production cursor made no progress while draining a captured watermark"
            )


def _execute_clock_cycle(
    binding: ContinuousFirstPullbackBindingV01,
    metrics: RuntimeMetrics,
    *,
    anchor: int,
    instant: datetime,
    timer_id: str,
) -> int:
    key = binding.prepare_clock_tick(instant, timer_id=timer_id)
    metrics.timers_prepared += 1
    timer_row = binding.conn.execute(
        "SELECT source_watermark_p1_rowid FROM "
        "paper_fp_binding_prepared_timers_v0_1 WHERE timer_key=?",
        (key,),
    ).fetchone()
    if timer_row is None:
        raise BindingConflict("prepared timer row was not persisted")
    watermark = int(timer_row[0])
    if binding.durable_p1_rowid < watermark:
        wait_started = time.perf_counter()
        metrics.timer_fence_wait_count += 1
        _drain_through(
            binding, metrics, anchor=anchor, watermark=watermark
        )
        metrics.timer_fence_wait_seconds += time.perf_counter() - wait_started
    result = binding.execute_prepared_timer(key)
    if result.status != "COMMITTED":
        raise BindingConflict(f"prepared timer did not commit: {result!r}")
    metrics.timers_executed += 1
    return watermark


def should_stop_for_duration(elapsed_seconds: float, maximum_seconds: float) -> bool:
    return elapsed_seconds >= maximum_seconds


def should_stop_early(elapsed_seconds: float, completed_signal: bool) -> bool:
    return elapsed_seconds >= MIN_RUNTIME_SECONDS and completed_signal


def _completed_signal_exists(conn: sqlite3.Connection) -> bool:
    row = conn.execute(
        "SELECT 1 FROM paper_positions GROUP BY signal_key "
        "HAVING COUNT(*)=3 AND SUM(CASE WHEN state='CLOSED' THEN 1 ELSE 0 END)=3 "
        "AND COUNT(DISTINCT track_id)=3 LIMIT 1"
    ).fetchone()
    if row is None:
        return False
    trades = load_completed_trades(conn)
    by_signal: dict[str, set[str]] = {}
    for trade in trades:
        by_signal.setdefault(trade.signal_key, set()).add(trade.track_id)
    return any(set(EXIT_TRACKS).issubset(tracks) for tracks in by_signal.values())


def classify_result(
    *,
    errors: list[str],
    integrity_ok: bool,
    raw_rows: int,
    fresh_launches: int,
    evaluated_mints: int,
    evaluations: int,
    timers_executed: int,
    graceful_shutdown: bool,
    restart_probe: bool,
    candidates: int,
    completed_signal: bool,
) -> str:
    if errors or not integrity_ok:
        return "FAIL"
    minimum = (
        raw_rows > 0
        and fresh_launches > 0
        and evaluated_mints > 0
        and evaluations > 0
        and timers_executed > 0
        and graceful_shutdown
        and restart_probe
    )
    if not minimum:
        return "FAIL_INSUFFICIENT_LIVE_EVIDENCE"
    if candidates == 0:
        return "PASS_PIPELINE_NO_CANDIDATE"
    return "PASS_FULL_TRADE" if completed_signal else "FAIL"


def _query_scalar(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> int:
    return int(conn.execute(sql, params).fetchone()[0])


def _group_counts(conn: sqlite3.Connection, sql: str) -> dict[str, int]:
    return {str(row[0]): int(row[1]) for row in conn.execute(sql).fetchall()}


def _production_evidence(
    db_path: Path, *, anchor: int, final_cursor: int
) -> dict[str, int]:
    conn = ContinuousMarketSourceV01.open_readonly(db_path)
    try:
        bounds = (anchor, final_cursor)
        fresh_launches = _query_scalar(
            conn,
            "SELECT COUNT(*) FROM pump_events WHERE rowid>? AND rowid<=? "
            "AND event_type='LAUNCH' AND source_decoded_file NOT IN "
            "('GAP_RECONCILIATION_V0_3_3','GAP_RECONCILIATION_V0_3_4')",
            bounds,
        )
        gap_rows = _query_scalar(
            conn,
            "SELECT COUNT(*) FROM pump_events WHERE rowid>? AND rowid<=? "
            "AND source_decoded_file IN "
            "('GAP_RECONCILIATION_V0_3_3','GAP_RECONCILIATION_V0_3_4')",
            bounds,
        )
        unsupported = _query_scalar(
            conn,
            "SELECT COUNT(*) FROM pump_events WHERE rowid>? AND rowid<=? "
            "AND source_decoded_file LIKE 'GAP_RECONCILIATION_%' "
            "AND source_decoded_file NOT IN "
            "('GAP_RECONCILIATION_V0_3_3','GAP_RECONCILIATION_V0_3_4')",
            bounds,
        )
        return {
            "fresh_launches": fresh_launches,
            "gap_rows": gap_rows,
            "unsupported_rows": unsupported,
        }
    finally:
        conn.close()


def _timestamp_audit(conn: sqlite3.Connection) -> bool:
    values: list[str] = []
    for table, columns in (
        ("paper_fp_binding_prepared_timers_v0_1", ("clock_timestamp", "created_at", "updated_at")),
        ("paper_orders", ("signal_observed_at", "created_at", "updated_at", "fill_at")),
        ("paper_positions", ("open_at", "updated_at", "exit_requested_at", "closed_at")),
    ):
        for column in columns:
            values.extend(
                str(row[0]) for row in conn.execute(
                    f"SELECT {column} FROM {table} WHERE {column} IS NOT NULL"
                ).fetchall()
            )
    for value in values:
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            return False
    return True


def _integrity_audit(
    paper_path: Path,
    *,
    source: ContinuousMarketSourceV01,
    final_cursor: int,
) -> dict[str, Any]:
    conn = sqlite3.connect(paper_path)
    conn.row_factory = sqlite3.Row
    try:
        quick = str(conn.execute("PRAGMA quick_check").fetchone()[0]).lower()
        runtime = conn.execute(
            "SELECT * FROM paper_fp_binding_runtime_v0_1 WHERE singleton=1"
        ).fetchone()
        duplicate_checks = {
            "strategy_evaluations": _query_scalar(
                conn,
                "SELECT COUNT(*) FROM (SELECT evaluation_id FROM "
                "paper_strategy_evaluations GROUP BY evaluation_id HAVING COUNT(*)>1)",
            ),
            "candidate_signals": _query_scalar(
                conn,
                "SELECT COUNT(*) FROM (SELECT candidate_signal_id FROM "
                "paper_strategy_evaluations WHERE candidate_signal_id IS NOT NULL "
                "GROUP BY candidate_signal_id HAVING COUNT(*)>1)",
            ),
            "terminal_decisions": _query_scalar(
                conn,
                "SELECT COUNT(*) FROM (SELECT decision_id FROM "
                "paper_terminal_trade_decisions GROUP BY decision_id HAVING COUNT(*)>1)",
            ),
            "orders": _query_scalar(
                conn,
                "SELECT COUNT(*) FROM (SELECT paper_order_id FROM paper_orders "
                "GROUP BY paper_order_id HAVING COUNT(*)>1)",
            ),
            "positions": _query_scalar(
                conn,
                "SELECT COUNT(*) FROM (SELECT paper_position_id FROM paper_positions "
                "GROUP BY paper_position_id HAVING COUNT(*)>1)",
            ),
            "exit_routes": _query_scalar(
                conn,
                "SELECT COUNT(*) FROM (SELECT route_id FROM paper_exit_execution_routes "
                "GROUP BY route_id HAVING COUNT(*)>1)",
            ),
            "marks": _query_scalar(
                conn,
                "SELECT COUNT(*) FROM (SELECT mark_id FROM paper_position_mtm_marks "
                "GROUP BY mark_id HAVING COUNT(*)>1)",
            ),
        }
        pending_outbox = _query_scalar(
            conn, "SELECT COUNT(*) FROM paper_fp_binding_outbox_v0_1 WHERE delivered=0"
        )
        pending_audit = _query_scalar(
            conn,
            "SELECT COUNT(*) FROM paper_fp_binding_evaluation_audit_v0_1 WHERE delivered=0",
        )
        incomplete_inputs = _query_scalar(
            conn, "SELECT COUNT(*) FROM paper_fp_binding_inputs_v0_1 WHERE complete=0"
        )
        incomplete_timers = _query_scalar(
            conn,
            "SELECT COUNT(*) FROM paper_fp_binding_prepared_timers_v0_1 "
            "WHERE status<>'COMPLETE'",
        )
        accounting = build_accounting_report(conn)
        observability = build_observability_report(conn)
        cross_track_ok = (
            accounting.cross_track_portfolio_aggregation_status
            == "PROHIBITED_ALTERNATIVE_TRACKS_REPORTED_SEPARATELY"
            and observability.cross_track_aggregation_status
            == "PROHIBITED_ALTERNATIVE_EXIT_TRACKS_REPORTED_SEPARATELY"
        )
        checks = {
            "quick_check": quick,
            "binding_fingerprint_exact": runtime is not None
            and str(runtime["binding_fingerprint"]) == BINDING_FINGERPRINT,
            "source_identity_exact": runtime is not None
            and str(runtime["market_source_identity"]) == source.source_identity,
            "anchor_exact": runtime is not None
            and int(runtime["start_after_p1_rowid"]) == source.start_after_p1_rowid,
            "durable_cursor_exact": runtime is not None
            and int(runtime["last_durable_p1_rowid"]) == final_cursor,
            "no_duplicate_identities": all(value == 0 for value in duplicate_checks.values()),
            "outbox_delivery_consistent": pending_outbox == 0 and pending_audit == 0,
            "inputs_complete": incomplete_inputs == 0,
            "timers_complete": incomplete_timers == 0,
            "timestamps_utc_aware": _timestamp_audit(conn),
            "cross_track_summation_prohibited": cross_track_ok,
        }
        checks["integrity_ok"] = quick == "ok" and all(
            bool(value) for key, value in checks.items() if key != "quick_check"
        )
        checks["duplicate_counts"] = duplicate_checks
        return checks
    finally:
        conn.close()


def restart_probe(
    paper_path: Path,
    source: ContinuousMarketSourceV01,
) -> tuple[bool, str | None]:
    before = paper_path.stat().st_size
    conn = connect_entry_router_db(paper_path)
    try:
        binding = ContinuousFirstPullbackBindingV01(conn, source)
        digest = binding.canonical_digest()
        ok = binding.durable_p1_rowid >= source.start_after_p1_rowid
    except Exception as exc:  # fail-closed probe evidence
        return False, f"{type(exc).__name__}:{exc}"
    finally:
        conn.close()
    after = paper_path.stat().st_size
    return ok and before == after, digest if ok else None


def _paper_summary(conn: sqlite3.Connection) -> dict[str, Any]:
    evaluations_by_role = _group_counts(
        conn,
        "SELECT role,COUNT(*) FROM paper_strategy_evaluations GROUP BY role ORDER BY role",
    )
    candidates_by_role = _group_counts(
        conn,
        "SELECT role,COUNT(*) FROM paper_strategy_evaluations "
        "WHERE candidate_signal_id IS NOT NULL GROUP BY role ORDER BY role",
    )
    outbox_by_type = _group_counts(
        conn,
        "SELECT event_type,COUNT(*) FROM paper_fp_binding_outbox_v0_1 "
        "GROUP BY event_type ORDER BY event_type",
    )
    state_counts = _group_counts(
        conn, "SELECT state,COUNT(*) FROM paper_positions GROUP BY state ORDER BY state"
    )
    accounting = build_accounting_report(conn)
    observability = build_observability_report(conn)
    trades = load_completed_trades(conn)
    excursions = {item.paper_position_id: item for item in observability.positions}
    positions: list[dict[str, Any]] = []
    rows = conn.execute(
        "SELECT p.*, er.total_entry_explicit_cost_lamports, "
        "xr.exit_reason,xr.total_exit_explicit_cost_lamports "
        "FROM paper_positions p "
        "LEFT JOIN paper_entry_routes er ON er.candidate_id=p.signal_key "
        "LEFT JOIN paper_exit_execution_routes xr "
        "ON xr.paper_position_id=p.paper_position_id "
        "ORDER BY p.signal_key,p.track_id"
    ).fetchall()
    trade_by_position = {trade.paper_position_id: trade for trade in trades}
    for row in rows:
        trade = trade_by_position.get(str(row["paper_position_id"]))
        excursion = excursions.get(str(row["paper_position_id"]))
        positions.append({
            "signal_key": str(row["signal_key"]),
            "mint": str(row["mint"]),
            "track_id": str(row["track_id"]),
            "exit_variant": str(row["exit_variant"]),
            "entry_time": str(row["open_at"]),
            "entry_price_numerator_raw": str(row["entry_price_numerator_raw"]),
            "entry_price_denominator_raw": str(row["entry_price_denominator_raw"]),
            "entry_explicit_cost_lamports": row["total_entry_explicit_cost_lamports"],
            "exit_time": row["closed_at"],
            "exit_price_numerator_raw": row["exit_price_numerator_raw"],
            "exit_price_denominator_raw": row["exit_price_denominator_raw"],
            "exit_reason": row["exit_reason"],
            "exit_explicit_cost_lamports": row["total_exit_explicit_cost_lamports"],
            "gross_pnl_lamports": None if trade is None else trade.gross_execution_pnl_lamports,
            "net_pnl_lamports": None if trade is None else trade.net_pnl_lamports,
            "mae_bps": None if excursion is None else excursion.mae_bps,
            "mfe_bps": None if excursion is None else excursion.mfe_bps,
            "terminal_state": str(row["state"]),
        })
    return {
        "evaluations": _query_scalar(conn, "SELECT COUNT(*) FROM paper_strategy_evaluations"),
        "evaluated_mints": _query_scalar(
            conn, "SELECT COUNT(DISTINCT mint) FROM paper_strategy_evaluations"
        ),
        "evaluations_by_role": evaluations_by_role,
        "transitions": _query_scalar(
            conn,
            "SELECT COUNT(*) FROM paper_strategy_evaluations WHERE transition_occurred=1",
        ),
        "candidates": _query_scalar(
            conn,
            "SELECT COUNT(*) FROM paper_strategy_evaluations "
            "WHERE candidate_signal_id IS NOT NULL",
        ),
        "candidates_by_winning_role": candidates_by_role,
        "terminal_skips": _query_scalar(
            conn,
            "SELECT COUNT(*) FROM paper_terminal_trade_decisions WHERE trade_or_skip='SKIP'",
        ),
        "outbox_events": outbox_by_type,
        "paper_entries": _query_scalar(
            conn, "SELECT COUNT(*) FROM paper_entry_routes WHERE state='FILLED'"
        ),
        "paper_exits": _query_scalar(
            conn, "SELECT COUNT(*) FROM paper_exit_execution_routes WHERE state='FILLED'"
        ),
        "track_state_counts": state_counts,
        "positions": positions,
        "accounting": asdict(accounting),
        "observability": asdict(observability),
        "completed_signal": _completed_signal_exists(conn),
    }


def run_production_smoke(*, launch_collector: bool) -> tuple[int, dict[str, Any]]:
    if _git_head() != "29e91738cf956421e533f75f01faba4f26ed5f8b":
        raise RuntimeError("production smoke must run from accepted checkpoint 29e9173")
    if P4_COST_BASELINE_0001.assumption_set_id != COST_BASELINE_ID:
        raise RuntimeError("cost baseline identity drift")
    if P4_COST_BASELINE_0001.fingerprint != COST_BASELINE_FINGERPRINT:
        raise RuntimeError("cost baseline fingerprint drift")
    if not PRODUCTION_DB.exists():
        raise FileNotFoundError(PRODUCTION_DB)

    started_at = utc_now()
    stamp = started_at.strftime("%Y%m%dT%H%M%S_%fZ")
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    paper_path = RUNTIME_DIR / f"phase4_firstpullback_production_smoke_{stamp}.sqlite3"
    json_path = RUNTIME_DIR / f"phase4_firstpullback_production_smoke_{stamp}.json"
    collector_log = RUNTIME_DIR / f"phase4_firstpullback_production_smoke_{stamp}_collector.log"
    collector_stop_file = RUNTIME_DIR / f"phase4_firstpullback_production_smoke_{stamp}.stop"
    if paper_path.exists() or json_path.exists():
        raise RuntimeError("new smoke artifact path already exists")

    database_identity = PRODUCTION_DB.resolve().as_posix()
    anchor = capture_source_anchor(PRODUCTION_DB, database_identity)
    source = ContinuousMarketSourceV01(
        PRODUCTION_DB,
        start_after_p1_rowid=anchor,
        database_identity=database_identity,
    )
    metrics = RuntimeMetrics([])
    errors: list[str] = []
    collector: ManagedCollectorChild | None = None
    collector_status = "EXISTING_COLLECTOR"
    collector_pid: int | None = None
    collector_target_completed = False
    conn: sqlite3.Connection | None = None
    binding: ContinuousFirstPullbackBindingV01 | None = None
    end_watermark = anchor
    graceful_shutdown = False
    early_success = False

    if launch_collector:
        collector = ManagedCollectorChild.start(
            (
                sys.executable,
                str(Path(__file__).resolve()),
                "--collector-child", str(collector_stop_file),
            ),
            collector_log,
            stop_file=collector_stop_file,
        )
        collector_status = "STARTED_BY_SMOKE"
        collector_pid = collector.process.pid

    try:
        conn = connect_entry_router_db(paper_path)
        binding = ContinuousFirstPullbackBindingV01(conn, source)
        loop_started = time.monotonic()
        next_clock = loop_started
        clock_index = 0
        while True:
            if (
                collector is not None
                and collector.process.poll() is not None
                and not collector_target_completed
            ):
                if collector.process.returncode == 0:
                    collector_target_completed = True
                else:
                    raise RuntimeError(
                        "managed collector exited before smoke stop request: "
                        f"exit_code={collector.process.returncode}"
                    )
            elapsed = time.monotonic() - loop_started
            if should_stop_for_duration(elapsed, MAX_RUNTIME_SECONDS):
                break
            latest = source.latest_p1_rowid()
            metrics.maximum_source_cursor_lag_rowids = max(
                metrics.maximum_source_cursor_lag_rowids,
                max(0, latest - binding.durable_p1_rowid),
            )
            now_mono = time.monotonic()
            if now_mono >= next_clock:
                instant = utc_now()
                _execute_clock_cycle(
                    binding,
                    metrics,
                    anchor=anchor,
                    instant=instant,
                    timer_id=f"SMOKE:CLOCK:{clock_index:06d}:{dt_text(instant)}",
                )
                clock_index += 1
                next_clock += CLOCK_CADENCE_SECONDS
                if next_clock <= now_mono:
                    next_clock = now_mono + CLOCK_CADENCE_SECONDS
            else:
                _process_once(binding, metrics, anchor=anchor)

            if should_stop_early(elapsed, _completed_signal_exists(conn)):
                early_success = True
                break
            sleep_for = min(MARKET_POLL_SECONDS, max(0.0, next_clock - time.monotonic()))
            if sleep_for > 0:
                time.sleep(sleep_for)

        final_instant = utc_now()
        end_watermark = source.latest_p1_rowid()
        final_key = binding.prepare_clock_tick(
            final_instant,
            timer_id=f"SMOKE:FINAL:{dt_text(final_instant)}",
        )
        metrics.timers_prepared += 1
        final_row = conn.execute(
            "SELECT source_watermark_p1_rowid FROM "
            "paper_fp_binding_prepared_timers_v0_1 WHERE timer_key=?",
            (final_key,),
        ).fetchone()
        if final_row is None or int(final_row[0]) != end_watermark:
            raise BindingConflict("final timer did not bind exact end watermark")
        if binding.durable_p1_rowid < end_watermark:
            wait_started = time.perf_counter()
            metrics.timer_fence_wait_count += 1
            _drain_through(binding, metrics, anchor=anchor, watermark=end_watermark)
            metrics.timer_fence_wait_seconds += time.perf_counter() - wait_started
        final_result = binding.execute_prepared_timer(final_key)
        if final_result.status != "COMMITTED":
            raise BindingConflict("final prepared timer did not commit")
        metrics.timers_executed += 1
        if binding.pending_outbox_count() != 0:
            raise BindingConflict("pending outbox remains at graceful stop")
        if binding.durable_p1_rowid != end_watermark:
            raise BindingConflict("final durable cursor differs from frozen end watermark")
        graceful_shutdown = True
    except BindingConflict as exc:
        metrics.conflicts += 1
        errors.append(f"BindingConflict:{exc}")
    except Exception as exc:
        errors.append(f"{type(exc).__name__}:{exc}")
    finally:
        if conn is not None:
            conn.close()
        collector_cleanup_ok = True
        collector_cleanup_method = "NOT_STARTED_BY_SMOKE"
        collector_exit_code: int | None = None
        if collector is not None:
            # The accepted workers include 30-second health sleeps plus bounded
            # RPC waits after the configured queue-drain window. Allow those
            # paths to unwind before declaring cleanup failure.
            collector_cleanup_ok = collector.stop(
                timeout_seconds=COLLECTOR_CLEANUP_TIMEOUT_SECONDS
            )
            collector_cleanup_method = str(collector.cleanup_method)
            collector_exit_code = collector.process.returncode
            if not collector_cleanup_ok:
                errors.append("managed collector did not complete graceful cleanup")

    ended_at = utc_now()
    duration_seconds = (ended_at - started_at).total_seconds()
    paper_summary: dict[str, Any] = {}
    integrity: dict[str, Any] = {"integrity_ok": False, "quick_check": "NOT_RUN"}
    restart_ok = False
    restart_digest: str | None = None
    if paper_path.exists():
        try:
            if graceful_shutdown:
                audit_cursor = end_watermark
            else:
                cursor_conn = sqlite3.connect(paper_path)
                try:
                    cursor_row = cursor_conn.execute(
                    "SELECT last_durable_p1_rowid FROM paper_fp_binding_runtime_v0_1 "
                    "WHERE singleton=1"
                    ).fetchone()
                    audit_cursor = anchor if cursor_row is None else max(
                        anchor, int(cursor_row[0])
                    )
                finally:
                    cursor_conn.close()
            integrity = _integrity_audit(
                paper_path, source=source, final_cursor=int(audit_cursor)
            )
            summary_conn = sqlite3.connect(paper_path)
            summary_conn.row_factory = sqlite3.Row
            try:
                paper_summary = _paper_summary(summary_conn)
            finally:
                summary_conn.close()
            restart_ok, restart_digest = restart_probe(paper_path, source)
            if not restart_ok:
                errors.append(f"restart probe failed: {restart_digest}")
        except Exception as exc:
            errors.append(f"postrun audit failed: {type(exc).__name__}:{exc}")

    production = _production_evidence(
        PRODUCTION_DB, anchor=anchor, final_cursor=end_watermark
    ) if end_watermark >= anchor else {
        "fresh_launches": 0, "gap_rows": 0, "unsupported_rows": 0
    }
    result_class = classify_result(
        errors=errors,
        integrity_ok=bool(integrity.get("integrity_ok", False)),
        raw_rows=metrics.raw_rows,
        fresh_launches=production["fresh_launches"],
        evaluated_mints=int(paper_summary.get("evaluated_mints", 0)),
        evaluations=int(paper_summary.get("evaluations", 0)),
        timers_executed=metrics.timers_executed,
        graceful_shutdown=graceful_shutdown,
        restart_probe=restart_ok,
        candidates=int(paper_summary.get("candidates", 0)),
        completed_signal=bool(paper_summary.get("completed_signal", False)),
    )
    per_second = max(duration_seconds, 0.000001)
    summary = {
        "smoke_model_id": MODEL_ID,
        "smoke_fingerprint": MODEL_FINGERPRINT,
        "spec": SMOKE_SPEC,
        "git_head": _git_head(),
        "started_at_utc": dt_text(started_at),
        "ended_at_utc": dt_text(ended_at),
        "duration_seconds": duration_seconds,
        "early_success_stop": early_success,
        "collector": {
            "status": collector_status,
            "started_by_smoke": launch_collector,
            "pid": collector_pid,
            "cleanup_ok": collector_cleanup_ok,
            "cleanup_method": collector_cleanup_method,
            "exit_code": collector_exit_code,
            "bounded_target_completed": collector_target_completed,
            "log_path": str(collector_log) if launch_collector else None,
        },
        "production_source": {
            "database_path": str(PRODUCTION_DB.resolve()),
            "database_identity": database_identity,
            "source_identity": source.source_identity,
            "start_anchor_p1_rowid": anchor,
            "end_watermark_p1_rowid": end_watermark,
            "final_durable_p1_rowid": end_watermark if graceful_shutdown else None,
            **production,
        },
        "runtime_counts": {
            "raw_rows_observed": metrics.raw_rows,
            "normalized_records": metrics.normalized_rows,
            "deterministic_skips": metrics.deterministic_skips,
            "semantic_events": metrics.semantic_events,
            "source_fetches": metrics.source_fetches,
            "launch_reconstruction_queries": metrics.launch_reconstruction_queries,
            "hydration_queries": 0 if binding is None else binding.reconstruction_queries,
            "timers_prepared": metrics.timers_prepared,
            "timers_executed": metrics.timers_executed,
            "timer_fence_wait_count": metrics.timer_fence_wait_count,
            "timer_fence_wait_seconds": metrics.timer_fence_wait_seconds,
            "conflicts": metrics.conflicts,
            "retries": metrics.retries,
            "sqlite_busy_locked_errors": metrics.sqlite_busy_locked_errors,
        },
        "performance": {
            "raw_rows_per_second": metrics.raw_rows / per_second,
            "normalized_records_per_second": metrics.normalized_rows / per_second,
            "semantic_events_per_second": metrics.semantic_events / per_second,
            "mean_batch_processing_latency_ms": (
                mean(metrics.batch_latencies_ms) if metrics.batch_latencies_ms else 0.0
            ),
            "p95_batch_processing_latency_ms": _percentile_95(metrics.batch_latencies_ms),
            "maximum_source_cursor_lag_rowids": metrics.maximum_source_cursor_lag_rowids,
        },
        "paper": paper_summary,
        "integrity": integrity,
        "restart_probe": {"passed": restart_ok, "canonical_digest": restart_digest},
        "graceful_shutdown": graceful_shutdown,
        "errors": errors,
        "result_class": result_class,
        "profitability_claimed": False,
        "cross_track_portfolio_summed": False,
        "paper_only": True,
        "artifact_paths": {
            "paper_db": str(paper_path),
            "json": str(json_path),
        },
    }
    write_json_artifact(json_path, summary)
    summary["artifact_sha256"] = {
        "paper_db": sha256_file(paper_path) if paper_path.exists() else None,
        "json": sha256_file(json_path),
    }
    print("=" * 112)
    print("CONTROLLED PRODUCTION FIRSTPULLBACK PAPER SMOKE v0.1")
    print("=" * 112)
    print(f"Model              : {MODEL_ID}")
    print(f"Fingerprint        : {MODEL_FINGERPRINT}")
    print(f"Start anchor       : {anchor}")
    print(f"Final cursor       : {end_watermark if graceful_shutdown else 'UNRESOLVED'}")
    print(f"Raw rows           : {metrics.raw_rows}")
    print(f"Fresh launches     : {production['fresh_launches']}")
    print(f"Evaluations        : {paper_summary.get('evaluations', 0)}")
    print(f"Candidates         : {paper_summary.get('candidates', 0)}")
    print(f"Paper DB           : {paper_path}")
    print(f"JSON summary       : {json_path}")
    print(f"RESULT CLASS       : {result_class}")
    print(f"RESULT: {'PASS' if result_class.startswith('PASS_') else 'FAIL'}")
    return (0 if result_class.startswith("PASS_") else 1), summary


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--collector-child", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--collector-child-probe", type=Path, help=argparse.SUPPRESS)
    collector = parser.add_mutually_exclusive_group(required=False)
    collector.add_argument(
        "--use-existing-collector", action="store_true",
        help="Use an independently verified healthy v0.3.4 collector.",
    )
    collector.add_argument(
        "--launch-managed-collector", action="store_true",
        help="Launch and clean up the accepted v0.3.4 collector as a child.",
    )
    args = parser.parse_args()
    if args.collector_child is not None:
        return run_collector_child(args.collector_child)
    if args.collector_child_probe is not None:
        return run_collector_child_probe(args.collector_child_probe)
    if not args.use_existing_collector and not args.launch_managed_collector:
        parser.error("one collector mode is required")
    code, _ = run_production_smoke(
        launch_collector=bool(args.launch_managed_collector)
    )
    return code


if __name__ == "__main__":
    raise SystemExit(main())
