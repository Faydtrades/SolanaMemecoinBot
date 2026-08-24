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
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from statistics import mean
from typing import Any, Callable, Sequence


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
SCRIPTS_ROOT = PROJECT_ROOT / "scripts"
for import_root in (SRC_ROOT, SCRIPTS_ROOT):
    if str(import_root) not in sys.path:
        sys.path.insert(0, str(import_root))

import phase4_continuous_firstpullback_production_smoke_v0_1 as smoke  # noqa: E402
from phase4.paper_continuous_firstpullback_binding_v0_1 import (  # noqa: E402
    GAP_COMPAT_FINGERPRINT,
    LOCKED_SELECTION_SHA256,
    MARKET_SOURCE_FINGERPRINT,
    MODEL_FINGERPRINT as BINDING_FINGERPRINT,
    RUNNER_SPEC_FINGERPRINT,
    BindingConflict,
    ContinuousFirstPullbackBindingV01,
)
from phase4.paper_continuous_market_source_v0_1 import (  # noqa: E402
    ContinuousMarketSourceV01,
)
from phase4.paper_cost_baseline_v0_1 import P4_COST_BASELINE_0001  # noqa: E402
from phase4.paper_entry_router_v0_1 import connect_entry_router_db  # noqa: E402


MODEL_ID = "P4-CONTINUOUS-FIRSTPULLBACK-MULTIHOUR-RUN-0001"
DEFAULT_DURATION_SECONDS = 14_400
MIN_DURATION_SECONDS = 3_600
MAX_DURATION_SECONDS = 21_600
MARKET_POLL_SECONDS = 0.5
CLOCK_CADENCE_SECONDS = 1.0
BATCH_SIZE = 250
COLLECTOR_DRAIN_SECONDS = 10
COLLECTOR_CLEANUP_TIMEOUT_SECONDS = 180.0
SOURCE_STALL_FAIL_SECONDS = 300.0
UNLIMITED_COLLECTOR_MAX_PUMP_EVENTS = None
RUNTIME_DIR = PROJECT_ROOT / "data" / "paper" / "multihour"
PRODUCTION_DB = smoke.PRODUCTION_DB
ACCEPTED_T009_SHA256 = (
    "9c6a786d56137aa0d1f99c24d8878e60ab4a5846e8ec91b8ec1fabd056b5d34d"
)
ACCEPTED_T009_FINGERPRINT = (
    "0ad1cd2eff06399b93736d452859bab4ea081ee84d8ac287435e4e4b35c6311b"
)
COST_BASELINE_ID = "P4-COST-BASELINE-0001"
COST_BASELINE_FINGERPRINT = (
    "9652aeffc792eb48d62348360c6ae804f660e43329195d1f169219b80ccab67c"
)
EXIT_TRACKS = {
    "FINAL-A": "E3_TP10_T15",
    "FINAL-B": "E4_ACT10_GB03_T15",
    "SENS-C": "SENS_TP20_T5",
}

MULTIHOUR_SPEC = {
    "model_id": MODEL_ID,
    "accepted_t009_fingerprint": ACCEPTED_T009_FINGERPRINT,
    "accepted_t009_file_sha256": ACCEPTED_T009_SHA256,
    "binding_fingerprint": BINDING_FINGERPRINT,
    "market_source_fingerprint": MARKET_SOURCE_FINGERPRINT,
    "gap_compatibility_fingerprint": GAP_COMPAT_FINGERPRINT,
    "continuous_runner_fingerprint": RUNNER_SPEC_FINGERPRINT,
    "firstpullback_selection_sha256": LOCKED_SELECTION_SHA256,
    "cost_baseline_id": COST_BASELINE_ID,
    "cost_baseline_fingerprint": COST_BASELINE_FINGERPRINT,
    "exit_tracks": EXIT_TRACKS,
    "duration_policy": {
        "default_seconds": DEFAULT_DURATION_SECONDS,
        "minimum_seconds": MIN_DURATION_SECONDS,
        "maximum_seconds": MAX_DURATION_SECONDS,
        "candidate_early_stop": False,
        "actual_duration_bound_into_session_identity": True,
    },
    "timing_policy": {
        "market_poll_seconds": MARKET_POLL_SECONDS,
        "clock_cadence_seconds": CLOCK_CADENCE_SECONDS,
        "batch_size": BATCH_SIZE,
    },
    "collector_policy": {
        "existing": "EXTERNALLY_VERIFIED_HEALTHY_V034",
        "managed": "OWNED_PROCESS_GROUP_ACCEPTED_V034_UNLIMITED_MODE",
        "managed_max_pump_events": UNLIMITED_COLLECTOR_MAX_PUMP_EVENTS,
        "drain_seconds": COLLECTOR_DRAIN_SECONDS,
        "cleanup_timeout_seconds": COLLECTOR_CLEANUP_TIMEOUT_SECONDS,
        "known_summary_keyerror": "legacy_v0_2_jobs_ignored_ONLY",
    },
    "source_anchor_policy": "FREEZE_LATEST_P1_ROWID_BEFORE_COLLECTOR_START",
    "production_db_policy": "SQLITE_URI_MODE_RO_AND_QUERY_ONLY",
    "source_continuity": {
        "stall_fail_seconds": SOURCE_STALL_FAIL_SECONDS,
        "activity_signal": "PRODUCTION_LATEST_P1_ROWID_ADVANCEMENT_ONLY",
        "failure_reason": "PRODUCTION_SOURCE_STALLED",
        "process_liveness_is_insufficient": True,
        "required_for_pass": True,
    },
    "runtime_artifacts": {
        "directory": "data/paper/multihour",
        "ignore_policy": "ROOT_GITIGNORE_EXACT_DIRECTORY_RULE",
        "new_database_and_json_each_run": True,
    },
    "clock_order": "CAPTURE_WATERMARK_DRAIN_THROUGH_EXECUTE_FROZEN_TIMER_CONTINUE",
    "graceful_end": "FREEZE_END_WATERMARK_DRAIN_EXECUTE_OUTBOX_CLOSE_NO_FABRICATION",
    "session_metrics": (
        "SOURCE_COUNTS",
        "STRATEGY_COUNTS_AND_CANDIDATE_DETAILS",
        "ENTRY_ACCEPT_REJECT",
        "ALTERNATIVE_TRACK_ACCOUNTING_OBSERVABILITY_EXIT_REASONS",
        "TIMER_CURSOR_RECONSTRUCTION_HYDRATION",
        "SQLITE_CONFLICT_RETRY_AND_LATENCY_HEALTH",
    ),
    "cross_track_portfolio_sum": "PROHIBITED",
    "sens_c_label": "SENSITIVITY ONLY",
    "paper_only": True,
}
MODEL_FINGERPRINT = hashlib.sha256(
    json.dumps(MULTIHOUR_SPEC, sort_keys=True, separators=(",", ":")).encode("utf-8")
).hexdigest()


def file_sha256(path: Path) -> str:
    return smoke.sha256_file(path)


def parse_duration_seconds(value: str | int) -> int:
    try:
        duration = int(value)
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError("duration-seconds must be an integer") from exc
    if not MIN_DURATION_SECONDS <= duration <= MAX_DURATION_SECONDS:
        raise argparse.ArgumentTypeError(
            f"duration-seconds must be in {MIN_DURATION_SECONDS}..{MAX_DURATION_SECONDS}"
        )
    return duration


def run_control_reason(
    elapsed_seconds: float,
    requested_duration_seconds: int,
    *,
    fail_closed_error: str | None = None,
) -> str | None:
    if fail_closed_error is not None:
        return "FAIL_CLOSED"
    if elapsed_seconds >= requested_duration_seconds:
        return "DURATION_REACHED"
    return None


class ProductionSourceStalled(RuntimeError):
    pass


@dataclass
class SourceContinuityWatchdog:
    last_observed_source_p1_rowid: int
    last_source_advance_monotonic: float
    source_advance_count: int = 0
    maximum_source_stall_seconds: float = 0.0
    stalled: bool = False

    def observe(self, latest_p1_rowid: int, now_monotonic: float) -> None:
        if self.stalled:
            raise ProductionSourceStalled("PRODUCTION_SOURCE_STALLED")
        latest = int(latest_p1_rowid)
        now = float(now_monotonic)
        if latest < self.last_observed_source_p1_rowid:
            raise BindingConflict(
                "non-monotonic production latest p1_rowid: "
                f"{latest} < {self.last_observed_source_p1_rowid}"
            )
        stall = max(0.0, now - self.last_source_advance_monotonic)
        self.maximum_source_stall_seconds = max(
            self.maximum_source_stall_seconds,
            stall,
        )
        if stall >= SOURCE_STALL_FAIL_SECONDS:
            self.stalled = True
            raise ProductionSourceStalled("PRODUCTION_SOURCE_STALLED")
        if latest > self.last_observed_source_p1_rowid:
            self.last_observed_source_p1_rowid = latest
            self.last_source_advance_monotonic = now
            self.source_advance_count += 1

    def snapshot(self, now_monotonic: float) -> dict[str, Any]:
        final_staleness = max(
            0.0,
            float(now_monotonic) - self.last_source_advance_monotonic,
        )
        maximum_stall = max(self.maximum_source_stall_seconds, final_staleness)
        continuity_ok = (
            not self.stalled
            and self.source_advance_count > 0
            and maximum_stall < SOURCE_STALL_FAIL_SECONDS
            and final_staleness < SOURCE_STALL_FAIL_SECONDS
        )
        return {
            "stall_fail_seconds": SOURCE_STALL_FAIL_SECONDS,
            "source_advance_count": self.source_advance_count,
            "maximum_source_stall_seconds": maximum_stall,
            "final_source_staleness_seconds": final_staleness,
            "continuity_ok": continuity_ok,
            "last_observed_source_p1_rowid": self.last_observed_source_p1_rowid,
        }

    def canonical_digest(self, now_monotonic: float) -> str:
        return hashlib.sha256(
            json.dumps(
                self.snapshot(now_monotonic),
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()


def session_identity(
    *, duration_seconds: int, database_identity: str, anchor: int
) -> str:
    payload = {
        "model_fingerprint": MODEL_FINGERPRINT,
        "requested_duration_seconds": parse_duration_seconds(duration_seconds),
        "database_identity": str(database_identity),
        "start_after_p1_rowid": int(anchor),
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def prepare_runtime_directory(path: Path = RUNTIME_DIR) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _creation_flags() -> int:
    return int(getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)) if os.name == "nt" else 0


@dataclass
class OwnedCollectorChild:
    process: subprocess.Popen[str]
    log_handle: Any
    cleanup_method: str = "RUNNING"
    cleanup_ok: bool = False

    @classmethod
    def start(cls, args: Sequence[str], log_path: Path) -> "OwnedCollectorChild":
        log_path.parent.mkdir(parents=True, exist_ok=True)
        handle = log_path.open("w", encoding="utf-8")
        try:
            process = subprocess.Popen(
                tuple(args),
                cwd=str(PROJECT_ROOT),
                stdin=subprocess.DEVNULL,
                stdout=handle,
                stderr=subprocess.STDOUT,
                text=True,
                creationflags=_creation_flags(),
            )
        except Exception:
            handle.close()
            raise
        return cls(process=process, log_handle=handle)

    def _interrupt_owned_process_group(self) -> None:
        if self.process.poll() is not None:
            return
        if os.name == "nt":
            self.process.send_signal(signal.CTRL_BREAK_EVENT)
        else:
            self.process.send_signal(signal.SIGINT)

    def stop(self, *, timeout_seconds: float) -> bool:
        if self.process.poll() is not None:
            self.cleanup_method = "ALREADY_EXITED"
            self.cleanup_ok = self.process.returncode == 0
            self.log_handle.close()
            return self.cleanup_ok
        self.cleanup_method = "OWNED_PROCESS_GROUP_INTERRUPT"
        try:
            self._interrupt_owned_process_group()
            self.process.wait(timeout=timeout_seconds)
            self.cleanup_ok = self.process.returncode == 0
        except (OSError, subprocess.TimeoutExpired):
            self.cleanup_method = "FORCED_TERMINATE_OWNED_CHILD_AFTER_TIMEOUT"
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


def start_managed_collector(log_path: Path) -> OwnedCollectorChild:
    return OwnedCollectorChild.start(
        (
            sys.executable,
            str(Path(__file__).resolve()),
            "--collector-child",
        ),
        log_path,
    )


def _install_owned_interrupt_handler() -> None:
    if os.name != "nt":
        return

    def raise_keyboard_interrupt(_signum: int, _frame: Any) -> None:
        raise KeyboardInterrupt

    signal.signal(signal.SIGBREAK, raise_keyboard_interrupt)


def run_collector_child() -> int:
    import live_pump_collector_v0_3_4 as collector

    _install_owned_interrupt_handler()
    try:
        asyncio.run(
            collector.run(
                UNLIMITED_COLLECTOR_MAX_PUMP_EVENTS,
                COLLECTOR_DRAIN_SECONDS,
            )
        )
    except KeyboardInterrupt:
        print("[STOP] Owned multi-hour collector interrupt received.")
    except KeyError as exc:
        if exc.args != ("legacy_v0_2_jobs_ignored",):
            raise
        print("[KNOWN] accepted v0.3.4 post-cleanup summary-label defect")
    return 0


def run_collector_child_probe(ready_path: Path | None = None) -> int:
    _install_owned_interrupt_handler()
    if ready_path is not None:
        ready_path.write_text("READY\n", encoding="utf-8")
    try:
        while True:
            time.sleep(0.05)
    except KeyboardInterrupt:
        return 0


def finalize_at_watermark(
    conn: sqlite3.Connection,
    binding: ContinuousFirstPullbackBindingV01,
    metrics: smoke.RuntimeMetrics,
    *,
    anchor: int,
    instant: datetime,
    timer_id: str,
    drain_fn: Callable[..., None] = smoke._drain_through,
) -> int:
    final_key = binding.prepare_clock_tick(instant, timer_id=timer_id)
    metrics.timers_prepared += 1
    row = conn.execute(
        "SELECT source_watermark_p1_rowid FROM "
        "paper_fp_binding_prepared_timers_v0_1 WHERE timer_key=?",
        (final_key,),
    ).fetchone()
    if row is None:
        raise BindingConflict("final prepared timer row was not persisted")
    end_watermark = int(row[0])
    if binding.durable_p1_rowid < end_watermark:
        wait_started = time.perf_counter()
        metrics.timer_fence_wait_count += 1
        drain_fn(
            binding,
            metrics,
            anchor=anchor,
            watermark=end_watermark,
        )
        metrics.timer_fence_wait_seconds += time.perf_counter() - wait_started
    result = binding.execute_prepared_timer(final_key)
    if result.status != "COMMITTED":
        raise BindingConflict("final prepared timer did not commit")
    if result.source_watermark_p1_rowid != end_watermark:
        raise BindingConflict("committed final timer returned a different watermark")
    metrics.timers_executed += 1
    if binding.pending_outbox_count() != 0:
        raise BindingConflict("pending outbox remains at graceful stop")
    if binding.durable_p1_rowid != end_watermark:
        raise BindingConflict("final durable cursor differs from frozen end watermark")
    return end_watermark


def preserve_source_stall_failure_state(
    binding: ContinuousFirstPullbackBindingV01,
) -> int:
    if binding.conn.in_transaction:
        raise BindingConflict("source-stall shutdown found an active transaction")
    if binding.pending_outbox_count() != 0:
        raise BindingConflict("source-stall shutdown found pending outbox work")
    if binding.active_timer_fence_p1_rowid is not None:
        raise BindingConflict("source-stall shutdown found an incomplete timer fence")
    return binding.durable_p1_rowid


def _candidate_details(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    return [
        {
            "evaluation_id": str(row[0]),
            "candidate_signal_id": str(row[1]),
            "mint": str(row[2]),
            "winning_role": str(row[3]),
            "evaluated_at_us": int(row[4]),
        }
        for row in conn.execute(
            "SELECT evaluation_id,candidate_signal_id,mint,role,evaluated_at_us "
            "FROM paper_strategy_evaluations WHERE candidate_signal_id IS NOT NULL "
            "ORDER BY evaluated_at_us,evaluation_id"
        ).fetchall()
    ]


def build_session_track_metrics(paper_summary: dict[str, Any]) -> dict[str, Any]:
    positions = list(paper_summary.get("positions", []))
    accounting_tracks = {
        str(item["track_id"]): item
        for item in paper_summary.get("accounting", {}).get("tracks", [])
    }
    observability_tracks = {
        str(item["track_id"]): item
        for item in paper_summary.get("observability", {}).get("tracks", [])
    }
    rejection_audit = paper_summary.get("accounting", {}).get("rejection_audit", {})
    result: dict[str, Any] = {}
    for track_id in EXIT_TRACKS:
        track_positions = [item for item in positions if item.get("track_id") == track_id]
        closed = [item for item in track_positions if item.get("terminal_state") == "CLOSED"]
        exits = Counter(
            str(item["exit_reason"])
            for item in closed
            if item.get("exit_reason") is not None
        )
        result[track_id] = {
            "label": "SENSITIVITY ONLY" if track_id == "SENS-C" else "PRIMARY ALTERNATIVE",
            "entries": len(track_positions),
            "closed": len(closed),
            "open": len(track_positions) - len(closed),
            "exit_reason_distribution": dict(sorted(exits.items())),
            "gross_pnl_lamports": sum(
                int(item["gross_pnl_lamports"])
                for item in closed
                if item.get("gross_pnl_lamports") is not None
            ),
            "net_pnl_lamports": sum(
                int(item["net_pnl_lamports"])
                for item in closed
                if item.get("net_pnl_lamports") is not None
            ),
            "accounting": accounting_tracks.get(track_id),
            "equity_and_drawdown": observability_tracks.get(track_id),
        }
    return {
        "cross_track_portfolio_summed": False,
        "entry_attempts": {
            "accepted": int(paper_summary.get("paper_entries", 0)),
            "rejected": int(rejection_audit.get("rejected_entry_routes", 0)),
            "total_routes": int(rejection_audit.get("entry_route_total", 0)),
        },
        "tracks": result,
    }


def classify_multihour_result(
    *,
    duration_reached: bool,
    errors: Sequence[str],
    integrity_ok: bool,
    graceful_shutdown: bool,
    restart_ok: bool,
    source_continuity_ok: bool,
    raw_rows: int,
    fresh_launches: int,
    evaluated_mints: int,
    evaluations: int,
    timers_executed: int,
) -> str:
    passed = (
        duration_reached
        and not errors
        and integrity_ok
        and graceful_shutdown
        and restart_ok
        and source_continuity_ok
        and raw_rows > 0
        and fresh_launches > 0
        and evaluated_mints > 0
        and evaluations > 0
        and timers_executed > 0
    )
    return "PASS_MULTI_HOUR" if passed else "FAIL"


def _validate_locked_contracts() -> None:
    if file_sha256(Path(smoke.__file__).resolve()) != ACCEPTED_T009_SHA256:
        raise RuntimeError("accepted T009 harness changed")
    if smoke.MODEL_FINGERPRINT != ACCEPTED_T009_FINGERPRINT:
        raise RuntimeError("accepted T009 fingerprint drift")
    if P4_COST_BASELINE_0001.assumption_set_id != COST_BASELINE_ID:
        raise RuntimeError("cost baseline identity drift")
    if P4_COST_BASELINE_0001.fingerprint != COST_BASELINE_FINGERPRINT:
        raise RuntimeError("cost baseline fingerprint drift")
    expected = (
        BINDING_FINGERPRINT,
        MARKET_SOURCE_FINGERPRINT,
        GAP_COMPAT_FINGERPRINT,
        RUNNER_SPEC_FINGERPRINT,
        LOCKED_SELECTION_SHA256,
    )
    actual = (
        "318e15b104c691821f5715b090c934e5b1cb3d9a8357fe0930d8825ef66d0daa",
        "242b84a26ddc9e80fc428b1f02202e30aab6f4009677b38b72861bad432881fb",
        "aabb765f81a29b2dd119607cdea92a2949dd3acec10aa204d246e6aa7a28a78c",
        "8dcab9d17f2ec71f4920a199b5745dae92dc3464a466860593231424e0a24dea",
        "408657c1d6dc39435b61e01b368dac69796481864b7462efd150a2d4e1b0daa1",
    )
    if expected != actual:
        raise RuntimeError("accepted component fingerprint drift")


def _require_clean_checkout() -> None:
    status = subprocess.run(
        ("git", "status", "--porcelain=v1"),
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    if status.strip():
        raise RuntimeError("multi-hour production run requires a clean checkout")


def run_multihour(
    *, duration_seconds: int, launch_collector: bool
) -> tuple[int, dict[str, Any]]:
    duration_seconds = parse_duration_seconds(duration_seconds)
    _validate_locked_contracts()
    _require_clean_checkout()
    if not PRODUCTION_DB.exists():
        raise FileNotFoundError(PRODUCTION_DB)

    started_at = smoke.utc_now()
    stamp = started_at.strftime("%Y%m%dT%H%M%S_%fZ")
    prepare_runtime_directory()
    paper_path = RUNTIME_DIR / f"phase4_firstpullback_multihour_{stamp}.sqlite3"
    json_path = RUNTIME_DIR / f"phase4_firstpullback_multihour_{stamp}.json"
    collector_log = RUNTIME_DIR / f"phase4_firstpullback_multihour_{stamp}_collector.log"
    if paper_path.exists() or json_path.exists():
        raise RuntimeError("new multi-hour artifact path already exists")

    database_identity = PRODUCTION_DB.resolve().as_posix()
    anchor = smoke.capture_source_anchor(PRODUCTION_DB, database_identity)
    source = ContinuousMarketSourceV01(
        PRODUCTION_DB,
        start_after_p1_rowid=anchor,
        database_identity=database_identity,
    )
    run_identity = session_identity(
        duration_seconds=duration_seconds,
        database_identity=database_identity,
        anchor=anchor,
    )
    metrics = smoke.RuntimeMetrics([])
    errors: list[str] = []
    collector: OwnedCollectorChild | None = None
    collector_pid: int | None = None
    collector_status = "EXISTING_COLLECTOR"
    conn: sqlite3.Connection | None = None
    binding: ContinuousFirstPullbackBindingV01 | None = None
    end_watermark = anchor
    graceful_shutdown = False
    duration_reached = False
    duration_boundary_instant: datetime | None = None
    stop_reason = "NOT_STARTED"
    continuity_started_monotonic = time.monotonic()
    continuity_stop_monotonic = continuity_started_monotonic
    continuity = SourceContinuityWatchdog(
        last_observed_source_p1_rowid=anchor,
        last_source_advance_monotonic=continuity_started_monotonic,
    )

    try:
        if launch_collector:
            collector = start_managed_collector(collector_log)
            collector_pid = collector.process.pid
            collector_status = "STARTED_BY_MULTIHOUR_RUN"
        conn = connect_entry_router_db(paper_path)
        binding = ContinuousFirstPullbackBindingV01(conn, source)
        loop_started = time.monotonic()
        next_clock = loop_started
        clock_index = 0
        while True:
            now_mono = time.monotonic()
            continuity_stop_monotonic = now_mono
            if collector is not None and collector.process.poll() is not None:
                raise RuntimeError(
                    "managed unlimited collector exited before duration bound: "
                    f"exit_code={collector.process.returncode}"
                )
            latest = source.latest_p1_rowid()
            continuity.observe(latest, now_mono)
            elapsed = now_mono - loop_started
            reason = run_control_reason(elapsed, duration_seconds)
            if reason == "DURATION_REACHED":
                duration_boundary_instant = smoke.utc_now()
                end_watermark = finalize_at_watermark(
                    conn,
                    binding,
                    metrics,
                    anchor=anchor,
                    instant=duration_boundary_instant,
                    timer_id=(
                        "MULTIHOUR:FINAL:"
                        f"{smoke.dt_text(duration_boundary_instant)}"
                    ),
                )
                duration_reached = True
                stop_reason = reason
                graceful_shutdown = True
                break
            metrics.maximum_source_cursor_lag_rowids = max(
                metrics.maximum_source_cursor_lag_rowids,
                max(0, latest - binding.durable_p1_rowid),
            )
            if now_mono >= next_clock:
                instant = smoke.utc_now()
                smoke._execute_clock_cycle(
                    binding,
                    metrics,
                    anchor=anchor,
                    instant=instant,
                    timer_id=f"MULTIHOUR:CLOCK:{clock_index:08d}:{smoke.dt_text(instant)}",
                )
                clock_index += 1
                next_clock += CLOCK_CADENCE_SECONDS
                if next_clock <= now_mono:
                    next_clock = now_mono + CLOCK_CADENCE_SECONDS
            else:
                smoke._process_once(binding, metrics, anchor=anchor)
            sleep_for = min(
                MARKET_POLL_SECONDS,
                max(0.0, next_clock - time.monotonic()),
            )
            if sleep_for > 0:
                time.sleep(sleep_for)

    except ProductionSourceStalled:
        continuity_stop_monotonic = time.monotonic()
        stop_reason = "PRODUCTION_SOURCE_STALLED"
        errors.append("PRODUCTION_SOURCE_STALLED")
        if conn is not None and binding is not None:
            try:
                # Do not capture a new live watermark here. In the late-advance
                # failure ordering, doing so could admit the row the watchdog
                # rejected or persist a timer beyond the accepted boundary.
                end_watermark = preserve_source_stall_failure_state(
                    binding
                )
                graceful_shutdown = True
            except BindingConflict as final_exc:
                metrics.conflicts += 1
                errors.append(f"stall state preservation BindingConflict:{final_exc}")
            except Exception as final_exc:
                errors.append(
                    "stall state preservation failed: "
                    f"{type(final_exc).__name__}:{final_exc}"
                )
    except BindingConflict as exc:
        continuity_stop_monotonic = time.monotonic()
        metrics.conflicts += 1
        stop_reason = "FAIL_CLOSED"
        errors.append(f"BindingConflict:{exc}")
    except Exception as exc:
        continuity_stop_monotonic = time.monotonic()
        stop_reason = "FAIL_CLOSED"
        errors.append(f"{type(exc).__name__}:{exc}")
    finally:
        if conn is not None:
            conn.close()
        collector_cleanup_ok = True
        collector_cleanup_method = "NOT_STARTED_BY_RUN"
        collector_exit_code: int | None = None
        collector_absent_after_cleanup = True
        if collector is not None:
            collector_cleanup_ok = collector.stop(
                timeout_seconds=COLLECTOR_CLEANUP_TIMEOUT_SECONDS
            )
            collector_cleanup_method = collector.cleanup_method
            collector_exit_code = collector.process.returncode
            collector_absent_after_cleanup = collector.process.poll() is not None
            if not collector_cleanup_ok or not collector_absent_after_cleanup:
                errors.append("managed collector did not complete owned-child cleanup")

    ended_at = smoke.utc_now()
    actual_duration_seconds = (ended_at - started_at).total_seconds()
    source_continuity = continuity.snapshot(continuity_stop_monotonic)
    paper_summary: dict[str, Any] = {}
    integrity: dict[str, Any] = {"integrity_ok": False, "quick_check": "NOT_RUN"}
    restart_ok = False
    restart_digest: str | None = None
    candidate_details: list[dict[str, Any]] = []
    if paper_path.exists():
        try:
            audit_cursor = end_watermark
            if not graceful_shutdown:
                cursor_conn = sqlite3.connect(paper_path)
                try:
                    row = cursor_conn.execute(
                        "SELECT last_durable_p1_rowid FROM "
                        "paper_fp_binding_runtime_v0_1 WHERE singleton=1"
                    ).fetchone()
                    audit_cursor = anchor if row is None else max(anchor, int(row[0]))
                finally:
                    cursor_conn.close()
            integrity = smoke._integrity_audit(
                paper_path,
                source=source,
                final_cursor=int(audit_cursor),
            )
            summary_conn = sqlite3.connect(paper_path)
            summary_conn.row_factory = sqlite3.Row
            try:
                paper_summary = smoke._paper_summary(summary_conn)
                candidate_details = _candidate_details(summary_conn)
            finally:
                summary_conn.close()
            paper_summary["candidate_details"] = candidate_details
            paper_summary["session_track_metrics"] = build_session_track_metrics(
                paper_summary
            )
            restart_ok, restart_digest = smoke.restart_probe(paper_path, source)
            if not restart_ok:
                errors.append(f"restart probe failed: {restart_digest}")
        except Exception as exc:
            errors.append(f"postrun audit failed: {type(exc).__name__}:{exc}")

    try:
        production = (
            smoke._production_evidence(
                PRODUCTION_DB,
                anchor=anchor,
                final_cursor=end_watermark,
            )
            if end_watermark >= anchor
            else {"fresh_launches": 0, "gap_rows": 0, "unsupported_rows": 0}
        )
    except Exception as exc:
        errors.append(f"production evidence failed: {type(exc).__name__}:{exc}")
        production = {"fresh_launches": 0, "gap_rows": 0, "unsupported_rows": 0}
    result_class = classify_multihour_result(
        duration_reached=duration_reached,
        errors=errors,
        integrity_ok=bool(integrity.get("integrity_ok", False)),
        graceful_shutdown=graceful_shutdown,
        restart_ok=restart_ok,
        source_continuity_ok=bool(source_continuity["continuity_ok"]),
        raw_rows=metrics.raw_rows,
        fresh_launches=production["fresh_launches"],
        evaluated_mints=int(paper_summary.get("evaluated_mints", 0)),
        evaluations=int(paper_summary.get("evaluations", 0)),
        timers_executed=metrics.timers_executed,
    )
    per_second = max(actual_duration_seconds, 0.000001)
    summary = {
        "multihour_model_id": MODEL_ID,
        "multihour_fingerprint": MODEL_FINGERPRINT,
        "session_identity": run_identity,
        "spec": MULTIHOUR_SPEC,
        "git_head": smoke._git_head(),
        "requested_duration_seconds": duration_seconds,
        "started_at_utc": smoke.dt_text(started_at),
        "ended_at_utc": smoke.dt_text(ended_at),
        "actual_duration_seconds": actual_duration_seconds,
        "duration_bound_reached": duration_reached,
        "duration_boundary_utc": (
            None
            if duration_boundary_instant is None
            else smoke.dt_text(duration_boundary_instant)
        ),
        "stop_reason": stop_reason,
        "candidate_early_stop": False,
        "collector": {
            "status": collector_status,
            "started_by_run": launch_collector,
            "pid": collector_pid,
            "unlimited_mode": launch_collector,
            "max_pump_events": UNLIMITED_COLLECTOR_MAX_PUMP_EVENTS,
            "cleanup_ok": collector_cleanup_ok,
            "cleanup_method": collector_cleanup_method,
            "exit_code": collector_exit_code,
            "absent_after_cleanup": collector_absent_after_cleanup,
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
        "source_continuity": source_continuity,
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
            "p95_batch_processing_latency_ms": smoke._percentile_95(
                metrics.batch_latencies_ms
            ),
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
        "artifact_paths": {"paper_db": str(paper_path), "json": str(json_path)},
    }
    smoke.write_json_artifact(json_path, summary)
    summary["artifact_sha256"] = {
        "paper_db": smoke.sha256_file(paper_path) if paper_path.exists() else None,
        "json": smoke.sha256_file(json_path),
    }
    print("=" * 112)
    print("BOUNDED MULTI-HOUR FIRSTPULLBACK PAPER RUN v0.1")
    print("=" * 112)
    print(f"Model              : {MODEL_ID}")
    print(f"Fingerprint        : {MODEL_FINGERPRINT}")
    print(f"Requested seconds  : {duration_seconds}")
    print(f"Start anchor       : {anchor}")
    print(f"Final cursor       : {end_watermark if graceful_shutdown else 'UNRESOLVED'}")
    print(f"Candidates         : {paper_summary.get('candidates', 0)}")
    print(f"Paper DB           : {paper_path}")
    print(f"JSON summary       : {json_path}")
    print(f"RESULT CLASS       : {result_class}")
    print(f"RESULT: {'PASS' if result_class == 'PASS_MULTI_HOUR' else 'FAIL'}")
    return (0 if result_class == "PASS_MULTI_HOUR" else 1), summary


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--collector-child", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument(
        "--collector-child-probe", action="store_true", help=argparse.SUPPRESS
    )
    parser.add_argument(
        "--collector-child-probe-ready", type=Path, help=argparse.SUPPRESS
    )
    parser.add_argument(
        "--duration-seconds",
        type=parse_duration_seconds,
        default=DEFAULT_DURATION_SECONDS,
        help=(
            f"Bounded run duration in seconds ({MIN_DURATION_SECONDS}.."
            f"{MAX_DURATION_SECONDS}); default {DEFAULT_DURATION_SECONDS}."
        ),
    )
    collector = parser.add_mutually_exclusive_group(required=False)
    collector.add_argument(
        "--use-existing-collector",
        action="store_true",
        help="Use an independently verified healthy v0.3.4 collector.",
    )
    collector.add_argument(
        "--launch-managed-collector",
        action="store_true",
        help="Launch accepted v0.3.4 unlimited mode in an owned process group.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    if args.collector_child:
        return run_collector_child()
    if args.collector_child_probe:
        return run_collector_child_probe(args.collector_child_probe_ready)
    if not args.use_existing_collector and not args.launch_managed_collector:
        parser.error("one collector mode is required")
    code, _ = run_multihour(
        duration_seconds=args.duration_seconds,
        launch_collector=bool(args.launch_managed_collector),
    )
    return code


if __name__ == "__main__":
    raise SystemExit(main())
