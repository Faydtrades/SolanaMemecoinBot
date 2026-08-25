from __future__ import annotations

import json
import shutil
import sqlite3
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
for import_root in (PROJECT_ROOT / "src", PROJECT_ROOT / "scripts"):
    if str(import_root) not in sys.path:
        sys.path.insert(0, str(import_root))

import phase4_continuous_firstpullback_binding_selftest_v0_1 as fixtures  # noqa: E402
from phase4.paper_continuous_firstpullback_binding_v0_3 import (  # noqa: E402
    ContinuousFirstPullbackBindingV03,
)
from phase4.paper_continuous_firstpullback_binding_v0_4 import (  # noqa: E402
    MODEL_FINGERPRINT,
    MODEL_ID,
    ContinuousFirstPullbackBindingV04,
)
from phase4.paper_continuous_market_source_v0_2 import (  # noqa: E402
    ContinuousMarketSourceV02,
)
from phase4.paper_entry_router_v0_1 import connect_entry_router_db  # noqa: E402
from phase4_continuous_firstpullback_multihour_run_v0_3 import (  # noqa: E402
    EvidenceBasedSourceContinuityWatchdogV03,
    ProductionSourceStalled,
)


BASE = datetime(2026, 8, 25, 6, 0, tzinfo=timezone.utc)
EXPECTED_BINDING_FINGERPRINT = (
    "e2c35ce63fe419ac4a934913dd26249d72411884cb115e0701f601b276b0f7d6"
)
THROUGHPUT_MINIMUM_RPS = 15.0


class MutableClock:
    def __init__(self, value: datetime = BASE) -> None:
        self.value = value

    def __call__(self) -> datetime:
        return self.value


class FailOnce:
    def __init__(self, target: str) -> None:
        self.target = target
        self.triggered = False

    def fence(self, phase: str) -> None:
        if not self.triggered and phase == self.target:
            self.triggered = True
            raise RuntimeError("INJECTED:" + phase)

    def binding(self, _input_key: str, phase: str) -> None:
        if not self.triggered and phase == self.target:
            self.triggered = True
            raise RuntimeError("INJECTED:" + phase)


def create_source(path: Path) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.execute(fixtures.SCHEMA_SQL)
        conn.execute("ALTER TABLE pump_events ADD COLUMN inserted_at_utc TEXT")
        conn.commit()
    finally:
        conn.close()


def _timestamp(rowid: int) -> datetime:
    return BASE + timedelta(milliseconds=125 * rowid)


def append_rows(path: Path, start: int, end: int) -> None:
    conn = sqlite3.connect(path)
    try:
        skip_rows = []
        for rowid in range(start, end + 1):
            at = _timestamp(rowid)
            within = (rowid - 1) % 20
            block = (rowid - 1) // 20
            if rowid == 20:
                fixtures.insert_row(
                    conn,
                    rowid,
                    "GAP-ONLY",
                    "LAUNCH",
                    observed_offset=max(1, rowid),
                    source="GAP_RECONCILIATION_V0_3_4",
                )
                conn.execute(
                    "UPDATE pump_events SET decoded_at_utc=?,inserted_at_utc=? "
                    "WHERE rowid=?",
                    (at.isoformat(), at.isoformat(), rowid),
                )
            elif within <= 4:
                mint = f"MINT-{block:05d}"
                event_type = "LAUNCH" if within == 0 else "BUY"
                fixtures.insert_row(
                    conn,
                    rowid,
                    mint,
                    event_type,
                    price_units=10_000 + (within * 75),
                    observed_offset=max(1, rowid),
                )
                conn.execute(
                    "UPDATE pump_events SET decoded_at_utc=?,inserted_at_utc=? "
                    "WHERE rowid=?",
                    (at.isoformat(), at.isoformat(), rowid),
                )
            else:
                skip_rows.append((
                    rowid,
                    f"skip-{rowid}",
                    f"skip-sig-{rowid}",
                    "BUY",
                    "LIVE_WEBSOCKET_EVENT_V0_3_4",
                    at.isoformat(),
                    at.isoformat(),
                ))
        conn.executemany(
            "INSERT INTO pump_events(rowid,event_key,signature,event_type,mint,"
            "source_decoded_file,decoded_at_utc,inserted_at_utc) "
            "VALUES(?,?,?,?,NULL,?,?,?)",
            skip_rows,
        )
        conn.commit()
    finally:
        conn.close()


def make_source(path: Path, identity: str) -> ContinuousMarketSourceV02:
    return ContinuousMarketSourceV02(
        path,
        start_after_p1_rowid=0,
        database_identity=identity,
    )


def stable_evaluation_rows(conn: sqlite3.Connection) -> tuple[tuple[Any, ...], ...]:
    return tuple(
        tuple(row)
        for row in conn.execute(
            "SELECT evaluation_id,run_id,role,mint,strategy_version,"
            "parameter_set_id,ingest_seq,evaluated_at_us,current_state,"
            "transition_occurred,reason_code,filters_passed_json,"
            "filters_failed_json,signal_type,candidate_signal_id,"
            "audit_snapshot_json,content_fingerprint "
            "FROM paper_strategy_evaluations ORDER BY evaluation_id"
        ).fetchall()
    )


def stable_audit_rows(conn: sqlite3.Connection) -> tuple[tuple[Any, ...], ...]:
    return tuple(
        tuple(row)
        for row in conn.execute(
            "SELECT input_key,ordinal,evaluation_json,content_fingerprint,"
            "accepted_evaluation_id,delivered "
            "FROM paper_fp_binding_evaluation_audit_v0_1 "
            "ORDER BY input_key,ordinal"
        ).fetchall()
    )


def profile_v03(source_path: Path, paper_path: Path) -> dict[str, Any]:
    source = make_source(source_path, "SELFTEST:T015:PROFILE-V03")
    conn = connect_entry_router_db(paper_path)
    binding = ContinuousFirstPullbackBindingV03(conn, source, wall_clock=lambda: BASE)
    totals = {
        "hydration_seconds": 0.0,
        "strategy_evaluation_seconds": 0.0,
        "semantic_outbox_seconds": 0.0,
        "cursor_commit_seconds": 0.0,
    }

    def wrap(metric: str, name: str) -> None:
        original = getattr(binding, name)

        def measured(*args, **kwargs):
            started = time.perf_counter()
            try:
                return original(*args, **kwargs)
            finally:
                totals[metric] += time.perf_counter() - started

        setattr(binding, name, measured)

    wrap("hydration_seconds", "_hydrate")
    wrap("strategy_evaluation_seconds", "_plan_record")
    original_evaluations = binding._drain_evaluations
    original_input = binding._drain_input

    def evaluations(*args, **kwargs):
        started = time.perf_counter()
        try:
            return original_evaluations(*args, **kwargs)
        finally:
            totals["semantic_outbox_seconds"] += time.perf_counter() - started

    def outbox(*args, **kwargs):
        started = time.perf_counter()
        try:
            return original_input(*args, **kwargs)
        finally:
            totals["semantic_outbox_seconds"] += time.perf_counter() - started

    binding._drain_evaluations = evaluations
    binding._drain_input = outbox
    wrap("cursor_commit_seconds", "_complete_production_input")
    timer = binding.prepare_clock_tick(BASE, timer_id="PROFILE-V03")
    watermark = int(binding._prepared_timer(timer)["source_watermark_p1_rowid"])
    raw = normalized = batches = 0
    started = time.perf_counter()
    while binding.durable_p1_rowid < watermark:
        result = binding.process_next_batch(batch_size=250)
        raw += result.raw_rows_fetched
        normalized += result.normalized_records
        batches += 1
    drain_seconds = time.perf_counter() - started
    timer_started = time.perf_counter()
    timer_result = binding.execute_prepared_timer(timer)
    timer_seconds = time.perf_counter() - timer_started
    evaluations_snapshot = stable_evaluation_rows(conn)
    audit_snapshot = stable_audit_rows(conn)
    conn.close()
    return {
        "raw_rows": raw,
        "normalized_rows": normalized,
        "batches": batches,
        "fence_interval_seconds": drain_seconds,
        "raw_rows_per_second": raw / max(drain_seconds, 0.000001),
        "timer_execution_seconds": timer_seconds,
        "timer_status": timer_result.status,
        "component_seconds": totals,
        "evaluation_rows": evaluations_snapshot,
        "audit_rows": audit_snapshot,
    }


def profile_v04(source_path: Path, paper_path: Path) -> dict[str, Any]:
    source = make_source(source_path, "SELFTEST:T015:PROFILE-V04")
    conn = connect_entry_router_db(paper_path)
    binding = ContinuousFirstPullbackBindingV04(conn, source, wall_clock=lambda: BASE)
    timer = binding.prepare_clock_tick(BASE, timer_id="PROFILE-V04")
    started = time.perf_counter()
    result = binding.drain_and_execute_prepared_timer(timer, batch_size=250)
    drain_seconds = time.perf_counter() - started
    evaluation_rows = stable_evaluation_rows(conn)
    audit_rows = stable_audit_rows(conn)
    metrics = binding.performance_metrics()
    conn.close()
    return {
        "raw_rows": result.fence_drain_raw_rows,
        "normalized_rows": result.fence_drain_normalized_rows,
        "batches": result.fence_drain_batches,
        "fence_interval_seconds": drain_seconds,
        "raw_rows_per_second": result.fence_drain_raw_rows
        / max(drain_seconds, 0.000001),
        "metrics": metrics,
        "evaluation_rows": evaluation_rows,
        "audit_rows": audit_rows,
    }


def crash_restart_case(root: Path, phase: str, *, binding_phase: bool) -> bool:
    source_path = root / f"crash_{phase}_source.sqlite3"
    paper_path = root / f"crash_{phase}_paper.sqlite3"
    create_source(source_path)
    append_rows(source_path, 1, 40)
    injector = FailOnce(phase)
    source = make_source(source_path, f"SELFTEST:T015:CRASH:{phase}")
    conn = connect_entry_router_db(paper_path)
    binding = ContinuousFirstPullbackBindingV04(
        conn,
        source,
        wall_clock=lambda: BASE,
        failure_injector=injector.binding if binding_phase else None,
        fence_failure_injector=injector.fence if not binding_phase else None,
    )
    timer = binding.prepare_clock_tick(BASE, timer_id="CRASH-TIMER")
    crashed = False
    try:
        binding.drain_and_execute_prepared_timer(timer, batch_size=10)
    except RuntimeError as exc:
        crashed = str(exc) == "INJECTED:" + phase
    conn.close()
    if not crashed or not injector.triggered:
        return False

    source = make_source(source_path, f"SELFTEST:T015:CRASH:{phase}")
    conn = connect_entry_router_db(paper_path)
    recovered = ContinuousFirstPullbackBindingV04(
        conn, source, wall_clock=lambda: BASE
    )
    recovered.drain_and_execute_prepared_timer(timer, batch_size=10)
    first_digest = recovered.canonical_digest()
    state = conn.execute(
        "SELECT last_durable_p1_rowid FROM paper_fp_binding_runtime_v0_1"
    ).fetchone()[0]
    timer_status = conn.execute(
        "SELECT status FROM paper_fp_binding_prepared_timers_v0_1 "
        "WHERE timer_key=?",
        (timer,),
    ).fetchone()[0]
    duplicates = conn.execute(
        "SELECT COUNT(*) FROM (SELECT input_key,COUNT(*) n "
        "FROM paper_fp_binding_inputs_v0_1 GROUP BY input_key HAVING n>1)"
    ).fetchone()[0]
    conn.close()
    conn = connect_entry_router_db(paper_path)
    replayed = ContinuousFirstPullbackBindingV04(
        conn, source, wall_clock=lambda: BASE
    )
    replayed.drain_and_execute_prepared_timer(timer, batch_size=10)
    second_digest = replayed.canonical_digest()
    quick = conn.execute("PRAGMA quick_check").fetchone()[0]
    conn.close()
    return (
        state == 40
        and timer_status == "COMPLETE"
        and duplicates == 0
        and quick == "ok"
        and first_digest == second_digest
    )


def check(name: str, condition: bool, checks: dict[str, bool]) -> None:
    checks[name] = bool(condition)


def main() -> int:
    checks: dict[str, bool] = {}
    evidence: dict[str, Any] = {}
    root = Path(tempfile.mkdtemp(prefix="phase4_t015_fence_"))
    try:
        check(
            "MODEL_IDENTITY",
            MODEL_ID == "P4-CONTINUOUS-FIRSTPULLBACK-BINDING-0004"
            and MODEL_FINGERPRINT == EXPECTED_BINDING_FINGERPRINT,
            checks,
        )

        profile_source = root / "profile_source.sqlite3"
        create_source(profile_source)
        append_rows(profile_source, 1, 500)
        v03 = profile_v03(profile_source, root / "profile_v03.sqlite3")
        v04 = profile_v04(profile_source, root / "profile_v04.sqlite3")
        evidence["v03_profile"] = {
            key: value
            for key, value in v03.items()
            if key not in {"evaluation_rows", "audit_rows"}
        }
        evidence["v04_profile"] = {
            key: value
            for key, value in v04.items()
            if key not in {"evaluation_rows", "audit_rows"}
        }
        evidence["v04_vs_v03_ratio"] = (
            v04["raw_rows_per_second"] / v03["raw_rows_per_second"]
        )
        check(
            "A_REPRODUCE_PROFILE_V03_FENCE_DELAY",
            v03["raw_rows"] == 500
            and v03["timer_status"] == "COMMITTED"
            and v03["component_seconds"]["semantic_outbox_seconds"]
            > v03["component_seconds"]["hydration_seconds"],
            checks,
        )
        check(
            "A2_EVALUATION_BATCHING_SEMANTIC_EQUIVALENCE",
            v03["evaluation_rows"] == v04["evaluation_rows"]
            and v03["audit_rows"] == v04["audit_rows"],
            checks,
        )

        ready_source = root / "ready_source.sqlite3"
        create_source(ready_source)
        append_rows(ready_source, 1, 5_000)
        source = make_source(ready_source, "SELFTEST:T015:READY")
        ready_paper = root / "ready_paper.sqlite3"
        conn = connect_entry_router_db(ready_paper)
        binding = ContinuousFirstPullbackBindingV04(
            conn, source, wall_clock=lambda: BASE
        )
        timer = binding.prepare_clock_tick(BASE, timer_id="READY-5000")
        append_rows(ready_source, 5_001, 5_010)
        sleeps = 0

        def forbidden_sleep() -> None:
            nonlocal sleeps
            sleeps += 1
            raise AssertionError("ready fence drain attempted a poll sleep")

        result = binding.drain_and_execute_prepared_timer(
            timer,
            batch_size=250,
            source_unavailable_wait=forbidden_sleep,
        )
        max_input_before_timer = conn.execute(
            "SELECT MAX(production_p1_rowid) FROM paper_fp_binding_inputs_v0_1"
        ).fetchone()[0]
        gap_kind = conn.execute(
            "SELECT input_kind FROM paper_fp_binding_inputs_v0_1 "
            "WHERE production_p1_rowid=20"
        ).fetchone()[0]
        check(
            "B_ACTIVE_DRAIN_READY_BACKLOG",
            result.final_durable_p1_rowid == 5_000
            and result.fence_drain_raw_rows == 5_000,
            checks,
        )
        check(
            "C_ZERO_INTENTIONAL_SLEEP_READY_BACKLOG",
            sleeps == 0
            and binding.performance_metrics()["passive_fence_wait_seconds"] == 0,
            checks,
        )
        check(
            "E_NO_W_PLUS_1_SEMANTIC_EFFECT_BEFORE_TIMER",
            max_input_before_timer == 5_000 and gap_kind == "SKIP",
            checks,
        )
        check(
            "F_5000_ROW_FENCE_DRAIN",
            result.fence_drain_batches == 20
            and binding.pending_outbox_count() == 0,
            checks,
        )
        resumed = binding.process_next_batch(batch_size=10)
        check(
            "I_FORWARD_PROGRESSION_RESUMES_AFTER_TIMER",
            resumed.durable_p1_rowid == 5_010
            and resumed.raw_rows_fetched == 10,
            checks,
        )
        digest_before = binding.canonical_digest()
        conn.close()
        conn = connect_entry_router_db(ready_paper)
        reopened = ContinuousFirstPullbackBindingV04(
            conn, source, wall_clock=lambda: BASE
        )
        digest_after = reopened.canonical_digest()
        check("P_RESTART_REPLAY_EXACT", digest_before == digest_after, checks)
        conn.close()

        order_source = root / "order_source.sqlite3"
        create_source(order_source)
        append_rows(order_source, 1, 100)
        source = make_source(order_source, "SELFTEST:T015:ORDER")
        conn = connect_entry_router_db(root / "order_paper.sqlite3")
        binding = ContinuousFirstPullbackBindingV04(
            conn, source, wall_clock=lambda: BASE
        )
        binding.prepare_clock_tick(BASE, timer_id="B-SAME-W")
        binding.prepare_clock_tick(BASE, timer_id="A-SAME-W")
        pending_at_w_minus_1 = binding.execute_prepared_timer("A-SAME-W")
        append_rows(order_source, 101, 200)
        later = binding.prepare_clock_tick(BASE + timedelta(seconds=1), timer_id="C-W2")
        order_result = binding.drain_and_execute_prepared_timer(
            later, batch_size=25
        )
        check(
            "G_MULTIPLE_SAME_W_TIMERS",
            pending_at_w_minus_1.status == "PENDING"
            and pending_at_w_minus_1.source_watermark_p1_rowid == 100
            and order_result.executed_timer_keys[:2]
            == ("A-SAME-W", "B-SAME-W"),
            checks,
        )
        check(
            "H_W1_W2_TIMER_ORDERING",
            order_result.executed_timer_keys == (
                "A-SAME-W",
                "B-SAME-W",
                "C-W2",
            )
            and order_result.final_durable_p1_rowid == 200,
            checks,
        )
        conn.close()

        unavailable_source_path = root / "unavailable_source.sqlite3"
        create_source(unavailable_source_path)
        append_rows(unavailable_source_path, 1, 10)
        source = make_source(
            unavailable_source_path, "SELFTEST:T015:SOURCE-UNAVAILABLE"
        )
        conn = connect_entry_router_db(root / "unavailable_paper.sqlite3")
        binding = ContinuousFirstPullbackBindingV04(
            conn, source, wall_clock=lambda: BASE
        )
        timer = binding.prepare_clock_tick(BASE, timer_id="UNAVAILABLE")
        original_latest = source.latest_p1_rowid
        availability_calls = 0

        def temporarily_unavailable() -> int:
            nonlocal availability_calls
            availability_calls += 1
            return 9 if availability_calls == 1 else original_latest()

        source.latest_p1_rowid = temporarily_unavailable
        progress_values: list[int] = []
        binding.drain_and_execute_prepared_timer(
            timer,
            batch_size=10,
            progress_probe=progress_values.append,
            source_unavailable_wait=lambda: time.sleep(0.002),
        )
        unavailable_metrics = binding.performance_metrics()
        check(
            "G2_SOURCE_UNAVAILABLE_WAIT_IS_SEPARATE_AND_BOUNDED",
            availability_calls >= 2
            and unavailable_metrics["source_unavailable_fence_wait_seconds"]
            >= 0.001
            and unavailable_metrics["passive_fence_wait_seconds"] == 0.0
            and progress_values
            and min(progress_values) >= 10
            and binding.durable_p1_rowid == 10,
            checks,
        )
        conn.close()

        source = make_source(
            unavailable_source_path, "SELFTEST:T015:SOURCE-NOOP-WAIT"
        )
        conn = connect_entry_router_db(root / "noop_wait_paper.sqlite3")
        binding = ContinuousFirstPullbackBindingV04(
            conn, source, wall_clock=lambda: BASE
        )
        timer = binding.prepare_clock_tick(BASE, timer_id="NOOP-WAIT")
        source.latest_p1_rowid = lambda: 9
        noop_rejected = False
        try:
            binding.drain_and_execute_prepared_timer(
                timer,
                batch_size=10,
                source_unavailable_wait=lambda: None,
            )
        except Exception as exc:
            noop_rejected = "returned without a bounded wait" in str(exc)
        check("G3_NOOP_WAIT_CANNOT_BUSY_LOOP", noop_rejected, checks)
        conn.close()

        crash_cases = {
            "J_CRASH_AFTER_FETCH": ("after_source_fetch", False),
            "K_CRASH_AFTER_NORMALIZATION": ("after_source_normalization", False),
            "L_CRASH_AFTER_SEMANTIC_PERSIST": (
                "after_strategy_state_persistence_before_runner",
                True,
            ),
            "M_CRASH_BEFORE_CURSOR_COMMIT": (
                "after_runner_items_before_production_cursor",
                True,
            ),
            "N_CRASH_AT_W_BEFORE_TIMER": (
                "after_final_cursor_before_timer",
                False,
            ),
            "O_CRASH_AFTER_TIMER": ("after_timer_execution", False),
        }
        for label, (phase, binding_phase) in crash_cases.items():
            check(
                label,
                crash_restart_case(root, phase, binding_phase=binding_phase),
                checks,
            )

        check(
            "Q_NO_DUPLICATE_IDENTITIES",
            all(checks[label] for label in crash_cases),
            checks,
        )

        boundary_source = root / "boundary_source.sqlite3"
        create_source(boundary_source)
        source = make_source(boundary_source, "SELFTEST:T015:BOUNDARY")
        clock = MutableClock(BASE)

        def reason_at(seconds: float) -> str | None:
            watchdog = EvidenceBasedSourceContinuityWatchdogV03(
                source=source,
                last_observed_source_p1_rowid=0,
                last_source_advance_monotonic=0.0,
                session_started_at_utc=BASE,
                wall_clock=clock,
            )
            clock.value = BASE + timedelta(seconds=seconds)
            try:
                watchdog.observe(0, seconds)
            except ProductionSourceStalled as exc:
                return str(exc)
            return None

        check("R_T014_SOURCE_STALL_UNCHANGED", reason_at(300.0) == "PRODUCTION_SOURCE_STALLED", checks)
        starvation_source_path = root / "starvation_source.sqlite3"
        create_source(starvation_source_path)
        append_rows(starvation_source_path, 1, 2_400)
        starvation_source = make_source(
            starvation_source_path, "SELFTEST:T015:STARVATION"
        )
        starvation_clock = MutableClock(_timestamp(2_400))
        starvation_watchdog = EvidenceBasedSourceContinuityWatchdogV03(
            source=starvation_source,
            last_observed_source_p1_rowid=0,
            last_source_advance_monotonic=0.0,
            session_started_at_utc=BASE,
            wall_clock=starvation_clock,
        )
        starvation_reason = None
        try:
            starvation_watchdog.observe(2_400, 300.0)
        except ProductionSourceStalled as exc:
            starvation_reason = str(exc)
        check(
            "S_T014_RUNNER_STARVATION_UNCHANGED",
            starvation_reason == "RUNNER_SOURCE_POLL_STARVED"
            and not starvation_watchdog.actual_source_stalled,
            checks,
        )
        check(
            "T_EXACT_BOUNDARIES_UNCHANGED",
            reason_at(299.999) is None
            and reason_at(300.0) == "PRODUCTION_SOURCE_STALLED"
            and reason_at(300.001) == "PRODUCTION_SOURCE_STALLED",
            checks,
        )

        benchmark_source = root / "benchmark_source.sqlite3"
        create_source(benchmark_source)
        append_rows(benchmark_source, 1, 4_000)
        source = make_source(benchmark_source, "SELFTEST:T015:20K")
        conn = connect_entry_router_db(root / "benchmark_paper.sqlite3")
        binding = ContinuousFirstPullbackBindingV04(
            conn, source, wall_clock=lambda: BASE
        )
        health_clock = MutableClock(BASE)
        monotonic = 0.0
        watchdog = EvidenceBasedSourceContinuityWatchdogV03(
            source=source,
            last_observed_source_p1_rowid=0,
            last_source_advance_monotonic=0.0,
            session_started_at_utc=BASE,
            wall_clock=health_clock,
        )
        processed = normalized = 0
        cursor_lags: list[int] = []
        timer_count = 0
        benchmark_started = time.perf_counter()
        target = 4_000
        while target <= 20_000:
            timer_key = binding.prepare_clock_tick(
                BASE + timedelta(milliseconds=125 * target),
                timer_id=f"BENCH-{target:05d}",
            )
            if target < 20_000:
                append_rows(benchmark_source, target + 1, target + 4_000)
            latest = source.latest_p1_rowid()
            health_clock.value = _timestamp(latest)

            def progress(latest_rowid: int) -> None:
                nonlocal monotonic
                monotonic += 0.05
                health_clock.value = _timestamp(latest_rowid)
                watchdog.observe(latest_rowid, monotonic)

            drained = binding.drain_and_execute_prepared_timer(
                timer_key,
                batch_size=250,
                progress_probe=progress,
                source_unavailable_wait=lambda: (_ for _ in ()).throw(
                    AssertionError("ready benchmark source became unavailable")
                ),
            )
            processed += drained.fence_drain_raw_rows
            normalized += drained.fence_drain_normalized_rows
            timer_count += len(drained.executed_timer_keys)
            cursor_lags.append(latest - binding.durable_p1_rowid)
            target += 4_000
        benchmark_seconds = time.perf_counter() - benchmark_started
        benchmark_metrics = binding.performance_metrics()
        health = watchdog.snapshot(monotonic)
        raw_rps = processed / max(benchmark_seconds, 0.000001)
        normalized_rps = normalized / max(benchmark_seconds, 0.000001)
        fence_rps = processed / max(
            benchmark_metrics["fence_drain_seconds"], 0.000001
        )
        maximum_lag = max(cursor_lags)
        evidence["throughput_20k"] = {
            "raw_rows": processed,
            "normalized_rows": normalized,
            "seconds": benchmark_seconds,
            "raw_rows_per_second": raw_rps,
            "normalized_rows_per_second": normalized_rps,
            "timer_count": timer_count,
            "fence_drain_rows_per_second": fence_rps,
            "maximum_cursor_lag_rowids": maximum_lag,
            "cursor_lags_after_timer": cursor_lags,
            "maximum_fence_drain_seconds": benchmark_metrics[
                "maximum_fence_drain_seconds"
            ],
            "maximum_runner_probe_interval_seconds": health[
                "maximum_runner_probe_interval_seconds"
            ],
            "performance": benchmark_metrics,
        }
        check(
            "D_FIXED_WATERMARK_UNDER_CONTINUED_WRITES",
            cursor_lags[:-1] == [4_000, 4_000, 4_000, 4_000]
            and cursor_lags[-1] == 0,
            checks,
        )
        check(
            "W_20K_END_TO_END_THROUGHPUT",
            processed == 20_000
            and normalized >= 4_900
            and timer_count == 5,
            checks,
        )
        check("X_MINIMUM_15_RAW_ROWS_PER_SECOND", raw_rps >= THROUGHPUT_MINIMUM_RPS, checks)
        check(
            "Y_CURSOR_LAG_DOES_NOT_MONOTONICALLY_DIVERGE",
            maximum_lag == 4_000 and cursor_lags[-1] == 0,
            checks,
        )
        check(
            "Z_NIGHTLY_STARVATION_SHAPE_REMOVED",
            not health["actual_source_stalled"]
            and not health["runner_source_poll_starved"]
            and health["maximum_runner_probe_interval_seconds"] < 300.0
            and benchmark_metrics["passive_fence_wait_seconds"] == 0,
            checks,
        )
        check(
            "U_SOURCE_V02_CACHE_PRESERVED",
            source.cache_metrics()["full_launch_rebuild_queries"] <= 1
            and source.cache_metrics()[
                "normal_monotonic_fetches_without_rebuild"
            ]
            == 79
            and source.cache_metrics()["incremental_launch_catchup_queries"]
            == 0
            and source.cache_metrics()["rewind_rebuild_queries"] == 0,
            checks,
        )
        conn.close()
    finally:
        shutil.rmtree(root, ignore_errors=True)

    print("=" * 108)
    print("TIMER-FENCE ACTIVE DRAIN + THROUGHPUT SELF-TEST V0.1")
    print("=" * 108)
    for name, passed in checks.items():
        print(f"{name}: {'PASS' if passed else 'FAIL'}")
    print("PROFILE_AND_BENCHMARK=" + json.dumps(evidence, sort_keys=True, default=str))
    passed = bool(checks) and all(checks.values())
    print(f"RESULT: {'PASS' if passed else 'FAIL'}")
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
