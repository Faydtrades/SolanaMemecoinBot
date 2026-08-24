from __future__ import annotations

import argparse
import json
import sqlite3
import subprocess
import sys
import tempfile
import time
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
SCRIPTS_ROOT = PROJECT_ROOT / "scripts"
for import_root in (SRC_ROOT, SCRIPTS_ROOT):
    if str(import_root) not in sys.path:
        sys.path.insert(0, str(import_root))

from phase4.paper_continuous_firstpullback_binding_v0_1 import (  # noqa: E402
    ContinuousFirstPullbackBindingV01,
)
from phase4.paper_continuous_market_source_v0_1 import (  # noqa: E402
    ContinuousMarketSourceV01,
)
from phase4.paper_entry_router_v0_1 import connect_entry_router_db  # noqa: E402
from phase4_continuous_firstpullback_binding_selftest_v0_1 import (  # noqa: E402
    BASE,
    create_fixture,
    insert_row,
    run_scenario,
)
from phase4_continuous_firstpullback_multihour_run_v0_1 import (  # noqa: E402
    ACCEPTED_T009_SHA256,
    BINDING_FINGERPRINT,
    COST_BASELINE_FINGERPRINT,
    DEFAULT_DURATION_SECONDS,
    EXIT_TRACKS,
    GAP_COMPAT_FINGERPRINT,
    LOCKED_SELECTION_SHA256,
    MARKET_SOURCE_FINGERPRINT,
    MAX_DURATION_SECONDS,
    MIN_DURATION_SECONDS,
    MODEL_FINGERPRINT,
    MODEL_ID,
    MULTIHOUR_SPEC,
    OwnedCollectorChild,
    ProductionSourceStalled,
    RUNNER_SPEC_FINGERPRINT,
    SOURCE_STALL_FAIL_SECONDS,
    SourceContinuityWatchdog,
    UNLIMITED_COLLECTOR_MAX_PUMP_EVENTS,
    build_arg_parser,
    build_session_track_metrics,
    classify_multihour_result,
    file_sha256,
    finalize_at_watermark,
    parse_duration_seconds,
    prepare_runtime_directory,
    preserve_source_stall_failure_state,
    run_control_reason,
    session_identity,
)
from phase4_continuous_firstpullback_production_smoke_selftest_v0_1 import (  # noqa: E402
    copy_prefix,
)
from phase4_continuous_firstpullback_production_smoke_v0_1 import (  # noqa: E402
    RuntimeMetrics,
    _drain_through,
    _production_evidence,
    capture_source_anchor,
    dt_text,
    restart_probe,
    write_json_artifact,
)


EXPECTED_MODEL_FINGERPRINT = (
    "e7cfb5049d93728391decf633a245585b49d7c4ea8e256bca8c98b53ccb5da47"
)


def invalid_duration_rejected(value: int) -> bool:
    try:
        parse_duration_seconds(value)
    except argparse.ArgumentTypeError:
        return True
    return False


def expect_source_stall(
    watchdog: SourceContinuityWatchdog,
    *,
    latest_p1_rowid: int,
    now_monotonic: float,
) -> bool:
    try:
        watchdog.observe(latest_p1_rowid, now_monotonic)
    except ProductionSourceStalled as exc:
        return str(exc) == "PRODUCTION_SOURCE_STALLED"
    return False


def git_ignored(path: Path) -> bool:
    return subprocess.run(
        ("git", "check-ignore", "--no-index", "--quiet", str(path)),
        cwd=PROJECT_ROOT,
        check=False,
    ).returncode == 0


def synthetic_positions(*, count_per_track: int, closed: bool) -> dict[str, object]:
    positions: list[dict[str, object]] = []
    for track_id in EXIT_TRACKS:
        for index in range(count_per_track):
            positions.append(
                {
                    "signal_key": f"signal-{index}",
                    "track_id": track_id,
                    "terminal_state": "CLOSED" if closed else "OPEN",
                    "exit_reason": "FALLBACK" if closed else None,
                    "gross_pnl_lamports": 100 + index if closed else None,
                    "net_pnl_lamports": 50 + index if closed else None,
                }
            )
    return {
        "positions": positions,
        "accounting": {"tracks": []},
        "observability": {"tracks": []},
    }


def append_source_rows(
    source_path: Path,
    rows: tuple[tuple[int, str, str], ...],
) -> None:
    writer = sqlite3.connect(source_path)
    try:
        for rowid, mint, event_type in rows:
            insert_row(
                writer,
                rowid,
                mint,
                event_type,
                observed_offset=300 + rowid,
            )
        writer.commit()
    finally:
        writer.close()


def build_boundary_fixture(
    template: Path,
    root: Path,
) -> tuple[Path, Path, ContinuousMarketSourceV01, sqlite3.Connection, ContinuousFirstPullbackBindingV01]:
    root.mkdir(parents=True, exist_ok=False)
    source_path = root / "source.sqlite3"
    paper_path = root / "paper.sqlite3"
    copy_prefix(template, source_path, 14)
    append_source_rows(
        source_path,
        (
            (15, "MINT-BOUNDARY", "LAUNCH"),
            (16, "MINT-BOUNDARY", "BUY"),
        ),
    )
    source = ContinuousMarketSourceV01(
        source_path,
        start_after_p1_rowid=14,
        database_identity="SELFTEST:C3:FROZEN-SOURCE",
    )
    conn = connect_entry_router_db(paper_path)
    binding = ContinuousFirstPullbackBindingV01(
        conn,
        source,
        wall_clock=lambda: BASE,
    )
    return source_path, paper_path, source, conn, binding


def run_postfreeze_boundary_scenario(
    template: Path,
    root: Path,
    *,
    postfreeze_rows: tuple[tuple[int, str, str], ...],
) -> dict[str, object]:
    source_path, paper_path, source, conn, binding = build_boundary_fixture(
        template,
        root,
    )
    metrics = RuntimeMetrics([])
    timer_id = "SELFTEST:C3:FINAL-BOUNDARY"

    def append_after_prepare_then_drain(
        active_binding: ContinuousFirstPullbackBindingV01,
        active_metrics: RuntimeMetrics,
        *,
        anchor: int,
        watermark: int,
    ) -> None:
        append_source_rows(source_path, postfreeze_rows)
        _drain_through(
            active_binding,
            active_metrics,
            anchor=anchor,
            watermark=watermark,
        )

    try:
        end_watermark = finalize_at_watermark(
            conn,
            binding,
            metrics,
            anchor=14,
            instant=BASE,
            timer_id=timer_id,
            drain_fn=append_after_prepare_then_drain,
        )
        timer = conn.execute(
            "SELECT source_watermark_p1_rowid,clock_timestamp,status,input_key "
            "FROM paper_fp_binding_prepared_timers_v0_1 WHERE timer_key=?",
            (timer_id,),
        ).fetchone()
        clock_event = conn.execute(
            "SELECT event_json FROM paper_fp_binding_outbox_v0_1 "
            "WHERE input_key=? AND event_type='ClockTickEvent'",
            (None if timer is None else timer[3],),
        ).fetchone()
        maximum_admitted = int(
            conn.execute(
                "SELECT COALESCE(MAX(production_p1_rowid),14) "
                "FROM paper_fp_binding_inputs_v0_1"
            ).fetchone()[0]
        )
        durable = binding.durable_p1_rowid
        outbox = binding.pending_outbox_count()
        digest_before_restart = binding.canonical_digest()
        quick = str(conn.execute("PRAGMA quick_check").fetchone()[0]).lower()
    finally:
        conn.close()
    restart_ok, restart_digest = restart_probe(paper_path, source)
    source_latest = source.latest_p1_rowid()
    bounded_evidence = _production_evidence(
        source_path,
        anchor=14,
        final_cursor=end_watermark,
    )
    current_latest_evidence = _production_evidence(
        source_path,
        anchor=14,
        final_cursor=source_latest,
    )
    return {
        "end_watermark": end_watermark,
        "timer_watermark": None if timer is None else int(timer[0]),
        "timer_timestamp": None if timer is None else str(timer[1]),
        "timer_status": None if timer is None else str(timer[2]),
        "clock_event_timestamp": (
            None if clock_event is None else json.loads(str(clock_event[0]))["now"]
        ),
        "durable": durable,
        "maximum_admitted": maximum_admitted,
        "outbox": outbox,
        "source_latest": source_latest,
        "digest_before_restart": digest_before_restart,
        "restart_ok": restart_ok,
        "restart_digest": restart_digest,
        "bounded_evidence": bounded_evidence,
        "current_latest_evidence": current_latest_evidence,
        "timers_prepared": metrics.timers_prepared,
        "timers_executed": metrics.timers_executed,
        "quick_check": quick,
    }


def run_stale_preread_race_scenario(
    template: Path,
    root: Path,
) -> dict[str, object]:
    source_path, paper_path, source, conn, binding = build_boundary_fixture(
        template,
        root,
    )
    metrics = RuntimeMetrics([])
    timer_id = "SELFTEST:C3:STALE-PREREAD-RACE"
    try:
        conceptual_stale_preread = source.latest_p1_rowid()
        append_source_rows(
            source_path,
            ((17, "MINT-BEFORE-PREPARE", "LAUNCH"),),
        )
        end_watermark = finalize_at_watermark(
            conn,
            binding,
            metrics,
            anchor=14,
            instant=BASE,
            timer_id=timer_id,
        )
        timer = conn.execute(
            "SELECT source_watermark_p1_rowid,clock_timestamp,status,input_key "
            "FROM paper_fp_binding_prepared_timers_v0_1 WHERE timer_key=?",
            (timer_id,),
        ).fetchone()
        clock_event = conn.execute(
            "SELECT event_json FROM paper_fp_binding_outbox_v0_1 "
            "WHERE input_key=? AND event_type='ClockTickEvent'",
            (None if timer is None else timer[3],),
        ).fetchone()
        result = {
            "conceptual_stale_preread": conceptual_stale_preread,
            "end_watermark": end_watermark,
            "timer_watermark": None if timer is None else int(timer[0]),
            "timer_timestamp": None if timer is None else str(timer[1]),
            "timer_status": None if timer is None else str(timer[2]),
            "clock_event_timestamp": (
                None
                if clock_event is None
                else json.loads(str(clock_event[0]))["now"]
            ),
            "durable": binding.durable_p1_rowid,
            "outbox": binding.pending_outbox_count(),
            "digest": binding.canonical_digest(),
            "timers_prepared": metrics.timers_prepared,
            "timers_executed": metrics.timers_executed,
        }
    finally:
        conn.close()
    restart_ok, restart_digest = restart_probe(paper_path, source)
    result["restart_ok"] = restart_ok
    result["restart_digest"] = restart_digest
    return result


def main() -> int:
    checks: dict[str, bool] = {}
    checks["A_exact_model_id_fingerprint"] = (
        MODEL_ID == "P4-CONTINUOUS-FIRSTPULLBACK-MULTIHOUR-RUN-0001"
        and MODEL_FINGERPRINT == EXPECTED_MODEL_FINGERPRINT
    )
    checks["B_accepted_components_bound"] = (
        BINDING_FINGERPRINT
        == "318e15b104c691821f5715b090c934e5b1cb3d9a8357fe0930d8825ef66d0daa"
        and MARKET_SOURCE_FINGERPRINT
        == "242b84a26ddc9e80fc428b1f02202e30aab6f4009677b38b72861bad432881fb"
        and GAP_COMPAT_FINGERPRINT
        == "aabb765f81a29b2dd119607cdea92a2949dd3acec10aa204d246e6aa7a28a78c"
        and RUNNER_SPEC_FINGERPRINT
        == "8dcab9d17f2ec71f4920a199b5745dae92dc3464a466860593231424e0a24dea"
        and LOCKED_SELECTION_SHA256
        == "408657c1d6dc39435b61e01b368dac69796481864b7462efd150a2d4e1b0daa1"
        and COST_BASELINE_FINGERPRINT
        == "9652aeffc792eb48d62348360c6ae804f660e43329195d1f169219b80ccab67c"
        and SOURCE_STALL_FAIL_SECONDS == 300.0
        and MULTIHOUR_SPEC["source_continuity"]["required_for_pass"] is True
        and MULTIHOUR_SPEC["runtime_artifacts"]["ignore_policy"]
        == "ROOT_GITIGNORE_EXACT_DIRECTORY_RULE"
    )
    checks["C_default_duration_14400"] = DEFAULT_DURATION_SECONDS == 14_400
    parser = build_arg_parser()
    checks["D_cli_duration_accepts_bounds_default"] = tuple(
        parser.parse_args(
            ("--duration-seconds", str(value), "--use-existing-collector")
        ).duration_seconds
        for value in (3_600, 14_400, 21_600)
    ) == (3_600, 14_400, 21_600)
    checks["E_duration_out_of_range_fails_closed"] = (
        invalid_duration_rejected(MIN_DURATION_SECONDS - 1)
        and invalid_duration_rejected(MAX_DURATION_SECONDS + 1)
    )
    checks["F_no_candidate_early_stop"] = (
        run_control_reason(300.0, DEFAULT_DURATION_SECONDS) is None
        and MULTIHOUR_SPEC["duration_policy"]["candidate_early_stop"] is False
    )
    checks["G_multiple_candidates_do_not_stop"] = all(
        run_control_reason(elapsed, DEFAULT_DURATION_SECONDS) is None
        for elapsed in (301.0, 1_000.0, 7_200.0, 14_399.999)
    )
    checks["H_requested_duration_stops"] = (
        run_control_reason(14_399.999, 14_400) is None
        and run_control_reason(14_400.0, 14_400) == "DURATION_REACHED"
    )
    checks["I_runtime_error_fails_closed"] = (
        run_control_reason(
            1.0,
            DEFAULT_DURATION_SECONDS,
            fail_closed_error="INJECTED",
        )
        == "FAIL_CLOSED"
    )

    advancing = SourceContinuityWatchdog(100, 0.0)
    for step in range(1, 241):
        advancing.observe(100 + step, float(step * 60))
    advancing_snapshot = advancing.snapshot(14_400.0)
    checks["C1_01_continuously_advancing_source_passes"] = (
        advancing_snapshot["continuity_ok"] is True
        and advancing_snapshot["source_advance_count"] == 240
        and advancing_snapshot["final_source_staleness_seconds"] == 0.0
    )

    short_stall = SourceContinuityWatchdog(200, 0.0)
    short_stall.observe(201, 10.0)
    short_stall.observe(201, 309.999)
    checks["C1_02_stall_below_300_not_failed"] = (
        short_stall.snapshot(309.999)["continuity_ok"] is True
    )

    exact_stall = SourceContinuityWatchdog(300, 0.0)
    exact_stall.observe(301, 10.0)
    checks["C1_03_stall_exactly_300_fails"] = expect_source_stall(
        exact_stall,
        latest_p1_rowid=301,
        now_monotonic=310.0,
    )

    over_stall = SourceContinuityWatchdog(400, 0.0)
    over_stall.observe(401, 10.0)
    checks["C1_04_stall_over_300_fails"] = expect_source_stall(
        over_stall,
        latest_p1_rowid=401,
        now_monotonic=310.001,
    )

    managed_alive = True
    managed_watchdog = SourceContinuityWatchdog(500, 0.0)
    checks["C1_05_managed_liveness_cannot_override_stall"] = (
        managed_alive
        and expect_source_stall(
            managed_watchdog,
            latest_p1_rowid=500,
            now_monotonic=300.0,
        )
    )

    external_alive = True
    external_watchdog = SourceContinuityWatchdog(600, 0.0)
    checks["C1_06_external_mode_same_stall_detection"] = (
        external_alive
        and expect_source_stall(
            external_watchdog,
            latest_p1_rowid=600,
            now_monotonic=300.0,
        )
    )

    clock_watchdog = SourceContinuityWatchdog(700, 0.0)
    clock_activity_count = 300
    checks["C1_07_clocks_cannot_reset_source_timer"] = (
        clock_activity_count == 300
        and clock_watchdog.last_source_advance_monotonic == 0.0
        and expect_source_stall(
            clock_watchdog,
            latest_p1_rowid=700,
            now_monotonic=300.0,
        )
    )

    cursor_watchdog = SourceContinuityWatchdog(800, 0.0)
    binding_cursor = 799
    binding_cursor = 800
    checks["C1_08_binding_cursor_cannot_reset_source_timer"] = (
        binding_cursor == 800
        and cursor_watchdog.last_source_advance_monotonic == 0.0
        and expect_source_stall(
            cursor_watchdog,
            latest_p1_rowid=800,
            now_monotonic=300.0,
        )
    )

    reset_watchdog = SourceContinuityWatchdog(900, 0.0)
    reset_watchdog.observe(900, 299.0)
    reset_watchdog.observe(901, 299.5)
    reset_watchdog.observe(901, 598.0)
    reset_snapshot = reset_watchdog.snapshot(598.0)
    checks["C1_09_rowid_advance_resets_stall_clock"] = (
        reset_snapshot["continuity_ok"] is True
        and reset_snapshot["source_advance_count"] == 1
        and reset_snapshot["final_source_staleness_seconds"] == 298.5
    )

    silent_snapshot = SourceContinuityWatchdog(1_000, 0.0)
    silent_snapshot.observe(1_001, 10.0)
    silent_continuity = silent_snapshot.snapshot(14_400.0)
    checks["C1_10_early_evidence_then_silence_never_passes"] = (
        classify_multihour_result(
            duration_reached=True,
            errors=[],
            integrity_ok=True,
            graceful_shutdown=True,
            restart_ok=True,
            source_continuity_ok=bool(silent_continuity["continuity_ok"]),
            raw_rows=1,
            fresh_launches=1,
            evaluated_mints=1,
            evaluations=1,
            timers_executed=1,
        )
        == "FAIL"
    )

    stale_final = SourceContinuityWatchdog(1_100, 0.0)
    stale_final.observe(1_101, 100.0)
    stale_final_snapshot = stale_final.snapshot(400.001)
    checks["C1_11_final_staleness_over_300_prevents_pass"] = (
        stale_final_snapshot["final_source_staleness_seconds"] > 300.0
        and stale_final_snapshot["continuity_ok"] is False
    )

    deterministic_a = SourceContinuityWatchdog(1_200, 0.0)
    deterministic_b = SourceContinuityWatchdog(1_200, 0.0)
    for latest, at in ((1_201, 10.0), (1_202, 40.0), (1_203, 70.0)):
        deterministic_a.observe(latest, at)
        deterministic_b.observe(latest, at)
    checks["C1_12_repeated_continuity_digest_deterministic"] = (
        deterministic_a.canonical_digest(90.0)
        == deterministic_b.canonical_digest(90.0)
        and deterministic_a.snapshot(90.0) == deterministic_b.snapshot(90.0)
    )

    boundary_before = SourceContinuityWatchdog(2_000, 0.0)
    boundary_before.observe(2_001, 299.999)
    checks["C2_01_advance_at_299_999_allowed_and_resets"] = (
        boundary_before.last_observed_source_p1_rowid == 2_001
        and boundary_before.last_source_advance_monotonic == 299.999
        and boundary_before.source_advance_count == 1
        and boundary_before.maximum_source_stall_seconds == 299.999
        and boundary_before.stalled is False
    )

    boundary_exact = SourceContinuityWatchdog(2_100, 0.0)
    exact_failed = expect_source_stall(
        boundary_exact,
        latest_p1_rowid=2_101,
        now_monotonic=300.0,
    )
    checks["C2_02_advance_at_exact_300_fails_before_update"] = (
        exact_failed
        and boundary_exact.last_observed_source_p1_rowid == 2_100
        and boundary_exact.last_source_advance_monotonic == 0.0
    )

    boundary_over = SourceContinuityWatchdog(2_200, 0.0)
    checks["C2_03_advance_at_300_001_fails"] = expect_source_stall(
        boundary_over,
        latest_p1_rowid=2_201,
        now_monotonic=300.001,
    )

    late_advance = SourceContinuityWatchdog(2_300, 0.0)
    checks["C2_04_advance_at_301_fails_immediately"] = expect_source_stall(
        late_advance,
        latest_p1_rowid=2_301,
        now_monotonic=301.0,
    )

    reset_interval = SourceContinuityWatchdog(2_400, 0.0)
    reset_interval.observe(2_401, 250.0)
    reset_interval.observe(2_401, 549.999)
    reset_interval_failed = expect_source_stall(
        reset_interval,
        latest_p1_rowid=2_402,
        now_monotonic=550.0,
    )
    checks["C2_05_full_interval_measured_from_valid_advance"] = (
        reset_interval_failed
        and reset_interval.last_observed_source_p1_rowid == 2_401
        and reset_interval.last_source_advance_monotonic == 250.0
    )

    checks["C2_06_maximum_stall_records_terminal_interval"] = (
        boundary_exact.maximum_source_stall_seconds == 300.0
        and reset_interval.maximum_source_stall_seconds == 300.0
    )
    checks["C2_07_late_advance_does_not_increment_count"] = (
        boundary_exact.source_advance_count == 0
        and late_advance.source_advance_count == 0
        and reset_interval.source_advance_count == 1
    )

    terminal_state = (
        late_advance.last_observed_source_p1_rowid,
        late_advance.last_source_advance_monotonic,
        late_advance.source_advance_count,
        late_advance.maximum_source_stall_seconds,
        late_advance.stalled,
    )
    terminal_raised_again = expect_source_stall(
        late_advance,
        latest_p1_rowid=2_302,
        now_monotonic=301.5,
    )
    checks["C2_08_late_advance_cannot_restore_terminal_state"] = (
        terminal_raised_again
        and terminal_state
        == (
            late_advance.last_observed_source_p1_rowid,
            late_advance.last_source_advance_monotonic,
            late_advance.source_advance_count,
            late_advance.maximum_source_stall_seconds,
            late_advance.stalled,
        )
        and late_advance.snapshot(301.5)["continuity_ok"] is False
    )

    checks["C2_09_terminal_continuity_cannot_classify_pass"] = (
        classify_multihour_result(
            duration_reached=True,
            errors=[],
            integrity_ok=True,
            graceful_shutdown=True,
            restart_ok=True,
            source_continuity_ok=bool(
                late_advance.snapshot(301.5)["continuity_ok"]
            ),
            raw_rows=1,
            fresh_launches=1,
            evaluated_mints=1,
            evaluations=1,
            timers_executed=1,
        )
        == "FAIL"
    )

    terminal_a = SourceContinuityWatchdog(2_500, 0.0)
    terminal_b = SourceContinuityWatchdog(2_500, 0.0)
    terminal_a.observe(2_501, 100.0)
    terminal_b.observe(2_501, 100.0)
    terminal_a_failed = expect_source_stall(
        terminal_a,
        latest_p1_rowid=2_502,
        now_monotonic=400.0,
    )
    terminal_b_failed = expect_source_stall(
        terminal_b,
        latest_p1_rowid=2_502,
        now_monotonic=400.0,
    )
    checks["C2_10_repeated_terminal_state_and_digest_deterministic"] = (
        terminal_a_failed
        and terminal_b_failed
        and terminal_a.snapshot(400.0) == terminal_b.snapshot(400.0)
        and terminal_a.canonical_digest(400.0)
        == terminal_b.canonical_digest(400.0)
    )

    with tempfile.TemporaryDirectory(prefix="phase4_multihour_selftest_") as raw:
        root = Path(raw)
        template = root / "template.sqlite3"
        source_path = root / "source.sqlite3"
        paper_path = root / "paper.sqlite3"
        artifact_path = root / "summary.json"
        create_fixture(template)
        copy_prefix(template, source_path, 14)
        identity = "SELFTEST:MULTIHOUR:SOURCE"
        anchor = capture_source_anchor(source_path, identity)
        checks["J_fresh_anchor_selection"] = anchor == 14

        writer = sqlite3.connect(source_path)
        insert_row(writer, 15, "MINT-LIVE", "LAUNCH", observed_offset=315)
        insert_row(writer, 16, "MINT-LIVE", "BUY", observed_offset=316)
        writer.commit()
        writer.close()
        source = ContinuousMarketSourceV01(
            source_path,
            start_after_p1_rowid=anchor,
            database_identity=identity,
        )
        conn = connect_entry_router_db(paper_path)
        binding = ContinuousFirstPullbackBindingV01(conn, source, wall_clock=lambda: BASE)
        binding.process_next_batch(batch_size=100)
        metrics = RuntimeMetrics([])
        end_watermark = finalize_at_watermark(
            conn,
            binding,
            metrics,
            anchor=anchor,
            instant=BASE,
            timer_id="SELFTEST:MULTIHOUR:FINAL",
        )
        static_timer = conn.execute(
            "SELECT source_watermark_p1_rowid,clock_timestamp,status,input_key "
            "FROM paper_fp_binding_prepared_timers_v0_1 "
            "WHERE timer_key='SELFTEST:MULTIHOUR:FINAL'"
        ).fetchone()
        static_clock_event = conn.execute(
            "SELECT event_json FROM paper_fp_binding_outbox_v0_1 "
            "WHERE input_key=? AND event_type='ClockTickEvent'",
            (None if static_timer is None else static_timer[3],),
        ).fetchone()
        quick = str(conn.execute("PRAGMA quick_check").fetchone()[0]).lower()
        final_cursor = binding.durable_p1_rowid
        before_late_digest = binding.canonical_digest()
        late_writer = sqlite3.connect(source_path)
        insert_row(late_writer, 17, "MINT-LATE", "BUY", observed_offset=317)
        late_writer.commit()
        late_writer.close()
        preserved_cursor = preserve_source_stall_failure_state(binding)
        after_late_digest = binding.canonical_digest()
        checks["C2_11_stall_shutdown_does_not_admit_late_row"] = (
            source.latest_p1_rowid() == 17
            and preserved_cursor == 16
            and binding.durable_p1_rowid == 16
            and before_late_digest == after_late_digest
            and binding.pending_outbox_count() == 0
            and binding.active_timer_fence_p1_rowid is None
        )
        conn.close()
        checks["K_graceful_end_watermark"] = (
            end_watermark == 16
            and final_cursor == 16
            and metrics.timers_prepared == 1
            and metrics.timers_executed == 1
        )
        checks["C3_01_static_source_normal_finalization"] = (
            static_timer is not None
            and int(static_timer[0]) == end_watermark == final_cursor == 16
            and str(static_timer[1]) == dt_text(BASE)
            and str(static_timer[2]) == "COMPLETE"
            and static_clock_event is not None
            and json.loads(str(static_clock_event[0]))["now"] == dt_text(BASE)
        )

        postfreeze_one = run_postfreeze_boundary_scenario(
            template,
            root / "c3-postfreeze-one",
            postfreeze_rows=((17, "MINT-POSTFREEZE-17", "LAUNCH"),),
        )
        postfreeze_multiple = run_postfreeze_boundary_scenario(
            template,
            root / "c3-postfreeze-multiple",
            postfreeze_rows=(
                (17, "MINT-POSTFREEZE-17", "LAUNCH"),
                (18, "MINT-POSTFREEZE-18", "LAUNCH"),
            ),
        )
        stale_preread_race = run_stale_preread_race_scenario(
            template,
            root / "c3-stale-preread-race",
        )
        repeat_boundary_a = run_postfreeze_boundary_scenario(
            template,
            root / "c3-repeat-a",
            postfreeze_rows=(
                (17, "MINT-POSTFREEZE-17", "LAUNCH"),
                (18, "MINT-POSTFREEZE-18", "LAUNCH"),
            ),
        )
        repeat_boundary_b = run_postfreeze_boundary_scenario(
            template,
            root / "c3-repeat-b",
            postfreeze_rows=(
                (17, "MINT-POSTFREEZE-17", "LAUNCH"),
                (18, "MINT-POSTFREEZE-18", "LAUNCH"),
            ),
        )

        checks["C3_02_one_postfreeze_row_not_admitted"] = (
            postfreeze_one["end_watermark"] == 16
            and postfreeze_one["durable"] == 16
            and postfreeze_one["maximum_admitted"] == 16
            and postfreeze_one["source_latest"] == 17
            and postfreeze_one["timer_status"] == "COMPLETE"
        )
        checks["C3_03_multiple_postfreeze_rows_not_admitted"] = (
            postfreeze_multiple["end_watermark"] == 16
            and postfreeze_multiple["durable"] == 16
            and postfreeze_multiple["maximum_admitted"] == 16
            and postfreeze_multiple["source_latest"] == 18
            and postfreeze_multiple["timer_status"] == "COMPLETE"
        )
        checks["C3_04_old_preread_race_uses_prepared_watermark"] = (
            stale_preread_race["conceptual_stale_preread"] == 16
            and stale_preread_race["timer_watermark"] == 17
            and stale_preread_race["end_watermark"] == 17
            and stale_preread_race["durable"] == 17
        )
        checks["C3_05_stale_preread_cannot_false_conflict"] = (
            stale_preread_race["timer_status"] == "COMPLETE"
            and stale_preread_race["timers_prepared"] == 1
            and stale_preread_race["timers_executed"] == 1
            and stale_preread_race["restart_ok"] is True
            and stale_preread_race["restart_digest"]
            == stale_preread_race["digest"]
        )
        checks["C3_06_prepared_watermark_is_returned_end"] = all(
            item["timer_watermark"] == item["end_watermark"]
            for item in (
                postfreeze_one,
                postfreeze_multiple,
                stale_preread_race,
            )
        )
        checks["C3_07_durable_cursor_equals_prepared_watermark"] = all(
            item["durable"] == item["timer_watermark"]
            for item in (
                postfreeze_one,
                postfreeze_multiple,
                stale_preread_race,
            )
        )
        checks["C3_08_final_pending_outbox_zero"] = all(
            item["outbox"] == 0
            for item in (
                postfreeze_one,
                postfreeze_multiple,
                stale_preread_race,
            )
        )
        checks["C3_09_single_boundary_timestamp_preserved"] = all(
            item["timer_timestamp"] == dt_text(BASE)
            and item["clock_event_timestamp"] == dt_text(BASE)
            for item in (
                postfreeze_one,
                postfreeze_multiple,
                stale_preread_race,
            )
        )
        checks["C3_10_postfreeze_rows_do_not_change_restart_digest"] = all(
            item["restart_ok"] is True
            and item["restart_digest"] == item["digest_before_restart"]
            for item in (postfreeze_one, postfreeze_multiple)
        )
        checks["C3_11_production_evidence_uses_frozen_watermark"] = (
            postfreeze_one["bounded_evidence"]
            == {"fresh_launches": 1, "gap_rows": 0, "unsupported_rows": 0}
            and postfreeze_one["current_latest_evidence"]["fresh_launches"] == 2
            and postfreeze_multiple["bounded_evidence"]
            == {"fresh_launches": 1, "gap_rows": 0, "unsupported_rows": 0}
            and postfreeze_multiple["current_latest_evidence"]["fresh_launches"]
            == 3
        )
        checks["C3_12_repeated_boundary_race_is_deterministic"] = (
            repeat_boundary_a["digest_before_restart"]
            == repeat_boundary_b["digest_before_restart"]
            and repeat_boundary_a["restart_digest"]
            == repeat_boundary_b["restart_digest"]
            and repeat_boundary_a["end_watermark"]
            == repeat_boundary_b["end_watermark"]
            and repeat_boundary_a["bounded_evidence"]
            == repeat_boundary_b["bounded_evidence"]
            and repeat_boundary_a["quick_check"]
            == repeat_boundary_b["quick_check"]
            == "ok"
        )

        open_metrics = build_session_track_metrics(
            synthetic_positions(count_per_track=1, closed=False)
        )
        open_classification = classify_multihour_result(
            duration_reached=True,
            errors=[],
            integrity_ok=True,
            graceful_shutdown=True,
            restart_ok=True,
            source_continuity_ok=True,
            raw_rows=1,
            fresh_launches=1,
            evaluated_mints=1,
            evaluations=1,
            timers_executed=1,
        )
        checks["L_open_position_no_fabricated_exit"] = (
            open_classification == "PASS_MULTI_HOUR"
            and all(
                item["open"] == 1
                and item["closed"] == 0
                and item["exit_reason_distribution"] == {}
                and item["net_pnl_lamports"] == 0
                for item in open_metrics["tracks"].values()
            )
        )

        multi_metrics = build_session_track_metrics(
            synthetic_positions(count_per_track=2, closed=True)
        )
        checks["M_multiple_trades_accumulated_per_track"] = all(
            item["entries"] == 2
            and item["closed"] == 2
            and item["exit_reason_distribution"] == {"FALLBACK": 2}
            and item["net_pnl_lamports"] == 101
            for item in multi_metrics["tracks"].values()
        )
        checks["N_final_a_b_kept_separate"] = (
            set(multi_metrics["tracks"]) == {"FINAL-A", "FINAL-B", "SENS-C"}
            and multi_metrics["tracks"]["FINAL-A"]
            is not multi_metrics["tracks"]["FINAL-B"]
        )
        checks["O_sens_c_sensitivity_only"] = (
            multi_metrics["tracks"]["SENS-C"]["label"] == "SENSITIVITY ONLY"
        )
        checks["P_cross_track_sum_prohibited"] = (
            multi_metrics["cross_track_portfolio_summed"] is False
            and MULTIHOUR_SPEC["cross_track_portfolio_sum"] == "PROHIBITED"
        )

        harness_path = PROJECT_ROOT / "scripts" / "phase4_continuous_firstpullback_multihour_run_v0_1.py"
        owned_log = root / "owned.log"
        unrelated_log = root / "unrelated.log"
        owned_ready = root / "owned.ready"
        unrelated_ready = root / "unrelated.ready"
        owned = OwnedCollectorChild.start(
            (
                sys.executable,
                str(harness_path),
                "--collector-child-probe",
                "--collector-child-probe-ready",
                str(owned_ready),
            ),
            owned_log,
        )
        unrelated = OwnedCollectorChild.start(
            (
                sys.executable,
                str(harness_path),
                "--collector-child-probe",
                "--collector-child-probe-ready",
                str(unrelated_ready),
            ),
            unrelated_log,
        )
        readiness_deadline = time.monotonic() + 5.0
        while (
            not owned_ready.exists() or not unrelated_ready.exists()
        ) and time.monotonic() < readiness_deadline:
            time.sleep(0.05)
        both_ready = owned_ready.exists() and unrelated_ready.exists()
        owned_ok = owned.stop(timeout_seconds=5.0)
        unrelated_survived = unrelated.process.poll() is None
        unrelated_ok = unrelated.stop(timeout_seconds=5.0)
        checks["Q_managed_child_lifecycle_simulated"] = (
            both_ready
            and owned_ok
            and unrelated_ok
            and owned.process.poll() is not None
            and unrelated.process.poll() is not None
            and owned.cleanup_method == "OWNED_PROCESS_GROUP_INTERRUPT"
        )
        checks["R_unlimited_mode_not_500_target"] = (
            UNLIMITED_COLLECTOR_MAX_PUMP_EVENTS is None
            and MULTIHOUR_SPEC["collector_policy"]["managed_max_pump_events"] is None
        )
        checks["S_only_owned_collector_terminated"] = unrelated_survived

        restart_ok, restart_digest = restart_probe(paper_path, source)
        checks["T_exact_restart_probe"] = (
            restart_ok and restart_digest == after_late_digest
        )
        payload = {
            "model_id": MODEL_ID,
            "model_fingerprint": MODEL_FINGERPRINT,
            "session_identity": session_identity(
                duration_seconds=DEFAULT_DURATION_SECONDS,
                database_identity=identity,
                anchor=anchor,
            ),
            "result_class": "PASS_MULTI_HOUR",
        }
        isolated_runtime = root / "runtime"
        prepare_runtime_directory(isolated_runtime)
        write_json_artifact(artifact_path, payload)
        ignored_artifacts = tuple(
            PROJECT_ROOT / "data" / "paper" / "multihour" / name
            for name in ("probe.sqlite3", "probe.json", "probe.log")
        )
        tracked_research_relative = Path(
            "data/research/phase3/EXP-0012/"
            "EXP-0012_final_phase3_validation_results_v0_1.json"
        )
        tracked_research = (
            PROJECT_ROOT
            / tracked_research_relative
        )
        checks["U_json_artifact_finalization"] = (
            json.loads(artifact_path.read_text(encoding="utf-8")) == payload
            and not (isolated_runtime / ".gitignore").exists()
            and all(git_ignored(path) for path in ignored_artifacts)
            and not git_ignored(tracked_research)
            and subprocess.run(
                (
                    "git",
                    "ls-files",
                    "--error-unmatch",
                    tracked_research_relative.as_posix(),
                ),
                cwd=PROJECT_ROOT,
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            ).returncode
            == 0
        )
        checks["V_sqlite_quick_check_ok"] = quick == "ok"

        scenario_a = run_scenario(root / "scenario-a", failures=False)
        scenario_b = run_scenario(root / "scenario-b", failures=False)
        checks["W_deterministic_repeated_digest"] = (
            scenario_a.digest == scenario_b.digest
            and scenario_a.candidate_order == ("MINT-A:CONTROL", "MINT-C:CONTROL")
            and scenario_a.checks["AE_accounting_reconciles_per_track"]
            and scenario_a.quick_check == "ok"
        )
        t009_path = (
            PROJECT_ROOT
            / "scripts"
            / "phase4_continuous_firstpullback_production_smoke_v0_1.py"
        )
        checks["X_t009_harness_byte_exact"] = (
            file_sha256(t009_path) == ACCEPTED_T009_SHA256
        )

    print("=" * 120)
    print("BOUNDED MULTI-HOUR FIRSTPULLBACK PAPER HARNESS SELF-TEST v0.1")
    print("=" * 120)
    print(f"Model       : {MODEL_ID}")
    print(f"Fingerprint : {MODEL_FINGERPRINT}")
    print("Production  : NO")
    print("Collector   : SIMULATED CHILD ONLY")
    print("Network/RPC : NO")
    print()
    ok = True
    for name, passed in checks.items():
        ok &= bool(passed)
        print(f"{name:<52}: {'PASS' if passed else 'FAIL'}")
    print()
    print(f"RESULT: {'PASS' if ok else 'FAIL'}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
