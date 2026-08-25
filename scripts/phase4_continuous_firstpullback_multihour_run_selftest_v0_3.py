from __future__ import annotations

import json
import shutil
import sqlite3
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
for import_root in (PROJECT_ROOT / "src", PROJECT_ROOT / "scripts"):
    if str(import_root) not in sys.path:
        sys.path.insert(0, str(import_root))

import phase4_continuous_firstpullback_multihour_run_v0_3 as harness  # noqa: E402
import phase4_continuous_market_source_selftest_v0_1 as v01_fixture  # noqa: E402
from phase4.paper_continuous_firstpullback_binding_v0_2 import (  # noqa: E402
    ContinuousFirstPullbackBindingV02,
)
from phase4.paper_continuous_firstpullback_binding_v0_3 import (  # noqa: E402
    MODEL_FINGERPRINT as BINDING_FINGERPRINT,
    MODEL_ID as BINDING_MODEL_ID,
    ContinuousFirstPullbackBindingV03,
)
from phase4.paper_continuous_market_source_v0_2 import (  # noqa: E402
    ContinuousMarketSourceV02,
)
from phase4.paper_entry_execution_deadline_v0_1 import (  # noqa: E402
    ELIGIBILITY_WINDOW_US,
    MODEL_FINGERPRINT as DEADLINE_FINGERPRINT,
)
from phase4.paper_entry_router_v0_1 import connect_entry_router_db  # noqa: E402


BASE = datetime(2026, 8, 25, 3, 0, tzinfo=timezone.utc)
EXPECTED_MODEL_FINGERPRINT = (
    "6fb400b77606cd1ef3fce32037c95f4ea1cd7299a71ee6c902e43eb2114917bb"
)
EXPECTED_BINDING_FINGERPRINT = (
    "45521225ccbe6c3b98a48b7e92523deea16d6ff9b91fdd37a415a1a45958babf"
)


class MutableClock:
    def __init__(self, value: datetime = BASE) -> None:
        self.value = value

    def __call__(self) -> datetime:
        return self.value


def create_source_db(path: Path, *, historical_age_seconds: float = 600.0) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.execute(v01_fixture.SCHEMA_SQL)
        conn.execute("ALTER TABLE pump_events ADD COLUMN inserted_at_utc TEXT")
        v01_fixture.insert_row(
            conn,
            rowid=1,
            mint="HISTORICAL",
            event_type="LAUNCH",
        )
        conn.execute(
            "UPDATE pump_events SET inserted_at_utc=? WHERE rowid=1",
            ((BASE - timedelta(seconds=historical_age_seconds)).isoformat(),),
        )
        conn.commit()
    finally:
        conn.close()


def append_health_row(path: Path, *, rowid: int, at: datetime) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            "INSERT INTO pump_events(rowid,event_key,signature,event_type,mint,"
            "source_decoded_file,decoded_at_utc,inserted_at_utc) "
            "VALUES(?,?,?,?,NULL,?,?,?)",
            (
                rowid,
                f"health-{rowid}",
                f"health-sig-{rowid}",
                "BUY",
                "LIVE_WEBSOCKET_EVENT_V0_3_4",
                at.isoformat(),
                at.isoformat(),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def make_watchdog(path: Path):
    source = ContinuousMarketSourceV02(
        path,
        start_after_p1_rowid=1,
        database_identity=f"SELFTEST:CONTINUITY:{path.name}",
    )
    clock = MutableClock()
    watchdog = harness.EvidenceBasedSourceContinuityWatchdogV03(
        source=source,
        last_observed_source_p1_rowid=1,
        last_source_advance_monotonic=0.0,
        session_started_at_utc=BASE,
        wall_clock=clock,
    )
    return source, clock, watchdog


def observe_reason(watchdog, latest: int, monotonic: float) -> str | None:
    try:
        watchdog.observe(latest, monotonic)
    except harness.ProductionSourceStalled as exc:
        return str(exc)
    return None


def check(name: str, condition: bool, checks: dict[str, bool]) -> None:
    checks[name] = bool(condition)


def main() -> int:
    checks: dict[str, bool] = {}
    evidence: dict[str, object] = {}
    root = Path(tempfile.mkdtemp(prefix="phase4_multihour_v03_"))
    try:
        harness._validate_locked_contracts_v03()
        check(
            "A_MODEL_AND_BINDING_IDENTITIES",
            harness.MODEL_ID
            == "P4-CONTINUOUS-FIRSTPULLBACK-MULTIHOUR-RUN-0003"
            and harness.MODEL_FINGERPRINT == EXPECTED_MODEL_FINGERPRINT
            and BINDING_MODEL_ID == "P4-CONTINUOUS-FIRSTPULLBACK-BINDING-0003"
            and BINDING_FINGERPRINT == EXPECTED_BINDING_FINGERPRINT,
            checks,
        )

        session_db = root / "session_start.sqlite3"
        create_source_db(session_db)
        source, clock, watchdog = make_watchdog(session_db)
        check(
            "L_SESSION_START_IGNORES_PRESESSION_HISTORICAL_GAP",
            observe_reason(watchdog, source.latest_p1_rowid(), 0.0) is None
            and not watchdog.actual_source_stalled,
            checks,
        )
        clock.value = BASE + timedelta(seconds=299.999)
        check(
            "M_299_999_SECONDS_HEALTHY",
            observe_reason(watchdog, source.latest_p1_rowid(), 299.999) is None,
            checks,
        )
        clock.value = BASE + timedelta(seconds=300)
        exact_reason = observe_reason(watchdog, source.latest_p1_rowid(), 300.0)
        check(
            "N_EXACT_300_SECONDS_SOURCE_STALL",
            exact_reason == "PRODUCTION_SOURCE_STALLED"
            and watchdog.actual_source_stalled,
            checks,
        )

        over_db = root / "over_boundary.sqlite3"
        create_source_db(over_db)
        over_source, over_clock, over_watchdog = make_watchdog(over_db)
        over_clock.value = BASE + timedelta(seconds=300.001)
        check(
            "N2_300_001_SECONDS_SOURCE_STALL",
            observe_reason(over_watchdog, over_source.latest_p1_rowid(), 300.001)
            == "PRODUCTION_SOURCE_STALLED",
            checks,
        )

        recovered_db = root / "recovered.sqlite3"
        create_source_db(recovered_db)
        source, clock, watchdog = make_watchdog(recovered_db)
        watchdog.observe(1, 0.0)
        append_health_row(recovered_db, rowid=2, at=BASE + timedelta(seconds=301))
        clock.value = BASE + timedelta(seconds=301)
        recovered_reason = observe_reason(watchdog, 2, 301.0)
        check(
            "O_LATE_ROW_DOES_NOT_HIDE_GENUINE_301S_OUTAGE",
            recovered_reason == "PRODUCTION_SOURCE_STALLED"
            and watchdog.actual_source_stalled
            and watchdog.runner_source_poll_starved,
            checks,
        )

        nightly_db = root / "nightly.sqlite3"
        create_source_db(nightly_db)
        source, clock, watchdog = make_watchdog(nightly_db)
        watchdog.observe(1, 0.0)
        rowid = 2
        offset = 3.516
        while offset < 396.578:
            append_health_row(
                nightly_db,
                rowid=rowid,
                at=BASE + timedelta(seconds=offset),
            )
            rowid += 1
            offset += 3.516
        clock.value = BASE + timedelta(seconds=396.578)
        nightly_reason = observe_reason(watchdog, rowid - 1, 396.578)
        nightly_snapshot = watchdog.snapshot(396.578)
        nightly_source = source
        nightly_watchdog = watchdog
        check(
            "P_NIGHTLY_FALSE_SOURCE_STALL_IMPOSSIBLE",
            nightly_reason == "RUNNER_SOURCE_POLL_STARVED"
            and not nightly_snapshot["actual_source_stalled"]
            and nightly_snapshot["runner_source_poll_starved"]
            and nightly_snapshot["maximum_actual_source_gap_seconds"] < 300.0,
            checks,
        )
        evidence["nightly_shape"] = nightly_snapshot

        healthy_db = root / "healthy.sqlite3"
        create_source_db(healthy_db)
        source, clock, watchdog = make_watchdog(healthy_db)
        watchdog.observe(1, 0.0)
        for rowid, offset in enumerate(range(4, 300, 4), start=2):
            append_health_row(
                healthy_db,
                rowid=rowid,
                at=BASE + timedelta(seconds=offset),
            )
        clock.value = BASE + timedelta(seconds=299)
        healthy_latest = source.latest_p1_rowid()
        healthy_reason = observe_reason(watchdog, healthy_latest, 299.0)
        check(
            "Q_HEALTHY_SOURCE_AND_RUNNER_UNDER_300S",
            healthy_reason is None
            and not watchdog.actual_source_stalled
            and not watchdog.runner_source_poll_starved,
            checks,
        )

        check(
            "R_ACTUAL_SOURCE_STALL_PRECEDES_RUNNER_STARVATION",
            recovered_reason == "PRODUCTION_SOURCE_STALLED"
            and recovered_reason != "RUNNER_SOURCE_POLL_STARVED",
            checks,
        )

        bounded_db = root / "bounded_probe.sqlite3"
        create_source_db(bounded_db)
        source, clock, watchdog = make_watchdog(bounded_db)
        for rowid in range(2, 12):
            append_health_row(
                bounded_db,
                rowid=rowid,
                at=BASE + timedelta(seconds=rowid),
            )
        clock.value = BASE + timedelta(seconds=12)
        watchdog.observe(11, 12.0)
        first_metrics = source.cache_metrics()
        clock.value = BASE + timedelta(seconds=13)
        watchdog.observe(11, 13.0)
        second_metrics = source.cache_metrics()
        check(
            "S_HEALTH_PROBE_SCANS_ONLY_NEW_BOUNDED_INTERVAL",
            first_metrics["source_health_rows_scanned"] == 10
            and second_metrics["source_health_rows_scanned"] == 10
            and second_metrics["source_health_probe_count"] == 2,
            checks,
        )

        malformed_db = root / "malformed_interval.sqlite3"
        create_source_db(malformed_db)
        source, clock, watchdog = make_watchdog(malformed_db)
        append_health_row(
            malformed_db,
            rowid=2,
            at=BASE + timedelta(seconds=1),
        )
        append_health_row(
            malformed_db,
            rowid=3,
            at=BASE + timedelta(seconds=20),
        )
        clock.value = BASE + timedelta(seconds=10)
        before_state = (
            watchdog.last_health_p1_rowid,
            watchdog.actual_source_advance_count,
            watchdog.last_actual_source_activity_utc,
        )
        malformed_rejected = False
        try:
            watchdog.observe(3, 10.0)
        except harness.accepted.BindingConflict:
            malformed_rejected = True
        after_state = (
            watchdog.last_health_p1_rowid,
            watchdog.actual_source_advance_count,
            watchdog.last_actual_source_activity_utc,
        )
        check(
            "S2_MALFORMED_INTERVAL_VALIDATION_IS_STATE_ATOMIC",
            malformed_rejected and before_state == after_state,
            checks,
        )

        binding_db = root / "binding_source.sqlite3"
        create_source_db(binding_db, historical_age_seconds=0.0)
        binding_source = ContinuousMarketSourceV02(
            binding_db,
            start_after_p1_rowid=1,
            database_identity="SELFTEST:BINDING-V03",
        )
        paper_path = root / "paper.sqlite3"
        conn = connect_entry_router_db(paper_path)
        binding = ContinuousFirstPullbackBindingV03(
            conn,
            binding_source,
            wall_clock=lambda: BASE,
        )
        runtime = conn.execute(
            "SELECT binding_model_id,binding_fingerprint "
            "FROM paper_fp_binding_runtime_v0_1 WHERE singleton=1"
        ).fetchone()
        check(
            "T_TIMER_WATERMARK_FENCE_IMPLEMENTATION_UNCHANGED",
            ContinuousFirstPullbackBindingV03.prepare_clock_tick
            is ContinuousFirstPullbackBindingV02.prepare_clock_tick
            and ContinuousFirstPullbackBindingV03.execute_prepared_timer
            is ContinuousFirstPullbackBindingV02.execute_prepared_timer
            and runtime[0] == BINDING_MODEL_ID
            and runtime[1] == BINDING_FINGERPRINT
            and binding.active_timer_fence_p1_rowid is None,
            checks,
        )
        conn.close()

        ro_conn = ContinuousMarketSourceV02.open_readonly(binding_db)
        try:
            rejected = False
            try:
                ro_conn.execute("DELETE FROM pump_events")
            except sqlite3.OperationalError:
                rejected = True
            check(
                "U_SOURCE_CONNECTION_QUERY_ONLY",
                rejected and ContinuousMarketSourceV02.connection_is_query_only(ro_conn),
                checks,
            )
        finally:
            ro_conn.close()

        terminal_digest = recovered_snapshot = watchdog_digest = None
        source, clock, replay_watchdog = make_watchdog(bounded_db)
        clock.value = BASE + timedelta(seconds=12)
        replay_watchdog.observe(11, 12.0)
        watchdog_digest = replay_watchdog.canonical_digest(12.0)
        source2, clock2, replay_watchdog2 = make_watchdog(bounded_db)
        clock2.value = BASE + timedelta(seconds=12)
        replay_watchdog2.observe(11, 12.0)
        terminal_digest = replay_watchdog2.canonical_digest(12.0)
        recovered_snapshot = replay_watchdog2.snapshot(12.0)
        check(
            "V_REPLAY_DETERMINISTIC_WITHOUT_DUPLICATE_EVIDENCE",
            watchdog_digest == terminal_digest
            and recovered_snapshot["actual_source_advance_count"] == 10,
            checks,
        )

        long_db = root / "long_healthy.sqlite3"
        create_source_db(long_db)
        source, clock, watchdog = make_watchdog(long_db)
        watchdog.observe(1, 0.0)
        survived = True
        for rowid in range(2, 116):
            offset = (rowid - 1) * 3.5
            append_health_row(
                long_db,
                rowid=rowid,
                at=BASE + timedelta(seconds=offset),
            )
            clock.value = BASE + timedelta(seconds=offset)
            if observe_reason(watchdog, rowid, offset) is not None:
                survived = False
                break
        check(
            "X_LONG_SESSION_WITH_HEALTHY_PROBES_SURVIVES_OLD_SHAPE",
            survived
            and not watchdog.actual_source_stalled
            and not watchdog.runner_source_poll_starved,
            checks,
        )
        wiring_db = root / "wiring.sqlite3"
        create_source_db(wiring_db)
        wiring_json = root / "wiring.json"
        original_v02_run = harness.accepted_v02.run_multihour
        original_source_global = harness.accepted.ContinuousMarketSourceV01
        original_watchdog_global = harness.accepted.SourceContinuityWatchdog
        wiring_observed: dict[str, object] = {}

        def fake_v02_run(*, duration_seconds: int, launch_collector: bool):
            wired_source = harness.accepted.ContinuousMarketSourceV01(
                wiring_db,
                start_after_p1_rowid=1,
                database_identity="SELFTEST:V03-WIRING",
            )
            wired_watchdog = harness.accepted.SourceContinuityWatchdog(
                last_observed_source_p1_rowid=1,
                last_source_advance_monotonic=0.0,
            )
            wired_watchdog.runner_source_poll_starved = True
            wired_watchdog.runner_starvation_count = 1
            wired_watchdog.failure_reason = "RUNNER_SOURCE_POLL_STARVED"
            wiring_observed.update({
                "source": wired_source,
                "watchdog": wired_watchdog,
                "binding_class": (
                    harness.accepted_v02.ContinuousFirstPullbackBindingV02
                ),
                "model_id": harness.accepted_v02.MODEL_ID,
            })
            payload = {
                "source_continuity": {},
                "runtime_counts": {},
                "stop_reason": "PRODUCTION_SOURCE_STALLED",
                "errors": ["PRODUCTION_SOURCE_STALLED"],
                "result_class": "FAIL",
                "artifact_paths": {
                    "paper_db": str(root / "wiring_absent_paper.sqlite3"),
                    "json": str(wiring_json),
                },
            }
            harness.accepted.smoke.write_json_artifact(wiring_json, payload)
            wiring_observed["first_write_stop_reason"] = json.loads(
                wiring_json.read_text(encoding="utf-8")
            )["stop_reason"]
            return 1, payload

        try:
            harness.accepted_v02.run_multihour = fake_v02_run
            wiring_code, wiring_summary = harness.run_multihour(
                duration_seconds=14_400,
                launch_collector=False,
            )
        finally:
            harness.accepted_v02.run_multihour = original_v02_run
        check(
            "X2_VERSIONED_WIRING_INJECTS_AND_RESTORES_ACCEPTED_GLOBALS",
            wiring_code == 1
            and isinstance(
                wiring_observed.get("source"), ContinuousMarketSourceV02
            )
            and isinstance(
                wiring_observed.get("watchdog"),
                harness.EvidenceBasedSourceContinuityWatchdogV03,
            )
            and wiring_observed.get("binding_class")
            is ContinuousFirstPullbackBindingV03
            and wiring_observed.get("model_id") == harness.MODEL_ID
            and wiring_observed.get("first_write_stop_reason")
            == "RUNNER_SOURCE_POLL_STARVED"
            and wiring_summary["multihour_model_id"] == harness.MODEL_ID
            and harness.accepted.ContinuousMarketSourceV01
            is original_source_global
            and harness.accepted.SourceContinuityWatchdog
            is original_watchdog_global,
            checks,
        )
        check(
            "Y_BLOCKED_RUNNER_CLASSIFIED_AS_RUNNER_STARVATION",
            nightly_reason == "RUNNER_SOURCE_POLL_STARVED"
            and nightly_snapshot["failure_reason"] == "RUNNER_SOURCE_POLL_STARVED",
            checks,
        )
        rewritten_json = root / "rewritten_runner_starvation.json"
        rewritten_summary = {
            "source_continuity": {},
            "runtime_counts": {},
            "stop_reason": "PRODUCTION_SOURCE_STALLED",
            "errors": ["PRODUCTION_SOURCE_STALLED"],
            "result_class": "FAIL",
            "artifact_paths": {
                "paper_db": str(root / "absent_paper.sqlite3"),
                "json": str(rewritten_json),
            },
        }
        harness._rewrite_v03_summary(
            rewritten_summary,
            source=nightly_source,
            watchdog=nightly_watchdog,
        )
        persisted_summary = json.loads(rewritten_json.read_text(encoding="utf-8"))
        check(
            "Y2_PERSISTED_SUMMARY_CANNOT_RETAIN_FALSE_SOURCE_STALL",
            rewritten_summary["stop_reason"] == "RUNNER_SOURCE_POLL_STARVED"
            and rewritten_summary["errors"] == ["RUNNER_SOURCE_POLL_STARVED"]
            and persisted_summary["stop_reason"] == "RUNNER_SOURCE_POLL_STARVED"
            and rewritten_summary["runtime_counts"][
                "launch_reconstruction_queries"
            ]
            == 0,
            checks,
        )
        check(
            "Z_T012_LATE_ENTRY_CONTRACT_STILL_BOUND",
            ELIGIBILITY_WINDOW_US == {
                "FINAL-A": 15_000_000,
                "FINAL-B": 15_000_000,
                "SENS-C": 5_000_000,
            }
            and harness.MULTIHOUR_SPEC["entry_execution_deadline_fingerprint"]
            == DEADLINE_FINGERPRINT,
            checks,
        )
    finally:
        shutil.rmtree(root, ignore_errors=True)

    print("=" * 104)
    print("CONTINUOUS FIRSTPULLBACK MULTI-HOUR RUN V0.3 SELF-TEST")
    print("=" * 104)
    for name, passed in checks.items():
        print(f"{name}: {'PASS' if passed else 'FAIL'}")
    print("CONTINUITY_EVIDENCE=" + json.dumps(evidence, sort_keys=True, default=str))
    passed = bool(checks) and all(checks.values())
    print(f"RESULT: {'PASS' if passed else 'FAIL'}")
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
