from __future__ import annotations

import shutil
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
for import_root in (PROJECT_ROOT / "src", PROJECT_ROOT / "scripts"):
    if str(import_root) not in sys.path:
        sys.path.insert(0, str(import_root))

import phase4_continuous_firstpullback_multihour_run_v0_4 as harness  # noqa: E402
import phase4_timer_fence_throughput_selftest_v0_1 as fixtures  # noqa: E402
from phase4.paper_continuous_firstpullback_binding_v0_4 import (  # noqa: E402
    MODEL_FINGERPRINT as BINDING_FINGERPRINT,
    ContinuousFirstPullbackBindingV04,
)
from phase4.paper_continuous_market_source_v0_2 import (  # noqa: E402
    ContinuousMarketSourceV02,
)
from phase4.paper_entry_router_v0_1 import connect_entry_router_db  # noqa: E402


BASE = datetime(2026, 8, 25, 7, 0, tzinfo=timezone.utc)
EXPECTED_HARNESS_FINGERPRINT = (
    "ce690a8fa864516fcb6fe7c834e4a4342655a9c01c80f522dc0ba9ba3673e0ad"
)
EXPECTED_BINDING_FINGERPRINT = (
    "e2c35ce63fe419ac4a934913dd26249d72411884cb115e0701f601b276b0f7d6"
)


def check(name: str, condition: bool, checks: dict[str, bool]) -> None:
    checks[name] = bool(condition)


def main() -> int:
    checks: dict[str, bool] = {}
    root = Path(tempfile.mkdtemp(prefix="phase4_multihour_v04_"))
    original_run = harness.accepted_v03.run_multihour
    original_binding = harness.accepted_v03.ContinuousFirstPullbackBindingV03
    original_watchdog = harness.accepted_v03.EvidenceBasedSourceContinuityWatchdogV03
    original_cycle = harness.accepted.smoke._execute_clock_cycle
    original_finalize = harness.accepted.finalize_at_watermark
    try:
        harness._validate_locked_contracts_v04()
        check(
            "MODEL_IDENTITIES",
            harness.MODEL_ID
            == "P4-CONTINUOUS-FIRSTPULLBACK-MULTIHOUR-RUN-0004"
            and harness.MODEL_FINGERPRINT == EXPECTED_HARNESS_FINGERPRINT
            and BINDING_FINGERPRINT == EXPECTED_BINDING_FINGERPRINT,
            checks,
        )
        check(
            "DURATION_POLICY_UNCHANGED",
            harness.parse_duration_seconds(14_400) == 14_400
            and harness.run_control_reason(14_399.999, 14_400) is None
            and harness.run_control_reason(14_400, 14_400)
            == "DURATION_REACHED",
            checks,
        )

        source_path = root / "source.sqlite3"
        fixtures.create_source(source_path)
        fixtures.append_rows(source_path, 1, 40)
        paper_path = root / "paper.sqlite3"
        observed: dict[str, object] = {}

        def fake_run(*, duration_seconds: int, launch_collector: bool):
            source = ContinuousMarketSourceV02(
                source_path,
                start_after_p1_rowid=0,
                database_identity="SELFTEST:T015:HARNESS-WIRING",
            )
            conn = connect_entry_router_db(paper_path)
            binding = harness.accepted_v03.ContinuousFirstPullbackBindingV03(
                conn, source, wall_clock=lambda: BASE + timedelta(seconds=5)
            )
            mono = time.monotonic()
            watchdog = harness.accepted_v03.EvidenceBasedSourceContinuityWatchdogV03(
                source=source,
                last_observed_source_p1_rowid=0,
                last_source_advance_monotonic=mono,
                session_started_at_utc=BASE,
                wall_clock=lambda: BASE + timedelta(seconds=5),
            )
            metrics = harness.accepted.smoke.RuntimeMetrics([])
            watermark = harness.accepted.smoke._execute_clock_cycle(
                binding,
                metrics,
                anchor=0,
                instant=BASE + timedelta(seconds=5),
                timer_id="V04-WIRING",
            )
            final_watermark = harness.accepted.finalize_at_watermark(
                conn,
                binding,
                metrics,
                anchor=0,
                instant=BASE + timedelta(seconds=6),
                timer_id="V04-FINAL",
            )
            summary = {
                "multihour_model_id": "OLD",
                "multihour_fingerprint": "OLD",
                "spec": {},
                "runtime_counts": {
                    "timer_fence_wait_count": 99,
                    "timer_fence_wait_seconds": 99.0,
                },
                "errors": [],
                "stop_reason": "DURATION_REACHED",
                "result_class": "PASS_MULTI_HOUR",
                "artifact_paths": {
                    "paper_db": str(paper_path),
                    "json": str(root / "summary.json"),
                },
            }
            harness.accepted_v03._apply_v03_summary_fields(
                summary, source=source, watchdog=watchdog
            )
            observed.update({
                "binding": binding,
                "watchdog": watchdog,
                "watermark": watermark,
                "final_watermark": final_watermark,
                "durable_p1_rowid": binding.durable_p1_rowid,
                "metrics": metrics,
            })
            conn.close()
            return 0, summary

        harness.accepted_v03.run_multihour = fake_run
        code, summary = harness.run_multihour(
            duration_seconds=14_400,
            launch_collector=False,
        )
        binding = observed["binding"]
        watchdog = observed["watchdog"]
        performance = summary["performance_breakdown_v0_4"]
        check(
            "VERSIONED_BINDING_WIRING",
            code == 0
            and isinstance(binding, ContinuousFirstPullbackBindingV04)
            and observed["durable_p1_rowid"] == 40
            and observed["watermark"] == 40
            and observed["final_watermark"] == 40,
            checks,
        )
        check(
            "ACTIVE_DRAIN_SUMMARY",
            summary["multihour_model_id"] == harness.MODEL_ID
            and summary["multihour_fingerprint"] == harness.MODEL_FINGERPRINT
            and summary["runtime_counts"]["timer_fence_wait_count"] == 0
            and summary["runtime_counts"]["timer_fence_wait_seconds"] == 0.0
            and performance["fence_drain_raw_rows"] == 40
            and performance["passive_fence_wait_seconds"] == 0.0,
            checks,
        )
        continuity = watchdog.snapshot(
            watchdog.last_probe_monotonic
            or watchdog.last_source_advance_monotonic
        )
        check(
            "T014_HEALTH_DURING_DRAIN",
            continuity["actual_source_advance_count"] == 40
            and not continuity["actual_source_stalled"]
            and not continuity["runner_source_poll_starved"],
            checks,
        )
        check(
            "ACCEPTED_GLOBALS_RESTORED",
            harness.accepted_v03.ContinuousFirstPullbackBindingV03
            is original_binding
            and harness.accepted_v03.EvidenceBasedSourceContinuityWatchdogV03
            is original_watchdog
            and harness.accepted.smoke._execute_clock_cycle is original_cycle
            and harness.accepted.finalize_at_watermark is original_finalize,
            checks,
        )
    finally:
        harness.accepted_v03.run_multihour = original_run
        shutil.rmtree(root, ignore_errors=True)

    print("=" * 104)
    print("CONTINUOUS FIRSTPULLBACK MULTI-HOUR RUN V0.4 SELF-TEST")
    print("=" * 104)
    for name, passed in checks.items():
        print(f"{name}: {'PASS' if passed else 'FAIL'}")
    passed = bool(checks) and all(checks.values())
    print(f"RESULT: {'PASS' if passed else 'FAIL'}")
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
