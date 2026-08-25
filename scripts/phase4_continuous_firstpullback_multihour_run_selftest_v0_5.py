from __future__ import annotations

import shutil
import sys
import tempfile
import time
from datetime import timedelta
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
for import_root in (PROJECT_ROOT / "src", PROJECT_ROOT / "scripts"):
    if str(import_root) not in sys.path:
        sys.path.insert(0, str(import_root))

import phase4_continuous_firstpullback_multihour_run_v0_5 as harness  # noqa: E402
import phase4_timer_fence_throughput_selftest_v0_1 as fixtures  # noqa: E402
from phase4.paper_continuous_firstpullback_binding_v0_5 import (  # noqa: E402
    MODEL_FINGERPRINT as BINDING_FINGERPRINT,
    AtomicSourceBatchConnection,
    ContinuousFirstPullbackBindingV05,
)
from phase4.paper_continuous_market_source_v0_2 import (  # noqa: E402
    ContinuousMarketSourceV02,
)


BASE = fixtures.BASE
EXPECTED_HARNESS_FINGERPRINT = (
    "7e439eb90ca0d44467501ae15008ecc4c733582dc14a04afae9a94ac9ab9f3ca"
)
EXPECTED_BINDING_FINGERPRINT = (
    "f66acb5926f3dd77d68c086d74e1bc07b43fad4307071c1ec8c3b7d1111660b7"
)


def check(name: str, condition: bool, checks: dict[str, bool]) -> None:
    checks[name] = bool(condition)


def main() -> int:
    checks: dict[str, bool] = {}
    root = Path(tempfile.mkdtemp(prefix="phase4_multihour_v05_"))
    original_run = harness.accepted_v04.accepted_v03.run_multihour
    original_binding = (
        harness.accepted_v04.accepted_v03.ContinuousFirstPullbackBindingV03
    )
    original_connector = harness.accepted.connect_entry_router_db
    original_v04_model_id = harness.accepted_v04.MODEL_ID
    try:
        harness._validate_locked_contracts_v05()
        check(
            "MODEL_IDENTITIES",
            harness.MODEL_ID
            == "P4-CONTINUOUS-FIRSTPULLBACK-MULTIHOUR-RUN-0005"
            and harness.MODEL_FINGERPRINT == EXPECTED_HARNESS_FINGERPRINT
            and BINDING_FINGERPRINT == EXPECTED_BINDING_FINGERPRINT,
            checks,
        )
        check(
            "DURATION_AND_TIMER_POLICY_UNCHANGED",
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
                database_identity="SELFTEST:T016:HARNESS-WIRING",
            )
            conn = harness.accepted.connect_entry_router_db(paper_path)
            binding = (
                harness.accepted_v04.accepted_v03.
                ContinuousFirstPullbackBindingV03(
                    conn,
                    source,
                    wall_clock=lambda: BASE + timedelta(seconds=5),
                )
            )
            mono = time.monotonic()
            watchdog = (
                harness.accepted_v04.accepted_v03.
                EvidenceBasedSourceContinuityWatchdogV03(
                    source=source,
                    last_observed_source_p1_rowid=0,
                    last_source_advance_monotonic=mono,
                    session_started_at_utc=BASE,
                    wall_clock=lambda: BASE + timedelta(seconds=5),
                )
            )
            metrics = harness.accepted.smoke.RuntimeMetrics([])
            watermark = harness.accepted.smoke._execute_clock_cycle(
                binding,
                metrics,
                anchor=0,
                instant=BASE + timedelta(seconds=5),
                timer_id="V05-WIRING",
            )
            final_watermark = harness.accepted.finalize_at_watermark(
                conn,
                binding,
                metrics,
                anchor=0,
                instant=BASE + timedelta(seconds=6),
                timer_id="V05-FINAL",
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
            harness.accepted_v04.accepted_v03._apply_v03_summary_fields(
                summary, source=source, watchdog=watchdog
            )
            observed.update({
                "binding": binding,
                "connection": conn,
                "watchdog": watchdog,
                "watermark": watermark,
                "final_watermark": final_watermark,
                "durable_p1_rowid": binding.durable_p1_rowid,
            })
            conn.close()
            return 0, summary

        harness.accepted_v04.accepted_v03.run_multihour = fake_run
        code, summary = harness.run_multihour(
            duration_seconds=14_400,
            launch_collector=False,
        )
        binding = observed["binding"]
        connection = observed["connection"]
        performance = summary["performance_breakdown_v0_5"]
        check(
            "VERSIONED_ATOMIC_BINDING_WIRING",
            code == 0
            and isinstance(binding, ContinuousFirstPullbackBindingV05)
            and isinstance(connection, AtomicSourceBatchConnection)
            and observed["durable_p1_rowid"] == 40
            and observed["watermark"] == 40
            and observed["final_watermark"] == 40,
            checks,
        )
        check(
            "ATOMIC_BATCH_SUMMARY",
            summary["multihour_model_id"] == harness.MODEL_ID
            and summary["multihour_fingerprint"] == harness.MODEL_FINGERPRINT
            and "performance_breakdown_v0_4" not in summary
            and performance["source_batch_commits"] >= 1
            and performance["source_batch_rollbacks"] == 0
            and performance["suppressed_commit_boundaries"] > 0
            and summary["runtime_counts"]["timer_fence_wait_seconds"] == 0.0,
            checks,
        )
        check(
            "ACCEPTED_GLOBALS_RESTORED",
            harness.accepted_v04.accepted_v03.
            ContinuousFirstPullbackBindingV03 is original_binding
            and harness.accepted.connect_entry_router_db is original_connector
            and harness.accepted_v04.MODEL_ID == original_v04_model_id,
            checks,
        )
    finally:
        harness.accepted_v04.accepted_v03.run_multihour = original_run
        shutil.rmtree(root, ignore_errors=True)

    print("=" * 104)
    print("CONTINUOUS FIRSTPULLBACK MULTI-HOUR RUN V0.5 SELF-TEST")
    print("=" * 104)
    for name, passed in checks.items():
        print(f"{name}: {'PASS' if passed else 'FAIL'}")
    passed = bool(checks) and all(checks.values())
    print(f"RESULT: {'PASS' if passed else 'FAIL'}")
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
