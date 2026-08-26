from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path
from typing import Any, Sequence


PROJECT_ROOT = Path(__file__).resolve().parents[1]
for import_root in (PROJECT_ROOT / "src", PROJECT_ROOT / "scripts"):
    if str(import_root) not in sys.path:
        sys.path.insert(0, str(import_root))

import phase4_continuous_firstpullback_multihour_run_v0_4 as accepted_v04  # noqa: E402
from phase4.paper_continuous_firstpullback_binding_v0_5 import (  # noqa: E402
    MODEL_FINGERPRINT as BINDING_FINGERPRINT,
    MODEL_ID as BINDING_MODEL_ID,
    ContinuousFirstPullbackBindingV05,
    connect_atomic_entry_router_db,
)
from phase4.paper_continuous_market_source_v0_2 import (  # noqa: E402
    MODEL_FINGERPRINT as MARKET_SOURCE_FINGERPRINT,
)


accepted = accepted_v04.accepted
MODEL_ID = "P4-CONTINUOUS-FIRSTPULLBACK-MULTIHOUR-RUN-0005"
ACCEPTED_V04_HARNESS_SHA256 = (
    "6fa45f93892bea10d8c67c31d63cd2de490f2c318df1133d6ac063b5c723d5ab"
)
MULTIHOUR_SPEC = json.loads(json.dumps(accepted_v04.MULTIHOUR_SPEC))
MULTIHOUR_SPEC.update({
    "model_id": MODEL_ID,
    "accepted_multihour_v0_4_sha256": ACCEPTED_V04_HARNESS_SHA256,
    "binding_model_id": BINDING_MODEL_ID,
    "binding_fingerprint": BINDING_FINGERPRINT,
    "source_batch_persistence": {
        "transaction": "ONE_ATOMIC_COMMIT_PER_FETCHED_SOURCE_BATCH",
        "journal_mode": "WAL",
        "synchronous": "FULL",
        "rollback": "WHOLE_BATCH_TO_PREVIOUS_DURABLE_CURSOR",
        "replay": "DETERMINISTIC_FROM_PREVIOUS_DURABLE_CURSOR",
    },
    "source_continuity_ordering": {
        "durable_order": "STRICTLY_INCREASING_P1_SQLITE_ROWID",
        "inserted_at_utc": "VALIDATED_UTC_METADATA_NOT_AN_ORDERING_KEY",
        "health_activity": "MAX_OBSERVED_INSERTED_AT_NOT_EARLIER_THAN_SESSION_START",
    },
})
MODEL_FINGERPRINT = hashlib.sha256(
    json.dumps(MULTIHOUR_SPEC, sort_keys=True, separators=(",", ":")).encode("utf-8")
).hexdigest()

DEFAULT_DURATION_SECONDS = accepted_v04.DEFAULT_DURATION_SECONDS
MIN_DURATION_SECONDS = accepted_v04.MIN_DURATION_SECONDS
MAX_DURATION_SECONDS = accepted_v04.MAX_DURATION_SECONDS
PRODUCTION_DB = accepted_v04.PRODUCTION_DB
RUNTIME_DIR = accepted_v04.RUNTIME_DIR
parse_duration_seconds = accepted_v04.parse_duration_seconds
run_control_reason = accepted_v04.run_control_reason
_ORIGINAL_APPLY_V04 = accepted_v04._apply_v04_summary_fields


class EvidenceBasedSourceContinuityWatchdogV05(
    accepted_v04.accepted_v03.EvidenceBasedSourceContinuityWatchdogV03
):
    """Use P1 rowid for order and a monotonic maximum for source health time."""

    def observe(self, latest_p1_rowid: int, now_monotonic: float) -> None:
        if self.failure_reason is not None:
            raise accepted_v04.accepted_v03.ProductionSourceStalled(
                self.failure_reason
            )
        latest = int(latest_p1_rowid)
        now_mono = float(now_monotonic)
        assert self.last_health_p1_rowid is not None
        assert self.last_actual_source_activity_utc is not None
        assert self.last_probe_monotonic is not None
        if latest < self.last_health_p1_rowid:
            raise accepted.BindingConflict(
                "non-monotonic production latest p1_rowid: "
                f"{latest} < {self.last_health_p1_rowid}"
            )
        runner_interval = max(0.0, now_mono - self.last_probe_monotonic)
        runner_starved = (
            runner_interval
            >= accepted_v04.accepted_v03.RUNNER_SOURCE_POLL_STARVATION_FAIL_SECONDS
        )
        probe_at = accepted_v04.accepted_v03._utc(self.wall_clock())
        new_rows = self.source.continuity_rows_after(
            self.last_health_p1_rowid,
            through_p1_rowid=latest,
        )
        if latest > self.last_health_p1_rowid and not new_rows:
            raise accepted.BindingConflict(
                "production latest p1_rowid advanced but bounded health rows are missing"
            )
        if new_rows and int(new_rows[-1][0]) != latest:
            raise accepted.BindingConflict(
                "bounded health interval does not reach production latest p1_rowid"
            )

        # Validate and calculate on local state. P1 rowid is the sole durable
        # order. inserted_at_utc remains validated source-health metadata, and
        # its effective activity value is the maximum seen so a newer row with
        # an older timestamp cannot move health state backwards.
        prior_activity = self.last_actual_source_activity_utc
        maximum_actual_gap = self.maximum_actual_source_gap_seconds
        actual_source_stalled = self.actual_source_stalled
        previous_rowid = self.last_health_p1_rowid
        for rowid, inserted_at in new_rows:
            if int(rowid) <= previous_rowid:
                raise accepted.BindingConflict(
                    "source health rows are not strictly increasing"
                )
            raw_activity_at = accepted_v04.accepted_v03._utc(inserted_at)
            if raw_activity_at > probe_at:
                raise accepted.BindingConflict(
                    "source inserted_at_utc is later than the continuity probe: "
                    f"p1 rowid {rowid}"
                )
            activity_at = max(raw_activity_at, self.session_started_at_utc)
            effective_activity_at = max(prior_activity, activity_at)
            gap = (effective_activity_at - prior_activity).total_seconds()
            maximum_actual_gap = max(maximum_actual_gap, gap)
            if gap >= accepted_v04.accepted_v03.SOURCE_STALL_FAIL_SECONDS:
                actual_source_stalled = True
            prior_activity = effective_activity_at
            previous_rowid = int(rowid)

        final_staleness = max(
            0.0,
            (probe_at - prior_activity).total_seconds(),
        )
        maximum_actual_gap = max(maximum_actual_gap, final_staleness)
        if final_staleness >= accepted_v04.accepted_v03.SOURCE_STALL_FAIL_SECONDS:
            actual_source_stalled = True

        self.maximum_runner_probe_interval_seconds = max(
            self.maximum_runner_probe_interval_seconds,
            runner_interval,
        )
        if runner_starved:
            self.runner_source_poll_starved = True
            self.runner_starvation_count += 1
        self.maximum_actual_source_gap_seconds = maximum_actual_gap
        self.actual_source_stalled = actual_source_stalled

        if new_rows:
            self.source_advance_count += 1
            self.actual_source_advance_count += len(new_rows)
            self.last_observed_source_p1_rowid = latest
            self.last_source_advance_monotonic = now_mono
            self.last_actual_source_activity_utc = prior_activity
        self.last_health_p1_rowid = latest
        self.last_probe_monotonic = now_mono

        if self.actual_source_stalled:
            self.failure_reason = "PRODUCTION_SOURCE_STALLED"
        elif self.runner_source_poll_starved:
            self.failure_reason = "RUNNER_SOURCE_POLL_STARVED"
        if self.failure_reason is not None:
            raise accepted_v04.accepted_v03.ProductionSourceStalled(
                self.failure_reason
            )


def _validate_locked_contracts_v05() -> None:
    accepted_v04._validate_locked_contracts_v04()
    if accepted.file_sha256(Path(accepted_v04.__file__).resolve()) != (
        ACCEPTED_V04_HARNESS_SHA256
    ):
        raise RuntimeError("accepted multi-hour v0.4 harness changed")
    expected = (MARKET_SOURCE_FINGERPRINT, BINDING_FINGERPRINT)
    actual = (
        "9cb094f52bf4b4fe28dc4828b1d9a52a3350cde664c7da5a489fa84e9a4085a9",
        "f66acb5926f3dd77d68c086d74e1bc07b43fad4307071c1ec8c3b7d1111660b7",
    )
    if expected != actual:
        raise RuntimeError("v0.5 source/binding fingerprint drift")


def _apply_v05_summary_fields(
    summary: dict[str, Any],
    *,
    binding: ContinuousFirstPullbackBindingV05,
    watchdog,
) -> None:
    _ORIGINAL_APPLY_V04(summary, binding=binding, watchdog=watchdog)
    performance = binding.performance_metrics()
    summary["multihour_model_id"] = MODEL_ID
    summary["multihour_fingerprint"] = MODEL_FINGERPRINT
    summary["spec"] = MULTIHOUR_SPEC
    summary["runtime_counts"].update(performance)
    summary.pop("performance_breakdown_v0_4", None)
    summary["performance_breakdown_v0_5"] = performance


def run_multihour(*, duration_seconds: int, launch_collector: bool):
    _validate_locked_contracts_v05()
    original_binding_class = accepted_v04.ContinuousFirstPullbackBindingV04
    original_watchdog_class = (
        accepted_v04.accepted_v03.EvidenceBasedSourceContinuityWatchdogV03
    )
    original_validate = accepted_v04._validate_locked_contracts_v04
    original_model_id = accepted_v04.MODEL_ID
    original_model_fingerprint = accepted_v04.MODEL_FINGERPRINT
    original_spec = accepted_v04.MULTIHOUR_SPEC
    original_binding_id = accepted_v04.BINDING_MODEL_ID
    original_binding_fingerprint = accepted_v04.BINDING_FINGERPRINT
    original_apply = accepted_v04._apply_v04_summary_fields
    original_connect = accepted.connect_entry_router_db
    try:
        accepted_v04.ContinuousFirstPullbackBindingV04 = (
            ContinuousFirstPullbackBindingV05
        )
        accepted_v04.accepted_v03.EvidenceBasedSourceContinuityWatchdogV03 = (
            EvidenceBasedSourceContinuityWatchdogV05
        )
        accepted_v04._validate_locked_contracts_v04 = lambda: None
        accepted_v04.MODEL_ID = MODEL_ID
        accepted_v04.MODEL_FINGERPRINT = MODEL_FINGERPRINT
        accepted_v04.MULTIHOUR_SPEC = MULTIHOUR_SPEC
        accepted_v04.BINDING_MODEL_ID = BINDING_MODEL_ID
        accepted_v04.BINDING_FINGERPRINT = BINDING_FINGERPRINT
        accepted_v04._apply_v04_summary_fields = _apply_v05_summary_fields
        accepted.connect_entry_router_db = connect_atomic_entry_router_db
        return accepted_v04.run_multihour(
            duration_seconds=duration_seconds,
            launch_collector=launch_collector,
        )
    finally:
        accepted.connect_entry_router_db = original_connect
        accepted_v04._apply_v04_summary_fields = original_apply
        accepted_v04.BINDING_FINGERPRINT = original_binding_fingerprint
        accepted_v04.BINDING_MODEL_ID = original_binding_id
        accepted_v04.MULTIHOUR_SPEC = original_spec
        accepted_v04.MODEL_FINGERPRINT = original_model_fingerprint
        accepted_v04.MODEL_ID = original_model_id
        accepted_v04._validate_locked_contracts_v04 = original_validate
        accepted_v04.accepted_v03.EvidenceBasedSourceContinuityWatchdogV03 = (
            original_watchdog_class
        )
        accepted_v04.ContinuousFirstPullbackBindingV04 = original_binding_class


def build_arg_parser():
    return accepted_v04.build_arg_parser()


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    if args.collector_child:
        return accepted.run_collector_child()
    if args.collector_child_probe:
        return accepted.run_collector_child_probe(args.collector_child_probe_ready)
    if not args.use_existing_collector and not args.launch_managed_collector:
        parser.error("one collector mode is required")
    code, _summary = run_multihour(
        duration_seconds=args.duration_seconds,
        launch_collector=bool(args.launch_managed_collector),
    )
    return code


if __name__ == "__main__":
    raise SystemExit(main())
