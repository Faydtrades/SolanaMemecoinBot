from __future__ import annotations

import hashlib
import json
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Sequence


PROJECT_ROOT = Path(__file__).resolve().parents[1]
for import_root in (PROJECT_ROOT / "src", PROJECT_ROOT / "scripts"):
    if str(import_root) not in sys.path:
        sys.path.insert(0, str(import_root))

import phase4_continuous_firstpullback_multihour_run_v0_2 as accepted_v02  # noqa: E402
from phase4.paper_continuous_firstpullback_binding_v0_3 import (  # noqa: E402
    MODEL_FINGERPRINT as BINDING_FINGERPRINT,
    MODEL_ID as BINDING_MODEL_ID,
    ContinuousFirstPullbackBindingV03,
)
from phase4.paper_continuous_market_source_v0_2 import (  # noqa: E402
    MODEL_FINGERPRINT as MARKET_SOURCE_FINGERPRINT,
    MODEL_ID as MARKET_SOURCE_MODEL_ID,
    ContinuousMarketSourceV02,
)


accepted = accepted_v02.accepted
MODEL_ID = "P4-CONTINUOUS-FIRSTPULLBACK-MULTIHOUR-RUN-0003"
SOURCE_STALL_FAIL_SECONDS = 300.0
RUNNER_SOURCE_POLL_STARVATION_FAIL_SECONDS = 300.0
ACCEPTED_V02_HARNESS_SHA256 = (
    "08075ba45b2fb59316070803983cb9d90de1698d7b0e95724f7181bd5c3bf47f"
)
MULTIHOUR_SPEC = json.loads(json.dumps(accepted_v02.MULTIHOUR_SPEC))
MULTIHOUR_SPEC.update({
    "model_id": MODEL_ID,
    "accepted_multihour_v0_2_sha256": ACCEPTED_V02_HARNESS_SHA256,
    "binding_model_id": BINDING_MODEL_ID,
    "binding_fingerprint": BINDING_FINGERPRINT,
    "market_source_model_id": MARKET_SOURCE_MODEL_ID,
    "market_source_fingerprint": MARKET_SOURCE_FINGERPRINT,
    "source_continuity": {
        "source_stall_fail_seconds": SOURCE_STALL_FAIL_SECONDS,
        "runner_poll_starvation_fail_seconds": (
            RUNNER_SOURCE_POLL_STARVATION_FAIL_SECONDS
        ),
        "source_activity_evidence": (
            "INCREMENTAL_PUMP_EVENTS_ROWID_AND_INSERTED_AT_UTC"
        ),
        "session_baseline": "BOUNDED_SESSION_START_UTC_NOT_HISTORICAL_ANCHOR_AGE",
        "source_failure_reason": "PRODUCTION_SOURCE_STALLED",
        "runner_failure_reason": "RUNNER_SOURCE_POLL_STARVED",
        "failure_precedence": (
            "PRODUCTION_SOURCE_STALLED_THEN_RUNNER_SOURCE_POLL_STARVED"
        ),
        "exact_boundary": "FAIL_IF_GREATER_THAN_OR_EQUAL_TO_300_SECONDS",
        "process_liveness_is_insufficient": True,
        "required_for_pass": True,
    },
})
MODEL_FINGERPRINT = hashlib.sha256(
    json.dumps(MULTIHOUR_SPEC, sort_keys=True, separators=(",", ":")).encode("utf-8")
).hexdigest()

DEFAULT_DURATION_SECONDS = accepted_v02.DEFAULT_DURATION_SECONDS
MIN_DURATION_SECONDS = accepted_v02.MIN_DURATION_SECONDS
MAX_DURATION_SECONDS = accepted_v02.MAX_DURATION_SECONDS
PRODUCTION_DB = accepted_v02.PRODUCTION_DB
RUNTIME_DIR = accepted_v02.RUNTIME_DIR
parse_duration_seconds = accepted_v02.parse_duration_seconds
run_control_reason = accepted_v02.run_control_reason
ProductionSourceStalled = accepted_v02.ProductionSourceStalled
finalize_at_watermark = accepted_v02.finalize_at_watermark
preserve_source_stall_failure_state = accepted_v02.preserve_source_stall_failure_state


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("continuity timestamps must be timezone-aware")
    return value.astimezone(timezone.utc)


@dataclass
class EvidenceBasedSourceContinuityWatchdogV03:
    """Separates actual source gaps from delayed runner health probes."""

    source: ContinuousMarketSourceV02
    last_observed_source_p1_rowid: int
    last_source_advance_monotonic: float
    session_started_at_utc: datetime
    wall_clock: Callable[[], datetime]
    last_health_p1_rowid: int | None = None
    last_actual_source_activity_utc: datetime | None = None
    last_probe_monotonic: float | None = None
    actual_source_advance_count: int = 0
    source_advance_count: int = 0
    maximum_actual_source_gap_seconds: float = 0.0
    maximum_runner_probe_interval_seconds: float = 0.0
    runner_starvation_count: int = 0
    actual_source_stalled: bool = False
    runner_source_poll_starved: bool = False
    failure_reason: str | None = None

    def __post_init__(self) -> None:
        self.last_observed_source_p1_rowid = int(
            self.last_observed_source_p1_rowid
        )
        self.last_source_advance_monotonic = float(
            self.last_source_advance_monotonic
        )
        self.session_started_at_utc = _utc(self.session_started_at_utc)
        if self.last_health_p1_rowid is None:
            self.last_health_p1_rowid = self.last_observed_source_p1_rowid
        if self.last_actual_source_activity_utc is None:
            self.last_actual_source_activity_utc = self.session_started_at_utc
        if self.last_probe_monotonic is None:
            self.last_probe_monotonic = self.last_source_advance_monotonic

    def observe(self, latest_p1_rowid: int, now_monotonic: float) -> None:
        if self.failure_reason is not None:
            raise ProductionSourceStalled(self.failure_reason)
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
            runner_interval >= RUNNER_SOURCE_POLL_STARVATION_FAIL_SECONDS
        )
        probe_at = _utc(self.wall_clock())
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

        # Validate and calculate on local state. A malformed later row must not
        # publish partial continuity advancement from earlier rows.
        prior_activity = self.last_actual_source_activity_utc
        maximum_actual_gap = self.maximum_actual_source_gap_seconds
        actual_source_stalled = self.actual_source_stalled
        previous_rowid = self.last_health_p1_rowid
        for rowid, inserted_at in new_rows:
            if int(rowid) <= previous_rowid:
                raise accepted.BindingConflict(
                    "source health rows are not strictly increasing"
                )
            raw_activity_at = _utc(inserted_at)
            if raw_activity_at > probe_at:
                raise accepted.BindingConflict(
                    "source inserted_at_utc is later than the continuity probe: "
                    f"p1 rowid {rowid}"
                )
            activity_at = max(raw_activity_at, self.session_started_at_utc)
            if activity_at < prior_activity:
                raise accepted.BindingConflict(
                    "non-monotonic source inserted_at_utc at p1 rowid "
                    f"{rowid}: {activity_at.isoformat()} < {prior_activity.isoformat()}"
                )
            gap = (activity_at - prior_activity).total_seconds()
            maximum_actual_gap = max(maximum_actual_gap, gap)
            if gap >= SOURCE_STALL_FAIL_SECONDS:
                actual_source_stalled = True
            prior_activity = activity_at
            previous_rowid = int(rowid)

        final_staleness = max(0.0, (probe_at - prior_activity).total_seconds())
        maximum_actual_gap = max(maximum_actual_gap, final_staleness)
        if final_staleness >= SOURCE_STALL_FAIL_SECONDS:
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
            raise ProductionSourceStalled(self.failure_reason)

    def snapshot(self, now_monotonic: float) -> dict[str, Any]:
        assert self.last_actual_source_activity_utc is not None
        final_staleness = max(
            0.0,
            (_utc(self.wall_clock()) - self.last_actual_source_activity_utc).total_seconds(),
        )
        maximum_actual_gap = max(
            self.maximum_actual_source_gap_seconds,
            final_staleness,
        )
        source_metrics = self.source.cache_metrics()
        continuity_ok = (
            self.failure_reason is None
            and self.actual_source_advance_count > 0
            and maximum_actual_gap < SOURCE_STALL_FAIL_SECONDS
            and final_staleness < SOURCE_STALL_FAIL_SECONDS
            and not self.runner_source_poll_starved
        )
        return {
            "stall_fail_seconds": SOURCE_STALL_FAIL_SECONDS,
            "runner_poll_starvation_fail_seconds": (
                RUNNER_SOURCE_POLL_STARVATION_FAIL_SECONDS
            ),
            "source_health_probe_count": source_metrics["source_health_probe_count"],
            "source_health_rows_scanned": source_metrics["source_health_rows_scanned"],
            "actual_source_advance_count": self.actual_source_advance_count,
            "source_advance_count": self.source_advance_count,
            "maximum_actual_source_gap_seconds": maximum_actual_gap,
            "maximum_source_stall_seconds": maximum_actual_gap,
            "final_actual_source_staleness_seconds": final_staleness,
            "final_source_staleness_seconds": final_staleness,
            "maximum_runner_probe_interval_seconds": max(
                self.maximum_runner_probe_interval_seconds,
                max(0.0, float(now_monotonic) - float(self.last_probe_monotonic)),
            ),
            "runner_starvation_count": self.runner_starvation_count,
            "actual_source_stalled": self.actual_source_stalled,
            "runner_source_poll_starved": self.runner_source_poll_starved,
            "failure_reason": self.failure_reason,
            "continuity_ok": continuity_ok,
            "last_observed_source_p1_rowid": self.last_observed_source_p1_rowid,
            "last_health_p1_rowid": self.last_health_p1_rowid,
        }

    def canonical_digest(self, now_monotonic: float) -> str:
        return hashlib.sha256(
            json.dumps(
                self.snapshot(now_monotonic),
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()


def _validate_locked_contracts_v03() -> None:
    accepted_v02._validate_locked_contracts_v02()
    if accepted.file_sha256(Path(accepted_v02.__file__).resolve()) != (
        ACCEPTED_V02_HARNESS_SHA256
    ):
        raise RuntimeError("accepted multi-hour v0.2 harness changed")
    expected = (
        MARKET_SOURCE_FINGERPRINT,
        BINDING_FINGERPRINT,
    )
    actual = (
        "9cb094f52bf4b4fe28dc4828b1d9a52a3350cde664c7da5a489fa84e9a4085a9",
        "45521225ccbe6c3b98a48b7e92523deea16d6ff9b91fdd37a415a1a45958babf",
    )
    if expected != actual:
        raise RuntimeError("v0.3 source/binding fingerprint drift")


def _apply_v03_summary_fields(
    summary: dict[str, Any],
    *,
    source: ContinuousMarketSourceV02,
    watchdog: EvidenceBasedSourceContinuityWatchdogV03,
) -> None:
    summary["multihour_model_id"] = MODEL_ID
    summary["multihour_fingerprint"] = MODEL_FINGERPRINT
    summary["spec"] = MULTIHOUR_SPEC
    summary["source_continuity"] = watchdog.snapshot(
        watchdog.last_probe_monotonic or watchdog.last_source_advance_monotonic
    )
    cache_metrics = source.cache_metrics()
    summary["runtime_counts"].update(cache_metrics)
    summary["runtime_counts"]["launch_reconstruction_queries"] = sum(
        cache_metrics[key]
        for key in (
            "full_launch_rebuild_queries",
            "incremental_launch_catchup_queries",
            "rewind_rebuild_queries",
        )
    )
    reason = watchdog.failure_reason
    if reason == "RUNNER_SOURCE_POLL_STARVED":
        summary["stop_reason"] = reason
        summary["errors"] = [
            reason if error == "PRODUCTION_SOURCE_STALLED" else error
            for error in summary["errors"]
        ]
        summary["result_class"] = "FAIL"
    elif reason == "PRODUCTION_SOURCE_STALLED":
        summary["stop_reason"] = reason


def _rewrite_v03_summary(
    summary: dict[str, Any],
    *,
    source: ContinuousMarketSourceV02,
    watchdog: EvidenceBasedSourceContinuityWatchdogV03,
) -> None:
    _apply_v03_summary_fields(summary, source=source, watchdog=watchdog)
    json_path = Path(summary["artifact_paths"]["json"])
    paper_path = Path(summary["artifact_paths"]["paper_db"])
    summary.pop("artifact_sha256", None)
    accepted.smoke.write_json_artifact(json_path, summary)
    summary["artifact_sha256"] = {
        "paper_db": (
            accepted.smoke.sha256_file(paper_path) if paper_path.exists() else None
        ),
        "json": accepted.smoke.sha256_file(json_path),
    }


def run_multihour(*, duration_seconds: int, launch_collector: bool):
    _validate_locked_contracts_v03()
    context: dict[str, Any] = {}

    def source_factory(*args, **kwargs):
        source = ContinuousMarketSourceV02(*args, **kwargs)
        context["source"] = source
        return source

    def watchdog_factory(**kwargs):
        source = context.get("source")
        if source is None:
            raise RuntimeError("source must exist before continuity watchdog")
        watchdog = EvidenceBasedSourceContinuityWatchdogV03(
            source=source,
            session_started_at_utc=accepted.smoke.utc_now(),
            wall_clock=accepted.smoke.utc_now,
            **kwargs,
        )
        context["watchdog"] = watchdog
        return watchdog

    v02_replacements = {
        "MODEL_ID": MODEL_ID,
        "MODEL_FINGERPRINT": MODEL_FINGERPRINT,
        "MULTIHOUR_SPEC": MULTIHOUR_SPEC,
        "BINDING_MODEL_ID": BINDING_MODEL_ID,
        "BINDING_FINGERPRINT": BINDING_FINGERPRINT,
        "ContinuousFirstPullbackBindingV02": ContinuousFirstPullbackBindingV03,
        "_validate_locked_contracts_v02": lambda: None,
    }
    v01_replacements = {
        "ContinuousMarketSourceV01": source_factory,
        "SourceContinuityWatchdog": watchdog_factory,
    }
    v02_originals = {
        name: getattr(accepted_v02, name) for name in v02_replacements
    }
    v01_originals = {name: getattr(accepted, name) for name in v01_replacements}
    original_write_json = accepted.smoke.write_json_artifact

    def write_json_v03(path: Path, payload: dict[str, Any]) -> None:
        source = context.get("source")
        watchdog = context.get("watchdog")
        if source is not None and watchdog is not None:
            _apply_v03_summary_fields(payload, source=source, watchdog=watchdog)
        original_write_json(path, payload)

    try:
        for name, value in v02_replacements.items():
            setattr(accepted_v02, name, value)
        for name, value in v01_replacements.items():
            setattr(accepted, name, value)
        accepted.smoke.write_json_artifact = write_json_v03
        code, summary = accepted_v02.run_multihour(
            duration_seconds=duration_seconds,
            launch_collector=launch_collector,
        )
    finally:
        accepted.smoke.write_json_artifact = original_write_json
        for name, value in v01_originals.items():
            setattr(accepted, name, value)
        for name, value in v02_originals.items():
            setattr(accepted_v02, name, value)
    source = context.get("source")
    watchdog = context.get("watchdog")
    if source is None or watchdog is None:
        raise RuntimeError("v0.3 source/continuity wiring was not exercised")
    _rewrite_v03_summary(summary, source=source, watchdog=watchdog)
    return code, summary


def build_arg_parser():
    return accepted_v02.build_arg_parser()


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
