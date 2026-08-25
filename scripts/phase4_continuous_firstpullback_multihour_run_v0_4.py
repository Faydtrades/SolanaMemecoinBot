from __future__ import annotations

import hashlib
import json
import sys
import time
from pathlib import Path
from typing import Any, Sequence


PROJECT_ROOT = Path(__file__).resolve().parents[1]
for import_root in (PROJECT_ROOT / "src", PROJECT_ROOT / "scripts"):
    if str(import_root) not in sys.path:
        sys.path.insert(0, str(import_root))

import phase4_continuous_firstpullback_multihour_run_v0_3 as accepted_v03  # noqa: E402
from phase4.paper_continuous_firstpullback_binding_v0_4 import (  # noqa: E402
    MODEL_FINGERPRINT as BINDING_FINGERPRINT,
    MODEL_ID as BINDING_MODEL_ID,
    ContinuousFirstPullbackBindingV04,
)
from phase4.paper_continuous_market_source_v0_2 import (  # noqa: E402
    MODEL_FINGERPRINT as MARKET_SOURCE_FINGERPRINT,
)


accepted = accepted_v03.accepted
MODEL_ID = "P4-CONTINUOUS-FIRSTPULLBACK-MULTIHOUR-RUN-0004"
ACCEPTED_V03_HARNESS_SHA256 = (
    "94ad68a8a40a03bc8fe895fd3a0491ce832b809e7884a00b2152dee2380ce090"
)
MULTIHOUR_SPEC = json.loads(json.dumps(accepted_v03.MULTIHOUR_SPEC))
MULTIHOUR_SPEC.update({
    "model_id": MODEL_ID,
    "accepted_multihour_v0_3_sha256": ACCEPTED_V03_HARNESS_SHA256,
    "binding_model_id": BINDING_MODEL_ID,
    "binding_fingerprint": BINDING_FINGERPRINT,
    "timer_fence_scheduler": {
        "fixed_watermark": True,
        "ready_backlog": "ACTIVE_DRAIN_WITHOUT_POLL_SLEEP",
        "source_unavailable": "BOUNDED_POLL_WAIT_REPORTED_SEPARATELY",
        "health_probe": "BEFORE_AND_AFTER_EACH_DRAIN_BATCH",
        "same_watermark_timers": "ALL_COMPLETE_BEFORE_W_PLUS_1",
        "later_watermark_timers": "NO_OVERTAKE",
    },
    "performance_instrumentation": (
        "NORMAL_FETCH_FENCE_DRAIN_HYDRATION_STRATEGY_OUTBOX_CURSOR_TIMER"
    ),
})
MODEL_FINGERPRINT = hashlib.sha256(
    json.dumps(MULTIHOUR_SPEC, sort_keys=True, separators=(",", ":")).encode("utf-8")
).hexdigest()

DEFAULT_DURATION_SECONDS = accepted_v03.DEFAULT_DURATION_SECONDS
MIN_DURATION_SECONDS = accepted_v03.MIN_DURATION_SECONDS
MAX_DURATION_SECONDS = accepted_v03.MAX_DURATION_SECONDS
PRODUCTION_DB = accepted_v03.PRODUCTION_DB
RUNTIME_DIR = accepted_v03.RUNTIME_DIR
parse_duration_seconds = accepted_v03.parse_duration_seconds
run_control_reason = accepted_v03.run_control_reason


def _validate_locked_contracts_v04() -> None:
    accepted_v03._validate_locked_contracts_v03()
    if accepted.file_sha256(Path(accepted_v03.__file__).resolve()) != (
        ACCEPTED_V03_HARNESS_SHA256
    ):
        raise RuntimeError("accepted multi-hour v0.3 harness changed")
    expected = (
        MARKET_SOURCE_FINGERPRINT,
        BINDING_FINGERPRINT,
    )
    actual = (
        "9cb094f52bf4b4fe28dc4828b1d9a52a3350cde664c7da5a489fa84e9a4085a9",
        "e2c35ce63fe419ac4a934913dd26249d72411884cb115e0701f601b276b0f7d6",
    )
    if expected != actual:
        raise RuntimeError("v0.4 source/binding fingerprint drift")


def _observe_batch(metrics, result, batch_seconds: float) -> None:
    metrics.batch_latencies_ms.append(batch_seconds * 1_000.0)
    metrics.source_fetches += 1
    metrics.raw_rows += result.raw_rows_fetched
    metrics.normalized_rows += result.normalized_records
    metrics.deterministic_skips += result.deterministic_skips
    metrics.semantic_events += result.runner_items_committed


def _execute_clock_cycle_v04(
    binding: ContinuousFirstPullbackBindingV04,
    metrics,
    *,
    anchor: int,
    instant,
    timer_id: str,
    watchdog,
) -> int:
    del anchor
    key = binding.prepare_clock_tick(instant, timer_id=timer_id)
    metrics.timers_prepared += 1

    def progress(latest: int) -> None:
        watchdog.observe(latest, time.monotonic())

    def unavailable_wait() -> None:
        watchdog.observe(
            int(watchdog.last_health_p1_rowid),
            time.monotonic(),
        )
        time.sleep(accepted.MARKET_POLL_SECONDS)

    result = binding.drain_and_execute_prepared_timer(
        key,
        batch_size=accepted.BATCH_SIZE,
        progress_probe=progress,
        source_unavailable_wait=unavailable_wait,
        batch_observer=lambda batch, elapsed: _observe_batch(
            metrics, batch, elapsed
        ),
    )
    metrics.timers_executed += len(result.executed_timer_keys)
    return result.requested_watermark_p1_rowid


def _finalize_at_watermark_v04(
    conn,
    binding: ContinuousFirstPullbackBindingV04,
    metrics,
    *,
    anchor: int,
    instant,
    timer_id: str,
    watchdog,
    **_ignored,
) -> int:
    del conn
    watermark = _execute_clock_cycle_v04(
        binding,
        metrics,
        anchor=anchor,
        instant=instant,
        timer_id=timer_id,
        watchdog=watchdog,
    )
    if binding.pending_outbox_count() != 0:
        raise accepted.BindingConflict("pending outbox remains at graceful stop")
    if binding.durable_p1_rowid != watermark:
        raise accepted.BindingConflict(
            "final durable cursor differs from frozen end watermark"
        )
    return watermark


def _apply_v04_summary_fields(
    summary: dict[str, Any],
    *,
    binding: ContinuousFirstPullbackBindingV04,
    watchdog,
) -> None:
    performance = binding.performance_metrics()
    continuity = watchdog.snapshot(
        watchdog.last_probe_monotonic or watchdog.last_source_advance_monotonic
    )
    performance["maximum_runner_probe_interval_seconds"] = continuity[
        "maximum_runner_probe_interval_seconds"
    ]
    summary["multihour_model_id"] = MODEL_ID
    summary["multihour_fingerprint"] = MODEL_FINGERPRINT
    summary["spec"] = MULTIHOUR_SPEC
    summary["runtime_counts"].update(performance)
    summary["runtime_counts"]["timer_fence_wait_count"] = 0
    summary["runtime_counts"]["timer_fence_wait_seconds"] = performance[
        "passive_fence_wait_seconds"
    ]
    summary["performance_breakdown_v0_4"] = performance


def run_multihour(*, duration_seconds: int, launch_collector: bool):
    _validate_locked_contracts_v04()
    context: dict[str, Any] = {}
    original_binding_class = accepted_v03.ContinuousFirstPullbackBindingV03
    original_watchdog_class = accepted_v03.EvidenceBasedSourceContinuityWatchdogV03
    original_apply = accepted_v03._apply_v03_summary_fields
    original_validate = accepted_v03._validate_locked_contracts_v03
    original_model_id = accepted_v03.MODEL_ID
    original_model_fingerprint = accepted_v03.MODEL_FINGERPRINT
    original_spec = accepted_v03.MULTIHOUR_SPEC
    original_binding_id = accepted_v03.BINDING_MODEL_ID
    original_binding_fingerprint = accepted_v03.BINDING_FINGERPRINT
    original_clock_cycle = accepted.smoke._execute_clock_cycle
    original_finalize = accepted.finalize_at_watermark

    class CapturingBindingV04(ContinuousFirstPullbackBindingV04):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            context.setdefault("binding", self)

    def watchdog_factory(*args, **kwargs):
        watchdog = original_watchdog_class(*args, **kwargs)
        context["watchdog"] = watchdog
        return watchdog

    def clock_cycle(binding, metrics, **kwargs):
        watchdog = context.get("watchdog")
        if watchdog is None:
            raise RuntimeError("v0.4 watchdog is unavailable during clock cycle")
        return _execute_clock_cycle_v04(
            binding,
            metrics,
            watchdog=watchdog,
            **kwargs,
        )

    def finalize(conn, binding, metrics, **kwargs):
        watchdog = context.get("watchdog")
        if watchdog is None:
            raise RuntimeError("v0.4 watchdog is unavailable during final drain")
        return _finalize_at_watermark_v04(
            conn,
            binding,
            metrics,
            watchdog=watchdog,
            **kwargs,
        )

    def apply(summary, *, source, watchdog):
        original_apply(summary, source=source, watchdog=watchdog)
        binding = context.get("binding")
        if binding is None:
            raise RuntimeError("v0.4 binding was not captured")
        _apply_v04_summary_fields(summary, binding=binding, watchdog=watchdog)

    try:
        accepted_v03.ContinuousFirstPullbackBindingV03 = CapturingBindingV04
        accepted_v03.EvidenceBasedSourceContinuityWatchdogV03 = watchdog_factory
        accepted_v03._apply_v03_summary_fields = apply
        accepted_v03._validate_locked_contracts_v03 = lambda: None
        accepted_v03.MODEL_ID = MODEL_ID
        accepted_v03.MODEL_FINGERPRINT = MODEL_FINGERPRINT
        accepted_v03.MULTIHOUR_SPEC = MULTIHOUR_SPEC
        accepted_v03.BINDING_MODEL_ID = BINDING_MODEL_ID
        accepted_v03.BINDING_FINGERPRINT = BINDING_FINGERPRINT
        accepted.smoke._execute_clock_cycle = clock_cycle
        accepted.finalize_at_watermark = finalize
        code, summary = accepted_v03.run_multihour(
            duration_seconds=duration_seconds,
            launch_collector=launch_collector,
        )
    finally:
        accepted.finalize_at_watermark = original_finalize
        accepted.smoke._execute_clock_cycle = original_clock_cycle
        accepted_v03.BINDING_FINGERPRINT = original_binding_fingerprint
        accepted_v03.BINDING_MODEL_ID = original_binding_id
        accepted_v03.MULTIHOUR_SPEC = original_spec
        accepted_v03.MODEL_FINGERPRINT = original_model_fingerprint
        accepted_v03.MODEL_ID = original_model_id
        accepted_v03._validate_locked_contracts_v03 = original_validate
        accepted_v03._apply_v03_summary_fields = original_apply
        accepted_v03.EvidenceBasedSourceContinuityWatchdogV03 = original_watchdog_class
        accepted_v03.ContinuousFirstPullbackBindingV03 = original_binding_class
    return code, summary


def build_arg_parser():
    return accepted_v03.build_arg_parser()


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
