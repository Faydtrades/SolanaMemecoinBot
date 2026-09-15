"""Finite observed-fit guards, separated from T010's unchanged safety rules.

The complete measured boundary is an explicit, hash-bound input. The rule adds
one measured full repeat range, then rounds upward only to the native unit.
It is an observed envelope with fail-closed enforcement, not a latency promise.
"""
from __future__ import annotations
import hashlib
from pathlib import Path

from .authority_controls_v0_1 import require
from phase5.shadow_domain_v0_1 import content_fingerprint

SCENARIOS = ("protocol98_pending", "four_hot73_hold", "hottest512_pending", "active64_hold")
RESOURCE_METRICS = ("STARTUP_US", "HOST_RSS_BYTES", "PROTECTIVE_STEP_US", "OLDEST_UNCONSUMED_AGE_US")
TIMING_METRICS = ("resource_max_age_us", "recovery_evidence_max_age_us")
HOST_METRICS = ("supervisor_startup_us", "supervisor_progress_us", "supervisor_termination_us",
    "child_private_bytes", "supervisor_private_bytes", "supervisor_rss_bytes")
SCENARIO_METRICS = {key: RESOURCE_METRICS+TIMING_METRICS+("child_private_bytes",) for key in SCENARIOS}
SCENARIO_METRICS.update(supervisor=RESOURCE_METRICS+TIMING_METRICS+HOST_METRICS,
    public_response=("HOST_RSS_BYTES", "child_private_bytes"))


def process_metrics(result, scenario):
    metrics = result.get("metrics")
    require(type(metrics) is dict and set(metrics) == set(SCENARIO_METRICS[scenario]),
        "T010_RESOURCE_COMPLETE_PER_SCENARIO_METRICS_REQUIRED")
    for key, value in metrics.items():
        require(type(value) is int and value >= 0 or key == "OLDEST_UNCONSUMED_AGE_US"
            and value is None and result.get("empty_backlog_verified") is True,
            "T010_RESOURCE_PROCESS_METRICS_REQUIRED")
    return metrics


def runtime_code_digest():
    root = Path(__file__).resolve().parents[1]
    return content_fingerprint(tuple((str(path.relative_to(root)).replace("\\", "/"),
        hashlib.sha256(path.read_bytes()).hexdigest()) for folder in ("live", "phase4", "phase5")
        for path in sorted((root/folder).glob("*.py"))))


def logical_digest(derivation):
    # All architectural and cumulative limits stay bound. Only outputs that
    # are measured independently, or references to those outputs, are omitted.
    return content_fingerprint({k: v for k, v in derivation.items()
        if k not in ("resource_limits", "measured_guards")})


def repeated_guard(values, unit):
    require(type(values) is list and len(values) >= 3
        and all(type(value) is int and value >= 0 for value in values)
        and type(unit) is int and unit > 0, "T010_RESOURCE_REPEATED_MEASUREMENT_REQUIRED")
    upper, lower = max(values), min(values)
    padding = max(upper-lower, unit)
    return ((upper+padding+unit-1)//unit)*unit


def derive_guards(measured, derivation, original_monitor):
    """Recompute a frozen measurement's entire invalidated guard set at once.

    Individual point results cannot be appended as new policy. The validation
    certificate must contain all four structural scenarios and the actual
    supervisor/public-response boundary, followed by separate quota validation.
    """
    from .runtime_dry_profile_v0_1 import read_reference
    keys = {"schema", "scope", "logical_digest", "runtime_code_digest", "native_digest",
        "measurement_boundary", "series", "units", "unchanged_safety", "limitations"}
    require(type(measured) is dict and set(measured) == keys,
        "T010_RESOURCE_FULL_MEASUREMENT_REQUIRED")
    require(measured["schema"] == "MEME_LIVE_T010_MEASURED_RESOURCE_GUARDS_V1"
        and measured["scope"] == "T010_DRY_NO_BROADCAST_NON_SUBMITTED"
        and measured["logical_digest"] == logical_digest(derivation)
        and measured["runtime_code_digest"] == runtime_code_digest(),
        "T010_RESOURCE_MEASUREMENT_CODE_OR_WORK_CONFLICT")
    require(measured["unchanged_safety"] == {"source_rows": 10000, "source_freshness_seconds": 30,
        "clock_policy": "EXACT_OWNER_APPROVED_T010_RECORD", "entry_deadlines": "ORIGINAL",
        "lifetime_us": 50400000000, "reserve_floor": "ORIGINAL"},
        "T010_RESOURCE_SAFETY_RELAXATION_DENIED")
    boundary = read_reference(measured["measurement_boundary"])
    require(boundary.get("schema") == "MEME_LIVE_T010_FULL_PROCESS_MEASUREMENT_BOUNDARY_V1"
        and boundary.get("logical_digest") == measured["logical_digest"]
        and boundary.get("runtime_code_digest") == measured["runtime_code_digest"]
        and boundary.get("native_digest") == measured["native_digest"]
        and boundary.get("series") == measured["series"]
        and boundary.get("unresolved_constraints") == [], "T010_RESOURCE_MEASUREMENT_BOUNDARY_CONFLICT")
    required = set(RESOURCE_METRICS+TIMING_METRICS+HOST_METRICS)
    require(set(measured["series"]) == required and set(measured["units"]) == required,
        "T010_RESOURCE_ALL_INVALIDATED_DIMENSIONS_REQUIRED")
    # Per-scenario originals are retained and hashed; final boundary review
    # inspects these records, including exact state shape and public buffers.
    require(set(boundary.get("scenarios", {})) == set(SCENARIOS)
        and len(boundary.get("supervisor_runs", [])) >= 3
        and len(boundary.get("public_response_runs", [])) >= 3,
        "T010_RESOURCE_COMPLETE_SCENARIO_SET_REQUIRED")
    collected = {key: [] for key in required}
    instances = set()
    cohorts = [(key, boundary["scenarios"][key]) for key in SCENARIOS]
    cohorts += [("supervisor", boundary["supervisor_runs"]), ("public_response", boundary["public_response_runs"])]
    for scenario, records in cohorts:
        require(type(records) is list and len(records) >= 3,
            "T010_RESOURCE_FRESH_PROCESS_REPEATS_REQUIRED")
        for reference in records:
            result = read_reference(reference)
            require(result.get("runtime_code_digest") == measured["runtime_code_digest"]
                and result.get("native_digest") == measured["native_digest"]
                and type(result.get("exit_code")) is int and result["exit_code"] == 0
                and result.get("fresh_process") is True and result.get("scenario") == scenario,
                "T010_RESOURCE_ORIGINAL_PROCESS_RESULT_REQUIRED")
            instance = result.get("process_instance_digest")
            require(type(instance) is str and len(instance) == 64
                and all(c in "0123456789abcdef" for c in instance) and instance not in instances,
                "T010_RESOURCE_DISTINCT_PROCESS_OBSERVATIONS_REQUIRED")
            instances.add(instance)
            metrics = process_metrics(result, scenario)
            for key, value in metrics.items():
                if value is not None:
                    collected[key].append(value)
    require(collected == measured["series"], "T010_RESOURCE_MEASUREMENT_SERIES_CONFLICT")
    guards = {}
    for metric in sorted(required):
        unit = measured["units"][metric]
        require(unit == (4096 if metric.endswith("BYTES") or metric.endswith("bytes") else 1),
            "T010_RESOURCE_NATIVE_ROUNDING_REQUIRED")
        value = repeated_guard(measured["series"][metric], unit)
        old = derivation["resource_limits"].get(metric, original_monitor.get(metric, 0))
        # A still-sufficient original guard is preserved exactly. Raising one
        # is permitted only when an observation actually disproves that guard.
        guards[metric] = old if old >= max(measured["series"][metric]) and old > 0 else value
    require(all(guards[key] < derivation["lifetime"]["hard_expiry_us"] for key in
        ("STARTUP_US", "PROTECTIVE_STEP_US", *TIMING_METRICS,
         "supervisor_startup_us", "supervisor_progress_us", "supervisor_termination_us")),
        "T010_RESOURCE_MEASUREMENT_EXCEEDS_SESSION")
    return guards
