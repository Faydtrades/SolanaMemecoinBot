"""Finite original DRY work and joint resource derivation for T010.

This is a capacity contract, never a promise that arbitrary market data yields
two candidates. An exhausted segment cannot qualify or renew its allowance.
Physical coverage is a separate, hash-bound prerequisite; point measurements
cannot turn the analytical limits below into a deployment qualification.
"""
from __future__ import annotations

import json
from dataclasses import asdict, fields, is_dataclass
from pathlib import Path

from phase5.shadow_domain_v0_1 import canonical_json, content_fingerprint
from .authority_controls_v0_1 import require
from .continuous_producer_v0_2 import STORAGE_FINGERPRINT, accepted
from .ledger_repository_v0_1 import COMMIT_VERSION, _COMMIT_KINDS

SCHEMA = "MEME_LIVE_T010_RESOURCE_ENVELOPE_V1"
SCOPE = "PUBLIC_ENVIRONMENT_REVIEWED"
OBSERVATION_US = 12*60*60*1000000
HARD_EXPIRY_US = 14*60*60*1000000
POLL_US = 50000
RSS_CONTRACT = "https://learn.microsoft.com/en-us/windows/win32/api/memoryapi/nf-memoryapi-setprocessworkingsetsizeex"
_PRIVATE_JOBS = []
OBSERVATION_POLICY = "C2_OBSERVATION_ONLY_V1"
OBSERVATION_METRICS = ("STARTUP_US", "parent_first_legal_unit_us", "HOST_RSS_BYTES",
    "child_private_bytes", "PROTECTIVE_STEP_US", "first_runtime_step_us",
    "resource_max_age_us", "recovery_evidence_max_age_us")


def _keys(value, expected, reason):
    require(type(value) is dict and set(value) == set(expected), reason)


def observation_only(envelope):
    """Explicit opt-in; an existing numerical envelope cannot silently downgrade."""
    policy = envelope.get("observation_policy")
    if policy is None:
        require("observation_evidence" not in envelope, "T010_OBSERVATION_POLICY_REQUIRED")
        return False
    require(policy == OBSERVATION_POLICY and envelope.get("observation_evidence") is not None
        and envelope.get("measured_guard_evidence") is None
        and "measured_guards" not in envelope["derivation"]
        and not set(OBSERVATION_METRICS).intersection(envelope["derivation"]["resource_limits"]),
        "T010_OBSERVATION_ENFORCEMENT_CONFLICT")
    return True


def validate_observations(reference):
    """Validate retained fresh-process evidence, without deriving numerical limits."""
    from .runtime_dry_profile_v0_1 import read_reference
    from .t010_resource_measurement_v0_1 import SCENARIOS, process_metrics, runtime_code_digest
    evidence = read_reference(reference)
    _keys(evidence, ("schema", "runtime_code_digest", "native_digest", "scenarios", "series"),
        "T010_OBSERVATION_EVIDENCE_REQUIRED")
    require(evidence["schema"] == "MEME_LIVE_T010_RESOURCE_OBSERVATIONS_V1"
        and evidence["runtime_code_digest"] == runtime_code_digest()
        and set(evidence["scenarios"]) == set(SCENARIOS), "T010_OBSERVATION_IDENTITY_CONFLICT")
    series = {key: [] for key in OBSERVATION_METRICS}
    instances = set()
    for scenario in sorted(SCENARIOS):
        references = evidence["scenarios"][scenario]
        require(type(references) is list and len(references) == 3, "T010_OBSERVATION_REPETITIONS_REQUIRED")
        for ref in references:
            value = read_reference(ref)
            require(value.get("scenario") == scenario
                and value["runtime_code_digest"] == evidence["runtime_code_digest"]
                and value["native_digest"] == evidence["native_digest"]
                and value["exit_code"] == 0 and value["fresh_process"] is True
                and value["unresolved_constraints"] == [], "T010_OBSERVATION_RESULT_CONFLICT")
            identity = value["process_instance_digest"]
            require(type(identity) is str and len(identity) == 64
                and all(c in "0123456789abcdef" for c in identity) and identity not in instances,
                "T010_OBSERVATION_DISTINCT_PROCESS_REQUIRED")
            instances.add(identity)
            metrics = process_metrics(value, scenario)
            original = read_reference(value["original_result"])
            require(original["process_instance"]["digest"] == identity
                and original["native_digest"] == evidence["native_digest"]
                and original["runtime_code_digest"] == evidence["runtime_code_digest"],
                "T010_OBSERVATION_ORIGINAL_RESULT_CONFLICT")
            values = {key: metrics[key] for key in OBSERVATION_METRICS
                if key not in ("parent_first_legal_unit_us", "first_runtime_step_us")}
            values.update(parent_first_legal_unit_us=original["parent_boundary"]["spawn_to_first_legal_unit_us"],
                first_runtime_step_us=original["first_step_us"])
            require(all(type(v) is int and v >= 0 for v in values.values()), "T010_OBSERVATION_VALUE_REQUIRED")
            for key, value in values.items():
                series[key].append(value)
    require(evidence["series"] == series, "T010_OBSERVATION_SERIES_CONFLICT")
    return evidence


def enforce_current_process_rss(limit):
    """Set and verify this isolated Windows process's original hard RSS limit.

    No other process handle, global setting or startup installation is involved.
    Paging/allocation/disk/timing remain independently bounded and qualified.
    """
    import ctypes
    import os
    from ctypes import wintypes as w
    require(os.name == "nt" and type(limit) is int and limit > 0, "T010_NATIVE_RSS_LIMIT_REQUIRED")
    api = ctypes.WinDLL("kernel32", use_last_error=True)
    api.GetCurrentProcess.restype = w.HANDLE
    api.GetProcessWorkingSetSizeEx.argtypes = [w.HANDLE, ctypes.POINTER(ctypes.c_size_t),
        ctypes.POINTER(ctypes.c_size_t), ctypes.POINTER(w.DWORD)]
    api.GetProcessWorkingSetSizeEx.restype = w.BOOL
    api.SetProcessWorkingSetSizeEx.argtypes = [w.HANDLE, ctypes.c_size_t, ctypes.c_size_t, w.DWORD]
    api.SetProcessWorkingSetSizeEx.restype = w.BOOL
    minimum, maximum, flags = ctypes.c_size_t(), ctypes.c_size_t(), w.DWORD()
    process = api.GetCurrentProcess()
    require(api.GetProcessWorkingSetSizeEx(process, ctypes.byref(minimum), ctypes.byref(maximum), ctypes.byref(flags)),
        "T010_NATIVE_RSS_CURRENT_QUOTA_UNAVAILABLE")
    require(minimum.value <= limit, "T010_NATIVE_RSS_MINIMUM_CONFLICT")
    required_flags = (flags.value & ~0x8) | 0x4  # HARDWS_MAX_DISABLE -> HARDWS_MAX_ENABLE
    require(api.SetProcessWorkingSetSizeEx(process, minimum.value, limit, required_flags),
        "T010_NATIVE_RSS_HARD_LIMIT_UNAVAILABLE")
    require(api.GetProcessWorkingSetSizeEx(process, ctypes.byref(minimum), ctypes.byref(maximum), ctypes.byref(flags))
        and maximum.value == limit and flags.value & 0x4 and not flags.value & 0x8,
        "T010_NATIVE_RSS_HARD_LIMIT_NOT_VERIFIED")
    return {"maximum_bytes":maximum.value, "minimum_bytes":minimum.value,
        "flags":flags.value, "contract":RSS_CONTRACT, "process":"CURRENT_ISOLATED_PROCESS_ONLY"}


def bound_inputs(inputs):
    # The profile binds the envelope; the envelope binds every independent
    # profile input. Only its own reference and its derived output are removed.
    result = json.loads(canonical_json(inputs))
    result.pop("resource_envelope", None)
    result["monitor"].pop("resource_limits", None)
    return content_fingerprint(result)


def enforce_current_process_private_bytes(limit):
    """Cap only this isolated child/qualifier using the existing Win32 Job API.

    Keep the anonymous non-inheritable handle until process exit. No named job,
    outside process or parent job is changed, and no breakaway flag is enabled.
    Failure to attach or verify is fail-closed before reconstruction starts.
    """
    import ctypes
    from .operations_windows_host_v0_1 import kernel, _Limits
    require(type(limit) is int and limit > 0, "T010_PRIVATE_MEMORY_FINITE_LIMIT_REQUIRED")
    if _PRIVATE_JOBS:
        require(_PRIVATE_JOBS[0][1] == limit, "T010_PRIVATE_MEMORY_LIMIT_REBIND_DENIED")
        return {"private_bytes": limit, "scope": "CURRENT_ISOLATED_PROCESS_ONLY"}
    api = kernel()
    job = api.CreateJobObjectW(None, None)
    require(bool(job), "T010_PRIVATE_MEMORY_JOB_UNAVAILABLE")
    try:
        limits = _Limits()
        limits.BasicLimitInformation.LimitFlags = 0x100  # JOB_OBJECT_LIMIT_PROCESS_MEMORY
        limits.ProcessMemoryLimit = limit
        require(api.SetInformationJobObject(job, 9, ctypes.byref(limits), ctypes.sizeof(limits))
            and api.AssignProcessToJobObject(job, api.GetCurrentProcess()),
            "T010_PRIVATE_MEMORY_CONTAINMENT_UNAVAILABLE")
        verified = _Limits()
        require(api.QueryInformationJobObject(job, 9, ctypes.byref(verified), ctypes.sizeof(verified), None)
            and verified.BasicLimitInformation.LimitFlags & 0x100 and verified.ProcessMemoryLimit == limit,
            "T010_PRIVATE_MEMORY_CONTAINMENT_UNVERIFIED")
        _PRIVATE_JOBS.append((job, limit))
    except Exception:
        api.CloseHandle(job)
        raise
    return {"private_bytes": limit, "scope": "CURRENT_ISOLATED_PROCESS_ONLY"}


def enforce_host_tree_private_bytes(boundary, guards):
    """Add finite memory limits to this host's already-owned containment job."""
    import ctypes
    from .operations_windows_host_v0_1 import _Limits
    require(boundary.assigned and boundary.job, "T010_RESOURCE_OWNED_HOST_JOB_REQUIRED")
    child, parent = guards["child_private_bytes"], guards["supervisor_private_bytes"]
    require(all(type(n) is int and n > 0 for n in (child, parent)), "T010_RESOURCE_HOST_MEMORY_LIMIT_REQUIRED")
    value = _Limits()
    api = boundary.api
    require(api.QueryInformationJobObject(boundary.job, 9, ctypes.byref(value), ctypes.sizeof(value), None),
        "T010_RESOURCE_HOST_JOB_LIMITS_UNAVAILABLE")
    require(value.BasicLimitInformation.LimitFlags & 0x2000
        and not value.BasicLimitInformation.LimitFlags & (0x800 | 0x1000),
        "T010_RESOURCE_ORIGINAL_JOB_CONTAINMENT_REQUIRED")
    value.BasicLimitInformation.LimitFlags |= 0x100 | 0x200
    value.ProcessMemoryLimit = max(child, parent)
    value.JobMemoryLimit = child+parent
    require(api.SetInformationJobObject(boundary.job, 9, ctypes.byref(value), ctypes.sizeof(value)),
        "T010_RESOURCE_HOST_JOB_MEMORY_LIMIT_UNAVAILABLE")
    actual = _Limits()
    require(api.QueryInformationJobObject(boundary.job, 9, ctypes.byref(actual), ctypes.sizeof(actual), None)
        and actual.BasicLimitInformation.LimitFlags == value.BasicLimitInformation.LimitFlags
        and actual.ProcessMemoryLimit == value.ProcessMemoryLimit and actual.JobMemoryLimit == child+parent,
        "T010_RESOURCE_HOST_JOB_MEMORY_LIMIT_UNVERIFIED")
    return {"process_private_bytes": max(child, parent), "tree_private_bytes": child+parent}


def derive(inputs, accepted_profile, extension):
    """Recompute limits from existing architectural caps, never observed maxima.

    Each segment may reserve one existing finite source-profile window. Partial
    final batches retain the original dispatch ceiling. Empty pages consume the
    full reservation too. Qualification retains its explicit driver step cap;
    the host additionally allows two terminal units and one recovery unit for
    each existing finite startup allowance. Nothing renews at the 1s A1 window.
    """
    original = accepted_profile["constructor_configuration"]
    p, dispatch = original["continuation_profile"], original["dispatch"]
    source = inputs["source_start"]["profile"]
    require(source == accepted_profile["source_configuration"]["profile"],
        "T010_RESOURCE_ORIGINAL_SOURCE_PROFILE_REQUIRED")
    require(all(type(v) is int and v > 0 for v in p.values()), "T010_RESOURCE_FINITE_PRODUCER_REQUIRED")
    rows, batch = source["max_rows_per_table"], dispatch["batch_rows"]
    require(0 < batch <= p["batch_rows"] and dispatch["page_rows"] <= p["delivery_page_rows"],
        "T010_RESOURCE_ORIGINAL_DISPATCH_REQUIRED")
    starts = original["restart_profile"]["max_attempts"]
    qsteps = inputs["driver"]["max_steps"]
    require(type(qsteps) is int and 1 <= qsteps <= 1000000 and type(starts) is int and starts > 0,
        "T010_RESOURCE_FINITE_SEGMENT_REQUIRED")
    source_units = (rows+batch-1)//batch
    target = 2
    work = {"max_startups":starts, "max_runtime_steps":source_units+target+starts,
        "target_dry_terminals":target}
    total_steps = qsteps+work["max_runtime_steps"]
    # A winner retires its mint in the same original producer transaction.
    # The original role loop breaks on the first SOL_NATIVE nongap winner, and
    # the durable mint lock suppresses all later winners. One retained candidate
    # therefore needs one launch AND one retired bundle, never four new roots.
    candidates = p["history_rows"]//2
    initial_commits, handoff_controls, stopped_reconcile = 3, 2, 1
    source_units_total = min(qsteps, source_units)+source_units
    # Every decision pops the original root; its durable nonacceptance or
    # admission survives cold queue reconstruction. Source observations do not
    # imply a new admission record when there is no original candidate.
    root_decisions = min(source_units_total, candidates)
    admitted_roots = 2*target
    ledger_rows = initial_commits+handoff_controls+candidates+root_decisions+3*admitted_roots+stopped_reconcile
    generations = 1+2+starts+1+1  # initialization, qualification, host, handoff, stopped reconcile
    # Closed commit schema. All public record keys are ASCII <=256 bytes;
    # attempt-stage ordinal keys are shorter within this finite step allowance.
    envelope = {"version":COMMIT_VERSION, "economic_domain_id":"f"*64,
        "sequence":ledger_rows, "kind":max(_COMMIT_KINDS, key=len), "record_key":"x"*256,
        "record_digest":"f"*64, "generation":generations,
        "generation_digest":"f"*64, "previous_digest":"f"*64}
    per_commit = len(canonical_json(envelope).encode())
    limits = dict(extension["limits"])
    limits.update(AGGREGATE_FEATURE_BYTES=p["retained_bytes"], HOTTEST_FEATURE_BYTES=p["hot_mint_bytes"],
        UNFINISHED_MINTS=p["active_mints"], NO_T0_MINTS=p["active_mints"],
        IDENTITY_ROWS=p["history_rows"], TOMBSTONE_ROWS=p["history_rows"]//2,
        PRODUCER_PENDING_ROWS=p["pending_rows"], PRODUCER_PENDING_BYTES=p["pending_bytes"],
        RETAINED_EVENTS=p["retained_events"], HOTTEST_EVENTS=p["hot_mint_events"],
        RETAINED_SERIALIZED_BYTES=p["retained_bytes"], HISTORY_BYTES=p["history_bytes"],
        CHECKPOINT_BYTES=p["checkpoint_bytes"], LEDGER_TAIL_ROWS=ledger_rows,
        LEDGER_TAIL_PAYLOAD_BYTES=ledger_rows*per_commit)
    # Historical observed-fit guards remain here until the separately bound
    # full-workload measurement is supplied. They are not safety invariants.
    # Source freshness, original deadlines and reserve floors remain independent.
    return {"work_budget":work, "resource_limits":limits,
        "lifetime":{"observation_us":OBSERVATION_US, "hard_expiry_us":HARD_EXPIRY_US},
        "qualification":{"max_startups":2, "max_runtime_steps":qsteps,
            "target_dry_terminals":2, "max_admitted_roots":target, "max_source_rows":rows},
        "host":{"max_source_rows":rows, "max_source_units":source_units, "max_admitted_roots":target},
        "source_unit_raw_bytes":p["record_bytes"],
        # Explicit T010 allocation: each retained source verdict receives one
        # existing bounded-record unit. This is not an inherited source-codec
        # limit. Both progress and the enclosing full verdict must fit.
        "source_record_bytes":p["record_bytes"],
        "joint":{"launch_plus_retired_rows":p["history_rows"], "maximum_candidate_roots":candidates},
        "cumulative":{"initial_ledger_commits":initial_commits, "handoff_control_commits":handoff_controls,
            "stopped_reconcile_commits":stopped_reconcile, "ledger_generations":generations,
            "ledger_commit_payload_bytes_per_row":per_commit,
            "source_observations":1+source_units_total, "source_units":source_units_total,
            "source_history_payload_bytes":(1+source_units_total)*p["record_bytes"],
            "root_decisions":root_decisions,
            "admitted_roots":admitted_roots,
            "supervisor_polls":HARD_EXPIRY_US//POLL_US+1,
            "runtime_monitor_observations":3*total_steps,
            "host_process_records":starts},
        "producer_profile":p, "dispatch":dispatch, "producer_storage_fingerprint":STORAGE_FINGERPRINT,
        "physical_validation_required":["ALL_FIVE_SQLITE_MAIN_WAL_SHM_JOURNAL_AND_FREELIST_PEAKS",
            "LEDGER_ALL_TABLES_AND_WRITER_GENERATIONS", "OPERATIONS_CONDITIONS_AND_RECEIPTS",
            "SOURCE_EVIDENCE_HISTORY", "HOST_MANIFEST_AND_RESERVATION_FILES",
            "ORIGINAL_COLD_REPLAY_RSS_AND_STARTUP", "PUBLIC_RESPONSE_TRANSIENT_RSS",
            "WORST_STATE_PROTECTIVE_JOURNAL_LOCK_BUDGET", "JOINT_STATE_BOUNDARY_SCENARIOS"]}


def validate(inputs, *, require_physical=True):
    from .runtime_dry_profile_v0_1 import read_reference
    record = read_reference(inputs["resource_envelope"])
    _keys(record, {"schema", "scope", "bound_inputs_digest", "derivation", "physical_evidence",
        "authorization_reference", "project_acceptance_claimed"} | ({"measured_guard_evidence",
        "observation_policy", "observation_evidence"} & record.keys()),
        "T010_RESOURCE_COMPLETE_ENVELOPE_REQUIRED")
    require(record["schema"] == SCHEMA and record["scope"] == SCOPE
        and inputs["qualification_substitutions"]["scope"] == SCOPE
        and record["bound_inputs_digest"] == bound_inputs(inputs)
        and type(record["authorization_reference"]) is str and bool(record["authorization_reference"].strip())
        and record["project_acceptance_claimed"] is False,
        "T010_RESOURCE_EXACT_PUBLIC_INPUT_BINDING_REQUIRED")
    calculated = derive(inputs, read_reference(inputs["accepted_profile"]), read_reference(inputs["accepted_extension"]))
    observing = observation_only(record)
    if observing:
        validate_observations(record["observation_evidence"])
        # Keep architectural ceilings and independent source/freshness contracts.
        # Empirical maxima are evidence, not admission thresholds.
        for key in OBSERVATION_METRICS:
            calculated["resource_limits"].pop(key, None)
    elif record.get("measured_guard_evidence") is not None:
        from .t010_resource_measurement_v0_1 import derive_guards, RESOURCE_METRICS
        original = read_reference(inputs["accepted_monitor"])["monitor"]
        monitor_values = {"resource_max_age_us": original["resource_max_age_us"],
            "recovery_evidence_max_age_us": original["policy"]["recovery_evidence_max_age_us"]}
        measured = read_reference(record["measured_guard_evidence"])
        guards = derive_guards(measured, calculated, monitor_values)
        calculated["resource_limits"].update({key: guards[key] for key in RESOURCE_METRICS})
        calculated["measured_guards"] = guards
        require(inputs["monitor"]["resource_max_age_us"] == guards["resource_max_age_us"]
            and inputs["monitor"]["recovery_evidence_max_age_us"] == guards["recovery_evidence_max_age_us"]
            and inputs["monitor"]["protective_qualification_digest"] == record["measured_guard_evidence"]["sha256"],
            "T010_RESOURCE_MONITOR_MEASUREMENT_CONFLICT")
        if record.get("physical_evidence") is not None:
            physical = read_reference(record["physical_evidence"])
            environment = read_reference(physical["environment"])
            calculated["resource_limits"]["HOST_DISK_RESERVE_BYTES"] = environment["requirements"][
                environment["volumes"]["owned"]["volume_id"]]
    require(record["derivation"] == calculated, "T010_RESOURCE_DERIVATION_CONFLICT")
    require(inputs["guard_amendment"] is None, "T010_RESOURCE_SNAPSHOT_AMENDMENT_DENIED")
    require(inputs["monitor"]["resource_limits"] == calculated["resource_limits"],
        "T010_RESOURCE_LIMITS_CONFLICT")
    if require_physical:
        require(record["physical_evidence"] is not None, "T010_RESOURCE_MAXIMUM_STATE_PHYSICAL_EVIDENCE_REQUIRED")
        physical = read_reference(record["physical_evidence"])
        validate_physical(physical, record, calculated, inputs=inputs)
    return record


def validate_physical(physical, record, calculated, *, inputs=None):
    """Require full derivation coverage; an ordinary point-run is ineligible.

    This deliberately does not manufacture a physical proof. The retained
    evidence must identify the independent maximum-state validation artifact,
    including a physical size model that accounts for each growing store.
    """
    _keys(physical, ("schema", "scope", "bound_inputs_digest", "derivation_digest", "coverage",
        "physical_size_model", "boundary_evidence", "host_identity_digest", "runtime_code_digest",
        "maxima", "disk_reserve_minimum_bytes", "environment", "unresolved_constraints"),
        "T010_RESOURCE_PHYSICAL_COVERAGE_REQUIRED")
    require(physical["schema"] == "MEME_LIVE_T010_MAXIMUM_STATE_RESOURCE_VALIDATION_V1"
        and physical["scope"] == "ISOLATED_ORIGINAL_BOUNDARY_VALIDATION"
        and physical["bound_inputs_digest"] == record["bound_inputs_digest"]
        and physical["derivation_digest"] == content_fingerprint(calculated)
        and physical["coverage"] == calculated["physical_validation_required"]
        and physical["unresolved_constraints"] == [], "T010_RESOURCE_PHYSICAL_COVERAGE_INCOMPLETE")
    from .runtime_dry_profile_v0_1 import read_reference
    # Concrete retained artifacts are mandatory. Their hashes are not replaced
    # by booleans or a measurement taken at an arbitrary smaller workload.
    model = read_reference(physical["physical_size_model"])
    boundary = read_reference(physical["boundary_evidence"])
    require(model.get("derivation_digest") == physical["derivation_digest"]
        and boundary.get("derivation_digest") == physical["derivation_digest"]
        and model.get("scope") == "ALL_GROWABLE_STORES_PHYSICAL_UPPER_BOUND"
        and boundary.get("scope") == "ORIGINAL_MAXIMUM_STATE_BOUNDARY_SCENARIOS",
        "T010_RESOURCE_BOUNDARY_ARTIFACT_BINDING_REQUIRED")
    from .t010_resource_measurement_v0_1 import runtime_code_digest, SCENARIOS, process_metrics
    from .t010_resource_environment_v0_1 import validate_binding, constructor_input_references
    from .t010_physical_certificate_v0_1 import validate_model
    observing = observation_only(record)
    require(inputs is not None and (observing or record.get("measured_guard_evidence") is not None),
        "T010_RESOURCE_FULL_MEASURED_INPUTS_REQUIRED")
    environment = read_reference(physical["environment"])
    validate_model(model, calculated, environment)
    # Loading a profile must remain possible for an original pending cold
    # recovery. Capacity is checked before fresh initialization/first launch
    # and before ordinary work, not before ownership has been reconstructed.
    validate_binding(environment, store_paths=inputs["store_paths"], check_reserve=False,
        immutable_references=constructor_input_references(inputs))
    measured = (validate_observations(record["observation_evidence"]) if observing
        else read_reference(record["measured_guard_evidence"]))
    require(physical["runtime_code_digest"] == runtime_code_digest() == measured["runtime_code_digest"]
        and physical["host_identity_digest"] == read_reference(inputs["accepted_extension"])["host_identity_digest"]
        and content_fingerprint(environment["native"]) == measured["native_digest"]
        and physical["maxima"] == {k: max(v) for k, v in measured["series"].items()}
        and physical["disk_reserve_minimum_bytes"] == calculated["resource_limits"]["HOST_DISK_RESERVE_BYTES"],
        "T010_RESOURCE_MEASURED_PHYSICAL_BINDING_CONFLICT")
    if observing:
        require(boundary.get("schema") == "MEME_LIVE_T010_OBSERVED_RESOURCE_BOUNDARY_V1"
            and boundary.get("runtime_code_digest") == physical["runtime_code_digest"]
            and boundary.get("measurement") == record["observation_evidence"]
            and boundary.get("scenarios") == measured["scenarios"]
            and boundary.get("unresolved_constraints") == []
            and "under_limits" not in boundary,
            "T010_RESOURCE_OBSERVATION_BOUNDARY_CONFLICT")
        # All identity, analytical size, native environment and volume bindings
        # above still apply. No nonexistent quota or under-quota run is claimed.
        return
    require(boundary.get("schema") == "MEME_LIVE_T010_QUALIFIED_RESOURCE_BOUNDARY_V1"
        and boundary.get("runtime_code_digest") == physical["runtime_code_digest"]
        and boundary.get("measurement") == record["measured_guard_evidence"]
        and boundary.get("unresolved_constraints") == []
        and set(boundary.get("under_limits", {})) == set(SCENARIOS) | {"supervisor", "public_response"},
        "T010_RESOURCE_UNDER_LIMITS_BOUNDARY_REQUIRED")
    guards = calculated["measured_guards"]
    calibration = read_reference(measured["measurement_boundary"])
    instances = {read_reference(reference)["process_instance_digest"] for references in (
        *calibration["scenarios"].values(), calibration["supervisor_runs"], calibration["public_response_runs"])
        for reference in references}
    for scenario, references in boundary["under_limits"].items():
        require(type(references) is list and len(references) >= 3, "T010_RESOURCE_CONTAINED_REPEATS_REQUIRED")
        for reference in references:
            result = read_reference(reference)
            require(result.get("scenario") == scenario and type(result.get("exit_code")) is int and result["exit_code"] == 0
                and result.get("fresh_process") is True
                and result.get("runtime_code_digest") == physical["runtime_code_digest"]
                and result.get("native_digest") == measured["native_digest"]
                and result.get("limit_binding_digest") == content_fingerprint(guards)
                and result.get("verified_quota") == {"rss_bytes": guards["HOST_RSS_BYTES"],
                    "child_private_bytes": guards["child_private_bytes"],
                    "tree_private_bytes": guards["child_private_bytes"]+guards["supervisor_private_bytes"]},
                "T010_RESOURCE_CONTAINED_RESULT_CONFLICT")
            instance = result.get("process_instance_digest")
            require(type(instance) is str and len(instance) == 64
                and all(c in "0123456789abcdef" for c in instance) and instance not in instances,
                "T010_RESOURCE_DISTINCT_CONTAINED_PROCESS_REQUIRED")
            instances.add(instance)
            metrics = process_metrics(result, scenario)
            require(all(value is None or value <= guards[key] for key, value in metrics.items()),
                "T010_RESOURCE_CONTAINED_GUARD_EXCEEDED")
    required_checks = {"volume_boundary", "physical_certificate", "measurement_guards", "process_private_limit"}
    require(set(boundary.get("validation", {})) == required_checks,
        "T010_RESOURCE_AFFECTED_BOUNDARY_TESTS_REQUIRED")
    for reference in boundary["validation"].values():
        result = read_reference(reference)
        require(result.get("runtime_code_digest") == physical["runtime_code_digest"]
            and result.get("exit_code") == 0 and type(result.get("checks")) is int and result["checks"] > 0,
            "T010_RESOURCE_AFFECTED_VALIDATION_REQUIRED")
    review = read_reference(boundary["independent_review"])
    require(review.get("runtime_code_digest") == physical["runtime_code_digest"]
        and review.get("scope") == "FROZEN_T010_RESOURCE_INTERFACES_AND_PROOF"
        and review.get("findings") == [], "T010_RESOURCE_INDEPENDENT_REVIEW_REQUIRED")


def check_initial_capacity(envelope):
    from .runtime_dry_profile_v0_1 import read_reference
    from .t010_resource_environment_v0_1 import check_volume_reserves, check_memory_reserve
    physical = read_reference(envelope["physical_evidence"])
    environment = read_reference(physical["environment"])
    return {"volumes": check_volume_reserves(environment["volumes"], environment["requirements"]),
        "memory": ("OBSERVATION_ONLY_NO_NUMERICAL_QUOTA" if observation_only(envelope)
            else check_memory_reserve(envelope["derivation"]["measured_guards"]))}


def check_host_journal_volume(envelope, path):
    from .runtime_dry_profile_v0_1 import read_reference
    from .t010_resource_environment_v0_1 import volume_observation
    physical = read_reference(envelope["physical_evidence"])
    expected = read_reference(physical["environment"])["volumes"]["owned"]
    observed = volume_observation(path)
    require(all(observed[key] == expected[key] for key in ("volume_id", "allocation_unit_bytes")),
        "T010_RESOURCE_HOST_JOURNAL_VOLUME_CONFLICT")
    return observed


class ResourceGate:
    """Owner-local durable source reservations, charged before public facts.

    The original producer transaction remains the exact byte-cap arbiter. A
    conservative joint-row reservation prevents a whole next source batch plus
    possible retirement from crossing the original identity cap. No history is
    erased and empty/failed/interrupted pages do not refund source capacity.
    """
    def __init__(self, path, envelope, segment, *, create=False):
        require(segment in ("qualification", "host"), "T010_RESOURCE_SEGMENT_REQUIRED")
        self.path, self.envelope, self.segment = Path(path), envelope, segment
        self.binding = content_fingerprint(envelope)
        self.spec = envelope["derivation"]
        self.observing = observation_only(envelope)
        self.environment = None
        if envelope.get("physical_evidence") is not None:
            from .runtime_dry_profile_v0_1 import read_reference
            physical = read_reference(envelope["physical_evidence"])
            self.environment = read_reference(physical["environment"])
        self.max_rows = self.spec[segment]["max_source_rows"]
        if create:
            require(not self.path.exists(), "T010_RESOURCE_RESERVATION_ALREADY_EXISTS")
            self._write({"schema":"MEME_LIVE_T010_RESOURCE_RESERVATIONS_V1", "envelope_digest":self.binding,
                "segment":segment, "source_rows_reserved":0, "source_units_reserved":0,
                "capture_rows_reserved":{table:0 for table in self._source_tables()},
                "source_records_reserved":0, "source_payload_bytes_reserved":0,
                "capacity_stop":None,
                "held_reason":None})
        self.snapshot()

    def _write(self, value):
        from .t010_public_host_v0_1 import write_record
        write_record(self.path, value)

    def bind_runtime(self, runtime):
        if self.environment is not None:
            from .t010_resource_environment_v0_1 import validate_binding
            # Verify engine/schema/path premises after original reconstruction.
            # Reserve failure must not preempt an already-admitted DRY recovery;
            # ordinary work checks the fresh reserve after that priority branch.
            validate_binding(self.environment, runtime=runtime, check_reserve=False)

    def snapshot(self):
        from .t010_public_host_v0_1 import read_record
        value = read_record(self.path)
        _keys(value, ("schema", "envelope_digest", "segment", "source_rows_reserved", "source_units_reserved",
            "capture_rows_reserved", "source_records_reserved", "source_payload_bytes_reserved",
            "capacity_stop", "held_reason"), "T010_RESOURCE_DURABLE_RESERVATION_REQUIRED")
        captured = value["capture_rows_reserved"]
        require(type(captured) is dict and set(captured) == set(self._source_tables())
            and all(type(count) is int and 0 <= count <= self.max_rows for count in captured.values()),
            "T010_RESOURCE_CAPTURE_RESERVATION_CONFLICT")
        max_records = (self.max_rows+self.spec["dispatch"]["batch_rows"]-1)//self.spec["dispatch"]["batch_rows"]
        max_records += int(self.segment == "qualification")
        require(type(value["source_records_reserved"]) is int and 0 <= value["source_records_reserved"] <= max_records
            and type(value["source_payload_bytes_reserved"]) is int
            and 0 <= value["source_payload_bytes_reserved"] <= max_records*self.spec["source_record_bytes"]
            and (value["capacity_stop"] is None or type(value["capacity_stop"]) is dict),
            "T010_RESOURCE_SOURCE_RECORD_RESERVATION_CONFLICT")
        require(value["schema"] == "MEME_LIVE_T010_RESOURCE_RESERVATIONS_V1"
            and value["envelope_digest"] == self.binding and value["segment"] == self.segment
            and type(value["source_rows_reserved"]) is int and 0 <= value["source_rows_reserved"] <= self.max_rows
            and type(value["source_units_reserved"]) is int
            and 0 <= value["source_units_reserved"] <= (self.max_rows+self.spec["dispatch"]["batch_rows"]-1)//self.spec["dispatch"]["batch_rows"]
            and (value["held_reason"] is None or type(value["held_reason"]) is str and len(value["held_reason"]) <= 128),
            "T010_RESOURCE_ORIGINAL_RESERVATION_CONFLICT")
        return value

    @staticmethod
    def _source_tables():
        from .source_health_v0_1 import TABLES
        return TABLES

    def reserve_capture(self, counts):
        """Charge all four exact metadata counts before reading their payloads."""
        require(type(counts) is dict and set(counts) == set(self._source_tables())
            and all(type(count) is int and count >= 0 for count in counts.values()),
            "T010_RESOURCE_CAPTURE_COUNTS_REQUIRED")
        value = self.snapshot()
        if value["held_reason"] is not None:
            return False
        captured = value["capture_rows_reserved"]
        if any(captured[table]+count > self.max_rows for table,count in counts.items()):
            return self.hold("SOURCE_CAPTURE_WINDOW_EXHAUSTED")
        value["capture_rows_reserved"] = {table:captured[table]+count for table,count in counts.items()}
        self._write(value)
        return True

    def reserve_source_verdict(self, verdict):
        """Bound JSON incrementally, before constructing any full JSON string.

        Original typed facts are never trimmed. An oversized uncommitted cut
        leaves capacity-stop metadata; it cannot replace a source verdict.
        Successful reservations persist before the original append and are
        never refunded after a lost acknowledgement or crash.
        """
        from .source_health_v0_1 import SourceVerdict
        require(type(verdict) is SourceVerdict, "T010_RESOURCE_ORIGINAL_SOURCE_VERDICT_REQUIRED")
        value = self.snapshot()
        if value["held_reason"] is not None:
            return False
        limit = self.spec["source_record_bytes"]
        def bounded_size(item):
            def record(part):
                if is_dataclass(part) and not isinstance(part,type):
                    return {field.name:getattr(part,field.name) for field in fields(part)}
                raise TypeError("T010_RESOURCE_SOURCE_JSON_TYPE_REQUIRED")
            encoder = json.JSONEncoder(default=record,sort_keys=True,separators=(",",":"),
                ensure_ascii=True,allow_nan=False)
            total = 0
            for piece in encoder.iterencode(item):
                total += len(piece)  # ensure_ascii makes bytes == characters
                if total > limit:
                    return total
            return total
        progress_bytes = bounded_size(verdict.progress)
        verdict_bytes = bounded_size(verdict) if progress_bytes <= limit else None
        if progress_bytes > limit or verdict_bytes > limit:
            value["held_reason"] = "SOURCE_RETAINED_RECORD_BYTE_CAPACITY"
            value["capacity_stop"] = {"schema":"T010_SOURCE_CAPACITY_STOP_METADATA_V1",
                "observed_at_utc":verdict.snapshot.observed_at_utc,
                "source_identity":verdict.binding.source_identity,
                "previous_verdict_digest":verdict.snapshot.previous_verdict_digest,
                "observed_disposition":verdict.disposition,
                "progress_bytes_at_least":progress_bytes, "verdict_bytes_at_least":verdict_bytes,
                "cursors":[asdict(cursor) for cursor in verdict.snapshot.cursors],
                "is_source_verdict":False}
            self._write(value)
            return False
        max_records = (self.max_rows+self.spec["dispatch"]["batch_rows"]-1)//self.spec["dispatch"]["batch_rows"]
        max_records += int(self.segment == "qualification")
        if value["source_records_reserved"] >= max_records:
            return self.hold("SOURCE_RECORD_COUNT_CAPACITY")
        value["source_records_reserved"] += 1
        value["source_payload_bytes_reserved"] += verdict_bytes
        self._write(value)
        return True

    def hold(self, reason):
        value = self.snapshot()
        value["held_reason"] = value["held_reason"] or reason
        self._write(value)
        return False

    def before_step(self, started):
        runtime = started.runtime
        require(runtime.capability == "NO_BROADCAST", "T010_RESOURCE_DRY_ONLY")
        value = self.snapshot()
        # An original admitted action requires no source/timer unit. Recovery
        # remains charged by the whole-session startup/Runtime work budget.
        pending = any(r.retired_sequence is None for r in runtime.ledger._custody.reservations)
        if pending or runtime._dry_recovery:
            runtime._resource_batch_rows = 0
            return True
        if value["held_reason"] is not None:
            return False
        if self.observing and self.environment is None:
            return self.hold("PHYSICAL_RESOURCE_EVIDENCE_UNAVAILABLE")
        if self.environment is not None:
            from .t010_resource_environment_v0_1 import check_volume_reserves, check_memory_reserve
            try:
                check_volume_reserves(self.environment["volumes"], self.environment["requirements"])
            except (ValueError, OSError, KeyError, TypeError):
                return self.hold("PHYSICAL_VOLUME_RESERVE_UNPROVEN")
            if self.spec.get("measured_guards") is not None:
                try:
                    check_memory_reserve(self.spec["measured_guards"])
                except (ValueError, OSError, KeyError, TypeError):
                    return self.hold("PHYSICAL_MEMORY_RESERVE_UNPROVEN")
        if not self._checkpoint_boundary(started):
            return False
        # Also cover a crash after the original verdict commit but before the
        # completed-step callback. A restart cannot append another failed cut.
        latest = runtime.source.latest()
        if latest is None or latest.disposition != "HEALTHY":
            return self.hold("SOURCE_EVIDENCE_NOT_HEALTHY")
        prior = 0 if self.segment == "qualification" else self.spec["qualification"]["max_admitted_roots"]
        if len(runtime.ledger._custody.reservations) >= prior+self.spec[self.segment]["max_admitted_roots"]:
            return self.hold("INTENDED_ADMITTED_ROOT_WORK_EXHAUSTED")
        remaining = self.max_rows-value["source_rows_reserved"]
        if not remaining:
            return self.hold("SOURCE_WINDOW_EXHAUSTED")
        batch = min(remaining, self.spec["dispatch"]["batch_rows"])
        # Read-only original checkpoint validation, including actual retained
        # bytes and resolved history. It does not call source or timers.
        from .operations_degradation_monitor_v0_1 import _producer_cut
        try:
            metrics, manifest, _ = _producer_cut(runtime.producer)
            if self.observing:
                required = ("IDENTITY_ROWS", "TOMBSTONE_ROWS", "UNFINISHED_MINTS",
                    "HISTORY_BYTES", "CHECKPOINT_BYTES", "PRODUCER_PENDING_ROWS", "PRODUCER_PENDING_BYTES")
                require(all(type(metrics[key]) is int and metrics[key] >= 0 for key in required),
                    "T010_REQUIRED_RESOURCE_STATE_INVALID")
        except (ValueError, OSError, KeyError, TypeError):
            if not self.observing:
                raise
            return self.hold("REQUIRED_RESOURCE_STATE_UNAVAILABLE")
        p = self.spec["producer_profile"]
        launch, retired, active = metrics["IDENTITY_ROWS"], metrics["TOMBSTONE_ROWS"], metrics["UNFINISHED_MINTS"]
        if launch+retired+active+2*batch > p["history_rows"]:
            return self.hold("JOINT_IDENTITY_RETIREMENT_CAPACITY")
        if active+batch > p["active_mints"]:
            return self.hold("ACTIVE_MINT_CAPACITY")
        for key in ("HISTORY_BYTES", "CHECKPOINT_BYTES", "PRODUCER_PENDING_ROWS", "PRODUCER_PENDING_BYTES"):
            if metrics[key] >= self.spec["resource_limits"][key]:
                return self.hold(key+"_CAPACITY")
        value["source_rows_reserved"] += batch
        value["source_units_reserved"] += 1
        self._write(value)
        runtime._resource_batch_rows = batch
        runtime._resource_raw_bytes = self.spec["source_unit_raw_bytes"]
        runtime._resource_capture_reservation = self.reserve_capture
        runtime._resource_source_append_guard = self.reserve_source_verdict
        return True

    def _checkpoint_boundary(self, started):
        from .t010_sqlite_boundary_v0_1 import checkpoint_owned_stores
        ok, reason, _ = checkpoint_owned_stores(started.runtime)
        return True if ok else self.hold(reason)

    def after_step(self, result, started=None):
        # RESTART with no reader wait establishes the next WAL reuse boundary.
        # Failure latches ordinary work held; original admitted DRY recovery
        # above remains first and consumes the unchanged whole-session budget.
        if started is not None:
            self._checkpoint_boundary(started)
        # A profile transaction may reject a record whose exact byte size was
        # not knowable before capture. Do not retry it forever or invent work.
        if result.work == "SOURCE_HELD":
            self.hold("ORIGINAL_SOURCE_OR_PROFILE_HELD")
        elif started is not None:
            # The original Runtime deliberately gates only entry on degraded
            # source evidence. This finite T010 segment must not spend all its
            # remaining source units appending evolving failed cuts. Retain the
            # first original failure in full, then observe without new work.
            latest = started.runtime.source.latest()
            if latest is None or latest.disposition != "HEALTHY":
                self.hold("SOURCE_EVIDENCE_NOT_HEALTHY")
