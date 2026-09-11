"""C1 measurement records for disposable, offline Operations workloads.

This is harness support, not a runtime observer or a qualification decision.
Configured continuation limits are not measurements. Missing facts remain null.
Producer facts are copied from its cached last profile evaluation without a DB
scan or checkpoint publication; their age is unknown. Bytes retain the producer's
definitions and must not be interpreted as RSS or SQLite allocation sizes.
"""
from __future__ import annotations

import ctypes
import math
import os
import shutil
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

from live.continuous_producer_v0_2 import ContinuationProfileV02


# Fixed C1 dimensions; downstream collectors supply missing facts with provenance.
# Each tuple is (unit, exact scope/definition). This is not a second sizing profile.
DEFINITIONS = {
    "active_feature_events": ("events", "All retained FEATURES rows; producer retained_events."),
    "active_feature_payload_bytes": ("bytes", "Sum of UTF-8 event_json payload bytes across retained FEATURES only."),
    "retained_state_bytes": ("bytes", "Sum of canonical JSON list UTF-8 sizes for FEATURES and RUNS, including list/row overhead; producer retained_bytes."),
    "hottest_mint_events": ("events", "Maximum retained FEATURES row count for one mint; producer hottest_events."),
    "hottest_mint_payload_bytes": ("bytes", "Maximum sum of UTF-8 event_json payload bytes for one mint; producer hottest_bytes. May identify a different mint than the event maximum."),
    "unfinished_mints": ("mints", "Distinct mints with retained FEATURES; producer active_mints. Not a count of historical identities."),
    "identity_rows": ("rows", "Immutable launch index insert counter; producer launch_rows."),
    "tombstone_rows": ("rows", "Immutable retired-run bundle insert counter; producer retired_rows."),
    "identity_tombstone_rows": ("rows", "Sum of producer launch_rows and retired_rows, matching history_rows cap."),
    "identity_tombstone_bytes": ("bytes", "Producer history_bytes insert counter accounting; logical history bytes, not physical index allocation."),
    "pending_rows": ("rows", "Sum of producer PENDING table rows, including unacknowledged input/output/audit/timer state."),
    "pending_bytes": ("bytes", "Sum of canonical JSON list UTF-8 sizes for producer PENDING tables, including empty list overhead."),
    "journal_tail_rows": ("rows", "Unresolved downstream journal tail; collector must name journal, durable boundary and included row types."),
    "journal_tail_bytes": ("bytes", "Unresolved downstream journal tail bytes; collector must specify byte representation and same boundary as rows."),
    "backlog_rows": ("rows", "Unconsumed raw source rows through a captured source fence; row count, not rowid subtraction."),
    "backlog_age_seconds": ("seconds", "UTC sample time minus oldest unconsumed input observed_at through the same fence; empty backlog has no age (UNKNOWN)."),
    "checkpoint_age_seconds": ("seconds", "UTC sample time minus evidenced last committed checkpoint publication time; generation and filesystem mtime are not publication timestamps."),
    "single_input_wall_seconds": ("seconds", "Elapsed wall duration of one explicitly bounded input-processing invocation; collector must identify input and included work. Not a batch average or a worst-case guarantee."),
    "operation_wall_seconds": ("seconds", "perf_counter elapsed duration for the explicitly named workload operation, excluding resource collection."),
    "operation_cpu_seconds": ("seconds", "process_time elapsed CPU for the current measurement process during the named operation, including its threads but excluding child processes."),
    "process_rss_bytes": ("bytes", "Current measurement process working-set point sample, including interpreter/imports; not incremental workload memory or a peak."),
    "process_lifetime_peak_rss_bytes": ("bytes", "OS-reported peak working set over this measurement process lifetime, including interpreter/imports and collection; not an operation-only peak or deployment guarantee."),
    "workload_disk_bytes": ("bytes", "Sum of logical st_size for the explicitly listed disposable workload files, including explicitly listed SQLite sidecars; not allocated blocks or disk I/O."),
    "disk_reserve_bytes": ("bytes", "Filesystem available/free bytes from disk_usage at the disposable workload directory; point sample, not a configured minimum reserve."),
    "restart_duration_seconds": ("seconds", "Wall duration between explicitly evidenced owned restart start and recovery-ready endpoints; excludes no phase implicitly."),
}

USAGE_FIELDS = {
    "active_feature_events": "retained_events", "retained_state_bytes": "retained_bytes",
    "hottest_mint_events": "hottest_events", "hottest_mint_payload_bytes": "hottest_bytes",
    "unfinished_mints": "active_mints", "identity_rows": "launch_rows",
    "tombstone_rows": "retired_rows", "identity_tombstone_bytes": "history_bytes",
    "pending_rows": "pending_rows", "pending_bytes": "pending_bytes",
}


@dataclass(frozen=True)
class Observation:
    state: str
    value: int | float | None
    provenance: str

    def __post_init__(self):
        if self.state not in ("MEASURED", "CONFIGURED", "UNKNOWN"):
            raise ValueError("invalid observation state")
        if not isinstance(self.provenance, str) or not self.provenance.strip():
            raise ValueError("explicit provenance/reason required")
        if self.state == "UNKNOWN":
            if self.value is not None:
                raise ValueError("unknown observation must be null")
        elif (type(self.value) not in (int, float) or self.value < 0
              or not math.isfinite(self.value)):
            raise ValueError("finite nonnegative numeric observation required")


def unknown(reason):
    return Observation("UNKNOWN", None, reason)


def measured(value, provenance):
    return Observation("MEASURED", value, provenance)


def time_operation(operation, *, scope):
    """Time one successful callable; propagate failures, never invent a result.

    Caller names its exact work and owns workload isolation. No per-input metric
    is derived: even a one-row batch can include checkpoint/normalization work.
    """
    if not isinstance(scope, str) or not scope.strip():
        raise ValueError("explicit operation scope required")
    wall, cpu = time.perf_counter(), time.process_time()
    result = operation()
    cpu_elapsed, wall_elapsed = time.process_time() - cpu, time.perf_counter() - wall
    return result, {
        "operation_wall_seconds": measured(wall_elapsed, "perf_counter; " + scope),
        "operation_cpu_seconds": measured(cpu_elapsed, "process_time; " + scope),
    }


def _process_memory():
    if os.name != "nt":
        absent = unknown("Current/peak RSS collection implemented only for Windows")
        return absent, absent

    class ProcessMemoryCounters(ctypes.Structure):
        _fields_ = [("cb", ctypes.c_ulong), ("PageFaultCount", ctypes.c_ulong)] + [
            (name, ctypes.c_size_t) for name in (
                "PeakWorkingSetSize", "WorkingSetSize", "QuotaPeakPagedPoolUsage",
                "QuotaPagedPoolUsage", "QuotaPeakNonPagedPoolUsage",
                "QuotaNonPagedPoolUsage", "PagefileUsage", "PeakPagefileUsage")]

    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    psapi = ctypes.WinDLL("psapi", use_last_error=True)
    kernel.GetCurrentProcess.restype = ctypes.c_void_p
    psapi.GetProcessMemoryInfo.argtypes = [ctypes.c_void_p, ctypes.POINTER(ProcessMemoryCounters), ctypes.c_ulong]
    psapi.GetProcessMemoryInfo.restype = ctypes.c_int
    counters = ProcessMemoryCounters()
    counters.cb = ctypes.sizeof(counters)
    if not psapi.GetProcessMemoryInfo(kernel.GetCurrentProcess(), ctypes.byref(counters), counters.cb):
        absent = unknown("GetProcessMemoryInfo failed; Windows error " + str(ctypes.get_last_error()))
        return absent, absent
    scope = "; measurement process pid=" + str(os.getpid())
    return (measured(counters.WorkingSetSize, "GetProcessMemoryInfo.WorkingSetSize" + scope),
            measured(counters.PeakWorkingSetSize, "GetProcessMemoryInfo.PeakWorkingSetSize; entire process lifetime" + scope))


def collect_resources(workload_root, relative_files):
    """Point-sample an explicitly listed disposable workload, without writes.

    A missing file makes the total unknown (including optional absent sidecars).
    Callers should list existing files and explain their inventory in the scope.
    Concurrent file changes are not atomic; freeze the workload for stable sizes.
    """
    root = Path(workload_root).resolve(strict=True)
    if not root.is_dir():
        raise ValueError("workload directory required")
    paths = []
    for relative in relative_files:
        if Path(relative).is_absolute():
            raise ValueError("workload-relative file names required")
        path = (root / relative).resolve()
        if not path.is_relative_to(root) or path in paths:
            raise ValueError("file must be unique and within disposable workload")
        paths.append(path)
    provenance = "logical st_size point samples; disposable root=" + str(root) + "; files=" + repr([str(p.relative_to(root)) for p in paths])
    disk = unknown("No workload file inventory supplied")
    if paths:
        try:
            if not all(p.is_file() for p in paths):
                raise OSError("listed workload file missing or not a regular file")
            disk = measured(sum(p.stat().st_size for p in paths), provenance)
        except OSError as exc:
            disk = unknown(provenance + "; " + str(exc))
    try:
        reserve = measured(shutil.disk_usage(root).free, "shutil.disk_usage.free; volume containing " + str(root))
    except OSError as exc:
        reserve = unknown("disk_usage unavailable: " + str(exc))
    rss, peak = _process_memory()
    return {"process_rss_bytes": rss, "process_lifetime_peak_rss_bytes": peak,
            "workload_disk_bytes": disk, "disk_reserve_bytes": reserve}


def collect_record(producer, *, workload_scope, observations=None):
    """Copy producer facts and attach explicitly sourced C1 workload observations.

    No thresholds, maximum claims or qualification success are calculated.
    Caller-supplied observations cannot replace the producer's accounting facts.
    """
    if type(producer.profile) is not ContinuationProfileV02:
        raise ValueError("ContinuationProfileV02 required")
    if not isinstance(workload_scope, str) or not workload_scope.strip():
        raise ValueError("explicit disposable workload scope required")
    facts = {name: unknown("Not collected in this measurement") for name in DEFINITIONS}
    usage = dict(getattr(producer, "last_profile_usage", {}))
    for name, field in USAGE_FIELDS.items():
        if field in usage:
            facts[name] = measured(usage[field], "producer.last_profile_usage." + field + "; cached profile evaluation, evaluation UTC/age unknown")
    if "launch_rows" in usage and "retired_rows" in usage:
        facts["identity_tombstone_rows"] = measured(usage["launch_rows"] + usage["retired_rows"], "producer.last_profile_usage.launch_rows + retired_rows; cached profile evaluation, evaluation UTC/age unknown")
    for name, observation in (observations or {}).items():
        if name not in DEFINITIONS or name in USAGE_FIELDS or name == "identity_tombstone_rows":
            raise ValueError("unknown metric or attempted producer accounting override: " + name)
        if type(observation) is not Observation or observation.state == "CONFIGURED":
            raise ValueError("workload observation must be measured or unknown; configuration is separate")
        if (observation.state == "MEASURED" and DEFINITIONS[name][0] != "seconds"
                and type(observation.value) is not int):
            raise ValueError("counts and bytes require integral measurements")
        facts[name] = observation
    return {
        "contract": "operations_c1_measurement_v0.1",
        "captured_at_utc": datetime.now(timezone.utc).isoformat(),
        "workload_scope": workload_scope,
        "interpretation": "Engineering measurement only; no production-host approval, qualification result, or latency guarantee. Capture time is not cached producer evaluation time.",
        "configured_profile": {name: asdict(Observation("CONFIGURED", value, "ContinuationProfileV02." + name))
                               for name, value in asdict(producer.profile).items()},
        "producer_counters": {name: asdict(measured(value, "producer.metrics." + name + "; cumulative since this producer instance initialization"))
                              for name, value in dict(producer.metrics).items()},
        "metrics": {name: {"unit": DEFINITIONS[name][0], "definition": DEFINITIONS[name][1], **asdict(fact)}
                    for name, fact in facts.items()},
    }
