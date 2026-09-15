"""Explicit, detached Windows T010 DRY host; no Authority issuance or store setup.

Only the public CLI constructs this graph. JSON cannot name Python callbacks.
Stop/status read the saved control binding independently of source approvals.
"""
from __future__ import annotations

import ctypes
from ctypes import wintypes as w
from dataclasses import asdict, dataclass
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sqlite3
from contextlib import closing
import sys
import time
from uuid import uuid4

from .authority_controls_v0_1 import require, utc_from_us
from .ledger_actions_v0_1 import utc_microseconds
from .ledger_domain_v0_1 import LedgerDomain
from .operations_ownership_v0_1 import OperationsStore
from .operations_supervisor_v0_1 import OperationsSupervisor, HealthProfile
from .operations_windows_host_v0_1 import WindowsHostBoundary, kernel
from .runtime_dry_profile_v0_1 import load_profile, read_reference, _keys
from .runtime_dry_public_facts_v0_1 import ReviewedPublicFacts
from .runtime_dry_public_qualification_v0_1 import validate_evidence
from .t010_preflight_v0_1 import preflight
from phase5.shadow_domain_v0_1 import content_fingerprint

SCHEMA = "MEME_LIVE_T010_PUBLIC_HOST_REQUEST_V1"
STATE_SCHEMA = "MEME_LIVE_T010_PUBLIC_HOST_STATE_V1"
OBSERVATION_US = 12*60*60*1000000
HARD_EXPIRY_US = 14*60*60*1000000
OWNER_KEYS = ("domain_id", "binding_digest", "generation", "process_identity", "nonce", "budget_id",
              "attempts", "window_start_us", "next_attempt_us", "max_attempts", "window_us", "backoff_us")


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def write_record(path, value):
    """Atomic, durable replacement with an integrity digest (not authentication)."""
    path = Path(path)
    value = dict(value)
    value["content_digest"] = content_fingerprint(value)
    payload = json.dumps(value, sort_keys=True, allow_nan=False)
    require(len(payload.encode("utf-8")) <= 16777216, "T010_HOST_RECORD_SIZE_DENIED")
    # One exclusive temporary per destination bounds interrupted writes too.
    # An orphan remains evidence and denies another write; never remove it or
    # silently overwrite an interrupted record. Completed JSON bytes are exact.
    temporary = path.with_name(path.name+".tmp")
    with temporary.open("x", encoding="utf-8") as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def read_record(path):
    raw = Path(path).read_bytes()
    require(len(raw) <= 16777216, "T010_HOST_RECORD_SIZE_DENIED")
    record = json.loads(raw)
    digest = record.pop("content_digest")
    require(content_fingerprint(record) == digest, "T010_HOST_RECORD_CORRUPT")
    return record


def boot_identity():
    """Stable native Windows boot timestamp, not rounded walltime minus uptime."""
    require(os.name == "nt", "T010_HOST_WINDOWS_REQUIRED")
    # SYSTEM_TIMEOFDAY_INFORMATION begins with LARGE_INTEGER BootTime.
    data = ctypes.create_string_buffer(48)
    api = ctypes.WinDLL("ntdll")
    query = api.NtQuerySystemInformation
    query.argtypes = [w.ULONG, ctypes.c_void_p, w.ULONG, ctypes.POINTER(w.ULONG)]
    query.restype = w.LONG
    returned = w.ULONG()
    require(query(3, data, len(data), ctypes.byref(returned)) == 0,
        "T010_HOST_BOOT_IDENTITY_UNAVAILABLE")
    return hashlib.sha256(data.raw[:8]).hexdigest()


def in_job():
    api = kernel()
    api.IsProcessInJob.argtypes = [w.HANDLE, w.HANDLE, ctypes.POINTER(w.BOOL)]
    api.IsProcessInJob.restype = w.BOOL
    result = w.BOOL()
    require(api.IsProcessInJob(api.GetCurrentProcess(), None, ctypes.byref(result)),
        "T010_HOST_LAUNCHER_JOB_UNAVAILABLE")
    return bool(result.value)


def detached_spawn(arguments, *, cwd):
    """Break away from an enclosing job or fail; no inherited console/handles.

    The new process independently audits remaining current-job limits before
    entering the named T010 job. Inaccessible ancestor properties are not
    proven; the reviewed normal external terminal remains required. No
    scheduler or boot changes.
    """
    require(os.name == "nt", "T010_HOST_WINDOWS_REQUIRED")
    flags = subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP
    if in_job():
        flags |= subprocess.CREATE_BREAKAWAY_FROM_JOB
    return subprocess.Popen(arguments, cwd=str(cwd), creationflags=flags, close_fds=True,
        stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def detached_context():
    """Allow no residual job limits after breakaway from the launcher job.

    Windows sandbox containment may leave membership in an outer job with no
    limits. Membership alone is not a launcher lifetime dependency. Any
    remaining limit (including KILL_ON_JOB_CLOSE) is conservatively denied.
    """
    if not in_job():
        return {"remaining_job_membership":False, "remaining_job_limit_flags":0}
    from .operations_windows_host_v0_1 import _Limits
    limits = _Limits()
    api = kernel()
    require(api.QueryInformationJobObject(None, 9, ctypes.byref(limits), ctypes.sizeof(limits), None),
        "T010_HOST_REMAINING_JOB_AUDIT_UNAVAILABLE")
    require(limits.BasicLimitInformation.LimitFlags == 0, "T010_HOST_LAUNCHER_JOB_LIMITS_REMAIN")
    return {"remaining_job_membership":True, "remaining_job_limit_flags":0}


@dataclass(frozen=True, slots=True)
class Lifetime:
    launch_utc_us: int
    launch_qpc_ns: int
    boot_id: str
    observation_target_utc_us: int
    hard_expiry_utc_us: int
    policy_expiry_utc_us: int
    grant_expiry_utc_us: int

    @classmethod
    def begin(cls, policy, grant, *, utc_us, qpc_ns, boot_id):
        for value in (policy, grant):
            start = utc_microseconds(value["entry_valid_from_utc"])
            end = utc_microseconds(value["entry_valid_through_utc"])
            require(start <= utc_us < end and 0 < end-start <= HARD_EXPIRY_US,
                "T010_HOST_BOUNDED_CURRENT_POLICY_AND_GRANT_REQUIRED")
        policy_end = utc_microseconds(policy["entry_valid_through_utc"])
        grant_end = utc_microseconds(grant["entry_valid_through_utc"])
        require(grant_end <= policy_end and utc_us+OBSERVATION_US <= grant_end,
            "T010_HOST_TWELVE_HOUR_WINDOW_REQUIRED")
        return cls(utc_us, qpc_ns, boot_id, utc_us+OBSERVATION_US,
            utc_us+HARD_EXPIRY_US, policy_end, grant_end)

    def reason(self, *, utc_us, qpc_ns, boot_id):
        if boot_id != self.boot_id or qpc_ns < self.launch_qpc_ns:
            return "CLOCK_BOOT_DISCONTINUITY"
        if utc_us < self.launch_utc_us:
            return "UTC_REGRESSION"
        elapsed = (qpc_ns-self.launch_qpc_ns)//1000
        if elapsed >= HARD_EXPIRY_US or utc_us >= self.hard_expiry_utc_us:
            return "HARD_EXPIRED"
        if utc_us >= min(self.policy_expiry_utc_us, self.grant_expiry_utc_us):
            return "AUTHORITY_WINDOW_EXPIRED"
        if elapsed >= OBSERVATION_US or utc_us >= self.observation_target_utc_us:
            return "OBSERVATION_TARGET_REACHED"
        return None


def validate_work_budget(value):
    _keys(value, ("max_startups", "max_runtime_steps", "target_dry_terminals"),
        "T010_HOST_EXPLICIT_TOTAL_WORK_BUDGET_REQUIRED")
    require(all(type(v) is int and v >= 1 for v in value.values())
        and value["target_dry_terminals"] <= value["max_runtime_steps"],
        "T010_HOST_FINITE_POSITIVE_WORK_BUDGET_REQUIRED")
    return dict(value)


def reviewed_work_budget(profile, reference):
    """Consume the exact profile-bound whole-session resource derivation."""
    profile.configuration()
    inputs = profile.record["inputs"]
    require(type(inputs.get("resource_envelope")) is dict
        and inputs["resource_envelope"] == reference,
        "T010_HOST_COMPLETE_RESOURCE_ENVELOPE_BINDING_UNAVAILABLE")
    from .t010_resource_envelope_v0_1 import validate
    return validate_work_budget(validate(inputs)["derivation"]["work_budget"])


class WorkBudget:
    """One original session, charged durably before every possible work dispatch.

    Only the named host owner writes this file. Missing/corrupt state never
    recreates capacity. Reservations are consumed even if spawn/send/crash fails.
    Recovery startup/steps use the same totals, not a renewable restart window.
    """
    def __init__(self, root, session, *, create=False):
        self.path = Path(root)/"work_budget.json"
        self.session_digest = content_fingerprint(session)
        self.resource_reference = session.get("resource_envelope")
        self.limit = validate_work_budget(session["work_budget"])
        if create:
            require(not self.path.exists(), "T010_HOST_WORK_BUDGET_ALREADY_EXISTS")
            write_record(self.path, {"schema":"MEME_LIVE_T010_TOTAL_WORK_BUDGET_V1",
                "session_digest":self.session_digest, "limits":self.limit,
                "host_entries_reserved":0, "startups_reserved":0, "runtime_steps_reserved":0, "runtime_steps_completed":0,
                "unconfirmed_steps":0, "pending_step":None, "terminal_roots":[], "pending_action":None})
        self.snapshot()

    def snapshot(self):
        value = read_record(self.path)
        _keys(value, ("schema", "session_digest", "limits", "host_entries_reserved", "startups_reserved", "runtime_steps_reserved",
            "runtime_steps_completed", "unconfirmed_steps", "pending_step", "terminal_roots", "pending_action"),
            "T010_HOST_WORK_BUDGET_RECORD_INVALID")
        validate_work_budget(value["limits"])
        require(value["schema"] == "MEME_LIVE_T010_TOTAL_WORK_BUDGET_V1"
            and value["session_digest"] == self.session_digest and value["limits"] == self.limit,
            "T010_HOST_ORIGINAL_WORK_BUDGET_BINDING_REQUIRED")
        require(all(type(value[k]) is int and value[k] >= 0 for k in
            ("host_entries_reserved", "startups_reserved", "runtime_steps_reserved", "runtime_steps_completed", "unconfirmed_steps"))
            and value["host_entries_reserved"] <= self.limit["max_startups"]
            and value["startups_reserved"] <= self.limit["max_startups"]
            and value["runtime_steps_reserved"] <= self.limit["max_runtime_steps"]
            and value["runtime_steps_completed"]+value["unconfirmed_steps"] <= value["runtime_steps_reserved"],
            "T010_HOST_WORK_BUDGET_COUNTER_INVALID")
        pending = value["pending_step"]
        require(pending is None or type(pending) is int and pending == value["runtime_steps_reserved"] and pending > 0,
            "T010_HOST_WORK_RESERVATION_INVALID")
        require(value["runtime_steps_completed"]+value["unconfirmed_steps"]+(pending is not None)
            == value["runtime_steps_reserved"], "T010_HOST_WORK_RESERVATION_ACCOUNTING_CONFLICT")
        self._report({"terminal_roots":value["terminal_roots"], "pending_action":value["pending_action"]}, initial=True)
        return value

    def _report(self, report, *, initial=False):
        _keys(report, ("terminal_roots", "pending_action"), "T010_HOST_ORIGINAL_WORK_REPORT_REQUIRED")
        require(type(report["terminal_roots"]) is list
            and len(report["terminal_roots"]) == len(set(report["terminal_roots"]))
            and len(report["terminal_roots"]) <= self.limit["max_runtime_steps"]
            and all(type(root) is str and len(root) == 64 and set(root) <= set("0123456789abcdef")
                for root in report["terminal_roots"])
            and (type(report["pending_action"]) is bool or initial and report["pending_action"] is None),
            "T010_HOST_ORIGINAL_WORK_REPORT_INVALID")

    def _merge(self, value, report):
        self._report(report)
        require(set(value["terminal_roots"]).issubset(report["terminal_roots"]),
            "T010_HOST_ORIGINAL_TERMINAL_REGRESSION")
        value.update(terminal_roots=sorted(report["terminal_roots"]), pending_action=report["pending_action"])

    @property
    def observation_state(self):
        state = self.snapshot()
        pending = state["pending_action"] is not False or state["pending_step"] is not None
        if self.resources_held():
            return "OBSERVING_RESOURCE_EXHAUSTED" if not pending else "OBSERVING_RESOURCE_EXHAUSTED_PENDING_UNPROVEN"
        if len(state["terminal_roots"]) >= self.limit["target_dry_terminals"]:
            return "OBSERVING_TARGET_REACHED" if not pending else "OBSERVING_HELD_PENDING_ACTION"
        return "OBSERVING_WORK_EXHAUSTED" if not pending else "OBSERVING_WORK_EXHAUSTED_PENDING_UNPROVEN"

    def _may_work(self, state):
        # Interrupted dispatch and original pending action retain recovery
        # priority within the same finite startup/step totals. The child gate
        # still prevents ordinary source growth after a resource hold.
        if state["pending_step"] is not None or state["pending_action"] is not False:
            return True
        return not self.resources_held() and len(state["terminal_roots"]) < self.limit["target_dry_terminals"]

    def resources_held(self):
        resource_path = self.path.parent/"resource_work.json"
        if resource_path.exists():
            from .t010_resource_envelope_v0_1 import ResourceGate
            gate = ResourceGate(resource_path, read_reference(self.resource_reference), "host")
            if gate.snapshot()["held_reason"] is not None:
                return True
        path = self.path.parent/"resource_hold.json"
        if not path.exists():
            return False
        value = read_record(path)
        require(value == {"session_digest":self.session_digest, "held":True},
            "T010_HOST_RESOURCE_HOLD_BINDING_CONFLICT")
        return True

    def hold_resources(self):
        write_record(self.path.parent/"resource_hold.json", {"session_digest":self.session_digest, "held":True})

    def before_host_entry(self):
        # A parent can die before it reserves any child startup. Charge that
        # separately against the same finite count before its unique manifest.
        value = self.snapshot()
        if value["host_entries_reserved"] >= self.limit["max_startups"]:
            return False
        value["host_entries_reserved"] += 1
        write_record(self.path, value)
        return True

    def before_start(self):
        value = self.snapshot()
        if (not self._may_work(value) or value["startups_reserved"] >= self.limit["max_startups"]
                or value["runtime_steps_reserved"] >= self.limit["max_runtime_steps"]):
            return False
        value["startups_reserved"] += 1
        write_record(self.path, value)
        return True

    def after_start(self, report):
        value = self.snapshot()
        require(value["startups_reserved"] > 0, "T010_HOST_UNRESERVED_STARTUP_REPORT")
        self._merge(value, report)
        if value["pending_step"] is not None:
            value["unconfirmed_steps"] += 1
            value["pending_step"] = None
        write_record(self.path, value)

    def before_step(self):
        value = self.snapshot()
        require(value["pending_step"] is None and value["startups_reserved"] > 0,
            "T010_HOST_STEP_WITHOUT_CONFIRMED_STARTUP")
        if not self._may_work(value) or value["runtime_steps_reserved"] >= self.limit["max_runtime_steps"]:
            return False
        value["runtime_steps_reserved"] += 1
        value["pending_step"] = value["runtime_steps_reserved"]
        write_record(self.path, value)
        return True

    def after_step(self, report):
        value = self.snapshot()
        require(value["pending_step"] is not None, "T010_HOST_UNRESERVED_COMPLETION_REPORT")
        self._merge(value, report)
        value["runtime_steps_completed"] += 1
        value["pending_step"] = None
        write_record(self.path, value)


@dataclass(frozen=True)
class PublicHostInputs:
    """Trusted spawn input. Construct actual public facts only inside the child."""
    profile: object
    settings: dict
    policy_digest: str
    grant_id: str
    grant_digest: str
    qualified_terminals: tuple
    resource_path: str | None = None
    facts: object = None
    driver: object = None
    resource_gate: object = None

    def runtime_before_start(self):
        if self.resource_path is not None:
            from .t010_resource_envelope_v0_1 import enforce_current_process_rss, observation_only
            envelope = read_reference(self.profile.record["inputs"]["resource_envelope"])
            if observation_only(envelope):
                verified = {"scope":"OBSERVATION_ONLY_NO_NUMERICAL_QUOTA", "quota_installed":False}
            else:
                limit = self.profile.record["inputs"]["monitor"]["resource_limits"]["HOST_RSS_BYTES"]
                verified = enforce_current_process_rss(limit)
            guards = envelope["derivation"].get("measured_guards")
            if guards is not None:
                from .t010_resource_envelope_v0_1 import enforce_current_process_private_bytes
                verified["private_memory"] = enforce_current_process_private_bytes(guards["child_private_bytes"])
            write_record(Path(self.resource_path).parent/"child_rss_quota.json", {
                "resource_envelope":self.profile.record["inputs"]["resource_envelope"], "quota":verified})

    def runtime_started(self, started, startup_us):
        begin = time.perf_counter_ns()
        state = started.audit.authority
        require(state.policy is not None and state.policy.content_digest == self.policy_digest
            and state.grant is not None and state.grant.grant_id == self.grant_id
            and content_fingerprint(asdict(state.grant)) == self.grant_digest,
            "T010_HOST_ORIGINAL_POLICY_GRANT_REQUIRED")
        for terminal in self.qualified_terminals:
            repo = started.runtime.ledger
            reservation = repo.reservation(terminal["root_id"])
            require(reservation is not None and reservation.retired_sequence is not None,
                "T010_HOST_ORIGINAL_QUALIFICATION_RECEIPT_MISSING")
            receipt = repo._port_at_sequence(reservation.retired_sequence)
            require(receipt.content_digest == terminal["terminal_digest"]
                and repo._simulation_input_digest(terminal["attempt_id"]) == terminal["simulation_input_digest"],
                "T010_HOST_ORIGINAL_QUALIFICATION_RECEIPT_CHANGED")
        facts = ReviewedPublicFacts(self.profile, self.settings)
        object.__setattr__(self, "facts", facts)
        object.__setattr__(self, "driver", self.profile.driver(facts))
        if self.resource_path is not None:
            from .t010_resource_envelope_v0_1 import ResourceGate
            object.__setattr__(self, "resource_gate", ResourceGate(self.resource_path,
                read_reference(self.profile.record["inputs"]["resource_envelope"]), "host"))
            self.resource_gate.bind_runtime(started.runtime)
        report = self._work_report(started)
        # Include policy/grant/qualification receipt checks, facts/driver/gate
        # construction and the original report. Parent bootstrap/IPC is
        # separately included by the qualified supervisor startup watchdog.
        facts.startup_us = startup_us + max(1, (time.perf_counter_ns()-begin)//1000)
        return report

    def runtime_before_step(self, started):
        return self.resource_gate is None or self.resource_gate.before_step(started)

    def _work_report(self, started):
        repo = started.runtime.ledger
        excluded = {item["root_id"] for item in self.qualified_terminals}
        with repo._trusted_read():
            roots = []
            for item in repo._custody.dry_dispositions:
                if item.input.root_id in excluded or item.input.simulation_input_digest is None:
                    continue
                receipt = repo._port_at_sequence(item.sequence)
                require(receipt.kind == "DRY_NON_SUBMITTED" and receipt.dry_terminal == item.input
                    and repo._simulation_input_digest(item.input.attempt_id) == item.input.simulation_input_digest,
                    "T010_HOST_TARGET_ORIGINAL_SIMULATION_RECEIPT_REQUIRED")
                roots.append(item.input.root_id)
            roots.sort()
            pending = any(item.retired_sequence is None for item in repo._custody.reservations)
        return {"terminal_roots":roots, "pending_action":pending}

    def runtime_completed(self, started, result):
        if self.resource_gate is not None:
            self.resource_gate.after_step(result, started)
        return self._work_report(started)

    def __call__(self, started):
        require(self.driver is not None, "T010_HOST_CHILD_STARTUP_NOT_MEASURED")
        return self.driver(started)


def validate_request(request, repo_root, *, utc_us):
    _keys(request, ("schema", "profile", "dossier", "package", "qualification", "facts", "health",
        "journal_root", "execution_review_reference", "handoff", "resource_envelope"), "T010_HOST_EXACT_REQUEST_REQUIRED")
    require(request["schema"] == SCHEMA and type(request["execution_review_reference"]) is str
        and bool(request["execution_review_reference"].strip()), "T010_HOST_EXPLICIT_EXECUTION_REVIEW_REQUIRED")
    profile = load_profile(read_reference(request["profile"]))
    require(profile.record["inputs"]["qualification_substitutions"]["scope"] == "PUBLIC_ENVIRONMENT_REVIEWED",
        "T010_HOST_PUBLIC_QUALIFIED_PROFILE_REQUIRED")
    dossier, package = read_reference(request["dossier"]), read_reference(request["package"])
    report = preflight(repo_root, dossier, package, evaluated_at_utc=utc_from_us(utc_us))
    require(report["ready"] is True, "T010_HOST_PREFLIGHT_DENIED")
    qualified = dossier.get("supported_dry")
    require(type(qualified) is dict and request["profile"] == qualified["profile"]
        and request["qualification"] == qualified["qualification"],
        "T010_HOST_EXACT_DOSSIER_QUALIFICATION_REQUIRED")
    require(package["dry_configuration"] == profile.record, "T010_HOST_EXACT_PACKAGE_PROFILE_REQUIRED")
    evidence = validate_evidence(read_reference(request["qualification"]), profile)
    require(evidence["source_content_digest"] == package["source_content_digest"]
        and request["facts"] == evidence["request"]["facts"], "T010_HOST_QUALIFIED_SOURCE_FACTS_CONFLICT")
    # Constructing facts only validates the exact reviewed public settings; no
    # observations or transport calls occur in this read-only check.
    settings = read_reference(request["facts"])
    facts = ReviewedPublicFacts(profile, settings)
    require(asdict(facts.public_clock.policy) == package["policy"]["clock"],
        "T010_HOST_CLOCK_POLICY_CONFLICT")
    handoff = read_reference(request["handoff"])
    _keys(handoff, ("schema", "profile_content_digest", "package_sha256", "qualification_sha256",
        "stores", "operations_control", "review_reference"), "T010_HOST_REVIEWED_HANDOFF_REQUIRED")
    require(handoff["schema"] == "MEME_LIVE_CURRENT_PUBLIC_DRY_HOST_HANDOFF_V1"
        and handoff["profile_content_digest"] == profile.record["content_digest"]
        and handoff["package_sha256"] == request["package"]["sha256"]
        and handoff["qualification_sha256"] == request["qualification"]["sha256"]
        and type(handoff["review_reference"]) is str and bool(handoff["review_reference"]),
        "T010_HOST_HANDOFF_BINDING_CONFLICT")
    require(len(handoff["stores"]) == 5 and {v["kind"]:v["path"] for v in handoff["stores"]}
        == profile.record["inputs"]["store_paths"], "T010_HOST_HANDOFF_EXACT_STORES_REQUIRED")
    original_ops = next(v for v in evidence["stores"] if v["kind"] == "operations")
    require(next(v for v in handoff["stores"] if v["kind"] == "operations") == original_ops,
        "T010_HOST_QUALIFICATION_OPERATIONS_BUDGET_CHANGED")
    reviewed_work_budget(profile, request["resource_envelope"])
    health = HealthProfile(**request["health"])
    resource_record = read_reference(request["resource_envelope"])
    from .runtime_dry_public_qualification_v0_1 import qualification_health
    qualification_liveness = qualification_health(evidence["request"], profile)
    if qualification_liveness is not None:
        require(health == qualification_liveness, "T010_HOST_QUALIFICATION_HEALTH_CONFLICT")
    measured_guards = resource_record["derivation"].get("measured_guards")
    if measured_guards is not None:
        require(asdict(health) == {key: measured_guards["supervisor_"+key]
            for key in ("startup_us", "progress_us", "termination_us")},
            "T010_HOST_MEASURED_WATCHDOG_CONFLICT")
    root = Path(request["journal_root"])
    require(root.is_absolute() and root.exists() and root.is_dir()
        and not str(root).startswith(("\\\\", "//")), "T010_HOST_EXISTING_LOCAL_JOURNAL_REQUIRED")
    if resource_record.get("physical_evidence") is not None:
        from .t010_resource_envelope_v0_1 import check_host_journal_volume
        check_host_journal_volume(resource_record, root)
    config = profile.configuration()
    forbidden = [Path(p).resolve().parent for p in (*config["dry_live_paths"], config["market_source"].db_path)]
    require(all(root.resolve() != p and not root.resolve().is_relative_to(p) for p in forbidden),
        "T010_HOST_JOURNAL_PRODUCTION_PATH_DENIED")
    return profile, package, evidence, settings, health


def _domain(record):
    require(record["mode"] == "DRY" and record["expected_empty_token_accounts"] == [],
        "T010_HOST_DRY_CONTROL_BINDING_REQUIRED")
    return LedgerDomain(**dict(record, expected_empty_token_accounts=()))


def _saved(root):
    root = Path(root).resolve(strict=True)
    session = read_record(root/"session.json")
    require(session["schema"] == STATE_SCHEMA and session["journal_root"] == str(root),
        "T010_HOST_SESSION_BINDING_CONFLICT")
    # Stop depends only on the durable identity, not mutable source/profile files.
    domain = _domain(session["domain"])
    require(domain.economic_domain_id == session["domain_id"], "T010_HOST_SESSION_DOMAIN_CONFLICT")
    return session, domain


def ledger_pending_audit(session, domain):
    """Read original retained dispositions without acquiring a Ledger writer."""
    from .ledger_repository_v0_1 import _schema_and_domain
    path = Path(session["ledger_path"]).resolve(strict=True)
    with closing(sqlite3.connect(path.as_uri()+"?mode=ro", uri=True)) as conn:
        conn.execute("PRAGMA query_only=ON")
        conn.execute("BEGIN")
        _schema_and_domain(conn, domain)
        require(conn.execute("PRAGMA integrity_check").fetchall() == [("ok",)]
            and not conn.execute("PRAGMA foreign_key_check").fetchall(), "T010_HOST_LEDGER_INTEGRITY_DENIED")
        rows = conn.execute("SELECT a.action_id,a.root_id,(SELECT d.disposition FROM ledger_inbox_dispositions d "
            "WHERE d.root_id=a.root_id ORDER BY d.ordinal DESC LIMIT 1) FROM ledger_pending_actions a").fetchall()
        terminals = {json.loads(row[0])["dry_terminal"]["root_id"] for row in conn.execute(
            "SELECT payload_json FROM ledger_consumer_groups WHERE kind='DRY_NON_SUBMITTED'")}
        pending = [{"action_id":a, "root_id":r, "retained_disposition":d} for a,r,d in rows if r not in terminals]
        return {"sqlite_integrity":"ok", "pending_original_actions":pending,
            "ledger_retirement_performed":False, "audit_scope":"READONLY_ORIGINAL_DISPOSITIONS"}


def status(root):
    """Strict mode=ro Operations access; no preflight, acquisition or repair."""
    session, domain = _saved(root)
    control = OperationsStore(session["operations_path"], domain, readonly=True).snapshot()
    owner = read_record(Path(root)/"owner.json")
    require(owner["session_digest"] == content_fingerprint(session), "T010_HOST_OWNER_SESSION_CONFLICT")
    require(control["budget_id"] == session["initial_control"]["budget_id"], "T010_HOST_BUDGET_CHANGED")
    budget = WorkBudget(root, session).snapshot()
    saved = read_record(Path(root)/"status.json") if (Path(root)/"status.json").exists() else {}
    require(not saved or saved["session_digest"] == content_fingerprint(session), "T010_HOST_STATUS_SESSION_CONFLICT")
    return {"state":saved.get("state", "STARTUP_UNPROVEN"), "generation":control["generation"],
        "attempts":control["attempts"], "budget_id":control["budget_id"], "stopped":bool(control["stopped"]),
        "exhausted":bool(control["exhausted"]), "run_id":session["run_id"],
        "lifetime":session["lifetime"], "work_budget":budget, "last_observed_utc_us":saved.get("observed_utc_us"),
        "completed_steps":saved.get("completed_steps", 0), "grants_permission":False,
        "recorded_supervisor_pid":saved.get("supervisor_pid"),
        "recorded_host_launch_id":saved.get("host_launch_id"),
        "health_scope":"LAST_DURABLE_OBSERVATION_NOT_PROCESS_LIVENESS", "capability":"NO_BROADCAST"}


def stop(root, *, now_us=None):
    """Commit original Operations stop; never look up/kill a PID or need approval files."""
    session, domain = _saved(root)
    store = OperationsStore(session["operations_path"], domain)
    control = store.snapshot()
    require(control["budget_id"] == session["initial_control"]["budget_id"], "T010_HOST_BUDGET_CHANGED")
    now = time.time_ns()//1000 if now_us is None else now_us
    # A regressed OS clock must not prevent a safety stop. Use the already
    # committed control floor; this is not a qualified Authority clock sample.
    if not control["stopped"]:
        store.operator_stop(now_us=max(now, control["last_control_us"]))
    write_record(Path(root)/"stop.json", {"session_digest":content_fingerprint(session),
        "state":"STOP_COMMITTED", "observed_utc_us":now, "control":store.snapshot()})
    return {"state":"STOP_COMMITTED", "grants_permission":False}


def reconcile(root):
    """Stopped-only original DRY retirement, with empty named job and control lock.

    This never acquires/resets Operations or admits work. The SQLite writer
    lock prevents a concurrent reset while existing receipts are reconciled.
    """
    stop(root)
    session, domain = _saved(root)
    boundary = WindowsHostBoundary(session["domain_id"]).enter()
    ledger = reconcile_stopped_ledger(session, domain)
    write_record(Path(root)/"status.json", {"session_digest":content_fingerprint(session),
        "state":"STOPPED_RECONCILED", "observed_utc_us":time.time_ns()//1000,
        "completed_steps":0, "prior_job_empty":True, "ledger":ledger})
    return {"state":"STOPPED_RECONCILED", "prior_job_empty":True, "ledger":ledger,
        "grants_permission":False}


def reconcile_stopped_ledger(session, domain):
    # Internal helper: caller must hold the original empty-job boundary.
    from .ledger_repository_v0_1 import LedgerRepository
    from .runtime_dry_v0_1 import finish_interrupted_dry
    store = OperationsStore(session["operations_path"], domain)
    with store._connection() as controls:
        controls.execute("BEGIN IMMEDIATE")
        try:
            before = store._read(controls)
            require(before["stopped"] == 1 and before["nonce"] is None
                and before["budget_id"] == session["initial_control"]["budget_id"],
                "T010_HOST_STOPPED_ORIGINAL_CONTROL_REQUIRED")
            audit = ledger_pending_audit(session, domain)
            pending = audit["pending_original_actions"]
            require(len(pending) <= 1, "T010_HOST_RECONCILE_SINGLE_ORIGINAL_ACTION_REQUIRED")
            terminals = []
            if pending:
                with closing(LedgerRepository.reopen(session["ledger_path"], domain)) as repo:
                    require(not repo._conn.execute("SELECT 1 FROM ledger_signature_bindings LIMIT 1").fetchone()
                        and not repo._conn.execute("SELECT 1 FROM ledger_chain_receipts LIMIT 1").fetchone()
                        and not repo._conn.execute("SELECT 1 FROM ledger_economic_postings LIMIT 1").fetchone(),
                        "T010_HOST_RECONCILE_DRY_UNSIGNED_GRAPH_REQUIRED")
                    original = repo.action(pending[0]["action_id"])
                    require(original is not None and original.side == "BUY", "T010_HOST_RECONCILE_ORIGINAL_DRY_BUY_REQUIRED")
                    previous = repo._authority.last_qualified_clock
                    floor = 0 if previous is None else utc_microseconds(previous.utc_upper_utc)
                    receipt = finish_interrupted_dry(repo, original.action_id,
                        recorded_at_utc=utc_from_us(max(time.time_ns()//1000, floor, before["last_control_us"])))
                    terminals.append(receipt.content_digest)
                    require(repo.audit()["posting_count"] == 0, "T010_HOST_RECONCILE_NO_ECONOMIC_POSTINGS_REQUIRED")
            require(store._read(controls) == before, "T010_HOST_RECONCILE_CONTROL_CHANGED")
            result = ledger_pending_audit(session, domain)
            require(not result["pending_original_actions"], "T010_HOST_RECONCILE_UNRESOLVED")
            return dict(result, ledger_retirement_performed=bool(terminals), terminal_digests=terminals,
                operations_control_unchanged=True)
        finally:
            controls.rollback()


def _matching_owner(current, expected):
    require(all(current[k] == expected[k] for k in OWNER_KEYS), "T010_HOST_MISSING_OR_CHANGED_OWNER_EVIDENCE")
    require(not current["stopped"] and not current["exhausted"], "T010_HOST_STOPPED_OR_EXHAUSTED")


def _owner_record(root, session, control, *, launch_id):
    write_record(root/"owner.json", {"session_digest":content_fingerprint(session),
        "control":control, "host_launch_id":launch_id})


def run(request, repo_root, *, launch_id, detached_required=True):
    # Reject remaining current-job limits after the explicit breakaway.
    # This does not attest inaccessible ancestor jobs; see the runbook.
    detach = detached_context() if detached_required else {"test_only":True}
    utc = time.time_ns()//1000
    # Existing-run expiry and safety stop precede current source/preflight checks.
    existing_root = Path(request["journal_root"])
    if (existing_root/"session.json").exists():
        saved_session, _ = _saved(existing_root)
        lifetime = Lifetime(**saved_session["lifetime"])
        expired = lifetime.reason(utc_us=utc, qpc_ns=time.perf_counter_ns(), boot_id=boot_identity())
        if expired:
            stop(existing_root)
            write_record(existing_root/"status.json", {"session_digest":content_fingerprint(saved_session),
                "state":expired, "observed_utc_us":utc, "completed_steps":0, "host_launch_id":launch_id,
                "durable_stop":True, "ledger_retirement_performed":False})
            return 0
    profile, package, evidence, settings, health = validate_request(request, repo_root, utc_us=utc)
    config = profile.configuration()
    root = Path(request["journal_root"]).resolve()
    boundary = WindowsHostBoundary(config["domain"].economic_domain_id).enter()
    resource_record = read_reference(request["resource_envelope"])
    guards = resource_record["derivation"].get("measured_guards")
    if guards is not None:
        from .t010_resource_envelope_v0_1 import enforce_host_tree_private_bytes, enforce_current_process_rss
        enforce_host_tree_private_bytes(boundary, guards)
        enforce_current_process_rss(guards["supervisor_rss_bytes"])
    store = OperationsStore(config["operations_path"], config["domain"])
    current = store.snapshot()
    boot = boot_identity()
    if (root/"session.json").exists():
        session, _ = _saved(root)
        require(session["request_digest"] == content_fingerprint(request), "T010_HOST_ORIGINAL_REQUEST_REQUIRED")
        previous = read_record(root/"owner.json")
        require(previous["session_digest"] == content_fingerprint(session), "T010_HOST_OWNER_SESSION_CONFLICT")
        lifetime = Lifetime(**session["lifetime"])
        # Expired stale sessions can be stopped without a new acquisition.
        reason = lifetime.reason(utc_us=utc, qpc_ns=time.perf_counter_ns(), boot_id=boot)
        if reason:
            stop(root)
            write_record(root/"status.json", {"session_digest":content_fingerprint(session),
                "state":reason, "observed_utc_us":utc, "completed_steps":0})
            return 0
        _matching_owner(current, previous["control"])
    else:
        require(not any((root/name).exists() for name in ("owner.json", "status.json", "stop.json", "work_budget.json")),
            "T010_HOST_ORPHAN_EVIDENCE_DENIED")
        # Exact postqualification files are required on first launch only.
        # Reopening never asks for an old file hash after legitimate mutations.
        handoff = read_reference(request["handoff"])
        require(all(sha(item["path"]) == item["sha256"] for item in handoff["stores"]),
            "T010_HOST_QUALIFIED_STORE_CHANGED")
        require(current == handoff["operations_control"], "T010_HOST_HANDOFF_CONTROL_CONFLICT")
        expected = dict(evidence["startup_audits"][-1]["owner"])
        require(all(current[k] == expected[k] for k in ("generation", "nonce", "process_identity")),
            "T010_HOST_QUALIFIED_OWNER_CONFLICT")
        require(not current["stopped"] and not current["exhausted"], "T010_HOST_STOPPED_OR_EXHAUSTED")
        if guards is not None or resource_record.get("physical_evidence") is not None:
            from .t010_resource_envelope_v0_1 import check_initial_capacity
            check_initial_capacity(read_reference(request["resource_envelope"]))
        lifetime = Lifetime.begin(package["policy"], package["grant"], utc_us=utc,
            qpc_ns=time.perf_counter_ns(), boot_id=boot)
        work_budget = reviewed_work_budget(profile, request["resource_envelope"])
        session = {"schema":STATE_SCHEMA, "run_id":uuid4().hex, "request_digest":content_fingerprint(request),
            "journal_root":str(root), "profile_reference":request["profile"],
            "dossier_reference":request["dossier"], "package_reference":request["package"],
            "qualification_reference":request["qualification"], "facts_reference":request["facts"],
            "handoff_reference":request["handoff"], "resource_envelope":request["resource_envelope"],
            "work_budget":work_budget,
            "source_content_digest":package["source_content_digest"], "startup_identity":profile.record["startup_identity"],
            "domain":profile.record["domain"], "domain_id":config["domain"].economic_domain_id,
            "operations_path":str(Path(config["operations_path"]).resolve()),
            "ledger_path":str(Path(config["ledger_path"]).resolve()), "initial_control":current,
            "policy_digest":content_fingerprint(package["policy"]), "grant_digest":content_fingerprint(package["grant"]),
            "grant_id":package["grant"]["grant_id"], "source_binding":asdict(config["source_binding"]),
            "lifetime":asdict(lifetime), "interpreter_path":str(Path(sys.executable).resolve()),
            "interpreter_sha256":sha(sys.executable)}
        write_record(root/"session.json", session)
        WorkBudget(root, session, create=True)
        from .t010_resource_envelope_v0_1 import ResourceGate
        ResourceGate(root/"resource_work.json", read_reference(request["resource_envelope"]), "host", create=True)
        _owner_record(root, session, current, launch_id=launch_id)
    require(session["interpreter_path"] == str(Path(sys.executable).resolve())
        and session["interpreter_sha256"] == sha(sys.executable), "T010_HOST_INTERPRETER_CHANGED")
    budget = WorkBudget(root, session)
    require(budget.before_host_entry(), "T010_HOST_ENTRY_BUDGET_EXHAUSTED")
    inputs = PublicHostInputs(profile, settings, session["policy_digest"], session["grant_id"], session["grant_digest"],
        tuple(evidence["terminals"]), str(root/"resource_work.json"))
    write_record(root/("process-"+launch_id+".json"), {"session_digest":content_fingerprint(session),
        "host_launch_id":launch_id, "supervisor_pid":os.getpid(), "started_utc_us":utc,
        "started_qpc_ns":time.perf_counter_ns(), "boot_id":boot, "detached_from_launcher_job":True,
        "detachment_audit":detach})
    supervisor = None
    reason = "HOST_ERROR"
    completed = 0
    try:
        supervisor = OperationsSupervisor(config, inputs, health, expected_generation=current["generation"], work_control=budget)
        previous_status = None
        last_write = 0
        retained_owner = None
        observing = False
        observation_store = OperationsStore(config["operations_path"], config["domain"], readonly=True)
        while True:
            now = time.time_ns()//1000
            mono = time.perf_counter_ns()
            reason = lifetime.reason(utc_us=now, qpc_ns=mono, boot_id=boot)
            if reason:
                break
            if observing:
                observed_control = observation_store.snapshot()
                if observed_control["stopped"]:
                    reason = "OPERATOR_STOPPED"
                    break
            else:
                facts = supervisor.poll(now_us=now, monotonic_us=mono//1000)
                observing = facts.state.startswith("OBSERVING_")
            completed = budget.snapshot()["runtime_steps_completed"]
            if supervisor.fence is not None and (facts.state == "RUNNING" or observing):
                control = observation_store.snapshot() if observing else store.snapshot()
                require(control["budget_id"] == session["initial_control"]["budget_id"], "T010_HOST_BUDGET_CHANGED")
                owner_key = tuple(control[k] for k in OWNER_KEYS)
                if retained_owner != owner_key:
                    _owner_record(root, session, control, launch_id=launch_id)
                    retained_owner = owner_key
            key = (facts.state, facts.generation, facts.last_work)
            if key != previous_status or mono-last_write >= 1000000000:
                write_record(root/"status.json", {"session_digest":content_fingerprint(session),
                    "state":facts.state, "observed_utc_us":now, "completed_steps":completed,
                    "generation":facts.generation, "host_launch_id":launch_id,
                    "supervisor_pid":os.getpid(), "detached_from_launcher_job":True,
                    "alerts":_jsonable(supervisor.alert_snapshot()), "work_budget":budget.snapshot()})
                previous_status, last_write = key, mono
            if facts.state.startswith("HELD_") or facts.state in ("OPERATOR_STOPPED", "RESTART_EXHAUSTED"):
                reason = facts.state
                break
            time.sleep(0.05)
    finally:
        # Stop is durable first; the exact contained child handle is joined.
        # Failure is recorded as held, never success. CLI exit closes the job,
        # ensuring even a blocked child cannot survive this host.
        finalized = False
        try:
            stop(root)
            if supervisor is not None and supervisor.process is not None:
                child = supervisor.process
                if child.exitcode is None:
                    child.terminate()
                child.join(timeout=health.termination_us/1000000)
                require(child.exitcode is not None, "T010_HOST_CHILD_TERMINATION_UNCONFIRMED")
                child.close()
            finalized = True
        finally:
            write_record(root/"status.json", {"session_digest":content_fingerprint(session),
                "state":reason if finalized else "HELD_FINALIZATION_UNPROVEN",
                "observed_utc_us":time.time_ns()//1000, "completed_steps":completed,
                "host_launch_id":launch_id, "supervisor_pid":os.getpid(), "durable_stop":finalized})
    return 0 if reason in ("OBSERVATION_TARGET_REACHED", "HARD_EXPIRED", "AUTHORITY_WINDOW_EXPIRED", "OPERATOR_STOPPED") else 2


def _jsonable(value):
    from dataclasses import is_dataclass
    return asdict(value) if is_dataclass(value) else value


def launch(request_reference, repo_root):
    request = read_reference(request_reference)
    # An expired existing session needs only its durable stop binding.
    root = Path(request["journal_root"])
    if (root/"session.json").exists():
        session, _ = _saved(root)
        if Lifetime(**session["lifetime"]).reason(utc_us=time.time_ns()//1000,
                qpc_ns=time.perf_counter_ns(), boot_id=boot_identity()):
            return dict(stop(root), state="EXPIRED_STOP_COMMITTED")
    validate_request(request, repo_root, utc_us=time.time_ns()//1000)
    launch_id = uuid4().hex
    process = detached_spawn([sys.executable, "-B", str(Path(repo_root)/"scripts/live_t010_public_host_v0_1.py"),
        "run", "--request", request_reference["path"], "--sha256", request_reference["sha256"],
        "--launch-id", launch_id, "--execute-reviewed-t010"], cwd=repo_root)
    # Retain the exact handle during the startup receipt wait. Closing our
    # handle after the receipt does not terminate the detached process.
    deadline = time.perf_counter_ns()+min(60000000, request["health"]["startup_us"]+5000000)*1000
    state_path = Path(request["journal_root"])/"status.json"
    while time.perf_counter_ns() < deadline:
        if state_path.exists():
            saved = read_record(state_path)
            if saved.get("host_launch_id") == launch_id:
                return {"state":saved["state"], "launch_id":launch_id,
                    "pid":process.pid, "journal_root":request["journal_root"], "grants_permission":False}
        if process.poll() not in (None, 0):
            raise RuntimeError("T010_HOST_DETACHED_STARTUP_DENIED")
        time.sleep(0.05)
    # No guessed-PID cleanup: the finite host still owns its original deadline.
    return {"state":"HELD_STARTUP_RECEIPT_UNAVAILABLE", "launch_id":launch_id,
        "journal_root":request["journal_root"], "grants_permission":False}
