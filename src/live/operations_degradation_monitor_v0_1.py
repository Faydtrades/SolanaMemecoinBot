"""C2 current owned Operations consumer; only original owners adjudicate truth.

No external I/O transport, new economic state, owner reset or recovery policy.

Human recovery boundary: an authorized operator may use DegradationStore.record
under the current OperationsOwnership.mutation_guard, with this exact domain,
policy, subject, latest active_digest and a fresh digest of reviewed original
proof. For stop/exhaustion this requires original authorized stop release/reset,
current exclusive owner, no Authority hard-stop, and full original barrier
revalidation. Source/producer integrity requires exact original lineage/anchors,
complete repaired history/checkpoint/tail and original accepted profile, never a
new anchor. Custody/economic incidents require the original owner-reconciled,
classified balances and intact replay/policy proof; absent such proof they stay
irreducible. A review receipt or acknowledgement alone is not positive proof.
A C2-gated durable unsent BUY retains its original attempt and capacity.
Unsigned attempts subsequently return original HELD/later-recovery disposition;
signed-unsent attempts request original public reconciliation. Restored resources
do not resume, resend, retire or release either. This incident requires original
attempt/capacity adjudication proof; acknowledgement or a later healthy metric
cannot clear it. This privileged audit boundary is not an automatic recovery.
The resource definitions are the C1 dimensions; thresholds are independently
reviewed configuration. Empty backlog proves age inapplicable, never age zero.
"""
from __future__ import annotations

import copy
import hashlib
import sqlite3
from contextlib import closing, contextmanager, nullcontext
from dataclasses import asdict, dataclass, field
from pathlib import Path

from phase5.shadow_domain_v0_1 import content_fingerprint
from . import operations_degradation_v0_1 as condition_contract
from .operations_degradation_v0_1 import (
    CONDITIONS, REQUIRED_METRICS, ConditionEvidence, DegradationPolicy, DegradationStore,
    digest, require, readiness_conditions, resource_condition, supervision_conditions,
)
from .operations_ownership_v0_1 import OperationsOwnership, OperationsStore
from .ledger_actions_v0_1 import utc_microseconds
from .source_health_v0_1 import CollectorSourceAdapter, INTEGRITY_REASONS

VERSION = "live_operations_degradation_monitor_v0.1"
HOST_METRICS = frozenset(("HOST_RSS_BYTES", "HOST_DISK_RESERVE_BYTES", "STARTUP_US", "PROTECTIVE_STEP_US"))
HUMAN = frozenset(("UNSENT_ATTEMPT_RECOVERY_REQUIRED", "SOURCE_IDENTITY_OR_HISTORY_BROKEN", "PRODUCER_INTEGRITY_UNAVAILABLE",
    "OWNER_FENCE_UNPROVEN", "OPERATOR_STOPPED", "RESTART_EXHAUSTED", "SUPERVISOR_HELD",
    "ECONOMIC_INTEGRITY_UNAVAILABLE", "CUSTODY_TRUTH_UNAVAILABLE"))
ERRORS = (ValueError, RuntimeError, sqlite3.DatabaseError, OSError, TypeError, AttributeError)


def monitor_configuration_digest(identity, *, path, host_identity_digest,
        resource_max_age_us, protective_qualification_digest):
    """Review-time binding, not automatic approval or a store reset."""
    code = tuple(hashlib.sha256(Path(file).read_bytes()).hexdigest()
        for file in (__file__, condition_contract.__file__))
    return content_fingerprint((VERSION, code, identity.configuration_digest, identity.runtime_code_digest,
        str(Path(path).resolve()), host_identity_digest, resource_max_age_us, protective_qualification_digest))


@dataclass(frozen=True, slots=True)
class MonitorConfiguration:
    path: str
    policy: DegradationPolicy
    host_identity_digest: str
    runtime_code_digest: str
    resource_max_age_us: int
    protective_qualification_digest: str | None

    def binding_for(self, identity):
        return monitor_configuration_digest(identity, path=self.path, host_identity_digest=self.host_identity_digest,
            resource_max_age_us=self.resource_max_age_us, protective_qualification_digest=self.protective_qualification_digest)

    def __post_init__(self):
        require(type(self.path) is str and type(self.policy) is DegradationPolicy)
        digest(self.host_identity_digest)
        digest(self.runtime_code_digest)
        require(type(self.resource_max_age_us) is int and 0 < self.resource_max_age_us <= 86400000000)
        if self.protective_qualification_digest is not None:
            digest(self.protective_qualification_digest)


@dataclass(frozen=True, slots=True)
class HostMetric:
    metric: str
    value: int | None
    observed_us: int
    evidence_digest: str

    def __post_init__(self):
        require(self.metric in HOST_METRICS)
        require(self.value is None or type(self.value) is int and 0 <= self.value < 2**63)
        OperationsStore._time(self.observed_us)
        digest(self.evidence_digest)


@dataclass(frozen=True, slots=True)
class HostObservations:
    """Trusted typed port, sampled anew at each ENTRY boundary.

    RSS/disk are fresh current-owner point samples. STARTUP_US measures this
    exact owner's original startup and remains valid for that owner. Protective
    timing is prior reviewed same-host/config/code qualification, not a demand
    to perform a new protective action. Neither timing sample is a rolling RSS
    sample. Missing qualification remains unknown rather than fabricated.
    """
    host_identity_digest: str
    configuration_digest: str
    owner_digest: str
    metrics: tuple[HostMetric, ...]

    def __post_init__(self):
        for value in (self.host_identity_digest, self.configuration_digest, self.owner_digest):
            digest(value)
        require(type(self.metrics) is tuple and all(type(item) is HostMetric for item in self.metrics)
            and len({item.metric for item in self.metrics}) == len(self.metrics))


@dataclass(frozen=True, slots=True)
class AlertCondition:
    condition: str
    state: str
    scope: str
    action: str
    subject_digest: str
    first_us: int
    last_us: int
    alerted_us: int | None
    recovered_us: int | None


@dataclass(frozen=True, slots=True)
class AlertSnapshot:
    conditions: tuple[AlertCondition, ...] = ()
    unavailable_code: str | None = None
    entry_held: bool = True
    grants_permission: bool = field(init=False, default=False)
    may_sign: bool = field(init=False, default=False)
    may_send: bool = field(init=False, default=False)


def unavailable(code="OPERATIONS_ALERT_STORE_UNAVAILABLE"):
    require(code in ("OPERATIONS_ALERT_STORE_UNAVAILABLE", "OPERATIONS_PROFILE_CONFIGURATION_REQUIRED",
        "OPERATIONS_CURRENT_OWNER_UNAVAILABLE", "OPERATIONS_CURRENT_EVIDENCE_UNAVAILABLE"))
    return AlertSnapshot(unavailable_code=code)


def alert_view(facts):
    return AlertSnapshot(tuple(AlertCondition(row.condition, row.state, row.scope, row.action,
        row.subject_digest, row.first_us, row.last_us, row.alerted_us, row.recovered_us)
        for row in facts.conditions), entry_held=facts.entry_held)


def _subject(domain, kind, identity):
    return content_fingerprint((domain.economic_domain_id, kind, identity))


def _record(store, code, subject, witness, now_us, *, healthy=False):
    """Only fresh positive samples clear explicitly recoverable conditions.

    Repeated identical read cuts do not manufacture new observation timestamps.
    Active samples coalesce into last_us without per-poll receipt history.
    Recovery must be newer than that latest real active sample.
    """
    rows = store.snapshot().conditions
    old = next((row for row in rows if (row.condition, row.subject_digest) == (code, subject)), None)
    if healthy:
        if old is None or old.state != "ACTIVE" or code in HUMAN:
            return
        if now_us <= old.last_us or witness == old.evidence_digest:
            return
        store.record(ConditionEvidence(code, subject, now_us, witness, "RECOVERED", CONDITIONS[code][1],
            old.active_digest), now_us=now_us)
    elif old is None or old.state != "ACTIVE" or now_us > old.last_us:
        store.record(ConditionEvidence(code, subject, now_us, witness), now_us=now_us, coalesce_active=True)


def _producer_cut(producer):
    """Validate a detached read-only observer; never alter owner caches/counters.

    Original bounded checkpoint verification supplies current usage. A bounded
    retained-event scan counts mints with no BUY/SELL, exactly the original
    engine first_tradable_event_at_us definition; no engine reconstruction. The clone shares only the read transaction;
    its counters and source launch cache are private.
    """
    from .continuous_producer_v0_2 import LiveContinuousProducerV02, FEATURES, accepted
    require(type(producer) is LiveContinuousProducerV02 and not producer._poisoned
        and not producer.conn.in_transaction, "OPERATIONS_PRODUCER_READ_CUT_REQUIRED")
    observed = copy.copy(producer)
    observed.metrics = dict(producer.metrics)
    observed.market_source = copy.copy(producer.market_source)
    producer.conn.execute("BEGIN")
    try:
        manifest = observed._validate_checkpoint()
        require(manifest["generation"] == producer._generation, "OPERATIONS_PRODUCER_GENERATION_CHANGED")
        observed.market_source._launch_by_mint = dict(manifest["launches"])
        no_t0 = {row[0] for row in producer.conn.execute(f"SELECT DISTINCT mint FROM {FEATURES}")}
        for row in producer.conn.execute(f"SELECT mint,event_json FROM {FEATURES}"):
            event = accepted._quote_event_from_json(row["event_json"])
            if event.base.event_type.value in ("BUY", "SELL"):
                no_t0.discard(row["mint"])
        usage = observed.last_profile_usage
        feature_bytes = producer.conn.execute(
            f"SELECT coalesce(sum(length(CAST(event_json AS BLOB))),0) FROM {FEATURES}").fetchone()[0]
        metrics = {
            "AGGREGATE_FEATURE_BYTES": feature_bytes, "HOTTEST_FEATURE_BYTES": usage["hottest_bytes"],
            "UNFINISHED_MINTS": usage["active_mints"],
            "NO_T0_MINTS": len(no_t0),
            "IDENTITY_ROWS": usage["launch_rows"], "TOMBSTONE_ROWS": usage["retired_rows"],
            "PRODUCER_PENDING_ROWS": usage["pending_rows"],
            "RETAINED_EVENTS": usage["retained_events"], "HOTTEST_EVENTS": usage["hottest_events"],
            "RETAINED_SERIALIZED_BYTES": usage["retained_bytes"],
            "PRODUCER_PENDING_BYTES": usage["pending_bytes"], "HISTORY_BYTES": usage["history_bytes"],
        }
        checkpoint_digest, metrics["CHECKPOINT_BYTES"] = producer.conn.execute(
            "SELECT manifest_digest,length(CAST(manifest_json AS BLOB)) FROM live_producer_checkpoint_v0_1 WHERE singleton=1").fetchone()
        return metrics, manifest, checkpoint_digest
    finally:
        producer.conn.execute("ROLLBACK")


class OperationsMonitor:
    def __init__(self, started, configuration, *, source_binding, source_profile, producer_profile):
        self._in_step, self._step_unavailable = False, False
        self.started, self.configuration = started, configuration
        self.source_binding, self.source_profile, self.producer_profile = source_binding, source_profile, producer_profile
        self.last = unavailable("OPERATIONS_PROFILE_CONFIGURATION_REQUIRED")
        self.last_metrics = ()
        self.backlog_age_applicable = None
        self.store = None
        if configuration is None:
            return
        try:
            require(type(configuration) is MonitorConfiguration
                and configuration.policy.reviewed_configuration_digest == configuration.binding_for(started.audit.identity)
                and configuration.runtime_code_digest == started.audit.identity.runtime_code_digest,
                "OPERATIONS_MONITOR_CONFIGURATION_CONFLICT")
            self.store = DegradationStore(configuration.path, started.runtime.ledger.domain, configuration.policy)
            self.last = alert_view(self.store.snapshot())
        except ERRORS:
            self.last = unavailable()

    def snapshot(self):
        if self.store is None:
            return self.last
        try:
            self.last = alert_view(self.store.snapshot())
        except ERRORS:
            self.last = unavailable()
        return self.last

    def note_work(self, result, sample):
        """Only an actual C2 gate that left durable unsent BUY work is an incident."""
        if (self.store is None or result.work != "OPERATIONS_ENTRY_HELD"
                or result.reason != "CURRENT_DEGRADATION_ENTRY_HOLD" or result.attempt_id is None):
            return self.last
        runtime = self.started.runtime
        try:
            with runtime._ownership.mutation_guard(runtime.ledger.domain), runtime.ledger._trusted_read():
                attempt = runtime.ledger.attempt(result.attempt_id)
                require(attempt is not None and attempt.preparation.action_id == result.action_id
                    and runtime.ledger.action(result.action_id).side == "BUY" and attempt.lane_held)
                require(attempt.recorded_stage in ("PREPARED", "EXACT_SIMULATED", "AUTHORIZED", "SIGNED_DURABLE"))
                now_us = utc_microseconds(sample.utc_upper_utc)
                witness = content_fingerprint((asdict(attempt), asdict(runtime._ownership.fence), sample.content_digest))
                _record(self.store, "UNSENT_ATTEMPT_RECOVERY_REQUIRED", result.attempt_id, witness, now_us)
                self.last = alert_view(self.store.advance(now_us=now_us))
        except ERRORS:
            self.last = unavailable()
        return self.last

    def _host_metrics(self, resources, now_us, owner_digest):
        config = self.configuration
        values = {metric: None for metric in HOST_METRICS}
        try:
            observed = resources() if callable(resources) else None
            require(type(observed) is HostObservations and observed.host_identity_digest == config.host_identity_digest
                and observed.configuration_digest == config.policy.reviewed_configuration_digest
                and observed.owner_digest == owner_digest)
            for item in observed.metrics:
                valid = item.observed_us <= now_us
                if item.metric in ("HOST_RSS_BYTES", "HOST_DISK_RESERVE_BYTES"):
                    valid = valid and now_us-item.observed_us <= config.resource_max_age_us
                elif item.metric == "PROTECTIVE_STEP_US":
                    valid = valid and config.protective_qualification_digest is not None \
                        and item.evidence_digest == config.protective_qualification_digest
                if valid:
                    values[item.metric] = item.value
            return values, content_fingerprint(observed)
        except ERRORS:
            return values, content_fingerprint(("HOST_EVIDENCE_UNAVAILABLE", owner_digest))

    @contextmanager
    def step_window(self):
        """Bound cumulative journal contention; never reuse a healthy ENTRY cut."""
        self._in_step, self._step_unavailable = True, False
        try:
            with self.store.contention_window() if self.store is not None else nullcontext():
                yield
        finally:
            self._in_step, self._step_unavailable = False, False

    def observe(self, sample, **facts):
        if self._in_step and self._step_unavailable:
            return self.last
        with self.store.contention_window() if self.store is not None else nullcontext():
            result = self._observe(sample, **facts)
        if self._in_step and result.unavailable_code is not None:
            self._step_unavailable = True
        return result

    def _observe(self, sample, *, entry=None, reduction=None, resources=None, readiness=None, source_cut_utc=None):
        runtime, config = self.started.runtime, self.configuration
        if self.store is None:
            return self.last
        now_us = utc_microseconds(sample.utc_upper_utc)
        try:
            self.store.snapshot()
        except ERRORS:
            self.last = unavailable()
            return self.last
        owner_valid = False
        owner = runtime._ownership
        try:
            require(type(owner) is OperationsOwnership and owner.fence == self.started.audit.owner_fence)
            # Every record and recovery is serialized against original stop/fence
            # transitions. A stale owner cannot write a clearing observation.
            with owner.mutation_guard(runtime.ledger.domain), runtime.ledger._trusted_read():
                owner_valid = True
                self._observe_current(sample, entry, reduction, resources, readiness, source_cut_utc)
                self.last = alert_view(self.store.advance(now_us=now_us))
        except ERRORS:
            if owner_valid:
                self.last = unavailable("OPERATIONS_CURRENT_EVIDENCE_UNAVAILABLE")
                return self.last
            # No recovery on failed evidence. Host-side sampling is independently
            # serialized against controls and records only an active owner alert.
            try:
                self.last = _control_alert(owner._store, self.store, "OWNER_FENCE_UNPROVEN", now_us)
            except ERRORS:
                self.last = unavailable()
        return self.last

    def _observe_current(self, sample, entry, reduction, resources, readiness, source_cut_utc):
        from .operations_readiness_v0_1 import _current
        runtime, store, config = self.started.runtime, self.store, self.configuration
        repo, now_us = runtime.ledger, utc_microseconds(sample.utc_upper_utc)
        owner_digest = content_fingerprint(asdict(runtime._ownership.fence))
        cut = repo.consumer_snapshot()
        readiness = _current(runtime, sample, entry, reduction) if readiness is None else readiness
        require(readiness.owner_fence == runtime._ownership.fence and readiness.reconstruction is not None
            and readiness.reconstruction.consumer_cut == cut["consumer_cut"], "OPERATIONS_READINESS_CUT_CHANGED")
        common = content_fingerprint((asdict(cut["consumer_cut"]), owner_digest, sample.content_digest))
        control_subject = _subject(repo.domain, "OPERATIONS_CONTROL", repo.domain.binding_digest)
        for code in readiness_conditions(readiness):
            if code in ("SOURCE_TRUTH_UNAVAILABLE", "PRODUCER_INTEGRITY_UNAVAILABLE"):
                continue  # Own exact current source/producer evidence below.
            subject = (_subject(repo.domain, "PROTECTION", readiness.protective.root_id)
                if code == "PROTECTIVE_ACTION_UNAVAILABLE" else control_subject)
            _record(store, code, subject, common, now_us)
        if not any(reason.startswith("CLOCK_") for reason in (*readiness.entry.reasons, *readiness.protective.reasons)) \
                and sample.status == "QUALIFIED":
            _record(store, "CLOCK_UNPROVEN", control_subject, common, now_us, healthy=True)
        if readiness.protective.state in ("READY", "SATISFIED_AWAITING_RETIREMENT") and readiness.protective.root_id:
            _record(store, "PROTECTIVE_ACTION_UNAVAILABLE", _subject(repo.domain, "PROTECTION", readiness.protective.root_id),
                common, now_us, healthy=True)
        metrics, producer_witness, manifest = {}, None, None
        producer_subject = _subject(repo.domain, "PRODUCER", (self.source_binding.source_identity, asdict(self.producer_profile)))
        try:
            require(runtime.producer is not None and runtime.producer.profile == self.producer_profile)
            metrics, manifest, checkpoint = _producer_cut(runtime.producer)
            producer_witness = content_fingerprint((common, checkpoint, manifest["cursor"], manifest["generation"]))
        except ERRORS:
            _record(store, "PRODUCER_INTEGRITY_UNAVAILABLE", producer_subject, common, now_us)
        source_subject = _subject(repo.domain, "SOURCE", (self.source_binding.source_identity, self.source_profile.fingerprint))
        verdict = None
        try:
            source = runtime.source
            require(source is not None)
            if source.binding != self.source_binding or source.profile != self.source_profile:
                _record(store, "SOURCE_IDENTITY_OR_HISTORY_BROKEN", source_subject, common, now_us)
            else:
                previous = source.latest_record()
                require(previous is not None and runtime.producer is not None)
                verdict = CollectorSourceAdapter(runtime.producer.market_source.db_path).observe(
                    self.source_binding, self.source_profile, observed_at_utc=sample.utc_upper_utc,
                    requested_cut_utc=source_cut_utc or previous[1].snapshot.requested_cut_utc, previous=previous[1])
                require(source.latest_record() == previous, "OPERATIONS_SOURCE_CUT_CHANGED")
                witness = content_fingerprint((common, verdict.content_digest))
                broken = bool(set(verdict.reasons) & INTEGRITY_REASONS or verdict.progress.integrity_reasons)
                if broken:
                    _record(store, "SOURCE_IDENTITY_OR_HISTORY_BROKEN", source_subject, witness, now_us)
                _record(store, "SOURCE_TRUTH_UNAVAILABLE", source_subject, witness, now_us,
                    healthy=not broken and verdict.disposition == "HEALTHY" and producer_witness is not None)
        except ERRORS:
            _record(store, "SOURCE_TRUTH_UNAVAILABLE", source_subject, common, now_us)
        backlog_count = None
        try:
            require(manifest is not None and verdict is not None)
            fence = next(item.rowid for item in verdict.snapshot.cursors if item.table == "pump_events")
            raw_path = Path(runtime.producer.market_source.db_path).resolve()
            with closing(sqlite3.connect(raw_path.as_uri()+"?mode=ro", uri=True)) as raw:
                raw.execute("BEGIN")
                require(raw.execute("SELECT max(rowid) FROM pump_events").fetchone()[0] == fence)
                backlog_count, oldest = raw.execute(
                    "SELECT count(*),min(decoded_at_utc) FROM pump_events WHERE rowid>? AND rowid<=?",
                    (manifest["cursor"], fence)).fetchone()
                require(fence >= manifest["cursor"])
            metrics["OLDEST_UNCONSUMED_AGE_US"] = None if oldest is None else now_us-utc_microseconds(oldest)
        except (StopIteration, *ERRORS):
            metrics["OLDEST_UNCONSUMED_AGE_US"] = None
            backlog_count = None
        self.backlog_age_applicable = None if backlog_count is None else backlog_count > 0
        metrics["LEDGER_TAIL_ROWS"], metrics["LEDGER_TAIL_PAYLOAD_BYTES"] = repo._conn.execute(
            "SELECT count(*),coalesce(sum(length(CAST(payload_json AS BLOB))),0) FROM ledger_commits WHERE seq>0 AND seq<=?",
            (cut["consumer_cut"].revision,)).fetchone()
        host, host_witness = self._host_metrics(resources, now_us, owner_digest)
        metrics.update(host)
        configured = {item.metric for item in config.policy.resource_limits}
        observed_metrics = REQUIRED_METRICS | configured
        self.last_metrics = tuple(sorted((metric, metrics.get(metric)) for metric in observed_metrics))
        coverage = _subject(repo.domain, "PROFILE_COVERAGE", config.policy.content_digest)
        _record(store, "PROFILE_UNRESOLVED", coverage, common, now_us, healthy=REQUIRED_METRICS <= configured)
        for metric in sorted(observed_metrics):
            subject = _subject(repo.domain, "RESOURCE", (config.policy.content_digest, metric))
            witness = content_fingerprint((common, producer_witness, host_witness, metric, metrics.get(metric), backlog_count))
            if metric == "OLDEST_UNCONSUMED_AGE_US" and backlog_count == 0 and metric in configured:
                condition = None  # Positive same-cut empty count, NOT a zero age.
            else:
                condition = resource_condition(config.policy, metric, metrics.get(metric),
                    configuration_digest=config.policy.reviewed_configuration_digest)
            for code in ("PROFILE_UNRESOLVED", "RESOURCE_EXCEEDED"):
                if condition == code:
                    _record(store, code, subject, witness, now_us)
                elif condition is None:
                    _record(store, code, subject, witness, now_us, healthy=True)
        self._attempts(cut, common, now_us)
        require(repo.consumer_snapshot()["consumer_cut"] == cut["consumer_cut"], "OPERATIONS_LEDGER_CUT_CHANGED")

    def _attempts(self, cut, common, now_us):
        repo, store = self.started.runtime.ledger, self.store
        # Incident subjects for attempts are their public original attempt digest.
        # Re-query active/recovered subjects as well as pending IDs, so absence
        # from pending is never used as proof that an UNKNOWN attempt resolved.
        ids = set(cut["pending_attempts"])
        ids.update(row.subject_digest for row in store.snapshot().conditions
            if row.condition in ("SEND_UNKNOWN", "FINALITY_UNRESOLVED", "SETTLEMENT_UNKNOWN"))
        for attempt_id in sorted(ids):
            attempt = repo.attempt(attempt_id)
            if attempt is None:
                continue
            finality = repo.attempt_finality(attempt_id)
            witness = content_fingerprint((common, asdict(attempt), asdict(finality)))
            positive = finality.positive_finality is not None and not finality.quarantined \
                and finality.proof_evidence_digest is not None
            if positive:
                for code in ("SEND_UNKNOWN", "FINALITY_UNRESOLVED"):
                    _record(store, code, attempt_id, witness, now_us, healthy=True)
            elif attempt.recorded_stage in ("SEND_CLAIMED", "OBSERVING", "UNKNOWN"):
                _record(store, "SEND_UNKNOWN", attempt_id, witness, now_us)
            if finality.quarantined or finality.provisional_seen:
                _record(store, "FINALITY_UNRESOLVED", attempt_id, witness, now_us, healthy=positive)
            settlement_unknown = ("UNKNOWN" in attempt.economic_disposition or "UNPARSED" in finality.disposition)
            if settlement_unknown:
                _record(store, "SETTLEMENT_UNKNOWN", attempt_id, witness, now_us)
            elif attempt.economically_applied and not attempt.custody_quarantined and positive:
                _record(store, "SETTLEMENT_UNKNOWN", attempt_id, witness, now_us, healthy=True)
        if cut["funding"].quarantine_reasons or cut["funding"].baseline_receipt_digest is None:
            _record(store, "CUSTODY_TRUTH_UNAVAILABLE", _subject(repo.domain, "CUSTODY", repo.domain.binding_digest), common, now_us)


def _control_alert(controls, alerts, code, now_us):
    # Original control writer serializes host facts and the corresponding alert;
    # this does not take, adopt, reset or release a Runtime owner.
    with controls._connection() as conn:
        conn.execute("BEGIN IMMEDIATE")
        state = controls._read(conn)
        require(now_us >= state["last_control_us"])
        subject = content_fingerprint((controls.domain_id, "OPERATIONS_CONTROL", controls.binding_digest))
        witness = content_fingerprint((code, state))
        _record(alerts, code, subject, witness, now_us)
        return alert_view(alerts.advance(now_us=now_us))


def observe_supervisor(configuration, controls, facts, *, now_us):
    config = configuration.get("degradation_config")
    if config is None:
        return unavailable("OPERATIONS_PROFILE_CONFIGURATION_REQUIRED")
    try:
        require(type(config) is MonitorConfiguration
            and config.policy.reviewed_configuration_digest == config.binding_for(configuration["expected_identity"])
            and config.runtime_code_digest == configuration["expected_identity"].runtime_code_digest)
        alerts = DegradationStore(config.path, configuration["domain"], config.policy)
        codes = supervision_conditions(facts)
        try:
            state = controls.snapshot()
        except ERRORS:
            # The independent alert store can retain a failure even when its
            # economic/control owner cannot be read. This is active-only; no
            # unreadable-state observation can recover anything or grant work.
            subject = content_fingerprint((controls.domain_id, "OPERATIONS_CONTROL", controls.binding_digest))
            witness = content_fingerprint(("CONTROL_UNREADABLE", facts.state, facts.generation))
            _record(alerts, "SUPERVISOR_HELD", subject, witness, now_us)
            return alert_view(alerts.advance(now_us=now_us))
        if state["stopped"]:
            codes += ("OPERATOR_STOPPED",)
        if state["exhausted"]:
            codes += ("RESTART_EXHAUSTED",)
        result = alert_view(alerts.snapshot())
        for code in sorted(set(codes)):
            result = _control_alert(controls, alerts, code, now_us)
        return alert_view(alerts.advance(now_us=now_us)) if not codes else result
    except ERRORS:
        return unavailable()
