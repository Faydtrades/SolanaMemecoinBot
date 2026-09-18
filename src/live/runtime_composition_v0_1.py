"""C2/C3 finite production Runtime composition; accepted components own semantics.

One serialized step selects protection, then truth reconciliation/application,
then entry with actual V1 capacity. Concrete external facts/transports configure
existing consumers, never replace admission, execution, settlement or protection.
Satisfied protection retires through Ledger before fresh candidate admission.
The separate C4 cold factory reconstructs these pointers from durable owners.
The domain fixes LIVE or NO_BROADCAST DRY capability for the lifetime of the root.
"""
from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass, replace
from datetime import datetime

from phase5.shadow_domain_v0_1 import content_fingerprint
from .authority_controls_v0_1 import require, TrustedClockSample, clock_reasons
from .authority_admission_v0_1 import EntryRequest
from .authority_message_control_v0_1 import MessageStageRequest, FreshStageConsumption
from .candidate_handoff_v0_1 import CandidateHandoffV01
from .continuous_producer_v0_2 import LiveContinuousProducerV02
from .evidence_store_v0_1 import SourceEvidenceStore
from .source_health_v0_1 import CollectorSourceAdapter, ZERO_DIGEST, utc, evaluate_source
from .ledger_repository_v0_1 import LedgerRepository
from .ledger_ports_v0_1 import RetirementInput
from .ledger_custody_v0_1 import REPLACEABLE
from .ledger_settlement_v0_1 import WalletSupportInput
from .ledger_actions_v0_1 import utc_microseconds
from .execution_message_v0_1 import produce_exact_message, prepare_exact_message
from .execution_readonly_v0_1 import ExecutionReadOnlyRpc
from .execution_signer_v0_1 import AutonomousLocalSigner
from .execution_send_v0_1 import SolanaSendTransport, persist_signed_envelope, send_exact
from .execution_reconciliation_v0_1 import observe_attempt_coverage, observe_attempt_finality
from .public_rpc_v0_1 import PublicReadOnlyRpc
from .position_controller_v0_1 import bind_actual_position
from .exit_observation_v0_1 import capture_exit_evidence, enrich_exit_evidence, commit_exit_evaluation, EVIDENCE, EVALUATION
from .protective_obligation_v0_1 import ensure_protective_obligation, stage_protective_sell
from .protective_outcome_v0_1 import protective_outcome
from .runtime_dry_v0_1 import run_dry, finish_interrupted_dry
from .operations_ownership_v0_1 import OperationsOwnership, mutation_guard, OperationsOwnershipError

VERSION = "live_runtime_composition_v0.1"


@dataclass(frozen=True, slots=True)
class EntryFacts:
    venue: str
    token_program: str
    wallet: object
    protocol: object = None


def capture_source_at_completion(adapter, binding, profile, sample, clock, *, cut, previous=None, **bounds):
    """Retain the original read/cut; timestamp only after that read completes."""
    snapshot = adapter.capture(binding, profile, observed_at_utc=sample.utc_upper_utc,
        requested_cut_utc=cut, previous=previous, **bounds)
    completed = clock()
    require(type(completed) is TrustedClockSample and completed.status == "QUALIFIED"
        and utc_microseconds(completed.utc_upper_utc) >= utc_microseconds(sample.utc_upper_utc),
        "RUNTIME_SOURCE_COMPLETION_CLOCK_REQUIRED")
    snapshot = replace(snapshot, observed_at_utc=completed.utc_upper_utc)
    return completed, evaluate_source(binding, profile, snapshot, previous=previous)


def entry_at_clock(entry, sample):
    """Re-evaluation input, never acceptance: original consumers still adjudicate."""
    if entry is None:
        return None
    require(type(entry) is EntryFacts and type(entry.wallet) is WalletSupportInput,
        "RUNTIME_ORIGINAL_ENTRY_WALLET_REQUIRED")
    require(utc_microseconds(entry.wallet.evaluated_at_utc) <= utc_microseconds(sample.utc_upper_utc),
        "RUNTIME_WALLET_EVALUATION_CLOCK_REGRESSION")
    wallet = replace(entry.wallet, evaluated_at_utc=sample.utc_upper_utc)
    protocol = entry.protocol
    if protocol is not None:
        from .pump_protocol_compatibility_v0_1 import PumpProtocolObservation
        require(type(protocol) is PumpProtocolObservation and protocol.wallet_support_digest == entry.wallet.digest,
            "RUNTIME_ORIGINAL_PROTOCOL_WALLET_BINDING_REQUIRED")
        protocol = replace(protocol, wallet_support_digest=wallet.digest)
    return replace(entry, wallet=wallet, protocol=protocol)


def admission_clock(clock, state, source_observed_at, entry=None):
    """One policy-bounded overlap wait; uncertain/future knowledge fails closed."""
    sample = clock()
    require(type(sample) is TrustedClockSample and not clock_reasons(state, sample),
        "RUNTIME_ADMISSION_QUALIFIED_CLOCK_REQUIRED")
    known = [utc_microseconds(source_observed_at)]
    if entry is not None:
        known.append(utc_microseconds(entry.wallet.observation.observed_at_utc))
        if entry.protocol is not None:
            known.append(utc_microseconds(entry.protocol.observed_at_utc))
    target = max(known)
    lower, upper = utc_microseconds(sample.utc_lower_utc), utc_microseconds(sample.utc_upper_utc)
    require(target <= upper, "RUNTIME_ADMISSION_FUTURE_KNOWLEDGE")
    if target > lower:
        # The validated policy interval bounds this single wait. Never alter
        # a provider observation or infer a clock from the evidence timestamp.
        time.sleep((target-lower)/1000000)
        sample = clock()
    require(type(sample) is TrustedClockSample and not clock_reasons(state, sample)
        and utc_microseconds(sample.utc_lower_utc) >= target,
        "RUNTIME_ADMISSION_KNOWLEDGE_UNPROVEN")
    return sample


@dataclass(frozen=True, slots=True)
class ExecutionPorts:
    rpc: ExecutionReadOnlyRpc
    wallet: object
    quote_policy: object
    plan_policy: object
    compute: object
    signer: AutonomousLocalSigner
    transport: SolanaSendTransport
    now_us: object

    def __post_init__(self):
        require(type(self.rpc) is ExecutionReadOnlyRpc and type(self.signer) is AutonomousLocalSigner
            and type(self.transport) is SolanaSendTransport and callable(self.now_us),
            "RUNTIME_CONCRETE_EXECUTION_PORTS_REQUIRED")


@dataclass(frozen=True, slots=True)
class DryExecutionPorts:
    """Public construction/simulation inputs; no mutation capability exists here."""
    rpc: ExecutionReadOnlyRpc
    wallet: object
    quote_policy: object
    plan_policy: object
    compute: object
    now_us: object
    qualification_interrupt_after_simulation: bool = False
    wallet_after_venue: object = None

    def __post_init__(self):
        require(type(self.rpc) is ExecutionReadOnlyRpc and callable(self.now_us)
            and type(self.qualification_interrupt_after_simulation) is bool
            and (self.wallet_after_venue is None or callable(self.wallet_after_venue)),
            "RUNTIME_DRY_READONLY_PORTS_REQUIRED")


@dataclass(frozen=True, slots=True)
class RuntimeStep:
    work: str
    reason: str
    root_id: str | None = None
    action_id: str | None = None
    attempt_id: str | None = None
    receipt_key: str | None = None
    queued_roots: tuple[str, ...] = ()


class RuntimeCompositionV01:
    """Bounded in-process arbitration over one existing V1 economic domain.

    Input handles are already installed and verified by their semantic owners.
    No key loading, policy selection/installation, networking loop or restart
    claim occurs here. A caller supplies only external evidence/resources for
    the work selected by step; omitting a resource holds that priority intact.
    """
    # Accepted pre-Operations cold factory bypasses __init__; it remains unowned.
    _ownership = None
    _operations_degradation = None

    def __init__(self, producer, handoff, ledger, source_store, *, batch_rows=32, page_rows=32, queued_roots=64,
                 ownership=None):
        require(type(producer) is LiveContinuousProducerV02 and type(handoff) is CandidateHandoffV01
            and type(ledger) is LedgerRepository and ledger.domain.mode in ("LIVE", "DRY")
            and type(source_store) is SourceEvidenceStore and handoff.producer is producer
            and handoff.ledger is ledger and source_store.binding == handoff.binding,
            "RUNTIME_ORIGINAL_COMPONENT_BINDINGS_REQUIRED")
        require(type(batch_rows) is int and 1 <= batch_rows <= producer.profile.batch_rows
            and type(page_rows) is int and 1 <= page_rows <= producer.profile.delivery_page_rows
            and type(queued_roots) is int and page_rows <= queued_roots <= 1024,
            "RUNTIME_FINITE_DISPATCH_PROFILE_REQUIRED")
        self.producer, self.handoff, self.ledger, self.source = producer, handoff, ledger, source_store
        self.batch_rows, self.page_rows, self.queue_limit = batch_rows, page_rows, queued_roots
        self._queue = []
        self._entry_action_id = None
        self._binding = None
        self._chain_key = None
        require(ownership is None or type(ownership) is OperationsOwnership
            and ownership.fence.domain_id == ledger.domain.economic_domain_id
            and ownership.fence.binding_digest == ledger.domain.binding_digest,
            "RUNTIME_OPERATIONS_DOMAIN_OWNER_REQUIRED")
        self._ownership = ownership
        self._fixed_domain = ledger.domain
        self._dry_recovery = False

    @property
    def capability(self):
        return "NO_BROADCAST" if self._fixed_domain.mode == "DRY" else "LIVE"

    @property
    def queued_roots(self):
        return tuple(self._queue)

    @property
    def position_binding(self):
        return self._binding

    def _key(self, kind, identity, at):
        return "runtime-"+kind+"-"+content_fingerprint({"version": VERSION, "kind": kind,
            "domain": self.ledger.domain.economic_domain_id, "identity": identity, "at": at,
            "cut": self.ledger.write_fence().last_receipt_digest})

    def _result(self, work, reason, *, action=None, attempt=None, key=None, root=None):
        return RuntimeStep(work, reason, root if action is None else action.root_id,
            None if action is None else action.action_id, attempt, key, self.queued_roots)

    def _source_health(self, sample, cut, clock):
        cut = utc(cut or sample.utc_lower_utc)
        prior = self.source.latest_record()
        previous = None if prior is None else prior[1]
        _, observed = capture_source_at_completion(CollectorSourceAdapter(self.producer.market_source.db_path),
            self.handoff.binding, self.source.profile, sample, clock, cut=cut, previous=previous,
            max_raw_bytes=getattr(self, "_resource_raw_bytes", None),
            reserve_rows=getattr(self, "_resource_capture_reservation", None))
        append_guard = getattr(self, "_resource_source_append_guard", None)
        require(append_guard is None or append_guard(observed), "RUNTIME_SOURCE_RECORD_CAPACITY_HELD")
        self.source.append(observed, expected_previous_digest=ZERO_DIGEST if previous is None else previous.content_digest)
        return observed

    def _source_page(self, sample, cut, clock):
        if self.producer is None:
            return "SOURCE_UNAVAILABLE_OR_PROFILE_EXHAUSTED"
        try:
            # Drain the accepted minimum prepared watermark, including a fence
            # already retained by the producer. Never skip it with a new clock.
            with self.producer._transaction():
                first = self.producer.conn.execute("SELECT timer_key FROM paper_fp_binding_prepared_timers_v0_1 "
                    "WHERE status<>'COMPLETE' ORDER BY source_watermark_p1_rowid,timer_key LIMIT 1").fetchone()
            timer = first[0] if first else self.producer.prepare_clock_tick(datetime.fromisoformat(sample.utc_upper_utc))
            # Optional trusted T010 reservation narrows only this source unit;
            # the accepted dispatch ceiling and normal Runtime stay unchanged.
            reserved = getattr(self, "_resource_batch_rows", self.batch_rows)
            require(type(reserved) is int and 0 < reserved <= self.batch_rows,
                "RUNTIME_DURABLE_SOURCE_RESERVATION_REQUIRED")
            raw_bytes = getattr(self, "_resource_raw_bytes", None)
            if raw_bytes is None:
                self.producer.process_next_batch(batch_size=reserved)
            else:
                self.producer.process_next_batch(batch_size=reserved, max_raw_bytes=raw_bytes)
            self.producer.execute_prepared_timer(timer)
            self._source_health(sample, cut, clock)
            available = self.queue_limit-len(self._queue)
            if available <= 0:
                return "QUEUE_FULL_SOURCE_ADVANCED"
            page = self.handoff.deliver_page(limit=min(self.page_rows, available))
        except (ValueError, RuntimeError, sqlite3.DatabaseError, OSError):
            return "SOURCE_UNAVAILABLE_OR_PROFILE_EXHAUSTED"
        for root in page.candidate_roots:
            from .ledger_actions_v0_1 import NON_ACCEPTANCE, TERMINAL_INBOX
            if (root not in self._queue and self.ledger.authority_acceptance(root) is None
                    and self.ledger.inbox_disposition(root) not in NON_ACCEPTANCE | TERMINAL_INBOX):
                self._queue.append(root)
        return "SOURCE_PAGE_CONSUMED"

    def _exit_key(self, kind, identity):
        return "runtime-"+kind+"-"+content_fingerprint({"version": VERSION,
            "binding": self._binding.binding_id, "identity": identity})

    def _protect(self, sample, proofs):
        binding = self._binding
        at = sample.utc_upper_utc
        records = self.ledger.exit_records(binding.binding_id)
        evaluations = [r for r in records if r.kind == EVALUATION]
        previous = evaluations[-1] if evaluations else None
        # Reuse a committed trigger; UNKNOWN and hard stop need truth work,
        # not an ever-growing history of identical protective evaluations.
        if previous is not None and previous.payload["state"]["state"] == "DUE":
            ensure_protective_obligation(self.ledger, binding, recorded_at_utc=at)
            return protective_outcome(self.ledger, binding)
        due = utc_microseconds(at) >= binding.fallback_fire_boundary_us
        evidence = [r for r in records if r.kind == EVIDENCE]
        sources = [r.payload["source"] for r in evidence if r.payload["source"] is not None]
        if not due:
            try:
                through = -1 if self.producer is None else self.producer.next_event_sequence-1
                after = sources[-1]["through_sequence"] if sources else -1
                proof_ids = tuple((seq, pair[0].content_digest, pair[1].content_digest)
                    for seq, pair in sorted((proofs or {}).items()))
                if through > after:
                    identity = (after, through, proof_ids)
                    key = self._exit_key("exit-evidence", identity)
                    prior = self.ledger.exit_record(key)
                    if prior is None:
                        capture_exit_evidence(self.ledger, binding, handoff=self.handoff, proofs=proofs or {},
                            command_id=key, recorded_at_utc=at)
                elif proofs:
                    key = self._exit_key("exit-enrichment", proof_ids)
                    if self.ledger.exit_record(key) is None:
                        enrich_exit_evidence(self.ledger, binding, proofs=proofs, command_id=key, recorded_at_utc=at)
            except (ValueError, RuntimeError, sqlite3.DatabaseError, OSError):
                # Retain the committed cut; the original fallback remains usable.
                pass
        records = self.ledger.exit_records(binding.binding_id)
        evidence = [r for r in records if r.kind == EVIDENCE]
        sequence = max((r.sequence for r in evidence), default=0)
        # Future retained observations become eligible only as actual clock
        # knowledge crosses their original timestamp, not on every poll.
        through = -1 if previous is None else previous.payload["state"]["through_event_sequence"]
        newly_timed = any(o["event_sequence"] > through and utc_microseconds(o["normalized"]["observed_at"]) <= utc_microseconds(at)
            for r in evidence if r.payload["source"] is not None for o in r.payload["source"]["observations"])
        if previous is None or due or sequence > previous.payload["highest_evidence_sequence"] or newly_timed:
            key = self._exit_key("exit-evaluate", (sequence, at, None if previous is None else previous.content_digest))
            commit_exit_evaluation(self.ledger, binding, command_id=key, timer_fence_utc=at)
        ensure_protective_obligation(self.ledger, binding, recorded_at_utc=at)
        return protective_outcome(self.ledger, binding)

    def _owner_current(self):
        try:
            with mutation_guard(self._ownership, self.ledger.domain):
                return True
        except (OperationsOwnershipError, sqlite3.DatabaseError, OSError):
            return False

    def _entry_degradation(self, clock, entry, resources, source_cut_utc=None):
        monitor = self._operations_degradation
        if self.capability == "NO_BROADCAST":
            return monitor is not None and monitor.snapshot().entry_held
        return monitor is not None and monitor.observe(clock(), entry=entry, resources=resources,
            source_cut_utc=source_cut_utc, clock=clock).entry_held

    def _execute(self, action, ports, clock, *, fence, ordinal, entry=None, operations_resources=None):
        require(self.capability == "LIVE", "RUNTIME_LIVE_EXECUTION_CAPABILITY_REQUIRED")
        if not self._owner_current():
            return self._result("OPERATIONS_HELD", "CURRENT_OWNER_REQUIRED", action=action)
        if action.side == "BUY" and self._entry_degradation(clock, entry, operations_resources):
            return self._result("OPERATIONS_ENTRY_HELD", "CURRENT_DEGRADATION_ENTRY_HOLD", action=action)
        if ports is None:
            return self._result("NEED_EXECUTION", "ORIGINAL_ACTION_AWAITS_EXTERNAL_PORTS", action=action)
        require(type(ports) is ExecutionPorts, "RUNTIME_CONCRETE_EXECUTION_PORTS_REQUIRED")
        production = produce_exact_message(self.ledger, action.action_id, ports.rpc, ports.wallet,
            ports.quote_policy, ports.plan_policy, ports.compute, clock=clock, now_us=ports.now_us)
        stored = prepare_exact_message(self.ledger, production, fence=fence, ordinal=ordinal)
        prep = stored.preparation
        source = self.source if action.side == "BUY" else None
        def consume(stage):
            attempt = self.ledger.attempt(prep.attempt_id)
            sample = clock()
            request = MessageStageRequest(self._key(stage.lower(), prep.attempt_id, sample.utc_upper_utc),
                action.action_id, prep.attempt_id, attempt.revision, stage, 0,
                None if stage == "SIGN" else attempt.primary_signature)
            return self.ledger.consume_authority_message_stage(request, sample, source, production.original.evidence,
                fence=self.ledger.write_fence())
        if action.side == "BUY" and self._entry_degradation(clock, entry, operations_resources):
            return self._result("OPERATIONS_ENTRY_HELD", "CURRENT_DEGRADATION_ENTRY_HOLD", action=action, attempt=prep.attempt_id)
        sign = consume("SIGN")
        if type(sign) is not FreshStageConsumption:
            return self._result("HELD", "AUTHORITY_SIGN_NOT_FRESHLY_GRANTED", action=action, attempt=prep.attempt_id,
                key=sign.original.request.command_id)
        envelope = ports.signer.sign_exact(self.ledger, production, sign, source_store=source, clock=clock,
            ownership=self._ownership)
        persist_signed_envelope(self.ledger, envelope, fence=self.ledger.write_fence())
        if action.side == "BUY" and self._entry_degradation(clock, entry, operations_resources):
            return self._result("OPERATIONS_ENTRY_HELD", "CURRENT_DEGRADATION_ENTRY_HOLD", action=action, attempt=prep.attempt_id)
        send = consume("SEND")
        if type(send) is not FreshStageConsumption:
            return self._result("HELD", "AUTHORITY_SEND_NOT_FRESHLY_GRANTED", action=action, attempt=prep.attempt_id,
                key=send.original.request.command_id)
        observation = send_exact(self.ledger, send, ports.transport, source_store=source, clock=clock,
            ownership=self._ownership)
        return self._result("SUBMISSION_OBSERVED", observation.outcome, action=action, attempt=prep.attempt_id)

    def _truth(self, attempt_id, sample, truth_rpc, wallet):
        attempt = self.ledger.attempt(attempt_id)
        action = self.ledger.action(attempt.preparation.action_id)
        if attempt.primary_signature is None:
            return self._result("HELD", "UNSIGNED_OR_CONSUMED_ATTEMPT_REQUIRES_LATER_RECOVERY", action=action, attempt=attempt_id)
        chain = None if self._chain_key is None else self.ledger.chain_receipt(self._chain_key)
        if chain is not None and chain.attempt_id != attempt_id:
            chain = None
        if chain is None or chain.decision.resulting_state.positive_finality is None:
            if truth_rpc is None:
                return self._result("NEED_RECONCILIATION", "ORIGINAL_SIGNED_ATTEMPT_AWAITS_PUBLIC_TRUTH", action=action, attempt=attempt_id)
            require(type(truth_rpc) is PublicReadOnlyRpc, "RUNTIME_CONCRETE_FINALITY_READ_REQUIRED")
            key = self._key("chain", attempt_id, sample.utc_upper_utc)
            chain = observe_attempt_finality(self.ledger, attempt_id, truth_rpc, ingestion_key=key,
                evaluated_at_utc=sample.utc_upper_utc, clock=lambda: sample.utc_upper_utc, fence=self.ledger.write_fence())
            if (chain.decision.resulting_state.disposition == "UNKNOWN"
                    and chain.observation.root is not None
                    and chain.observation.root.block_height > attempt.preparation.lease.last_valid_block_height):
                key = self._key("coverage", attempt_id, sample.utc_upper_utc)
                chain = observe_attempt_coverage(self.ledger, attempt_id, truth_rpc, ingestion_key=key,
                    evaluated_at_utc=sample.utc_upper_utc, clock=lambda: sample.utc_upper_utc,
                    fence=self.ledger.write_fence())
            self._chain_key = chain.ingestion_key
            return self._result("RECONCILED", chain.decision.resulting_state.disposition, action=action, attempt=attempt_id, key=key)
        if wallet is None and chain.decision.resulting_state.positive_finality != "PROVEN_NON_LANDED":
            return self._result("NEED_APPLICATION", "POSITIVE_TRUTH_REQUIRES_ORIGINAL_WALLET_SUPPORT", action=action, attempt=attempt_id)
        key = self._key("application", attempt_id, sample.utc_upper_utc)
        receipt = self.ledger.apply_settlement(attempt_id, chain_receipt_key=chain.ingestion_key, support=wallet,
            ingestion_key=key, recorded_at_utc=sample.utc_upper_utc, fence=self.ledger.write_fence())
        if action.side == "BUY" and receipt.decision.disposition == "FINALIZED_SUCCESS_APPLIED":
            self._binding = bind_actual_position(self.ledger, position_id=action.position_id, root_id=action.root_id,
                mint=action.mint, selected_track=action.selected_exit_track, policy_digest=action.policy_digest,
                acquisition_application_key=key).binding
            self._entry_action_id = None
        return self._result("APPLIED", receipt.decision.disposition, action=action, attempt=attempt_id, key=key)

    def _retire(self, sample, wallet):
        binding = self._binding
        if wallet is None:
            return self._result("NEED_RETIREMENT", "SATISFIED_REQUIRES_CURRENT_WALLET_SUPPORT", root=binding.root_id)
        context = self.ledger.protective_position_context(binding.position_id, binding.binding_id)
        applications = [r for r in context["applications"]
            if r.decision.disposition == "FINALIZED_SUCCESS_APPLIED" and r.decision.proposal.base_units_delta < 0]
        require(bool(applications), "RUNTIME_RETIREMENT_ACTUAL_REDUCTION_REQUIRED")
        terminal = max(applications, key=lambda r: r.sequence)
        action = self.ledger.action(terminal.decision.proposal.action_id)
        snapshot = context["snapshot"]
        key = self._key("retirement", binding.root_id, sample.utc_upper_utc)
        # This reference identifies the Runtime request, not a new Authority
        # grant. Ledger verifies actual terminal proof and current wallet cut.
        intent = RetirementInput(snapshot["consumer_cut"], binding.root_id,
            self.ledger.reservation(binding.root_id).reservation_id, binding.position_id,
            terminal.attempt_id, "FULLY_REDUCED", wallet.digest, key,
            content_fingerprint({"binding": binding.binding_id, "application": terminal.content_digest,
                "cut": snapshot["consumer_cut"].commit_digest, "wallet": wallet.digest}), sample.utc_upper_utc)
        receipt = self.ledger.retire(intent, wallet, ingestion_key=key, fence=snapshot["fence"])
        if receipt.retirement_disposition == "RETIRED":
            self._binding = self._entry_action_id = self._chain_key = None
        return self._result("RETIREMENT_"+receipt.retirement_disposition,
            ":".join(receipt.retirement_reasons) or "ACTUAL_LEDGER_CAPACITY_RELEASED",
            action=action, attempt=terminal.attempt_id, key=key)

    def _retire_unacquired(self, action, terminal, sample, wallet, snapshot):
        if wallet is None:
            return self._result("NEED_RETIREMENT", "EXPIRED_UNACQUIRED_REQUIRES_CURRENT_WALLET_SUPPORT", action=action)
        if (type(wallet) is not WalletSupportInput or wallet.evaluated_at_utc != sample.utc_upper_utc
                or wallet.required_min_context_slot < snapshot["required_wallet_context_slot"]):
            return self._result("RETIREMENT_WITHHELD", "CURRENT_WALLET_SUPPORT_REQUIRED", action=action)
        reason = ("FAILED_NO_ACQUISITION" if terminal.decision.disposition == "FINALIZED_FAILURE_APPLIED"
                  else "PROVEN_NON_LANDED")
        key = self._key("retirement", action.root_id, sample.utc_upper_utc)
        intent = RetirementInput(snapshot["consumer_cut"], action.root_id,
            self.ledger.reservation(action.root_id).reservation_id, action.position_id,
            terminal.attempt_id, reason, wallet.digest, key,
            content_fingerprint({"action": action.action_id, "application": terminal.content_digest,
                "cut": snapshot["consumer_cut"].commit_digest, "wallet": wallet.digest}), sample.utc_upper_utc)
        receipt = self.ledger.retire(intent, wallet, ingestion_key=key, fence=snapshot["fence"])
        if receipt.retirement_disposition == "RETIRED":
            self._entry_action_id = self._chain_key = None
        return self._result("RETIREMENT_"+receipt.retirement_disposition,
            ":".join(receipt.retirement_reasons) or "ACTUAL_LEDGER_CAPACITY_RELEASED",
            action=action, attempt=terminal.attempt_id, key=key)

    def step(self, *, clock, entry=None, operations_resources=None, **resources):
        require(self.ledger.domain == self._fixed_domain, "RUNTIME_FIXED_DOMAIN_CHANGED")
        if self.capability == "NO_BROADCAST":
            require(all(resources.get(key) is None for key in (
                "truth_rpc", "application_wallet", "retirement_wallet", "exit_proofs", "protective_max_units"))
                and (resources.get("execution") is None or type(resources["execution"]) is DryExecutionPorts),
                "RUNTIME_DRY_CAPABILITY_ESCALATION_DENIED")
        else:
            require(not isinstance(resources.get("execution"), DryExecutionPorts),
                "RUNTIME_LIVE_EXECUTION_PORTS_REQUIRED")
        monitor = self._operations_degradation
        if monitor is None:
            return self._monitored_step(clock=clock, entry=entry, operations_resources=operations_resources, **resources)
        with monitor.step_window():
            return self._monitored_step(clock=clock, entry=entry, operations_resources=operations_resources, **resources)

    def _monitored_step(self, *, clock, entry=None, operations_resources=None, **resources):
        """Observe current degradation around original work; it only gates ENTRY."""
        monitor = self._operations_degradation
        original_clock = clock
        if monitor is not None:
            initial_sample = original_clock()
            monitor.observe(initial_sample, entry=entry, resources=operations_resources,
                source_cut_utc=resources.get("source_cut_utc"), clock=original_clock)
        if self.capability == "NO_BROADCAST":
            try:
                # One bounded DRY unit, including source/admission/terminal writes,
                # serializes with takeover and STOP. No nested monitor lock.
                with mutation_guard(self._ownership, self.ledger.domain):
                    result = self._step(clock=clock, entry=entry, operations_resources=operations_resources, **resources)
            except OperationsOwnershipError:
                result = self._result("OPERATIONS_HELD", "CURRENT_OWNER_REQUIRED")
        else:
            result = self._step(clock=clock, entry=entry, operations_resources=operations_resources, **resources)
        if monitor is not None:
            final_sample = clock()
            monitor.observe(final_sample, entry=entry, resources=operations_resources,
                source_cut_utc=resources.get("source_cut_utc"), clock=clock)
            monitor.note_work(result, clock())
        return result

    def _dry_pending(self, sample, ports, clock, entry, resources):
        """Original DRY terminal owns release; never enter LIVE execution/truth."""
        snapshot = self.ledger.consumer_snapshot(max_positions=1, max_reservations=1)
        require(not snapshot["positions"] and not snapshot["protections"]
            and self._binding is None and self._chain_key is None,
            "RUNTIME_DRY_NO_LIVE_ECONOMICS_REQUIRED")
        if self._entry_action_id is None:
            require(not snapshot["reservations"] and not snapshot["pending_attempts"],
                "RUNTIME_DRY_ORIGINAL_ACTION_RECONSTRUCTION_REQUIRED")
            return None
        action = self.ledger.action(self._entry_action_id)
        require(action is not None and action.side == "BUY", "RUNTIME_DRY_ORIGINAL_BUY_REQUIRED")
        reservation = self.ledger.reservation(action.root_id)
        # A terminal may already be durable when delivery was interrupted.
        if self._dry_recovery or reservation.retired_sequence is not None:
            receipt = finish_interrupted_dry(self.ledger, action.action_id, recorded_at_utc=sample.utc_upper_utc)
        else:
            if self._entry_degradation(clock, entry, resources):
                return self._result("OPERATIONS_ENTRY_HELD", "CURRENT_DEGRADATION_ENTRY_HOLD", action=action)
            if self.producer is None or self.source is None:
                return self._result("SOURCE_HELD", "COLD_SOURCE_RECONSTRUCTION_UNAVAILABLE", action=action)
            if ports is None:
                return self._result("NEED_EXECUTION", "ORIGINAL_DRY_ACTION_AWAITS_PUBLIC_INPUTS", action=action)
            require(type(ports) is DryExecutionPorts, "RUNTIME_DRY_READONLY_PORTS_REQUIRED")
            # An exception after admission marks this in-process action for the
            # same original recovery used on cold reopen, without another read.
            self._dry_recovery = True
            receipt = run_dry(self.ledger, action.action_id, ports.rpc, ports.wallet,
                ports.quote_policy, ports.plan_policy, ports.compute, clock=clock, now_us=ports.now_us,
                qualification_interrupt_after_simulation=ports.qualification_interrupt_after_simulation,
                wallet_after_venue=ports.wallet_after_venue)
        self._entry_action_id = None
        self._dry_recovery = False
        return self._result("NON_SUBMITTED", receipt.dry_terminal.reason, action=action,
            attempt=receipt.dry_terminal.attempt_id, key=receipt.ingestion_key)

    def _step(self, *, clock, entry=None, execution=None, truth_rpc=None, application_wallet=None,
             retirement_wallet=None, exit_proofs=None, source_cut_utc=None, operations_resources=None,
             protective_max_units=None):
        """One bounded next legal work unit. Resource presence never sets priority."""
        sample = clock()
        require(type(sample) is TrustedClockSample and sample.status == "QUALIFIED", "RUNTIME_QUALIFIED_CLOCK_REQUIRED")
        if self.capability == "NO_BROADCAST":
            dry_result = self._dry_pending(sample, execution, clock, entry, operations_resources)
            if dry_result is not None:
                return dry_result
            return self._admit_next(sample, clock, entry, operations_resources, source_cut_utc)
        elif not self._owner_current():
            return self._result("OPERATIONS_HELD", "CURRENT_OWNER_REQUIRED")
        protection = None
        if self._binding is not None:
            protection = self._protect(sample, exit_proofs)
            if protection.mutation_eligible:
                if protection.state == "STAGE_ACTION":
                    action = (stage_protective_sell(self.ledger, self._binding) if protective_max_units is None
                              else stage_protective_sell(self.ledger, self._binding, max_units=protective_max_units))
                    return self._result("PROTECTIVE_ACTION_STAGED", protection.reason, action=action)
                if protection.state in ("PREPARE_ATTEMPT", "REPLACE_ATTEMPT"):
                    return self._execute(protection.action, execution, clock,
                        fence=protection.fence, ordinal=protection.next_attempt_ordinal)
        snapshot = self.ledger.consumer_snapshot()
        if snapshot["pending_attempts"]:
            require(len(snapshot["pending_attempts"]) == 1, "RUNTIME_V1_SINGLE_MUTATION_LANE_REQUIRED")
            return self._truth(snapshot["pending_attempts"][0], sample, truth_rpc, application_wallet)
        if protection is not None and protection.state == "SATISFIED":
            return self._retire(sample, retirement_wallet)
        if self._entry_action_id is not None and self._binding is None:
            action = self.ledger.action(self._entry_action_id)
            own = [item for item in snapshot["reservations"] if item.root_id == action.root_id]
            if snapshot["positions"] or len(snapshot["reservations"]) != 1 or len(own) != 1 or snapshot["funding"].quarantine_reasons:
                return self._result("HELD", "V1_OCCUPIED_OR_QUARANTINED", action=action)
            ordinal = 1
            with self.ledger._trusted_read():
                attempts = self.ledger._root_attempts(action.root_id)
                resolution = None if not attempts else self.ledger._custody.resolution(attempts[-1].preparation.attempt_id)
                terminal = None if resolution is None else next((item for item in self.ledger._applications.values()
                    if item.attempt_id == resolution.attempt_id and item.sequence == resolution.application_sequence), None)
                time_denials = clock_reasons(self.ledger._authority, sample) if attempts else ()
            if attempts:
                if (terminal is None or terminal.decision.disposition not in REPLACEABLE
                        or any(item.preparation.action_id != action.action_id or item.lane_held
                            or item.chain_quarantined or item.economic_disposition not in REPLACEABLE for item in attempts)):
                    return self._result("HELD", "ENTRY_OUTCOME_REQUIRES_LATER_RETIREMENT", action=action)
                deadline = action.claimed_entry_deadline_us
                if time_denials or deadline is None:
                    return self._result("HELD", "ORIGINAL_ENTRY_CLOCK_UNPROVEN", action=action)
                if utc_microseconds(sample.utc_lower_utc) > deadline:
                    return self._retire_unacquired(action, terminal, sample, retirement_wallet, snapshot)
                if utc_microseconds(sample.utc_upper_utc) > deadline:
                    return self._result("HELD", "ORIGINAL_DEADLINE_AMBIGUOUS", action=action)
                ordinal = attempts[-1].preparation.ordinal+1
            if self.producer is None:
                return self._result("SOURCE_HELD", "COLD_SOURCE_RECONSTRUCTION_UNAVAILABLE", action=action)
            try:
                self._source_health(sample, source_cut_utc, clock)
            except (ValueError, RuntimeError, sqlite3.DatabaseError, OSError):
                return self._result("SOURCE_HELD", "CURRENT_SOURCE_UNAVAILABLE", action=action)
            return self._execute(action, execution, clock, fence=snapshot["fence"], ordinal=ordinal,
                entry=entry, operations_resources=operations_resources)
        return self._admit_next(sample, clock, entry, operations_resources, source_cut_utc, protection)

    def _admit_next(self, sample, clock, entry, operations_resources, source_cut_utc, protection=None):
        """Shared original producer/Authority path; capability selects no economics."""
        source_state = self._source_page(sample, source_cut_utc, clock)
        snapshot = self.ledger.consumer_snapshot()
        if snapshot["positions"] or snapshot["reservations"] or snapshot["funding"].quarantine_reasons:
            reason = "V1_OCCUPIED_OR_QUARANTINED" if protection is None else "PROTECTED_"+protection.state+":"+protection.reason
            return self._result("ENTRY_HELD", reason)
        if source_state == "SOURCE_UNAVAILABLE_OR_PROFILE_EXHAUSTED":
            return self._result("SOURCE_HELD", source_state)
        if (entry is None or type(entry) is EntryFacts and entry.protocol is None) and self._entry_degradation(
                clock, entry, operations_resources, source_cut_utc):
            return self._result("OPERATIONS_ENTRY_HELD", "CURRENT_DEGRADATION_ENTRY_HOLD")
        if not self._queue:
            return self._result("SOURCE_ADVANCED", source_state)
        root = self._queue[0]
        if entry is None:
            return self._result("NEED_ENTRY_FACTS", "ORIGINAL_QUEUED_CANDIDATE", root=root)
        require(type(entry) is EntryFacts, "RUNTIME_CONCRETE_ENTRY_FACTS_REQUIRED")
        policy = self.ledger.authority_snapshot()["policy"]
        if policy is None:
            return self._result("ENTRY_HELD", "EXTERNAL_AUTHORITY_POLICY_REQUIRED", root=root)
        if self.capability == "LIVE" and not self._owner_current():
            return self._result("OPERATIONS_HELD", "CURRENT_OWNER_REQUIRED", root=root)
        try:
            sample = admission_clock(clock, self.ledger._authority,
                self.source.latest().snapshot.observed_at_utc, entry)
            entry = entry_at_clock(entry, sample)
        except (ValueError, RuntimeError, TypeError):
            return self._result("ENTRY_HELD", "CURRENT_ADMISSION_KNOWLEDGE_UNPROVEN", root=root)
        if entry.protocol is not None:
            from .pump_protocol_compatibility_v0_1 import PumpProtocolObservation, rejection_proof
            require(type(entry.protocol) is PumpProtocolObservation, "RUNTIME_TYPED_PUMP_PROTOCOL_FACTS_REQUIRED")
            if clock_reasons(self.ledger._authority, sample):
                return self._result("ENTRY_HELD", "PUMP_PROTOCOL_QUALIFIED_CLOCK_REQUIRED", root=root)
            disposition, selected_venue, reason = entry.protocol.classify(
                self.ledger.domain, self.ledger.candidate(root), sample, entry.wallet)
            if disposition == "UNKNOWN":
                return self._result("ENTRY_HELD", reason, root=root)
            if disposition == "UNSUPPORTED":
                # An original nonacceptance is durable but never changes wallet
                # custody, consumes a grant, reserves funds or permits signing.
                # It precedes wallet comparison so an unsupported candidate
                # cannot quarantine an otherwise supported dedicated wallet.
                key = self._key("protocol-reject", root, entry.protocol.content_digest)
                self.ledger.record_nonacceptance(root, "REJECTED", external_reference=reason,
                    external_record_digest=entry.protocol.content_digest,
                    recorded_at_utc=sample.utc_lower_utc, idempotency_key=key, fence=snapshot["fence"],
                    protocol_evidence=rejection_proof(entry.protocol, entry.wallet, sample))
                self._queue.pop(0)
                return self._result("CANDIDATE_UNSUPPORTED", reason, root=root, key=key)
            entry = EntryFacts(selected_venue, entry.protocol.read.accounts[0].account.owner, entry.wallet, entry.protocol)
        if entry.protocol is not None and self._entry_degradation(clock, entry, operations_resources, source_cut_utc):
            return self._result("OPERATIONS_ENTRY_HELD", "CURRENT_DEGRADATION_ENTRY_HOLD")
        try:
            sample = admission_clock(clock, self.ledger._authority,
                self.source.latest().snapshot.observed_at_utc, entry)
            entry = entry_at_clock(entry, sample)
        except (ValueError, RuntimeError, TypeError):
            return self._result("ENTRY_HELD", "CURRENT_ADMISSION_KNOWLEDGE_UNPROVEN", root=root)
        if entry.protocol is not None and entry.protocol.classify(
                self.ledger.domain, self.ledger.candidate(root), sample, entry.wallet)[0] != "SUPPORTED":
            return self._result("ENTRY_HELD", "CURRENT_PROTOCOL_REVALIDATION_REQUIRED", root=root)
        key = self._key("admission", root, sample.utc_upper_utc)
        receipt = self.ledger.admit_authority_entry(EntryRequest(root, entry.venue, entry.token_program, policy.selected_track),
            sample, self.source, entry.wallet, command_id=key, fence=snapshot["fence"])
        self._queue.pop(0)
        if not receipt.accepted:
            return self._result("ENTRY_DENIED", "ACTUAL_AUTHORITY_NONACCEPTANCE", key=key, root=root)
        action = self.ledger.reservation(root).admission.action
        self._entry_action_id = action.action_id
        return self._result("ENTRY_ADMITTED", "ACTUAL_AUTHORITY_ADMISSION_NOT_MESSAGE_PERMISSION", action=action, key=key)
