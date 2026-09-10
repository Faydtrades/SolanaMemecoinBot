"""C4 finite LIVE cold reconstruction. Durable owners remain the only truth.

No installation, recovery verdict, readiness grant, process loop or ownership
protocol lives here. A failed source reopen cannot erase intact economics.
"""
from __future__ import annotations

import sqlite3
from contextlib import closing
from dataclasses import dataclass, field
from pathlib import Path

from phase5.shadow_domain_v0_1 import content_fingerprint
from .authority_controls_v0_1 import require
from .candidate_handoff_v0_1 import CandidateHandoffV01, CONFIG
from .continuous_producer_v0_1 import connect_producer_db
from .continuous_producer_v0_2 import LiveContinuousProducerV02, ContinuationProfileV02, COUNTERS
from .evidence_store_v0_1 import SourceEvidenceStore
from .exit_observation_v0_1 import EVALUATION
from .ledger_custody_v0_1 import FundingView
from .ledger_actions_v0_1 import NON_ACCEPTANCE, TERMINAL_INBOX
from .ledger_ports_v0_1 import ConsumerCut
from .ledger_repository_v0_1 import LedgerRepository
from .position_controller_v0_1 import bind_actual_position
from .protective_outcome_v0_1 import protective_outcome
from .runtime_composition_v0_1 import RuntimeCompositionV01


@dataclass(frozen=True, slots=True)
class ReconstructionFacts:
    """Bounded historical facts, explicitly without current action permission."""
    consumer_cut: ConsumerCut
    funding: FundingView
    source_state: str
    producer_lineage_id: str | None
    producer_checkpoint_digest: str | None
    producer_cursor: int | None
    retained_source_disposition: str | None
    retained_source_digest: str | None
    retained_source_observed_at_utc: str | None
    initial_handoff_replay_deferred: bool
    queued_roots: tuple[str, ...]
    consumed_roots_seen: int
    entry_action_id: str | None
    position_id: str | None
    binding_id: str | None
    remaining_units: int | None
    exit_cut_digest: str | None
    protective_state: str | None
    obligation_id: str | None
    pending_attempt_id: str | None
    chain_receipt_key: str | None
    grants_permission: bool = field(init=False, default=False)


def _existing(path, names, singleton):
    """Prevent accepted install-capable factories from creating missing stores."""
    path = Path(path).resolve()
    with closing(sqlite3.connect(path.as_uri()+"?mode=ro", uri=True)) as conn:
        for name in names:
            require(conn.execute("SELECT 1 FROM sqlite_master WHERE name=?", (name,)).fetchone(),
                "RUNTIME_COLD_EXISTING_SOURCE_SCHEMA_REQUIRED")
        require(conn.execute("SELECT 1 FROM "+singleton+" WHERE singleton=1").fetchone(),
            "RUNTIME_COLD_EXISTING_SOURCE_IDENTITY_REQUIRED")


class ColdRuntimeV01(RuntimeCompositionV01):
    """Created only by reopen_live; owns and closes its reopened local handles."""

    def __init__(self):
        raise TypeError("RUNTIME_COLD_REOPEN_REQUIRED")

    def close(self):
        if self.source is not None:
            self.source.close()
        if self._producer_conn is not None:
            self._producer_conn.close()
        self.ledger.close()

    def _consumed(self, root):
        # Accepted or durably denied/terminal inbox dispositions are consumed
        # work. An inconclusive call without a durable disposition is not one.
        with self.ledger._trusted_read():
            return (self.ledger.authority_acceptance(root) is not None
                or self.ledger.inbox_disposition(root) in NON_ACCEPTANCE | TERMINAL_INBOX)

    def _retain_page(self, page):
        for root in page.candidate_roots:
            if self._consumed(root):
                self._consumed_seen += 1
            elif root not in self._queue:
                self._queue.append(root)

    def _source_page(self, sample, cut):
        result = super()._source_page(sample, cut)
        self._queue[:] = [root for root in self._queue if not self._consumed(root)]
        return result

    def reconstruction_facts(self):
        with self.ledger._trusted_read():
            snapshot = self.ledger.consumer_snapshot(max_positions=1, max_reservations=1)
            binding = self._binding
            evaluations = () if binding is None else tuple(r for r in self.ledger.exit_records(binding.binding_id)
                if r.kind == EVALUATION)
            outcome = protective_outcome(self.ledger, binding) if evaluations else None
            position = snapshot["positions"][0] if snapshot["positions"] else None
            retained = None if self.source is None else self.source.latest_record()
            source = None if retained is None else retained[1]
            facts = ReconstructionFacts(snapshot["consumer_cut"], snapshot["funding"], self._source_state,
                self._lineage, self._checkpoint, self._cursor,
                None if source is None else source.disposition,
                None if source is None else source.content_digest,
                None if source is None else source.snapshot.observed_at_utc,
                self._initial_replay_deferred,
                self.queued_roots, self._consumed_seen,
                self._entry_action_id, None if position is None else position.position_id,
                None if binding is None else binding.binding_id,
                None if position is None else position.remaining_units,
                None if not evaluations else evaluations[-1].content_digest,
                None if outcome is None else outcome.state,
                None if outcome is None else outcome.obligation.obligation_id,
                next(iter(snapshot["pending_attempts"]), None), self._chain_key)
            require(self.ledger.consumer_snapshot()["consumer_cut"] == snapshot["consumer_cut"],
                "RUNTIME_COLD_COMMON_CUT_CHANGED")
            return facts

    def _restore_economics(self):
        with self.ledger._trusted_read():
            snapshot = self.ledger.consumer_snapshot(max_positions=1, max_reservations=1)
            require(self.ledger.baseline() is not None, "RUNTIME_COLD_ESTABLISHED_BASELINE_REQUIRED")
            require(not snapshot["funding"].quarantine_reasons, "RUNTIME_COLD_ECONOMIC_QUARANTINE")
            require(len(snapshot["pending_attempts"]) <= 1, "RUNTIME_V1_SINGLE_MUTATION_LANE_REQUIRED")
            reservations, positions = snapshot["reservations"], snapshot["positions"]
            action = None
            if reservations:
                action = reservations[0].admission.action
                economics = self.ledger.authority_candidate_economics(action.root_id)
                require(economics["admitted_action"] == action
                    and self.ledger.authority_acceptance(action.root_id) is not None,
                    "RUNTIME_COLD_ORIGINAL_AUTHORITY_ACTION_REQUIRED")
                self._entry_action_id = action.action_id
            if positions:
                position = positions[0]
                require(action is not None and action.position_id == position.position_id,
                    "RUNTIME_COLD_POSITION_RESERVATION_CONFLICT")
                binding_id = content_fingerprint({"identity": "LEDGER_POSITION_PROTECTION",
                    "domain": self.ledger.domain.economic_domain_id, "position_id": position.position_id,
                    "track": action.selected_exit_track, "policy_digest": action.policy_digest})
                context = self.ledger.protective_position_context(position.position_id, binding_id)
                applications = [r for r in context["applications"]
                    if r.decision.disposition == "FINALIZED_SUCCESS_APPLIED"
                    and r.decision.proposal.action_id == action.action_id
                    and r.decision.proposal.base_units_delta > 0]
                require(len(applications) == 1, "RUNTIME_COLD_ORIGINAL_BUY_APPLICATION_REQUIRED")
                self._binding = bind_actual_position(self.ledger, position_id=position.position_id,
                    root_id=position.root_id, mint=position.mint, selected_track=action.selected_exit_track,
                    policy_digest=action.policy_digest, acquisition_application_key=applications[0].ingestion_key).binding
                require(self._binding.binding_id == binding_id, "RUNTIME_COLD_ORIGINAL_BINDING_ID_REQUIRED")
                self._entry_action_id = None
                if any(r.kind == EVALUATION for r in context["exit_records"]):
                    protective_outcome(self.ledger, self._binding)
            if snapshot["pending_attempts"]:
                attempt_id = snapshot["pending_attempts"][0]
                attempt = self.ledger.attempt(attempt_id)
                pending_action = self.ledger.action(attempt.preparation.action_id)
                require(action is not None and pending_action.root_id == action.root_id,
                    "RUNTIME_COLD_PENDING_ORIGINAL_ROOT_REQUIRED")
                row = self.ledger._conn.execute("SELECT ingestion_key FROM ledger_chain_receipts "
                    "WHERE attempt_id=? ORDER BY evidence_revision DESC LIMIT 1", (attempt_id,)).fetchone()
                if row is not None:
                    chain = self.ledger.chain_receipt(row[0])
                    require(chain.attempt_id == attempt_id, "RUNTIME_COLD_ORIGINAL_CHAIN_REQUIRED")
                    self._chain_key = chain.ingestion_key
            require(self.ledger.consumer_snapshot()["consumer_cut"] == snapshot["consumer_cut"],
                "RUNTIME_COLD_COMMON_CUT_CHANGED")


def reopen_live(ledger_path, domain, *, producer_path, market_source, producer_profile,
                source_path, source_binding, source_profile, database_identity,
                batch_rows=32, page_rows=32, queued_roots=64):
    """Open existing owners and reconstruct economics before candidate replay.

    Supplied configuration is checked by the original factories. No candidate,
    action, application, selected binding or restoration verdict is supplied.
    Active economic work defers A3 replay to priority-ordered Runtime steps.
    Otherwise initial replay is bounded by the finite pending profile and queue;
    SOURCE_RECONSTRUCTED describes owner reconstruction, not a drained outbox.
    """
    require(domain.mode == "LIVE", "RUNTIME_COLD_LIVE_DOMAIN_REQUIRED")
    require(type(producer_profile) is ContinuationProfileV02
        and type(batch_rows) is int and 1 <= batch_rows <= producer_profile.batch_rows
        and type(page_rows) is int and 1 <= page_rows <= producer_profile.delivery_page_rows
        and type(queued_roots) is int and page_rows <= queued_roots <= 1024,
        "RUNTIME_FINITE_DISPATCH_PROFILE_REQUIRED")
    root = object.__new__(ColdRuntimeV01)
    root.ledger = LedgerRepository.reopen(ledger_path, domain)
    root.producer = root.handoff = root.source = root._producer_conn = None
    root._entry_action_id = root._binding = root._chain_key = None
    root._lineage = root._checkpoint = root._cursor = None
    root._queue, root._consumed_seen = [], 0
    root._initial_replay_deferred = False
    root.batch_rows, root.page_rows, root.queue_limit = batch_rows, page_rows, queued_roots
    root._source_state = "SOURCE_RECONSTRUCTION_UNAVAILABLE"
    try:
        root._restore_economics()  # Economic failure is never converted to a source hold.
        try:
            _existing(producer_path, ("live_producer_checkpoint_v0_1", COUNTERS, CONFIG),
                "live_producer_checkpoint_v0_1")
            _existing(source_path, ("source_domain", "source_evidence", "source_domain_no_update",
                "source_domain_no_delete", "source_evidence_no_update", "source_evidence_no_delete"), "source_domain")
            root._producer_conn = connect_producer_db(producer_path)
            root.producer = LiveContinuousProducerV02(root._producer_conn, market_source, profile=producer_profile)
            root.handoff = CandidateHandoffV01(root.producer, root.ledger, source_binding,
                database_identity=database_identity)
            root.source = SourceEvidenceStore(source_path, source_binding, source_profile)
            root._initial_replay_deferred = root._entry_action_id is not None or root._binding is not None
            # An original reservation/action also retains any pending attempt.
            # Keep A3's starting cursor untouched until protective/truth work
            # allows the inherited scheduler to request a bounded source page.
            if not root._initial_replay_deferred:
                # No cursor injection: replay validates every original ACK/inbox.
                # Queue space can shrink a page to one row. Bound by original
                # retained rows, not a page-size assumption that could skip ACKs.
                for _ in range(producer_profile.pending_rows+1):
                    available = root.queue_limit-len(root._queue)
                    if not available:
                        break
                    page = root.handoff.deliver_page(limit=min(page_rows, available))
                    root._retain_page(page)
                    if not page.scanned_rows:
                        break
                else:
                    raise ValueError("RUNTIME_COLD_HANDOFF_REPLAY_BOUND")
            with root.producer._transaction():
                manifest = root.producer._validate_checkpoint()
                root._lineage = root.producer.source_identity
                root._checkpoint = root._producer_conn.execute(
                    "SELECT manifest_digest FROM live_producer_checkpoint_v0_1 WHERE singleton=1").fetchone()[0]
                root._cursor = manifest["cursor"]
            root._source_state = "SOURCE_RECONSTRUCTED"
        except (ValueError, RuntimeError, sqlite3.DatabaseError, OSError):
            if root.source is not None:
                root.source.close()
            if root._producer_conn is not None:
                root._producer_conn.close()
            root.producer = root.handoff = root.source = root._producer_conn = None
            root._queue.clear()
        root.reconstruction_facts()  # Recheck intact economics after independent source work.
        return root
    except BaseException:
        root.close()
        raise
