"""B1: one reconstructed exit binding from the authoritative LIVE journal.

The immutable candidate/admitted action and applied acquisition are the durable
binding record. This projection writes no second economic truth and never makes
inventory usable. B2 owns versioned evaluation/cuts; B3 owns protective handoff.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field

from phase4.paper_exit_orchestrator_v0_1 import (
    ExitTrackSpec, LOCKED_EXIT_SPECS, LOCKED_EXIT_SPEC_FINGERPRINT,
)
from phase5.shadow_domain_v0_1 import content_fingerprint
from .authority_controls_v0_1 import bind_entry
from .ledger_actions_v0_1 import CandidateInboxInput, position_identity
from .ledger_domain_v0_1 import LedgerContractError
from .ledger_ports_v0_1 import ConsumerCut
from .ledger_repository_v0_1 import LedgerRepository

VERSION = "live_position_controller_v0.1"


@dataclass(frozen=True, slots=True)
class SelectedExitBinding:
    economic_domain_id: str
    position_id: str
    root_id: str
    mint: str
    candidate: CandidateInboxInput
    buy_action_id: str
    buy_action_digest: str
    policy_ref: str
    policy_digest: str
    spec: ExitTrackSpec
    acquisition_application_key: str
    acquisition_application_digest: str
    acquisition_proposal_digest: str
    acquisition_signature: str
    acquisition_chain_receipt_key: str
    acquisition_chain_receipt_digest: str
    acquisition_recorded_at_utc: str
    fallback_deadline_us: int
    fallback_fire_boundary_us: int
    locked_specs_fingerprint: str = field(init=False, default=LOCKED_EXIT_SPEC_FINGERPRINT)
    version: str = field(init=False, default=VERSION)

    @property
    def binding_id(self):
        # Identical identity to the accepted Ledger protective-handoff port.
        return content_fingerprint({"identity": "LEDGER_POSITION_PROTECTION",
            "domain": self.economic_domain_id, "position_id": self.position_id,
            "track": self.spec.track_id, "policy_digest": self.policy_digest})

    @property
    def content_digest(self):
        return content_fingerprint(asdict(self))


@dataclass(frozen=True, slots=True)
class PositionControllerSnapshot:
    binding: SelectedExitBinding
    cut: ConsumerCut
    acquired_units: int
    sold_units: int
    remaining_units: int
    ledger_position_status: str
    inventory_usable: bool
    state: str = field(init=False, default="BOUND")


def bind_actual_position(ledger: LedgerRepository, *, position_id: str, root_id: str,
                         mint: str, selected_track: str, policy_digest: str,
                         acquisition_application_key: str,
                         previous: SelectedExitBinding | None = None) -> PositionControllerSnapshot:
    """Reconstruct one selected binding and *current actual* attributed units.

    Selection is an assertion against the already admitted original terms, never
    a new selection. The application key must identify the positive applied BUY,
    not an observation, modeled fill, replay receipt or later SELL. No timer is
    evaluated here: the exact original deadline/fire boundary is exposed to B2.
    Downstream consumers must revalidate supplied bindings with this function
    (previous=...), since dataclass immutability is not authenticity.
    A repeated call/reopen reconstructs the same binding while residual quantity
    is read afresh, including zero after a proven reduction (historical binding,
    not obligation satisfaction). Every read is Ledger-guarded; before/after cuts reject a mixed
    journal revision. The returned cut is evidence, never mutation permission.
    """
    if type(ledger) is not LedgerRepository or ledger.domain.mode != "LIVE":
        raise LedgerContractError("RUNTIME_LIVE_LEDGER_REQUIRED")
    snapshot = ledger.consumer_snapshot()
    economics = ledger.authority_candidate_economics(root_id)
    candidate, action = economics["candidate"], economics["admitted_action"]
    position = ledger.position_history(position_id)
    if (snapshot["funding"].quarantine_reasons or position is None or position.acquired_units <= 0 or position.remaining_units < 0
            or action is None or action.side != "BUY" or type(selected_track) is not str
            or selected_track not in LOCKED_EXIT_SPECS
            or (position.root_id, position.mint, action.root_id, action.mint, action.position_id) !=
               (root_id, mint, root_id, mint, position_id)
            or position_id != position_identity(ledger.domain, root_id, mint)
            or candidate.mint != mint or candidate.trade_root(ledger.domain) != root_id
            or (action.selected_exit_track, action.policy_digest, action.candidate_digest) !=
               (selected_track, policy_digest, candidate.content_digest)):
        raise LedgerContractError("RUNTIME_ACTUAL_POSITION_OR_SELECTED_ORIGINAL_TERMS_REQUIRED")
    # Real Authority bindings, when present, must retain their original signal
    # and track. The entry deadline is deliberately NOT used as an exit timer.
    bind_entry(ledger.domain, candidate, selected_track, economics["entry_binding"])
    acquisition = ledger.application_receipt(acquisition_application_key)
    proposal = None if acquisition is None else acquisition.decision.proposal
    if (acquisition is None or acquisition.decision.disposition != "FINALIZED_SUCCESS_APPLIED"
            or proposal is None or proposal.base_units_delta <= 0
            or (proposal.economic_domain_id, proposal.root_id, proposal.action_id, proposal.signature,
                proposal.base_units_delta) != (ledger.domain.economic_domain_id, root_id, action.action_id,
                position.acquisition_signature, position.acquired_units)):
        raise LedgerContractError("RUNTIME_APPLIED_ACTUAL_BUY_ACQUISITION_REQUIRED")
    chain = ledger.chain_receipt(acquisition.chain_receipt_key)
    attempt = ledger.attempt(acquisition.attempt_id)
    if (chain is None or chain.content_digest != acquisition.chain_receipt_digest
            or chain.decision.resulting_state.positive_finality != "FINALIZED_SUCCESS"
            or attempt is None or attempt.preparation.action_id != action.action_id
            or proposal.attempt_id != acquisition.attempt_id):
        raise LedgerContractError("RUNTIME_ORIGINAL_FINALIZED_BUY_PROOF_REQUIRED")
    spec = LOCKED_EXIT_SPECS[selected_track]
    deadline = candidate.generated_at_us + spec.fallback_ms * 1000
    binding = SelectedExitBinding(ledger.domain.economic_domain_id, position_id, root_id, mint,
        candidate, action.action_id, action.content_digest, action.policy_ref, policy_digest, spec,
        acquisition.ingestion_key, acquisition.content_digest, proposal.content_digest, proposal.signature,
        acquisition.chain_receipt_key, acquisition.chain_receipt_digest, acquisition.recorded_at_utc,
        deadline, deadline + 1)
    protection = ledger.protection(position_id)
    if protection is not None:
        handoff = protection.handoff
        if ((handoff.binding_id, handoff.candidate, handoff.acquisition_signature,
             handoff.acquisition_proposal_digest, handoff.acquired_units, handoff.fallback_anchor_us,
             handoff.fallback_deadline_us, handoff.fallback_fire_boundary_us,
             handoff.knowledge_chain_receipt_digest) !=
                (binding.binding_id, candidate, proposal.signature, proposal.content_digest,
                 position.acquired_units, candidate.generated_at_us, deadline, deadline+1,
                 chain.content_digest)):
            raise LedgerContractError("RUNTIME_EXISTING_PROTECTION_ORIGINAL_BINDING_CONFLICT")
    if previous is not None and (type(previous) is not SelectedExitBinding or previous != binding):
        raise LedgerContractError("RUNTIME_CONTROLLER_REBINDING_FORBIDDEN")
    if (ledger.consumer_snapshot()["consumer_cut"] != snapshot["consumer_cut"]
            or economics["cut"] != snapshot["consumer_cut"]):
        raise LedgerContractError("RUNTIME_CONTROLLER_COMMON_CUT_CHANGED")
    return PositionControllerSnapshot(binding, snapshot["consumer_cut"], position.acquired_units,
        position.sold_units, position.remaining_units, position.status, position.usable)
