"""B4 Runtime response to authoritative protective SELL outcomes.

This is a deterministic read of existing Ledger facts, never a retry, signing
or send grant. Execution consumes the returned original fence/ordinal through
its existing exact-message preparation port and fresh Authority boundaries.
No elapsed-time inference, non-landing proof, quantity or progress is stored.
"""
from __future__ import annotations

from dataclasses import dataclass

from .ledger_actions_v0_1 import PendingAction
from .ledger_custody_v0_1 import REPLACEABLE
from .ledger_domain_v0_1 import LedgerContractError
from .ledger_ports_v0_1 import ConsumerCut
from .ledger_repository_v0_1 import LedgerWriteFence
from .protective_obligation_v0_1 import ProtectiveObligationSnapshot, SELL_REF, _read

VERSION = "live_protective_outcome_v0.1"


@dataclass(frozen=True, slots=True)
class ProtectiveOutcome:
    obligation: ProtectiveObligationSnapshot
    cut: ConsumerCut
    fence: LedgerWriteFence
    state: str
    reason: str
    action: PendingAction | None
    latest_attempt_id: str | None
    disposition: str | None
    next_attempt_ordinal: int | None
    mutation_eligible: bool


def protective_outcome(ledger, binding):
    """Reconstruct held, residual, replacement and actual satisfaction state.

    PREPARE_ATTEMPT / REPLACE_ATTEMPT are eligibility at this common cut only.
    Pass this exact fence and ordinal to Execution.prepare_exact_message; a
    later cut is not a substitute. Current SIGN and SEND still need Authority.
    An existing prepared or signed attempt remains HELD here: this module does
    not resume transport or implement cancellation/recovery orchestration.
    """
    snapshot, context = _read(ledger, binding)
    attempts = context["attempts"]
    resolutions = {r.attempt_id:r for r in context["resolutions"]}
    pending = [a for a in attempts if a.preparation.attempt_id not in resolutions
        and a.recorded_stage != "CANCELLED_UNSIGNED"]
    action = latest = disposition = ordinal = None
    state, reason = "HELD", "ORIGINAL_PROTECTION_REQUIRED"
    eligible = False
    if snapshot.handoff is not None:
        if snapshot.remaining_units == 0:
            successes = [r for r in resolutions.values() if r.disposition == "FINALIZED_SUCCESS_APPLIED"
                and any(a.action_id == r.action_id and a.side == "SELL" for _,a in context["actions"])]
            applied = {r.sequence:r for r in context["applications"]}
            positive = any(r.application_sequence in applied
                and applied[r.application_sequence].decision.disposition == "FINALIZED_SUCCESS_APPLIED"
                and applied[r.application_sequence].decision.proposal.base_units_delta < 0
                and applied[r.application_sequence].decision.proposal.content_digest == r.proposal_digest
                for r in successes)
            if (not positive or pending or snapshot.claiming_actions
                    or snapshot.sold_units != snapshot.acquired_units):
                raise LedgerContractError("RUNTIME_OUTCOME_ACTUAL_SATISFACTION_CONFLICT")
            state, reason = "SATISFIED", "ACTUAL_SUCCESSFUL_REDUCTION_APPLIED_TO_ZERO"
        elif snapshot.state != "DUE":
            state, reason = "MONITORING", "COMMITTED_TRIGGER_NOT_DUE"
        elif not snapshot.claiming_actions:
            state, reason = "STAGE_ACTION", "ACTUAL_UNENCUMBERED_RESIDUAL"
            eligible = not pending and not snapshot.mutation_lane_held
        elif (len(snapshot.claiming_actions) != 1
                or not snapshot.claiming_actions[0].external_decision_ref.startswith(SELL_REF)):
            reason = "OTHER_UNRESOLVED_REDUCTION_CLAIM"
        else:
            action = snapshot.claiming_actions[0]
            history = sorted((a for a in attempts if a.preparation.action_id == action.action_id),
                key=lambda a:a.preparation.ordinal)
            if not history:
                state, reason, ordinal = "PREPARE_ATTEMPT", "ORIGINAL_IMMUTABLE_SELL", 1
                eligible = not pending and not snapshot.mutation_lane_held
            else:
                last = history[-1]
                latest = last.preparation.attempt_id
                resolution = resolutions.get(latest)
                disposition = last.economic_disposition
                reason = "AUTHORITATIVE_APPLICATION_REQUIRED"
                if resolution is not None and resolution.disposition in REPLACEABLE:
                    # Earlier attempts also remain part of the same immutable
                    # action. No unresolved sibling can disappear on reopen.
                    resolved_history = all(a.preparation.attempt_id in resolutions
                        or a.recorded_stage == "CANCELLED_UNSIGNED" for a in history)
                    if resolved_history and not pending:
                        state, reason = "REPLACE_ATTEMPT", resolution.disposition
                        ordinal = last.preparation.ordinal+1
                        eligible = not snapshot.mutation_lane_held
                elif last.recorded_stage == "CANCELLED_UNSIGNED":
                    reason = "CANCELLATION_RETRY_OUTSIDE_THIS_TRANSITION"
        if snapshot.hard_stop_command is not None and state != "SATISFIED":
            eligible = False
            reason = "GLOBAL_HARD_STOP"
        elif eligible is False and state in ("STAGE_ACTION", "PREPARE_ATTEMPT", "REPLACE_ATTEMPT"):
            reason = "CURRENT_MUTATION_LANE_OR_PENDING_ATTEMPT"
    return ProtectiveOutcome(snapshot, snapshot.cut, context["snapshot"]["fence"], state, reason,
        action, latest, disposition, ordinal, eligible)
