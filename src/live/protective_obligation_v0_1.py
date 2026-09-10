"""B3 durable original protection and immutable actual-unit SELL decisions.

Ledger already owns all durable facts: B1 acquisition/candidate, B2 evaluations,
initial protective handoff, actions and actual applications. This module adds
no quantity/progress store and no retry or execution permission. A failed or
unresolved action remains a claim; B4 owns lawful attempt progression.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass

from phase5.shadow_domain_v0_1 import canonical_json, content_fingerprint
from .exit_observation_v0_1 import EVALUATION, ExitJournalRecord
from .ledger_actions_v0_1 import PendingAction
from .ledger_domain_v0_1 import LedgerContractError, ledger_utc
from .ledger_ports_v0_1 import ProtectiveHandoffInput, ConsumerCut
from .position_controller_v0_1 import SelectedExitBinding, bind_actual_position
from .public_rpc_v0_1 import u64

VERSION = "live_protective_obligation_v0.1"
SELL_REF = "RUNTIME_B3_SELL:"
PROTECTION_REF = "RUNTIME_B3_EVALUATION:"


def require(value, reason):
    if not value:
        raise LedgerContractError("RUNTIME_PROTECTION_"+reason)


@dataclass(frozen=True, slots=True)
class ProtectiveObligationSnapshot:
    binding: SelectedExitBinding
    obligation_id: str
    handoff: ProtectiveHandoffInput | None
    evaluation: ExitJournalRecord
    cut: ConsumerCut
    state: str
    acquired_units: int
    sold_units: int
    remaining_units: int
    encumbered_units: int
    unencumbered_units: int
    claiming_actions: tuple[PendingAction, ...]
    hard_stop_command: str | None
    mutation_lane_held: bool


def _obligation_id(binding):
    return content_fingerprint({"identity": "LEDGER_FULL_REDUCTION_OBLIGATION", "binding_id": binding.binding_id})


def _validate_evaluation(binding, record):
    require(type(record) is ExitJournalRecord and record.kind == EVALUATION
        and record.binding_id == binding.binding_id and record.payload["binding"] == canonical_json(asdict(binding)),
        "ORIGINAL_DURABLE_EVALUATION_REQUIRED")


def _handoff(binding, evaluation, cut, units, at):
    _validate_evaluation(binding, evaluation)
    at = ledger_utc(at)
    payload, state = evaluation.payload, evaluation.payload["state"]
    require(at >= payload["recorded_at_utc"], "HANDOFF_BEFORE_EVALUATION")
    return ProtectiveHandoffInput(cut, binding.root_id, binding.position_id, binding.candidate,
        binding.spec.track_id, binding.policy_digest, binding.acquisition_signature,
        binding.acquisition_proposal_digest, units, binding.candidate.generated_at_us,
        binding.fallback_deadline_us, binding.fallback_fire_boundary_us,
        "LOCKED_EXIT_SPECS", binding.locked_specs_fingerprint, VERSION, binding.content_digest,
        state["state"], state["trigger_kind"], state["trigger_at_us"],
        evaluation.content_digest if state["state"] == "DUE" else None,
        binding.acquisition_chain_receipt_digest, payload["recorded_at_utc"],
        PROTECTION_REF+evaluation.content_digest,
        content_fingerprint({"version": VERSION, "binding": binding.content_digest,
            "evaluation": evaluation.content_digest, "actual_acquired_units": units}), at)


def _historical_units(context, before_sequence=None):
    return sum(r.decision.proposal.base_units_delta for r in context["applications"]
        if r.decision.disposition == "FINALIZED_SUCCESS_APPLIED"
        and (before_sequence is None or r.sequence < before_sequence))


def _decision_digest(binding, handoff, evaluation, remaining, units, ordinal, token_program):
    return content_fingerprint({"version": VERSION, "binding": binding.content_digest,
        "handoff": handoff.content_digest, "evaluation": evaluation.content_digest,
        "actual_remaining_before_units": remaining, "input_units": units, "ordinal": ordinal,
        "token_program": token_program})


def _original_action(binding, context, sequence, action, handoff, evaluations):
    if not action.external_decision_ref.startswith(SELL_REF):
        return False  # Other accepted actors still encumber their staged units.
    digest = action.external_decision_ref[len(SELL_REF):]
    evaluation = evaluations.get(digest)
    remaining = _historical_units(context, sequence)
    require(evaluation is not None and evaluation.sequence < sequence and evaluation.payload["state"]["state"] == "DUE"
        and 0 < action.input_units <= remaining, "ORIGINAL_SELL_DECISION_REQUIRED")
    expected = PendingAction(binding.root_id, binding.candidate.content_digest, "SELL", binding.mint,
        context["position"].token_program, binding.position_id, action.input_units, SELL_REF+evaluation.content_digest,
        _decision_digest(binding, handoff, evaluation, remaining, action.input_units, action.ordinal,
            context["position"].token_program), binding.policy_ref, binding.policy_digest,
        binding.spec.track_id, handoff.obligation_id, action.ordinal)
    require(action == expected, "ORIGINAL_SELL_TERMS_OR_ACTUAL_UNITS_CHANGED")
    return True


def _read(ledger, binding):
    require(type(binding) is SelectedExitBinding, "B1_BINDING_REQUIRED")
    original = bind_actual_position(ledger, position_id=binding.position_id, root_id=binding.root_id,
        mint=binding.mint, selected_track=binding.spec.track_id, policy_digest=binding.policy_digest,
        acquisition_application_key=binding.acquisition_application_key, previous=binding)
    context = ledger.protective_position_context(binding.position_id, binding.binding_id)
    require(original.cut == context["snapshot"]["consumer_cut"]
        and context["authority"]["fence"] == context["snapshot"]["fence"], "COMMON_CUT_CHANGED")
    records = [r for r in context["exit_records"] if r.kind == EVALUATION]
    require(bool(records), "COMMITTED_B2_EVALUATION_REQUIRED")
    for record in records:
        _validate_evaluation(binding, record)
    evaluation = records[-1]
    evaluations = {r.content_digest:r for r in records}
    protection = context["protection"]
    handoff = None if protection is None else protection.handoff
    if handoff is not None:
        require(handoff.obligation_decision_ref.startswith(PROTECTION_REF), "ORIGINAL_B3_HANDOFF_REQUIRED")
        initial = evaluations.get(handoff.obligation_decision_ref[len(PROTECTION_REF):])
        require(initial is not None and initial.sequence < protection.sequence, "ORIGINAL_HANDOFF_EVALUATION_REQUIRED")
        require(handoff == _handoff(binding, initial, handoff.cut, original.acquired_units, handoff.recorded_at_utc),
            "ORIGINAL_HANDOFF_CHANGED")
    require(_historical_units(context) == original.remaining_units, "ACTUAL_APPLICATION_QUANTITY_CONFLICT")
    resolutions = {r.attempt_id:r for r in context["resolutions"]}
    claims = []
    for seq, action in context["actions"]:
        if action.side != "SELL":
            continue
        require(handoff is not None and action.obligation_id == handoff.obligation_id, "ORIGINAL_OBLIGATION_CLAIM_REQUIRED")
        _original_action(binding, context, seq, action, handoff, evaluations)
        attempts = [a for a in context["attempts"] if a.preparation.action_id == action.action_id]
        outcomes = [resolutions.get(a.preparation.attempt_id) for a in attempts]
        # Only positive applied reduction frees a B3 action claim. Failure,
        # non-landing, cancellation and UNKNOWN do not select a replacement.
        reduced = bool(outcomes) and all(r is not None or a.recorded_stage == "CANCELLED_UNSIGNED"
            for a,r in zip(attempts,outcomes)) and any(
            r is not None and r.disposition == "FINALIZED_SUCCESS_APPLIED" for r in outcomes)
        if not reduced:
            claims.append(action)
    encumbered = sum(a.input_units for a in claims)
    require(encumbered <= original.remaining_units, "CLAIMS_EXCEED_ACTUAL_REMAINING")
    snapshot = ProtectiveObligationSnapshot(binding, _obligation_id(binding), handoff, evaluation,
        original.cut, evaluation.payload["state"]["state"], original.acquired_units, original.sold_units,
        original.remaining_units, encumbered, original.remaining_units-encumbered, tuple(claims),
        context["authority"]["hard_stop_command"], context["lane"]["held"])
    return snapshot, context


def protective_obligation_snapshot(ledger, binding):
    """Revalidate actual custody and trusted B2/initial-handoff/action history."""
    return _read(ledger, binding)[0]


def ensure_protective_obligation(ledger, binding, *, recorded_at_utc):
    """Persist initial original protection and usable inventory atomically.

    Source/ENTRY/hard-stop controls cannot erase protection. A hard stop still
    denies new SELL decisions below and remains an Authority SIGN/SEND gate.
    """
    snapshot, context = _read(ledger, binding)
    if snapshot.handoff is not None:
        return snapshot
    require(snapshot.remaining_units == snapshot.acquired_units and snapshot.remaining_units > 0,
        "ORIGINAL_UNPROTECTED_ACQUISITION_REQUIRED")
    supplied = _handoff(binding, snapshot.evaluation, snapshot.cut, snapshot.acquired_units, recorded_at_utc)
    ledger.record_protective_handoff(supplied, ingestion_key="runtime-protection:"+snapshot.obligation_id,
        fence=context["snapshot"]["fence"])
    return protective_obligation_snapshot(ledger, binding)


def stage_protective_sell(ledger, binding, *, max_units=None):
    """Persist one immutable reduction from current unencumbered actual units.

    max_units is an explicit optional reduction ceiling, never a strategy
    parameter. The default claims all currently unencumbered units. Existing
    staged B3 decisions replay unchanged; they confer no retry/message grant.
    All new decisions use the exact read fence, so a competing action, hard
    stop, settlement or B2 commit invalidates this calculation before staging.
    """
    if max_units is not None:
        u64(max_units)
        require(max_units > 0, "POSITIVE_REDUCTION_CEILING_REQUIRED")
    snapshot, context = _read(ledger, binding)
    require(snapshot.handoff is not None and snapshot.state == "DUE", "COMMITTED_DUE_PROTECTION_REQUIRED")
    require(snapshot.hard_stop_command is None, "GLOBAL_HARD_STOP")
    if snapshot.claiming_actions:
        require(len(snapshot.claiming_actions) == 1 and snapshot.claiming_actions[0].external_decision_ref.startswith(SELL_REF),
            "EXISTING_UNRESOLVED_REDUCTION_CLAIM")
        action = snapshot.claiming_actions[0]
        require(max_units is None or action.input_units == min(snapshot.remaining_units,max_units),
            "PENDING_ACTION_REPLACEMENT_NOT_AUTHORIZED")
        return action
    require(not snapshot.mutation_lane_held and snapshot.unencumbered_units > 0, "CURRENT_MUTATION_LANE_OR_UNITS_UNAVAILABLE")
    units = snapshot.unencumbered_units if max_units is None else min(snapshot.unencumbered_units,max_units)
    ordinal = max((a.ordinal for _,a in context["actions"] if a.side == "SELL"),default=0)+1
    action = PendingAction(binding.root_id, binding.candidate.content_digest, "SELL", binding.mint,
        context["position"].token_program, binding.position_id, units, SELL_REF+snapshot.evaluation.content_digest,
        _decision_digest(binding,snapshot.handoff,snapshot.evaluation,snapshot.remaining_units,units,ordinal,
            context["position"].token_program),binding.policy_ref,binding.policy_digest,binding.spec.track_id,
        snapshot.obligation_id,ordinal)
    return ledger.stage_action(action,fence=context["snapshot"]["fence"])
