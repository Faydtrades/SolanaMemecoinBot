"""C1 finite DRY Runtime composition over accepted read-only Execution and L6.

Only an original Authority-admitted DRY BUY can enter this graph. There is no
signer, signed envelope, send claim, mutation transport or settlement consumer.
Simulation evidence is a read result; Ledger alone owns NON_SUBMITTED and the
release of tentative capacity. Process interruption is finished from durable
facts by finish_interrupted_dry, without regenerating an attempt or message.
"""
from __future__ import annotations

import json
from dataclasses import asdict

from phase5.shadow_domain_v0_1 import canonical_json, content_fingerprint
from .authority_controls_v0_1 import require
from .authority_message_evidence_v0_1 import capture_message_context
from .authority_message_codec_v0_1 import simulation_run_digest
from .execution_message_v0_1 import (
    ComputeBudget, _intent, _route_plan, construct_exact_readonly_message,
    simulate_exact_readonly_message,
)
from .execution_readonly_v0_1 import ExecutionReadOnlyRpc
from .ledger_actions_v0_1 import AttemptPreparation, AttemptStageInput, utc_microseconds
from .ledger_ports_v0_1 import DryTerminalInput

VERSION = "live_runtime_dry_v0.1"


def _original(repository, action_id):
    require(repository.domain.mode == "DRY", "RUNTIME_DRY_FIXED_DOMAIN_REQUIRED")
    action = repository.action(action_id)
    require(action is not None and action.side == "BUY", "RUNTIME_DRY_ORIGINAL_BUY_REQUIRED")
    reservation = repository.reservation(action.root_id)
    require(reservation is not None and reservation.admission.action == action,
        "RUNTIME_DRY_ORIGINAL_ADMISSION_REQUIRED")
    acceptance = repository.authority_acceptance(action.root_id)
    candidate = repository.candidate(action.root_id)
    require(acceptance is not None and acceptance.accepted and candidate is not None
        and candidate.trade_root(repository.domain) == action.root_id
        and candidate.content_digest == action.candidate_digest
        and acceptance.admission_digest == repository._port_at_sequence(reservation.sequence).content_digest,
        "RUNTIME_DRY_ACTUAL_AUTHORITY_LINEAGE_REQUIRED")
    return action, reservation


def _terminal(repository, action, reservation):
    if reservation.retired_sequence is None:
        require(reservation.status == "TENTATIVE_DRY", "RUNTIME_DRY_TENTATIVE_CAPACITY_REQUIRED")
        return None
    receipt = repository._port_at_sequence(reservation.retired_sequence)
    require(receipt.kind == "DRY_NON_SUBMITTED" and receipt.dry_terminal.action_id == action.action_id,
        "RUNTIME_DRY_ORIGINAL_TERMINAL_REQUIRED")
    return receipt


def finish_interrupted_dry(repository, action_id, *, recorded_at_utc):
    """Idempotent L6 handoff after interruption; never produces or simulates.

    Original terminal receipt/cut/key wins on redelivery, even after reopen.
    Before preparation there is genuinely no attempt or simulation identity.
    Prepared and simulated histories retain their actual original lineage.
    """
    with repository._trusted_read():
        action, reservation = _original(repository, action_id)
        existing = _terminal(repository, action, reservation)
        if existing is not None:
            return existing
        attempts = repository._root_attempts(action.root_id)
        attempt = None if not attempts else max(attempts, key=lambda item: item.preparation.ordinal)
        prep = None if attempt is None else attempt.preparation
        simulation = None if prep is None else repository._simulation_input_digest(prep.attempt_id)
        reason = ("DRY_INTERRUPTED_BEFORE_PREPARATION" if prep is None else
            "DRY_CAPABILITY" if simulation is not None else "DRY_INTERRUPTED_NO_SIGNED_GRAPH")
        snapshot = repository.consumer_snapshot()
        facts = {"version": VERSION, "domain": repository.domain.economic_domain_id,
            "candidate_digest": action.candidate_digest, "action_digest": action.content_digest,
            "preparation_digest": None if prep is None else prep.content_digest,
            "simulation_input_digest": simulation, "reason": reason}
        supplied = DryTerminalInput(snapshot["consumer_cut"], action.root_id, action.action_id,
            None if prep is None else prep.attempt_id, facts["preparation_digest"],
            None if prep is None else prep.message_sha256, simulation, reason,
            "RUNTIME_DRY_NON_SUBMITTED", content_fingerprint(facts), recorded_at_utc)
        fence = snapshot["fence"]
    return repository.finish_dry(supplied, ingestion_key="runtime-dry-"+action.root_id, fence=fence)


def _simulation_record(run, rpc, preparation):
    """Bounded original-result witness for L2's finite external stage record.

    Exact original run/read/request/result hashes bind the retained outcome.
    Large RPC logs/CPI/account payloads stay at the read-evidence boundary;
    this receipt never purports to be a full A4a evidence replay or permission.
    """
    result = run.results[0]
    raw = json.loads(rpc.records[-1])
    return {"version": VERSION, "preparation_digest": preparation.content_digest,
        "simulation_run_digest": simulation_run_digest(run),
        "original_read_record_digest": rpc.record_digest,
        "request_digest": content_fingerprint(raw["params"]),
        "response_digest": content_fingerprint(raw["result"]),
        "lease_digest": run.leases[0].fingerprint,
        "validity_digest": run.validity[0].fingerprint,
        "envelope_digest": run.envelopes[0].fingerprint,
        "simulation_attempt_digest": run.attempts[0].fingerprint,
        "result_digest": result.fingerprint, "outcome": result.outcome.value,
        "reason_code": result.reason_code, "context_slot": result.context_slot,
        "observed_at_us": result.observed_at_us, "units_consumed": result.units_consumed,
        "loaded_accounts_data_size": result.loaded_accounts_data_size}


def run_dry(repository, action_id, rpc, wallet, quote_policy, plan_policy, compute, *, clock, now_us):
    """One original DRY action -> exact construction/read -> durable terminal.

    Redelivery of prepared work finishes its durable history; it does not retry
    transport, replace bytes or create another ordinal. Qualified clocks and
    original wallet facts remain external boundaries, as in Q1 Execution.
    """
    with repository._trusted_read():
        action, reservation = _original(repository, action_id)
        existing = _terminal(repository, action, reservation)
        if existing is not None:
            return existing
        interrupted = bool(repository._root_attempts(action.root_id))
        context = capture_message_context(repository, action_id)
    if interrupted:
        return finish_interrupted_dry(repository, action_id, recorded_at_utc=clock().utc_upper_utc)
    require(type(rpc) is ExecutionReadOnlyRpc and not rpc.records and type(compute) is ComputeBudget,
        "RUNTIME_DRY_FRESH_READONLY_SESSION_REQUIRED")
    require(rpc.profile.fingerprint == context.domain.expected_profile_fingerprint,
        "RUNTIME_DRY_ORIGINAL_PROVIDER_REQUIRED")
    policy = context.original_policy
    priority = (compute.units*compute.micro_lamports+999999)//1000000
    require(priority <= policy.costs.priority_fee_within_total_lamports,
        "RUNTIME_DRY_PRIORITY_POLICY_BOUND")
    initial_clock = clock()
    require(initial_clock.status == "QUALIFIED"
        and (initial_clock.provider_id, initial_clock.provider_fingerprint) ==
            (policy.clock.provider_id, policy.clock.provider_fingerprint)
        and 0 <= utc_microseconds(initial_clock.utc_upper_utc)-utc_microseconds(initial_clock.utc_lower_utc)
            <= policy.clock.maximum_uncertainty_us,
        "RUNTIME_DRY_QUALIFIED_CLOCK_REQUIRED")
    intent = _intent(context, utc_microseconds(initial_clock.utc_lower_utc))
    try:
        rpc.bind_genesis(context.domain.genesis_hash)
        reads, state, route, quote, plan = _route_plan(context, intent, rpc, wallet, quote_policy, plan_policy, initial_clock)
        started, lease, config, envelope, setup, fee, validity = construct_exact_readonly_message(
            plan, context.domain.wallet, compute, rpc, now_us=now_us)
    except (ValueError, RuntimeError):
        return finish_interrupted_dry(repository, action_id, recorded_at_utc=clock().utc_upper_utc)
    construction = {"version": VERSION, "context_cut": asdict(context.cut),
        "candidate_digest": context.candidate.content_digest, "action_digest": action.content_digest,
        "original_policy_digest": policy.content_digest, "intent_digest": intent.fingerprint,
        "state_digest": state.fingerprint, "route_digest": route.fingerprint, "quote_digest": quote.fingerprint,
        "plan_digest": plan.fingerprint, "read_digest": rpc.record_digest,
        "envelope_digest": envelope.fingerprint}
    config_digest = content_fingerprint({"version": VERSION, "original_policy_digest": policy.content_digest,
        "quote_policy": quote_policy.payload(), "plan_policy": plan_policy.payload(), "compute": asdict(compute)})
    prepared_at = clock().utc_upper_utc
    read_times = tuple(json.loads(record)["observed_at_us"] for record in rpc.records)
    require(all(left <= right for left, right in zip(read_times, read_times[1:]))
        and read_times[-1] <= utc_microseconds(prepared_at), "RUNTIME_DRY_PREPARATION_TIME_CONFLICT")
    prep = AttemptPreparation(action.action_id, action.content_digest, 1, envelope.message_hex, plan.fingerprint,
        config_digest, lease, wallet.observation.anchor, prepared_at,
        "RUNTIME_DRY_EXACT_PREPARATION", content_fingerprint(construction))
    with repository._trusted_read():
        require(capture_message_context(repository, action_id) == context, "RUNTIME_DRY_PREPARATION_CONTEXT_CHANGED")
        fence = repository.write_fence()
    stored = repository.prepare_attempt(prep, fence=fence)
    try:
        simulation_started = now_us()
        require(utc_microseconds(prepared_at) <= simulation_started, "RUNTIME_DRY_SIMULATION_PRECEDES_PREPARATION")
        run = simulate_exact_readonly_message(plan, lease, config, envelope, validity, rpc, now_us=lambda: simulation_started)
        recorded_at = clock().utc_upper_utc
        require(simulation_started <= run.results[0].observed_at_us <= utc_microseconds(recorded_at),
            "RUNTIME_DRY_SIMULATION_TIME_CONFLICT")
    except (ValueError, RuntimeError):
        return finish_interrupted_dry(repository, action_id, recorded_at_utc=clock().utc_upper_utc)
    original = _simulation_record(run, rpc, prep)
    stage = AttemptStageInput(prep.attempt_id, "EXACT_SIMULATED", action.content_digest, prep.message_sha256,
        lease.fingerprint, config_digest, recorded_at, "RUNTIME_DRY_ORIGINAL_SIMULATION",
        content_fingerprint(original), external_record_json=canonical_json(original))
    repository.record_external_attempt_stage(stage, idempotency_key="runtime-dry-simulation-"+prep.attempt_id,
        expected_attempt_revision=stored.revision, fence=repository.write_fence())
    return finish_interrupted_dry(repository, action_id, recorded_at_utc=clock().utc_upper_utc)
