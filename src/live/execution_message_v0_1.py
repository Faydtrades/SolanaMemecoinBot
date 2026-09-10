"""Q1: immutable accepted LIVE economics to exact simulation and Ledger preparation.

One bounded read sequence, one lease, one final legacy message, one simulation.
Authority validates the original evidence; its result is context, never permission.
No signer, send, replacement policy, scheduler or Shadow persistence is reachable.
"""
from __future__ import annotations
import base64
import hashlib
import json
from dataclasses import dataclass, asdict, replace
from solders.compute_budget import set_compute_unit_limit, set_compute_unit_price
from solders.message import Message
from solders.pubkey import Pubkey
from solders.hash import Hash
from phase5 import shadow_unsigned_plan_simulation_v0_1 as plans
from phase5 import shadow_venue_route_quote_v0_1 as venue
from phase5.shadow_domain_v0_1 import ExecutionIntentV01, IntentRole, IntentSide, canonical_json, content_fingerprint
from .authority_controls_v0_1 import require
from .authority_message_evidence_v0_1 import (
    capture_message_context, _entry_intent, ExternalMessageEvidence, OriginalVenueRead,
    MessageValidationInput, SimulationProvenance, validate_message_evidence,
)
from .authority_message_codec_v0_1 import encode_validation_input, decode_validation_input, simulation_run_digest
from .ledger_actions_v0_1 import AttemptPreparation, utc_microseconds
from .wallet_evidence_v0_1 import ledger_account_evidence
from .public_rpc_v0_1 import u64
from .execution_readonly_v0_1 import ExecutionReadOnlyRpc

VERSION = "live_execution_message_v0.1"

@dataclass(frozen=True, slots=True)
class ComputeBudget:
    units: int
    micro_lamports: int

    def __post_init__(self):
        require(0 < u64(self.units) <= (1 << 32)-1, "EXECUTION_COMPUTE_UNITS_INVALID")
        u64(self.micro_lamports)

@dataclass(frozen=True, slots=True)
class ExactMessageProduction:
    """Canonical original evidence is the immutable consumer transfer contract."""
    validation_input_json: str
    original_read_records: tuple[str, ...]
    version: str = VERSION

    def __post_init__(self):
        require(self.version == VERSION and type(self.original_read_records) is tuple,
            "EXECUTION_ORIGINAL_PRODUCTION_REQUIRED")
        original = decode_validation_input(self.validation_input_json)
        require(encode_validation_input(original) == self.validation_input_json,
            "EXECUTION_CANONICAL_INPUT_REQUIRED")
        require(original.evidence.simulation_provenance.original_read_record_digest == content_fingerprint(
            {"version": "live_execution_reads_v0.1", "reads": self.original_read_records}),
            "EXECUTION_ORIGINAL_READ_RECORD_CONFLICT")
        _validate_capture(original, self.original_read_records)

    @property
    def original(self):
        return decode_validation_input(self.validation_input_json)

    @property
    def validation(self):
        return validate_message_evidence(self.original)

    @property
    def message_hex(self):
        return self.original.evidence.simulation.envelopes[0].message_hex

    @property
    def content_digest(self):
        return content_fingerprint(asdict(self))


def _validate_capture(original, records):
    """Bind finite original transport facts back to the exact consumer evidence."""
    rows = tuple(json.loads(value) for value in records)
    methods = ("getGenesisHash", "getMultipleAccounts", "getMultipleAccounts", "getMultipleAccounts",
        "getLatestBlockhash", "getMultipleAccounts", "getFeeForMessage", "getBlockHeight",
        "isBlockhashValid", "simulateTransaction")
    require(tuple(row["method"] for row in rows) == methods, "EXECUTION_SINGLE_READ_SEQUENCE_REQUIRED")
    for raw, row in zip(records, rows):
        require(canonical_json(row) == raw and set(row) == {"method", "params", "result", "observed_at_us"},
            "EXECUTION_PUBLIC_READ_RECORD_SHAPE_INVALID")
        u64(row["observed_at_us"])
    require(all(left["observed_at_us"] <= right["observed_at_us"] for left, right in zip(rows, rows[1:])),
        "EXECUTION_CAPTURE_TIME_REGRESSION")
    e = original.evidence
    run = e.simulation
    require(len(run.envelopes) == 1, "EXECUTION_SINGLE_EXACT_MESSAGE_REQUIRED")
    lease, validity, envelope, result = run.leases[0], run.validity[0], run.envelopes[0], run.results[0]
    require(lease.observed_at_us == rows[4]["observed_at_us"]
        and validity.observed_at_us == rows[8]["observed_at_us"]
        and result.observed_at_us == e.simulation_provenance.observed_at_us == rows[9]["observed_at_us"]
        and e.simulation_provenance.started_at_us <= lease.observed_at_us
        and validity.observed_at_us <= run.attempts[0].observed_at_us <= result.observed_at_us,
        "EXECUTION_CAPTURE_SIMULATION_TIME_CONFLICT")
    floor = max(json.loads(e.supplied_plan_json)["prerequisite_slot"], lease.context_slot)
    config = plans.SimulationRequestConfigV01(floor)
    require(rows[0]["params"] == [] and rows[0]["result"] == original.context.domain.genesis_hash,
        "EXECUTION_CAPTURE_GENESIS_CONFLICT")
    require(rows[4]["result"]["context"]["slot"] == lease.context_slot
        and rows[4]["result"]["value"] == {"blockhash": lease.blockhash, "lastValidBlockHeight": lease.last_valid_block_height}
        and rows[7]["result"] == validity.block_height
        and rows[8]["params"] == [lease.blockhash, {"commitment": "confirmed", "minContextSlot": floor}]
        and rows[8]["result"]["value"] is validity.rpc_valid
        and rows[8]["result"]["context"]["slot"] == validity.validity_context_slot,
        "EXECUTION_CAPTURE_LEASE_VALIDITY_CONFLICT")
    require(rows[9]["params"] == [envelope.transaction_base64, config.rpc_payload()]
        and rows[9]["result"]["context"]["slot"] == result.context_slot,
        "EXECUTION_CAPTURE_EXACT_SIMULATION_CONFLICT")
    response = rows[9]["result"]["value"]
    for name, serialized in (("err", result.error_json), ("innerInstructions", result.inner_instructions_json),
                            ("returnData", result.return_data_json), ("accounts", result.returned_accounts_json)):
        require(canonical_json(response.get(name)) == serialized, "EXECUTION_CAPTURE_SIMULATION_RESULT_CONFLICT")
    if result.outcome is plans.SimulationOutcome.SIMULATION_SUCCESS:
        require(response.get("unitsConsumed") == result.units_consumed
            and response.get("loadedAccountsDataSize") == result.loaded_accounts_data_size
            and tuple(response.get("logs") or ()) == result.logs, "EXECUTION_CAPTURE_SIMULATION_METRICS_CONFLICT")
    require(e.fee is not None and rows[6]["params"] == [e.fee.request_message_base64,
        {"commitment": "confirmed", "minContextSlot": floor}]
        and base64.b64decode(e.fee.request_message_base64).hex() == envelope.message_hex
        and rows[6]["result"]["value"] == e.fee.fee_lamports
        and rows[6]["result"]["context"]["slot"] == e.fee.cut.context_slot
        and rows[6]["observed_at_us"] == e.fee.cut.observed_at_us,
        "EXECUTION_CAPTURE_EXACT_FEE_CONFLICT")
    for row, batch in zip((rows[1], rows[2], rows[3], rows[5]),
            (e.venue_read.primary, e.venue_read.dependent, e.venue_read.verification, e.setup_accounts)):
        require(batch is not None and row["params"][0] == list(batch.requested_keys)
            and row["params"][1]["commitment"] == batch.cut.commitment
            and row["observed_at_us"] == batch.cut.observed_at_us
            and row["result"]["context"]["slot"] == batch.cut.context_slot
            and len(row["result"]["value"]) == len(batch.accounts), "EXECUTION_CAPTURE_ACCOUNT_CUT_CONFLICT")
        for raw, account in zip(row["result"]["value"], batch.accounts):
            if account is None:
                require(raw is None, "EXECUTION_CAPTURE_ACCOUNT_ABSENCE_CONFLICT")
            else:
                require(raw is not None and raw["owner"] == account.account.owner and raw["lamports"] == account.lamports
                    and raw["executable"] is account.executable and raw["rentEpoch"] == account.rent_epoch
                    and raw["data"] == [base64.b64encode(account.account.data).decode(), "base64"],
                    "EXECUTION_CAPTURE_ACCOUNT_BYTES_CONFLICT")


def _intent(context, decision_at_us):
    entry = _entry_intent(context)
    if context.action.side == "BUY":
        require(context.action == context.admitted_buy, "EXECUTION_ACCEPTED_ENTRY_REQUIRED")
        return entry
    c, action, protection, position = context.candidate, context.action, context.protection, context.position
    require(protection is not None and position is not None and 0 < action.input_units <= position.remaining_units,
        "EXECUTION_ACTUAL_REDUCTION_UNITS_REQUIRED")
    return ExecutionIntentV01(c.candidate_signal_id, c.candidate_run_id, c.strategy_evaluation_id,
        c.strategy_version, c.parameter_set_id, c.producer_run_id, c.source_event_key, c.signal_ingest_seq,
        decision_at_us, action.mint, IntentRole.EXIT, IntentSide.SELL, "MEME_BASE_UNITS", action.input_units,
        action.position_id, entry.intent_id, action.external_decision_ref, action.selected_exit_track,
        protection.handoff.binding_id)


def _route_plan(context, intent, rpc, wallet, quote_policy, plan_policy, clock):
    """Build using original surrounding reads and accepted Phase-5 factories."""
    mint = context.action.mint
    keys = (venue.derive_bonding_curve_pda(mint), venue.derive_pumpswap_pool_pda(mint))
    first = rpc.account_batch(keys, min_context_slot=max(context.required_wallet_context_slot, context.domain.minimum_context_slot))
    dependencies = {mint, venue.derive_pump_global_pda(), venue.derive_pump_fee_config_pda()}
    if first.accounts[1] is not None:
        decoded = venue.decode_pumpswap_pool(first.accounts[1].account, mint)
        dependencies.update((decoded.pool_base_token_account, decoded.pool_quote_token_account,
            venue.derive_pumpswap_global_pda(), venue.derive_pumpswap_fee_config_pda()))
    dependent = rpc.account_batch(tuple(sorted(dependencies)), min_context_slot=first.cut.context_slot)
    last = rpc.account_batch(keys, min_context_slot=dependent.cut.context_slot)
    require(first.accounts == last.accounts, "EXECUTION_VENUE_CHANGED_DURING_READ")
    reads = OriginalVenueRead(first, dependent, last)
    values = {key: value for batch in (first, dependent) for key, value in zip(batch.requested_keys, batch.accounts)}
    slots = {key: batch.cut.context_slot for batch in (first, dependent) for key in batch.requested_keys}
    def raw(key):
        value = values[key]
        require(value is not None and not value.executable, "EXECUTION_ORIGINAL_VENUE_ACCOUNT_UNAVAILABLE")
        return value.account
    curve_keys = (keys[0], mint, venue.derive_pump_global_pda(), venue.derive_pump_fee_config_pda())
    curve_slots = tuple(slots[key] for key in curve_keys)
    curve = venue.build_pump_state(intent, *(raw(key) for key in curve_keys), slot_min=min(curve_slots),
        slot_max=max(curve_slots), observed_at_us=last.cut.observed_at_us, account_slots=curve_slots)
    pool = None
    if first.accounts[1] is not None:
        pool_keys = (keys[1], decoded.pool_base_token_account, decoded.pool_quote_token_account, mint,
            venue.derive_pumpswap_global_pda(), venue.derive_pumpswap_fee_config_pda())
        pool_slots = tuple(slots[key] for key in pool_keys)
        pool = venue.build_pumpswap_state(intent, *(raw(key) for key in pool_keys), slot_min=min(pool_slots),
            slot_max=max(pool_slots), observed_at_us=last.cut.observed_at_us, account_slots=pool_slots)
    route = venue.decide_route(intent, curve, pool)
    state = curve if route.selected_state_id == curve.state_id else pool
    require(state is not None and route.selected_state_id == state.state_id, "EXECUTION_ROUTE_UNAVAILABLE")
    actual_venue = "PUMP" if type(state) is venue.PumpBondingCurveStateV01 else "PUMPSWAP"
    require(actual_venue in context.original_policy.allowed_venues and state.base_token_program == context.action.token_program,
        "EXECUTION_VENUE_OR_TOKEN_PROGRAM_UNSUPPORTED")
    port = ledger_account_evidence(wallet.observation, expected_wallet=context.domain.wallet,
        expected_genesis=context.domain.genesis_hash, expected_profile_fingerprint=context.domain.expected_profile_fingerprint,
        required_min_context_slot=max(context.required_wallet_context_slot, wallet.required_min_context_slot, last.cut.context_slot),
        now_utc=clock.utc_upper_utc)
    require(port.account_facts_usable, "EXECUTION_WALLET_ORIGINAL_FACTS_UNAVAILABLE")
    explicit = dict(zip(wallet.observation.explicit_read.requested_keys, wallet.observation.explicit_read.accounts))
    actor = plans.PublicShadowActorV01(context.domain.wallet)
    base = plans.derive_associated_token_address(actor.public_key, mint, context.action.token_program)
    wsol = plans.derive_associated_token_address(actor.public_key, venue.WSOL_MINT, venue.TOKEN_PROGRAM_ID)
    require({base, wsol, actor.public_key} <= explicit.keys(), "EXECUTION_WALLET_ACCOUNT_COVERAGE_REQUIRED")
    snapshot = plans.build_actor_account_snapshot(state, actor, base_account=None if explicit[base] is None else explicit[base].account,
        quote_account=None if explicit[wsol] is None else explicit[wsol].account,
        context_slot=port.assessment.context_slot, observed_at_us=utc_microseconds(wallet.observation.observed_at_utc))
    recipients = plans.decode_verified_recipient_evidence(state, raw(venue.derive_pump_global_pda()
        if actual_venue == "PUMP" else venue.derive_pumpswap_global_pda()))
    quote = venue.create_executable_quote(intent, state, route, quote_policy)
    costs = context.original_policy.costs
    require(quote_policy.slippage_bps <= costs.maximum_slippage_bps and quote.price_impact_ppm <= costs.maximum_impact_bps*100
        and quote.fees.total_fee <= costs.venue_fee_within_quote_cap_lamports, "EXECUTION_QUOTE_POLICY_BOUND")
    plan = plans.build_unsigned_transaction_plan(intent, state, route, quote, actor, plan_policy, recipients, snapshot)
    return reads, state, route, quote, plan


def produce_exact_message(repository, action_id, rpc, wallet, quote_policy, plan_policy, compute, *, clock, now_us):
    """One finite Q1 call. Errors fail closed; returned validation grants nothing.

    Clock suppliers are the externally qualified clock boundary, not freshness
    overrides. Wallet facts come from the original accepted Evidence observer.
    No refresh/replacement occurs on failed simulation or unavailable reads.
    """
    require(type(rpc) is ExecutionReadOnlyRpc and type(compute) is ComputeBudget and not rpc.records,
        "EXECUTION_FRESH_FINITE_READ_SESSION_REQUIRED")
    context = capture_message_context(repository, action_id)
    require(context.domain.mode == "LIVE", "EXECUTION_LIVE_DOMAIN_REQUIRED")
    selected = repository.authority_message_profile(context.action.policy_digest)
    require(selected is not None, "EXECUTION_SELECTED_MESSAGE_PROFILE_REQUIRED")
    profile = selected.command.profile
    require(rpc.profile.fingerprint == context.domain.expected_profile_fingerprint == profile.rpc_profile_fingerprint,
        "EXECUTION_ORIGINAL_PROVIDER_BINDING_REQUIRED")
    require(compute.units <= profile.maximum_compute_units and compute.micro_lamports <= profile.maximum_compute_unit_price_micro_lamports,
        "EXECUTION_COMPUTE_PROFILE_BOUND")
    priority = (compute.units*compute.micro_lamports+999999)//1000000
    require(priority <= context.original_policy.costs.priority_fee_within_total_lamports, "EXECUTION_PRIORITY_POLICY_BOUND")
    initial_clock = clock()
    intent = _intent(context, utc_microseconds(initial_clock.utc_lower_utc))
    rpc.bind_genesis(context.domain.genesis_hash)
    reads, state, route, quote, plan = _route_plan(context, intent, rpc, wallet, quote_policy, plan_policy, initial_clock)
    started = u64(now_us())
    lease = plans.acquire_blockhash_lease(rpc, plan, observed_at_us=started)
    lease = replace(lease, observed_at_us=rpc.last_observed_at_us)
    config = plans.SimulationRequestConfigV01(max(plan.prerequisite_slot, lease.context_slot))
    instructions = [set_compute_unit_limit(compute.units), set_compute_unit_price(compute.micro_lamports)]
    instructions.extend(ix.materialize() for ix in plan.instructions)
    message = Message.new_with_blockhash(instructions, Pubkey.from_string(context.domain.wallet), Hash.from_string(lease.blockhash))
    raw = bytes(message)
    wire = b"\x01"+bytes(64)+raw
    require(message.header.num_required_signatures == 1 and len(wire) <= 1232, "EXECUTION_FINAL_MESSAGE_SHAPE_UNSUPPORTED")
    envelope = plans.SimulationEnvelopeV01(plan.plan_id, plan.fingerprint, lease.lease_id, lease.fingerprint,
        config.fingerprint, canonical_json(config.payload()), raw.hex(), base64.b64encode(wire).decode(), 1, 1,
        hashlib.sha256(raw).hexdigest(), hashlib.sha256(wire).hexdigest())
    setup = rpc.account_batch(tuple(map(str, message.account_keys)), min_context_slot=config.min_context_slot)
    fee = rpc.fee_for_message(envelope.message_hex, min_context_slot=config.min_context_slot)
    validity = plans.verify_blockhash_lease(rpc, plan, lease, observed_at_us=u64(now_us()))
    validity = replace(validity, observed_at_us=rpc.last_observed_at_us)
    require(validity.rpc_valid and validity.block_height <= lease.last_valid_block_height, "EXECUTION_LEASE_UNAVAILABLE_OR_EXPIRED")
    attempt = plans.SimulationAttemptV01(plan.plan_id, lease.lease_id, envelope.envelope_id, config.fingerprint,
        validity.fingerprint, 0, u64(now_us()))
    response = rpc.simulate_transaction(envelope.transaction_base64, config=config.rpc_payload())
    result = plans.classify_simulation_response(plan, attempt, lease, envelope, config, response, observed_at_us=rpc.last_observed_at_us)
    run = plans.SimulationRunEvidenceV01((lease,), (validity,), (envelope,), (attempt,), (result,))
    provenance = SimulationProvenance(context.domain.genesis_hash, rpc.profile.fingerprint,
        "LIVE_EXECUTION_EXACT_READ", rpc.record_digest, simulation_run_digest(run), started, rpc.last_observed_at_us)
    evidence = ExternalMessageEvidence(intent, reads, wallet, quote_policy, plan_policy,
        *(canonical_json(value.payload()) for value in (state, route, quote, plan)), run, provenance, fee, setup)
    original = MessageValidationInput(context, profile, clock(), evidence)
    output = ExactMessageProduction(encode_validation_input(original), rpc.records)
    # Execute the actual A4a replay, including failures. Never substitute approval.
    output.validation
    return output


def prepare_exact_message(repository, production, *, fence, ordinal=1):
    """Explicit preparation; Ledger owns prior resolution and next ordinal.

    A replacement has newly produced/simulated exact bytes and retains the same
    immutable action. This call never refreshes a lease or grants SIGN/SEND.
    """
    require(type(production) is ExactMessageProduction, "EXECUTION_ORIGINAL_PRODUCTION_REQUIRED")
    original = production.original
    require(production.validation.disposition == "SUPPORTED_CONTEXT_ONLY", "EXECUTION_MESSAGE_CONTEXT_UNSUPPORTED")
    action, evidence = original.context.action, original.evidence
    envelope = evidence.simulation.envelopes[0]
    preparation = AttemptPreparation(action.action_id, action.content_digest, ordinal, envelope.message_hex,
        envelope.plan_fingerprint, original.profile.content_digest, evidence.simulation.leases[0],
        evidence.wallet.observation.anchor, original.clock.utc_upper_utc, "LIVE_EXECUTION_EXACT_PREPARATION", production.content_digest)
    existing = repository.attempt(preparation.attempt_id)
    if existing is None:
        require(capture_message_context(repository, action.action_id) == original.context,
            "EXECUTION_PREPARATION_CONTEXT_CHANGED")
    else:
        # Same original delivery is a historical idempotent lookup, never new
        # message permission or a replacement attempt after a crash/retry.
        require(existing.preparation == preparation, "EXECUTION_PREPARATION_RETRY_CONFLICT")
    return repository.prepare_attempt(preparation, fence=fence)
