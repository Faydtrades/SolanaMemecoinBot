"""Closed exact codec for A4a original public evidence; no dynamic registry.

The supplied decoded P5 payloads remain claims. Only the validator re-derives
their semantics from the retained original bytes. No factory token is encoded.
"""
from __future__ import annotations

from dataclasses import asdict
from phase5.shadow_domain_v0_1 import ExecutionIntentV01, canonical_json, content_fingerprint
from phase5 import shadow_unsigned_plan_simulation_v0_1 as plans
from phase5 import shadow_venue_route_quote_v0_1 as venue
from .authority_controls_v0_1 import OperatorProvenance, policy_from_record, clock_from_record, require
from .authority_admission_v0_1 import admission_receipt_from_record
from .ledger_actions_v0_1 import action_from_json, candidate_from_json
from .ledger_domain_v0_1 import LedgerDomain, LedgerContractError
from .ledger_ports_v0_1 import ConsumerCut, ProtectionFact, handoff_from_record, port_receipt_from_record
from .ledger_custody_v0_1 import PositionView
from .ledger_settlement_v0_1 import WalletSupportInput
from .ledger_evidence_codec_v0_1 import (
    strict_json_object, _account_record, _account, wallet_observation_to_json, wallet_observation_from_json,
)
from .wallet_evidence_v0_1 import ExpectedTokenAccount
from .authority_message_evidence_v0_1 import (
    VERSION, MessageValidationProfile, PublicReadCut, OriginalAccountBatch, OriginalVenueRead,
    FeeForMessageRead, SimulationProvenance, ExternalMessageEvidence, MessageContext, MessageValidationInput,
)

CODEC_VERSION = "live_authority_message_codec_v0.1"
MAX_PAYLOAD_BYTES = 16*1024*1024


def _object(value, keys):
    require(type(value) is dict and set(value) == set(keys), "MESSAGE_CODEC_FIELDS_INVALID")
    return dict(value)


def _array(value):
    require(type(value) is list, "MESSAGE_CODEC_ARRAY_INVALID")
    return value


def _batch_record(value):
    return None if value is None else {"cut": asdict(value.cut), "requested_keys": value.requested_keys,
                                     "accounts": tuple(_account_record(item) for item in value.accounts)}


def _batch(value):
    if value is None:
        return None
    row = _object(value, ("cut", "requested_keys", "accounts"))
    return OriginalAccountBatch(PublicReadCut(**row["cut"]), tuple(_array(row["requested_keys"])),
                                tuple(_account(item) for item in _array(row["accounts"])))


def _simulation_record(value):
    return {"leases": [item.payload() for item in value.leases], "validity": [item.payload() for item in value.validity],
        "envelopes": [item.payload() for item in value.envelopes], "attempts": [item.payload() for item in value.attempts],
        "results": [item.payload() for item in value.results]}


def simulation_run_digest(value):
    return content_fingerprint(_simulation_record(value))


def _simulation(value):
    value = _object(value, ("leases", "validity", "envelopes", "attempts", "results"))
    require(all(1 <= len(_array(rows)) <= 2 for rows in value.values()), "MESSAGE_CODEC_SIMULATION_BOUND_INVALID")
    leases, validity, envelopes, attempts, results = [], [], [], [], []
    for raw in value["leases"]:
        row = dict(raw)
        require(row.pop("schema_version") == plans.LEASE_SCHEMA_VERSION, "MESSAGE_CODEC_LEASE_VERSION_INVALID")
        leases.append(plans.BlockhashLeaseV01(**row))
    for raw in value["validity"]:
        row = dict(raw)
        require(row.pop("schema_version") == plans.VALIDITY_SCHEMA_VERSION, "MESSAGE_CODEC_VALIDITY_VERSION_INVALID")
        validity.append(plans.BlockhashValidityEvidenceV01(**row))
    for raw in value["envelopes"]:
        row = dict(raw)
        require(row.pop("schema_version") == plans.ENVELOPE_SCHEMA_VERSION, "MESSAGE_CODEC_ENVELOPE_VERSION_INVALID")
        row["request_config_json"] = canonical_json(row.pop("request_config"))
        envelopes.append(plans.SimulationEnvelopeV01(**row))
    for raw in value["attempts"]:
        row = dict(raw)
        require(row.pop("schema_version") == plans.ATTEMPT_SCHEMA_VERSION, "MESSAGE_CODEC_ATTEMPT_VERSION_INVALID")
        attempts.append(plans.SimulationAttemptV01(**row))
    for raw in value["results"]:
        row = dict(raw)
        require(row.pop("schema_version") == plans.RESULT_SCHEMA_VERSION, "MESSAGE_CODEC_RESULT_VERSION_INVALID")
        row["outcome"] = plans.SimulationOutcome(row["outcome"])
        row["logs"] = tuple(_array(row["logs"]))
        require(len(row["logs"]) <= 4096 and all(type(log) is str and len(log) <= 16384 for log in row["logs"]),
                "MESSAGE_CODEC_PUBLIC_LOG_BOUND_INVALID")
        for name in ("error", "return_data", "inner_instructions", "returned_accounts"):
            row[name+"_json"] = canonical_json(row.pop(name))
        results.append(plans.ShadowSimulationResultV01(**row))
    return plans.SimulationRunEvidenceV01(tuple(leases), tuple(validity), tuple(envelopes), tuple(attempts), tuple(results))


def encode_validation_input(value):
    try:
        require(type(value) is MessageValidationInput, "MESSAGE_CODEC_EXACT_INPUT_REQUIRED")
        c, e = value.context, value.evidence
        record = {"codec_version": CODEC_VERSION, "context": {
            "domain": c.domain.to_record(), "candidate": c.candidate.to_record(), "acceptance": asdict(c.acceptance),
            "ledger_admission": asdict(c.ledger_admission),
            "admitted_buy": c.admitted_buy.to_record(), "action": c.action.to_record(),
            "original_policy": asdict(c.original_policy), "entry_policy": None if c.entry_policy is None else asdict(c.entry_policy),
            "cut": asdict(c.cut), "required_wallet_context_slot": c.required_wallet_context_slot,
            "position": None if c.position is None else asdict(c.position),
            "protection": None if c.protection is None else asdict(c.protection)},
            "profile": asdict(value.profile), "clock": asdict(value.clock), "evidence": {
                "version": e.version, "intent": e.intent.to_record(),
                "venue_read": {"primary": _batch_record(e.venue_read.primary), "dependent": _batch_record(e.venue_read.dependent),
                               "verification": _batch_record(e.venue_read.verification)},
                "wallet": {"observation_json": wallet_observation_to_json(e.wallet.observation),
                    "evaluated_at_utc": e.wallet.evaluated_at_utc, "required_min_context_slot": e.wallet.required_min_context_slot},
                "quote_policy": e.quote_policy.payload(), "plan_policy": e.plan_policy.payload(),
                "supplied_state_json": e.supplied_state_json, "supplied_route_json": e.supplied_route_json,
                "supplied_quote_json": e.supplied_quote_json, "supplied_plan_json": e.supplied_plan_json,
                "simulation": _simulation_record(e.simulation), "simulation_provenance": asdict(e.simulation_provenance),
                "fee": None if e.fee is None else asdict(e.fee),
                "setup_accounts": _batch_record(e.setup_accounts)}}
        payload = canonical_json(record)
        require(len(payload.encode()) <= MAX_PAYLOAD_BYTES, "MESSAGE_CODEC_PAYLOAD_BOUND_EXCEEDED")
        return payload
    except Exception:
        raise LedgerContractError("MESSAGE_ORIGINAL_INPUT_ENCODING_INVALID") from None


def decode_validation_input(payload):
    try:
        record = _object(strict_json_object(payload), ("codec_version", "context", "profile", "clock", "evidence"))
        require(record["codec_version"] == CODEC_VERSION, "MESSAGE_CODEC_VERSION_INVALID")
        row = _object(record["context"], ("domain", "candidate", "acceptance", "ledger_admission", "admitted_buy", "action", "original_policy",
            "entry_policy", "cut", "required_wallet_context_slot", "position", "protection"))
        domain = dict(row["domain"])
        domain["expected_empty_token_accounts"] = tuple(ExpectedTokenAccount(**item) for item in _array(domain["expected_empty_token_accounts"]))
        protection = row["protection"]
        if protection is not None:
            protection = _object(protection, ("handoff", "sequence", "obligation_state", "implements_runtime_controller"))
            require(protection.pop("implements_runtime_controller") is False, "MESSAGE_CODEC_PROTECTION_PERMISSION_INVALID")
            protection["handoff"] = handoff_from_record(protection["handoff"])
            protection = ProtectionFact(**protection)
        context = MessageContext(LedgerDomain(**domain), candidate_from_json(canonical_json(row["candidate"])),
            admission_receipt_from_record(row["acceptance"]), port_receipt_from_record(row["ledger_admission"]),
            action_from_json(canonical_json(row["admitted_buy"])),
            action_from_json(canonical_json(row["action"])), policy_from_record(row["original_policy"]),
            None if row["entry_policy"] is None else policy_from_record(row["entry_policy"]), ConsumerCut(**row["cut"]),
            row["required_wallet_context_slot"], None if row["position"] is None else PositionView(**row["position"]), protection)
        profile = dict(record["profile"])
        require(profile.pop("version") == VERSION, "MESSAGE_CODEC_PROFILE_VERSION_INVALID")
        profile["approval"] = OperatorProvenance(**profile["approval"])
        profile = MessageValidationProfile(**profile)
        e = _object(record["evidence"], ("version", "intent", "venue_read", "wallet", "quote_policy", "plan_policy",
            "supplied_state_json", "supplied_route_json", "supplied_quote_json", "supplied_plan_json", "simulation", "simulation_provenance", "fee", "setup_accounts"))
        require(e.pop("version") == VERSION, "MESSAGE_CODEC_EVIDENCE_VERSION_INVALID")
        intent = ExecutionIntentV01.from_record(e.pop("intent"))
        reads = _object(e.pop("venue_read"), ("primary", "dependent", "verification"))
        reads = OriginalVenueRead(_batch(reads["primary"]), _batch(reads["dependent"]), _batch(reads["verification"]))
        wallet = _object(e.pop("wallet"), ("observation_json", "evaluated_at_utc", "required_min_context_slot"))
        wallet = WalletSupportInput(wallet_observation_from_json(wallet["observation_json"]), wallet["evaluated_at_utc"], wallet["required_min_context_slot"])
        q = _object(e.pop("quote_policy"), ("schema_version", "slippage_bps"))
        require(q.pop("schema_version") == venue.POLICY_SCHEMA_VERSION, "MESSAGE_CODEC_QUOTE_POLICY_VERSION_INVALID")
        q = venue.QuotePolicyV01(**q)
        pp = dict(e.pop("plan_policy"))
        require(pp.pop("schema_version") == plans.POLICY_SCHEMA_VERSION, "MESSAGE_CODEC_PLAN_POLICY_VERSION_INVALID")
        pp = plans.TransactionPlanPolicyV01(**pp)
        simulation = _simulation(e.pop("simulation"))
        provenance = SimulationProvenance(**e.pop("simulation_provenance"))
        fee = e.pop("fee")
        if fee is not None:
            fee = dict(fee)
            fee["cut"] = PublicReadCut(**fee["cut"])
            fee = FeeForMessageRead(**fee)
        setup = _batch(e.pop("setup_accounts"))
        evidence = ExternalMessageEvidence(intent, reads, wallet, q, pp, simulation=simulation,
            simulation_provenance=provenance, fee=fee, setup_accounts=setup, **e)
        result = MessageValidationInput(context, profile, clock_from_record(record["clock"]), evidence)
        require(encode_validation_input(result) == payload, "MESSAGE_CODEC_CANONICAL_ROUNDTRIP_CONFLICT")
        return result
    except Exception:
        raise LedgerContractError("MESSAGE_ORIGINAL_INPUT_DECODING_INVALID") from None
