"""Finite original profile/stage records. Fresh call deliveries have no codec."""
from __future__ import annotations

from dataclasses import asdict
from phase5.shadow_domain_v0_1 import canonical_json
from .authority_controls_v0_1 import OperatorProvenance, eligibility_from_record, require
from .authority_message_evidence_v0_1 import MessageValidationProfile, VERSION as MESSAGE_VERSION
from .authority_message_codec_v0_1 import encode_validation_input, decode_validation_input
from .authority_message_control_v0_1 import (
    VERSION, MessageProfileCommand, MessageProfileReceipt, MessageStageRequest,
    MessageStageOriginal, MessageStageDecision, CurrentMessageRisk, MessageStageReceipt,
)
from .ledger_domain_v0_1 import LedgerContractError, digest_value
from .ledger_evidence_codec_v0_1 import strict_json_object
from .public_rpc_v0_1 import u64

CODEC_VERSION = "live_authority_message_control_codec_v0.1"
MAX_BYTES = 16*1024*1024


def _load(payload):
    require(type(payload) is str and len(payload.encode()) <= MAX_BYTES, "AUTHORITY_MESSAGE_RECORD_BOUND_INVALID")
    row = strict_json_object(payload)
    require(row.pop("codec_version") == CODEC_VERSION, "AUTHORITY_MESSAGE_RECORD_VERSION_INVALID")
    return row


def _json(row):
    value = canonical_json({"codec_version": CODEC_VERSION, **row})
    require(len(value.encode()) <= MAX_BYTES, "AUTHORITY_MESSAGE_RECORD_BOUND_INVALID")
    return value


def _fixed(row, values):
    row = dict(row)
    for name, expected in values.items():
        actual = row.pop(name)
        require(type(actual) is type(expected) and actual == expected, "AUTHORITY_MESSAGE_HISTORICAL_FLAGS_INVALID")
    return row


def profile_command_from_record(record):
    row = _fixed(record, {"version": VERSION})
    profile = _fixed(row.pop("profile"), {"version": MESSAGE_VERSION})
    profile["approval"] = OperatorProvenance(**profile["approval"])
    row["profile"] = MessageValidationProfile(**profile)
    row["approval"] = OperatorProvenance(**row["approval"])
    value = MessageProfileCommand(**row)
    require(canonical_json(asdict(value)) == canonical_json(record), "AUTHORITY_MESSAGE_PROFILE_CODEC_CONFLICT")
    return value


def profile_receipt_to_json(value):
    require(type(value) is MessageProfileReceipt, "AUTHORITY_MESSAGE_PROFILE_RECEIPT_REQUIRED")
    return _json(asdict(value))


def profile_receipt_from_json(payload):
    try:
        row = _fixed(_load(payload), {"version": VERSION, "grants_message_permission": False})
        row["command"] = profile_command_from_record(row["command"])
        u64(row["sequence"])
        require(row["sequence"] > 0, "AUTHORITY_MESSAGE_PROFILE_SEQUENCE_INVALID")
        for name in ("previous_authority_digest", "resulting_authority_digest", "previous_common_digest"):
            digest_value(row[name])
        if row["previous_selection_digest"] is not None:
            digest_value(row["previous_selection_digest"])
        value = MessageProfileReceipt(**row)
        require(profile_receipt_to_json(value) == payload, "AUTHORITY_MESSAGE_PROFILE_RECEIPT_CODEC_CONFLICT")
        return value
    except Exception:
        raise LedgerContractError("AUTHORITY_MESSAGE_PROFILE_RECEIPT_DECODING_INVALID") from None


def _original_record(value):
    require(type(value) is MessageStageOriginal, "AUTHORITY_MESSAGE_STAGE_ORIGINAL_REQUIRED")
    return {"request": asdict(value.request), "validation_json": encode_validation_input(value.validation),
        "selection_digest": value.selection_digest, "source": None if value.source is None else asdict(value.source)}


def _original(row):
    require(set(row) == {"request", "validation_json", "selection_digest", "source"}, "AUTHORITY_MESSAGE_ORIGINAL_FIELDS_INVALID")
    request = MessageStageRequest(**_fixed(row["request"], {"version": VERSION}))
    return MessageStageOriginal(request, decode_validation_input(row["validation_json"]), row["selection_digest"],
        None if row["source"] is None else eligibility_from_record(row["source"]))


def original_to_json(value):
    return _json(_original_record(value))


def original_from_json(payload):
    try:
        value = _original(_load(payload))
        require(original_to_json(value) == payload, "AUTHORITY_MESSAGE_ORIGINAL_CODEC_CONFLICT")
        return value
    except Exception:
        raise LedgerContractError("AUTHORITY_MESSAGE_ORIGINAL_DECODING_INVALID") from None


def receipt_to_json(value):
    require(type(value) is MessageStageReceipt, "AUTHORITY_MESSAGE_STAGE_RECEIPT_REQUIRED")
    record = asdict(value)
    record["original"] = _original_record(value.original)
    return _json(record)


def receipt_from_json(payload):
    try:
        row = _fixed(_load(payload), {"version": VERSION, "historical_only": True,
            "grants_message_permission": False, "may_sign": False, "may_send": False})
        row["original"] = _original(row["original"])
        decision = _fixed(row["decision"], {"version": VERSION, "grants_message_permission": False, "may_sign": False, "may_send": False})
        risk = _fixed(decision["risk"], {"funds_adopted_from_snapshot": False, "releases_reservation": False})
        # Aggregate required/outstanding values may exceed u64 ONLY in a denied
        # record; exact arbitrary Python integers preserve the overflow evidence.
        for name, value in risk.items():
            require(value is None or type(value) is int and 0 <= value < (1<<128), "AUTHORITY_MESSAGE_RISK_INTEGER_INVALID")
        decision["risk"] = CurrentMessageRisk(**risk)
        for name in ("reasons", "clock_reasons"):
            require(type(decision[name]) is list and len(decision[name]) <= 256 and all(type(v) is str
                and 0 < len(v) <= 160 for v in decision[name]), "AUTHORITY_MESSAGE_REASON_BOUND_INVALID")
            decision[name] = tuple(decision[name])
        require(decision["disposition"] in ("CURRENT_STAGE_DENIED_OR_UNRESOLVED", "CONSUMED_AT_ORIGINAL_CURRENT_CUT"),
            "AUTHORITY_MESSAGE_DISPOSITION_INVALID")
        require((not decision["reasons"]) == (decision["disposition"] == "CONSUMED_AT_ORIGINAL_CURRENT_CUT"),
            "AUTHORITY_MESSAGE_POSITIVE_REASONS_CONFLICT")
        for name in ("validation_digest", "prior_sign_digest", "prior_send_digest", "previous_rebroadcast_digest"):
            if decision[name] is not None:
                digest_value(decision[name])
        row["decision"] = MessageStageDecision(**decision)
        for name in ("sequence", "writer_generation"):
            u64(row[name])
            require(row[name] > 0, "AUTHORITY_MESSAGE_SEQUENCE_INVALID")
        for name in ("comparison_digest", "previous_authority_digest", "resulting_authority_digest",
                     "previous_custody_digest", "resulting_custody_digest", "previous_common_digest"):
            digest_value(row[name])
        value = MessageStageReceipt(**row)
        require(receipt_to_json(value) == payload, "AUTHORITY_MESSAGE_RECEIPT_CODEC_CONFLICT")
        return value
    except Exception:
        raise LedgerContractError("AUTHORITY_MESSAGE_RECEIPT_DECODING_INVALID") from None
