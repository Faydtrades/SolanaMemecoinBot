"""Finite persistence codec for original, sanitized Wallet Evidence only.

No transport, generic type registry, dynamic construction or alternate assessment.
JSON integers remain integers (including the complete u64 range); bytes use
canonical base64. Decoding reconstructs the accepted immutable Evidence types.
"""
from __future__ import annotations

import base64
import json
from dataclasses import asdict

from phase5.shadow_domain_v0_1 import canonical_json
from phase5.shadow_venue_route_quote_v0_1 import RpcAccountV01
from .public_rpc_v0_1 import (
    MODEL_ID as EVIDENCE_MODEL_ID, FinalizedBlockAnchor, PublicAccount, PublicAccountRead, PublicRpcProfile,
    RpcContext, TokenInventoryRead,
)
from .wallet_evidence_v0_1 import (
    ExpectedTokenAccount, PublicReadFailure, WalletEvidenceRequest, WalletObservation,
)

CODEC_VERSION = "live_ledger_wallet_evidence_codec_v0.1"
MAX_PAYLOAD_BYTES = 16 * 1024 * 1024
_PROFILE_KEYS = (
    "provider_id", "provider_version", "max_response_bytes", "max_account_bytes",
    "max_inventory_accounts_per_program", "max_requests", "max_total_response_bytes",
    "request_timeout_seconds", "observation_timeout_seconds", "observation_freshness_seconds",
    "finalized_block_freshness_seconds", "max_block_signatures", "max_transaction_wire_bytes",
    "max_inner_instructions", "max_instruction_data_bytes", "trust_profile",
)
_ANCHOR_KEYS = ("slot", "blockhash", "previous_blockhash", "parent_slot", "block_height",
                "block_time", "provider_fingerprint", "commitment")
_OBSERVATION_KEYS = ("request", "profile", "started_at_utc", "observed_at_utc", "observed_genesis",
    "observed_genesis_end", "initial_finalized_slot", "finalized_upper_slot", "anchor", "inventories",
    "explicit_read", "mint_reads", "failures", "schema")


class LedgerEvidenceCodecError(ValueError):
    pass


def _object(value: object, keys: tuple[str, ...]) -> dict:
    if type(value) is not dict or set(value) != set(keys):
        raise LedgerEvidenceCodecError("WALLET_EVIDENCE_FIELDS_INVALID")
    return value


def _array(value: object) -> list:
    if type(value) is not list:
        raise LedgerEvidenceCodecError("WALLET_EVIDENCE_ARRAY_INVALID")
    return value


def _unique(pairs: list[tuple[str, object]]) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise LedgerEvidenceCodecError("DUPLICATE_JSON_KEY")
        result[key] = value
    return result


def _non_integer(_value: str) -> object:
    raise LedgerEvidenceCodecError("NON_INTEGER_JSON_NUMBER")


def strict_json_object(payload: str) -> dict:
    """A bounded exact-integer object, also used for concrete Ledger receipts."""
    try:
        if type(payload) is not str or len(payload.encode("utf-8")) > MAX_PAYLOAD_BYTES:
            raise ValueError
        record = json.loads(payload, object_pairs_hook=_unique, parse_float=_non_integer,
                            parse_constant=_non_integer)
        if type(record) is not dict:
            raise ValueError
        return record
    except Exception:
        raise LedgerEvidenceCodecError("INVALID_LEDGER_JSON") from None


def _account_record(value: PublicAccount | None) -> dict | None:
    if value is None:
        return None
    return {"pubkey": value.account.pubkey, "owner": value.account.owner,
            "data_base64": base64.b64encode(value.account.data).decode("ascii"),
            "lamports": value.lamports, "executable": value.executable, "rent_epoch": value.rent_epoch}


def _account(value: object) -> PublicAccount | None:
    if value is None:
        return None
    row = _object(value, ("pubkey", "owner", "data_base64", "lamports", "executable", "rent_epoch"))
    if type(row["data_base64"]) is not str:
        raise ValueError
    raw = base64.b64decode(row["data_base64"], validate=True)
    if base64.b64encode(raw).decode("ascii") != row["data_base64"]:
        raise ValueError
    return PublicAccount(RpcAccountV01(row["pubkey"], row["owner"], raw),
                         row["lamports"], row["executable"], row["rent_epoch"])


def _context(value: object) -> RpcContext:
    return RpcContext(**_object(value, ("slot", "api_version", "commitment")))


def _read_record(value: PublicAccountRead | None) -> dict | None:
    if value is None:
        return None
    return {"context": asdict(value.context), "requested_keys": value.requested_keys,
            "accounts": [_account_record(item) for item in value.accounts]}


def _read(value: object) -> PublicAccountRead | None:
    if value is None:
        return None
    row = _object(value, ("context", "requested_keys", "accounts"))
    return PublicAccountRead(_context(row["context"]), tuple(_array(row["requested_keys"])),
                             tuple(_account(item) for item in _array(row["accounts"])))


def _observation_record(observation: WalletObservation) -> dict:
    return {
        "request": asdict(observation.request), "profile": asdict(observation.profile),
        "started_at_utc": observation.started_at_utc, "observed_at_utc": observation.observed_at_utc,
        "observed_genesis": observation.observed_genesis, "observed_genesis_end": observation.observed_genesis_end,
        "initial_finalized_slot": observation.initial_finalized_slot,
        "finalized_upper_slot": observation.finalized_upper_slot,
        "anchor": None if observation.anchor is None else asdict(observation.anchor),
        "inventories": [{"wallet": item.wallet, "program": item.program, "context": asdict(item.context),
                         "accounts": [_account_record(account) for account in item.accounts]}
                        for item in observation.inventories],
        "explicit_read": _read_record(observation.explicit_read),
        "mint_reads": [_read_record(item) for item in observation.mint_reads],
        "failures": [asdict(item) for item in observation.failures], "schema": observation.schema,
    }


def wallet_observation_to_json(observation: WalletObservation) -> str:
    try:
        if type(observation) is not WalletObservation:
            raise ValueError
        result = canonical_json({"codec_version": CODEC_VERSION, "evidence_model_id": EVIDENCE_MODEL_ID,
                                 "evidence_digest": observation.content_digest,
                                 "observation": _observation_record(observation)})
        if len(result.encode("utf-8")) > MAX_PAYLOAD_BYTES:
            raise ValueError
        return result
    except Exception:
        raise LedgerEvidenceCodecError("INVALID_WALLET_EVIDENCE") from None


def wallet_observation_from_json(payload: str) -> WalletObservation:
    try:
        envelope = _object(strict_json_object(payload), ("codec_version", "evidence_model_id", "evidence_digest", "observation"))
        if envelope["codec_version"] != CODEC_VERSION or envelope["evidence_model_id"] != EVIDENCE_MODEL_ID:
            raise ValueError
        row = _object(envelope["observation"], _OBSERVATION_KEYS)
        request = _object(row["request"], ("wallet", "genesis_hash", "min_context_slot", "expected_accounts"))
        expected = tuple(ExpectedTokenAccount(**_object(item, ("pubkey", "mint", "program")))
                         for item in _array(request["expected_accounts"]))
        inventories = []
        for item in _array(row["inventories"]):
            item = _object(item, ("wallet", "program", "context", "accounts"))
            inventories.append(TokenInventoryRead(item["wallet"], item["program"], _context(item["context"]),
                                                  tuple(_account(account) for account in _array(item["accounts"]))))
        observation = WalletObservation(
            WalletEvidenceRequest(request["wallet"], request["genesis_hash"], request["min_context_slot"], expected),
            PublicRpcProfile(**_object(row["profile"], _PROFILE_KEYS)), row["started_at_utc"], row["observed_at_utc"],
            row["observed_genesis"], row["observed_genesis_end"], row["initial_finalized_slot"], row["finalized_upper_slot"],
            None if row["anchor"] is None else FinalizedBlockAnchor(**_object(row["anchor"], _ANCHOR_KEYS)),
            tuple(inventories), _read(row["explicit_read"]),
            tuple(_read(item) for item in _array(row["mint_reads"])),
            tuple(PublicReadFailure(**_object(item, ("operation", "code"))) for item in _array(row["failures"])), row["schema"],
        )
        if observation.content_digest != envelope["evidence_digest"] or wallet_observation_to_json(observation) != payload:
            raise ValueError
        return observation
    except Exception:
        # Never include corrupted JSON, raw diagnostic bodies or input values.
        raise LedgerEvidenceCodecError("INVALID_WALLET_EVIDENCE_PAYLOAD") from None
