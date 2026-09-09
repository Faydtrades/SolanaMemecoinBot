"""Finite original Transaction/Coverage Evidence codec; no transport or registry."""
from __future__ import annotations

import base64
import json
from dataclasses import asdict

from phase5.shadow_domain_v0_1 import canonical_json
from .ledger_evidence_codec_v0_1 import (
    LedgerEvidenceCodecError, _PROFILE_KEYS, _ANCHOR_KEYS, _unique, _non_integer,
)
from .public_rpc_v0_1 import MODEL_ID, FinalizedBlockAnchor, PublicRpcProfile
from .transaction_evidence_v0_1 import (
    TransactionObservation, TransactionRequest, TransactionReadFailure, TransactionOutcome,
    SignatureStatusFact, ExactTransactionFact, InstructionFact, LookupFact, TokenBalanceFact, CanonicalSignatureBlock,
)
from .transaction_coverage_v0_1 import CanonicalCoverageObservation, CoverageRequest, CoverageLimits

CODEC_VERSION = "live_ledger_chain_evidence_codec_v0.1"
# The accepted RPC profile allows 128 MiB total public responses. Coverage
# retains signature vectors; its journal budget must exceed Wallet's 16 MiB.
MAX_CHAIN_PAYLOAD_BYTES = 160 * 1024 * 1024
_TX_KEYS = ("primary_signature", "signatures", "slot", "block_time", "version", "wire_bytes", "wire_sha256",
    "message_bytes", "message_sha256", "recent_blockhash", "header", "static_accounts", "lookups", "loaded_writable",
    "loaded_readonly", "outer_instructions", "inner_instructions", "inner_recorded_outer_indexes", "pre_lamports",
    "post_lamports", "pre_tokens", "post_tokens", "fee_lamports", "outcome", "provider_fingerprint", "commitment")
_BLOCK_KEYS = ("slot", "blockhash", "previous_blockhash", "parent_slot", "block_height", "block_time", "signatures",
               "provider_fingerprint", "commitment")


def _object(value, keys):
    if type(value) is not dict or set(value) != set(keys):
        raise ValueError
    return dict(value)


def _array(value):
    if type(value) is not list:
        raise ValueError
    return value


def _bytes(value):
    if type(value) is not str:
        raise ValueError
    raw = base64.b64decode(value, validate=True)
    if base64.b64encode(raw).decode("ascii") != value:
        raise ValueError
    return raw


def _outcome(value):
    return None if value is None else TransactionOutcome(**_object(value, ("succeeded", "error_fingerprint")))


def _anchor(value):
    return None if value is None else FinalizedBlockAnchor(**_object(value, _ANCHOR_KEYS))


def _block(value):
    if value is None:
        return None
    row = _object(value, _BLOCK_KEYS)
    row["signatures"] = tuple(_array(row["signatures"]))
    return CanonicalSignatureBlock(**row)


def _instruction(value):
    row = _object(value, ("outer_index", "inner_index", "program_id_index", "account_indexes", "data", "stack_height"))
    row["account_indexes"] = tuple(_array(row["account_indexes"]))
    row["data"] = _bytes(row["data"])
    return InstructionFact(**row)


def _transaction(value):
    if value is None:
        return None
    row = _object(value, _TX_KEYS)
    for key in ("wire_bytes", "message_bytes"):
        row[key] = _bytes(row[key])
    for key in ("signatures", "header", "static_accounts", "loaded_writable", "loaded_readonly",
                "inner_recorded_outer_indexes", "pre_lamports", "post_lamports"):
        row[key] = tuple(_array(row[key]))
    lookups = []
    for value in _array(row["lookups"]):
        item = _object(value, ("table", "writable_indexes", "readonly_indexes"))
        lookups.append(LookupFact(item["table"], tuple(_array(item["writable_indexes"])), tuple(_array(item["readonly_indexes"]))))
    row["lookups"] = tuple(lookups)
    for key in ("outer_instructions", "inner_instructions"):
        row[key] = tuple(_instruction(item) for item in _array(row[key]))
    for key in ("pre_tokens", "post_tokens"):
        row[key] = tuple(TokenBalanceFact(**_object(item, ("account_index", "mint", "owner", "program", "amount", "decimals")))
                         for item in _array(row[key]))
    row["outcome"] = _outcome(row["outcome"])
    return ExactTransactionFact(**row)


def chain_observation_to_json(observation: TransactionObservation | CanonicalCoverageObservation) -> str:
    try:
        if type(observation) not in (TransactionObservation, CanonicalCoverageObservation):
            raise ValueError
        row = asdict(observation)
        if type(observation) is TransactionObservation and row["transaction"] is not None:
            tx = row["transaction"]
            for key in ("wire_bytes", "message_bytes"):
                tx[key] = base64.b64encode(tx[key]).decode("ascii")
            for key in ("outer_instructions", "inner_instructions"):
                for instruction in tx[key]:
                    instruction["data"] = base64.b64encode(instruction["data"]).decode("ascii")
        result = canonical_json({"codec_version": CODEC_VERSION, "evidence_model_id": MODEL_ID,
            "kind": "TRANSACTION" if type(observation) is TransactionObservation else "COVERAGE",
            "evidence_digest": observation.content_digest, "observation": row})
        if len(result.encode("utf-8")) > MAX_CHAIN_PAYLOAD_BYTES:
            raise ValueError
        return result
    except Exception:
        raise LedgerEvidenceCodecError("INVALID_CHAIN_EVIDENCE") from None


def chain_observation_from_json(payload: str) -> TransactionObservation | CanonicalCoverageObservation:
    try:
        if type(payload) is not str or len(payload.encode("utf-8")) > MAX_CHAIN_PAYLOAD_BYTES:
            raise ValueError
        decoded = json.loads(payload, object_pairs_hook=_unique, parse_float=_non_integer, parse_constant=_non_integer)
        envelope = _object(decoded, ("codec_version", "evidence_model_id", "kind", "evidence_digest", "observation"))
        if envelope["codec_version"] != CODEC_VERSION or envelope["evidence_model_id"] != MODEL_ID:
            raise ValueError
        common = ("request", "profile", "started_at_utc", "observed_at_utc", "genesis_start", "genesis_end", "root", "failures")
        if envelope["kind"] == "TRANSACTION":
            row = _object(envelope["observation"], common + ("status", "transaction", "membership_block", "schema"))
            row["request"] = TransactionRequest(**_object(row["request"], ("genesis_hash", "signature", "min_finalized_root_slot")))
            status = row["status"]
            if status is not None:
                status = _object(status, ("signature", "context_slot", "api_version", "slot", "confirmation_status", "confirmations",
                                          "outcome", "search_transaction_history", "context_semantics"))
                status["outcome"] = _outcome(status["outcome"])
                status = SignatureStatusFact(**status)
            row["status"] = status
            row["transaction"] = _transaction(row["transaction"])
            row["membership_block"] = _block(row["membership_block"])
        elif envelope["kind"] == "COVERAGE":
            row = _object(envelope["observation"], common + ("limits", "initial_finalized_root_slot", "chosen_upper_slot", "blocks_descending", "scope"))
            request = _object(row["request"], ("genesis_hash", "signature", "recent_blockhash", "last_valid_block_height", "original_lower_anchor", "upper_slot"))
            request["original_lower_anchor"] = _anchor(request["original_lower_anchor"])
            row["request"] = CoverageRequest(**request)
            row["limits"] = CoverageLimits(**_object(row["limits"], ("max_blocks", "max_slot_span")))
            row["blocks_descending"] = tuple(_block(item) for item in _array(row["blocks_descending"]))
        else:
            raise ValueError
        row["profile"] = PublicRpcProfile(**_object(row["profile"], _PROFILE_KEYS))
        row["root"] = _anchor(row["root"])
        row["failures"] = tuple(TransactionReadFailure(**_object(item, ("operation", "code"))) for item in _array(row["failures"]))
        value = TransactionObservation(**row) if envelope["kind"] == "TRANSACTION" else CanonicalCoverageObservation(**row)
        if value.content_digest != envelope["evidence_digest"] or chain_observation_to_json(value) != payload:
            raise ValueError
        return value
    except Exception:
        raise LedgerEvidenceCodecError("INVALID_CHAIN_EVIDENCE_PAYLOAD") from None
