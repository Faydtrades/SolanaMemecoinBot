"""Concrete pending-input/action/attempt facts, never an admission or send grant.

Only pure accepted candidate identity and public lease types are reused. No
Shadow economics, transaction construction, signer, transport or retry policy.
"""
from __future__ import annotations

import base64
import hashlib
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from decimal import Decimal

from solders.message import MessageV0, from_bytes_versioned, to_bytes_versioned
from solders.transaction import VersionedTransaction
from phase2.strategy_first_pullback_v0_2 import FirstPullbackStrategyV02
from phase5.shadow_domain_v0_1 import canonical_json, content_fingerprint
from phase5.shadow_unsigned_plan_simulation_v0_1 import BlockhashLeaseV01
from .ledger_domain_v0_1 import LedgerContractError, LedgerDomain, digest_value, ledger_utc
from .ledger_evidence_codec_v0_1 import strict_json_object
from .public_rpc_v0_1 import FinalizedBlockAnchor, TOKEN_PROGRAMS, public_key, u64
from .source_health_v0_1 import CursorWitness, SourceBinding

ACTION_VERSION = "live_ledger_pending_actions_v0.1"
ATTEMPT_VERSION = "live_ledger_attempt_storage_v0.1"
PENDING_ADMISSION = "PENDING_EXTERNAL_ADMISSION"
NON_ACCEPTANCE = frozenset(("DENIED_RETRYABLE", "REJECTED", "EXPIRED"))
TERMINAL_INBOX = frozenset(("REJECTED", "EXPIRED"))
LEDGER_FINALITY_STATES = frozenset(("CONFIRMED_PROVISIONAL", "FINALIZED_SUCCESS", "FINALIZED_FAILURE",
                                  "PROVEN_NON_LANDED", "CANCELLED_NEVER_SUBMITTED"))
EXTERNAL_STAGES = frozenset(("EXACT_SIMULATED", "AUTHORIZED", "SIGNED_DURABLE", "SEND_CLAIMED", "OBSERVING", "UNKNOWN"))
_NEXT = {"PREPARED": {"EXACT_SIMULATED", "UNKNOWN", "CANCELLED_UNSIGNED"},
         "EXACT_SIMULATED": {"AUTHORIZED", "UNKNOWN", "CANCELLED_UNSIGNED"},
         "AUTHORIZED": {"SIGNED_DURABLE", "UNKNOWN", "CANCELLED_UNSIGNED"},
         "SIGNED_DURABLE": {"SEND_CLAIMED", "UNKNOWN"},
         "SEND_CLAIMED": {"OBSERVING", "UNKNOWN", "SEND_CLAIMED"},
         "OBSERVING": {"OBSERVING", "UNKNOWN"}, "UNKNOWN": {"UNKNOWN"}, "CANCELLED_UNSIGNED": set()}


def public_reference(value: object) -> str:
    if type(value) is not str or re.fullmatch(r"[A-Za-z0-9_:.-]{1,256}", value) is None:
        raise LedgerContractError("LEDGER_PUBLIC_REFERENCE_INVALID")
    return value


def utc_microseconds(value: str) -> int:
    dt = datetime.fromisoformat(ledger_utc(value))
    delta = dt - datetime(1970, 1, 1, tzinfo=timezone.utc)
    return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds


@dataclass(frozen=True, slots=True)
class CandidateInboxInput:
    candidate_signal_id: str
    candidate_run_id: str
    run_started_at_us: int
    mint: str
    strategy_version: str
    parameter_set_id: str
    parameter_fingerprint: str
    model_fingerprint: str
    strategy_evaluation_id: str
    generated_at_us: int
    source_event_key: str
    signal_key: str
    source_cursor: int
    signal_ingest_seq: int
    producer_run_id: str
    producer_lineage_id: str
    source_binding: SourceBinding
    source_record_digest: str
    winner_candidate_id: str
    winner_role: str
    winner_binding_digest: str
    reference_price_text: str
    reference_price_kind: str
    price_identity: str
    rule_reference_price_numerator_raw: int
    rule_reference_price_denominator_raw: int
    reference_at_us: int
    admission_status: str = field(init=False, default="RECEIVED_UNADMITTED")
    version: str = field(init=False, default=ACTION_VERSION)

    def __post_init__(self) -> None:
        try:
            public_key(self.mint)
            for value in (self.candidate_signal_id, self.candidate_run_id, self.strategy_version,
                    self.parameter_set_id, self.strategy_evaluation_id, self.source_event_key, self.signal_key,
                    self.producer_run_id, self.producer_lineage_id, self.winner_candidate_id, self.winner_role,
                    self.reference_price_kind, self.price_identity):
                public_reference(value)
            for value in (self.parameter_fingerprint, self.model_fingerprint, self.source_record_digest,
                          self.winner_binding_digest):
                digest_value(value)
            for value in (self.run_started_at_us, self.generated_at_us, self.source_cursor,
                          self.signal_ingest_seq, self.reference_at_us):
                u64(value)
            if (type(self.source_binding) is not SourceBinding or self.source_cursor == 0 or self.signal_ingest_seq == 0
                    or not self.run_started_at_us <= self.reference_at_us <= self.generated_at_us):
                raise ValueError
            # Reuse the accepted canonical formulas, never re-run strategy or
            # select a winner. Original run start, event and parameter IDs stay fixed.
            stable = FirstPullbackStrategyV02._stable_id
            if self.candidate_run_id != stable("fp1run", self.mint, self.run_started_at_us,
                                               self.strategy_version, self.parameter_set_id):
                raise ValueError
            if self.candidate_signal_id != stable("fp1sig", self.candidate_run_id, self.source_event_key,
                                                  self.parameter_set_id, self.strategy_version):
                raise ValueError
            if type(self.reference_price_text) is not str or not 1 <= len(self.reference_price_text) <= 128:
                raise ValueError
            price = Decimal(self.reference_price_text)
            if not price.is_finite() or price <= 0 or self.reference_price_text.strip() != self.reference_price_text:
                raise ValueError
            for value in (self.rule_reference_price_numerator_raw, self.rule_reference_price_denominator_raw):
                if type(value) is not int or value <= 0:
                    raise ValueError
        except Exception:
            raise LedgerContractError("LEDGER_CANONICAL_CANDIDATE_INVALID") from None

    @property
    def canonical_candidate_key(self) -> str:
        return content_fingerprint({"candidate_run_id": self.candidate_run_id, "candidate_signal_id": self.candidate_signal_id})

    def trade_root(self, domain: LedgerDomain) -> str:
        return content_fingerprint({"identity": "LIVE_TRADE_ROOT", "economic_domain_id": domain.economic_domain_id,
                                    "canonical_candidate_key": self.canonical_candidate_key})

    def to_record(self) -> dict:
        return asdict(self)

    @property
    def content_digest(self) -> str:
        return content_fingerprint(self.to_record())


def position_identity(domain: LedgerDomain, root_id: str, mint: str) -> str:
    digest_value(root_id)
    public_key(mint)
    return content_fingerprint({"identity": "LIVE_POSITION", "economic_domain_id": domain.economic_domain_id,
                                "trade_root": root_id, "mint": mint})


@dataclass(frozen=True, slots=True)
class PendingAction:
    root_id: str
    candidate_digest: str
    side: str
    mint: str
    token_program: str
    position_id: str
    input_units: int
    external_decision_ref: str
    external_decision_digest: str
    policy_ref: str
    policy_digest: str
    selected_exit_track: str
    obligation_id: str | None = None
    ordinal: int = 1
    claimed_entry_deadline_us: int | None = None
    deadline_binding_digest: str | None = None
    admission_status: str = field(init=False, default=PENDING_ADMISSION)
    has_real_authority_grant: bool = field(init=False, default=False)
    version: str = field(init=False, default=ACTION_VERSION)

    def __post_init__(self) -> None:
        try:
            for value in (self.root_id, self.candidate_digest, self.position_id, self.external_decision_digest, self.policy_digest):
                digest_value(value)
            public_key(self.mint)
            for value in (self.external_decision_ref, self.policy_ref, self.selected_exit_track):
                public_reference(value)
            u64(self.input_units)
            u64(self.ordinal)
            if self.input_units == 0 or self.ordinal == 0 or self.token_program not in TOKEN_PROGRAMS:
                raise ValueError
            if self.side == "BUY":
                if self.obligation_id is not None or self.ordinal != 1:
                    raise ValueError
                u64(self.claimed_entry_deadline_us)
                digest_value(self.deadline_binding_digest)
            elif self.side == "SELL":
                public_reference(self.obligation_id)
                if self.claimed_entry_deadline_us is not None or self.deadline_binding_digest is not None:
                    raise ValueError
            else:
                raise ValueError
        except Exception:
            raise LedgerContractError("LEDGER_PENDING_ACTION_INVALID") from None

    @property
    def action_id(self) -> str:
        identity = {"identity": "LIVE_ACTION", "root_id": self.root_id, "side": self.side}
        if self.side == "SELL":
            identity.update(position_id=self.position_id, mint=self.mint, obligation_id=self.obligation_id,
                            input_units=self.input_units, ordinal=self.ordinal)
        return content_fingerprint(identity)

    def to_record(self) -> dict:
        return asdict(self)

    @property
    def content_digest(self) -> str:
        return content_fingerprint(self.to_record())


def decode_message(message_hex: str):
    try:
        if type(message_hex) is not str or len(message_hex) > 2464:
            raise ValueError
        raw = bytes.fromhex(message_hex)
        message = from_bytes_versioned(raw)
        if raw.hex() != message_hex or to_bytes_versioned(message) != raw or message.header.num_required_signatures < 1:
            raise ValueError
        if type(message) is MessageV0:
            message.sanitize()
        else:
            # Legacy Message exposes no sanitize method in the accepted runtime.
            # Validate structural indexes/header directly without constructing a transaction.
            count, required = len(message.account_keys), message.header.num_required_signatures
            if (not 1 <= required <= count <= 256 or message.header.num_readonly_signed_accounts >= required
                    or message.header.num_readonly_unsigned_accounts > count-required
                    or len(set(message.account_keys)) != count
                    or any(instruction.program_id_index >= count or any(index >= count for index in instruction.accounts)
                           for instruction in message.instructions)):
                raise ValueError
        return message
    except Exception:
        raise LedgerContractError("LEDGER_EXACT_PUBLIC_MESSAGE_INVALID") from None


@dataclass(frozen=True, slots=True)
class AttemptPreparation:
    action_id: str
    action_content_digest: str
    ordinal: int
    message_hex: str
    plan_digest: str
    message_policy_digest: str
    lease: BlockhashLeaseV01
    finalized_lower_anchor: FinalizedBlockAnchor
    prepared_at_utc: str
    external_preparation_ref: str
    external_preparation_digest: str
    validity_profile: str = "RECENT_BLOCKHASH"
    admission_status: str = field(init=False, default=PENDING_ADMISSION)
    has_real_authority_grant: bool = field(init=False, default=False)
    version: str = field(init=False, default=ATTEMPT_VERSION)

    def __post_init__(self) -> None:
        try:
            for value in (self.action_id, self.action_content_digest, self.plan_digest,
                          self.message_policy_digest, self.external_preparation_digest):
                digest_value(value)
            u64(self.ordinal)
            public_reference(self.external_preparation_ref)
            object.__setattr__(self, "prepared_at_utc", ledger_utc(self.prepared_at_utc))
            if (self.ordinal < 1 or type(self.lease) is not BlockhashLeaseV01 or type(self.finalized_lower_anchor) is not FinalizedBlockAnchor
                    or self.validity_profile not in ("RECENT_BLOCKHASH", "DURABLE_NONCE", "UNKNOWN")):
                raise ValueError
            message = decode_message(self.message_hex)
            lower = self.finalized_lower_anchor
            if (str(message.recent_blockhash) != self.lease.blockhash or lower.slot > self.lease.context_slot
                    or lower.block_height > self.lease.last_valid_block_height
                    or lower.block_time * 1_000_000 > self.lease.observed_at_us
                    or self.lease.observed_at_us > utc_microseconds(self.prepared_at_utc)):
                raise ValueError
        except Exception:
            raise LedgerContractError("LEDGER_ATTEMPT_PREPARATION_INVALID") from None

    @property
    def message_sha256(self) -> str:
        return hashlib.sha256(bytes.fromhex(self.message_hex)).hexdigest()

    @property
    def message_id(self) -> str:
        return content_fingerprint({"identity": "LIVE_EXACT_MESSAGE", "action_id": self.action_id,
            "plan_digest": self.plan_digest, "message_sha256": self.message_sha256,
            "lease_fingerprint": self.lease.fingerprint, "message_policy_digest": self.message_policy_digest})

    @property
    def attempt_id(self) -> str:
        return content_fingerprint({"identity": "LIVE_ATTEMPT", "action_id": self.action_id,
                                    "ordinal": self.ordinal, "message_id": self.message_id})

    def to_record(self) -> dict:
        return asdict(self)

    @property
    def content_digest(self) -> str:
        return content_fingerprint(self.to_record())


@dataclass(frozen=True, slots=True)
class AttemptStageInput:
    attempt_id: str
    target_stage: str
    action_content_digest: str
    message_sha256: str
    lease_fingerprint: str
    message_policy_digest: str
    recorded_at_utc: str
    external_reference: str
    external_record_digest: str
    signed_wire_base64: str | None = None
    admission_status: str = field(init=False, default=PENDING_ADMISSION)
    has_real_authority_grant: bool = field(init=False, default=False)
    version: str = field(init=False, default=ATTEMPT_VERSION)

    def __post_init__(self) -> None:
        try:
            for value in (self.attempt_id, self.action_content_digest, self.message_sha256,
                          self.lease_fingerprint, self.message_policy_digest, self.external_record_digest):
                digest_value(value)
            public_reference(self.external_reference)
            object.__setattr__(self, "recorded_at_utc", ledger_utc(self.recorded_at_utc))
            if self.target_stage not in EXTERNAL_STAGES or ((self.target_stage == "SIGNED_DURABLE") != (self.signed_wire_base64 is not None)):
                raise ValueError
            if self.signed_wire_base64 is not None:
                self.signed_public_transaction()
        except Exception:
            raise LedgerContractError("LEDGER_EXTERNAL_ATTEMPT_STAGE_INVALID_OR_FINALITY_OWNED") from None

    def signed_public_transaction(self):
        """Validate supplied public byte/lineage shape, not approval or signing.

        Cryptographic/chain adjudication is not asserted by this external claim.
        """
        try:
            if type(self.signed_wire_base64) is not str or len(self.signed_wire_base64) > 1644:
                raise ValueError
            wire = base64.b64decode(self.signed_wire_base64, validate=True)
            transaction = VersionedTransaction.from_bytes(wire)
            transaction.sanitize()
            if (bytes(transaction) != wire or len(wire) > 1232
                    or base64.b64encode(wire).decode("ascii") != self.signed_wire_base64
                    or not transaction.signatures or any(bytes(sig) == bytes(64) for sig in transaction.signatures)
                    or hashlib.sha256(to_bytes_versioned(transaction.message)).hexdigest() != self.message_sha256):
                raise ValueError
            return transaction
        except Exception:
            raise LedgerContractError("LEDGER_EXTERNAL_SIGNED_WIRE_INVALID") from None

    def to_record(self) -> dict:
        return asdict(self)


def validate_stage_transition(prepared: AttemptPreparation, previous: str, stage: AttemptStageInput, last_at: str) -> None:
    if (stage.attempt_id != prepared.attempt_id or stage.action_content_digest != prepared.action_content_digest
            or stage.message_sha256 != prepared.message_sha256 or stage.lease_fingerprint != prepared.lease.fingerprint
            or stage.message_policy_digest != prepared.message_policy_digest
            or stage.recorded_at_utc < last_at or stage.target_stage not in _NEXT[previous]):
        raise LedgerContractError("LEDGER_ATTEMPT_TRANSITION_OR_EXACT_BINDING_CONFLICT")
    if stage.signed_wire_base64 is not None:
        if to_bytes_versioned(stage.signed_public_transaction().message).hex() != prepared.message_hex:
            raise LedgerContractError("LEDGER_SIGNED_MESSAGE_CHANGED")


@dataclass(frozen=True, slots=True)
class StoredAttempt:
    preparation: AttemptPreparation
    recorded_stage: str
    revision: int
    prepared_generation: int
    last_recorded_at_utc: str
    primary_signature: str | None
    signed_wire_digest: str | None
    lane_held: bool
    chain_finality: str = "UNOBSERVED"
    chain_quarantined: bool = False
    admission_status: str = field(init=False, default=PENDING_ADMISSION)
    has_real_authority_grant: bool = field(init=False, default=False)
    may_send: bool = field(init=False, default=False)


def candidate_from_json(payload: str) -> CandidateInboxInput:
    try:
        row = strict_json_object(payload)
        original = canonical_json(row)
        source = row.pop("source_binding")
        source["anchors"] = tuple(CursorWitness(**item) for item in source["anchors"])
        row.pop("admission_status")
        row.pop("version")
        value = CandidateInboxInput(**row, source_binding=SourceBinding(**source))
        if canonical_json(value.to_record()) != original or original != payload:
            raise ValueError
        return value
    except Exception:
        raise LedgerContractError("LEDGER_CANDIDATE_CONTENT_INVALID") from None


def action_from_json(payload: str) -> PendingAction:
    try:
        row = strict_json_object(payload)
        for key in ("admission_status", "has_real_authority_grant", "version"):
            row.pop(key)
        value = PendingAction(**row)
        if canonical_json(value.to_record()) != payload:
            raise ValueError
        return value
    except Exception:
        raise LedgerContractError("LEDGER_ACTION_CONTENT_INVALID") from None


def preparation_from_json(payload: str) -> AttemptPreparation:
    try:
        row = strict_json_object(payload)
        lease = row.pop("lease")
        for key in ("schema_version", "fingerprint", "lease_id"):
            lease.pop(key)
        row["lease"] = BlockhashLeaseV01(**lease)
        row["finalized_lower_anchor"] = FinalizedBlockAnchor(**row["finalized_lower_anchor"])
        for key in ("admission_status", "has_real_authority_grant", "version"):
            row.pop(key)
        value = AttemptPreparation(**row)
        if canonical_json(value.to_record()) != payload:
            raise ValueError
        return value
    except Exception:
        raise LedgerContractError("LEDGER_PREPARATION_CONTENT_INVALID") from None


def stage_from_record(record: dict) -> AttemptStageInput:
    try:
        row = dict(record)
        for key in ("admission_status", "has_real_authority_grant", "version"):
            row.pop(key)
        value = AttemptStageInput(**row)
        if canonical_json(value.to_record()) != canonical_json(record):
            raise ValueError
        return value
    except Exception:
        raise LedgerContractError("LEDGER_STAGE_CONTENT_INVALID") from None
