"""Q2 autonomous local signing of one current, exact LIVE message.

No key loading, key serialization, arbitrary-message API, persistence or send.
The host supplies an isolated in-memory solders key and a current trusted clock
port. Qualification supplies ephemeral synthetic keys only. The local key call
holds the existing Ledger writer fence and a no-write source-journal lock; this
is a finite last-call critical section, not a cross-database commit protocol.
"""
from __future__ import annotations

import base64
import hashlib
from dataclasses import dataclass, field, replace

from solders.keypair import Keypair
from solders.message import Message
from solders.signature import Signature
from solders.transaction import VersionedTransaction

from phase5.shadow_domain_v0_1 import content_fingerprint
from .authority_controls_v0_1 import TrustedClockSample, clock_reasons
from .authority_message_control_v0_1 import FreshStageConsumption, MessageStageReceipt
from .authority_message_evidence_v0_1 import validate_message_evidence
from .evidence_store_v0_1 import SourceEvidenceStore
from .execution_message_v0_1 import ExactMessageProduction
from .ledger_actions_v0_1 import AttemptPreparation, utc_microseconds
from .ledger_domain_v0_1 import digest_value
from .ledger_repository_v0_1 import LedgerRepository
from .source_health_v0_1 import source_consumer_evidence

VERSION = "live_execution_signer_v0.1"


class ExecutionSigningError(ValueError):
    """Finite public error; external/key/clock exception bodies are excluded."""


def _require(condition, reason):
    if not condition:
        raise ExecutionSigningError(reason)


def _release_guard(connection):
    try:
        if connection is not None and connection.in_transaction:
            connection.execute("ROLLBACK")
    except Exception:
        raise ExecutionSigningError("EXECUTION_LAST_CALL_GUARD_RELEASE_UNRESOLVED") from None


def _exact_preparation(preparation, receipt, production_digest):
    _require(type(preparation) is AttemptPreparation and type(receipt) is MessageStageReceipt
        and receipt.consumed and receipt.original.request.stage == "SIGN", "EXECUTION_EXACT_SIGN_RECEIPT_REQUIRED")
    original = receipt.original.validation
    action, simulation = original.context.action, original.evidence.simulation
    request = receipt.original.request
    _require(original.context.domain.mode == "LIVE" and request.primary_signature is None
        and request.rebroadcast_ordinal == 0 and request.attempt_id == preparation.attempt_id
        and request.action_id == action.action_id == preparation.action_id
        and preparation.action_content_digest == action.content_digest
        and len(simulation.envelopes) == 1 and preparation.message_hex == simulation.envelopes[0].message_hex
        and preparation.message_sha256 == simulation.envelopes[0].message_sha256
        and preparation.plan_digest == simulation.envelopes[0].plan_fingerprint
        and preparation.lease == simulation.leases[0]
        and preparation.message_policy_digest == original.profile.content_digest
        and preparation.external_preparation_ref == "LIVE_EXECUTION_EXACT_PREPARATION"
        and preparation.external_preparation_digest == production_digest,
        "EXECUTION_PREPARATION_AUTHORITY_MESSAGE_BINDING_CONFLICT")
    message = Message.from_bytes(bytes.fromhex(preparation.message_hex))
    _require(message.header.num_required_signatures == 1 and message.header.num_readonly_signed_accounts == 0
        and str(message.account_keys[0]) == original.context.domain.wallet,
        "EXECUTION_EXACT_WALLET_MESSAGE_REQUIRED")
    return message


@dataclass(frozen=True, slots=True)
class VerifiedSignedEnvelope:
    """Immutable public output for Q3. Verification is never send permission."""
    preparation: AttemptPreparation
    production_digest: str
    authority_receipt: MessageStageReceipt
    consumed_common_digest: str
    signing_clock: TrustedClockSample
    primary_signature: str
    signed_wire_base64: str
    version: str = field(init=False, default=VERSION)
    may_send: bool = field(init=False, default=False)

    def __post_init__(self):
        try:
            digest_value(self.production_digest)
            digest_value(self.consumed_common_digest)
            message = _exact_preparation(self.preparation, self.authority_receipt, self.production_digest)
            _require(type(self.signing_clock) is TrustedClockSample
                and self.signing_clock.previous_sample_digest == self.authority_receipt.original.validation.clock.content_digest,
                "EXECUTION_SIGNING_CLOCK_CONTINUATION_REQUIRED")
            wire = base64.b64decode(self.signed_wire_base64, validate=True)
            signature = Signature.from_string(self.primary_signature)
            transaction = VersionedTransaction.from_bytes(wire)
            transaction.sanitize()
            _require(len(wire) <= 1232 and wire == b"\x01"+bytes(signature)+bytes(message)
                and bytes(transaction) == wire and tuple(transaction.signatures) == (signature,)
                and base64.b64encode(wire).decode("ascii") == self.signed_wire_base64
                and signature.verify(message.account_keys[0], bytes(message))
                and all(transaction.verify_with_results()), "EXECUTION_SIGNATURE_EXACT_WALLET_MESSAGE_VERIFICATION_FAILED")
        except ExecutionSigningError:
            raise
        except Exception:
            raise ExecutionSigningError("EXECUTION_PUBLIC_SIGNED_ENVELOPE_INVALID") from None

    @property
    def content_digest(self):
        return content_fingerprint({"version": self.version, "preparation_digest": self.preparation.content_digest,
            "production_digest": self.production_digest, "authority_receipt_digest": self.authority_receipt.content_digest,
            "consumed_common_digest": self.consumed_common_digest, "signing_clock_digest": self.signing_clock.content_digest,
            "primary_signature": self.primary_signature, "signed_wire_base64": self.signed_wire_base64, "may_send": False})

    @property
    def signed_wire_digest(self):
        return hashlib.sha256(base64.b64decode(self.signed_wire_base64)).hexdigest()

    @property
    def wallet(self):
        return self.authority_receipt.original.validation.context.domain.wallet


def _fresh_clock(repository, receipt, sample):
    original = receipt.original.validation
    previous, policy = original.clock, original.context.original_policy
    _require(type(sample) is TrustedClockSample and sample is not previous
        and sample.content_digest != previous.content_digest
        and sample.epoch_id == previous.epoch_id and sample.monotonic_ns > previous.monotonic_ns
        and not clock_reasons(replace(repository._authority, policy=policy), sample),
        "EXECUTION_CURRENT_CLOCK_CONTINUATION_UNPROVEN")
    _require(validate_message_evidence(replace(original, clock=sample)).disposition == "SUPPORTED_CONTEXT_ONLY",
        "EXECUTION_MESSAGE_EVIDENCE_STALE_AT_KEY_CALL")
    if original.context.action.side == "BUY":
        # The durable consumption established the selected grant/control state.
        # Only temporal predicates can change while both journal locks are held.
        status = repository.authority_grant_status(original.context.acceptance.eligibility.decision.grant_id)
        issuance = status["issuance"]
        _require(issuance is not None and policy.entry_valid_from_utc <= sample.utc_lower_utc
            and sample.utc_upper_utc <= policy.entry_valid_through_utc
            and issuance.entry_valid_from_utc <= sample.utc_lower_utc
            and sample.utc_upper_utc <= issuance.entry_valid_through_utc
            and utc_microseconds(sample.utc_upper_utc) <= original.context.acceptance.eligibility.decision.binding.deadline_us,
            "EXECUTION_ENTRY_INTERVAL_EXPIRED_AT_KEY_CALL")
        source = receipt.original.source.source
        port = source_consumer_evidence(source, expected_source_identity=original.context.candidate.source_binding.source_identity,
            required_cut_utc=source.snapshot.requested_cut_utc, now_utc=sample.utc_upper_utc)
        _require(port.observed_prefix_supported and not port.reasons, "EXECUTION_SOURCE_STALE_AT_KEY_CALL")


class AutonomousLocalSigner:
    """A key facility with one public operation: sign the current exact approval.

    The private key remains inside this object; no representation or transfer
    contract includes it. The process/composition root is trusted, not a Python
    sandbox against code with arbitrary reflection or monkeypatch access.
    """
    __slots__ = ("__key",)

    def __init__(self, key):
        _require(type(key) is Keypair, "EXECUTION_ISOLATED_LOCAL_KEY_REQUIRED")
        self.__key = key

    def __repr__(self):
        return "AutonomousLocalSigner(<isolated local key>)"

    def __reduce_ex__(self, protocol):
        raise TypeError("isolated signer cannot be serialized")

    def sign_exact(self, repository, production, delivery, *, source_store, clock):
        """Spend fresh A4b SIGN once, guard continuation, then call local crypto.

        Clock is a fresh, bounded local trusted provider call, never the old
        sampled value or a network fetch. No caller callback follows its final
        recheck. A failed/lost call leaves the durable SIGN consumed and lane
        held, even if no public signature was returned. Q3 owns persistence.
        """
        ledger_owned = source_owned = False
        try:
            _require(type(delivery) is FreshStageConsumption and delivery.stage == "SIGN",
                "EXECUTION_FRESH_SIGN_DELIVERY_REQUIRED")
            delivery._claim_execution()
            _require(type(repository) is LedgerRepository and type(production) is ExactMessageProduction
                and callable(clock), "EXECUTION_EXACT_LOCAL_SIGN_INPUT_REQUIRED")
            _require(delivery._uses_source_store(source_store), "EXECUTION_ORIGINAL_SOURCE_STORE_REQUIRED")
            receipt = delivery.receipt
            original = production.original
            approved = receipt.original.validation
            _require(original.evidence == approved.evidence and original.profile == approved.profile
                and original.context.action == approved.context.action
                and original.context.domain == approved.context.domain,
                "EXECUTION_ORIGINAL_PRODUCTION_APPROVAL_CONFLICT")
            _require(not repository._conn.in_transaction, "EXECUTION_OWNED_LAST_CALL_TRANSACTION_REQUIRED")
            fence = repository.write_fence()
            _require((fence.generation, fence.revision, fence.last_receipt_digest) ==
                (delivery.writer_generation, receipt.sequence, delivery.consumed_common_digest),
                "EXECUTION_CONSUMED_LEDGER_CONTINUATION_CHANGED")
            ledger_owned = True
            current = repository._begin_economic_write(fence)
            repository._new_revision(fence, current)
            _require(repository.authority_message_receipt(receipt.original.request.command_id) == receipt,
                "EXECUTION_ORIGINAL_DURABLE_SIGN_REQUIRED")
            attempt = repository.attempt(receipt.original.request.attempt_id)
            _require(attempt is not None and attempt.revision == receipt.original.request.expected_attempt_revision
                and attempt.prepared_generation == delivery.writer_generation and attempt.lane_held
                and repository._lane()[0] == attempt.preparation.attempt_id
                and attempt.recorded_stage in ("PREPARED", "EXACT_SIMULATED", "AUTHORIZED")
                and attempt.primary_signature is None and not attempt.chain_quarantined,
                "EXECUTION_INTACT_UNSIGNED_ATTEMPT_REQUIRED")
            preparation = attempt.preparation
            message = _exact_preparation(preparation, receipt, production.content_digest)
            raw = bytes(message)
            _require(self.__key.pubkey() == message.account_keys[0], "EXECUTION_LOCAL_SIGNER_WALLET_MISMATCH")
            expected_source = None
            if approved.context.action.side == "BUY":
                _require(type(source_store) is SourceEvidenceStore and not source_store._conn.in_transaction,
                    "EXECUTION_CURRENT_SOURCE_STORE_REQUIRED")
                source_owned = True
                source_store._conn.execute("BEGIN IMMEDIATE")
                source = receipt.original.source
                expected_source = (source.source_sequence, source.source)
                _require(source_store.binding == source.source.binding and source_store.profile == source.source.profile
                    and source_store.latest_record() == expected_source, "EXECUTION_CONSUMED_SOURCE_CONTINUATION_CHANGED")
            else:
                _require(source_store is None and receipt.original.source is None, "EXECUTION_SELL_ENTRY_SOURCE_FORBIDDEN")
            # All possible provider callbacks run before this final recheck.
            try:
                sample = clock()
            except Exception:
                raise ExecutionSigningError("EXECUTION_CURRENT_CLOCK_UNAVAILABLE") from None
            _require(repository._conn.in_transaction and repository.write_fence() == fence
                and (not source_owned or source_store._conn.in_transaction
                     and source_store.latest_record() == expected_source), "EXECUTION_LAST_CALL_GUARD_INTERRUPTED")
            with repository._trusted_read():
                _fresh_clock(repository, receipt, sample)
            signature = self.__key.sign_message(raw)
            # The primitive returns public signature bytes only. Verify against
            # the exact original message and expected wallet before transferring.
            return VerifiedSignedEnvelope(preparation, production.content_digest, receipt,
                delivery.consumed_common_digest, sample, str(signature),
                base64.b64encode(b"\x01"+bytes(signature)+raw).decode("ascii"))
        except ExecutionSigningError:
            raise
        except Exception:
            raise ExecutionSigningError("EXECUTION_LOCAL_SIGNING_UNRESOLVED") from None
        finally:
            try:
                if source_owned:
                    _release_guard(source_store._conn)
            finally:
                if ledger_owned:
                    _release_guard(repository._conn)
