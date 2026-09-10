"""Q3 exact durable envelope, current one-call claim and sanitized observation.

Ledger's existing attempt journal owns storage, lane and finality. Public send
observations grant nothing and never settle funds. No scheduler, automatic
retry, replacement decision, key loading or arbitrary mutating RPC is exposed.
"""
from __future__ import annotations

import time
from dataclasses import asdict, dataclass, field
from threading import Lock

import httpx

from phase5.shadow_domain_v0_1 import canonical_json, content_fingerprint
from .authority_controls_v0_1 import TrustedClockSample, ClockReconciliation
from .authority_message_control_v0_1 import FreshStageConsumption
from .evidence_store_v0_1 import SourceEvidenceStore
from .execution_signer_v0_1 import VerifiedSignedEnvelope, _fresh_clock, _release_guard
from .ledger_actions_v0_1 import AttemptStageInput
from .ledger_domain_v0_1 import digest_value, ledger_utc
from .ledger_evidence_codec_v0_1 import strict_json_object
from .ledger_repository_v0_1 import LedgerRepository
from .public_rpc_v0_1 import PublicRpcProfile, primary_signature, u64

VERSION = "live_execution_send_v0.1"
_ENVELOPE = "LIVE_EXECUTION_VERIFIED_ENVELOPE"
_CLAIM = "LIVE_EXECUTION_SEND_CLAIM"
_UNCERTAIN = "LIVE_EXECUTION_POSSIBLE_SEND_UNKNOWN"
_OBSERVATION = "LIVE_EXECUTION_SEND_OBSERVATION"
_OUTCOMES = frozenset(("ACKNOWLEDGED", "NULL_RESPONSE", "RPC_ERROR", "INVALID_RESPONSE",
    "SIGNATURE_MISMATCH", "HTTP_FAILURE", "TRANSPORT_UNRESOLVED", "RESPONSE_LIMIT", "TIME_LIMIT"))


class ExecutionSendError(ValueError):
    """Only fixed local reasons cross the transport/clock/storage boundary."""


def _require(condition, reason):
    if not condition:
        raise ExecutionSendError(reason)


def _stage(prep, target, at, reference, original, wire=None):
    return AttemptStageInput(prep.attempt_id, target, prep.action_content_digest, prep.message_sha256,
        prep.lease.fingerprint, prep.message_policy_digest, at, reference, content_fingerprint(original),
        wire, canonical_json(original))


def _record(repository, stage, key, expected, fence):
    value = repository.record_external_attempt_stage(stage, idempotency_key=key,
        expected_attempt_revision=expected, fence=fence)
    after = repository.write_fence()
    _require(after.generation == fence.generation and after.generation_digest == fence.generation_digest
        and after.revision == fence.revision+1 and value.revision == expected+1
        and repository.attempt_stage_inputs(stage.attempt_id)[-1] == stage,
        "EXECUTION_EXPECTED_OWN_JOURNAL_CONTINUATION_REQUIRED")
    return value, after


def _envelope_record(envelope):
    return {"version": VERSION, "kind": _ENVELOPE, "envelope_digest": envelope.content_digest,
        "production_digest": envelope.production_digest,
        "sign_command_id": envelope.authority_receipt.original.request.command_id,
        "sign_receipt_digest": envelope.authority_receipt.content_digest,
        "consumed_common_digest": envelope.consumed_common_digest,
        "signing_clock": asdict(envelope.signing_clock)}


def load_signed_envelope(repository, attempt_id):
    """Reconstruct and cryptographically verify public durable output; no grant."""
    try:
        _require(type(repository) is LedgerRepository, "EXECUTION_LEDGER_REQUIRED")
        with repository._trusted_read():
            attempt = repository.attempt(attempt_id)
            stages = repository.attempt_stage_inputs(attempt_id)
            found = tuple(s for s in stages if s.external_reference == _ENVELOPE)
            _require(len(found) == 1 and found[0].target_stage == "SIGNED_DURABLE",
                "EXECUTION_DURABLE_VERIFIED_ENVELOPE_REQUIRED")
            stage = found[0]
            record = strict_json_object(stage.external_record_json)
            receipt = repository.authority_message_receipt(record["sign_command_id"])
            clock = dict(record["signing_clock"])
            if clock["reconciliation"] is not None:
                clock["reconciliation"] = ClockReconciliation(**clock["reconciliation"])
            envelope = VerifiedSignedEnvelope(attempt.preparation, record["production_digest"], receipt,
                record["consumed_common_digest"], TrustedClockSample(**clock), attempt.primary_signature, stage.signed_wire_base64)
            _require(_envelope_record(envelope) == record and envelope.signed_wire_digest == attempt.signed_wire_digest,
                "EXECUTION_DURABLE_ENVELOPE_BINDING_CONFLICT")
            _require(repository._conn.execute("SELECT content_digest FROM ledger_commits WHERE seq=?", (receipt.sequence,)).fetchone()
                == (envelope.consumed_common_digest,), "EXECUTION_DURABLE_SIGN_COMMIT_BINDING_CONFLICT")
            return envelope
    except ExecutionSendError:
        raise
    except Exception:
        raise ExecutionSendError("EXECUTION_DURABLE_ENVELOPE_UNRESOLVED") from None


def persist_signed_envelope(repository, envelope, *, fence):
    """Idempotently retain Q2's exact output, including interrupted stage writes.

    Historical SIGN is sufficient to store already signed public bytes. It is
    never sufficient for an external call. Lost signature output keeps Ledger's
    consumed SIGN unresolved; this function cannot sign or infer non-submission.
    """
    try:
        _require(type(repository) is LedgerRepository and type(envelope) is VerifiedSignedEnvelope,
            "EXECUTION_VERIFIED_SIGNED_ENVELOPE_REQUIRED")
        prep, receipt = envelope.preparation, envelope.authority_receipt
        with repository._trusted_read():
            _require(repository.authority_message_receipt(receipt.original.request.command_id) == receipt,
                "EXECUTION_ORIGINAL_DURABLE_SIGN_REQUIRED")
            _require(repository._conn.execute("SELECT content_digest FROM ledger_commits WHERE seq=?", (receipt.sequence,)).fetchone()
                == (envelope.consumed_common_digest,), "EXECUTION_DURABLE_SIGN_COMMIT_BINDING_CONFLICT")
        attempt = repository.attempt(prep.attempt_id)
        _require(attempt is not None and attempt.preparation == prep, "EXECUTION_EXACT_PREPARATION_REQUIRED")
        if attempt.primary_signature is not None:
            loaded = load_signed_envelope(repository, prep.attempt_id)
            _require(loaded == envelope, "EXECUTION_SIGNED_ENVELOPE_RETRY_CONFLICT")
            return loaded
        _require(attempt.lane_held and attempt.recorded_stage in ("PREPARED", "EXACT_SIMULATED", "AUTHORIZED")
            and not attempt.chain_quarantined, "EXECUTION_INTACT_SIGNED_PERSISTENCE_REQUIRED")
        current = fence
        for target in ("EXACT_SIMULATED", "AUTHORIZED", "SIGNED_DURABLE"):
            if target == "EXACT_SIMULATED" and attempt.recorded_stage != "PREPARED":
                continue
            if target == "AUTHORIZED" and attempt.recorded_stage != "EXACT_SIMULATED":
                continue
            final = target == "SIGNED_DURABLE"
            original = _envelope_record(envelope) if final else {
                "version": VERSION, "kind": "LIVE_EXECUTION_"+target, "sign_receipt_digest": receipt.content_digest,
                "envelope_digest": envelope.content_digest}
            stage = _stage(prep, target, envelope.signing_clock.utc_upper_utc,
                _ENVELOPE if final else "LIVE_EXECUTION_"+target, original, envelope.signed_wire_base64 if final else None)
            attempt, current = _record(repository, stage, "execution-envelope-"+target, attempt.revision, current)
        return load_signed_envelope(repository, prep.attempt_id)
    except ExecutionSendError:
        raise
    except Exception:
        raise ExecutionSendError("EXECUTION_ENVELOPE_PERSISTENCE_UNRESOLVED") from None


@dataclass(frozen=True, slots=True)
class SendObservation:
    attempt_id: str
    primary_signature: str
    signed_wire_digest: str
    authority_receipt_digest: str
    authority_stage: str
    rebroadcast_ordinal: int
    claim_digest: str
    provider_profile_digest: str
    call_started_at_utc: str
    outcome: str
    acknowledged_signature: str | None
    version: str = field(init=False, default=VERSION)
    possible_send: bool = field(init=False, default=True)
    finality: str = field(init=False, default="UNKNOWN")
    may_retry: bool = field(init=False, default=False)

    def __post_init__(self):
        for value in (self.attempt_id, self.signed_wire_digest, self.authority_receipt_digest,
                self.claim_digest, self.provider_profile_digest):
            digest_value(value)
        primary_signature(self.primary_signature)
        ledger_utc(self.call_started_at_utc)
        u64(self.rebroadcast_ordinal)
        _require(self.outcome in _OUTCOMES and self.authority_stage in ("SEND", "REBROADCAST")
            and (self.rebroadcast_ordinal == 0 if self.authority_stage == "SEND" else self.rebroadcast_ordinal > 0)
            and (self.acknowledged_signature == self.primary_signature if self.outcome == "ACKNOWLEDGED"
                 else self.acknowledged_signature is None), "EXECUTION_SANITIZED_OBSERVATION_INVALID")

    @property
    def content_digest(self):
        return content_fingerprint(asdict(self))


def load_send_observations(repository, attempt_id):
    """Read immutable original observations; finality remains a Ledger read."""
    result = []
    try:
        envelope = load_signed_envelope(repository, attempt_id)
        stages = repository.attempt_stage_inputs(attempt_id)
        claims = {s.external_record_digest: strict_json_object(s.external_record_json)
            for s in stages if s.external_reference == _CLAIM}
        for stage in stages:
            if stage.external_reference != _OBSERVATION:
                continue
            record = strict_json_object(stage.external_record_json)
            supplied = {k: v for k, v in record.items() if k not in ("version", "possible_send", "finality", "may_retry")}
            value = SendObservation(**supplied)
            _require(asdict(value) == record and value.attempt_id == attempt_id
                and value.primary_signature == envelope.primary_signature and value.signed_wire_digest == envelope.signed_wire_digest,
                "EXECUTION_STORED_OBSERVATION_BINDING_CONFLICT")
            claim = claims.get(value.claim_digest)
            _require(claim is not None and claim["envelope_digest"] == envelope.content_digest
                and all(claim[k] == record[k] for k in ("attempt_id", "primary_signature", "signed_wire_digest",
                    "authority_receipt_digest", "authority_stage", "rebroadcast_ordinal", "provider_profile_digest")),
                "EXECUTION_STORED_OBSERVATION_CLAIM_CONFLICT")
            result.append(value)
        return tuple(result)
    except ExecutionSendError:
        raise
    except Exception:
        raise ExecutionSendError("EXECUTION_STORED_OBSERVATION_UNRESOLVED") from None


_CLAIM_TOKEN = object()


class _ClaimedSend:
    """Ephemeral exact one-call ticket. Durable claims cannot recreate tickets."""
    __slots__ = ("envelope", "receipt", "digest", "_spent", "_lock")

    def __init__(self, envelope, receipt, digest, *, _token):
        _require(_token is _CLAIM_TOKEN, "EXECUTION_FRESH_SEND_CLAIM_REQUIRED")
        for name, value in (("envelope", envelope), ("receipt", receipt), ("digest", digest), ("_spent", False), ("_lock", Lock())):
            object.__setattr__(self, name, value)

    def __setattr__(self, name, value):
        raise AttributeError("immutable exact send ticket")

    def _spend(self):
        with self._lock:
            _require(not self._spent, "EXECUTION_SEND_TICKET_ALREADY_SPENT")
            object.__setattr__(self, "_spent", True)

    def __copy__(self):
        return self

    def __deepcopy__(self, memo):
        return self

    def __reduce_ex__(self, protocol):
        raise TypeError("ephemeral send ticket cannot be serialized")


class SolanaSendTransport:
    """Closed sendTransaction operation, one HTTP request per claimed ticket.

    No redirects, environment proxies, transparent retry, generic RPC method or
    raw wire public operation. The supplied HTTP boundary is trusted host code;
    deterministic qualification uses httpx.MockTransport exclusively.
    """
    __slots__ = ("_endpoint", "profile", "_client", "_requests", "_response_bytes", "_lock")

    def __init__(self, endpoint, profile, *, transport=None):
        try:
            url = httpx.URL(endpoint)
            _require(type(profile) is PublicRpcProfile and url.scheme == "https" and url.host
                and not url.userinfo and not url.fragment, "EXECUTION_SEND_ENDPOINT_OR_PROFILE_INVALID")
            self._endpoint, self.profile = url, profile
            self._requests, self._response_bytes, self._lock = 0, 0, Lock()
            self._client = httpx.Client(transport=transport, trust_env=False, follow_redirects=False,
                timeout=httpx.Timeout(min(profile.request_timeout_seconds, profile.observation_timeout_seconds)),
                headers={"Content-Type": "application/json", "Accept-Encoding": "identity"})
        except Exception:
            raise ExecutionSendError("EXECUTION_SEND_TRANSPORT_CONFIGURATION_INVALID") from None

    def __repr__(self):
        return "SolanaSendTransport(<private endpoint>)"

    def close(self):
        self._client.close()

    def _send_claimed(self, ticket):
        _require(type(ticket) is _ClaimedSend, "EXECUTION_FRESH_SEND_CLAIM_REQUIRED")
        ticket._spend()
        with self._lock:
            _require(self._requests < self.profile.max_requests, "EXECUTION_SEND_REQUEST_BUDGET_EXHAUSTED")
            self._requests += 1
            request_id = self._requests
        prep = ticket.envelope.preparation
        request = canonical_json({"jsonrpc": "2.0", "id": request_id, "method": "sendTransaction", "params": [
            ticket.envelope.signed_wire_base64, {"encoding": "base64", "skipPreflight": False,
                "preflightCommitment": "confirmed", "maxRetries": 0, "minContextSlot": prep.lease.context_slot}]}).encode()
        _require(len(request) <= 4096, "EXECUTION_SEND_REQUEST_TOO_LARGE")
        started = time.monotonic()
        # A finite full-response byte cap plus a socket timeout and elapsed
        # check per streamed chunk bound an honest transport's finite call.
        limit = min(4096, self.profile.max_response_bytes)
        deadline = min(self.profile.request_timeout_seconds, self.profile.observation_timeout_seconds)
        try:
            with self._client.stream("POST", self._endpoint, content=request) as response:
                if response.status_code != 200:
                    return "HTTP_FAILURE", None
                if (response.headers.get("content-encoding", "identity").lower() != "identity"
                        or response.headers.get("content-type", "").split(";", 1)[0].lower() != "application/json"):
                    return "INVALID_RESPONSE", None
                content = bytearray()
                for chunk in response.iter_bytes():
                    with self._lock:
                        self._response_bytes += len(chunk)
                        total = self._response_bytes
                    if len(content)+len(chunk) > limit or total > self.profile.max_total_response_bytes:
                        return "RESPONSE_LIMIT", None
                    if time.monotonic()-started >= deadline:
                        return "TIME_LIMIT", None
                    content.extend(chunk)
            if time.monotonic()-started >= deadline:
                return "TIME_LIMIT", None
            value = strict_json_object(bytes(content).decode("utf-8"))
            if value.get("jsonrpc") != "2.0" or type(value.get("id")) is not int or value["id"] != request_id:
                return "INVALID_RESPONSE", None
            if set(value) == {"jsonrpc", "id", "error"}:
                return "RPC_ERROR", None
            if set(value) != {"jsonrpc", "id", "result"}:
                return "INVALID_RESPONSE", None
            if value["result"] is None:
                return "NULL_RESPONSE", None
            if value["result"] != ticket.envelope.primary_signature:
                return "SIGNATURE_MISMATCH", None
            return "ACKNOWLEDGED", value["result"]
        except Exception:
            return "TRANSPORT_UNRESOLVED", None


def _source_guard(receipt, source_store):
    if receipt.original.validation.context.action.side == "BUY":
        _require(type(source_store) is SourceEvidenceStore and not source_store._conn.in_transaction,
            "EXECUTION_CURRENT_SOURCE_STORE_REQUIRED")
        source_store._conn.execute("BEGIN IMMEDIATE")
        return receipt.original.source.source_sequence, receipt.original.source.source
    _require(source_store is None and receipt.original.source is None, "EXECUTION_SELL_ENTRY_SOURCE_FORBIDDEN")
    return None


def _last_call(repository, receipt, envelope, fence, source_store, source_expected, clock, previous=None):
    current = repository._begin_economic_write(fence)
    repository._new_revision(fence, current)
    _require(repository.authority_message_receipt(receipt.original.request.command_id) == receipt,
        "EXECUTION_ORIGINAL_DURABLE_SEND_REQUIRED")
    attempt = repository.attempt(envelope.preparation.attempt_id)
    _require(attempt is not None and attempt.lane_held and repository._lane()[0] == attempt.preparation.attempt_id
        and not attempt.chain_quarantined and not attempt.economically_applied
        and attempt.chain_finality not in ("FINALIZED_SUCCESS", "FINALIZED_FAILURE", "PROVEN_NON_LANDED")
        and attempt.primary_signature == envelope.primary_signature and attempt.signed_wire_digest == envelope.signed_wire_digest,
        "EXECUTION_CURRENT_EXACT_UNRESOLVED_ATTEMPT_REQUIRED")
    try:
        sample = clock()
    except Exception:
        raise ExecutionSendError("EXECUTION_CURRENT_CLOCK_UNAVAILABLE") from None
    _require(repository._conn.in_transaction and repository.write_fence() == fence
        and (source_expected is None or source_store._conn.in_transaction
            and source_store.binding == source_expected[1].binding and source_store.profile == source_expected[1].profile
            and source_store.latest_record() == source_expected), "EXECUTION_LAST_CALL_GUARD_INTERRUPTED")
    with repository._trusted_read():
        _fresh_clock(repository, receipt, sample)
    if previous is not None:
        _require(sample.monotonic_ns > previous.monotonic_ns and sample.utc_lower_utc >= previous.utc_lower_utc,
            "EXECUTION_POST_CLAIM_CLOCK_CONTINUATION_REQUIRED")
    return attempt, sample


def send_exact(repository, delivery, transport, *, source_store, clock):
    """Fresh A4b SEND/next REBROADCAST -> durable claim -> one exact call.

    Both journals stay writer fenced through the final current-clock recheck
    and external call. Claim commits necessarily release/reacquire the Ledger
    lock; every continuation admits only the exact expected own journal write.
    The claim itself means possible send even if a crash precedes UNKNOWN or
    the HTTP call. A lost delivery/response can never recreate a one-call ticket.
    """
    ledger_owned = source_owned = False
    try:
        _require(type(delivery) is FreshStageConsumption and delivery.stage in ("SEND", "REBROADCAST"),
            "EXECUTION_FRESH_SEND_DELIVERY_REQUIRED")
        delivery._claim_execution()
        _require(type(repository) is LedgerRepository and type(transport) is SolanaSendTransport and callable(clock),
            "EXECUTION_EXACT_SEND_INPUT_REQUIRED")
        _require(delivery._uses_source_store(source_store), "EXECUTION_ORIGINAL_SOURCE_STORE_REQUIRED")
        receipt = delivery.receipt
        request = receipt.original.request
        envelope = load_signed_envelope(repository, request.attempt_id)
        _require(request.primary_signature == envelope.primary_signature
            and receipt.decision.prior_sign_digest == envelope.authority_receipt.content_digest
            and transport.profile.fingerprint == receipt.original.validation.profile.rpc_profile_fingerprint,
            "EXECUTION_CURRENT_SEND_ENVELOPE_AUTHORITY_CONFLICT")
        fence = repository.write_fence()
        _require((fence.generation, fence.revision, fence.last_receipt_digest) ==
            (delivery.writer_generation, receipt.sequence, delivery.consumed_common_digest),
            "EXECUTION_CONSUMED_LEDGER_CONTINUATION_CHANGED")
        _require(not repository._conn.in_transaction, "EXECUTION_OWNED_LAST_CALL_TRANSACTION_REQUIRED")
        # Mark ownership only after excluding a caller-owned source transaction.
        if source_store is not None:
            _require(type(source_store) is SourceEvidenceStore and not source_store._conn.in_transaction,
                "EXECUTION_CURRENT_SOURCE_STORE_REQUIRED")
            source_owned = True
        source_expected = _source_guard(receipt, source_store)
        ledger_owned = True
        attempt, sample = _last_call(repository, receipt, envelope, fence, source_store, source_expected, clock)
        _require(attempt.revision == request.expected_attempt_revision
            and (attempt.recorded_stage == "SIGNED_DURABLE" if request.stage == "SEND"
                 else attempt.recorded_stage in ("SEND_CLAIMED", "OBSERVING", "UNKNOWN")),
            "EXECUTION_CURRENT_SEND_ATTEMPT_REVISION_OR_STAGE_CONFLICT")
        _release_guard(repository._conn)
        original = {"version": VERSION, "kind": _CLAIM, "attempt_id": request.attempt_id,
            "envelope_digest": envelope.content_digest, "primary_signature": envelope.primary_signature,
            "signed_wire_digest": envelope.signed_wire_digest, "authority_receipt_digest": receipt.content_digest,
            "authority_stage": request.stage, "rebroadcast_ordinal": request.rebroadcast_ordinal,
            "consumed_common_digest": delivery.consumed_common_digest, "claim_clock": asdict(sample),
            "provider_profile_digest": transport.profile.fingerprint, "possible_send": True, "finality": "UNKNOWN"}
        key = "execution-send-"+receipt.content_digest
        target = "SEND_CLAIMED" if request.stage == "SEND" else attempt.recorded_stage
        stage = _stage(envelope.preparation, target, sample.utc_upper_utc, _CLAIM, original)
        attempt, fence = _record(repository, stage, key, attempt.revision, fence)
        if attempt.recorded_stage != "UNKNOWN":
            uncertain = _stage(envelope.preparation, "UNKNOWN", sample.utc_upper_utc, _UNCERTAIN,
                {"version": VERSION, "claim_digest": content_fingerprint(original), "possible_send": True, "finality": "UNKNOWN"})
            attempt, fence = _record(repository, uncertain, key+"-unknown", attempt.revision, fence)
        attempt, final_clock = _last_call(repository, receipt, envelope, fence, source_store, source_expected, clock, sample)
        ticket = _ClaimedSend(envelope, receipt, content_fingerprint(original), _token=_CLAIM_TOKEN)
        try:
            outcome, acknowledged = transport._send_claimed(ticket)
        except Exception:
            outcome, acknowledged = "TRANSPORT_UNRESOLVED", None
        observation = SendObservation(request.attempt_id, envelope.primary_signature, envelope.signed_wire_digest,
            receipt.content_digest, request.stage, request.rebroadcast_ordinal, ticket.digest,
            transport.profile.fingerprint, final_clock.utc_upper_utc, outcome, acknowledged)
        _release_guard(repository._conn)
        stage = _stage(envelope.preparation, "UNKNOWN", final_clock.utc_upper_utc, _OBSERVATION, asdict(observation))
        _record(repository, stage, key+"-observation", attempt.revision, fence)
        return observation
    except ExecutionSendError:
        raise
    except Exception:
        raise ExecutionSendError("EXECUTION_SEND_UNRESOLVED") from None
    finally:
        try:
            if ledger_owned:
                _release_guard(repository._conn)
        finally:
            if source_owned:
                _release_guard(source_store._conn)
