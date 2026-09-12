"""Q4 durable Execution identity -> bounded original Evidence -> Ledger.

This read/ingestion handoff grants no retry, releases no lane and posts no
economic settlement. Ledger owns every finality and reconciliation decision.
"""
from __future__ import annotations

from .execution_send_v0_1 import load_signed_envelope
from .ledger_domain_v0_1 import ledger_utc
from .ledger_repository_v0_1 import LedgerRepository
from .public_rpc_v0_1 import PublicReadOnlyRpc
from .transaction_evidence_v0_1 import TransactionEvidenceAdapter, TransactionRequest
from .transaction_coverage_v0_1 import CanonicalCoverageAdapter, CoverageRequest


class ExecutionReconciliationError(ValueError):
    """Fixed local errors; private transport/clock details never escape."""


def observe_attempt_finality(repository, attempt_id, rpc, *, ingestion_key,
        evaluated_at_utc, fence, clock=None):
    """Read only the durable signature and commit original evidence atomically.

    The supplied fence spans the public read. A concurrent Ledger change rejects
    ingestion; the caller cannot silently attach this read to a newer attempt.
    Signed output is sufficient even when a crash left no claim or send receipt.
    Missing/failed/contradictory public facts still reach Ledger as evidence.
    """
    try:
        if type(repository) is not LedgerRepository or type(rpc) is not PublicReadOnlyRpc:
            raise ExecutionReconciliationError("EXECUTION_BOUND_LEDGER_PUBLIC_READ_REQUIRED")
        evaluated_at_utc = ledger_utc(evaluated_at_utc)
        with repository._trusted_read():
            if repository.write_fence() != fence:
                raise ExecutionReconciliationError("EXECUTION_CURRENT_RECONCILIATION_FENCE_REQUIRED")
            envelope = load_signed_envelope(repository, attempt_id)
            baseline = repository.baseline()
            domain = repository.domain
            if baseline is None or rpc.profile.fingerprint != domain.expected_profile_fingerprint:
                raise ExecutionReconciliationError("EXECUTION_ORIGINAL_BASELINE_PROVIDER_REQUIRED")
            request = TransactionRequest(domain.genesis_hash, envelope.primary_signature,
                max(domain.minimum_context_slot, baseline.observation.anchor.slot,
                    envelope.preparation.finalized_lower_anchor.slot))
        try:
            observation = TransactionEvidenceAdapter(rpc, request, clock=clock).observe()
        except Exception:
            # Even a callback raising our own error type cannot export its text.
            raise ExecutionReconciliationError("EXECUTION_ORIGINAL_OBSERVATION_UNRESOLVED") from None
        # No Execution pre-adjudication: retain even missing or contradictory
        # originals so Ledger's accepted cross-observation reducer owns truth.
        return repository.ingest_chain_observation(attempt_id, observation,
            ingestion_key=ingestion_key, evaluated_at_utc=evaluated_at_utc, fence=fence)
    except ExecutionReconciliationError:
        raise
    except Exception:
        raise ExecutionReconciliationError("EXECUTION_FINALITY_HANDOFF_UNRESOLVED") from None


def observe_attempt_coverage(repository, attempt_id, rpc, *, ingestion_key,
        evaluated_at_utc, fence, clock=None):
    """Retain one bounded canonical read of the original signed validity range.

    Ledger owns completeness, nonlanding and conflicts. This handoff grants no
    replacement permission and does not apply the resulting disposition.
    """
    try:
        if type(repository) is not LedgerRepository or type(rpc) is not PublicReadOnlyRpc:
            raise ExecutionReconciliationError("EXECUTION_BOUND_LEDGER_PUBLIC_READ_REQUIRED")
        evaluated_at_utc = ledger_utc(evaluated_at_utc)
        with repository._trusted_read():
            if repository.write_fence() != fence:
                raise ExecutionReconciliationError("EXECUTION_CURRENT_RECONCILIATION_FENCE_REQUIRED")
            envelope = load_signed_envelope(repository, attempt_id)
            baseline = repository.baseline()
            domain = repository.domain
            if baseline is None or rpc.profile.fingerprint != domain.expected_profile_fingerprint:
                raise ExecutionReconciliationError("EXECUTION_ORIGINAL_BASELINE_PROVIDER_REQUIRED")
            prep = envelope.preparation
            request = CoverageRequest(domain.genesis_hash, envelope.primary_signature,
                prep.lease.blockhash, prep.lease.last_valid_block_height, prep.finalized_lower_anchor)
        try:
            observation = CanonicalCoverageAdapter(rpc, request, clock=clock).observe()
        except Exception:
            raise ExecutionReconciliationError("EXECUTION_ORIGINAL_OBSERVATION_UNRESOLVED") from None
        return repository.ingest_chain_observation(attempt_id, observation,
            ingestion_key=ingestion_key, evaluated_at_utc=evaluated_at_utc, fence=fence)
    except ExecutionReconciliationError:
        raise
    except Exception:
        raise ExecutionReconciliationError("EXECUTION_FINALITY_HANDOFF_UNRESOLVED") from None
