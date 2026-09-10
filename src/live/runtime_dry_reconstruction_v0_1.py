"""C4 DRY-only cold graph; C1 owns interruption and NON_SUBMITTED release."""
from __future__ import annotations

from dataclasses import dataclass, field

from .authority_controls_v0_1 import require
from .ledger_ports_v0_1 import ConsumerCut
from .ledger_repository_v0_1 import LedgerRepository
from .runtime_dry_v0_1 import _original, _terminal, finish_interrupted_dry


@dataclass(frozen=True, slots=True)
class DryReconstructionFacts:
    consumer_cut: ConsumerCut
    root_id: str
    action_id: str
    attempt_id: str | None
    disposition: str
    terminal_receipt_key: str | None
    tentative_capacity_held: bool
    grants_permission: bool = field(init=False, default=False)


class ColdDryRuntimeV01:
    def __init__(self):
        raise TypeError("RUNTIME_COLD_DRY_REOPEN_REQUIRED")

    def close(self):
        self.ledger.close()

    def reconstruction_facts(self):
        with self.ledger._trusted_read():
            snapshot = self.ledger.consumer_snapshot()
            action, reservation = _original(self.ledger, self.action_id)
            terminal = _terminal(self.ledger, action, reservation)
            attempts = self.ledger._root_attempts(action.root_id)
            latest = max(attempts, key=lambda a:a.preparation.ordinal) if attempts else None
            result = DryReconstructionFacts(snapshot["consumer_cut"], action.root_id, action.action_id,
                None if latest is None else latest.preparation.attempt_id,
                self.ledger.inbox_disposition(action.root_id),
                None if terminal is None else terminal.ingestion_key,
                reservation.status == "TENTATIVE_DRY")
            require(self.ledger.consumer_snapshot()["consumer_cut"] == snapshot["consumer_cut"],
                "RUNTIME_COLD_DRY_COMMON_CUT_CHANGED")
            return result

    def step(self, *, recorded_at_utc):
        return finish_interrupted_dry(self.ledger, self.action_id, recorded_at_utc=recorded_at_utc)


def reopen_dry(ledger_path, domain, action_id):
    """Resolve an original action handle; never construct a replacement attempt."""
    require(domain.mode == "DRY", "RUNTIME_DRY_FIXED_DOMAIN_REQUIRED")
    root = object.__new__(ColdDryRuntimeV01)
    root.ledger = LedgerRepository.reopen(ledger_path, domain)
    root.action_id = action_id
    try:
        root.reconstruction_facts()
        return root
    except BaseException:
        root.close()
        raise
