"""Concrete LIVE custody binding and original-Evidence opening adjudication.

There are no candidate, position, execution, settlement or admission reducers here.
An established baseline is a historical receipt, not current spending authority.
"""
from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone

from phase5.shadow_domain_v0_1 import canonical_json, content_fingerprint
from phase5.shadow_venue_route_quote_v0_1 import WSOL_MINT
from .public_rpc_v0_1 import block_hash, immutable_tuple, public_key, u64
from .wallet_evidence_v0_1 import ExpectedTokenAccount, WalletObservation, ledger_account_evidence

SCHEMA_VERSION = "live_ledger_v0.4"
DOMAIN_VERSION = "live_ledger_domain_v0.1"
BASELINE_VERSION = "live_ledger_opening_baseline_v0.1"
OPENING_PROFILE = "NATIVE_RECYCLING_V1"
ZERO_DIGEST = "0" * 64


class LedgerContractError(ValueError):
    """Only fixed local codes; never echo observations or credentials."""


def digest_value(value: object) -> str:
    if type(value) is not str or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise LedgerContractError("LEDGER_DIGEST_INVALID")
    return value


def ledger_utc(value: str) -> str:
    try:
        if type(value) is not str:
            raise ValueError
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
            raise ValueError
        return parsed.astimezone(timezone.utc).isoformat()
    except Exception:
        raise LedgerContractError("LEDGER_UTC_REQUIRED") from None


@dataclass(frozen=True, slots=True)
class LedgerDomain:
    genesis_hash: str
    wallet: str
    expected_profile_fingerprint: str
    known_native_wallet_lamports: int | None
    expected_empty_token_accounts: tuple[ExpectedTokenAccount, ...] = ()
    minimum_context_slot: int = 0
    mode: str = "LIVE"
    schema_version: str = SCHEMA_VERSION
    domain_version: str = DOMAIN_VERSION
    opening_profile: str = OPENING_PROFILE

    def __post_init__(self) -> None:
        try:
            block_hash(self.genesis_hash)
            public_key(self.wallet)
            digest_value(self.expected_profile_fingerprint)
            if self.known_native_wallet_lamports is not None:
                u64(self.known_native_wallet_lamports)
            u64(self.minimum_context_slot)
            immutable_tuple(self.expected_empty_token_accounts, ExpectedTokenAccount)
            keys = tuple(item.pubkey for item in self.expected_empty_token_accounts)
            if (len(keys) > 32 or len(set(keys)) != len(keys) or self.wallet in keys
                    or any(item.mint == WSOL_MINT for item in self.expected_empty_token_accounts)
                    or self.mode != "LIVE" or self.schema_version != SCHEMA_VERSION
                    or self.domain_version != DOMAIN_VERSION or self.opening_profile != OPENING_PROFILE):
                raise ValueError
        except Exception:
            raise LedgerContractError("LEDGER_DOMAIN_CONTRACT_INVALID") from None

    @property
    def economic_domain_id(self) -> str:
        # Metadata/config changes cannot regenerate the economic custody identity.
        return content_fingerprint({"genesis_hash": self.genesis_hash, "wallet": self.wallet, "mode": self.mode})

    def to_record(self) -> dict:
        return asdict(self)

    @property
    def binding_digest(self) -> str:
        return content_fingerprint(self.to_record())

    def serialize(self) -> str:
        return canonical_json(self.to_record())


@dataclass(frozen=True, slots=True)
class OpeningAccountFact:
    pubkey: str
    mint: str
    program: str
    presence: str
    observed_lamports: int | None


@dataclass(frozen=True, slots=True)
class OpeningBaselineDecision:
    economic_domain_id: str
    binding_digest: str
    evidence_digest: str
    evaluated_at_utc: str
    required_min_context_slot: int
    disposition: str
    reasons: tuple[str, ...]
    native_account_presence: str
    inventory_coverage: str
    coherent_context: str
    supported_shape: str
    context_slot: int | None
    observed_native_lamports: int | None
    owned_native_lamports: int | None
    known_token_accounts: tuple[OpeningAccountFact, ...]
    locked_token_account_lamports: int | None
    version: str = BASELINE_VERSION

    def to_record(self) -> dict:
        return asdict(self)

    @property
    def content_digest(self) -> str:
        return content_fingerprint(self.to_record())


def adjudicate_opening_baseline(domain: LedgerDomain, observation: WalletObservation, *,
                               evaluated_at_utc: str, required_min_context_slot: int) -> OpeningBaselineDecision:
    """Re-derive the accepted port at the recorded ORIGINAL ingestion inputs.

Replay supplies those same inputs; it never silently substitutes today's clock.
Absence and failed coverage are retained separately. Only a fully supported
opening cut can recognize configured native funding or locked account lamports.
"""
    if type(domain) is not LedgerDomain or type(observation) is not WalletObservation:
        raise LedgerContractError("LEDGER_ORIGINAL_WALLET_EVIDENCE_REQUIRED")
    evaluated_at_utc = ledger_utc(evaluated_at_utc)
    u64(required_min_context_slot)
    port = ledger_account_evidence(observation, expected_wallet=domain.wallet,
        expected_genesis=domain.genesis_hash, expected_profile_fingerprint=domain.expected_profile_fingerprint,
        required_min_context_slot=max(domain.minimum_context_slot, required_min_context_slot), now_utc=evaluated_at_utc)
    assessment = port.assessment
    reasons = set((*assessment.reasons, *port.consumer_reasons))
    quarantine = (assessment.inventory_coverage == "CONTRADICTORY" or assessment.supported_shape == "UNSUPPORTED"
                  or any("MISMATCH" in reason or "CONTRADICTION" in reason for reason in reasons))
    if required_min_context_slot < domain.minimum_context_slot:
        reasons.add("OPENING_CONTEXT_FLOOR_BELOW_BINDING")
    if domain.known_native_wallet_lamports is None:
        reasons.add("OPENING_KNOWN_NATIVE_FUNDING_MISSING")
    if observation.request.expected_accounts != domain.expected_empty_token_accounts:
        reasons.add("OPENING_EXPECTED_ACCOUNT_BINDING_MISMATCH")
        quarantine = True

    explicit = observation.explicit_read
    by_key = {} if explicit is None else dict(zip(explicit.requested_keys, explicit.accounts, strict=True))
    native = by_key.get(domain.wallet)
    observed_native = None if native is None else native.lamports
    if (observed_native is not None and domain.known_native_wallet_lamports is not None
            and observed_native != domain.known_native_wallet_lamports):
        reasons.add("OPENING_NATIVE_FUNDING_MISMATCH")
        quarantine = True
    known = {item.pubkey: item for item in domain.expected_empty_token_accounts}
    for token in assessment.tokens:
        if token.mint == WSOL_MINT or token.native_rent_reserve is not None:
            # Even an empty WSOL account is outside the initial native profile.
            reasons.add("PREEXISTING_WSOL_ACCOUNT_UNSUPPORTED")
            quarantine = True
        if token.amount is not None and token.amount != 0:
            reasons.add("UNATTRIBUTED_PREEXISTING_TOKEN_UNITS")
            quarantine = True
        expected = known.get(token.pubkey)
        if expected is None or (token.mint, token.program) != (expected.mint, expected.program):
            reasons.add("UNATTRIBUTED_PREEXISTING_TOKEN_ACCOUNT")
            quarantine = True

    account_facts = tuple(OpeningAccountFact(item.pubkey, item.mint, item.program,
        "UNKNOWN" if item.pubkey not in by_key else "ABSENT" if by_key[item.pubkey] is None else "PRESENT",
        None if by_key.get(item.pubkey) is None else by_key[item.pubkey].lamports)
        for item in domain.expected_empty_token_accounts)
    established = port.account_facts_usable and not reasons
    disposition = "ESTABLISHED" if established else "QUARANTINED" if quarantine else "UNRESOLVED"
    return OpeningBaselineDecision(domain.economic_domain_id, domain.binding_digest, observation.content_digest,
        evaluated_at_utc, required_min_context_slot, disposition, tuple(sorted(reasons)),
        assessment.native_account_presence, assessment.inventory_coverage, assessment.coherent_context,
        assessment.supported_shape, assessment.context_slot, observed_native,
        observed_native if established else None, account_facts,
        # Exact Python total, potentially larger than u64. This is observed locked
        # account SOL, NOT a measured rent-exemption minimum and NOT native wallet SOL.
        sum(item.observed_lamports or 0 for item in account_facts) if established else None)
