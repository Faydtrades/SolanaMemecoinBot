"""Bounded current Pump compatibility facts and explicit non-economic denial.

Official Pump IDLs f216b6724c6ede79d7cef9ce210b741f7e17e93b and
HOLDER_REWARDS_README define the appended creator-fee/holder fields. Historical
research decoders remain intact; production checks these additional semantics.
"""
from dataclasses import dataclass
from .public_rpc_v0_1 import PublicAccountRead, FinalizedBlockAnchor, evidence_fingerprint, public_key
from .ledger_actions_v0_1 import utc_microseconds
from .wallet_evidence_v0_1 import assess_wallet_accounts, ledger_account_evidence
from phase5 import shadow_venue_route_quote_v0_1 as venue


def current_state_flags(account, *, pump):
    """Decode known tail, old missing fields, and zero allocation padding.

    A missing holder byte is officially false. Partial creator-fee fields,
    invalid bools and nonzero unknown tails are never guessed or ignored.
    """
    offset = 115 if pump else 261
    tail = account.data[offset:]
    if not tail:
        return (0, False, False)
    if len(tail) < 9 or tail[8] not in (0, 1) or len(tail) >= 10 and tail[9] not in (0, 1):
        raise ValueError("PUMP_CURRENT_STATE_TAIL_INVALID")
    if any(tail[10:]):
        raise ValueError("PUMP_UNKNOWN_STATE_TAIL_UNSUPPORTED")
    return (int.from_bytes(tail[:8], "little"), bool(tail[8]), len(tail) >= 10 and bool(tail[9]))


def selected_state_compatibility(mint, curve_account, pool_account):
    """Return selected venue and stable reason; no quote or economic acceptance."""
    curve = venue.decode_pump_curve(curve_account, mint)
    selected, raw, pump = curve, curve_account, True
    if curve.complete:
        if pool_account is None:
            return None, "PUMP_MIGRATION_POOL_UNAVAILABLE"
        selected, raw, pump = venue.decode_pumpswap_pool(pool_account, mint), pool_account, False
    _, can_edit_fee, holder = current_state_flags(raw, pump=pump)
    selected_venue = "PUMP" if pump else "PUMPSWAP"
    if selected.is_mayhem_mode:
        return selected_venue, "PUMP_MAYHEM_UNSUPPORTED"
    if selected.is_cashback_coin:
        return selected_venue, "PUMP_CASHBACK_UNSUPPORTED"
    if holder:
        return selected_venue, "PUMP_HOLDER_REWARDS_UNSUPPORTED"
    if can_edit_fee:
        return selected_venue, "PUMP_EDITABLE_CREATOR_FEE_UNSUPPORTED"
    # SOL uses the standard dynamic fee table. The stored creator_fee_bps
    # value is parsed but is not a replacement fee source for this scope.
    return selected_venue, None


@dataclass(frozen=True, slots=True)
class PumpProtocolObservation:
    mint: str
    wallet_support_digest: str
    genesis_start: str
    genesis_end: str
    profile_fingerprint: str
    read: PublicAccountRead
    anchor: FinalizedBlockAnchor
    observed_at_utc: str

    def __post_init__(self):
        public_key(self.mint)
        if type(self.read) is not PublicAccountRead or type(self.anchor) is not FinalizedBlockAnchor:
            raise ValueError("PUMP_ORIGINAL_PUBLIC_PROTOCOL_FACTS_REQUIRED")
        utc_microseconds(self.observed_at_utc)

    @property
    def content_digest(self):
        return evidence_fingerprint(self)

    def classify(self, domain, candidate, sample, wallet):
        """UNKNOWN holds; only a current positive incompatibility is skipped."""
        stamp = utc_microseconds(self.observed_at_utc)
        observation = wallet.observation
        if (self.mint != candidate.mint or self.wallet_support_digest != wallet.digest
                or self.genesis_start != domain.genesis_hash or self.genesis_end != domain.genesis_hash
                or self.profile_fingerprint != domain.expected_profile_fingerprint
                or self.anchor.provider_fingerprint != self.profile_fingerprint
                or self.read.context.slot != self.anchor.slot
                or observation.anchor is None or self.anchor.slot < observation.anchor.slot
                or stamp > utc_microseconds(sample.utc_lower_utc)
                or utc_microseconds(sample.utc_upper_utc)-stamp >= observation.profile.observation_freshness_seconds*1000000):
            return "UNKNOWN", None, "PUMP_CURRENT_PROTOCOL_CONTEXT_UNPROVEN"
        assessment = assess_wallet_accounts(observation)
        support = ledger_account_evidence(observation, expected_wallet=domain.wallet,
            expected_genesis=domain.genesis_hash, expected_profile_fingerprint=domain.expected_profile_fingerprint,
            required_min_context_slot=max(domain.minimum_context_slot, wallet.required_min_context_slot),
            now_utc=sample.utc_upper_utc)
        chain_age = utc_microseconds(sample.utc_upper_utc) - self.anchor.block_time * 1000000
        if (support.consumer_reasons or chain_age < 0
                or chain_age >= observation.profile.finalized_block_freshness_seconds * 1000000):
            return "UNKNOWN", None, "PUMP_CURRENT_PROTOCOL_CONTEXT_UNPROVEN"
        if assessment.coherent_context != "COHERENT" or assessment.inventory_coverage != "COMPLETE":
            return "UNKNOWN", None, "PUMP_CURRENT_WALLET_CONTEXT_UNPROVEN"
        keys = (self.mint, venue.derive_bonding_curve_pda(self.mint), venue.derive_pumpswap_pool_pda(self.mint))
        if self.read.requested_keys != keys:
            return "UNKNOWN", None, "PUMP_EXACT_PROTOCOL_ACCOUNT_SET_REQUIRED"
        mint, curve, pool = self.read.accounts
        if mint is None or curve is None:
            return "UNKNOWN", None, "PUMP_FINALIZED_MINT_OR_CURVE_UNAVAILABLE"
        original = {key:value for read in observation.mint_reads for key,value in zip(read.requested_keys,read.accounts)}
        if self.mint not in original or original[self.mint] is None or original[self.mint].account != mint.account:
            return "UNKNOWN", None, "PUMP_MINT_CHANGED_SINCE_WALLET_OBSERVATION"
        if mint.executable or curve.executable or pool is not None and pool.executable:
            return "UNSUPPORTED", None, "PUMP_EXECUTABLE_DATA_ACCOUNT_UNSUPPORTED"
        try:
            if mint.account.owner in (venue.TOKEN_2022_PROGRAM_ID, venue.TOKEN_PROGRAM_ID):
                from .pump_current_state_v0_1 import validate_current_pump_supply
                validate_current_pump_supply(curve.account, mint.account, self.mint)
            else:
                raise ValueError("PUMP_TOKEN_PROGRAM_UNSUPPORTED")
            # Original wallet mint base checks remain required for legacy SPL.
            shape = next((m for m in assessment.mints if m.pubkey == self.mint), None)
            if shape is None:
                return "UNKNOWN", None, "PUMP_ORIGINAL_MINT_ASSESSMENT_UNAVAILABLE"
            if shape.reasons:
                raise ValueError("PUMP_MINT_SHAPE_UNSUPPORTED")
            selected, reason = selected_state_compatibility(self.mint, curve.account, None if pool is None else pool.account)
        except (ValueError, venue.VenueStateError):
            return "UNSUPPORTED", None, "PUMP_MINT_OR_PROTOCOL_SHAPE_UNSUPPORTED"
        if reason == "PUMP_MIGRATION_POOL_UNAVAILABLE":
            return "UNKNOWN", None, reason
        return ("SUPPORTED", selected, "PUMP_NORMAL_SOL_SUPPORTED") if reason is None else ("UNSUPPORTED", selected, reason)


def rejection_proof(protocol, wallet, sample):
    """Retain the original facts, not a caller's classification assertion."""
    import json
    from dataclasses import asdict
    from .ledger_evidence_codec_v0_1 import _read_record, wallet_observation_to_json
    return json.loads(json.dumps({"schema":"PUMP_PROTOCOL_NONACCEPTANCE_PROOF_V1",
        "protocol":{**asdict(protocol), "read":_read_record(protocol.read)},
        "wallet_observation_json":wallet_observation_to_json(wallet.observation),
        "wallet_evaluated_at_utc":wallet.evaluated_at_utc,
        "wallet_required_min_context_slot":wallet.required_min_context_slot,
        "clock":asdict(sample)}))


def validate_rejection_proof(proof, domain, authority, candidate, record):
    from .ledger_evidence_codec_v0_1 import _object, _read, wallet_observation_from_json
    from .ledger_settlement_v0_1 import WalletSupportInput
    from .authority_controls_v0_1 import TrustedClockSample, clock_reasons, require
    row = _object(proof, ("schema", "protocol", "wallet_observation_json",
        "wallet_evaluated_at_utc", "wallet_required_min_context_slot", "clock"))
    require(row["schema"] == "PUMP_PROTOCOL_NONACCEPTANCE_PROOF_V1", "PUMP_REJECTION_PROOF_SCHEMA_INVALID")
    data = _object(row["protocol"], ("mint", "wallet_support_digest", "genesis_start", "genesis_end",
        "profile_fingerprint", "read", "anchor", "observed_at_utc"))
    protocol = PumpProtocolObservation(**{**data, "read":_read(data["read"]),
        "anchor":FinalizedBlockAnchor(**data["anchor"])})
    wallet = WalletSupportInput(wallet_observation_from_json(row["wallet_observation_json"]),
        row["wallet_evaluated_at_utc"], row["wallet_required_min_context_slot"])
    sample = TrustedClockSample(**row["clock"])
    disposition, _, reason = protocol.classify(domain, candidate, sample, wallet)
    require(not clock_reasons(authority, sample) and disposition == "UNSUPPORTED"
        and record["disposition"] == "REJECTED" and record["external_reference"] == reason
        and record["external_record_digest"] == protocol.content_digest
        and record["recorded_at_utc"] == sample.utc_lower_utc,
        "PUMP_ORIGINAL_NONACCEPTANCE_PROOF_CONFLICT")
