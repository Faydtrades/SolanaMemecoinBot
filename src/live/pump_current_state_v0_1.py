"""Current supported Pump state with separately evidenced mint supply.

The curve retains its creation supply. Current GetFeesWithQuoteMint uses the
actual mint supply, which can decrease after an SPL/Token2022 Burn. Historical
Phase5 state construction and payloads retain their original semantics.
"""
from dataclasses import dataclass, field
from phase5 import shadow_venue_route_quote_v0_1 as venue
from .pump_token2022_profile_v0_1 import parse_pump_token2022_mint, PROFILE

STATE_SCHEMA = "live-pump-current-state-v0.1"


@dataclass(frozen=True, slots=True)
class CurrentLegacyPumpMint:
    mint: str
    supply: int
    profile: str = field(init=False, default="PUMP_SOL_LEGACY_SPL_BASE_V1")


def current_pump_mint(account):
    if account.owner == venue.TOKEN_PROGRAM_ID:
        base = venue.decode_mint_account(account, account.pubkey)
        if (len(account.data) != 82 or base.decimals != 6
                or account.data[:4] != bytes(4) or account.data[46:50] != bytes(4)):
            raise venue.VenueStateError("CURRENT_PUMP_LEGACY_BASE_MINT_UNSUPPORTED")
        return CurrentLegacyPumpMint(account.pubkey, base.supply)
    return parse_pump_token2022_mint(account)


def validate_current_pump_supply(curve_raw, mint_raw, mint):
    if mint_raw.pubkey != mint:
        raise venue.VenueStateError("CURRENT_PUMP_MINT_IDENTITY_CONFLICT")
    curve = venue.decode_pump_curve(curve_raw, mint)
    parsed = current_pump_mint(mint_raw)
    if not 0 < parsed.supply <= curve.token_total_supply:
        raise venue.VenueStateError("CURRENT_PUMP_MINT_SUPPLY_OUTSIDE_INITIAL")
    if curve.real_token_reserves > parsed.supply:
        raise venue.VenueStateError("CURRENT_PUMP_REAL_RESERVES_EXCEED_MINT_SUPPLY")
    return parsed


@dataclass(frozen=True, slots=True)
class PumpCurrentBondingCurveStateV01(venue.PumpBondingCurveStateV01):
    current_mint_account: venue.RpcAccountV01
    schema_version: str = field(init=False, default=STATE_SCHEMA)

    def __post_init__(self):
        parsed = current_pump_mint(self.current_mint_account)
        if (parsed.mint != self.mint or self.base_token_program != self.current_mint_account.owner
                or not 0 < parsed.supply <= self.decoded.token_total_supply
                or self.decoded.real_token_reserves > parsed.supply):
            raise venue.VenueStateError("CURRENT_PUMP_SUPPLY_IDENTITY_CONFLICT")
        mint_evidence = tuple(item for item in self.accounts if item.pubkey == self.mint)
        if len(mint_evidence) != 1 or self.current_mint_account.evidence(
                mint_evidence[0].context_slot) != mint_evidence[0]:
            raise venue.VenueStateError("CURRENT_PUMP_MINT_EVIDENCE_CONFLICT")
        venue.PumpBondingCurveStateV01.__post_init__(self)

    @property
    def current_mint_supply(self):
        return current_pump_mint(self.current_mint_account).supply

    @property
    def fee_mint_supply(self):
        return self.current_mint_supply

    def payload(self):
        return dict(venue.PumpBondingCurveStateV01.payload(self),
            current_mint_profile=current_pump_mint(self.current_mint_account).profile, current_mint_supply=self.current_mint_supply,
            fee_supply_basis="CURRENT_RAW_MINT_SUPPLY")


def build_current_pump_state(intent, curve_account, mint_account, global_account,
                             fee_account, *, slot_min, slot_max, observed_at_us,
                             account_slots=None):
    validate_current_pump_supply(curve_account, mint_account, intent.mint)
    curve = venue.decode_pump_curve(curve_account, intent.mint)
    if account_slots is None:
        if slot_min != slot_max:
            raise venue.SnapshotCoherenceError("multi-slot evidence requires per-account slots")
        account_slots = (slot_max,) * 4
    if len(account_slots) != 4 or min(account_slots) != slot_min or max(account_slots) != slot_max:
        raise venue.SnapshotCoherenceError("account slots do not match declared slot span")
    global_state = venue.decode_pump_global(global_account, account_slots[2])
    fee_config = venue.decode_fee_config(fee_account, account_slots[3], venue.derive_pump_fee_config_pda())
    accounts = tuple(raw.evidence(slot) for raw, slot in zip(
        (curve_account, mint_account, global_account, fee_account), account_slots, strict=True))
    return PumpCurrentBondingCurveStateV01(intent.intent_id, intent.mint,
        curve_account.pubkey, curve, mint_account.owner, global_state, fee_config,
        observed_at_us, slot_min, slot_max, accounts, mint_account)


def pump_state_builder(wallet_schema, mint_account, curve_account=None):
    # The explicit current Wallet contract authorizes the semantic extension.
    from .wallet_evidence_v0_1 import SCHEMA
    legacy_burn = (mint_account.owner == venue.TOKEN_PROGRAM_ID and curve_account is not None
        and venue.decode_mint_account(mint_account, mint_account.pubkey).supply
            != venue.decode_pump_curve(curve_account, mint_account.pubkey).token_total_supply)
    if (wallet_schema == SCHEMA and (legacy_burn
            or mint_account.owner == venue.TOKEN_2022_PROGRAM_ID and len(mint_account.data) > 82)):
        return build_current_pump_state
    return venue.build_pump_state
