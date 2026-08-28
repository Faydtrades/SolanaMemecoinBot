from __future__ import annotations

import ast
import hashlib
import json
import shutil
import sqlite3
import sys
import tempfile
from dataclasses import FrozenInstanceError, replace
from pathlib import Path
from typing import Any, Callable

from solders.pubkey import Pubkey


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from phase5.shadow_domain_v0_1 import (  # noqa: E402
    MODEL_FINGERPRINT as T001_FINGERPRINT,
    ExecutionIntentV01,
    IntentRole,
    IntentSide,
    ShadowDeterminismConflict,
    ShadowState,
)
from phase5.shadow_repository_v0_1 import open_shadow_repository  # noqa: E402
from phase5.shadow_venue_repository_v0_1 import open_venue_repository  # noqa: E402
from phase5.shadow_venue_route_quote_v0_1 import (  # noqa: E402
    FEE_CONFIG_DISCRIMINATOR,
    MODEL_FINGERPRINT,
    MODEL_ID,
    PUMP_CURVE_DISCRIMINATOR,
    PUMP_FEES_PROGRAM_ID,
    PUMP_GLOBAL_DISCRIMINATOR,
    PUMP_PROGRAM_ID,
    PUMPSWAP_GLOBAL_DISCRIMINATOR,
    PUMPSWAP_POOL_DISCRIMINATOR,
    PUMPSWAP_PROGRAM_ID,
    SYSTEM_PROGRAM_ID,
    TOKEN_2022_PROGRAM_ID,
    TOKEN_PROGRAM_ID,
    WSOL_MINT,
    AccountEvidenceV01,
    FeeConfigV01,
    PumpBondingCurveStateV01,
    PumpSwapStateV01,
    QuoteError,
    QuotePolicyV01,
    ReadOnlyRpcV01,
    RouteOutcome,
    RpcAccountBatchV01,
    RpcAccountV01,
    SnapshotCoherenceError,
    VenueStateError,
    build_pump_state,
    build_pumpswap_state,
    coherent_dependent_read,
    create_executable_quote,
    decode_fee_config,
    decode_pump_curve,
    decode_pumpswap_pool,
    derive_bonding_curve_pda,
    derive_pump_fee_config_pda,
    derive_pump_global_pda,
    derive_pump_pool_authority,
    derive_pumpswap_fee_config_pda,
    derive_pumpswap_global_pda,
    derive_pumpswap_pool_pda,
    derive_pumpswap_pool_pda_with_bump,
    decide_route,
)


BASE_US = 1_788_000_000_000_000
SLOT = 350_000_000
EXPECTED_T001_FINGERPRINT = "506312a81b6d8acc724cb27d6d450086bc3fc4986dcdea4831b998a3a8ec91e3"
PROTECTED_HASHES = {
    "scripts/phase4_continuous_firstpullback_multihour_run_v0_5.py":
        "22d4c0fa40e53e62fb305c75a832f6a633b496a8f10382d75353444e1392db37",
    "scripts/phase4_continuous_firstpullback_external_observability_run_v0_1.py":
        "6f517b63220ae19b2edde282aea580ae42a18794c73291115a92a4e0b850518a",
    "scripts/phase4_continuous_firstpullback_multihour_run_v0_6.py":
        "8bf11f86f333220dd8e8617021123d99566186f9ba90b9afaaa7b3fd731caf61",
    "scripts/phase4_continuous_firstpullback_extended_observability_run_v0_1.py":
        "5f7bcfa1de4216f1e939a59295ec26d93fda968632ee1cec1de95232ece595d2",
}


def check(name: str, condition: bool, checks: dict[str, bool]) -> None:
    checks[name] = bool(condition)


def expect(error: type[BaseException], callback: Callable[[], Any]) -> bool:
    try:
        callback()
    except error:
        return True
    return False


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def pk(seed: int) -> str:
    return str(Pubkey.from_bytes(bytes((seed,)) * 32))


MINT = pk(1)
CREATOR = pk(2)
COIN_CREATOR = pk(3)
LP_MINT = pk(4)
BASE_VAULT = pk(5)
QUOTE_VAULT = pk(6)
ADMIN = pk(7)


def pack_pubkey(value: str) -> bytes:
    return bytes(Pubkey.from_string(value))


def intent(side: IntentSide = IntentSide.BUY, amount: int = 100_000_000) -> ExecutionIntentV01:
    entry = side is IntentSide.BUY
    return ExecutionIntentV01(
        candidate_signal_id="p5-t002-candidate-0001",
        candidate_run_id="p5-t002-run-0001",
        strategy_evaluation_id="p5-t002-eval-0001" if entry else None,
        strategy_version="v1.1",
        parameter_set_id="EXP-0012:FINAL",
        source_run_id="offline-fixture",
        source_event_key="fixture:1" if entry else "fixture:2",
        source_ingest_seq=1 if entry else 2,
        decision_at_us=BASE_US if entry else BASE_US + 1,
        mint=MINT,
        role=IntentRole.ENTRY if entry else IntentRole.EXIT,
        side=side,
        input_asset="SOL_LAMPORTS" if entry else "MEME_BASE_UNITS",
        input_amount_base_units=amount,
        position_id=None if entry else "position-1",
        parent_entry_intent_id=None if entry else "P5EI-parent-entry-fixture",
        exit_decision_id=None if entry else "exit-decision-1",
        exit_track_id=None if entry else "FINAL-A",
        exit_lifecycle_id=None if entry else "exit-lifecycle-1",
    )


def fees_blob(lp: int, protocol: int, creator: int) -> bytes:
    return lp.to_bytes(8, "little") + protocol.to_bytes(8, "little") + creator.to_bytes(8, "little")


def fee_config_data(
    *,
    domain_program: str = PUMP_PROGRAM_ID,
    thresholds: tuple[int, ...] = (0, 40_000_000_000),
    rates: tuple[tuple[int, int, int], ...] = ((20, 100, 50), (15, 80, 40)),
) -> bytes:
    rows = b"".join(
        threshold.to_bytes(16, "little") + fees_blob(*rate)
        for threshold, rate in zip(thresholds, rates, strict=True)
    )
    return (
        FEE_CONFIG_DISCRIMINATOR
        + bytes((Pubkey.find_program_address(
            [b"fee_config", bytes(Pubkey.from_string(domain_program))],
            Pubkey.from_string(PUMP_FEES_PROGRAM_ID),
        )[1],))
        + pack_pubkey(ADMIN)
        + fees_blob(25, 5, 0)
        + len(thresholds).to_bytes(4, "little")
        + rows
        + (0).to_bytes(4, "little")
    )


def pump_global_data() -> bytes:
    return (
        PUMP_GLOBAL_DISCRIMINATOR
        + b"\x01" + pack_pubkey(ADMIN) + pack_pubkey(pk(8))
        + (1_000_000_000_000).to_bytes(8, "little")
        + (30_000_000_000).to_bytes(8, "little")
        + (700_000_000_000).to_bytes(8, "little")
        + (1_000_000_000_000).to_bytes(8, "little")
        + (100).to_bytes(8, "little")
        + pack_pubkey(pk(9)) + b"\x01"
        + (15_000_001).to_bytes(8, "little")
        + (50).to_bytes(8, "little")
    )


def pumpswap_global_data(flags: int = 0) -> bytes:
    return (
        PUMPSWAP_GLOBAL_DISCRIMINATOR + pack_pubkey(ADMIN)
        + (20).to_bytes(8, "little") + (5).to_bytes(8, "little")
        + bytes((flags,)) + b"".join(pack_pubkey(pk(10 + i)) for i in range(8))
        + (5).to_bytes(8, "little")
    )


def curve_data(
    *,
    virtual_token: int = 1_000_000_000_000,
    virtual_quote: int = 30_000_000_000,
    real_token: int = 700_000_000_000,
    real_quote: int = 20_000_000_000,
    supply: int = 1_000_000_000_000,
    complete: bool = False,
    quote_mint: str = WSOL_MINT,
) -> bytes:
    return (
        PUMP_CURVE_DISCRIMINATOR
        + b"".join(value.to_bytes(8, "little") for value in (
            virtual_token, virtual_quote, real_token, real_quote, supply
        ))
        + bytes((int(complete),)) + pack_pubkey(CREATOR) + b"\x00\x00"
        + pack_pubkey(quote_mint)
    )


def pool_data(
    *,
    index: int = 0,
    creator: str | None = None,
    base_mint: str = MINT,
    quote_mint: str = WSOL_MINT,
    virtual_quote: int = 250_000_000,
) -> bytes:
    return (
        PUMPSWAP_POOL_DISCRIMINATOR
        + bytes((derive_pumpswap_pool_pda_with_bump(MINT)[1],))
        + index.to_bytes(2, "little")
        + pack_pubkey(creator or derive_pump_pool_authority(MINT))
        + pack_pubkey(base_mint) + pack_pubkey(quote_mint) + pack_pubkey(LP_MINT)
        + pack_pubkey(BASE_VAULT) + pack_pubkey(QUOTE_VAULT)
        + (1_000_000).to_bytes(8, "little") + pack_pubkey(COIN_CREATOR)
        + b"\x00\x00" + virtual_quote.to_bytes(16, "little", signed=True)
    )


def token_account(
    pubkey: str,
    mint_value: str,
    amount: int,
    owner: str = TOKEN_PROGRAM_ID,
    authority: str | None = None,
) -> RpcAccountV01:
    data = bytearray(165)
    data[0:32] = pack_pubkey(mint_value)
    data[32:64] = pack_pubkey(authority or derive_pumpswap_pool_pda(MINT))
    data[64:72] = amount.to_bytes(8, "little")
    data[108] = 1
    return RpcAccountV01(pubkey, owner, bytes(data))


def mint_account(owner: str = TOKEN_PROGRAM_ID, supply: int = 1_000_000_000_000) -> RpcAccountV01:
    data = bytearray(82)
    data[36:44] = supply.to_bytes(8, "little")
    data[44] = 6
    data[45] = 1
    return RpcAccountV01(MINT, owner, bytes(data))


def account(pubkey: str, owner: str, data: bytes) -> RpcAccountV01:
    return RpcAccountV01(pubkey, owner, data)


def fee_account(program: str) -> RpcAccountV01:
    pda = derive_pump_fee_config_pda() if program == PUMP_PROGRAM_ID else derive_pumpswap_fee_config_pda()
    return account(pda, PUMP_FEES_PROGRAM_ID, fee_config_data(domain_program=program))


def build_curve_state(
    item: ExecutionIntentV01,
    *,
    complete: bool = False,
    real_token: int | None = None,
    token_program: str = TOKEN_PROGRAM_ID,
    observed: int = BASE_US + 10,
) -> PumpBondingCurveStateV01:
    actual_real = 0 if complete else 700_000_000_000
    if real_token is not None:
        actual_real = real_token
    return build_pump_state(
        item,
        account(derive_bonding_curve_pda(MINT), PUMP_PROGRAM_ID, curve_data(real_token=actual_real, complete=complete)),
        mint_account(token_program),
        account(derive_pump_global_pda(), PUMP_PROGRAM_ID, pump_global_data()),
        fee_account(PUMP_PROGRAM_ID),
        slot_min=SLOT,
        slot_max=SLOT,
        observed_at_us=observed,
    )


def build_pool_state(
    item: ExecutionIntentV01,
    *,
    virtual_quote: int = 250_000_000,
    token_program: str = TOKEN_PROGRAM_ID,
) -> PumpSwapStateV01:
    return build_pumpswap_state(
        item,
        account(derive_pumpswap_pool_pda(MINT), PUMPSWAP_PROGRAM_ID, pool_data(virtual_quote=virtual_quote)),
        token_account(BASE_VAULT, MINT, 500_000_000_000, token_program),
        token_account(QUOTE_VAULT, WSOL_MINT, 20_000_000_000),
        mint_account(token_program),
        account(derive_pumpswap_global_pda(), PUMPSWAP_PROGRAM_ID, pumpswap_global_data()),
        fee_account(PUMPSWAP_PROGRAM_ID),
        slot_min=SLOT,
        slot_max=SLOT + 1,
        observed_at_us=BASE_US + 20,
        account_slots=(SLOT, SLOT + 1, SLOT + 1, SLOT, SLOT, SLOT),
    )


class FakeRpc(ReadOnlyRpcV01):
    def __init__(self, batches: list[RpcAccountBatchV01]) -> None:
        self.batches = list(batches)
        self.minimums: list[int | None] = []

    def get_multiple_accounts(
        self, pubkeys: tuple[str, ...], *, min_context_slot: int | None = None
    ) -> RpcAccountBatchV01:
        self.minimums.append(min_context_slot)
        if not self.batches:
            raise AssertionError("unexpected fake RPC read")
        return self.batches.pop(0)


def batch(slot: int, data: bytes = b"primary") -> RpcAccountBatchV01:
    return RpcAccountBatchV01(slot, (account(pk(30), PUMP_PROGRAM_ID, data),))


def main() -> int:
    checks: dict[str, bool] = {}
    evidence: dict[str, Any] = {}
    buy = intent()
    sell = intent(IntentSide.SELL, 10_000_000_000)
    curve = build_curve_state(buy)
    completed = build_curve_state(buy, complete=True)
    pool = build_pool_state(buy)

    # A. Account identity, Borsh, appended state, and token-program validation.
    check("A01_VALID_PUMP_CURVE_FIXTURE", curve.executable and not curve.completed, checks)
    wrong_owner = account(derive_bonding_curve_pda(MINT), PUMPSWAP_PROGRAM_ID, curve_data())
    check("A02_WRONG_PUMP_OWNER_REJECTED", expect(VenueStateError, lambda: decode_pump_curve(wrong_owner, MINT)), checks)
    wrong_disc = account(derive_bonding_curve_pda(MINT), PUMP_PROGRAM_ID, b"12345678" + curve_data()[8:])
    check("A03_WRONG_PUMP_DISCRIMINATOR_REJECTED", expect(VenueStateError, lambda: decode_pump_curve(wrong_disc, MINT)), checks)
    truncated = account(derive_bonding_curve_pda(MINT), PUMP_PROGRAM_ID, curve_data()[:-1])
    check("A04_TRUNCATED_PUMP_ACCOUNT_REJECTED", expect(VenueStateError, lambda: decode_pump_curve(truncated, MINT)), checks)
    check("A05_VALID_COMPLETED_CURVE_EXPLICIT", completed.completed and not completed.executable, checks)
    inconsistent = account(derive_bonding_curve_pda(MINT), PUMP_PROGRAM_ID, curve_data(real_token=1, complete=True))
    check("A06_INCONSISTENT_COMPLETION_REJECTED", expect(VenueStateError, lambda: decode_pump_curve(inconsistent, MINT)), checks)
    check("A07_VALID_CANONICAL_PUMPSWAP", pool.pool == derive_pumpswap_pool_pda(MINT), checks)
    arbitrary = account(pk(31), PUMPSWAP_PROGRAM_ID, pool_data())
    check("A08_ARBITRARY_POOL_REJECTED", expect(VenueStateError, lambda: decode_pumpswap_pool(arbitrary, MINT)), checks)
    wrong_base = account(derive_pumpswap_pool_pda(MINT), PUMPSWAP_PROGRAM_ID, pool_data(base_mint=pk(32)))
    check("A09_WRONG_BASE_MINT_REJECTED", expect(VenueStateError, lambda: decode_pumpswap_pool(wrong_base, MINT)), checks)
    wrong_quote = account(derive_pumpswap_pool_pda(MINT), PUMPSWAP_PROGRAM_ID, pool_data(quote_mint=pk(33)))
    check("A10_WRONG_QUOTE_MINT_REJECTED", expect(VenueStateError, lambda: decode_pumpswap_pool(wrong_quote, MINT)), checks)
    wrong_creator = account(derive_pumpswap_pool_pda(MINT), PUMPSWAP_PROGRAM_ID, pool_data(creator=CREATOR))
    check("A11_WRONG_CREATOR_REJECTED", expect(VenueStateError, lambda: decode_pumpswap_pool(wrong_creator, MINT)), checks)
    wrong_index = account(derive_pumpswap_pool_pda(MINT), PUMPSWAP_PROGRAM_ID, pool_data(index=1))
    check("A12_WRONG_INDEX_REJECTED", expect(VenueStateError, lambda: decode_pumpswap_pool(wrong_index, MINT)), checks)
    bad_bump_bytes = bytearray(pool_data())
    bad_bump_bytes[8] = (bad_bump_bytes[8] + 1) % 256
    bad_bump = account(derive_pumpswap_pool_pda(MINT), PUMPSWAP_PROGRAM_ID, bytes(bad_bump_bytes))
    check("A12B_WRONG_POOL_BUMP_REJECTED", expect(VenueStateError, lambda: decode_pumpswap_pool(bad_bump, MINT)), checks)
    malformed_vault = token_account(BASE_VAULT, MINT, 1)
    object.__setattr__(malformed_vault, "data", malformed_vault.data[:100])
    check("A13_MALFORMED_VAULT_REJECTED", expect(VenueStateError, lambda: build_pumpswap_state(
        buy, account(derive_pumpswap_pool_pda(MINT), PUMPSWAP_PROGRAM_ID, pool_data()),
        malformed_vault, token_account(QUOTE_VAULT, WSOL_MINT, 20_000_000_000), mint_account(),
        account(derive_pumpswap_global_pda(), PUMPSWAP_PROGRAM_ID, pumpswap_global_data()),
        fee_account(PUMPSWAP_PROGRAM_ID), slot_min=SLOT, slot_max=SLOT, observed_at_us=BASE_US,
    )), checks)
    wrong_authority_vault = token_account(BASE_VAULT, MINT, 500_000_000_000, authority=pk(99))
    check("A13B_WRONG_VAULT_AUTHORITY_REJECTED", expect(VenueStateError, lambda: build_pumpswap_state(
        buy, account(derive_pumpswap_pool_pda(MINT), PUMPSWAP_PROGRAM_ID, pool_data()),
        wrong_authority_vault, token_account(QUOTE_VAULT, WSOL_MINT, 20_000_000_000), mint_account(),
        account(derive_pumpswap_global_pda(), PUMPSWAP_PROGRAM_ID, pumpswap_global_data()),
        fee_account(PUMPSWAP_PROGRAM_ID), slot_min=SLOT, slot_max=SLOT, observed_at_us=BASE_US,
    )), checks)
    negative_virtual = build_pool_state(buy, virtual_quote=-200_000_000)
    check("A14_SIGNED_I128_VIRTUAL_QUOTE_HANDLED", negative_virtual.effective_quote_reserve == 19_800_000_000, checks)
    check("A15_NEGATIVE_EFFECTIVE_QUOTE_REJECTED", expect(VenueStateError, lambda: build_pool_state(buy, virtual_quote=-20_000_000_001)), checks)
    token2022 = build_pool_state(buy, token_program=TOKEN_2022_PROGRAM_ID)
    check("A16_TOKEN_2022_BASE_MINT_ACCEPTED", token2022.base_token_program == TOKEN_2022_PROGRAM_ID, checks)
    malformed_fee = account(derive_pump_fee_config_pda(), PUMP_FEES_PROGRAM_ID, fee_config_data()[:-1])
    check("A17_MALFORMED_FEE_CONFIG_REJECTED", expect(VenueStateError, lambda: decode_fee_config(malformed_fee, SLOT, malformed_fee.pubkey)), checks)

    # B. Slot/coherence contract. Local observed time is stored but cannot alter ordering.
    coherent_rpc = FakeRpc([batch(10), batch(11, b"dep"), batch(12)])
    coherent = coherent_dependent_read(coherent_rpc, (pk(30),), lambda _: (pk(31),))
    check("B01_COHERENT_ADVANCING_CONTEXT_ACCEPTED", coherent.slot_min == 10 and coherent.slot_max == 12, checks)
    check("B02_MIN_CONTEXT_SLOT_PROPAGATED", coherent_rpc.minimums == [None, 10, 11], checks)
    regressing_rpc = FakeRpc([batch(10), batch(9), batch(10)])
    check("B03_CONTEXT_REGRESSION_REJECTED", expect(SnapshotCoherenceError, lambda: coherent_dependent_read(regressing_rpc, (pk(30),), lambda _: (pk(31),))), checks)
    retry_rpc = FakeRpc([batch(10, b"a"), batch(11), batch(12, b"b"), batch(12, b"b"), batch(13), batch(14, b"b")])
    retried = coherent_dependent_read(retry_rpc, (pk(30),), lambda _: (pk(31),))
    check("B04_ROUTE_STATE_CHANGE_RETRIED", retried.attempts == 2, checks)
    race_rpc = FakeRpc([batch(10, b"a"), batch(11), batch(12, b"b"), batch(12, b"b"), batch(13), batch(14, b"c")])
    check("B05_UNRESOLVED_RACE_FAILS_CLOSED", expect(SnapshotCoherenceError, lambda: coherent_dependent_read(race_rpc, (pk(30),), lambda _: (pk(31),), max_attempts=2)), checks)
    later_local_earlier_slot = replace(curve, observed_at_us=BASE_US + 999_999)
    check("B06_LOCAL_TIME_DOES_NOT_OVERRIDE_SLOT", later_local_earlier_slot.slot_min == curve.slot_min, checks)
    check("B07_MULTISLOT_WITHOUT_ACCOUNT_SLOTS_REJECTED", expect(SnapshotCoherenceError, lambda: build_pump_state(
        buy,
        account(derive_bonding_curve_pda(MINT), PUMP_PROGRAM_ID, curve_data()),
        mint_account(), account(derive_pump_global_pda(), PUMP_PROGRAM_ID, pump_global_data()),
        fee_account(PUMP_PROGRAM_ID), slot_min=SLOT, slot_max=SLOT + 1, observed_at_us=BASE_US,
    )), checks)
    check("B08_PER_ACCOUNT_SLOTS_PRESERVED", tuple(item.context_slot for item in pool.accounts) == (SLOT, SLOT + 1, SLOT + 1, SLOT, SLOT, SLOT), checks)
    check("B09_INCOMPLETE_EVIDENCE_SET_REJECTED", expect(VenueStateError, lambda: replace(curve, accounts=curve.accounts[:-1])), checks)
    dishonest_accounts = tuple(replace(item, context_slot=SLOT + 1) for item in curve.accounts)
    check("B10_DISHONEST_EVIDENCE_SPAN_REJECTED", expect(VenueStateError, lambda: replace(curve, accounts=dishonest_accounts)), checks)

    # C. Deterministic route table and migration gap.
    active_route = decide_route(buy, curve, None)
    pending_route = decide_route(buy, completed, None)
    swap_route = decide_route(buy, completed, pool)
    ambiguous_route = decide_route(buy, curve, pool)
    unsupported_route = decide_route(buy, None, None)
    check("C01_ACTIVE_CURVE_ROUTES_PUMP", active_route.outcome is RouteOutcome.ROUTE_PUMP_BONDING_CURVE, checks)
    check("C02_COMPLETED_ABSENT_POOL_MIGRATION_PENDING", pending_route.outcome is RouteOutcome.NO_ROUTE_MIGRATION_PENDING, checks)
    check("C03_COMPLETED_CANONICAL_POOL_ROUTES_SWAP", swap_route.outcome is RouteOutcome.ROUTE_PUMPSWAP_CANONICAL, checks)
    check("C04_CONFLICTING_EXECUTABLES_AMBIGUOUS", ambiguous_route.outcome is RouteOutcome.NO_ROUTE_AMBIGUOUS, checks)
    check("C05_UNSUPPORTED_FAILS_CLOSED", unsupported_route.outcome is RouteOutcome.NO_ROUTE_UNSUPPORTED, checks)
    different_intent_curve = build_curve_state(intent(amount=100_000_001))
    check("C06_BROKEN_LINEAGE_INVALID", decide_route(buy, different_intent_curve, None).outcome is RouteOutcome.NO_ROUTE_INVALID_STATE, checks)

    # D/E. Hand-derived immutable vectors. Constants are independent worksheet outputs.
    policy = QuotePolicyV01(100)
    pump_buy = create_executable_quote(buy, curve, active_route, policy)
    sell_curve = build_curve_state(sell)
    pump_sell = create_executable_quote(sell, sell_curve, decide_route(sell, sell_curve, None), policy)
    check("D01_PUMP_BUY_SPENDABLE_REFERENCE", pump_buy.spendable_quote_input == 98_522_166, checks)
    check("D02_PUMP_BUY_OUTPUT_REFERENCE", pump_buy.expected_base_amount == 3_273_322_372, checks)
    check("D03_PUMP_BUY_FEES_REFERENCE", (pump_buy.fees.protocol_fee, pump_buy.fees.creator_fee, pump_buy.total_quote_debit) == (985_222, 492_611, 99_999_999), checks)
    check("D04_PUMP_BUY_SLIPPAGE_FLOOR", pump_buy.minimum_output == 3_240_589_148, checks)
    check("D05_PUMP_SELL_RAW_REFERENCE", pump_sell.expected_quote_amount == 297_029_702, checks)
    check("D06_PUMP_SELL_NET_REFERENCE", pump_sell.net_quote_proceeds == 292_574_255, checks)
    check("D07_PUMP_SELL_FEES_REFERENCE", (pump_sell.fees.protocol_fee, pump_sell.fees.creator_fee) == (2_970_298, 1_485_149), checks)
    check("D08_PUMP_SELL_SLIPPAGE_FLOOR", pump_sell.minimum_output == 289_648_512, checks)
    check("D09_PUMP_QUOTE_REPLAY_DETERMINISTIC", pump_buy == create_executable_quote(buy, curve, active_route, policy), checks)
    changed_curve = build_curve_state(buy, observed=BASE_US + 11)
    changed_quote = create_executable_quote(buy, changed_curve, decide_route(buy, changed_curve, None), policy)
    check("D10_EXACT_STATE_CHANGE_CHANGES_QUOTE_ID", changed_quote.quote_id != pump_buy.quote_id, checks)
    zero_curve_data = curve_data(virtual_token=0, real_token=0, complete=False)
    check("D11_ZERO_RESERVE_FAILS_CLOSED", expect(VenueStateError, lambda: decode_pump_curve(account(derive_bonding_curve_pda(MINT), PUMP_PROGRAM_ID, zero_curve_data), MINT)), checks)
    huge_buy = intent(amount=50_000_000_000)
    thin_curve = build_curve_state(huge_buy, real_token=1)
    check("D12_REAL_RESERVE_EXHAUSTION_FAILS_CLOSED", expect(QuoteError, lambda: create_executable_quote(huge_buy, thin_curve, decide_route(huge_buy, thin_curve, None), policy)), checks)
    wrong_asset = replace(buy, input_asset="WSOL_BASE_UNITS")
    wrong_asset_curve = build_curve_state(wrong_asset)
    check("D13_BUY_ASSET_SEMANTICS_FAIL_CLOSED", expect(QuoteError, lambda: create_executable_quote(wrong_asset, wrong_asset_curve, decide_route(wrong_asset, wrong_asset_curve, None), policy)), checks)
    check("D14_MALFORMED_BUY_QUOTE_SHAPE_REJECTED", expect(QuoteError, lambda: replace(pump_buy, reserve_quote_delta=0)), checks)

    swap_buy = create_executable_quote(buy, pool, decide_route(buy, completed, pool), policy)
    sell_pool = build_pool_state(sell)
    swap_sell = create_executable_quote(sell, sell_pool, decide_route(sell, build_curve_state(sell, complete=True), sell_pool), policy)
    check("E01_SWAP_BUY_SPENDABLE_REFERENCE", swap_buy.spendable_quote_input == 98_667_981, checks)
    check("E02_SWAP_BUY_OUTPUT_REFERENCE", swap_buy.expected_base_amount == 2_424_433_385, checks)
    check("E03_SWAP_BUY_FEES_REFERENCE", (swap_buy.fees.lp_fee, swap_buy.fees.protocol_fee, swap_buy.fees.creator_fee) == (148_002, 789_344, 394_672), checks)
    check("E04_SWAP_BUY_SLIPPAGE_FLOOR", swap_buy.minimum_output == 2_400_189_051, checks)
    check("E05_SWAP_SELL_RAW_REFERENCE", swap_sell.expected_quote_amount == 397_058_823, checks)
    check("E06_SWAP_SELL_NET_REFERENCE", swap_sell.net_quote_proceeds == 391_698_527, checks)
    check("E07_SWAP_SELL_FEES_REFERENCE", (swap_sell.fees.lp_fee, swap_sell.fees.protocol_fee, swap_sell.fees.creator_fee) == (595_589, 3_176_471, 1_588_236), checks)
    check("E08_SWAP_SELL_SLIPPAGE_FLOOR", swap_sell.minimum_output == 387_781_541, checks)
    raw_only_pool = build_pool_state(buy, virtual_quote=0)
    raw_only_quote = create_executable_quote(buy, raw_only_pool, decide_route(buy, completed, raw_only_pool), policy)
    check("E09_NONZERO_VIRTUAL_RESERVE_CHANGES_OUTPUT", raw_only_quote.expected_base_amount != swap_buy.expected_base_amount, checks)
    check("E10_EFFECTIVE_QUOTE_RESERVE_EXACT", pool.effective_quote_reserve == 20_250_000_000, checks)
    check("E11_PRICE_IMPACT_INTEGER_DETERMINISTIC", type(swap_buy.price_impact_ppm) is int and swap_buy.price_impact_ppm > 0, checks)
    thin_pool = replace(pool, raw_quote_reserve=1, effective_quote_reserve=250_000_001)
    check("E12_INSUFFICIENT_REAL_LIQUIDITY_FAILS_CLOSED", expect(QuoteError, lambda: create_executable_quote(sell, replace(thin_pool, intent_id=sell.intent_id), decide_route(sell, build_curve_state(sell, complete=True), replace(thin_pool, intent_id=sell.intent_id)), policy)), checks)

    # F. Dynamic fee tiers and fingerprint binding.
    config = decode_fee_config(fee_account(PUMP_PROGRAM_ID), SLOT, derive_pump_fee_config_pda())
    check("F01_BELOW_THRESHOLD_FIRST_TIER", config.select(39_999_999_999)[0] == 0, checks)
    check("F02_EXACT_THRESHOLD_NEXT_TIER", config.select(40_000_000_000)[0] == 1, checks)
    check("F03_ABOVE_THRESHOLD_NEXT_TIER", config.select(40_000_000_001)[0] == 1, checks)
    changed_fee_account = account(derive_pump_fee_config_pda(), PUMP_FEES_PROGRAM_ID, fee_config_data(rates=((20, 101, 50), (15, 80, 40))))
    changed_config = decode_fee_config(changed_fee_account, SLOT, derive_pump_fee_config_pda())
    check("F04_FEE_STATE_CHANGE_CHANGES_FINGERPRINT", changed_config.fingerprint != config.fingerprint, checks)
    empty_fee_account = account(derive_pump_fee_config_pda(), PUMP_FEES_PROGRAM_ID, fee_config_data(thresholds=(), rates=()))
    check("F05_MISSING_TIERS_FAIL_CLOSED", expect(VenueStateError, lambda: decode_fee_config(empty_fee_account, SLOT, derive_pump_fee_config_pda())), checks)
    wrong_fee_owner = account(derive_pump_fee_config_pda(), PUMP_PROGRAM_ID, fee_config_data())
    check("F06_WRONG_FEE_OWNER_FAILS_CLOSED", expect(VenueStateError, lambda: decode_fee_config(wrong_fee_owner, SLOT, derive_pump_fee_config_pda())), checks)
    bad_fee_bump_data = bytearray(fee_config_data())
    bad_fee_bump_data[8] = (bad_fee_bump_data[8] + 1) % 256
    bad_fee_bump = account(derive_pump_fee_config_pda(), PUMP_FEES_PROGRAM_ID, bytes(bad_fee_bump_data))
    check("F07_WRONG_FEE_PDA_BUMP_FAILS_CLOSED", expect(VenueStateError, lambda: decode_fee_config(bad_fee_bump, SLOT, derive_pump_fee_config_pda())), checks)

    # G. T001-integrated append-only persistence under a temporary data/shadow path.
    root = Path(tempfile.mkdtemp(prefix="phase5_t002_"))
    try:
        database = root / "data" / "shadow" / "t002.sqlite3"
        t001 = open_shadow_repository(database)
        try:
            t001.register_intent(buy)
            t001.transition(buy.intent_id, ShadowState.ELIGIBILITY_CHECKED, idempotency_key="ELIGIBLE", effective_at_us=BASE_US + 1, reason_code="OFFLINE_ELIGIBLE", evidence={})
            t002 = open_venue_repository(database)
            try:
                check("G01_STATE_PERSISTS_AFTER_ELIGIBILITY", t002.persist_state(curve) == curve.state_id, checks)
                check("G02_STATE_EXACT_REPLAY_IDEMPOTENT", t002.persist_state(curve) == curve.state_id, checks)
                forged_state = replace(curve)
                object.__setattr__(forged_state, "observed_at_us", curve.observed_at_us + 1)
                check("G02B_SAME_ID_CONFLICT_FAILS_CLOSED", expect(ShadowDeterminismConflict, lambda: t002.persist_state(forged_state)), checks)
                check("G03_ROUTE_BEFORE_PREREQUISITE_REJECTED", expect(ShadowDeterminismConflict, lambda: t002.persist_route(active_route)), checks)
                t001.transition(buy.intent_id, ShadowState.ROUTE_BOUND, idempotency_key="ROUTE", effective_at_us=BASE_US + 2, reason_code="ROUTE_BOUND", evidence={"route_fingerprint": active_route.fingerprint})
                incomplete_swap_route = decide_route(buy, completed, pool)
                check("G03B_ALL_ROUTE_STATES_MUST_BE_PERSISTED", expect(ShadowDeterminismConflict, lambda: t002.persist_route(incomplete_swap_route)), checks)
                check("G04_ROUTE_PERSISTS_WITH_EXACT_LINEAGE", t002.persist_route(active_route) == active_route.route_id, checks)
                check("G05_QUOTE_BEFORE_PREREQUISITE_REJECTED", expect(ShadowDeterminismConflict, lambda: t002.persist_quote(pump_buy)), checks)
                t001.transition(buy.intent_id, ShadowState.QUOTE_BOUND, idempotency_key="QUOTE", effective_at_us=BASE_US + 3, reason_code="QUOTE_BOUND", evidence={"quote_fingerprint": pump_buy.fingerprint})
                check("G06_QUOTE_PERSISTS_WITH_EXACT_LINEAGE", t002.persist_quote(pump_buy) == pump_buy.quote_id, checks)
                check("G07_QUOTE_EXACT_REPLAY_IDEMPOTENT", t002.persist_quote(pump_buy) == pump_buy.quote_id, checks)

                forged_intent_fp = replace(
                    pump_buy,
                    intent_fingerprint="0" * 64,
                )
                check(
                    "G07B_FORGED_INTENT_FINGERPRINT_REJECTED",
                    expect(
                        ShadowDeterminismConflict,
                        lambda: t002.persist_quote(forged_intent_fp),
                    ),
                    checks,
                )

                forged_amount = replace(
                    pump_buy,
                    input_amount=pump_buy.input_amount + 1,
                )
                check(
                    "G07C_FORGED_INTENT_AMOUNT_REJECTED",
                    expect(
                        ShadowDeterminismConflict,
                        lambda: t002.persist_quote(forged_amount),
                    ),
                    checks,
                )

                forged_venue = replace(
                    pump_buy,
                    venue=type(pump_buy.venue).PUMPSWAP_CANONICAL,
                )
                check(
                    "G07D_FORGED_VENUE_REJECTED",
                    expect(
                        ShadowDeterminismConflict,
                        lambda: t002.persist_quote(forged_venue),
                    ),
                    checks,
                )

                forged_slot = replace(
                    pump_buy,
                    slot_max=pump_buy.slot_max + 1,
                )
                check(
                    "G07E_FORGED_SLOT_SPAN_REJECTED",
                    expect(
                        ShadowDeterminismConflict,
                        lambda: t002.persist_quote(forged_slot),
                    ),
                    checks,
                )

                forged_fee = replace(
                    pump_buy,
                    fees=replace(
                        pump_buy.fees,
                        fee_config_fingerprint="0" * 64,
                    ),
                )
                check(
                    "G07F_FORGED_FEE_CONFIG_FINGERPRINT_REJECTED",
                    expect(
                        ShadowDeterminismConflict,
                        lambda: t002.persist_quote(forged_fee),
                    ),
                    checks,
                )

                before = t002.canonical_digest()
                check("G08_FOREIGN_KEYS_ENABLED_CLEAN", t002.foreign_keys_enabled and t002.foreign_key_violations() == (), checks)
                check("G09_SQLITE_QUICK_CHECK_OK", t002.quick_check() == "ok", checks)
                check("G09B_FULL_REPOSITORY_AUDIT_OK", t002.audit(), checks)
                update_blocked = expect(sqlite3.IntegrityError, lambda: t002._conn.execute("UPDATE shadow_t002_quotes SET content_fingerprint='x' WHERE quote_id=?", (pump_buy.quote_id,)))
                t002._conn.rollback()
                delete_blocked = expect(sqlite3.IntegrityError, lambda: t002._conn.execute("DELETE FROM shadow_t002_venue_states WHERE state_id=?", (curve.state_id,)))
                t002._conn.rollback()
                check("G10_APPEND_ONLY_TRIGGERS_ENFORCED", update_blocked and delete_blocked, checks)
                check("G11_CANONICAL_JSON_RETRIEVAL", t002.get_json("quote", pump_buy.quote_id) == pump_buy.payload(), checks)
            finally:
                t002.close()
            reopened = open_venue_repository(database)
            try:
                check("G12_REOPEN_PRESERVES_EVIDENCE", reopened.get_json("state", curve.state_id) == curve.payload() and reopened.get_json("route", active_route.route_id) == active_route.payload() and reopened.get_json("quote", pump_buy.quote_id) == pump_buy.payload(), checks)
                check("G13_REOPEN_PRESERVES_DIGEST", reopened.canonical_digest() == before, checks)
                check("G13B_REOPEN_FULL_AUDIT_OK", reopened.audit(), checks)
                evidence["repository_digest"] = before
            finally:
                reopened.close()
            check("G14_STATE_MACHINE_STOPS_AT_QUOTE_BOUND", t001.current_state(buy.intent_id) is ShadowState.QUOTE_BOUND, checks)
        finally:
            t001.close()
        forbidden = PROJECT_ROOT / "data" / "paper" / "p5_t002_forbidden.sqlite3"
        check("G15_PHASE4_PATH_REJECTED_BEFORE_WRITE", expect(ValueError, lambda: open_venue_repository(forbidden)) and not forbidden.exists(), checks)
    finally:
        shutil.rmtree(root, ignore_errors=True)

    # H. Capability and regression surface.
    production_files = sorted((SRC_ROOT / "phase5").rglob("*.py"))
    source_text = "\n".join(path.read_text(encoding="utf-8") for path in production_files)
    tree = ast.parse(source_text)
    forbidden_names = {"keypair", "private_key", "send_transaction", "send_raw_transaction", "broadcast", "submit_transaction"}
    names = {node.id.lower() for node in ast.walk(tree) if isinstance(node, ast.Name)} | {node.attr.lower() for node in ast.walk(tree) if isinstance(node, ast.Attribute)}
    check("H01_NO_PROHIBITED_EXECUTION_API", not (names & forbidden_names), checks)
    check("H02_READ_ONLY_RPC_SURFACE_ONLY", set(ReadOnlyRpcV01.__dict__) >= {"get_multiple_accounts"}, checks)
    check("H03_T001_FINGERPRINT_UNCHANGED", T001_FINGERPRINT == EXPECTED_T001_FINGERPRINT, checks)
    actual_hashes = {path: sha256(PROJECT_ROOT / path) for path in PROTECTED_HASHES}
    check("H04_PROTECTED_PHASE4_HASHES_EXACT", actual_hashes == PROTECTED_HASHES, checks)
    check("H05_NO_PHASE4_IMPORT_FROM_PRODUCTION", "from phase4" not in source_text and "import phase4" not in source_text, checks)
    check("H06_IMMUTABLE_STATE_ROUTE_QUOTE", all(expect(FrozenInstanceError, lambda item=item: setattr(item, "intent_id", "x")) for item in (curve, active_route, pump_buy)), checks)
    check("H07_QUOTE_HAS_NO_BLOCKHASH", "blockhash" not in pump_buy.payload(), checks)
    check("H08_ROUTE_AND_QUOTE_NOT_IN_INTENT", not ({"route_id", "quote_id", "venue_state_id"} & set(buy.to_record())), checks)

    evidence["protected_hashes"] = actual_hashes
    evidence["pump_buy_reference"] = pump_buy.payload()
    evidence["pump_sell_reference"] = pump_sell.payload()
    evidence["swap_buy_reference"] = swap_buy.payload()
    evidence["swap_sell_reference"] = swap_sell.payload()

    print("=" * 112)
    print("PHASE-5 SHADOW VENUE + ROUTE + EXECUTABLE QUOTE v0.1 SELF-TEST")
    print("=" * 112)
    for name, passed in checks.items():
        print(f"{name}: {'PASS' if passed else 'FAIL'}")
    print(f"MODEL_ID: {MODEL_ID}")
    print(f"MODEL_FINGERPRINT: {MODEL_FINGERPRINT}")
    print(f"REPOSITORY_DIGEST: {evidence.get('repository_digest')}")
    print(f"PUMP_BUY_QUOTE_ID: {pump_buy.quote_id}")
    print(f"PUMPSWAP_BUY_QUOTE_ID: {swap_buy.quote_id}")
    print(f"CHECKS: {sum(checks.values())}/{len(checks)}")
    passed = bool(checks) and all(checks.values())
    print(f"RESULT: {'PASS' if passed else 'FAIL'}")
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
