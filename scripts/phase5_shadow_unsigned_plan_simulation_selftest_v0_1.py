from __future__ import annotations

import ast
import base64
import hashlib
import json
import shutil
import sqlite3
import struct
import sys
import tempfile
from dataclasses import FrozenInstanceError, replace
from pathlib import Path
from typing import Any, Callable

from solders.hash import Hash
from solders.pubkey import Pubkey
from solders.transaction import Transaction


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

import phase5_shadow_venue_route_quote_selftest_v0_1 as fx  # noqa: E402
from phase5.shadow_domain_v0_1 import (  # noqa: E402
    MODEL_FINGERPRINT as T001_FINGERPRINT,
    IntentSide,
    ShadowDeterminismConflict,
    ShadowState,
)
from phase5.shadow_repository_v0_1 import open_shadow_repository  # noqa: E402
from phase5.shadow_simulation_repository_v0_1 import (  # noqa: E402
    open_simulation_repository,
    record_plan_state,
    record_simulation_state,
)
from phase5.shadow_unsigned_plan_simulation_v0_1 import (  # noqa: E402
    MODEL_FINGERPRINT,
    MODEL_ID,
    PUMP_BUY_ACCOUNTS,
    PUMP_BUY_CONTRACT_FINGERPRINT,
    PUMP_BUY_EXACT_QUOTE_IN_V2_DISCRIMINATOR,
    PUMP_SELL_ACCOUNTS,
    PUMP_SELL_CONTRACT_FINGERPRINT,
    PUMP_SELL_V2_DISCRIMINATOR,
    PUMPSWAP_BUY_ACCOUNTS,
    PUMPSWAP_BUY_CONTRACT_FINGERPRINT,
    PUMPSWAP_BUY_EXACT_QUOTE_IN_DISCRIMINATOR,
    PUMPSWAP_SELL_ACCOUNTS,
    PUMPSWAP_SELL_CONTRACT_FINGERPRINT,
    PUMPSWAP_SELL_DISCRIMINATOR,
    ActorAccountSnapshotV01,
    BlockHeightResponseV01,
    BlockhashLeaseV01,
    BlockhashValidityResponseV01,
    LatestBlockhashResponseV01,
    PlanError,
    PublicShadowActorV01,
    SimulationEvidenceError,
    SimulationOutcome,
    SimulationRequestConfigV01,
    SimulationRpcResponseV01,
    TransactionPlanPolicyV01,
    UnsignedTransactionPlanV01,
    VerifiedRecipientEvidenceV01,
    build_actor_account_snapshot,
    build_unsigned_transaction_plan,
    decode_verified_recipient_evidence,
    derive_associated_token_address,
    derive_pumpswap_pool_v2,
    derive_user_volume_accumulator,
    materialize_simulation_envelope,
    run_bounded_simulation,
    validate_pumpswap_remaining_account_layout,
)
from phase5.shadow_venue_repository_v0_1 import open_venue_repository  # noqa: E402
from phase5.shadow_venue_route_quote_v0_1 import (  # noqa: E402
    MODEL_FINGERPRINT as T002_FINGERPRINT,
    PUMP_PROGRAM_ID,
    PUMPSWAP_PROGRAM_ID,
    SYSTEM_PROGRAM_ID,
    TOKEN_2022_PROGRAM_ID,
    TOKEN_PROGRAM_ID,
    WSOL_MINT,
    QuoteError,
    QuotePolicyV01,
    RpcAccountV01,
    build_pump_state,
    build_pumpswap_state,
    create_executable_quote,
    decide_route,
    derive_bonding_curve_pda,
    derive_pump_global_pda,
    derive_pumpswap_global_pda,
    derive_pumpswap_pool_pda,
)


BASE_US = 1_788_100_000_000_000
ACTOR = fx.pk(70)
EXPECTED_T001 = "506312a81b6d8acc724cb27d6d450086bc3fc4986dcdea4831b998a3a8ec91e3"
EXPECTED_T002 = "3b0219e1b30bceb7c77bde4674ada8069a1cbf8aaf8669844dc03b09c53d59d0"
OFFICIAL_PUMP_IDL_SHA256 = "b90bc471327f671449271d5d1d42354d1fae6f5a06502f5834459a3108138e49"
OFFICIAL_PUMPSWAP_IDL_SHA256 = "6b5c7ec4e5ef9742fa99dc57b0d75b1031b379bba02a7e1b3c5a4cad68d77e56"
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


def pump_global_full() -> bytes:
    normal = tuple(fx.pk(80 + index) for index in range(8))
    reserved = tuple(fx.pk(90 + index) for index in range(8))
    buyback = tuple(fx.pk(100 + index) for index in range(8))
    return (
        fx.PUMP_GLOBAL_DISCRIMINATOR + b"\x01" + fx.pack_pubkey(fx.ADMIN)
        + fx.pack_pubkey(normal[0])
        + b"".join(value.to_bytes(8, "little") for value in (
            1_000_000_000_000, 30_000_000_000, 700_000_000_000,
            1_000_000_000_000, 100,
        ))
        + fx.pack_pubkey(fx.pk(78)) + b"\x01"
        + (15_000_001).to_bytes(8, "little") + (50).to_bytes(8, "little")
        + b"".join(fx.pack_pubkey(value) for value in normal[1:])
        + fx.pack_pubkey(fx.pk(108)) + fx.pack_pubkey(fx.pk(109)) + b"\x01"
        + fx.pack_pubkey(fx.pk(110)) + fx.pack_pubkey(reserved[0]) + b"\x01"
        + b"".join(fx.pack_pubkey(value) for value in reserved[1:]) + b"\x01"
        + b"".join(fx.pack_pubkey(value) for value in buyback)
    )


def pumpswap_global_full() -> bytes:
    normal = tuple(fx.pk(112 + index) for index in range(8))
    reserved = tuple(fx.pk(120 + index) for index in range(8))
    buyback = tuple(fx.pk(128 + index) for index in range(8))
    return (
        fx.PUMPSWAP_GLOBAL_DISCRIMINATOR + fx.pack_pubkey(fx.ADMIN)
        + (20).to_bytes(8, "little") + (5).to_bytes(8, "little") + b"\x00"
        + b"".join(fx.pack_pubkey(value) for value in normal)
        + (5).to_bytes(8, "little")
        + fx.pack_pubkey(fx.pk(136)) + fx.pack_pubkey(fx.pk(137))
        + fx.pack_pubkey(reserved[0]) + b"\x00"
        + b"".join(fx.pack_pubkey(value) for value in reserved[1:]) + b"\x01"
        + b"".join(fx.pack_pubkey(value) for value in buyback)
    )


def curve_bytes(*, complete: bool = False, mayhem: bool = False, cashback: bool = False) -> bytes:
    data = bytearray(fx.curve_data(complete=complete, real_token=0 if complete else 700_000_000_000))
    data[-34] = int(mayhem)
    data[-33] = int(cashback)
    return bytes(data)


def pool_bytes(
    *,
    cashback: bool = False,
    mayhem: bool = False,
    coin_creator: str = fx.COIN_CREATOR,
) -> bytes:
    data = bytearray(fx.pool_data())
    coin_creator_offset = 8 + 1 + 2 + 6 * 32 + 8
    data[coin_creator_offset:coin_creator_offset + 32] = fx.pack_pubkey(coin_creator)
    data[-18] = int(mayhem)
    data[-17] = int(cashback)
    return bytes(data)


def make_pump_state(item: Any, *, token_program: str = TOKEN_PROGRAM_ID, complete: bool = False) -> tuple[Any, RpcAccountV01]:
    global_account = fx.account(derive_pump_global_pda(), PUMP_PROGRAM_ID, pump_global_full())
    state = build_pump_state(
        item,
        fx.account(derive_bonding_curve_pda(fx.MINT), PUMP_PROGRAM_ID, curve_bytes(complete=complete)),
        fx.mint_account(token_program), global_account, fx.fee_account(PUMP_PROGRAM_ID),
        slot_min=fx.SLOT, slot_max=fx.SLOT, observed_at_us=BASE_US,
    )
    return state, global_account


def make_swap_state(
    item: Any,
    *,
    token_program: str = TOKEN_PROGRAM_ID,
    cashback: bool = False,
    coin_creator: str = fx.COIN_CREATOR,
) -> tuple[Any, RpcAccountV01]:
    global_account = fx.account(derive_pumpswap_global_pda(), PUMPSWAP_PROGRAM_ID, pumpswap_global_full())
    state = build_pumpswap_state(
        item,
        fx.account(
            derive_pumpswap_pool_pda(fx.MINT), PUMPSWAP_PROGRAM_ID,
            pool_bytes(cashback=cashback, coin_creator=coin_creator),
        ),
        fx.token_account(fx.BASE_VAULT, fx.MINT, 500_000_000_000, token_program),
        fx.token_account(fx.QUOTE_VAULT, WSOL_MINT, 20_000_000_000),
        fx.mint_account(token_program), global_account, fx.fee_account(PUMPSWAP_PROGRAM_ID),
        slot_min=fx.SLOT, slot_max=fx.SLOT + 1, observed_at_us=BASE_US,
        account_slots=(fx.SLOT, fx.SLOT + 1, fx.SLOT + 1, fx.SLOT, fx.SLOT, fx.SLOT),
    )
    return state, global_account


def actor_token(pubkey: str, mint: str, amount: int, token_program: str) -> RpcAccountV01:
    return fx.token_account(pubkey, mint, amount, token_program, authority=ACTOR)


def make_plan(
    venue: str,
    side: IntentSide,
    *,
    token_program: str = TOKEN_PROGRAM_ID,
    cashback: bool = False,
    quote_exists: bool = False,
    coin_creator: str = fx.COIN_CREATOR,
) -> tuple[Any, ...]:
    item = fx.intent(side, 100_000_000 if side is IntentSide.BUY else 10_000_000_000)
    actor = PublicShadowActorV01(ACTOR)
    policy = TransactionPlanPolicyV01()
    if venue == "pump":
        state, global_account = make_pump_state(item, token_program=token_program)
        route = decide_route(item, state, None)
    else:
        state, global_account = make_swap_state(
            item, token_program=token_program, cashback=cashback,
            coin_creator=coin_creator,
        )
        completed, _ = make_pump_state(item, token_program=token_program, complete=True)
        route = decide_route(item, completed, state)
    quote = create_executable_quote(item, state, route, QuotePolicyV01(100))
    recipients = decode_verified_recipient_evidence(state, global_account)
    base_key = derive_associated_token_address(ACTOR, fx.MINT, token_program)
    quote_key = derive_associated_token_address(ACTOR, WSOL_MINT, TOKEN_PROGRAM_ID)
    base = None if side is IntentSide.BUY else actor_token(base_key, fx.MINT, item.input_amount_base_units, token_program)
    quote_account = (
        actor_token(quote_key, WSOL_MINT, 123_456, TOKEN_PROGRAM_ID)
        if quote_exists else None
    )
    snapshot = build_actor_account_snapshot(
        state, actor, base_account=base, quote_account=quote_account,
        context_slot=state.slot_max, observed_at_us=BASE_US + 1,
    )
    plan = build_unsigned_transaction_plan(
        item, state, route, quote, actor, policy, recipients, snapshot
    )
    return item, state, route, quote, actor, policy, recipients, snapshot, plan


def blockhash(seed: int) -> str:
    return str(Hash.from_bytes(bytes((seed,)) * 32))


class FakeSimulationRpc:
    def __init__(
        self,
        latest: list[LatestBlockhashResponseV01],
        heights: list[BlockHeightResponseV01],
        validity: list[BlockhashValidityResponseV01],
        responses: list[SimulationRpcResponseV01 | BaseException],
    ) -> None:
        self.latest = list(latest)
        self.heights = list(heights)
        self.validity = list(validity)
        self.responses = list(responses)
        self.calls: list[tuple[str, Any]] = []

    def get_latest_blockhash(self, *, commitment: str, min_context_slot: int) -> LatestBlockhashResponseV01:
        self.calls.append(("get_latest_blockhash", (commitment, min_context_slot)))
        return self.latest.pop(0)

    def get_block_height(self, *, commitment: str, min_context_slot: int) -> BlockHeightResponseV01:
        self.calls.append(("get_block_height", (commitment, min_context_slot)))
        return self.heights.pop(0)

    def is_blockhash_valid(self, value: str, *, commitment: str, min_context_slot: int) -> BlockhashValidityResponseV01:
        self.calls.append(("is_blockhash_valid", (value, commitment, min_context_slot)))
        return self.validity.pop(0)

    def simulate_transaction(self, transaction_base64: str, *, config: dict[str, Any]) -> SimulationRpcResponseV01:
        self.calls.append(("simulate_transaction", (transaction_base64, dict(config))))
        value = self.responses.pop(0)
        if isinstance(value, BaseException):
            raise value
        return value


def fake_rpc(plan: UnsignedTransactionPlanV01, value: dict[str, Any] | BaseException, *, context_delta: int = 2) -> FakeSimulationRpc:
    slot = plan.prerequisite_slot + 1
    response = value if isinstance(value, BaseException) else SimulationRpcResponseV01(slot + context_delta, value)
    return FakeSimulationRpc(
        [LatestBlockhashResponseV01(slot, blockhash(1), 100)],
        [BlockHeightResponseV01(100)],
        [BlockhashValidityResponseV01(slot, True)],
        [response],
    )


def main() -> int:
    checks: dict[str, bool] = {}
    evidence: dict[str, Any] = {}
    pump_buy = make_plan("pump", IntentSide.BUY)
    pump_sell = make_plan("pump", IntentSide.SELL)
    swap_buy = make_plan("swap", IntentSide.BUY, token_program=TOKEN_2022_PROGRAM_ID, cashback=True)
    swap_buy_existing_quote = make_plan(
        "swap", IntentSide.BUY, token_program=TOKEN_2022_PROGRAM_ID,
        cashback=True, quote_exists=True,
    )
    swap_sell = make_plan("swap", IntentSide.SELL)
    swap_sell_existing_quote = make_plan("swap", IntentSide.SELL, quote_exists=True)
    swap_buy_populated_creator = make_plan("swap", IntentSide.BUY)
    swap_buy_default_creator = make_plan(
        "swap", IntentSide.BUY, coin_creator=SYSTEM_PROGRAM_ID
    )
    swap_sell_default_creator = make_plan(
        "swap", IntentSide.SELL, coin_creator=SYSTEM_PROGRAM_ID
    )
    swap_cashback_buy_default_creator = make_plan(
        "swap", IntentSide.BUY, cashback=True, coin_creator=SYSTEM_PROGRAM_ID
    )
    swap_cashback_sell_populated_creator = make_plan(
        "swap", IntentSide.SELL, cashback=True
    )
    swap_cashback_sell_default_creator = make_plan(
        "swap", IntentSide.SELL, cashback=True, coin_creator=SYSTEM_PROGRAM_ID
    )
    plans = [pump_buy[-1], pump_sell[-1], swap_buy[-1], swap_sell[-1]]

    # A. Stable plans and verified construction boundaries.
    check("A01_PLAN_REPLAY_DETERMINISTIC", pump_buy[-1] == make_plan("pump", IntentSide.BUY)[-1], checks)
    changed_actor = PublicShadowActorV01(fx.pk(71))
    pb = pump_buy
    changed_snapshot = build_actor_account_snapshot(
        pb[1], changed_actor, base_account=None, quote_account=None,
        context_slot=pb[1].slot_max, observed_at_us=BASE_US + 1,
    )
    changed_plan = build_unsigned_transaction_plan(
        pb[0], pb[1], pb[2], pb[3], changed_actor, pb[5], pb[6], changed_snapshot
    )
    check("A02_ACTOR_CHANGE_CHANGES_PLAN", changed_plan.plan_id != pb[-1].plan_id, checks)
    changed_quote = create_executable_quote(pb[0], pb[1], pb[2], QuotePolicyV01(200))
    changed_quote_plan = build_unsigned_transaction_plan(
        pb[0], pb[1], pb[2], changed_quote, pb[4], pb[5], pb[6], pb[7]
    )
    check("A03_QUOTE_CHANGE_CHANGES_PLAN", changed_quote_plan.plan_id != pb[-1].plan_id, checks)
    check("A04_ROUTE_STATE_MISMATCH_REJECTED", expect(PlanError, lambda: build_unsigned_transaction_plan(
        pb[0], pb[1], swap_buy[2], pb[3], pb[4], pb[5], pb[6], pb[7]
    )), checks)
    forged_quote = replace(pb[3], route_fingerprint="0" * 64)
    check("A05_FORGED_QUOTE_LINEAGE_REJECTED", expect(PlanError, lambda: build_unsigned_transaction_plan(
        pb[0], pb[1], pb[2], forged_quote, pb[4], pb[5], pb[6], pb[7]
    )), checks)
    plan_text = json.dumps(pb[-1].payload(), sort_keys=True).lower()
    check("A06_PLAN_HAS_NO_BLOCKHASH", "blockhash" not in plan_text, checks)
    check("A07_PLAN_HAS_NO_SIGNATURE", "signature" not in plan_text, checks)
    check("A08_PLAN_HAS_NO_MUTABLE_TRANSACTION", not any(isinstance(value, Transaction) for value in pb[-1].__dict__.values()) if hasattr(pb[-1], "__dict__") else True, checks)
    check("A09_PLAN_FROZEN", expect(FrozenInstanceError, lambda: setattr(pb[-1], "prerequisite_slot", 1)), checks)
    check("A10_RECIPIENT_FACTORY_GUARDED", expect((TypeError, PlanError), lambda: VerifiedRecipientEvidenceV01(
        pb[1].fingerprint, pb[6].global_account, pb[6].normal_fee_recipients,
        pb[6].reserved_fee_recipients, pb[6].buyback_fee_recipients, object()
    )), checks)
    check("A11_ACCOUNT_SNAPSHOT_FACTORY_GUARDED", expect((TypeError, PlanError), lambda: ActorAccountSnapshotV01(
        ACTOR, fx.MINT, pb[1].fingerprint, pb[1].slot_max, BASE_US,
        TOKEN_PROGRAM_ID, TOKEN_PROGRAM_ID, None, None, object()
    )), checks)
    check("A12_PLAN_FACTORY_GUARDED", expect((TypeError, PlanError), lambda: replace(pb[-1], _factory_token=object())), checks)
    tampered_global = RpcAccountV01(pb[6].global_account.pubkey, PUMP_PROGRAM_ID, pump_global_full() + b"x")
    check("A13_T002_GLOBAL_HASH_MISMATCH_REJECTED", expect(PlanError, lambda: decode_verified_recipient_evidence(pb[1], tampered_global)), checks)
    check("A14_EMBEDDED_EVIDENCE_FINGERPRINTS_EXACT", all(
        hashlib.sha256(value.encode()).hexdigest() != "" for value in (
            pb[-1].actor_payload_json, pb[-1].policy_payload_json,
            pb[-1].account_snapshot_payload_json, pb[-1].recipient_evidence_payload_json,
        )
    ), checks)

    # B. Four official instruction paths and static IDL-derived reference vectors.
    pump_buy_ix = next(item for item in pump_buy[-1].instructions if item.name == "pump_buy_exact_quote_in_v2")
    pump_sell_ix = next(item for item in pump_sell[-1].instructions if item.name == "pump_sell_v2")
    swap_buy_ix = next(item for item in swap_buy[-1].instructions if item.name == "pumpswap_buy_exact_quote_in")
    swap_sell_ix = next(item for item in swap_sell[-1].instructions if item.name == "pumpswap_sell")
    check("B01_PUMP_BUY_PROGRAM", pump_buy_ix.program_id == PUMP_PROGRAM_ID, checks)
    check("B02_PUMP_BUY_EXACT_ARGS", pump_buy_ix.data_hex == "c2ab1c46684d5b2f00e1f505000000005c7727c100000000", checks)
    check("B03_PUMP_BUY_OFFICIAL_META_COUNT_FLAGS", len(pump_buy_ix.accounts) == 27 and tuple((x.writable, x.authority_required) for x in pump_buy_ix.accounts) == tuple((w, a) for _, w, a in PUMP_BUY_ACCOUNTS), checks)
    check("B04_PUMP_BUY_CONTRACT_FINGERPRINT", pump_buy_ix.contract_fingerprint == "925990a04e2e2d3c82a260557b7c8f24a98f9804e7de6671b7b417addcb7c1c8", checks)
    check("B05_PUMP_SELL_PROGRAM", pump_sell_ix.program_id == PUMP_PROGRAM_ID, checks)
    check("B06_PUMP_SELL_EXACT_ARGS", pump_sell_ix.data_hex == "5df6823ce7e940b200e40b540200000080af431100000000", checks)
    check("B07_PUMP_SELL_OFFICIAL_META_COUNT_FLAGS", len(pump_sell_ix.accounts) == 26 and tuple((x.writable, x.authority_required) for x in pump_sell_ix.accounts) == tuple((w, a) for _, w, a in PUMP_SELL_ACCOUNTS), checks)
    check("B08_PUMP_SELL_CONTRACT_FINGERPRINT", pump_sell_ix.contract_fingerprint == "affe70abf949247b888febaa456bf25822b36d90348b506192806a07dac195c4", checks)
    check("B09_SWAP_BUY_EXACT_QUOTE_IN_NAME_PROGRAM", swap_buy_ix.name == "pumpswap_buy_exact_quote_in" and swap_buy_ix.program_id == PUMPSWAP_PROGRAM_ID and all(item.name != "pumpswap_buy" for item in swap_buy[-1].instructions), checks)
    check("B10_SWAP_BUY_EXACT_QUOTE_IN_BYTES", swap_buy_ix.data_hex == "c62e1552b4d9e87000e1f505000000007bfa0f8f0000000001", checks)
    swap_buy_data = bytes.fromhex(swap_buy_ix.data_hex)
    check("B10A_SWAP_BUY_DISCRIMINATOR_EXACT", swap_buy_data[:8] == PUMPSWAP_BUY_EXACT_QUOTE_IN_DISCRIMINATOR == bytes((198, 46, 21, 82, 180, 217, 232, 112)), checks)
    check("B10B_SWAP_BUY_INPUT_BUDGET_FROM_T002", struct.unpack("<Q", swap_buy_data[8:16])[0] == swap_buy[3].input_amount, checks)
    check("B10C_SWAP_BUY_MINIMUM_FROM_T002", struct.unpack("<Q", swap_buy_data[16:24])[0] == swap_buy[3].minimum_output, checks)
    check("B10D_SWAP_BUY_NOT_MINIMUM_ONLY_EXACT_OUTPUT", struct.unpack("<Q", swap_buy_data[8:16])[0] != swap_buy[3].minimum_output and swap_buy[3].spendable_quote_input < swap_buy[3].input_amount, checks)
    check("B11_SWAP_BUY_BASE_FLAGS_PLUS_REMAINING", tuple((x.writable, x.authority_required) for x in swap_buy_ix.accounts[:23]) == tuple((w, a) for _, w, a in PUMPSWAP_BUY_ACCOUNTS) and len(swap_buy_ix.accounts) == 27, checks)
    check("B12_SWAP_BUY_CONTRACT_FINGERPRINT", swap_buy_ix.contract_fingerprint == "c1ee7da925bb29dcecf8bcfa2985bf7c65083e5b5ca0c7743ec1f944bc26da43", checks)
    check("B13_SWAP_SELL_PROGRAM", swap_sell_ix.program_id == PUMPSWAP_PROGRAM_ID, checks)
    check("B14_SWAP_SELL_EXACT_ARGS", swap_sell_ix.data_hex == "33e685a4017f83ad00e40b5402000000a5131d1700000000", checks)
    check("B15_SWAP_SELL_BASE_FLAGS_PLUS_REMAINING", tuple((x.writable, x.authority_required) for x in swap_sell_ix.accounts[:21]) == tuple((w, a) for _, w, a in PUMPSWAP_SELL_ACCOUNTS) and len(swap_sell_ix.accounts) == 24, checks)
    check("B16_SWAP_SELL_CONTRACT_FINGERPRINT", swap_sell_ix.contract_fingerprint == "33f9086933c5945be1fc739375f71c8598bb47c178089d0a19411fb99c230cf4", checks)
    check("B17_TOKEN_2022_BASE_PROPAGATED", swap_buy[-1].base_token_program == TOKEN_2022_PROGRAM_ID and any(meta.pubkey == TOKEN_2022_PROGRAM_ID for meta in swap_buy_ix.accounts), checks)
    check("B18_LEGACY_BASE_PROPAGATED", pump_buy[-1].base_token_program == TOKEN_PROGRAM_ID, checks)
    check("B19_SDK_COMPAT_SETUP_ORDER", tuple(item.name for item in swap_buy[-1].instructions) == (
        "create_associated_token_account_idempotent", "create_associated_token_account_idempotent",
        "system_transfer", "sync_native", "pumpswap_buy_exact_quote_in", "close_native_account",
    ), checks)
    check("B20_SELL_REQUIRES_BASE_ACCOUNT", expect(PlanError, lambda: build_unsigned_transaction_plan(
        pump_sell[0], pump_sell[1], pump_sell[2], pump_sell[3], pump_sell[4], pump_sell[5],
        pump_sell[6], build_actor_account_snapshot(pump_sell[1], pump_sell[4], base_account=None,
        quote_account=None, context_slot=pump_sell[1].slot_max, observed_at_us=BASE_US)
    )), checks)
    check("B21_RECIPIENT_SELECTION_DETERMINISTIC", pump_buy_ix.accounts[6].pubkey == make_plan("pump", IntentSide.BUY)[-1].instructions[-1].accounts[6].pubkey, checks)
    check("B22_OFFICIAL_IDL_HASHES_PINNED", len(OFFICIAL_PUMP_IDL_SHA256) == 64 and len(OFFICIAL_PUMPSWAP_IDL_SHA256) == 64, checks)
    check("B23_PUMPSWAP_ECONOMICS_NOT_RECOMPUTED", struct.unpack("<Q", swap_buy_data[8:16])[0] == swap_buy[3].input_amount and struct.unpack("<Q", swap_buy_data[16:24])[0] == swap_buy[3].minimum_output, checks)
    check("B24_NONCANONICAL_INSTRUCTION_HEX_REJECTED", expect(PlanError, lambda: replace(pump_buy_ix, data_hex=pump_buy_ix.data_hex.upper())), checks)
    malformed_swap_quote = replace(
        swap_buy[3], spendable_quote_input=swap_buy[3].spendable_quote_input + 1
    )
    check("B25_MALFORMED_SWAP_BUY_QUOTE_SHAPE_FAILS_CLOSED", expect(PlanError, lambda: build_unsigned_transaction_plan(
        swap_buy[0], swap_buy[1], swap_buy[2], malformed_swap_quote,
        swap_buy[4], swap_buy[5], swap_buy[6], swap_buy[7],
    )), checks)
    quote_ata = derive_associated_token_address(ACTOR, WSOL_MINT, TOKEN_PROGRAM_ID)
    def creates_quote_ata(plan: UnsignedTransactionPlanV01) -> bool:
        return any(
            item.name == "create_associated_token_account_idempotent"
            and item.accounts[1].pubkey == quote_ata for item in plan.instructions
        )
    def closes_quote_ata(plan: UnsignedTransactionPlanV01) -> bool:
        return any(
            item.name == "close_native_account" and item.accounts[0].pubkey == quote_ata
            for item in plan.instructions
        )
    check("B26_BUY_ABSENT_WSOL_CREATE_TRANSFER_SYNC_BUY_CLOSE", creates_quote_ata(swap_buy[-1]) and closes_quote_ata(swap_buy[-1]) and tuple(item.name for item in swap_buy[-1].instructions[-5:]) == (
        "create_associated_token_account_idempotent", "system_transfer", "sync_native",
        "pumpswap_buy_exact_quote_in", "close_native_account",
    ), checks)
    buy_transfers = tuple(
        next(item for item in plan.instructions if item.name == "system_transfer")
        for plan in (swap_buy[-1], swap_buy_existing_quote[-1])
    )
    check("B26A_BUY_NATIVE_TRANSFER_USES_EXACT_INPUT_BUDGET", all(
        struct.unpack("<IQ", bytes.fromhex(item.data_hex)) == (2, swap_buy[3].input_amount)
        for item in buy_transfers
    ), checks)
    check("B27_BUY_PREEXISTING_WSOL_NEVER_CREATED_OR_CLOSED", not creates_quote_ata(swap_buy_existing_quote[-1]) and not closes_quote_ata(swap_buy_existing_quote[-1]) and tuple(item.name for item in swap_buy_existing_quote[-1].instructions[-3:]) == (
        "system_transfer", "sync_native", "pumpswap_buy_exact_quote_in",
    ), checks)
    check("B28_SELL_ABSENT_WSOL_CREATE_SELL_CLOSE", creates_quote_ata(swap_sell[-1]) and closes_quote_ata(swap_sell[-1]) and tuple(item.name for item in swap_sell[-1].instructions) == (
        "create_associated_token_account_idempotent", "pumpswap_sell", "close_native_account",
    ), checks)
    check("B29_SELL_PREEXISTING_WSOL_NEVER_CREATED_OR_CLOSED", not creates_quote_ata(swap_sell_existing_quote[-1]) and not closes_quote_ata(swap_sell_existing_quote[-1]) and tuple(item.name for item in swap_sell_existing_quote[-1].instructions) == ("pumpswap_sell",), checks)
    check("B30_WSOL_SNAPSHOT_LINEAGE_CHANGES_PLAN_ID", swap_buy[7].quote_exists is False and swap_buy_existing_quote[7].quote_exists is True and swap_buy[-1].plan_id != swap_buy_existing_quote[-1].plan_id and swap_sell[-1].plan_id != swap_sell_existing_quote[-1].plan_id, checks)

    def pumpswap_ix(bundle: tuple[Any, ...]) -> Any:
        name = (
            "pumpswap_buy_exact_quote_in"
            if bundle[0].side is IntentSide.BUY else "pumpswap_sell"
        )
        return next(item for item in bundle[-1].instructions if item.name == name)

    pool_v2 = derive_pumpswap_pool_v2(fx.MINT)
    reference_pool_v2 = str(Pubkey.find_program_address(
        [b"pool-v2", bytes(Pubkey.from_string(fx.MINT))],
        Pubkey.from_string(PUMPSWAP_PROGRAM_ID),
    )[0])
    noncash_buy_populated_ix = pumpswap_ix(swap_buy_populated_creator)
    noncash_buy_default_ix = pumpswap_ix(swap_buy_default_creator)
    noncash_sell_default_ix = pumpswap_ix(swap_sell_default_creator)
    cashback_buy_default_ix = pumpswap_ix(swap_cashback_buy_default_creator)
    cashback_sell_populated_ix = pumpswap_ix(swap_cashback_sell_populated_creator)
    cashback_sell_default_ix = pumpswap_ix(swap_cashback_sell_default_creator)
    user_volume = derive_user_volume_accumulator(PUMPSWAP_PROGRAM_ID, ACTOR)
    cashback_quote_ata = derive_associated_token_address(
        user_volume, WSOL_MINT, TOKEN_PROGRAM_ID
    )
    check("B31_NONCASHBACK_BUY_POPULATED_POOL_V2_EXACT_ORDER", len(noncash_buy_populated_ix.accounts) == 26 and tuple(
        item.pubkey for item in noncash_buy_populated_ix.accounts[23:]
    ) == (pool_v2, noncash_buy_populated_ix.accounts[-2].pubkey,
          noncash_buy_populated_ix.accounts[-1].pubkey) and sum(
        item.pubkey == pool_v2 for item in noncash_buy_populated_ix.accounts
    ) == 1, checks)
    check("B32_NONCASHBACK_BUY_DEFAULT_POOL_V2_REQUIRED_EXACT_COUNT", len(noncash_buy_default_ix.accounts) == 26 and tuple(
        item.pubkey for item in noncash_buy_default_ix.accounts[23:]
    ) == (pool_v2, noncash_buy_default_ix.accounts[-2].pubkey,
          noncash_buy_default_ix.accounts[-1].pubkey) and sum(
        item.pubkey == pool_v2 for item in noncash_buy_default_ix.accounts
    ) == 1, checks)
    check("B33_NONCASHBACK_SELL_DEFAULT_POOL_V2_REQUIRED_EXACT_COUNT", len(noncash_sell_default_ix.accounts) == 24 and tuple(
        item.pubkey for item in noncash_sell_default_ix.accounts[21:]
    ) == (pool_v2, noncash_sell_default_ix.accounts[-2].pubkey,
          noncash_sell_default_ix.accounts[-1].pubkey) and sum(
        item.pubkey == pool_v2 for item in noncash_sell_default_ix.accounts
    ) == 1, checks)
    check("B34_CASHBACK_BUY_DEFAULT_EXACT_REMAINING_ORDER", len(cashback_buy_default_ix.accounts) == 27 and tuple(
        item.pubkey for item in cashback_buy_default_ix.accounts[23:]
    ) == (cashback_quote_ata, pool_v2, cashback_buy_default_ix.accounts[-2].pubkey,
          cashback_buy_default_ix.accounts[-1].pubkey), checks)
    check("B35_CASHBACK_SELL_DEFAULT_EXACT_REMAINING_ORDER", len(cashback_sell_default_ix.accounts) == 26 and tuple(
        item.pubkey for item in cashback_sell_default_ix.accounts[21:]
    ) == (cashback_quote_ata, user_volume, pool_v2,
          cashback_sell_default_ix.accounts[-2].pubkey,
          cashback_sell_default_ix.accounts[-1].pubkey), checks)
    check("B36_POOL_V2_PDA_EXACT_SEEDS_AND_PROGRAM", pool_v2 == reference_pool_v2, checks)
    pool_v2_metas = tuple(
        item for instruction in (
            noncash_buy_populated_ix, noncash_buy_default_ix,
            swap_sell_ix, noncash_sell_default_ix,
            swap_buy_ix, cashback_buy_default_ix, cashback_sell_populated_ix,
            cashback_sell_default_ix,
        ) for item in instruction.accounts if item.pubkey == pool_v2
    )
    check("B37_POOL_V2_READONLY_NON_AUTHORITY_ALL_LAYOUTS", len(pool_v2_metas) == 8 and all(
        not item.writable and not item.authority_required for item in pool_v2_metas
    ), checks)
    check("B38_COIN_CREATOR_DOES_NOT_CONTROL_POOL_V2_PRESENCE", all(
        sum(item.pubkey == pool_v2 for item in instruction.accounts) == 1
        for instruction in (
            noncash_buy_populated_ix, noncash_buy_default_ix,
            swap_sell_ix, noncash_sell_default_ix,
            swap_buy_ix, cashback_buy_default_ix,
            cashback_sell_populated_ix, cashback_sell_default_ix,
        )
    ), checks)
    omitted_pool_v2 = (
        noncash_buy_default_ix.accounts[:23] + noncash_buy_default_ix.accounts[24:]
    )
    duplicate_pool_v2 = (
        noncash_buy_default_ix.accounts[:24]
        + (noncash_buy_default_ix.accounts[23],)
        + noncash_buy_default_ix.accounts[24:]
    )
    layout_args = {
        "side": IntentSide.BUY,
        "cashback": False,
        "actor_public_key": ACTOR,
        "mint": fx.MINT,
        "buyback_fee_recipient": noncash_buy_default_ix.accounts[-2].pubkey,
    }
    check("B39_OMITTED_POOL_V2_LAYOUT_REJECTED", expect(
        PlanError,
        lambda: validate_pumpswap_remaining_account_layout(
            accounts=omitted_pool_v2, **layout_args
        ),
    ), checks)
    check("B40_DUPLICATE_POOL_V2_LAYOUT_REJECTED", expect(
        PlanError,
        lambda: validate_pumpswap_remaining_account_layout(
            accounts=duplicate_pool_v2, **layout_args
        ),
    ), checks)
    misflagged_pool_v2 = list(noncash_buy_default_ix.accounts)
    misflagged_pool_v2[23] = replace(misflagged_pool_v2[23], writable=True)
    check("B41_MISFLAGGED_POOL_V2_LAYOUT_REJECTED", expect(
        PlanError,
        lambda: validate_pumpswap_remaining_account_layout(
            accounts=misflagged_pool_v2, **layout_args
        ),
    ), checks)
    reordered_pool_v2 = (
        noncash_buy_default_ix.accounts[:23]
        + (noncash_buy_default_ix.accounts[24], noncash_buy_default_ix.accounts[23])
        + noncash_buy_default_ix.accounts[25:]
    )
    check("B42_REORDERED_POOL_V2_LAYOUT_REJECTED", expect(
        PlanError,
        lambda: validate_pumpswap_remaining_account_layout(
            accounts=reordered_pool_v2, **layout_args
        ),
    ), checks)

    # C. Zero-placeholder serialization and exact request policy.
    lease1 = BlockhashLeaseV01(pump_buy[-1].plan_id, blockhash(1), pump_buy[-1].prerequisite_slot + 1, 100, "confirmed", BASE_US)
    config1 = SimulationRequestConfigV01(lease1.context_slot)
    envelope1 = materialize_simulation_envelope(pump_buy[-1], lease1, config1)
    envelope1b = materialize_simulation_envelope(pump_buy[-1], lease1, config1)
    parsed = Transaction.from_bytes(base64.b64decode(envelope1.transaction_base64))
    check("C01_ENVELOPE_DETERMINISTIC", envelope1 == envelope1b, checks)
    check("C02_ZERO_PLACEHOLDERS_ONLY", len(parsed.signatures) == envelope1.required_authority_count and all(bytes(value) == b"\x00" * 64 for value in parsed.signatures), checks)
    check("C03_REQUIRED_AUTHORITY_COUNT_EXACT", envelope1.required_authority_count == parsed.message.header.num_required_signatures == 1, checks)
    check("C04_MESSAGE_INSTRUCTION_COUNT_EXACT", len(parsed.message.instructions) == len(pump_buy[-1].instructions), checks)
    lease2 = BlockhashLeaseV01(pump_buy[-1].plan_id, blockhash(2), pump_buy[-1].prerequisite_slot + 1, 100, "confirmed", BASE_US)
    envelope2 = materialize_simulation_envelope(pump_buy[-1], lease2, SimulationRequestConfigV01(lease2.context_slot))
    check("C05_BLOCKHASH_CHANGES_ENVELOPE", envelope2.envelope_id != envelope1.envelope_id and envelope2.wire_sha256 != envelope1.wire_sha256, checks)
    check("C06_BLOCKHASH_DOES_NOT_CHANGE_PLAN", lease1.plan_id == lease2.plan_id == pump_buy[-1].plan_id, checks)
    expected_config = {"encoding": "base64", "sigVerify": False, "replaceRecentBlockhash": False, "commitment": "confirmed", "minContextSlot": lease1.context_slot, "innerInstructions": True}
    check("C07_REQUEST_CONFIG_EXACT", config1.rpc_payload() == expected_config, checks)
    check("C08_SIG_VERIFY_TRUE_REJECTED", expect(SimulationEvidenceError, lambda: SimulationRequestConfigV01(lease1.context_slot, sig_verify=True)), checks)
    check("C09_BLOCKHASH_REPLACEMENT_REJECTED", expect(SimulationEvidenceError, lambda: SimulationRequestConfigV01(lease1.context_slot, replace_recent_blockhash=True)), checks)
    check("C10_WRONG_MIN_CONTEXT_REJECTED", expect(SimulationEvidenceError, lambda: materialize_simulation_envelope(pump_buy[-1], lease1, SimulationRequestConfigV01(lease1.context_slot + 1))), checks)
    check("C11_NONCANONICAL_WIRE_ENCODING_REJECTED", expect(SimulationEvidenceError, lambda: replace(envelope1, transaction_base64=envelope1.transaction_base64 + "\n")), checks)

    # D. Lease boundary, bounded refresh, response classification, and preservation.
    success_value = {"err": None, "logs": ["Program log: ok"], "unitsConsumed": 321, "returnData": {"programId": PUMP_PROGRAM_ID, "data": ["AA==", "base64"]}, "innerInstructions": [], "loadedAccountsDataSize": 99, "accounts": None}
    success_rpc = fake_rpc(pump_buy[-1], success_value)
    success_run = run_bounded_simulation(success_rpc, pump_buy[-1], observed_at_us=BASE_US)
    result = success_run.final_result
    check("D01_SUCCESS_CLASSIFIED", result.outcome is SimulationOutcome.SIMULATION_SUCCESS, checks)
    check("D02_SUCCESS_EVIDENCE_EXACT", result.logs == ("Program log: ok",) and result.units_consumed == 321 and result.loaded_accounts_data_size == 99 and json.loads(result.return_data_json)["programId"] == PUMP_PROGRAM_ID, checks)
    causal_minimums = (
        success_rpc.calls[0][1][1], success_rpc.calls[1][1][1], success_rpc.calls[2][1][2]
    )
    check("D03_MIN_CONTEXT_PROPAGATED", all(value >= pump_buy[-1].prerequisite_slot for value in causal_minimums), checks)
    check("D04_SIM_REQUEST_EXACT", success_rpc.calls[-1][0] == "simulate_transaction" and success_rpc.calls[-1][1][1] == expected_config, checks)
    rejected = run_bounded_simulation(fake_rpc(pump_buy[-1], {"err": {"InstructionError": [0, "Custom"]}, "logs": []}), pump_buy[-1], observed_at_us=BASE_US).final_result
    check("D05_PROGRAM_REJECTION_DISTINCT", rejected.outcome is SimulationOutcome.PROGRAM_REJECTED, checks)
    failed = run_bounded_simulation(fake_rpc(pump_buy[-1], RuntimeError("offline fixture")), pump_buy[-1], observed_at_us=BASE_US).final_result
    check("D06_RPC_FAILURE_DISTINCT", failed.outcome is SimulationOutcome.RPC_FAILURE, checks)
    malformed = run_bounded_simulation(fake_rpc(pump_buy[-1], {"err": None, "logs": "bad"}), pump_buy[-1], observed_at_us=BASE_US).final_result
    check("D07_MALFORMED_DISTINCT", malformed.outcome is SimulationOutcome.MALFORMED_RESPONSE, checks)
    malformed_json = run_bounded_simulation(
        fake_rpc(pump_buy[-1], {"err": None, "logs": [], "returnData": b"not-json"}),
        pump_buy[-1], observed_at_us=BASE_US,
    ).final_result
    check("D07B_RETURNED_MALFORMED_CONTEXT_PRESERVED", malformed_json.outcome is SimulationOutcome.MALFORMED_RESPONSE and malformed_json.context_slot is not None, checks)
    regressed = run_bounded_simulation(fake_rpc(pump_buy[-1], {"err": None, "logs": []}, context_delta=-2), pump_buy[-1], observed_at_us=BASE_US).final_result
    check("D08_CONTEXT_REGRESSION_DISTINCT", regressed.outcome is SimulationOutcome.CONTEXT_REGRESSION, checks)
    slot = pump_buy[-1].prerequisite_slot + 1
    boundary_rpc = FakeSimulationRpc(
        [LatestBlockhashResponseV01(slot, blockhash(3), 100)], [BlockHeightResponseV01(100)],
        [BlockhashValidityResponseV01(slot, True)], [SimulationRpcResponseV01(slot, {"err": None, "logs": []})],
    )
    boundary = run_bounded_simulation(boundary_rpc, pump_buy[-1], observed_at_us=BASE_US)
    check("D09_LAST_VALID_HEIGHT_BOUNDARY_ACCEPTED", boundary.final_result.outcome is SimulationOutcome.SIMULATION_SUCCESS, checks)
    refresh_rpc = FakeSimulationRpc(
        [LatestBlockhashResponseV01(slot, blockhash(4), 100), LatestBlockhashResponseV01(slot + 1, blockhash(5), 200)],
        [BlockHeightResponseV01(101), BlockHeightResponseV01(150)],
        [BlockhashValidityResponseV01(slot, True), BlockhashValidityResponseV01(slot + 1, True)],
        [SimulationRpcResponseV01(slot + 1, {"err": None, "logs": []})],
    )
    refreshed = run_bounded_simulation(refresh_rpc, pump_buy[-1], observed_at_us=BASE_US)
    check("D10_ONE_REFRESH_NEW_LEASE_ATTEMPT", len(refreshed.leases) == 2 and refreshed.leases[0].lease_id != refreshed.leases[1].lease_id and refreshed.attempts[0].attempt_id != refreshed.attempts[1].attempt_id, checks)
    check("D11_REFRESH_PRESERVES_PLAN", all(item.plan_id == pump_buy[-1].plan_id for item in refreshed.leases + refreshed.attempts), checks)
    check("D12_HISTORICAL_EXPIRY_PRESERVED", refreshed.results[0].outcome is SimulationOutcome.BLOCKHASH_EXPIRED and refreshed.results[1].outcome is SimulationOutcome.SIMULATION_SUCCESS, checks)
    twice_rpc = FakeSimulationRpc(
        [LatestBlockhashResponseV01(slot, blockhash(6), 100), LatestBlockhashResponseV01(slot + 1, blockhash(7), 200)],
        [BlockHeightResponseV01(101), BlockHeightResponseV01(200)],
        [BlockhashValidityResponseV01(slot, True), BlockhashValidityResponseV01(slot + 1, False)], [],
    )
    twice = run_bounded_simulation(twice_rpc, pump_buy[-1], observed_at_us=BASE_US)
    check("D13_SECOND_EXPIRY_FAILS_CLOSED", len(twice.attempts) == 2 and twice.final_result.outcome is SimulationOutcome.BLOCKHASH_EXPIRED, checks)
    failed_refresh_rpc = FakeSimulationRpc(
        [LatestBlockhashResponseV01(slot, blockhash(8), 100), LatestBlockhashResponseV01(pump_buy[-1].prerequisite_slot - 1, blockhash(9), 200)],
        [BlockHeightResponseV01(101)], [BlockhashValidityResponseV01(slot, True)], [],
    )
    check("D13B_REFRESH_ACQUISITION_FAILURE_NOT_FALSE_EXPIRY", expect(SimulationEvidenceError, lambda: run_bounded_simulation(failed_refresh_rpc, pump_buy[-1], observed_at_us=BASE_US)), checks)
    check("D14_REFRESH_BOUND_IS_FIXED", expect(SimulationEvidenceError, lambda: run_bounded_simulation(boundary_rpc, pump_buy[-1], observed_at_us=BASE_US, maximum_refreshes=2)), checks)
    bad_latest = FakeSimulationRpc([LatestBlockhashResponseV01(pump_buy[-1].prerequisite_slot - 1, blockhash(8), 100)], [], [], [])
    check("D15_BLOCKHASH_CONTEXT_REGRESSION_REJECTED", expect(SimulationEvidenceError, lambda: run_bounded_simulation(bad_latest, pump_buy[-1], observed_at_us=BASE_US)), checks)
    check("D16_FAKE_RPC_SURFACE_NARROW", {name for name in dir(FakeSimulationRpc) if not name.startswith("_")} == {"get_latest_blockhash", "get_block_height", "is_blockhash_valid", "simulate_transaction"}, checks)

    # E. Same-owner append-only persistence and T001 lifecycle mapping.
    shadow_root = PROJECT_ROOT / "data" / "shadow"
    shadow_root.mkdir(parents=True, exist_ok=True)
    root = Path(tempfile.mkdtemp(prefix="phase5_t003_", dir=shadow_root))
    database = root / "t003.sqlite3"
    try:
        item, state, route, quote, _, _, _, _, plan = pump_buy
        shadow = open_shadow_repository(database)
        shadow.register_intent(item)
        shadow.transition(item.intent_id, ShadowState.ELIGIBILITY_CHECKED, idempotency_key="E", effective_at_us=BASE_US + 1, reason_code="OFFLINE", evidence={})
        venue_repo = open_venue_repository(database)
        venue_repo.persist_state(state)
        shadow.transition(item.intent_id, ShadowState.ROUTE_BOUND, idempotency_key="R", effective_at_us=BASE_US + 2, reason_code="ROUTE", evidence={})
        venue_repo.persist_route(route)
        simulation_repo = open_simulation_repository(database)
        check("E01_PLAN_REQUIRES_QUOTE_BOUND", expect(ShadowDeterminismConflict, lambda: simulation_repo.persist_plan(plan)), checks)
        shadow.transition(item.intent_id, ShadowState.QUOTE_BOUND, idempotency_key="Q", effective_at_us=BASE_US + 3, reason_code="QUOTE", evidence={})
        venue_repo.persist_quote(quote)
        check("E02_PLAN_STATE_RECORDED", record_plan_state(shadow, simulation_repo, plan, effective_at_us=BASE_US + 4) == plan.plan_id and shadow.current_state(item.intent_id) is ShadowState.PLAN_BUILT, checks)
        check("E03_PLAN_REPLAY_IDEMPOTENT", simulation_repo.persist_plan(plan) == plan.plan_id, checks)
        simulation_repo.persist_attempt_evidence(success_run.leases[0], success_run.validity[0], success_run.envelopes[0], success_run.attempts[0])
        conflicting_attempt = replace(success_run.attempts[0], observed_at_us=success_run.attempts[0].observed_at_us + 1)
        check("E04_ATTEMPT_INDEX_CONFLICT_REJECTED", expect(ShadowDeterminismConflict, lambda: simulation_repo.persist_attempt_evidence(success_run.leases[0], success_run.validity[0], success_run.envelopes[0], conflicting_attempt)), checks)
        simulation_repo.persist_result(success_run.results[0])
        conflicting_result = replace(success_run.results[0], reason_code="CONFLICT")
        check("E05_ATTEMPT_RESULT_CONFLICT_REJECTED", expect(ShadowDeterminismConflict, lambda: simulation_repo.persist_result(conflicting_result)), checks)
        final_state = record_simulation_state(shadow, simulation_repo, plan, success_run, effective_at_us=BASE_US + 5)
        check("E06_SUCCESS_REACHES_COMPLETED", final_state is ShadowState.COMPLETED and shadow.current_state(item.intent_id) is ShadowState.COMPLETED, checks)
        check("E07_EXACT_RUN_REPLAY_IDEMPOTENT", simulation_repo.persist_run(success_run) == (success_run.results[0].result_id,), checks)
        check("E08_FOREIGN_KEYS_ENABLED_CLEAN", simulation_repo.foreign_keys_enabled and not simulation_repo._conn.execute("PRAGMA foreign_key_check").fetchall(), checks)
        check("E09_QUICK_CHECK_OK", simulation_repo.quick_check() == "ok", checks)
        check("E10_FULL_AUDIT_OK", simulation_repo.audit(), checks)
        digest = simulation_repo.canonical_digest()
        check("E11_CANONICAL_PLAN_JSON", simulation_repo.get_json("plan", plan.plan_id) == plan.payload(), checks)
        check("E12_APPEND_ONLY_TRIGGER", expect(sqlite3.IntegrityError, lambda: simulation_repo._conn.execute("UPDATE shadow_t003_plans SET plan_json=plan_json WHERE plan_id=?", (plan.plan_id,))), checks)
        simulation_repo.close()
        reopened = open_simulation_repository(database)
        check("E13_REOPEN_EXACT", reopened.get_json("result", result.result_id) == result.payload(), checks)
        check("E14_REOPEN_DIGEST_STABLE", reopened.canonical_digest() == digest, checks)
        check("E15_REOPEN_AUDIT_OK", reopened.audit(), checks)
        evidence["repository_digest"] = digest
        reopened.close()
        venue_repo.close()
        shadow.close()
    finally:
        shutil.rmtree(root)

    # F. Structural firewall and protected-baseline regression evidence.
    production_files = [
        PROJECT_ROOT / "src" / "phase5" / "shadow_unsigned_plan_simulation_v0_1.py",
        PROJECT_ROOT / "src" / "phase5" / "shadow_simulation_repository_v0_1.py",
    ]
    trees = [ast.parse(path.read_text(encoding="utf-8")) for path in production_files]
    names = {node.id.lower() for tree in trees for node in ast.walk(tree) if isinstance(node, ast.Name)}
    attrs = {node.attr.lower() for tree in trees for node in ast.walk(tree) if isinstance(node, ast.Attribute)}
    text = "\n".join(path.read_text(encoding="utf-8").lower() for path in production_files)
    prohibited = ("key" + "pair", "null" + "signer", "send" + "transaction", "sendraw" + "transaction", "broad" + "cast", "live_enabled")
    check("F01_NO_PRIVATE_CAPABILITY_IDENTIFIERS", not any(value in names | attrs for value in prohibited), checks)
    check("F02_NO_SUBMISSION_TEXT", not any(value in text.replace("_", "") for value in prohibited), checks)
    check("F03_NO_GENERIC_RPC_ESCAPE_HATCH", "method_name" not in text and "rpc_call" not in text, checks)
    check("F04_PUBLIC_ACTOR_FIELDS_ONLY", tuple(PublicShadowActorV01.__dataclass_fields__) == ("public_key", "schema_version", "fingerprint"), checks)
    check("F05_ALL_PLACEHOLDER_BYTES_PROVEN_ZERO", all(bytes(sig) == b"\x00" * 64 for sig in parsed.signatures), checks)
    check("F06_T001_FINGERPRINT_EXACT", T001_FINGERPRINT == EXPECTED_T001, checks)
    check("F07_T002_FINGERPRINT_EXACT", T002_FINGERPRINT == EXPECTED_T002, checks)
    check("F08_PROTECTED_PHASE4_HASHES_EXACT", all(sha256(PROJECT_ROOT / path) == expected for path, expected in PROTECTED_HASHES.items()), checks)
    check("F09_NO_PHASE4_IMPORTS", all("phase4" not in path.read_text(encoding="utf-8") for path in production_files), checks)
    check("F10_MODEL_ID_EXACT", MODEL_ID == "P5-SHADOW-UNSIGNED-PLAN-SIMULATION-0001", checks)
    check("F11_CORRECTED_MODEL_FINGERPRINT_EXACT", MODEL_FINGERPRINT == "a7b619bb3e85a6a2310969437080c125c821ffaeb8ae8abd40de9b08cca84a2a", checks)

    passed = sum(checks.values())
    print("=" * 112)
    print("PHASE-5 SHADOW UNSIGNED PLAN + NON-BROADCAST SIMULATION v0.1 SELF-TEST")
    print("=" * 112)
    for name, ok in checks.items():
        print(f"{name}: {'PASS' if ok else 'FAIL'}")
    print(f"MODEL_ID: {MODEL_ID}")
    print(f"MODEL_FINGERPRINT: {MODEL_FINGERPRINT}")
    print(f"PUMP_BUY_CONTRACT_FINGERPRINT: {PUMP_BUY_CONTRACT_FINGERPRINT}")
    print(f"PUMP_SELL_CONTRACT_FINGERPRINT: {PUMP_SELL_CONTRACT_FINGERPRINT}")
    print(f"PUMPSWAP_BUY_CONTRACT_FINGERPRINT: {PUMPSWAP_BUY_CONTRACT_FINGERPRINT}")
    print(f"PUMPSWAP_SELL_CONTRACT_FINGERPRINT: {PUMPSWAP_SELL_CONTRACT_FINGERPRINT}")
    print(f"OFFICIAL_PUMP_IDL_SHA256: {OFFICIAL_PUMP_IDL_SHA256}")
    print(f"OFFICIAL_PUMPSWAP_IDL_SHA256: {OFFICIAL_PUMPSWAP_IDL_SHA256}")
    print(f"REPOSITORY_DIGEST: {evidence.get('repository_digest', 'UNAVAILABLE')}")
    print(f"CHECKS: {passed}/{len(checks)}")
    print(f"RESULT: {'PASS' if passed == len(checks) else 'FAIL'}")
    return 0 if passed == len(checks) else 1


if __name__ == "__main__":
    raise SystemExit(main())
