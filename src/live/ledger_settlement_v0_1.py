"""Pure, whole-transaction attribution for the concrete LIVE venue shapes.

Transaction metadata and closed instruction flows supply deltas. Original
Wallet Evidence supplies support/ownership, never a replacement fill balance.
Proposals still require L5 atomic custody validation and application.
"""
from __future__ import annotations

import struct
from dataclasses import asdict, dataclass, field
from itertools import combinations

from solders.instruction import AccountMeta, Instruction
from solders.pubkey import Pubkey
from solders.compute_budget import ID as COMPUTE_BUDGET_ID
from solders.system_program import decode_create_account, decode_transfer
from spl.token.instructions import decode_transfer as decode_token_transfer, decode_transfer_checked, decode_initialize_account3
from phase5.shadow_domain_v0_1 import IntentSide, content_fingerprint
from phase5.shadow_venue_route_quote_v0_1 import (
    PUMP_PROGRAM_ID, PUMPSWAP_PROGRAM_ID, PUMP_FEES_PROGRAM_ID, SYSTEM_PROGRAM_ID,
    TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID, WSOL_MINT, derive_bonding_curve_pda,
    derive_pump_global_pda, derive_pumpswap_global_pda, derive_pumpswap_pool_pda,
    derive_pump_fee_config_pda, derive_pumpswap_fee_config_pda,
)
from phase5.shadow_unsigned_plan_simulation_v0_1 import (
    ASSOCIATED_TOKEN_PROGRAM_ID, PUMP_BUY_ACCOUNTS, PUMP_SELL_ACCOUNTS,
    PUMPSWAP_BUY_ACCOUNTS, PUMPSWAP_SELL_ACCOUNTS,
    PUMP_BUY_EXACT_QUOTE_IN_V2_DISCRIMINATOR, PUMP_SELL_V2_DISCRIMINATOR,
    PUMPSWAP_BUY_EXACT_QUOTE_IN_DISCRIMINATOR, PUMPSWAP_SELL_DISCRIMINATOR,
    PlanAccountMetaV01, derive_associated_token_address, derive_event_authority, derive_sharing_config,
    derive_global_volume_accumulator, derive_user_volume_accumulator, derive_pump_creator_vault, validate_pumpswap_remaining_account_layout,
)
from .ledger_actions_v0_1 import PendingAction, StoredAttempt
from .ledger_domain_v0_1 import LedgerDomain, ledger_utc
from .ledger_finality_v0_1 import LedgerChainReceipt
from .public_rpc_v0_1 import u64
from .transaction_evidence_v0_1 import TransactionObservation, ledger_transaction_evidence, _known_finalized_anchors_consistent
from .wallet_evidence_v0_1 import WalletObservation, ledger_account_evidence

VERSION = "live_ledger_settlement_v0.1"
# Narrow read-only CPI envelopes reviewed from primary sources by CHIEF.
# https://github.com/coral-xyz/anchor/blob/v0.31.1/lang/src/event.rs
# https://github.com/coral-xyz/anchor/blob/v0.31.1/lang/attribute/event/src/lib.rs
ANCHOR_EVENT_TAG = bytes.fromhex("e445a52e51cb9a1d")
# pump-fun/pump-public-docs, revision 9c82f61cb711b044a17f770ab8ce9f9bdf78f333,
# idl/pump_fees.json SHA256 ea2fb5dae0252375513ff8072a111affe83a0fd4db7a2e8840b3b39bace5ec83.
GET_FEES_TAG = bytes.fromhex("e7257e55cf5b3f34")
PUMP_TRADE_EVENT_TAG = bytes.fromhex("bddb7fd34ee661ee")


def _pump_trade_event(data):
    """Pinned Pump IDL TradeEvent only; values are cross-checks, never fills.

    pump.json at the same accepted revision as get_fees; SHA256
    b90bc471327f671449271d5d1d42354d1fae6f5a06502f5834459a3108138e49.
    No other event schema,
    shareholder distribution or historical-layout guessing is supported.
    """
    _require(data[:16] == ANCHOR_EVENT_TAG+PUMP_TRADE_EVENT_TAG and len(data) <= 2048,
             "PUMP_TRADE_EVENT_SCHEMA_UNSUPPORTED")
    offset = 16
    def take(size):
        nonlocal offset
        _require(offset+size <= len(data), "PUMP_TRADE_EVENT_TRUNCATED")
        value = data[offset:offset+size]
        offset += size
        return value
    def number():
        return int.from_bytes(take(8), "little")
    def key():
        return str(Pubkey.from_bytes(take(32)))
    def boolean():
        value = take(1)[0]
        _require(value in (0, 1), "PUMP_TRADE_EVENT_BOOL_INVALID")
        return bool(value)
    mint, sol_amount, token_amount, is_buy, user = key(), number(), number(), boolean(), key()
    take(8)  # timestamp i64; not a replacement observation/evaluation clock
    for _ in range(4):
        number()  # opaque reserve provenance, never modeled inventory
    fee_recipient, fee_basis_points, fee, creator, creator_fee_basis_points, creator_fee = key(), number(), number(), key(), number(), number()
    boolean()  # track_volume
    for _ in range(3):
        number()
    take(8)  # last_update_timestamp i64
    name_length = int.from_bytes(take(4), "little")
    _require(name_length <= 64, "PUMP_TRADE_EVENT_NAME_BOUND")
    name = take(name_length).decode("ascii")
    mayhem = boolean()
    cashback_basis_points, cashback, buyback_basis_points, buyback_fee = number(), number(), number(), number()
    shareholder_count = int.from_bytes(take(4), "little")
    _require(not mayhem and cashback == cashback_basis_points == shareholder_count == 0,
             "PUMP_TRADE_EVENT_DISTRIBUTION_PROFILE_UNSUPPORTED")
    quote_mint, quote_amount = key(), number()
    number()
    number()
    _require(offset == len(data), "PUMP_TRADE_EVENT_TRAILING_BYTES_UNSUPPORTED")
    return {"mint": mint, "sol_amount": sol_amount, "token_amount": token_amount, "is_buy": is_buy, "user": user,
        "fee_recipient": fee_recipient, "fee": fee, "creator": creator, "creator_fee": creator_fee,
        "buyback_fee": buyback_fee, "ix_name": name, "quote_mint": quote_mint, "quote_amount": quote_amount}


@dataclass(frozen=True, slots=True)
class WalletSupportInput:
    observation: WalletObservation
    evaluated_at_utc: str
    required_min_context_slot: int

    def __post_init__(self):
        if type(self.observation) is not WalletObservation:
            raise ValueError("ORIGINAL_WALLET_OBSERVATION_REQUIRED")
        object.__setattr__(self, "evaluated_at_utc", ledger_utc(self.evaluated_at_utc))
        u64(self.required_min_context_slot)

    @property
    def digest(self):
        return content_fingerprint({"observation": self.observation.content_digest,
            "evaluated_at_utc": self.evaluated_at_utc, "required_min_context_slot": self.required_min_context_slot})


@dataclass(frozen=True, slots=True)
class SettlementComponent:
    component_id: str
    kind: str
    asset: str
    account: str
    units: int
    counterparty: str | None = None


@dataclass(frozen=True, slots=True)
class SettlementProposal:
    proposal_id: str
    economic_domain_id: str
    root_id: str
    action_id: str
    attempt_id: str
    signature: str
    basis_digest: str
    transaction_evidence_digest: str
    wallet_support_digest: str
    native_wallet_delta: int
    base_units_delta: int
    wsol_units_delta: int
    transaction_fee_lamports: int
    quote_asset: str
    quote_principal_units: int
    venue_fee_units: int
    account_funding_lamports: int
    account_refund_lamports: int
    locked_account_lamports_delta: int
    components: tuple[SettlementComponent, ...]
    requires_atomic_custody_validation: bool = field(init=False, default=True)
    applied: bool = field(init=False, default=False)
    version: str = field(init=False, default=VERSION)

    @property
    def content_digest(self):
        return content_fingerprint(asdict(self))


@dataclass(frozen=True, slots=True)
class SettlementResult:
    disposition: str
    reasons: tuple[str, ...]
    proposal: SettlementProposal | None


class _Unapplied(ValueError):
    pass


class _Quarantined(ValueError):
    pass


def _require(condition, reason, *, contradiction=False):
    if not condition:
        raise (_Quarantined if contradiction else _Unapplied)(reason)


def _writable(tx, index):
    required, readonly_signed, readonly_unsigned = tx.header
    static = len(tx.static_accounts)
    return (index < required-readonly_signed or required <= index < static-readonly_unsigned
            or static <= index < static+len(tx.loaded_writable))


def _instruction(tx, fact):
    return Instruction(Pubkey.from_string(tx.account_keys[fact.program_id_index]), fact.data,
        [AccountMeta(Pubkey.from_string(tx.account_keys[i]), i < tx.header[0], _writable(tx, i)) for i in fact.account_indexes])


def _roles(tx, action, wallet):
    selected = [ix for ix in tx.outer_instructions if tx.account_keys[ix.program_id_index] in (PUMP_PROGRAM_ID, PUMPSWAP_PROGRAM_ID)]
    _require(len(selected) == 1, "EXACTLY_ONE_SUPPORTED_VENUE_INSTRUCTION_REQUIRED")
    ix = selected[0]
    program = tx.account_keys[ix.program_id_index]
    pump, buy = program == PUMP_PROGRAM_ID, action.side == "BUY"
    schema = (PUMP_BUY_ACCOUNTS if buy else PUMP_SELL_ACCOUNTS) if pump else (PUMPSWAP_BUY_ACCOUNTS if buy else PUMPSWAP_SELL_ACCOUNTS)
    tag = (PUMP_BUY_EXACT_QUOTE_IN_V2_DISCRIMINATOR if buy else PUMP_SELL_V2_DISCRIMINATOR) if pump else (
        PUMPSWAP_BUY_EXACT_QUOTE_IN_DISCRIMINATOR if buy else PUMPSWAP_SELL_DISCRIMINATOR)
    _require(len(ix.data) == (25 if not pump and buy else 24) and ix.data[:8] == tag,
             "VENUE_INSTRUCTION_SIDE_OR_ENCODING_UNSUPPORTED")
    amount, minimum = struct.unpack("<QQ", ix.data[8:24])
    _require(amount == action.input_units and amount > 0, "ACTION_INPUT_AND_ACTUAL_MESSAGE_CONFLICT", contradiction=True)
    if not pump and buy:
        _require(ix.data[24] in (0, 1), "VENUE_VOLUME_FLAG_INVALID")
    _require(len(ix.account_indexes) >= len(schema), "VENUE_ACCOUNT_LAYOUT_INCOMPLETE")
    roles = {name: tx.account_keys[index] for (name, _, _), index in zip(schema, ix.account_indexes)}
    for (_name, writable, signer), index in zip(schema, ix.account_indexes):
        _require(not writable or _writable(tx, index), "VENUE_WRITABLE_ACCOUNT_CONFLICT", contradiction=True)
        _require(not signer or index == 0, "VENUE_AUTHORITY_CONFLICT", contradiction=True)
    expected = {"user": wallet, "base_mint": action.mint, "quote_mint": WSOL_MINT,
        "base_token_program": action.token_program, "quote_token_program": TOKEN_PROGRAM_ID,
        "associated_token_program": ASSOCIATED_TOKEN_PROGRAM_ID, "system_program": SYSTEM_PROGRAM_ID,
        "fee_program": PUMP_FEES_PROGRAM_ID, "program": program, "event_authority": derive_event_authority(program),
        "fee_config": derive_pump_fee_config_pda() if pump else derive_pumpswap_fee_config_pda()}
    base = derive_associated_token_address(wallet, action.mint, action.token_program)
    quote = derive_associated_token_address(wallet, WSOL_MINT, TOKEN_PROGRAM_ID)
    user_volume = derive_user_volume_accumulator(program, wallet)
    if pump:
        _require(len(ix.account_indexes) == len(schema), "PUMP_REMAINING_ACCOUNTS_UNSUPPORTED")
        curve = derive_bonding_curve_pda(action.mint)
        expected.update(global_=derive_pump_global_pda())
        expected["global"] = expected.pop("global_")
        expected.update(bonding_curve=curve, associated_base_user=base, associated_quote_user=quote,
            associated_base_bonding_curve=derive_associated_token_address(curve, action.mint, action.token_program),
            associated_quote_bonding_curve=derive_associated_token_address(curve, WSOL_MINT, TOKEN_PROGRAM_ID),
            sharing_config=derive_sharing_config(action.mint), user_volume_accumulator=user_volume,
            associated_user_volume_accumulator=derive_associated_token_address(user_volume, WSOL_MINT, TOKEN_PROGRAM_ID))
        if buy:
            expected["global_volume_accumulator"] = derive_global_volume_accumulator(program)
        for prefix, owner in (("associated_quote_fee_recipient", "fee_recipient"),
                              ("associated_quote_buyback_fee_recipient", "buyback_fee_recipient"),
                              ("associated_creator_vault", "creator_vault")):
            expected[prefix] = derive_associated_token_address(roles[owner], WSOL_MINT, TOKEN_PROGRAM_ID)
        pool_base, pool_quote = roles["associated_base_bonding_curve"], roles["associated_quote_bonding_curve"]
        fee_native = (roles["fee_recipient"], roles["buyback_fee_recipient"], roles["creator_vault"])
        quote_owners = {roles["associated_quote_fee_recipient"]: roles["fee_recipient"],
            roles["associated_quote_buyback_fee_recipient"]: roles["buyback_fee_recipient"],
            roles["associated_creator_vault"]: roles["creator_vault"], roles["associated_user_volume_accumulator"]: user_volume}
        pool = curve
    else:
        pool = derive_pumpswap_pool_pda(action.mint)
        expected.update(pool=pool, global_config=derive_pumpswap_global_pda(), user_base_token_account=base,
            user_quote_token_account=quote,
            protocol_fee_recipient_token_account=derive_associated_token_address(roles["protocol_fee_recipient"], WSOL_MINT, TOKEN_PROGRAM_ID),
            coin_creator_vault_ata=derive_associated_token_address(roles["coin_creator_vault_authority"], WSOL_MINT, TOKEN_PROGRAM_ID))
        if buy:
            expected.update(global_volume_accumulator=derive_global_volume_accumulator(program), user_volume_accumulator=user_volume)
        count = len(ix.account_indexes)-len(schema)
        cashback = count != 3
        _require(count in ((3, 4) if buy else (3, 5)), "PUMPSWAP_REMAINING_LAYOUT_UNSUPPORTED")
        remaining = [PlanAccountMetaV01(tx.account_keys[i], _writable(tx, i), i < tx.header[0]) for i in ix.account_indexes]
        buyback = tx.account_keys[ix.account_indexes[-2]]
        validate_pumpswap_remaining_account_layout(side=IntentSide(action.side), cashback=cashback,
            actor_public_key=wallet, mint=action.mint, buyback_fee_recipient=buyback, accounts=remaining)
        _require(not cashback, "CASHBACK_ESCROW_ATTRIBUTION_UNSUPPORTED")
        pool_base, pool_quote = roles["pool_base_token_account"], roles["pool_quote_token_account"]
        fee_native = ()
        quote_owners = {roles["protocol_fee_recipient_token_account"]: roles["protocol_fee_recipient"],
            roles["coin_creator_vault_ata"]: roles["coin_creator_vault_authority"],
            tx.account_keys[ix.account_indexes[-1]]: buyback}
    _require(all(roles.get(name) == value for name, value in expected.items()), "VENUE_ACCOUNT_IDENTITY_CONFLICT", contradiction=True)
    _require(wallet not in (pool, *fee_native, *quote_owners.values()) and base != pool_base and quote != pool_quote,
             "VENUE_AND_WALLET_ROLE_ALIAS_CONFLICT", contradiction=True)
    token_roles = {base: (action.mint, action.token_program, wallet), pool_base: (action.mint, action.token_program, pool),
                   quote: (WSOL_MINT, TOKEN_PROGRAM_ID, wallet), pool_quote: (WSOL_MINT, TOKEN_PROGRAM_ID, pool)}
    token_roles.update({key: (WSOL_MINT, TOKEN_PROGRAM_ID, owner) for key, owner in quote_owners.items()})
    _require(action.mint != WSOL_MINT and len({base, quote, pool_base, pool_quote, *quote_owners}) == 4+len(quote_owners)
             and pool not in fee_native, "TOKEN_OR_NATIVE_ROLE_ALIAS_CONFLICT", contradiction=True)
    return ix, program, roles, token_roles, base, quote, pool, pool_base, pool_quote, fee_native, tuple(quote_owners), minimum


def attribute_settlement(domain: LedgerDomain, action: PendingAction, attempt: StoredAttempt,
        receipt: LedgerChainReceipt, support: WalletSupportInput) -> SettlementResult:
    """Return every supported component together, or no proposal at all."""
    try:
        return SettlementResult("PROPOSED", (), _attribute(domain, action, attempt, receipt, support))
    except _Quarantined as exc:
        return SettlementResult("QUARANTINED", (str(exc),), None)
    except _Unapplied as exc:
        return SettlementResult("UNAPPLIED", (str(exc),), None)
    except Exception:
        return SettlementResult("UNAPPLIED", ("SETTLEMENT_INPUT_OR_INSTRUCTION_INVALID",), None)


def _attribute(domain, action, attempt, receipt, support):
    _require(type(domain) is LedgerDomain and type(action) is PendingAction and type(attempt) is StoredAttempt
             and type(receipt) is LedgerChainReceipt and type(support) is WalletSupportInput, "IMMUTABLE_SETTLEMENT_INPUTS_REQUIRED")
    obs = receipt.observation
    _require(type(obs) is TransactionObservation and obs.transaction is not None, "EXACT_TRANSACTION_RECEIPT_REQUIRED")
    tx = obs.transaction
    state = receipt.decision.resulting_state
    expected_finality = "FINALIZED_SUCCESS" if tx.outcome.succeeded else "FINALIZED_FAILURE"
    _require(state.positive_finality == expected_finality and not state.quarantined
        and receipt.decision.observation_disposition == expected_finality
        and attempt.chain_finality == expected_finality+"_UNAPPLIED" and not attempt.chain_quarantined,
        "SUPPORTED_UNAPPLIED_LEDGER_FINALITY_REQUIRED")
    _require(attempt.preparation.action_id == action.action_id and attempt.preparation.action_content_digest == action.content_digest
        and receipt.attempt_id == attempt.preparation.attempt_id and receipt.preparation_digest == attempt.preparation.content_digest
        and receipt.signed_wire_digest == attempt.signed_wire_digest == tx.wire_sha256
        and attempt.primary_signature == tx.primary_signature and tx.message_bytes.hex() == attempt.preparation.message_hex,
        "SETTLEMENT_ORIGINAL_ACTION_ATTEMPT_WIRE_CONFLICT", contradiction=True)
    port = ledger_transaction_evidence(obs, expected_genesis=domain.genesis_hash, expected_signature=attempt.primary_signature,
        expected_profile_fingerprint=domain.expected_profile_fingerprint, now_utc=receipt.decision.evaluated_at_utc,
        expected_message_sha256=attempt.preparation.message_sha256)
    _require(port.disposition == "SUPPORTED_FINALIZED_OBSERVATION", "ORIGINAL_TRANSACTION_PORT_NOT_SUPPORTED")
    _require(tx.header[0] == 1 and tx.account_keys[0] == domain.wallet, "WALLET_FEE_PAYER_AND_AUTHORITY_REQUIRED", contradiction=True)
    wallet = domain.wallet
    account_port = ledger_account_evidence(support.observation, expected_wallet=wallet, expected_genesis=domain.genesis_hash,
        expected_profile_fingerprint=domain.expected_profile_fingerprint, required_min_context_slot=support.required_min_context_slot,
        now_utc=support.evaluated_at_utc)
    _require(support.required_min_context_slot >= tx.slot and account_port.account_facts_usable,
             "ORIGINAL_ACCOUNT_SUPPORT_UNAVAILABLE_OR_UNSUPPORTED")
    anchors = (support.observation.anchor, obs.root, obs.membership_block, attempt.preparation.finalized_lower_anchor)
    _require(all(_known_finalized_anchors_consistent(a, b) for a, b in combinations(anchors, 2)),
             "SETTLEMENT_ACCOUNT_AND_TRANSACTION_ANCHOR_CONFLICT", contradiction=True)
    venue_ix, program, roles, token_roles, base, quote, pool, pool_base, pool_quote, fee_native, fee_tokens, minimum = _roles(tx, action, wallet)
    keys, indexes = tx.account_keys, {key: i for i, key in enumerate(tx.account_keys)}
    native_delta = [post-pre for pre, post in zip(tx.pre_lamports, tx.post_lamports)]
    _require(sum(native_delta) == -tx.fee_lamports, "WHOLE_NATIVE_BALANCE_CONSERVATION_CONFLICT", contradiction=True)
    expected_accounts = {(item.pubkey, item.mint, item.program) for item in support.observation.request.expected_accounts}
    _require({(base, action.mint, action.token_program), (quote, WSOL_MINT, TOKEN_PROGRAM_ID)} <= expected_accounts,
             "EXPLICIT_BASE_AND_WSOL_SUPPORT_BINDING_REQUIRED")
    shapes = {item.pubkey: item for item in account_port.assessment.tokens}
    mints = {item.pubkey: item for item in account_port.assessment.mints}
    _require(action.mint in mints and WSOL_MINT in mints, "BASE_AND_WSOL_MINT_SUPPORT_REQUIRED")
    presence = {key: value is not None for key, value in zip(support.observation.explicit_read.requested_keys, support.observation.explicit_read.accounts)}
    pre, post = ({keys[item.account_index]: item for item in rows} for rows in (tx.pre_tokens, tx.post_tokens))
    for key in set(pre) | set(post):
        _require(key in token_roles, "UNATTRIBUTED_TRANSACTION_TOKEN_ACCOUNT")
        for fact in (pre.get(key), post.get(key)):
            if fact is None:
                continue
            _require((fact.mint, fact.program, fact.owner) == token_roles[key]
                     and fact.decimals == mints[fact.mint].decimals, "TOKEN_BALANCE_IDENTITY_OR_DECIMALS_CONFLICT", contradiction=True)
        if key in pre and key in post:
            _require((pre[key].mint, pre[key].owner, pre[key].program, pre[key].decimals)
                     == (post[key].mint, post[key].owner, post[key].program, post[key].decimals),
                     "TOKEN_PRE_POST_IDENTITY_CONFLICT", contradiction=True)
    basis = content_fingerprint({"version": VERSION, "domain": domain.binding_digest, "action": action.content_digest,
        "attempt": attempt.preparation.content_digest, "receipt": receipt.content_digest, "support": support.digest})
    components = []
    def component(kind, asset, account, units, counterparty=None):
        if units:
            identity = content_fingerprint({"signature": tx.primary_signature, "action_id": action.action_id,
                "kind": kind, "asset": asset, "account": account, "counterparty": counterparty})
            _require(all(item.component_id != identity for item in components), "DUPLICATE_SETTLEMENT_COMPONENT_IDENTITY", contradiction=True)
            components.append(SettlementComponent(identity, kind, asset, account, units, counterparty))
    component("NETWORK_FEE", "SOL", wallet, -tx.fee_lamports)
    if not tx.outcome.succeeded:
        _validate_outer_shapes(tx, venue_ix, token_roles, quote, wallet)
        _validate_failed_inner(tx, venue_ix, program, roles, token_roles, fee_native, fee_tokens, wallet)
        _require(pre == post and all(delta == (-tx.fee_lamports if i == 0 else 0) for i, delta in enumerate(native_delta)),
                 "FAILED_TRANSACTION_HAS_FILL_OR_UNEXPLAINED_EFFECTS", contradiction=True)
        return _proposal(domain, action, attempt, receipt, support, basis, native_delta[0], 0, 0,
                         "SOL" if program == PUMP_PROGRAM_ID else "WSOL", 0, 0, 0, 0, 0, components)
    return _successful(domain, action, attempt, receipt, support, tx, venue_ix, program, roles, token_roles,
        base, quote, pool, pool_base, pool_quote, fee_native, fee_tokens, minimum, pre, post, shapes, presence,
        native_delta, basis, components, component)


def _proposal(domain, action, attempt, receipt, support, basis, native, base, wsol, quote_asset,
              principal, venue_fees, funding, refunds, locked, components):
    return SettlementProposal(content_fingerprint({"economic_domain_id": domain.economic_domain_id,
        "signature": attempt.primary_signature, "action_id": action.action_id}), domain.economic_domain_id,
        action.root_id, action.action_id, attempt.preparation.attempt_id, attempt.primary_signature, basis,
        receipt.observation.content_digest, support.digest, native, base, wsol, receipt.observation.transaction.fee_lamports,
        quote_asset, principal, venue_fees, funding, refunds, locked, tuple(components))


def _validate_outer_shapes(tx, venue_ix, token_roles, quote, wallet):
    compute_seen = set()
    for ix in tx.outer_instructions:
        program = tx.account_keys[ix.program_id_index]
        accounts = tuple(tx.account_keys[i] for i in ix.account_indexes)
        if ix == venue_ix:
            continue
        if program == str(COMPUTE_BUDGET_ID):
            # solana-labs/solana v1.18.26 sdk/src/compute_budget.rs:
            # Canonical ComputeBudgetInstruction: SetComputeUnitLimit(u32),
            # SetComputeUnitPrice(u64). Neither supplies an economic fee split.
            _require(not accounts and len(ix.data) in (5, 9) and ix.data[0] in (2, 3)
                     and len(ix.data) == (5 if ix.data[0] == 2 else 9)
                     and ix.data[0] not in compute_seen, "COMPUTE_BUDGET_SHAPE_OR_DUPLICATE_UNSUPPORTED")
            compute_seen.add(ix.data[0])
        elif program == ASSOCIATED_TOKEN_PROGRAM_ID:
            _require(ix.data == b"\x01" and len(accounts) == 6 and accounts[0] == wallet
                     and accounts[1] in token_roles and accounts[2:4] == (token_roles[accounts[1]][2], token_roles[accounts[1]][0])
                     and accounts[4:] == (SYSTEM_PROGRAM_ID, token_roles[accounts[1]][1])
                     and accounts[1] == derive_associated_token_address(accounts[2], accounts[3], accounts[5])
                     and ix.outer_index < venue_ix.outer_index, "ATA_OUTER_SHAPE_UNSUPPORTED")
        elif program == SYSTEM_PROGRAM_ID:
            _require(len(ix.data) == 12 and ix.data[:4] == bytes((2, 0, 0, 0)) and accounts == (wallet, quote)
                     and ix.outer_index < venue_ix.outer_index, "OUTER_NATIVE_TRANSFER_UNSUPPORTED")
        elif program == TOKEN_PROGRAM_ID:
            _require((ix.data == b"\x11" and accounts == (quote,) and ix.outer_index < venue_ix.outer_index)
                     or (ix.data == b"\x09" and accounts == (quote, wallet, wallet) and ix.outer_index > venue_ix.outer_index),
                     "OUTER_TOKEN_LIFECYCLE_UNSUPPORTED")
        else:
            raise _Unapplied("OUTER_INSTRUCTION_UNSUPPORTED")


def _validate_failed_inner(tx, venue_ix, program, roles, token_roles, fee_native, fee_tokens, wallet):
    """A failed trace can be a prefix. Recognize its shapes, never apply it.

    Complete pre/post equality is checked separately and is the fee-only proof;
    an unknown executed instruction still prevents whole attribution.
    """
    nested = None
    prior_outer = None
    outer_by_index = {ix.outer_index: ix for ix in tx.outer_instructions}
    for ix in tx.inner_instructions:
        outer = outer_by_index[ix.outer_index]
        pid, accounts = tx.account_keys[ix.program_id_index], tuple(tx.account_keys[i] for i in ix.account_indexes)
        if prior_outer != ix.outer_index or ix.stack_height == 2:
            nested = None
        prior_outer = ix.outer_index
        ata = tx.account_keys[outer.account_indexes[1]] if tx.account_keys[outer.program_id_index] == ASSOCIATED_TOKEN_PROGRAM_ID else nested
        if pid == ASSOCIATED_TOKEN_PROGRAM_ID:
            _require(outer == venue_ix and ix.stack_height == 2 and len(accounts) == 6
                     and ix.data in (b"", b"\x00", b"\x01") and accounts[0] == wallet and accounts[1] in fee_tokens,
                     "FAILED_NESTED_ATA_SHAPE_UNSUPPORTED")
            mint, tprog, owner = token_roles[accounts[1]]
            _require(accounts[2:] == (owner, mint, SYSTEM_PROGRAM_ID, tprog)
                     and accounts[1] == derive_associated_token_address(owner, mint, tprog), "FAILED_ATA_IDENTITY_CONFLICT", contradiction=True)
            nested = accounts[1]
            continue
        _require(ix.stack_height == (3 if nested is not None else 2), "FAILED_INNER_STACK_UNSUPPORTED")
        if pid == program:
            _require(outer == venue_ix and nested is None and accounts == (roles["event_authority"],)
                     and all(not _writable(tx, i) for i in ix.account_indexes)
                     and ix.data.startswith(ANCHOR_EVENT_TAG) and 16 <= len(ix.data) <= 65536, "FAILED_EVENT_SHAPE_UNSUPPORTED")
        elif pid == PUMP_FEES_PROGRAM_ID:
            _require(outer == venue_ix and nested is None and accounts == (roles["fee_config"], program)
                     and all(not _writable(tx, i) for i in ix.account_indexes) and len(ix.data) == 34
                     and ix.data[:8] == GET_FEES_TAG and ix.data[8] in (0, 1) and ix.data[33] in (0, 1),
                     "FAILED_GET_FEES_SHAPE_UNSUPPORTED")
        elif pid == SYSTEM_PROGRAM_ID:
            if len(ix.data) == 52 and ix.data[:4] == bytes(4):
                _require(len(accounts) == 2 and accounts[0] == wallet, "FAILED_CREATE_PAYER_UNSUPPORTED")
                decoded = decode_create_account(_instruction(tx, ix))
                _require(decoded["lamports"] > 0 and ((ata is not None and accounts[1] == ata
                         and str(decoded["owner"]) == token_roles[ata][1] and decoded["space"] == 165)
                         or (outer == venue_ix and ata is None and accounts[1] == roles.get("user_volume_accumulator")
                         and str(decoded["owner"]) == program and 0 < decoded["space"] <= 4096)), "FAILED_CREATE_ROLE_UNSUPPORTED")
            else:
                _require(outer == venue_ix and ata is None and program == PUMP_PROGRAM_ID and len(ix.data) == 12
                         and ix.data[:4] == bytes((2, 0, 0, 0)) and len(accounts) == 2 and accounts[0] == wallet
                         and accounts[1] in (roles["bonding_curve"], *fee_native), "FAILED_NATIVE_TRANSFER_UNSUPPORTED")
        elif pid in (TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID):
            if ata is not None:
                mint, tprog, owner = token_roles[ata]
                _require(pid == tprog and ((ix.data in (b"\x15", b"\x15\x07\x00") and accounts == (mint,))
                         or (pid == TOKEN_PROGRAM_ID and ix.data == b"\x16" and accounts == (ata,))
                         or (ix.data == b"\x12"+bytes(Pubkey.from_string(owner)) and accounts == (ata, mint))),
                         "FAILED_TOKEN_INITIALIZATION_UNSUPPORTED")
            else:
                checked = len(ix.data) == 10 and ix.data[0] == 12
                _require(outer == venue_ix and (checked or (len(ix.data) == 9 and ix.data[0] == 3))
                         and len(accounts) == (4 if checked else 3), "FAILED_TOKEN_TRANSFER_UNSUPPORTED")
                decoded = (decode_transfer_checked if checked else decode_token_transfer)(_instruction(tx, ix))
                source, dest = str(decoded.source), str(decoded.dest)
                _require(source in token_roles and dest in token_roles and token_roles[source][:2] == token_roles[dest][:2]
                         and pid == token_roles[source][1] and str(decoded.owner) == token_roles[source][2]
                         and (not checked or str(decoded.mint) == token_roles[source][0]), "FAILED_TOKEN_TRANSFER_IDENTITY_CONFLICT", contradiction=True)
        else:
            raise _Unapplied("FAILED_INNER_INSTRUCTION_UNSUPPORTED")


def _successful(domain, action, attempt, receipt, support, tx, venue_ix, program, roles, token_roles,
        base, quote, pool, pool_base, pool_quote, fee_native, fee_tokens, minimum, pre, post, shapes, presence,
        native_delta, basis, components, component):
    wallet, pump, buy = domain.wallet, program == PUMP_PROGRAM_ID, action.side == "BUY"
    _validate_outer_shapes(tx, venue_ix, token_roles, quote, wallet)
    keys = tx.account_keys
    index = {key: i for i, key in enumerate(keys)}
    lamports = dict(zip(keys, tx.pre_lamports))
    lamports[wallet] -= tx.fee_lamports
    quantities = {key: value.amount for key, value in pre.items()}
    created, initialized, closed, synced = {}, set(), set(), set()
    size_queried, immutable_initialized = set(), set()
    wrapped = funding = refunds = locked = 0
    flows = []
    pump_events = []
    pump_transfers = {key: 0 for key in keys}
    ata_outer = {}
    groups = {outer: [] for outer in tx.inner_recorded_outer_indexes}
    for inner in tx.inner_instructions:
        groups[inner.outer_index].append(inner)
    required_groups = {venue_ix.outer_index}
    for ix in tx.outer_instructions:
        if keys[ix.program_id_index] == ASSOCIATED_TOKEN_PROGRAM_ID:
            ata_outer[ix.outer_index] = keys[ix.account_indexes[1]]
            required_groups.add(ix.outer_index)
    _require(required_groups <= groups.keys(), "REQUIRED_INNER_INSTRUCTION_COVERAGE_MISSING")

    def move_native(source, destination, amount):
        _require(source != destination and source in lamports and destination in lamports,
                 "NATIVE_FLOW_ACCOUNT_CONFLICT", contradiction=True)
        _require(lamports[source] >= amount, "NATIVE_FLOW_OVERDRAW_CONFLICT", contradiction=True)
        lamports[source] -= amount
        lamports[destination] += amount

    def decode_inner(ix, nested_ata=None):
        nonlocal funding, locked
        pid = keys[ix.program_id_index]
        accounts = tuple(keys[i] for i in ix.account_indexes)
        ata = nested_ata or ata_outer.get(ix.outer_index)
        _require(ix.stack_height == (3 if nested_ata is not None else 2),
                 "INNER_STACK_OR_PROGRAM_SHAPE_UNSUPPORTED")
        if pid == program:
            _require(ata is None and ix.outer_index == venue_ix.outer_index
                     and accounts == (derive_event_authority(program),)
                     and all(not _writable(tx, i) for i in ix.account_indexes)
                     and ix.data.startswith(ANCHOR_EVENT_TAG) and 16 <= len(ix.data) <= 65536,
                     "ANCHOR_EVENT_ENVELOPE_UNSUPPORTED")
            if pump and ix.data[8:16] == PUMP_TRADE_EVENT_TAG:
                pump_events.append(_pump_trade_event(ix.data))
            return
        if pid == PUMP_FEES_PROGRAM_ID:
            _require(ata is None and ix.outer_index == venue_ix.outer_index
                     and accounts == (roles["fee_config"], program)
                     and all(not _writable(tx, i) for i in ix.account_indexes)
                     and len(ix.data) == 34 and ix.data[:8] == GET_FEES_TAG
                     and ix.data[8] in (0, 1) and ix.data[33] in (0, 1), "GET_FEES_ENVELOPE_UNSUPPORTED")
            return
        if pid == SYSTEM_PROGRAM_ID:
            if len(ix.data) == 52 and ix.data[:4] == bytes(4):
                if ata is None:
                    # Accepted venue IDLs name this exact derived setup PDA.
                    # Observed allocation is provenance, never a rent estimate
                    # or decoded ownership of its program-controlled funds.
                    volume = roles.get("user_volume_accumulator")
                    decoded = decode_create_account(_instruction(tx, ix))
                    _require(buy and accounts == (wallet, volume)
                             and "program:"+volume not in created and lamports[volume] == 0
                             and str(decoded["owner"]) == program and 0 < decoded["space"] <= 4096
                             and decoded["lamports"] > 0, "VENUE_ACCOUNT_SETUP_UNSUPPORTED")
                    amount = decoded["lamports"]
                    move_native(wallet, volume, amount)
                    # Separate namespace from token-account lifecycle facts.
                    created["program:"+volume] = amount
                    initialized.add("program:"+volume)
                    funding += amount
                    component("VENUE_ACCOUNT_SETUP", "SOL", wallet, -amount, volume)
                    return
                _require(ata is not None and accounts == (wallet, ata) and ata not in created
                         and ata not in pre and lamports[ata] == 0, "ACCOUNT_CREATE_LIFECYCLE_UNSUPPORTED")
                decoded = decode_create_account(_instruction(tx, ix))
                _require(str(decoded["owner"]) == token_roles[ata][1] and decoded["space"] == 165
                         and decoded["lamports"] > 0, "ACCOUNT_CREATE_SHAPE_UNSUPPORTED")
                amount = decoded["lamports"]
                move_native(wallet, ata, amount)
                created[ata] = amount
                if token_roles[ata][2] == wallet:
                    funding += amount
                    locked += amount
                    component("ACCOUNT_FUNDING", "SOL", wallet, -amount, ata)
                    component("ACCOUNT_LOCK", "LOCKED_LAMPORTS", ata, amount, wallet)
                else:
                    funding += amount
                    component("EXTERNAL_ACCOUNT_FUNDING", "SOL", wallet, -amount, ata)
                return
            _require(ata is None and ix.outer_index == venue_ix.outer_index and pump
                     and len(ix.data) == 12 and ix.data[:4] == bytes((2, 0, 0, 0)) and len(accounts) == 2
                     and set(accounts) <= {wallet, pool, *fee_native}, "INNER_NATIVE_FLOW_UNSUPPORTED")
            decoded = decode_transfer(_instruction(tx, ix))
            _require(decoded["lamports"] > 0, "ZERO_NATIVE_TRANSFER_UNSUPPORTED")
            source, destination = accounts
            _require(source == wallet and destination in (pool, *fee_native), "PUMP_SYSTEM_TRANSFER_DIRECTION_UNSUPPORTED")
            move_native(source, destination, decoded["lamports"])
            pump_transfers[source] -= decoded["lamports"]
            pump_transfers[destination] += decoded["lamports"]
            return
        _require(pid in (TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID), "INNER_INSTRUCTION_UNSUPPORTED")
        if ata is not None:
            _require(pid == token_roles[ata][1], "ATA_TOKEN_PROGRAM_CONFLICT", contradiction=True)
            if ix.data in (b"\x15", b"\x15\x07\x00"):
                # Canonical ATA tools/account.rs requests ImmutableOwner (7)
                # using GetAccountDataSize tag21 + one little-endian u16.
                _require(accounts == (token_roles[ata][0],) and ata not in size_queried and ata not in created,
                         "ATA_SIZE_QUERY_UNSUPPORTED")
                size_queried.add(ata)
                return
            if ix.data == b"\x16":
                _require(pid == TOKEN_PROGRAM_ID and accounts == (ata,) and ata in created and ata not in immutable_initialized,
                         "ATA_EXTENSION_INITIALIZATION_UNSUPPORTED")
                immutable_initialized.add(ata)
                return
            _require(len(ix.data) == 33 and ix.data[0] == 18 and accounts == (ata, token_roles[ata][0])
                     and ata in created and ata not in initialized, "ATA_INITIALIZE_LIFECYCLE_UNSUPPORTED")
            decoded = decode_initialize_account3(_instruction(tx, ix))
            _require(str(decoded.owner) == token_roles[ata][2], "ATA_INITIALIZED_OWNER_CONFLICT", contradiction=True)
            initialized.add(ata)
            quantities[ata] = 0
            return
        _require(ix.outer_index == venue_ix.outer_index, "TOKEN_FLOW_OUTSIDE_VENUE_UNSUPPORTED")
        checked = len(ix.data) == 10 and ix.data[0] == 12
        _require(checked or (len(ix.data) == 9 and ix.data[0] == 3), "TOKEN_INSTRUCTION_UNSUPPORTED")
        _require(len(accounts) == (4 if checked else 3), "TOKEN_MULTISIG_OR_ACCOUNT_SHAPE_UNSUPPORTED")
        decoded = (decode_transfer_checked if checked else decode_token_transfer)(_instruction(tx, ix))
        source, destination, authority = str(decoded.source), str(decoded.dest), str(decoded.owner)
        _require(source in token_roles and destination in token_roles and source != destination,
                 "UNATTRIBUTED_TOKEN_TRANSFER")
        mint, token_program, owner = token_roles[source]
        _require(token_roles[destination][:2] == (mint, token_program) and pid == token_program and authority == owner,
                 "TOKEN_TRANSFER_IDENTITY_CONFLICT", contradiction=True)
        if checked:
            _require(str(decoded.mint) == mint and decoded.decimals == (pre.get(source) or post.get(source)
                     or pre.get(destination) or post.get(destination)).decimals, "CHECKED_TOKEN_METADATA_CONFLICT", contradiction=True)
        allowed = ((source, destination) == ((pool_base, base) if buy else (base, pool_base)) if mint == action.mint else
                   (not pump and ((source == quote and destination in (pool_quote, *fee_tokens)) if buy else
                    (source == pool_quote and destination in (quote, *fee_tokens)) or (source == quote and destination in fee_tokens))))
        _require(allowed, "TOKEN_TRANSFER_DIRECTION_OR_VENUE_UNSUPPORTED")
        amount = decoded.amount
        _require(source in quantities and destination in quantities and quantities[source] >= amount,
                 "TOKEN_FLOW_MISSING_OPENING_OR_OVERDRAW", contradiction=True)
        quantities[source] -= amount
        quantities[destination] += amount
        if mint == WSOL_MINT:
            move_native(source, destination, amount)
        flows.append((source, destination, mint, amount))

    for outer in tx.outer_instructions:
        pid = keys[outer.program_id_index]
        accounts = tuple(keys[i] for i in outer.account_indexes)
        if outer == venue_ix or pid == ASSOCIATED_TOKEN_PROGRAM_ID:
            nested_ata = None
            for inner in groups.get(outer.outer_index, ()):
                inner_pid = keys[inner.program_id_index]
                if outer == venue_ix and inner.stack_height == 2:
                    nested_ata = None
                    if inner_pid == ASSOCIATED_TOKEN_PROGRAM_ID:
                        accounts_inner = tuple(keys[i] for i in inner.account_indexes)
                        _require(inner.data in (b"", b"\x00", b"\x01") and len(accounts_inner) == 6
                                 and accounts_inner[0] == wallet and accounts_inner[1] in fee_tokens,
                                 "NESTED_ATA_ROLE_OR_SHAPE_UNSUPPORTED")
                        nested_ata = accounts_inner[1]
                        mint, tprog, owner = token_roles[nested_ata]
                        _require(accounts_inner[2:] == (owner, mint, SYSTEM_PROGRAM_ID, tprog)
                                 and nested_ata == derive_associated_token_address(owner, mint, tprog),
                                 "NESTED_ATA_IDENTITY_CONFLICT", contradiction=True)
                        continue
                decode_inner(inner, nested_ata if inner.stack_height == 3 else None)
            if pid == ASSOCIATED_TOKEN_PROGRAM_ID and accounts[1] in created:
                _require(accounts[1] in initialized, "CREATED_ACCOUNT_INITIALIZATION_MISSING")
        else:
            _require(not groups.get(outer.outer_index), "UNEXPECTED_INNER_INSTRUCTIONS")
            if pid == SYSTEM_PROGRAM_ID:
                _require(not pump and buy and quote in initialized and quote not in synced,
                         "WRAP_NATIVE_LIFECYCLE_UNSUPPORTED")
                amount = decode_transfer(_instruction(tx, outer))["lamports"]
                _require(amount == action.input_units and wrapped == 0, "WRAP_AND_ACTION_INPUT_CONFLICT", contradiction=True)
                move_native(wallet, quote, amount)
                wrapped = amount
                component("WRAP_NATIVE", "SOL", wallet, -amount, quote)
                component("WRAP_NATIVE", "WSOL", quote, amount, wallet)
            elif pid == TOKEN_PROGRAM_ID and outer.data == b"\x11":
                _require(not pump and quote in initialized and quote not in synced and wrapped > 0,
                         "SYNC_NATIVE_LIFECYCLE_UNSUPPORTED")
                quantities[quote] += wrapped
                synced.add(quote)
            elif pid == TOKEN_PROGRAM_ID and outer.data == b"\x09":
                _require(not pump and quote in initialized and quote not in closed,
                         "CLOSE_EXISTING_OR_UNKNOWN_WSOL_UNSUPPORTED")
                reserve, units = created[quote], quantities[quote]
                _require(lamports[quote] == reserve+units, "WSOL_BACKING_CONFLICT", contradiction=True)
                move_native(quote, wallet, reserve+units)
                closed.add(quote)
                quantities[quote] = 0
                refunds += reserve
                locked -= reserve
                component("ACCOUNT_REFUND", "SOL", wallet, reserve, quote)
                component("ACCOUNT_UNLOCK", "LOCKED_LAMPORTS", quote, -reserve, wallet)
                component("UNWRAP_NATIVE", "WSOL", quote, -units, wallet)
                component("UNWRAP_NATIVE", "SOL", wallet, units, quote)

    _require(set(created) == initialized, "ACCOUNT_CREATE_INITIALIZE_COVERAGE_CONFLICT", contradiction=True)
    _require(all(key.startswith("program:") or (key in size_queried and key in immutable_initialized) for key in created),
             "CANONICAL_ATA_CREATE_COVERAGE_INCOMPLETE")
    for key in set(quantities) | set(pre) | set(post):
        _require(key in pre or key in created, "MISSING_PRE_TOKEN_IS_NOT_ZERO")
        _require(key in post or key in closed, "MISSING_POST_TOKEN_IS_NOT_ZERO")
        _require(quantities[key] == (post[key].amount if key in post else 0), "TOKEN_MOVEMENT_CONSERVATION_CONFLICT", contradiction=True)
        if key in created:
            _require(tx.pre_lamports[index[key]] == 0, "CREATED_ACCOUNT_PRE_LAMPORTS_CONFLICT", contradiction=True)
        if key in closed:
            _require(tx.post_lamports[index[key]] == 0 and key not in post, "CLOSED_ACCOUNT_POST_FACT_CONFLICT", contradiction=True)
    for key in (base, quote):
        if key in post:
            _require(presence.get(key) is True and key in shapes and shapes[key].authority == wallet,
                     "POST_ACCOUNT_SUPPORT_MISSING")
        else:
            _require(presence.get(key) is False, "ABSENT_POST_ACCOUNT_SUPPORT_NOT_EXPLICIT")
    _require(base in post and (base in pre or base in created), "BASE_ACCOUNT_COMPLETE_LIFECYCLE_REQUIRED")
    base_delta = post[base].amount-(pre[base].amount if base in pre else 0)
    _require((base_delta >= minimum and base_delta > 0) if buy else base_delta == -action.input_units,
             "ACTUAL_BASE_MOVEMENT_AND_ACTION_CONFLICT", contradiction=True)
    component("BASE_ACQUIRED" if buy else "BASE_SOLD", action.mint, base, base_delta, pool_base)
    wsol_delta = (post[quote].amount if quote in post else 0)-(pre[quote].amount if quote in pre else 0)
    if pump:
        _require(quote not in pre and quote not in post and quote not in created,
                 "PUMP_DIRECT_NATIVE_REQUIRES_ABSENT_WSOL")
        if buy:
            # Pinned Pump BUY.md: curve extension target115, creator vault0.
            # solana v1.18.26 sdk/program/src/rent.rs minimum_balance is monotone
            # with length. Canonical ATA create funded a fresh165-byte account
            # in this very transaction. That observed funding bounds the rent
            # needed by both smaller targets, without assigning a numerical
            # rent minimum or interpreting an arbitrary account balance as one.
            witness = created.get(base)
            _require(witness is not None and tx.pre_lamports[index[pool]] >= witness
                     and tx.pre_lamports[index[roles["creator_vault"]]] >= witness,
                     "PUMP_NATIVE_SETUP_EXCLUSION_WITNESS_REQUIRED")
        else:
            # The System-owned payer cannot be debited directly by the venue.
            # A complete trace with no such System transfer excludes a hidden
            # wallet-funded top-up from the supported SELL proceeds profile.
            _require(pump_transfers[wallet] == 0, "PUMP_SELL_NATIVE_SETUP_OR_DEBIT_UNSUPPORTED")
        residual = {key: tx.post_lamports[index[key]]-lamports[key] for key in keys}
        _require(all(value == 0 for value in residual.values()) if buy else
                 all(value >= 0 for key, value in residual.items() if key != pool),
                 "PUMP_NATIVE_INSTRUCTION_AND_DIRECT_FLOW_CONFLICT", contradiction=True)
        _require(all(value == 0 for key, value in residual.items() if key not in (wallet, pool, *fee_native)),
                 "UNATTRIBUTED_NATIVE_BALANCE_CHANGE")
        residual = {key: value+pump_transfers[key] for key, value in residual.items()}
        principal = residual[pool] if buy else -residual[pool]
        fees = sum(residual[key] for key in set(fee_native))
        _require(len(pump_events) == 1, "EXACT_PUMP_TRADE_CLASSIFICATION_EVENT_REQUIRED")
        event = pump_events[0]
        _require(event["mint"] == action.mint and event["user"] == wallet and event["is_buy"] == buy
                 and event["token_amount"] == abs(base_delta) and event["fee_recipient"] == roles["fee_recipient"]
                 and event["quote_mint"] in (WSOL_MINT, SYSTEM_PROGRAM_ID)
                 and derive_pump_creator_vault(event["creator"]) == roles["creator_vault"],
                 "PUMP_TRADE_EVENT_IDENTITY_OR_ACTUAL_UNITS_CONFLICT", contradiction=True)
        classified_fees = {}
        for key, value in ((roles["fee_recipient"], event["fee"]), (roles["creator_vault"], event["creator_fee"]),
                           (roles["buyback_fee_recipient"], event["buyback_fee"])):
            classified_fees[key] = classified_fees.get(key, 0)+value
        _require(all(residual[key] == amount for key, amount in classified_fees.items()),
                 "PUMP_RECIPIENT_SETUP_OR_FEE_CLASSIFICATION_UNRESOLVED")
        _require(principal > 0 and all(residual[key] >= 0 for key in set(fee_native))
                 and residual[wallet] == (-principal-fees if buy else principal-fees),
                 "VENUE_NATIVE_FLOW_CONFLICT", contradiction=True)
        _require(principal+fees <= action.input_units if buy else principal-fees >= minimum,
                 "ACTUAL_QUOTE_MOVEMENT_AND_ACTION_CONFLICT", contradiction=True)
        component("VENUE_PRINCIPAL", "SOL", wallet, -principal if buy else principal, pool)
        for key in sorted(set(fee_native)):
            component("VENUE_FEE", "SOL", wallet, -residual[key], key)
    else:
        _require(all(lamports[key] == tx.post_lamports[index[key]] for key in keys),
                 "UNATTRIBUTED_NATIVE_BALANCE_CHANGE")
        if buy:
            _require(quote in closed and quote in synced, "BUY_NATIVE_RECYCLING_LIFECYCLE_REQUIRED")
        principal = sum(amount for source, destination, mint, amount in flows if mint == WSOL_MINT
                        and (source, destination) == ((quote, pool_quote) if buy else (pool_quote, quote)))
        fees = sum(amount for _source, destination, mint, amount in flows if mint == WSOL_MINT and destination in fee_tokens)
        # Sell principal includes quote directed by the pool straight to fee accounts.
        if not buy:
            principal += sum(amount for source, destination, mint, amount in flows
                             if mint == WSOL_MINT and source == pool_quote and destination in fee_tokens)
        net = -principal-fees if buy else principal-fees
        _require(principal > 0 and ((-net <= action.input_units) if buy else net >= minimum),
                 "ACTUAL_QUOTE_MOVEMENT_AND_ACTION_CONFLICT", contradiction=True)
        component("VENUE_PRINCIPAL", "WSOL", quote, -principal if buy else principal, pool_quote)
        for key in sorted(set(fee_tokens)):
            amount = sum(units for _source, destination, mint, units in flows if mint == WSOL_MINT and destination == key)
            component("VENUE_FEE", "WSOL", quote, -amount, key)
    _require(sum(item.units for item in components if item.asset == "SOL") == native_delta[0]
             and sum(item.units for item in components if item.asset == "WSOL") == wsol_delta,
             "OWNED_COMPONENT_BALANCE_CONFLICT", contradiction=True)
    return _proposal(domain, action, attempt, receipt, support, basis, native_delta[0], base_delta, wsol_delta,
                     "SOL" if pump else "WSOL", principal, fees, funding, refunds, locked, components)
