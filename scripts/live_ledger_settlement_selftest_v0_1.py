"""L4: actual mocked adapters -> durable Ledger finality -> pure proposals.

Public unsigned plan fixtures supply message/account shape only. Every economic
amount below is independent actual transaction metadata; no Shadow inventory is
imported into Ledger. No private key, signature creation, send or network.
"""
from __future__ import annotations

import base64
import copy
import json
import struct
import sys
import tempfile
from dataclasses import replace
from pathlib import Path

from solders.hash import Hash
from solders.instruction import Instruction
from solders.message import Message, to_bytes_versioned
from solders.pubkey import Pubkey

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from phase5.shadow_domain_v0_1 import IntentSide, canonical_json, content_fingerprint
from phase5.shadow_unsigned_plan_simulation_v0_1 import BlockhashLeaseV01
from live.ledger_domain_v0_1 import LedgerDomain
from live.ledger_actions_v0_1 import PendingAction, AttemptPreparation, position_identity
from live.ledger_repository_v0_1 import LedgerRepository
from live.ledger_settlement_v0_1 import (
    attribute_settlement, WalletSupportInput, ANCHOR_EVENT_TAG, GET_FEES_TAG, PUMP_TRADE_EVENT_TAG,
    PUMP_PROGRAM_ID, PUMPSWAP_PROGRAM_ID, TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID,
    SYSTEM_PROGRAM_ID, ASSOCIATED_TOKEN_PROGRAM_ID, WSOL_MINT, COMPUTE_BUDGET_ID,
    derive_associated_token_address, derive_user_volume_accumulator,
)
from live.wallet_evidence_v0_1 import WalletEvidenceRequest, ExpectedTokenAccount
from live_ledger_baseline_selftest_v0_1 import observe, ingest
from live_ledger_actions_selftest_v0_1 import candidate, advance
from live_ledger_finality_selftest_v0_1 import scenario, transaction_observation
from live_wallet_evidence_selftest_v0_1 import (
    Scenario as WalletScenario, account, mint_data, token_data, GENESIS, NOW, PROFILE, utc,
)
from live_transaction_evidence_selftest_v0_1 import RECENT, bh
import phase5_shadow_unsigned_plan_simulation_selftest_v0_1 as plans

WALLET, MINT = plans.ACTOR, plans.fx.MINT
R, FEE, FUNDING = 2_039_280, 7_777, 5_000_000_000
CHECKS = {}


def check(name, condition):
    CHECKS[name] = bool(condition)
    if not condition:
        raise AssertionError(name)


def digest(value):
    return content_fingerprint(value)


def b58(data):
    alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
    number, result = int.from_bytes(data, "big"), ""
    while number:
        number, digit = divmod(number, 58)
        result = alphabet[digit]+result
    return "1"*(len(data)-len(data.lstrip(b"\0")))+result


class Fixture:
    def __init__(self, venue="pump", side="BUY", *, retained=False, failed=False,
                 token_program=TOKEN_PROGRAM_ID, extra_output=17, compute=True, volume_setup=True, outer_transform=None):
        self.venue, self.side, self.failed, self.token_program = venue, side, failed, token_program
        self.plan_tuple = plans.make_plan(venue, IntentSide(side), quote_exists=retained, token_program=token_program)
        self.plan = self.plan_tuple[-1]
        # Materialization is confined to this inert fixture. Production settlement
        # only reads the persisted exact message and never builds transactions.
        instructions = [item.materialize() for item in self.plan.instructions]
        if compute:
            instructions = [Instruction(COMPUTE_BUDGET_ID, b"\x02"+struct.pack("<I", 250000), []),
                            Instruction(COMPUTE_BUDGET_ID, b"\x03"+struct.pack("<Q", 11111), [])]+instructions
        if outer_transform:
            instructions = outer_transform(instructions)
        self.message = Message.new_with_blockhash(instructions, Pubkey.from_string(WALLET), Hash.from_string(RECENT))
        self.message_bytes = to_bytes_versioned(self.message)
        self.wire = b"\x01"+bytes([49])*64+self.message_bytes
        self.keys = tuple(map(str, self.message.account_keys))
        self.index = {key: i for i, key in enumerate(self.keys)}
        self.base = derive_associated_token_address(WALLET, MINT, token_program)
        self.quote = derive_associated_token_address(WALLET, WSOL_MINT, TOKEN_PROGRAM_ID)
        self.program = PUMP_PROGRAM_ID if venue == "pump" else PUMPSWAP_PROGRAM_ID
        planned_venue = next(item for item in self.plan.instructions if item.program_id == self.program)
        from live.ledger_settlement_v0_1 import PUMP_BUY_ACCOUNTS, PUMP_SELL_ACCOUNTS, PUMPSWAP_BUY_ACCOUNTS, PUMPSWAP_SELL_ACCOUNTS
        schema = (PUMP_BUY_ACCOUNTS if side == "BUY" else PUMP_SELL_ACCOUNTS) if venue == "pump" else (
            PUMPSWAP_BUY_ACCOUNTS if side == "BUY" else PUMPSWAP_SELL_ACCOUNTS)
        self.roles = {name: meta.pubkey for (name, _, _), meta in zip(schema, planned_venue.accounts)}
        self.units, self.minimum = struct.unpack("<QQ", bytes.fromhex(planned_venue.data_hex)[8:24])
        self.actual_base = self.minimum+extra_output if side == "BUY" else self.units
        self.base_residual = 123_456_789 if side == "SELL" else 0
        self.principal = 95_000_000 if side == "BUY" else self.minimum+2_000_000
        self.venue_fees = 1_000_000
        self.pre_native = {key: 1_000_000_000 for key in self.keys}
        self.pre_native[WALLET] = FUNDING
        self.pre_native[self.quote] = R+123456 if retained else 0
        self.pre_native[self.base] = R if side == "SELL" else 0
        self.pre_token = {}
        self.post_token = {}
        if side == "SELL":
            self.token(self.base, MINT, WALLET, self.units+self.base_residual, token_program)
        if retained:
            self.token(self.quote, WSOL_MINT, WALLET, 123456, TOKEN_PROGRAM_ID)
        if venue == "pump":
            self.pool = self.roles["bonding_curve"]
            self.pool_base = self.roles["associated_base_bonding_curve"]
            self.pool_quote = self.roles["associated_quote_bonding_curve"]
            self.fee_accounts = (self.roles["fee_recipient"], self.roles["buyback_fee_recipient"], self.roles["creator_vault"])
        else:
            self.pool = self.roles["pool"]
            self.pool_base = self.roles["pool_base_token_account"]
            self.pool_quote = self.roles["pool_quote_token_account"]
            self.fee_accounts = (self.roles["protocol_fee_recipient_token_account"], self.roles["coin_creator_vault_ata"],
                                 planned_venue.accounts[-1].pubkey)
            self.token(self.pool_quote, WSOL_MINT, self.pool, 10_000_000_000, TOKEN_PROGRAM_ID)
            owners = (self.roles["protocol_fee_recipient"], self.roles["coin_creator_vault_authority"], planned_venue.accounts[-2].pubkey)
            for key, owner in zip(self.fee_accounts, owners):
                self.token(key, WSOL_MINT, owner, 0, TOKEN_PROGRAM_ID)
            if side == "BUY":
                for key in self.fee_accounts[:2]:
                    self.pre_token.pop(key)
                    self.pre_native[key] = 0
        self.token(self.pool_base, MINT, self.pool, 100_000_000_000_000, token_program)
        self.post_native = dict(self.pre_native)
        self.post_token = copy.deepcopy(self.pre_token)
        self.post_native[WALLET] -= FEE
        self.groups = []
        for outer_index, instruction in enumerate(instructions):
            pid, accounts = str(instruction.program_id), tuple(str(meta.pubkey) for meta in instruction.accounts)
            if pid == ASSOCIATED_TOKEN_PROGRAM_ID:
                ata, mint, owner, tprog = accounts[1], accounts[3], accounts[2], accounts[5]
                rows = [self.inner(tprog, (mint,), b"\x15\x07\x00"),
                        self.inner(SYSTEM_PROGRAM_ID, (WALLET, ata), struct.pack("<IQQ", 0, R, 165)+bytes(Pubkey.from_string(tprog))),
                        self.inner(tprog, (ata,), b"\x16"),
                        self.inner(tprog, (ata, mint), b"\x12"+bytes(Pubkey.from_string(owner)))]
                self.groups.append({"index": outer_index, "instructions": rows})
                self.move(WALLET, ata, R)
                self.post_token[ata] = self.token_row(ata, mint, owner, 0, tprog)
            elif pid == SYSTEM_PROGRAM_ID:
                self.move(WALLET, self.quote, self.units)
            elif pid == TOKEN_PROGRAM_ID and instruction.data == b"\x11":
                self.set_amount(self.post_token[self.quote], self.units)
            elif pid == self.program:
                rows = []
                self.venue_index = outer_index
                if side == "BUY" and volume_setup:
                    volume = derive_user_volume_accumulator(self.program, WALLET)
                    self.pre_native[volume] = self.post_native[volume] = 0
                    self.move(WALLET, volume, 1_844_400)
                    rows.append(self.inner(SYSTEM_PROGRAM_ID, (WALLET, volume),
                        struct.pack("<IQQ", 0, 1_844_400, 137 if venue == "pump" else 256)+bytes(Pubkey.from_string(self.program))))
                if venue == "swap" and side == "BUY":
                    for ata, owner in zip(self.fee_accounts[:2], owners[:2]):
                        rows.append(self.inner(ASSOCIATED_TOKEN_PROGRAM_ID,
                            (WALLET, ata, owner, WSOL_MINT, SYSTEM_PROGRAM_ID, TOKEN_PROGRAM_ID), b"\x01"))
                        nested = [self.inner(TOKEN_PROGRAM_ID, (WSOL_MINT,), b"\x15\x07\x00"),
                            self.inner(SYSTEM_PROGRAM_ID, (WALLET, ata), struct.pack("<IQQ", 0, R, 165)+bytes(Pubkey.from_string(TOKEN_PROGRAM_ID))),
                            self.inner(TOKEN_PROGRAM_ID, (ata,), b"\x16"),
                            self.inner(TOKEN_PROGRAM_ID, (ata, WSOL_MINT), b"\x12"+bytes(Pubkey.from_string(owner)))]
                        for row in nested:
                            row["stackHeight"] = 3
                        rows.extend(nested)
                        self.move(WALLET, ata, R)
                        self.post_token[ata] = self.token_row(ata, WSOL_MINT, owner, 0, TOKEN_PROGRAM_ID)
                source, dest, authority = ((self.pool_base, self.base, self.pool) if side == "BUY" else (self.base, self.pool_base, WALLET))
                rows.append(self.transfer(source, dest, authority, self.actual_base, MINT, token_program))
                if venue == "pump":
                    if side == "BUY":
                        self.move(WALLET, self.pool, self.principal)
                        rows.append(self.inner(SYSTEM_PROGRAM_ID, (WALLET, self.pool), struct.pack("<IQ", 2, self.principal)))
                        for key, fee in zip(self.fee_accounts, (500000, 300000, 200000)):
                            self.move(WALLET, key, fee)
                            rows.append(self.inner(SYSTEM_PROGRAM_ID, (WALLET, key), struct.pack("<IQ", 2, fee)))
                    else:
                        self.move(self.pool, WALLET, self.principal-self.venue_fees)
                        for key, fee in zip(self.fee_accounts, (500000, 300000, 200000)):
                            self.move(self.pool, key, fee)
                else:
                    source, dest, authority = ((self.quote, self.pool_quote, WALLET) if side == "BUY" else (self.pool_quote, self.quote, self.pool))
                    # SELL pool distributes fees directly: actual gross principal
                    # includes these transfers, without inverse quote arithmetic.
                    amount = self.principal if side == "BUY" else self.principal-self.venue_fees
                    rows.append(self.transfer(source, dest, authority, amount, WSOL_MINT, TOKEN_PROGRAM_ID))
                    for key, fee in zip(self.fee_accounts, (500000, 300000, 200000)):
                        rows.append(self.transfer(source, key, authority, fee, WSOL_MINT, TOKEN_PROGRAM_ID))
                event = self.trade_event() if venue == "pump" else ANCHOR_EVENT_TAG+bytes(16)
                rows += [self.inner(self.program, (self.roles["event_authority"],), event),
                         self.inner(self.roles["fee_program"], (self.roles["fee_config"], self.program), GET_FEES_TAG+b"\x00"+bytes(16)+bytes(8)+b"\x00")]
                self.groups.append({"index": outer_index, "instructions": rows})
            elif pid == TOKEN_PROGRAM_ID and instruction.data == b"\x09":
                self.move(self.quote, WALLET, self.post_native[self.quote])
                self.post_token.pop(self.quote)
        self.scenario = scenario(failed=failed)
        if failed:
            self.post_native = dict(self.pre_native)
            self.post_native[WALLET] -= FEE
            self.post_token = copy.deepcopy(self.pre_token)
            self.groups = []
        self.scenario.tx = {"slot": 108, "blockTime": NOW+5, "version": "legacy",
            "transaction": [base64.b64encode(self.wire).decode(), "base64"], "meta": {
                "err": self.scenario.status["err"], "fee": FEE,
                "preBalances": [self.pre_native[key] for key in self.keys],
                "postBalances": [self.post_native[key] for key in self.keys],
                "preTokenBalances": list(self.pre_token.values()), "postTokenBalances": list(self.post_token.values()),
                "loadedAddresses": {"writable": [], "readonly": []}, "innerInstructions": self.groups}}

    def token_row(self, key, mint, owner, amount, program):
        return {"accountIndex": self.index[key], "mint": mint, "owner": owner, "programId": program,
                "uiTokenAmount": {"amount": str(amount), "decimals": 9 if mint == WSOL_MINT else 6}}

    def trade_event(self, *, fee=500000, creator_fee=200000, buyback_fee=300000, token_amount=None, quote_amount=None):
        pk = lambda key: bytes(Pubkey.from_string(key))
        u = lambda value: struct.pack("<Q", value)
        name = ("buy_exact_quote_in_v2" if self.side == "BUY" else "sell_v2").encode()
        return (ANCHOR_EVENT_TAG+PUMP_TRADE_EVENT_TAG+pk(MINT)+u(self.principal)
            +u(self.actual_base if token_amount is None else token_amount)+bytes((self.side == "BUY",))+pk(WALLET)
            +struct.pack("<q", NOW+5)+bytes(32)+pk(self.roles["fee_recipient"])+u(50)+u(fee)+pk(plans.fx.CREATOR)
            +u(20)+u(creator_fee)+b"\x00"+bytes(24)+struct.pack("<q", NOW+5)+struct.pack("<I", len(name))+name
            +b"\x00"+u(0)+u(0)+u(30)+u(buyback_fee)+struct.pack("<I", 0)+pk(WSOL_MINT)
            +u(self.principal if quote_amount is None else quote_amount)+bytes(16))

    def token(self, key, mint, owner, amount, program):
        self.pre_token[key] = self.token_row(key, mint, owner, amount, program)
        self.pre_native[key] = R+amount if mint == WSOL_MINT else R

    @staticmethod
    def set_amount(row, value):
        row["uiTokenAmount"]["amount"] = str(value)

    def inner(self, program, accounts, data):
        return {"programIdIndex": self.index[program], "accounts": [self.index[key] for key in accounts],
                "data": b58(data), "stackHeight": 2}

    def move(self, source, destination, amount):
        self.post_native[source] -= amount
        self.post_native[destination] += amount

    def transfer(self, source, destination, owner, amount, mint, program):
        for key, sign in ((source, -1), (destination, 1)):
            row = self.post_token[key]
            self.set_amount(row, int(row["uiTokenAmount"]["amount"])+sign*amount)
        if mint == WSOL_MINT:
            self.move(source, destination, amount)
        return self.inner(program, (source, mint, destination, owner), b"\x0c"+struct.pack("<QB", amount, 9 if mint == WSOL_MINT else 6))

    def wallet_scenario(self, *, support=False, amount_override=None, tail=b""):
        value = WalletScenario()
        value.accounts = {WALLET: account(SYSTEM_PROGRAM_ID, lamports=FUNDING)}
        if not support:
            return value
        value.context = value.multiple_context = value.initial_slot = value.upper_slot = 116
        def transform(payload, envelope):
            if payload["method"] == "getBlock":
                envelope["result"].update(blockhash=bh(116), previousBlockhash=bh(114), parentSlot=114, blockHeight=95, blockTime=NOW+12)
            return envelope
        value.transform = transform
        for mint, token_program, decimals in ((MINT, self.token_program, 6), (WSOL_MINT, TOKEN_PROGRAM_ID, 9)):
            value.accounts[mint] = account(token_program, mint_data(decimals))
        for key in (self.base, self.quote):
            if key not in self.post_token:
                continue
            row = self.post_token[key]
            amount = int(row["uiTokenAmount"]["amount"]) if amount_override is None else amount_override
            reserve = R if row["mint"] == WSOL_MINT else None
            value.add_token(token=key, mint=row["mint"], program=row["programId"],
                data=token_data(row["mint"], WALLET, amount, reserve=reserve, tail=tail),
                mint_bytes=mint_data(row["uiTokenAmount"]["decimals"]), lamports=R+(amount if reserve else 0))
        return value

    def support(self, *, value=None, at=NOW+14):
        expected = (ExpectedTokenAccount(self.base, MINT, self.token_program), ExpectedTokenAccount(self.quote, WSOL_MINT, TOKEN_PROGRAM_ID))
        return WalletSupportInput(observe(value or self.wallet_scenario(support=True),
            request=WalletEvidenceRequest(WALLET, GENESIS, 108, expected), at=at), utc(at), 108)

    def setup(self, path):
        binding = LedgerDomain(GENESIS, WALLET, PROFILE.fingerprint, FUNDING, minimum_context_slot=100)
        repo = LedgerRepository.initialize(path, binding)
        baseline = observe(self.wallet_scenario(), request=WalletEvidenceRequest(WALLET, GENESIS, 100))
        ingest(repo, baseline)
        item = candidate(MINT)
        repo.receive_candidate(item, fence=repo.write_fence())
        root = item.trade_root(binding)
        action = PendingAction(root, item.content_digest, self.side, MINT, self.token_program,
            position_identity(binding, root, MINT), self.units, "UNVERIFIED_EXTERNAL_TERMS", digest("terms"),
            "EXTERNAL_POLICY", digest("policy"), "FINAL-A", None if self.side == "BUY" else "obligation", 1,
            (NOW+300)*1_000_000 if self.side == "BUY" else None, digest("deadline") if self.side == "BUY" else None)
        repo.stage_action(action, fence=repo.write_fence())
        prep = AttemptPreparation(action.action_id, action.content_digest, 1, self.message_bytes.hex(), self.plan.fingerprint,
            digest("message-policy"), BlockhashLeaseV01(self.plan.plan_id, RECENT, 102, 93, "confirmed", NOW*1_000_000),
            baseline.anchor, utc(NOW+1), "EXTERNAL_PREPARATION", digest("preparation"))
        repo.prepare_attempt(prep, fence=repo.write_fence())
        for stage, at in (("EXACT_SIMULATED", NOW+2), ("AUTHORIZED", NOW+3), ("SIGNED_DURABLE", NOW+4)):
            advance(repo, prep, stage, at=at)
        observation = transaction_observation(self.scenario)
        receipt = repo.ingest_chain_observation(prep.attempt_id, observation, ingestion_key="transaction",
            evaluated_at_utc=utc(NOW+10), fence=repo.write_fence())
        return repo, binding, action, prep, receipt


def main():
    with tempfile.TemporaryDirectory(prefix="live-ledger-settlement-") as temporary:
        directory = Path(temporary)
        results = {}
        for name, fixture in (("pump-buy", Fixture()), ("pump-sell", Fixture(side="SELL")),
            ("swap-buy", Fixture("swap")), ("swap-sell", Fixture("swap", "SELL")),
            ("retained-wsol", Fixture("swap", "SELL", retained=True)), ("failed", Fixture(failed=True))):
            repo, binding, action, prep, receipt = fixture.setup(directory / (name+".sqlite3"))
            with repo:
                support = fixture.support()
                result = attribute_settlement(binding, action, repo.attempt(prep.attempt_id), receipt, support)
                if result.proposal is None:
                    print(name, result, flush=True)
                check(name+"_actual_adapter_to_Ledger_finality", receipt.decision.observation_disposition ==
                      ("FINALIZED_FAILURE" if fixture.failed else "FINALIZED_SUCCESS"))
                check(name+"_whole_proposal", result.disposition == "PROPOSED")
                proposal = result.proposal
                check(name+"_total_meta_fee_exact_once", proposal.transaction_fee_lamports == FEE
                      and sum(c.units for c in proposal.components if c.kind == "NETWORK_FEE") == -FEE)
                check(name+"_native_components_reconcile", sum(c.units for c in proposal.components if c.asset == "SOL") == proposal.native_wallet_delta)
                check(name+"_WSOL_components_reconcile", sum(c.units for c in proposal.components if c.asset == "WSOL") == proposal.wsol_units_delta)
                check(name+"_no_application_or_lane_release", not proposal.applied and proposal.requires_atomic_custody_validation and repo.mutation_lane()["held"])
                check(name+"_deterministic_exact_retry", attribute_settlement(binding, action, repo.attempt(prep.attempt_id), receipt, support) == result)
                if fixture.failed:
                    check(name+"_only_actual_fee_component", len(proposal.components) == 1 and proposal.native_wallet_delta == -FEE
                          and proposal.base_units_delta == proposal.wsol_units_delta == 0)
                else:
                    check(name+"_actual_base_raw_units", proposal.base_units_delta == (fixture.actual_base if fixture.side == "BUY" else -fixture.units))
                    check(name+"_actual_principal_and_venue_fees", proposal.quote_principal_units == fixture.principal and proposal.venue_fee_units == fixture.venue_fees)
                    if fixture.side == "BUY":
                        check(name+"_actual_output_differs_from_original_quote", proposal.base_units_delta == fixture.minimum+17
                              and proposal.base_units_delta != fixture.plan_tuple[3].expected_base_amount)
                        check(name+"_first_wallet_volume_setup_is_program_outflow", any(c.kind == "VENUE_ACCOUNT_SETUP" and c.units == -1_844_400 for c in proposal.components))
                    else:
                        check(name+"_observed_residual_not_full_close", int(fixture.post_token[fixture.base]["uiTokenAmount"]["amount"]) == fixture.base_residual)
                    if name == "swap-buy":
                        check("first_swap_fee_ATAs_funded_as_external_accounts", sum(c.units for c in proposal.components if c.kind == "EXTERNAL_ACCOUNT_FUNDING") == -2*R)
                    if fixture.venue == "swap" and name != "retained-wsol":
                        check(name+"_exact_refund_and_no_retained_backing_double_count", proposal.account_refund_lamports == R and proposal.wsol_units_delta == 0)
                    if name == "retained-wsol":
                        check("retained_WSOL_never_becomes_native_SOL", proposal.native_wallet_delta == -FEE
                              and proposal.wsol_units_delta == fixture.principal-fixture.venue_fees and proposal.account_refund_lamports == 0)
                alternative = fixture.support(value=fixture.wallet_scenario(support=True, amount_override=7))
                alternative_result = attribute_settlement(binding, action, repo.attempt(prep.attempt_id), receipt, alternative)
                check(name+"_snapshot_amount_is_not_fill_truth", alternative_result.proposal is not None
                      and alternative_result.proposal.base_units_delta == proposal.base_units_delta
                      and alternative_result.proposal.wsol_units_delta == proposal.wsol_units_delta)
                results[name] = proposal.content_digest
            with LedgerRepository.reopen(directory / (name+".sqlite3"), binding) as reopened:
                check(name+"_reopen_original_receipt_same_proposal", attribute_settlement(binding, action, reopened.attempt(prep.attempt_id),
                      reopened.chain_receipt("transaction"), support).proposal == proposal)

        def denied(name, fixture, *, support_value=None, support_change=None, expected=None):
            repo, binding, action, prep, receipt = fixture.setup(directory / (name+".sqlite3"))
            with repo:
                support = fixture.support(value=support_value)
                if support_change:
                    support = support_change(support)
                result = attribute_settlement(binding, action, repo.attempt(prep.attempt_id), receipt, support)
                check(name+"_whole_unapplied", result.proposal is None and result.disposition in ("UNAPPLIED", "QUARANTINED"))
                check(name+"_lane_held_no_partial_application", repo.mutation_lane()["held"] and repo.audit()["chain_receipt_count"] == 1)
                if expected:
                    if expected not in result.reasons:
                        print(name, result, flush=True)
                    check(name+"_exact_reason", expected in result.reasons)
                check(name+"_sanitized", "DO_NOT_RETAIN" not in repr(result))

        fixture = Fixture(); fixture.scenario.tx["meta"]["postBalances"][0] += 1
        denied("native-not-conserved", fixture, expected="WHOLE_NATIVE_BALANCE_CONSERVATION_CONFLICT")
        fixture = Fixture(); meta = fixture.scenario.tx["meta"]
        meta["postBalances"][0] -= 1; meta["postBalances"][fixture.index[MINT]] += 1
        denied("unattributed-native-outflow", fixture)
        fixture = Fixture(); fixture.scenario.tx["meta"]["postTokenBalances"][0]["uiTokenAmount"]["amount"] = "1"
        denied("token-delta-conflict", fixture, expected="TOKEN_MOVEMENT_CONSERVATION_CONFLICT")
        fixture = Fixture(side="SELL"); fixture.scenario.tx["meta"]["preTokenBalances"] = [row for row in fixture.scenario.tx["meta"]["preTokenBalances"] if row["accountIndex"] != fixture.index[fixture.base]]
        denied("missing-pre-is-not-zero", fixture)
        fixture = Fixture(); fixture.scenario.tx["meta"]["postTokenBalances"] = [row for row in fixture.scenario.tx["meta"]["postTokenBalances"] if row["accountIndex"] != fixture.index[fixture.base]]
        denied("missing-post-is-not-zero", fixture)
        fixture = Fixture(); fixture.scenario.tx["meta"]["innerInstructions"] = []
        denied("missing-required-inner-coverage", fixture, expected="REQUIRED_INNER_INSTRUCTION_COVERAGE_MISSING")
        fixture = Fixture(); fixture.groups[0]["instructions"].pop(2)
        denied("missing-immutable-owner-call", fixture, expected="CANONICAL_ATA_CREATE_COVERAGE_INCOMPLETE")
        fixture = Fixture(); fixture.groups[0]["instructions"][0]["data"] = b58(b"\x15\x08\x00")
        denied("unknown-account-extension-query", fixture)
        fixture = Fixture(); fixture.groups[0]["instructions"][-1]["data"] = b58(b"\x12"+bytes(Pubkey.from_string(MINT)))
        denied("ATA-wrong-initialized-owner", fixture, expected="ATA_INITIALIZED_OWNER_CONFLICT")
        fixture = Fixture(); fixture.groups[0]["instructions"][-1]["stackHeight"] = None
        denied("nullable-stack-needs-later-evidence", fixture, expected="INNER_STACK_OR_PROGRAM_SHAPE_UNSUPPORTED")
        fixture = Fixture(); fixture.groups[-1]["instructions"][-2]["data"] = b58(bytes(24))
        denied("unknown-self-CPI-is-not-event", fixture, expected="ANCHOR_EVENT_ENVELOPE_UNSUPPORTED")
        fixture = Fixture(); fixture.groups[-1]["instructions"][-1]["data"] = b58(GET_FEES_TAG+bytes(25))
        denied("unknown-get-fees-length", fixture, expected="GET_FEES_ENVELOPE_UNSUPPORTED")
        fixture = Fixture(); fixture.groups[-1]["instructions"][-1]["data"] = b58(GET_FEES_TAG+b"\x02"+bytes(25))
        denied("noncanonical-get-fees-bool", fixture, expected="GET_FEES_ENVELOPE_UNSUPPORTED")
        fixture = Fixture(); fixture.groups[-1]["instructions"][-2]["data"] = b58(ANCHOR_EVENT_TAG+bytes(16))
        denied("Pump-fee-classification-event-missing", fixture, expected="EXACT_PUMP_TRADE_CLASSIFICATION_EVENT_REQUIRED")
        fixture = Fixture(); fixture.groups[-1]["instructions"][-2]["data"] = b58(fixture.trade_event()+b"\x00")
        denied("Pump-event-schema-trailing-data", fixture, expected="PUMP_TRADE_EVENT_TRAILING_BYTES_UNSUPPORTED")
        fixture = Fixture(); data = bytearray(fixture.trade_event()); data[64] = 2
        fixture.groups[-1]["instructions"][-2]["data"] = b58(bytes(data))
        denied("Pump-event-noncanonical-bool", fixture, expected="PUMP_TRADE_EVENT_BOOL_INVALID")
        fixture = Fixture(); fixture.groups[-1]["instructions"][-2]["data"] = b58(fixture.trade_event(token_amount=fixture.actual_base+1))
        denied("event-cannot-supply-different-fill", fixture, expected="PUMP_TRADE_EVENT_IDENTITY_OR_ACTUAL_UNITS_CONFLICT")
        fixture = Fixture(); fixture.groups[-1]["instructions"][-2]["data"] = b58(fixture.trade_event(creator_fee=200001))
        denied("event-fees-must-equal-actual-transfers", fixture, expected="PUMP_RECIPIENT_SETUP_OR_FEE_CLASSIFICATION_UNRESOLVED")
        fixture = Fixture(); meta = fixture.scenario.tx["meta"]; target = fixture.roles["creator_vault"]
        meta["postBalances"][0] -= 111; meta["postBalances"][fixture.index[target]] += 111
        fixture.groups[-1]["instructions"].insert(-2, fixture.inner(SYSTEM_PROGRAM_ID, (WALLET, target), struct.pack("<IQ", 2, 111)))
        denied("topup-inside-input-slack-is-not-a-fee", fixture, expected="PUMP_RECIPIENT_SETUP_OR_FEE_CLASSIFICATION_UNRESOLVED")
        for role in ("creator_vault", "bonding_curve"):
            fixture = Fixture(); meta = fixture.scenario.tx["meta"]; account_index = fixture.index[fixture.roles[role]]
            reduction = meta["preBalances"][account_index]-(R-1)
            meta["preBalances"][account_index] -= reduction; meta["postBalances"][account_index] -= reduction
            denied("missing-relative-rent-bound-"+role, fixture, expected="PUMP_NATIVE_SETUP_EXCLUSION_WITNESS_REQUIRED")
        fixture = Fixture(); meta = fixture.scenario.tx["meta"]
        meta["preBalances"][fixture.index[fixture.base]] = R; meta["postBalances"][0] += R
        meta["preTokenBalances"].append(fixture.token_row(fixture.base, MINT, WALLET, 0, TOKEN_PROGRAM_ID))
        fixture.groups[0]["instructions"] = []
        denied("existing-base-no-fresh-rent-witness", fixture, expected="PUMP_NATIVE_SETUP_EXCLUSION_WITNESS_REQUIRED")
        fixture = Fixture(side="SELL"); meta = fixture.scenario.tx["meta"]; target = fixture.roles["creator_vault"]
        meta["postBalances"][0] -= 1; meta["postBalances"][fixture.index[target]] += 1
        fixture.groups[-1]["instructions"].insert(0, fixture.inner(SYSTEM_PROGRAM_ID, (WALLET, target), struct.pack("<IQ", 2, 1)))
        denied("sell-native-topup-never-hidden-in-proceeds", fixture, expected="PUMP_SELL_NATIVE_SETUP_OR_DEBIT_UNSUPPORTED")
        fixture = Fixture("swap"); nested = next(row for row in fixture.groups[-1]["instructions"] if row["stackHeight"] == 3)
        nested["stackHeight"] = 4
        denied("no-generic-recursive-CPI", fixture)
        fixture = Fixture("swap"); row = next(row for row in fixture.groups[-1]["instructions"] if row["programIdIndex"] == fixture.index[ASSOCIATED_TOKEN_PROGRAM_ID])
        row["accounts"][2] = fixture.index[WALLET]
        denied("nested-fee-ATA-owner-conflict", fixture, expected="NESTED_ATA_IDENTITY_CONFLICT")
        fixture = Fixture(); fixture.groups[-1]["instructions"][0]["data"] = b58(struct.pack("<IQQ", 0, 1_844_400, 4097)+bytes(Pubkey.from_string(PUMP_PROGRAM_ID)))
        denied("volume-allocation-profile-bound", fixture, expected="VENUE_ACCOUNT_SETUP_UNSUPPORTED")
        fixture = Fixture(); fixture.groups[-1]["instructions"][0]["data"] = b58(struct.pack("<IQQ", 0, 1_844_400, 137)+bytes(Pubkey.from_string(TOKEN_PROGRAM_ID)))
        denied("volume-setup-wrong-program", fixture, expected="VENUE_ACCOUNT_SETUP_UNSUPPORTED")
        fixture = Fixture(failed=True); fixture.scenario.tx["meta"]["postBalances"][0] -= 1; fixture.scenario.tx["meta"]["postBalances"][fixture.index[MINT]] += 1
        denied("failed-unexplained-effect", fixture, expected="FAILED_TRANSACTION_HAS_FILL_OR_UNEXPLAINED_EFFECTS")
        fixture = Fixture(failed=True); fixture.scenario.tx["meta"]["innerInstructions"] = [{"index": fixture.venue_index,
            "instructions": [fixture.inner(PUMP_PROGRAM_ID, (fixture.roles["event_authority"],), bytes(24))]}]
        denied("failed-unknown-instruction", fixture, expected="FAILED_EVENT_SHAPE_UNSUPPORTED")
        fixture = Fixture(outer_transform=lambda rows: [rows[0], *rows])
        denied("duplicate-compute-limit", fixture, expected="COMPUTE_BUDGET_SHAPE_OR_DUPLICATE_UNSUPPORTED")
        fixture = Fixture(outer_transform=lambda rows: [Instruction(COMPUTE_BUDGET_ID, b"\x01"+bytes(4), []), *rows[1:]])
        denied("unknown-compute-variant", fixture, expected="COMPUTE_BUDGET_SHAPE_OR_DUPLICATE_UNSUPPORTED")
        fixture = Fixture(outer_transform=lambda rows: [Instruction(COMPUTE_BUDGET_ID, b"\x02"+bytes(8), []), *rows[1:]])
        denied("wrong-compute-length", fixture, expected="COMPUTE_BUDGET_SHAPE_OR_DUPLICATE_UNSUPPORTED")
        fixture = Fixture(); value = fixture.wallet_scenario(support=True, tail=b"\x02\x07\x00\x00\x00")
        denied("unsupported-wallet-token-extension", fixture, support_value=value, expected="ORIGINAL_ACCOUNT_SUPPORT_UNAVAILABLE_OR_UNSUPPORTED")
        fixture = Fixture(); value = fixture.wallet_scenario(support=True); value.program_failure = TOKEN_2022_PROGRAM_ID
        denied("incomplete-wallet-inventory", fixture, support_value=value, expected="ORIGINAL_ACCOUNT_SUPPORT_UNAVAILABLE_OR_UNSUPPORTED")
        fixture = Fixture(); value = fixture.wallet_scenario(support=True); value.genesis = bh(199)
        denied("wallet-domain-conflict", fixture, support_value=value)
        fixture = Fixture(); denied("new-stale-support-denied", fixture, support_change=lambda support: replace(support, evaluated_at_utc=utc(NOW+100)))
        fixture = Fixture(); denied("support-floor-behind-transaction", fixture, support_change=lambda support: replace(support, required_min_context_slot=107))
        fixture = Fixture(); value = fixture.wallet_scenario(support=True); value.accounts.pop(fixture.base); value.inventory[TOKEN_PROGRAM_ID] = []
        denied("post-account-support-absent", fixture, support_value=value, expected="POST_ACCOUNT_SUPPORT_MISSING")
        fixture = Fixture(token_program=TOKEN_2022_PROGRAM_ID)
        value = fixture.wallet_scenario(support=True, tail=b"\x02\x07\x00\x00\x00")
        denied("Token2022-ImmutableOwner-original-v0.1-stays-unsupported", fixture, support_value=value,
               support_change=lambda support: replace(support, observation=replace(support.observation, schema="live_wallet_account_evidence_v0.1")),
               expected="ORIGINAL_ACCOUNT_SUPPORT_UNAVAILABLE_OR_UNSUPPORTED")
        fixture = Fixture(); value = fixture.wallet_scenario(support=True)
        value.accounts[fixture.base] = account(TOKEN_PROGRAM_ID, token_data(MINT, WALLET, 7, delegate=MINT))
        denied("delegated-wallet-token-support-denied", fixture, support_value=value)
        fixture = Fixture(); value = fixture.wallet_scenario(support=True)
        value.accounts[fixture.base] = account(TOKEN_PROGRAM_ID, token_data(MINT, WALLET, 7, frozen=True))
        denied("frozen-wallet-token-support-denied", fixture, support_value=value)
        fixture = Fixture(); fixture.scenario.tx["meta"]["postTokenBalances"][0]["owner"] = WALLET
        denied("counterparty-token-owner-conflict", fixture, expected="TOKEN_BALANCE_IDENTITY_OR_DECIMALS_CONFLICT")
        fixture = Fixture(); fixture.groups[-1]["instructions"][2]["data"] = b58(struct.pack("<IQ", 2, 1))
        denied("native-CPI-amount-cannot-be-ignored", fixture, expected="PUMP_NATIVE_INSTRUCTION_AND_DIRECT_FLOW_CONFLICT")
        fixture = Fixture(); fixture.scenario.tx = None
        denied("finalized-status-without-metadata-no-proposal", fixture, expected="EXACT_TRANSACTION_RECEIPT_REQUIRED")

        for name, fixture in (("plain-Token2022-existing-SELL", Fixture(side="SELL", token_program=TOKEN_2022_PROGRAM_ID)),
                              ("failed-known-CPI-prefix", Fixture(failed=True)), ("exact-u64-balances", Fixture()),
                              ("different-actual-output", Fixture(extra_output=314159)), ("opaque-event-quote-fields", Fixture())):
            if name == "failed-known-CPI-prefix":
                prefix = Fixture().groups[0]
                fixture.scenario.tx["meta"]["innerInstructions"] = [{"index": prefix["index"], "instructions": prefix["instructions"][:2]}]
            if name == "exact-u64-balances":
                maximum = (1 << 64)-1
                pre = next(row for row in fixture.scenario.tx["meta"]["preTokenBalances"] if row["accountIndex"] == fixture.index[fixture.pool_base])
                post = next(row for row in fixture.scenario.tx["meta"]["postTokenBalances"] if row["accountIndex"] == fixture.index[fixture.pool_base])
                pre["uiTokenAmount"]["amount"] = str(maximum)
                post["uiTokenAmount"]["amount"] = str(maximum-fixture.actual_base)
                fixture.scenario.tx["meta"]["preBalances"][fixture.index[MINT]] = maximum
                fixture.scenario.tx["meta"]["postBalances"][fixture.index[MINT]] = maximum
            if name == "opaque-event-quote-fields":
                data = bytearray(fixture.trade_event(quote_amount=(1 << 64)-1)); data[48:56] = bytes(8)
                fixture.groups[-1]["instructions"][-2]["data"] = b58(bytes(data))
            repo, binding, action, prep, receipt = fixture.setup(directory / (name+".sqlite3"))
            with repo:
                result = attribute_settlement(binding, action, repo.attempt(prep.attempt_id), receipt, fixture.support())
                if result.proposal is None:
                    print(name, result, flush=True)
                check(name+"_actual_adapter_whole_proposal", result.proposal is not None)
                if name == "exact-u64-balances":
                    check("raw_u64_never_float_or_signed64", receipt.observation.transaction.pre_lamports[fixture.index[MINT]] == maximum
                          and result.proposal.base_units_delta == fixture.actual_base)
                if name == "opaque-event-quote-fields":
                    check("event-native-fields-never-become-fill-or-principal", result.proposal.quote_principal_units == fixture.principal
                          and result.proposal.base_units_delta == fixture.actual_base)
                if name == "different-actual-output":
                    check("actual_output_changes_without_quote_recomputation", result.proposal.base_units_delta == fixture.minimum+314159)
                    wrong_action = replace(action, input_units=action.input_units+1)
                    check("action_amount_conflict_has_no_proposal", attribute_settlement(binding, wrong_action, repo.attempt(prep.attempt_id),
                          receipt, fixture.support()).proposal is None)
                    wrong = copy.deepcopy(fixture.scenario)
                    wrong.tx["meta"]["fee"] += 1
                    conflict = repo.ingest_chain_observation(prep.attempt_id, transaction_observation(wrong, at=NOW+11),
                        ingestion_key="conflict", evaluated_at_utc=utc(NOW+11), fence=repo.write_fence())
                    check("later_L3_quarantine_blocks_old_positive_proposal", conflict.decision.resulting_state.quarantined
                          and attribute_settlement(binding, action, repo.attempt(prep.attempt_id), receipt, fixture.support()).proposal is None)

        fixture = Fixture(); full = copy.deepcopy(fixture.scenario)
        fixture.groups[0]["instructions"][-1]["stackHeight"] = None
        repo, binding, action, prep, receipt = fixture.setup(directory / "nullable-enrichment.sqlite3")
        with repo:
            support = fixture.support()
            initial = attribute_settlement(binding, action, repo.attempt(prep.attempt_id), receipt, support)
            check("nullable-original-receipt-unapplied", initial.proposal is None)
            later = repo.ingest_chain_observation(prep.attempt_id, transaction_observation(full, at=NOW+11),
                ingestion_key="enriched", evaluated_at_utc=utc(NOW+11), fence=repo.write_fence())
            enriched = attribute_settlement(binding, action, repo.attempt(prep.attempt_id), later, support)
            check("later-compatible-exact-metadata-resolves-attribution", enriched.proposal is not None
                  and later.decision.resulting_state.proof_evidence_digest == receipt.observation.content_digest)
            check("original-proof-receipt-remains-historical", repo.chain_receipt("transaction") == receipt)
        with LedgerRepository.reopen(directory / "nullable-enrichment.sqlite3", binding) as repo:
            check("aged-original-time-and-enrichment-replay", attribute_settlement(binding, action, repo.attempt(prep.attempt_id),
                  repo.chain_receipt("enriched"), support) == enriched)
    print(canonical_json({"status": "IMPLEMENTED_PENDING_PROJECT_REVIEW", "checks": len(CHECKS), "all_true": all(CHECKS.values()), "proposal_digests": results}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
