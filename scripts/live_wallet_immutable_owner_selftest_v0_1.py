"""A0 finite ImmutableOwner compatibility, original-version replay, temp DBs only."""
from __future__ import annotations

import base64
import json
import sys
import tempfile
from dataclasses import replace
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from phase5.shadow_domain_v0_1 import content_fingerprint
from phase5.shadow_unsigned_plan_simulation_v0_1 import BlockhashLeaseV01
from live.wallet_evidence_v0_1 import (SCHEMA, LEGACY_SCHEMA, ExpectedTokenAccount, WalletEvidenceRequest,
    ledger_account_evidence)
from live.ledger_domain_v0_1 import LedgerDomain
from live.ledger_repository_v0_1 import LedgerRepository
from live.ledger_evidence_codec_v0_1 import wallet_observation_to_json, wallet_observation_from_json, LedgerEvidenceCodecError
from live.ledger_actions_v0_1 import PendingAction, AttemptPreparation, position_identity
from live.ledger_settlement_v0_1 import WalletSupportInput, attribute_settlement
from live_ledger_baseline_selftest_v0_1 import observe, ingest
from live_ledger_actions_selftest_v0_1 import candidate
from live_ledger_finality_selftest_v0_1 import transaction_observation
import live_wallet_evidence_selftest_v0_1 as wf
import live_ledger_settlement_selftest_v0_1 as sf

TAIL = bytes.fromhex("0207000000")
CHECKS = {}


def check(name, condition):
    CHECKS[name] = bool(condition)
    if not condition:
        raise AssertionError(name)


def raises(kind, call):
    try:
        call()
    except kind:
        return True
    return False


def port(observation):
    return ledger_account_evidence(observation, expected_wallet=observation.request.wallet,
        expected_genesis=wf.GENESIS, expected_profile_fingerprint=wf.PROFILE.fingerprint,
        required_min_context_slot=observation.request.min_context_slot, now_utc=wf.utc())


def token_scenario(data, *, program=wf.TOKEN_2022_PROGRAM_ID, mint=wf.MINT22):
    return wf.Scenario().add_token(token=wf.TOKEN22, mint=mint, program=program, data=data)


def evidence_cases():
    request = WalletEvidenceRequest(wf.WALLET, wf.GENESIS, 100,
        (ExpectedTokenAccount(wf.TOKEN22, wf.MINT22, wf.TOKEN_2022_PROGRAM_ID),))
    for amount in (0, 17, (1 << 64)-1):
        observation = observe(token_scenario(wf.token_data(wf.MINT22, amount=amount, tail=TAIL)), request=request)
        value = port(observation)
        label = "amount-"+str(amount)
        check(label+"_actual_adapter_emits_new_version", observation.schema == SCHEMA == "live_wallet_account_evidence_v0.2")
        check(label+"_canonical_exact_account_supported", value.account_facts_usable and value.assessment.tokens[0].amount == amount
            and value.assessment.tokens[0].program == wf.TOKEN_2022_PROGRAM_ID and not value.assessment.tokens[0].reasons)
        payload = wallet_observation_to_json(observation)
        check(label+"_exact_schema_bytes_digest_roundtrip", wallet_observation_from_json(payload) == observation
            and observation.explicit_read.accounts[1].account.data[-5:] == TAIL)
        old = replace(observation, schema=LEGACY_SCHEMA)
        old_payload = wallet_observation_to_json(old)
        restored = wallet_observation_from_json(old_payload)
        check(label+"_original_v01_remains_unsupported", not port(restored).account_facts_usable
            and "UNSUPPORTED_TOKEN_EXTENSIONS_OR_LENGTH" in port(restored).assessment.reasons)
        check(label+"_schema_part_of_immutable_identity", old.content_digest != observation.content_digest and restored == old)
        envelope = json.loads(old_payload)
        envelope["observation"]["schema"] = SCHEMA
        check(label+"_schema_relabel_without_original_digest_denied", raises(LedgerEvidenceCodecError,
            lambda: wallet_observation_from_json(json.dumps(envelope, sort_keys=True, separators=(",", ":")))))
    malformed = {
        "missing-type": TAIL[1:], "missing-length": TAIL[:-1], "type-only": TAIL[:1],
        "wrong-account-type": b"\x01"+TAIL[1:], "uninitialized-account-type": b"\x00"+TAIL[1:],
        "unrelated-extension": b"\x02\x08\x00\x00\x00", "unknown-extension": b"\x02\xff\xff\x00\x00",
        "big-endian-type": b"\x02\x00\x07\x00\x00", "nonzero-length": b"\x02\x07\x00\x01\x00",
        "nonempty-extension": b"\x02\x07\x00\x01\x00\x00", "duplicate-extension": TAIL+TAIL[1:],
        "trailing-padding": TAIL+b"\x00", "second-unrelated-extension": TAIL+b"\x08\x00\x00\x00",
    }
    for name, tail in malformed.items():
        observation = observe(token_scenario(wf.token_data(wf.MINT22, tail=tail)), request=request)
        value = port(observation)
        check(name+"_whole_account_denied", not value.account_facts_usable and value.assessment.supported_shape == "UNSUPPORTED")
        check(name+"_sanitized_original_preserved", wallet_observation_from_json(wallet_observation_to_json(observation)) == observation
            and "DO_NOT_RETAIN" not in repr(observation))
    for name, changes in (("delegate", {"delegate": wf.OTHER}), ("delegated-amount", {"delegated": 1}),
            ("frozen", {"frozen": True}), ("external-close", {"close": wf.OTHER}), ("foreign-owner", {"owner": wf.OTHER}),
            ("native-reserve", {"reserve": 123})):
        observation = observe(token_scenario(wf.token_data(wf.MINT22, tail=TAIL, **changes)), request=request)
        check(name+"_existing_constraint_not_relaxed", not port(observation).account_facts_usable)
    raw = bytearray(wf.token_data(wf.MINT22, tail=TAIL)); raw[72:76] = (2).to_bytes(4, "little")
    check("invalid_base_coption_still_denied", not port(observe(token_scenario(bytes(raw)), request=request)).account_facts_usable)
    own_close = observe(token_scenario(wf.token_data(wf.MINT22, tail=TAIL, close=wf.WALLET)), request=request)
    check("wallet_close_authority_exactly_preserved", port(own_close).account_facts_usable and port(own_close).assessment.tokens[0].close_authority == wf.WALLET)
    spl_request = replace(request, expected_accounts=(replace(request.expected_accounts[0], program=wf.TOKEN_PROGRAM_ID),))
    check("legacy_SPL_cannot_claim_extension_support", not port(observe(token_scenario(wf.token_data(wf.MINT22, tail=TAIL),
        program=wf.TOKEN_PROGRAM_ID), request=spl_request)).account_facts_usable)
    failed = token_scenario(wf.token_data(wf.MINT22, tail=TAIL)); failed.program_failure = wf.TOKEN_PROGRAM_ID
    check("new_shape_does_not_upgrade_missing_coverage", not port(observe(failed, request=request)).account_facts_usable)
    incoherent = token_scenario(wf.token_data(wf.MINT22, tail=TAIL)); incoherent.multiple_context = 102
    check("new_shape_does_not_upgrade_incoherent_cut", not port(observe(incoherent, request=request)).account_facts_usable)
    conflict = token_scenario(wf.token_data(wf.MINT22, tail=TAIL))
    conflict.explicit_override[wf.TOKEN22] = wf.account(wf.TOKEN_2022_PROGRAM_ID, wf.token_data(wf.MINT22, amount=42, tail=TAIL))
    check("same_context_original_account_conflict_retained", port(observe(conflict, request=request)).assessment.inventory_coverage == "CONTRADICTORY")


def baseline_cases(directory):
    expected = (ExpectedTokenAccount(wf.TOKEN22, wf.MINT22, wf.TOKEN_2022_PROGRAM_ID),)
    binding = LedgerDomain(wf.GENESIS, wf.WALLET, wf.PROFILE.fingerprint, 5000000000,
        minimum_context_slot=100, expected_empty_token_accounts=expected)
    request = WalletEvidenceRequest(wf.WALLET, wf.GENESIS, 100, expected)
    observation = observe(token_scenario(wf.token_data(wf.MINT22, amount=0, tail=TAIL)), request=request)
    old = replace(observation, schema=LEGACY_SCHEMA)
    path = directory / "original-version.sqlite3"
    with LedgerRepository.initialize(path, binding) as repo:
        denied = ingest(repo, old, key="old-original")
        check("legacy_original_baseline_is_durable_denial", denied.decision.disposition == "QUARANTINED" and repo.baseline() is None)
    with LedgerRepository.reopen(path, binding) as repo:
        check("legacy_denial_survives_new_code_reopen", repo.receipt("old-original") == denied and repo.baseline() is None)
        accepted = ingest(repo, observation, key="new-original")
        check("new_version_existing_empty_ImmutableOwner_baseline", accepted.decision.disposition == "ESTABLISHED"
            and repo.consumer_snapshot()["accounts"][0].observed_lamports == 2039280
            and repo.consumer_snapshot()["accounts"][0].units == 0)
        check("new_observation_does_not_rewrite_historical_unsupported", repo.receipt("old-original") == denied)
        digest = repo.audit()["custody_digest"]
    with LedgerRepository.reopen(path, binding) as repo:
        check("mixed_versions_replay_original_evaluations", repo.audit()["custody_digest"] == digest
            and repo.receipt("old-original") == denied and repo.baseline() == accepted)
    return {"legacy_denial": denied.content_digest, "new_baseline": accepted.content_digest, "custody": digest}


def custody_case(directory, *, legacy=False, hostile=False):
    fixture = sf.Fixture("swap", token_program=wf.TOKEN_2022_PROGRAM_ID)
    # Actual independent chain fixture: the canonical ATA already exists empty.
    # The idempotent outer ATA call creates nothing and moves no setup lamports.
    meta = fixture.scenario.tx["meta"]
    for group in fixture.groups:
        outer = fixture.message.instructions[group["index"]]
        if fixture.keys[outer.program_id_index] == sf.ASSOCIATED_TOKEN_PROGRAM_ID and fixture.keys[outer.accounts[1]] == fixture.base:
            group["instructions"] = []
    meta["preTokenBalances"].append(fixture.token_row(fixture.base, sf.MINT, sf.WALLET, 0, wf.TOKEN_2022_PROGRAM_ID))
    meta["preBalances"][fixture.index[fixture.base]] = sf.R
    meta["postBalances"][0] += sf.R
    expected = (ExpectedTokenAccount(fixture.base, sf.MINT, wf.TOKEN_2022_PROGRAM_ID),)
    binding = LedgerDomain(wf.GENESIS, sf.WALLET, wf.PROFILE.fingerprint, sf.FUNDING,
        minimum_context_slot=100, expected_empty_token_accounts=expected)
    value = wf.Scenario()
    value.accounts = {sf.WALLET: wf.account(sf.SYSTEM_PROGRAM_ID, lamports=sf.FUNDING)}
    value.add_token(token=fixture.base, mint=sf.MINT, program=wf.TOKEN_2022_PROGRAM_ID,
        data=wf.token_data(sf.MINT, sf.WALLET, 0, tail=TAIL))
    baseline = observe(value, request=WalletEvidenceRequest(sf.WALLET, wf.GENESIS, 100, expected))
    name = "legacy-support" if legacy else "hostile-support" if hostile else "canonical-support"
    path = directory / (name+".sqlite3")
    with LedgerRepository.initialize(path, binding) as repo:
        ingest(repo, baseline)
        item = candidate(sf.MINT)
        root = repo.receive_candidate(item, fence=repo.write_fence())
        # Unadmitted external L2 fixture terms; no Authority decision or grant.
        action = PendingAction(root, item.content_digest, "BUY", sf.MINT, wf.TOKEN_2022_PROGRAM_ID,
            position_identity(binding, root, sf.MINT), fixture.units, "EXTERNAL_FIXTURE_TERMS", sf.digest("terms"),
            "EXTERNAL_POLICY", sf.digest("policy"), "FINAL-A", None, 1, item.generated_at_us + 15_000_000, sf.digest("deadline"))
        repo.stage_action(action, fence=repo.write_fence())
        prep = AttemptPreparation(action.action_id, action.content_digest, 1, fixture.message_bytes.hex(), fixture.plan.fingerprint,
            sf.digest("message-policy"), BlockhashLeaseV01(fixture.plan.plan_id, sf.RECENT, 102, 93, "confirmed", wf.NOW*1_000_000),
            baseline.anchor, wf.utc(wf.NOW+1), "EXTERNAL_PREPARATION", sf.digest("preparation"))
        repo.prepare_attempt(prep, fence=repo.write_fence())
        for target, at in (("EXACT_SIMULATED", wf.NOW+2), ("AUTHORIZED", wf.NOW+3), ("SIGNED_DURABLE", wf.NOW+4)):
            sf.advance(repo, prep, target, at=at)
        chain = repo.ingest_chain_observation(prep.attempt_id, transaction_observation(fixture.scenario), ingestion_key="transaction",
            evaluated_at_utc=wf.utc(wf.NOW+10), fence=repo.write_fence())
        support = fixture.support(value=fixture.wallet_scenario(support=True, tail=TAIL+(TAIL[1:] if hostile else b"")))
        if legacy:
            support = replace(support, observation=replace(support.observation, schema=LEGACY_SCHEMA))
        applied = repo.apply_settlement(prep.attempt_id, chain_receipt_key="transaction", support=support,
            ingestion_key="apply", recorded_at_utc=wf.utc(wf.NOW+20), fence=repo.write_fence())
        if legacy or hostile:
            check(name+"_whole_application_unapplied", applied.decision.proposal is None and repo.audit()["posting_count"] == 0
                and repo.mutation_lane()["held"] and not repo.consumer_snapshot()["positions"])
        else:
            check("canonical170_actual_fill_applied", applied.decision.disposition == "FINALIZED_SUCCESS_APPLIED"
                and repo.consumer_snapshot()["positions"][0].remaining_units == fixture.actual_base
                and fixture.actual_base != fixture.plan_tuple[3].expected_base_amount)
            check("canonical170_actual_native_fee_and_lamport_facts", repo.consumer_snapshot()["funding"].native_lamports == meta["postBalances"][0]
                and repo.consumer_snapshot()["funding"].network_fees_paid_lamports == sf.FEE
                and next(account for account in repo.consumer_snapshot()["accounts"] if account.pubkey == fixture.base).observed_lamports == sf.R)
        check(name+"_original_support_is_persisted_exactly", repo.wallet_support(support.digest) == support)
        digest = repo.audit()["custody_digest"]
    with LedgerRepository.reopen(path, binding) as repo:
        check(name+"_original_application_replay", repo.audit()["custody_digest"] == digest and repo.application_receipt("apply") == applied
            and repo.wallet_support(support.digest) == support)
    return {"application": applied.content_digest, "custody": digest, "support": support.digest}


def main():
    evidence_cases()
    with tempfile.TemporaryDirectory(prefix="live-wallet-immutable-owner-") as temporary:
        directory = Path(temporary)
        results = {"baseline": baseline_cases(directory), "canonical": custody_case(directory),
            "legacy": custody_case(directory, legacy=True), "hostile": custody_case(directory, hostile=True)}
    print(json.dumps({"status": "IMPLEMENTED_PENDING_PROJECT_REVIEW", "checks": len(CHECKS), "all_checks": all(CHECKS.values()),
        "check_digest": content_fingerprint(CHECKS), "results": results}, sort_keys=True))


if __name__ == "__main__":
    main()
