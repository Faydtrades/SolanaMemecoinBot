"""L6 concrete producer inputs -> atomic Ledger groups. Inert public/temp fixtures."""
from __future__ import annotations

import copy
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
from dataclasses import asdict, replace, FrozenInstanceError
from pathlib import Path
from contextlib import closing

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from phase5.shadow_domain_v0_1 import content_fingerprint, canonical_json
from phase5.shadow_unsigned_plan_simulation_v0_1 import BlockhashLeaseV01
from live.ledger_domain_v0_1 import LedgerDomain, LedgerContractError
from live.ledger_repository_v0_1 import LedgerRepository, LedgerConflict, LedgerJournalError
from live.ledger_actions_v0_1 import PendingAction, AttemptPreparation, position_identity
from live.ledger_ports_v0_1 import (AdmissionInput, NativeReservationVector, ProtectiveHandoffInput,
    RetirementInput, DryTerminalInput, port_receipt_from_record)
from live.ledger_settlement_v0_1 import WalletSupportInput, attribute_settlement
from live.ledger_finality_v0_1 import adjudicate_chain_observation
from live.wallet_evidence_v0_1 import WalletEvidenceRequest
from live_ledger_actions_selftest_v0_1 import candidate, stage
from live_ledger_baseline_selftest_v0_1 import observe, ingest
from live_ledger_finality_selftest_v0_1 import transaction_observation
from live_wallet_evidence_selftest_v0_1 import GENESIS, PROFILE, NOW, utc
import live_ledger_settlement_selftest_v0_1 as sf
import live_ledger_custody_selftest_v0_1 as cf

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


def cut(repo):
    return repo.consumer_snapshot()["consumer_cut"]


def admission(repo, action, at=NOW):
    return AdmissionInput(cut(repo), action, NativeReservationVector(action.input_units, 12000, 1500000,
        12000000, 24000, 5000000), "FIXTURE_EXTERNAL_AUTHORITY_DECISION", content_fingerprint("external-decision"), utc(at))


def begin(path, *, mode="LIVE", venue="pump", failed=False, admitted=True, staged=False, suffix="a"):
    fixture = sf.Fixture(venue, failed=failed)
    fixture.mint = sf.MINT
    binding = LedgerDomain(GENESIS, sf.WALLET, PROFILE.fingerprint, sf.FUNDING, minimum_context_slot=100, mode=mode)
    repo = LedgerRepository.initialize(path, binding)
    baseline = observe(fixture.wallet_scenario(), request=WalletEvidenceRequest(sf.WALLET, GENESIS, 100))
    ingest(repo, baseline)
    item = candidate(sf.MINT, suffix=suffix)
    root = repo.receive_candidate(item, fence=repo.write_fence())
    action = PendingAction(root, item.content_digest, "BUY", sf.MINT, fixture.token_program,
        position_identity(binding, root, sf.MINT), fixture.units, "FIXTURE_EXTERNAL_TERMS", sf.digest("terms"),
        "EXTERNAL_POLICY", sf.digest("policy"), "FINAL-A", None, 1, (NOW+300)*1_000_000, sf.digest("deadline"))
    if staged:
        repo.stage_action(action, fence=repo.write_fence())
    supplied = admission(repo, action)
    if admitted:
        repo.admit(supplied, ingestion_key="admit", fence=repo.write_fence())
    return repo, fixture, action, supplied


def prepare(repo, fixture, action, *, ordinal=1):
    return AttemptPreparation(action.action_id, action.content_digest, ordinal, fixture.message_bytes.hex(), fixture.plan.fingerprint,
        sf.digest("message-policy"), BlockhashLeaseV01(fixture.plan.plan_id, sf.RECENT, 102, 93, "confirmed", NOW*1_000_000),
        repo.baseline().observation.anchor, utc(NOW+1), "FIXTURE_EXTERNAL_PREPARATION", sf.digest("preparation"))


def finalize(repo, fixture, action):
    prep = prepare(repo, fixture, action)
    repo.prepare_attempt(prep, fence=repo.write_fence())
    for target, offset in (("EXACT_SIMULATED", 2), ("AUTHORIZED", 3), ("SIGNED_DURABLE", 4)):
        sf.advance(repo, prep, target, at=NOW+offset)
    observation = transaction_observation(fixture.scenario)
    receipt = repo.ingest_chain_observation(prep.attempt_id, observation, ingestion_key="transaction",
        evaluated_at_utc=utc(NOW+10), fence=repo.write_fence())
    support, _, _ = cf.composed_support(repo, fixture)
    return prep, receipt, support


def handoff(repo, action, prep, chain, support, *, due=False, at=None):
    proposal = attribute_settlement(repo.domain, action, repo.attempt(prep.attempt_id), chain, support).proposal
    assert proposal is not None
    item = repo.candidate(action.root_id)
    at = NOW+20 if due else NOW+14 if at is None else at
    deadline = item.generated_at_us+15_000_000  # Locked FINAL-A fixture, no Runtime evaluator.
    return ProtectiveHandoffInput(cut(repo), action.root_id, action.position_id, item, action.selected_exit_track,
        action.policy_digest, proposal.signature, proposal.content_digest, proposal.base_units_delta,
        item.generated_at_us, deadline, deadline+1, "EXTERNAL_ORIGINAL_FALLBACK", sf.digest("fallback"),
        "EXTERNAL_RUNTIME_BINDING", sf.digest("runtime-binding"), "DUE" if due else "MONITORING",
        "FALLBACK" if due else "NONE", deadline+1 if due else None, sf.digest("trigger") if due else None,
        chain.content_digest, utc(at), "EXTERNAL_RUNTIME_OBLIGATION", sf.digest("obligation"), utc(at))


def acquisition(repo, fixture, action, *, due=False):
    prep, chain, support = finalize(repo, fixture, action)
    supplied = handoff(repo, action, prep, chain, support, due=due)
    group = repo.apply_settlement_with_ports(prep.attempt_id, chain_receipt_key="transaction", support=support,
        ingestion_key="acquire-protect", recorded_at_utc=supplied.recorded_at_utc, handoff=supplied, fence=repo.write_fence())
    return prep, chain, support, supplied, group


def retirement(repo, action, prep, support, *, reason="FULLY_REDUCED", at=NOW+20):
    support = replace(support, evaluated_at_utc=utc(at))
    supplied = RetirementInput(cut(repo), action.root_id, repo.reservation(action.root_id).reservation_id,
        action.position_id, prep.attempt_id, reason, support.digest, "EXTERNAL_AUTHORITY_RETIREMENT",
        sf.digest("retirement"), utc(at))
    return supplied, support


def normal_cases(directory):
    digests = {}
    for venue in ("pump", "swap"):
        path = directory / (venue+".sqlite3")
        repo, fixture, action, original = begin(path, venue=venue)
        binding = repo.domain
        with repo:
            admitted = repo.port_receipt("admit")
            check(venue+"_atomic_admission_terms_reservation_acceptance", repo.action(action.action_id) == action
                and repo.inbox_disposition(action.root_id) == "ACCEPTED" and admitted.created_action_digest == action.content_digest
                and len(repo.consumer_snapshot()["reservations"]) == 1 and not admitted.has_real_authority_grant)
            check(venue+"_exact_admission_retry", repo.admit(original, ingestion_key="another-delivery", fence=repo.write_fence()) == admitted)
            prep, chain, support, supplied, group = acquisition(repo, fixture, action, due=venue == "swap")
            position = repo.consumer_snapshot()["positions"][0]
            check(venue+"_actual_acquisition_and_protection_one_commit", position.usable and position.remaining_units == fixture.actual_base
                and repo.protection(action.position_id).sequence == repo.application_receipt(group.application_key).sequence == group.sequence
                and group.application_disposition == "FINALIZED_SUCCESS_APPLIED" and not repo.mutation_lane()["held"])
            check(venue+"_original_obligation_state", repo.protection(action.position_id).obligation_state == ("DUE" if venue == "swap" else "MONITORING"))
            check(venue+"_whole_group_exact_retry", repo.apply_settlement_with_ports(prep.attempt_id, chain_receipt_key="transaction", support=support,
                ingestion_key="acquire-protect", recorded_at_utc=supplied.recorded_at_utc, handoff=supplied, fence=repo.write_fence()) == group)
            check(venue+"_snapshot_immutable", raises(FrozenInstanceError, lambda: setattr(position, "usable", False)))
            repo.audit()
            partial = fixture.actual_base//3
            next_fixture, sell, sell_prep, sell_chain, sell_support, at = cf.next_step(repo, fixture, support.observation.anchor,
                number=50, quantity=partial, position_id=action.position_id, protective_handoff=supplied)
            reduced = cf.apply(repo, sell_prep, sell_support, chain="chain-50", key="partial", at=at+20)
            check(venue+"_actual_partial_preserves_protection", reduced.decision.disposition == "FINALIZED_SUCCESS_APPLIED"
                and repo.position_history(action.position_id).remaining_units == fixture.actual_base-partial
                and repo.protection(action.position_id).obligation_state == supplied.obligation_state)
            final_fixture, final_sell, final_prep, final_chain, final_support, final_at = cf.next_step(repo, next_fixture, sell_support.observation.anchor,
                number=51, quantity=fixture.actual_base-partial, position_id=action.position_id, protective_handoff=supplied, action_ordinal=2)
            intent, comparison = retirement(repo, final_sell, final_prep, final_support, at=final_at+20)
            closed = repo.apply_settlement_with_ports(final_prep.attempt_id, chain_receipt_key="chain-51", support=final_support,
                ingestion_key="sell-retire", recorded_at_utc=utc(final_at+20), retirement=intent, retirement_support=comparison, fence=repo.write_fence())
            check(venue+"_whole_full_reduction_and_retirement", closed.application_disposition == "FINALIZED_SUCCESS_APPLIED"
                and closed.retirement_disposition == "RETIRED" and not repo.consumer_snapshot()["positions"]
                and not repo.consumer_snapshot()["reservations"] and repo.position_history(action.position_id).remaining_units == 0
                and repo.inbox_disposition(action.root_id) == "RETIRED")
            historic_application = repo.application_receipt(closed.application_key)
            actual_native = repo.consumer_snapshot()["funding"].native_lamports
            late_value = copy.deepcopy(final_fixture.scenario)
            late_value.tx["meta"]["fee"] += 1
            late_value.tx["meta"]["postBalances"][0] -= 1
            late_observation = transaction_observation(late_value, at=final_at+21,
                request=cf.TransactionRequest(GENESIS, cf.sig(51), final_prep.finalized_lower_anchor.slot))
            repo.ingest_chain_observation(final_prep.attempt_id, late_observation, ingestion_key="after-retirement-conflict",
                evaluated_at_utc=utc(final_at+21), fence=repo.write_fence())
            check(venue+"_late_retired_contradiction_preserves_history_money", repo.port_receipt("sell-retire") == closed
                and repo.application_receipt(closed.application_key) == historic_application
                and repo.consumer_snapshot()["funding"].native_lamports == actual_native)
            check(venue+"_late_retired_contradiction_quarantines_no_capacity_grant", bool(repo.consumer_snapshot()["funding"].quarantine_reasons)
                and not repo.consumer_snapshot()["authority_current"] and not closed.has_real_authority_grant
                and repo.attempt(final_prep.attempt_id).current_disposition == "CUSTODY_QUARANTINED"
                and not repo.mutation_lane()["held"])
            digests[venue] = repo.audit()["custody_digest"]
        with LedgerRepository.reopen(path, binding) as repo:
            check(venue+"_original_groups_replay", repo.audit()["custody_digest"] == digests[venue]
                and repo.port_receipt("sell-retire") == closed and repo.wallet_support(comparison.digest) == comparison
                and repo.protection(action.position_id).obligation_state == "RETIRED_BY_FLAT_PROOF"
                and repo.application_receipt(closed.application_key) == historic_application
                and bool(repo.consumer_snapshot()["funding"].quarantine_reasons))
    return digests


def admission_cases(directory):
    for staged in (False, True):
        name = "staged" if staged else "unstaged"
        path = directory / (name+"-admission.sqlite3")
        repo, fixture, action, supplied = begin(path, admitted=False, staged=staged)
        binding = repo.domain
        with repo:
            starting = repo.audit()
            for label, invalid in (("domain", replace(supplied, cut=replace(supplied.cut, economic_domain_id="0"*64))),
                    ("custody-cut", replace(supplied, cut=replace(supplied.cut, custody_digest="0"*64))),
                    ("source", replace(supplied, action=replace(action, candidate_digest="0"*64)))):
                check(name+"_denies_"+label, raises(LedgerContractError, lambda invalid=invalid: repo.admit(invalid, ingestion_key="bad", fence=repo.write_fence())))
                check(name+"_"+label+"_no_partial_terms_reservation", repo.audit() == starting)
            receipt = repo.admit(supplied, ingestion_key="admit", fence=repo.write_fence())
            check(name+"_exact_optional_action_child", (receipt.created_action_digest is None) is staged
                and repo.audit()["revision"] == starting["revision"]+1 and repo.audit()["action_count"] == 1)
            check(name+"_changed_amount_policy_no_new_root", raises(LedgerContractError, lambda: repo.admit(
                replace(supplied, action=replace(action, policy_digest="1"*64)), ingestion_key="changed", fence=repo.write_fence())))
            check(name+"_nonacceptance_cannot_drop_reservation", raises(LedgerConflict, lambda: repo.record_nonacceptance(action.root_id, "REJECTED",
                external_reference="EXTERNAL", external_record_digest="1"*64, recorded_at_utc=utc(NOW+1), idempotency_key="reject", fence=repo.write_fence())))
            check(name+"_codec_rejects_unknown_fields", raises(TypeError, lambda: port_receipt_from_record({**receipt.to_record(), "untrusted": True})))
            old_fence = repo.write_fence()
            digest = repo.audit()["custody_digest"]
        with LedgerRepository.reopen(path, binding) as repo:
            check(name+"_reopen_admission_replay", repo.audit()["custody_digest"] == digest and repo.port_receipt("admit") == receipt)
            check(name+"_old_generation_denied_even_exact_retry", raises(LedgerConflict, lambda: repo.admit(supplied, ingestion_key="admit", fence=old_fence)))
    path = directory / "multiple-reservations.sqlite3"
    repo, fixture, action, _ = begin(path)
    with repo:
        first_cut = cut(repo)
        item = candidate(cf.key(22), suffix="distinct")
        root = repo.receive_candidate(item, fence=repo.write_fence())
        second_action = replace(action, root_id=root, candidate_digest=item.content_digest, mint=item.mint,
            position_id=position_identity(repo.domain, root, item.mint), input_units=action.input_units+17)
        supplied = admission(repo, second_action)
        maximum = (1 << 64)-1
        supplied = replace(supplied, reservation=replace(supplied.reservation, network_fee_lamports=maximum, protective_setup_lamports=maximum))
        check("common_funding_cut_includes_inbox_append", raises(LedgerContractError, lambda: repo.admit(replace(supplied, cut=first_cut), ingestion_key="stale-cut", fence=repo.write_fence())))
        repo.admit(supplied, ingestion_key="second-admission", fence=repo.write_fence())
        check("two_distinct_root_mint_reservations_no_singleton_policy", len(repo.consumer_snapshot()["reservations"]) == 2
            and repo.consumer_snapshot()["funding"].native_lamports == sf.FUNDING)
        check("exact_u64_external_headroom_is_not_float_or_allocation", repo.reservation(root).admission.reservation.network_fee_lamports == maximum)
        repo.audit()


def protection_cases(directory):
    path = directory / "protection-denials.sqlite3"
    repo, fixture, action, _ = begin(path)
    with repo:
        prep, chain, support = finalize(repo, fixture, action)
        supplied = handoff(repo, action, prep, chain, support, due=True)
        original = repo.audit()
        variants = (("track", replace(supplied, selected_track="FINAL-B")),
            ("position", replace(supplied, position_id="1"*64)),
            ("reference", replace(supplied, candidate=replace(supplied.candidate, reference_price_text="0.000001"))),
            ("proof", replace(supplied, acquisition_proposal_digest="1"*64)),
            ("policy", replace(supplied, selected_policy_digest="1"*64)))
        for name, value in variants:
            check("handoff_wrong_"+name, raises(LedgerContractError, lambda value=value: repo.apply_settlement_with_ports(prep.attempt_id,
                chain_receipt_key="transaction", support=support, ingestion_key="bad-"+name, recorded_at_utc=value.recorded_at_utc,
                handoff=value, fence=repo.write_fence())))
            check("handoff_wrong_"+name+"_whole_group_rolled_back", repo.audit() == original and repo.mutation_lane()["held"])
        check("deadline_fire_boundary_distinct", raises(LedgerContractError, lambda: replace(supplied, fallback_fire_boundary_us=supplied.fallback_deadline_us)))
        check("late_monitoring_placeholder_denied", raises(LedgerContractError, lambda: replace(supplied,
            obligation_state="MONITORING", trigger_kind="NONE", trigger_at_us=None, trigger_evidence_digest=None)))
        applied = cf.apply(repo, prep, support)
        check("standalone_acquisition_owned_but_unusable", not repo.consumer_snapshot()["positions"][0].usable)
        supplied = replace(supplied, cut=cut(repo))
        group = repo.record_protective_handoff(supplied, ingestion_key="protect", fence=repo.write_fence())
        check("standalone_due_handoff_and_usability_atomic", repo.consumer_snapshot()["positions"][0].usable
            and repo.protection(action.position_id).obligation_state == "DUE")
        check("bound_due_obligation_cannot_be_replaced", raises(LedgerContractError, lambda: repo.record_protective_handoff(
            replace(supplied, runtime_binding_digest="1"*64), ingestion_key="replacement", fence=repo.write_fence())))
        wrong_sell = replace(action, side="SELL", input_units=1, obligation_id="wrong", claimed_entry_deadline_us=None, deadline_binding_digest=None)
        check("admitted_reduction_requires_original_protection_obligation", raises(LedgerContractError, lambda: repo.stage_action(wrong_sell, fence=repo.write_fence())))
        check("same_identity_duplicate_group_preserves_original_receipt", repo.record_protective_handoff(supplied, ingestion_key="another", fence=repo.write_fence()) == group)
        _, wallet, request = cf.composed_support(repo, fixture)
        wallet.accounts[sf.WALLET]["lamports"] += 1
        incoming = WalletSupportInput(observe(wallet, request=request, at=NOW+14), utc(NOW+20), request.min_context_slot)
        repo.compare_wallet_observation(incoming, ingestion_key="late-mismatch", fence=repo.write_fence())
        check("current_quarantine_masks_historical_protected_usability", not repo.consumer_snapshot()["positions"][0].usable
            and repo.position_history(action.position_id).has_protective_handoff and repo.application_receipt("apply") == applied)
        repo.audit()
    path = directory / "quarantined-before-handoff.sqlite3"
    repo, fixture, action, _ = begin(path)
    with repo:
        prep, chain, support = finalize(repo, fixture, action)
        supplied = handoff(repo, action, prep, chain, support, due=True)
        cf.apply(repo, prep, support)
        _, wallet, request = cf.composed_support(repo, fixture)
        wallet.accounts[sf.WALLET]["lamports"] += 1
        incoming = WalletSupportInput(observe(wallet, request=request, at=NOW+14), utc(NOW+20), request.min_context_slot)
        repo.compare_wallet_observation(incoming, ingestion_key="mismatch", fence=repo.write_fence())
        supplied = replace(supplied, cut=cut(repo))
        check("handoff_cannot_ignore_current_funding_quarantine", raises(LedgerContractError, lambda: repo.record_protective_handoff(
            supplied, ingestion_key="protect", fence=repo.write_fence())) and not repo.consumer_snapshot()["positions"][0].usable)
        repo.audit()


def retirement_cases(directory):
    results = {}
    for shared in (False, True):
        for kind in ("matched", "incomplete", "contradictory"):
            name = ("shared-" if shared else "standalone-")+kind
            path = directory / (name+".sqlite3")
            repo, fixture, action, _ = begin(path, failed=True)
            binding = repo.domain
            with repo:
                prep, chain, support = finalize(repo, fixture, action)
                if not shared:
                    cf.apply(repo, prep, support)
                _, wallet, request = cf.composed_support(repo, fixture)
                if kind == "incomplete":
                    wallet.program_failure = sf.TOKEN_2022_PROGRAM_ID
                elif kind == "contradictory":
                    wallet.accounts[sf.WALLET]["lamports"] += 11
                original = WalletSupportInput(observe(wallet, request=request, at=NOW+14), utc(NOW+20), request.min_context_slot)
                intent, original = retirement(repo, action, prep, original, reason="FAILED_NO_ACQUISITION")
                if shared:
                    result = repo.apply_settlement_with_ports(prep.attempt_id, chain_receipt_key="transaction", support=support,
                        ingestion_key="retire", recorded_at_utc=utc(NOW+20), retirement=intent, retirement_support=original, fence=repo.write_fence())
                else:
                    result = repo.retire(intent, original, ingestion_key="retire", fence=repo.write_fence())
                check(name+"_finite_retirement_result", result.retirement_disposition == ("RETIRED" if kind == "matched" else "WITHHELD"))
                check(name+"_original_wallet_evidence_retained", repo.wallet_support(original.digest) == original
                    and repo.consumer_snapshot()["latest_comparison"].input_digest == original.digest)
                check(name+"_exact_fee_only_not_undone_by_comparison", repo.consumer_snapshot()["funding"].native_lamports == sf.FUNDING-sf.FEE
                    and repo.audit()["posting_count"] == 1 and not repo.consumer_snapshot()["positions"])
                check(name+"_held_reservation_only_released_on_positive_match", (repo.reservation(action.root_id).status == "RETIRED") is (kind == "matched"))
                check(name+"_positive_mismatch_quarantines_without_inventing_capital", bool(repo.consumer_snapshot()["funding"].quarantine_reasons) is (kind == "contradictory"))
                if shared:
                    check(name+"_both_subresults_explicit", result.application_disposition == "FINALIZED_FAILURE_APPLIED")
                results[name] = repo.audit()["custody_digest"]
            with LedgerRepository.reopen(path, binding) as repo:
                check(name+"_withholding_reason_and_evidence_replay", repo.audit()["custody_digest"] == results[name] and repo.port_receipt("retire") == result)
    path = directory / "incomplete-settlement-and-retirement.sqlite3"
    repo, fixture, action, _ = begin(path)
    with repo:
        fixture.scenario.tx["meta"]["innerInstructions"] = []
        prep, chain, support = finalize(repo, fixture, action)
        intent, comparison = retirement(repo, action, prep, support, reason="FAILED_NO_ACQUISITION")
        result = repo.apply_settlement_with_ports(prep.attempt_id, chain_receipt_key="transaction", support=support,
            ingestion_key="whole-unapplied", recorded_at_utc=utc(NOW+20), retirement=intent, retirement_support=comparison, fence=repo.write_fence())
        check("incomplete_attribution_and_retirement_apply_nothing", result.application_disposition == "UNAPPLIED"
            and result.retirement_disposition == "WITHHELD" and repo.audit()["posting_count"] == 0
            and repo.consumer_snapshot()["funding"].native_lamports == sf.FUNDING and repo.mutation_lane()["held"])
        check("pending_effects_remain_unknown_not_external_transfer", repo.consumer_snapshot()["latest_comparison"].disposition == "PENDING_EFFECTS_UNKNOWN")
        repo.audit()
    path = directory / "retained-wsol-retirement.sqlite3"
    repo, fixture, action, _ = begin(path, venue="swap")
    binding = repo.domain
    with repo:
        prep, chain, support, protection, _ = acquisition(repo, fixture, action, due=True)
        sold, sell, sell_prep, sell_chain, sell_support, at = cf.next_step(repo, fixture, support.observation.anchor, number=60,
            quantity=fixture.actual_base, keep_wsol=True, position_id=action.position_id, protective_handoff=protection)
        intent, comparison = retirement(repo, sell, sell_prep, sell_support, at=at+20)
        result = repo.apply_settlement_with_ports(sell_prep.attempt_id, chain_receipt_key="chain-60", support=sell_support,
            ingestion_key="retained-retire", recorded_at_utc=utc(at+20), retirement=intent, retirement_support=comparison, fence=repo.write_fence())
        check("retained_wsol_survives_lawful_position_retirement", result.retirement_disposition == "RETIRED"
            and any(item.mint == sf.WSOL_MINT and item.units == sold.principal-sold.venue_fees for item in repo.consumer_snapshot()["accounts"])
            and not repo.consumer_snapshot()["positions"])
        results["retained-wsol"] = repo.audit()["custody_digest"]
    with LedgerRepository.reopen(path, binding) as repo:
        check("retained_wsol_reconstruction_no_writeoff", repo.audit()["custody_digest"] == results["retained-wsol"])
    return results


def dry_input(repo, action, prep=None, *, reason=None, at=NOW+10):
    return DryTerminalInput(cut(repo), action.root_id, action.action_id,
        None if prep is None else prep.attempt_id, None if prep is None else prep.content_digest,
        None if prep is None else prep.message_sha256,
        None if prep is None else repo._simulation_input_digest(prep.attempt_id),
        reason or ("DRY_INTERRUPTED_BEFORE_PREPARATION" if prep is None else "DRY_INTERRUPTED_NO_SIGNED_GRAPH"),
        "EXTERNAL_RUNTIME_DRY_DISPOSITION", sf.digest("dry-disposition"), utc(at))


def changed_preparation(repo, fixture, action, *, ordinal=2, at=NOW+5):
    message = fixture.message
    header = message.header
    updated = cf.Message.new_with_compiled_instructions(header.num_required_signatures, header.num_readonly_signed_accounts,
        header.num_readonly_unsigned_accounts, message.account_keys, cf.Hash.from_string(cf.bh(179)), message.instructions)
    original = prepare(repo, fixture, action)
    return replace(original, ordinal=ordinal, message_hex=cf.to_bytes_versioned(updated).hex(), prepared_at_utc=utc(at),
        lease=replace(original.lease, blockhash=cf.bh(179), observed_at_us=int((at-1)*1_000_000)))


def dry_cases(directory):
    results = {}
    for kind in ("before-preparation", "prepared", "simulated", "cancelled-predecessor"):
        path = directory / ("dry-"+kind+".sqlite3")
        repo, fixture, action, admitted = begin(path, mode="DRY")
        binding = repo.domain
        prep = None
        with repo:
            if kind != "before-preparation":
                prep = prepare(repo, fixture, action)
                repo.prepare_attempt(prep, fence=repo.write_fence())
                if kind == "cancelled-predecessor":
                    repo.cancel_attempt_locally(prep.attempt_id, recorded_at_utc=utc(NOW+3), idempotency_key="cancel",
                        expected_attempt_revision=0, fence=repo.write_fence())
                    prep = changed_preparation(repo, fixture, action)
                    repo.prepare_attempt(prep, fence=repo.write_fence())
                if kind in ("simulated", "cancelled-predecessor"):
                    sf.advance(repo, prep, "EXACT_SIMULATED", at=NOW+6)
                for target in ("AUTHORIZED", "SIGNED_DURABLE", "SEND_CLAIMED", "OBSERVING"):
                    check(kind+"_dry_denies_"+target, raises(LedgerContractError, lambda target=target: sf.advance(repo, prep, target, at=NOW+7)))
            original_count = repo.audit()["attempt_count"]
        # Process/generation loss is positive DRY-mode proof, never LIVE proof.
        with LedgerRepository.reopen(path, binding) as repo:
            supplied = dry_input(repo, action, prep, reason="DRY_CAPABILITY" if kind == "simulated" else None)
            receipt = repo.finish_dry(supplied, ingestion_key="dry-terminal", fence=repo.write_fence())
            check(kind+"_dry_terminal_and_reservation_release", repo.inbox_disposition(action.root_id) == "NON_SUBMITTED"
                and not repo.consumer_snapshot()["reservations"] and not repo.mutation_lane()["held"])
            check(kind+"_dry_no_economic_graph", repo.audit()["posting_count"] == repo.audit()["application_receipt_count"] == 0
                and not repo.consumer_snapshot()["positions"] and repo.consumer_snapshot()["funding"].native_lamports == sf.FUNDING
                and repo.audit()["attempt_count"] == original_count)
            if prep is not None:
                check(kind+"_dry_exact_original_lineage_retained", repo.attempt(prep.attempt_id).current_disposition == "NON_SUBMITTED"
                    and repo.attempt(prep.attempt_id).preparation == prep and repo.attempt(prep.attempt_id).primary_signature is None)
            check(kind+"_dry_exact_duplicate", repo.finish_dry(supplied, ingestion_key="repeated-terminal", fence=repo.write_fence()) == receipt)
            check(kind+"_dry_candidate_redelivery_tombstone", repo.receive_candidate(repo.candidate(action.root_id), fence=repo.write_fence()) == action.root_id
                and repo.inbox_disposition(action.root_id) == "NON_SUBMITTED")
            check(kind+"_dry_terminal_cannot_prepare_again", raises(LedgerContractError, lambda: repo.prepare_attempt(
                changed_preparation(repo, fixture, action, ordinal=original_count+1), fence=repo.write_fence())))
            check(kind+"_dry_actual_application_denied", raises(LedgerContractError, lambda: repo.apply_settlement("1"*64,
                chain_receipt_key="x", support=None, ingestion_key="x", recorded_at_utc=utc(NOW+20), fence=repo.write_fence())))
            results[kind] = repo.audit()["custody_digest"]
        with LedgerRepository.reopen(path, binding) as repo:
            check(kind+"_dry_reopen_exact_terminal", repo.port_receipt("dry-terminal") == receipt and repo.audit()["custody_digest"] == results[kind])
        check(kind+"_dry_cannot_reopen_live", raises(LedgerConflict, lambda: LedgerRepository.reopen(path, replace(binding, mode="LIVE"))))
    path = directory / "live-not-dry.sqlite3"
    repo, fixture, action, _ = begin(path)
    binding = repo.domain
    with repo:
        prep, chain, support = finalize(repo, fixture, action)
        dry = replace(binding, mode="DRY")
        check("direct_pure_L3_DRY_denied", raises(LedgerContractError, lambda: adjudicate_chain_observation(dry, repo.attempt(prep.attempt_id),
            fixture.wire, utc(NOW+4), repo.baseline().observation.anchor, chain.observation, evaluated_at_utc=utc(NOW+10))))
        check("direct_pure_L4_DRY_whole_unapplied", attribute_settlement(dry, action, repo.attempt(prep.attempt_id), chain, support).proposal is None)
        check("LIVE_cannot_claim_DRY_never_submitted", raises(LedgerContractError, lambda: repo.finish_dry(dry_input(repo, action, prep), ingestion_key="fake-dry", fence=repo.write_fence())))
        check("positive_finality_keeps_own_lane_until_economic_disposition", repo.mutation_lane()["held"])
    check("LIVE_cannot_reopen_DRY", raises(LedgerConflict, lambda: LedgerRepository.reopen(path, replace(binding, mode="DRY"))))
    return results


def comparison_target_cases(directory):
    path = directory / "comparison-effects-target.sqlite3"
    repo, fixture, action, _ = begin(path, failed=True)
    binding = repo.domain
    with repo:
        # Actual opening facts establish an earlier comparison before any attempt.
        baseline = repo.baseline()
        initial = WalletSupportInput(baseline.observation, utc(NOW), 100)
        early = repo.compare_wallet_observation(initial, ingestion_key="early", fence=repo.write_fence())
        prep, chain, support = finalize(repo, fixture, action)
        intent, comparison = retirement(repo, action, prep, support, reason="FAILED_NO_ACQUISITION")
        before = cut(repo)
        group = repo.apply_settlement_with_ports(prep.attempt_id, chain_receipt_key="transaction", support=support,
            ingestion_key="fee-retire", recorded_at_utc=utc(NOW+20), retirement=intent, retirement_support=comparison, fence=repo.write_fence())
        view = repo.consumer_snapshot()["latest_comparison"]
        child = repo.application_receipt(group.application_key)
        check("shared_comparison_parent_commit_cut_remains_exact", view.common_revision == before.revision and view.common_digest == before.commit_digest)
        check("shared_comparison_explicit_post_settlement_effect_target", view.effects_through_sequence == group.sequence
            and view.target_custody_digest == child.decision.resulting_custody_digest
            and early.comparison.effects_through_sequence < view.effects_through_sequence)
        check("public_comparison_exposes_latest_usable_wallet_floor", view.recognized_wallet_context_floor == support.observation.anchor.slot
            and repo.consumer_snapshot()["required_wallet_context_slot"] == support.observation.anchor.slot)
        repo.audit()
    for differs in (False, True, "incomplete"):
        name = "later-incomplete" if differs == "incomplete" else "later-different" if differs else "later-matching"
        path = directory / (name+"-wallet-cut.sqlite3")
        repo, fixture, action, _ = begin(path, failed=True)
        binding = repo.domain
        with repo:
            prep, chain, support = finalize(repo, fixture, action)
            if differs:
                _, wallet, request = cf.composed_support(repo, fixture)
                if differs == "incomplete":
                    wallet.program_failure = sf.TOKEN_2022_PROGRAM_ID
                else:
                    wallet.accounts[sf.WALLET]["lamports"] += 23
                support = WalletSupportInput(observe(wallet, request=request, at=NOW+14), utc(NOW+14), 108)
            cf.apply(repo, prep, support)
            _, wallet, request = cf.composed_support(repo, fixture)
            wallet.context = wallet.multiple_context = wallet.initial_slot = wallet.upper_slot = 110
            def transform(payload, envelope):
                if payload["method"] == "getBlock":
                    envelope["result"].update(blockhash=cf.bh(110), previousBlockhash=cf.bh(108), parentSlot=108, blockHeight=93, blockTime=NOW+6)
                return envelope
            wallet.transform = transform
            original = WalletSupportInput(observe(wallet, request=request, at=NOW+14), utc(NOW+20), 108)
            intent, original = retirement(repo, action, prep, original, reason="FAILED_NO_ACQUISITION")
            withheld = repo.retire(intent, original, ingestion_key="old-wallet-cut", fence=repo.write_fence())
            check(name+"_older_wallet_cut_cannot_retire", withheld.retirement_disposition == "WITHHELD"
                and "COMPARISON_BEHIND_LATEST_USABLE_WALLET_ACCOUNT_CUT" in repo.consumer_snapshot()["latest_comparison"].reasons)
            check(name+"_older_comparison_retained_unresolved", repo.wallet_support(original.digest) == original
                and repo.consumer_snapshot()["required_wallet_context_slot"] == 116 and repo.reservation(action.root_id).status == "RESERVED")
            digest = repo.audit()["custody_digest"]
        with LedgerRepository.reopen(path, binding) as repo:
            check(name+"_wallet_cut_replay", repo.audit()["custody_digest"] == digest)


def execute_fault_port(repo, operation):
    action = repo.action(repo._conn.execute("SELECT action_id FROM ledger_pending_actions ORDER BY commit_seq LIMIT 1").fetchone()[0]) if operation != "admission" else None
    fixture = sf.Fixture(failed=operation in ("retirement", "withheld", "standalone-retirement"))
    fixture.mint = sf.MINT
    if operation == "admission":
        item = repo.candidate(repo._conn.execute("SELECT root_id FROM ledger_candidate_inbox").fetchone()[0])
        action = PendingAction(item.trade_root(repo.domain), item.content_digest, "BUY", sf.MINT, fixture.token_program,
            position_identity(repo.domain, item.trade_root(repo.domain), sf.MINT), fixture.units, "FIXTURE_EXTERNAL_TERMS", sf.digest("terms"),
            "EXTERNAL_POLICY", sf.digest("policy"), "FINAL-A", None, 1, (NOW+300)*1_000_000, sf.digest("deadline"))
        prior = repo.port_receipt("port")
        supplied = admission(repo, action) if prior is None else prior.admission
        return repo.admit(supplied, ingestion_key="port", fence=repo.write_fence())
    prior = repo.port_receipt("port")
    row = repo._conn.execute("SELECT attempt_id FROM ledger_attempts ORDER BY commit_seq DESC LIMIT 1").fetchone()
    prep = None if row is None else repo.attempt(row[0]).preparation
    if operation in ("dry", "dry-prepared"):
        supplied = dry_input(repo, action, prep) if prior is None else prior.dry_terminal
        return repo.finish_dry(supplied, ingestion_key="port", fence=repo.write_fence())
    chain = repo.chain_receipt("transaction")
    support, wallet, request = cf.composed_support(repo, fixture)
    if operation == "protection":
        supplied = handoff(repo, action, prep, chain, support, due=True) if prior is None else prior.handoff
        return repo.apply_settlement_with_ports(prep.attempt_id, chain_receipt_key="transaction", support=support,
            ingestion_key="port", recorded_at_utc=utc(NOW+20), handoff=supplied, fence=repo.write_fence())
    if operation == "withheld":
        wallet.accounts[sf.WALLET]["lamports"] += 11
    original = WalletSupportInput(observe(wallet, request=request, at=NOW+14), utc(NOW+20), request.min_context_slot)
    supplied, original = retirement(repo, action, prep, original, reason="FAILED_NO_ACQUISITION") if prior is None else (prior.retirement, original)
    if operation == "standalone-retirement":
        return repo.retire(supplied, original, ingestion_key="port", fence=repo.write_fence())
    return repo.apply_settlement_with_ports(prep.attempt_id, chain_receipt_key="transaction", support=support,
        ingestion_key="port", recorded_at_utc=utc(NOW+20), retirement=supplied, retirement_support=original, fence=repo.write_fence())


def fault_setup(path, operation):
    repo, fixture, action, supplied = begin(path, mode="DRY" if operation.startswith("dry") else "LIVE",
        admitted=operation != "admission", failed=operation in ("retirement", "withheld", "standalone-retirement"))
    if operation not in ("admission", "dry", "dry-prepared"):
        prep, chain, support = finalize(repo, fixture, action)
        if operation == "standalone-retirement":
            cf.apply(repo, prep, support)
    elif operation == "dry-prepared":
        prep = prepare(repo, fixture, action)
        repo.prepare_attempt(prep, fence=repo.write_fence())
        sf.advance(repo, prep, "EXACT_SIMULATED", at=NOW+2)
    return repo


def child(operation, cut_name, path):
    binding = LedgerDomain(GENESIS, sf.WALLET, PROFILE.fingerprint, sf.FUNDING, minimum_context_slot=100,
        mode="DRY" if operation.startswith("dry") else "LIVE")
    if operation == "compete":
        try:
            with LedgerRepository.reopen(path, binding):
                return 94
        except LedgerConflict:
            return 93
    with LedgerRepository.reopen(path, binding) as repo:
        if cut_name in ("before", "after"):
            original = repo._commit
            def crash():
                if cut_name == "before":
                    os._exit(91)
                original()
                os._exit(92)
            repo._commit = crash
        else:
            original = repo._conn
            target = {"action-child": "INSERT INTO ledger_pending_actions", "application-child": "INSERT INTO ledger_application_receipts",
                "group": "INSERT INTO ledger_consumer_groups", "common": "INSERT INTO ledger_commits"}[cut_name]
            class InterruptedBoundary:
                def __getattr__(self, name):
                    return getattr(original, name)
                def execute(self, command, *args):
                    result = original.execute(command, *args)
                    if command.startswith(target):
                        os._exit(91)
                    return result
            repo._conn = InterruptedBoundary()
        execute_fault_port(repo, operation)
    return 99


def crash_cases(directory):
    results = {}
    for operation in ("admission", "protection", "retirement", "withheld", "standalone-retirement", "dry", "dry-prepared"):
        cuts = ("before", "after", "group", "common")
        if operation == "admission":
            cuts += ("action-child",)
        if operation in ("protection", "retirement", "withheld"):
            cuts += ("application-child",)
        digests = []
        for cut_name in cuts:
            name = operation+"-"+cut_name
            path = directory / ("crash-"+name+".sqlite3")
            with fault_setup(path, operation) as repo:
                binding = repo.domain
                starting, start_snapshot, start_lane = repo.audit(), repo.consumer_snapshot(), repo.mutation_lane()
            completed = subprocess.run([sys.executable, "-B", str(Path(__file__).resolve()), "--child", operation, cut_name, str(path)],
                cwd=ROOT, capture_output=True, text=True, timeout=30)
            check(name+"_actual_process_cut", completed.returncode == (92 if cut_name == "after" else 91) and not completed.stdout and not completed.stderr)
            with LedgerRepository.reopen(path, binding) as repo:
                if cut_name != "after":
                    check(name+"_no_partial_group_or_projection", repo.port_receipt("port") is None and repo.audit()["revision"] == starting["revision"]
                        and repo.consumer_snapshot()["custody_digest"] == start_snapshot["custody_digest"] and repo.mutation_lane() == start_lane
                        and repo.audit()["posting_count"] == starting["posting_count"])
                else:
                    check(name+"_whole_common_group_survives", repo.port_receipt("port") is not None and repo.audit()["revision"] == starting["revision"]+1)
                receipt = execute_fault_port(repo, operation)
                check(name+"_retry_one_atomic_group", repo.audit()["revision"] == starting["revision"]+1 and repo.port_receipt("port") == receipt)
                if operation == "admission":
                    check(name+"_terms_reservation_acceptance_together", repo.audit()["action_count"] == 1 and len(repo.consumer_snapshot()["reservations"]) == 1)
                elif operation == "protection":
                    check(name+"_actual_inventory_protection_together", repo.consumer_snapshot()["positions"][0].usable and repo.audit()["posting_count"] == 9)
                elif operation.startswith("dry"):
                    check(name+"_non_submitted_release_no_economics", not repo.consumer_snapshot()["reservations"] and repo.audit()["posting_count"] == 0 and not repo.mutation_lane()["held"])
                else:
                    check(name+"_whole_fee_postings_correct_reservation", repo.audit()["posting_count"] == 1 and
                        bool(repo.consumer_snapshot()["reservations"]) is (operation == "withheld") and
                        receipt.retirement_disposition == ("WITHHELD" if operation == "withheld" else "RETIRED"))
                digests.append(receipt.content_digest)
        check(operation+"_all_crash_cuts_same_original_receipt", len(set(digests)) == 1)
        results[operation] = digests[0]
    for operation in ("admission", "protection", "retirement", "dry"):
        for cut_name in ("before-publication", "after-publication"):
            name = operation+"-"+cut_name
            path = directory / ("lostack-"+name+".sqlite3")
            with fault_setup(path, operation) as repo:
                binding = repo.domain
                original_connection, original_commit = repo._conn, repo._commit
                class LostAcknowledgement:
                    def __getattr__(self, name):
                        return getattr(original_connection, name)
                    def execute(self, command, *args):
                        result = original_connection.execute(command, *args)
                        if command == "COMMIT":
                            raise RuntimeError("inert fault")
                        return result
                def after():
                    original_commit()
                    raise RuntimeError("inert fault")
                if cut_name == "before-publication":
                    repo._conn = LostAcknowledgement()
                else:
                    repo._commit = after
                check(name+"_uncertain_ack_reported", raises(LedgerJournalError, lambda: execute_fault_port(repo, operation)))
                repo._conn, repo._commit = original_connection, original_commit
                if cut_name == "before-publication":
                    check(name+"_unpublished_custody_not_trusted", raises(LedgerConflict, repo.consumer_snapshot))
                else:
                    check(name+"_published_group_retry", execute_fault_port(repo, operation) == repo.port_receipt("port"))
            with LedgerRepository.reopen(path, binding) as repo:
                check(name+"_durable_original_group_recovered", execute_fault_port(repo, operation) == repo.port_receipt("port"))
                repo.audit()
    return results


def opening_support(at=NOW+20):
    # Independent read-only fixture: no executed transaction, initial native only.
    value = sf.Fixture().wallet_scenario()
    value.context = value.multiple_context = value.initial_slot = value.upper_slot = 116
    def transform(payload, envelope):
        if payload["method"] == "getBlock":
            envelope["result"].update(blockhash=cf.bh(116), previousBlockhash=cf.bh(114), parentSlot=114, blockHeight=95, blockTime=NOW+12)
        return envelope
    value.transform = transform
    request = WalletEvidenceRequest(sf.WALLET, GENESIS, 101)
    return WalletSupportInput(observe(value, request=request, at=NOW+14), utc(at), 101)


def no_acquisition_cases(directory):
    results = {}
    for kind in ("nonlanding", "cancelled-unsigned", "lost-generation", "signed-unknown"):
        path = directory / (kind+"-retire.sqlite3")
        repo, fixture, action, _ = begin(path)
        binding = repo.domain
        prep = prepare(repo, fixture, action)
        repo.prepare_attempt(prep, fence=repo.write_fence())
        if kind in ("nonlanding", "signed-unknown"):
            for target, offset in (("EXACT_SIMULATED", 2), ("AUTHORIZED", 3), ("SIGNED_DURABLE", 4)):
                sf.advance(repo, prep, target, at=NOW+offset)
        if kind == "lost-generation":
            repo.close()
            repo = LedgerRepository.reopen(path, binding)
        with repo:
            if kind == "nonlanding":
                value = cf.scenario(absent=True)
                value.tx = value.status = None
                observed = cf.coverage_observation(value, at=NOW+12, request=cf.CoverageRequest(GENESIS, cf.sig(49),
                    prep.lease.blockhash, prep.lease.last_valid_block_height, prep.finalized_lower_anchor))
                repo.ingest_chain_observation(prep.attempt_id, observed, ingestion_key="coverage", evaluated_at_utc=utc(NOW+12), fence=repo.write_fence())
                supplied, support = retirement(repo, action, prep, opening_support(), reason="PROVEN_NON_LANDED")
                receipt = repo.apply_settlement_with_ports(prep.attempt_id, chain_receipt_key="coverage", support=None,
                    ingestion_key="resolve-retire", recorded_at_utc=utc(NOW+20), retirement=supplied, retirement_support=support, fence=repo.write_fence())
                check("canonical_nonlanding_resolution_retirement_atomic", receipt.application_disposition == "PROVEN_NON_LANDED_RESOLVED"
                    and receipt.retirement_disposition == "RETIRED")
            else:
                cancelled = repo.cancel_attempt_locally(prep.attempt_id, recorded_at_utc=utc(NOW+10), idempotency_key="cancel",
                    expected_attempt_revision=repo.attempt(prep.attempt_id).revision, fence=repo.write_fence())
                expected_stage = "CANCELLED_UNSIGNED" if kind == "cancelled-unsigned" else "UNKNOWN"
                check(kind+"_positive_cancel_or_retained_uncertainty", cancelled.recorded_stage == expected_stage)
                check(kind+"_local_cancel_alone_does_not_release_encumbrance", repo.reservation(action.root_id).status == "RESERVED")
                supplied, support = retirement(repo, action, prep, opening_support(), reason="POSITIVE_UNSIGNED_CANCEL")
                receipt = repo.retire(supplied, support, ingestion_key="resolve-retire", fence=repo.write_fence())
                check(kind+"_retire_only_positive_proof", receipt.retirement_disposition == ("RETIRED" if kind == "cancelled-unsigned" else "WITHHELD"))
            check(kind+"_no_acquisition_money_or_position_invented", repo.audit()["posting_count"] == 0 and not repo.consumer_snapshot()["positions"]
                and repo.consumer_snapshot()["funding"].native_lamports == sf.FUNDING)
            check(kind+"_lane_and_reservation_remain_for_uncertainty", repo.mutation_lane()["held"] is (kind in ("lost-generation", "signed-unknown"))
                and bool(repo.consumer_snapshot()["reservations"]) is (kind in ("lost-generation", "signed-unknown")))
            results[kind] = repo.audit()["custody_digest"]
        with LedgerRepository.reopen(path, binding) as repo:
            check(kind+"_positive_or_unknown_retirement_replay", repo.audit()["custody_digest"] == results[kind]
                and repo.port_receipt("resolve-retire") == receipt)
    path = directory / "partial-retirement-denied.sqlite3"
    repo, fixture, action, _ = begin(path)
    with repo:
        prep, chain, support, protection, _ = acquisition(repo, fixture, action, due=True)
        partial = fixture.actual_base//2
        sold, sell, sell_prep, chain, sell_support, at = cf.next_step(repo, fixture, support.observation.anchor,
            number=70, quantity=partial, position_id=action.position_id, protective_handoff=protection)
        supplied, comparison = retirement(repo, sell, sell_prep, sell_support, at=at+20)
        result = repo.apply_settlement_with_ports(sell_prep.attempt_id, chain_receipt_key="chain-70", support=sell_support,
            ingestion_key="partial-retire", recorded_at_utc=utc(at+20), retirement=supplied, retirement_support=comparison, fence=repo.write_fence())
        check("partial_actual_fill_remains_whole_retirement_withheld", result.application_disposition == "FINALIZED_SUCCESS_APPLIED"
            and result.retirement_disposition == "WITHHELD" and "RETIREMENT_ACTUAL_RESIDUAL_REMAINS" in result.retirement_reasons
            and repo.position_history(action.position_id).remaining_units == fixture.actual_base-partial)
        check("partial_retirement_keeps_due_protection_reservation", repo.protection(action.position_id).obligation_state == "DUE"
            and repo.reservation(action.root_id).status == "RESERVED" and repo.consumer_snapshot()["positions"][0].usable)
        repo.audit()
    return results


def storage_cases(directory):
    path = directory / "guarded-current-group.sqlite3"
    repo = fault_setup(path, "protection")
    binding = repo.domain
    with repo:
        group = execute_fault_port(repo, "protection")
        completed = subprocess.run([sys.executable, "-B", str(Path(__file__).resolve()), "--child", "compete", "before", str(path)],
            cwd=ROOT, capture_output=True, text=True, timeout=30)
        check("competing_writer_cannot_mutate_consumer_group", completed.returncode == 93 and not completed.stdout and not completed.stderr)
        original_fence = repo.write_fence()
        with closing(sqlite3.connect(path)) as outside:
            original = outside.execute("SELECT content_digest FROM ledger_funding_projection").fetchone()[0]
            outside.execute("UPDATE ledger_funding_projection SET content_digest=?", ("0"*64,))
            outside.execute("UPDATE ledger_funding_projection SET content_digest=?", (original,))
            outside.commit()
        check("outside_projection_commit_invalidates_snapshot", raises(LedgerConflict, repo.consumer_snapshot))
        check("outside_commit_cannot_be_blessed_by_group_retry", raises(LedgerConflict, lambda: execute_fault_port(repo, "protection")))
    with LedgerRepository.reopen(path, binding) as repo:
        check("explicit_reopen_verifies_structurally_unchanged_projection", repo.port_receipt("port") == group and repo.consumer_snapshot()["positions"][0].usable)
        check("stale_generation_group_retry_denied", raises(LedgerConflict, lambda: repo.record_protective_handoff(group.handoff, ingestion_key="port", fence=original_fence)))
        # Historical receipt evaluation never switches to today's clock on audit.
        check("aged_applied_handoff_is_historical_not_current_authority", not repo.consumer_snapshot()["authority_current"]
            and repo.application_receipt(group.application_key).recorded_at_utc == utc(NOW+20))
        repo.audit()
    for kind in ("group-payload", "child-digest", "group-link", "missing-support", "projection", "orphan-group", "mode", "version"):
        path = directory / ("tamper-"+kind+".sqlite3")
        with fault_setup(path, "retirement") as repo:
            binding = repo.domain
            receipt = execute_fault_port(repo, "retirement")
        with closing(sqlite3.connect(path)) as conn:
            conn.execute("PRAGMA foreign_keys=OFF")
            if kind == "version":
                conn.execute("PRAGMA user_version=4")
            elif kind == "projection":
                conn.execute("UPDATE ledger_funding_projection SET payload_json='{}'")
            else:
                table = "ledger_consumer_groups" if kind in ("group-payload", "group-link", "orphan-group") else "ledger_application_receipts" if kind == "child-digest" else "ledger_custody_inputs" if kind == "missing-support" else "ledger_domain"
                triggers = conn.execute("SELECT name,sql FROM sqlite_master WHERE type='trigger' AND tbl_name=?", (table,)).fetchall()
                for trigger, _ in triggers:
                    conn.execute('DROP TRIGGER "'+trigger+'"')
                if kind == "group-payload":
                    conn.execute("UPDATE ledger_consumer_groups SET payload_json='{}' WHERE ingestion_key='port'")
                elif kind == "child-digest":
                    conn.execute("UPDATE ledger_application_receipts SET receipt_digest=?", ("1"*64,))
                elif kind == "group-link":
                    conn.execute("UPDATE ledger_consumer_groups SET retired_reservation=NULL WHERE ingestion_key='port'")
                elif kind == "missing-support":
                    conn.execute("DELETE FROM ledger_custody_inputs WHERE input_digest=?", (receipt.retirement.wallet_support_digest,))
                elif kind == "orphan-group":
                    conn.execute("DELETE FROM ledger_consumer_groups WHERE ingestion_key='port'")
                else:
                    record = json.loads(conn.execute("SELECT payload_json FROM ledger_domain").fetchone()[0])
                    record["mode"] = "DRY"
                    conn.execute("UPDATE ledger_domain SET payload_json=?", (canonical_json(record),))
                for _, sql in triggers:
                    conn.execute(sql)
            conn.commit()
        check(kind+"_hostile_store_reopen_denied", raises(LedgerContractError, lambda: LedgerRepository.reopen(path, binding)))


def main():
    with tempfile.TemporaryDirectory(prefix="live-ledger-ports-") as temporary:
        directory = Path(temporary)
        results = normal_cases(directory)
        admission_cases(directory)
        protection_cases(directory)
        results.update(retirement_cases(directory))
        results["dry"] = dry_cases(directory)
        comparison_target_cases(directory)
        results["resolutions"] = no_acquisition_cases(directory)
        storage_cases(directory)
        results["crashes"] = crash_cases(directory)
    print(json.dumps({"status": "IMPLEMENTED_PENDING_PROJECT_REVIEW", "checks": len(CHECKS), "all_checks": all(CHECKS.values()),
        "check_digest": content_fingerprint(CHECKS), "results": results}, sort_keys=True))


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--child":
        raise SystemExit(child(sys.argv[2], sys.argv[3], Path(sys.argv[4])))
    main()
