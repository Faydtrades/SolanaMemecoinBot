"""B1 original accepted settlement fixtures -> selected controller, temp DB only."""
from __future__ import annotations

import copy
import json
import sys
import tempfile
from dataclasses import FrozenInstanceError, replace
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from phase4.paper_exit_orchestrator_v0_1 import LOCKED_EXIT_SPECS
from phase5.shadow_domain_v0_1 import content_fingerprint
from live.ledger_domain_v0_1 import LedgerContractError
from live.ledger_repository_v0_1 import LedgerRepository
from live.position_controller_v0_1 import bind_actual_position
from live_ledger_actions_selftest_v0_1 import candidate
from live_ledger_finality_selftest_v0_1 import transaction_observation
from live_wallet_evidence_selftest_v0_1 import NOW, utc, key
import live_ledger_ports_selftest_v0_1 as pf
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


def begin(path, track="FINAL-A", **kwargs):
    repo, fixture, action, _ = pf.begin(path, admitted=False, **kwargs)
    # Explicit deterministic engineering selection, never production policy.
    action = replace(action, selected_exit_track=track,
        policy_ref="SYNTHETIC_ENGINEERING_SELECTION_" + track,
        policy_digest=content_fingerprint({"synthetic_engineering_track": track}))
    repo.admit(pf.admission(repo, action), ingestion_key="admit", fence=repo.write_fence())
    return repo, fixture, action


def read(repo, action, **overrides):
    args = dict(position_id=action.position_id, root_id=action.root_id, mint=action.mint,
        selected_track=action.selected_exit_track, policy_digest=action.policy_digest,
        acquisition_application_key="acquire")
    args.update(overrides)
    return bind_actual_position(repo, **args)


def normal_cases(directory):
    evidence = {}
    for track in LOCKED_EXIT_SPECS:
        path = directory / (track + ".sqlite3")
        repo, fixture, action = begin(path, track)
        domain = repo.domain
        with repo:
            check(track+"_unsettled_no_controller", raises(LedgerContractError, lambda: read(repo, action)))
            prep, chain, support = pf.finalize(repo, fixture, action)
            check(track+"_finality_without_application_no_controller", raises(LedgerContractError, lambda: read(repo, action)))
            at = NOW+14 if track == "FINAL-B" else NOW+20
            applied = cf.apply(repo, prep, support, key="acquire", at=at)
            before = repo.audit()
            state = read(repo, action)
            binding, original = state.binding, repo.candidate(action.root_id)
            check(track+"_actual_finalized_buy_only", applied.decision.disposition == "FINALIZED_SUCCESS_APPLIED"
                and state.acquired_units == state.remaining_units == fixture.actual_base
                and state.remaining_units != action.input_units and state.sold_units == 0)
            check(track+"_one_selected_controller", binding.spec == LOCKED_EXIT_SPECS[track]
                and binding.spec.track_id == action.selected_exit_track and binding.policy_digest == action.policy_digest)
            check(track+"_original_candidate_exact", binding.candidate == original
                and binding.candidate.rule_reference_price_numerator_raw == original.rule_reference_price_numerator_raw
                and binding.candidate.rule_reference_price_denominator_raw == original.rule_reference_price_denominator_raw
                and binding.candidate.reference_price_text == original.reference_price_text
                and binding.candidate.source_binding == original.source_binding)
            check(track+"_original_fallback", binding.fallback_deadline_us == original.generated_at_us
                + LOCKED_EXIT_SPECS[track].fallback_ms*1000
                and binding.fallback_fire_boundary_us == binding.fallback_deadline_us+1
                and binding.fallback_deadline_us != action.claimed_entry_deadline_us)
            check(track+"_late_finality_or_exact_deadline_preserved",
                (binding.fallback_fire_boundary_us < at*1_000_000) if track != "FINAL-B"
                else binding.fallback_deadline_us == at*1_000_000)
            check(track+"_original_acquisition_proof", binding.acquisition_application_digest == applied.content_digest
                and binding.acquisition_chain_receipt_digest == chain.content_digest
                and binding.acquisition_signature == applied.decision.proposal.signature)
            check(track+"_bound_only_no_usable_inventory_or_obligation", state.state == "BOUND"
                and not state.inventory_usable and repo.protection(action.position_id) is None
                and state.ledger_position_status == "OWNED_PROTECTION_PENDING")
            check(track+"_idempotent_no_write", read(repo, action, previous=binding) == state and repo.audit() == before)
            check(track+"_immutable", raises(FrozenInstanceError, lambda: setattr(binding, "policy_digest", "0"*64)))
            for name, override in (("root", {"root_id": "0"*64}), ("mint", {"mint": key(45)}),
                    ("position", {"position_id": "0"*64}), ("track", {"selected_track": "FINAL-B" if track != "FINAL-B" else "FINAL-A"}),
                    ("multiple_tracks", {"selected_track": "FINAL-A+FINAL-B"}), ("policy", {"policy_digest": "0"*64}),
                    ("missing_application", {"acquisition_application_key": "missing"}),
                    ("rebinding", {"previous": replace(binding, fallback_deadline_us=binding.fallback_deadline_us+1)})):
                check(track+"_denies_"+name, raises(LedgerContractError, lambda override=override: read(repo, action, **override)))
            check(track+"_denials_unchanged_journal", repo.audit() == before)
            evidence[track] = {"binding_id": binding.binding_id, "binding_digest": binding.content_digest,
                "actual_units": state.remaining_units, "intended_buy_input_units": action.input_units,
                "signal_at_us": original.generated_at_us, "fallback_deadline_us": binding.fallback_deadline_us,
                "fallback_fire_boundary_us": binding.fallback_fire_boundary_us,
                "acquisition_recorded_at_utc": binding.acquisition_recorded_at_utc,
                "application_digest": applied.content_digest}
        with LedgerRepository.reopen(path, domain) as reopened:
            restored = read(reopened, action, previous=binding)
            check(track+"_reopen_exact_binding_quantity", restored.binding == binding
                and restored.remaining_units == state.remaining_units and restored.cut == state.cut)
    return evidence


def negative_cases(directory):
    for name in ("failed", "unknown", "confirmed"):
        repo, fixture, action = begin(directory / (name+".sqlite3"), failed=name == "failed")
        with repo:
            original_tx = fixture.scenario.tx
            if name == "unknown":
                fixture.scenario.tx = fixture.scenario.status = None
            elif name == "confirmed":
                fixture.scenario.status["confirmationStatus"] = "confirmed"
                fixture.scenario.status["confirmations"] = 1
                fixture.scenario.tx = None
            prep = pf.prepare(repo, fixture, action)
            repo.prepare_attempt(prep, fence=repo.write_fence())
            for target, offset in (("EXACT_SIMULATED", 2), ("AUTHORIZED", 3), ("SIGNED_DURABLE", 4)):
                pf.sf.advance(repo, prep, target, at=NOW+offset)
            observation = transaction_observation(fixture.scenario)
            repo.ingest_chain_observation(prep.attempt_id, observation, ingestion_key="transaction",
                evaluated_at_utc=utc(NOW+10), fence=repo.write_fence())
            fixture.scenario.tx = original_tx
            support, _, _ = cf.composed_support(repo, fixture)
            applied = cf.apply(repo, prep, support, key="acquire")
            if name == "confirmed" and applied.decision.disposition == "FINALIZED_SUCCESS_APPLIED":
                raise AssertionError("confirmed fixture must not finalize")
            check(name+"_no_actual_position", not repo.consumer_snapshot()["positions"])
            check(name+"_denied", raises(LedgerContractError, lambda: read(repo, action)))
            if name == "failed":
                check("failed_actual_fee_not_position", applied.decision.disposition == "FINALIZED_FAILURE_APPLIED"
                    and repo.consumer_snapshot()["funding"].network_fees_paid_lamports > 0)


def residual_case(directory):
    path = directory / "actual-residual.sqlite3"
    repo, fixture, action = begin(path)
    domain = repo.domain
    with repo:
        prep, chain, support = pf.finalize(repo, fixture, action)
        cf.apply(repo, prep, support, key="acquire")
        original = read(repo, action)
        # Existing external L6 fixture supplies protection solely to let accepted
        # Ledger produce a real partial reduction. B1 implements no obligation.
        handoff = pf.handoff(repo, action, prep, chain, support, due=True)
        repo.record_protective_handoff(handoff, ingestion_key="external-fixture-protection", fence=repo.write_fence())
        partial = fixture.actual_base//3
        reduced_fixture, _, sell_prep, _, sell_support, at = cf.next_step(repo, fixture, support.observation.anchor,
            number=50, quantity=partial, position_id=action.position_id, protective_handoff=handoff)
        applied = cf.apply(repo, sell_prep, sell_support, chain="chain-50", key="partial", at=at+20)
        residual = read(repo, action, previous=original.binding)
        check("partial_actual_applied_residual", applied.decision.disposition == "FINALIZED_SUCCESS_APPLIED"
            and residual.remaining_units == fixture.actual_base-partial and residual.sold_units == partial)
        check("partial_preserves_controller_identity", residual.binding == original.binding
            and residual.binding.binding_id == handoff.binding_id and residual.cut != original.cut)
        check("sell_cannot_replace_acquisition", raises(LedgerContractError,
            lambda: read(repo, action, acquisition_application_key="partial")))
        repo.audit()
    with LedgerRepository.reopen(path, domain) as repo:
        restored = read(repo, action, previous=original.binding)
        check("partial_reopen_actual_quantity", restored == residual)
        _, _, final_prep, _, final_support, final_at = cf.next_step(repo, reduced_fixture,
            sell_support.observation.anchor, number=51, quantity=residual.remaining_units,
            position_id=action.position_id, protective_handoff=handoff, action_ordinal=2)
        cf.apply(repo, final_prep, final_support, chain="chain-51", key="flat", at=final_at+20)
        flat = read(repo, action, previous=original.binding)
        check("flat_historical_binding_zero_actual_units", flat.binding == original.binding
            and flat.remaining_units == 0 and flat.sold_units == flat.acquired_units
            and flat.ledger_position_status == "FLAT_PENDING_RECONCILIATION")
    with LedgerRepository.reopen(path, domain) as repo:
        check("flat_reopen_same_binding_not_satisfaction", read(repo, action, previous=original.binding) == flat)
    return {"binding_id": residual.binding.binding_id, "acquired_units": residual.acquired_units,
        "sold_units": residual.sold_units, "remaining_units": residual.remaining_units}


def cut_and_quarantine_cases(directory):
    repo, fixture, action = begin(directory / "coherent-cut.sqlite3")
    with repo:
        prep, _, support = pf.finalize(repo, fixture, action)
        cf.apply(repo, prep, support, key="acquire")
        original_reader = repo.application_receipt
        def advance_during_read(ingestion_key):
            value = original_reader(ingestion_key)
            repo.receive_candidate(candidate(key(55), suffix="cut-change"), fence=repo.write_fence())
            return value
        repo.application_receipt = advance_during_read
        check("mixed_common_cut_denied", raises(LedgerContractError, lambda: read(repo, action)))
        repo.application_receipt = original_reader
        check("fresh_current_cut_reconstructs", read(repo, action).cut == repo.consumer_snapshot()["consumer_cut"])
        conflict = copy.deepcopy(fixture.scenario)
        conflict.tx["meta"]["fee"] += 1
        conflict.tx["meta"]["postBalances"][0] -= 1
        repo.ingest_chain_observation(prep.attempt_id, transaction_observation(conflict, at=NOW+21),
            ingestion_key="conflicting-finality", evaluated_at_utc=utc(NOW+21), fence=repo.write_fence())
        check("quarantined_actual_position_denied", bool(repo.consumer_snapshot()["funding"].quarantine_reasons)
            and raises(LedgerContractError, lambda: read(repo, action)))


def conflicting_protection_case(directory):
    repo, fixture, action = begin(directory / "wrong-original-fallback.sqlite3")
    with repo:
        prep, chain, support = pf.finalize(repo, fixture, action)
        cf.apply(repo, prep, support, key="acquire")
        handoff = pf.handoff(repo, action, prep, chain, support, due=True)
        shifted = replace(handoff, fallback_deadline_us=handoff.fallback_deadline_us+1_000_000,
            fallback_fire_boundary_us=handoff.fallback_fire_boundary_us+1_000_000,
            trigger_at_us=handoff.trigger_at_us+1_000_000)
        # The accepted Ledger port validates supplied Runtime decisions without
        # executing the locked evaluator; B1 now denies this semantic conflict.
        repo.record_protective_handoff(shifted, ingestion_key="external-wrong-timer", fence=repo.write_fence())
        check("existing_protection_wrong_original_timer_denied", raises(LedgerContractError, lambda: read(repo, action)))


def main():
    with tempfile.TemporaryDirectory(prefix="live-position-b1-") as temporary:
        directory = Path(temporary)
        evidence = normal_cases(directory)
        negative_cases(directory)
        cut_and_quarantine_cases(directory)
        conflicting_protection_case(directory)
        evidence["residual"] = residual_case(directory)
    print(json.dumps({"status": "IMPLEMENTED_PENDING_PROJECT_REVIEW", "checks": len(CHECKS),
        "all_checks": all(CHECKS.values()), "check_digest": content_fingerprint(CHECKS),
        "evidence": evidence}, sort_keys=True))


if __name__ == "__main__":
    main()
