"""Temporary deterministic L2 journal qualification; no signer or real transport.

Message fixtures are inert public no-instruction bytes. Nonzero signature-shaped
fixture bytes are deliberately not cryptographic signing and convey no authority.
"""
from __future__ import annotations

import base64
import hashlib
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
from contextlib import closing
from dataclasses import replace
from pathlib import Path

from solders.hash import Hash
from solders.pubkey import Pubkey

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from phase2.strategy_first_pullback_v0_2 import FirstPullbackStrategyV02
from phase5.shadow_domain_v0_1 import canonical_json
from phase5.shadow_unsigned_plan_simulation_v0_1 import BlockhashLeaseV01
from phase5.shadow_venue_route_quote_v0_1 import TOKEN_PROGRAM_ID
from live.source_health_v0_1 import CursorWitness, SourceBinding, TABLES
from live.ledger_actions_v0_1 import (
    CandidateInboxInput, PendingAction, AttemptPreparation, AttemptStageInput, PENDING_ADMISSION,
    LEDGER_FINALITY_STATES, position_identity, action_from_json,
)
from live.ledger_domain_v0_1 import LedgerContractError
from live.ledger_repository_v0_1 import LedgerRepository, LedgerConflict, LedgerJournalError, _DDL
from live_ledger_baseline_selftest_v0_1 import domain, observe, ingest
from live_wallet_evidence_selftest_v0_1 import Scenario, WALLET, MINT, MINT22, NOW, PROFILE, utc, TOKEN_2022_PROGRAM_ID

CHECKS = {}


def digest(value):
    return hashlib.sha256(value.encode("ascii")).hexdigest()


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


def candidate(mint=MINT, suffix="a"):
    started, generated = (NOW-100)*1_000_000, (NOW-1)*1_000_000
    event = "candidate-event-" + suffix
    run = FirstPullbackStrategyV02._stable_id("fp1run", mint, started, "v1.1", "FIXTURE_PARAMETERS")
    signal = FirstPullbackStrategyV02._stable_id("fp1sig", run, event, "FIXTURE_PARAMETERS", "v1.1")
    source = SourceBinding("FIXTURE_LINEAGE", utc(NOW-1000),
        tuple(CursorWitness(table, 1, digest(table)) for table in TABLES[:3]))
    return CandidateInboxInput(candidate_signal_id=signal, candidate_run_id=run, run_started_at_us=started,
        mint=mint, strategy_version="v1.1", parameter_set_id="FIXTURE_PARAMETERS",
        parameter_fingerprint=digest("parameters"), model_fingerprint=digest("model"),
        strategy_evaluation_id="evaluation-"+suffix, generated_at_us=generated, source_event_key=event,
        signal_key="signal-"+suffix, source_cursor=77, signal_ingest_seq=76,
        producer_run_id="original-producer-run", producer_lineage_id="original-producer-lineage", source_binding=source,
        source_record_digest=digest("source-record-"+suffix), winner_candidate_id=signal, winner_role="PRIMARY",
        winner_binding_digest=digest("winner-"+suffix), reference_price_text="0.000000123400",
        reference_price_kind="ORIGINAL_CANDIDATE_PROXY", price_identity="SOL-per-base-raw",
        rule_reference_price_numerator_raw=(1 << 100)*1234, rule_reference_price_denominator_raw=(1 << 100)*10**10,
        reference_at_us=generated)


def action_for(item, *, side="BUY", units=1000000, ordinal=1):
    binding = domain()
    root = item.trade_root(binding)
    return PendingAction(root_id=root, candidate_digest=item.content_digest, side=side, mint=item.mint,
        token_program=TOKEN_PROGRAM_ID, position_id=position_identity(binding, root, item.mint), input_units=units,
        external_decision_ref="UNVERIFIED_EXTERNAL_TERMS", external_decision_digest=digest("external-terms"),
        policy_ref="EXTERNAL_POLICY_REFERENCE", policy_digest=digest("policy"), selected_exit_track="FINAL-A",
        obligation_id=None if side == "BUY" else "obligation-"+root, ordinal=ordinal,
        claimed_entry_deadline_us=(NOW+300)*1_000_000 if side == "BUY" else None,
        deadline_binding_digest=digest("external-deadline") if side == "BUY" else None)


def preparation(action, *, ordinal=1, byte=31, at=NOW+1):
    blockhash = Hash.from_bytes(bytes([byte])*32)
    # Serialized legacy message: one public payer, a fixed blockhash, no instructions.
    raw = bytes((1, 0, 0, 1)) + bytes(Pubkey.from_string(WALLET)) + bytes(blockhash) + bytes((0,))
    lease = BlockhashLeaseV01("EXTERNAL_PLAN_REFERENCE", str(blockhash), 102, 200, "confirmed", NOW*1_000_000)
    return AttemptPreparation(action.action_id, action.content_digest, ordinal, raw.hex(), digest("plan"), digest("message-policy"),
        lease, observe().anchor, utc(at), "EXTERNAL_PREPARATION", digest("external-preparation"))


def stage(prepared, target, *, at=NOW+2, signature_byte=49):
    wire = (bytes((1,)) + bytes([signature_byte])*64 + bytes.fromhex(prepared.message_hex)
            if target == "SIGNED_DURABLE" else None)
    return AttemptStageInput(prepared.attempt_id, target, prepared.action_content_digest, prepared.message_sha256,
        prepared.lease.fingerprint, prepared.message_policy_digest, utc(at), "EXTERNAL_"+target,
        digest("external-"+target), None if wire is None else base64.b64encode(wire).decode("ascii"))


def advance(repo, prepared, target, *, at=NOW+2, key=None):
    previous = repo.attempt(prepared.attempt_id)
    return repo.record_external_attempt_stage(stage(prepared, target, at=at), idempotency_key=key or target,
        expected_attempt_revision=previous.revision, fence=repo.write_fence())


def cancel(repo, prep, *, at=NOW+10, key="cancel"):
    return repo.cancel_attempt_locally(prep.attempt_id, recorded_at_utc=utc(at), idempotency_key=key,
        expected_attempt_revision=repo.attempt(prep.attempt_id).revision, fence=repo.write_fence())


def setup(path, *, staged=True):
    repo = LedgerRepository.initialize(path, domain())
    ingest(repo, observe())
    item = candidate()
    root = repo.receive_candidate(item, fence=repo.write_fence())
    action = action_for(item)
    if staged:
        repo.stage_action(action, fence=repo.write_fence())
    return repo, item, root, action


def child(mode, path):
    if mode == "--compete":
        try:
            with LedgerRepository.reopen(path, domain()) as repo:
                repo.stage_action(action_for(candidate()), fence=repo.write_fence())
        except LedgerConflict:
            return 83
        return 99
    with LedgerRepository.reopen(path, domain()) as repo:
        action = action_for(candidate())
        operation, cut = mode.removeprefix("--").split("-")
        original = repo._commit

        def crash():
            if cut == "before":
                os._exit(81)
            original()
            os._exit(82)

        repo._commit = crash
        if operation == "action":
            repo.stage_action(action, fence=repo.write_fence())
        else:
            repo.prepare_attempt(preparation(action), fence=repo.write_fence())
    return 98


def run_child(mode, path):
    return subprocess.run([sys.executable, "-B", str(Path(__file__).resolve()), mode, str(path)],
                          cwd=ROOT, capture_output=True, text=True, timeout=30)


class ConnectionCut:
    """Test-only connection wrapper; no production fault hooks."""
    def __init__(self, conn, path, cut):
        self.conn, self.path, self.cut, self.used = conn, path, cut, False

    def __getattr__(self, name):
        return getattr(self.conn, name)

    def external_commit(self):
        with closing(sqlite3.connect(self.path, isolation_level=None)) as other:
            other.execute("PRAGMA application_id=0")

    def execute(self, sql, *args):
        if not self.used and self.cut == "before_begin" and sql == "BEGIN IMMEDIATE":
            self.used = True
            self.external_commit()
        result = self.conn.execute(sql, *args)
        if not self.used and self.cut == "after_commit" and sql == "COMMIT":
            self.used = True
            self.external_commit()
        return result


def main():
    with tempfile.TemporaryDirectory(prefix="live-ledger-actions-") as temporary:
        directory = Path(temporary)
        path = directory / "actions.sqlite3"
        repo, item, root, buy = setup(path)
        with repo:
            first_baseline = repo.baseline()
            check("canonical_inbox_delivery_needs_no_Authority_deadline", "entry_deadline_us" not in item.to_record()
                  and "deadline_binding_digest" not in item.to_record() and item.admission_status == "RECEIVED_UNADMITTED")
            check("inbox_preserves_original_candidate_source_winner_and_exact_reference", repo.candidate(root) == item
                  and repo.candidate(root).source_binding == item.source_binding
                  and repo.candidate(root).reference_price_text == "0.000000123400"
                  and repo.candidate(root).rule_reference_price_numerator_raw == (1 << 100)*1234)
            revision = repo.audit()["revision"]
            check("at_least_once_candidate_input_is_one_durable_root", repo.receive_candidate(item, fence=repo.write_fence()) == root
                  and repo.audit()["candidate_count"] == 1 and repo.audit()["revision"] == revision)
            for name, changed in (("source", replace(item, source_record_digest=digest("changed"))),
                                  ("winner", replace(item, winner_binding_digest=digest("changed"))),
                                  ("original_price", replace(item, reference_price_text="0.1")),
                                  ("raw_price", replace(item, rule_reference_price_numerator_raw=1)),
                                  ("original_time", replace(item, generated_at_us=item.generated_at_us+1))):
                check(f"same_candidate_changed_{name}_cannot_reroot", changed.trade_root(domain()) == root and raises(LedgerConflict,
                      lambda changed=changed: repo.receive_candidate(changed, fence=repo.write_fence())))
            check("changing_original_run_or_parameters_cannot_keep_canonical_ID", raises(LedgerContractError,
                  lambda: replace(item, run_started_at_us=item.run_started_at_us+1)) and raises(LedgerContractError,
                  lambda: replace(item, parameter_set_id="different")))
            for name, changed in (("amount", replace(buy, input_units=buy.input_units+1)),
                                  ("policy", replace(buy, policy_digest=digest("newpolicy"))),
                                  ("deadline", replace(buy, claimed_entry_deadline_us=(NOW+999)*1_000_000))):
                check(f"BUY_{name}_cannot_create_second_root_or_action", changed.root_id == root and changed.action_id == buy.action_id
                      and raises(LedgerConflict, lambda changed=changed: repo.stage_action(changed, fence=repo.write_fence())))
            check("pending_action_and_external_AUTHORIZED_claim_are_not_admission",
                  repo.action(buy.action_id).admission_status == PENDING_ADMISSION
                  and not repo.action(buy.action_id).has_real_authority_grant and repo.inbox_disposition(root) == "RECEIVED")
            check("inbox_cannot_accept_without_future_atomic_owners", raises(LedgerConflict, lambda: repo.record_nonacceptance(
                root, "ACCEPTED", external_reference="none", external_record_digest=digest("none"), recorded_at_utc=utc(),
                idempotency_key="not-authority", fence=repo.write_fence())))
            forged = buy.to_record()
            forged["has_real_authority_grant"] = True
            check("persisted_pending_claim_cannot_become_a_grant", raises(LedgerContractError,
                  lambda: action_from_json(canonical_json(forged))))

            other = candidate(MINT22, "b")
            other_root = repo.receive_candidate(other, fence=repo.write_fence())
            other_buy = action_for(other, units=(1 << 64)-1)
            repo.stage_action(other_buy, fence=repo.write_fence())
            sell = action_for(item, side="SELL", units=(1 << 64)-1)
            other_sell = action_for(other, side="SELL", units=77)
            repo.stage_action(sell, fence=repo.write_fence())
            repo.stage_action(other_sell, fence=repo.write_fence())
            check("two_mints_positions_and_obligations_have_distinct_pending_identity", root != other_root
                  and buy.position_id != other_buy.position_id and sell.action_id != other_sell.action_id
                  and repo.audit()["action_count"] == 4)
            changed_sell = replace(sell, input_units=sell.input_units-1)
            check("SELL_same_ordinal_cannot_change_authorized_quantity_claim", changed_sell.action_id != sell.action_id
                  and raises(LedgerConflict, lambda: repo.stage_action(changed_sell, fence=repo.write_fence())))
            check("exact_u64_pending_terms_remain_JSON_integers", repo.action(other_buy.action_id).input_units == (1 << 64)-1
                  and type(json.loads(repo._conn.execute("SELECT payload_json FROM ledger_pending_actions WHERE action_id=?",
                  (other_buy.action_id,)).fetchone()[0])["input_units"]) is int)

            # The no-commit insertion seam can be used by later concrete atomic
            # owners; a rollback leaves neither pending terms nor any admission.
            residual = action_for(other, side="SELL", units=33, ordinal=2)
            current = repo._begin_economic_write(repo.write_fence())
            repo._insert_pending_action(residual, current.revision+1)
            repo._conn.execute("ROLLBACK")
            check("no_commit_action_seam_does_not_force_a_separate_admission_commit", repo.action(residual.action_id) is None
                  and repo.inbox_disposition(other_root) == "RECEIVED")

            prepared = preparation(buy)
            invalid_header = bytearray.fromhex(prepared.message_hex)
            invalid_header[1] = 1
            check("invalid_readonly_fee_payer_message_rejected_before_storage", raises(LedgerContractError,
                  lambda: replace(prepared, message_hex=bytes(invalid_header).hex())))
            older_anchor = replace(prepared.finalized_lower_anchor, slot=100, parent_slot=99, block_height=89)
            check("attempt_lower_anchor_cannot_precede_established_custody_baseline", raises(LedgerConflict,
                  lambda: repo.prepare_attempt(replace(prepared, finalized_lower_anchor=older_anchor), fence=repo.write_fence())))
            initial_fence = repo.write_fence()
            active = repo.prepare_attempt(prepared, fence=initial_fence)
            check("prepared_exact_public_message_lease_anchor_are_durable_before_exposure", active.preparation == prepared
                  and active.recorded_stage == "PREPARED" and active.primary_signature is None and active.lane_held
                  and not active.may_send and not active.has_real_authority_grant
                  and active.preparation.lease.fingerprint == prepared.lease.fingerprint
                  and active.preparation.finalized_lower_anchor == observe().anchor)
            check("same_prepared_input_exact_retry_ignores_stale_revision", repo.prepare_attempt(prepared, fence=initial_fence) == active)
            check("other_root_cannot_bypass_held_wallet_lane", raises(LedgerConflict,
                  lambda: repo.prepare_attempt(preparation(other_buy, byte=32), fence=repo.write_fence())))
            for name, changed in (("message", preparation(buy, byte=33)), ("ordinal", preparation(buy, ordinal=2, byte=34)),
                                  ("plan", replace(prepared, plan_digest=digest("changedplan")))):
                check(f"pending_attempt_{name}_replacement_denied", raises(LedgerConflict,
                      lambda changed=changed: repo.prepare_attempt(changed, fence=repo.write_fence())))
            check("prepared_exact_last_valid_and_anchor_are_immutable", raises(LedgerConflict,
                lambda: repo.prepare_attempt(replace(prepared, lease=replace(prepared.lease, last_valid_block_height=201)), fence=repo.write_fence())))
            check("cannot_skip_exact_simulation_before_authorized_claim", raises(LedgerContractError,
                  lambda: advance(repo, prepared, "AUTHORIZED")))
            check("exact_stage_message_binding_cannot_change", raises(LedgerContractError,
                  lambda: repo.record_external_attempt_stage(replace(stage(prepared, "EXACT_SIMULATED"), message_sha256=digest("wrong")),
                  idempotency_key="wrong-message", expected_attempt_revision=0, fence=repo.write_fence())))
            for index, target in enumerate(("EXACT_SIMULATED", "AUTHORIZED", "SIGNED_DURABLE", "SEND_CLAIMED", "OBSERVING"), start=2):
                view = advance(repo, prepared, target, at=NOW+index)
                check(f"stored_{target}_is_a_claim_with_no_grant_or_send", view.recorded_stage == target
                      and view.admission_status == PENDING_ADMISSION and not view.has_real_authority_grant and not view.may_send)
            before_repeat = repo.attempt(prepared.attempt_id)
            same_transport = advance(repo, prepared, "OBSERVING", at=NOW+7, key="same-byte-observation")
            check("same_byte_transport_observation_stays_same_attempt_signature", same_transport.preparation.attempt_id == prepared.attempt_id
                  and same_transport.primary_signature == before_repeat.primary_signature
                  and repo.audit()["attempt_count"] == 1
                  and repo._conn.execute("SELECT COUNT(*) FROM ledger_signature_bindings").fetchone()[0] == 1)
            duplicate_stage = stage(prepared, "OBSERVING", at=NOW+7)
            check("exact_stage_delivery_replay_does_not_advance_revision", repo.record_external_attempt_stage(duplicate_stage,
                  idempotency_key="same-byte-observation", expected_attempt_revision=before_repeat.revision,
                  fence=repo.write_fence()).revision == same_transport.revision)
            check("same_stage_key_different_content_rejected", raises(LedgerConflict,
                  lambda: repo.record_external_attempt_stage(replace(duplicate_stage, external_record_digest=digest("changed")),
                  idempotency_key="same-byte-observation", expected_attempt_revision=before_repeat.revision, fence=repo.write_fence())))
            unknown = advance(repo, prepared, "UNKNOWN", at=NOW+8)
            check("UNKNOWN_preserves_exact_lineage_and_held_lane", unknown.recorded_stage == "UNKNOWN" and unknown.lane_held
                  and unknown.primary_signature == same_transport.primary_signature and repo.mutation_lane()["held"])
            competing_revision = repo.audit()["revision"]
            competing = run_child("--compete", path)
            check("competing_OS_writer_cannot_touch_pending_economic_state", competing.returncode == 83
                  and not competing.stdout and not competing.stderr and repo.audit()["revision"] == competing_revision)
            for target in LEDGER_FINALITY_STATES:
                check(f"caller_cannot_assert_{target}", raises(LedgerContractError, lambda target=target: stage(prepared, target)))
            check("UNKNOWN_cannot_be_erased_by_a_new_external_simulation_claim", raises(LedgerContractError,
                  lambda: advance(repo, prepared, "EXACT_SIMULATED", at=NOW+9, key="bad-unknown-reset")))
            canceled = cancel(repo, prepared, at=NOW+9)
            check("signed_or_claimed_cancel_is_UNKNOWN_never_never_submitted", canceled.recorded_stage == "UNKNOWN" and canceled.lane_held)
            check("expiry_tombstone_cannot_discard_possible_landing_attempt", raises(LedgerConflict,
                  lambda: repo.record_nonacceptance(root, "EXPIRED", external_reference="external-expiry",
                    external_record_digest=digest("expiry"), recorded_at_utc=utc(NOW+1000), idempotency_key="expiry",
                    fence=repo.write_fence())))
            stale_view = repo.write_fence()
            unknown_scenario = Scenario()
            unknown_scenario.program_failure = TOKEN_2022_PROGRAM_ID
            extra_wallet = ingest(repo, observe(unknown_scenario), key="later-unknown-wallet")
            check("original_wallet_receipts_interleave_with_economic_commits", extra_wallet.sequence > 1
                  and repo.baseline() == first_baseline and repo.audit()["replayed_receipts"] == 2)
            check("new_economic_write_rejects_stale_global_CAS", raises(LedgerConflict,
                  lambda: repo.receive_candidate(candidate(MINT22, "new-candidate"), fence=stale_view)))
            before_reopen = repo.audit()
            old_generation_fence = repo.write_fence()
        with LedgerRepository.reopen(path, domain()) as repo:
            recovered = repo.attempt(prepared.attempt_id)
            check("reopen_reconstructs_UNKNOWN_signature_and_lane_from_durable_history", recovered == canceled
                  and repo.mutation_lane()["active_attempt_id"] == prepared.attempt_id
                  and repo.audit()["last_receipt_digest"] == before_reopen["last_receipt_digest"]
                  and repo.audit()["generation"] == before_reopen["generation"]+1)
            check("stale_writer_generation_cannot_mutate_or_replay", raises(LedgerConflict,
                  lambda: repo.receive_candidate(item, fence=old_generation_fence)))
            check("timeout_new_ordinal_or_new_position_cannot_replace_UNKNOWN", raises(LedgerConflict,
                  lambda: repo.prepare_attempt(preparation(buy, ordinal=2, byte=55, at=NOW+5000), fence=repo.write_fence()))
                  and raises(LedgerConflict, lambda: repo.prepare_attempt(preparation(other_sell, byte=56, at=NOW+5000), fence=repo.write_fence())))
            stable_digest = repo.audit()["last_receipt_digest"]
            check("same_candidate_and_action_replay_preserve_tombstones_and_terms", repo.receive_candidate(item, fence=repo.write_fence()) == root
                  and repo.stage_action(buy, fence=repo.write_fence()) == buy and repo.audit()["last_receipt_digest"] == stable_digest)

        for prior in ("PREPARED", "EXACT_SIMULATED", "AUTHORIZED"):
            local, _item, local_root, local_buy = setup(directory / f"cancel-{prior}.sqlite3")
            with local:
                prep = preparation(local_buy)
                local.prepare_attempt(prep, fence=local.write_fence())
                if prior != "PREPARED":
                    advance(local, prep, "EXACT_SIMULATED", at=NOW+2)
                if prior == "AUTHORIZED":
                    advance(local, prep, "AUTHORIZED", at=NOW+3)
                final = cancel(local, prep, at=NOW+4)
                check(f"{prior}_cancel_in_intact_writer_generation_is_positive_unsigned",
                      final.recorded_stage == "CANCELLED_UNSIGNED" and not final.lane_held and not local.mutation_lane()["held"])
                check(f"{prior}_same_bytes_cannot_be_new_attempt_after_cancel", raises(LedgerConflict,
                      lambda: local.prepare_attempt(replace(prep, ordinal=2, prepared_at_utc=utc(NOW+5)), fence=local.write_fence())))
                new = preparation(local_buy, ordinal=2, byte=44, at=NOW+5)
                local.prepare_attempt(new, fence=local.write_fence())
                check(f"{prior}_explicit_pending_next_message_preserves_action_and_amount",
                      local.attempt(new.attempt_id).preparation.action_id == local_buy.action_id
                      and not local.attempt(new.attempt_id).has_real_authority_grant)
                cancel(local, new, at=NOW+6, key="next-cancel")
                local.record_nonacceptance(local_root, "EXPIRED", external_reference="external-expiry",
                    external_record_digest=digest("expiry"), recorded_at_utc=utc(NOW+7), idempotency_key="expired", fence=local.write_fence())
                check(f"{prior}_terminal_tombstone_blocks_new_attempt", raises(LedgerConflict,
                      lambda: local.prepare_attempt(preparation(local_buy, ordinal=3, byte=45, at=NOW+8), fence=local.write_fence())))
                check(f"{prior}_terminal_tombstone_is_rebuilt", local.audit()["attempt_count"] == 2
                      and local.receive_candidate(_item, fence=local.write_fence()) == local_root
                      and local.inbox_disposition(local_root) == "EXPIRED")

        for prior in ("PREPARED", "SIGNED_DURABLE"):
            recovery_path = directory / f"restart-{prior}.sqlite3"
            local, _item, _root, local_buy = setup(recovery_path)
            with local:
                prep = preparation(local_buy)
                local.prepare_attempt(prep, fence=local.write_fence())
                if prior == "SIGNED_DURABLE":
                    advance(local, prep, "EXACT_SIMULATED", at=NOW+2)
                    advance(local, prep, "AUTHORIZED", at=NOW+3)
                    advance(local, prep, "SIGNED_DURABLE", at=NOW+4)
            with LedgerRepository.reopen(recovery_path, domain()) as local:
                uncertain = cancel(local, prep)
                check(f"restart_{prior}_no_claim_is_not_export_or_unsigned_proof", uncertain.recorded_stage == "UNKNOWN"
                      and uncertain.lane_held and local.audit()["attempt_count"] == 1)

        for terminal in ("REJECTED", "EXPIRED"):
            local, inp, terminal_root, pending_buy = setup(directory / f"tombstone-{terminal}.sqlite3", staged=False)
            with local:
                local.record_nonacceptance(terminal_root, "DENIED_RETRYABLE", external_reference="external-denial",
                    external_record_digest=digest("denial"), recorded_at_utc=utc(), idempotency_key="retryable", fence=local.write_fence())
                local.record_nonacceptance(terminal_root, terminal, external_reference="external-terminal",
                    external_record_digest=digest("terminal"), recorded_at_utc=utc(NOW+1), idempotency_key="terminal", fence=local.write_fence())
                check(f"{terminal}_blocks_action_staging_without_deleting_candidate", raises(LedgerConflict,
                      lambda: local.stage_action(pending_buy, fence=local.write_fence())) and local.candidate(terminal_root) == inp)
                local.audit()
            with LedgerRepository.reopen(directory / f"tombstone-{terminal}.sqlite3", domain()) as local:
                check(f"{terminal}_survives_restart_and_at_least_once_replay", local.receive_candidate(inp, fence=local.write_fence()) == terminal_root
                      and local.inbox_disposition(terminal_root) == terminal and local.audit()["candidate_count"] == 1)

        for cut in ("before_begin", "after_commit"):
            race_path = directory / f"guard-{cut}.sqlite3"
            with LedgerRepository.initialize(race_path, domain()) as local:
                ingest(local, observe())
                local._conn = ConnectionCut(local._conn, race_path, cut)
                if cut == "before_begin":
                    check("external_commit_before_BEGIN_is_checked_inside_transaction", raises(LedgerConflict,
                          lambda: local.receive_candidate(candidate(), fence=local.write_fence()))
                          and local._conn.execute("SELECT COUNT(*) FROM ledger_candidate_inbox").fetchone()[0] == 0)
                else:
                    local.receive_candidate(candidate(), fence=local.write_fence())
                    check("external_commit_after_COMMIT_is_never_blessed_as_trusted_cache", raises(LedgerConflict,
                          lambda: local.receive_candidate(candidate(MINT22, "race-b"), fence=local.write_fence()))
                          and local._conn.execute("SELECT COUNT(*) FROM ledger_candidate_inbox").fetchone()[0] == 1)

        unchanged_path = directory / "wallet-external-noop.sqlite3"
        with LedgerRepository.initialize(unchanged_path, domain()) as local:
            original_observation = observe()
            original_receipt = ingest(local, original_observation)
            original_fence = local.write_fence()
            with closing(sqlite3.connect(unchanged_path, isolation_level=None)) as other:
                application_id = other.execute("PRAGMA application_id").fetchone()[0]
                other.execute(f"PRAGMA application_id={application_id}")
            check("external_same_metadata_commit_changes_data_version_without_changing_facts",
                  local._conn.execute("PRAGMA data_version").fetchone()[0] != local._verified_data_version
                  and local.write_fence() == original_fence and local.baseline() == original_receipt)
            check("Wallet_ingestion_rejects_structurally_valid_external_commit", raises(LedgerConflict,
                  lambda: ingest(local, original_observation)) and local.write_fence() == original_fence)
        with LedgerRepository.reopen(unchanged_path, domain()) as local:
            check("explicit_reopen_reverifies_unchanged_Wallet_journal_and_exact_retry",
                  ingest(local, original_observation) == original_receipt
                  and local.audit()["revision"] == original_fence.revision
                  and local.write_fence().generation == original_fence.generation+1)

        # A targeted assertion proves economic append does not re-decode the
        # entire Wallet Evidence history under already-verified exclusive ownership.
        import live.ledger_repository_v0_1 as storage_module
        bounded, inp, bounded_root, bounded_buy = setup(directory / "bounded-read.sqlite3", staged=False)
        with bounded:
            original_decode = storage_module.wallet_observation_from_json
            storage_module.wallet_observation_from_json = lambda _value: (_ for _ in ()).throw(AssertionError("unbounded wallet reread"))
            try:
                bounded.stage_action(bounded_buy, fence=bounded.write_fence())
                bounded.prepare_attempt(preparation(bounded_buy), fence=bounded.write_fence())
                check("economic_appends_do_not_redecode_all_original_wallet_Evidence", True)
            finally:
                storage_module.wallet_observation_from_json = original_decode
            bounded.audit()

        crash_results = {}
        for operation in ("action", "attempt"):
            for cut in ("before", "after"):
                crash_path = directory / f"{operation}-{cut}.sqlite3"
                local, _item, _root, pending = setup(crash_path, staged=operation == "attempt")
                local.close()
                result = run_child(f"--{operation}-{cut}", crash_path)
                expected = 0 if cut == "before" else 1
                check(f"{operation}_{cut}_COMMIT_child_reached_cut", result.returncode == (81 if cut == "before" else 82)
                      and not result.stdout and not result.stderr)
                with LedgerRepository.reopen(crash_path, domain()) as local:
                    count = local.audit()["action_count" if operation == "action" else "attempt_count"]
                    check(f"{operation}_{cut}_atomic_fact_and_common_commit_recovery", count == expected
                          and (operation == "action" or local.mutation_lane()["held"] == bool(expected)))
                    if operation == "action":
                        value = local.stage_action(pending, fence=local.write_fence())
                        fact_digest = value.content_digest
                    else:
                        value = local.prepare_attempt(preparation(pending), fence=local.write_fence())
                        fact_digest = value.preparation.content_digest
                    check(f"{operation}_{cut}_exact_retry_converges_to_one_fact",
                          local.audit()["action_count" if operation == "action" else "attempt_count"] == 1)
                    crash_results[f"{operation}-{cut}"] = {"child_exit": result.returncode, "recovered_before_retry": count,
                                                           "after_retry": 1, "fact_digest": fact_digest}
            check(f"{operation}_both_crash_cuts_keep_same_economic_identity_and_content",
                  crash_results[f"{operation}-before"]["fact_digest"] == crash_results[f"{operation}-after"]["fact_digest"])

        damaged = directory / "tampered-lane.sqlite3"
        local, _item, _root, pending = setup(damaged)
        with local:
            local.prepare_attempt(preparation(pending), fence=local.write_fence())
        with closing(sqlite3.connect(damaged, isolation_level=None)) as connection:
            connection.execute("UPDATE ledger_mutation_lane SET active_attempt_id=NULL")
        check("reopen_rejects_fabricated_lane_release", raises(LedgerJournalError, lambda: LedgerRepository.reopen(damaged, domain())))
        incompatible = directory / "old-version.sqlite3"
        with LedgerRepository.initialize(incompatible, domain()):
            pass
        with closing(sqlite3.connect(incompatible, isolation_level=None)) as connection:
            connection.execute("PRAGMA user_version=1")
        check("old_fixture_storage_version_requires_explicit_binding_not_auto_migration", raises(LedgerJournalError,
              lambda: LedgerRepository.reopen(incompatible, domain())))

        report = {"status": "IMPLEMENTED_PENDING_PROJECT_REVIEW", "checks": CHECKS, "failed": [],
            "real_network_requests": 0, "private_key_or_signing_operations": 0, "database_scope": "TEMPORARY_DIRECTORY_ONLY",
            "crash_cuts": crash_results, "replayed_unknown_commit_digest": stable_digest,
            "canonical_root": root, "BUY_action_id": buy.action_id, "attempt_id": prepared.attempt_id}
    check("all_temporary_databases_and_child_artifacts_cleaned", not directory.exists())
    report["check_count"] = len(CHECKS)
    print(json.dumps(report, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] in ("--action-before", "--action-after", "--attempt-before", "--attempt-after", "--compete"):
        raise SystemExit(child(sys.argv[1], Path(sys.argv[2])))
    if len(sys.argv) != 1:
        raise SystemExit("unsupported selftest arguments")
    raise SystemExit(main())
