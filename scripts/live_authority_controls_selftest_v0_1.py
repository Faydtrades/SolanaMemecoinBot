"""A1 deterministic public controls/source/clock journal fixtures, no real authority operator.

Actual temporary CollectorSourceAdapter and SourceEvidenceStore feed Ledger.
Synthetic HUMAN_EXTERNAL values prove the contract, not production approval.
"""
from __future__ import annotations

import json
import os
import subprocess
import sqlite3
import sys
import tempfile
from contextlib import closing
from dataclasses import FrozenInstanceError, asdict, replace
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from phase5.shadow_domain_v0_1 import canonical_json, content_fingerprint
from live.authority_controls_v0_1 import (
    OperatorProvenance, EntrySizeLimits, CostLimits, ClockPolicy, AuthorityPolicy, ArmingGrant,
    ControlCommand, TrustedClockSample, ClockReconciliation, authority_receipt_from_record,
)
from live.ledger_repository_v0_1 import LedgerRepository, LedgerConflict, LedgerJournalError, _DDL
from live.ledger_domain_v0_1 import LedgerContractError, ZERO_DIGEST
from live.ledger_actions_v0_1 import utc_microseconds
from live.evidence_store_v0_1 import SourceEvidenceStore
from live.source_health_v0_1 import SourceProfile
import live_source_health_selftest_v0_1 as source_fixture
from live_ledger_actions_selftest_v0_1 import candidate, digest
from live_ledger_baseline_selftest_v0_1 import domain, observe, ingest
from live_wallet_evidence_selftest_v0_1 import NOW, utc

source_fixture.BASE = datetime.fromtimestamp(NOW-8, timezone.utc)
CHECKS = {}
FIXTURES = []


def check(name, value):
    CHECKS[name] = bool(value)
    if not value:
        raise AssertionError(name)


def raises(call, cls=LedgerContractError):
    try:
        call()
    except cls:
        return True
    return False


def operator(at=NOW, name="owner"):
    return OperatorProvenance(name, "SYNTHETIC_PUBLIC_APPROVAL", digest(name), utc(at), "HUMAN_EXTERNAL")


def policy(item, *, name="p1", track="FINAL-A", approval=None, binding=None):
    binding = binding or domain()
    return AuthorityPolicy(binding.economic_domain_id, name, approval or operator(), utc(NOW-100), utc(NOW+100),
        ("PUMP", "PUMPSWAP"), track, item.strategy_version, item.parameter_set_id, item.parameter_fingerprint,
        item.model_fingerprint, item.winner_binding_digest, item.source_binding.source_identity,
        SourceProfile().fingerprint, binding.expected_profile_fingerprint, "LEDGER_SETTLEMENT_SUPPORTED_V0_1",
        EntrySizeLimits(1000000, 1, 1000000, 1000000, 2000000, 1000000, 1),
        CostLimits(10000, 10000, 1000, 3000000, 3000000, 10000, 3000000, 3000000, 2, 20000, 100, 100),
        ClockPolicy("SYNTHETIC_TRUSTED_CLOCK", digest("clock-provider"), 1000000, 100, 3600000000))


def control(repo, operation, command_id, *, at=NOW, policy_value=None, grant=None, target=None, barrier=None):
    approval = policy_value.approval if policy_value is not None else grant.approval if grant is not None else operator(at)
    command = ControlCommand(command_id, repo.domain.economic_domain_id, operation, approval,
        policy_value, grant, target, barrier)
    return repo.record_authority_control(command, fence=repo.write_fence())


def arm(repo, policy_value, *, name="grant1", scope="ENTRY_NORMAL", root=None, at=NOW):
    grant = ArmingGrant(name, policy_value.content_digest, scope, root, utc(NOW), utc(NOW+100), operator(at))
    return control(repo, "ARM", "arm-"+name, grant=grant)


def clock(repo, at=NOW+4, *, upper=None, epoch="boot1", status="QUALIFIED", reconciliation=None, monotonic_ns=None):
    previous = repo.authority_snapshot()["last_qualified_clock"]
    return TrustedClockSample("SYNTHETIC_TRUSTED_CLOCK", digest("clock-provider"), epoch,
        (at-NOW)*1000000000 if monotonic_ns is None else monotonic_ns,
        utc(at), utc(at if upper is None else upper), digest("sample-"+str(at)),
        ZERO_DIGEST if previous is None else previous.content_digest, status, reconciliation)


class Fixture:
    def __init__(self, directory, name, *, track="FINAL-A", mode="LIVE", arm_now=True, install_now=True, profile=None):
        FIXTURES.append(self)
        self.directory, self.name = directory, name
        self.raw = directory/(name+"-source.sqlite3")
        binding = source_fixture.fixture(self.raw)
        self.source = SourceEvidenceStore(directory/(name+"-evidence.sqlite3"), binding, profile or SourceProfile())
        self.initial = source_fixture.observe(self.raw, binding, profile=self.source.profile)
        self.source.append(self.initial, expected_previous_digest=ZERO_DIGEST)
        self.item = replace(candidate(), source_binding=binding)
        self.domain = domain(mode=mode)
        self.path = directory/(name+"-ledger.sqlite3")
        self.repo = LedgerRepository.initialize(self.path, self.domain)
        ingest(self.repo, observe())
        self.root = self.repo.receive_candidate(self.item, fence=self.repo.write_fence())
        self.policy = replace(policy(self.item, track=track, binding=self.domain), source_profile_fingerprint=self.source.profile.fingerprint)
        if install_now:
            control(self.repo, "INSTALL_POLICY", "install", policy_value=self.policy)
        if arm_now and install_now:
            arm(self.repo, self.policy, scope="DRY" if mode == "DRY" else "ENTRY_NORMAL")

    def evaluate(self, name, *, at=NOW+4, track=None, sample=None, store=None):
        return self.repo.evaluate_authority_entry(self.root, track or self.policy.selected_track,
            sample or clock(self.repo, at), store or self.source, command_id=name, fence=self.repo.write_fence())

    def reopen(self):
        self.repo.close()
        self.repo = LedgerRepository.reopen(self.path, self.domain)

    def close(self):
        self.repo.close()
        self.source.close()


def basic(directory):
    f = Fixture(directory, "basic")
    receipt = f.evaluate("eligible")
    check("actual_source_and_equal_whole_second_clock_are_eligible", receipt.decision.disposition == "ELIGIBLE_CONTEXT_ONLY")
    check("eligibility_never_admission_or_message_grant", not receipt.decision.grants_message_permission and not receipt.decision.consumes_grant
        and not f.repo.consumer_snapshot()["reservations"] and f.repo.inbox_disposition(f.root) == "RECEIVED")
    check("exact_original_source_is_persisted", receipt.original.source == f.source.latest_record()[1]
        and receipt.original.source_sequence == 1 and receipt.decision.source_identity == f.initial.binding.source_identity)
    check("typed_canonical_receipt_roundtrip", authority_receipt_from_record(json.loads(canonical_json(asdict(receipt)))) == receipt)
    check("public_policy_facts_immutable", raises(lambda: setattr(f.policy.size, "fixed_quote_lamports", 999), FrozenInstanceError))
    original_fence = f.repo.write_fence()
    check("exact_retry_converges_without_revision", f.evaluate("eligible", sample=receipt.original.clock) == receipt and f.repo.write_fence() == original_fence)
    check("same_command_changed_clock_conflicts", raises(lambda: f.evaluate("eligible", at=NOW+5)))
    f.reopen()
    check("historical_original_receipt_replays_without_today", f.repo.authority_receipt("eligible") == receipt)
    check("source_and_clock_checkpoint_survive_reopen", f.repo.authority_snapshot()["last_qualified_clock"] == receipt.original.clock
        and f.repo.authority_snapshot()["current_source_checkpoint"].record_digest == f.initial.content_digest)
    check("snapshot_never_claims_current_authority", f.repo.authority_snapshot()["authority_current"] is False)
    expired = f.evaluate("expired", at=NOW+15)
    check("backlog_after_restart_proves_original_expiry", expired.decision.disposition == "EXPIRED"
        and expired.decision.binding.deadline_us == f.item.generated_at_us+15000000)
    changed = replace(f.policy, policy_id="p2", selected_track="FINAL-B", approval=operator(NOW+15))
    control(f.repo, "INSTALL_POLICY", "replacement", policy_value=changed)
    check("retryable_or_expired_binding_cannot_change_track", raises(lambda: f.evaluate("other-track", at=NOW+15, track="FINAL-B")))
    check("new_policy_cannot_refresh_age", f.evaluate("still-expired", at=NOW+16).decision.disposition == "EXPIRED")
    original_digest = f.repo.audit()["authority_digest"]
    f.reopen()
    check("mixed_control_eligibility_history_reconstructs", f.repo.audit()["authority_digest"] == original_digest)
    f.close()
    return receipt.content_digest, original_digest


def controls(directory):
    f = Fixture(directory, "controls", arm_now=False)
    check("install_does_not_autoarm", f.repo.authority_snapshot()["armed_entry_grant"] is None
        and "ENTRY_UNARMED" in f.evaluate("unarmed").decision.reasons)
    once = arm(f.repo, f.policy, scope="ENTRY_ONCE", root=f.root)
    check("one_time_issuance_unconsumed", f.evaluate("once").decision.disposition == "ELIGIBLE_CONTEXT_ONLY"
        and f.repo.authority_grant("grant1") == once.original.grant and not f.repo.consumer_snapshot()["reservations"])
    stop = control(f.repo, "HARD_STOP", "hard", at=NOW+4)
    control(f.repo, "STOP_ENTRY", "entry", at=NOW+4)
    changed = replace(f.policy, policy_id="p2", approval=operator(NOW+4))
    control(f.repo, "INSTALL_POLICY", "replace", policy_value=changed)
    f.reopen()
    check("independent_stops_survive_policy_and_reopen", f.repo.authority_snapshot()["hard_stop_command"] == "hard"
        and f.repo.authority_snapshot()["entry_stop_command"] == "entry")
    check("arm_cannot_bypass_hard_stop", raises(lambda: arm(f.repo, changed, name="new", at=NOW+4)))
    check("release_requires_exact_latch", raises(lambda: control(f.repo, "RELEASE_STOP", "wrong-release", at=NOW+4, target="wrong", barrier=digest("barrier"))))
    control(f.repo, "RELEASE_STOP", "release-hard", at=NOW+4, target="hard", barrier=digest("barrier"))
    check("release_one_stop_does_not_clear_entry_or_arm", f.repo.authority_snapshot()["hard_stop_command"] is None
        and f.repo.authority_snapshot()["entry_stop_command"] == "entry" and f.repo.authority_snapshot()["armed_entry_grant"] is None)
    control(f.repo, "RELEASE_STOP", "release-entry", at=NOW+4, target="entry", barrier=digest("barrier"))
    check("release_alone_is_no_message_permission", "ENTRY_UNARMED" in f.evaluate("released").decision.reasons)
    arm(f.repo, changed, name="g2", at=NOW+4)
    control(f.repo, "REVOKE_GRANT", "revoke", at=NOW+4, target="g2")
    f.reopen()
    check("revocation_survives_reopen", f.repo.authority_snapshot()["armed_entry_grant"] is None)
    historical = arm(f.repo, changed, name="g2", at=NOW+4)
    check("exact_old_arm_retry_does_not_rearm_revoked_grant", historical.original.grant.grant_id == "g2"
        and f.repo.authority_snapshot()["armed_entry_grant"] is None)
    check("revoked_grant_id_cannot_be_recreated", raises(lambda: control(f.repo, "ARM", "new-command-old-grant", grant=historical.original.grant)))
    check("old_grant_id_cannot_be_reissued_under_new_policy", raises(lambda: arm(f.repo, changed, name="grant1", at=NOW+4)))
    check("same_policy_id_different_content_conflicts", raises(lambda: control(f.repo, "INSTALL_POLICY", "policy-conflict",
        policy_value=replace(changed, size=replace(changed.size, fixed_quote_lamports=999)))))
    check("wrong_policy_domain_denied", raises(lambda: control(f.repo, "INSTALL_POLICY", "wrong-domain",
        policy_value=replace(changed, policy_id="wrong", economic_domain_id=digest("other")))))
    check("operator_time_regression_denied", raises(lambda: control(f.repo, "DISARM_ENTRY", "backdated", at=NOW)))
    check("automatic_approval_origin_denied", raises(lambda: replace(operator(), origin="AUTO")))
    check("unavailable_human_risk_fields_cannot_default", raises(lambda: EntrySizeLimits(), TypeError))
    check("negative_integer_budget_denied", raises(lambda: replace(changed.costs, network_total_fee_lamports=-1), ValueError))
    check("float_budget_denied", raises(lambda: replace(changed.size, fixed_quote_lamports=1.5), ValueError))
    check("unknown_venue_denied", raises(lambda: replace(changed, allowed_venues=("UNKNOWN",))))
    check("unknown_account_profile_denied", raises(lambda: replace(changed, account_profile="ANY")))
    wide=replace(changed, policy_id="exact-u64", size=EntrySizeLimits(*([(1<<64)-1]*7)))
    control(f.repo, "INSTALL_POLICY", "wide-policy", policy_value=wide)
    f.reopen()
    check("policy_exact_u64_survives_SQL_and_replay", f.repo.authority_policy("exact-u64").size.fixed_quote_lamports == (1<<64)-1)
    check("one_time_requires_exact_root", raises(lambda: ArmingGrant("unbound", wide.content_digest, "ENTRY_ONCE", None,
        utc(NOW), utc(NOW+100), operator())))
    check("grant_unknown_root_denied", raises(lambda: arm(f.repo, wide, name="unknown-root", scope="ENTRY_ONCE", root=digest("missing-root"), at=NOW+4)))
    check("all_mutation_stop_distinct_from_entry", stop.original.operation == "HARD_STOP"
        and f.repo.authority_snapshot()["protection_requires_position_obligation_and_current_resource_validation"])
    f.close()
    f = Fixture(directory, "zero", arm_now=False)
    zero = replace(f.policy, policy_id="zero", size=EntrySizeLimits(0, 0, 0, 0, 0, 0, 0))
    control(f.repo, "INSTALL_POLICY", "zero", policy_value=zero)
    arm(f.repo, zero)
    check("zero_policy_stays_denied_when_explicitly_armed", "EXPLICIT_ZERO_ENTRY_POLICY" in f.evaluate("zero-denied").decision.reasons)
    f.close()
    f = Fixture(directory, "dry", mode="DRY")
    check("dry_scope_context_is_explicit", f.evaluate("dry-context").decision.disposition == "ELIGIBLE_CONTEXT_ONLY"
        and f.repo.authority_snapshot()["armed_entry_grant"].scope == "DRY")
    check("live_grant_cannot_arm_dry", raises(lambda: arm(f.repo, f.policy, name="live-in-dry")))
    f.close()
    f = Fixture(directory, "live-mode")
    check("dry_grant_cannot_arm_live", raises(lambda: arm(f.repo, f.policy, name="dry-in-live", scope="DRY")))
    f.close()


def clocks(directory):
    for track, offset in (("FINAL-A", 14), ("FINAL-B", 14), ("SENS-C", 4)):
        f=Fixture(directory, "deadline-"+track, track=track)
        r=f.evaluate("equal", at=NOW+offset)
        check(track+"_inclusive_deadline", r.decision.disposition == "ELIGIBLE_CONTEXT_ONLY")
        sample=clock(f.repo, NOW+offset, upper=NOW+offset+1)
        r=f.evaluate("straddle", sample=sample)
        check(track+"_straddle_not_expired", r.decision.disposition == "DENIED_UNPROVEN"
            and "ORIGINAL_DEADLINE_AMBIGUOUS" in r.decision.reasons)
        f.close()
    f=Fixture(directory, "clock-faults")
    first=f.evaluate("first")
    bad=f.evaluate("backward", at=NOW+3)
    check("backward_clock_denies_without_expiry", bad.decision.disposition == "DENIED_UNPROVEN"
        and "CLOCK_BACKWARD_BOUNDS" in bad.decision.reasons)
    f.reopen()
    check("known_good_anchor_retained_after_bad_clock", f.repo.authority_snapshot()["last_qualified_clock"] == first.original.clock
        and f.repo.authority_snapshot()["clock_requires_reconciliation"])
    no_proof=f.evaluate("no-proof", at=NOW+5)
    check("clock_fault_recovery_requires_reconciliation", "CLOCK_CONTINUITY_RECONCILIATION_REQUIRED" in no_proof.decision.reasons)
    old=first.original.clock
    proof=ClockReconciliation(old.content_digest, old.epoch_id, old.monotonic_ns, digest("public-clock-checkpoint"))
    recovered=f.evaluate("recovered", sample=clock(f.repo, NOW+6, reconciliation=proof))
    check("explicit_clock_reconciliation_recovers_original_window", recovered.decision.disposition == "ELIGIBLE_CONTEXT_ONLY")
    bad=f.evaluate("unknown-far-future", sample=clock(f.repo, NOW+1000, status="UNKNOWN"))
    check("unknown_future_clock_does_not_prove_expired", bad.decision.disposition == "DENIED_UNPROVEN")
    f.close()
    f=Fixture(directory, "clock-new-epoch-proof")
    old=f.evaluate("original").original.clock
    proof=ClockReconciliation(old.content_digest, old.epoch_id, old.monotonic_ns, digest("public-reboot-checkpoint"))
    result=f.evaluate("new-epoch-qualified", sample=clock(f.repo, NOW+5, epoch="boot2", monotonic_ns=100, reconciliation=proof))
    check("explicit_cross_epoch_checkpoint_recovers_without_renewing_deadline", result.decision.disposition == "ELIGIBLE_CONTEXT_ONLY"
        and result.decision.binding.deadline_us == f.item.generated_at_us+15000000)
    f.reopen()
    check("new_epoch_original_clock_proof_replays", f.repo.authority_receipt("new-epoch-qualified") == result)
    f.close()
    for name, change, reason in (
        ("new-epoch", {"epoch_id":"boot2"}, "CLOCK_CONTINUITY_RECONCILIATION_REQUIRED"),
        ("provider", {"provider_fingerprint":digest("wrong")}, "CLOCK_PROVIDER_UNSUPPORTED"),
        ("mono", {"monotonic_ns":1}, "CLOCK_MONOTONIC_REGRESSION"),
        ("drift", {"monotonic_ns":900000000000}, "CLOCK_UTC_MONOTONIC_DIVERGENCE"),
        ("checkpoint", {"previous_sample_digest":ZERO_DIGEST}, "CLOCK_CHECKPOINT_DIGEST_CONFLICT"),
        ("uncertainty", {"utc_upper_utc":utc(NOW+7)}, "CLOCK_UNCERTAINTY_EXCEEDED")):
        f=Fixture(directory, "clock-"+name)
        f.evaluate("initial")
        result=f.evaluate("fault", sample=replace(clock(f.repo, NOW+5), **change))
        check(name+"_clock_denial", result.decision.disposition == "DENIED_UNPROVEN" and reason in result.decision.reasons)
        f.reopen()
        check(name+"_clock_denial_replays", f.repo.authority_receipt("fault") == result)
        f.close()


def sources(directory):
    f=Fixture(directory, "prepolicy-gap", install_now=False)
    source_fixture.add_gap(f.raw, "PENDING")
    gap=source_fixture.observe(f.raw, f.source.binding, previous=f.initial, now=13)
    f.source.append(gap, expected_previous_digest=f.initial.content_digest)
    unconfigured=f.evaluate("unconfigured-gap", at=NOW+5)
    check("unconfigured_source_gap_is_durable", "POLICY_MISSING" in unconfigured.decision.reasons
        and unconfigured.original.source_sequence == 2)
    f.reopen()
    control(f.repo, "INSTALL_POLICY", "install-after-gap", policy_value=f.policy)
    arm(f.repo, f.policy)
    old_store=SourceEvidenceStore(directory/"prepolicy-old-copy.sqlite3", f.source.binding, f.source.profile)
    old_store.append(f.initial, expected_previous_digest=ZERO_DIGEST)
    denied=f.evaluate("after-policy-old-copy", at=NOW+6, store=old_store)
    check("policy_install_cannot_erase_prepolicy_gap_checkpoint", "SOURCE_SEQUENCE_OR_RETAINED_RECORD_CONFLICT" in denied.decision.reasons)
    f.reopen()
    check("prepolicy_source_rollback_denial_replays", f.repo.authority_receipt("after-policy-old-copy") == denied)
    old_store.close(); f.close()
    f=Fixture(directory, "source-stale", profile=SourceProfile(freshness_seconds=5))
    stale=f.evaluate("stale", at=NOW+8)
    check("stale_source_denies_within_original_deadline", stale.decision.disposition == "DENIED_UNPROVEN"
        and "LIVE_RECEIPT_STALE" in stale.decision.reasons)
    source_fixture.change(f.raw, "INSERT INTO websocket_observations VALUES (?,?,?,?,?)", ("sig3", 103, source_fixture.at(18), 1, ""))
    fresh=source_fixture.observe(f.raw, f.source.binding, previous=f.initial, now=19, cut=17, profile=f.source.profile)
    f.source.append(fresh, expected_previous_digest=f.initial.content_digest)
    recovered=f.evaluate("fresh", at=NOW+11)
    check("same_lineage_source_recovery_restores_eligibility_within_fixed_window", recovered.decision.disposition == "ELIGIBLE_CONTEXT_ONLY"
        and recovered.decision.binding == stale.decision.binding and recovered.original.source_sequence == 2)
    f.reopen()
    check("source_recovery_original_inputs_replay", f.repo.authority_receipt("fresh") == recovered)
    f.close()
    f=Fixture(directory, "source-gap")
    good=f.evaluate("healthy")
    source_fixture.add_gap(f.raw, "PENDING")
    gap=source_fixture.observe(f.raw, f.source.binding, previous=f.initial, now=13)
    f.source.append(gap, expected_previous_digest=f.initial.content_digest)
    denied=f.evaluate("new-gap", at=NOW+5)
    check("latest_gap_overrides_old_healthy", denied.original.source_sequence == 2 and "CURRENT_SOURCE_NOT_USABLE" in denied.decision.reasons)
    old_store=SourceEvidenceStore(directory/"old-source-copy.sqlite3", f.source.binding, f.source.profile)
    old_store.append(f.initial, expected_previous_digest=ZERO_DIGEST)
    f.reopen()
    rollback=f.evaluate("rollback", at=NOW+6, store=old_store)
    check("older_copied_source_store_cannot_restore_health", "SOURCE_SEQUENCE_OR_RETAINED_RECORD_CONFLICT" in rollback.decision.reasons)
    check("old_exact_receipt_is_only_historical", f.evaluate("healthy", sample=good.original.clock) == good
        and f.repo.authority_snapshot()["current_source_checkpoint"].sequence == 2)
    old_store.close(); f.close()
    f=Fixture(directory, "source-wrong")
    wrong_binding=replace(f.source.binding, lineage="WRONG")
    wrong=SourceEvidenceStore(directory/"wrong-source.sqlite3", wrong_binding, f.source.profile)
    wrong_value=source_fixture.observe(f.raw, wrong_binding)
    wrong.append(wrong_value, expected_previous_digest=ZERO_DIGEST)
    check("wrong_source_identity_denies", "CONSUMER_SOURCE_IDENTITY_MISMATCH" in f.evaluate("wrong", store=wrong).decision.reasons)
    wrong.close(); f.close()
    f=Fixture(directory, "source-coverage")
    earlier=replace(f.item, generated_at_us=utc_microseconds(source_fixture.at(2)), reference_at_us=utc_microseconds(source_fixture.at(2)))
    # Separate inbox identity remains canonical; change event/signal via fixture.
    original=candidate(suffix="before-coverage")
    earlier=replace(original, source_binding=f.source.binding, generated_at_us=earlier.generated_at_us, reference_at_us=earlier.reference_at_us)
    root=f.repo.receive_candidate(earlier, fence=f.repo.write_fence())
    result=f.repo.evaluate_authority_entry(root, "FINAL-A", clock(f.repo), f.source, command_id="before-coverage", fence=f.repo.write_fence())
    check("healthy_cut_does_not_cover_preorigin_signal", "ORIGINAL_SIGNAL_SOURCE_COVERAGE_UNPROVEN" in result.decision.reasons)
    f.close()


class Interrupted(BaseException):
    pass


class ConnectionCut:
    def __init__(self, connection, sql_prefix, after):
        self.connection, self.prefix, self.after, self.fired = connection, sql_prefix, after, False

    def __getattr__(self, name):
        return getattr(self.connection, name)

    def execute(self, sql, *args):
        hit=not self.fired and sql.startswith(self.prefix)
        if hit:
            self.fired=True
            if not self.after:
                raise Interrupted()
        result=self.connection.execute(sql,*args)
        if hit and self.after:
            raise Interrupted()
        return result


def faults(directory):
    for operation in ("control", "eligibility"):
        cuts=[("INSERT INTO ledger_authority_records",False), ("INSERT INTO ledger_authority_projection",False),
              ("INSERT INTO ledger_commits",False), ("COMMIT",False), ("COMMIT",True)]
        if operation == "eligibility":
            cuts.insert(1,("INSERT INTO ledger_authority_bindings",False))
        for index,(prefix,after) in enumerate(cuts):
            name=f"{operation}-cut-{index}"
            f=Fixture(directory,name)
            before=f.repo.write_fence().revision
            supplied_clock=clock(f.repo)
            original=f.repo._conn
            f.repo._conn=ConnectionCut(original,prefix,after)
            call=(lambda: control(f.repo,"HARD_STOP","fault-command")) if operation=="control" else (lambda: f.evaluate("fault-command", sample=supplied_clock))
            check(name+"_interruption_reported", raises(call, LedgerJournalError))
            f.repo._conn=original
            committed=prefix=="COMMIT" and after
            if committed:
                check(name+"_lost_ack_read_fail_closed", raises(lambda: f.repo.authority_snapshot()))
            f.reopen()
            receipt=f.repo.authority_receipt("fault-command")
            check(name+"_atomic_reconstruction", (receipt is not None)==committed and f.repo.write_fence().revision==before+int(committed))
            if not committed:
                check(name+"_binding_or_stop_not_partially_visible", f.repo.authority_entry_binding(f.root) is None
                    and f.repo.authority_snapshot()["hard_stop_command"] is None)
            retried=call()
            check(name+"_retry_commits_once", f.repo.write_fence().revision==before+1 and retried==f.repo.authority_receipt("fault-command"))
            f.close()
    f=Fixture(directory,"cache-publication")
    class PublicationCut(LedgerRepository):
        def __setattr__(self,name,value):
            if name=="_authority":
                raise Interrupted()
            super().__setattr__(name,value)
    f.repo.__class__=PublicationCut
    check("interrupted_authority_cache_publication_reports_lost_ack", raises(lambda:control(f.repo,"HARD_STOP","cache-stop"),LedgerJournalError))
    f.repo.__class__=LedgerRepository
    check("unpublished_authority_cache_never_gets_trusted_head", raises(lambda:f.repo.authority_snapshot()))
    f.reopen()
    check("committed_stop_reconstructed_after_cache_publication_loss", f.repo.authority_snapshot()["hard_stop_command"]=="cache-stop")
    f.close()
    f=Fixture(directory,"receipt-size-bound")
    import live.ledger_evidence_codec_v0_1 as shared_codec
    command=ControlCommand("bounded",f.domain.economic_domain_id,"HARD_STOP",operator())
    original_limit=shared_codec.MAX_PAYLOAD_BYTES
    before=f.repo.write_fence()
    try:
        # Original command fits, but its complete immutable receipt does not.
        shared_codec.MAX_PAYLOAD_BYTES=len(canonical_json(asdict(command)).encode("utf-8"))+1
        check("oversize_complete_receipt_rejected_before_insert",raises(lambda:f.repo.record_authority_control(command,fence=before),ValueError))
    finally:
        shared_codec.MAX_PAYLOAD_BYTES=original_limit
    check("oversize_receipt_cannot_leave_partial_stop_or_head",f.repo.write_fence()==before
        and f.repo.authority_snapshot()["hard_stop_command"] is None and f.repo.authority_receipt("bounded") is None)
    f.reopen()
    check("size_denial_leaves_reopenable_journal",f.repo.authority_snapshot()["hard_stop_command"] is None)
    f.close()
    f=Fixture(directory,"outside")
    fence=f.repo.write_fence()
    f.evaluate("initial")
    command=ControlCommand("stale",f.domain.economic_domain_id,"DISARM_ENTRY",operator())
    check("common_cas_fences_stale_control",raises(lambda:f.repo.record_authority_control(command,fence=fence)))
    with closing(sqlite3.connect(f.path)) as outside, outside:
        outside.execute("PRAGMA user_version=7")
    check("outside_noop_commit_invalidates_public_read",raises(lambda:f.repo.authority_snapshot()))
    check("outside_noop_commit_invalidates_control_write",raises(lambda:control(f.repo,"HARD_STOP","outside-stop")))
    f.reopen()
    check("explicit_reopen_reverifies_unchanged_projection",f.repo.authority_receipt("initial") is not None)
    stale=f.repo.write_fence(); f.reopen()
    check("reopen_generation_fences_old_caller",raises(lambda:f.repo.record_authority_control(command,fence=stale)))
    check("competing_writer_excluded",raises(lambda:LedgerRepository.reopen(f.path,f.domain)))
    f.close()
    for kind in ("projection","binding","v5"):
        f=Fixture(directory,"tamper-"+kind); f.evaluate("initial"); f.repo.close()
        with closing(sqlite3.connect(f.path)) as conn, conn:
            if kind=="projection":
                conn.execute("UPDATE ledger_authority_projection SET content_digest=?",(digest("wrong"),))
            elif kind=="binding":
                conn.execute("DROP TRIGGER ledger_authority_bindings_no_update")
                conn.execute("UPDATE ledger_authority_bindings SET payload_json='{}'")
                conn.execute(_DDL["ledger_authority_bindings_no_update"])
            else:
                conn.execute("PRAGMA user_version=5")
        check(kind+"_reopen_fails_closed",raises(lambda:LedgerRepository.reopen(f.path,f.domain)))
        f.source.close()


def abrupt_processes(directory):
    for operation in ("control", "eligibility"):
        for cut in ("before", "after"):
            f=Fixture(directory, "process-"+operation+"-"+cut)
            before=f.repo.write_fence().revision
            f.repo.close(); f.source.close()
            result=subprocess.run([sys.executable, "-B", str(Path(__file__).resolve()), "--crash", str(f.path),
                str(directory/(f.name+"-evidence.sqlite3")), f.root, operation, cut], cwd=ROOT,
                capture_output=True, text=True, timeout=30)
            check(operation+cut+"_actual_process_exited_at_commit_cut", result.returncode == (81 if cut=="before" else 82))
            f.repo=LedgerRepository.reopen(f.path, f.domain)
            f.source=SourceEvidenceStore(directory/(f.name+"-evidence.sqlite3"), f.item.source_binding, SourceProfile())
            receipt=f.repo.authority_receipt("process-crash")
            check(operation+cut+"_actual_process_atomic_reopen", (receipt is not None)==(cut=="after")
                and f.repo.write_fence().revision==before+int(cut=="after"))
            if operation=="control":
                retried=control(f.repo,"HARD_STOP","process-crash")
            else:
                retried=f.evaluate("process-crash", sample=clock(f.repo) if receipt is None else receipt.original.clock)
            check(operation+cut+"_actual_process_exact_retry_once", f.repo.write_fence().revision==before+1
                and retried==f.repo.authority_receipt("process-crash"))
            f.close()


def crash_child():
    path, source_path, root, operation, cut=sys.argv[2:]
    repo=LedgerRepository.reopen(Path(path), domain())
    source=SourceEvidenceStore(Path(source_path), repo.candidate(root).source_binding, SourceProfile())
    class ExitConnection:
        def __getattr__(self,name):
            return getattr(connection,name)
        def execute(self,sql,*args):
            if sql=="COMMIT" and cut=="before":
                os._exit(81)
            result=connection.execute(sql,*args)
            if sql=="COMMIT" and cut=="after":
                os._exit(82)
            return result
    connection=repo._conn
    repo._conn=ExitConnection()
    if operation=="control":
        control(repo,"HARD_STOP","process-crash")
    else:
        repo.evaluate_authority_entry(root,"FINAL-A",clock(repo),source,command_id="process-crash",fence=repo.write_fence())
    raise AssertionError("unreached crash cut")


def main():
    with tempfile.TemporaryDirectory(prefix="live-authority-a1-") as temporary:
        directory=Path(temporary)
        try:
            receipt,state=basic(directory)
            controls(directory)
            clocks(directory)
            sources(directory)
            faults(directory)
            abrupt_processes(directory)
        finally:
            for fixture in FIXTURES:
                fixture.close()
    print(json.dumps({"status":"IMPLEMENTED_PENDING_PROJECT_REVIEW","checks":len(CHECKS),"all_checks":all(CHECKS.values()),
        "check_digest":content_fingerprint(CHECKS),"eligibility_receipt_digest":receipt,"replayed_control_state_digest":state},sort_keys=True))


if __name__ == "__main__":
    crash_child() if len(sys.argv)>1 and sys.argv[1]=="--crash" else main()
