"""A2 actual source/Wallet Evidence and Ledger -> UNADMITTED sizing fixtures.

External Ledger admission/retirement fixtures are explicitly NOT real Authority
acceptance. A3 owns that later grant/reservation composition.
"""
from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import tempfile
from contextlib import closing
from dataclasses import FrozenInstanceError, asdict, replace
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"src"))
from phase5.shadow_domain_v0_1 import content_fingerprint
from phase5.shadow_venue_route_quote_v0_1 import TOKEN_PROGRAM_ID
from live.authority_economics_v0_1 import propose_fixed_entry, fixed_size_reasons
from live.authority_controls_v0_1 import EntrySizeLimits
from live.ledger_domain_v0_1 import LedgerContractError
from live.ledger_repository_v0_1 import LedgerRepository, LedgerJournalError
from live.ledger_actions_v0_1 import PendingAction
from live.ledger_ports_v0_1 import AdmissionInput, NativeReservationVector, RetirementInput
from live.ledger_settlement_v0_1 import WalletSupportInput
from live.wallet_evidence_v0_1 import WalletEvidenceRequest
from live.evidence_store_v0_1 import SourceEvidenceStore
from live.source_health_v0_1 import SourceProfile
import live_authority_controls_selftest_v0_1 as a1
from live_authority_controls_selftest_v0_1 import Fixture, clock, control, arm, operator, digest, raises
from live_ledger_actions_selftest_v0_1 import candidate, preparation
from live_ledger_baseline_selftest_v0_1 import observe, domain
from live_wallet_evidence_selftest_v0_1 import NOW, utc, WALLET, GENESIS, Scenario

CHECKS={}


def check(name,value):
    CHECKS[name]=bool(value)
    if not value:
        raise AssertionError(name)


def propose(f,name,*,at=NOW+4,sample=None,item=None):
    return propose_fixed_entry(f.repo,item or f.item,sample or clock(f.repo,at),f.source,
        request_id=name,fence=f.repo.write_fence())


def change_policy(f,name,*,amount=None,minimum=None,ceiling=None,trade=None,track=None,at=NOW):
    changes={key:value for key,value in (("fixed_quote_lamports",amount),("minimum_quote_lamports",minimum),
        ("ceiling_quote_lamports",ceiling),("max_trade_notional_lamports",trade)) if value is not None}
    f.policy=replace(f.policy,policy_id=name,approval=operator(at),size=replace(f.policy.size,**changes),
        selected_track=track or f.policy.selected_track)
    control(f.repo,"INSTALL_POLICY","install-"+name,policy_value=f.policy)
    arm(f.repo,f.policy,name="grant-"+name,at=at)


def pending(proposal,*,units=None,deadline_digest=None):
    return PendingAction(proposal.root_id,proposal.candidate_digest,"BUY",proposal.mint,TOKEN_PROGRAM_ID,
        proposal.position_id,proposal.native_quote_cap_lamports if units is None else units,
        "EXTERNAL_UNADMITTED_FIXTURE_TERMS",digest("fixture-terms"),proposal.policy_id,proposal.policy_digest,
        proposal.selected_track,None,1,proposal.original_deadline_us,
        proposal.original_deadline_binding_digest if deadline_digest is None else deadline_digest)


def proposals(directory):
    f=Fixture(directory,"proposal")
    before=f.repo.audit()
    first=propose(f,"first")
    p=first.proposal
    check("actual_Evidence_Ledger_A1_fixed_native_proposal",first.disposition=="UNADMITTED_ECONOMICS_PROPOSAL"
        and p.native_quote_cap_lamports==f.policy.size.fixed_quote_lamports)
    check("no_admission_reservation_action_or_fill_created",f.repo.audit()["action_count"]==0
        and not f.repo.consumer_snapshot()["reservations"] and not f.repo.consumer_snapshot()["positions"]
        and f.repo.inbox_disposition(f.root)=="RECEIVED" and not p.admitted and not p.reserved and not p.grants_message_permission)
    check("full_native_quote_cap_includes_venue_fee_once",p.asset_kind=="NATIVE_SOL_FULL_QUOTE_CAP"
        and p.quote_cap_includes_venue_fees and p.native_quote_cap_lamports==1000000)
    check("current_cut_and_original_identity_bound",p.cut==f.repo.consumer_snapshot()["consumer_cut"]
        and p.root_id==f.root and p.candidate_digest==f.item.content_digest
        and p.original_deadline_us==f.item.generated_at_us+15000000)
    check("original_deadline_binding_digest_exposed",p.original_deadline_binding_digest==content_fingerprint(asdict(f.repo.authority_entry_binding(f.root))))
    check("candidate_reference_provenance_unchanged",f.repo.candidate(f.root)==f.item and f.repo.audit()["candidate_count"]==before["candidate_count"])
    check("proposal_immutable",raises(lambda:setattr(p,"native_quote_cap_lamports",10),FrozenInstanceError))
    for field in ("admitted","reserved","grants_message_permission"):
        check(field+"_cannot_be_relabelled",raises(lambda:replace(p,**{field:True}),ValueError))
    check("result_cannot_be_relabelled_admitted",raises(lambda:replace(first,disposition="ADMITTED")))
    check("result_cannot_be_relabelled_permission",raises(lambda:replace(first,grants_message_permission=True),ValueError))
    for name,value in (("bool",True),("float",1.5),("overflow",1<<64),("negative",-1)):
        check(name+"_proposal_amount_denied",raises(lambda:replace(p,native_quote_cap_lamports=value),ValueError))
    sample=f.repo.authority_receipt(p.eligibility_command_id).original.clock
    same=propose(f,"first",sample=sample)
    check("old_positive_request_is_historical_not_new_proposal",same.disposition=="HISTORICAL_REQUEST_REQUIRES_REEVALUATION"
        and same.proposal is None and same.root_id==first.root_id)
    fresh=propose(f,"fresh",at=NOW+5)
    check("fresh_duplicate_has_same_root_amount_distinct_current_cut",fresh.proposal.root_id==p.root_id
        and fresh.proposal.native_quote_cap_lamports==p.native_quote_cap_lamports and fresh.proposal.cut.revision>p.cut.revision)
    f.reopen()
    change_policy(f,"changed-size",amount=777,at=NOW+5)
    resized=propose(f,"unadmitted-new-policy",at=NOW+6)
    check("unadmitted_policy_change_may_propose_new_size_same_root",resized.proposal.native_quote_cap_lamports==777
        and resized.root_id==first.root_id and not f.repo.consumer_snapshot()["reservations"])
    check("old_returned_proposal_remains_immutable",p.native_quote_cap_lamports==1000000 and p.policy_id=="p1")
    check("same_identity_changed_candidate_content_conflicts",raises(lambda:propose(f,"changed-candidate",at=NOW+7,
        item=replace(f.item,reference_price_text="0.5"))))
    final=resized.proposal.content_digest
    f.close()
    return final


def sizes(directory):
    base=EntrySizeLimits(100,10,200,150,1000,1000,1)
    for name,changes,reason in (("zero",{"fixed_quote_lamports":0},"FIXED_SIZE_ZERO"),
        ("minimum",{"fixed_quote_lamports":9},"FIXED_SIZE_BELOW_MINIMUM"),
        ("ceiling",{"fixed_quote_lamports":201},"FIXED_SIZE_ABOVE_CEILING"),
        ("trade",{"fixed_quote_lamports":151},"FIXED_SIZE_ABOVE_TRADE_CAP")):
        check(name+"_exact_size_denial",reason in fixed_size_reasons(replace(base,**changes)))
    check("inclusive_size_boundaries",not fixed_size_reasons(replace(base,fixed_quote_lamports=10))
        and not fixed_size_reasons(replace(base,fixed_quote_lamports=150)))
    for name,value in (("bool",True),("float",1.0),("overflow",1<<64),("negative",-1)):
        check(name+"_configured_size_denied",raises(lambda:replace(base,fixed_quote_lamports=value),ValueError))
    f=Fixture(directory,"minimum-denial")
    change_policy(f,"below-minimum",amount=5,minimum=10)
    denied=propose(f,"size-denied")
    check("below_minimum_not_silently_raised",denied.proposal is None and denied.disposition=="DENIED_FIXED_SIZE"
        and f.repo.inbox_disposition(f.root)=="DENIED_RETRYABLE")
    with closing(sqlite3.connect(f.path)) as conn:
        payload=json.loads(conn.execute("SELECT payload_json FROM ledger_inbox_dispositions WHERE root_id=?",(f.root,)).fetchone()[0])
    check("durable_denial_reference_matches_returned_original_decision",payload["external_record_digest"]==denied.content_digest)
    f.reopen()
    change_policy(f,"now-valid",amount=10)
    recovered=propose(f,"valid-new-policy",at=NOW+5)
    check("retryable_denial_can_recover_within_original_window",recovered.proposal.native_quote_cap_lamports==10
        and recovered.root_id==denied.root_id and f.repo.inbox_disposition(f.root)=="DENIED_RETRYABLE")
    f.close()
    f=Fixture(directory,"min-vs-cap")
    change_policy(f,"no-rounding",amount=5,minimum=10,trade=8)
    denied=propose(f,"deny")
    check("minimum_cannot_raise_size_over_cap",denied.proposal is None and "FIXED_SIZE_BELOW_MINIMUM" in denied.reasons)
    f.close()
    f=Fixture(directory,"u64")
    change_policy(f,"u64",amount=(1<<64)-1,minimum=1,ceiling=(1<<64)-1,trade=(1<<64)-1)
    wide=propose(f,"wide")
    check("exact_u64_unadmitted_amount_has_no_SQL_or_float_loss",wide.proposal.native_quote_cap_lamports==(1<<64)-1)
    check("u64_proposal_does_not_claim_actual_wallet_capacity",not wide.proposal.reserved
        and wide.proposal.requires_current_A3_revalidation and not f.repo.consumer_snapshot()["reservations"])
    f.close()


def expiry(directory):
    f=Fixture(directory,"expired")
    expired=propose(f,"expired",at=NOW+15)
    check("qualified_lower_bound_creates_real_inbox_expiry",expired.disposition=="EXPIRED"
        and f.repo.inbox_disposition(f.root)=="EXPIRED" and f.repo.authority_entry_binding(f.root).deadline_us==f.item.generated_at_us+15000000)
    f.reopen()
    change_policy(f,"fresh-policy",amount=10,at=NOW+15)
    still=propose(f,"later-policy",at=NOW+16)
    check("restart_fresh_policy_cannot_renew_expired_inbox",still.disposition=="TERMINAL_INBOX" and still.proposal is None
        and still.inbox_disposition=="EXPIRED")
    f.close()
    for name in ("straddle","unknown","backward"):
        f=Fixture(directory,"expiry-"+name)
        if name=="backward":
            propose(f,"known",at=NOW+4)
            sample=clock(f.repo,NOW+3)
        elif name=="unknown":
            sample=clock(f.repo,NOW+1000,status="UNKNOWN")
        else:
            sample=clock(f.repo,NOW+14,upper=NOW+15)
        result=propose(f,name,sample=sample)
        check(name+"_cannot_manufacture_terminal_expiry",result.disposition=="DENIED_UNPROVEN"
            and result.proposal is None and f.repo.inbox_disposition(f.root)=="RECEIVED")
        check(name+"_original_A1_denial_is_durable",result.eligibility_receipt_digest is not None and f.repo.audit()["authority_receipt_count"]>=3)
        f.reopen()
        check(name+"_restart_preserves_unresolved_inbox",f.repo.inbox_disposition(f.root)=="RECEIVED")
        f.close()
    f=Fixture(directory,"track")
    propose(f,"first")
    change_policy(f,"other-track",track="FINAL-B")
    result=propose(f,"other-track",at=NOW+5)
    check("new_policy_track_cannot_replace_original_binding",result.proposal is None
        and "POLICY_ORIGINAL_CANDIDATE_PROFILE_CONFLICT" in result.reasons and f.repo.authority_entry_binding(f.root).track=="FINAL-A")
    f.close()


def stages_and_accepted(directory):
    for name,kind in (("exact","exact"),("amount","amount"),("deadline-binding","deadline")):
        f=Fixture(directory,"staged-"+name)
        initial=propose(f,"initial").proposal
        action=pending(initial,units=initial.native_quote_cap_lamports+1 if kind=="amount" else None,
            deadline_digest=digest("unrelated-deadline") if kind=="deadline" else None)
        f.repo.stage_action(action,fence=f.repo.write_fence())
        result=propose(f,"with-staged",at=NOW+5)
        check(name+"_staged_terms_profile",result.disposition==("UNADMITTED_ECONOMICS_PROPOSAL" if kind=="exact" else "STAGED_TERMS_CONFLICT"))
        check(name+"_staging_is_not_admission",result.original_admitted_action is None and not f.repo.consumer_snapshot()["reservations"]
            and f.repo.action(action.action_id)==action)
        f.close()
    f=Fixture(directory,"external-admitted")
    initial=propose(f,"proposal").proposal
    action=pending(initial)
    # Explicit legacy Ledger external port fixture, NOT Authority A3 acceptance.
    supplied=AdmissionInput(f.repo.consumer_snapshot()["consumer_cut"],action,
        NativeReservationVector(action.input_units,5000,0,0,5000,0),"EXTERNAL_FIXTURE_NOT_A3",digest("external-admission"),utc(NOW+4))
    admission=f.repo.admit(supplied,ingestion_key="external-admit",fence=f.repo.write_fence())
    change_policy(f,"different-accepted-policy",amount=7,at=NOW+4)
    result=propose(f,"duplicate",at=NOW+5)
    check("external_admitted_fixture_returns_original_terms_not_new_amount",result.disposition=="ORIGINAL_LEDGER_ADMITTED_TERMS"
        and result.original_admitted_action==action and result.proposal is None and not result.grants_message_permission)
    check("external_storage_acceptance_does_not_claim_A3_authority",result.admission_receipt_digest==admission.content_digest
        and not admission.has_real_authority_grant)
    f.reopen()
    check("accepted_original_terms_survive_restart_policy_change",propose(f,"reopened",at=NOW+6).original_admitted_action==action)
    # Actual intact-generation unsigned cancellation plus original Wallet Evidence
    # supports no-acquisition retirement through the accepted external Ledger port.
    prep=preparation(action,at=NOW+6)
    f.repo.prepare_attempt(prep,fence=f.repo.write_fence())
    f.repo.cancel_attempt_locally(prep.attempt_id,recorded_at_utc=utc(NOW+7),idempotency_key="cancel",
        expected_attempt_revision=f.repo.attempt(prep.attempt_id).revision,fence=f.repo.write_fence())
    wallet=Scenario(); wallet.initial_slot=101
    observation=observe(wallet,request=WalletEvidenceRequest(WALLET,GENESIS,101),at=NOW+8)
    support=WalletSupportInput(observation,utc(NOW+8),101)
    intent=RetirementInput(f.repo.consumer_snapshot()["consumer_cut"],f.root,f.repo.reservation(f.root).reservation_id,
        action.position_id,prep.attempt_id,"POSITIVE_UNSIGNED_CANCEL",support.digest,"EXTERNAL_FIXTURE_RETIREMENT",digest("retirement"),utc(NOW+8))
    retired=f.repo.retire(intent,support,ingestion_key="retire",fence=f.repo.write_fence())
    check("actual_no_acquisition_retirement_fixture",retired.retirement_disposition=="RETIRED"
        and f.repo.inbox_disposition(f.root)=="CLOSED_NO_ACQUISITION")
    f.reopen()
    historical=propose(f,"after-retirement",at=NOW+1000)
    check("retired_root_never_reacquires_or_resizes",historical.original_admitted_action==action and historical.proposal is None
        and historical.inbox_disposition=="CLOSED_NO_ACQUISITION" and not f.repo.consumer_snapshot()["reservations"])
    f.close()


def guarded_cuts(directory):
    f=Fixture(directory,"control-race")
    original_snapshot=f.repo.authority_candidate_economics
    reads=0
    def changed_snapshot(root):
        nonlocal reads
        reads+=1
        if reads==2:
            control(f.repo,"STOP_ENTRY","intervening-stop",at=NOW+4)
        return original_snapshot(root)
    f.repo.authority_candidate_economics=changed_snapshot
    denied=propose(f,"race")
    check("changed_common_control_cut_cannot_publish_positive_proposal",denied.proposal is None
        and "COMMON_ELIGIBILITY_CUT_CHANGED" in denied.reasons)
    f.repo.authority_candidate_economics=original_snapshot
    f.close()
    f=Fixture(directory,"source-selection-race")
    original_latest=f.source.latest_record
    reads=0
    def changed_latest():
        nonlocal reads
        reads+=1
        if reads==2:
            f.source.latest_record=original_latest
            a1.source_fixture.add_gap(f.raw,"PENDING")
            gap=a1.source_fixture.observe(f.raw,f.source.binding,previous=f.initial,now=13)
            f.source.append(gap,expected_previous_digest=f.initial.content_digest)
        return original_latest()
    f.source.latest_record=changed_latest
    denied=propose(f,"source-race")
    check("changed_source_selection_cannot_publish_old_healthy_proposal",denied.proposal is None
        and "CURRENT_SOURCE_SELECTION_CHANGED" in denied.reasons)
    f.source.latest_record=original_latest
    f.close()
    f=Fixture(directory,"stale-old-request")
    result=propose(f,"old")
    sample=f.repo.authority_receipt(result.proposal.eligibility_command_id).original.clock
    a1.source_fixture.add_gap(f.raw,"PENDING")
    gap=a1.source_fixture.observe(f.raw,f.source.binding,previous=f.initial,now=13)
    f.source.append(gap,expected_previous_digest=f.initial.content_digest)
    check("old_healthy_request_cannot_supply_current_proposal_after_gap",propose(f,"old",sample=sample).proposal is None)
    gap_result=propose(f,"current-gap",at=NOW+5)
    check("new_request_selects_latest_actual_gap",gap_result.proposal is None and "CURRENT_SOURCE_NOT_USABLE" in gap_result.reasons)
    with closing(sqlite3.connect(f.path)) as conn,conn:
        conn.execute("PRAGMA user_version=8")
    check("outside_commit_blocks_guarded_economics_snapshot",raises(lambda:f.repo.authority_candidate_economics(f.root)))
    check("outside_commit_blocks_public_proposal_path",raises(lambda:propose(f,"outside",at=NOW+6)))
    f.reopen()
    check("reopen_preserves_original_root_and_gap_denial",f.repo.authority_candidate_economics(f.root)["candidate"]==f.item)
    f.close()


def crashes(directory):
    for cut in ("after-eligibility","before-expiry","after-expiry"):
        f=Fixture(directory,"crash-"+cut)
        original=f.repo._conn
        class CutConnection:
            commits=0
            def __getattr__(self,name):
                return getattr(original,name)
            def execute(self,sql,*args):
                if sql=="COMMIT":
                    self.commits+=1  # candidate duplicate, eligibility, expiry
                    if cut=="before-expiry" and self.commits==3:
                        raise KeyboardInterrupt()
                result=original.execute(sql,*args)
                if sql=="COMMIT" and ((cut=="after-eligibility" and self.commits==2) or (cut=="after-expiry" and self.commits==3)):
                    raise KeyboardInterrupt()
                return result
        sample=clock(f.repo,NOW+15)
        f.repo._conn=CutConnection()
        check(cut+"_interrupted_at_original_cut",raises(lambda:propose(f,"expiry-crash",sample=sample),KeyboardInterrupt))
        f.repo._conn=original
        f.reopen()
        before=f.repo.inbox_disposition(f.root)
        check(cut+"_expiry_reconstruction",before==("EXPIRED" if cut=="after-expiry" else "RECEIVED"))
        result=propose(f,"expiry-crash",sample=sample)
        check(cut+"_retry_finishes_qualified_expiry_once",result.proposal is None and f.repo.inbox_disposition(f.root)=="EXPIRED")
        with closing(sqlite3.connect(f.path)) as conn:
            check(cut+"_one_terminal_record",conn.execute("SELECT COUNT(*) FROM ledger_inbox_dispositions").fetchone()[0]==1)
        f.close()


def abrupt_expiry(directory):
    for cut in ("after-eligibility","after-expiry"):
        f=Fixture(directory,"process-"+cut)
        before=f.repo.write_fence().revision
        f.repo.close(); f.source.close()
        child=subprocess.run([sys.executable,"-B",str(Path(__file__).resolve()),"--crash",str(f.path),
            str(directory/(f.name+"-evidence.sqlite3")),f.root,cut],cwd=ROOT,capture_output=True,text=True,timeout=30)
        check(cut+"_abrupt_child_reached_exact_cut",child.returncode==(91 if cut=="after-eligibility" else 92))
        f.repo=LedgerRepository.reopen(f.path,f.domain)
        f.source=SourceEvidenceStore(directory/(f.name+"-evidence.sqlite3"),f.item.source_binding,SourceProfile())
        check(cut+"_abrupt_process_atomic_expiry_state",f.repo.inbox_disposition(f.root)==("EXPIRED" if cut=="after-expiry" else "RECEIVED"))
        key="a2-eligibility:"+content_fingerprint({"request_id":"process-expiry"})
        original=f.repo.authority_receipt(key)
        result=propose(f,"process-expiry",sample=original.original.clock)
        check(cut+"_abrupt_process_retry_finishes_expiry_once",result.proposal is None
            and f.repo.inbox_disposition(f.root)=="EXPIRED" and f.repo.write_fence().revision==before+2)
        f.close()


def crash_child():
    path,source_path,root,cut=sys.argv[2:]
    repo=LedgerRepository.reopen(Path(path),domain())
    item=repo.candidate(root)
    source=SourceEvidenceStore(Path(source_path),item.source_binding,SourceProfile())
    original=repo._conn
    class ExitConnection:
        commits=0
        def __getattr__(self,name):
            return getattr(original,name)
        def execute(self,sql,*args):
            result=original.execute(sql,*args)
            if sql=="COMMIT":
                self.commits+=1
                if cut=="after-eligibility" and self.commits==2:
                    os._exit(91)
                if cut=="after-expiry" and self.commits==3:
                    os._exit(92)
            return result
    repo._conn=ExitConnection()
    propose_fixed_entry(repo,item,clock(repo,NOW+15),source,request_id="process-expiry",fence=repo.write_fence())
    raise AssertionError("unreached crash cut")


def main():
    with tempfile.TemporaryDirectory(prefix="live-authority-a2-") as temporary:
        directory=Path(temporary)
        try:
            proposal_digest=proposals(directory)
            sizes(directory)
            expiry(directory)
            stages_and_accepted(directory)
            guarded_cuts(directory)
            crashes(directory)
            abrupt_expiry(directory)
        finally:
            for f in a1.FIXTURES:
                f.close()
    print(json.dumps({"status":"IMPLEMENTED_PENDING_PROJECT_REVIEW","checks":len(CHECKS),"all_checks":all(CHECKS.values()),
        "check_digest":content_fingerprint(CHECKS),"unadmitted_proposal_digest":proposal_digest},sort_keys=True))


if __name__=="__main__":
    crash_child() if len(sys.argv)>1 and sys.argv[1]=="--crash" else main()
