"""Actual A3 admission -> external public A4 evidence -> current one-use stages.

All operator/clock/Execution inputs are deterministic external fixtures. Public
signature bytes are inert and never cryptographically produced or submitted.
"""
from __future__ import annotations
import base64
import copy
import json
import os
import sqlite3
import struct
import subprocess
import sys
import tempfile
from contextlib import closing
from dataclasses import asdict, replace
from pathlib import Path
from solders.message import Message
from solders.hash import Hash
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/"src"))
from phase5.shadow_domain_v0_1 import canonical_json, content_fingerprint
from live.authority_message_control_v0_1 import MessageProfileCommand, MessageStageRequest, MessageStageReceipt, FreshStageConsumption
from live.authority_message_control_codec_v0_1 import receipt_to_json, receipt_from_json, profile_receipt_to_json, profile_receipt_from_json
from live.ledger_actions_v0_1 import AttemptPreparation, AttemptStageInput
from live.ledger_actions_v0_1 import decode_message
from live.ledger_domain_v0_1 import LedgerContractError
from live.public_rpc_v0_1 import PublicRpcError
from live.ledger_repository_v0_1 import LedgerRepository
import live_authority_message_evidence_selftest_v0_1 as a4
a3, sf, NOW = a4.a3, a4.sf, a4.NOW
CHECKS = {}


def check(name, condition):
    CHECKS[name] = bool(condition)
    if not condition:
        raise AssertionError(name)


def fails(call):
    try:
        call()
    except (LedgerContractError, PublicRpcError):
        return True
    return False


def prepare(f, action, value, plan, *, at=NOW+8, conflict=None, lower=None):
    prep = AttemptPreparation(action.action_id, action.content_digest, 1, value.evidence.simulation.envelopes[-1].message_hex,
        plan.fingerprint, value.profile.content_digest, value.evidence.simulation.leases[-1], lower or a3.observe().anchor,
        a3.utc(at), "EXTERNAL_PUBLIC_PREPARATION", content_fingerprint("external-public-preparation"))
    if conflict=="wrong-plan":prep=replace(prep,plan_digest=content_fingerprint("wrong-plan"))
    elif conflict=="wrong-profile":prep=replace(prep,message_policy_digest=content_fingerprint("wrong-profile"))
    elif conflict=="wrong-message":
        message=decode_message(prep.message_hex);h=message.header
        changed=Message.new_with_compiled_instructions(h.num_required_signatures,h.num_readonly_signed_accounts,
            h.num_readonly_unsigned_accounts,message.account_keys,Hash.from_string(sf.bh(210)),message.instructions)
        prep=replace(prep,message_hex=a4.to_bytes_versioned(changed).hex(),lease=replace(prep.lease,blockhash=sf.bh(210)))
    f.repo.prepare_attempt(prep, fence=f.repo.write_fence())
    return prep


def fixture(directory, name, *, route="pump", program=a3.TOKEN_PROGRAM_ID, preparation_conflict=None, wallet_delta=0, once=False):
    if once:
        f,settlement,scenario=a3.actual_fixture(directory,name,venue=route,program=program)
        a3.install(f,name+"-costs",costs={"setup_outflow_lamports":10000000,"refundable_account_lock_lamports":10000000,
            "protective_setup_lamports":10000000,"protective_refundable_lock_lamports":10000000})
        a3.arm(f.repo,f.policy,name="once-original",scope="ENTRY_ONCE",root=f.root)
        scenario.accounts[sf.MINT]=a3.account(program,sf.plans.fx.mint_account(program).data)
        accepted=a3.admit(f,"once-admission",support=a3.wallet(f,scenario=scenario,program=program),program=program)
        check("real_once_admission_consumes_original_grant",accepted.accepted and accepted.consumed_grant_id=="once-original")
        action=f.repo.reservation(f.root).admission.action
    else:f, settlement, scenario, action = a4.fixture(directory, name, route, program)
    f.wallet_scenario = scenario
    if wallet_delta:
        scenario=copy.deepcopy(scenario);scenario.slot_calls=scenario.genesis_calls=0
        scenario.accounts[f.domain.wallet]["lamports"]+=wallet_delta
    value, plan = a4.evidence(f, action, scenario, route=route)
    profile = f.repo.record_authority_message_profile(MessageProfileCommand("profile", "INSTALL_AND_SELECT", value.profile,
        value.profile.approval), fence=f.repo.write_fence())
    prep = prepare(f, action, value, plan, conflict=preparation_conflict)
    return f, action, value, prep, profile


def request(f, prep, name, stage="SIGN", ordinal=0):
    attempt = f.repo.attempt(prep.attempt_id)
    return MessageStageRequest(name, prep.action_id, prep.attempt_id, attempt.revision, stage, ordinal,
        None if stage == "SIGN" else attempt.primary_signature)


def consume(f, value, req, *, at=NOW+9, sample=None, evidence=None, fence=None):
    action = f.repo.action(req.action_id)
    return f.repo.consume_authority_message_stage(req, sample or a3.clock(f.repo, at),
        f.source if action.side == "BUY" else None, evidence or value.evidence, fence=fence or f.repo.write_fence())


def historical(value):
    return value.receipt if type(value) is FreshStageConsumption else value


def advance(f, prep, target, *, at=NOW+10):
    attempt = f.repo.attempt(prep.attempt_id)
    wire = None
    if target == "SIGNED_DURABLE":
        # Exactly one inert public signature, no signing primitive or key.
        wire = base64.b64encode(b"\x01"+b"\x2a"*64+bytes.fromhex(prep.message_hex)).decode()
    stage = AttemptStageInput(prep.attempt_id, target, prep.action_content_digest, prep.message_sha256,
        prep.lease.fingerprint, prep.message_policy_digest, a3.utc(at), "EXTERNAL_"+target,
        content_fingerprint(target), wire)
    return f.repo.record_external_attempt_stage(stage, idempotency_key="external-"+target+"-"+str(at),
        expected_attempt_revision=attempt.revision, fence=f.repo.write_fence())


def signed(f, prep, *, at=NOW+10):
    for target in ("EXACT_SIMULATED", "AUTHORIZED", "SIGNED_DURABLE"):
        advance(f, prep, target, at=at)


def basic(directory):
    digests=[]
    for route,program in (("pump",a3.TOKEN_PROGRAM_ID),("swap",a3.TOKEN_PROGRAM_ID),("swap",a3.TOKEN_2022_PROGRAM_ID)):
        name=route+("-22" if program==a3.TOKEN_2022_PROGRAM_ID else "-legacy")
        f,action,value,prep,profile=fixture(directory,name,route=route,program=program)
        check(name+"_real_A3_acceptance",f.repo.authority_acceptance(action.root_id).accepted)
        before=f.repo.write_fence();sample=a3.clock(f.repo,NOW+9);req=request(f,prep,"sign")
        delivered=consume(f,value,req,sample=sample)
        if type(delivered) is not FreshStageConsumption:
            print("INITIAL_DENIAL", delivered.decision.reasons)
        check(name+"_fresh_current_SIGN_consumed",type(delivered) is FreshStageConsumption)
        receipt=historical(delivered)
        check(name+"_single_common_commit",receipt.sequence==before.revision+1==f.repo.write_fence().revision
            and f.repo.comparison_receipt("sign").sequence==receipt.sequence)
        check(name+"_own_PREPARED_lane_not_false_denial",f.repo.comparison_receipt("sign").comparison.disposition=="PENDING_EFFECTS_UNKNOWN"
            and not f.repo.comparison_receipt("sign").comparison.differences)
        check(name+"_original_current_cut_captured_after_prepare",receipt.original.validation.context.cut.revision==before.revision)
        check(name+"_historical_never_permission",not receipt.may_sign and not receipt.may_send and not receipt.grants_message_permission)
        replay=consume(f,value,req,sample=sample)
        check(name+"_exact_retry_only_historical",type(replay) is MessageStageReceipt and replay==receipt and f.repo.write_fence().revision==receipt.sequence)
        check(name+"_original_codec",receipt_from_json(receipt_to_json(receipt))==receipt
            and profile_receipt_from_json(profile_receipt_to_json(profile))==profile)
        check(name+"_no_actual_economics",not f.repo.consumer_snapshot()["positions"] and f.repo.consumer_snapshot()["funding"].native_lamports==f.domain.known_native_wallet_lamports)
        signed(f,prep)
        sent=consume(f,value,request(f,prep,"send","SEND"),at=NOW+11)
        check(name+"_real_prior_SIGN_then_SEND",type(sent) is FreshStageConsumption and sent.receipt.decision.prior_sign_digest==receipt.content_digest)
        advance(f,prep,"SEND_CLAIMED",at=NOW+12)
        rebroadcast=consume(f,value,request(f,prep,"rebroadcast","REBROADCAST",1),at=NOW+13)
        check(name+"_same_signature_rebroadcast",type(rebroadcast) is FreshStageConsumption
            and rebroadcast.receipt.decision.prior_send_digest==sent.receipt.content_digest)
        digests.append(receipt.content_digest)
        f.reopen()
        check(name+"_all_original_receipts_reopen",f.repo.authority_message_receipt("sign")==receipt
            and f.repo.authority_message_receipt("send")==sent.receipt and f.repo.authority_message_receipt("rebroadcast")==rebroadcast.receipt)
        check(name+"_reopen_retains_lane_and_reservation",f.repo.attempt(prep.attempt_id).lane_held and f.repo.reservation(action.root_id).status!="RETIRED")
        f.repo.audit();f.close()
    return digests


def denials(directory):
    cases=("hard-stop","entry-stop","disarm","policy-replaced","grant-revoked","clock-unknown","clock-backward",
        "clock-straddle","expired","missing-fee","null-fee","missing-setup","bad-simulation","old-wallet","wallet-difference",
        "wallet-incomplete","source-gap","source-rollback","wrong-attempt-revision","wrong-plan","wrong-profile","wrong-message")
    for label in cases:
        f,action,value,prep,profile=fixture(directory,label,preparation_conflict=label if label in ("wrong-plan","wrong-profile","wrong-message") else None)
        req=request(f,prep,label);sample=a3.clock(f.repo,NOW+9);e=value.evidence
        expected=None
        if label in ("hard-stop","entry-stop","disarm"):
            a3.control(f.repo,{"hard-stop":"HARD_STOP","entry-stop":"STOP_ENTRY","disarm":"DISARM_ENTRY"}[label],label,at=NOW+8)
        elif label=="policy-replaced":
            a3.install(f,"new-entry")
        elif label=="grant-revoked":
            a3.control(f.repo,"REVOKE_GRANT","revoke",target=f.repo.authority_acceptance(f.root).eligibility.decision.grant_id,at=NOW+8)
        elif label=="clock-unknown":sample=replace(sample,status="UNKNOWN")
        elif label=="clock-backward":sample=a3.clock(f.repo,NOW+2)
        elif label=="clock-straddle":sample=a3.clock(f.repo,NOW+14,upper=NOW+15)
        elif label=="expired":sample=a3.clock(f.repo,NOW+15)
        elif label=="missing-fee":e=replace(e,fee=None)
        elif label=="null-fee":e=replace(e,fee=replace(e.fee,outcome="NULL",fee_lamports=None))
        elif label=="missing-setup":e=replace(e,setup_accounts=None)
        elif label=="bad-simulation":e=a4.with_result(value,error_json=canonical_json({"InstructionError":[0,"Custom"]})).evidence
        elif label in ("old-wallet","wallet-difference","wallet-incomplete"):
            s=copy.deepcopy(f.wallet_scenario);s.slot_calls=s.genesis_calls=0
            s.accounts[action.mint]=a3.account(action.token_program,sf.plans.fx.mint_account().data)
            if label=="wallet-difference":s.accounts[f.domain.wallet]["lamports"]-=1
            if label=="wallet-incomplete":s.program_failure=a3.TOKEN_PROGRAM_ID
            support=a3.wallet(f,scenario=s,at=NOW-50 if label=="old-wallet" else NOW+5)
            e=replace(e,wallet=support)
        elif label in ("source-gap","source-rollback"):
            prior=f.source.latest_record()[1]
            a3.a1.source_fixture.change(f.raw,"DELETE FROM websocket_observations")
            gap=a3.a1.source_fixture.observe(f.raw,f.source.binding,previous=prior,now=8,cut=5)
            f.source.append(gap,expected_previous_digest=prior.content_digest)
            if label=="source-rollback":
                denied=consume(f,value,replace(req,command_id="observe-gap"),sample=sample)
                check(label+"_new_gap_retained",not historical(denied).consumed)
                # A distinct old copy claims its latest record is healthy. The
                # retained original sequence/digest still detects that rollback.
                old=a3.SourceEvidenceStore(directory/(label+"-old.sqlite3"),f.source.binding,f.source.profile)
                old.append(f.initial,expected_previous_digest="0"*64)
                f.source=old;sample=a3.clock(f.repo,NOW+10)
        elif label=="wrong-attempt-revision":req=replace(req,expected_attempt_revision=7)
        before=f.repo.write_fence()
        result=consume(f,value,req,sample=sample,evidence=e)
        receipt=historical(result)
        check(label+"_durable_current_denial",type(result) is MessageStageReceipt and not receipt.consumed and bool(receipt.decision.reasons))
        check(label+"_comparison_and_denial_atomic",receipt.sequence==before.revision+1 and f.repo.comparison_receipt(label).sequence==receipt.sequence)
        check(label+"_no_consumed_slot",f.repo._conn.execute("SELECT COUNT(*) FROM ledger_authority_message_stages WHERE consumed_key IS NOT NULL").fetchone()[0]==0)
        check(label+"_no_funding_adoption",f.repo.consumer_snapshot()["funding"].native_lamports==f.domain.known_native_wallet_lamports)
        if label in ("wrong-plan","wrong-profile","wrong-message"):
            check(label+"_isolated_from_A4a_and_lane",a4.validate_message_evidence(value).disposition=="SUPPORTED_CONTEXT_ONLY"
                and f.repo.attempt(prep.attempt_id).recorded_stage=="PREPARED" and f.repo.attempt(prep.attempt_id).lane_held
                and "STORED_PREPARATION_EXACT_MESSAGE_PLAN_PROFILE_LEASE_CONFLICT" in receipt.decision.reasons)
        f.reopen();check(label+"_original_denial_replay",f.repo.authority_message_receipt(label)==receipt);f.close()


def stage_lineage(directory):
    for label in ("claim-only","repeat-sign","repeat-send","skip-broadcast","reselect-profile","cancel-after-sign","reopen-unsigned","unknown-rebroadcast"):
        f,action,value,prep,profile=fixture(directory,label)
        if label=="reopen-unsigned":
            f.reopen()
            result=consume(f,value,request(f,prep,"sign"))
            check(label+"_cannot_sign_predecessor_unsigned",not historical(result).consumed)
        else:
            if label!="claim-only":
                first=consume(f,value,request(f,prep,"sign"));check(label+"_first_real_SIGN",type(first) is FreshStageConsumption)
            if label=="cancel-after-sign":
                result=f.repo.cancel_attempt_locally(prep.attempt_id,recorded_at_utc=a3.utc(NOW+10),idempotency_key="cancel",expected_attempt_revision=0,fence=f.repo.write_fence())
                check(label+"_possible_sign_holds_UNKNOWN",result.recorded_stage=="UNKNOWN" and result.lane_held and result.primary_signature is None)
            elif label=="repeat-sign":
                result=consume(f,value,request(f,prep,"sign-again"),at=NOW+10)
                check(label+"_one_successful_SIGN_per_attempt",not historical(result).consumed and "AUTHORITY_STAGE_ALREADY_CONSUMED" in result.decision.reasons)
            else:
                signed(f,prep)
                if label=="reselect-profile":
                    newer=replace(value.profile,profile_id="relaxed",maximum_compute_units=300000)
                    f.repo.record_authority_message_profile(MessageProfileCommand("newer","INSTALL_AND_SELECT",newer,newer.approval),fence=f.repo.write_fence())
                    selected=f.repo.record_authority_message_profile(MessageProfileCommand("select-old","SELECT_EXISTING",value.profile,a3.operator(NOW+10)),fence=f.repo.write_fence())
                    check(label+"_selection_epoch_changed",selected.content_digest!=profile.content_digest and selected.command.profile==profile.command.profile)
                result=consume(f,value,request(f,prep,"send","SEND"),at=NOW+11)
                if label in ("claim-only","reselect-profile"):
                    check(label+"_SEND_denied",type(result) is MessageStageReceipt and not result.consumed)
                else:
                    check(label+"_first_SEND",type(result) is FreshStageConsumption)
                    if label=="repeat-send":
                        again=consume(f,value,request(f,prep,"send-again","SEND"),at=NOW+12)
                        check(label+"_one_successful_SEND",not historical(again).consumed)
                    else:
                        advance(f,prep,"SEND_CLAIMED",at=NOW+12)
                        if label=="unknown-rebroadcast":
                            advance(f,prep,"UNKNOWN",at=NOW+12)
                            repeated=consume(f,value,request(f,prep,"same-bytes","REBROADCAST",1),at=NOW+13)
                            check(label+"_same_signed_bytes_remain_same_attempt",type(repeated) is FreshStageConsumption
                                and repeated.receipt.original.request.primary_signature==f.repo.attempt(prep.attempt_id).primary_signature
                                and f.repo.attempt(prep.attempt_id).recorded_stage=="UNKNOWN")
                        else:
                            skipped=consume(f,value,request(f,prep,"skipped","REBROADCAST",2),at=NOW+13)
                            check(label+"_ordinal_must_be_next",not historical(skipped).consumed)
        f.reopen();check(label+"_lane_and_history_survive",f.repo.attempt(prep.attempt_id).lane_held);f.repo.audit();f.close()


class ConnectionCut:
    def __init__(self, connection, callback, when):
        self.connection,self.callback,self.when=connection,callback,when
    def __getattr__(self,name):
        return getattr(self.connection,name)
    def execute(self,sql,*args):
        if sql=="COMMIT" and self.when=="before":self.callback()
        value=self.connection.execute(sql,*args)
        if sql=="COMMIT" and self.when=="after":self.callback()
        return value


def storage_cases(directory):
    for when in ("before","after"):
        f,action,value,prep,profile=fixture(directory,"exception-"+when)
        req=request(f,prep,"consume");sample=a3.clock(f.repo,NOW+9);old=f.repo.write_fence()
        actual=f.repo._conn
        def interrupt():raise RuntimeError("isolated transaction fault")
        f.repo._conn=ConnectionCut(actual,interrupt,when)
        check(when+"_commit_exception_no_delivery",fails(lambda:consume(f,value,req,sample=sample)))
        f.repo._conn=actual
        if when=="before":
            check(when+"_group_rolled_back",f.repo.write_fence()==old and f.repo.authority_message_receipt("consume") is None)
            result=consume(f,value,req,sample=sample)
            check(when+"_intact_retry_one_fresh_delivery",type(result) is FreshStageConsumption)
        else:
            check(when+"_cache_not_blessed",fails(lambda:f.repo.consumer_snapshot()))
            f.reopen()
            receipt=f.repo.authority_message_receipt("consume")
            check(when+"_whole_group_survived",receipt.consumed and f.repo.comparison_receipt("consume").sequence==receipt.sequence)
            retry=consume(f,value,req,sample=sample)
            check(when+"_lost_ack_retry_historical_only",type(retry) is MessageStageReceipt and retry==receipt)
        f.reopen();check(when+"_reopen_retains_signature_uncertainty",f.repo.attempt(prep.attempt_id).lane_held);f.close()
    f,action,value,prep,profile=fixture(directory,"post-commit-control")
    actual=f.repo._commit;called=False
    def control_after_commit():
        nonlocal called
        actual()
        if not called:
            called=True
            a3.control(f.repo,"HARD_STOP","between-commit-and-delivery",at=NOW+9)
    f.repo._commit=control_after_commit
    check("same_writer_changed_head_withholds_delivery",fails(lambda:consume(f,value,request(f,prep,"consume"))))
    f.repo._commit=actual
    check("same_writer_cut_still_spent",f.repo.authority_message_receipt("consume").consumed
        and f.repo.authority_snapshot()["hard_stop_command"]=="between-commit-and-delivery")
    f.reopen();check("same_writer_changed_head_replays",f.repo.authority_message_receipt("consume").consumed);f.close()
    for label in ("outside-noop","stale-cas","competing-writer","tamper","missing-child","extra-child","profile-row-tamper"):
        f,action,value,prep,profile=fixture(directory,label)
        if label=="outside-noop":
            with closing(sqlite3.connect(f.path)) as connection:
                version=connection.execute("PRAGMA user_version").fetchone()[0]
                connection.execute(f"PRAGMA user_version={version}")
                connection.commit()
            check(label+"_trusted_profile_read_denies",fails(lambda:f.repo.authority_message_profile(value.profile.policy_digest)))
            check(label+"_write_denies",fails(lambda:consume(f,value,request(f,prep,"consume"))))
            f.reopen();check(label+"_explicit_reopen_reverifies",f.repo.authority_message_profile(value.profile.policy_digest)==profile)
        elif label=="stale-cas":
            fence=f.repo.write_fence()
            f.repo.record_authority_message_profile(MessageProfileCommand("reselect","SELECT_EXISTING",value.profile,a3.operator()),fence=fence)
            check(label+"_cannot_append",fails(lambda:consume(f,value,request(f,prep,"consume"),fence=fence)))
        elif label=="competing-writer":
            check(label+"_OS_guard",fails(lambda:LedgerRepository.reopen(f.path,f.domain)))
        else:
            consume(f,value,request(f,prep,"consume"));f.repo.close()
            with closing(sqlite3.connect(f.path)) as connection:
                if label=="extra-child":
                    row=connection.execute("SELECT * FROM ledger_wallet_comparisons WHERE ingestion_key='consume'").fetchone()
                    payload=json.loads(row[-1]);payload["sequence"]=profile.sequence;payload["ingestion_key"]="extra"
                    connection.execute("INSERT INTO ledger_wallet_comparisons VALUES(?,?,?,?,?)",
                        (profile.sequence,"extra",row[2],content_fingerprint(payload),canonical_json(payload)))
                else:
                    trigger={"tamper":"ledger_authority_message_stages_no_update","missing-child":"ledger_wallet_comparisons_no_delete",
                        "profile-row-tamper":"ledger_authority_message_profiles_no_update"}[label]
                    ddl=connection.execute("SELECT sql FROM sqlite_master WHERE name=?",(trigger,)).fetchone()[0]
                    connection.execute("DROP TRIGGER "+trigger)
                    if label=="tamper":connection.execute("UPDATE ledger_authority_message_stages SET consumed_key=NULL")
                    elif label=="missing-child":connection.execute("DELETE FROM ledger_wallet_comparisons WHERE ingestion_key='consume'")
                    else:connection.execute("UPDATE ledger_authority_message_profiles SET profile_id='different-profile'")
                    connection.execute(ddl)
                connection.commit()
            check(label+"_semantic_history_rejection_with_intact_schema",fails(lambda:LedgerRepository.reopen(f.path,f.domain)))
        f.close()


def codec_and_scope(directory):
    f,action,value,prep,profile=fixture(directory,"codec")
    result=consume(f,value,request(f,prep,"sign"));receipt=result.receipt
    for name,mutate in (("permission",lambda r:r.update(may_sign=True)),("historical",lambda r:r.update(historical_only=False)),
        ("version",lambda r:r.update(codec_version="unknown")),("float",lambda r:r["decision"]["risk"].update(native_lamports=5.0)),
        ("bool",lambda r:r["decision"]["risk"].update(native_lamports=True)),("extra",lambda r:r.update(execute=True))):
        row=json.loads(receipt_to_json(receipt));mutate(row)
        check("codec_"+name+"_cannot_relabel_receipt",fails(lambda:receipt_from_json(canonical_json(row))))
    try:result.stage="SEND"
    except AttributeError:immutable=True
    else:immutable=False
    check("fresh_delivery_immutable_no_codec",immutable and fails(lambda:receipt_to_json(result)))
    check("profile_same_id_different_content_conflicts",fails(lambda:f.repo.record_authority_message_profile(
        MessageProfileCommand("conflict","INSTALL_AND_SELECT",replace(value.profile,maximum_compute_units=1),value.profile.approval),fence=f.repo.write_fence())))
    check("profile_selection_operator_order",fails(lambda:f.repo.record_authority_message_profile(
        MessageProfileCommand("old","SELECT_EXISTING",value.profile,a3.operator(NOW-1)),fence=f.repo.write_fence())))
    f.close()
    f,action,value,prep,profile=fixture(directory,"enclosing-bound")
    large=a4.with_result(value,logs=tuple('"'*16000 for _ in range(500)))
    encoded=a4.encode_validation_input(large)
    check("valid_A4a_input_within_its_bound",len(encoded.encode())<16*1024*1024)
    cut=f.repo.write_fence()
    check("enclosing_record_overhead_rejected_before_children",fails(lambda:consume(f,large,request(f,prep,"oversize")))
        and f.repo.write_fence()==cut and f.repo.authority_message_receipt("oversize") is None and f.repo.comparison_receipt("oversize") is None)
    f.close()
    for label in ("disarm-new-grant","stop-release-new-grant"):
        f,action,value,prep,profile=fixture(directory,label)
        original=f.repo.authority_acceptance(f.root)
        if label.startswith("stop"):
            a3.control(f.repo,"STOP_ENTRY","stop",at=NOW+8)
            a3.control(f.repo,"RELEASE_STOP","release",at=NOW+8,target="stop",barrier=content_fingerprint("explicit-release-proof"))
        else:a3.control(f.repo,"DISARM_ENTRY","disarm",at=NOW+8)
        a3.arm(f.repo,f.policy,name="new-recurring",at=NOW+8)
        denied=consume(f,value,request(f,prep,"cannot-transfer"))
        check(label+"_no_implicit_original_grant_transfer",not denied.consumed and "ORIGINAL_ENTRY_GRANT_NOT_CURRENTLY_SELECTED" in denied.decision.reasons)
        check(label+"_original_terms_unchanged",f.repo.authority_acceptance(f.root)==original and f.repo.action(action.action_id)==action)
        cancelled=f.repo.cancel_attempt_locally(prep.attempt_id,recorded_at_utc=a3.utc(NOW+10),idempotency_key="positive-cancel",expected_attempt_revision=0,fence=f.repo.write_fence())
        check(label+"_positive_unsigned_cancel_no_reservation_release",cancelled.recorded_stage=="CANCELLED_UNSIGNED" and f.repo.reservation(f.root).status!="RETIRED")
        f.reopen();f.close()


def crash_child(directory, stage, when):
    f,action,value,prep,profile=fixture(directory,"crash")
    if stage=="SEND":
        consume(f,value,request(f,prep,"sign"));signed(f,prep)
    req=request(f,prep,"crash-consume",stage)
    actual=f.repo._conn
    f.repo._conn=ConnectionCut(actual,lambda:os._exit(71 if when=="before" else 72),when)
    consume(f,value,req,at=NOW+9 if stage=="SIGN" else NOW+11)
    raise AssertionError("hard process cut not reached")


def process_crashes(directory):
    for stage in ("SIGN","SEND"):
        for when in ("before","after"):
            target=directory/("process-"+stage+"-"+when);target.mkdir()
            run=subprocess.run([sys.executable,"-B",str(Path(__file__).resolve()),"--crash",str(target),stage,when],cwd=ROOT,
                capture_output=True,text=True,timeout=90)
            check(stage+when+"_actual_process_exit",run.returncode==(71 if when=="before" else 72))
            with LedgerRepository.reopen(target/"crash-actual.sqlite3",a3.domain(wallet=sf.WALLET)) as repo:
                receipt=repo.authority_message_receipt("crash-consume")
                comparison=repo.comparison_receipt("crash-consume")
                check(stage+when+"_zero_or_whole_common_group",(receipt is None and comparison is None) if when=="before" else
                    receipt.consumed and receipt.sequence==comparison.sequence and type(receipt) is MessageStageReceipt)
                row=repo._conn.execute("SELECT attempt_id FROM ledger_attempts").fetchone()
                attempt=repo.attempt(row[0])
                check(stage+when+"_restart_holds_exact_lane",attempt.lane_held and attempt.prepared_generation<repo.write_fence().generation)
                if stage=="SIGN":
                    cancelled=repo.cancel_attempt_locally(attempt.preparation.attempt_id,recorded_at_utc=a3.utc(NOW+12),
                        idempotency_key="after-process-cancel",expected_attempt_revision=attempt.revision,fence=repo.write_fence())
                    check(stage+when+"_predecessor_no_signature_stays_UNKNOWN",cancelled.recorded_stage=="UNKNOWN" and cancelled.lane_held)
                repo.audit()


def current_truth_and_source(directory):
    for delta in (-1,1):
        name="coherent-wallet-"+str(delta)
        f,action,value,prep,profile=fixture(directory,name,wallet_delta=delta)
        check(name+"_A4a_is_independently_supported",a4.validate_message_evidence(value).disposition=="SUPPORTED_CONTEXT_ONLY")
        result=consume(f,value,request(f,prep,"current"))
        comparison=f.repo.comparison_receipt("current").comparison
        check(name+"_actual_Ledger_comparison_denies",not result.consumed and comparison.differences
            and "CURRENT_COMPLETE_UNCHANGED_CUSTODY_COMPARISON_REQUIRED" in result.decision.reasons)
        check(name+"_unknown_effect_not_external_transfer",comparison.disposition=="PENDING_EFFECTS_UNKNOWN"
            and not comparison.proves_external_transfer and f.repo.consumer_snapshot()["funding"].native_lamports==f.domain.known_native_wallet_lamports)
        f.reopen();check(name+"_original_unchanged_custody_replay",f.repo.authority_message_receipt("current")==result);f.close()
    import live.authority_message_control_v0_1 as module
    f,action,value,prep,profile=fixture(directory,"source-race")
    original_validate=module.validate_message_evidence
    def changed_source(original):
        result=original_validate(original)
        prior=f.source.latest_record()[1]
        a3.a1.source_fixture.change(f.raw,"DELETE FROM websocket_observations")
        gap=a3.a1.source_fixture.observe(f.raw,f.source.binding,previous=prior,now=8,cut=5)
        f.source.append(gap,expected_previous_digest=prior.content_digest)
        return result
    old=f.repo.write_fence()
    module.validate_message_evidence=changed_source
    try:check("latest_source_rechecked_before_positive_commit",fails(lambda:consume(f,value,request(f,prep,"race"))))
    finally:module.validate_message_evidence=original_validate
    check("source_race_rolls_back_entire_group",f.repo.write_fence()==old and f.repo.authority_message_receipt("race") is None
        and f.repo.comparison_receipt("race") is None)
    result=consume(f,value,request(f,prep,"observed-gap"))
    check("new_current_gap_denies_and_is_retained",not result.consumed and "CURRENT_SOURCE_NOT_USABLE" in result.decision.reasons)
    f.reopen();f.close()
    f,action,value,prep,profile=fixture(directory,"source-postcommit-race")
    original_commit=f.repo._commit;changed=False
    def gap_after_commit():
        nonlocal changed
        original_commit()
        if not changed:
            changed=True
            prior=f.source.latest_record()[1]
            a3.a1.source_fixture.change(f.raw,"DELETE FROM websocket_observations")
            gap=a3.a1.source_fixture.observe(f.raw,f.source.binding,previous=prior,now=8,cut=5)
            f.source.append(gap,expected_previous_digest=prior.content_digest)
    f.repo._commit=gap_after_commit
    check("postcommit_source_change_withholds_fresh_delivery",fails(lambda:consume(f,value,request(f,prep,"consumed"))))
    f.repo._commit=original_commit
    receipt=f.repo.authority_message_receipt("consumed")
    check("postcommit_source_change_leaves_slot_spent",receipt.consumed and type(receipt) is MessageStageReceipt)
    f.reopen();check("postcommit_source_change_original_receipt_replay",f.repo.authority_message_receipt("consumed")==receipt);f.close()
    f,action,value,prep,profile=fixture(directory,"source-postcommit-read-error")
    original_commit,original_latest=f.repo._commit,f.source.latest_record
    def unavailable():raise sqlite3.OperationalError("isolated public source read failure")
    def read_error_after_commit():
        original_commit()
        f.source.latest_record=unavailable
    f.repo._commit=read_error_after_commit
    check("postcommit_source_read_error_withholds_fresh_delivery",fails(lambda:consume(f,value,request(f,prep,"consumed"))))
    f.repo._commit,f.source.latest_record=original_commit,original_latest
    receipt=f.repo.authority_message_receipt("consumed")
    check("postcommit_source_read_error_leaves_slot_spent",receipt.consumed and type(receipt) is MessageStageReceipt
        and f.repo.attempt(prep.attempt_id).lane_held)
    f.reopen();check("postcommit_source_read_error_replays_historically",f.repo.authority_message_receipt("consumed")==receipt)
    cancelled=f.repo.cancel_attempt_locally(prep.attempt_id,recorded_at_utc=a3.utc(NOW+10),idempotency_key="lost-delivery-cancel",
        expected_attempt_revision=f.repo.attempt(prep.attempt_id).revision,fence=f.repo.write_fence())
    check("postcommit_source_read_error_cannot_recreate_unsigned_release",cancelled.recorded_stage=="UNKNOWN" and cancelled.lane_held)
    f.close()


def continuity_and_modes(directory):
    from live.authority_controls_v0_1 import ClockReconciliation
    f,action,value,prep,profile=fixture(directory,"clock-recovery")
    unknown=consume(f,value,request(f,prep,"unknown"),sample=a3.clock(f.repo,NOW+9,status="UNKNOWN"))
    known=consume(f,value,request(f,prep,"known-without-continuity"),at=NOW+10)
    check("unknown_clock_cannot_self_recover",not unknown.consumed and not known.consumed
        and "CLOCK_CONTINUITY_RECONCILIATION_REQUIRED" in known.decision.reasons)
    previous=f.repo.authority_snapshot()["last_qualified_clock"]
    proof=ClockReconciliation(previous.content_digest,previous.epoch_id,previous.monotonic_ns,content_fingerprint("EXTERNAL_CLOCK_RECONCILIATION"))
    sample=a3.clock(f.repo,NOW+11,reconciliation=proof)
    positive=consume(f,value,request(f,prep,"reconciled"),sample=sample)
    check("explicit_original_clock_continuity_recovers_inside_window",type(positive) is FreshStageConsumption
        and f.repo.action(action.action_id).claimed_entry_deadline_us==action.claimed_entry_deadline_us)
    f.reopen();f.close()
    f,action,value,prep,profile=fixture(directory,"integer-boundary")
    base=request(f,prep,"maximum")
    for name,bad in (("bool",True),("float",1.5),("overflow",1<<64),("negative",-1)):
        check("stage_ordinal_"+name+"_rejected",fails(lambda:replace(base,expected_attempt_revision=bad)))
    result=consume(f,value,replace(base,expected_attempt_revision=(1<<64)-1))
    check("exact_u64_current_claim_preserved_without_SQL_loss",not result.consumed
        and receipt_from_json(receipt_to_json(result)).original.request.expected_attempt_revision==(1<<64)-1)
    stale=f.repo.write_fence();f.reopen()
    check("old_writer_generation_cannot_consume",fails(lambda:consume(f,value,request(f,prep,"old-generation"),fence=stale)))
    # An unresolved held lane cannot be replaced by a new message/ordinal.
    p=prep;message=decode_message(p.message_hex);h=message.header
    changed=Message.new_with_compiled_instructions(h.num_required_signatures,h.num_readonly_signed_accounts,h.num_readonly_unsigned_accounts,
        message.account_keys,Hash.from_string(sf.bh(211)),message.instructions)
    replacement=replace(p,ordinal=2,message_hex=a4.to_bytes_versioned(changed).hex(),lease=replace(p.lease,blockhash=sf.bh(211)))
    check("held_pending_lane_blocks_new_message_ordinal",fails(lambda:f.repo.prepare_attempt(replacement,fence=f.repo.write_fence())))
    f.close()
    dry=a3.Fixture(directory,"dry",mode="DRY")
    profile=replace(value.profile,economic_domain_id=dry.domain.economic_domain_id,policy_digest=dry.policy.content_digest)
    check("DRY_cannot_install_live_message_scope",fails(lambda:dry.repo.record_authority_message_profile(
        MessageProfileCommand("dry-profile","INSTALL_AND_SELECT",profile,profile.approval),fence=dry.repo.write_fence())))
    dry.reopen();dry.close()
    f,action,value,prep,profile=fixture(directory,"once-inclusive",once=True)
    result=consume(f,value,request(f,prep,"original-once-sign"),at=NOW+14)
    check("spent_one_time_admission_retains_exact_root_message_scope",type(result) is FreshStageConsumption
        and f.repo.authority_grant_status("once-original")["consumed_root"]==action.root_id)
    check("ENTRY_stage_original_deadline_is_inclusive",result.receipt.original.validation.clock.utc_upper_utc==a3.utc(NOW+14)
        and action.claimed_entry_deadline_us==(NOW+14)*1000000)
    expired=consume(f,value,request(f,prep,"expired-once-sign"),at=NOW+15)
    check("later_ENTRY_stage_expiry_retains_admission_and_spent_grant",not expired.consumed and "ORIGINAL_CANDIDATE_EXPIRED" in expired.decision.reasons
        and f.repo.inbox_disposition(action.root_id)=="ACCEPTED" and f.repo.reservation(action.root_id).status=="RESERVED")
    f.reopen();check("once_scope_never_recreated_on_reopen",f.repo.authority_message_receipt("original-once-sign")==result.receipt
        and f.repo.authority_grant_status("once-original")["consumed_by_command"]=="once-admission");f.close()


def tight_opening(directory,name):
    f,settlement,scenario=a3.actual_fixture(directory,name)
    f.repo.close()
    # Independently specified synthetic funding covers precisely the original
    # quote, entry setup, failure pool and ONE future exit. It is not a sizing
    # value read back from Authority or an injected funding projection.
    funding=105962960
    f.domain=replace(f.domain,known_native_wallet_lamports=funding)
    f.path=directory/(name+"-tight.sqlite3");f.repo=LedgerRepository.initialize(f.path,f.domain)
    scenario.accounts[f.domain.wallet]["lamports"]=funding
    scenario.accounts[sf.MINT]=a3.account(a3.TOKEN_PROGRAM_ID,sf.plans.fx.mint_account().data)
    scenario.slot_calls=scenario.genesis_calls=0
    a3.ingest(f.repo,a3.observe(scenario,request=a3.WalletEvidenceRequest(f.domain.wallet,a3.GENESIS,100)))
    f.root=f.repo.receive_candidate(f.item,fence=f.repo.write_fence())
    f.policy=replace(f.policy,costs=replace(f.policy.costs,setup_outflow_lamports=1844400,refundable_account_lock_lamports=sf.R,
        protective_setup_lamports=0,protective_refundable_lock_lamports=sf.R))
    a3.control(f.repo,"INSTALL_POLICY","install",policy_value=f.policy);a3.arm(f.repo,f.policy)
    accepted=a3.admit(f,"admit",support=a3.wallet(f,scenario=scenario))
    check(name+"_true_A3_exact_initial_budget",accepted.accepted and accepted.risk.required_native_lamports==funding)
    buy=f.repo.reservation(f.root).admission.action
    # The original public transaction buys actual units at full allowed100m
    # gross venue debit:99m principal+1m fees. Adjust the actual CPI and metadata
    # coherently, without using Ledger balances as the oracle.
    meta=settlement.scenario.tx["meta"];old=meta["preBalances"][0]
    meta["preBalances"][0]=funding;meta["postBalances"][0]+=funding-old-4000000
    meta["postBalances"][settlement.index[settlement.pool]]+=4000000
    settlement.principal=99000000
    for group in settlement.groups:
        for row in group["instructions"]:
            data=a3.cf._b58data(row["data"],65536)
            if row["programIdIndex"]==settlement.index[sf.SYSTEM_PROGRAM_ID] and len(data)==12 and data[:4]==struct.pack("<I",2) and row["accounts"]==[0,settlement.index[settlement.pool]]:
                row["data"]=sf.b58(struct.pack("<IQ",2,99000000))
            if data.startswith(sf.ANCHOR_EVENT_TAG+sf.PUMP_TRADE_EVENT_TAG):row["data"]=sf.b58(settlement.trade_event())
    return f,settlement,scenario,buy


def protected_position(directory,name,*,full=False,route="swap",failed_reduction=False,partial_applied=False,keep_wsol=False,tight=False,failed_fee=None):
    f,settlement,scenario,buy=tight_opening(directory,name) if tight else a4.fixture(directory,name)
    prep,chain,_=a3.actual_finality(f,settlement,buy)
    support,post,account_request=a3.cf.composed_support(f.repo,settlement)
    post.accounts[sf.MINT]=a3.account(a3.TOKEN_PROGRAM_ID,sf.plans.fx.mint_account().data)
    post.slot_calls=post.genesis_calls=0
    support=a3.WalletSupportInput(a3.observe(post,request=account_request,at=NOW+14),a3.utc(NOW+14),account_request.min_context_slot)
    handoff=a3.ports.handoff(f.repo,buy,prep,chain,support)
    applied=f.repo.apply_settlement_with_ports(prep.attempt_id,chain_receipt_key="transaction",support=support,
        ingestion_key="real-buy",recorded_at_utc=a3.utc(NOW+14),handoff=handoff,fence=f.repo.write_fence())
    check(name+"_actual_applied_protected_BUY",applied.application_disposition=="FINALIZED_SUCCESS_APPLIED")
    previous=settlement
    now=NOW+24;ordinal=1
    if failed_reduction or partial_applied or keep_wsol:
        def actual_failed_fee(fixture):
            if failed_fee is not None:
                meta=fixture.scenario.tx["meta"]
                meta["postBalances"][0]-=failed_fee-meta["fee"]
                meta["fee"]=failed_fee
        previous,old_sell,old_prep,old_chain,old_support,at=a3.cf.next_step(f.repo,settlement,support.observation.anchor,
            number=61,quantity=settlement.actual_base//4,position_id=buy.position_id,protective_handoff=handoff,
            failed=failed_reduction,keep_wsol=keep_wsol,mutate=actual_failed_fee)
        # This is an explicitly external prior Execution fixture, not an A4
        # consumption claim. A5 separately proves one continuous A4 message.
        receipt=a3.cf.apply(f.repo,old_prep,old_support,chain="chain-61",key="actual-reduction",at=at+20)
        check(name+"_actual_prior_reduction_application",receipt.decision.disposition==
            ("FINALIZED_FAILURE_APPLIED" if failed_reduction else "FINALIZED_SUCCESS_APPLIED"))
        support,post,account_request=a3.cf.composed_support(f.repo,previous,root=old_support.observation.anchor,at=at+21)
        post.accounts[sf.MINT]=a3.account(a3.TOKEN_PROGRAM_ID,sf.plans.fx.mint_account().data)
        post.slot_calls=post.genesis_calls=0
        now=at+31;ordinal=2
    position=f.repo.position_history(buy.position_id)
    quantity=position.remaining_units if full else position.remaining_units//3
    sell=a3.PendingAction(buy.root_id,buy.candidate_digest,"SELL",buy.mint,buy.token_program,buy.position_id,quantity,
        "EXTERNAL_RUNTIME_REDUCTION",content_fingerprint("runtime-reduction"),buy.policy_ref,buy.policy_digest,buy.selected_exit_track,handoff.obligation_id,ordinal)
    f.repo.stage_action(sell,fence=f.repo.write_fence())
    context=a4.capture_message_context(f.repo,sell.action_id);item=context.candidate
    intent=a4.ExecutionIntentV01(item.candidate_signal_id,item.candidate_run_id,item.strategy_evaluation_id,item.strategy_version,
        item.parameter_set_id,item.producer_run_id,item.source_event_key,item.signal_ingest_seq,(now-6)*1000000,item.mint,
        a4.IntentRole.EXIT,a4.IntentSide.SELL,"MEME_BASE_UNITS",quantity,sell.position_id,a4._entry_intent(context).intent_id,
        sell.external_decision_ref,sell.selected_exit_track,handoff.binding_id)
    slot=support.observation.anchor.slot
    value,plan=a4.evidence(f,sell,post,route=route,at=now,context_slot=slot,intent=intent)
    # A3's opening fixture helper deliberately requests floor101. The actual
    # current producer here requests the already recognized account/chain cut.
    account_request=replace(value.evidence.wallet.observation.request,min_context_slot=slot)
    post.initial_slot=slot;post.slot_calls=post.genesis_calls=0
    current_support=a3.WalletSupportInput(a3.observe(post,request=account_request,at=now-4),a3.utc(now-4),slot)
    value=replace(value,evidence=replace(value.evidence,wallet=current_support))
    if support.observation.anchor.block_height >= value.evidence.simulation.validity[-1].block_height:
        run=value.evidence.simulation
        validity=replace(run.validity[-1],block_height=support.observation.anchor.block_height+1)
        attempt=replace(run.attempts[-1],validity_fingerprint=validity.fingerprint)
        result=replace(run.results[-1],attempt_id=attempt.attempt_id)
        value=a4.with_run(value,replace(run,validity=(*run.validity[:-1],validity),attempts=(*run.attempts[:-1],attempt),results=(*run.results[:-1],result)))
    profile=f.repo.record_authority_message_profile(MessageProfileCommand("profile","INSTALL_AND_SELECT",value.profile,value.profile.approval),fence=f.repo.write_fence())
    prep=prepare(f,sell,value,plan,at=now-1,lower=support.observation.anchor)
    return f,buy,sell,value,prep,profile,now,handoff


def protective_cases(directory):
    for label in ("protective-migration","protective-entry-denied","protective-global-stop","protective-original-revoke",
                  "protective-full","protective-paid-failure","protective-paid-partial","protective-retained-wsol"):
        f,buy,sell,value,prep,profile,at,handoff=protected_position(directory,label,full=label=="protective-full",
            failed_reduction=label=="protective-paid-failure",partial_applied=label=="protective-paid-partial",keep_wsol=label=="protective-retained-wsol")
        if label=="protective-entry-denied":
            original_policy=f.policy
            a3.install(f,"new-entry",clock=replace(f.policy.clock,provider_id="UNRELATED_NEW_ENTRY_CLOCK"))
            new_profile=replace(value.profile,profile_id="new-entry-profile",policy_digest=f.policy.content_digest,approval=a3.operator(at-1))
            f.repo.record_authority_message_profile(MessageProfileCommand("new-entry-profile","INSTALL_AND_SELECT",new_profile,new_profile.approval),fence=f.repo.write_fence())
            a3.control(f.repo,"STOP_ENTRY","entry-stop",at=at-1)
            a3.control(f.repo,"DISARM_ENTRY","entry-disarm",at=at-1)
        elif label=="protective-global-stop":a3.control(f.repo,"HARD_STOP","all-stop",at=at-1)
        elif label=="protective-original-revoke":
            grant=f.repo.authority_acceptance(buy.root_id).eligibility.decision.grant_id
            a3.control(f.repo,"REVOKE_GRANT","original-hard-revoke",target=grant,at=at-1)
        result=consume(f,value,request(f,prep,"protect"),at=at)
        record=historical(result)
        positive=label not in ("protective-global-stop","protective-original-revoke")
        if positive and not record.consumed:print("PROTECTIVE_DENIAL",label,record.decision.reasons)
        check(label+"_current_original_scope",record.consumed==positive)
        risk=record.decision.risk
        check(label+"_no_entry_deadline_for_reduction",record.original.validation.clock.utc_lower_utc>a3.utc(NOW+15)
            and "ORIGINAL_CANDIDATE_EXPIRED" not in record.decision.reasons and record.original.source is None)
        check(label+"_actual_remaining_units",risk.remaining_position_units==f.repo.position_history(buy.position_id).remaining_units)
        check(label+"_reservation_not_released",f.repo.reservation(buy.root_id).status=="RESERVED")
        if positive:
            original=record.original.validation.context.original_policy
            expected=0 if label=="protective-full" else original.costs.protective_network_fee_lamports+original.costs.protective_setup_lamports+original.costs.protective_refundable_lock_lamports
            check(label+"_residual_has_another_full_exit_budget",risk.future_protection_lamports==expected)
        if label=="protective-paid-failure":
            check(label+"_actual_failed_SELL_fee_pool",risk.failed_attempt_count==1 and risk.remaining_failed_attempt_count==1
                and risk.failure_allowance_lamports==20000-sf.FEE and risk.historical_network_paid_lamports==sf.FEE*2)
        if label=="protective-paid-partial":
            check(label+"_paid_success_fee_not_deducted_twice",risk.failed_attempt_count==0 and risk.failure_allowance_lamports==20000
                and risk.historical_network_paid_lamports==sf.FEE*2)
        if label=="protective-retained-wsol":
            accounts=f.repo.consumer_snapshot()["accounts"]
            check(label+"_WSOL_and_locks_separate",risk.observed_wsol_units>0 and risk.observed_locked_lamports==sum(a.observed_locked_lamports for a in accounts)
                and risk.native_lamports==f.repo.consumer_snapshot()["funding"].native_lamports)
        f.reopen();check(label+"_original_scope_and_due_binding_replay",f.repo.authority_message_receipt("protect")==record
            and next(p for p in f.repo.consumer_snapshot()["protections"] if p.handoff.position_id==buy.position_id).handoff==handoff)
        f.close()


def protective_stage_changes(directory):
    for stage in ("SEND","REBROADCAST"):
        for control in ("HARD_STOP","REVOKE_GRANT"):
            name="protective-"+stage+"-"+control
            f,buy,sell,value,prep,profile,at,handoff=protected_position(directory,name)
            sign=consume(f,value,request(f,prep,"sign"),at=at)
            check(name+"_positive_original_protective_SIGN",type(sign) is FreshStageConsumption)
            signed(f,prep,at=at+1)
            if stage=="REBROADCAST":
                send=consume(f,value,request(f,prep,"send","SEND"),at=at+2)
                check(name+"_positive_original_protective_SEND",type(send) is FreshStageConsumption)
                advance(f,prep,"SEND_CLAIMED",at=at+3)
            grant=f.repo.authority_acceptance(buy.root_id).eligibility.decision.grant_id
            a3.control(f.repo,control,"stop-between-stages",at=at+1 if stage=="SEND" else at+3,
                target=grant if control=="REVOKE_GRANT" else None)
            result=consume(f,value,request(f,prep,"blocked-stage",stage,1 if stage=="REBROADCAST" else 0),at=at+2 if stage=="SEND" else at+4)
            reason="GLOBAL_HARD_STOP_LATCHED" if control=="HARD_STOP" else "ORIGINAL_SCOPE_GRANT_MISSING_OR_HARD_REVOKED"
            check(name+"_specific_current_all_stage_stop",not result.consumed and reason in result.decision.reasons
                and result.decision.validation_disposition=="SUPPORTED_CONTEXT_ONLY")
            f.reopen();check(name+"_held_across_restart",f.repo.attempt(prep.attempt_id).lane_held);f.close()
    for fee,positive in ((8889,True),(8890,False)):
        name="remaining-failure-fee-"+str(fee)
        f,buy,sell,value,prep,profile,at,handoff=protected_position(directory,name,failed_reduction=True,failed_fee=11111)
        value=replace(value,evidence=replace(value.evidence,fee=replace(value.evidence.fee,fee_lamports=fee)))
        check(name+"_exact_fee_still_inside_original_policy",a4.validate_message_evidence(value).disposition=="SUPPORTED_CONTEXT_ONLY")
        result=historical(consume(f,value,request(f,prep,"fee"),at=at))
        check(name+"_actual_paid_failure_pool_boundary",result.consumed==positive and result.decision.risk.failure_allowance_lamports==8889)
        if not positive:
            check(name+"_precise_remaining_fee_denial","CURRENT_EXACT_FEE_EXCEEDS_REMAINING_FAILURE_ALLOWANCE" in result.decision.reasons)
        f.reopen();f.close()
    for full in (False,True):
        name="tight-"+("full" if full else "partial")
        f,buy,sell,value,prep,profile,at,handoff=protected_position(directory,name,full=full,tight=True)
        check(name+"_current_A4a_supported",a4.validate_message_evidence(value).disposition=="SUPPORTED_CONTEXT_ONLY")
        result=historical(consume(f,value,request(f,prep,"tight"),at=at))
        if result.consumed!=full:print("TIGHT",full,result.decision.reasons,result.decision.risk)
        check(name+"_actual_funding_full_vs_residual_headroom",result.consumed==full
            and result.decision.risk.native_lamports==2071503 and result.decision.risk.historical_network_paid_lamports==sf.FEE)
        if not full:check(name+"_precise_residual_cost_denial","CURRENT_GROSS_COST_FAILURE_AND_RESIDUAL_EXIT_HEADROOM_INSUFFICIENT" in result.decision.reasons)
        check(name+"_does_not_free_reservation",f.repo.reservation(buy.root_id).status=="RESERVED")
        f.reopen();f.close()


def main():
    with tempfile.TemporaryDirectory(prefix="authority-message-control-") as directory:
        directory=Path(directory)
        try:
            digests=basic(directory)
            denials(directory)
            stage_lineage(directory)
            storage_cases(directory)
            codec_and_scope(directory)
            process_crashes(directory)
            current_truth_and_source(directory)
            continuity_and_modes(directory)
            protective_cases(directory)
            protective_stage_changes(directory)
        finally:
            for f in a3.a1.FIXTURES:
                f.close()
    print(json.dumps({"status":"IMPLEMENTED_PENDING_PROJECT_REVIEW","checks":len(CHECKS),"all_true":all(CHECKS.values()),
        "check_digest":content_fingerprint(CHECKS),"result_digests":digests},sort_keys=True))


if __name__=="__main__":
    if len(sys.argv)>1 and sys.argv[1]=="--crash":crash_child(Path(sys.argv[2]),sys.argv[3],sys.argv[4])
    else:main()
