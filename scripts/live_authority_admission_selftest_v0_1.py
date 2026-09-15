"""A3 original MockTransport facts -> true atomic Authority entry admission.

Policies/operator/clock inputs are explicit deterministic fixtures. Settlement
uses external future Execution/Runtime fixture ports, never real signing/send.
"""
from __future__ import annotations
import copy
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
from contextlib import closing
from types import SimpleNamespace
from dataclasses import asdict, replace
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"src"))
from phase5.shadow_domain_v0_1 import content_fingerprint, canonical_json
from phase5.shadow_unsigned_plan_simulation_v0_1 import derive_associated_token_address
from phase5.shadow_venue_route_quote_v0_1 import TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID, WSOL_MINT, SYSTEM_PROGRAM_ID
from live.authority_admission_v0_1 import EntryRequest, admission_receipt_from_record
from live.authority_controls_v0_1 import AuthorityPolicyV02, LOCKED_SELECTION_SHA256, LOCKED_PARAMETER_SET_BY_ROLE, policy_from_record
from live.ledger_actions_v0_1 import PendingAction, position_identity
from phase2.strategy_first_pullback_v0_2 import FirstPullbackStrategyV02
from live.ledger_domain_v0_1 import LedgerContractError
from live.ledger_repository_v0_1 import STORAGE_VERSION, LedgerRepository, LedgerJournalError, _DDL
from live.ledger_settlement_v0_1 import WalletSupportInput
from live.wallet_evidence_v0_1 import ExpectedTokenAccount, WalletEvidenceRequest
from live.evidence_store_v0_1 import SourceEvidenceStore
import live_authority_controls_selftest_v0_1 as a1
from live_authority_controls_selftest_v0_1 import Fixture, clock, control, arm, operator, digest, raises
from live_ledger_baseline_selftest_v0_1 import observe, domain, ingest
from live_ledger_actions_selftest_v0_1 import candidate
from live_wallet_evidence_selftest_v0_1 import NOW, utc, WALLET, GENESIS, Scenario, account, mint_data, token_data, PROFILE
import live_ledger_settlement_selftest_v0_1 as sf
import live_ledger_custody_selftest_v0_1 as cf
import live_ledger_ports_selftest_v0_1 as ports
CHECKS={}

def check(name,value):
    CHECKS[name]=bool(value)
    if not value: raise AssertionError(name)

def wallet(f, *, at=NOW+4, program=TOKEN_PROGRAM_ID, scenario=None):
    value=scenario or Scenario()
    if scenario is None:
        value.accounts={f.domain.wallet:account(SYSTEM_PROGRAM_ID,lamports=f.domain.known_native_wallet_lamports)}
    value.initial_slot=101
    value.slot_calls=value.genesis_calls=0
    expected={a.pubkey:(a.mint,a.program) for a in f.repo.consumer_snapshot()["accounts"]}
    base=derive_associated_token_address(f.domain.wallet,f.item.mint,program)
    quote=derive_associated_token_address(f.domain.wallet,WSOL_MINT,TOKEN_PROGRAM_ID)
    expected.update({base:(f.item.mint,program),quote:(WSOL_MINT,TOKEN_PROGRAM_ID)})
    for mint,pid in expected.values():
        value.accounts.setdefault(mint,account(pid,mint_data(9 if mint==WSOL_MINT else 6)))
    req=WalletEvidenceRequest(f.domain.wallet,GENESIS,101,tuple(ExpectedTokenAccount(key,mint,pid) for key,(mint,pid) in sorted(expected.items())))
    return WalletSupportInput(observe(value,request=req,at=at),utc(at),101)

def admit(f,name,*,at=NOW+4,support=None,sample=None,program=TOKEN_PROGRAM_ID,venue="PUMP"):
    return f.repo.admit_authority_entry(EntryRequest(f.root,venue,program,f.policy.selected_track),sample or clock(f.repo,at),f.source,
        support or wallet(f,at=at,program=program),command_id=name,fence=f.repo.write_fence())

def basic(directory):
    f=Fixture(directory,"basic",arm_now=False)
    arm(f.repo,f.policy,scope="ENTRY_ONCE",root=f.root)
    before=f.repo.audit()
    support=wallet(f)
    sample=clock(f.repo)
    receipt=admit(f,"admit",support=support,sample=sample)
    check("actual_current_source_wallet_Ledger_admission",receipt.accepted)
    check("one_commit_eligibility_comparison_action_reservation_consumption",f.repo.audit()["revision"]==before["revision"]+1
        and receipt.sequence==f.repo.authority_receipt("admit").sequence==f.repo.port_receipt("admit").sequence==f.repo.comparison_receipt("admit").sequence)
    check("one_time_consumed_exact_root",receipt.consumed_grant_id=="grant1" and f.repo.authority_grant_status("grant1")["consumed_root"]==f.root)
    check("accepted_inbox_and_actual_terms",f.repo.inbox_disposition(f.root)=="ACCEPTED"
        and f.repo.reservation(f.root).admission.action.input_units==1000000 and not f.repo.consumer_snapshot()["positions"])
    risk=receipt.risk
    check("venue_fee_is_inside_quote_not_added_twice",risk.required_native_lamports==13040000
        and risk.reservation.venue_fee_lamports==0 and risk.venue_fee_included_bound_lamports==10000)
    check("original_known_native_not_paid_cost_double_debit",risk.native_lamports==5000000000 and risk.outstanding_native_lamports==0)
    check("receipt_and_input_retrievable_immutable",admission_receipt_from_record(json.loads(canonical_json(asdict(receipt))))==receipt
        and f.repo.wallet_support(receipt.wallet_support_digest)==support and not receipt.grants_message_permission)
    at=f.repo.write_fence()
    check("exact_duplicate_converges",admit(f,"admit",sample=sample,support=support)==receipt and f.repo.write_fence()==at)
    check("request_content_conflict",raises(lambda:admit(f,"admit",sample=sample,support=support,venue="PUMPSWAP")))
    f.reopen()
    check("original_aged_receipt_replays",f.repo.authority_admission_receipt("admit")==receipt and f.repo.authority_acceptance(f.root)==receipt)
    duplicate=admit(f,"repeat",at=NOW+5)
    check("new_request_cannot_second_consume_or_resize",not duplicate.accepted and "ONE_TIME_ENTRY_GRANT_ALREADY_CONSUMED" in duplicate.risk.reasons
        and f.repo.audit()["action_count"]==1 and f.repo.authority_acceptance(f.root)==receipt)
    check("one_time_issuance_getter_is_only_historical",f.repo.authority_grant("grant1") is not None
        and f.repo.authority_grant_status("grant1")["consumed_by_command"]=="admit")
    f.repo.audit()
    f.close()
    return receipt.content_digest

def install(f,name,*,size=None,costs=None,**fields):
    f.policy=replace(f.policy,policy_id=name,approval=operator(),size=replace(f.policy.size,**(size or {})),
        costs=replace(f.policy.costs,**(costs or {})),**fields)
    control(f.repo,"INSTALL_POLICY","install-"+name,policy_value=f.policy)
    arm(f.repo,f.policy,name="grant-"+name)


def denied_cases(directory):
    for name,changes,reason in (
        ("fixed-below-min",{"size":{"fixed_quote_lamports":1,"minimum_quote_lamports":2}},"FIXED_SIZE_BELOW_MINIMUM"),
        ("trade-cap",{"size":{"max_trade_notional_lamports":999999}},"FIXED_SIZE_ABOVE_TRADE_CAP"),
        ("global-cap",{"size":{"max_global_exposure_lamports":999999}},"GLOBAL_ORIGINAL_CAP_EXPOSURE_EXCEEDED"),
        ("mint-cap",{"size":{"max_mint_exposure_lamports":999999}},"MINT_ORIGINAL_CAP_EXPOSURE_EXCEEDED"),
        ("max2",{"size":{"max_open_positions":2}},"V1_REQUIRES_EXPLICIT_MAX_ONE_POSITION_POLICY"),
        ("no-protection",{"costs":{"protective_network_fee_lamports":0}},"EXPLICIT_COST_OR_PROTECTIVE_FAILURE_BUDGET_UNPROVEN"),
        ("failed-budget",{"costs":{"failed_attempt_network_budget_lamports":1}},"EXPLICIT_COST_OR_PROTECTIVE_FAILURE_BUDGET_UNPROVEN"),
        ("overflow",{"costs":{"setup_outflow_lamports":(1<<64)-1}},"EXACT_U64_BUDGET_AGGREGATE_OVERFLOW"),
        ("venue",{"allowed_venues":("PUMPSWAP",)},"VENUE_NOT_ALLOWED")):
        f=Fixture(directory,name)
        install(f,name,**changes)
        receipt=admit(f,name)
        check(name+"_explicit_denial",not receipt.accepted and reason in receipt.risk.reasons)
        check(name+"_no_action_reservation_consumption",not f.repo.consumer_snapshot()["reservations"]
            and f.repo.audit()["action_count"]==0 and receipt.consumed_grant_id is None)
        check(name+"_denial_binds_returned_risk",f.repo._conn.execute("SELECT external_record_digest FROM (SELECT json_extract(payload_json,'$.external_record_digest') AS external_record_digest FROM ledger_inbox_dispositions)").fetchone()[0]==receipt.risk.content_digest)
        f.reopen();check(name+"_original_denial_replay",f.repo.authority_admission_receipt(name)==receipt);f.close()
    for name in ("withdrawal","deposit","unsupported","unknown","missing-mint","missing-explicit","retained-wsol","historical-new-token22","wrong-genesis","old-wallet","behind-floor"):
        f=Fixture(directory,name)
        scenario=Scenario()
        program=TOKEN_PROGRAM_ID
        if name in ("withdrawal","deposit"):
            scenario.accounts[WALLET]["lamports"]+=(-1 if name=="withdrawal" else 1)
        elif name=="unsupported":
            scenario.add_token(data=token_data(f.item.mint,amount=1,tail=b"extension"))
        elif name=="unknown": scenario.program_failure=TOKEN_PROGRAM_ID
        elif name=="retained-wsol":
            key=derive_associated_token_address(WALLET,WSOL_MINT,TOKEN_PROGRAM_ID)
            scenario.add_token(token=key,mint=WSOL_MINT,data=token_data(WSOL_MINT,amount=0,reserve=2039280))
        elif name=="historical-new-token22":program=TOKEN_2022_PROGRAM_ID
        elif name=="wrong-genesis":scenario.genesis=sf.bh(199)
        support=wallet(f,scenario=scenario,program=program)
        if name=="historical-new-token22":
            support=replace(support,observation=replace(support.observation,schema="live_wallet_account_evidence_v0.2"))
        if name=="missing-mint":
            support=replace(support,observation=replace(support.observation,mint_reads=()))
        if name=="missing-explicit":
            support=WalletSupportInput(observe(at=NOW+4),utc(NOW+4),101)
        if name=="old-wallet":
            support=replace(support,observation=replace(support.observation,started_at_utc=utc(NOW-100),observed_at_utc=utc(NOW-100)))
        if name=="behind-floor":support=replace(support,required_min_context_slot=100)
        receipt=admit(f,name,support=support,program=program)
        check(name+"_fresh_truth_denial",not receipt.accepted)
        check(name+"_original_wallet_retained",f.repo.wallet_support(support.digest)==support)
        if name in ("withdrawal","deposit"):
            check(name+"_durable_quarantine_not_new_capital",bool(f.repo.consumer_snapshot()["funding"].quarantine_reasons)
                and f.repo.consumer_snapshot()["funding"].native_lamports==5000000000)
        f.reopen();check(name+"_replay",f.repo.authority_admission_receipt(name)==receipt);f.close()
    for name,at,upper,status,expect in (("deadline",NOW+14,None,"QUALIFIED",True),
        ("expired",NOW+15,None,"QUALIFIED",False),("straddle",NOW+14,NOW+15,"QUALIFIED",False),
        ("clock-unknown",NOW+15,None,"UNKNOWN",False)):
        f=Fixture(directory,name)
        sample=clock(f.repo,at,upper=upper,status=status)
        support=wallet(f,at=upper or at)
        receipt=admit(f,name,at=at,sample=sample,support=support)
        check(name+"_original_deadline_semantics",receipt.accepted==expect)
        check(name+"_expiry_only_positive_lower",(f.repo.inbox_disposition(f.root)=="EXPIRED")== (name=="expired"))
        f.reopen();check(name+"_time_replay",f.repo.authority_admission_receipt(name)==receipt);f.close()
    # Same native truth at an explicitly configured small opening funding cut.
    f=Fixture(directory,"insufficient")
    f.repo.close()
    f.path=directory/"small-ledger.sqlite3"
    f.domain=replace(f.domain,known_native_wallet_lamports=1000)
    f.repo=LedgerRepository.initialize(f.path,f.domain)
    scenario=Scenario();scenario.accounts[WALLET]["lamports"]=1000
    ingest(f.repo,observe(scenario));f.root=f.repo.receive_candidate(f.item,fence=f.repo.write_fence())
    f.policy=replace(f.policy,economic_domain_id=f.domain.economic_domain_id)
    control(f.repo,"INSTALL_POLICY","install",policy_value=f.policy);arm(f.repo,f.policy)
    scenario=Scenario();scenario.accounts[WALLET]["lamports"]=1000
    receipt=admit(f,"small",support=wallet(f,scenario=scenario))
    check("actual_coherent_insufficient_funding",not receipt.accepted and "INSUFFICIENT_UNENCUMBERED_NATIVE_SOL" in receipt.risk.reasons)
    f.reopen();f.close()


def staging_and_source(directory):
    for field,value in (("policy_ref","wrong-policy"),("deadline_binding_digest",digest("wrong-deadline"))):
        f=Fixture(directory,"staged-"+field)
        eligibility=f.evaluate("original")
        action=PendingAction(f.root,f.item.content_digest,"BUY",f.item.mint,TOKEN_PROGRAM_ID,position_identity(f.domain,f.root,f.item.mint),
            f.policy.size.fixed_quote_lamports,"EXTERNAL_STAGED_TERMS",digest("staged"),f.policy.policy_id,f.policy.content_digest,
            "FINAL-A",None,1,eligibility.decision.binding.deadline_us,content_fingerprint(asdict(eligibility.decision.binding)))
        f.repo.stage_action(replace(action,**{field:value}),fence=f.repo.write_fence())
        receipt=admit(f,"denied",at=NOW+5)
        check("staged_"+field+"_conflict",not receipt.accepted and "STAGED_IMMUTABLE_TERMS_CONFLICT" in receipt.risk.reasons)
        f.reopen();f.close()
    f=Fixture(directory,"source-race",arm_now=False)
    arm(f.repo,f.policy,scope="ENTRY_ONCE",root=f.root)
    old=f.repo._insert_wallet_comparison
    def changed(*args,**kwargs):
        result=old(*args,**kwargs)
        a1.source_fixture.add_gap(f.raw,"PENDING")
        value=a1.source_fixture.observe(f.raw,f.source.binding,previous=f.initial,now=12)
        f.source.append(value,expected_previous_digest=f.initial.content_digest)
        return result
    f.repo._insert_wallet_comparison=changed
    before=f.repo.audit()
    check("source_changed_before_commit_rejects_positive",raises(lambda:admit(f,"race")))
    f.repo._insert_wallet_comparison=old
    check("source_race_rolls_back_all_children_consumption",f.repo.audit()==before and f.repo.authority_grant_status("grant1")["consumed_root"] is None)
    denial=admit(f,"new-gap")
    check("new_request_uses_latest_gap",not denial.accepted and denial.eligibility.original.source_sequence==2)
    f.reopen();f.close()


def static_candidate(mint,binding,suffix="v02",generated=None):
    original=candidate(mint,suffix=suffix)
    parameter=LOCKED_PARAMETER_SET_BY_ROLE["CONTROL"]
    stable=FirstPullbackStrategyV02._stable_id
    run=stable("fp1run",mint,original.run_started_at_us,original.strategy_version,parameter)
    signal=stable("fp1sig",run,original.source_event_key,parameter,original.strategy_version)
    return replace(original,candidate_run_id=run,candidate_signal_id=signal,winner_candidate_id=signal,
        parameter_set_id=parameter,winner_role="CONTROL",source_binding=binding,
        generated_at_us=original.generated_at_us if generated is None else generated*1000000,
        reference_at_us=original.reference_at_us if generated is None else generated*1000000)


def policy_v02(old):
    values=asdict(old)
    values.pop("winner_binding_digest");values.pop("version")
    values.update(approval=old.approval,size=old.size,costs=old.costs,clock=old.clock,
        selection_manifest_digest=LOCKED_SELECTION_SHA256,winner_role="CONTROL")
    return AuthorityPolicyV02(**values)


def actual_fixture(directory,name,*,venue="pump",failed=False,program=TOKEN_PROGRAM_ID):
    f=Fixture(directory,name,install_now=False,arm_now=False)
    f.repo.close()
    fixture=sf.Fixture(venue,failed=failed,token_program=program)
    fixture.mint=sf.MINT
    f.domain=domain(wallet=sf.WALLET)
    scenario=fixture.wallet_scenario()
    if program==TOKEN_2022_PROGRAM_ID:
        expected=(ExpectedTokenAccount(fixture.base,sf.MINT,program),)
        f.domain=replace(f.domain,expected_empty_token_accounts=expected)
        scenario.add_token(token=fixture.base,mint=sf.MINT,program=program,data=token_data(sf.MINT,sf.WALLET,0,tail=b"\x02\x07\x00\x00\x00"))
        for group in fixture.groups:
            outer=fixture.message.instructions[group["index"]]
            if fixture.keys[outer.program_id_index]==sf.ASSOCIATED_TOKEN_PROGRAM_ID and fixture.keys[outer.accounts[1]]==fixture.base:
                group["instructions"]=[]
        meta=fixture.scenario.tx["meta"]
        meta["preTokenBalances"].append(fixture.token_row(fixture.base,sf.MINT,sf.WALLET,0,program))
        meta["preBalances"][fixture.index[fixture.base]]=sf.R
        meta["postBalances"][0]+=sf.R
    f.path=directory/(name+"-actual.sqlite3")
    f.repo=LedgerRepository.initialize(f.path,f.domain)
    ingest(f.repo,observe(scenario,request=WalletEvidenceRequest(sf.WALLET,GENESIS,100,f.domain.expected_empty_token_accounts)))
    f.item=static_candidate(sf.MINT,f.source.binding)
    f.root=f.repo.receive_candidate(f.item,fence=f.repo.write_fence())
    legacy=a1.policy(f.item,binding=f.domain)
    f.policy=policy_v02(replace(legacy,size=replace(legacy.size,fixed_quote_lamports=fixture.units,
        ceiling_quote_lamports=fixture.units,max_trade_notional_lamports=fixture.units,max_global_exposure_lamports=3*fixture.units,
        max_mint_exposure_lamports=fixture.units),costs=replace(legacy.costs,venue_fee_within_quote_cap_lamports=2000000)))
    control(f.repo,"INSTALL_POLICY","install",policy_value=f.policy);arm(f.repo,f.policy)
    return f,fixture,scenario


def actual_finality(f,fixture,action):
    prep=replace(ports.prepare(f.repo,fixture,action),prepared_at_utc=utc(NOW+5))
    f.repo.prepare_attempt(prep,fence=f.repo.write_fence())
    for target,at in (("EXACT_SIMULATED",NOW+6),("AUTHORIZED",NOW+7),("SIGNED_DURABLE",NOW+8)):
        sf.advance(f.repo,prep,target,at=at)
    chain=f.repo.ingest_chain_observation(prep.attempt_id,cf.transaction_observation(fixture.scenario),ingestion_key="transaction",
        evaluated_at_utc=utc(NOW+10),fence=f.repo.write_fence())
    support,value,request=cf.composed_support(f.repo,fixture)
    if fixture.token_program==TOKEN_2022_PROGRAM_ID:
        units=int(next(row for row in fixture.scenario.tx["meta"]["postTokenBalances"] if row["accountIndex"]==fixture.index[fixture.base])["uiTokenAmount"]["amount"])
        value.accounts[fixture.base]=account(TOKEN_2022_PROGRAM_ID,token_data(sf.MINT,sf.WALLET,units,tail=b"\x02\x07\x00\x00\x00"),sf.R)
        value.slot_calls=value.genesis_calls=0
        support=WalletSupportInput(observe(value,request=request,at=NOW+14),utc(NOW+14),request.min_context_slot)
    return prep,chain,support


def later_candidate(f,mint,suffix,generated):
    # Extend the actual temp collector's observed prefix, with independent public
    # producer events and receipt times. No SourceVerdict is manufactured.
    event="next-event-"+suffix;signature="next-signature-"+suffix
    at=a1.source_fixture.at(generated-NOW+8)
    a1.source_fixture.change(f.raw,"INSERT INTO pump_events VALUES (?,?,?,?,?)",(event,signature,200,a1.source_fixture.LIVE,at))
    a1.source_fixture.change(f.raw,"INSERT INTO websocket_observations VALUES (?,?,?,?,?)",(signature,200,a1.source_fixture.at(generated-NOW+10),1,"fixture"))
    previous=f.source.latest_record()[1]
    value=a1.source_fixture.observe(f.raw,f.source.binding,previous=previous,now=generated-NOW+12,cut=generated-NOW+9)
    f.source.append(value,expected_previous_digest=previous.content_digest)
    f.item=static_candidate(mint,f.source.binding,suffix,generated)
    f.root=f.repo.receive_candidate(f.item,fence=f.repo.write_fence())


def later_wallet(f,fixture,lower,at):
    support,value,request=cf.composed_support(f.repo,fixture,root=lower,at=at)
    expected={item.pubkey:(item.mint,item.program) for item in request.expected_accounts}
    expected[derive_associated_token_address(f.domain.wallet,f.item.mint,TOKEN_PROGRAM_ID)]=(f.item.mint,TOKEN_PROGRAM_ID)
    value.accounts[f.item.mint]=account(TOKEN_PROGRAM_ID,mint_data(6))
    request=replace(request,min_context_slot=lower.slot,expected_accounts=tuple(ExpectedTokenAccount(key,mint,pid) for key,(mint,pid) in sorted(expected.items())))
    value.slot_calls=value.genesis_calls=0
    observation=observe(value,request=request,at=at)
    return WalletSupportInput(observation,utc(at),request.min_context_slot)


def economic_cases(directory):
    results={}
    for label,venue,failed,program in (("pump","pump",False,TOKEN_PROGRAM_ID),("swap","swap",False,TOKEN_PROGRAM_ID),
            ("failed","swap",True,TOKEN_PROGRAM_ID),("token22","swap",False,TOKEN_2022_PROGRAM_ID)):
        f,fixture,value=actual_fixture(directory,label,venue=venue,failed=failed,program=program)
        receipt=admit(f,"admit",program=program,venue="PUMP" if venue=="pump" else "PUMPSWAP",support=wallet(f,scenario=value,program=program))
        check(label+"_true_current_authority_admission",receipt.accepted)
        action=f.repo.reservation(f.root).admission.action
        check(label+"_static_manifest_keeps_original_winner",receipt.eligibility.decision.policy_digest==f.policy.content_digest
            and f.repo.candidate(f.root).winner_binding_digest==digest("winner-v02"))
        prep,chain,support=actual_finality(f,fixture,action)
        if failed:
            applied=cf.apply(f.repo,prep,support,at=NOW+14)
            check(label+"_actual_failed_fee_only",applied.decision.disposition=="FINALIZED_FAILURE_APPLIED"
                and not f.repo.consumer_snapshot()["positions"])
        else:
            handoff=ports.handoff(f.repo,action,prep,chain,support)
            group=f.repo.apply_settlement_with_ports(prep.attempt_id,chain_receipt_key="transaction",support=support,
                ingestion_key="apply-protect",recorded_at_utc=utc(NOW+14),handoff=handoff,fence=f.repo.write_fence())
            check(label+"_actual_units_and_protection",group.application_disposition=="FINALIZED_SUCCESS_APPLIED"
                and f.repo.position_history(action.position_id).remaining_units==fixture.actual_base)
        f.reopen()
        check(label+"_applied_receipt_and_money_replay",f.repo.authority_acceptance(action.root_id)==receipt
            and f.repo.consumer_snapshot()["funding"].native_lamports==fixture.scenario.tx["meta"]["postBalances"][0])
        # The next candidate is genuinely distinct. It cannot enter while the
        # prior root remains owned, unresolved, or flat pending retirement.
        old_root=action.root_id
        later_candidate(f,cf.key(22),"occupied-"+label,NOW+15)
        support2=later_wallet(f,fixture,support.observation.anchor,NOW+19)
        denied=admit(f,"occupied",at=NOW+19,support=support2)
        check(label+"_admitted_or_actual_position_occupies_v1",not denied.accepted and "V1_OCCUPIED_POSITION_OR_UNRESOLVED_ADMISSION" in denied.risk.reasons)
        cost=next(row for row in denied.risk.root_encumbrances if row.root_id==old_root)
        check(label+"_paid_fee_not_subtracted_twice",cost.paid_failed_network_lamports==(sf.FEE if failed else 0)
            and cost.principal_lamports==(fixture.units if failed else 0)
            and cost.network_lamports==(f.policy.costs.network_total_fee_lamports+f.policy.costs.failed_attempt_network_budget_lamports-sf.FEE if failed else f.policy.costs.failed_attempt_network_budget_lamports))
        f.reopen();check(label+"_risk_after_actual_effect_replays",f.repo.authority_admission_receipt("occupied")==denied)
        if label=="pump":
            partial=fixture.actual_base//3
            reduced_fixture,sell,sp,sc,ss,at=cf.next_step(f.repo,fixture,support2.observation.anchor,number=50,quantity=partial,
                position_id=action.position_id,protective_handoff=handoff)
            cf.apply(f.repo,sp,ss,chain="chain-50",key="partial",at=at+20)
            # Shared failed BUY/SELL allowance and next-exit protection are
            # derived from actual applied entries, without mutating economics.
            f.item=f.repo.candidate(f.root)
            later_candidate(f,cf.key(23),"partial",at+21)
            current=later_wallet(f,reduced_fixture,ss.observation.anchor,at+25)
            held=admit(f,"partial-held",at=at+25,support=current)
            cost=next(row for row in held.risk.root_encumbrances if row.root_id==old_root)
            check("partial_exit_preserves_full_next_exit_headroom",not held.accepted and cost.protective_lamports==
                f.policy.costs.protective_network_fee_lamports+f.policy.costs.protective_setup_lamports+f.policy.costs.protective_refundable_lock_lamports
                and cost.paid_reduction_network_lamports==sf.FEE)
            final_fixture,sell2,p2,c2,s2,at2=cf.next_step(f.repo,reduced_fixture,current.observation.anchor,number=51,
                quantity=fixture.actual_base-partial,position_id=action.position_id,protective_handoff=handoff,action_ordinal=2)
            retirement,comparison=ports.retirement(f.repo,sell2,p2,s2,at=at2+20)
            retired=f.repo.apply_settlement_with_ports(p2.attempt_id,chain_receipt_key="chain-51",support=s2,
                ingestion_key="retire",recorded_at_utc=utc(at2+20),retirement=retirement,retirement_support=comparison,fence=f.repo.write_fence())
            check("actual_full_sell_and_retirement_releases_encumbrance",retired.retirement_disposition=="RETIRED" and not f.repo.consumer_snapshot()["reservations"])
            old_winner=f.repo.candidate(old_root).winner_binding_digest
            later_candidate(f,cf.key(24),"next-after-retirement",at2+21)
            new_support=later_wallet(f,final_fixture,s2.observation.anchor,at2+25)
            next_receipt=admit(f,"next",at=at2+25,support=new_support)
            check("recurring_arming_admits_next_mint_without_rearm",next_receipt.accepted and next_receipt.eligibility.decision.grant_id=="grant1")
            check("per_candidate_winner_digest_never_fabricated_shared",f.item.winner_binding_digest!=old_winner
                and f.repo.candidate(old_root).winner_binding_digest==old_winner)
            check("retired_paid_costs_not_outstanding_again",next_receipt.risk.outstanding_native_lamports==0
                and next_receipt.risk.historical_network_paid_lamports==3*sf.FEE)
            f.reopen();check("recurring_actual_history_reconstructs",f.repo.authority_acceptance(f.root)==next_receipt and f.repo.authority_acceptance(old_root)==receipt)
        results[label]=f.repo.audit()["custody_digest"]
        f.close()
    return results


def lineage_cases(directory):
    for state in ("admitted", "unknown"):
        f,fixture,value=actual_fixture(directory,"held-"+state)
        first=admit(f,"first",support=wallet(f,scenario=value))
        oldroot=f.root
        action=f.repo.reservation(oldroot).admission.action
        if state=="unknown":
            prep=replace(ports.prepare(f.repo,fixture,action),prepared_at_utc=utc(NOW+5))
            f.repo.prepare_attempt(prep,fence=f.repo.write_fence())
            sf.advance(f.repo,prep,"UNKNOWN",at=NOW+6)
            f.reopen()
        later_candidate(f,cf.key(25),"competing",NOW+7)
        second=admit(f,"second",at=NOW+11,support=wallet(f,at=NOW+11))
        check(state+"_new_root_blocked_before_position",not second.accepted and not f.repo.consumer_snapshot()["positions"]
            and second.eligibility.decision.disposition=="ELIGIBLE_CONTEXT_ONLY")
        encumbrance=next(row for row in second.risk.root_encumbrances if row.root_id==oldroot)
        check(state+"_worst_case_native_held",encumbrance.outstanding_lamports==first.risk.required_native_lamports)
        if state=="unknown":check("unknown_survives_reopen_no_timeout_release",f.repo.mutation_lane()["held"]
            and "COMPETING_POSSIBLY_LANDING_MUTATION_LANE" in second.risk.reasons)
        f.reopen();check(state+"_risk_reconstruction",f.repo.authority_admission_receipt("second")==second);f.close()
    f,fixture,value=actual_fixture(directory,"spent-after-retire",venue="swap",failed=True)
    arm(f.repo,f.policy,name="once",scope="ENTRY_ONCE",root=f.root)
    first=admit(f,"first",venue="PUMPSWAP",support=wallet(f,scenario=value))
    action=f.repo.reservation(f.root).admission.action
    prep,chain,support=actual_finality(f,fixture,action)
    applied=cf.apply(f.repo,prep,support,at=NOW+14)
    intent,comparison=ports.retirement(f.repo,action,prep,support,reason="FAILED_NO_ACQUISITION",at=NOW+15)
    retired=f.repo.retire(intent,comparison,ingestion_key="retire",fence=f.repo.write_fence())
    check("failed_fee_only_lawful_retirement",retired.retirement_disposition=="RETIRED" and applied.decision.proposal.base_units_delta==0)
    f.reopen()
    check("one_time_consumption_survives_failure_and_retirement",f.repo.authority_grant_status("once")["consumed_root"]==action.root_id)
    later_candidate(f,cf.key(26),"after-once",NOW+16)
    current=later_wallet(f,fixture,support.observation.anchor,NOW+20)
    denied=admit(f,"spent",at=NOW+20,support=current)
    check("one_time_retirement_is_not_rearming",not denied.accepted and "ONE_TIME_ENTRY_GRANT_ALREADY_CONSUMED" in denied.risk.reasons)
    control(f.repo,"REVOKE_GRANT","revoke-once",at=NOW+20,target="once")
    check("revocation_and_consumption_both_retained",f.repo.authority_grant_status("once")["revoked"]
        and f.repo.authority_grant_status("once")["consumed_root"]==action.root_id)
    # A new recurring grant does not erase a prior winning-mint tombstone.
    arm(f.repo,f.policy,name="recurring",at=NOW+20)
    later_candidate(f,action.mint,"same-mint-after-retire",NOW+21)
    current=later_wallet(f,fixture,current.observation.anchor,NOW+25)
    tombstone=admit(f,"same-mint",at=NOW+25,support=current)
    check("winning_mint_tombstone_permanent_after_paid_failure",not tombstone.accepted and "PERMANENT_ACCEPTED_ROOT_OR_WINNING_MINT_TOMBSTONE" in tombstone.risk.reasons)
    check("retired_root_historical_immutable_terms",f.repo.authority_acceptance(action.root_id)==first)
    f.reopen();check("grant_tombstone_history_replays",f.repo.authority_admission_receipt("same-mint")==tombstone);f.close()
    f=Fixture(directory,"dry",mode="DRY")
    receipt=admit(f,"dry")
    check("dry_scope_only_tentative_reservation",receipt.accepted and receipt.consumed_grant_id is None
        and f.repo.reservation(f.root).status=="TENTATIVE_DRY" and f.repo.audit()["posting_count"]==0)
    f.reopen();check("dry_admission_mode_replays",f.repo.authority_admission_receipt("dry")==receipt);f.close()
    # Version branches retain exact original v0.1 semantics; an explicit V02
    # install is required to use recurring static selection, never reinterpretation.
    f,fixture,value=actual_fixture(directory,"policy-versions")
    check("finite_v02_codec_exact",policy_from_record(json.loads(canonical_json(asdict(f.policy))))==f.policy)
    old=a1.policy(f.item,binding=f.domain)
    check("v01_exact_original_winner_semantics_retained",policy_from_record(json.loads(canonical_json(asdict(old))))==old
        and old.winner_binding_digest==f.item.winner_binding_digest)
    for name,fields in (("manifest",{"selection_manifest_digest":digest("wrong")}),
            ("role",{"winner_role":"PRIMARY"}),("parameters",{"parameter_set_id":"FIXTURE_PARAMETERS"})):
        check("v02_rejects_"+name,raises(lambda fields=fields:replace(f.policy,**fields)))
    control(f.repo,"HARD_STOP","stop",at=NOW+4)
    stopped=admit(f,"stopped",support=wallet(f,scenario=value))
    check("hard_stop_blocks_actual_admission",not stopped.accepted and "GLOBAL_HARD_STOP_LATCHED" in stopped.risk.reasons)
    f.reopen();f.close()


def reduction_resource_cases(directory):
    for label,failed,retained in (("failed-sell",True,False),("retained-proceeds",False,True)):
        f,fixture,value=actual_fixture(directory,label,venue="swap")
        first=admit(f,"first",venue="PUMPSWAP",support=wallet(f,scenario=value))
        action=f.repo.reservation(f.root).admission.action
        prep,chain,support=actual_finality(f,fixture,action)
        handoff=ports.handoff(f.repo,action,prep,chain,support)
        f.repo.apply_settlement_with_ports(prep.attempt_id,chain_receipt_key="transaction",support=support,
            ingestion_key="buy",recorded_at_utc=utc(NOW+14),handoff=handoff,fence=f.repo.write_fence())
        newfixture,sell,sp,sc,ss,at=cf.next_step(f.repo,fixture,support.observation.anchor,number=53,
            quantity=fixture.actual_base,position_id=action.position_id,protective_handoff=handoff,failed=failed,keep_wsol=retained)
        if failed:
            applied=cf.apply(f.repo,sp,ss,chain="chain-53",key="failed-sell",at=at+20)
            check("failed_SELL_is_actual_fee_only",applied.decision.disposition=="FINALIZED_FAILURE_APPLIED"
                and f.repo.position_history(action.position_id).remaining_units==fixture.actual_base)
        else:
            intent,comparison=ports.retirement(f.repo,sell,sp,ss,at=at+20)
            group=f.repo.apply_settlement_with_ports(sp.attempt_id,chain_receipt_key="chain-53",support=ss,
                ingestion_key="retained-retire",recorded_at_utc=utc(at+20),retirement=intent,retirement_support=comparison,fence=f.repo.write_fence())
            check("retirement_does_not_erase_actual_retained_WSOL",group.retirement_disposition=="RETIRED"
                and any(item.mint==WSOL_MINT and item.units>0 for item in f.repo.consumer_snapshot()["accounts"]))
        later_candidate(f,cf.key(27),label,at+21)
        latest=later_wallet(f,newfixture,ss.observation.anchor,at+25)
        denied=admit(f,"next",at=at+25,support=latest)
        check(label+"_fresh_source_eligibility",denied.eligibility.decision.disposition=="ELIGIBLE_CONTEXT_ONLY")
        if failed:
            costs=next(item for item in denied.risk.root_encumbrances if item.root_id==action.root_id)
            check("failed_SELL_consumes_shared_failure_pool_once",costs.failed_attempt_count==1 and costs.remaining_failed_attempt_count==1
                and costs.paid_failed_network_lamports==sf.FEE and costs.failure_budget_remaining_lamports==20000-sf.FEE)
            check("failed_SELL_preserves_full_future_exit_headroom",costs.protective_lamports==6010000
                and costs.paid_reduction_network_lamports==sf.FEE and costs.principal_lamports==0)
        else:
            check("retired_retained_WSOL_is_not_new_entry_native",not denied.accepted and "ENTRY_REQUIRES_ABSENT_WSOL_ACCOUNT" in denied.risk.reasons
                and denied.risk.known_wsol_units>0 and denied.risk.outstanding_native_lamports==0
                and denied.risk.native_lamports==newfixture.scenario.tx["meta"]["postBalances"][0])
        f.reopen();check(label+"_full_original_risk_replay",f.repo.authority_admission_receipt("next")==denied);f.close()
    f=Fixture(directory,"max-u64")
    f.repo.close();f.path=directory/"max-u64-actual.sqlite3"
    f.domain=replace(f.domain,known_native_wallet_lamports=(1<<64)-1)
    f.repo=LedgerRepository.initialize(f.path,f.domain)
    value=Scenario();value.accounts[WALLET]["lamports"]=(1<<64)-1
    ingest(f.repo,observe(value));f.root=f.repo.receive_candidate(f.item,fence=f.repo.write_fence())
    control(f.repo,"INSTALL_POLICY","install",policy_value=f.policy);arm(f.repo,f.policy)
    value=Scenario();value.accounts[WALLET]["lamports"]=(1<<64)-1
    maximum=admit(f,"maximum",support=wallet(f,scenario=value))
    check("exact_u64_above_signed64_native_funding",maximum.accepted and maximum.risk.native_lamports==(1<<64)-1)
    f.reopen();check("exact_u64_roundtrip_without_float_or_SQL_loss",f.repo.authority_admission_receipt("maximum")==maximum);f.close()
    f=Fixture(directory,"recover-size")
    install(f,"deny",size={"fixed_quote_lamports":0})
    denied=admit(f,"zero")
    binding=f.repo.authority_entry_binding(f.root)
    install(f,"recover",size={"fixed_quote_lamports":1000000})
    recovered=admit(f,"recovered",at=NOW+5)
    check("denied_size_can_recover_before_original_deadline",not denied.accepted and recovered.accepted
        and f.repo.authority_entry_binding(f.root)==binding)
    check("accepted_amount_frozen_under_changed_policy",f.repo.reservation(f.root).admission.action.input_units==1000000)
    f.reopen();f.close()


def failure_boundaries(directory):
    for budget,accepted in ((39999,False),(40000,True)):
        f=Fixture(directory,"failure-bound-"+str(budget))
        install(f,"bound",costs={"protective_network_fee_lamports":20000,"failed_attempt_network_budget_lamports":budget})
        receipt=admit(f,"bound")
        check(str(budget)+"_failure_budget_uses_larger_entry_exit_cap",receipt.accepted is accepted)
        if accepted:
            check("exact_failure_boundary_reserved_once",receipt.risk.reservation.network_fee_lamports==50000
                and receipt.risk.required_native_lamports==13070000)
        else:
            check("underfunded_failed_exit_count_denied", "EXPLICIT_COST_OR_PROTECTIVE_FAILURE_BUDGET_UNPROVEN" in receipt.risk.reasons)
        f.reopen();check(str(budget)+"_original_failure_boundary_replay",f.repo.authority_admission_receipt("bound")==receipt);f.close()


def fault_cases(directory):
    for index,(prefix,after) in enumerate((
        ("INSERT INTO ledger_custody_inputs",False),("INSERT INTO ledger_wallet_comparisons",True),
        ("INSERT INTO ledger_pending_actions",True),("INSERT INTO ledger_consumer_groups",True),
        ("INSERT INTO ledger_authority_records",True),("INSERT INTO ledger_authority_bindings",True),
        ("INSERT INTO ledger_authority_admissions",True),("INSERT INTO ledger_commits",False),
        ("COMMIT",False),("COMMIT",True))):
        f=Fixture(directory,"cut-"+str(index),arm_now=False)
        arm(f.repo,f.policy,scope="ENTRY_ONCE",root=f.root)
        sample,support=clock(f.repo),wallet(f)
        prior=f.repo.write_fence().revision
        connection=f.repo._conn
        f.repo._conn=a1.ConnectionCut(connection,prefix,after)
        check("cut-"+str(index)+"_interruption",raises(lambda:admit(f,"atomic",sample=sample,support=support),LedgerJournalError))
        f.repo._conn=connection
        committed=prefix=="COMMIT" and after
        if committed:check("lost_ack_never_trusts_partial_cache",raises(lambda:f.repo.consumer_snapshot()))
        f.reopen()
        check("cut-"+str(index)+"_atomic_all_or_zero",f.repo.write_fence().revision==prior+int(committed)
            and (f.repo.authority_admission_receipt("atomic") is not None)==committed
            and f.repo.audit()["action_count"]==int(committed)
            and (f.repo.authority_grant_status("grant1")["consumed_root"] is not None)==committed)
        retried=admit(f,"atomic",sample=sample,support=support)
        check("cut-"+str(index)+"_retry_once",retried.accepted and f.repo.write_fence().revision==prior+1)
        f.close()
    for attribute in ("_custody","_authority"):
        f=Fixture(directory,"publish-"+attribute)
        class PublicationCut(LedgerRepository):
            def __setattr__(self,name,value):
                if name==attribute:raise a1.Interrupted()
                super().__setattr__(name,value)
        f.repo.__class__=PublicationCut
        check(attribute+"_publication_loss_reports",raises(lambda:admit(f,"publication"),LedgerJournalError))
        f.repo.__class__=LedgerRepository
        check(attribute+"_no_trusted_mixed_cache",raises(lambda:f.repo.authority_snapshot()))
        f.reopen();check(attribute+"_original_full_group_rebuilt",f.repo.authority_admission_receipt("publication").accepted);f.close()
    f=Fixture(directory,"outside")
    fence=f.repo.write_fence();f.evaluate("clock")
    check("stale_common_cut_cannot_admit",raises(lambda:f.repo.admit_authority_entry(EntryRequest(f.root,"PUMP",TOKEN_PROGRAM_ID,"FINAL-A"),
        clock(f.repo,NOW+5),f.source,wallet(f,at=NOW+5),command_id="stale",fence=fence)))
    with closing(sqlite3.connect(f.path)) as conn,conn:conn.execute(f"PRAGMA user_version={STORAGE_VERSION}")
    check("outside_structurally_valid_commit_invalidates_reads",raises(lambda:f.repo.authority_acceptance(f.root)))
    check("outside_structurally_valid_commit_invalidates_admission",raises(lambda:admit(f,"outside")))
    f.reopen();check("reopen_reverifies_unchanged_journal",f.repo.authority_acceptance(f.root) is None)
    fence=f.repo.write_fence();sample=clock(f.repo,NOW+5);support=wallet(f,at=NOW+5);f.reopen()
    check("stale_generation_denied",raises(lambda:f.repo.admit_authority_entry(EntryRequest(f.root,"PUMP",TOKEN_PROGRAM_ID,"FINAL-A"),
        sample,f.source,support,command_id="generation",fence=fence)))
    check("competing_journal_writer_excluded",raises(lambda:LedgerRepository.reopen(f.path,f.domain)))
    f.close()
    for kind in ("v6","projection","parent-risk","child-link","consumption"):
        f=Fixture(directory,"tamper-"+kind,arm_now=False)
        arm(f.repo,f.policy,scope="ENTRY_ONCE",root=f.root)
        original=admit(f,"original");f.repo.close()
        with closing(sqlite3.connect(f.path)) as conn,conn:
            if kind=="v6":conn.execute("PRAGMA user_version=6")
            elif kind=="projection":conn.execute("UPDATE ledger_authority_projection SET content_digest=?",(digest("tampered"),))
            elif kind=="child-link":
                conn.execute("DROP TRIGGER ledger_wallet_comparisons_no_update")
                conn.execute("UPDATE ledger_wallet_comparisons SET ingestion_key='unlinked'")
                conn.execute(_DDL["ledger_wallet_comparisons_no_update"])
            elif kind=="consumption":
                conn.execute("DROP TRIGGER ledger_authority_admissions_no_update")
                conn.execute("UPDATE ledger_authority_admissions SET consumed_grant_id=NULL")
                conn.execute(_DDL["ledger_authority_admissions_no_update"])
            else:
                for table in ("ledger_authority_admissions","ledger_commits"):
                    conn.execute("DROP TRIGGER "+table+"_no_update")
                record=asdict(original);record["risk"]["available_native_lamports"]+=1
                new_digest=content_fingerprint(record)
                conn.execute("UPDATE ledger_authority_admissions SET payload_json=?,receipt_digest=?",(canonical_json(record),new_digest))
                commit=json.loads(conn.execute("SELECT payload_json FROM ledger_commits WHERE seq=?",(original.sequence,)).fetchone()[0])
                commit["record_digest"]=new_digest;commit_digest=content_fingerprint(commit)
                conn.execute("UPDATE ledger_commits SET payload_json=?,record_digest=?,content_digest=? WHERE seq=?",
                    (canonical_json(commit),new_digest,commit_digest,original.sequence))
                conn.execute("UPDATE ledger_head SET last_receipt_digest=?",(commit_digest,))
                for table in ("ledger_authority_admissions","ledger_commits"):conn.execute(_DDL[table+"_no_update"])
        check(kind+"_tamper_reopen_denied",raises(lambda:LedgerRepository.reopen(f.path,f.domain)))
        f.close()


def abrupt_processes(directory):
    results={}
    for cut in ("before","after"):
        f=Fixture(directory,"process-"+cut,arm_now=False)
        arm(f.repo,f.policy,scope="ENTRY_ONCE",root=f.root)
        before=f.repo.write_fence().revision
        f.close()
        source_path=directory/(f.name+"-evidence.sqlite3")
        result=subprocess.run([sys.executable,"-B",str(Path(__file__).resolve()),"--crash",str(f.path),str(source_path),f.root,cut],
            cwd=ROOT,capture_output=True,text=True,timeout=30)
        check(cut+"_actual_process_commit_cut",result.returncode==(81 if cut=="before" else 82))
        f.repo=LedgerRepository.reopen(f.path,f.domain)
        f.source=SourceEvidenceStore(source_path,f.item.source_binding,f.source.profile)
        receipt=f.repo.authority_admission_receipt("process")
        check(cut+"_abrupt_all_or_zero",(receipt is not None)==(cut=="after") and f.repo.audit()["action_count"]==int(cut=="after"))
        retried=admit(f,"process",sample=clock(f.repo) if receipt is None else receipt.eligibility.original.clock,
            support=wallet(f) if receipt is None else f.repo.wallet_support(receipt.wallet_support_digest))
        check(cut+"_restart_exactly_once_consumption",retried.accepted and f.repo.write_fence().revision==before+1
            and f.repo.authority_grant_status("grant1")["consumed_root"]==f.root)
        results[cut]=retried.content_digest;f.close()
    return results


def crash_child():
    path,source_path,root,cut=sys.argv[2:]
    repo=LedgerRepository.reopen(Path(path),domain())
    item=repo.candidate(root)
    source=SourceEvidenceStore(Path(source_path),item.source_binding,a1.SourceProfile())
    f=SimpleNamespace(repo=repo,domain=repo.domain,item=item,root=root,source=source,policy=repo.authority_snapshot()["policy"])
    support,sample=wallet(f),clock(repo)
    connection=repo._conn
    class ExitConnection:
        def __getattr__(self,name):return getattr(connection,name)
        def execute(self,sql,*args):
            if sql=="COMMIT" and cut=="before":os._exit(81)
            value=connection.execute(sql,*args)
            if sql=="COMMIT" and cut=="after":os._exit(82)
            return value
    repo._conn=ExitConnection()
    admit(f,"process",sample=sample,support=support)
    raise AssertionError("crash cut not reached")


def main():
    with tempfile.TemporaryDirectory(prefix="authority-a3-") as tmp:
        try:
            result=basic(Path(tmp))
            denied_cases(Path(tmp))
            staging_and_source(Path(tmp))
            economic=economic_cases(Path(tmp))
            lineage_cases(Path(tmp))
            reduction_resource_cases(Path(tmp))
            failure_boundaries(Path(tmp))
            fault_cases(Path(tmp))
            crashes=abrupt_processes(Path(tmp))
        finally:
            for fixture in a1.FIXTURES: fixture.close()
    print(json.dumps({"checks":len(CHECKS),"checks_digest":content_fingerprint(CHECKS),"admission_digest":result,"economic_digests":economic,"crash_digests":crashes},sort_keys=True))

if __name__=="__main__":
    try:
        if len(sys.argv)>1 and sys.argv[1]=="--crash":crash_child()
        else:main()
    finally:
        for fixture in a1.FIXTURES: fixture.close()
