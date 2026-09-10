"""Q3 actual Q1/Q2/A4b/Ledger with ephemeral keys and fake HTTP only."""
from __future__ import annotations

import base64
import copy
import json
import os
import pickle
import sqlite3
import subprocess
import sys
import tempfile
from contextlib import contextmanager
from dataclasses import asdict, replace
from pathlib import Path
from unittest.mock import patch

import httpx

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'src'))
import live_execution_signer_selftest_v0_1 as q2
import live.execution_send_v0_1 as send
from live.authority_message_control_v0_1 import FreshStageConsumption
from live.ledger_actions_v0_1 import AttemptStageInput, stage_from_record
from phase5.shadow_domain_v0_1 import canonical_json, content_fingerprint

q1, a3, control, NOW = q2.q1, q2.a3, q2.control, q2.NOW
CHECKS = {}


def check(name, value):
    CHECKS[name] = bool(value)
    if not value:
        raise AssertionError(name)


def fails(call):
    try:
        call()
    except (ValueError, TypeError, RuntimeError, AttributeError):
        return True
    return False


class Boundary:
    def __init__(self, f, outcome='ACKNOWLEDGED', callback=None):
        self.f, self.outcome, self.callback, self.requests = f, outcome, callback, []

    def __call__(self, request):
        original = json.loads(request.content)
        self.requests.append(original)
        wire = base64.b64decode(original['params'][0])
        from solders.transaction import VersionedTransaction
        transaction = VersionedTransaction.from_bytes(wire)
        signature = str(transaction.signatures[0])
        attempt_id = self.f.repo.mutation_lane()['active_attempt_id']
        persisted = send.load_signed_envelope(self.f.repo, attempt_id)
        stages = self.f.repo.attempt_stage_inputs(attempt_id)
        assert self.f.repo.attempt(attempt_id).recorded_stage == 'UNKNOWN'
        assert any(s.external_reference == send._CLAIM for s in stages)
        assert persisted.signed_wire_base64 == original['params'][0] and all(transaction.verify_with_results())
        assert original['method'] == 'sendTransaction' and original['params'][1] == {
            'encoding':'base64', 'skipPreflight':False, 'preflightCommitment':'confirmed',
            'maxRetries':0, 'minContextSlot':persisted.preparation.lease.context_slot}
        if self.callback:
            self.callback()
        value = {'jsonrpc':'2.0', 'id':original['id'], 'result':signature}
        if self.outcome == 'NULL_RESPONSE': value['result'] = None
        elif self.outcome == 'SIGNATURE_MISMATCH': value['result'] = q2.sf.b58(b'\x63'*64)
        elif self.outcome == 'RPC_ERROR':
            value.pop('result'); value['error'] = {'message':'PRIVATE_PROVIDER_BODY', 'data':'PRIVATE_HEADER'}
        elif self.outcome == 'HTTP_FAILURE': return httpx.Response(503, text='PRIVATE_ERROR')
        elif self.outcome == 'TRANSPORT_UNRESOLVED': raise httpx.ReadTimeout('PRIVATE_ENDPOINT_AND_HEADER')
        elif self.outcome == 'INVALID_RESPONSE': value['id'] = True
        elif self.outcome == 'RESPONSE_LIMIT': return httpx.Response(200, content=b'x'*4097, headers={'content-type':'application/json'})
        return httpx.Response(200, json=value)


def clocks(f, at):
    calls = []
    def sample():
        value = a3.clock(f.repo, at)
        calls.append(value)
        return replace(value, monotonic_ns=value.monotonic_ns+len(calls))
    return sample


@contextmanager
def fixture(directory, name, *, side='BUY', route='pump', outcome='ACKNOWLEDGED', callback=None):
    with q2.fixture(directory, name, side=side, route=route) as (f, key, production, delivery, at):
        envelope = q2.invoke(f, key, production, delivery, at)
        persisted = send.persist_signed_envelope(f.repo, envelope, fence=f.repo.write_fence())
        check(name+'_verified_persistence', persisted == envelope)
        fresh = control.consume(f, production.original,
            control.request(f, envelope.preparation, 'send', 'SEND'), at=at+2)
        check(name+'_actual_SEND_delivery', type(fresh) is FreshStageConsumption)
        boundary = Boundary(f, outcome, callback)
        transport = send.SolanaSendTransport('https://private.invalid/PRIVATE_ENDPOINT?token=PRIVATE_TOKEN', a3.PROFILE,
            transport=httpx.MockTransport(boundary))
        try:
            yield f, production, envelope, fresh, boundary, transport, at+2
        finally:
            transport.close()


def invoke(f, fresh, transport, at, **kwargs):
    return send.send_exact(f.repo, fresh, transport, source_store=kwargs.pop('source_store',
        f.source if fresh.receipt.original.validation.context.action.side == 'BUY' else None),
        clock=kwargs.pop('clock', clocks(f, at+1)), **kwargs)


def healthy(directory):
    for side in ('BUY','SELL'):
        for route in ('pump','swap'):
            name = side+'_'+route
            with fixture(directory, name, side=side, route=route) as (f, production, envelope, fresh, boundary, transport, at):
                observation = invoke(f, fresh, transport, at)
                check(name+'_one_exact_call_ACK_unknown', len(boundary.requests)==1 and observation.outcome=='ACKNOWLEDGED'
                    and observation.finality=='UNKNOWN' and not observation.may_retry)
                check(name+'_durable_observation', send.load_send_observations(f.repo, envelope.preparation.attempt_id)==(observation,))
                check(name+'_ACK_no_settlement', f.repo.attempt(envelope.preparation.attempt_id).recorded_stage=='UNKNOWN'
                    and f.repo.attempt(envelope.preparation.attempt_id).lane_held
                    and not f.repo.attempt(envelope.preparation.attempt_id).economically_applied)
                check(name+'_immutable_sanitized', fails(lambda:setattr(observation,'outcome','changed'))
                    and 'PRIVATE_' not in repr(observation) and 'PRIVATE_ENDPOINT' not in repr(transport))
                for old in (fresh, copy.copy(fresh), fresh.receipt):
                    check(name+'_replay_'+str(len(CHECKS)), fails(lambda:invoke(f,old,transport,at)))
                check(name+'_replay_no_request', len(boundary.requests)==1)
                current = f.repo.write_fence()
                check(name+'_envelope_retry_readonly', send.persist_signed_envelope(f.repo,envelope,fence=current)==envelope
                    and f.repo.write_fence()==current)
                f.reopen()
                check(name+'_restart_reconstructs', send.load_signed_envelope(f.repo,envelope.preparation.attempt_id)==envelope
                    and send.load_send_observations(f.repo,envelope.preparation.attempt_id)==(observation,))
                repeated = control.consume(f,production.original,
                    control.request(f,envelope.preparation,'rebroadcast','REBROADCAST',1),at=at+1)
                check(name+'_fresh_rebroadcast', type(repeated) is FreshStageConsumption)
                second = invoke(f,repeated,transport,at+1)
                check(name+'_same_wire_signature_next_ordinal', len(boundary.requests)==2
                    and boundary.requests[0]['params']==boundary.requests[1]['params']
                    and second.primary_signature==observation.primary_signature and second.rebroadcast_ordinal==1)
                check(name+'_two_durable_observations',send.load_send_observations(f.repo,envelope.preparation.attempt_id)==(observation,second))
                f.repo.audit()


def transport_failures(directory):
    for outcome in sorted(send._OUTCOMES-{'ACKNOWLEDGED','TIME_LIMIT'}):
        with fixture(directory,'transport-'+outcome,outcome=outcome) as (f,production,envelope,fresh,boundary,transport,at):
            observed = invoke(f,fresh,transport,at)
            check(outcome+'_immutable_unknown', observed.outcome==outcome and observed.finality=='UNKNOWN' and observed.possible_send)
            check(outcome+'_one_call_no_retry', len(boundary.requests)==1 and fails(lambda:invoke(f,fresh,transport,at)))
            check(outcome+'_durable_restart', send.load_send_observations(f.repo,envelope.preparation.attempt_id)==(observed,))
            check(outcome+'_no_provider_details', 'PRIVATE' not in canonical_json(asdict(observed)))
            check(outcome+'_replacement_blocked', fails(lambda:q1.prepare_exact_message(f.repo,production,fence=f.repo.write_fence(),ordinal=2)))
            f.repo.audit()


def guards(directory):
    labels = ('hard-stop','entry-stop','revoke','reopen','source-gap','source-copy','old-clock','clock-error',
        'final-deadline','clock-repeat','ledger-unlock','source-unlock','reentrant','caller-transaction',
        'caller-source-transaction','source-after-claim','control-after-claim','lease-after-claim','profile-mismatch')
    for label in labels:
        with fixture(directory,'guard-'+label) as (f,production,envelope,fresh,boundary,transport,at):
            current_clock, source = clocks(f,at+1), f.source
            external = None
            if label in ('hard-stop','entry-stop','revoke'):
                a3.control(f.repo,{'hard-stop':'HARD_STOP','entry-stop':'STOP_ENTRY','revoke':'REVOKE_GRANT'}[label],label,at=at,
                    **({'target': f.repo.authority_acceptance(f.root).eligibility.decision.grant_id} if label=='revoke' else {}))
            elif label=='reopen': f.reopen()
            elif label=='source-gap': source_gap(f)
            elif label=='source-copy':
                external=q2.SourceEvidenceStore(directory/'guard-source-clone.sqlite3',f.source.binding,f.source.profile)
                external.append(f.source.latest_record()[1],expected_previous_digest='0'*64)
                source=external
            elif label=='old-clock': current_clock=lambda:fresh.receipt.original.validation.clock
            elif label=='clock-error':
                def current_clock(): raise RuntimeError('PRIVATE_CALLBACK_BODY')
            elif label in ('final-deadline','clock-repeat','lease-after-claim'):
                counter=[]
                def current_clock():
                    counter.append(True)
                    when=at+1 if len(counter)==1 or label=='clock-repeat' else NOW+15 if label=='final-deadline' else NOW+50
                    return a3.clock(f.repo,when)
            elif label in ('ledger-unlock','source-unlock'):
                def current_clock():
                    (f.repo if label=='ledger-unlock' else f.source)._conn.execute('ROLLBACK')
                    return a3.clock(f.repo,at+1)
            elif label=='reentrant':
                def current_clock():
                    invoke(f,fresh,transport,at)
                    return a3.clock(f.repo,at+1)
            elif label=='caller-transaction': f.repo._conn.execute('BEGIN')
            elif label=='caller-source-transaction': f.source._conn.execute('BEGIN')
            elif label=='profile-mismatch': transport.profile=replace(a3.PROFILE,provider_version='different')
            original_record=send._record
            fired=[]
            def record(*args):
                result=original_record(*args)
                if args[1].external_reference==send._CLAIM and not fired:
                    fired.append(True)
                    if label=='source-after-claim':
                        f.source._conn.execute('ROLLBACK')
                        source_gap(f)
                    elif label=='control-after-claim':
                        a3.control(f.repo,'HARD_STOP','post-claim-stop',at=at+1)
                return result
            try:
                with patch.object(send,'_record',record):
                    try: invoke(f,fresh,transport,at,clock=current_clock,source_store=source)
                    except (send.ExecutionSendError,q2.signer.ExecutionSigningError) as exc:
                        check(label+'_fixed_public_reason','PRIVATE' not in str(exc))
                    else: raise AssertionError(label)
                check(label+'_no_HTTP',not boundary.requests)
                if label=='caller-transaction':check(label+'_retains_caller_transaction',f.repo._conn.in_transaction)
                if label=='caller-source-transaction':check(label+'_retains_caller_transaction',f.source._conn.in_transaction)
            finally:
                if f.repo._conn.in_transaction:f.repo._conn.execute('ROLLBACK')
                if f.source._conn.in_transaction:f.source._conn.execute('ROLLBACK')
                if external is not None:external.close()
            check(label+'_spent_delivery',fails(lambda:invoke(f,fresh,transport,at)))
            check(label+'_held_lane',f.repo.attempt(envelope.preparation.attempt_id).lane_held)
            claimed=any(s.external_reference==send._CLAIM for s in f.repo.attempt_stage_inputs(envelope.preparation.attempt_id))
            check(label+'_claim_cut',claimed == (label in ('final-deadline','clock-repeat','lease-after-claim','source-after-claim','control-after-claim')))
            f.repo.audit()


def source_gap(f):
    previous=f.source.latest_record()[1]
    a3.a1.source_fixture.change(f.raw,'DELETE FROM websocket_observations')
    gap=a3.a1.source_fixture.observe(f.raw,f.source.binding,previous=previous,now=8,cut=5)
    f.source.append(gap,expected_previous_digest=previous.content_digest)


def crash_cuts(directory):
    for cut in ('before-claim','after-claim','after-unknown','after-HTTP','after-observation'):
        with fixture(directory,'crash-'+cut) as (f,production,envelope,fresh,boundary,transport,at):
            original_record=send._record
            def record(repo,stage,*args):
                reference=stage.external_reference
                if cut=='before-claim' and reference==send._CLAIM or cut=='after-HTTP' and reference==send._OBSERVATION:
                    raise SystemExit(77)
                value=original_record(repo,stage,*args)
                if (cut=='after-claim' and reference==send._CLAIM or cut=='after-unknown' and reference==send._UNCERTAIN
                        or cut=='after-observation' and reference==send._OBSERVATION):
                    raise SystemExit(77)
                return value
            try:
                with patch.object(send,'_record',record):invoke(f,fresh,transport,at)
            except SystemExit as exc:check(cut+'_synthetic_process_cut',exc.code==77)
            else:raise AssertionError(cut)
            f.reopen()
            check(cut+'_restart_exact_envelope',send.load_signed_envelope(f.repo,envelope.preparation.attempt_id)==envelope)
            attempt=f.repo.attempt(envelope.preparation.attempt_id)
            stages=f.repo.attempt_stage_inputs(envelope.preparation.attempt_id)
            check(cut+'_claim_durable',any(s.external_reference==send._CLAIM for s in stages)==(cut!='before-claim'))
            check(cut+'_claim_never_releases',attempt.lane_held and attempt.recorded_stage==
                ('SIGNED_DURABLE' if cut=='before-claim' else 'SEND_CLAIMED' if cut=='after-claim' else 'UNKNOWN'))
            check(cut+'_external_call_cut',len(boundary.requests)==(1 if cut in ('after-HTTP','after-observation') else 0))
            check(cut+'_observation_cut',len(send.load_send_observations(f.repo,envelope.preparation.attempt_id))==(1 if cut=='after-observation' else 0))
            check(cut+'_historical_not_permission',fails(lambda:invoke(f,f.repo.authority_message_receipt('send'),transport,at)))
            f.repo.audit()
    for target in ('EXACT_SIMULATED','AUTHORIZED','SIGNED_DURABLE'):
        with q2.fixture(directory,'persist-cut-'+target) as (f,key,production,delivery,at):
            envelope=q2.invoke(f,key,production,delivery,at)
            before=f.repo.write_fence()
            check(target+'_malformed_original_SIGN_commit_before_write',fails(lambda:send.persist_signed_envelope(f.repo,
                replace(envelope,consumed_common_digest='0'*64),fence=before)) and f.repo.write_fence()==before)
            original_record=send._record
            def record(repo,stage,*args):
                value=original_record(repo,stage,*args)
                if stage.target_stage==target:raise SystemExit(77)
                return value
            try:
                with patch.object(send,'_record',record):send.persist_signed_envelope(f.repo,envelope,fence=f.repo.write_fence())
            except SystemExit:pass
            else:raise AssertionError(target)
            f.reopen()
            check(target+'_persistence_resumes_exact',send.persist_signed_envelope(f.repo,envelope,fence=f.repo.write_fence())==envelope)
            check(target+'_no_send_claim',not any(s.external_reference==send._CLAIM for s in f.repo.attempt_stage_inputs(envelope.preparation.attempt_id)))
            f.repo.audit()


def journal_and_tickets(directory):
    with fixture(directory,'journal-ticket') as (f,production,envelope,fresh,boundary,transport,at):
        plain=AttemptStageInput(envelope.preparation.attempt_id,'UNKNOWN',envelope.preparation.action_content_digest,
            envelope.preparation.message_sha256,envelope.preparation.lease.fingerprint,envelope.preparation.message_policy_digest,
            a3.utc(at),'EXTERNAL_LEGACY',content_fingerprint('legacy'))
        legacy=asdict(plain);legacy.pop('external_record_json')
        check('legacy_optional_field_omitted',plain.to_record()==legacy and stage_from_record(legacy)==plain)
        for label,payload,digest in (('wrong-digest','{}','0'*64),('noncanonical','{ "x":1}',content_fingerprint({'x':1})),
                ('oversize',canonical_json({'x':'a'*16384}),content_fingerprint({'x':'a'*16384}))):
            check('journal_'+label,fails(lambda:replace(plain,external_record_json=payload,external_record_digest=digest)))
        tickets=[]
        original=send.SolanaSendTransport._send_claimed
        def retain(self,ticket):
            tickets.append(ticket)
            return original(self,ticket)
        locks=[]
        def inspect_locks():
            for path in (f.path,Path(f.source._conn.execute('PRAGMA database_list').fetchone()[2])):
                conn=sqlite3.connect(path,timeout=0,isolation_level=None)
                try:
                    try:conn.execute('BEGIN IMMEDIATE')
                    except sqlite3.OperationalError:locks.append(True)
                    else:locks.append(False);conn.execute('ROLLBACK')
                finally:conn.close()
        boundary.callback=inspect_locks
        with patch.object(send.SolanaSendTransport,'_send_claimed',retain):observation=invoke(f,fresh,transport,at)
        check('observation_boolean_ordinal_rejected',fails(lambda:replace(observation,rebroadcast_ordinal=False)))
        check('HTTP_both_current_journals_fenced',locks==[True,True])
        ticket=tickets[0]
        check('ticket_copy_same',copy.copy(ticket) is ticket and copy.deepcopy(ticket) is ticket)
        check('ticket_no_pickle_or_mutation',fails(lambda:pickle.dumps(ticket)) and fails(lambda:setattr(ticket,'digest','changed')))
        check('ticket_replay_no_HTTP',fails(lambda:transport._send_claimed(ticket)) and len(boundary.requests)==1)
        check('transport_no_arbitrary_wire',fails(lambda:transport._send_claimed(envelope)))
        check('transport_closed_public_API',[n for n in dir(transport) if not n.startswith('_')]==['close','profile'])
        check('transaction_locks_released',not f.repo._conn.in_transaction and not f.source._conn.in_transaction)
        f.repo.audit()


def nonlanding(f,envelope,*,partial=False,at=NOW+80,key='proof'):
    """Actual bounded coverage adapter -> actual L3; no injected decision."""
    import live_ledger_finality_selftest_v0_1 as lf
    from live.transaction_coverage_v0_1 import CoverageRequest
    prep=envelope.preparation
    lower=prep.finalized_lower_anchor
    scenario=lf.scenario(absent=True)
    scenario.tx=scenario.status=None
    scenario.blocks={lower.slot:{'blockhash':lower.blockhash,'previousBlockhash':lower.previous_blockhash,
        'parentSlot':lower.parent_slot,'blockHeight':lower.block_height,'blockTime':lower.block_time,'signatures':[]}}
    last_slot,last_hash=lower.slot,lower.blockhash
    final_height=prep.lease.last_valid_block_height+1
    count=final_height-lower.block_height
    for offset,height in enumerate(range(lower.block_height+1,final_height+1),1):
        slot=lower.slot+offset
        scenario.blocks[slot]={'blockhash':lf.bh(slot),'previousBlockhash':last_hash,'parentSlot':last_slot,
            'blockHeight':height,'blockTime':NOW+28+offset*50//count,'signatures':[]}
        last_slot,last_hash=slot,lf.bh(slot)
    scenario.initial_slot=scenario.root_slot=last_slot
    request=CoverageRequest(f.domain.genesis_hash,envelope.primary_signature,prep.lease.blockhash,
        prep.lease.last_valid_block_height,lower,last_slot-3 if partial else None)
    observed=lf.coverage_observation(scenario,at=at,request=request)
    receipt=f.repo.ingest_chain_observation(prep.attempt_id,observed,ingestion_key=key,
        evaluated_at_utc=a3.utc(at),fence=f.repo.write_fence())
    return receipt, observed.root


def replacement_production(f,production,root,*,at=NOW+85):
    """Fresh deterministic original public reads; unchanged independently held wallet facts."""
    from live_wallet_evidence_selftest_v0_1 import Scenario
    action=production.original.context.action
    prior=production.original.evidence.wallet.observation
    scenario=Scenario()
    scenario.accounts={}
    for batch in (prior.explicit_read,*prior.mint_reads):
        for original in batch.accounts:
            if original is not None:
                raw=original.account
                scenario.accounts[raw.pubkey]=a3.account(raw.owner,raw.data,original.lamports,
                    executable=original.executable,rentEpoch=original.rent_epoch)
    for inventory in prior.inventories:
        scenario.inventory[inventory.program]=list(dict.fromkeys(x.account.pubkey for x in inventory.accounts))
    slot=root.slot+1
    scenario.context=scenario.multiple_context=scenario.initial_slot=scenario.upper_slot=slot
    def transform(request,response):
        if request['method']=='getBlock':response['result'].update(blockhash=q2.sf.bh(slot),previousBlockhash=root.blockhash,
            parentSlot=root.slot,blockHeight=root.block_height+1,blockTime=at-5)
        return response
    scenario.transform=transform
    context=q1.capture_message_context(f.repo,action.action_id)
    intent=q1._intent(context,at*1000000)
    seed,_=q2.a4.evidence(f,action,scenario,at=at,context_slot=slot,intent=intent,wallet_floor=slot,
        recent_blockhash=q2.sf.bh(230),last_valid_height=240,validity_height=root.block_height+1)
    class CurrentBoundary(q1.PublicTransport):
        def __call__(self,request):
            response=super().__call__(request)
            value=response.json();method=json.loads(request.content)['method']
            if method=='getLatestBlockhash':value['result']['value']['lastValidBlockHeight']=240
            if method=='getBlockHeight':value['result']=root.block_height+1
            return httpx.Response(200,json=value)
    boundary=CurrentBoundary(seed)
    with q1.ExecutionReadOnlyRpc('https://fake.invalid/',a3.PROFILE,now_us=boundary.now,
            transport=httpx.MockTransport(boundary)) as rpc:
        return q1.produce_exact_message(f.repo,action.action_id,rpc,seed.evidence.wallet,seed.evidence.quote_policy,
            seed.evidence.plan_policy,q1.ComputeBudget(250000,1000),clock=lambda:seed.clock,
            now_us=lambda:seed.evidence.simulation.leases[0].observed_at_us)


def replacement_boundary(directory):
    for side in ('BUY','SELL'):
        with q2.fixture(directory,'replacement-'+side,side=side) as (f,key,production,sign_delivery,at):
            envelope=q2.invoke(f,key,production,sign_delivery,at)
            send.persist_signed_envelope(f.repo,envelope,fence=f.repo.write_fence())
            fresh=control.consume(f,production.original,control.request(f,envelope.preparation,'send','SEND'),at=at+2)
            boundary=Boundary(f,'TRANSPORT_UNRESOLVED')
            transport=send.SolanaSendTransport('https://fake.invalid/',a3.PROFILE,transport=httpx.MockTransport(boundary))
            try:
                invoke(f,fresh,transport,at+2)
                partial,root=nonlanding(f,envelope,partial=True,key='partial')
                check(side+'_tail_missing_is_not_nonlanding',partial.decision.resulting_state.positive_finality is None)
                refused=f.repo.apply_settlement(envelope.preparation.attempt_id,chain_receipt_key='partial',support=None,
                    ingestion_key='refused',recorded_at_utc=a3.utc(NOW+80),fence=f.repo.write_fence())
                check(side+'_insufficient_proof_holds_lane',refused.decision.disposition=='UNAPPLIED'
                    and f.repo.attempt(envelope.preparation.attempt_id).lane_held)
                if side=='SELL':
                    next_production=replacement_production(f,production,root)
                    check('SELL_new_message_valid_before_permission',next_production.validation.disposition=='SUPPORTED_CONTEXT_ONLY'
                        and next_production.message_hex!=production.message_hex)
                    check('SELL_no_replacement_while_original_may_land',fails(lambda:q1.prepare_exact_message(f.repo,next_production,
                        fence=f.repo.write_fence(),ordinal=2)))
                proof,root=nonlanding(f,envelope,at=NOW+81,key='complete')
                check(side+'_actual_authoritative_nonlanding',proof.decision.resulting_state.positive_finality=='PROVEN_NON_LANDED'
                    and f.repo.attempt(envelope.preparation.attempt_id).lane_held)
                if side=='SELL':
                    next_production=replacement_production(f,production,root)
                    check('SELL_proof_without_Ledger_resolution_still_held',fails(lambda:q1.prepare_exact_message(f.repo,next_production,
                        fence=f.repo.write_fence(),ordinal=2)))
                resolved=f.repo.apply_settlement(envelope.preparation.attempt_id,chain_receipt_key='complete',support=None,
                    ingestion_key='resolved',recorded_at_utc=a3.utc(NOW+82),fence=f.repo.write_fence())
                check(side+'_actual_resolved_no_retry_grant',resolved.decision.disposition=='PROVEN_NON_LANDED_RESOLVED'
                    and not resolved.decision.may_retry and not f.repo.attempt(envelope.preparation.attempt_id).lane_held)
                if side=='BUY':
                    expired=replacement_production(f,production,root)
                    check('BUY_original_deadline_still_blocks_after_proof',expired.validation.disposition!='SUPPORTED_CONTEXT_ONLY'
                        and fails(lambda:q1.prepare_exact_message(f.repo,expired,fence=f.repo.write_fence(),ordinal=2)))
                else:
                    next_production=replacement_production(f,production,root)
                    prepared=q1.prepare_exact_message(f.repo,next_production,fence=f.repo.write_fence(),ordinal=2)
                    check('SELL_replacement_unchanged_action_obligation_actual_units',prepared.preparation.ordinal==2
                        and next_production.original.context.action==production.original.context.action
                        and prepared.preparation.action_content_digest==envelope.preparation.action_content_digest)
                    check('SELL_old_SIGN_is_not_new_permission',fails(lambda:q2.invoke(f,key,next_production,sign_delivery,NOW+85)))
                    new_sign=control.consume(f,next_production.original,control.request(f,prepared.preparation,'replacement-sign'),at=NOW+85)
                    check('SELL_new_actual_A4b_SIGN',type(new_sign) is FreshStageConsumption)
                    replacement=q2.invoke(f,key,next_production,new_sign,NOW+85)
                    send.persist_signed_envelope(f.repo,replacement,fence=f.repo.write_fence())
                    new_send=control.consume(f,next_production.original,control.request(f,prepared.preparation,'replacement-send','SEND'),at=NOW+87)
                    check('SELL_new_actual_A4b_SEND',type(new_send) is FreshStageConsumption)
                    observation=invoke(f,new_send,transport,NOW+87)
                    check('SELL_only_proven_replacement_changes_signature',len(boundary.requests)==2
                        and replacement.primary_signature!=envelope.primary_signature and observation.finality=='UNKNOWN')
                    check('SELL_exact_retry_returns_same_preparation',q1.prepare_exact_message(f.repo,next_production,
                        fence=f.repo.write_fence(),ordinal=2).preparation==prepared.preparation)
                f.reopen()
                check(side+'_resolution_and_original_signature_survive_restart',send.load_signed_envelope(f.repo,envelope.preparation.attempt_id)==envelope
                    and f.repo.attempt(envelope.preparation.attempt_id).economic_disposition=='PROVEN_NON_LANDED_RESOLVED')
                f.repo.audit()
            finally:transport.close()


def crash_child(directory):
    with fixture(directory,'abrupt') as (f,production,envelope,fresh,boundary,transport,at):
        (directory/'public-crash-context.json').write_text(canonical_json({'domain':f.domain.to_record(),
            'attempt_id':envelope.preparation.attempt_id,'envelope_digest':envelope.content_digest,
            'ledger_path':str(f.path),'source_path':f.source._conn.execute('PRAGMA database_list').fetchone()[2]}),encoding='utf-8')
        original=send._record
        def terminate(repo,stage,*args):
            value=original(repo,stage,*args)
            if stage.external_reference==send._CLAIM:
                os._exit(77)
            return value
        with patch.object(send,'_record',terminate):invoke(f,fresh,transport,at)
    raise AssertionError('abrupt cut not reached')


def abrupt_process(directory):
    from live.ledger_domain_v0_1 import LedgerDomain
    target=directory/'abrupt-process';target.mkdir()
    run=subprocess.run([sys.executable,'-B',str(Path(__file__).resolve()),'--crash-child',str(target)],
        cwd=ROOT,capture_output=True,text=True,timeout=60)
    check('abrupt_actual_process_exit',run.returncode==77)
    public=json.loads((target/'public-crash-context.json').read_text(encoding='utf-8'))
    domain=dict(public['domain'])
    domain['expected_empty_token_accounts']=tuple(a3.ExpectedTokenAccount(**x) for x in domain['expected_empty_token_accounts'])
    with send.LedgerRepository.reopen(Path(public['ledger_path']),LedgerDomain(**domain)) as repo:
        envelope=send.load_signed_envelope(repo,public['attempt_id'])
        attempt=repo.attempt(public['attempt_id'])
        check('abrupt_verified_envelope_reconstructed',envelope.content_digest==public['envelope_digest'])
        check('abrupt_claim_without_finally_is_possible_send',attempt.recorded_stage=='SEND_CLAIMED' and attempt.lane_held
            and any(s.external_reference==send._CLAIM for s in repo.attempt_stage_inputs(public['attempt_id'])))
        check('abrupt_no_fake_observation',send.load_send_observations(repo,public['attempt_id'])==())
        check('abrupt_generation_fences_old_writer',attempt.prepared_generation<repo.write_fence().generation)
        check('abrupt_SIGN_SEND_only_historical',type(repo.authority_message_receipt('send')) is not FreshStageConsumption)
        repo.audit()
    with sqlite3.connect(public['source_path'],timeout=0,isolation_level=None) as conn:
        conn.execute('BEGIN IMMEDIATE')
        check('abrupt_OS_released_source_lock',conn.in_transaction)
        conn.execute('ROLLBACK')
        check('abrupt_source_integrity',conn.execute('PRAGMA integrity_check').fetchone()==('ok',))


def transport_budgets(directory):
    for label in ('request','total-response','elapsed'):
        with fixture(directory,'budget-'+label) as (f,production,envelope,fresh,boundary,transport,at):
            if label=='request':transport._requests=transport.profile.max_requests
            if label=='total-response':transport._response_bytes=transport.profile.max_total_response_bytes
            original=send.SolanaSendTransport._send_claimed
            def elapsed(self,ticket):
                ticks=iter((0,60,60))
                with patch.object(send.time,'monotonic',lambda:next(ticks,60)):
                    return original(self,ticket)
            with patch.object(send.SolanaSendTransport,'_send_claimed',elapsed if label=='elapsed' else original):
                value=invoke(f,fresh,transport,at)
            check(label+'_bounded_send_outcome',value.outcome=={'request':'TRANSPORT_UNRESOLVED',
                'total-response':'RESPONSE_LIMIT','elapsed':'TIME_LIMIT'}[label])
            check(label+'_no_hidden_retry',len(boundary.requests)==(0 if label=='request' else 1))
            check(label+'_stays_unknown_and_held',value.finality=='UNKNOWN' and f.repo.attempt(envelope.preparation.attempt_id).lane_held)


def main():
    with tempfile.TemporaryDirectory(prefix='live-execution-q3-') as directory:
        directory = Path(directory)
        healthy(directory)
        transport_failures(directory)
        guards(directory)
        crash_cuts(directory)
        journal_and_tickets(directory)
        replacement_boundary(directory)
        abrupt_process(directory)
        transport_budgets(directory)
    print(canonical_json({'status':'IMPLEMENTED_PENDING_PROJECT_REVIEW','checks':len(CHECKS),'all_checks':all(CHECKS.values())}))


if __name__=='__main__':
    if len(sys.argv)==3 and sys.argv[1]=='--crash-child':crash_child(Path(sys.argv[2]))
    else:main()
