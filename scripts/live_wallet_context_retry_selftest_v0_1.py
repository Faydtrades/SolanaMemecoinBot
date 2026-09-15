"""Same-context retries on disposable transports; no public evidence/network."""
import copy
import asyncio
import time
import json
import sys
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'src'),str(ROOT/'scripts')]
import httpx
import live_wallet_evidence_selftest_v0_1 as fixture
from live import public_rpc_v0_1 as api
from live.wallet_evidence_v0_1 import WalletEvidenceAdapter,WalletEvidenceRequest,assess_wallet_accounts
CHECKS={}
def check(name,value):
    CHECKS[name]=bool(value)
    assert value,name
def error(call):
    try:call()
    except api.PublicRpcError as exc:return str(exc)
    raise AssertionError('expected denial')
def unavailable(payload):
    return {'jsonrpc':'2.0','id':payload['id'],'error':{'code':-32016,'message':'Minimum context unavailable',
        'data':{'contextSlot':payload['params'][-1]['minContextSlot']-1}}}
def direct(transform,profile=fixture.PROFILE,retry=True):
    scenario=fixture.Scenario();scenario.transform=transform
    with api.PublicReadOnlyRpc(fixture.ENDPOINT,profile,transport=httpx.MockTransport(scenario.handle)) as rpc:
        result=error(lambda:rpc.get_multiple_accounts((fixture.WALLET,),min_context_slot=101,retry_min_context=retry))
        return result,scenario.calls,rpc._requests,rpc._response_bytes
def main():
    plain=fixture.Scenario();transient=fixture.Scenario();counts={}
    def transform(payload,envelope):
        method=payload['method'];floor=payload['params'][-1].get('minContextSlot',0) if payload['params'] and type(payload['params'][-1]) is dict else 0
        if method=='getMultipleAccounts' or method=='getSlot' and floor==101:
            counts[method]=counts.get(method,0)+1
            if counts[method]<=2:return unavailable(payload)
        return envelope
    transient.transform=transform
    observations=[]
    for scenario in (plain,transient):
        with api.PublicReadOnlyRpc(fixture.ENDPOINT,fixture.PROFILE,transport=httpx.MockTransport(scenario.handle)) as rpc:
            observations.append(WalletEvidenceAdapter(rpc,WalletEvidenceRequest(fixture.WALLET,fixture.GENESIS,100),
                clock=lambda:fixture.utc(),concurrent_cohort=True,retry_min_context=True).observe())
            check('shared_budget_'+str(len(observations)),rpc._requests==len(scenario.calls))
    check('retry_observation_preserves_original_facts_and_digest',observations[0]==observations[1])
    check('retry_preserves_exact_coherent_context',assess_wallet_accounts(observations[1]).coherent_context=='COHERENT')
    calls=[p for p,t in transient.calls if p['method']=='getMultipleAccounts']
    check('every_retry_keeps_identical_floor_and_keys',len(calls)==3 and all(p['params']==calls[0]['params'] for p in calls))
    check('retry_ids_remain_unique',[p['id'] for p,t in transient.calls]==list(range(1,len(transient.calls)+1)))
    reason,calls,requests,charged=direct(lambda p,e:unavailable(p),retry=False)
    check('default_still_one_attempt',reason=='RPC_ERROR_OR_INVALID_ENVELOPE' and len(calls)==requests==1 and charged>0)
    cases={
        'wrong_id':lambda e:e.update(id=e['id']+1),
        'boolean_id':lambda e:e.update(id=True),
        'wrong_version':lambda e:e.update(jsonrpc='1.0'),
        'other_error':lambda e:e['error'].update(code=-32005),
        'string_error_code':lambda e:e['error'].update(code='-32016'),
        'boolean_context':lambda e:e['error']['data'].update(contextSlot=True),
        'not_below_floor':lambda e:e['error']['data'].update(contextSlot=101),
        'negative_context':lambda e:e['error']['data'].update(contextSlot=-1),
        'missing_data':lambda e:e['error'].pop('data'),
        'extra_error_field':lambda e:e['error'].update(other=True),
        'ambiguous_result_and_error':lambda e:e.update(result={}),
    }
    for name,change in cases.items():
        def malformed(p,e):
            value=unavailable(p);change(value);return value
        reason,calls,requests,charged=direct(malformed)
        check(name+'_never_retried',len(calls)==requests==1 and reason=='RPC_ERROR_OR_INVALID_ENVELOPE')
    reason,calls,requests,charged=direct(lambda p,e:httpx.Response(429,text='DO_NOT_RETAIN'))
    check('http429_never_retried',reason=='RPC_HTTP_FAILURE' and requests==1)
    with patch.object(api.asyncio,'sleep',return_value=None):
        reason,calls,requests,charged=direct(lambda p,e:unavailable(p),profile=replace(fixture.PROFILE,max_requests=3))
    check('same_global_request_budget_exhausts',reason=='RPC_REQUEST_BUDGET_EXHAUSTED' and len(calls)==requests==3)
    reason,calls,requests,charged=direct(lambda p,e:unavailable(p),profile=replace(fixture.PROFILE,max_total_response_bytes=150))
    check('failed_responses_charge_original_byte_budget',reason=='RPC_RESPONSE_BUDGET_EXHAUSTED' and charged>150 and requests==2)
    for name,profile,expected in [('single_request',replace(fixture.PROFILE,request_timeout_seconds=1),1),
                                  ('whole_observation',replace(fixture.PROFILE,observation_timeout_seconds=1),1)]:
        now=[0.0]
        async def advance(delay):now[0]+=delay
        with (patch.object(api.time,'monotonic',side_effect=lambda:now[0]),
              patch.object(api.asyncio,'sleep',side_effect=advance)):
            reason,calls,requests,charged=direct(lambda p,e:unavailable(p),profile=profile)
        check(name+'_deadline_not_renewed',reason=='RPC_MINIMUM_CONTEXT_TIMEOUT' and now[0]<=expected+0.000001)
        check(name+'_transport_timeout_shrinks',all(0<t['read']<=expected for p,t in calls) and calls[-1][1]['read']<calls[0][1]['read'])
    reason,calls,requests,charged=direct(lambda p,e:unavailable(p),retry=1)
    check('retry_flag_requires_boolean',reason=='EXPLICIT_MINIMUM_CONTEXT_RETRY_REQUIRED' and requests==0)
    # A successful but different slot is never rewritten to the requested one.
    changed=fixture.Scenario();changed.multiple_context=102
    with api.PublicReadOnlyRpc(fixture.ENDPOINT,fixture.PROFILE,transport=httpx.MockTransport(changed.handle)) as rpc:
        observation=WalletEvidenceAdapter(rpc,WalletEvidenceRequest(fixture.WALLET,fixture.GENESIS,100),
            clock=lambda:fixture.utc(),concurrent_cohort=True,retry_min_context=True).observe()
    check('slot_overshoot_still_incoherent',assess_wallet_accounts(observation).coherent_context=='INCOHERENT')
    # Real async sleeps verify cancellation while blocked inside a sub-8KiB
    # transport read, rather than merely checking after a buffered yield.
    class Trickle(httpx.AsyncByteStream):
        def __init__(self):self.chunks=0;self.closed=False
        async def __aiter__(self):
            while True:
                await asyncio.sleep(0.6)
                self.chunks+=1
                yield b' '*512
        async def aclose(self):self.closed=True
    stream=Trickle()
    with api.PublicReadOnlyRpc(fixture.ENDPOINT,replace(fixture.PROFILE,request_timeout_seconds=1),
            transport=httpx.MockTransport(lambda r:httpx.Response(200,headers={'content-type':'application/json'},stream=stream))) as rpc:
        start=time.monotonic()
        reason=error(lambda:rpc.get_multiple_accounts((fixture.WALLET,),min_context_slot=101,retry_min_context=True))
        elapsed=time.monotonic()-start
        check('blocked_trickle_cancelled_and_closed',reason=='RPC_MINIMUM_CONTEXT_TIMEOUT' and stream.chunks==1
              and stream.closed and elapsed<2 and rpc._response_bytes==512)
    for kind in ('account','slot'):
        now=[0.0];scenario=fixture.Scenario()
        with (patch.object(api.time,'monotonic',side_effect=lambda:now[0]),
                api.PublicReadOnlyRpc(fixture.ENDPOINT,replace(fixture.PROFILE,request_timeout_seconds=1),
                    transport=httpx.MockTransport(scenario.handle)) as rpc):
            if kind=='account':
                original=rpc._account
                def decode(*args):
                    result=original(*args);now[0]=2;return result
                with patch.object(rpc,'_account',side_effect=decode):
                    reason=error(lambda:rpc.get_multiple_accounts((fixture.WALLET,),min_context_slot=101,retry_min_context=True))
            else:
                original=api.u64
                def decode(value):
                    result=original(value)
                    if rpc._requests:now[0]=2
                    return result
                with patch.object(api,'u64',side_effect=decode):
                    reason=error(lambda:rpc.get_finalized_slot(min_context_slot=100,retry_min_context=True))
            check(kind+'_late_semantic_success_denied',reason=='RPC_MINIMUM_CONTEXT_TIMEOUT')
    async def nested_loop_probe():
        with api.PublicReadOnlyRpc(fixture.ENDPOINT,fixture.PROFILE,transport=httpx.MockTransport(fixture.Scenario().handle)) as rpc:
            return error(lambda:rpc.get_finalized_slot(min_context_slot=100,retry_min_context=True)),rpc._requests
    check('nested_loop_fails_closed_without_request',asyncio.run(nested_loop_probe())==('RPC_SYNCHRONOUS_CALL_CONTEXT_REQUIRED',0))
    async def resolver_cleanup(request):
        # Model non-cancellable native DNS work. Executor shutdown is allowed
        # to finish late, but no expired fact may escape. The exact process
        # supervisor, not this request timer, owns hard termination.
        await asyncio.get_running_loop().run_in_executor(None,time.sleep,1.2)
        return httpx.Response(200,json={'jsonrpc':'2.0','id':1,'result':101})
    with api.PublicReadOnlyRpc(fixture.ENDPOINT,replace(fixture.PROFILE,request_timeout_seconds=1),
            transport=httpx.MockTransport(resolver_cleanup)) as rpc:
        reason=error(lambda:rpc.get_finalized_slot(min_context_slot=100,retry_min_context=True))
        check('native_cleanup_cannot_release_expired_fact',reason=='RPC_MINIMUM_CONTEXT_TIMEOUT' and rpc._requests==1)
    print(json.dumps({'checks':CHECKS,'count':len(CHECKS),'network':False,'production_data':False},sort_keys=True))
if __name__=='__main__':main()
