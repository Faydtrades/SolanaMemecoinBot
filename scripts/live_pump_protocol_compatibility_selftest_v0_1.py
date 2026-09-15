"""Current protocol facts, original durable denial and subsequent admission. Offline."""
from pathlib import Path
import sys, tempfile, json
from dataclasses import replace
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'src'),str(ROOT/'scripts')]
import live_runtime_composition_selftest_v0_1 as c
import phase5_shadow_venue_route_quote_selftest_v0_1 as v
from live.pump_protocol_compatibility_v0_1 import PumpProtocolObservation, selected_state_compatibility, rejection_proof, validate_rejection_proof
from live.public_rpc_v0_1 import PublicAccountRead, PublicAccount, RpcContext
from live.runtime_composition_v0_1 import EntryFacts
from live.ledger_repository_v0_1 import LedgerRepository
CHECKS={}
def check(name,value):
    CHECKS[name]=bool(value);assert value,name

def facts(f,wallet,raw):
    mint=f.item.mint; original=next(a for r in wallet.observation.mint_reads for a in r.accounts if a.account.pubkey==mint)
    anchor=wallet.observation.anchor
    keys=(mint,v.derive_bonding_curve_pda(mint),v.derive_pumpswap_pool_pda(mint))
    read=PublicAccountRead(RpcContext(anchor.slot,'test','finalized'),keys,
        (original,PublicAccount(v.RpcAccountV01(keys[1],v.PUMP_PROGRAM_ID,raw),1000000000,False,0),None))
    return PumpProtocolObservation(mint,wallet.digest,f.domain.genesis_hash,f.domain.genesis_hash,
        f.domain.expected_profile_fingerprint,read,anchor,wallet.evaluated_at_utc)

def run():
    with tempfile.TemporaryDirectory(prefix='pump-current-protocol-') as tmp:
        f=c.Fixture(Path(tmp),'skip')
        try:
            f.candidate_ready();f.configure()
            f.scenario.accounts[f.item.mint]=c.a3.account(v.TOKEN_PROGRAM_ID,v.mint_account().data)
            w=c.a3.wallet(f,scenario=f.scenario)
            raw=v.curve_data()+bytes(36)
            normal=facts(f,w,raw);sample=c.a3.clock(f.repo,c.NOW+4)
            check('normal_sol_supported',normal.classify(f.domain,f.item,sample,w)==('SUPPORTED','PUMP','PUMP_NORMAL_SOL_SUPPORTED'))
            variants={'mayhem':raw[:81]+b'\1'+raw[82:], 'cashback':raw[:82]+b'\1'+raw[83:],
                'holder':raw[:124]+b'\1'+raw[125:], 'editable_fee':raw[:123]+b'\1'+raw[124:],
                'unknown_tail':raw[:-1]+b'\1', 'custom_quote':v.curve_data(quote_mint=v.MINT)+bytes(36)}
            for name,data in variants.items():
                check(name+'_unsupported',facts(f,w,data).classify(f.domain,f.item,sample,w)[0]=='UNSUPPORTED')
            check('stale_fact_holds',replace(normal,observed_at_utc=c.a3.utc(c.NOW-40)).classify(f.domain,f.item,sample,w)[0]=='UNKNOWN')
            check('other_wallet_holds',replace(normal,wallet_support_digest='0'*64).classify(f.domain,f.item,sample,w)[0]=='UNKNOWN')
            check('missing_curve_holds',replace(normal,read=replace(normal.read,accounts=(normal.read.accounts[0],None,None))).classify(f.domain,f.item,sample,w)[0]=='UNKNOWN')
            before=f.repo.consumer_snapshot();oldroot=f.root
            unsupported=facts(f,w,variants['mayhem'])
            result=f.step(entry=EntryFacts('PUMP',v.TOKEN_PROGRAM_ID,w,unsupported))
            check('original_runtime_durable_skip',result.work=='CANDIDATE_UNSUPPORTED' and f.repo.inbox_disposition(oldroot)=='REJECTED')
            check('no_reservation_or_position',f.repo.reservation(oldroot) is None and f.repo.consumer_snapshot()['positions']==before['positions'])
            row=f.repo._conn.execute('SELECT payload_json FROM ledger_inbox_dispositions WHERE root_id=?',(oldroot,)).fetchone()
            record=json.loads(row[0]);check('full_original_proof_retained',record['version']=='live_ledger_nonacceptance_v0.2' and record['protocol_evidence']['protocol']['read']['accounts'][1]['data_base64'])
            f.repo.close();f.repo=LedgerRepository.reopen(f.path,f.domain);f.runtime.ledger=f.repo
            f.handoff.ledger=f.repo
            check('cold_reconstruction_preserves_skip',f.repo.inbox_disposition(oldroot)=='REJECTED')
            # Replaying the same source page cannot enqueue the rejected root.
            f.runtime._source_page(c.a3.clock(f.repo,c.NOW+4),c.a3.utc(c.NOW+1))
            check('replayed_source_does_not_requeue',oldroot not in f.runtime.queued_roots)
            proof=record['protocol_evidence'];proof['protocol']['read']['accounts'][1]['owner']=v.TOKEN_PROGRAM_ID
            try:validate_rejection_proof(proof,f.domain,f.repo._authority,f.item,record);denied=False
            except ValueError:denied=True
            check('modified_original_facts_denied',denied)
            f.append(c.hf.NEXT)
            for _ in range(128):
                result=f.step(c.NOW+18)
                if f.runtime.queued_roots:break
            f.root=f.runtime.queued_roots[0];f.item=f.repo.candidate(f.root)
            check('later_original_candidate_reached',f.root!=oldroot)
            f.scenario.accounts[f.item.mint]=c.a3.account(v.TOKEN_PROGRAM_ID,v.mint_account().data)
            w2=c.a3.wallet(f,scenario=f.scenario,at=c.NOW+18)
            normal2=facts(f,w2,raw)
            result=f.runtime.step(clock=lambda:c.a3.clock(f.repo,c.NOW+18),source_cut_utc=c.a3.utc(c.NOW+15),
                entry=EntryFacts('PUMP',v.TOKEN_PROGRAM_ID,w2,normal2))
            check('later_supported_candidate_admitted',f.repo.reservation(f.root) is not None)
        finally:f.close()
    print(json.dumps({'status':'IMPLEMENTED_PENDING_PROJECT_REVIEW','checks':CHECKS,'check_count':len(CHECKS),'network':False,'T010':False},sort_keys=True))
if __name__=='__main__':run()
