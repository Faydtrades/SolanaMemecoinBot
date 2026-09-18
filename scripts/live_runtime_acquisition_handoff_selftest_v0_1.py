"""Causal acquisition/admission regression. Isolated stores; no public network."""
from pathlib import Path
from dataclasses import replace
from types import SimpleNamespace
import sys, tempfile, json
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'src'),str(ROOT/'scripts')]
import live_runtime_composition_selftest_v0_1 as c
import live_source_health_selftest_v0_1 as sf
import live_pump_protocol_compatibility_selftest_v0_1 as pf
from live.runtime_composition_v0_1 import capture_source_at_completion, entry_at_clock, admission_clock, EntryFacts
from live.source_health_v0_1 import CollectorSourceAdapter, SourceProfile, evaluate_source, source_consumer_evidence
from live.authority_controls_v0_1 import EligibilityInput, evaluate_entry, utc_from_us
from live.ledger_actions_v0_1 import utc_microseconds
from live.wallet_evidence_v0_1 import ledger_account_evidence
CHECKS={}
def check(n,v):
    CHECKS[n]=bool(v)
    assert v,n
def fails(call):
    try: call()
    except (ValueError,RuntimeError): return True
    return False
def monitor_freshness(root):
    import live_runtime_owned_dry_selftest_v0_1 as owned
    from live.operations_degradation_monitor_v0_1 import _subject, _record
    # Original monitor/store/owned-runtime fixture; only external clock facts
    # are synthetic. Exercise both callback and fixed-sample consumers.
    for label, age_us, callback in (
            ('fresh', 29995000, True), ('expired', 30010000, True),
            ('exact_expiry', 30000000, True), ('fixed_fresh', 29995000, False),
            ('fixed_expired', 30010000, False)):
        f = owned.fixture(root, 'monitor_' + label)
        try:
            monitor = f.runtime._operations_degradation
            prior = f.source.latest()
            receipt = utc_microseconds(prior.progress.last_live_receipt_utc)
            acquired, consumed = receipt + 29990000, receipt + age_us
            before = acquired - 10000
            base = owned.a3.clock(f.repo, owned.NOW + 4)
            base_us = utc_microseconds(base.utc_upper_utc)
            def sample(us):
                return replace(base, utc_lower_utc=utc_from_us(us), utc_upper_utc=utc_from_us(us),
                    monotonic_ns=base.monotonic_ns + (us - base_us) * 1000)
            subject = _subject(f.domain, 'SOURCE', (f.binding.source_identity, f.source.profile.fingerprint))
            _record(monitor.store, 'SOURCE_TRUTH_UNAVAILABLE', subject, 'a' * 64, before - 1)
            wallet = owned.a3.wallet(f, scenario=f.scenario)
            host = owned.fixtures.host(f, owned.NOW + 4)
            host = replace(host, metrics=tuple(replace(v, observed_us=before) for v in host.metrics))
            clocks = iter((sample(acquired), sample(consumed)))
            alerts = monitor.observe(sample(before if callback else consumed),
                entry=EntryFacts('PUMP', owned.sf.TOKEN_PROGRAM_ID, wallet) if callback else None,
                resources=lambda: host, source_cut_utc=prior.snapshot.requested_cut_utc,
                **({'clock': lambda: next(clocks)} if callback else {}))
            conditions = [v for v in monitor.store.snapshot().conditions
                if v.condition == 'SOURCE_TRUTH_UNAVAILABLE']
            fresh = age_us < 30000000
            check('monitor_' + label + '_recovery_matches_original_freshness',
                len(conditions) == 1 and conditions[0].state == ('RECOVERED' if fresh else 'ACTIVE'))
            check('monitor_' + label + '_source_history_unchanged', f.source.latest() == prior)
            check('monitor_' + label + '_never_grants_permission',
                not alerts.grants_permission and not alerts.may_sign and not alerts.may_send)
        finally:
            owned.fixtures.close(f)

def main():
    with tempfile.TemporaryDirectory(prefix='causal-handoff-') as tmp:
        root=Path(tmp)
        f=c.Fixture(root,'admission')
        try:
            f.candidate_ready();f.configure()
            base=c.a3.clock(f.repo,c.NOW+4)
            def sample(at):
                us=utc_microseconds(at)
                return replace(base,utc_lower_utc=at,utc_upper_utc=at,
                    monotonic_ns=base.monotonic_ns+(us-utc_microseconds(base.utc_upper_utc))*1000)
            raw=root/'source.sqlite';binding=sf.fixture(raw);adapter=CollectorSourceAdapter(raw)
            before=sample(sf.at(9));after=sample(sf.at(12))
            old=adapter.observe(binding,SourceProfile(),observed_at_utc=before.utc_upper_utc,requested_cut_utc=sf.at(9))
            check('old_late_read_reproduces_future_and_secondary_tail',
                {'FUTURE_SOURCE_UTC','UNOBSERVED_RECEIPT_TAIL'} <= set(old.reasons))
            ended,good=capture_source_at_completion(adapter,binding,SourceProfile(),before,lambda:after,cut=sf.at(9))
            check('post_capture_clock_closes_R29',good.disposition=='HEALTHY' and utc_microseconds(good.snapshot.observed_at_utc)==utc_microseconds(after.utc_upper_utc))
            check('original_receipts_and_cut_preserved',good.snapshot.receipts==old.snapshot.receipts and good.snapshot.requested_cut_utc==old.snapshot.requested_cut_utc)
            rollback = evaluate_source(binding, SourceProfile(),
                replace(good.snapshot, observed_at_utc=utc_from_us(utc_microseconds(good.snapshot.observed_at_utc)-1)), previous=good)
            check('one_microsecond_source_clock_rollback_still_rejected',
                'EVIDENCE_CLOCK_OR_CUT_REGRESSION' in rollback.reasons)
            check('source_consumer_wrong_identity_still_rejected', not source_consumer_evidence(good,
                expected_source_identity='0'*64, required_cut_utc=good.snapshot.requested_cut_utc,
                now_utc=good.snapshot.observed_at_utc).observed_prefix_supported)
            check('source_consumer_wrong_cut_still_rejected', not source_consumer_evidence(good,
                expected_source_identity=binding.source_identity, required_cut_utc=sf.at(8),
                now_utc=good.snapshot.observed_at_utc).observed_prefix_supported)
            _,future=capture_source_at_completion(adapter,binding,SourceProfile(),before,lambda:before,cut=sf.at(9))
            check('future_source_still_rejected','FUTURE_SOURCE_UTC' in future.reasons)
            _,stale=capture_source_at_completion(adapter,binding,SourceProfile(),after,lambda:sample(sf.at(50)),cut=sf.at(9))
            check('stale_source_still_rejected',stale.disposition!='HEALTHY')
            _,tail=capture_source_at_completion(adapter,binding,SourceProfile(),after,lambda:after,cut=sf.at(11))
            check('unobserved_tail_still_rejected','UNOBSERVED_RECEIPT_TAIL' in tail.reasons)
            sf.add_gap(raw,'COMPLETE')
            _,gap=capture_source_at_completion(adapter,binding,SourceProfile(),after,lambda:after,cut=sf.at(9))
            check('start_now_gap_continuity_not_relaxed',gap.disposition!='HEALTHY')
            # Original Authority evaluator, no predicate copy.
            sequence,source=f.source.latest_record()
            old_clock=replace(base,utc_lower_utc=utc_from_us(utc_microseconds(source.snapshot.observed_at_utc)-10000),
                utc_upper_utc=source.snapshot.observed_at_utc)
            supplied=EligibilityInput(f.root,f.item.content_digest,f.policy.selected_track,old_clock,sequence,source,None)
            denial,_=evaluate_entry(f.domain,f.repo._authority,f.item,f.repo.inbox_disposition(f.root),supplied)
            check('old_same_interval_R34_rejected_by_original_authority','SOURCE_KNOWLEDGE_CUT_UNPROVEN' in denial.reasons)
            later=replace(base,utc_lower_utc=utc_from_us(utc_microseconds(base.utc_upper_utc)+100000),
                utc_upper_utc=utc_from_us(utc_microseconds(base.utc_upper_utc)+120000),
                monotonic_ns=base.monotonic_ns+120000000)
            positive,_=evaluate_entry(f.domain,f.repo._authority,f.item,f.repo.inbox_disposition(f.root),replace(supplied,clock=later))
            check('later_clock_original_authority_eligible',positive.disposition=='ELIGIBLE_CONTEXT_ONLY')
            wallet=c.a3.wallet(f,scenario=f.scenario)
            entry=EntryFacts('PUMP',c.sf.TOKEN_PROGRAM_ID,wallet)
            bound=entry_at_clock(entry,later)
            check('wallet_bound_exact_final_upper',bound.wallet.evaluated_at_utc==later.utc_upper_utc
                and bound.wallet.observation is wallet.observation and bound.wallet.required_min_context_slot==wallet.required_min_context_slot)
            check('future_knowledge_not_waited_or_backdated',fails(lambda:admission_clock(lambda:base,f.repo._authority,
                utc_from_us(utc_microseconds(base.utc_upper_utc)+1000000),entry)))
            source_us = utc_microseconds(source.snapshot.observed_at_utc)
            overlap = replace(base, utc_lower_utc=utc_from_us(source_us-1000),
                utc_upper_utc=utc_from_us(source_us+1000), monotonic_ns=base.monotonic_ns+1001000)
            samples = iter((overlap, later))
            check('one_bounded_overlap_resample_proves_knowledge',
                admission_clock(lambda:next(samples),f.repo._authority,source.snapshot.observed_at_utc,entry)==later)
            check('unresolved_overlap_remains_held',fails(lambda:admission_clock(lambda:overlap,
                f.repo._authority,source.snapshot.observed_at_utc,entry)))
            protocol = pf.facts(f, wallet, pf.v.curve_data()+bytes(36))
            check('forged_original_protocol_wallet_binding_rejected', fails(lambda:entry_at_clock(
                replace(entry,protocol=replace(protocol,wallet_support_digest='0'*64)),later)))
            # True Runtime -> original Ledger/Authority integration, positive-width clock.
            count=[0]
            def clock():
                count[0]+=1;delta=count[0]*100000
                return replace(base,utc_lower_utc=utc_from_us(utc_microseconds(base.utc_upper_utc)+delta-20000),
                    utc_upper_utc=utc_from_us(utc_microseconds(base.utc_upper_utc)+delta),
                    monotonic_ns=base.monotonic_ns+delta*1000)
            result=f.runtime.step(clock=clock,entry=entry,source_cut_utc=c.a3.utc(c.NOW+1))
            check('actual_runtime_to_original_authority_admitted',result.work=='ENTRY_ADMITTED')
            check('same_original_candidate_reserved',f.repo.reservation(f.root) is not None)
        finally:f.close()
        monitor_freshness(root)
    print(json.dumps(dict(status='IMPLEMENTED_PENDING_PROJECT_REVIEW',checks=CHECKS,count=len(CHECKS),network=False)))
if __name__=='__main__':main()
