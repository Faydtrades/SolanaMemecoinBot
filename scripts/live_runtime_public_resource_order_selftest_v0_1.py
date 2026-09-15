"""Original public resource provider/monitor ordering; synthetic external facts."""
import sys,json
from pathlib import Path
from dataclasses import dataclass
from types import SimpleNamespace as N
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'src'))
from live import runtime_dry_public_facts_v0_1 as module
from live.operations_degradation_monitor_v0_1 import OperationsMonitor
from live.authority_controls_v0_1 import utc_from_us
from live.ledger_actions_v0_1 import utc_microseconds
from phase5.shadow_domain_v0_1 import content_fingerprint
@dataclass(frozen=True)
class Fence:value:str='f'*64
config=N(path=str(ROOT/'fake.sqlite'),host_identity_digest='a'*64,runtime_code_digest='b'*64,protective_qualification_digest='c'*64,policy=N(reviewed_configuration_digest='d'*64),resource_max_age_us=1000000)
facts=object.__new__(module.ReviewedPublicFacts);facts.config={'degradation_config':config};facts.startup_us=74000000
driver={'quote_policy':{'slippage_bps':500},'plan_policy':{},'compute':{'units':250000,'micro_lamports':1000}}
facts.profile=N(record={'startup_identity':{'configuration_digest':'d'*64},'inputs':{'driver':driver}});facts.settings={'protective_timing':{},'source_lag_us':0,'venue':'PUMP','token_program':'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'}
counter=[100]
def clock(started=None):
    counter[0]+=1;return N(utc_lower_utc=utc_from_us(counter[0]),utc_upper_utc=utc_from_us(counter[0]))
facts.clock=clock
started=N(runtime=N(_ownership=N(fence=Fence()),_dry_recovery=True,queued_roots=[]))
monitor=object.__new__(OperationsMonitor);monitor.configuration=config
timing={'schema':'MEME_LIVE_REVIEWED_PROTECTIVE_TIMING_V1','host_identity_digest':'a'*64,'runtime_code_digest':'b'*64,'qualification_digest':'c'*64,'configuration_digest':'d'*64,'protective_step_us':2000000}
checks={}
def check(name,value):checks[name]=bool(value);assert value,name
with patch.object(module,'read_reference',return_value=timing),patch.object(module,'windows_working_set_bytes',side_effect=[500000000,500001000]):
    value=facts(started)
    try:value.operations_resources();denied=False
    except ValueError:denied=True
    check('unpaired_snapshot_denied',denied)
    sample=value.clock();first=value.operations_resources()
    stamp=utc_microseconds(sample.utc_upper_utc)
    check('observation_precedes_corresponding_runtime_sample',all(v.observed_us<stamp for v in first.metrics))
    owner=content_fingerprint({'value':'f'*64})
    observed,_=monitor._host_metrics(value.operations_resources,stamp,owner)
    check('original_monitor_accepts_all_four_metrics',all(v is not None for v in observed.values()))
    stale,_=monitor._host_metrics(value.operations_resources,stamp+config.resource_max_age_us+1,owner)
    check('original_stale_rss_and_disk_denial_preserved',stale['HOST_RSS_BYTES'] is None and stale['HOST_DISK_RESERVE_BYTES'] is None)
    future,_=monitor._host_metrics(value.operations_resources,stamp-2,owner)
    check('original_future_metric_denial_preserved',all(v is None for v in future.values()))
    wrong,_=monitor._host_metrics(value.operations_resources,stamp,'0'*64)
    check('original_owner_identity_denial_preserved',all(v is None for v in wrong.values()))
    sample=value.clock();second=value.operations_resources()
    check('each_runtime_clock_refreshes_the_snapshot',first!=second and next(v.value for v in second.metrics if v.metric=='HOST_RSS_BYTES')==500001000)
# Resource errors remain in the monitor channel, without reusing old metrics.
with patch.object(module,'read_reference',return_value=timing),patch.object(module,'windows_working_set_bytes',return_value=500000000):
    value=facts(started)
    for error_type in module.HOST_OBSERVATION_ERRORS:
        value.clock()
        error=error_type('ISOLATED_RESOURCE_UNAVAILABLE')
        with patch.object(facts,'resources',side_effect=error):
            sample=value.clock()
            try:value.operations_resources();same=False
            except module.HOST_OBSERVATION_ERRORS as observed_error:same=observed_error is error
            check('resource_error_preserved_'+error_type.__name__,same)
            observed,_=monitor._host_metrics(value.operations_resources,utc_microseconds(sample.utc_upper_utc),owner)
            check('no_stale_snapshot_'+error_type.__name__,all(v is None for v in observed.values()))
        sample=value.clock()
        observed,_=monitor._host_metrics(value.operations_resources,utc_microseconds(sample.utc_upper_utc),owner)
        check('fresh_resource_recovery_'+error_type.__name__,all(v is not None for v in observed.values()))
    with patch.object(facts,'resources',side_effect=OSError('RESOURCE_ERROR')),patch.object(facts,'clock',side_effect=ValueError('CLOCK_ERROR')):
        try:value.clock();fatal=False
        except ValueError as error:fatal=str(error)=='CLOCK_ERROR'
        check('clock_failure_remains_fatal',fatal)
print(json.dumps({'scope':'ORIGINAL_PUBLIC_PROVIDER_AND_MONITOR_SYNTHETIC_EXTERNAL_FACTS','checks':checks,'count':len(checks),'T010':'NOT_STARTED'},sort_keys=True))
