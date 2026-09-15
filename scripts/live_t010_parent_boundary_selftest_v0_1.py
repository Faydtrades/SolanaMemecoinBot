"""Original T010 parent counters and bounded atomic journal failure boundary."""
import io,json,sys,tempfile,subprocess,os
from pathlib import Path
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'src'),str(ROOT/'scripts')]
from live import t010_public_host_v0_1 as host
from live_t010_work_budget_selftest_v0_1 import make_budget,fails
if len(sys.argv)>1 and sys.argv[1]=='--crash-journal':
    with patch.object(host.os,'replace',side_effect=lambda *args:os._exit(19)):
        host.write_record(Path(sys.argv[2]),{'interrupted_original_write':True})
    raise AssertionError('missing interruption')
checks={}
def check(name,value):
    checks[name]=bool(value)
    assert value,name
with tempfile.TemporaryDirectory() as name:
    root=Path(name)
    session,budget=make_budget(root/'budget',starts=2,steps=3)
    check('parent_entry_reserved_before_manifest',budget.before_host_entry() and budget.snapshot()['host_entries_reserved']==1)
    reopened=host.WorkBudget(root/'budget',session)
    check('interrupted_parent_does_not_refund_entry',reopened.before_host_entry() and reopened.snapshot()['host_entries_reserved']==2)
    check('parent_entry_bound_plus_one_denied',not reopened.before_host_entry())
    check('parent_and_child_totals_are_independent_finite',reopened.snapshot()['startups_reserved']==0 and reopened.before_start())
    reopened.after_start({'terminal_roots':[],'pending_action':False})
    reopened.hold_resources()
    check('confirmed_no_pending_still_holds_ordinary_work',not reopened.before_start() and not reopened.before_step())
    state=reopened.snapshot();host.write_record(reopened.path,dict(state,pending_action=True))
    check('pending_resource_hold_allows_bounded_restart',reopened.before_start())
    check('pending_resource_hold_allows_bounded_recovery_step',reopened.before_step())
    reopened.after_step({'terminal_roots':['e'*64],'pending_action':False})
    check('terminal_no_pending_cannot_rearm_ordinary_work',not reopened.before_start() and not reopened.before_step())
    state=reopened.snapshot();host.write_record(reopened.path,dict(state,host_entries_reserved=3))
    check('parent_counter_corruption_denied',fails(reopened.snapshot))
    session,budget=make_budget(root/'uncertain',starts=2,steps=2)
    budget.before_start();budget.after_start({'terminal_roots':[],'pending_action':False});budget.before_step();budget.hold_resources()
    check('interrupted_unconfirmed_step_allows_reconstruction',budget.before_start())
    budget.after_start({'terminal_roots':[],'pending_action':True})
    check('recovery_still_consumes_last_original_step',budget.before_step())
    budget.after_step({'terminal_roots':[],'pending_action':True})
    check('pending_never_extends_original_step_or_start_limit',not budget.before_step() and not budget.before_start())
    target=root/'record.json'
    values=[{}, {'unicode':'\u00e9\n\u2028','nested':[1,False,None]}, {'long':'x'*8192}]
    for i,value in enumerate(values):
        expected=dict(value);expected['content_digest']=host.content_fingerprint(expected)
        original=io.StringIO();json.dump(expected,original,sort_keys=True,allow_nan=False)
        host.write_record(target,value)
        check('original_write_codec_bytes_exact_'+str(i),target.read_text()==original.getvalue() and host.read_record(target)==value)
    before=target.read_bytes()
    with patch.object(host.os,'replace',side_effect=OSError('ISOLATED_INTERRUPTION_BEFORE_REPLACE')):
        check('interrupted_replace_propagates',fails(lambda:host.write_record(target,{'new':True})))
    orphan=target.with_name(target.name+'.tmp');orphan_before=orphan.read_bytes()
    check('interruption_preserves_previous_record_and_one_orphan',target.read_bytes()==before and len(list(root.glob('record.json*.tmp')))==1)
    for i in range(3):
        check('repeated_write_preserves_orphan_'+str(i),fails(lambda:host.write_record(target,{'retry':i}))
            and target.read_bytes()==before and orphan.read_bytes()==orphan_before and len(list(root.glob('record.json*.tmp')))==1)
    abrupt=root/'abrupt.json';host.write_record(abrupt,{'original':True});prior=abrupt.read_bytes()
    child=subprocess.run([sys.executable,'-B',str(Path(__file__).resolve()),'--crash-journal',str(abrupt)],
        cwd=ROOT,capture_output=True,timeout=20)
    check('actual_process_death_after_fsync_preserves_original',child.returncode==19 and abrupt.read_bytes()==prior)
    orphan=abrupt.with_name(abrupt.name+'.tmp');retained=orphan.read_bytes()
    check('actual_process_death_retains_one_fail_closed_temporary',fails(lambda:host.write_record(abrupt,{'retry':True}))
        and orphan.read_bytes()==retained and len(list(root.glob('abrupt.json*.tmp')))==1)
    exact=root/'exact.json'
    base={'value':''};enclosed=dict(base,content_digest=host.content_fingerprint(base))
    overhead=len(json.dumps(enclosed,sort_keys=True,allow_nan=False).encode())
    exact_value={'value':'x'*(16777216-overhead)}
    host.write_record(exact,exact_value)
    check('existing_reader_exact_size_boundary_roundtrip',exact.stat().st_size==16777216 and host.read_record(exact)==exact_value)
    before=exact.read_bytes()
    check('size_boundary_plus_one_preserves_existing_record',fails(lambda:host.write_record(exact,{'value':exact_value['value']+'x'}))
        and exact.read_bytes()==before and not exact.with_name(exact.name+'.tmp').exists())
    oversized=root/'oversized.json'
    check('reader_size_contract_enforced_before_new_file',fails(lambda:host.write_record(oversized,{'value':'x'*16777216}))
        and not oversized.exists() and not oversized.with_name(oversized.name+'.tmp').exists())
print(json.dumps({'scope':'ORIGINAL_PARENT_BOUNDARY_ISOLATED_TESTS','checks':checks,'count':len(checks),'T010':'NOT_STARTED'},sort_keys=True))
