"""Exact supported-profile dossier/preflight tests. Synthetic approvals grant nothing."""
from __future__ import annotations

import copy
import json
import socket
import sqlite3
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
from live import acceptance_dossier_v0_1 as d
from live import t010_preflight_v0_1 as gate
import live_t010_preflight_selftest_v0_1 as fixtures

CHECKS = {}


def check(name, value):
    if not value:
        raise AssertionError(name)
    CHECKS[name] = True


def qualification(profile_directory, directory):
    profile_ref = {'path':str(profile_directory/'supported-profile.json'),
        'sha256':d.sha256((profile_directory/'supported-profile.json').read_bytes())}
    qualification_ref = {'path':str(profile_directory/'profile-qualification.json'),
        'sha256':d.sha256((profile_directory/'profile-qualification.json').read_bytes())}
    profile = d.read_json(profile_ref['path'])
    qualified = d.read_json(qualification_ref['path'])
    before = {str(p):p.read_bytes() for p in profile_directory.rglob('*') if p.is_file()}
    with patch.object(sqlite3,'connect',side_effect=AssertionError('database called')), \
         patch.object(socket,'socket',side_effect=AssertionError('network called')):
        dossier = d.build_dossier(ROOT,dry_profile=profile_ref,dry_qualification=qualification_ref)
        check('exact_supported_dossier_rebuild',d.verify_dossier(ROOT,dossier)==dossier)
        check('explicit_accepted_core_amendment',dossier['source']['accepted_runtime_changed'] is True
            and set(dossier['source']['authorized_core_overlay'])==set(d.CORE_OVERLAY))
        missing = gate.preflight(ROOT,dossier,evaluated_at_utc=fixtures.AT)
        check('missing_external_inputs_denied_without_static_gap',missing['ready'] is False
            and not any(row['class']=='ENGINEERING' for row in missing['reasons'])
            and {row.get('row') for row in missing['reasons'] if row['class']=='HUMAN_EXTERNAL'}=={'M09','M10','M58'})
        accepted = d.read_json(dossier['deployment']['profile']['path'])
        accepted['source_configuration'].update(binding=profile['inputs']['source_start']['binding'],
            profile=profile['inputs']['source_start']['profile'])
        package = fixtures.fixture(directory,dossier,accepted)
        package['dry_configuration'] = copy.deepcopy(profile)
        report = gate.preflight(ROOT,dossier,package,evaluated_at_utc=fixtures.AT)
        check('same_frozen_tooling_complete_synthetic_structure',report['ready'] is True
            and report['readiness']=='STRUCTURALLY_READY_PENDING_EXTERNAL_AUTHORIZATION'
            and report['readiness_scope']=='STRUCTURAL_PREFLIGHT_ONLY'
            and report['structural_validation']['state']=='STRUCTURALLY_COMPLETE')
        check('structural_success_never_authorization',all(report[key] is False for key in
            ('grants_permission','authenticated_approval','current_environment_proven','capital_authority','t010_executed')))
        for name, mutate in (
            ('qualification-stale-source',lambda r:r.update(source_content_digest='0'*64)),
            ('qualification-stale-profile',lambda r:r.update(profile_content_digest='0'*64)),
            ('qualification-wrong-startup',lambda r:r['startup_identity'].update(runtime_code_digest='0'*64)),
            ('qualification-hidden-substitution',lambda r:r['qualification_substitutions'].update(synthetic_baseline_and_rpc=False)),
            ('qualification-wrong-domain',lambda r:r['domain'].update(mode='LIVE')),
            ('qualification-wrong-source-mapping',lambda r:r['original_source_mapping'].update(market_source_identity='0'*64)),
            ('qualification-wrong-amendment',lambda r:r['guard_amendment'].update(sha256='0'*64)),
            ('qualification-safety-bool-required',lambda r:r.update(signer_send_broadcast=0)),
            ('qualification-missing-recovery',lambda r:r.update(cold_interruption_recovery_count=0))):
            changed=copy.deepcopy(qualified);mutate(changed)
            reference=fixtures.record(directory/(name+'.json'),changed)
            try:d._qualified_dry(profile_ref,reference,dossier['source'],dossier['deployment'])
            except (ValueError,KeyError,TypeError,OSError):check(name,True)
            else:raise AssertionError(name)
        wrong_deployment=copy.deepcopy(dossier['deployment'])
        wrong_deployment['accepted_monitor']['sha256']='0'*64
        try:d._qualified_dry(profile_ref,qualification_ref,dossier['source'],wrong_deployment)
        except ValueError:check('wrong-accepted-monitor-denied',True)
        else:raise AssertionError('wrong monitor accepted')
        for name, mutate in (
            ('stale-dossier',lambda p:p.update(source_content_digest='0'*64)),
            ('stale-profile-input',lambda p:p['dry_configuration']['inputs']['driver'].update(max_steps=2)),
            ('live-capability',lambda p:p['capabilities'].update(sign=True))):
            changed=copy.deepcopy(package);mutate(changed)
            # Exact freeze was independently rebuilt above; avoid repeatedly
            # traversing the immutable graph for these focused package negatives.
            with patch.object(d,'verify_dossier',return_value=dossier):
                denied=gate.preflight(ROOT,dossier,changed,evaluated_at_utc=fixtures.AT)
            check(name,denied['ready'] is False and denied['grants_permission'] is False)
        try:d._qualified_dry(profile_ref,None,dossier['source'],dossier['deployment'])
        except ValueError:check('absent-qualification-denied',True)
        else:raise AssertionError('absent qualification accepted')
    check('qualified_artifacts_and_stores_unchanged',before=={
        str(p):p.read_bytes() for p in profile_directory.rglob('*') if p.is_file()})
    check('preflight_no_store_initialization',not any(directory.glob('*.sqlite*')))
    fixtures.record(directory/'synthetic-structural-package.json',package)
    fixtures.record(directory/'synthetic-structural-report.json',report)
    return {'checks':dict(CHECKS),'source_content_digest':dossier['source']['content_digest'],
        'scope':'Synthetic structural approval records only; no actual approval, T010, database or network access.'}


def main():
    profile_directory=Path(sys.argv[1]).resolve()
    if len(sys.argv)>2:
        directory=Path(sys.argv[2]).resolve();directory.mkdir(parents=True,exist_ok=True)
        result=qualification(profile_directory,directory)
    else:
        with tempfile.TemporaryDirectory(prefix='step11c-profile-freeze-') as tmp:
            result=qualification(profile_directory,Path(tmp))
    print(json.dumps(result,sort_keys=True))


if __name__=='__main__':main()
