"""One hash-pinned Windows entry, currently restricted to retained qualification.

No store initialization, private material, execution ports or network adapters.
The canonical public path is independently denied while stores are absent.
Task Scheduler must use the exact manifest path and SHA, with a BootTrigger.
"""
from __future__ import annotations

import argparse
from dataclasses import asdict, replace
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import socket
import sys
import time
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'src'), str(ROOT/'scripts')]


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def read(path):
    return json.loads(Path(path).read_text(encoding='utf-8-sig'))


def require(value, code):
    if not value:
        raise RuntimeError(code)


def write(path, value):
    path = Path(path)
    temporary = path.with_name(path.name + '.' + uuid4().hex + '.tmp')
    with temporary.open('x', encoding='utf-8') as stream:
        stream.write(json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + '\n')
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def code_inventory():
    paths = sorted((ROOT/'src').rglob('*.py'))
    paths += [Path(__file__), ROOT/'src/live/operations_windows_host_v0_1.py']
    return {str(p.relative_to(ROOT)).replace('\\', '/'): sha(p) for p in paths}


def interpreter_binding():
    return dict(launcher_path=str(Path(sys.executable).resolve()),
        launcher_sha256=sha(sys.executable), base_path=str(Path(sys._base_executable).resolve()),
        base_sha256=sha(sys._base_executable), version=sys.version)


def configuration(manifest):
    # Rehydrate only explicit public typed values; never pickle/import a callback
    # supplied by a manifest and never derive approved identity from open stores.
    from live import operations_startup_v0_1 as startup
    from live.ledger_domain_v0_1 import LedgerDomain
    from live.wallet_evidence_v0_1 import ExpectedTokenAccount
    from live.continuous_producer_v0_2 import ContinuationProfileV02
    from live.source_health_v0_1 import SourceBinding, SourceProfile, CursorWitness
    from phase4.paper_continuous_market_source_v0_2 import ContinuousMarketSourceV02
    c = manifest['startup']
    record = dict(c['domain'])
    record['expected_empty_token_accounts'] = tuple(ExpectedTokenAccount(**x) for x in record['expected_empty_token_accounts'])
    domain = LedgerDomain(**record)
    record = dict(c['source_binding'])
    record['anchors'] = tuple(CursorWitness(**x) for x in record['anchors'])
    identity = dict(c['expected_identity'])
    identity['contracts'] = tuple(tuple(x) for x in identity['contracts'])
    identity['restart_profile'] = startup.ownership.RestartProfile(**identity['restart_profile'])
    paths = c['paths']
    return dict(domain=domain, operations_path=Path(paths['operations']), ledger_path=Path(paths['ledger']),
        producer_path=Path(paths['producer']), source_path=Path(paths['evidence_store']),
        market_source=ContinuousMarketSourceV02(paths['market_source'],
            start_after_p1_rowid=c['market_anchor'], database_identity=c['database_identity']),
        database_identity=c['database_identity'], producer_profile=ContinuationProfileV02(**c['producer_profile']),
        source_binding=SourceBinding(**record), source_profile=SourceProfile(**c['source_profile']),
        expected_identity=startup.StartupIdentity(**identity))


def retained_inputs(started):
    """Original retained synthetic clock only; no economic execution resources."""
    previous = started.runtime.ledger._authority.last_qualified_clock
    require(previous is not None and previous.reconciliation is None, 'HOST_RETAINED_CLOCK_REQUIRED')
    delta = 10000000
    utc = (datetime.fromisoformat(previous.utc_upper_utc) + timedelta(microseconds=delta)).isoformat()
    sample = replace(previous, monotonic_ns=previous.monotonic_ns + delta*1000,
        utc_lower_utc=utc, utc_upper_utc=utc, previous_sample_digest=previous.content_digest)
    return dict(clock=lambda: sample)


def validate(manifest_path, expected_sha):
    require(sha(manifest_path) == expected_sha, 'HOST_MANIFEST_HASH_CONFLICT')
    m = read(manifest_path)
    require(m['schema'] == 'MEME_LIVE_WINDOWS_HOST_V1', 'HOST_MANIFEST_SCHEMA_CONFLICT')
    require(Path(m['checkout']).resolve() == ROOT and Path(m['python']).resolve() == Path(sys.executable).resolve(),
            'HOST_EXECUTABLE_PATH_CONFLICT')
    require(m['interpreter_binding'] == interpreter_binding(), 'HOST_INTERPRETER_BINDING_CONFLICT')
    require(m['code_files'] == code_inventory(), 'HOST_CODE_BINDING_CONFLICT')
    require(m['host'] == socket.gethostname(), 'HOST_MACHINE_CONFLICT')
    for binding in m['accepted_bindings']:
        require(sha(binding['path']) == binding['sha256'], 'HOST_ACCEPTED_BINDING_CONFLICT')
    target = read(m['accepted_bindings'][0]['path'])
    paths = m['startup']['paths']
    require(len(set(str(Path(p).resolve()).casefold() for p in paths.values())) == 5,
            'HOST_DISTINCT_STORE_PATHS_REQUIRED')
    if m['scope'] == 'CANONICAL_LIVE_DENIAL_ONLY':
        require(paths == {k:target['paths'][k] for k in paths}, 'HOST_CANONICAL_PATH_CONFLICT')
        # No market-source open or economic-store construction precedes this.
        require(all(Path(paths[k]).is_file() for k in ('operations','ledger','producer','evidence_store')),
                'HOST_CANONICAL_EXISTING_STORES_REQUIRED')
        raise RuntimeError('HOST_CANONICAL_ACTIVATION_NOT_AUTHORIZED')
    require(m['scope'] == 'SYNTHETIC_RETAINED_QUALIFICATION', 'HOST_SCOPE_NOT_AUTHORIZED')
    require(m['startup']['domain']['wallet'] != target['wallet_public_key'], 'HOST_SYNTHETIC_DOMAIN_REQUIRED')
    scope_root = Path(m['qualification_root']).resolve(strict=True)
    require(scope_root.is_relative_to(Path(target['paths']['evidence_root']).resolve()), 'HOST_QUALIFICATION_PATH_CONFLICT')
    require(all(Path(p).resolve().is_relative_to(scope_root) and Path(p).is_file() for p in paths.values()),
            'HOST_EXISTING_QUALIFICATION_STORES_REQUIRED')
    require(Path(m['journal_root']).resolve().is_relative_to(scope_root), 'HOST_JOURNAL_PATH_CONFLICT')
    require(type(m['run_seconds']) is int and 1 <= m['run_seconds'] <= 3600, 'HOST_BOUNDED_RUN_REQUIRED')
    return m


def run(manifest_path, expected_sha):
    m = validate(manifest_path, expected_sha)
    from live.operations_windows_host_v0_1 import WindowsHostBoundary
    from live.operations_supervisor_v0_1 import OperationsSupervisor, HealthProfile
    from live.operations_ownership_v0_1 import OperationsStore
    from live.operations_startup_v0_1 import configured_identity
    config = configuration(m)
    # Preflight the frozen interface/config comparison before any acquisition.
    identity = config['expected_identity']
    actual = configured_identity(**{k:v for k,v in config.items() if k != 'expected_identity'},
        producer_schema_digest=identity.producer_schema_digest,
        evidence_schema_digest=identity.evidence_schema_digest, restart_profile=identity.restart_profile)
    require(actual == identity, 'HOST_REVIEWED_STARTUP_IDENTITY_CONFLICT')
    boundary = WindowsHostBoundary(config['domain'].economic_domain_id).enter()
    root = Path(m['journal_root'])
    root.mkdir(exist_ok=True)
    launch = uuid4().hex
    journal = root/(launch + '.jsonl')
    def record(event, **facts):
        value = dict(event=event, utc=datetime.now(timezone.utc).isoformat(), launch=launch,
            supervisor_pid=os.getpid(), manifest_sha256=expected_sha, host=socket.gethostname(), **facts)
        with journal.open('a', encoding='utf-8') as stream:
            stream.write(json.dumps(value, sort_keys=True, allow_nan=False) + '\n')
            stream.flush()
            os.fsync(stream.fileno())
    store = OperationsStore(config['operations_path'], config['domain'])
    control = store.snapshot()
    previous_path = root/'owner.json'
    expected = m['initial_control']
    if previous_path.is_file():
        previous = read(previous_path)
        require(previous['manifest_sha256'] == expected_sha, 'HOST_PRIOR_MANIFEST_CONFLICT')
        expected = previous['control']
    # A crash before a proven startup handshake deliberately holds. Unknown
    # ownership outside the named job is not silently adopted or killed.
    if not control['stopped'] and not control['exhausted']:
        require(all(control[k] == expected[k] for k in ('generation','process_identity','nonce','budget_id')),
                'HOST_UNREVIEWED_CONTROL_OWNER')
    record('HOST_ENTERED', control=control, startup_identity=asdict(identity),
        domain=config['domain'].to_record(), paths=m['startup']['paths'], prior_job_active_processes=0,
        interpreter_binding=m['interpreter_binding'])
    supervisor = OperationsSupervisor(config, retained_inputs, HealthProfile(**m['health']),
        expected_generation=control['generation'])
    deadline = time.monotonic() + m['run_seconds']
    prior = None
    while time.monotonic() < deadline:
        facts = supervisor.poll(now_us=time.time_ns()//1000, monotonic_us=time.monotonic_ns()//1000)
        key = (facts.state, facts.pid, facts.generation, facts.last_work)
        if key != prior:
            if supervisor.fence is not None and facts.state == 'RUNNING':
                current = store.snapshot()
                write(previous_path, dict(manifest_sha256=expected_sha, control=current,
                    fence=asdict(supervisor.fence), supervisor_pid=os.getpid(), launch=launch))
            record('SUPERVISOR', facts=asdict(facts))
            prior = key
        if facts.state in ('OPERATOR_STOPPED','RESTART_EXHAUSTED') or facts.state.startswith('HELD_'):
            record('HOST_HELD', state=facts.state, control=store.snapshot())
            return 0 if facts.state in ('OPERATOR_STOPPED','RESTART_EXHAUSTED') else 2
        time.sleep(0.05)
    record('HOST_FINITE_END', control=store.snapshot())
    return 0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--manifest', type=Path, required=True)
    parser.add_argument('--sha256', required=True)
    args = parser.parse_args()
    try:
        return run(args.manifest.resolve(strict=True), args.sha256)
    except Exception as error:
        # Public fixed local error codes only. Never print arbitrary exception
        # text/provider data. Scheduler captures nonzero startup failure.
        code = str(error)
        if not code.startswith('HOST_') or not code.replace('_','').isalnum():
            code = 'HOST_STARTUP_FAILED_' + type(error).__name__
        print(json.dumps(dict(status='DENIED', reason=code)), flush=True)
        return 2


if __name__ == '__main__':
    # os._exit closes the noninherited job handle, killing only this contained
    # process tree, and avoids multiprocessing's implicit join of active child.
    result = main()
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(result)
