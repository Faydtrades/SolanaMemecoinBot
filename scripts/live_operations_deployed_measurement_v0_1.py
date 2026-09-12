"""Finite M52 current-target observations using one retained public C1 fixture.

No fixture setup, private material, execution ports, network or LIVE-store writes.
The copied economic domain stays synthetic; the public target is provenance only.
"""
from __future__ import annotations
import argparse
import hashlib
import json
import sqlite3
import sys
from contextlib import closing
from dataclasses import asdict, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'src'), str(ROOT/'scripts')]
from live_operations_measurement_v0_1 import collect_record, collect_resources, time_operation
from live import operations_startup_v0_1 as startup
from live.continuous_producer_v0_2 import ContinuationProfileV02, FEATURES
from live.ledger_domain_v0_1 import LedgerDomain
from live.operations_ownership_v0_1 import OperationsStore
from live.source_health_v0_1 import SourceBinding, SourceProfile, CursorWitness
from phase4.paper_continuous_market_source_v0_2 import ContinuousMarketSourceV02

C1 = Path(r'C:\Users\Mari1\AppData\Local\Temp\meme-live-c1-measurements-181d025-c13-d28b4a\composed-r4')
FIXTURE = C1/'acquired-open'
PROFILE_FILE = ROOT/'docs/live/MEME_LIVE_OPERATIONS_C1_ENGINEERING_PROFILE_V0_1.json'
EXPECTED_HEAD = '3630b2bb5598e894bf7d851a2a2d857ab8dda9e1'


def read(path):
    return json.loads(Path(path).read_text(encoding='utf-8-sig'))


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def write(path, value):
    Path(path).write_text(json.dumps(value, sort_keys=True, indent=2, allow_nan=False)+'\n', encoding='utf-8')


def canonical_digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def ro(path, immutable=False):
    return sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro'+('&immutable=1' if immutable else ''), uri=True)


def state(started, directory):
    runtime = started.runtime
    producer = runtime.producer
    assert producer is not None, 'original source reconstruction unavailable'
    inventory = sorted(p.name for p in directory.iterdir() if p.is_file() and (p.name.endswith('.sqlite') or p.name.endswith('.sqlite-wal') or p.name.endswith('.sqlite-shm')))
    metrics = collect_record(producer, workload_scope='One isolated retained synthetic C1 workload on target evidence volume; not real-wallet holdings', observations=collect_resources(directory, inventory))
    checkpoint = producer.conn.execute('SELECT generation,manifest_digest,manifest_json FROM live_producer_checkpoint_v0_1 WHERE singleton=1').fetchone()
    with runtime.ledger._trusted_read():
        head = runtime.ledger._conn.execute('SELECT revision,last_receipt_digest FROM ledger_head WHERE singleton=1').fetchone()
        tail = runtime.ledger._conn.execute('SELECT count(*),coalesce(sum(length(CAST(payload_json AS BLOB))),0) FROM ledger_commits WHERE seq>0 AND seq<=?', (head[0],)).fetchone()
        admissions = runtime.ledger._conn.execute('SELECT count(*) FROM ledger_authority_admissions').fetchone()[0]
    payload = producer.conn.execute(f'SELECT coalesce(sum(length(CAST(event_json AS BLOB))),0) FROM {FEATURES}').fetchone()[0]
    return dict(record=metrics, checkpoint=dict(generation=checkpoint[0], digest=checkpoint[1], manifest_utf8_bytes=len(checkpoint[2].encode())), ledger_tail=dict(rows=tail[0], payload_utf8_bytes=tail[1], through_revision=head[0], through_digest=head[1], definition='ledger_commits seq>0 through captured head; full original replay, excludes indexed economic payloads and SQLite allocation'), aggregate_feature_payload_bytes=payload, reconstruction=asdict(runtime.reconstruction_facts()), admissions=admissions, producer_lineage=producer.source_identity, producer_profile_usage=dict(producer.last_profile_usage), files={name:(directory/name).stat().st_size for name in inventory})


def run(manifest_path, expected_manifest_sha, output):
    manifest_path = manifest_path.resolve(strict=True)
    assert sha(manifest_path) == expected_manifest_sha
    target = read(manifest_path)
    assert target['code']['source_checkpoint'] == EXPECTED_HEAD
    qualification_root = (Path(target['paths']['evidence_root'])/'step11a-3630b2').resolve(strict=True)
    output = output.resolve()
    assert output.parent == qualification_root and not output.exists(), 'fresh qualification child required'
    canonical_live_paths = [Path(target['paths'][k]).resolve() for k in ('operations','ledger','producer','evidence_store')]
    assert not any(path.exists() for path in canonical_live_paths), 'expected uninitialized target stores'
    output.mkdir()
    copied = output/'retained-synthetic-c1'
    copied.mkdir()
    accepted = read(PROFILE_FILE)
    fixture_ref = accepted['evidence']['C1.3']['fixture_hashes.json']
    assert sha(fixture_ref['path']) == fixture_ref['sha256']
    fixture_hashes = read(fixture_ref['path'])
    originals = {}
    # Byte-copy inactive accepted SQLite main/WAL/SHM files. Never open the originals
    # with SQLite: even a read-only WAL connection could create/update shared memory.
    for relative, expected in fixture_hashes.items():
        source = C1/relative
        payload = source.read_bytes()
        actual = hashlib.sha256(payload).hexdigest()
        assert actual == expected, 'accepted fixture bytes changed: '+relative
        originals[relative] = actual
        (copied/source.name).write_bytes(payload)
    with closing(ro(copied/'fixture-ledger.sqlite')) as conn:
        domain_record = json.loads(conn.execute('SELECT payload_json FROM ledger_domain WHERE singleton=1').fetchone()[0])
    assert domain_record['expected_empty_token_accounts'] == []
    domain_record['expected_empty_token_accounts'] = ()
    domain = LedgerDomain(**domain_record)
    assert domain.wallet != target['wallet_public_key'], 'synthetic economics must remain distinct'
    with closing(ro(copied/'fixture-evidence.sqlite')) as conn:
        binding_json, profile_json = conn.execute('SELECT binding_json,profile_json FROM source_domain').fetchone()
    binding_record = json.loads(binding_json)
    binding_record['anchors'] = tuple(CursorWitness(**x) for x in binding_record['anchors'])
    binding, source_profile = SourceBinding(**binding_record), SourceProfile(**json.loads(profile_json))
    prior_config = read(C1/'config.json')
    profile = ContinuationProfileV02(**prior_config['producer_profile'])
    identity_prior = prior_config['startup_identity']
    # Accepted explicit fixture identity is preserved when its exact bytes move.
    database_identity = 'A3:SYNTHETIC:RETAINED:COLLECTOR'
    market = ContinuousMarketSourceV02(copied/'fixture-raw.sqlite', start_after_p1_rowid=1, database_identity=database_identity)
    operation_path = copied/'fixture-operations.sqlite'
    store = OperationsStore(operation_path, domain)
    control = store.snapshot()
    configuration = dict(operations_path=operation_path, ledger_path=copied/'fixture-ledger.sqlite', producer_path=copied/'fixture-producer.sqlite', market_source=market, producer_profile=profile, source_path=copied/'fixture-evidence.sqlite', source_binding=binding, source_profile=source_profile, database_identity=database_identity)
    expected_identity = startup.configured_identity(domain, **configuration, producer_schema_digest=identity_prior['producer_schema_digest'], evidence_schema_digest=identity_prior['evidence_schema_digest'], restart_profile=startup.ownership.RestartProfile(*(control[k] for k in ('max_attempts','window_us','backoff_us'))))
    started = None
    sample_start = datetime.now(timezone.utc).isoformat()
    try:
        started, open_timing = time_operation(lambda: startup.start_live(domain=domain, **configuration, expected_identity=expected_identity, process_identity='M52:ISOLATED:REOPEN', now_us=control['last_control_us']+1000001, replace_generation=control['generation']), scope='one original in-process start_live including owned acquisition, all integrity/replay/source reconstruction, monitor constructor; excludes imports, file copies, process spawn and measurement collection')
        before = state(started, copied)
        runtime = started.runtime
        previous_clock = runtime.ledger._authority.last_qualified_clock
        assert previous_clock is not None and previous_clock.reconciliation is None
        calls = 0
        def clock():
            nonlocal calls
            calls += 1
            delta_us = 10000000+calls
            utc = (datetime.fromisoformat(previous_clock.utc_upper_utc)+timedelta(microseconds=delta_us)).isoformat()
            return replace(previous_clock, monotonic_ns=previous_clock.monotonic_ns+delta_us*1000, utc_lower_utc=utc, utc_upper_utc=utc, previous_sample_digest=previous_clock.content_digest)
        work, step_timing = time_operation(lambda: runtime.step(clock=clock), scope='one original Runtime.step with existing due synthetic protection, missing monitor config and no execution ports; excludes collection; synthetic economic clock, real perf_counter interval')
        after = state(started, copied)
        assert before['reconstruction']['source_state'] == 'SOURCE_RECONSTRUCTED'
        assert after['reconstruction']['remaining_units'] == before['reconstruction']['remaining_units'] > 0
        assert before['admissions'] == after['admissions'] == 1
        assert work.work in ('NEED_EXECUTION','PROTECTIVE_ACTION_STAGED'), asdict(work)
        measurement = dict(startup_timing={k:asdict(v) for k,v in open_timing.items()}, protective_timing={k:asdict(v) for k,v in step_timing.items()}, work=asdict(work), before=before, after=after, current_startup_identity=asdict(expected_identity), fixture_domain=domain.to_record(), fixture_source_binding=asdict(binding), fixture_source_profile=asdict(source_profile), fixture_profile=asdict(profile), monitor_status=asdict(runtime._operations_degradation.snapshot()), synthetic_clock_source=asdict(previous_clock), sample_start_utc=sample_start, sample_end_utc=datetime.now(timezone.utc).isoformat())
    finally:
        if started is not None:
            started.close()
    # Only immutable main-file metadata on the protected market source. No rows,
    # full scan, full-file hash, backup or WAL connection; no lineage inference.
    raw_path = Path(target['paths']['market_source']).resolve(strict=True)
    stat = raw_path.stat()
    with closing(ro(raw_path, immutable=True)) as conn:
        conn.set_progress_handler(lambda: 1, 100000)
        schema = conn.execute("SELECT type,name,tbl_name,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name").fetchall()
        required = ('pump_events','collector_events','websocket_observations','gap_jobs_v034')
        tables = {name:[list(row) for row in conn.execute('PRAGMA table_info('+name+')')] for name in required}
    source_fact = dict(canonical_path=str(raw_path), stat_identity=dict(device=stat.st_dev, file_id=stat.st_ino), main_file_length_bytes=stat.st_size, schema_digest=canonical_digest(schema), required_table_columns=tables, observation_scope='immutable=1 main SQLite file schema only; no market rows read; no current WAL coverage, health, source anchors or lineage established', source_binding=None, production_market_rows_read=0)
    for relative, expected in originals.items():
        assert sha(C1/relative) == expected, 'retained original modified'
    assert not any(path.exists() for path in canonical_live_paths)
    result = dict(schema='M52_TARGET_BOUND_RETAINED_MEASUREMENT_V01', status='IMPLEMENTED_PENDING_PROJECT_REVIEW', target_public_configuration=dict(path=str(manifest_path), sha256=expected_manifest_sha, content=target), current_source=dict(head=EXPECTED_HEAD, script_sha256=sha(Path(__file__)), loaded_repo_modules={str(Path(module.__file__).resolve().relative_to(ROOT)):sha(module.__file__) for module in tuple(sys.modules.values()) if getattr(module,'__file__',None) and Path(module.__file__).suffix=='.py' and Path(module.__file__).resolve().is_relative_to(ROOT)}), accepted_fixture=dict(root=str(FIXTURE), manifest_sha256=fixture_ref['sha256'], original_files_unchanged=True, copied_paths=str(copied)), measurement=measurement, actual_market_source_metadata=source_fact, qualification_scope='Real configured target manifest, Windows host and D-volume qualification paths; original copied synthetic workload/economics. No real-wallet holdings, canonical LIVE store reconstruction or healthy live source claim. Configured continuation caps not qualified capacities.', guard_thresholds=None, qualification_limitations=['One finite retained workload; no sustained arrivals, maximum-capacity search or percentile/worst-case latency guarantee.','Original in-process startup; excludes process spawn/replacement and M46 platform proof.','Monitor configuration is absent and ENTRY held; M53 must bind measured profile and qualify monitor/recovery overhead.','Canonical LIVE stores remain absent. Actual SourceBinding anchors/lineage and startup schema/domain/config choices are not inferred from protected source main-file metadata.'], safety=dict(canonical_live_stores_created=False, signing_sending=False, execution_ports_supplied=False, network_calls=False, fixture_originals_modified=False, production_market_rows_read=0))
    write(output/'result.json', result)
    print(json.dumps(dict(result=str(output/'result.json'), sha256=sha(output/'result.json'), work=asdict(work), startup_seconds=open_timing['operation_wall_seconds'].value, protective_seconds=step_timing['operation_wall_seconds'].value, profile_usage=after['producer_profile_usage'], resources={name:after['record']['metrics'][name] for name in ('process_rss_bytes','process_lifetime_peak_rss_bytes','workload_disk_bytes','disk_reserve_bytes')}),sort_keys=True))


if __name__ == '__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--public-manifest',required=True,type=Path)
    parser.add_argument('--manifest-sha256',required=True)
    parser.add_argument('--output',required=True,type=Path)
    args=parser.parse_args()
    run(args.public_manifest,args.manifest_sha256,args.output)
