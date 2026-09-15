"""Small original-API resume fixtures; never opens retained stress witnesses."""
from __future__ import annotations

import copy
import hashlib
import io
import json
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from contextlib import closing, redirect_stdout
from dataclasses import asdict
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'src'), str(ROOT / 'scripts')]

import live_t010_joint_hot_measure_v0_3 as caller
import live_t010_monitor_full_state_v0_1 as builder
from live.continuous_producer_v0_2 import ContinuationProfileV02
from live.ledger_domain_v0_1 import LedgerDomain
from live.operations_degradation_monitor_v0_1 import MonitorConfiguration
from live.operations_degradation_v0_1 import DegradationPolicy, DegradationStore, ResourceLimit
from live.operations_ownership_v0_1 import RestartProfile
from live.operations_startup_v0_1 import StartupIdentity
from live.source_health_v0_1 import SourceProfile
from phase5.shadow_domain_v0_1 import content_fingerprint


class InterruptedAfterCommit(Exception):
    pass


def stream_digest(rows):
    return hashlib.sha256(''.join(row['digest'] + '\n' for row in rows).encode()).hexdigest()


class ResumeSelftest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='prep123h-resume-')
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.origin = self.root / 'origin'
        self.origin.mkdir()
        self.output = self.root / 'output'
        self.stage = self.root / 'stage'
        self.db = self.stage / 'monitor.sqlite'
        self.progress = self.stage / 'monitor-construction-progress.jsonl'
        self.final = self.output / 'active' / 'monitor.sqlite'
        self.domain = LedgerDomain('1' * 32, '1' * 32, 'a' * 64, 1000000000)
        self.policy = DegradationPolicy('b' * 64, 1, 1000000,
            tuple(ResourceLimit(metric, 1000000000) for metric in sorted(builder.METRICS)))
        monitor = MonitorConfiguration(str(self.origin / 'monitor.sqlite'), self.policy,
            'c' * 64, 'd' * 64, 1000000, 'e' * 64)
        paths = {}
        # Caller plumbing needs closed SQLite inputs, not economic histories.
        for name in ('ledger', 'operations', 'producer', 'source', 'raw', 'monitor'):
            path = self.origin / (name + '.sqlite')
            paths[name] = str(path)
            with closing(sqlite3.connect(path)) as conn:
                if name == 'ledger':
                    conn.execute('CREATE TABLE ledger_attempts (attempt_id TEXT PRIMARY KEY)')
                    conn.executemany('INSERT INTO ledger_attempts VALUES (?)', [('a' * 64,), ('b' * 64,), ('c' * 64,)])
                else:
                    conn.execute('CREATE TABLE fixture (value TEXT)')
                    conn.execute('INSERT INTO fixture VALUES (?)', (name,))
                conn.commit()
        self.base = dict(paths=paths, monitor=asdict(monitor), now=100000,
            live_paths=[str(self.origin / 'dry-session.sqlite')], fixture_domain=asdict(self.domain))
        caller.write(self.origin / 'configuration.json', self.base)
        caller.write(self.origin / 'host-resources.json', {'fixture': 'resume-only'})
        derivation = self.root / 'derivation.json'
        derivation.write_text('{}\n', encoding='utf-8')
        # Only external configuration/derivation inputs are substituted. Store
        # construction, snapshots, evidence and every record call stay original.
        self.patches = [patch.object(builder, 'DERIVATION', derivation),
            patch.object(builder, 'DERIVATION_SHA', caller.sha(derivation)),
            patch.object(builder.copies, 'build_configuration', self.configuration)]
        for context in self.patches:
            context.start()
            self.addCleanup(context.stop)

    def configuration(self, record):
        m = copy.deepcopy(record['monitor'])
        p = m.pop('policy')
        p['resource_limits'] = tuple(ResourceLimit(**item) for item in p['resource_limits'])
        monitor = MonitorConfiguration(**m, policy=DegradationPolicy(**p))
        d = dict(record['fixture_domain'])
        d['expected_empty_token_accounts'] = tuple(d['expected_empty_token_accounts'])
        domain = LedgerDomain(**d)
        identity = StartupIdentity('DRY_LIVE', 'fixture-v1', 'd' * 64, 'fixture',
            RestartProfile(100, 1000000, 0), (),
            content_fingerprint((record['paths'], record['live_paths'], record.get('codec_cost_inputs'))),
            'f' * 64, 'a' * 64)
        return dict(degradation_config=monitor, expected_identity=identity, domain=domain,
            source_binding=SimpleNamespace(source_identity='8' * 64),
            source_profile=SourceProfile(), producer_profile=ContinuationProfileV02())

    def run_prepare(self, *, resume=False, stop=None, observations=3):
        original_record = DegradationStore.record
        events = []

        def recording(store, evidence, **kwargs):
            self.assertEqual(kwargs, dict(now_us=evidence.observed_us, coalesce_active=True))
            facts = original_record(store, evidence, **kwargs)
            events.append(asdict(evidence))
            if len(events) == stop:
                raise InterruptedAfterCommit()
            return facts

        with patch.object(DegradationStore, 'record', recording), redirect_stdout(io.StringIO()):
            caller.measure(self.origin, self.output, 1, observations, True, self.stage, resume=resume)
        return events

    def receipts(self):
        with closing(sqlite3.connect(self.db.as_uri() + '?mode=ro', uri=True)) as conn:
            conn.row_factory = sqlite3.Row
            return [dict(row) for row in conn.execute('SELECT * FROM receipts ORDER BY rowid')]

    def interrupted(self, count):
        with self.assertRaises(InterruptedAfterCommit):
            self.run_prepare(stop=count)
        rows = self.receipts()
        self.assertEqual(len(rows), count)
        return rows

    def reconstruction(self):
        return json.loads((self.stage / 'monitor-reconstruction.json').read_text())

    def check_finalization(self, count=98):
        evidence = self.reconstruction()
        self.assertEqual(evidence['receipts'], count)
        self.assertEqual(evidence['target_receipts'], count)
        self.assertEqual(evidence['receipt_insertion_stream_sha256'], stream_digest(self.receipts()))
        self.assertEqual(evidence['sqlite_integrity'], 'ok')
        self.assertTrue(evidence['final_copy_verified'])
        self.assertEqual(caller.sha(self.db), caller.sha(self.final))
        manifest = json.loads((self.output / 'prepared-start-manifest.json').read_text())
        self.assertEqual(manifest['monitor'], evidence)
        for name, digest in manifest['files'].items():
            self.assertEqual(caller.sha(self.output / 'prepared-start' / name), digest)
        config = json.loads((self.output / 'active' / 'configuration.json').read_text())
        self.assertEqual(config['paths']['monitor'], str(self.final))
        self.assertEqual(config['monitor']['path'], str(self.final))
        args = self.configuration(config)
        self.assertEqual(asdict(DegradationStore(self.db, args['domain'], args['degradation_config'].policy).snapshot())['conditions'],
            tuple(evidence['final_conditions']))

    def test_same_identity_prefix_suffix_and_log_lag(self):
        prefix = self.interrupted(19)  # Next operation recovers a retained ACTIVE condition.
        metadata_before = self.db.read_bytes()
        checkpoint = dict(records=16, elapsed_us=123, digest_stream_sha256=stream_digest(prefix[:16]))
        self.progress.write_text(json.dumps(checkpoint) + '\n', encoding='utf-8')
        log_before = self.progress.read_bytes()
        nonmonitor = {p: caller.sha(p) for p in (self.output / 'active').iterdir()}
        suffix = self.run_prepare(resume=True)
        resumed_rows = self.receipts()
        resumed_evidence = self.reconstruction()
        self.assertEqual(resumed_rows[:19], prefix)
        self.assertEqual(len(suffix), 98 - 19)
        self.assertEqual(resumed_evidence['resumed_from_receipts'], 19)
        self.assertEqual(self.progress.read_bytes(), log_before)
        for path, digest in nonmonitor.items():
            self.assertEqual(caller.sha(path), digest)
        self.check_finalization()
        # Compare at the IDENTICAL configured paths/identity, using only this
        # disposable test's directories. No path-bound cross-identity comparison.
        for path in (self.output, self.stage):
            self.assertTrue(path.resolve().is_relative_to(self.root))
            shutil.rmtree(path)
        fresh = self.run_prepare()
        self.assertEqual(self.receipts(), resumed_rows)
        self.assertEqual(fresh[19:], suffix)
        self.assertEqual(self.reconstruction()['final_conditions'], resumed_evidence['final_conditions'])
        stamps = [item['observed_us'] for item in fresh]
        self.assertEqual(stamps, list(range(self.base['now'] * 1000000 - 1097, self.base['now'] * 1000000 - 999)))
        self.assertNotEqual(metadata_before, self.db.read_bytes())
        self.check_finalization()

    def test_identity_mismatch_rejected_before_append(self):
        self.interrupted(19)
        before = self.db.read_bytes(), self.progress.read_bytes()
        for field in ('policy_binding', 'domain'):
            with self.subTest(field=field):
                changed = copy.deepcopy(self.base)
                if field == 'policy_binding':
                    changed['monitor']['host_identity_digest'] = '9' * 64
                else:
                    changed['fixture_domain']['expected_profile_fingerprint'] = '9' * 64
                caller.write(self.origin / 'configuration.json', changed)
                with patch.object(DegradationStore, 'record', side_effect=AssertionError('UNEXPECTED_APPEND')) as record:
                    with self.assertRaisesRegex(ValueError, 'OPERATIONS_DEGRADATION'):
                        with redirect_stdout(io.StringIO()):
                            caller.measure(self.origin, self.output, 1, 3, True, self.stage, resume=True)
                    record.assert_not_called()
                self.assertEqual((self.db.read_bytes(), self.progress.read_bytes()), before)

    def test_changed_original_time_rejected_before_append(self):
        self.interrupted(19)
        before = self.db.read_bytes(), self.progress.read_bytes()
        self.base['now'] += 1
        caller.write(self.origin / 'configuration.json', self.base)
        with self.assertRaisesRegex(AssertionError, 'RESUME_ORIGINAL_TIME_OR_TARGET_CHANGED'):
            self.run_prepare(resume=True)
        self.assertEqual((self.db.read_bytes(), self.progress.read_bytes()), before)

    def test_completed_count_finalizes_without_append(self):
        prefix = self.interrupted(98)
        before = self.db.read_bytes()
        self.assertFalse(self.final.exists())
        self.assertEqual(self.run_prepare(resume=True), [])
        self.assertEqual(self.receipts(), prefix)
        self.assertEqual(self.db.read_bytes(), before)
        self.check_finalization()
        # An already finalized checkpoint also needs no receipt or new identity.
        self.assertEqual(self.run_prepare(resume=True), [])
        self.assertEqual(self.db.read_bytes(), before)
        self.check_finalization()

    def test_fresh_default_unchanged(self):
        self.assertEqual(len(self.run_prepare()), 98)
        self.assertNotIn('resumed_from_receipts', self.reconstruction())
        self.check_finalization()
        before = self.db.read_bytes()
        with self.assertRaises(FileExistsError):
            self.run_prepare()
        self.assertEqual(self.db.read_bytes(), before)

    def test_resume_requires_existing_output_and_prepare_only(self):
        with self.assertRaisesRegex(AssertionError, 'RESUME_EXISTING_OUTPUT_REQUIRED'):
            self.run_prepare(resume=True)
        self.assertFalse(self.output.exists())
        with self.assertRaisesRegex(AssertionError, 'RESUME_REQUIRES_PREPARE_ONLY'):
            caller.measure(self.origin, self.output, 1, resume=True)
        self.assertFalse(self.output.exists())

    def test_cli_resume_forwarding(self):
        command = [sys.executable, '-B', str(Path(caller.__file__).resolve()), 'measure',
            '--origin', str(self.origin), '--output', str(self.output), '--resume',
            '--prepare-only', '--monitor-observations', '3', '--monitor-staging-root', str(self.stage)]
        result = subprocess.run(command, capture_output=True, text=True, timeout=30)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('RESUME_EXISTING_OUTPUT_REQUIRED', result.stderr)
        self.assertFalse(self.output.exists())


if __name__ == '__main__':
    unittest.main(verbosity=2)
