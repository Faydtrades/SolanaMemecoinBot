"""Isolated journal contention regression; generated fixture key, no public I/O."""
from __future__ import annotations

import argparse
from contextlib import closing, contextmanager
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import threading
import time
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'src'), str(ROOT / 'scripts')]
import live_step10_s01_s02_selftest_v0_1 as prior
from live import runtime_composition_v0_1 as runtime
from live import operations_degradation_v0_1 as journal


@contextmanager
def real_lock(path, seconds):
    ready = threading.Event()
    failures = []
    def locked():
        try:
            with closing(sqlite3.connect(path, isolation_level=None)) as conn:
                conn.execute('BEGIN EXCLUSIVE')
                ready.set()
                time.sleep(seconds)
                conn.rollback()
        except BaseException as exc:
            failures.append(type(exc).__name__)
            ready.set()
    worker = threading.Thread(target=locked)
    worker.start()
    if not ready.wait(5) or failures:
        raise AssertionError(('isolated lock setup failed', failures))
    try:
        yield
    finally:
        worker.join(5)
        if worker.is_alive() or failures:
            raise AssertionError(('isolated lock cleanup failed', failures))


def execution_case(directory, expect_held):
    key = prior.c2.Keypair()  # Disposable fixture, never a deployment key.
    with patch.object(prior.sf, 'WALLET', str(key.pubkey())), patch.object(prior.sf.plans, 'ACTOR', str(key.pubkey())):
        f = prior.prepare(directory, 'journal-budget')
        original = runtime.persist_signed_envelope
        boundary = {'outside_journal_seconds': None}
        lock_context = None
        monitor = f.runtime._operations_degradation
        original_observe = monitor.observe
        def observed(*args, **kwargs):
            nonlocal lock_context
            result = original_observe(*args, **kwargs)
            if lock_context is not None and result.unavailable_code is not None:
                lock_context.__exit__(None, None, None)
                lock_context = None
            return result
        def persisted(*args, **kwargs):
            nonlocal lock_context
            result = original(*args, **kwargs)
            started = time.monotonic()
            # Simulate unrelated execution CPU/I/O after the exact signed
            # envelope is durable, outside all degradation journal operations.
            time.sleep(1.05)
            boundary['outside_journal_seconds'] = time.monotonic() - started
            lock_context = real_lock(f.runtime._operations_degradation.store.path, .08)
            lock_context.__enter__()
            return result
        try:
            action = f.buy
            with patch.object(runtime, 'persist_signed_envelope', persisted), patch.object(monitor, 'observe', observed), prior.external_mint(action.mint), prior.retained_accumulator_rpc(f, action):
                result, reads, sends = prior.c2.execution(f, key, action, at=prior.NOW+6, number=71)
            if lock_context is not None:
                lock_context.__exit__(None, None, None)
                lock_context = None
            stages = [json.loads(row[0])['to_stage'] for row in f.repo._conn.execute(
                'SELECT payload_json FROM ledger_attempt_stages ORDER BY commit_seq')]
            permissions = [json.loads(row[0])['original']['request']['stage'] for row in f.repo._conn.execute(
                'SELECT payload_json FROM ledger_authority_message_stages ORDER BY commit_seq')]
            observations = {'runtime_step': asdict(result), 'send_requests': len(sends.requests),
                'attempt_stages': stages, 'authority_stages': permissions, **boundary,
                'conditions': [asdict(row) for row in f.runtime._operations_degradation.store.snapshot().conditions]}
            if expect_held:
                assert result.work == 'OPERATIONS_ENTRY_HELD' and result.reason == 'CURRENT_DEGRADATION_ENTRY_HOLD'
                assert sends.requests == [] and permissions == ['SIGN'] and stages[-1] == 'SIGNED_DURABLE'
            else:
                assert result.work == 'SUBMISSION_OBSERVED' and len(sends.requests) == 1
                assert permissions == ['SIGN', 'SEND']
                observations['journal_budget'] = budget_cases(monitor.store)
            return observations
        finally:
            if lock_context is not None:
                lock_context.__exit__(None, None, None)
            prior.ops.close(f)


def budget_cases(store):
    samples = {}
    # Exact shared-budget proof independent of SQLite/OS timeout wake-up
    # overhead. Production uses the high-resolution monotonic perf counter.
    instant = [0.0]
    with patch.object(journal, 'monotonic', lambda: instant[0]), store.contention_window():
        with store._connection() as conn:
            instant[0] += .25
            assert conn.execute('PRAGMA busy_timeout').fetchone()[0] == 750
        instant[0] += 100  # Outside journal work never consumes the allowance.
        assert store._busy_seconds() == .75
        with store.contention_window(), store._connection() as conn:
            instant[0] += .25
            assert conn.execute('PRAGMA busy_timeout').fetchone()[0] == 500
        assert store._busy_seconds() == .5
        with store._connection() as conn:
            instant[0] += .5
            assert conn.execute('PRAGMA busy_timeout').fetchone()[0] == 0
        assert store._busy_seconds() == 0
    def read_locked(seconds, expected_error):
        with real_lock(store.path, seconds):
            started = time.perf_counter()
            try:
                store.snapshot()
                failed = False
            except ValueError as exc:
                assert str(exc) == 'OPERATIONS_DEGRADATION_STORE_UNAVAILABLE'
                failed = True
            elapsed = time.perf_counter()-started
        assert failed == expected_error, (seconds, elapsed, failed)
        return elapsed
    with store.contention_window():
        samples['first_overlap_seconds'] = read_locked(.25, False)
        after_first = store._busy_seconds()
        with store.contention_window():
            samples['second_overlap_seconds'] = read_locked(1.10, True)
        assert store._busy_seconds() < .01, 'nested window renewed exhausted allowance'
        samples['exhausted_overlap_seconds'] = read_locked(.08, True)
        assert samples['exhausted_overlap_seconds'] < .05
        samples['combined_wait_seconds'] = samples['first_overlap_seconds'] + samples['second_overlap_seconds']
        # SQLite/OS return overhead is measured separately from the configured
        # timeout. The shared configured allowance is exactly one second.
        assert .8 <= samples['combined_wait_seconds'] < 1.2
        assert .50 < after_first < .80
    with store.contention_window():
        samples['fresh_window_seconds'] = read_locked(.04, False)
    with store.contention_window():
        samples['single_exhaustion_seconds'] = read_locked(1.20, True)
        assert .8 <= samples['single_exhaustion_seconds'] < 1.2
    # SQL executed after time has elapsed inside one connection must refresh
    # its timeout too, rather than receiving one second per statement.
    with store.contention_window(), store._connection() as conn:
        before = store._busy_seconds()
        conn.execute('SELECT 1')
        timeout_ms = conn.execute('PRAGMA busy_timeout').fetchone()[0]
        assert 0 <= timeout_ms <= int(before*1000)
    samples['checks'] = ['unrelated_runtime_time_not_debited', 'brief_overlap_waited',
        'nested_window_does_not_renew', 'multiple_overlaps_share_one_second',
        'exhaustion_remains_fail_closed', 'fresh_outer_window_resets',
        'single_persistent_overlap_bounded', 'statement_timeout_refreshes',
        'exact_cumulative_configured_budget_1000_750_500_0ms']
    return samples


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--evidence-dir', type=Path, required=True)
    parser.add_argument('--expect-held', action='store_true', help='Capture the pre-correction defect, not a success assertion.')
    args = parser.parse_args()
    args.evidence_dir.mkdir(parents=True, exist_ok=False)
    with tempfile.TemporaryDirectory(prefix='journal-budget-') as directory:
        result = execution_case(Path(directory), args.expect_held)
    record = {'status': 'IMPLEMENTED_PENDING_PROJECT_REVIEW', 'scope': 'ISOLATED_REAL_SQLITE_LOCK_SYNTHETIC_EXTERNAL_FACTS',
        'expected_pre_correction_hold': args.expect_held, 'execution': result,
        'code_sha256': {name: hashlib.sha256((ROOT/name).read_bytes()).hexdigest() for name in (
            'src/live/operations_degradation_v0_1.py', 'src/live/operations_degradation_monitor_v0_1.py',
            'src/live/runtime_composition_v0_1.py', str(Path(__file__).relative_to(ROOT)).replace('\\', '/'))}}
    (args.evidence_dir/'result.json').write_text(json.dumps(record, indent=2, sort_keys=True)+'\n')
    print(json.dumps({'work': result['runtime_step']['work'], 'reason': result['runtime_step']['reason'],
        'send_requests': result['send_requests'], 'authority_stages': result['authority_stages']}))


if __name__ == '__main__':
    main()
