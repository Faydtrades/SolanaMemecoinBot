"""Disposable original-producer lifecycle/accounting checks; no public claims."""
from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
from dataclasses import replace
from datetime import timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'src'), str(ROOT / 'scripts')]
from live_continuous_producer_selftest_v0_2 import (
    BASE, open_producer, empty_source, check, reject, CHECKS,
    ContinuationProfileV02, TABLES, accepted,
)
from live_candidate_handoff_selftest_v0_1 import Fixture, NEXT
from live.continuous_producer_v0_2 import RESOLVED_KEYS


def row_snapshot(p):
    return {t: [dict(r) for r in p.conn.execute(f'SELECT * FROM {t} ORDER BY rowid')]
            for t in (TABLES[1], *TABLES[4:])}


def tick(p, key):
    p.prepare_clock_tick(BASE + timedelta(seconds=400), drive_exit_clock=False, timer_id=key)
    return p.execute_prepared_timer(key)


def timers(root):
    raw = root / 'empty.sqlite'
    empty_source(raw)
    path = root / 'timers.sqlite'
    profile = replace(ContinuationProfileV02(), pending_rows=1)
    c, p = open_producer(path, raw, profile=profile)
    for n in range(64):
        tick(p, f't{n:04d}')
        assert p.last_profile_usage['pending_rows'] == 0
    check('64 source-free timers resolve without pending ratchet',
          p.last_profile_usage['resolved_history_rows'] == 128 and p.durable_p1_rowid == 0)
    before = row_snapshot(p)
    usage = dict(p.last_profile_usage)
    p.wall_clock = lambda: BASE + timedelta(days=1)
    p.execute_prepared_timer('t0000')
    check('completed timer replay preserves all original bytes and resource accounting',
          row_snapshot(p) == before and p.last_profile_usage == usage)
    for table, rows in before.items():
        if not rows:
            continue
        for statement in (f'DELETE FROM {table}', f'UPDATE {table} SET rowid=rowid+10000'):
            reject('immutable completed row denies ' + statement, lambda s=statement: c.execute(s))
    checkpoint = p.checkpoint_manifest()
    c.close()
    c, p = open_producer(path, raw, profile=profile)
    check('cold reconstruction preserves full timer history and exact checkpoint',
          row_snapshot(p) == before and p.checkpoint_manifest() == checkpoint)
    p.prepare_clock_tick(BASE, timer_id='pending-1')
    check('one unresolved timer uses exact configured pending boundary', p.last_profile_usage['pending_rows'] == 1)
    checkpoint = p.checkpoint_manifest()
    reject('pending boundary plus one fails closed', lambda: p.prepare_clock_tick(BASE, timer_id='pending-2'))
    c.close()
    c, p = open_producer(path, raw, profile=profile)
    check('pending exhaustion preserves unresolved obligation and previous checkpoint',
          p.checkpoint_manifest() == checkpoint and p.last_profile_usage['pending_rows'] == 1)
    c.close()

    # Same exact input identity and serialization; numerical cap is an explicit
    # isolated fixture, never a mutation or widening of a deployment profile.
    c, p = open_producer(root / 'measure.sqlite', raw)
    tick(p, 'bounded')
    exact = p.last_profile_usage['history_bytes']
    c.close()
    for cap in (exact, exact - 1):
        bounded = replace(profile, history_bytes=cap)
        store = root / f'bound-{cap}.sqlite'
        c, p = open_producer(store, raw, profile=bounded)
        p.prepare_clock_tick(BASE + timedelta(seconds=400), timer_id='bounded')
        if cap == exact:
            p.execute_prepared_timer('bounded')
            check('history exact byte boundary accepts complete immutable bundle', p.last_profile_usage['history_bytes'] == cap)
        else:
            checkpoint = p.checkpoint_manifest()
            reject('history byte boundary plus one rejects resolution transaction', lambda: p.execute_prepared_timer('bounded'))
            c.close()
            c, p = open_producer(store, raw, profile=bounded)
            check('history exhaustion preserves prepared timer and no partial input',
                  p.checkpoint_manifest() == checkpoint and not row_snapshot(p)[TABLES[1]])
        c.close()
    print('LIFECYCLE_EMPTY_TIMER_HISTORY_BYTES', exact)


def crashcuts(root):
    raw = root / 'crash-source.sqlite'
    empty_source(raw)
    for phase in ('before_input_resolution', 'after_input_resolution',
                  'before_checkpoint_publish', 'after_checkpoint_publish_before_commit'):
        path = root / (phase + '.sqlite')
        c, p = open_producer(path, raw)
        p.prepare_clock_tick(BASE, timer_id='crash')
        before = p.checkpoint_manifest()
        def fail(key, stage):
            if stage == phase:
                raise RuntimeError('deterministic isolated crash cut')
        p.failure_injector = fail
        try:
            p.execute_prepared_timer('crash')
        except RuntimeError:
            pass
        else:
            raise AssertionError('missing crash injection ' + phase)
        c.close()
        c, p = open_producer(path, raw)
        check('atomic pre-resolution recovery ' + phase, p.checkpoint_manifest() == before)
        p.execute_prepared_timer('crash')
        check('cold retry resolves exactly once ' + phase,
              p.last_profile_usage['pending_rows'] == 0 and p.last_profile_usage['resolved_history_rows'] == 2)
        c.close()


def delivery(root):
    f = Fixture(root, 'delivery')
    try:
        f.produce()
        before = dict(f.producer.last_profile_usage)
        inputs = [dict(r) for r in f.conn.execute(f'SELECT * FROM {TABLES[1]} ORDER BY input_key')]
        check('completed source inputs are immutable history while their children remain pending',
              before['resolved_history_rows'] == len(inputs) and all(r['complete'] == 1 for r in inputs)
              and before['pending_rows'] == sum(f.conn.execute(f'SELECT count(*) FROM {t} WHERE delivered=0').fetchone()[0]
                                              for t in (TABLES[4], TABLES[5])))
        roots = f.deliver()
        check('real Ledger handoff resolves only the acknowledged candidate outbox row',
              len(roots) == 1 and f.producer.last_profile_usage['pending_rows'] == before['pending_rows'] - 1
              and f.producer.last_profile_usage['resolved_history_rows'] == before['resolved_history_rows'] + 1)
        immutable = dict(f.conn.execute(f'SELECT * FROM {TABLES[5]} WHERE delivered=1').fetchone())
        reject('acknowledged original candidate cannot be changed', lambda: f.conn.execute(
            f'UPDATE {TABLES[5]} SET event_json=? WHERE event_sequence=?', ('{}', immutable['event_sequence'])))
        f.conn.rollback()
        audit = f.conn.execute(f'SELECT * FROM {TABLES[4]} WHERE delivered=0 LIMIT 1').fetchone()
        # Explicit fixture audit consumer: production has no such consumer and
        # never invents this ACK. Exercise the original supported transition.
        prior = f.producer.last_profile_usage['pending_rows']
        with f.producer._transaction():
            f.conn.execute(f'UPDATE {TABLES[4]} SET delivered=1,accepted_evaluation_id=? WHERE input_key=? AND ordinal=?',
                           ('FIXTURE:AUDIT:RECEIPT', audit['input_key'], audit['ordinal']))
        check('explicit audit receipt moves one obligation to immutable history',
              f.producer.last_profile_usage['pending_rows'] == prior - 1)
        reject('delivered audit cannot regress to pending', lambda: f.conn.execute(
            f'UPDATE {TABLES[4]} SET delivered=0 WHERE input_key=? AND ordinal=?', (audit['input_key'], audit['ordinal'])))
        f.conn.rollback()
        original_runs = f.producer.original_runs(f.repo.candidate(roots[0]).mint)
        f.append(NEXT)
        f.produce()
        second = f.deliver()
        check('multiple actual candidates retain separate original immutable lookup',
              len(second) == 1 and second[0] != roots[0]
              and f.producer.original_runs(f.repo.candidate(roots[0]).mint) == original_runs)
        before = row_snapshot(f.producer)
        usage = dict(f.producer.last_profile_usage)
        f.reopen()
        check('cold restore retains exact acknowledged bytes and true undelivered obligations',
              row_snapshot(f.producer) == before and f.producer.last_profile_usage == usage)
        replay = f.deliver()
        check('idempotent handoff after cold restore does not fabricate acknowledgements',
              set(replay) == set(roots + second) and row_snapshot(f.producer) == before
              and f.producer.last_profile_usage == usage)
        check('original historical candidate payload remains addressable after retirement',
              any(dict(row) == immutable for row in f.producer.producer_events()))
        check('isolated lifecycle integrity and foreign keys',
              f.conn.execute('PRAGMA integrity_check').fetchone()[0] == 'ok'
              and f.conn.execute('PRAGMA foreign_key_check').fetchone() is None)
        print('LIFECYCLE_RESOURCE_USAGE', json.dumps(f.producer.last_profile_usage, sort_keys=True))
        insert_conflicts(f)
    finally:
        f.close()


def insert_conflicts(f):
    tick(f.producer, 'immutable-insert-review')
    sequence = 100000
    for table, keys in RESOLVED_KEYS.items():
        predicate = 'complete=1 AND production_p1_rowid IS NOT NULL' if table == TABLES[1] else (
            "status='COMPLETE'" if table == TABLES[6] else 'delivered=1')
        original = dict(f.conn.execute(f'SELECT rowid AS original_rowid,* FROM {table} WHERE {predicate} LIMIT 1').fetchone())
        rowid = original.pop('original_rowid')
        original['rowid'] = rowid
        def insert(values, mode):
            names = ','.join(values)
            f.conn.execute(f'INSERT {mode} INTO {table} ({names}) VALUES ({",".join("?" for _ in values)})', tuple(values.values()))
        for key in keys:
            for mode in ('OR REPLACE', 'OR IGNORE'):
                sequence += 1
                changed = dict(original)
                for column in {c for k in keys for c in k}:
                    changed[column] = sequence if column in ('rowid', 'event_sequence', 'production_p1_rowid', 'ordinal') else 'GUARD:NEW:' + str(sequence)
                for column in key:
                    changed[column] = original[column]
                if table == TABLES[5]:
                    # INTEGER PRIMARY KEY is the rowid alias, one identity.
                    if 'rowid' in key or 'event_sequence' in key:
                        changed['event_sequence'] = changed['rowid'] = rowid
                changed['content_fingerprint'] = 'f' * 64
                before = row_snapshot(f.producer)
                checkpoint = f.producer.checkpoint_manifest()
                check('replacement defense works with recursive triggers disabled', f.conn.execute('PRAGMA recursive_triggers').fetchone()[0] == 0)
                try:
                    with f.producer._transaction():
                        insert(changed, mode)
                except sqlite3.IntegrityError as exc:
                    check('changed collision rejected before conflict policy ' + table + str(key) + mode,
                          'immutable resolved producer history' in str(exc))
                else:
                    raise AssertionError('changed insert collision silently accepted ' + table + str(key))
                unchanged_checkpoint = f.producer.checkpoint_manifest() == checkpoint
                f.reopen()  # Handoff attachment legitimately publishes a fresh generation.
                check('rejected replacement preserves rollback checkpoint and cold original bytes',
                      unchanged_checkpoint and row_snapshot(f.producer) == before)
        for mode in ('', 'OR IGNORE', 'OR REPLACE'):
            for explicit_rowid in (False, True):
                exact = dict(original)
                if not explicit_rowid:
                    exact.pop('rowid')
                before = row_snapshot(f.producer)
                with f.producer._transaction():
                    insert(exact, mode)
                check('exact duplicate keeps immutable row and original rowid ' + table + mode + str(explicit_rowid),
                      row_snapshot(f.producer) == before and f.conn.execute(
                          f'SELECT rowid FROM {table} WHERE rowid=?', (rowid,)).fetchone()[0] == rowid)
        f.reopen()
        check('exact insert replay cold recovery remains unchanged ' + table, row_snapshot(f.producer) == before)
        # A still-pending OLD row must never replace a separate immutable row.
        # Keep injected fixture obligations inside a rollback-only transaction;
        # no invented production completion or receipt is committed.
        for key in keys:
            for mode in ('OR REPLACE', 'OR IGNORE'):
                for identical_target in (False, True):
                    sequence += 1
                    pending = dict(original)
                    pending['rowid'] = sequence
                    for column in {c for k in keys for c in k} - {'rowid'}:
                        pending[column] = sequence if column in ('event_sequence', 'production_p1_rowid', 'ordinal') else 'GUARD:PENDING:' + str(sequence)
                    if table in (TABLES[4], TABLES[5]):
                        pending['input_key'] = original['input_key']
                    if table == TABLES[6]:
                        pending['input_key'] = None
                    terminal = 'complete' if table == TABLES[1] else 'status' if table == TABLES[6] else 'delivered'
                    pending[terminal] = 'PREPARED' if terminal == 'status' else 0
                    changed = dict(original) if identical_target else dict(pending)
                    for column in key:
                        changed[column] = original[column]
                    if table == TABLES[5] and ('rowid' in key or 'event_sequence' in key):
                        changed['event_sequence'] = changed['rowid'] = rowid
                    before = row_snapshot(f.producer)
                    checkpoint = f.producer.checkpoint_manifest()
                    class FixtureRollback(Exception):
                        pass
                    try:
                        with f.producer._transaction():
                            insert(pending, '')
                            try:
                                f.conn.execute(f'UPDATE {mode} {table} SET ' + ','.join(c+'=?' for c in changed)
                                    + ' WHERE rowid=?', (*changed.values(), sequence))
                            except sqlite3.IntegrityError as exc:
                                check('pending update collision cannot replace immutable target ' + table + str(key) + mode + str(identical_target),
                                      'immutable resolved producer history' in str(exc)
                                      and f.conn.execute(f'SELECT {terminal} FROM {table} WHERE rowid=?', (sequence,)).fetchone()[0] == pending[terminal])
                            else:
                                raise AssertionError('pending update collision silently accepted')
                            raise FixtureRollback()
                    except FixtureRollback:
                        pass
                    unchanged_checkpoint = f.producer.checkpoint_manifest() == checkpoint
                    f.reopen()
                    check('pending collision fixture rollback and cold original bytes',
                          unchanged_checkpoint and row_snapshot(f.producer) == before)


def main():
    with tempfile.TemporaryDirectory(prefix='producer-lifecycle-') as directory:
        root = Path(directory)
        timers(root)
        crashcuts(root)
        delivery(root)
    print('LIFECYCLE_CHECKS', len(CHECKS))
    print('RESULT IMPLEMENTED_PENDING_PROJECT_REVIEW')


if __name__ == '__main__':
    main()
