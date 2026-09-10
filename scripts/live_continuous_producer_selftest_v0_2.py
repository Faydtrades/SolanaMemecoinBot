"""A2 actual-producer differential, finite-profile and immutable-history fixtures."""
from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
import time
from contextlib import closing
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "scripts")]
from live.continuous_producer_v0_2 import (
    LiveContinuousProducerV02, ContinuationProfileV02, LAUNCHES, RETIRED, COUNTERS,
    FEATURES, RUNS, TABLES, accepted,
)
from live_continuous_producer_selftest_v0_1 import (
    BASE, source, create_fixture, copy_fixture_rows, insert_row, connect_producer_db,
    LiveContinuousProducerV01, ProducerConflict, ContinuousFirstPullbackBindingV05,
    connect_atomic_entry_router_db,
)
from phase4_continuous_firstpullback_binding_selftest_v0_1 import SCHEMA_SQL

CHECKS = []


def check(label, result):
    if not result:
        raise AssertionError(label)
    CHECKS.append(label)
    print("OK", label)


def reject(label, call):
    try:
        call()
    except (ProducerConflict, sqlite3.DatabaseError):
        check(label, True)
    else:
        check(label, False)


def open_producer(path, fixture, *, cls=LiveContinuousProducerV02, **kwargs):
    conn = connect_producer_db(path)
    try:
        return conn, cls(conn, source(fixture), wall_clock=lambda: BASE, **kwargs)
    except BaseException:
        conn.close()
        raise


def open_close(path, fixture, **kwargs):
    conn, producer = open_producer(path, fixture, **kwargs)
    conn.close()


def drain(producer, batch_size=32):
    while producer.process_next_batch(batch_size=batch_size).raw_rows_fetched:
        pass


def outputs(producer):
    result = []
    for row in producer.conn.execute(f"SELECT * FROM {TABLES[5]} ORDER BY event_sequence"):
        values = dict(row)
        values.pop("delivered")
        result.append(values)
    return result


def audits(producer):
    result = []
    for row in producer.conn.execute(f"SELECT * FROM {TABLES[4]} ORDER BY input_key,ordinal"):
        values = dict(row)
        values.pop("delivered")
        values.pop("accepted_evaluation_id")
        result.append(values)
    return result


def active(producer):
    return accepted._primitive(producer._engine._tokens)


def copy_db(origin, target):
    with closing(sqlite3.connect(origin)) as a, closing(sqlite3.connect(target)) as b:
        a.backup(b)


def append(fixture, rows):
    with closing(sqlite3.connect(fixture)) as conn:
        for rowid, mint, kind, price, offset in rows:
            insert_row(conn, rowid, mint, kind, price_units=price, observed_offset=offset)
        conn.commit()


def empty_source(path):
    with closing(sqlite3.connect(path)) as conn:
        conn.execute(SCHEMA_SQL)
        conn.commit()


def differential(root):
    template, fixture = root / "template.sqlite", root / "source.sqlite"
    create_fixture(template)
    append(template, [(35, "NO-T0", "LAUNCH", 10000, 70), (36, "NO-T0", "BUY", 10000, 1000)])
    copy_fixture_rows(template, fixture, through_rowid=12)
    path = root / "live.sqlite"
    c, a1 = open_producer(path, fixture, cls=LiveContinuousProducerV01)
    baseline_conn = connect_atomic_entry_router_db(root / "accepted.sqlite")
    baseline = ContinuousFirstPullbackBindingV05(baseline_conn, source(fixture), wall_clock=lambda: BASE)
    drain(a1, 3)
    drain(baseline, 3)
    saved, lineage, sequence = outputs(a1), a1.source_identity, a1.next_event_sequence
    engine_before = active(a1)
    c.close()
    c, producer = open_producer(path, fixture)
    check("bounded A1 migration preserves lineage cursor sequence and exact active replay",
          producer.source_identity == lineage and producer.next_event_sequence == sequence
          and outputs(producer) == saved and active(producer) == engine_before)
    copy_fixture_rows(template, fixture, through_rowid=14)
    drain(producer, 3)
    drain(baseline, 3)
    check("actual accepted next canonical candidate and audit bytes unchanged",
          outputs(producer) == outputs(baseline) and audits(producer) == audits(baseline))
    check("winner feature/run history retired only after exact output persistence",
          "MINT-A" not in producer._engine._tokens
          and c.execute(f"SELECT 1 FROM {FEATURES} WHERE mint='MINT-A'").fetchone() is None
          and c.execute(f"SELECT 1 FROM {RUNS} WHERE mint='MINT-A'").fetchone() is None
          and producer._retired("MINT-A")["reason"] == "WINNER_SUPPRESSED")
    expected_runs = tuple(dict(row) for row in baseline_conn.execute(f"SELECT * FROM {RUNS} WHERE mint='MINT-A' ORDER BY role_rank"))
    check("permanent original run parameter deadline inputs available to A3", producer.original_runs("MINT-A") == expected_runs)
    candidates_before = [row for row in outputs(producer) if row["event_type"] == "CandidateEvaluationEvent"]
    checkpoint = producer.checkpoint_manifest()
    c.close()
    c, producer = open_producer(path, fixture)
    check("winner reopen reads no retired bundles and replays only unfinished features",
          producer.metrics["retired_bundle_rows_read"] == 0 and producer.metrics["replayed_events"] == 2)
    check("reopen stays same checkpoint and original candidate identity", producer.checkpoint_manifest() == checkpoint
          and candidates_before == [row for row in outputs(producer) if row["event_type"] == "CandidateEvaluationEvent"])
    copy_fixture_rows(template, fixture, through_rowid=16)
    for binding in (producer, baseline):
        binding.prepare_clock_tick(BASE + timedelta(seconds=308), drive_exit_clock=False, timer_id="B")
        binding.prepare_clock_tick(BASE + timedelta(seconds=308), drive_exit_clock=False, timer_id="A")
    copy_fixture_rows(template, fixture, through_rowid=17)
    c.close()
    c, producer = open_producer(path, fixture)
    reject("same-watermark timer order retained", lambda: producer.execute_prepared_timer("B"))
    c.close()
    c, producer = open_producer(path, fixture)
    check("captured timer waits for original watermark", producer.execute_prepared_timer("A").status == "PENDING")
    for binding in (producer, baseline):
        drain(binding, 256)
    check("sparse bounded source fence stops at captured watermark", producer.durable_p1_rowid == 16)
    for binding in (producer, baseline):
        binding.execute_prepared_timer("A")
        binding.execute_prepared_timer("B")
    check("all locked roles terminal retirement follows accepted timer output",
          producer._retired("MINT-B")["reason"] == "ALL_LOCKED_ROLES_TERMINAL"
          and outputs(producer) == outputs(baseline) and audits(producer) == audits(baseline))
    copy_fixture_rows(template, fixture, through_rowid=35)
    for binding in (producer, baseline):
        drain(binding, 3)
    check("later retired rows still emit exact independent market observations without duplicate winner",
          outputs(producer) == outputs(baseline) and audits(producer) == audits(baseline)
          and sum(row["event_type"] == "CandidateEvaluationEvent" for row in outputs(producer)) == 2)
    check("unfinished no-t0 has all original no-deadline runs", producer._engine._tokens["NO-T0"].first_tradable_event_at_us is None
          and all(row["deadline_json"] is None for row in producer.original_runs("NO-T0")))
    producer.submit_clock_tick(BASE + timedelta(days=1000))
    baseline.submit_clock_tick(BASE + timedelta(days=1000), drive_exit_clock=False)
    no_t0 = active(producer)
    c.close()
    c, producer = open_producer(path, fixture)
    check("no-t0 never expires by age and survives complete replay", active(producer) == no_t0 and "NO-T0" in no_t0)
    copy_fixture_rows(template, fixture, through_rowid=36)
    for binding in (producer, baseline):
        drain(binding, 3)
    check("late first trade keeps accepted launch age and strategy output", outputs(producer) == outputs(baseline)
          and active(producer)["NO-T0"] == active(baseline)["NO-T0"])
    check("original winning runs immutable after later observations", producer.original_runs("MINT-A") == expected_runs)
    check("all unresolved outputs/audits stay unacknowledged", c.execute(f"SELECT COUNT(*) FROM {TABLES[5]} WHERE delivered<>0").fetchone()[0] == 0
          and c.execute(f"SELECT COUNT(*) FROM {TABLES[4]} WHERE delivered<>0").fetchone()[0] == 0)
    all_events = outputs(producer)
    paged, after = [], -1
    while page := producer.producer_events(after_sequence=after, limit=2):
        paged.extend({key: value for key, value in row.items() if key != "delivered"} for row in page)
        after = page[-1]["event_sequence"]
    check("bounded A3 event pages preserve original causal ordering", paged == all_events)
    reject("oversized delivery page denied", lambda: producer.producer_events(limit=257))
    reject("oversized source batch denied", lambda: producer.process_next_batch(batch_size=257))
    check("dedicated producer store has no economic runner", not hasattr(producer, "runner"))
    print("A2_EQUIVALENT_OUTPUT_DIGEST", accepted._fingerprint(all_events))
    print("A2_CANDIDATE_IDS", [json.loads(row["event_json"])["candidate"]["candidate_id"] for row in all_events if row["event_type"] == "CandidateEvaluationEvent"])
    check("isolated producer SQLite integrity and foreign keys", c.execute("PRAGMA quick_check").fetchone()[0] == "ok"
          and c.execute("PRAGMA foreign_key_check").fetchone() is None)
    c.close()
    baseline_conn.close()
    return path, fixture


def fail_closed(root, path, fixture):
    changes = [
        f"UPDATE {TABLES[5]} SET event_json='{{}}' WHERE event_sequence=0",
        f"DELETE FROM {FEATURES} WHERE mint='NO-T0'",
        f"UPDATE {COUNTERS} SET retired_rows=retired_rows+1",
        f"DROP TRIGGER {RETIRED}_delete",
    ]
    for index, sql in enumerate(changes):
        target = root / f"tampered-{index}.sqlite"
        copy_db(path, target)
        with closing(sqlite3.connect(target)) as c:
            c.execute(sql)
            c.commit()
        reject(f"bounded tamper denied on reopen {index}", lambda: open_close(target, fixture))
    c, p = open_producer(path, fixture)
    reject("immutable tombstone cannot be deleted", lambda: c.execute(f"DELETE FROM {RETIRED} WHERE mint='MINT-A'"))
    c.rollback()
    reject("immutable original launch cannot be altered", lambda: c.execute(f"UPDATE {LAUNCHES} SET launch_rowid=20 WHERE mint='MINT-A'"))
    c.rollback()
    c.close()
    reject("reopen cannot silently change numerical profile", lambda: open_close(path, fixture, profile=replace(ContinuationProfileV02(), active_mints=129)))
    for field, value in (("storage", "future-v999"), ("launches", {"NO-T0": 0})):
        target = root / ("version-" + field + ".sqlite")
        copy_db(path, target)
        with closing(sqlite3.connect(target)) as c:
            payload = json.loads(c.execute("SELECT manifest_json FROM live_producer_checkpoint_v0_1").fetchone()[0])
            payload[field] = value
            c.execute("UPDATE live_producer_checkpoint_v0_1 SET manifest_json=?,manifest_digest=?", (accepted._json(payload), accepted._fingerprint(payload)))
            c.commit()
        reject("unsupported storage/provenance denied " + field, lambda: open_close(target, fixture))

    template, fixture2 = root / "atomic-template.sqlite", root / "atomic-source.sqlite"
    create_fixture(template)
    copy_fixture_rows(template, fixture2, through_rowid=12)
    target = root / "atomic.sqlite"
    c, p = open_producer(target, fixture2)
    drain(p, 3)
    unchanged, manifest = active(p), p.checkpoint_manifest()
    copy_fixture_rows(template, fixture2, through_rowid=14)
    for phase in ("before_terminal_retirement", "after_terminal_retirement", "before_checkpoint_publish", "after_checkpoint_publish_before_commit"):
        def fail(key, actual, wanted=phase):
            if actual == wanted:
                raise ProducerConflict("injected failure")
        p.failure_injector = fail
        reject("atomic cut " + phase, lambda: p.process_next_batch(batch_size=2))
        reject("poisoned in-memory continuation denied " + phase, p.checkpoint)
        c.close()
        c, p = open_producer(target, fixture2)
        check("atomic reopen retains exact previous active state " + phase,
              p.checkpoint_manifest() == manifest and active(p) == unchanged and p._retired("MINT-A") is None)
    drain(p, 3)
    check("atomic retry produces exactly one original winner", sum(row["event_type"] == "CandidateEvaluationEvent" for row in outputs(p)) == 1)
    c2, p2 = open_producer(target, fixture2)
    p.checkpoint()
    reject("stale bounded producer generation denied", p2.checkpoint)
    c2.close()
    c.execute(f"UPDATE {TABLES[5]} SET event_json='{{}}' WHERE event_sequence=0")
    c.commit()
    reject("same-instance pending tamper denied before publication", p.checkpoint)
    c.close()


def exhaustion(root):
    profiles = {
        "active": replace(ContinuationProfileV02(), active_mints=1),
        "hot_events": replace(ContinuationProfileV02(), hot_mint_events=1),
        "aggregate_events": replace(ContinuationProfileV02(), retained_events=1),
        "hot_bytes": replace(ContinuationProfileV02(), hot_mint_bytes=100),
        "retained_bytes": replace(ContinuationProfileV02(), retained_bytes=100),
        "pending_rows": replace(ContinuationProfileV02(), pending_rows=1),
        "pending_bytes": replace(ContinuationProfileV02(), pending_bytes=100),
        "history_rows": replace(ContinuationProfileV02(), history_rows=1),
        "history_bytes": replace(ContinuationProfileV02(), history_bytes=1),
        "record_bytes": replace(ContinuationProfileV02(), record_bytes=100),
    }
    for name, profile in profiles.items():
        fixture, path = root / (name + "-source.sqlite"), root / (name + ".sqlite")
        empty_source(fixture)
        c, p = open_producer(path, fixture, profile=profile)
        initial = p.checkpoint_manifest()
        mint2 = "ONE" if name == "hot_events" else "TWO"
        append(fixture, [(1, "ONE", "LAUNCH", 10000, 1), (2, mint2, "LAUNCH", 10000, 2)])
        reject("explicit profile exhaustion " + name, lambda: p.process_next_batch(batch_size=2))
        c.close()
        c, p = open_producer(path, fixture, profile=profile)
        check("exhaustion preserves original checkpoint and unresolved source " + name,
              p.checkpoint_manifest() == initial and p.durable_p1_rowid == 0)
        c.close()
    # Migration denies unsupported retained input before A1's full replay.
    fixture, path = root / "legacy-source.sqlite", root / "legacy.sqlite"
    empty_source(fixture)
    append(fixture, [(1, "OLD", "LAUNCH", 10000, 1), (2, "OLD", "LAUNCH", 10000, 2)])
    c, p = open_producer(path, fixture, cls=LiveContinuousProducerV01)
    drain(p, 2)
    original = p.checkpoint_manifest()
    c.close()
    reject("oversized A1 migration fails closed without new lineage", lambda: open_close(path, fixture, profile=replace(ContinuationProfileV02(), retained_events=1)))
    c, p = open_producer(path, fixture, cls=LiveContinuousProducerV01)
    check("denied migration leaves valid A1 checkpoint unchanged", p.checkpoint_manifest() == original)
    c.close()


def measured_load(root):
    # All terminal history is produced by the accepted live planner from actual
    # normalized fixtures; no precomputed candidate/tombstone insertion or ack.
    measurements = []
    pattern = [("LAUNCH", 10000), ("BUY", 10000), ("BUY", 10600), ("BUY", 10600),
               ("SELL", 9400), ("SELL", 9400), ("BUY", 9700), ("BUY", 9900), ("BUY", 10100)]
    for count in (8, 64, 256):
        fixture, path = root / f"load-{count}-source.sqlite", root / f"load-{count}.sqlite"
        empty_source(fixture)
        rows = []
        rowid = 0
        for mint_number in range(count):
            for kind, price in pattern:
                rowid += 1
                rows.append((rowid, f"WIN-{mint_number:04}", kind, price, rowid))
        rowid += 1
        rows.append((rowid, "UNFINISHED", "LAUNCH", 10000, rowid))
        append(fixture, rows)
        c, p = open_producer(path, fixture)
        start = time.perf_counter()
        drain(p, 64)
        build_ms = round((time.perf_counter() - start) * 1000, 3)
        check(f"load {count} uses actual accepted candidates and terminal retirement", c.execute(f"SELECT COUNT(*) FROM {RETIRED}").fetchone()[0] == count
              and sum(row["event_type"] == "CandidateEvaluationEvent" for row in outputs(p)) == count)
        pending = p.last_profile_usage["pending_rows"]
        c.close()
        start = time.perf_counter()
        c, p = open_producer(path, fixture)
        reopen_ms = round((time.perf_counter() - start) * 1000, 3)
        check(f"load {count} reopen only replays exact single unfinished event", p.metrics["replayed_events"] == 1
              and p.metrics["retired_bundle_rows_read"] == 0 and p.last_profile_usage["active_mints"] == 1)
        before = dict(p.metrics)
        append(fixture, [(rowid + 1, "WIN-0000", "BUY", 10200, rowid + 1)])
        start = time.perf_counter()
        p.process_next_batch(batch_size=1)
        current_ms = round((time.perf_counter() - start) * 1000, 3)
        lookups = p.metrics["retired_bundle_rows_read"] - before["retired_bundle_rows_read"]
        check(f"load {count} current retired row needs one terminal bundle and emits one observation",
              lookups == 1 and outputs(p)[-1]["event_type"] == "MarketObservationEvent"
              and sum(row["event_type"] == "CandidateEvaluationEvent" for row in outputs(p)) == count)
        plans = [tuple(row) for row in c.execute(f"EXPLAIN QUERY PLAN SELECT * FROM {RETIRED} WHERE mint=?", ("WIN-0000",))]
        check(f"load {count} uses indexed terminal lookup", any("SEARCH" in row[3] and "INDEX" in row[3] for row in plans))
        measurement = {"retired_mints": count, "build_ms": build_ms, "reopen_ms": reopen_ms,
                       "current_one_row_ms": current_ms, "reopen_replayed_events": 1,
                       "reopen_retired_bundle_reads": 0, "current_retired_bundle_reads": lookups,
                       "pending_rows_before_current": pending, "profile_usage": p.last_profile_usage,
                       "store_bytes": sum(file.stat().st_size for file in (path, Path(str(path) + "-wal")) if file.exists())}
        print("A2_MEASUREMENT", json.dumps(measurement, sort_keys=True))
        measurements.append(measurement)
        c.close()
    check("retired history scales while active replay and terminal point reads remain fixed",
          {m["reopen_replayed_events"] for m in measurements} == {1}
          and {m["current_retired_bundle_reads"] for m in measurements} == {1})
    check("unresolved pending growth explicitly measured within finite profile", all(
        left["pending_rows_before_current"] < right["pending_rows_before_current"]
        for left, right in zip(measurements, measurements[1:])))


def captured_and_byte_bounds(root):
    fixture, path = root / "capture-source.sqlite", root / "capture.sqlite"
    empty_source(fixture)
    append(fixture, [(1, "CAPTURED", "LAUNCH", 10000, 1), (2, "CAPTURED", "BUY", 10000, 2)])
    baseline_conn, baseline = open_producer(root / "capture-baseline.sqlite", fixture, cls=LiveContinuousProducerV01)
    drain(baseline, 2)
    c, p = open_producer(path, fixture)
    original_normalize = p.market_source._normalize_rows
    def mutate_after_capture(*args, **kwargs):
        result = original_normalize(*args, **kwargs)
        with closing(sqlite3.connect(fixture)) as writer:
            writer.execute("UPDATE pump_events SET event_key='CHANGED-AFTER-CAPTURE' WHERE rowid=1")
            writer.commit()
        return result
    with patch.object(p.market_source, "_normalize_rows", mutate_after_capture):
        p.process_next_batch(batch_size=2)
    check("captured source mutation cannot change hydrated accepted event", outputs(p) == outputs(baseline)
          and active(p) == active(baseline) and p.reconstruction_queries == 0)
    # A source append during normalization belongs to the next captured batch,
    # where the old mint's original launch is obtained by point lookup.
    append(fixture, [(3, "NEW", "LAUNCH", 10000, 3)])
    def append_after_capture(*args, **kwargs):
        result = original_normalize(*args, **kwargs)
        append(fixture, [(4, "CAPTURED", "BUY", 10600, 4)])
        return result
    with patch.object(p.market_source, "_normalize_rows", append_after_capture):
        result = p.process_next_batch(batch_size=2)
    check("appended row never enters an uncached captured prefix", result.raw_rows_fetched == 1 and p.durable_p1_rowid == 3)
    p.process_next_batch(batch_size=2)
    check("next captured prefix restores old launch without scan", p.durable_p1_rowid == 4
          and outputs(p)[-1]["event_type"] == "MarketObservationEvent"
          and p.market_source.cache_metrics()["full_launch_rebuild_queries"] == 0)
    c.close()
    baseline_conn.close()

    # A row factory witness would observe payload materialization in Python.
    # Oversized raw, pending, legacy, manifest and terminal payloads must be
    # denied using SQLite sizes before that witness sees the payload bytes.
    for kind in ("source", "pending", "legacy", "manifest", "terminal"):
        fixture, path = root / (kind + "-bytes-source.sqlite"), root / (kind + "-bytes.sqlite")
        empty_source(fixture)
        profile = replace(ContinuationProfileV02(), record_bytes=2048, pending_bytes=16384,
                          retained_bytes=16384, checkpoint_bytes=8192, history_bytes=16384)
        if kind == "terminal":
            profile = replace(profile, pending_bytes=131072)
        cls = LiveContinuousProducerV01 if kind == "legacy" else LiveContinuousProducerV02
        c, p = open_producer(path, fixture, cls=cls, **({} if kind == "legacy" else {"profile": profile}))
        oversized = "X" * 40000
        if kind == "source":
            append(fixture, [(1, "BIG", "LAUNCH", 10000, 1)])
            with closing(sqlite3.connect(fixture)) as writer:
                writer.execute("UPDATE pump_events SET event_key=?", (oversized,))
                writer.commit()
        elif kind == "manifest":
            c.execute("UPDATE live_producer_checkpoint_v0_1 SET manifest_json=?", (oversized,))
            c.commit()
        elif kind == "terminal":
            pattern = [("LAUNCH", 10000), ("BUY", 10000), ("BUY", 10600), ("BUY", 10600),
                       ("SELL", 9400), ("SELL", 9400), ("BUY", 9700), ("BUY", 9900), ("BUY", 10100)]
            append(fixture, [(i + 1, "BIG", kind, price, i + 1) for i, (kind, price) in enumerate(pattern)])
            p.process_next_batch(batch_size=9)
            check("oversized terminal fixture originates in actual accepted winner", p._retired("BIG") is not None)
            # Deliberately corrupt this isolated real-produced bundle to test
            # the point-read byte boundary independently of the schema guard.
            c.execute(f"DROP TRIGGER {RETIRED}_update")
            c.execute(f"UPDATE {RETIRED} SET bundle_json=? WHERE mint='BIG'", (oversized,))
            c.commit()
        else:
            c.execute(f"INSERT INTO {TABLES[1]} VALUES('P1:1','SKIP',1,?,1,'x','x')", (oversized,))
            c.commit()
        loaded = []
        def witness(conn, cursor):
            if oversized in cursor:
                loaded.append(True)
            return sqlite3.Row(conn, cursor)
        c.row_factory = witness
        if kind == "source":
            original_open = p.market_source.open_readonly
            def open_witness(path):
                source_conn = original_open(path)
                source_conn.row_factory = witness
                return source_conn
            with patch.object(p.market_source, "open_readonly", open_witness):
                reject("oversized source denied before normalization", lambda: p.process_next_batch(batch_size=1))
        elif kind == "legacy":
            reject("oversized A1 byte migration denied before payload load", lambda: LiveContinuousProducerV02(c, source(fixture), profile=profile))
        elif kind == "terminal":
            reject("oversized terminal point read denied before payload load", lambda: p.original_runs("BIG"))
        else:
            reject("oversized checkpointed " + kind + " denied before payload load", p.checkpoint)
        check("SQLite byte preflight prevents Python payload load " + kind, not loaded)
        c.close()

    fixture, path = root / "nonempty-limit-source.sqlite", root / "nonempty-limit.sqlite"
    empty_source(fixture)
    profile = replace(ContinuationProfileV02(), hot_mint_events=2)
    append(fixture, [(1, "UNFINISHED", "LAUNCH", 10000, 1), (2, "UNFINISHED", "LAUNCH", 10000, 2)])
    c, p = open_producer(path, fixture, profile=profile)
    p.process_next_batch(batch_size=2)
    saved, manifest = active(p), p.checkpoint_manifest()
    append(fixture, [(3, "UNFINISHED", "LAUNCH", 10000, 3)])
    reject("irreducible hot limit denies growth with unfinished state", lambda: p.process_next_batch(batch_size=1))
    c.close()
    c, p = open_producer(path, fixture, profile=profile)
    check("nonempty unfinished checkpoint survives capacity incident exactly", active(p) == saved and p.checkpoint_manifest() == manifest)
    c.close()


def measured_active(root):
    fixture, path = root / "active-load-source.sqlite", root / "active-load.sqlite"
    empty_source(fixture)
    rows = [(i + 1, "HOT", "LAUNCH", 10000, i + 1) for i in range(256)]
    rows.extend((257 + i, f"ACTIVE-{i:03}", "LAUNCH", 10000, 257 + i) for i in range(127))
    append(fixture, rows)
    c, p = open_producer(path, fixture)
    start = time.perf_counter()
    drain(p, 64)
    build_ms = round((time.perf_counter() - start) * 1000, 3)
    saved = active(p)
    c.close()
    start = time.perf_counter()
    c, p = open_producer(path, fixture)
    reopen_ms = round((time.perf_counter() - start) * 1000, 3)
    check("representative hottest and aggregate unfinished replay is exact", active(p) == saved
          and p.last_profile_usage["active_mints"] == 128 and p.last_profile_usage["hottest_events"] == 256
          and p.metrics["replayed_events"] == 383)
    print("A2_ACTIVE_MEASUREMENT", json.dumps({"build_ms": build_ms, "reopen_ms": reopen_ms,
                                               "profile_usage": p.last_profile_usage}, sort_keys=True))
    c.close()


def main():
    with tempfile.TemporaryDirectory(prefix="live-producer-a2-") as directory:
        root = Path(directory)
        path, fixture = differential(root)
        fail_closed(root, path, fixture)
        exhaustion(root)
        captured_and_byte_bounds(root)
        measured_active(root)
        measured_load(root)
        print("A2_CHECKS", len(CHECKS))
        print("A2_PROFILE", json.dumps(ContinuationProfileV02().__dict__, sort_keys=True))
        print("RESULT IMPLEMENTED_PENDING_PROJECT_REVIEW")


if __name__ == "__main__":
    main()
