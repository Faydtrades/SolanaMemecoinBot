"""A1 producer-only differential and checkpoint fixtures; temporary SQLite only."""
from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
from contextlib import closing
from unittest.mock import patch
from datetime import timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))
from live.continuous_producer_v0_1 import (
    LiveContinuousProducerV01, ProducerConflict, connect_producer_db, TABLES,
)
from phase4 import paper_continuous_firstpullback_binding_v0_1 as accepted
from phase4.paper_continuous_firstpullback_binding_v0_5 import (
    ContinuousFirstPullbackBindingV05, connect_atomic_entry_router_db,
)
from phase4.paper_continuous_market_source_v0_2 import ContinuousMarketSourceV02
from phase4_continuous_firstpullback_binding_selftest_v0_1 import (
    BASE, SOURCE_IDENTITY, create_fixture, copy_fixture_rows, insert_row,
)

CHECKS = []


def check(label, result):
    if not result:
        raise AssertionError(label)
    CHECKS.append(label)
    print("OK", label)


def source(path, *, identity=SOURCE_IDENTITY, anchor=0):
    return ContinuousMarketSourceV02(path, start_after_p1_rowid=anchor, database_identity=identity)


def open_live(path, fixture, **kwargs):
    conn = connect_producer_db(path)
    try:
        return conn, LiveContinuousProducerV01(conn, source(fixture), wall_clock=lambda: BASE, **kwargs)
    except BaseException:
        conn.close()
        raise


def reject(label, operation):
    try:
        operation()
    except (ProducerConflict, sqlite3.DatabaseError):
        check(label, True)
    else:
        check(label, False)


def state(producer):
    result = {}
    for table in TABLES:
        if table.endswith("runtime_v0_1"):
            continue
        rows = []
        for row in producer.conn.execute(f"SELECT * FROM {table} ORDER BY rowid"):
            values = dict(row)
            for field in ("delivered", "accepted_evaluation_id"):
                values.pop(field, None)
            rows.append(values)
        result[table] = rows
    result["engine"] = accepted._primitive(producer._engine._tokens)
    result["cursor"] = producer.durable_p1_rowid
    result["sequence"] = producer.next_event_sequence
    result["launches"] = dict(producer.market_source._launch_by_mint)
    return result


def drain(producer, batch_size=3):
    while producer.process_next_batch(batch_size=batch_size).raw_rows_fetched:
        pass


def main():
    with tempfile.TemporaryDirectory(prefix="live-producer-a1-") as directory:
        root = Path(directory)
        template, fixture = root / "template.sqlite", root / "source.sqlite"
        create_fixture(template)
        # A launch-only unfinished mint has no t0 and must not be aged out.
        with closing(sqlite3.connect(template)) as conn:
            insert_row(conn, 35, "NO-T0", "LAUNCH", observed_offset=70)
            insert_row(conn, 36, "NO-T0", "BUY", observed_offset=1000)
            insert_row(conn, 37, "NO-T0", "BUY", price_units=10600, observed_offset=1001)
            conn.commit()
        baseline_conn = connect_atomic_entry_router_db(root / "accepted.sqlite")
        copy_fixture_rows(template, fixture, through_rowid=12)
        baseline = ContinuousFirstPullbackBindingV05(baseline_conn, source(fixture), wall_clock=lambda: BASE)
        live_path = root / "live.sqlite"
        conn, live = open_live(live_path, fixture)
        drain(baseline)
        drain(live)
        check("accepted unfinished per-mint planner differential", state(live) == state(baseline))
        before = state(live)
        manifest = live.checkpoint()
        identity = live.source_identity
        conn.close()
        conn, live = open_live(live_path, fixture)
        check("checkpoint/reopen exact engine runs deadlines cursor output sequence", state(live) == before)
        check("same producer lineage and manifest across reopen", live.source_identity == identity and live.checkpoint_manifest() == manifest)
        check("launch registry hydration avoids prefix reconstruction", live.market_source.cache_metrics()["full_launch_rebuild_queries"] == 0)

        copy_fixture_rows(template, fixture, through_rowid=14)
        drain(baseline)
        drain(live)
        check("next canonical candidate identity/order equals accepted v0.5", state(live) == state(baseline))
        candidates = [r for r in live.producer_events() if r["event_type"] == "CandidateEvaluationEvent"]
        check("candidate fixture actually produced winner", len(candidates) == 1)
        original_candidate = candidates[0]
        before = state(live)
        conn.close()
        conn, live = open_live(live_path, fixture)
        check("restart after candidate preserves original signal bytes", state(live) == before and candidates == [r for r in live.producer_events() if r["event_type"] == "CandidateEvaluationEvent"])
        check("producer does not claim output or audit delivery", all(r["delivered"] == 0 for r in live.producer_events()) and conn.execute("SELECT COUNT(*) FROM paper_fp_binding_evaluation_audit_v0_1 WHERE delivered<>0").fetchone()[0] == 0)
        check("no economic runner or tables", not hasattr(live, "runner") and {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")} == set(TABLES) | {"live_producer_checkpoint_v0_1"})

        # Restart with a captured timer and source backlog; newer source rows
        # cannot jump the immutable fence or renew the deadline.
        copy_fixture_rows(template, fixture, through_rowid=16)
        tick = BASE + timedelta(seconds=308)
        key = live.prepare_clock_tick(tick, timer_id="TIMER-A")
        baseline.prepare_clock_tick(tick, drive_exit_clock=False, timer_id="TIMER-A")
        live.prepare_clock_tick(tick, timer_id="TIMER-B")
        baseline.prepare_clock_tick(tick, drive_exit_clock=False, timer_id="TIMER-B")
        original_timer = dict(conn.execute("SELECT * FROM paper_fp_binding_prepared_timers_v0_1 WHERE timer_key=?", (key,)).fetchone())
        copy_fixture_rows(template, fixture, through_rowid=17)
        conn.close()
        conn, live = open_live(live_path, fixture)
        check("incomplete timer key timestamp watermark preserved", original_timer == dict(conn.execute("SELECT * FROM paper_fp_binding_prepared_timers_v0_1 WHERE timer_key=?", (key,)).fetchone()))
        check("timer waits for original source fence", live.execute_prepared_timer(key).status == "PENDING")
        drain(live, 1000)
        drain(baseline, 1000)
        check("source backlog fenced before later row", live.durable_p1_rowid == 16 and state(live) == state(baseline))
        live.execute_prepared_timer(key)
        baseline.execute_prepared_timer(key)
        live.execute_prepared_timer("TIMER-B")
        baseline.execute_prepared_timer("TIMER-B")
        check("accepted strategy timer output/state differential", state(live) == state(baseline))
        check("original candidate unchanged by due timer/reopen", live.producer_events()[original_candidate["event_sequence"]] == original_candidate)
        copy_fixture_rows(template, fixture, through_rowid=35)
        drain(live)
        drain(baseline)
        check("multiple candidates and sparse/gap source differential", state(live) == state(baseline))
        no_t0 = live._engine._tokens["NO-T0"]
        check("fixture retains unfinished launch with no t0", no_t0.first_tradable_event_at_us is None and conn.execute("SELECT COUNT(*) FROM paper_fp_binding_strategy_runs_v0_1 WHERE mint='NO-T0' AND deadline_json IS NULL").fetchone()[0] == 4)
        saved = state(live)
        conn.close()
        conn, live = open_live(live_path, fixture)
        check("no-t0 survives exact retained replay", state(live) == saved)
        copy_fixture_rows(template, fixture, through_rowid=37)
        drain(live)
        drain(baseline)
        check("late first trade keeps original launch age/run identity", state(live) == state(baseline) and live._engine._tokens["NO-T0"].token_created_at_us == no_t0.token_created_at_us)
        check("source cache has no reopen launch prefix scan", live.market_source.cache_metrics()["full_launch_rebuild_queries"] == 0)
        check("SQLite producer integrity", conn.execute("PRAGMA quick_check").fetchone()[0] == "ok" and conn.execute("PRAGMA foreign_key_check").fetchone() is None)
        final_digest = accepted._fingerprint(state(live))
        print("EQUIVALENT_PRODUCER_DIGEST", final_digest)
        print("CANDIDATE_IDS", [json.loads(r["event_json"])["candidate"]["candidate_id"] for r in live.producer_events() if r["event_type"] == "CandidateEvaluationEvent"])
        print("CHECKPOINT_GENERATION", live.checkpoint_manifest()["generation"])
        conn.close()
        baseline_conn.close()

        # Isolated copies for fail-closed malformed checkpoints and binding.
        def open_altered(path, selected_source):
            c = connect_producer_db(path)
            try:
                LiveContinuousProducerV01(c, selected_source, wall_clock=lambda: BASE)
            finally:
                c.close()
        reject("changed source identity rejected", lambda: open_altered(live_path, source(fixture, identity="OTHER")))
        reject("changed original anchor rejected", lambda: open_altered(live_path, source(fixture, anchor=4)))
        for index, sql in enumerate((
                "UPDATE live_producer_checkpoint_v0_1 SET manifest_json='{}'",
                "DELETE FROM paper_fp_binding_feature_events_v0_1 WHERE production_p1_rowid=4",
                "UPDATE paper_fp_binding_strategy_runs_v0_1 SET deadline_json=NULL WHERE mint='MINT-A'",
                "UPDATE paper_fp_binding_outbox_v0_1 SET event_key='ALTERED' WHERE event_sequence=0",
                "UPDATE paper_fp_binding_runtime_v0_1 SET next_event_sequence=9000",
                "DELETE FROM live_producer_checkpoint_v0_1",
        )):
            target = root / f"corrupt-{index}.sqlite"
            with closing(sqlite3.connect(live_path)) as origin, closing(sqlite3.connect(target)) as altered:
                origin.backup(altered)
                altered.execute(sql)
                altered.commit()
            reject(f"malformed checkpoint/state rejected {index}", lambda: open_altered(target, source(fixture)))

        # Well-checksummed but unsupported schema or invalid launch provenance
        # must still fail structurally rather than being treated as a new run.
        for field, value in (("schema", "future-v999"), ("launches", {"NO-T0": 0})):
            target = root / f"invalid-{field}.sqlite"
            with closing(sqlite3.connect(live_path)) as origin, closing(sqlite3.connect(target)) as altered:
                origin.backup(altered)
                payload = json.loads(altered.execute("SELECT manifest_json FROM live_producer_checkpoint_v0_1").fetchone()[0])
                payload[field] = value
                altered.execute("UPDATE live_producer_checkpoint_v0_1 SET manifest_json=?,manifest_digest=?",
                                (accepted._json(payload), accepted._fingerprint(payload)))
                altered.commit()
            reject("unsupported manifest field rejected " + field, lambda: open_altered(target, source(fixture)))

        timer_path = root / "order.sqlite"
        c, p = open_live(timer_path, fixture)
        p.prepare_clock_tick(BASE, timer_id="B")
        p.prepare_clock_tick(BASE, timer_id="A")
        reject("same-watermark timer keys enforce accepted order", lambda: p.execute_prepared_timer("B"))
        c.close()
        c, p = open_live(timer_path, fixture)
        c.execute("UPDATE live_producer_checkpoint_v0_1 SET manifest_json='{}'")
        c.commit()
        reject("same-instance manifest tamper rejected before publication", p.checkpoint)
        c.close()

        # Atomic failure after engine/source advance must leave the previous
        # durable state intact and make the failed object unusable.
        rollback_source = root / "rollback-source.sqlite"
        copy_fixture_rows(template, rollback_source, through_rowid=12)
        rollback_path = root / "rollback.sqlite"
        c, p = open_live(rollback_path, rollback_source)
        drain(p)
        unchanged = state(p)
        for phase in ("before_checkpoint_publish", "after_checkpoint_publish_before_commit"):
            copy_fixture_rows(template, rollback_source, through_rowid=14)
            def fail(key, actual, wanted=phase):
                if actual == wanted:
                    raise ProducerConflict("injected atomic failure")
            p.failure_injector = fail
            reject("atomic injected failure " + phase, lambda: p.process_next_batch(batch_size=2))
            reject("failed instance cannot continue " + phase, lambda: p.process_next_batch())
            c.close()
            c, p = open_live(rollback_path, rollback_source)
            check("rollback/reopen restores previous exact state " + phase, state(p) == unchanged)
        drain(p)
        check("replay after failed publication emits exactly one candidate", len([r for r in p.producer_events() if r["event_type"] == "CandidateEvaluationEvent"]) == 1)
        c2, p2 = open_live(rollback_path, rollback_source)
        p.checkpoint()
        reject("stale concurrent producer generation denied", p2.checkpoint)
        c2.close()
        # Concurrent writer during replay: reopening must retain one snapshot.
        saved = state(p)
        checkpoint_before = p.checkpoint_manifest()
        engine_process = accepted.DenominationAwareFeatureEngineV01.process
        advanced = []
        def publish_during_replay(engine, event):
            if not advanced:
                advanced.append(True)
                p.checkpoint()
            return engine_process(engine, event)
        with patch.object(accepted.DenominationAwareFeatureEngineV01, "process", publish_during_replay):
            c2, p2 = open_live(rollback_path, rollback_source)
        check("concurrent reopen hydrates single published snapshot", state(p2) == saved and p2._generation == checkpoint_before["generation"])
        reject("snapshot reopened before concurrent publish cannot write stale state", p2.checkpoint)
        c2.close()
        # Same generation mutation cannot be blessed by a new publication.
        c.execute("UPDATE paper_fp_binding_outbox_v0_1 SET event_json='{}' WHERE event_sequence=0")
        c.commit()
        reject("same-instance retained-state tamper rejected before publication", p.checkpoint)
        c.close()
        # Source highwater guard is also checked during an active instance.
        check_path = root / "highwater.sqlite"
        c, p = open_live(check_path, rollback_source)
        drain(p)
        # A source regression is never repaired with a fresh anchor.
        with closing(sqlite3.connect(rollback_source)) as s:
            s.execute("DELETE FROM pump_events WHERE rowid>=14")
            s.commit()
        reject("source highwater regression rejected during continuation", p.process_next_batch)
        c.close()
        reject("source highwater regression rejected on reopen", lambda: open_live(check_path, rollback_source))
        print("A1_CHECKS", len(CHECKS))
        print("RESULT IMPLEMENTED_PENDING_PROJECT_REVIEW")


if __name__ == "__main__":
    main()
