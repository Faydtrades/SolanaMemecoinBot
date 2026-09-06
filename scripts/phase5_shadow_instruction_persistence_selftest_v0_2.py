from __future__ import annotations

import json
import sqlite3
import tempfile
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

import phase5_shadow_continuous_execution_selftest_v0_1 as fx
from phase5 import shadow_simulation_repository_v0_1 as repo


def rows(db, table):
    return [tuple(row) for row in db.execute(f"SELECT * FROM {table} ORDER BY 1,2")]


def legacy_fixture(path):
    """Recreate only this test-owned DB's exact pre-correction relation."""
    with closing(sqlite3.connect(path)) as db, db:
        before = rows(db, "shadow_t003_plan_instructions")
        db.execute("BEGIN IMMEDIATE")
        db.execute("""CREATE TABLE legacy_instructions (
            plan_id TEXT NOT NULL REFERENCES shadow_t003_plans(plan_id), sequence INTEGER NOT NULL,
            instruction_id TEXT NOT NULL UNIQUE, content_fingerprint TEXT NOT NULL,
            instruction_json TEXT NOT NULL, PRIMARY KEY(plan_id,sequence))""")
        db.execute("INSERT INTO legacy_instructions SELECT * FROM shadow_t003_plan_instructions")
        db.execute("DROP TABLE shadow_t003_plan_instructions")
        db.execute("ALTER TABLE legacy_instructions RENAME TO shadow_t003_plan_instructions")
        repo._execute_schema_sql(db, repo._append_only_triggers("shadow_t003_plan_instructions"))
        db.execute("DROP TABLE shadow_t003_persistence_contract")
        assert rows(db, "shadow_t003_plan_instructions") == before
        assert repo._instruction_layout(db) == "legacy"


def snapshot(path):
    with closing(sqlite3.connect(path)) as db:
        return list(db.iterdump())


def main():
    checks = {}
    def check(name, condition):
        if not condition:
            raise AssertionError(name)
        checks[name] = True
    with tempfile.TemporaryDirectory(prefix="t003_relation_", dir=fx.ROOT / "data" / "shadow") as directory:
        root = Path(directory)
        def fixture(name, entries=1, exits=False):
            paper, shadow = root / (name + ".paper.sqlite3"), root / (name + ".shadow.sqlite3")
            fx.paper_fixture(paper, entries=entries, exits=exits)
            net = fx.Network(venue="swap", base=True)
            with net.rpc() as rpc, fx.c.ContinuousShadowExecutionV01(paper, shadow, fx.fx.ACTOR, rpc, clock_us=lambda: fx.fx.BASE_US+100) as app:
                app.cycle()
                check(name + " success", all(row[0] == "COMPLETED" for row in app.conn.execute("SELECT current_state FROM shadow_state_machines")))
            return paper, shadow
        paper, shadow = fixture("shared", entries=2, exits=True)
        with repo.open_simulation_repository(shadow) as app:
            shared = app._conn.execute("SELECT instruction_id,COUNT(DISTINCT plan_id) FROM shadow_t003_plan_instructions GROUP BY instruction_id HAVING COUNT(DISTINCT plan_id)>1").fetchall()
            check("same content identity shared across plans", len(shared) > 0)
            check("all plans exact ordered instructions", app.audit())
            before = rows(app._conn, "shadow_t003_plan_instructions")
            row = before[0]
            check("duplicate occurrence rejected", fx.fx.expect(sqlite3.IntegrityError, lambda: app._conn.execute("INSERT INTO shadow_t003_plan_instructions VALUES(?,?,?,?,?)", row)))
            app._conn.rollback()
            check("duplicate id in same plan rejected", fx.fx.expect(sqlite3.IntegrityError, lambda: app._conn.execute("INSERT INTO shadow_t003_plan_instructions VALUES(?,?,?,?,?)", (row[0], 999, *row[2:]))))
            app._conn.rollback()
            check("sequence unique per plan", fx.fx.expect(sqlite3.IntegrityError, lambda: app._conn.execute("INSERT INTO shadow_t003_plan_instructions VALUES(?,?,?,?,?)", (row[0], row[1], "different", *row[3:]))))
            app._conn.rollback()
            check("append-only unchanged", fx.fx.expect(sqlite3.IntegrityError, lambda: app._conn.execute("UPDATE shadow_t003_plan_instructions SET instruction_json='{}'")))
            app._conn.rollback()
            check("cross-plan ordering unchanged", before == rows(app._conn, "shadow_t003_plan_instructions"))
            digest = app.canonical_digest()
        with repo.open_simulation_repository(shadow) as app:
            check("reopen digest deterministic", app.canonical_digest() == digest)
            check("foreign keys", app._conn.execute("PRAGMA foreign_key_check").fetchall() == [])
            check("quick check", app.quick_check() == "ok")
        # Replay uses the identical accepted plan object rebuilt from C2's journal.
        net = fx.Network(venue="swap", base=True)
        with net.rpc() as rpc, fx.c.ContinuousShadowExecutionV01(paper, shadow, fx.fx.ACTOR, rpc) as app:
            identity = app.conn.execute("SELECT intent_id FROM shadow_execution_intents WHERE role='ENTRY' ORDER BY intent_id LIMIT 1").fetchone()[0]
            intent = app.shadow.get_intent(identity)
            app._pipeline(intent, app._read(identity, "start")["at_us"], replay_only=True)
            check("exact plan replay idempotent", before == rows(app.conn, "shadow_t003_plan_instructions") and not net.calls)
            target = app.conn.execute("SELECT x.plan_id,x.sequence,x.instruction_json FROM shadow_t003_plan_instructions x JOIN shadow_t003_plans p USING(plan_id) WHERE p.intent_id=? ORDER BY x.sequence LIMIT 1", (identity,)).fetchone()
            with app.conn:
                app.conn.execute("DROP TRIGGER shadow_t003_plan_instructions_immutable_update")
                app.conn.execute("UPDATE shadow_t003_plan_instructions SET instruction_json='{}' WHERE plan_id=? AND sequence=?", (target[0], target[1]))
            check("same plan conflicting persisted instruction replay rejected", fx.fx.expect(fx.c.d.ShadowDeterminismConflict, lambda: app._pipeline(intent, app._read(identity, "start")["at_us"], replay_only=True)))
            with app.conn:
                app.conn.execute("UPDATE shadow_t003_plan_instructions SET instruction_json=? WHERE plan_id=? AND sequence=?", (target[2], target[0], target[1]))
                repo._execute_schema_sql(app.conn, repo._append_only_triggers("shadow_t003_plan_instructions"))
            check("other plans unaffected by failed replay", rows(app.conn, "shadow_t003_plan_instructions") == before)
        _, legacy = fixture("legacy")
        with repo.open_simulation_repository(legacy) as app:
            old_digest = app.canonical_digest()
            tables = [row[0] for row in app._conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'shadow_t003_%' AND name<>'shadow_t003_persistence_contract'")]
            all_rows = {table: rows(app._conn, table) for table in tables}
        legacy_fixture(legacy)
        with repo.open_simulation_repository(legacy) as app:
            check("legacy migrates", repo._instruction_layout(app._conn) == "corrected")
            check("all original rows exact", all(rows(app._conn, table) == value for table, value in all_rows.items()))
            check("economic digest unchanged by migration", app.canonical_digest() == old_digest)
            check("new persistence binding", app._conn.execute("SELECT persistence_fingerprint FROM shadow_t003_persistence_contract").fetchone()[0] == repo.PERSISTENCE_FINGERPRINT)
        migrated = snapshot(legacy)
        with repo.open_simulation_repository(legacy) as app:
            check("second open audit", app.audit())
        check("second open no migration changes", snapshot(legacy) == migrated)
        for mutation in ("payload", "sequence", "foreign_key", "copy_failure", "post_copy_failure"):
            _, path = fixture("invalid_" + mutation)
            legacy_fixture(path)
            if mutation in ("payload", "sequence", "foreign_key"):
                with closing(sqlite3.connect(path)) as db, db:
                    db.execute("DROP TRIGGER shadow_t003_plan_instructions_immutable_update")
                    column, value = {"payload": ("instruction_json", "{}"), "sequence": ("sequence", 999),
                                     "foreign_key": ("plan_id", "missing-plan")}[mutation]
                    db.execute(f"UPDATE shadow_t003_plan_instructions SET {column}=? WHERE rowid=(SELECT MIN(rowid) FROM shadow_t003_plan_instructions)", (value,))
                    repo._execute_schema_sql(db, repo._append_only_triggers("shadow_t003_plan_instructions"))
            original = snapshot(path)
            original_audit = repo.ShadowSimulationRepositoryV01.audit
            calls = [0]
            def fail_after_copy(self):
                calls[0] += 1
                if calls[0] == 2:
                    raise fx.c.d.ShadowDeterminismConflict("injected post-copy audit failure")
                return original_audit(self)
            if mutation == "copy_failure":
                with closing(sqlite3.connect(path)) as db, db:
                    db.execute("CREATE TABLE shadow_t003_plan_instructions_migrating(sentinel TEXT)")
                original = snapshot(path)
            if mutation == "post_copy_failure":
                with patch.object(repo.ShadowSimulationRepositoryV01, "audit", fail_after_copy):
                    check(mutation + " fail closed", fx.fx.expect(fx.c.d.ShadowDeterminismConflict, lambda: repo.open_simulation_repository(path)))
            else:
                check(mutation + " fail closed", fx.fx.expect((fx.c.d.ShadowDeterminismConflict, sqlite3.OperationalError), lambda: repo.open_simulation_repository(path)))
            check(mutation + " rollback exact", snapshot(path) == original)
    print(json.dumps({"checks": checks, "check_count": len(checks),
        "old_persistence_fingerprint": repo.LEGACY_PERSISTENCE_FINGERPRINT,
        "new_persistence_fingerprint": repo.PERSISTENCE_FINGERPRINT}, sort_keys=True))
    print("RESULT: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
