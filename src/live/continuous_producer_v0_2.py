"""Bounded continuation of the A1 logical producer (no economic consumer).

Only unfinished features/runs are replayed. Immutable launch and terminal-run
evidence is addressed by mint; unresolved original outputs/audits/inputs remain
in the finite pending profile. No age expiry, acknowledgements, or recent-window
approximation. SQLite integrity/recovery qualification remains Operations work;
ordinary continuation checks the bounded checkpoint and immutable-store guards.
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass

from .continuous_producer_v0_1 import (
    LiveContinuousProducerV01, ProducerConflict, TABLES, SCHEMA_VERSION,
    MODEL_FINGERPRINT, SPEC, accepted,
)

STORAGE_VERSION = "live_producer_continuation_v0.3"
LAUNCHES = "live_producer_launches_v0_2"
RETIRED = "live_producer_retired_v0_2"
COUNTERS = "live_producer_history_counters_v0_2"
FEATURES, RUNS = TABLES[2:4]
PENDING = (TABLES[1], *TABLES[4:])
RESOLVED = {TABLES[1]: "complete=1", TABLES[4]: "delivered=1",
            TABLES[5]: "delivered=1", TABLES[6]: "status='COMPLETE'"}
RESOLVED_COLUMNS = {
    TABLES[1]: "input_key,input_kind,production_p1_rowid,content_fingerprint,complete,created_at,updated_at",
    TABLES[4]: "input_key,ordinal,evaluation_json,content_fingerprint,accepted_evaluation_id,delivered",
    TABLES[5]: "event_sequence,input_key,ordinal,event_key,event_type,event_json,content_fingerprint,delivered",
    TABLES[6]: "timer_key,clock_type,clock_timestamp,drive_exit_clock,source_watermark_p1_rowid,content_fingerprint,status,input_key,created_at,updated_at",
}
RESOLVED_KEYS = {
    TABLES[1]: (('input_key',), ('production_p1_rowid',), ('rowid',)),
    TABLES[4]: (('input_key', 'ordinal'), ('rowid',)),
    TABLES[5]: (('event_sequence',), ('event_key',), ('input_key', 'ordinal'), ('rowid',)),
    TABLES[6]: (('timer_key',), ('input_key',), ('rowid',)),
}
STORAGE_SPEC = {
    "version": STORAGE_VERSION, "lineage_model": MODEL_FINGERPRINT,
    "reconstruction": "COMPLETE_ACTIVE_PER_MINT_REPLAY",
    "retirement": "WINNER_OR_ALL_LOCKED_ROLES_TERMINAL_WITH_ORIGINAL_RUN_BUNDLE",
    "history": "IMMUTABLE_ORIGINAL_RESOLVED_ROWS_PLUS_TERMINAL_BUNDLES_BYTE_BOUNDED_CHECKPOINTED",
    "pending": "INCOMPLETE_INPUT_UNDELIVERED_OUTPUT_AUDIT_INCOMPLETE_TIMER_ONLY",
    "resolved_guards": "UPDATE_DELETE_AND_ALL_UNIQUE_ROWID_INSERT_COLLISIONS_EXACT_REPLAY_IGNORED",
    "source": "SINGLE_CAPTURED_BOUNDED_PREFIX_ACCEPTED_V02_NORMALIZATION",
}
STORAGE_FINGERPRINT = accepted._fingerprint(STORAGE_SPEC)


@dataclass(frozen=True)
class ContinuationProfileV02:
    """Engineering profile, not deployment sizing or indefinite operation."""
    active_mints: int = 128
    hot_mint_events: int = 4096
    hot_mint_bytes: int = 8 * 1024 * 1024
    retained_events: int = 16384
    retained_bytes: int = 32 * 1024 * 1024
    pending_rows: int = 32768
    pending_bytes: int = 64 * 1024 * 1024
    history_rows: int = 100000
    history_bytes: int = 128 * 1024 * 1024
    batch_rows: int = 256
    record_bytes: int = 1024 * 1024
    checkpoint_bytes: int = 1024 * 1024
    delivery_page_rows: int = 256

    def __post_init__(self):
        if any(type(value) is not int or value <= 0 for value in asdict(self).values()):
            raise ProducerConflict("positive integral continuation profile required")
        if self.batch_rows > 10000:
            raise ProducerConflict("continuation batch exceeds accepted source limit")


def _schema():
    statements = [
        f"CREATE TABLE {COUNTERS} (singleton INTEGER PRIMARY KEY CHECK(singleton=1), "
        "launch_rows INTEGER NOT NULL, retired_rows INTEGER NOT NULL, history_bytes INTEGER NOT NULL)",
        f"CREATE TABLE {LAUNCHES} (mint TEXT PRIMARY KEY, launch_rowid INTEGER NOT NULL, "
        "content_digest TEXT NOT NULL)",
        f"CREATE TABLE {RETIRED} (mint TEXT PRIMARY KEY REFERENCES {LAUNCHES}(mint), "
        "bundle_json TEXT NOT NULL, content_digest TEXT NOT NULL)",
        f"CREATE INDEX live_producer_feature_mint_v0_2 ON {FEATURES}(mint,production_p1_rowid)",
        f"CREATE INDEX live_producer_timer_fence_v0_2 ON {TABLES[6]}"
        "(source_watermark_p1_rowid,timer_key) WHERE status<>'COMPLETE'",
    ]
    for table, counter, byte_expr in (
        (LAUNCHES, "launch_rows", "length(CAST(NEW.mint AS BLOB))+8+64"),
        (RETIRED, "retired_rows", "length(CAST(NEW.mint AS BLOB))+length(CAST(NEW.bundle_json AS BLOB))+64"),
    ):
        for operation in ("UPDATE", "DELETE"):
            statements.append(f"CREATE TRIGGER {table}_{operation.lower()} BEFORE {operation} ON {table} "
                              "BEGIN SELECT RAISE(ABORT,'immutable producer history'); END")
        statements.append(f"CREATE TRIGGER {table}_insert AFTER INSERT ON {table} BEGIN "
                          f"UPDATE {COUNTERS} SET {counter}={counter}+1,history_bytes=history_bytes+{byte_expr} "
                          "WHERE singleton=1; END")
    # Keep the original row and foreign-key identity. Resolution changes its
    # resource class once, and freezes all its bytes. Exact no-op ACKs remain
    # legal; unacknowledged children keep their own operational obligations.
    for table, predicate in RESOLVED.items():
        old = "OLD." + predicate
        same = " AND ".join(f"NEW.{column} IS OLD.{column}" for column in ('rowid', *RESOLVED_COLUMNS[table].split(',')))
        statements.append(f"CREATE TRIGGER {table}_resolved_update BEFORE UPDATE ON {table} "
            f"WHEN {old} AND NOT ({same}) BEGIN SELECT RAISE(ABORT,'immutable resolved producer history'); END")
        statements.append(f"CREATE TRIGGER {table}_resolved_delete BEFORE DELETE ON {table} "
            f"WHEN {old} BEGIN SELECT RAISE(ABORT,'immutable resolved producer history'); END")
        # REPLACE's implicit delete need not execute DELETE triggers when
        # recursive_triggers=OFF. Guard conflicts before SQLite applies any
        # INSERT conflict policy. '=' deliberately follows UNIQUE NULL rules.
        collision = ' OR '.join('(' + ' AND '.join(f'saved.{c}=NEW.{c}' for c in key) + ')'
                                for key in RESOLVED_KEYS[table])
        equal = ' AND '.join(f'saved.{c} IS NEW.{c}' for c in RESOLVED_COLUMNS[table].split(','))
        # SQLite exposes -1 for an unspecified rowid in BEFORE INSERT. Exact
        # natural-key replay is ignored, so it retains the original rowid.
        equal += ' AND (NEW.rowid=-1 OR saved.rowid IS NEW.rowid)'
        existing = f'SELECT 1 FROM {table} AS saved WHERE saved.{predicate} AND ({collision})'
        # An unresolved OLD row may also replace a different resolved row via
        # UPDATE OR REPLACE. Even identical target bytes must not merge away
        # that independent pending obligation.
        statements.append(f"CREATE TRIGGER {table}_resolved_update_collision BEFORE UPDATE ON {table} "
            f"WHEN EXISTS ({existing} AND saved.rowid<>OLD.rowid) "
            "BEGIN SELECT RAISE(ABORT,'immutable resolved producer history'); END")
        statements.append(f"CREATE TRIGGER {table}_resolved_insert BEFORE INSERT ON {table} "
            f"WHEN EXISTS ({existing}) BEGIN SELECT CASE WHEN EXISTS ({existing} AND NOT ({equal})) "
            "THEN RAISE(ABORT,'immutable resolved producer history') END; SELECT RAISE(IGNORE); END")
    return statements


class LiveContinuousProducerV02(LiveContinuousProducerV01):
    """Storage v0.3 accounting; preserves A1 model/run/event/source identity.

    Resolved original rows stay in their original keyed tables, independently
    frozen by schema guards and committed in the exact checkpoint digest.
    Their canonical bytes consume permanent history capacity, never pending.
    No implicit V02 store migration or loss of an unresolved obligation.
    """

    def __init__(self, conn, market_source, *, profile=None, **kwargs):
        self.profile = profile or ContinuationProfileV02()
        if type(self.profile) is not ContinuationProfileV02:
            raise ProducerConflict("versioned continuation profile required")
        self.metrics = {"replayed_events": 0, "launch_lookups": 0,
                        "retired_lookups": 0, "retired_bundle_rows_read": 0,
                        "checkpoint_rows_read": 0, "source_rows_read": 0}
        self._bounded = conn.execute("SELECT 1 FROM sqlite_master WHERE name=?", (COUNTERS,)).fetchone() is not None
        super().__init__(conn, market_source, **kwargs)
        if not self._bounded:
            try:
                with conn.source_batch_transaction():
                    manifest = self._validate_checkpoint()
                    if manifest["generation"] != self._generation:
                        raise ProducerConflict("stale A1 migration generation")
                    for sql in _schema():
                        conn.execute(sql)
                    conn.execute(f"INSERT INTO {COUNTERS} VALUES(1,0,0,0)")
                    self._bounded = True
                    for mint, launch in manifest["launches"].items():
                        self._remember_launch(mint, launch)
                    self._retire_terminal()
                    self._publish_checkpoint()
                self._generation += 1
            except BaseException:
                self._poisoned = True
                raise

    def _legacy_preflight(self, conn):
        # LIMIT prevents an oversized legacy history from being materialized.
        caps = {TABLES[0]: 1, FEATURES: self.profile.retained_events,
                RUNS: self.profile.active_mints * len(accepted.LOCKED_ROLE_ORDER)}
        total_bytes = 0
        for table in TABLES:
            cap = caps.get(table, self.profile.pending_rows)
            rows = self._bounded_rows(conn, table, cap, self.profile.retained_bytes + self.profile.pending_bytes)
            total_bytes += len(accepted._json([dict(row) for row in rows]).encode())
        row = conn.execute("SELECT length(CAST(manifest_json AS BLOB)) FROM live_producer_checkpoint_v0_1").fetchone()
        if (total_bytes > self.profile.retained_bytes + self.profile.pending_bytes
                or row is None or row[0] > self.profile.checkpoint_bytes):
            raise ProducerConflict("A1 migration byte profile exhausted")

    @staticmethod
    def _row_bytes_expression(conn, table):
        # SQLite length(BLOB) reads the stored byte count without returning the
        # payload to Python. Column names come only from this known table schema.
        columns = [row[1] for row in conn.execute(f'PRAGMA table_info("{table}")')]
        if not columns:
            raise ProducerConflict("missing bounded table " + table)
        return "+".join('coalesce(length(CAST("' + name.replace('"', '""') + '" AS BLOB)),0)' for name in columns)

    def _bounded_rows(self, conn, table, row_cap, byte_cap, *, after=None):
        expression = self._row_bytes_expression(conn, table)
        where = "" if after is None else " WHERE rowid>?"
        args = () if after is None else (after,)
        sizes = conn.execute(f"SELECT ({expression}) FROM {table}{where} ORDER BY rowid LIMIT ?",
                             (*args, row_cap + 1 if after is None else row_cap)).fetchall()
        if (len(sizes) > row_cap or sum(row[0] for row in sizes) > byte_cap
                or after is not None and any(row[0] > self.profile.record_bytes for row in sizes)):
            raise ProducerConflict("bounded row/byte profile exhausted before payload load: " + table)
        projection = "*" if after is None else "rowid AS p1_rowid,*"
        return conn.execute(f"SELECT {projection} FROM {table}{where} ORDER BY rowid LIMIT ?",
                            (*args, row_cap)).fetchall()

    def _history_counters(self):
        row = self.conn.execute(f"SELECT * FROM {COUNTERS} WHERE singleton=1").fetchone()
        if row is None or any(type(row[key]) is not int or row[key] < 0
                              for key in ("launch_rows", "retired_rows", "history_bytes")):
            raise ProducerConflict("invalid history counters")
        if (row["launch_rows"] + row["retired_rows"] > self.profile.history_rows
                or row["history_bytes"] > self.profile.history_bytes):
            raise ProducerConflict("permanent history profile exhausted")
        return dict(row)

    def _schema_digest(self):
        names = [sql.split()[2] for sql in _schema()]
        rows = []
        for name in names:
            row = self.conn.execute("SELECT sql FROM sqlite_master WHERE name=?", (name,)).fetchone()
            if row is None:
                raise ProducerConflict("missing continuation schema guard: " + name)
            rows.append(row[0])
        expected = _schema()
        if rows != expected:
            raise ProducerConflict("continuation schema guard changed")
        return accepted._fingerprint(rows)

    def _manifest_state(self):
        if not self._bounded:
            return super()._manifest_state()
        result = {}
        pending_rows = pending_bytes = retained_bytes = 0
        resolved_rows = resolved_bytes = 0
        resolved_digests = {}
        hot = {}
        caps = {TABLES[0]: 1, FEATURES: self.profile.retained_events,
                RUNS: self.profile.active_mints * len(accepted.LOCKED_ROLE_ORDER)}
        for table in TABLES:
            cap = caps.get(table, self.profile.pending_rows)
            byte_cap = (self.profile.pending_bytes - pending_bytes if table in PENDING else
                        self.profile.retained_bytes - retained_bytes if table in (FEATURES, RUNS) else self.profile.checkpoint_bytes)
            if table in PENDING:
                # Every canonical row costs at least two bytes. Thus this
                # combined LIMIT also bounds the retained history scan before
                # loading payloads; history cannot become an unlimited archive.
                cap += self.profile.history_bytes // 2
                byte_cap += self.profile.history_bytes - resolved_bytes
            rows = [dict(row) for row in self._bounded_rows(self.conn, table, cap, byte_cap)]
            self.metrics["checkpoint_rows_read"] += len(rows)
            if len(rows) > cap:
                raise ProducerConflict("continuation row profile exhausted: " + table)
            size = len(accepted._json(rows).encode())
            if table in PENDING:
                column = 'complete' if table == TABLES[1] else 'status' if table == TABLES[6] else 'delivered'
                terminal = 'COMPLETE' if column == 'status' else 1
                unresolved = [row for row in rows if row[column] != terminal]
                resolved = [row for row in rows if row[column] == terminal]
                pending_rows += len(unresolved)
                pending_bytes += len(accepted._json(unresolved).encode())
                resolved_rows += len(resolved)
                resolved_bytes += len(accepted._json(resolved).encode()) if resolved else 0
                resolved_digests[table] = accepted._fingerprint(resolved)
            elif table in (FEATURES, RUNS):
                retained_bytes += size
            if table == FEATURES:
                for row in rows:
                    count, total = hot.get(row["mint"], (0, 0))
                    hot[row["mint"]] = (count + 1, total + len(row["event_json"].encode()))
            result[table] = rows
        if len(hot) > self.profile.active_mints:
            raise ProducerConflict("active mint profile exhausted; unfinished state retained")
        if any(count > self.profile.hot_mint_events or size > self.profile.hot_mint_bytes
               for count, size in hot.values()):
            raise ProducerConflict("hottest mint profile exhausted; unfinished state retained")
        if retained_bytes > self.profile.retained_bytes:
            raise ProducerConflict("retained byte profile exhausted")
        if pending_rows > self.profile.pending_rows or pending_bytes > self.profile.pending_bytes:
            raise ProducerConflict("unresolved pending profile exhausted; acknowledgement required")
        result[COUNTERS] = self._history_counters()
        history_bytes = result[COUNTERS]['history_bytes'] + resolved_bytes
        if history_bytes > self.profile.history_bytes:
            raise ProducerConflict("resolved immutable history byte profile exhausted")
        result['resolved_history'] = {'rows': resolved_rows, 'bytes': resolved_bytes,
            'table_digests': resolved_digests}
        result["schema_guard"] = self._schema_digest()
        self.last_profile_usage = {"active_mints": len(hot), "retained_events": sum(v[0] for v in hot.values()),
                                   "retained_bytes": retained_bytes, "pending_rows": pending_rows,
                                   "pending_bytes": pending_bytes, "hottest_events": max((v[0] for v in hot.values()), default=0),
                                   "hottest_bytes": max((v[1] for v in hot.values()), default=0),
                                   **result[COUNTERS],
                                   'identity_history_bytes': result[COUNTERS]['history_bytes'],
                                   'resolved_history_rows': resolved_rows, 'resolved_history_bytes': resolved_bytes,
                                   'history_bytes': history_bytes}
        return result

    def _mark_input_complete(self, input_key):
        # Original COMPLETE replays must not rewrite immutable observation UTC.
        self._inject(input_key, 'before_input_resolution')
        self.conn.execute(f"UPDATE {TABLES[1]} SET complete=1,updated_at=? WHERE input_key=? AND complete=0",
            (accepted._dt_text(self.wall_clock()), input_key))
        self._inject(input_key, 'after_input_resolution')

    def execute_prepared_timer(self, timer_key):
        # The inherited method rewrites updated_at even on completed redelivery.
        # Preserve its original result and exact durable bytes without repeating
        # planning, acknowledgement or source movement.
        with self._transaction():
            timer = self._prepared_timer(timer_key)
            if timer['status'] == 'COMPLETE':
                audits = self.conn.execute(f"SELECT count(*) FROM {TABLES[4]} WHERE input_key=?",
                    (timer['input_key'],)).fetchone()[0]
                return accepted.BindingTimerResultV01(timer_key, 'COMMITTED', timer['source_watermark_p1_rowid'], audits, 0, 0)
            first = self.conn.execute(f"SELECT timer_key FROM {TABLES[6]} WHERE status<>'COMPLETE' "
                "ORDER BY source_watermark_p1_rowid,timer_key LIMIT 1").fetchone()
            if first[0] != timer_key:
                raise ProducerConflict("execute minimum watermark/timer key first")
            return accepted.ContinuousFirstPullbackBindingV01.execute_prepared_timer(self, timer_key)

    def _publish_checkpoint(self):
        if not self._bounded:
            return super()._publish_checkpoint()
        cursor = self.durable_p1_rowid
        # The accepted source's cache is only the current bounded batch. Its
        # durable registry stores even qualifying launches with no price point.
        for mint, launch in self.market_source._launch_by_mint.items():
            if launch <= cursor:
                self._remember_launch(mint, launch)
        self._retire_terminal()
        launches = {mint: self._launch(mint) for mint in self._engine._tokens}
        self.market_source._launch_by_mint = launches
        self.market_source._launch_state_through_p1_rowid = cursor
        previous = self.conn.execute("SELECT * FROM live_producer_checkpoint_v0_1 WHERE singleton=1").fetchone()
        generation = int(previous["generation"]) + 1
        manifest = {
            "schema": SCHEMA_VERSION, "model": MODEL_FINGERPRINT, "spec": SPEC,
            "lineage": self.source_identity, "source": self.market_source.source_identity,
            "anchor": self.market_source.start_after_p1_rowid, "cursor": cursor,
            "next_event_sequence": self.next_event_sequence, "launches": launches,
            "launch_through": cursor, "generation": generation,
            "predecessor": previous["manifest_digest"],
            "state_digest": accepted._fingerprint(self._manifest_state()),
            "storage": STORAGE_VERSION, "profile": asdict(self.profile),
            "storage_fingerprint": STORAGE_FINGERPRINT,
        }
        if len(accepted._json(manifest).encode()) > self.profile.checkpoint_bytes:
            raise ProducerConflict("checkpoint byte profile exhausted")
        self._inject("CHECKPOINT", "before_checkpoint_publish")
        self.conn.execute("UPDATE live_producer_checkpoint_v0_1 SET generation=?,manifest_json=?,manifest_digest=? WHERE singleton=1",
                          (generation, accepted._json(manifest), accepted._fingerprint(manifest)))
        self._inject("CHECKPOINT", "after_checkpoint_publish_before_commit")

    def _validate_checkpoint(self):
        if not self._bounded:
            self._legacy_preflight(self.conn)
            return super()._validate_checkpoint()
        try:
            size = self.conn.execute("SELECT length(CAST(manifest_json AS BLOB)) FROM live_producer_checkpoint_v0_1 WHERE singleton=1").fetchone()
            if size is None or size[0] > self.profile.checkpoint_bytes:
                raise ValueError("missing/oversized checkpoint")
            row = self.conn.execute("SELECT * FROM live_producer_checkpoint_v0_1 WHERE singleton=1").fetchone()
            manifest = json.loads(row["manifest_json"])
            if accepted._fingerprint(manifest) != row["manifest_digest"]:
                raise ValueError("checkpoint checksum")
            expected = {"schema", "model", "spec", "lineage", "source", "anchor", "cursor",
                        "next_event_sequence", "launches", "launch_through", "generation", "predecessor",
                        "state_digest", "storage", "storage_fingerprint", "profile"}
            if (set(manifest) != expected or manifest["storage"] != STORAGE_VERSION
                    or manifest["storage_fingerprint"] != STORAGE_FINGERPRINT
                    or manifest["profile"] != asdict(self.profile)
                    or manifest["schema"] != SCHEMA_VERSION or manifest["model"] != MODEL_FINGERPRINT
                    or manifest["spec"] != SPEC or manifest["lineage"] != self.source_identity
                    or manifest["source"] != self.market_source.source_identity
                    or manifest["anchor"] != self.market_source.start_after_p1_rowid):
                raise ValueError("source/anchor/version/lineage/profile conflict")
            for key in ("anchor", "cursor", "next_event_sequence", "launch_through", "generation"):
                if type(manifest[key]) is not int or manifest[key] < 0:
                    raise ValueError("checkpoint counter")
            if (manifest["generation"] != row["generation"] or manifest["generation"] < 1
                    or manifest["cursor"] < manifest["anchor"] or manifest["launch_through"] != manifest["cursor"]
                    or manifest["cursor"] != self.durable_p1_rowid or manifest["next_event_sequence"] != self.next_event_sequence
                    or manifest["state_digest"] != accepted._fingerprint(self._manifest_state())):
                raise ValueError("bounded checkpoint retained state/cursor conflict")
            launches = manifest["launches"]
            if not isinstance(launches, dict) or len(launches) > self.profile.active_mints:
                raise ValueError("active launch registry")
            active_mints = {row[0] for row in self.conn.execute(f"SELECT DISTINCT mint FROM {FEATURES}")}
            if set(launches) != active_mints:
                raise ValueError("active launch/feature mismatch")
            for mint, launch in launches.items():
                if (type(launch) is not int or not manifest["anchor"] < launch <= manifest["cursor"]
                        or self._launch(mint) != launch or self._retired(mint) is not None):
                    raise ValueError("active launch provenance/terminal conflict")
            if self.market_source.latest_p1_rowid() < manifest["cursor"]:
                raise ValueError("source cursor regression")
            self._verify_prepared_timers()
            for timer in self.conn.execute(f"SELECT * FROM {TABLES[6]}"):
                if timer["clock_type"] != "STRATEGY_CLOCK" or timer["drive_exit_clock"]:
                    raise ValueError("LIVE producer strategy timers only")
            # These checks cover the bounded pending child set via indexed FK
            # lookups; no PRAGMA that walks lifetime immutable history here.
            for table in (TABLES[4], TABLES[5], TABLES[6]):
                for child in self.conn.execute(f"SELECT input_key FROM {table} WHERE input_key IS NOT NULL"):
                    if self.conn.execute(f"SELECT 1 FROM {TABLES[1]} WHERE input_key=?", (child[0],)).fetchone() is None:
                        raise ValueError("pending ownership foreign key")
            return manifest
        except Exception as exc:
            self._poisoned = True
            raise ProducerConflict(f"LIVE bounded checkpoint restore denied: {exc}") from exc

    def _launch(self, mint):
        self.metrics["launch_lookups"] += 1
        row = self.conn.execute(f"SELECT * FROM {LAUNCHES} WHERE mint=?", (mint,)).fetchone()
        if row is None:
            return None
        payload = {"mint": mint, "launch_rowid": row["launch_rowid"]}
        if (accepted._fingerprint(payload) != row["content_digest"]
                or type(row["launch_rowid"]) is not int
                or row["launch_rowid"] <= self.market_source.start_after_p1_rowid):
            raise ProducerConflict("immutable launch evidence conflict")
        return row["launch_rowid"]

    def _remember_launch(self, mint, launch):
        previous = self._launch(mint)
        if previous is not None:
            if previous != launch:
                raise ProducerConflict("original launch identity changed")
            return
        payload = {"mint": mint, "launch_rowid": launch}
        self.conn.execute(f"INSERT INTO {LAUNCHES} VALUES(?,?,?)", (mint, launch, accepted._fingerprint(payload)))

    def _retired(self, mint):
        self.metrics["retired_lookups"] += 1
        size = self.conn.execute(f"SELECT length(CAST(bundle_json AS BLOB)) FROM {RETIRED} WHERE mint=?", (mint,)).fetchone()
        if size is not None and size[0] > self.profile.history_bytes:
            raise ProducerConflict("oversized terminal bundle before payload load")
        row = self.conn.execute(f"SELECT * FROM {RETIRED} WHERE mint=?", (mint,)).fetchone()
        if row is None:
            return None
        self.metrics["retired_bundle_rows_read"] += 1
        if len(row["bundle_json"].encode()) > self.profile.history_bytes:
            raise ProducerConflict("oversized terminal bundle")
        bundle = json.loads(row["bundle_json"])
        if (accepted._fingerprint(bundle) != row["content_digest"] or bundle["mint"] != mint
                or bundle["lineage"] != self.source_identity or bundle["launch_rowid"] != self._launch(mint)
                or not self._terminal_reason(bundle["runs"])):
            raise ProducerConflict("immutable terminal evidence conflict")
        return bundle

    @staticmethod
    def _terminal_reason(rows):
        if any(row["candidate_signal_id"] is not None for row in rows):
            return "WINNER_SUPPRESSED"
        if ({row["role"] for row in rows} == set(accepted.LOCKED_ROLE_ORDER)
                and all(accepted._run_from_json(row["run_json"]).finished for row in rows)):
            return "ALL_LOCKED_ROLES_TERMINAL"
        return None

    def _retire_terminal(self):
        for mint in tuple(self._engine._tokens):
            rows = [dict(row) for row in self.conn.execute(f"SELECT * FROM {RUNS} WHERE mint=? ORDER BY role_rank", (mint,))]
            reason = self._terminal_reason(rows)
            if reason is None:
                continue
            # All production planning/outputs are durable in this same atomic
            # batch. Preserve exact run/deadline/parameter evidence permanently;
            # all unresolved input/output/audit/timer rows remain producer-owned.
            bundle = {"mint": mint, "lineage": self.source_identity, "launch_rowid": self._launch(mint),
                      "retired_through": self.durable_p1_rowid, "reason": reason, "runs": rows}
            if bundle["launch_rowid"] is None:
                raise ProducerConflict("cannot retire without original launch")
            self._inject(mint, "before_terminal_retirement")
            self.conn.execute(f"INSERT INTO {RETIRED} VALUES(?,?,?)", (mint, accepted._json(bundle), accepted._fingerprint(bundle)))
            self.conn.execute(f"DELETE FROM {FEATURES} WHERE mint=?", (mint,))
            self.conn.execute(f"DELETE FROM {RUNS} WHERE mint=?", (mint,))
            del self._engine._tokens[mint]
            self._inject(mint, "after_terminal_retirement")

    def _rebuild_engine(self):
        engine = super()._rebuild_engine()
        self.metrics["replayed_events"] += sum(len(token.known_events) for token in engine._tokens.values())
        return engine

    def _plan_record(self, input_key, record, qevent, content_fp):
        if len(accepted._json(qevent).encode()) > self.profile.record_bytes:
            raise ProducerConflict("normalized record byte profile exhausted")
        retired = self._retired(record.mint)
        if retired is None:
            token = self._engine._tokens.get(record.mint)
            if token is not None and len(token.known_events) >= self.profile.hot_mint_events:
                raise ProducerConflict("hottest mint event profile exhausted; reopen previous checkpoint")
            if token is None and len(self._engine._tokens) >= self.profile.active_mints:
                raise ProducerConflict("active mint profile exhausted; unfinished state retained")
            return super()._plan_record(input_key, record, qevent, content_fp)
        # Accepted planner emits exactly this independent observation after its
        # winner/terminal strategy suppression. Step 8B owns consuming it.
        observation = accepted.MarketObservationEvent(
            mint=record.mint, observed_at=record.observed_at, ingest_seq=record.ingest_seq,
            price_identity=record.price_identity, price_numerator_raw=record.price_numerator_raw,
            price_denominator_raw=record.price_denominator_raw,
            current_virtual_token_reserve_raw=record.current_virtual_token_reserve_raw,
            is_gap_recovery=record.is_gap_recovery,
        )
        self._persist_input_and_events(input_key, "MARKET", record.production_p1_rowid, content_fp,
                                       [(f"{record.event_key}:MARKET", observation)], evaluations=[])

    def _hydrate(self, record):
        # Normalize the exact captured raw input again through the accepted
        # compatibility adapter. Never reopen/re-read a mutable source prefix.
        row = self._captured_rows.get(record.production_p1_rowid)
        if row is None:
            raise ProducerConflict("missing captured source input")
        compat = accepted.Phase1GapSourceCompatibilityV01.row_to_event(row)
        qevent = compat.event
        if qevent is None:
            raise ProducerConflict("captured accepted input no longer normalizes")
        base, point = qevent.base, accepted.QuoteAwarePriceEngineV01.point(qevent)
        projection = (int(base.ingest_seq), str(base.event_key), str(base.mint), str(base.event_type.value),
                      accepted._us_to_datetime(int(base.event_at_us)), accepted._us_to_datetime(int(base.observed_at_us)),
                      str(point.identity.label), point.numerator_reserve_raw, point.token_reserve_raw,
                      base.source == accepted.IngestionSource.GAP_RECOVERY, str(base.source.value),
                      compat.raw_source_decoded_file or "", compat.compatibility_applied)
        expected = (record.ingest_seq, record.event_key, record.mint, record.event_type, record.event_at,
                    record.observed_at, record.price_identity, record.price_numerator_raw, record.price_denominator_raw,
                    record.is_gap_recovery, record.ingestion_source, record.raw_source_decoded_file,
                    record.source_compatibility_applied)
        if projection != expected:
            raise ProducerConflict("captured Phase-2 input differs from accepted source projection")
        return qevent

    def process_next_batch(self, *, batch_size=256, max_raw_bytes=None):
        if type(batch_size) is not int or not 1 <= batch_size <= self.profile.batch_rows:
            raise ProducerConflict("source batch outside continuation profile")
        with self._transaction():
            after = self.durable_p1_rowid
            fence = self._active_timer_fence()
            limit = batch_size if fence is None else min(batch_size, fence - after)
            if limit == 0:
                return accepted.BindingBatchResultV01(after, after, 0, 0, 0, 0, after)
            source = self.market_source
            conn = source.open_readonly(source.db_path)
            try:
                conn.execute("BEGIN")
                source.validate_schema(conn)
                byte_cap = limit*self.profile.record_bytes
                if max_raw_bytes is not None:
                    if type(max_raw_bytes) is not int or not 0 < max_raw_bytes <= byte_cap:
                        raise ProducerConflict("finite narrower source capture byte reservation required")
                    byte_cap = max_raw_bytes
                rows = self._bounded_rows(conn, "pump_events", limit, byte_cap, after=after)
            finally:
                conn.close()
            self._captured_rows = {row["p1_rowid"]: row for row in rows}
            self.metrics["source_rows_read"] += len(rows)
            launches = {}
            for row in rows:
                if len(accepted._json(dict(row)).encode()) > self.profile.record_bytes:
                    raise ProducerConflict("raw source record byte profile exhausted")
                mint = row["mint"]
                if mint and mint not in launches:
                    launch = self._launch(str(mint))
                    if launch is not None:
                        launches[str(mint)] = launch
            batch = source._normalize_rows(rows, requested_after=after, batch_limit=limit, launch_by_mint=launches)
            source._launch_by_mint = launches
            source._launch_state_through_p1_rowid = batch.highest_fetched_p1_rowid
            source._metrics["normal_monotonic_fetches_without_rebuild"] += 1
            records = {r.production_p1_rowid: r for r in batch.records}
            skips = {s.production_p1_rowid: s for s in batch.skips}
            fetched = tuple(sorted(set(records) | set(skips)))
            if len(fetched) != batch.raw_rows_fetched:
                raise ProducerConflict("market source fetched-row accounting is incomplete")
            admitted = tuple(rowid for rowid in fetched if fence is None or rowid <= fence)
            for rowid in admitted:
                if rowid <= after:
                    raise ProducerConflict("market source returned non-ascending rowid")
                if rowid in records:
                    self._handle_record(records[rowid])
                else:
                    self._handle_skip(rowid, accepted._fingerprint(skips[rowid]))
            self._captured_rows = {}
            return accepted.BindingBatchResultV01(after, admitted[-1] if admitted else after, len(admitted),
                                                  sum(rowid in records for rowid in admitted),
                                                  sum(rowid in skips for rowid in admitted), 0, self.durable_p1_rowid)

    def producer_events(self, *, after_sequence=-1, limit=None):
        """Bounded original ordered page. A3 owns delivery and acknowledgement."""
        page_size = self.profile.delivery_page_rows if limit is None else limit
        if type(page_size) is not int or not 1 <= page_size <= self.profile.delivery_page_rows:
            raise ProducerConflict("delivery page outside continuation profile")
        return tuple(dict(row) for row in self.conn.execute(
            f"SELECT * FROM {TABLES[5]} WHERE event_sequence>? ORDER BY event_sequence LIMIT ?", (after_sequence, page_size)))

    def original_runs(self, mint):
        """Point-addressed immutable terminal or exact active run inputs for A3."""
        retired = self._retired(mint)
        if retired is not None:
            return tuple(retired["runs"])
        return tuple(dict(row) for row in self.conn.execute(f"SELECT * FROM {RUNS} WHERE mint=? ORDER BY role_rank", (mint,)))
