"""Checkpointable LIVE producer; accepted pure planner, no economic runner.

The seven ``paper_fp_binding_*`` tables are the accepted *producer* schema,
reused verbatim to avoid a second strategy/timer implementation. No paper
runner, fill, position, admission or ledger is instantiated. Outputs and audits
remain unacknowledged until the separately owned LIVE handoff is implemented.

A1 retains complete per-mint feature history. This is exact replay, not a
bounded deployment qualification. A2 owns retirement and indexed active tails.
"""
from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from phase4 import paper_continuous_firstpullback_binding_v0_1 as accepted
from phase4.paper_continuous_firstpullback_binding_v0_4 import MODEL_FINGERPRINT as FENCE_FINGERPRINT
from phase4.paper_continuous_firstpullback_binding_v0_5 import (
    AtomicSourceBatchConnection, MODEL_FINGERPRINT as ATOMIC_FINGERPRINT,
)
from phase4.paper_continuous_market_source_v0_2 import (
    ContinuousMarketSourceV02, MODEL_FINGERPRINT as SOURCE_FINGERPRINT,
)

MODEL_ID = "LIVE-CONTINUOUS-PRODUCER-0001"
SCHEMA_VERSION = "live_continuous_producer_v0.1"
SPEC = {
    "model_id": MODEL_ID, "schema_version": SCHEMA_VERSION,
    "planner": accepted.MODEL_FINGERPRINT, "fences": FENCE_FINGERPRINT,
    "atomic_batch": ATOMIC_FINGERPRINT, "source": SOURCE_FINGERPRINT,
    "parameters": accepted._fingerprint(accepted.locked_parameter_sets()),
    "selection": accepted.LOCKED_SELECTION_SHA256,
    "restore": "COMPLETE_RETAINED_PER_MINT_REPLAY",
    "economic_runner": "NONE", "timer": "STRATEGY_ONLY",
}
MODEL_FINGERPRINT = accepted._fingerprint(SPEC)
TABLES = tuple("paper_fp_binding_" + suffix + "_v0_1" for suffix in (
    "runtime", "inputs", "feature_events", "strategy_runs", "evaluation_audit",
    "outbox", "prepared_timers",
))
ProducerConflict = accepted.BindingConflict


def connect_producer_db(path: Path) -> AtomicSourceBatchConnection:
    """Open a dedicated producer store; never initialize paper economics."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, factory=AtomicSourceBatchConnection)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    return conn


class LiveContinuousProducerV01(accepted.ContinuousFirstPullbackBindingV01):
    model_id = MODEL_ID
    model_fingerprint = MODEL_FINGERPRINT
    schema_version = SCHEMA_VERSION

    def __init__(self, conn: AtomicSourceBatchConnection,
                 market_source: ContinuousMarketSourceV02, *, wall_clock=None,
                 failure_injector=None):
        # Deliberately do not call any accepted binding constructor: those
        # constructors attach a paper economic runner.
        if (type(market_source) is not ContinuousMarketSourceV02
                or market_source.model_fingerprint != SOURCE_FINGERPRINT):
            raise ProducerConflict("accepted source v0.2 required")
        if not isinstance(conn, AtomicSourceBatchConnection) or conn.in_transaction:
            raise ProducerConflict("clean atomic producer connection required")
        if (str(conn.execute("PRAGMA journal_mode").fetchone()[0]).lower() != "wal"
                or int(conn.execute("PRAGMA synchronous").fetchone()[0]) != 2):
            raise ProducerConflict("producer requires WAL/FULL")
        self.conn, self.market_source = conn, market_source
        self.wall_clock = wall_clock or (lambda: datetime.now(timezone.utc))
        self.failure_injector = failure_injector
        self.params_by_role = accepted.locked_parameter_sets()
        self.reconstruction_queries = 0
        self._poisoned = False
        self._generation = 0
        self._binding_source_identity = accepted._fingerprint({
            "model_id": MODEL_ID, "fingerprint": MODEL_FINGERPRINT,
            "source_identity": market_source.source_identity,
            "anchor": market_source.start_after_p1_rowid,
        })
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if tables and "live_producer_checkpoint_v0_1" not in tables:
            raise ProducerConflict("existing database is not a LIVE producer store")
        if not tables:
            accepted._create_schema(conn)
            conn.execute("CREATE TABLE live_producer_checkpoint_v0_1 ("
                         "singleton INTEGER PRIMARY KEY CHECK(singleton=1),"
                         "generation INTEGER NOT NULL, manifest_json TEXT NOT NULL,"
                         "manifest_digest TEXT NOT NULL)")
            conn.commit()
            with conn.source_batch_transaction():
                stamp = accepted._dt_text(self.wall_clock())
                anchor = market_source.start_after_p1_rowid
                conn.execute("INSERT INTO paper_fp_binding_runtime_v0_1 VALUES(1,?,?,?,?,?,?,?,?,0,?,?)",
                             (MODEL_ID, MODEL_FINGERPRINT, market_source.source_identity,
                              anchor, anchor, "NO_ECONOMIC_RUNNER", MODEL_FINGERPRINT,
                              self.source_identity, stamp, stamp))
                market_source._launch_by_mint = {}
                market_source._launch_state_through_p1_rowid = anchor
                self._publish_checkpoint()
        self._restore_checkpoint()

    def _manifest_state(self):
        # A1 full retained-state digest; A2 replaces lifetime scans with the
        # checked active-state/retirement representation, without dropping
        # unresolved history or changing lineage.
        return {table: [dict(row) for row in self.conn.execute(
            f"SELECT * FROM {table} ORDER BY rowid")] for table in TABLES}

    def _publish_checkpoint(self):
        cursor = self.durable_p1_rowid
        launches = {mint: rowid for mint, rowid in self.market_source._launch_by_mint.items()
                    if rowid <= cursor}
        self.market_source._launch_by_mint = launches
        self.market_source._launch_state_through_p1_rowid = cursor
        previous = self.conn.execute("SELECT * FROM live_producer_checkpoint_v0_1 WHERE singleton=1").fetchone()
        generation = 1 if previous is None else int(previous["generation"]) + 1
        manifest = {
            "schema": SCHEMA_VERSION, "model": MODEL_FINGERPRINT, "spec": SPEC,
            "lineage": self.source_identity, "source": self.market_source.source_identity,
            "anchor": self.market_source.start_after_p1_rowid, "cursor": cursor,
            "next_event_sequence": self.next_event_sequence, "launches": launches,
            "launch_through": cursor, "generation": generation,
            "predecessor": None if previous is None else previous["manifest_digest"],
            "state_digest": accepted._fingerprint(self._manifest_state()),
        }
        self._inject("CHECKPOINT", "before_checkpoint_publish")
        self.conn.execute("INSERT INTO live_producer_checkpoint_v0_1 VALUES(1,?,?,?) "
                          "ON CONFLICT(singleton) DO UPDATE SET generation=excluded.generation,"
                          "manifest_json=excluded.manifest_json,manifest_digest=excluded.manifest_digest",
                          (generation, accepted._json(manifest), accepted._fingerprint(manifest)))
        self._inject("CHECKPOINT", "after_checkpoint_publish_before_commit")

    def _restore_checkpoint(self):
        # One SQLite read snapshot covers manifest, retained tables and replay.
        # A concurrent publication may leave this object stale, but cannot mix
        # two generations; the next mutation then rejects its old generation.
        try:
            with self.conn.source_batch_transaction():
                manifest = self._validate_checkpoint()
                self.market_source._launch_by_mint = dict(manifest["launches"])
                self.market_source._launch_state_through_p1_rowid = manifest["cursor"]
                self._engine = self._rebuild_engine()
                self._generation = manifest["generation"]
        except Exception:
            self._poisoned = True
            raise

    def _validate_checkpoint(self):
        try:
            if self.conn.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                raise ValueError("SQLite integrity")
            if self.conn.execute("PRAGMA foreign_key_check").fetchone() is not None:
                raise ValueError("SQLite foreign keys")
            row = self.conn.execute("SELECT * FROM live_producer_checkpoint_v0_1 WHERE singleton=1").fetchone()
            if row is None:
                raise ValueError("missing checkpoint")
            manifest = json.loads(row["manifest_json"])
            if accepted._fingerprint(manifest) != row["manifest_digest"]:
                raise ValueError("checkpoint checksum")
            expected = {"schema", "model", "spec", "lineage", "source", "anchor", "cursor",
                        "next_event_sequence", "launches", "launch_through", "generation", "predecessor", "state_digest"}
            if set(manifest) != expected:
                raise ValueError("checkpoint schema")
            if (manifest["schema"] != SCHEMA_VERSION or manifest["model"] != MODEL_FINGERPRINT
                    or manifest["spec"] != SPEC or manifest["lineage"] != self.source_identity
                    or manifest["source"] != self.market_source.source_identity
                    or manifest["anchor"] != self.market_source.start_after_p1_rowid):
                raise ValueError("source/anchor/version/lineage conflict")
            for key in ("anchor", "cursor", "next_event_sequence", "launch_through", "generation"):
                if type(manifest[key]) is not int or manifest[key] < 0:
                    raise ValueError("checkpoint counter")
            if (manifest["generation"] != row["generation"] or manifest["generation"] < 1
                    or manifest["cursor"] < manifest["anchor"]
                    or manifest["launch_through"] != manifest["cursor"]
                    or manifest["cursor"] != self.durable_p1_rowid
                    or manifest["next_event_sequence"] != self.next_event_sequence
                    or manifest["state_digest"] != accepted._fingerprint(self._manifest_state())):
                raise ValueError("checkpoint retained state/cursor conflict")
            launches = manifest["launches"]
            if not isinstance(launches, dict) or any(
                    not isinstance(mint, str) or not mint or type(launch) is not int
                    or not manifest["anchor"] < launch <= manifest["cursor"]
                    for mint, launch in launches.items()):
                raise ValueError("launch provenance")
            if self.market_source.latest_p1_rowid() < manifest["cursor"]:
                raise ValueError("source cursor regression")
            self._verify_prepared_timers()
            for timer in self.conn.execute("SELECT * FROM paper_fp_binding_prepared_timers_v0_1"):
                if timer["clock_type"] != "STRATEGY_CLOCK" or timer["drive_exit_clock"]:
                    raise ValueError("LIVE producer strategy timers only")
            return manifest
        except Exception as exc:
            self._poisoned = True
            raise ProducerConflict(f"LIVE checkpoint restore denied: {exc}") from exc

    def _rebuild_engine(self):
        # LIVE cold reconstruction consumes every retained event through the
        # original process mutation/ordering path. Historical market snapshots
        # are pure derived values and were immediately discarded by this loop.
        # Suppress only those unused computations on this private local object;
        # return an ordinary original engine for every subsequent live event.
        class ColdAccumulator(accepted.DenominationAwareFeatureEngineV01):
            def _build_state(self, acc, trigger):
                return None
        replay = ColdAccumulator()
        # Per-mint complete replay is the A2 retirement seam. No recent-window
        # approximation and no inferred launch-age expiry are permitted.
        for row in self.conn.execute("SELECT mint,event_json FROM paper_fp_binding_feature_events_v0_1 "
                                     "ORDER BY mint,production_p1_rowid"):
            event = accepted._quote_event_from_json(row["event_json"])
            if event.base.mint != row["mint"] or event.base.mint not in self.market_source._launch_by_mint:
                raise ProducerConflict("retained feature/launch registry conflict")
            replay.process(event)
        engine = accepted.DenominationAwareFeatureEngineV01()
        engine._tokens = replay._tokens
        return engine

    @contextmanager
    def _transaction(self):
        if self._poisoned:
            raise ProducerConflict("producer instance failed; reopen durable checkpoint")
        try:
            with self.conn.source_batch_transaction():
                manifest = self._validate_checkpoint()
                if manifest["generation"] != self._generation:
                    raise ProducerConflict("stale producer generation; reopen")
                yield
                self._publish_checkpoint()
            self._generation += 1
        except BaseException:
            # Planner/source caches may have advanced even if SQLite rolled
            # back. Do not allow any later operation on this stale instance.
            self._poisoned = True
            raise

    def checkpoint(self):
        with self._transaction():
            pass
        return self.checkpoint_manifest()

    def checkpoint_manifest(self):
        row = self.conn.execute("SELECT manifest_json FROM live_producer_checkpoint_v0_1 WHERE singleton=1").fetchone()
        return json.loads(row[0])

    def canonical_digest(self):
        return accepted._fingerprint(self._manifest_state())

    def process_next_batch(self, *, batch_size=1000):
        with self._transaction():
            result = super().process_next_batch(batch_size=batch_size)
        return result

    def prepare_clock_tick(self, now, *, drive_exit_clock=False, timer_id=None):
        if drive_exit_clock:
            raise ProducerConflict("LIVE producer has no exit clock")
        with self._transaction():
            result = super().prepare_clock_tick(now, drive_exit_clock=False, timer_id=timer_id)
        return result

    def execute_prepared_timer(self, timer_key):
        with self._transaction():
            timer = self._prepared_timer(timer_key)
            if timer["status"] != "COMPLETE":
                first = self.conn.execute("SELECT timer_key FROM paper_fp_binding_prepared_timers_v0_1 "
                                          "WHERE status<>'COMPLETE' ORDER BY source_watermark_p1_rowid,timer_key LIMIT 1").fetchone()
                if first[0] != timer_key:
                    raise ProducerConflict("execute minimum watermark/timer key first")
            result = super().execute_prepared_timer(timer_key)
        return result

    def submit_clock_tick(self, now, *, drive_exit_clock=False):
        return self.execute_prepared_timer(self.prepare_clock_tick(now, drive_exit_clock=drive_exit_clock))

    def prepare_exit_clock_tick(self, *args, **kwargs):
        raise ProducerConflict("LIVE producer has no exit clock")

    def submit_exit_clock_tick(self, *args, **kwargs):
        raise ProducerConflict("LIVE producer has no exit clock")

    def _drain_evaluations(self, input_key):
        return 0  # A3 owns acknowledgement; producer persists exact audit bytes.

    def _drain_input(self, input_key):
        return 0  # Never invoke paper economics or claim downstream delivery.

    def _mark_input_complete(self, input_key):
        self.conn.execute("UPDATE paper_fp_binding_inputs_v0_1 SET complete=1,updated_at=? WHERE input_key=?",
                          (accepted._dt_text(self.wall_clock()), input_key))

    def _complete_production_input(self, input_key, rowid):
        self._mark_input_complete(input_key)
        self.conn.execute("UPDATE paper_fp_binding_runtime_v0_1 SET last_durable_p1_rowid=?,updated_at=? WHERE singleton=1",
                          (rowid, accepted._dt_text(self.wall_clock())))

    def producer_events(self, *, after_sequence=-1):
        """Ordered original events; A3 owns durable delivery/acknowledgement."""
        return tuple(dict(row) for row in self.conn.execute(
            "SELECT * FROM paper_fp_binding_outbox_v0_1 WHERE event_sequence>? ORDER BY event_sequence",
            (after_sequence,)))
