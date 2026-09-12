"""A3 bounded original producer output -> durable fixed LIVE or DRY Ledger inbox.

Two stores deliberately have separate commits. Ledger receipt precedes the
producer ACK; replay resolves the intervening crash by exact inbox identity.
Receipt is not admission. Current SourceEvidence/Authority still own ENTRY.
Unrelated outputs and nonwinning candidates remain retained and unacknowledged.
"""
from __future__ import annotations

import json
from contextlib import closing
from dataclasses import asdict, dataclass

from .continuous_producer_v0_2 import LiveContinuousProducerV02, TABLES, accepted
from .continuous_producer_v0_1 import ProducerConflict
from .ledger_actions_v0_1 import CandidateInboxInput, utc_microseconds
from .source_health_v0_1 import SourceBinding, CollectorSourceAdapter, _SELECT, _fact, witness

VERSION = "live_candidate_handoff_v0.1"
CONFIG = "live_candidate_handoff_config_v0_1"
DDL = (
    f"CREATE TABLE {CONFIG} (singleton INTEGER PRIMARY KEY CHECK(singleton=1), payload TEXT NOT NULL CHECK(length(CAST(payload AS BLOB))<=8192))",
    f"CREATE TRIGGER {CONFIG}_no_update BEFORE UPDATE ON {CONFIG} BEGIN SELECT RAISE(ABORT,'immutable handoff binding'); END",
    f"CREATE TRIGGER {CONFIG}_no_delete BEFORE DELETE ON {CONFIG} BEGIN SELECT RAISE(ABORT,'immutable handoff binding'); END",
)


@dataclass(frozen=True, slots=True)
class HandoffPage:
    after_sequence: int
    through_sequence: int
    candidate_roots: tuple[str, ...]
    retained_nonwinner_sequences: tuple[int, ...]
    scanned_rows: int


class CandidateHandoffV01:
    """One serialized producer owner, bounded page per call, no background loop.

    ``database_identity`` is the reviewed public identity assigned to the A2
    source. Persisting its mapping to SourceBinding avoids assuming the two
    independently defined identity hashes are interchangeable. Anchors are
    verified against that source's actual read-only database.

    ``initialize=True`` is only for explicit first installation of the reviewed
    mapping. Normal construction/reopen requires the durable binding to exist;
    loss is never repaired automatically, including before a producer ACK.
    """

    def __init__(self, producer, ledger, binding, *, database_identity, initialize=False, failure_injector=None):
        if type(producer) is not LiveContinuousProducerV02 or type(binding) is not SourceBinding:
            raise ProducerConflict("A2 producer and explicit source binding required")
        if ledger.domain.mode not in ("LIVE", "DRY"):
            raise ProducerConflict("A3 requires fixed LIVE or DRY inbox domain")
        if type(initialize) is not bool:
            raise ProducerConflict("explicit initial handoff installation flag required")
        self.producer, self.ledger, self.binding = producer, ledger, binding
        self.failure_injector = failure_injector
        source = producer.market_source
        if (database_identity != source.database_identity
                or source.start_after_p1_rowid != binding.anchors[0].rowid):
            raise ProducerConflict("handoff database identity/pump anchor mismatch")
        self._payload = accepted._json({
            "version": VERSION, "database_identity": database_identity,
            "source_identity": source.source_identity, "source_binding": asdict(binding),
            "producer_lineage": producer.source_identity,
            "economic_domain_id": ledger.domain.economic_domain_id,
        })
        if len(self._payload.encode()) > 8192:
            raise ProducerConflict("handoff configuration byte bound")
        self._after = -1
        with producer._transaction():
            exists = producer.conn.execute("SELECT 1 FROM sqlite_master WHERE name=?", (CONFIG,)).fetchone()
            if exists is None:
                if not initialize:
                    raise ProducerConflict("handoff binding missing; reopen cannot initialize")
                if producer.conn.execute(f"SELECT 1 FROM {TABLES[5]} WHERE delivered=1 LIMIT 1").fetchone():
                    raise ProducerConflict("missing handoff binding with existing receipts")
                for sql in DDL:
                    producer.conn.execute(sql)
                producer.conn.execute(f"INSERT INTO {CONFIG} VALUES(1,?)", (self._payload,))
            self._validate_binding()

    def _cut(self, name):
        if self.failure_injector:
            self.failure_injector(name)

    def _validate_binding(self):
        p = self.producer
        for sql in DDL:
            row = p.conn.execute("SELECT sql FROM sqlite_master WHERE name=?", (sql.split()[2],)).fetchone()
            if row is None or row[0] != sql:
                raise ProducerConflict("handoff binding schema changed")
        size = p.conn.execute(f"SELECT length(CAST(payload AS BLOB)) FROM {CONFIG} WHERE singleton=1").fetchone()
        if size is None or size[0] > 8192:
            raise ProducerConflict("missing/oversized handoff binding")
        if p.conn.execute(f"SELECT payload FROM {CONFIG} WHERE singleton=1").fetchone()[0] != self._payload:
            raise ProducerConflict("handoff destination/source rebinding denied")
        # Point reads only; no mutable candidate reconstruction or history scan.
        try:
            with closing(CollectorSourceAdapter(p.market_source.db_path)._open()) as conn:
                conn.execute("BEGIN")
                for anchor in self.binding.anchors:
                    columns = _SELECT[anchor.table].split(",")
                    size_sql = "+".join(f"coalesce(length(CAST({column} AS BLOB)),0)" for column in columns)
                    size = conn.execute(f"SELECT {size_sql} FROM {anchor.table} WHERE rowid=?", (anchor.rowid,)).fetchone()
                    if size is None or size[0] > p.profile.record_bytes:
                        raise ValueError
                    row = conn.execute(f"SELECT {_SELECT[anchor.table]} FROM {anchor.table} WHERE rowid=?", (anchor.rowid,)).fetchone()
                    if row is None or witness(anchor.table, _fact(anchor.table, row)) != anchor:
                        raise ValueError
                row = conn.execute("SELECT event_type FROM collector_events WHERE id=?", (self.binding.anchors[1].rowid,)).fetchone()
                if row[0] != "COLLECTOR_V0_3_START":
                    raise ValueError
        except Exception:
            raise ProducerConflict("handoff source anchor continuity invalid") from None

    def _candidate(self, row):
        p = self.producer
        payload = json.loads(row["event_json"])
        if accepted._fingerprint({"cursor": row["event_sequence"], "event_key": row["event_key"],
                "event_type": row["event_type"], "event": payload}) != row["content_fingerprint"]:
            raise ProducerConflict("original output content mismatch")
        envelope, audit = payload["candidate"], payload["evaluation"]
        runs = p.original_runs(envelope["mint"])
        winners = [r for r in runs if r["candidate_signal_id"] == envelope["candidate_id"]]
        if not winners:
            return None  # Explicit retained nonwinner, never an economic input/ACK.
        if len(winners) != 1:
            raise ProducerConflict("ambiguous original winner")
        original = winners[0]
        run = json.loads(original["run_json"])
        ref = run["reclaim_ref"]
        source = p.conn.execute(f"SELECT * FROM {TABLES[1]} WHERE input_key=?", (row["input_key"],)).fetchone()
        generated = utc_microseconds(envelope["signal_observed_at"])
        if (source is None or source["input_kind"] != "MARKET" or source["complete"] != 1
                or source["production_p1_rowid"] != envelope["signal_ingest_seq"]
                or source["production_p1_rowid"] <= p.market_source.start_after_p1_rowid
                or audit["candidate_signal_id"] != envelope["candidate_id"]
                or audit["run_id"] != run["run_id"] or audit["mint"] != run["mint"]
                or audit["role"] != original["role"] or audit["ingest_seq"] != envelope["signal_ingest_seq"]
                or audit["evaluated_at_us"] != generated or ref["observed_at_us"] != generated
                or original["last_ingest_seq"] != envelope["signal_ingest_seq"]
                or not run["signal_emitted"] or envelope["price_identity"] != "SOL_NATIVE"
                or audit["audit_snapshot"]["known_gap_affecting_state"]
                or audit["audit_snapshot"]["triggering_event_key"] != ref["event_key"]
                or row["event_key"] != ref["event_key"] + ":CANDIDATE:" + original["role"]
                or any(envelope[k] != run[k] or audit[k] != run[k]
                       for k in ("mint", "parameter_set_id", "strategy_version"))):
            raise ProducerConflict("original candidate provenance conflict")
        params = p.params_by_role[original["role"]]
        if original["parameter_set_id"] != envelope["parameter_set_id"]:
            raise ProducerConflict("original parameter binding conflict")
        return CandidateInboxInput(
            candidate_signal_id=envelope["candidate_id"], candidate_run_id=run["run_id"],
            run_started_at_us=run["started_at_us"], mint=envelope["mint"],
            strategy_version=envelope["strategy_version"], parameter_set_id=envelope["parameter_set_id"],
            parameter_fingerprint=accepted._fingerprint(params), model_fingerprint=p.model_fingerprint,
            strategy_evaluation_id="evaluation:" + accepted._fingerprint(audit), generated_at_us=generated,
            source_event_key=ref["event_key"], signal_key=row["event_key"],
            source_cursor=source["production_p1_rowid"], signal_ingest_seq=envelope["signal_ingest_seq"],
            producer_run_id=p.source_identity, producer_lineage_id=p.source_identity,
            # This is the accepted normalized source record digest retained in
            # the original input, not a digest of mutable raw database bytes.
            source_binding=self.binding, source_record_digest=source["content_fingerprint"],
            winner_candidate_id=envelope["candidate_id"], winner_role=original["role"],
            winner_binding_digest=accepted._fingerprint({"selection": accepted.LOCKED_SELECTION_SHA256,
                "lineage": p.source_identity, "winner": original, "output": row["content_fingerprint"]}),
            reference_price_text=ref["price_proxy"], reference_price_kind="RECLAIM_REFERENCE",
            price_identity=envelope["price_identity"],
            rule_reference_price_numerator_raw=envelope["price_numerator_raw"],
            rule_reference_price_denominator_raw=envelope["price_denominator_raw"],
            reference_at_us=ref["observed_at_us"])

    def deliver_page(self, *, limit=None):
        """Deliver in original sequence. Restart rechecks prior ACKs in bounded pages.

        No caller-supplied cursor can bypass preceding pending candidates. The
        cursor advances only after every candidate in this page has a receipt.
        """
        p = self.producer
        with p._transaction():
            self._validate_binding()
            rows = p.producer_events(after_sequence=self._after, limit=limit)
            captured = [(row, self._candidate(row)) for row in rows if row["event_type"] == "CandidateEvaluationEvent"]
        self._cut("after_capture")
        roots, nonwinners = [], []
        for row, candidate in captured:
            if candidate is None:
                nonwinners.append(row["event_sequence"])
                continue
            root = candidate.trade_root(self.ledger.domain)
            # A replacement/rolled-back inbox cannot use producer ACK as proof.
            if row["delivered"] and self.ledger.candidate(root) != candidate:
                raise ProducerConflict("acknowledged candidate missing/conflicting in destination")
            self._cut("before_ledger_receive")
            received = self.ledger.receive_candidate(candidate, fence=self.ledger.write_fence())
            self._cut("after_ledger_commit")
            if received != root or self.ledger.candidate(root) != candidate:
                raise ProducerConflict("durable exact Ledger receipt required")
            with p._transaction():
                self._validate_binding()
                current = p.conn.execute(f"SELECT * FROM {TABLES[5]} WHERE event_sequence=?", (row["event_sequence"],)).fetchone()
                if current is None or any(current[k] != row[k] for k in row if k != "delivered"):
                    raise ProducerConflict("captured output changed before acknowledgement")
                if self._candidate(dict(current)) != candidate:
                    raise ProducerConflict("captured candidate changed before acknowledgement")
                p.conn.execute(f"UPDATE {TABLES[5]} SET delivered=1 WHERE event_sequence=?", (row["event_sequence"],))
                self._cut("before_ack_commit")
            self._cut("after_ack_commit")
            roots.append(root)
        after = self._after
        if rows:
            self._after = rows[-1]["event_sequence"]
        return HandoffPage(after, self._after, tuple(roots), tuple(nonwinners), len(rows))
