"""Dedicated append-only G001 source journal, with no economic tables."""
from __future__ import annotations

import sqlite3
from pathlib import Path

from .source_health_v0_1 import (
    ZERO_DIGEST, SourceBinding, SourceProfile, SourceVerdict, canonical_json,
    evaluate_source, verdict_from_json,
)


class SourceJournalConflict(ValueError):
    pass


class SourceEvidenceStore:
    """One fixed source binding/profile; atomic append and predecessor CAS.

    Failed observations are durable evidence too. Their unchanged progress lets
    a later observation resume the same source lineage. Stored dispositions are
    re-derived on append and restart; no caller-supplied healthy flag is trusted.
    """

    def __init__(self, path: str | Path, binding: SourceBinding, profile: SourceProfile) -> None:
        self.binding, self.profile = binding, profile
        self._conn = sqlite3.connect(Path(path).resolve(), timeout=1.0, isolation_level=None)
        try:
            tables = {row[0] for row in self._conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'")}
            if tables - {"source_domain", "source_evidence"}:
                raise SourceJournalConflict("dedicated source evidence store required")
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=FULL")
            self._conn.execute("PRAGMA foreign_keys=ON")
            if self._conn.execute("PRAGMA integrity_check").fetchone() != ("ok",):
                raise SourceJournalConflict("source journal integrity failed")
            self._conn.executescript("""
                CREATE TABLE IF NOT EXISTS source_domain (
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    binding_json TEXT NOT NULL,
                    profile_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS source_evidence (
                    seq INTEGER PRIMARY KEY,
                    previous_digest TEXT NOT NULL,
                    content_digest TEXT NOT NULL UNIQUE,
                    payload_json TEXT NOT NULL
                );
                CREATE TRIGGER IF NOT EXISTS source_evidence_no_update
                BEFORE UPDATE ON source_evidence BEGIN
                    SELECT RAISE(ABORT,'immutable source evidence');
                END;
                CREATE TRIGGER IF NOT EXISTS source_evidence_no_delete
                BEFORE DELETE ON source_evidence BEGIN
                    SELECT RAISE(ABORT,'immutable source evidence');
                END;
                CREATE TRIGGER IF NOT EXISTS source_domain_no_update
                BEFORE UPDATE ON source_domain BEGIN
                    SELECT RAISE(ABORT,'immutable source domain');
                END;
                CREATE TRIGGER IF NOT EXISTS source_domain_no_delete
                BEFORE DELETE ON source_domain BEGIN
                    SELECT RAISE(ABORT,'immutable source domain');
                END;
            """)
            self._conn.execute("BEGIN IMMEDIATE")
            row = self._conn.execute("SELECT binding_json,profile_json FROM source_domain WHERE singleton=1").fetchone()
            expected = (canonical_json(binding), canonical_json(profile))
            if row is None:
                self._conn.execute("INSERT INTO source_domain VALUES (1,?,?)", expected)
            elif row != expected:
                raise SourceJournalConflict("source binding or profile changed")
            self._conn.execute("COMMIT")
            self._verify_history()
        except Exception:
            self._conn.close()
            raise

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> SourceEvidenceStore:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def _verify_history(self) -> None:
        previous = None
        sequence = 0
        for seq, predecessor, content, payload in self._conn.execute(
                "SELECT seq,previous_digest,content_digest,payload_json FROM source_evidence ORDER BY seq"):
            sequence += 1
            current = verdict_from_json(payload)
            expected_previous = ZERO_DIGEST if previous is None else previous.content_digest
            if (seq != sequence or predecessor != expected_previous or current.content_digest != content
                    or current.binding != self.binding or current.profile != self.profile
                    or current.snapshot.previous_verdict_digest != expected_previous
                    or evaluate_source(self.binding, self.profile, current.snapshot, previous=previous) != current):
                raise SourceJournalConflict("source journal evidence chain invalid")
            previous = current

    def latest(self) -> SourceVerdict | None:
        record = self.latest_record()
        return None if record is None else record[1]

    def latest_record(self) -> tuple[int, SourceVerdict] | None:
        """One SELECT captures the committed sequence and exact immutable cut."""
        row = self._conn.execute(
            "SELECT seq,content_digest,payload_json FROM source_evidence ORDER BY seq DESC LIMIT 1").fetchone()
        return self._record(row)

    def read_record(self, sequence: int) -> tuple[int, SourceVerdict] | None:
        if type(sequence) is not int or sequence < 1:
            raise ValueError("positive source evidence sequence required")
        row = self._conn.execute(
            "SELECT seq,content_digest,payload_json FROM source_evidence WHERE seq=?", (sequence,)).fetchone()
        return self._record(row)

    @staticmethod
    def _record(row: tuple | None) -> tuple[int, SourceVerdict] | None:
        if row is None:
            return None
        verdict = verdict_from_json(row[2])
        if verdict.content_digest != row[1]:
            raise SourceJournalConflict("source journal digest mismatch")
        return row[0], verdict

    def count(self) -> int:
        return self._conn.execute("SELECT COUNT(*) FROM source_evidence").fetchone()[0]

    def append(self, verdict: SourceVerdict, *, expected_previous_digest: str) -> int:
        if verdict.binding != self.binding or verdict.profile != self.profile:
            raise SourceJournalConflict("source journal domain mismatch")
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            duplicate = self._conn.execute(
                "SELECT seq,payload_json FROM source_evidence WHERE content_digest=?",
                (verdict.content_digest,)).fetchone()
            if duplicate is not None:
                if duplicate[1] != canonical_json(verdict):
                    raise SourceJournalConflict("same evidence identity with different content")
                self._conn.execute("COMMIT")
                return duplicate[0]
            previous = self.latest()
            actual_previous = ZERO_DIGEST if previous is None else previous.content_digest
            if expected_previous_digest != actual_previous or verdict.snapshot.previous_verdict_digest != actual_previous:
                raise SourceJournalConflict("source evidence predecessor CAS failed")
            if evaluate_source(self.binding, self.profile, verdict.snapshot, previous=previous) != verdict:
                raise SourceJournalConflict("source verdict does not derive from its evidence")
            sequence = self.count() + 1
            self._conn.execute("INSERT INTO source_evidence VALUES (?,?,?,?)",
                               (sequence, actual_previous, verdict.content_digest, canonical_json(verdict)))
            self._conn.execute("COMMIT")
            return sequence
        except Exception:
            self._conn.execute("ROLLBACK")
            raise
