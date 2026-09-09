"""Isolated deterministic G001 adapter/journal/consumer qualification."""
from __future__ import annotations

import hashlib
import json
import sqlite3
import sys
import tempfile
from contextlib import closing
from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from live.evidence_store_v0_1 import SourceEvidenceStore, SourceJournalConflict
from live.source_health_v0_1 import (
    ZERO_DIGEST, CollectorSourceAdapter, ControlFact, PumpFact, ReceiptFact,
    SourceBinding, SourceProfile, SourceProgress, canonical_json,
    source_consumer_evidence, verdict_from_json, witness,
)


BASE = datetime(2026, 9, 9, tzinfo=timezone.utc)
LIVE = "LIVE_WEBSOCKET_EVENT_V0_3_4"
GAP = "GAP_RECONCILIATION_V0_3_4"
CHECKS: dict[str, bool] = {}


def at(seconds: int) -> str:
    return (BASE + timedelta(seconds=seconds)).isoformat(timespec="microseconds")


def check(name: str, value: bool) -> None:
    CHECKS[name] = bool(value)
    if not value:
        raise AssertionError(name)


def raises(cls: type[Exception], call) -> bool:
    try:
        call()
    except cls:
        return True
    return False


def change(path: Path, sql: str, args: tuple = ()) -> None:
    with closing(sqlite3.connect(path)) as conn, conn:
        conn.execute(sql, args)


def fixture(path: Path) -> SourceBinding:
    with closing(sqlite3.connect(path)) as conn, conn:
        conn.executescript("""
            CREATE TABLE pump_events (
                event_key TEXT PRIMARY KEY, signature TEXT NOT NULL, slot INTEGER,
                source_decoded_file TEXT NOT NULL, inserted_at_utc TEXT NOT NULL);
            CREATE TABLE collector_events (
                id INTEGER PRIMARY KEY, event_type TEXT NOT NULL,
                happened_at_utc TEXT NOT NULL, details_json TEXT NOT NULL);
            CREATE TABLE websocket_observations (
                signature TEXT PRIMARY KEY, slot INTEGER, received_at_utc TEXT NOT NULL,
                executed INTEGER NOT NULL, raw_notification_json TEXT);
            CREATE TABLE gap_jobs_v034 (
                gap_key TEXT PRIMARY KEY, gap_started_at_utc TEXT NOT NULL,
                gap_ended_at_utc TEXT NOT NULL, status TEXT NOT NULL,
                before_slot INTEGER, after_slot INTEGER,
                confirmed_blocks_checked INTEGER NOT NULL, events_recovered INTEGER NOT NULL,
                last_error TEXT);
        """)
        conn.executemany("INSERT INTO collector_events VALUES (?,?,?,?)", (
            (1, "COLLECTOR_V0_3_START", at(0), '{"db_path":"DO_NOT_RETAIN","raw_path":"DO_NOT_RETAIN"}'),
            (2, "WS_CONNECTED", at(1), '{"error":"DO_NOT_RETAIN"}'),
            (3, "PUMP_SUBSCRIPTION_ACTIVE", at(2), '{"subscription_id":1}')))
        conn.executemany("INSERT INTO pump_events VALUES (?,?,?,?,?)", (
            ("event1", "sig1", 101, LIVE, at(3)), ("event2", "sig2", 102, LIVE, at(8))))
        conn.executemany("INSERT INTO websocket_observations VALUES (?,?,?,?,?)", (
            ("sig1", 101, at(3), 1, "DO_NOT_RETAIN"), ("sig2", 102, at(10), 1, "DO_NOT_RETAIN")))
    return SourceBinding("SYNTHETIC:COLLECTOR:ONE", at(3), (
        witness("pump_events", PumpFact(1, "event1", "sig1", 101, LIVE, at(3))),
        witness("collector_events", ControlFact(1, "COLLECTOR_V0_3_START", at(0))),
        witness("websocket_observations", ReceiptFact(1, "sig1", 101, at(3), 1))))


def observe(path: Path, binding: SourceBinding, *, previous=None, profile=None, now=12, cut=9,
            adapter_cls=CollectorSourceAdapter):
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    result = adapter_cls(path).observe(binding, profile or SourceProfile(),
                                      observed_at_utc=at(now), requested_cut_utc=at(cut), previous=previous)
    if hashlib.sha256(path.read_bytes()).hexdigest() != before:
        raise AssertionError("source adapter changed its fixture")
    return result


def add_gap(path: Path, status: str) -> None:
    change(path, "INSERT INTO gap_jobs_v034 VALUES (?,?,?,?,?,?,?,?,?)",
           ("public-gap", at(4), at(5), status, 101, 104, 2, 1, "DO_NOT_RETAIN"))


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="meme_live_g001_") as directory:
        temporary = Path(directory)
        count = 0

        def fresh():
            nonlocal count
            count += 1
            path = temporary / f"collector-{count}.sqlite3"
            return path, fixture(path)

        path, binding = fresh()
        good = observe(path, binding)
        view = source_consumer_evidence(good, expected_source_identity=binding.source_identity,
                                        required_cut_utc=at(9), now_utc=at(12))
        check("healthy_actual_adapter_to_consumer_observed_prefix", good.disposition == "HEALTHY"
              and good.covered_from_utc == at(3) and good.covered_through_utc == at(9)
              and view.observed_prefix_supported and view.evidence_digest == good.content_digest)
        check("sanitized_immutable_nested_facts", "DO_NOT_RETAIN" not in canonical_json(good)
              and raises(FrozenInstanceError, lambda: setattr(good.snapshot.receipts[0], "slot", 999))
              and raises(ValueError, lambda: replace(good.snapshot, receipts=list(good.snapshot.receipts))))
        check("source_connection_enforces_query_only", raises(sqlite3.OperationalError,
              lambda: readonly_write(path)))
        check("immutable_roundtrip_same_digest", verdict_from_json(canonical_json(good)) == good)
        check("source_consumer_checks_binding_cut_and_age", all(not item.observed_prefix_supported for item in (
            source_consumer_evidence(good, expected_source_identity="0" * 64, required_cut_utc=at(9), now_utc=at(12)),
            source_consumer_evidence(good, expected_source_identity=binding.source_identity, required_cut_utc=at(10), now_utc=at(12)),
            source_consumer_evidence(good, expected_source_identity=binding.source_identity, required_cut_utc=at(9), now_utc=at(40)))))
        check("forged_healthy_constructor_rejected", raises(ValueError,
              lambda: replace(good, progress=replace(good.progress, connection_state="LOST"))))

        unknown_path, unknown_binding = fresh()
        change(unknown_path, "DELETE FROM collector_events WHERE id=3")
        unknown = observe(unknown_path, unknown_binding)
        check("receipt_without_subscription_is_unknown", unknown.disposition == "UNKNOWN"
              and "SUBSCRIPTION_COVERAGE_UNOBSERVED" in unknown.reasons)
        unordered_path, unordered_binding = fresh()
        change(unordered_path, "DELETE FROM collector_events WHERE id=2")
        check("subscription_requires_ordered_connection_controls", observe(unordered_path, unordered_binding).disposition == "LOST")
        tail_path, tail_binding = fresh()
        change(tail_path, "DELETE FROM websocket_observations WHERE rowid=2")
        tail = observe(tail_path, tail_binding)
        check("recent_pump_activity_does_not_cover_unobserved_tail", tail.disposition == "UNKNOWN"
              and "UNOBSERVED_RECEIPT_TAIL" in tail.reasons)

        for status in ("PENDING", "RETRY", "IN_PROGRESS", "DONE", "DONE_NO_BOUNDARY", "DONE_NO_INTERIOR"):
            gap_path, gap_binding = fresh()
            add_gap(gap_path, status)
            result = observe(gap_path, gap_binding)
            check(f"explicit_gap_{status}_denies", result.disposition == "GAP"
                  and "DO_NOT_RETAIN" not in canonical_json(result))
        gap_path, gap_binding = fresh()
        gap_initial = observe(gap_path, gap_binding)
        add_gap(gap_path, "DONE")
        gap_result = observe(gap_path, gap_binding, previous=gap_initial)
        change(gap_path, "INSERT INTO websocket_observations VALUES (?,?,?,?,?)", ("sig3", 103, at(20), 1, ""))
        gap_later = observe(gap_path, gap_binding, previous=gap_result, now=22, cut=19)
        check("new_receipt_preserves_recorded_gap_and_prior_digest", gap_later.disposition == "GAP"
              and gap_later.progress.gaps == gap_result.progress.gaps
              and gap_later.snapshot.previous_verdict_digest == gap_result.content_digest)
        control_gap_path, control_gap_binding = fresh()
        change(control_gap_path, "INSERT INTO collector_events VALUES (?,?,?,?)",
               (4, "DATA_GAP_CLOSED", at(6), json.dumps({"gap_started_at_utc": at(4), "gap_ended_at_utc": at(5), "error": "DO_NOT_RETAIN"})))
        check("gap_control_denies_when_job_missing", observe(control_gap_path, control_gap_binding).disposition == "GAP")

        for kind in ("recorded_gap", "control_gap", "uncertain_boundary", "interrupted_boundary"):
            later_path, later_binding = fresh()
            initial = observe(later_path, later_binding, now=10)
            change(later_path, "INSERT INTO websocket_observations VALUES (?,?,?,?,?)", ("sig3", 103, at(12), 1, ""))
            reason = "UNRESOLVED_RECORDED_GAP"
            if kind == "recorded_gap":
                change(later_path, "INSERT INTO gap_jobs_v034 VALUES (?,?,?,?,?,?,?,?,?)",
                       ("later-gap", at(10), at(11), "DONE", 102, 103, 2, 1, "DO_NOT_RETAIN"))
            elif kind == "control_gap":
                change(later_path, "INSERT INTO collector_events VALUES (?,?,?,?)", (4, "DATA_GAP_CLOSED", at(11),
                       json.dumps({"gap_started_at_utc": at(10), "gap_ended_at_utc": at(11)})))
            else:
                reason = "UNCERTAIN_OR_INTERRUPTED_PREFIX"
                event = "DECODE_ERROR" if kind == "uncertain_boundary" else "WS_DISCONNECTED"
                change(later_path, "INSERT INTO collector_events VALUES (?,?,?,?)", (4, event, at(11), "{}"))
            later = observe(later_path, later_binding, previous=initial, now=13)
            check(f"older_requested_cut_cannot_hide_later_{kind}", reason in later.reasons
                  and not source_consumer_evidence(later, expected_source_identity=later_binding.source_identity,
                      required_cut_utc=at(9), now_utc=at(13)).observed_prefix_supported
                  and later.binding == initial.binding and later.snapshot.requested_cut_utc == at(9))
            check(f"healthy_constructor_rejects_later_{kind}", raises(ValueError, lambda: replace(
                later, disposition="HEALTHY", reasons=(), covered_from_utc=at(3), covered_through_utc=at(9))))
            later_journal_path = temporary / f"{kind}-evidence.sqlite3"
            with SourceEvidenceStore(later_journal_path, later_binding, SourceProfile()) as journal:
                journal.append(initial, expected_previous_digest=ZERO_DIGEST)
                journal.append(later, expected_previous_digest=initial.content_digest)
                captured = journal.latest_record()
            with SourceEvidenceStore(later_journal_path, later_binding, SourceProfile()) as journal:
                retained = observe(later_path, later_binding, previous=journal.latest(), now=14)
                journal.append(retained, expected_previous_digest=later.content_digest)
                captured_port = source_consumer_evidence(journal.read_record(captured[0])[1],
                    expected_source_identity=later_binding.source_identity, required_cut_utc=at(9), now_utc=at(14))
                check(f"later_{kind}_survives_reopen_and_captured_consumption", reason in retained.reasons
                      and reason in captured_port.reasons and not captured_port.observed_prefix_supported
                      and journal.read_record(captured[0]) == captured)

        before_path, before_binding = fresh()
        change(before_path, "INSERT INTO gap_jobs_v034 VALUES (?,?,?,?,?,?,?,?,?)",
               ("before-origin", at(0), at(1), "DONE", 1, 2, 0, 0, ""))
        check("gap_ending_before_original_origin_keeps_historical_scope", observe(before_path, before_binding).disposition == "HEALTHY")

        for kind in ("WS_STALE", "WS_DISCONNECTED", "LIVE_COLLECTION_STOPPED"):
            lost_path, lost_binding = fresh()
            change(lost_path, "INSERT INTO collector_events VALUES (?,?,?,?)", (4, kind, at(11), "{}"))
            check(f"{kind}_is_lost", observe(lost_path, lost_binding).disposition == "LOST")
        repeat_path, repeat_binding = fresh()
        with closing(sqlite3.connect(repeat_path)) as conn, conn:
            conn.executemany("INSERT INTO collector_events VALUES (?,?,?,?)", (
                (4, "COLLECTOR_V0_3_START", at(11), "{}"), (5, "WS_CONNECTED", at(12), "{}"),
                (6, "PUMP_SUBSCRIPTION_ACTIVE", at(13), "{}")))
            conn.execute("INSERT INTO websocket_observations VALUES (?,?,?,?,?)", ("sig3", 103, at(15), 1, ""))
        repeated = observe(repeat_path, repeat_binding, now=16, cut=14)
        check("uncertain_restart_cannot_reset_coverage_origin", repeated.disposition == "UNKNOWN"
              and "UNCERTAIN_OR_INTERRUPTED_PREFIX" in repeated.reasons)

        clock_path, clock_binding = fresh()
        clock_initial = observe(clock_path, clock_binding)
        change(clock_path, "INSERT INTO pump_events VALUES (?,?,?,?,?)", ("event3", "sig3", 103, LIVE, at(4)))
        decreasing = observe(clock_path, clock_binding, previous=clock_initial)
        check("decreasing_pump_utc_preserves_rowid_order_and_maximum_activity", decreasing.disposition == "HEALTHY"
              and decreasing.progress.cursors[0].rowid == 3 and decreasing.progress.maximum_pump_activity_utc == at(8))
        change(clock_path, "INSERT INTO pump_events VALUES (?,?,?,?,?)", ("event4", "sig4", 104, LIVE, at(30)))
        future = observe(clock_path, clock_binding, previous=decreasing)
        check("future_source_utc_denies_without_progress_commit", "FUTURE_SOURCE_UTC" in future.reasons
              and future.progress == decreasing.progress)
        bad_clock = observe(path, binding, previous=good, now=11)
        check("probe_clock_regression_denies", "EVIDENCE_CLOCK_OR_CUT_REGRESSION" in bad_clock.reasons)
        for mutation, expected, name in (
            ("DELETE FROM pump_events WHERE rowid=2", "SOURCE_CURSOR_REGRESSION", "cursor_regression"),
            ("DELETE FROM pump_events WHERE rowid=1", "SOURCE_WITNESS_MISSING", "missing_anchor"),
            ("UPDATE pump_events SET event_key='changed' WHERE rowid=1", "SOURCE_WITNESS_CHANGED", "changed_anchor"),
            ("UPDATE pump_events SET event_key='changed' WHERE rowid=2", "SOURCE_WITNESS_CHANGED", "changed_cursor")):
            changed_path, changed_binding = fresh()
            initial = observe(changed_path, changed_binding)
            change(changed_path, mutation)
            result = observe(changed_path, changed_binding, previous=initial)
            check(name, expected in result.reasons and result.progress.cursors == initial.progress.cursors)
        restored_path, restored_binding = fresh()
        restored_initial = observe(restored_path, restored_binding)
        change(restored_path, "UPDATE pump_events SET event_key='changed' WHERE rowid=1")
        broken = observe(restored_path, restored_binding, previous=restored_initial)
        change(restored_path, "UPDATE pump_events SET event_key='event1' WHERE rowid=1")
        restored = observe(restored_path, restored_binding, previous=broken)
        check("restored_anchor_requires_integrity_reconciliation", restored.disposition == "REGRESSION"
              and restored.progress.integrity_reasons == broken.progress.integrity_reasons)
        lineage = observe(path, replace(binding, lineage="SYNTHETIC:OTHER"), previous=good)
        check("changed_lineage_denied", "SOURCE_IDENTITY_CHANGED" in lineage.reasons)
        unsupported_path, unsupported_binding = fresh()
        change(unsupported_path, "UPDATE pump_events SET source_decoded_file='FUTURE_SOURCE' WHERE rowid=2")
        check("unknown_source_marker_denied", "SOURCE_MARKER_UNSUPPORTED" in observe(unsupported_path, unsupported_binding).reasons)
        recovery_path, recovery_binding = fresh()
        recovery_initial = observe(recovery_path, recovery_binding)
        change(recovery_path, "INSERT INTO pump_events VALUES (?,?,?,?,?)", ("recovered", "sigOld", 50, GAP, at(50)))
        recovery = observe(recovery_path, recovery_binding, previous=recovery_initial, now=51, cut=10)
        check("recovery_row_does_not_refresh_live_receipt", recovery.disposition == "STALE"
              and recovery.progress.last_live_receipt_utc == at(10))

        budget_path, budget_binding = fresh()
        bounded = SourceProfile(max_rows_per_table=3)
        bounded_initial = observe(budget_path, budget_binding, profile=bounded)
        with closing(sqlite3.connect(budget_path)) as conn, conn:
            conn.executemany("INSERT INTO pump_events VALUES (?,?,?,?,?)",
                             [(f"extra{i}", f"extraSig{i}", 200+i, LIVE, at(8)) for i in range(4)])
        exhausted = observe(budget_path, budget_binding, profile=bounded, previous=bounded_initial)
        check("page_exhaustion_never_advances_partial_cursor", "SOURCE_PAGE_BUDGET_EXHAUSTED" in exhausted.reasons
              and exhausted.progress == bounded_initial.progress and not exhausted.snapshot.cursors)
        missing_path, missing_binding = fresh()
        change(missing_path, "DROP TABLE websocket_observations")
        check("missing_schema_is_unknown", "SOURCE_UNAVAILABLE_OR_SCHEMA_MISSING" in observe(missing_path, missing_binding).reasons)

        class FailingAdapter(CollectorSourceAdapter):
            def _open(self):
                raise sqlite3.OperationalError("DO_NOT_RETAIN credential-bearing error")

        failure = observe(path, binding, previous=good, adapter_cls=FailingAdapter)
        check("read_failure_preserves_progress_and_sanitizes_error", failure.disposition != "HEALTHY"
              and failure.progress == good.progress and "DO_NOT_RETAIN" not in canonical_json(failure))

        journal_path = temporary / "source-evidence.sqlite3"
        with SourceEvidenceStore(journal_path, binding, SourceProfile()) as journal:
            check("first_durable_append", journal.append(good, expected_previous_digest=ZERO_DIGEST) == 1)
            check("exact_idempotent_append", journal.append(good, expected_previous_digest=ZERO_DIGEST) == 1 and journal.count() == 1)
            check("journal_rejects_forged_progress", raises(SourceJournalConflict, lambda: journal.append(
                replace(failure, progress=SourceProgress()), expected_previous_digest=good.content_digest)))
            journal.append(failure, expected_previous_digest=good.content_digest)
            check("journal_rejects_domain_reanchor", raises(SourceJournalConflict,
                  lambda: journal.append(lineage, expected_previous_digest=failure.content_digest)))
        with SourceEvidenceStore(journal_path, binding, SourceProfile()) as journal:
            reopened = journal.latest()
            check("reopen_reconstructs_exact_previous_verdict", reopened == failure and journal.count() == 2)
            recovered = observe(path, binding, previous=reopened, now=13, cut=10)
            check("transient_read_failure_can_recover_same_identity", recovered.disposition == "HEALTHY")
            journal.append(recovered, expected_previous_digest=reopened.content_digest)
            captured_record = journal.latest_record()
            stale = observe(path, binding, previous=recovered, now=50, cut=10)
            check("stale_receipt_after_restart_denies", stale.disposition == "STALE")
            with SourceEvidenceStore(journal_path, binding, SourceProfile()) as racer:
                alternative = observe(path, binding, previous=recovered, now=51, cut=10)
                journal.append(stale, expected_previous_digest=recovered.content_digest)
                check("atomic_captured_sequence_keeps_exact_cut_after_later_append",
                      captured_record == (3, recovered) and racer.read_record(captured_record[0]) == captured_record)
                check("concurrent_writer_predecessor_CAS", raises(SourceJournalConflict,
                      lambda: racer.append(alternative, expected_previous_digest=recovered.content_digest)))
            change(path, "INSERT INTO websocket_observations VALUES (?,?,?,?,?)", ("fresh", 300, at(52), 1, ""))
            fresh_again = observe(path, binding, previous=stale, now=53, cut=51)
            check("transient_staleness_recovers_without_reset", fresh_again.disposition == "HEALTHY")
            journal.append(fresh_again, expected_previous_digest=stale.content_digest)
        with SourceEvidenceStore(journal_path, binding, SourceProfile()) as journal:
            check("durable_coverage_and_cursor_reopen", journal.latest() == fresh_again and journal.count() == 5)
            check("exact_evidence_sequence_survives_reopen", journal.read_record(3) == captured_record
                  and journal.latest_record() == (5, fresh_again) and journal.read_record(999) is None)
        check("journal_refuses_changed_binding_or_profile", raises(SourceJournalConflict,
              lambda: SourceEvidenceStore(journal_path, replace(binding, lineage="OTHER"), SourceProfile()))
              and raises(SourceJournalConflict, lambda: SourceEvidenceStore(journal_path, binding, SourceProfile(freshness_seconds=40))))
        source_before = hashlib.sha256(path.read_bytes()).hexdigest()
        check("journal_cannot_open_collector_as_output", raises(SourceJournalConflict,
              lambda: SourceEvidenceStore(path, binding, SourceProfile()))
              and source_before == hashlib.sha256(path.read_bytes()).hexdigest())
        with closing(sqlite3.connect(journal_path)) as conn, conn:
            check("evidence_rows_are_append_only", raises(sqlite3.IntegrityError,
                  lambda: conn.execute("UPDATE source_evidence SET payload_json='{}' WHERE seq=1")))
            check("source_store_contains_no_economic_state", {r[0] for r in conn.execute(
                  "SELECT name FROM sqlite_master WHERE type='table'")} == {"source_domain", "source_evidence"})
            check("sqlite_integrity", conn.execute("PRAGMA integrity_check").fetchone() == ("ok",))
        # Identity breaks remain latched across an actual durable reopen.
        break_journal_path = temporary / "identity-break-evidence.sqlite3"
        with SourceEvidenceStore(break_journal_path, restored_binding, SourceProfile()) as journal:
            journal.append(restored_initial, expected_previous_digest=ZERO_DIGEST)
            journal.append(broken, expected_previous_digest=restored_initial.content_digest)
        with SourceEvidenceStore(break_journal_path, restored_binding, SourceProfile()) as journal:
            denied = observe(restored_path, restored_binding, previous=journal.latest())
            check("integrity_break_survives_durable_reopen", denied.disposition == "REGRESSION")
        check("all_adapter_reads_left_source_fixtures_unchanged", True)
    print(json.dumps({"status": "IMPLEMENTED_PENDING_PROJECT_REVIEW", "checks": CHECKS,
                      "check_count": len(CHECKS), "failed": [k for k, v in CHECKS.items() if not v],
                      "data": "TEMPORARY_SYNTHETIC_SQLITE_ONLY", "network": "NONE"}, sort_keys=True, indent=2))
    return 0


def readonly_write(path: Path) -> None:
    conn = CollectorSourceAdapter(path)._open()
    try:
        conn.execute("DELETE FROM pump_events")
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
