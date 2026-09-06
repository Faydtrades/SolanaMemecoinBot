from __future__ import annotations

import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from phase6.frozen_oos_collection_lifecycle_v0_1 import (  # noqa: E402
    EXTENSION_SECONDS,
    H1_MINIMUM_ELIGIBLE_ENTRIES,
    MAXIMUM_DURATION_SECONDS,
    MINIMUM_DURATION_SECONDS,
    MODEL_FINGERPRINT,
    NO_PEEK_STATUS_FIELDS,
    P6_POLICY_SET_SHA256,
    P6_PROTOCOL_FINGERPRINT,
    FrozenOOSLifecycleV01,
    LifecycleError,
    StartRequest,
    _read_envelope,
    _write_envelope,
    canonical_sha256,
    dt_parse,
    dt_text,
    git_head,
    git_status,
    h1_eligible_count,
    process_birth_token,
    protocol_review_payload,
    run_worker,
    sha256_file,
    source_coverage_probe,
)


UTC = timezone.utc
START = datetime(2026, 9, 7, 0, 0, 0, tzinfo=UTC)


@dataclass
class MutableClock:
    value: datetime = START

    def __call__(self) -> datetime:
        return self.value


def _source(path: Path, *, post_end: bool = False, pending_gap: bool = False) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.executescript(
            """
            CREATE TABLE pump_events(
                event_key TEXT, signature TEXT, slot INTEGER, event_type TEXT,
                mint TEXT, user_wallet TEXT, sol_amount_lamports INTEGER,
                token_amount_raw INTEGER, is_buy INTEGER,
                virtual_sol_reserve_raw INTEGER,
                virtual_token_reserve_raw INTEGER, real_sol_reserve_raw INTEGER,
                real_token_reserve_raw INTEGER, event_at_utc TEXT,
                received_at_utc TEXT, confirmation_status TEXT,
                source_program TEXT, source_decoded_file TEXT,
                inserted_at_utc TEXT,
                pump_timestamp INTEGER, creator_wallet TEXT,
                virtual_sol_reserves INTEGER, virtual_token_reserves INTEGER,
                real_sol_reserves INTEGER, real_token_reserves INTEGER,
                decoded_at_utc TEXT, quote_mint TEXT, quote_amount_raw INTEGER,
                virtual_quote_reserves INTEGER, real_quote_reserves INTEGER
            );
            CREATE TABLE gap_jobs_v034(
                gap_key TEXT PRIMARY KEY, gap_started_at_utc TEXT NOT NULL,
                gap_ended_at_utc TEXT NOT NULL, status TEXT NOT NULL
            );
            """
        )
        rows = [
            ("e1", "s1", 1, "LAUNCH", "M1", "U", 1, 1, 1, 1, 1, 1, 1,
             dt_text(START + timedelta(seconds=1)), dt_text(START + timedelta(seconds=1)),
             "confirmed", "pump", "LIVE_WEBSOCKET_EVENT_V0_3_4", dt_text(START + timedelta(seconds=1))),
            ("e2", "s2", 2, "TRADE", "M1", "U", 1, 1, 1, 1, 1, 1, 1,
             dt_text(START + timedelta(hours=72) - timedelta(microseconds=1)),
             dt_text(START + timedelta(hours=72) - timedelta(microseconds=1)),
             "confirmed", "pump", "LIVE_WEBSOCKET_EVENT_V0_3_4",
             dt_text(START + timedelta(hours=72))),
        ]
        if post_end:
            rows.append(
                ("e3", "s3", 3, "TRADE", "M1", "U", 1, 1, 1, 1, 1, 1, 1,
                 dt_text(START + timedelta(hours=72)), dt_text(START + timedelta(hours=72)),
                 "confirmed", "pump", "LIVE_WEBSOCKET_EVENT_V0_3_4",
                 dt_text(START + timedelta(hours=73)))
            )
        conn.executemany(
            "INSERT INTO pump_events("
            "event_key,signature,slot,event_type,mint,user_wallet,"
            "sol_amount_lamports,token_amount_raw,is_buy,"
            "virtual_sol_reserve_raw,virtual_token_reserve_raw,"
            "real_sol_reserve_raw,real_token_reserve_raw,event_at_utc,"
            "received_at_utc,confirmation_status,source_program,"
            "source_decoded_file,inserted_at_utc) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            rows,
        )
        conn.execute(
            "INSERT INTO gap_jobs_v034 VALUES(?,?,?,?)",
            ("recovered-long-interval", dt_text(START + timedelta(seconds=1)),
             dt_text(START + timedelta(hours=72)), "DONE"),
        )
        if pending_gap:
            conn.execute(
                "INSERT INTO gap_jobs_v034 VALUES(?,?,?,?)",
                ("gap-1", dt_text(START + timedelta(hours=1)),
                 dt_text(START + timedelta(hours=2)), "RETRY"),
            )
        conn.commit()
    finally:
        conn.close()


def _paper(path: Path, *, include_post_end: bool = False, variant: int = 0) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.executescript(
            """
            CREATE TABLE paper_continuous_signal_contexts_v0_1(
                signal_key TEXT PRIMARY KEY, route_id TEXT, mint TEXT
            );
            CREATE TABLE paper_entry_routes(
                route_id TEXT PRIMARY KEY, signal_observed_at TEXT, state TEXT
            );
            CREATE TABLE paper_fp_binding_inputs_v0_1(
                input_key TEXT PRIMARY KEY, production_p1_rowid INTEGER
            );
            """
        )
        observations = [START + timedelta(hours=1), START + timedelta(hours=7)]
        if include_post_end:
            observations.extend([
                START + timedelta(hours=72) - timedelta(microseconds=1),
                START + timedelta(hours=72),
                START + timedelta(hours=73),
            ])
        for index, observed in enumerate(observations):
            route = f"r{variant}-{index}"
            conn.execute(
                "INSERT INTO paper_continuous_signal_contexts_v0_1 VALUES(?,?,?)",
                (f"signal-{variant}-{index}", route, f"M{index}"),
            )
            conn.execute(
                "INSERT INTO paper_entry_routes VALUES(?,?,?)",
                (route, dt_text(observed), "FILLED"),
            )
            conn.execute(
                "INSERT INTO paper_fp_binding_inputs_v0_1 VALUES(?,?)",
                (f"input-{variant}-{index}", index + 1),
            )
        conn.commit()
    finally:
        conn.close()


class Fixture:
    def __init__(self, base: Path, *, run_id: str = "P6-OOS-SELFTEST") -> None:
        self.base = base
        self.base.mkdir(parents=True, exist_ok=False)
        self.source = base / "source.sqlite3"
        self.runtime = base / "runtime"
        self.clock = MutableClock()
        _source(self.source)
        self.manager = FrozenOOSLifecycleV01(
            PROJECT_ROOT, self.runtime, clock=self.clock, verify_runtime_files=True
        )
        self.run_id = run_id

    def start(self) -> dict:
        manifest = self.manager.start(StartRequest(
            source_db_path=self.source,
            runtime_root=self.runtime,
            repository_commit=git_head(PROJECT_ROOT),
            start_at=START,
            source_start_cursor=0,
            run_id=self.run_id,
        ))
        _paper(Path(manifest["paper_db_path"]))
        return manifest

    def complete(self, *, cursor: int = 2, count: int = 2) -> None:
        self.clock.value = START + timedelta(hours=72)
        self.manager.record_progress(
            self.run_id,
            source_cursor=cursor,
            source_watermark=cursor,
            source_row_count=count,
            candidate_count=count,
            h1_eligible_sample_count=1,
            coverage_complete_through=self.clock.value,
        )


def _raises(reason: str, callback: Callable[[], object]) -> None:
    try:
        callback()
    except LifecycleError as exc:
        assert exc.reason == reason, (exc.reason, reason)
    else:
        raise AssertionError(f"expected {reason}")


class Matrix:
    def __init__(self) -> None:
        self.results: list[tuple[str, bool]] = []

    def check(self, name: str, callback: Callable[[], None]) -> None:
        callback()
        self.results.append((name, True))
        print(f"[PASS] {name}")


def main() -> int:
    matrix = Matrix()
    git_before = git_status(PROJECT_ROOT)
    protected = {
        path: sha256_file(PROJECT_ROOT / path)
        for path in (
            "src/phase6/frozen_oos_hypothesis_evaluation_v0_1.py",
            "scripts/phase6_frozen_oos_hypothesis_evaluation_v0_1.py",
            "scripts/phase6_frozen_oos_hypothesis_evaluation_selftest_v0_1.py",
            "data/research/phase6/P6_OOS_PROTOCOL_0001/protocol_manifest.json",
            "data/research/phase6/P6_OOS_PROTOCOL_0001/policy_definitions.json",
        )
    }
    with tempfile.TemporaryDirectory(prefix="p6_t002_selftest_") as raw:
        base = Path(raw)

        def fresh_identity() -> None:
            fixture = Fixture(base / "t01")
            manifest = fixture.start()
            assert manifest["run_id"] == fixture.run_id
            assert manifest["source_start_cursor"] == 0
            assert _read_envelope(fixture.manager.run_dir(fixture.run_id) / "run_manifest.json") == manifest
        matrix.check("01 fresh START creates one immutable OOS identity", fresh_identity)

        def exact_72h() -> None:
            fixture = Fixture(base / "t02")
            manifest = fixture.start()
            assert dt_parse(manifest["minimum_end_at_utc"]) - dt_parse(manifest["start_at_utc"]) == timedelta(seconds=MINIMUM_DURATION_SECONDS)
        matrix.check("02 initial end equals start plus exactly 72 hours", exact_72h)

        def duplicate_start() -> None:
            fixture = Fixture(base / "t03")
            fixture.start()
            _raises("UNFINISHED_RUN_EXISTS", lambda: fixture.manager.start(StartRequest(
                source_db_path=fixture.source, runtime_root=fixture.runtime,
                repository_commit=git_head(PROJECT_ROOT), start_at=START,
                source_start_cursor=0, run_id="SECOND-RUN",
            )))
        matrix.check("03 duplicate START fails closed", duplicate_start)

        def stop_resume_identity() -> None:
            fixture = Fixture(base / "t04")
            manifest = fixture.start()
            token = process_birth_token(os.getpid())
            assert token
            fixture.manager.begin_segment(fixture.run_id, pid=os.getpid(), birth_token=token)
            fixture.manager.end_segment(fixture.run_id, termination="STOP_REQUESTED")
            fixture.manager.begin_segment(fixture.run_id, pid=os.getpid(), birth_token=token)
            assert fixture.manager._manifest(fixture.run_id)["run_id"] == manifest["run_id"]
        matrix.check("04 STOP plus RESUME retains run_id", stop_resume_identity)

        def stop_resume_times() -> None:
            fixture = Fixture(base / "t05")
            manifest = fixture.start()
            before = (manifest["start_at_utc"], manifest["initial_target_end_at_utc"])
            token = process_birth_token(os.getpid())
            assert token
            fixture.manager.begin_segment(fixture.run_id, pid=os.getpid(), birth_token=token)
            fixture.manager.end_segment(fixture.run_id, termination="STOP_REQUESTED")
            after = fixture.manager._manifest(fixture.run_id)
            assert before == (after["start_at_utc"], after["initial_target_end_at_utc"])
        matrix.check("05 STOP plus RESUME retains exact start and end", stop_resume_times)

        def cursor_no_duplicate() -> None:
            fixture = Fixture(base / "t06")
            fixture.start()
            fixture.manager.record_progress(
                fixture.run_id, source_cursor=2, source_watermark=2,
                source_row_count=2, candidate_count=1, h1_eligible_sample_count=1,
                coverage_complete_through=START + timedelta(hours=1),
            )
            fixture.manager.record_progress(
                fixture.run_id, source_cursor=2, source_watermark=2,
                source_row_count=2, candidate_count=1, h1_eligible_sample_count=1,
                coverage_complete_through=START + timedelta(hours=1),
            )
            _raises("CORRUPT_LIFECYCLE_STATE", lambda: fixture.manager.record_progress(
                fixture.run_id, source_cursor=2, source_watermark=2,
                source_row_count=3, candidate_count=1, h1_eligible_sample_count=1,
                coverage_complete_through=START + timedelta(hours=1),
            ))
        matrix.check("06 durable cursor resumes without duplicate source accounting", cursor_no_duplicate)

        def multiple_segments() -> None:
            fixture = Fixture(base / "t07")
            fixture.start()
            token = process_birth_token(os.getpid())
            assert token
            for _ in range(3):
                fixture.manager.begin_segment(fixture.run_id, pid=os.getpid(), birth_token=token)
                fixture.manager.end_segment(fixture.run_id, termination="STOP_REQUESTED")
            state = fixture.manager._state(fixture.run_id)
            assert [s["segment_index"] for s in state["segments"]] == [1, 2, 3]
            assert len({s["segment_id"] for s in state["segments"]}) == 3
        matrix.check("07 multiple stop/resume segments reconcile deterministically", multiple_segments)

        def abrupt_resume() -> None:
            fixture = Fixture(base / "t08")
            fixture.start()
            fixture.manager.begin_segment(fixture.run_id, pid=999_999_991, birth_token="dead")
            state = fixture.manager.reconcile_process(fixture.run_id)
            assert state["segments"][0]["termination"] == "STALE_PROCESS_AFTER_RESTART"
            token = process_birth_token(os.getpid())
            assert token
            segment = fixture.manager.begin_segment(fixture.run_id, pid=os.getpid(), birth_token=token)
            assert segment["segment_index"] == 2
        matrix.check("08 abrupt termination can resume", abrupt_resume)

        def stale_pid() -> None:
            fixture = Fixture(base / "t09")
            fixture.start()
            fixture.manager.begin_segment(fixture.run_id, pid=999_999_992, birth_token="stale")
            assert fixture.manager.reconcile_process(fixture.run_id)["lifecycle_state"] == "STOPPED"
        matrix.check("09 stale PID after restart is handled", stale_pid)

        def pid_reuse() -> None:
            fixture = Fixture(base / "t10")
            fixture.start()
            fixture.manager.begin_segment(fixture.run_id, pid=os.getpid(), birth_token="wrong-birth-token")
            state = fixture.manager.reconcile_process(fixture.run_id)
            assert state["active_process"] is None
        matrix.check("10 PID reuse cannot impersonate old process", pid_reuse)

        def source_mismatch() -> None:
            fixture = Fixture(base / "t11")
            fixture.start()
            other = fixture.base / "other.sqlite3"
            _source(other)
            _raises("SOURCE_IDENTITY_CHANGED", lambda: fixture.manager.verify_identity(fixture.run_id, source_db_path=other))
        matrix.check("11 source identity mismatch fails closed", source_mismatch)

        def paper_mismatch() -> None:
            fixture = Fixture(base / "t12")
            fixture.start()
            _raises("PAPER_IDENTITY_CHANGED", lambda: fixture.manager.verify_identity(fixture.run_id, paper_db_path=fixture.base / "other.sqlite3"))
        matrix.check("12 paper identity mismatch fails closed", paper_mismatch)

        def protocol_mismatch() -> None:
            fixture = Fixture(base / "t13")
            fixture.start()
            path = fixture.manager.run_dir(fixture.run_id) / "run_manifest.json"
            path.chmod(0o600)
            payload = _read_envelope(path)
            payload["protocol_fingerprint"] = "0" * 64
            _write_envelope(path, payload)
            _raises("PROTOCOL_MISMATCH", lambda: fixture.manager.verify_identity(fixture.run_id))
        matrix.check("13 protocol or policy mismatch fails closed", protocol_mismatch)

        def repo_mismatch() -> None:
            fixture = Fixture(base / "t14")
            fixture.start()
            _raises("REPOSITORY_RUNTIME_MISMATCH", lambda: fixture.manager.verify_identity(fixture.run_id, repository_commit="0" * 40))
        matrix.check("14 repository/runtime incompatibility fails closed", repo_mismatch)

        def recovered_gap() -> None:
            fixture = Fixture(base / "t15")
            fixture.start()
            fixture.complete()
            assert fixture.manager._state(fixture.run_id)["coverage_status"] == "COMPLETE_THROUGH_TARGET"
        matrix.check("15 recoverable downtime catches up to target", recovered_gap)

        def unrecoverable_gap() -> None:
            fixture = Fixture(base / "t16")
            fixture.start()
            state = fixture.manager.mark_invalid(fixture.run_id, "UNRECOVERABLE_SOURCE_GAP")
            assert state["lifecycle_state"] == "INVALID"
            assert state["coverage_status"] == "OOS_COVERAGE_INCOMPLETE"
        matrix.check("16 unrecoverable gap becomes invalid incomplete coverage", unrecoverable_gap)

        def post_end_excluded() -> None:
            fixture = Fixture(base / "t17")
            manifest = fixture.start()
            paper = Path(manifest["paper_db_path"])
            paper.unlink()
            _paper(paper, include_post_end=True)
            count = h1_eligible_count(paper, START, START + timedelta(hours=72))
            assert count == 1  # Day-one 01:00 only; exact-end/post-end rows are excluded.
        matrix.check("17 post-end rows excluded by half-open frozen window", post_end_excluded)

        def late_resume_exact_end() -> None:
            fixture = Fixture(base / "t18")
            fixture.start()
            fixture.clock.value = START + timedelta(hours=73)
            fixture.manager.record_progress(
                fixture.run_id, source_cursor=3, source_watermark=3,
                source_row_count=3, candidate_count=3, h1_eligible_sample_count=1,
                coverage_complete_through=START + timedelta(hours=72),
            )
            status = fixture.manager.status(fixture.run_id, now=fixture.clock.value)
            assert status["end_at_utc"] == dt_text(START + timedelta(hours=72))
        matrix.check("18 late resume remains bounded at exact frozen end", late_resume_exact_end)

        def early_finalize() -> None:
            fixture = Fixture(base / "t19")
            fixture.start()
            _raises("END_BOUNDARY_NOT_REACHED", lambda: fixture.manager.finalize(fixture.run_id, now=START + timedelta(hours=71)))
        matrix.check("19 finalize before boundary fails closed", early_finalize)

        def incomplete_finalize() -> None:
            fixture = Fixture(base / "t20")
            fixture.start()
            fixture.clock.value = START + timedelta(hours=72)
            _raises("SOURCE_COVERAGE_INCOMPLETE", lambda: fixture.manager.finalize(fixture.run_id, now=fixture.clock.value))
        matrix.check("20 finalize with incomplete coverage fails closed", incomplete_finalize)

        finalized: dict[str, str] = {}

        def successful_finalize() -> None:
            fixture = Fixture(base / "t21")
            fixture.start()
            fixture.complete()
            handoff = fixture.manager.finalize(fixture.run_id, now=fixture.clock.value)
            assert handoff["evaluation_performed"] is False
            assert handoff["source_coverage"]["status"] == "COMPLETE_THROUGH_TARGET"
            assert Path(handoff["source_db_path"]).exists()
            assert Path(handoff["paper_db_path"]).exists()
            finalized["digest"] = handoff["frozen_outcome_digest"]
        matrix.check("21 successful finalize emits frozen handoff manifest", successful_finalize)

        def refinalize_idempotent() -> None:
            fixture = Fixture(base / "t22")
            fixture.start()
            fixture.complete()
            first = fixture.manager.finalize(fixture.run_id, now=fixture.clock.value)
            first_hash = sha256_file(fixture.manager.run_dir(fixture.run_id) / "frozen_handoff_manifest.json")
            second = fixture.manager.finalize(fixture.run_id, now=fixture.clock.value + timedelta(hours=1))
            assert first == second
            assert first_hash == sha256_file(fixture.manager.run_dir(fixture.run_id) / "frozen_handoff_manifest.json")
        matrix.check("22 re-finalization is idempotent", refinalize_idempotent)

        def sample_status() -> None:
            fixture = Fixture(base / "t23")
            fixture.start()
            status = fixture.manager.status(fixture.run_id)
            assert status["h1_eligible_sample_count"] == 0
        matrix.check("23 H1 sample count is available before finalization", sample_status)

        def no_peek_status() -> None:
            fixture = Fixture(base / "t24")
            fixture.start()
            status = fixture.manager.status(fixture.run_id)
            assert tuple(status) == NO_PEEK_STATUS_FIELDS
            forbidden = ("pnl", "profit", "expectancy", "win_rate", "drawdown", "ranking", "go_decision", "winner", "loser")
            assert not any(term in key.lower() for key in status for term in forbidden)
        matrix.check("24 STATUS exposes no outcome/performance metric", no_peek_status)

        def no_evaluation_import() -> None:
            source = (PROJECT_ROOT / "src/phase6/frozen_oos_collection_lifecycle_v0_1.py").read_text(encoding="utf-8")
            assert "evaluate_frozen_inputs_v0_1" not in source
            assert "run_sqlite_evaluation_v0_1" not in source
        matrix.check("25 collection never invokes P6-T001 evaluation", no_evaluation_import)

        def valid_extension() -> None:
            fixture = Fixture(base / "t26")
            fixture.start()
            fixture.clock.value = START + timedelta(hours=72)
            state = fixture.manager.extend(
                fixture.run_id, seconds=EXTENSION_SECONDS,
                reason="H1_SAMPLE_INSUFFICIENT", decided_at=fixture.clock.value,
            )
            assert state["duration_seconds"] == 96 * 3600
        matrix.check("26 72h to 96h extension obeys sample rule", valid_extension)

        def arbitrary_extension() -> None:
            fixture = Fixture(base / "t27")
            fixture.start()
            _raises("INVALID_EXTENSION", lambda: fixture.manager.extend(
                fixture.run_id, seconds=12 * 3600, reason="TECHNICAL_COMPLETENESS",
                decided_at=START + timedelta(hours=72),
            ))
        matrix.check("27 arbitrary extension duration rejected", arbitrary_extension)

        def extension_has_no_outcome_surface() -> None:
            fixture = Fixture(base / "t28")
            fixture.start()
            signature = __import__("inspect").signature(fixture.manager.extend)
            forbidden = {"pnl", "profit_factor", "expectancy", "win_rate", "drawdown", "ranking"}
            assert forbidden.isdisjoint(signature.parameters)
        matrix.check("28 extension API cannot examine outcome metrics", extension_has_no_outcome_surface)

        def maximum_window() -> None:
            fixture = Fixture(base / "t29")
            fixture.start()
            fixture.clock.value = START + timedelta(hours=72)
            extension_count = (
                (MAXIMUM_DURATION_SECONDS - MINIMUM_DURATION_SECONDS)
                // EXTENSION_SECONDS
            )
            for index in range(extension_count):
                decision = START + timedelta(
                    seconds=MINIMUM_DURATION_SECONDS + index * EXTENSION_SECONDS
                )
                fixture.manager.extend(
                    fixture.run_id, seconds=EXTENSION_SECONDS,
                    reason="TECHNICAL_COMPLETENESS", decided_at=decision,
                )
            _raises("INVALID_EXTENSION", lambda: fixture.manager.extend(
                fixture.run_id, seconds=EXTENSION_SECONDS,
                reason="TECHNICAL_COMPLETENESS",
                decided_at=START + timedelta(seconds=MAXIMUM_DURATION_SECONDS),
            ))
        matrix.check("29 maximum 168-hour window enforced", maximum_window)

        def artifacts_external() -> None:
            fixture = Fixture(base / "t30")
            manifest = fixture.start()
            assert not Path(manifest["paper_db_path"]).is_relative_to(PROJECT_ROOT)
            assert not fixture.manager.run_dir(fixture.run_id).is_relative_to(PROJECT_ROOT)
        matrix.check("30 runtime artifacts stay outside repository", artifacts_external)

        def git_unchanged_during_synthetic() -> None:
            assert git_status(PROJECT_ROOT) == git_before
        matrix.check("31 synthetic lifecycle leaves Git worktree unchanged", git_unchanged_during_synthetic)

        def inputs_readonly() -> None:
            fixture = Fixture(base / "t32")
            manifest = fixture.start()
            before_source = sha256_file(fixture.source)
            before_paper = sha256_file(Path(manifest["paper_db_path"]))
            fixture.manager.status(fixture.run_id)
            source_coverage_probe(fixture.source, start=START, target=START + timedelta(hours=72))
            assert sha256_file(fixture.source) == before_source
            assert sha256_file(Path(manifest["paper_db_path"])) == before_paper
        matrix.check("32 inspection is non-destructive to source and paper inputs", inputs_readonly)

        def no_dangerous_capability() -> None:
            texts = "\n".join(
                (PROJECT_ROOT / path).read_text(encoding="utf-8").lower()
                for path in (
                    "src/phase6/frozen_oos_collection_lifecycle_v0_1.py",
                    "scripts/phase6_frozen_oos_collection_lifecycle_v0_1.py",
                )
            )
            forbidden_imports = ("solders.keypair", "send_transaction", "send_raw_transaction", "broadcast_transaction")
            assert not any(item in texts for item in forbidden_imports)
        matrix.check("33 no signer/send/broadcast/custody capability introduced", no_dangerous_capability)

        def protocol_binding() -> None:
            payload = protocol_review_payload()
            assert payload["protocol_fingerprint"] == P6_PROTOCOL_FINGERPRINT
            assert payload["policy_set_sha256"] == P6_POLICY_SET_SHA256
            assert len(MODEL_FINGERPRINT) == 64
        matrix.check("34 lifecycle binds accepted P6-T001 protocol exactly", protocol_binding)

        def phase5_untouched() -> None:
            changed = subprocess.run(
                ("git", "diff", "--name-only", "--", "src/phase5", "scripts/phase5*", "docs/phase5"),
                cwd=PROJECT_ROOT, check=True, capture_output=True, text=True,
            ).stdout.strip()
            assert not changed
        matrix.check("35 Phase-5 implementation remains untouched", phase5_untouched)

        equivalence: dict[str, str] = {}

        def finalize_path(root: Path, interrupted: bool) -> str:
            fixture = Fixture(root, run_id="P6-OOS-EQUIVALENCE")
            fixture.start()
            if interrupted:
                fixture.manager.begin_segment(fixture.run_id, pid=999_999_993, birth_token="abrupt")
                fixture.manager.reconcile_process(fixture.run_id)
            fixture.complete()
            return fixture.manager.finalize(fixture.run_id, now=fixture.clock.value)["frozen_outcome_digest"]

        def uninterrupted_resume_equivalence() -> None:
            uninterrupted = finalize_path(base / "equiv_a", False)
            resumed = finalize_path(base / "equiv_b", True)
            assert uninterrupted == resumed
            equivalence.update({"uninterrupted": uninterrupted, "stop_resume": resumed})
        matrix.check("36 uninterrupted and stop/resume frozen outcomes are identical", uninterrupted_resume_equivalence)

        def backward_state_rejected() -> None:
            fixture = Fixture(base / "t37")
            fixture.start()
            fixture.manager.record_progress(
                fixture.run_id, source_cursor=2, source_watermark=2,
                source_row_count=2, candidate_count=1, h1_eligible_sample_count=1,
                coverage_complete_through=START + timedelta(hours=1),
            )
            _raises("CORRUPT_LIFECYCLE_STATE", lambda: fixture.manager.record_progress(
                fixture.run_id, source_cursor=1, source_watermark=2,
                source_row_count=2, candidate_count=1, h1_eligible_sample_count=1,
                coverage_complete_through=START + timedelta(hours=1),
            ))
        matrix.check("37 backward durable state transition rejected", backward_state_rejected)

        def active_writer_finalize() -> None:
            fixture = Fixture(base / "t38")
            fixture.start()
            fixture.complete()
            token = process_birth_token(os.getpid())
            assert token
            fixture.manager.begin_segment(fixture.run_id, pid=os.getpid(), birth_token=token)
            _raises("ACTIVE_WRITER_CONFLICT", lambda: fixture.manager.finalize(fixture.run_id, now=fixture.clock.value))
        matrix.check("38 active writer blocks finalization", active_writer_finalize)

        def corrupt_state_digest() -> None:
            fixture = Fixture(base / "t39")
            fixture.start()
            path = fixture.manager.run_dir(fixture.run_id) / "lifecycle_state.json"
            raw_state = json.loads(path.read_text(encoding="utf-8"))
            raw_state["payload"]["source_cursor"] = 999
            path.write_text(json.dumps(raw_state), encoding="utf-8")
            _raises("CORRUPT_LIFECYCLE_STATE", lambda: fixture.manager._state(fixture.run_id))
        matrix.check("39 corrupt durable state fails closed", corrupt_state_digest)

        def no_chained_early_extension() -> None:
            fixture = Fixture(base / "t40")
            fixture.start()
            fixture.manager.extend(
                fixture.run_id, seconds=EXTENSION_SECONDS,
                reason="TECHNICAL_COMPLETENESS",
                decided_at=START + timedelta(hours=72),
            )
            _raises("INVALID_EXTENSION", lambda: fixture.manager.extend(
                fixture.run_id, seconds=EXTENSION_SECONDS,
                reason="TECHNICAL_COMPLETENESS",
                decided_at=START + timedelta(hours=72),
            ))
        matrix.check("40 extension cannot be chained before current boundary", no_chained_early_extension)

        def handoff_state_crash_recovery() -> None:
            fixture = Fixture(base / "t41")
            fixture.start()
            fixture.complete()
            first = fixture.manager.finalize(fixture.run_id, now=fixture.clock.value)
            state_path = fixture.manager.run_dir(fixture.run_id) / "lifecycle_state.json"
            state = _read_envelope(state_path)
            state["lifecycle_state"] = "STOPPED"
            state["frozen_handoff_sha256"] = None
            _write_envelope(state_path, state)
            recovered = fixture.manager.finalize(
                fixture.run_id, now=fixture.clock.value + timedelta(seconds=1)
            )
            assert recovered == first
            assert fixture.manager._state(fixture.run_id)["lifecycle_state"] == "FROZEN_READY_FOR_EVALUATION"
        matrix.check("41 crash after handoff write recovers idempotently", handoff_state_crash_recovery)

        def target_tamper_rejected() -> None:
            fixture = Fixture(base / "t42")
            fixture.start()
            path = fixture.manager.run_dir(fixture.run_id) / "lifecycle_state.json"
            state = _read_envelope(path)
            state["target_end_at_utc"] = dt_text(START + timedelta(hours=73))
            _write_envelope(path, state)
            _raises("CORRUPT_LIFECYCLE_STATE", lambda: fixture.manager.verify_identity(fixture.run_id))
        matrix.check("42 recomputed-envelope target tamper is rejected", target_tamper_rejected)

        def live_gap_rechecked_at_finalize() -> None:
            fixture = Fixture(base / "t43")
            fixture.start()
            fixture.complete()
            conn = sqlite3.connect(fixture.source)
            try:
                conn.execute(
                    "INSERT INTO gap_jobs_v034 VALUES(?,?,?,?)",
                    ("late-gap", dt_text(START + timedelta(hours=70)),
                     dt_text(START + timedelta(hours=71)), "RETRY"),
                )
                conn.commit()
            finally:
                conn.close()
            _raises("SOURCE_COVERAGE_INCOMPLETE", lambda: fixture.manager.finalize(
                fixture.run_id, now=fixture.clock.value
            ))
        matrix.check("43 finalize rechecks current source gap ledger", live_gap_rechecked_at_finalize)

        def partial_snapshot_recovery() -> None:
            fixture = Fixture(base / "t44")
            fixture.start()
            fixture.complete()
            frozen_source = fixture.manager.run_dir(fixture.run_id) / "frozen_source.sqlite3"
            shutil.copy2(fixture.source, frozen_source)
            handoff = fixture.manager.finalize(fixture.run_id, now=fixture.clock.value)
            assert handoff["source_db_sha256"] == sha256_file(frozen_source)
            assert Path(handoff["paper_db_path"]).exists()
        matrix.check("44 partial snapshot finalization resumes safely", partial_snapshot_recovery)

        def same_path_replacement_rejected() -> None:
            fixture = Fixture(base / "t45-source")
            fixture.start()
            fixture.source.unlink()
            _source(fixture.source)
            _raises("SOURCE_IDENTITY_CHANGED", lambda: fixture.manager.verify_identity(fixture.run_id))

            paper_fixture = Fixture(base / "t45-paper")
            manifest = paper_fixture.start()
            paper = Path(manifest["paper_db_path"])
            paper.unlink()
            _paper(paper)
            _raises("PAPER_IDENTITY_CHANGED", lambda: paper_fixture.manager.verify_identity(paper_fixture.run_id))
        matrix.check("45 same-path source and paper replacement is rejected", same_path_replacement_rejected)

        def unproven_interval_detected() -> None:
            fixture = Fixture(base / "t46")
            conn = sqlite3.connect(fixture.source)
            try:
                conn.execute("DELETE FROM gap_jobs_v034")
                conn.commit()
            finally:
                conn.close()
            coverage, gaps = source_coverage_probe(
                fixture.source, start=START, target=START + timedelta(hours=72)
            )
            assert coverage == START + timedelta(hours=72)
            assert any(item.startswith("UNPROVEN_SOURCE_INTERVAL:") for item in gaps)
        matrix.check("46 elapsed time without gap proof is coverage-incomplete", unproven_interval_detected)

        def accepted_binding_worker_preflight() -> None:
            root = base / "t47"
            root.mkdir(parents=True, exist_ok=False)
            source = root / "source.sqlite3"
            runtime = root / "runtime"
            _source(source)
            clock = MutableClock()
            manager = FrozenOOSLifecycleV01(
                PROJECT_ROOT, runtime, clock=clock, verify_runtime_files=True
            )
            run_id = "P6-OOS-WORKER-PREFLIGHT"
            manifest = manager.start(StartRequest(
                source_db_path=source,
                runtime_root=runtime,
                repository_commit=git_head(PROJECT_ROOT),
                start_at=START,
                source_start_cursor=0,
                run_id=run_id,
            ))
            token = process_birth_token(os.getpid())
            assert token
            manager.begin_segment(run_id, pid=os.getpid(), birth_token=token)
            (manager.run_dir(run_id) / "stop.request").write_text(
                "GRACEFUL_STOP_REQUESTED\n", encoding="ascii"
            )
            assert run_worker(manager, run_id, poll_seconds=0.0, clock_seconds=0.0) == 0
            state = manager._state(run_id)
            assert state["lifecycle_state"] == "STOPPED"
            assert state["source_cursor"] == 2
            assert Path(manifest["paper_db_path"]).is_file()
        matrix.check("47 short worker preflight reuses accepted Phase-4 binding", accepted_binding_worker_preflight)

    assert git_status(PROJECT_ROOT) == git_before
    for path, digest in protected.items():
        assert sha256_file(PROJECT_ROOT / path) == digest, path
    evidence = {
        "model_fingerprint": MODEL_FINGERPRINT,
        "checks_passed": len(matrix.results),
        "checks_total": len(matrix.results),
        "uninterrupted_vs_resume": equivalence,
        "protected_p6_t001_sha256_unchanged": protected,
        "real_git_status_before_after_equal": True,
        "production_or_live_started": False,
        "evaluation_performed": False,
    }
    print(json.dumps(evidence, sort_keys=True, indent=2))
    print(f"CHECKS: {len(matrix.results)}/{len(matrix.results)}")
    print("RESULT: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
