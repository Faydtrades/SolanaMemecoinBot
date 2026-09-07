from __future__ import annotations

import argparse
import ast
import json
import os
import sqlite3
import stat
import subprocess
import sys
import tempfile
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterator


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from phase6 import frozen_oos_collection_lifecycle_v0_1 as lifecycle  # noqa: E402


UTC = timezone.utc
START = datetime(2026, 9, 7, 0, 0, 0, tzinfo=UTC)
END = START + timedelta(hours=72)
ARMED_AT = START - timedelta(hours=1)
FAKE_CORRECTED_COMMIT = "f" * 40
CLI_PATH = PROJECT_ROOT / "scripts/phase6_frozen_oos_collection_lifecycle_v0_1.py"
VENV_PYTHON = PROJECT_ROOT / ".venv/Scripts/python.exe"


@dataclass
class MutableClock:
    value: datetime

    def __call__(self) -> datetime:
        return self.value


class Matrix:
    def __init__(self) -> None:
        self.results: list[str] = []

    def check(self, name: str, callback: Callable[[], None]) -> None:
        callback()
        self.results.append(name)
        print(f"[PASS] {name}")


def _source_schema(path: Path) -> None:
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
                inserted_at_utc TEXT, pump_timestamp INTEGER,
                creator_wallet TEXT, virtual_sol_reserves INTEGER,
                virtual_token_reserves INTEGER, real_sol_reserves INTEGER,
                real_token_reserves INTEGER, decoded_at_utc TEXT,
                quote_mint TEXT, quote_amount_raw INTEGER,
                virtual_quote_reserves INTEGER, real_quote_reserves INTEGER
            );
            CREATE TABLE gap_jobs_v034(
                gap_key TEXT PRIMARY KEY, gap_started_at_utc TEXT NOT NULL,
                gap_ended_at_utc TEXT NOT NULL, status TEXT NOT NULL
            );
            """
        )
        conn.commit()
    finally:
        conn.close()


def _insert_event(
    path: Path,
    *,
    rowid: int,
    event_key: str,
    slot: int,
    event_type: str,
    mint: str,
    observed_at: datetime,
) -> None:
    stamp = lifecycle.dt_text(observed_at)
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            "INSERT INTO pump_events("
            "rowid,event_key,signature,slot,event_type,mint,user_wallet,"
            "sol_amount_lamports,token_amount_raw,is_buy,"
            "virtual_sol_reserve_raw,virtual_token_reserve_raw,"
            "real_sol_reserve_raw,real_token_reserve_raw,event_at_utc,"
            "received_at_utc,confirmation_status,source_program,"
            "source_decoded_file,inserted_at_utc) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                rowid, event_key, f"sig-{event_key}", slot, event_type, mint,
                "USER", 1_000_000, 10_000_000, 1, 30_000_000_000,
                1_000_000_000, 1, 1, stamp, stamp, "confirmed", "pump",
                "LIVE_WEBSOCKET_EVENT_V0_3_4", stamp,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _gap(path: Path, key: str, status: str, start: datetime, end: datetime) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            "INSERT INTO gap_jobs_v034 VALUES(?,?,?,?)",
            (key, lifecycle.dt_text(start), lifecycle.dt_text(end), status),
        )
        conn.commit()
    finally:
        conn.close()


class MigrationFixture:
    def __init__(self, root: Path, *, suffix: str) -> None:
        self.root = root
        root.mkdir(parents=True, exist_ok=False)
        self.source = root / "source.sqlite3"
        self.runtime = root / "runtime"
        self.clock = MutableClock(ARMED_AT)
        _source_schema(self.source)
        _insert_event(
            self.source,
            rowid=lifecycle.KNOWN_SALVAGE_SOURCE_ANCHOR,
            event_key="anchor",
            slot=1,
            event_type="LAUNCH",
            mint="ANCHOR",
            observed_at=START - timedelta(seconds=1),
        )
        _insert_event(
            self.source,
            rowid=lifecycle.KNOWN_SALVAGE_SOURCE_ANCHOR + 1,
            event_key="after-anchor",
            slot=2,
            event_type="TRADE",
            mint="OOS-MINT",
            observed_at=START - timedelta(minutes=1),
        )
        _gap(self.source, "window", "DONE", START, END)
        self.manager = lifecycle.FrozenOOSLifecycleV01(
            PROJECT_ROOT,
            self.runtime,
            clock=self.clock,
            verify_runtime_files=True,
        )
        self.run_id = lifecycle.KNOWN_SALVAGE_RUN_ID
        self.manifest = self.manager.start(
            lifecycle.StartRequest(
                source_db_path=self.source,
                runtime_root=self.runtime,
                repository_commit=lifecycle.git_head(PROJECT_ROOT),
                start_at=START,
                source_start_cursor=lifecycle.KNOWN_SALVAGE_SOURCE_ANCHOR,
                run_id=self.run_id,
            )
        )
        self.manifest.update({
            "model_id": lifecycle.PREDECESSOR_MODEL_ID,
            "schema_version": lifecycle.PREDECESSOR_SCHEMA_VERSION,
            "model_fingerprint": lifecycle.PREDECESSOR_MODEL_FINGERPRINT,
            "repository_commit": lifecycle.ACCEPTED_PARENT_COMMIT,
        })
        manifest_path = self.manager.run_dir(self.run_id) / "run_manifest.json"
        manifest_path.chmod(stat.S_IWRITE)
        lifecycle._write_envelope(manifest_path, self.manifest)
        state = self.manager._state(self.run_id)
        state.update({
            "lifecycle_state": "ACTIVE",
            "active_process": {
                "pid": 2_147_483_000,
                "birth_token": "dead-launcher",
                "segment_id": f"{self.run_id}-segment-0001",
            },
            "segments": [{
                "segment_id": f"{self.run_id}-segment-0001",
                "segment_index": 1,
                "started_at_utc": lifecycle.dt_text(ARMED_AT),
                "start_cursor": lifecycle.KNOWN_SALVAGE_SOURCE_ANCHOR,
                "ended_at_utc": None,
                "end_cursor": None,
                "termination": None,
                "pid": 2_147_483_000,
                "birth_token": "dead-launcher",
            }],
        })
        lifecycle._write_envelope(
            self.manager.run_dir(self.run_id) / "lifecycle_state.json", state
        )
        (self.manager.run_dir(self.run_id) / "segment_0001.log").write_text(
            json.dumps({
                "detail": "ACTIVE_WRITER_CONFLICT: worker is not the registered segment",
                "reason": "ACTIVE_WRITER_CONFLICT",
                "result": "FAIL_CLOSED",
            }, sort_keys=True),
            encoding="utf-8",
        )
        self.clock.value = START + timedelta(hours=1)

    def state(self) -> dict[str, object]:
        return self.manager._state(self.run_id)


@contextmanager
def _checkpointed_runtime() -> Iterator[None]:
    old_head = lifecycle.git_head
    old_status = lifecycle.git_status
    lifecycle.git_head = lambda _root: FAKE_CORRECTED_COMMIT
    lifecycle.git_status = lambda _root: ""
    try:
        yield
    finally:
        lifecycle.git_head = old_head
        lifecycle.git_status = old_status


def _raises(reason: str, callback: Callable[[], object]) -> None:
    try:
        callback()
    except lifecycle.LifecycleError as exc:
        assert exc.reason == reason, (exc.reason, reason)
    else:
        raise AssertionError(f"expected {reason}")


def _wait_until(callback: Callable[[], bool], timeout: float = 20.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if callback():
            return
        time.sleep(0.05)
    raise AssertionError("timed out waiting for worker state")


def _real_windows_preflight(root: Path) -> dict[str, object]:
    if os.name != "nt":
        raise AssertionError("real Windows preflight requires Windows")
    if not VENV_PYTHON.is_file():
        raise AssertionError(f"missing required interpreter: {VENV_PYTHON}")
    root.mkdir(parents=True, exist_ok=False)
    source = root / "windows-source.sqlite3"
    runtime = root / "windows-runtime"
    _source_schema(source)
    start = lifecycle.utc_now() + timedelta(seconds=1.5)
    end = start + timedelta(seconds=lifecycle.MINIMUM_DURATION_SECONDS)
    _gap(source, "windows-window", "DONE", start, end)
    manager = lifecycle.FrozenOOSLifecycleV01(
        PROJECT_ROOT, runtime, verify_runtime_files=True
    )
    run_id = "P6-T002B-WINDOWS-PREFLIGHT"
    manifest = manager.start(lifecycle.StartRequest(
        source_db_path=source,
        runtime_root=runtime,
        repository_commit=lifecycle.git_head(PROJECT_ROOT),
        start_at=start,
        source_start_cursor=0,
        run_id=run_id,
    ))
    owned_pids: list[int] = []
    launcher_pids: list[int] = []
    try:
        first = lifecycle.launch_worker_process(
            manager, run_id, cli_path=CLI_PATH, python_path=VENV_PYTHON
        )
        first_status = manager.status(run_id)
        owned_pids.append(int(first["pid"]))
        launcher_pids.append(int(first["launcher_pid"]))
        assert first["ownership_state"] == "OWNED"
        assert lifecycle.process_matches(first["pid"], first["birth_token"])
        assert first_status["process_health"] == "RUNNING_OWNED_PROCESS"
        assert int(first_status["source_cursor"]) == 0
        _insert_event(
            source, rowid=1, event_key="windows-first", slot=1,
            event_type="LAUNCH", mint="WINDOWS-A", observed_at=start,
        )
        _wait_until(lambda: int(manager._state(run_id)["source_cursor"]) >= 1)
        assert Path(manifest["paper_db_path"]).stat().st_size > 0
        assert lifecycle._sqlite_quick_check(
            Path(manifest["paper_db_path"]), readonly=True
        ) == "ok"
        lifecycle.request_stop(manager, run_id)
        _wait_until(lambda: manager._state(run_id).get("active_process") is None)
        stopped = manager._state(run_id)
        assert stopped["lifecycle_state"] == "STOPPED"

        _insert_event(
            source, rowid=2, event_key="windows-second", slot=2,
            event_type="TRADE", mint="WINDOWS-A",
            observed_at=start + timedelta(seconds=1),
        )
        second = lifecycle.launch_worker_process(
            manager, run_id, cli_path=CLI_PATH, python_path=VENV_PYTHON
        )
        owned_pids.append(int(second["pid"]))
        launcher_pids.append(int(second["launcher_pid"]))
        _wait_until(lambda: int(manager._state(run_id)["source_cursor"]) >= 2)
        lifecycle.request_stop(manager, run_id)
        _wait_until(lambda: manager._state(run_id).get("active_process") is None)
        final = manager._state(run_id)
        assert len(final["segments"]) == 2
        assert all(item["ownership_state"] == "OWNED" for item in final["segments"])
        assert final["source_cursor"] == 2
        assert final["source_row_count"] == 2
        conn = sqlite3.connect(
            Path(manifest["paper_db_path"]).resolve().as_uri() + "?mode=ro", uri=True
        )
        conn.execute("PRAGMA query_only=ON")
        try:
            rowids = tuple(int(row[0]) for row in conn.execute(
                "SELECT production_p1_rowid FROM paper_fp_binding_inputs_v0_1 "
                "WHERE production_p1_rowid IS NOT NULL ORDER BY production_p1_rowid"
            ))
        finally:
            conn.close()
        assert rowids == (1, 2)
        assert len(set(owned_pids)) == 2
        return {
            "interpreter": str(VENV_PYTHON),
            "python_version": sys.version.split()[0],
            "run_id": run_id,
            "launcher_pids": launcher_pids,
            "actual_worker_pids": owned_pids,
            "launcher_worker_pid_differed": all(
                launcher != worker
                for launcher, worker in zip(launcher_pids, owned_pids)
            ),
            "segments": len(final["segments"]),
            "final_source_cursor": final["source_cursor"],
            "paper_db_quick_check": "ok",
            "paper_db_nonzero": Path(manifest["paper_db_path"]).stat().st_size > 0,
            "processed_rowids": list(rowids),
            "graceful_stop_resume": True,
        }
    finally:
        try:
            if manager._state(run_id).get("active_process") is not None:
                lifecycle.request_stop(manager, run_id)
                _wait_until(lambda: manager._state(run_id).get("active_process") is None)
        except (OSError, lifecycle.LifecycleError, AssertionError):
            pass


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--windows-preflight", action="store_true")
    args = parser.parse_args(argv)
    matrix = Matrix()
    git_before = lifecycle.git_status(PROJECT_ROOT)
    real_run_dir = Path(r"D:\Tradingbot\runtime_oos") / lifecycle.KNOWN_SALVAGE_RUN_ID
    real_before = {
        path.name: lifecycle.sha256_file(path)
        for path in real_run_dir.iterdir()
        if path.is_file()
    } if real_run_dir.is_dir() else {}

    with tempfile.TemporaryDirectory(prefix="p6_t002b_selftest_") as raw:
        root = Path(raw)
        fixture = MigrationFixture(root / "migration", suffix="main")

        def pending_has_no_owner() -> None:
            token = "atomic-claim-token"
            segment = fixture.manager.prepare_segment_launch(fixture.run_id, claim_token=token)
            state = fixture.manager._state(fixture.run_id)
            # The synthetic predecessor begins ACTIVE; close it as stale first.
            del segment, state
        # Migration fixtures deliberately model the predecessor ACTIVE failure,
        # so ownership primitives use a normal v0.2 fixture below.
        normal_root = root / "normal"
        normal_root.mkdir()
        normal_source = normal_root / "source.sqlite3"
        _source_schema(normal_source)
        _insert_event(
            normal_source, rowid=1, event_key="normal-anchor", slot=1,
            event_type="LAUNCH", mint="NORMAL", observed_at=START,
        )
        _gap(normal_source, "normal-window", "DONE", START, END)
        normal_clock = MutableClock(START)
        normal = lifecycle.FrozenOOSLifecycleV01(
            PROJECT_ROOT, normal_root / "runtime", clock=normal_clock,
            verify_runtime_files=True,
        )
        normal_run = "P6-T002B-NORMAL"
        normal.start(lifecycle.StartRequest(
            source_db_path=normal_source,
            runtime_root=normal_root / "runtime",
            repository_commit=lifecycle.git_head(PROJECT_ROOT),
            start_at=START,
            source_start_cursor=1,
            run_id=normal_run,
        ))
        claim_token = "single-use-claim"
        pending = normal.prepare_segment_launch(normal_run, claim_token=claim_token)

        matrix.check(
            "01 parent creates an unowned pending segment",
            lambda: (
                None if pending["ownership_state"] == "CLAIM_PENDING"
                and pending["pid"] is None and pending["birth_token"] is None
                else (_ for _ in ()).throw(AssertionError("parent granted ownership"))
            ),
        )

        actual_token = lifecycle.process_birth_token(os.getpid())
        assert actual_token
        matrix.check(
            "02 forged process identity cannot claim pending segment",
            lambda: _raises(
                "ACTIVE_WRITER_CONFLICT",
                lambda: normal.claim_segment(
                    normal_run, segment_id=pending["segment_id"],
                    claim_token=claim_token, pid=os.getpid(),
                    birth_token="forged-birth-token",
                ),
            ),
        )
        claimed = normal.claim_segment(
            normal_run, segment_id=pending["segment_id"], claim_token=claim_token,
            pid=os.getpid(), birth_token=actual_token,
        )
        matrix.check(
            "03 actual worker PID and birth token become owner",
            lambda: (
                None if claimed["pid"] == os.getpid()
                and claimed["birth_token"] == actual_token
                and claimed["ownership_state"] == "OWNED"
                else (_ for _ in ()).throw(AssertionError("worker ownership mismatch"))
            ),
        )
        matrix.check(
            "04 atomic claim removes claim secret from active state",
            lambda: (
                None if "claim_token_sha256" not in normal._state(normal_run)["active_process"]
                else (_ for _ in ()).throw(AssertionError("claim secret retained"))
            ),
        )
        matrix.check(
            "05 competing second claim fails closed",
            lambda: _raises(
                "ACTIVE_WRITER_CONFLICT",
                lambda: normal.claim_segment(
                    normal_run, segment_id=pending["segment_id"],
                    claim_token=claim_token, pid=os.getpid(), birth_token=actual_token,
                ),
            ),
        )

        def launcher_is_not_owner() -> None:
            normal.record_launcher_pid(
                normal_run, segment_id=pending["segment_id"], launcher_pid=2_147_482_999
            )
            status = normal.status(normal_run, now=START)
            assert status["process_health"] == "RUNNING_OWNED_PROCESS"
            state = normal._state(normal_run)
            assert state["active_process"]["pid"] == os.getpid()
            assert state["segments"][-1]["launcher_pid"] == 2_147_482_999
        matrix.check("06 dead launcher identity cannot replace worker ownership", launcher_is_not_owner)

        def competitor_cannot_end_owner() -> None:
            before = normal._state(normal_run)
            normal.fail_segment_startup(
                normal_run, segment_id=pending["segment_id"],
                pid=os.getpid() + 1, birth_token="competitor", reason="COMPETITOR",
            )
            assert normal._state(normal_run) == before
        matrix.check("07 competing worker cannot close legitimate owner", competitor_cannot_end_owner)

        def owned_stop() -> None:
            normal.end_segment(normal_run, termination="TEST_STOP")
            status = normal.status(normal_run, now=START)
            assert status["process_health"] == "NOT_RUNNING"
            assert status["lifecycle_state"] == "STOPPED"
        matrix.check("08 stopped worker is reflected accurately in status", owned_stop)

        def failed_startup_not_running() -> None:
            token = "failed-start"
            segment = normal.prepare_segment_launch(normal_run, claim_token=token)
            normal.fail_segment_startup(
                normal_run, segment_id=segment["segment_id"],
                claim_token=token, reason="SYNTHETIC_STARTUP_FAILURE",
            )
            status = normal.status(normal_run, now=START)
            assert status["process_health"] == "NOT_RUNNING"
            assert status["lifecycle_state"] == "STOPPED"
            assert status["last_error"] == "SYNTHETIC_STARTUP_FAILURE"
        matrix.check("09 fail-closed startup cannot remain falsely running", failed_startup_not_running)

        def claim_timeout_reconciles() -> None:
            token = "timeout"
            segment = normal.prepare_segment_launch(normal_run, claim_token=token)
            normal_clock.value += timedelta(seconds=lifecycle.WORKER_CLAIM_TIMEOUT_SECONDS + 1)
            state = normal.reconcile_process(normal_run)
            assert state["active_process"] is None
            assert state["segments"][-1]["termination"] == "WORKER_CLAIM_TIMEOUT"
        matrix.check("10 abandoned pending claim times out safely", claim_timeout_reconciles)

        def pid_reuse() -> None:
            normal_clock.value = START + timedelta(seconds=20)
            normal.begin_segment(normal_run, pid=os.getpid(), birth_token="wrong-token")
            assert normal.reconcile_process(normal_run)["active_process"] is None
        matrix.check("11 PID reuse protection remains enforced", pid_reuse)

        def exited_process_not_alive() -> None:
            process = subprocess.Popen(
                (sys.executable, "-c", "pass"),
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
            process.wait(timeout=10)
            assert lifecycle.process_birth_token(process.pid) is None
        matrix.check("12 terminated Windows process is not reported alive", exited_process_not_alive)

        def claimed_startup_failure_is_terminal() -> None:
            token = "claimed-failure"
            segment = normal.prepare_segment_launch(normal_run, claim_token=token)
            normal.claim_segment(
                normal_run, segment_id=segment["segment_id"], claim_token=token,
                pid=os.getpid(), birth_token=actual_token,
            )
            state = normal.fail_segment_startup(
                normal_run, segment_id=segment["segment_id"],
                pid=os.getpid(), birth_token=actual_token,
                reason="SYNTHETIC_POST_CLAIM_STARTUP_FAILURE",
            )
            assert state["lifecycle_state"] == "INVALID"
            assert state["active_process"] is None
            assert normal.status(normal_run, now=normal_clock.value)["process_health"] == "NOT_RUNNING"
        matrix.check("13 post-claim startup failure is durably terminal", claimed_startup_failure_is_terminal)

        eligibility = fixture.manager.salvage_check(fixture.run_id, now=fixture.clock.value)
        matrix.check(
            "14 exact predecessor failure is salvage eligible",
            lambda: (
                None if eligibility["result"] == "SALVAGE_AUTHORIZED"
                else (_ for _ in ()).throw(AssertionError(eligibility))
            ),
        )
        matrix.check(
            "15 frozen run ID start end and anchor are exact",
            lambda: (
                None if eligibility["scientific_identity"] == {
                    "start_at_utc": lifecycle.KNOWN_SALVAGE_START,
                    "end_at_utc": lifecycle.KNOWN_SALVAGE_END,
                    "source_start_cursor": lifecycle.KNOWN_SALVAGE_SOURCE_ANCHOR,
                    "protocol_fingerprint": lifecycle.P6_PROTOCOL_FINGERPRINT,
                    "policy_set_sha256": lifecycle.P6_POLICY_SET_SHA256,
                } else (_ for _ in ()).throw(AssertionError("identity drift"))
            ),
        )
        matrix.check(
            "16 salvage-check exposes no outcome metric",
            lambda: (
                None if not ({"pnl", "profit_factor", "expectancy", "win_rate", "drawdown", "go", "no_go"}
                             & {key.lower() for key in eligibility})
                else (_ for _ in ()).throw(AssertionError("peek field exposed"))
            ),
        )

        def salvage_read_only() -> None:
            before = {
                path.name: lifecycle.sha256_file(path)
                for path in fixture.manager.run_dir(fixture.run_id).iterdir()
                if path.is_file()
            }
            fixture.manager.salvage_check(fixture.run_id, now=fixture.clock.value)
            after = {
                path.name: lifecycle.sha256_file(path)
                for path in fixture.manager.run_dir(fixture.run_id).iterdir()
                if path.is_file()
            }
            assert before == after
        matrix.check("17 salvage-check is byte-for-byte read-only", salvage_read_only)

        def wrong_failure() -> None:
            item = MigrationFixture(root / "wrong-failure", suffix="wrong")
            (item.manager.run_dir(item.run_id) / "segment_0001.log").write_text(
                json.dumps({"reason": "OTHER", "result": "FAIL_CLOSED"}),
                encoding="utf-8",
            )
            assert item.manager.salvage_check(item.run_id)["result"] == "SALVAGE_NOT_AUTHORIZED"
        matrix.check("18 different failure signature cannot migrate", wrong_failure)

        def progress_rejected() -> None:
            item = MigrationFixture(root / "progress", suffix="progress")
            state = item.state()
            state["source_cursor"] += 1
            state["source_watermark"] += 1
            state["source_row_count"] = 1
            lifecycle._write_envelope(item.manager.run_dir(item.run_id) / "lifecycle_state.json", state)
            assert item.manager.salvage_check(item.run_id)["result"] == "SALVAGE_NOT_AUTHORIZED"
        matrix.check("19 prior scientific progress fails salvage closed", progress_rejected)

        def evaluation_rejected() -> None:
            item = MigrationFixture(root / "evaluation", suffix="evaluation")
            (item.manager.run_dir(item.run_id) / "gate_evaluation.json").write_text(
                "{}\n", encoding="utf-8"
            )
            assert item.manager.salvage_check(item.run_id)["result"] == "SALVAGE_NOT_AUTHORIZED"
        matrix.check("20 evaluation artifact makes salvage ineligible", evaluation_rejected)

        def paper_state_rejected() -> None:
            item = MigrationFixture(root / "paper", suffix="paper")
            Path(item.manifest["paper_db_path"]).write_bytes(b"unexpected")
            assert item.manager.salvage_check(item.run_id)["result"] == "SALVAGE_NOT_AUTHORIZED"
        matrix.check("21 unexpected paper state makes salvage ineligible", paper_state_rejected)

        def source_identity_rejected() -> None:
            item = MigrationFixture(root / "source-id", suffix="source-id")
            item.source.unlink()
            _source_schema(item.source)
            assert item.manager.salvage_check(item.run_id)["result"] == "SALVAGE_NOT_AUTHORIZED"
        matrix.check("22 source object identity mismatch fails closed", source_identity_rejected)

        def unrecoverable_gap_rejected() -> None:
            item = MigrationFixture(root / "gap-failed", suffix="gap-failed")
            _gap(item.source, "failed-gap", "FAILED", START, START + timedelta(minutes=1))
            check = item.manager.salvage_check(item.run_id)
            assert check["result"] == "SALVAGE_NOT_AUTHORIZED"
            assert check["source_coverage"]["assessment"] == "UNRECOVERABLE_SOURCE_GAP"
            with _checkpointed_runtime():
                _raises(
                    "SALVAGE_NOT_AUTHORIZED",
                    lambda: item.manager.migrate_salvage(item.run_id),
                )
            state = item.state()
            assert state["lifecycle_state"] == "INVALID"
            assert state["last_error"] == "UNRECOVERABLE_SOURCE_GAP"
        matrix.check("23 unrecoverable source gap fails salvage closed", unrecoverable_gap_rejected)

        def recoverable_gap_allowed() -> None:
            item = MigrationFixture(root / "gap-pending", suffix="gap-pending")
            _gap(item.source, "pending-gap", "PENDING", START, START + timedelta(minutes=1))
            check = item.manager.salvage_check(item.run_id)
            assert check["result"] == "SALVAGE_AUTHORIZED"
            assert check["source_coverage"]["assessment"] == "RECOVERABLE_GAPS_PENDING"
        matrix.check("24 accepted recoverable gap path remains eligible", recoverable_gap_allowed)

        migration_record: dict[str, object] = {}

        def migrate_exact_predecessor() -> None:
            nonlocal migration_record
            with _checkpointed_runtime():
                migration_record = fixture.manager.migrate_salvage(fixture.run_id)
                fixture.manager.verify_identity(
                    fixture.run_id, repository_commit=FAKE_CORRECTED_COMMIT
                )
            assert migration_record["migration_reason"] == lifecycle.MIGRATION_REASON
            assert migration_record["original_repository_commit"] == lifecycle.ACCEPTED_PARENT_COMMIT
            assert migration_record["corrected_runtime_commit"] == FAKE_CORRECTED_COMMIT
        matrix.check("25 exact predecessor migrates to explicit corrected binding", migrate_exact_predecessor)

        matrix.check(
            "26 protocol policy and Phase-4 hashes remain unchanged",
            lambda: (
                None if migration_record["protocol_fingerprint_before_after"]
                == [lifecycle.P6_PROTOCOL_FINGERPRINT] * 2
                and migration_record["policy_set_sha256_before_after"]
                == [lifecycle.P6_POLICY_SET_SHA256] * 2
                and migration_record["phase4_runtime_sha256_before_after"][0]
                == migration_record["phase4_runtime_sha256_before_after"][1]
                else (_ for _ in ()).throw(AssertionError("protected binding drift"))
            ),
        )
        matrix.check(
            "27 migration preserves exact scientific identity",
            lambda: (
                None if migration_record["scientific_identity_before_after"][0]
                == migration_record["scientific_identity_before_after"][1]
                == {
                    "start_at_utc": lifecycle.KNOWN_SALVAGE_START,
                    "end_at_utc": lifecycle.KNOWN_SALVAGE_END,
                    "source_start_cursor": lifecycle.KNOWN_SALVAGE_SOURCE_ANCHOR,
                } else (_ for _ in ()).throw(AssertionError("scientific identity changed"))
            ),
        )

        def migration_idempotent() -> None:
            with _checkpointed_runtime():
                again = fixture.manager.migrate_salvage(fixture.run_id)
            assert again == migration_record
        matrix.check("28 repeated migration is exactly idempotent", migration_idempotent)

        def migration_crash_recovery() -> None:
            item = MigrationFixture(root / "migration-recovery", suffix="recovery")
            pre_state = item.state()
            with _checkpointed_runtime():
                record = item.manager.migrate_salvage(item.run_id)
                lifecycle._write_envelope(
                    item.manager.run_dir(item.run_id) / "lifecycle_state.json", pre_state
                )
                recovered = item.manager.migrate_salvage(item.run_id)
            assert recovered == record
            assert lifecycle.canonical_sha256(item.state()) == record["post_migration_state_payload_sha256"]
        matrix.check("29 migration record-first replay recovers atomically", migration_crash_recovery)

        def uncheckpointed_rejected() -> None:
            item = MigrationFixture(root / "uncheckpointed", suffix="uncheckpointed")
            _raises("CORRECTED_RUNTIME_NOT_CHECKPOINTED", lambda: item.manager.migrate_salvage(item.run_id))
        matrix.check("30 predecessor commit cannot self-migrate", uncheckpointed_rejected)

        def capability_absence() -> None:
            forbidden = {
                "sign", "sign_transaction", "send_transaction", "send_raw_transaction",
                "broadcast", "broadcast_transaction", "keypair", "private_key", "wallet",
            }
            names: set[str] = set()
            for path in (
                PROJECT_ROOT / "src/phase6/frozen_oos_collection_lifecycle_v0_1.py",
                PROJECT_ROOT / "scripts/phase6_frozen_oos_collection_lifecycle_v0_1.py",
            ):
                tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
                names.update(
                    node.name.lower() for node in ast.walk(tree)
                    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
                )
            assert not names.intersection(forbidden)
        matrix.check("31 no signer send broadcast or custody capability", capability_absence)

        catchup_evidence: dict[str, object] = {}

        def migrated_catchup_and_resume() -> None:
            _insert_event(
                fixture.source,
                rowid=lifecycle.KNOWN_SALVAGE_SOURCE_ANCHOR + 2,
                event_key="boundary-after-migration",
                slot=3,
                event_type="LAUNCH",
                mint="BOUNDARY-MINT",
                observed_at=START,
            )
            _insert_event(
                fixture.source,
                rowid=lifecycle.KNOWN_SALVAGE_SOURCE_ANCHOR + 3,
                event_key="exact-end-after-migration",
                slot=4,
                event_type="LAUNCH",
                mint="POST-END-MINT",
                observed_at=END,
            )
            token = lifecycle.process_birth_token(os.getpid())
            assert token
            with _checkpointed_runtime():
                fixture.manager.begin_segment(
                    fixture.run_id, pid=os.getpid(), birth_token=token
                )
                (fixture.manager.run_dir(fixture.run_id) / "stop.request").write_text(
                    "GRACEFUL_STOP_REQUESTED\n", encoding="ascii"
                )
                assert lifecycle.run_worker(
                    fixture.manager, fixture.run_id,
                    poll_seconds=0.0, clock_seconds=0.0,
                ) == 0
            state = fixture.state()
            paper = Path(fixture.manifest["paper_db_path"])
            conn = sqlite3.connect(paper.resolve().as_uri() + "?mode=ro", uri=True)
            conn.execute("PRAGMA query_only=ON")
            try:
                rowids = tuple(int(row[0]) for row in conn.execute(
                    "SELECT production_p1_rowid FROM paper_fp_binding_inputs_v0_1 "
                    "WHERE production_p1_rowid IS NOT NULL ORDER BY production_p1_rowid"
                ))
            finally:
                conn.close()
            catchup_evidence.update({
                "rowids": rowids,
                "source_cursor": state["source_cursor"],
                "source_row_count": state["source_row_count"],
                "candidate_count": state["candidate_count"],
                "segment_count": len(state["segments"]),
                "run_id": state["run_id"],
            })
            assert rowids == (
                lifecycle.KNOWN_SALVAGE_SOURCE_ANCHOR + 1,
                lifecycle.KNOWN_SALVAGE_SOURCE_ANCHOR + 2,
                lifecycle.KNOWN_SALVAGE_SOURCE_ANCHOR + 3,
            )
            assert state["source_cursor"] == lifecycle.KNOWN_SALVAGE_SOURCE_ANCHOR + 3
        matrix.check("32 migrated run catches up from the original source anchor", migrated_catchup_and_resume)

        def no_duplicates_or_contamination() -> None:
            assert len(catchup_evidence["rowids"]) == len(set(catchup_evidence["rowids"]))
            assert catchup_evidence["source_row_count"] == 3
            assert catchup_evidence["candidate_count"] == 0
        matrix.check(
            "33 catch-up is duplicate-free and pre-window/exact-end safe",
            no_duplicates_or_contamination,
        )

        def resumed_identity_unchanged() -> None:
            manifest = fixture.manager._manifest(fixture.run_id)
            assert catchup_evidence["run_id"] == lifecycle.KNOWN_SALVAGE_RUN_ID
            assert catchup_evidence["segment_count"] == 2
            assert manifest["start_at_utc"] == lifecycle.KNOWN_SALVAGE_START
            assert fixture.state()["target_end_at_utc"] == lifecycle.KNOWN_SALVAGE_END
            assert manifest["source_start_cursor"] == lifecycle.KNOWN_SALVAGE_SOURCE_ANCHOR
        matrix.check("34 migrated resume preserves run and frozen boundaries", resumed_identity_unchanged)

        def different_predecessor_rejected() -> None:
            item = MigrationFixture(root / "wrong-predecessor", suffix="wrong-predecessor")
            manifest = item.manager._manifest(item.run_id)
            manifest["repository_commit"] = "e" * 40
            lifecycle._write_envelope(
                item.manager.run_dir(item.run_id) / "run_manifest.json", manifest
            )
            assert item.manager.salvage_check(item.run_id)["result"] == "SALVAGE_NOT_AUTHORIZED"
        matrix.check("35 only the exact accepted predecessor can migrate", different_predecessor_rejected)

        def recursively_no_peek() -> None:
            forbidden = {
                "pnl", "profit_factor", "expectancy", "win_rate", "drawdown",
                "strategy_ranking", "go", "no_go", "conditional",
            }
            discovered: set[str] = set()

            def visit(value: object) -> None:
                if isinstance(value, dict):
                    for key, child in value.items():
                        discovered.add(str(key).lower())
                        visit(child)
                elif isinstance(value, list):
                    for child in value:
                        visit(child)

            visit(eligibility)
            assert not discovered.intersection(forbidden)
        matrix.check("36 salvage evidence is recursively no-peek", recursively_no_peek)

        def exact_claim_deadline_and_stale_parent() -> None:
            boundary_root = root / "claim-boundary"
            boundary_root.mkdir()
            source = boundary_root / "source.sqlite3"
            _source_schema(source)
            _insert_event(
                source, rowid=1, event_key="boundary-anchor", slot=1,
                event_type="LAUNCH", mint="BOUNDARY", observed_at=START,
            )
            _gap(source, "boundary-window", "DONE", START, END)
            clock = MutableClock(START)
            manager = lifecycle.FrozenOOSLifecycleV01(
                PROJECT_ROOT, boundary_root / "runtime", clock=clock,
                verify_runtime_files=True,
            )
            run_id = "P6-T002B-CLAIM-BOUNDARY"
            manager.start(lifecycle.StartRequest(
                source_db_path=source,
                runtime_root=boundary_root / "runtime",
                repository_commit=lifecycle.git_head(PROJECT_ROOT),
                start_at=START,
                source_start_cursor=1,
                run_id=run_id,
            ))
            claim_token = "exact-deadline-token"
            pending = manager.prepare_segment_launch(run_id, claim_token=claim_token)
            active = manager._state(run_id)["active_process"]
            clock.value = lifecycle.dt_parse(active["claim_deadline_at_utc"])
            worker_token = lifecycle.process_birth_token(os.getpid())
            assert worker_token
            manager.claim_segment(
                run_id, segment_id=pending["segment_id"],
                claim_token=claim_token, pid=os.getpid(),
                birth_token=worker_token,
            )
            before = manager._state(run_id)
            manager.fail_segment_startup(
                run_id, segment_id=pending["segment_id"],
                claim_token=claim_token, reason="STALE_PARENT_TIMEOUT",
            )
            assert manager._state(run_id) == before
            manager.end_segment(run_id, termination="TEST_STOP")

            late_token = "after-deadline-token"
            late = manager.prepare_segment_launch(run_id, claim_token=late_token)
            active = manager._state(run_id)["active_process"]
            clock.value = lifecycle.dt_parse(active["claim_deadline_at_utc"]) + timedelta(
                microseconds=1
            )
            _raises(
                "ACTIVE_WRITER_CONFLICT",
                lambda: manager.claim_segment(
                    run_id, segment_id=late["segment_id"],
                    claim_token=late_token, pid=os.getpid(),
                    birth_token=worker_token,
                ),
            )
            reconciled = manager.reconcile_process(run_id)
            assert reconciled["active_process"] is None
            assert reconciled["segments"][-1]["termination"] == "WORKER_CLAIM_TIMEOUT"
        matrix.check(
            "37 exact claim deadline is ordered and stale parent cannot close owner",
            exact_claim_deadline_and_stale_parent,
        )

        def unrelated_pending_gap_cannot_mask_unproven_interval() -> None:
            item = MigrationFixture(root / "unproven-interval", suffix="unproven")
            conn = sqlite3.connect(item.source)
            try:
                conn.execute("DELETE FROM gap_jobs_v034")
                conn.commit()
            finally:
                conn.close()
            _gap(
                item.source, "unrelated-pending", "PENDING",
                START + timedelta(minutes=30), START + timedelta(minutes=31),
            )
            check = item.manager.salvage_check(item.run_id)
            assert check["result"] == "SALVAGE_NOT_AUTHORIZED"
            assert check["source_coverage"]["assessment"] == "UNRECOVERABLE_SOURCE_GAP"
        matrix.check(
            "38 unrelated pending job cannot mask unproven source interval",
            unrelated_pending_gap_cannot_mask_unproven_interval,
        )

        windows_evidence: dict[str, object] | None = None
        if args.windows_preflight:
            windows_evidence = _real_windows_preflight(root / "real-windows")
            matrix.check(
                "39 real Windows venv launcher/worker ownership preflight",
                lambda: (
                    None if windows_evidence
                    and windows_evidence["launcher_worker_pid_differed"]
                    and windows_evidence["graceful_stop_resume"]
                    else (_ for _ in ()).throw(AssertionError(windows_evidence))
                ),
            )

    assert lifecycle.git_status(PROJECT_ROOT) == git_before
    real_after = {
        path.name: lifecycle.sha256_file(path)
        for path in real_run_dir.iterdir()
        if path.is_file()
    } if real_run_dir.is_dir() else {}
    assert real_before == real_after
    evidence = {
        "task_id": "MEME-P6-T002B",
        "model_id": lifecycle.MODEL_ID,
        "schema_version": lifecycle.SCHEMA_VERSION,
        "model_fingerprint": lifecycle.MODEL_FINGERPRINT,
        "checks_passed": len(matrix.results),
        "checks_total": len(matrix.results),
        "real_windows_preflight": windows_evidence,
        "real_oos_run_unchanged": real_before == real_after,
        "production_or_live_oos_started": False,
        "evaluation_performed": False,
    }
    print(json.dumps(evidence, sort_keys=True, indent=2))
    print(f"CHECKS: {len(matrix.results)}/{len(matrix.results)}")
    print("RESULT: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
