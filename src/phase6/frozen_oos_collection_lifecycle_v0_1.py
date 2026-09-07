from __future__ import annotations

import hashlib
import json
import os
import secrets
import shutil
import signal
import sqlite3
import stat
import subprocess
import sys
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping, Sequence


UTC = timezone.utc
MODEL_ID = "P6-FROZEN-OOS-COLLECTION-LIFECYCLE-0002"
SCHEMA_VERSION = "phase6_frozen_oos_collection_lifecycle_v0.2"
PROTOCOL_ID = "P6-OOS-PROTOCOL-0001"
P6_PROTOCOL_FINGERPRINT = (
    "0cd06a3269c997aaa01a0e1a796e9a15088e9af7b3a4137930a5cc7beb9cdb87"
)
P6_POLICY_SET_SHA256 = (
    "c4a2a277af1d1c7e1e8316e258b2a6da2abd302cd31d2498e4e7a45d08b2582c"
)
ACCEPTED_PARENT_COMMIT = "3c4a7d852a96d2e27262a43721f576ae46cee591"
PREDECESSOR_MODEL_ID = "P6-FROZEN-OOS-COLLECTION-LIFECYCLE-0001"
PREDECESSOR_SCHEMA_VERSION = "phase6_frozen_oos_collection_lifecycle_v0.1"
PREDECESSOR_MODEL_FINGERPRINT = (
    "1d74b5daadefc791e81a5d2a2e93d5f80da4ef9ca85142f573e29ec765ce7a36"
)
MIGRATION_REASON = "WINDOWS_WORKER_OWNERSHIP_CORRECTION"
MIGRATION_SCHEMA_VERSION = "phase6_lifecycle_migration_v0.1"
WORKER_CLAIM_TIMEOUT_SECONDS = 10.0
KNOWN_SALVAGE_RUN_ID = "P6-OOS-20260907T000000Z-e4114796cf4b"
KNOWN_SALVAGE_START = "2026-09-07T00:00:00.000000+00:00"
KNOWN_SALVAGE_END = "2026-09-10T00:00:00.000000+00:00"
KNOWN_SALVAGE_SOURCE_ANCHOR = 1_255_696

MINIMUM_DURATION_SECONDS = 72 * 60 * 60
EXTENSION_SECONDS = 24 * 60 * 60
MAXIMUM_DURATION_SECONDS = 7 * 24 * 60 * 60
MAXIMUM_ARMING_LEAD_SECONDS = 2 * 60 * 60
H1_MINIMUM_ELIGIBLE_ENTRIES = 150
SOURCE_STALL_FAIL_SECONDS = 30.0
MINIMUM_PRESTART_POLL_SECONDS = 0.05
MAXIMUM_PRESTART_POLL_SECONDS = 1.0
DEFAULT_RUNTIME_ROOT = Path(r"D:\Tradingbot\runtime_oos")
DEFAULT_SOURCE_DB = Path(r"D:\Tradingbot\solana_memecoin_bot_phase1_v0_1\data\db\tradingbot.sqlite3")

SOURCE_MODEL_ID = "P4-CONTINUOUS-MARKET-SOURCE-0002"
SOURCE_MODEL_FINGERPRINT = (
    "9cb094f52bf4b4fe28dc4828b1d9a52a3350cde664c7da5a489fa84e9a4085a9"
)
BINDING_MODEL_ID = "P4-CONTINUOUS-FIRSTPULLBACK-BINDING-0005"
BINDING_MODEL_FINGERPRINT = (
    "f66acb5926f3dd77d68c086d74e1bc07b43fad4307071c1ec8c3b7d1111660b7"
)
RUNNER_MODEL_ID = "P4-CONTINUOUS-PAPER-RUNNER-0002"
RUNNER_MODEL_FINGERPRINT = (
    "66725275f7e01e510b369df08281be9769d057113896f60f2ac652c2efe072fb"
)
MULTIHOUR_MODEL_ID = "P4-CONTINUOUS-FIRSTPULLBACK-MULTIHOUR-RUN-0006"
MULTIHOUR_MODEL_FINGERPRINT = (
    "434045e916e4be7e463a81ea17db487cc2b777af8d5f8874cf4b9b6b84013c9c"
)

RUNTIME_FILE_SHA256 = {
    "scripts/live_pump_collector_v0_3_4.py": (
        "cf44a5af308f4e95d116585717aa4b9da80e1918a4ba20a4ed7f469eaee35aa1"
    ),
    "scripts/phase4_continuous_firstpullback_multihour_run_v0_6.py": (
        "8bf11f86f333220dd8e8617021123d99566186f9ba90b9afaaa7b3fd731caf61"
    ),
    "src/phase4/paper_continuous_firstpullback_binding_v0_5.py": (
        "806f11af051edd8b2d8d0f07d5bae0129856f16fd01bbe435ca4612e813586ee"
    ),
    "src/phase4/paper_continuous_market_source_v0_2.py": (
        "4a04043d86523018b9933b6a174e120999bc41d25edd35ed10a97118e793ed85"
    ),
    "src/phase4/paper_continuous_runner_v0_2.py": (
        "27a42d3ca7e4cdaf878d1a707b43c2ddeb42bafa4c28d06b9cb5aa3f2e5116c3"
    ),
}

NO_PEEK_STATUS_FIELDS = (
    "run_id",
    "start_at_utc",
    "end_at_utc",
    "elapsed_seconds",
    "remaining_seconds",
    "lifecycle_state",
    "segment_count",
    "process_health",
    "source_cursor",
    "source_watermark",
    "source_row_count",
    "candidate_count",
    "h1_eligible_sample_count",
    "coverage_status",
    "paper_db_path",
    "source_db_path",
    "database_integrity",
    "last_durable_progress_at_utc",
    "last_error",
    "restart_count",
)

SPEC: dict[str, Any] = {
    "model_id": MODEL_ID,
    "schema_version": SCHEMA_VERSION,
    "protocol_id": PROTOCOL_ID,
    "protocol_fingerprint": P6_PROTOCOL_FINGERPRINT,
    "policy_set_sha256": P6_POLICY_SET_SHA256,
    "minimum_duration_seconds": MINIMUM_DURATION_SECONDS,
    "extension_seconds": EXTENSION_SECONDS,
    "maximum_duration_seconds": MAXIMUM_DURATION_SECONDS,
    "exact_boundary_arming": {
        "explicit_start_required": True,
        "past_start_allowed": False,
        "maximum_lead_seconds": MAXIMUM_ARMING_LEAD_SECONDS,
        "source_anchor": "PHYSICAL_ROWID_CAPTURED_AT_ARMING",
        "prestart_worker": "DURABLE_ARMED_WAIT_WITH_ZERO_SCIENTIFIC_ELAPSED_TIME",
        "prewindow_rows": "CONSUMED_FOR_CAUSAL_CONTEXT_BUT_EXCLUDED_FROM_P6_T001_WINDOW",
    },
    "worker_ownership": {
        "registration": "ATOMIC_ACTUAL_WORKER_SELF_CLAIM",
        "launcher_pid_is_authoritative": False,
        "birth_token_required": True,
        "claim_timeout_seconds": WORKER_CLAIM_TIMEOUT_SECONDS,
        "single_claim": True,
    },
    "compatible_migration": {
        "predecessor_commit": ACCEPTED_PARENT_COMMIT,
        "predecessor_model_id": PREDECESSOR_MODEL_ID,
        "predecessor_model_fingerprint": PREDECESSOR_MODEL_FINGERPRINT,
        "reason": MIGRATION_REASON,
        "scientific_identity_changes": False,
    },
    "extension_reasons": ["H1_SAMPLE_INSUFFICIENT", "TECHNICAL_COMPLETENESS"],
    "identity": "ONE_IMMUTABLE_RUN_ACROSS_RUNTIME_SEGMENTS",
    "resume": "SAME_SOURCE_SCOPE_SAME_PAPER_DB_SAME_DURABLE_CURSOR",
    "coverage": "DATA_COVERAGE_NOT_WALL_CLOCK",
    "source_stall_fail_seconds": SOURCE_STALL_FAIL_SECONDS,
    "coverage_gap_proof": "DONE_V034_GAP_JOB_REQUIRED_FOR_STALLED_INTERVAL",
    "end_boundary": "START_PLUS_PREDECLARED_DURATION_EXCLUSIVE",
    "status_surface": list(NO_PEEK_STATUS_FIELDS),
    "evaluation_during_collection": False,
    "paper_only": True,
    "runtime_models": {
        "source": [SOURCE_MODEL_ID, SOURCE_MODEL_FINGERPRINT],
        "binding": [BINDING_MODEL_ID, BINDING_MODEL_FINGERPRINT],
        "runner": [RUNNER_MODEL_ID, RUNNER_MODEL_FINGERPRINT],
        "harness": [MULTIHOUR_MODEL_ID, MULTIHOUR_MODEL_FINGERPRINT],
    },
}


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("utf-8")


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


MODEL_FINGERPRINT = canonical_sha256(SPEC)


class LifecycleError(RuntimeError):
    """Fail-closed lifecycle contract violation."""

    def __init__(self, reason: str, detail: str | None = None) -> None:
        super().__init__(reason if detail is None else f"{reason}: {detail}")
        self.reason = reason
        self.detail = detail


def utc_now() -> datetime:
    return datetime.now(UTC)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(UTC)


def dt_text(value: datetime) -> str:
    return _utc(value).isoformat(timespec="microseconds")


def dt_parse(value: str) -> datetime:
    return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))


def sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + f".{os.getpid()}.tmp")
    with temporary.open("wb") as handle:
        handle.write(canonical_bytes(dict(payload)) + b"\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _write_envelope(path: Path, payload: Mapping[str, Any], *, immutable: bool = False) -> None:
    body = dict(payload)
    envelope = {"payload": body, "payload_sha256": canonical_sha256(body)}
    if path.exists() and immutable:
        existing = _read_envelope(path)
        if existing != body:
            raise LifecycleError("CORRUPT_LIFECYCLE_STATE", f"immutable artifact differs: {path}")
        return
    _atomic_json(path, envelope)
    if immutable:
        try:
            path.chmod(stat.S_IREAD)
        except OSError:
            pass


def _read_envelope(path: Path) -> dict[str, Any]:
    try:
        envelope = json.loads(path.read_text(encoding="utf-8"))
        payload = envelope["payload"]
        expected = str(envelope["payload_sha256"])
    except (OSError, KeyError, TypeError, json.JSONDecodeError) as exc:
        raise LifecycleError("CORRUPT_LIFECYCLE_STATE", str(path)) from exc
    if not isinstance(payload, dict) or canonical_sha256(payload) != expected:
        raise LifecycleError("CORRUPT_LIFECYCLE_STATE", f"digest mismatch: {path}")
    return payload


@contextmanager
def _exclusive_lock(run_dir: Path, *, timeout_seconds: float = 5.0) -> Iterator[None]:
    lock = run_dir / ".lifecycle.lock"
    deadline = time.monotonic() + timeout_seconds
    descriptor: int | None = None
    while descriptor is None:
        try:
            descriptor = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(descriptor, f"{os.getpid()}\n".encode("ascii"))
        except FileExistsError:
            if time.monotonic() >= deadline:
                raise LifecycleError("CORRUPT_LIFECYCLE_STATE", "lifecycle lock is busy")
            time.sleep(0.02)
    try:
        yield
    finally:
        os.close(descriptor)
        try:
            lock.unlink()
        except FileNotFoundError:
            pass


def _resolved(path: str | Path) -> Path:
    return Path(path).expanduser().resolve()


def _file_object_token(path: Path) -> str:
    info = path.stat()
    return canonical_sha256({
        "resolved_path": path.resolve().as_posix(),
        "device": int(info.st_dev),
        "inode_or_file_index": int(info.st_ino),
        "windows_creation_time_ns": int(info.st_ctime_ns) if os.name == "nt" else None,
    })


def _path_identity(
    path: Path,
    *,
    kind: str,
    run_id: str,
    anchor: int,
    file_object_token: str,
) -> str:
    return canonical_sha256({
        "kind": kind,
        "path": path.as_posix(),
        "run_id": run_id,
        "source_anchor": int(anchor),
        "source_model_fingerprint": SOURCE_MODEL_FINGERPRINT,
        "file_object_token": file_object_token,
    })


def _inside(path: Path, parent: Path) -> bool:
    try:
        path.resolve().relative_to(parent.resolve())
        return True
    except ValueError:
        return False


def git_head(repo_root: Path) -> str:
    result = subprocess.run(
        ("git", "rev-parse", "HEAD"), cwd=repo_root, check=True,
        capture_output=True, text=True,
    )
    return result.stdout.strip()


def git_status(repo_root: Path) -> str:
    return subprocess.run(
        ("git", "status", "--porcelain=v1"), cwd=repo_root, check=True,
        capture_output=True, text=True,
    ).stdout


def runtime_contract(repo_root: Path, *, verify_files: bool = True) -> dict[str, Any]:
    file_hashes: dict[str, str] = {}
    for relative, expected in RUNTIME_FILE_SHA256.items():
        actual = sha256_file(repo_root / relative)
        if verify_files and actual != expected:
            raise LifecycleError("REPOSITORY_RUNTIME_MISMATCH", relative)
        file_hashes[relative] = actual
    protocol_path = repo_root / "data/research/phase6/P6_OOS_PROTOCOL_0001/protocol_manifest.json"
    policy_path = repo_root / "data/research/phase6/P6_OOS_PROTOCOL_0001/policy_definitions.json"
    protocol = json.loads(protocol_path.read_text(encoding="utf-8"))
    policies = json.loads(policy_path.read_text(encoding="utf-8"))
    if protocol.get("model_fingerprint") != P6_PROTOCOL_FINGERPRINT:
        raise LifecycleError("PROTOCOL_MISMATCH", "protocol fingerprint")
    if canonical_sha256(policies) != P6_POLICY_SET_SHA256:
        raise LifecycleError("PROTOCOL_MISMATCH", "policy set")
    return {
        "runtime_file_sha256": file_hashes,
        "source_model_id": SOURCE_MODEL_ID,
        "source_model_fingerprint": SOURCE_MODEL_FINGERPRINT,
        "binding_model_id": BINDING_MODEL_ID,
        "binding_model_fingerprint": BINDING_MODEL_FINGERPRINT,
        "runner_model_id": RUNNER_MODEL_ID,
        "runner_model_fingerprint": RUNNER_MODEL_FINGERPRINT,
        "multihour_model_id": MULTIHOUR_MODEL_ID,
        "multihour_model_fingerprint": MULTIHOUR_MODEL_FINGERPRINT,
    }


def source_anchor(path: str | Path) -> int:
    from phase4.paper_continuous_market_source_v0_2 import ContinuousMarketSourceV02

    absolute = _resolved(path)
    conn = ContinuousMarketSourceV02.open_readonly(absolute)
    try:
        ContinuousMarketSourceV02.validate_schema(conn)
        row = conn.execute("SELECT COALESCE(MAX(rowid),0) FROM pump_events").fetchone()
        return int(row[0])
    finally:
        conn.close()


def process_birth_token(pid: int) -> str | None:
    if pid <= 0:
        return None
    if os.name == "nt":
        try:
            import ctypes
            from ctypes import wintypes

            query = 0x1000
            handle = ctypes.windll.kernel32.OpenProcess(query, False, int(pid))
            if not handle:
                return None
            try:
                exit_code = wintypes.DWORD()
                exit_ok = ctypes.windll.kernel32.GetExitCodeProcess(
                    handle, ctypes.byref(exit_code)
                )
                if not exit_ok or int(exit_code.value) != 259:  # STILL_ACTIVE
                    return None
                creation = wintypes.FILETIME()
                exit_time = wintypes.FILETIME()
                kernel = wintypes.FILETIME()
                user = wintypes.FILETIME()
                ok = ctypes.windll.kernel32.GetProcessTimes(
                    handle, ctypes.byref(creation), ctypes.byref(exit_time),
                    ctypes.byref(kernel), ctypes.byref(user),
                )
                if not ok:
                    return None
                ticks = (int(creation.dwHighDateTime) << 32) | int(creation.dwLowDateTime)
                return f"win-filetime:{ticks}"
            finally:
                ctypes.windll.kernel32.CloseHandle(handle)
        except (AttributeError, OSError):
            return None
    proc_stat = Path(f"/proc/{pid}/stat")
    try:
        parts = proc_stat.read_text(encoding="ascii").split()
        return f"proc-start:{parts[21]}"
    except (OSError, IndexError):
        return None


def process_matches(pid: int | None, token: str | None) -> bool:
    if pid is None or token is None:
        return False
    return process_birth_token(int(pid)) == token


@dataclass(frozen=True)
class StartRequest:
    source_db_path: Path
    runtime_root: Path
    repository_commit: str
    start_at: datetime
    source_start_cursor: int
    run_id: str
    paper_db_path: Path | None = None


class FrozenOOSLifecycleV01:
    """Durable, no-peek orchestration around accepted Phase-4 state."""

    def __init__(
        self,
        repo_root: str | Path,
        runtime_root: str | Path = DEFAULT_RUNTIME_ROOT,
        *,
        clock: Callable[[], datetime] = utc_now,
        verify_runtime_files: bool = True,
    ) -> None:
        self.repo_root = _resolved(repo_root)
        self.runtime_root = _resolved(runtime_root)
        self.clock = clock
        self.verify_runtime_files = verify_runtime_files
        if _inside(self.runtime_root, self.repo_root):
            raise LifecycleError("CORRUPT_LIFECYCLE_STATE", "runtime root must be outside repository")

    def run_dir(self, run_id: str) -> Path:
        if not run_id or any(char not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_" for char in run_id):
            raise ValueError("run_id contains unsupported characters")
        return self.runtime_root / run_id

    def _manifest(self, run_id: str) -> dict[str, Any]:
        return _read_envelope(self.run_dir(run_id) / "run_manifest.json")

    def _state(self, run_id: str) -> dict[str, Any]:
        return _read_envelope(self.run_dir(run_id) / "lifecycle_state.json")

    def _save_state(self, run_id: str, state: Mapping[str, Any]) -> None:
        _write_envelope(self.run_dir(run_id) / "lifecycle_state.json", state)

    def _unfinished_runs(self) -> list[str]:
        if not self.runtime_root.exists():
            return []
        result: list[str] = []
        for manifest_path in self.runtime_root.glob("*/run_manifest.json"):
            run_id = manifest_path.parent.name
            try:
                state = self._state(run_id)
            except LifecycleError:
                result.append(run_id)
                continue
            if state.get("lifecycle_state") not in {"FROZEN_READY_FOR_EVALUATION", "INVALID"}:
                result.append(run_id)
        return sorted(result)

    def start(self, request: StartRequest) -> dict[str, Any]:
        start = _utc(request.start_at)
        armed_at = _utc(self.clock())
        arming_lead = start - armed_at
        if arming_lead < timedelta(0):
            raise LifecycleError(
                "INVALID_START_BOUNDARY",
                "requested start must not precede the durable arming instant",
            )
        if arming_lead > timedelta(seconds=MAXIMUM_ARMING_LEAD_SECONDS):
            raise LifecycleError(
                "INVALID_START_BOUNDARY",
                f"requested start exceeds {MAXIMUM_ARMING_LEAD_SECONDS}-second arming lead",
            )
        source = _resolved(request.source_db_path)
        if not source.is_file():
            raise FileNotFoundError(source)
        if request.source_start_cursor < 0:
            raise ValueError("source_start_cursor must be non-negative")
        if _resolved(request.runtime_root) != self.runtime_root:
            raise ValueError("request runtime_root differs from manager runtime_root")
        unfinished = self._unfinished_runs()
        if unfinished:
            raise LifecycleError("UNFINISHED_RUN_EXISTS", ",".join(unfinished))
        run_dir = self.run_dir(request.run_id)
        if run_dir.exists():
            raise LifecycleError("DUPLICATE_START", request.run_id)
        contract = runtime_contract(self.repo_root, verify_files=self.verify_runtime_files)
        current_head = git_head(self.repo_root)
        if current_head != request.repository_commit:
            raise LifecycleError("REPOSITORY_RUNTIME_MISMATCH", "requested commit is not HEAD")
        paper = _resolved(request.paper_db_path or (run_dir / "paper_oos.sqlite3"))
        if _inside(paper, self.repo_root):
            raise LifecycleError("PAPER_IDENTITY_CHANGED", "paper DB must be outside repository")
        if paper.exists():
            raise LifecycleError("PAPER_IDENTITY_CHANGED", "paper DB already exists")
        run_dir.mkdir(parents=True, exist_ok=False)
        paper.parent.mkdir(parents=True, exist_ok=True)
        paper.touch(exist_ok=False)
        minimum_end = start + timedelta(seconds=MINIMUM_DURATION_SECONDS)
        source_file_token = _file_object_token(source)
        paper_file_token = _file_object_token(paper)
        source_identity = _path_identity(
            source, kind="SOURCE_DB", run_id=request.run_id,
            anchor=request.source_start_cursor,
            file_object_token=source_file_token,
        )
        paper_identity = _path_identity(
            paper, kind="PAPER_DB", run_id=request.run_id,
            anchor=request.source_start_cursor,
            file_object_token=paper_file_token,
        )
        manifest = {
            "model_id": MODEL_ID,
            "schema_version": SCHEMA_VERSION,
            "model_fingerprint": MODEL_FINGERPRINT,
            "run_id": request.run_id,
            "repository_commit": request.repository_commit,
            "protocol_id": PROTOCOL_ID,
            "protocol_fingerprint": P6_PROTOCOL_FINGERPRINT,
            "policy_set_sha256": P6_POLICY_SET_SHA256,
            "source_db_path": source.as_posix(),
            "source_database_identity": source_identity,
            "source_file_object_token": source_file_token,
            "source_scope_identity": canonical_sha256({
                "source_database_identity": source_identity,
                "source_start_cursor": request.source_start_cursor,
                "source_model_fingerprint": SOURCE_MODEL_FINGERPRINT,
            }),
            "paper_db_path": paper.as_posix(),
            "paper_database_identity": paper_identity,
            "paper_file_object_token": paper_file_token,
            "requested_start_at_utc": dt_text(start),
            "start_at_utc": dt_text(start),
            "armed_at_utc": dt_text(armed_at),
            "arming_lead_microseconds": (
                arming_lead.days * 86_400_000_000
                + arming_lead.seconds * 1_000_000
                + arming_lead.microseconds
            ),
            "minimum_end_at_utc": dt_text(minimum_end),
            "initial_target_end_at_utc": dt_text(minimum_end),
            "duration_requirement_seconds": MINIMUM_DURATION_SECONDS,
            "source_start_cursor": request.source_start_cursor,
            "source_start_watermark_utc": dt_text(start),
            "no_peek_contract": {
                "status_fields": list(NO_PEEK_STATUS_FIELDS),
                "outcome_evaluation_during_collection": False,
                "performance_based_extension": False,
            },
            "runtime_contract": contract,
            "created_at_utc": dt_text(armed_at),
        }
        state = {
            "run_id": request.run_id,
            "lifecycle_state": "STOPPED",
            "target_end_at_utc": dt_text(minimum_end),
            "duration_seconds": MINIMUM_DURATION_SECONDS,
            "source_cursor": request.source_start_cursor,
            "source_watermark": request.source_start_cursor,
            "source_row_count": 0,
            "candidate_count": 0,
            "h1_eligible_sample_count": 0,
            "coverage_status": "COLLECTING",
            "coverage_complete_through_utc": dt_text(start),
            "unresolved_source_gaps": [],
            "durable_work_pending": False,
            "database_integrity": {"source": "NOT_CHECKED", "paper": "NOT_CREATED"},
            "segments": [],
            "active_process": None,
            "extension_history": [],
            "last_durable_progress_at_utc": dt_text(armed_at),
            "last_error": None,
            "frozen_handoff_sha256": None,
        }
        _write_envelope(run_dir / "run_manifest.json", manifest, immutable=True)
        self._save_state(request.run_id, state)
        return manifest

    def verify_identity(
        self,
        run_id: str,
        *,
        repository_commit: str | None = None,
        source_db_path: str | Path | None = None,
        paper_db_path: str | Path | None = None,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        manifest = self._manifest(run_id)
        state = self._state(run_id)
        if manifest.get("run_id") != run_id or state.get("run_id") != run_id:
            raise LifecycleError("CORRUPT_LIFECYCLE_STATE", "run ID mismatch")
        manifest_fingerprint = manifest.get("model_fingerprint")
        if manifest_fingerprint == MODEL_FINGERPRINT:
            if (
                manifest.get("model_id") != MODEL_ID
                or manifest.get("schema_version") != SCHEMA_VERSION
            ):
                raise LifecycleError("PROTOCOL_MISMATCH", "lifecycle model identity")
            expected_commit = str(manifest["repository_commit"])
        elif manifest_fingerprint == PREDECESSOR_MODEL_FINGERPRINT:
            if (
                manifest.get("model_id") != PREDECESSOR_MODEL_ID
                or manifest.get("schema_version") != PREDECESSOR_SCHEMA_VERSION
            ):
                raise LifecycleError("PROTOCOL_MISMATCH", "predecessor model identity")
            migration = _read_envelope(
                self.run_dir(run_id) / "lifecycle_migration_v0_2.json"
            )
            if (
                migration.get("migration_reason") != MIGRATION_REASON
                or migration.get("original_repository_commit")
                != manifest.get("repository_commit")
                or migration.get("original_lifecycle_model_fingerprint")
                != PREDECESSOR_MODEL_FINGERPRINT
                or migration.get("corrected_lifecycle_model_fingerprint")
                != MODEL_FINGERPRINT
                or state.get("runtime_migration", {}).get("migration_id")
                != migration.get("migration_id")
                or canonical_sha256(migration.get("post_migration_state_payload"))
                != migration.get("post_migration_state_payload_sha256")
                or state.get("runtime_migration")
                != migration.get("post_migration_state_payload", {}).get("runtime_migration")
            ):
                raise LifecycleError("PROTOCOL_MISMATCH", "lifecycle migration")
            expected_commit = str(migration["corrected_runtime_commit"])
        else:
            raise LifecycleError("PROTOCOL_MISMATCH", "lifecycle fingerprint")
        if manifest.get("protocol_fingerprint") != P6_PROTOCOL_FINGERPRINT:
            raise LifecycleError("PROTOCOL_MISMATCH", "P6 protocol")
        if manifest.get("policy_set_sha256") != P6_POLICY_SET_SHA256:
            raise LifecycleError("PROTOCOL_MISMATCH", "P6 policy set")
        self._validate_temporal_state(manifest, state)
        runtime_contract(self.repo_root, verify_files=self.verify_runtime_files)
        observed_commit = repository_commit or git_head(self.repo_root)
        if observed_commit != expected_commit:
            raise LifecycleError("REPOSITORY_RUNTIME_MISMATCH", "repository commit changed")
        expected_source = _resolved(manifest["source_db_path"])
        observed_source = _resolved(source_db_path or expected_source)
        if observed_source != expected_source or not expected_source.is_file():
            raise LifecycleError("SOURCE_IDENTITY_CHANGED")
        expected_paper = _resolved(manifest["paper_db_path"])
        observed_paper = _resolved(paper_db_path or expected_paper)
        if observed_paper != expected_paper:
            raise LifecycleError("PAPER_IDENTITY_CHANGED")
        source_identity = _path_identity(
            expected_source, kind="SOURCE_DB", run_id=run_id,
            anchor=int(manifest["source_start_cursor"]),
            file_object_token=_file_object_token(expected_source),
        )
        if _file_object_token(expected_source) != manifest.get("source_file_object_token"):
            raise LifecycleError("SOURCE_IDENTITY_CHANGED", "source file object changed")
        paper_identity = _path_identity(
            expected_paper, kind="PAPER_DB", run_id=run_id,
            anchor=int(manifest["source_start_cursor"]),
            file_object_token=_file_object_token(expected_paper),
        )
        if _file_object_token(expected_paper) != manifest.get("paper_file_object_token"):
            raise LifecycleError("PAPER_IDENTITY_CHANGED", "paper file object changed")
        if source_identity != manifest.get("source_database_identity"):
            raise LifecycleError("SOURCE_IDENTITY_CHANGED")
        if paper_identity != manifest.get("paper_database_identity"):
            raise LifecycleError("PAPER_IDENTITY_CHANGED")
        return manifest, state

    @staticmethod
    def _validate_temporal_state(
        manifest: Mapping[str, Any], state: Mapping[str, Any]
    ) -> None:
        start = dt_parse(str(manifest["start_at_utc"]))
        requested_start = dt_parse(str(manifest["requested_start_at_utc"]))
        armed_at = dt_parse(str(manifest["armed_at_utc"]))
        if requested_start != start:
            raise LifecycleError("CORRUPT_LIFECYCLE_STATE", "requested start drift")
        arming_lead = start - armed_at
        expected_lead_microseconds = (
            arming_lead.days * 86_400_000_000
            + arming_lead.seconds * 1_000_000
            + arming_lead.microseconds
        )
        if arming_lead < timedelta(0) or arming_lead > timedelta(
            seconds=MAXIMUM_ARMING_LEAD_SECONDS
        ):
            raise LifecycleError("CORRUPT_LIFECYCLE_STATE", "arming lead out of bounds")
        if int(manifest["arming_lead_microseconds"]) != expected_lead_microseconds:
            raise LifecycleError("CORRUPT_LIFECYCLE_STATE", "arming lead drift")
        minimum_end = dt_parse(str(manifest["minimum_end_at_utc"]))
        initial_end = dt_parse(str(manifest["initial_target_end_at_utc"]))
        if minimum_end != start + timedelta(seconds=MINIMUM_DURATION_SECONDS):
            raise LifecycleError("CORRUPT_LIFECYCLE_STATE", "minimum end drift")
        if initial_end != minimum_end:
            raise LifecycleError("CORRUPT_LIFECYCLE_STATE", "initial target drift")
        if int(manifest["duration_requirement_seconds"]) != MINIMUM_DURATION_SECONDS:
            raise LifecycleError("CORRUPT_LIFECYCLE_STATE", "duration requirement drift")
        duration = int(state["duration_seconds"])
        if not MINIMUM_DURATION_SECONDS <= duration <= MAXIMUM_DURATION_SECONDS:
            raise LifecycleError("CORRUPT_LIFECYCLE_STATE", "duration out of bounds")
        if (duration - MINIMUM_DURATION_SECONDS) % EXTENSION_SECONDS:
            raise LifecycleError("CORRUPT_LIFECYCLE_STATE", "duration increment drift")
        target = dt_parse(str(state["target_end_at_utc"]))
        if target != start + timedelta(seconds=duration):
            raise LifecycleError("CORRUPT_LIFECYCLE_STATE", "target end drift")
        history = list(state["extension_history"])
        expected_extensions = (duration - MINIMUM_DURATION_SECONDS) // EXTENSION_SECONDS
        if len(history) != expected_extensions:
            raise LifecycleError("CORRUPT_LIFECYCLE_STATE", "extension history drift")
        expected_end = minimum_end
        for item in history:
            if int(item["extension_seconds"]) != EXTENSION_SECONDS:
                raise LifecycleError("CORRUPT_LIFECYCLE_STATE", "extension size drift")
            if item["reason"] not in {"H1_SAMPLE_INSUFFICIENT", "TECHNICAL_COMPLETENESS"}:
                raise LifecycleError("CORRUPT_LIFECYCLE_STATE", "extension reason drift")
            if dt_parse(str(item["decided_at_utc"])) < expected_end:
                raise LifecycleError("CORRUPT_LIFECYCLE_STATE", "extension decided before boundary")
            expected_end += timedelta(seconds=EXTENSION_SECONDS)
            if dt_parse(str(item["new_target_end_at_utc"])) != expected_end:
                raise LifecycleError("CORRUPT_LIFECYCLE_STATE", "extension target drift")
        if int(state["source_cursor"]) < int(manifest["source_start_cursor"]):
            raise LifecycleError("CORRUPT_LIFECYCLE_STATE", "cursor precedes anchor")
        if int(state["source_watermark"]) < int(state["source_cursor"]):
            raise LifecycleError("CORRUPT_LIFECYCLE_STATE", "watermark precedes cursor")
        valid_states = {"STOPPED", "ACTIVE", "INVALID", "FROZEN_READY_FOR_EVALUATION"}
        if state["lifecycle_state"] not in valid_states:
            raise LifecycleError("CORRUPT_LIFECYCLE_STATE", "unknown lifecycle state")
        segments = list(state["segments"])
        if [int(item["segment_index"]) for item in segments] != list(range(1, len(segments) + 1)):
            raise LifecycleError("CORRUPT_LIFECYCLE_STATE", "segment ordering drift")
        active = state.get("active_process")
        if state["lifecycle_state"] == "ACTIVE" and active is None:
            raise LifecycleError("CORRUPT_LIFECYCLE_STATE", "ACTIVE state has no process")
        if active is not None:
            matches = [
                item for item in segments
                if item["segment_id"] == active.get("segment_id")
                and item["ended_at_utc"] is None
            ]
            if len(matches) != 1:
                raise LifecycleError("CORRUPT_LIFECYCLE_STATE", "active segment reference drift")
            ownership = active.get("ownership_state", "OWNED")
            if ownership == "CLAIM_PENDING":
                if (
                    active.get("pid") is not None
                    or active.get("birth_token") is not None
                    or not active.get("claim_token_sha256")
                    or not active.get("claim_deadline_at_utc")
                ):
                    raise LifecycleError("CORRUPT_LIFECYCLE_STATE", "invalid pending claim")
                dt_parse(str(active["claim_deadline_at_utc"]))
            elif ownership == "OWNED":
                if int(active.get("pid", 0)) <= 0 or not active.get("birth_token"):
                    raise LifecycleError("CORRUPT_LIFECYCLE_STATE", "invalid owned process")
            else:
                raise LifecycleError("CORRUPT_LIFECYCLE_STATE", "unknown ownership state")

    def begin_segment(self, run_id: str, *, pid: int, birth_token: str) -> dict[str, Any]:
        """Directly register the calling process (used by deterministic tests)."""
        manifest, _ = self.verify_identity(run_id)
        del manifest
        run_dir = self.run_dir(run_id)
        with _exclusive_lock(run_dir):
            state = self._state(run_id)
            if state["lifecycle_state"] in {"INVALID", "FROZEN_READY_FOR_EVALUATION"}:
                raise LifecycleError("CORRUPT_LIFECYCLE_STATE", "run is terminal")
            active = state.get("active_process")
            if active and active.get("ownership_state") == "CLAIM_PENDING":
                deadline = dt_parse(str(active["claim_deadline_at_utc"]))
                if _utc(self.clock()) <= deadline:
                    raise LifecycleError("ACTIVE_WRITER_CONFLICT")
            if active and process_matches(active.get("pid"), active.get("birth_token")):
                raise LifecycleError("ACTIVE_WRITER_CONFLICT")
            if active:
                stale = [
                    item for item in state["segments"]
                    if item["segment_id"] == active["segment_id"]
                ]
                if len(stale) != 1:
                    raise LifecycleError("CORRUPT_LIFECYCLE_STATE", "stale segment missing")
                stale[0]["ended_at_utc"] = dt_text(self.clock())
                stale[0]["end_cursor"] = int(state["source_cursor"])
                stale[0]["termination"] = "STALE_PROCESS_AFTER_RESTART"
            segment_index = len(state["segments"]) + 1
            segment_id = f"{run_id}-segment-{segment_index:04d}"
            started = dt_text(self.clock())
            segment = {
                "segment_id": segment_id,
                "segment_index": segment_index,
                "started_at_utc": started,
                "start_cursor": int(state["source_cursor"]),
                "ended_at_utc": None,
                "end_cursor": None,
                "termination": None,
                "pid": int(pid),
                "birth_token": birth_token,
                "launcher_pid": int(pid),
                "ownership_state": "OWNED",
                "claimed_at_utc": started,
            }
            state["segments"].append(segment)
            state["active_process"] = {
                "pid": int(pid), "birth_token": birth_token,
                "segment_id": segment_id,
                "ownership_state": "OWNED",
            }
            state["lifecycle_state"] = "ACTIVE"
            state["last_error"] = None
            self._save_state(run_id, state)
            return segment

    def prepare_segment_launch(
        self, run_id: str, *, claim_token: str
    ) -> dict[str, Any]:
        """Create one unowned segment which only the launched worker may claim."""
        if not claim_token:
            raise ValueError("claim_token is required")
        self.verify_identity(run_id)
        run_dir = self.run_dir(run_id)
        with _exclusive_lock(run_dir):
            state = self._state(run_id)
            if state["lifecycle_state"] in {"INVALID", "FROZEN_READY_FOR_EVALUATION"}:
                raise LifecycleError("CORRUPT_LIFECYCLE_STATE", "run is terminal")
            active = state.get("active_process")
            if active is not None:
                raise LifecycleError("ACTIVE_WRITER_CONFLICT")
            segment_index = len(state["segments"]) + 1
            segment_id = f"{run_id}-segment-{segment_index:04d}"
            started = _utc(self.clock())
            deadline = started + timedelta(seconds=WORKER_CLAIM_TIMEOUT_SECONDS)
            claim_sha256 = hashlib.sha256(claim_token.encode("utf-8")).hexdigest()
            segment = {
                "segment_id": segment_id,
                "segment_index": segment_index,
                "started_at_utc": dt_text(started),
                "start_cursor": int(state["source_cursor"]),
                "ended_at_utc": None,
                "end_cursor": None,
                "termination": None,
                "pid": None,
                "birth_token": None,
                "launcher_pid": None,
                "ownership_state": "CLAIM_PENDING",
                "claimed_at_utc": None,
            }
            state["segments"].append(segment)
            state["active_process"] = {
                "pid": None,
                "birth_token": None,
                "segment_id": segment_id,
                "ownership_state": "CLAIM_PENDING",
                "claim_token_sha256": claim_sha256,
                "claim_deadline_at_utc": dt_text(deadline),
            }
            state["lifecycle_state"] = "ACTIVE"
            state["last_error"] = None
            self._save_state(run_id, state)
            return dict(segment)

    def record_launcher_pid(
        self, run_id: str, *, segment_id: str, launcher_pid: int
    ) -> dict[str, Any]:
        """Record launch diagnostics without granting worker ownership."""
        run_dir = self.run_dir(run_id)
        with _exclusive_lock(run_dir):
            state = self._state(run_id)
            matches = [
                item for item in state["segments"]
                if item["segment_id"] == segment_id
            ]
            if len(matches) != 1 or matches[0]["ended_at_utc"] is not None:
                raise LifecycleError("CORRUPT_LIFECYCLE_STATE", "launch segment missing")
            matches[0]["launcher_pid"] = int(launcher_pid)
            self._save_state(run_id, state)
            return dict(matches[0])

    def claim_segment(
        self,
        run_id: str,
        *,
        segment_id: str,
        claim_token: str,
        pid: int,
        birth_token: str,
    ) -> dict[str, Any]:
        """Atomically bind the actual worker process to one pending segment."""
        if not claim_token or not birth_token or int(pid) <= 0:
            raise LifecycleError("ACTIVE_WRITER_CONFLICT", "invalid worker claim")
        if not process_matches(pid, birth_token):
            raise LifecycleError(
                "ACTIVE_WRITER_CONFLICT", "worker process identity is not live"
            )
        run_dir = self.run_dir(run_id)
        with _exclusive_lock(run_dir):
            state = self._state(run_id)
            active = state.get("active_process")
            claim_sha256 = hashlib.sha256(claim_token.encode("utf-8")).hexdigest()
            if (
                not active
                or active.get("segment_id") != segment_id
                or active.get("ownership_state") != "CLAIM_PENDING"
                or active.get("claim_token_sha256") != claim_sha256
            ):
                raise LifecycleError("ACTIVE_WRITER_CONFLICT", "segment claim rejected")
            if _utc(self.clock()) > dt_parse(str(active["claim_deadline_at_utc"])):
                raise LifecycleError("ACTIVE_WRITER_CONFLICT", "segment claim expired")
            matches = [
                item for item in state["segments"]
                if item["segment_id"] == segment_id and item["ended_at_utc"] is None
            ]
            if len(matches) != 1:
                raise LifecycleError("CORRUPT_LIFECYCLE_STATE", "pending segment missing")
            segment = matches[0]
            claimed_at = dt_text(self.clock())
            segment.update({
                "pid": int(pid),
                "birth_token": birth_token,
                "ownership_state": "OWNED",
                "claimed_at_utc": claimed_at,
            })
            state["active_process"] = {
                "pid": int(pid),
                "birth_token": birth_token,
                "segment_id": segment_id,
                "ownership_state": "OWNED",
            }
            self._save_state(run_id, state)
            return dict(segment)

    def fail_segment_startup(
        self,
        run_id: str,
        *,
        segment_id: str,
        reason: str,
        claim_token: str | None = None,
        pid: int | None = None,
        birth_token: str | None = None,
    ) -> dict[str, Any]:
        """Close only the pending or owned segment proven by the caller."""
        run_dir = self.run_dir(run_id)
        with _exclusive_lock(run_dir):
            state = self._state(run_id)
            active = state.get("active_process")
            if not active or active.get("segment_id") != segment_id:
                return state
            ownership = active.get("ownership_state")
            authorized = False
            if ownership == "CLAIM_PENDING" and claim_token is not None:
                authorized = active.get("claim_token_sha256") == hashlib.sha256(
                    claim_token.encode("utf-8")
                ).hexdigest()
            elif ownership == "OWNED":
                authorized = (
                    int(active.get("pid", -1)) == int(pid or -1)
                    and active.get("birth_token") == birth_token
                )
            if not authorized:
                return state
            matches = [
                item for item in state["segments"]
                if item["segment_id"] == segment_id and item["ended_at_utc"] is None
            ]
            if len(matches) != 1:
                raise LifecycleError("CORRUPT_LIFECYCLE_STATE", "startup segment missing")
            segment = matches[0]
            segment["ended_at_utc"] = dt_text(self.clock())
            segment["end_cursor"] = int(state["source_cursor"])
            segment["termination"] = "WORKER_STARTUP_FAILED"
            state["active_process"] = None
            state["lifecycle_state"] = "INVALID" if ownership == "OWNED" else "STOPPED"
            state["last_error"] = str(reason)
            if ownership == "OWNED":
                state["coverage_status"] = "OOS_COVERAGE_INCOMPLETE"
            self._save_state(run_id, state)
            return state

    def record_progress(
        self,
        run_id: str,
        *,
        source_cursor: int,
        source_watermark: int,
        source_row_count: int,
        candidate_count: int,
        h1_eligible_sample_count: int,
        coverage_complete_through: datetime,
        unresolved_source_gaps: Sequence[str] = (),
        durable_work_pending: bool = False,
        process_token: str | None = None,
    ) -> dict[str, Any]:
        run_dir = self.run_dir(run_id)
        with _exclusive_lock(run_dir):
            state = self._state(run_id)
            active = state.get("active_process")
            if process_token is not None and (
                not active
                or active.get("ownership_state", "OWNED") != "OWNED"
                or active.get("birth_token") != process_token
            ):
                raise LifecycleError("ACTIVE_WRITER_CONFLICT", "segment process token mismatch")
            if int(source_cursor) < int(state["source_cursor"]):
                raise LifecycleError("CORRUPT_LIFECYCLE_STATE", "durable cursor moved backward")
            if int(source_watermark) < int(source_cursor):
                raise LifecycleError("CORRUPT_LIFECYCLE_STATE", "watermark precedes cursor")
            if int(source_watermark) < int(state["source_watermark"]):
                raise LifecycleError("CORRUPT_LIFECYCLE_STATE", "watermark moved backward")
            prior_counts = (
                int(state["source_row_count"]), int(state["candidate_count"]),
                int(state["h1_eligible_sample_count"]),
            )
            counts = (int(source_row_count), int(candidate_count), int(h1_eligible_sample_count))
            if any(new < old for new, old in zip(counts, prior_counts)):
                raise LifecycleError("CORRUPT_LIFECYCLE_STATE", "durable count moved backward")
            if (
                int(source_cursor) == int(state["source_cursor"])
                and counts[0] != prior_counts[0]
            ):
                raise LifecycleError(
                    "CORRUPT_LIFECYCLE_STATE",
                    "source-row count changed without durable cursor advancement",
                )
            coverage = _utc(coverage_complete_through)
            if coverage < dt_parse(state["coverage_complete_through_utc"]):
                raise LifecycleError("CORRUPT_LIFECYCLE_STATE", "coverage moved backward")
            state.update({
                "source_cursor": int(source_cursor),
                "source_watermark": int(source_watermark),
                "source_row_count": counts[0],
                "candidate_count": counts[1],
                "h1_eligible_sample_count": counts[2],
                "coverage_complete_through_utc": dt_text(coverage),
                "unresolved_source_gaps": sorted(set(map(str, unresolved_source_gaps))),
                "durable_work_pending": bool(durable_work_pending),
                "coverage_status": (
                    "COMPLETE_THROUGH_TARGET"
                    if coverage >= dt_parse(state["target_end_at_utc"])
                    and not unresolved_source_gaps
                    else "INCOMPLETE"
                ),
                "last_durable_progress_at_utc": dt_text(self.clock()),
            })
            self._save_state(run_id, state)
            return state

    def end_segment(self, run_id: str, *, termination: str, error: str | None = None) -> dict[str, Any]:
        run_dir = self.run_dir(run_id)
        with _exclusive_lock(run_dir):
            state = self._state(run_id)
            active = state.get("active_process")
            if active is None:
                return state
            segment_id = active["segment_id"]
            matches = [item for item in state["segments"] if item["segment_id"] == segment_id]
            if len(matches) != 1:
                raise LifecycleError("CORRUPT_LIFECYCLE_STATE", "active segment missing")
            segment = matches[0]
            if segment["ended_at_utc"] is None:
                segment["ended_at_utc"] = dt_text(self.clock())
                segment["end_cursor"] = int(state["source_cursor"])
                segment["termination"] = str(termination)
            state["active_process"] = None
            state["lifecycle_state"] = "INVALID" if error else "STOPPED"
            state["last_error"] = error
            if error:
                state["coverage_status"] = "OOS_COVERAGE_INCOMPLETE"
            self._save_state(run_id, state)
            return state

    def reconcile_process(self, run_id: str) -> dict[str, Any]:
        run_dir = self.run_dir(run_id)
        with _exclusive_lock(run_dir):
            state = self._state(run_id)
            active = state.get("active_process")
            if not active:
                return state
            if active.get("ownership_state") == "CLAIM_PENDING":
                if _utc(self.clock()) <= dt_parse(str(active["claim_deadline_at_utc"])):
                    return state
                termination = "WORKER_CLAIM_TIMEOUT"
                last_error = "WORKER_CLAIM_TIMEOUT"
            elif process_matches(active.get("pid"), active.get("birth_token")):
                return state
            else:
                termination = "STALE_PROCESS_AFTER_RESTART"
                last_error = state.get("last_error")
            matches = [
                item for item in state["segments"]
                if item["segment_id"] == active["segment_id"]
            ]
            if len(matches) != 1:
                raise LifecycleError("CORRUPT_LIFECYCLE_STATE", "active segment missing")
            segment = matches[0]
            segment["ended_at_utc"] = dt_text(self.clock())
            segment["end_cursor"] = int(state["source_cursor"])
            segment["termination"] = termination
            state["active_process"] = None
            state["lifecycle_state"] = "STOPPED"
            state["last_error"] = last_error
            self._save_state(run_id, state)
            return state

    def status(self, run_id: str, *, now: datetime | None = None) -> dict[str, Any]:
        manifest, _ = self.verify_identity(run_id)
        state = self.reconcile_process(run_id)
        instant = _utc(now or self.clock())
        start = dt_parse(manifest["start_at_utc"])
        end = dt_parse(state["target_end_at_utc"])
        active = state.get("active_process")
        if active and active.get("ownership_state") == "CLAIM_PENDING":
            process_health = "STARTING_UNCLAIMED"
        elif active and process_matches(active.get("pid"), active.get("birth_token")):
            process_health = "RUNNING_OWNED_PROCESS"
        else:
            process_health = "NOT_RUNNING"
        reported_state = state["lifecycle_state"]
        if instant < start and state["lifecycle_state"] == "ACTIVE":
            reported_state = "ARMED_WAITING_FOR_START"
        elif instant < start and state["lifecycle_state"] == "STOPPED":
            reported_state = "ARMED_STOPPED"
        result = {
            "run_id": run_id,
            "start_at_utc": dt_text(start),
            "end_at_utc": dt_text(end),
            "elapsed_seconds": max(0, min(int((instant - start).total_seconds()), int((end - start).total_seconds()))),
            "remaining_seconds": max(0, int((end - instant).total_seconds())),
            "lifecycle_state": reported_state,
            "segment_count": len(state["segments"]),
            "process_health": process_health,
            "source_cursor": state["source_cursor"],
            "source_watermark": state["source_watermark"],
            "source_row_count": state["source_row_count"],
            "candidate_count": state["candidate_count"],
            "h1_eligible_sample_count": state["h1_eligible_sample_count"],
            "coverage_status": state["coverage_status"],
            "paper_db_path": manifest["paper_db_path"],
            "source_db_path": manifest["source_db_path"],
            "database_integrity": state["database_integrity"],
            "last_durable_progress_at_utc": state["last_durable_progress_at_utc"],
            "last_error": state["last_error"],
            "restart_count": max(0, len(state["segments"]) - 1),
        }
        if tuple(result) != NO_PEEK_STATUS_FIELDS:
            raise LifecycleError("CORRUPT_LIFECYCLE_STATE", "status surface drift")
        return result

    def salvage_check(
        self, run_id: str, *, now: datetime | None = None
    ) -> dict[str, Any]:
        """Read-only eligibility proof for the one known T002A failed launch."""
        checked_at = _utc(now or self.clock())
        checks: dict[str, bool] = {
            "exact_known_run_id": run_id == KNOWN_SALVAGE_RUN_ID,
        }
        result: dict[str, Any] = {
            "result": "SALVAGE_NOT_AUTHORIZED",
            "run_id": run_id,
            "checked_at_utc": dt_text(checked_at),
            "checks": checks,
            "migration_reason": MIGRATION_REASON,
            "evaluation_performed": False,
        }
        if not checks["exact_known_run_id"]:
            return result
        run_dir = self.run_dir(run_id)
        try:
            manifest = _read_envelope(run_dir / "run_manifest.json")
            state = _read_envelope(run_dir / "lifecycle_state.json")
        except (LifecycleError, FileNotFoundError, OSError, ValueError) as exc:
            result["error"] = f"{type(exc).__name__}:{exc}"
            return result

        migration_path = run_dir / "lifecycle_migration_v0_2.json"
        checks.update({
            "not_already_migrated": not migration_path.exists(),
            "exact_predecessor_commit": manifest.get("repository_commit") == ACCEPTED_PARENT_COMMIT,
            "exact_predecessor_model": (
                manifest.get("model_id") == PREDECESSOR_MODEL_ID
                and manifest.get("schema_version") == PREDECESSOR_SCHEMA_VERSION
                and manifest.get("model_fingerprint") == PREDECESSOR_MODEL_FINGERPRINT
            ),
            "exact_scientific_start": manifest.get("start_at_utc") == KNOWN_SALVAGE_START,
            "exact_scientific_end": (
                manifest.get("minimum_end_at_utc") == KNOWN_SALVAGE_END
                and state.get("target_end_at_utc") == KNOWN_SALVAGE_END
            ),
            "exact_source_anchor": (
                int(manifest.get("source_start_cursor", -1)) == KNOWN_SALVAGE_SOURCE_ANCHOR
                and int(state.get("source_cursor", -1)) == KNOWN_SALVAGE_SOURCE_ANCHOR
                and int(state.get("source_watermark", -1)) == KNOWN_SALVAGE_SOURCE_ANCHOR
            ),
            "protocol_unchanged": manifest.get("protocol_fingerprint") == P6_PROTOCOL_FINGERPRINT,
            "policy_set_unchanged": manifest.get("policy_set_sha256") == P6_POLICY_SET_SHA256,
            "no_scientific_progress": (
                int(state.get("source_row_count", -1)) == 0
                and int(state.get("candidate_count", -1)) == 0
                and int(state.get("h1_eligible_sample_count", -1)) == 0
                and state.get("coverage_complete_through_utc") == KNOWN_SALVAGE_START
                and state.get("durable_work_pending") is False
            ),
            "no_evaluation_handoff": (
                state.get("frozen_handoff_sha256") is None
                and not (run_dir / "frozen_handoff_manifest.json").exists()
            ),
        })
        active = state.get("active_process") or {}
        checks["registered_process_is_dead"] = bool(active) and not process_matches(
            active.get("pid"), active.get("birth_token")
        )
        log_path = run_dir / "segment_0001.log"
        try:
            failure = json.loads(log_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            failure = {}
        checks["exact_worker_failure_signature"] = (
            failure.get("reason") == "ACTIVE_WRITER_CONFLICT"
            and failure.get("result") == "FAIL_CLOSED"
            and failure.get("detail")
            == "ACTIVE_WRITER_CONFLICT: worker is not the registered segment"
        )
        checks["exact_unadvanced_segment"] = (
            state.get("lifecycle_state") == "ACTIVE"
            and len(state.get("segments", [])) == 1
            and state["segments"][0].get("segment_id") == active.get("segment_id")
            and int(state["segments"][0].get("start_cursor", -1))
            == KNOWN_SALVAGE_SOURCE_ANCHOR
            and state["segments"][0].get("end_cursor") is None
            and state["segments"][0].get("ended_at_utc") is None
        )

        paper = _resolved(manifest.get("paper_db_path", run_dir / "missing-paper"))
        checks["paper_identity_unchanged"] = (
            paper.is_file()
            and _file_object_token(paper) == manifest.get("paper_file_object_token")
            and _path_identity(
                paper, kind="PAPER_DB", run_id=run_id,
                anchor=KNOWN_SALVAGE_SOURCE_ANCHOR,
                file_object_token=_file_object_token(paper),
            ) == manifest.get("paper_database_identity")
        )
        checks["paper_scientific_state_absent"] = paper.is_file() and paper.stat().st_size == 0
        allowed_runtime_names = {
            "run_manifest.json", "lifecycle_state.json", "paper_oos.sqlite3",
        }
        unexpected_artifacts = [
            path for path in run_dir.iterdir()
            if path.name not in allowed_runtime_names
            and not (
                path.is_file()
                and path.name.startswith("segment_")
                and path.name.endswith(".log")
            )
        ]
        checks["performance_and_evaluation_artifacts_absent"] = not unexpected_artifacts

        source = _resolved(manifest.get("source_db_path", run_dir / "missing-source"))
        source_summary: dict[str, Any] = {
            "source_db_path": source.as_posix(),
            "connection_mode": "mode=ro; PRAGMA query_only=ON",
            "source_start_cursor": KNOWN_SALVAGE_SOURCE_ANCHOR,
        }
        try:
            from phase4.paper_continuous_market_source_v0_2 import ContinuousMarketSourceV02

            conn = ContinuousMarketSourceV02.open_readonly(source)
            try:
                ContinuousMarketSourceV02.validate_schema(conn)
                anchor_present = conn.execute(
                    "SELECT 1 FROM pump_events WHERE rowid=?",
                    (KNOWN_SALVAGE_SOURCE_ANCHOR,),
                ).fetchone() is not None
                maximum_rowid = int(
                    conn.execute("SELECT COALESCE(MAX(rowid),0) FROM pump_events").fetchone()[0]
                )
                gap_rows = conn.execute(
                    "SELECT gap_key,status,gap_started_at_utc,gap_ended_at_utc "
                    "FROM gap_jobs_v034 WHERE gap_ended_at_utc>=? "
                    "AND gap_started_at_utc<? ORDER BY gap_started_at_utc,gap_key",
                    (KNOWN_SALVAGE_START, dt_text(min(
                        max(checked_at, dt_parse(KNOWN_SALVAGE_START)),
                        dt_parse(KNOWN_SALVAGE_END),
                    ))),
                ).fetchall()
                gap_status_counts = {
                    str(status): sum(1 for item in gap_rows if str(item[1]) == str(status))
                    for status in sorted({str(item[1]) for item in gap_rows})
                }
                active_recovery_gaps = {
                    f"{row[0]}:{row[1]}"
                    for row in gap_rows
                    if str(row[1]) in {"PENDING", "RETRY", "IN_PROGRESS"}
                }
            finally:
                conn.close()
            source_token = _file_object_token(source)
            source_identity_ok = (
                source_token == manifest.get("source_file_object_token")
                and _path_identity(
                    source, kind="SOURCE_DB", run_id=run_id,
                    anchor=KNOWN_SALVAGE_SOURCE_ANCHOR,
                    file_object_token=source_token,
                ) == manifest.get("source_database_identity")
            )
            coverage_target = min(max(checked_at, dt_parse(KNOWN_SALVAGE_START)), dt_parse(KNOWN_SALVAGE_END))
            coverage, gaps = source_coverage_probe(
                source,
                start=dt_parse(KNOWN_SALVAGE_START),
                target=coverage_target,
                source_start_cursor=KNOWN_SALVAGE_SOURCE_ANCHOR,
            )
            recoverable_statuses = {
                status for status in gap_status_counts
                if status in {"PENDING", "RETRY", "IN_PROGRESS"}
                or status.startswith("DONE")
            }
            recovery_available = (
                recoverable_statuses == set(gap_status_counts)
                and all(gap in active_recovery_gaps for gap in gaps)
            )
            checks.update({
                "source_identity_continuity": source_identity_ok,
                "original_anchor_row_present": anchor_present,
                "subsequent_source_rows_present": maximum_rowid > KNOWN_SALVAGE_SOURCE_ANCHOR,
                "source_gap_ledger_inspectable": True,
                "source_coverage_recovery_path_available": recovery_available,
            })
            source_summary.update({
                "maximum_pump_events_rowid": maximum_rowid,
                "rows_after_anchor": maximum_rowid - KNOWN_SALVAGE_SOURCE_ANCHOR,
                "coverage_target_utc": dt_text(coverage_target),
                "coverage_complete_through_utc": dt_text(coverage),
                "unresolved_coverage_gap_count": len(gaps),
                "gap_status_counts": gap_status_counts,
                "coverage_proven_through_check_time": coverage >= coverage_target and not gaps,
                "assessment": (
                    "PROVEN_THROUGH_CHECK_TIME"
                    if coverage >= coverage_target and not gaps
                    else "RECOVERABLE_GAPS_PENDING"
                    if recovery_available
                    else "UNRECOVERABLE_SOURCE_GAP"
                ),
            })
        except (LifecycleError, OSError, sqlite3.DatabaseError, ValueError) as exc:
            checks.update({
                "source_identity_continuity": False,
                "original_anchor_row_present": False,
                "subsequent_source_rows_present": False,
                "source_gap_ledger_inspectable": False,
                "source_coverage_recovery_path_available": False,
            })
            source_summary["inspection_error"] = f"{type(exc).__name__}:{exc}"

        try:
            current_runtime = runtime_contract(
                self.repo_root, verify_files=self.verify_runtime_files
            )
            checks["phase4_runtime_hashes_unchanged"] = (
                manifest.get("runtime_contract") == current_runtime
            )
        except (LifecycleError, OSError, ValueError):
            checks["phase4_runtime_hashes_unchanged"] = False
        result.update({
            "original_repository_commit": manifest.get("repository_commit"),
            "original_lifecycle_model_id": manifest.get("model_id"),
            "original_lifecycle_model_fingerprint": manifest.get("model_fingerprint"),
            "corrected_runtime_commit_candidate": git_head(self.repo_root),
            "corrected_lifecycle_model_id": MODEL_ID,
            "corrected_lifecycle_model_fingerprint": MODEL_FINGERPRINT,
            "pre_migration_state_payload_sha256": canonical_sha256(state),
            "scientific_identity": {
                "start_at_utc": manifest.get("start_at_utc"),
                "end_at_utc": state.get("target_end_at_utc"),
                "source_start_cursor": manifest.get("source_start_cursor"),
                "protocol_fingerprint": manifest.get("protocol_fingerprint"),
                "policy_set_sha256": manifest.get("policy_set_sha256"),
            },
            "source_coverage": source_summary,
            "paper_db": {
                "path": paper.as_posix(),
                "size_bytes": paper.stat().st_size if paper.is_file() else None,
                "scientific_state_present": not checks["paper_scientific_state_absent"],
            },
        })
        if all(checks.values()):
            result["result"] = "SALVAGE_AUTHORIZED"
        return result

    def migrate_salvage(self, run_id: str) -> dict[str, Any]:
        """Atomically bind the known predecessor run to a reviewed checkpoint."""
        current_commit = git_head(self.repo_root)
        if current_commit == ACCEPTED_PARENT_COMMIT or git_status(self.repo_root).strip():
            raise LifecycleError(
                "CORRECTED_RUNTIME_NOT_CHECKPOINTED",
                "migration requires one clean corrected Git checkpoint",
            )
        run_dir = self.run_dir(run_id)
        migration_path = run_dir / "lifecycle_migration_v0_2.json"
        eligibility = None if migration_path.exists() else self.salvage_check(run_id)
        with _exclusive_lock(run_dir):
            state = self._state(run_id)
            current_state_sha = canonical_sha256(state)
            if migration_path.exists():
                existing = _read_envelope(migration_path)
                if (
                    existing.get("corrected_runtime_commit") != current_commit
                    or existing.get("corrected_lifecycle_model_fingerprint") != MODEL_FINGERPRINT
                ):
                    raise LifecycleError("PROTOCOL_MISMATCH", "existing migration binding")
                if current_state_sha == existing.get("post_migration_state_payload_sha256"):
                    return existing
                if current_state_sha == existing.get("pre_migration_state_payload_sha256"):
                    self._save_state(run_id, existing["post_migration_state_payload"])
                    return existing
                raise LifecycleError("CORRUPT_LIFECYCLE_STATE", "migration replay state mismatch")

            if eligibility is None or eligibility.get("result") != "SALVAGE_AUTHORIZED":
                if (
                    eligibility is not None
                    and eligibility.get("source_coverage", {}).get("assessment")
                    == "UNRECOVERABLE_SOURCE_GAP"
                ):
                    active = state.get("active_process")
                    matches = [
                        item for item in state["segments"]
                        if active and item["segment_id"] == active.get("segment_id")
                    ]
                    if len(matches) == 1 and matches[0]["ended_at_utc"] is None:
                        matches[0]["ended_at_utc"] = dt_text(self.clock())
                        matches[0]["end_cursor"] = int(state["source_cursor"])
                        matches[0]["termination"] = "UNRECOVERABLE_SOURCE_GAP"
                    state["active_process"] = None
                    state["lifecycle_state"] = "INVALID"
                    state["last_error"] = "UNRECOVERABLE_SOURCE_GAP"
                    state["coverage_status"] = "OOS_COVERAGE_INCOMPLETE"
                    self._save_state(run_id, state)
                raise LifecycleError("SALVAGE_NOT_AUTHORIZED")
            if current_state_sha != eligibility["pre_migration_state_payload_sha256"]:
                raise LifecycleError("CORRUPT_LIFECYCLE_STATE", "state changed after salvage check")
            manifest = self._manifest(run_id)
            migrated_at = _utc(self.clock())
            post_state = json.loads(json.dumps(state))
            active = post_state.get("active_process")
            matches = [
                item for item in post_state["segments"]
                if active and item["segment_id"] == active.get("segment_id")
            ]
            if len(matches) != 1 or matches[0]["ended_at_utc"] is not None:
                raise LifecycleError("SALVAGE_NOT_AUTHORIZED", "failed segment shape changed")
            matches[0]["ended_at_utc"] = dt_text(migrated_at)
            matches[0]["end_cursor"] = KNOWN_SALVAGE_SOURCE_ANCHOR
            matches[0]["termination"] = MIGRATION_REASON
            migration_id = "P6M-" + canonical_sha256({
                "run_id": run_id,
                "pre_state": current_state_sha,
                "corrected_commit": current_commit,
                "corrected_fingerprint": MODEL_FINGERPRINT,
            })[:32]
            post_state["active_process"] = None
            post_state["lifecycle_state"] = "STOPPED"
            post_state["last_error"] = "MIGRATED_AFTER_WINDOWS_WORKER_OWNERSHIP_FAILURE"
            post_state["runtime_migration"] = {
                "migration_id": migration_id,
                "migration_reason": MIGRATION_REASON,
                "migrated_at_utc": dt_text(migrated_at),
                "original_repository_commit": manifest["repository_commit"],
                "original_lifecycle_model_fingerprint": manifest["model_fingerprint"],
                "corrected_runtime_commit": current_commit,
                "corrected_lifecycle_model_fingerprint": MODEL_FINGERPRINT,
            }
            post_state_sha = canonical_sha256(post_state)
            migration = {
                "migration_schema_version": MIGRATION_SCHEMA_VERSION,
                "migration_id": migration_id,
                "migration_reason": MIGRATION_REASON,
                "migrated_at_utc": dt_text(migrated_at),
                "run_id": run_id,
                "original_repository_commit": manifest["repository_commit"],
                "original_lifecycle_model_id": manifest["model_id"],
                "original_lifecycle_model_fingerprint": manifest["model_fingerprint"],
                "corrected_runtime_commit": current_commit,
                "corrected_lifecycle_model_id": MODEL_ID,
                "corrected_lifecycle_model_fingerprint": MODEL_FINGERPRINT,
                "pre_migration_state_payload_sha256": current_state_sha,
                "post_migration_state_payload_sha256": post_state_sha,
                "post_migration_state_payload": post_state,
                "protocol_fingerprint_before_after": [
                    manifest["protocol_fingerprint"], P6_PROTOCOL_FINGERPRINT,
                ],
                "policy_set_sha256_before_after": [
                    manifest["policy_set_sha256"], P6_POLICY_SET_SHA256,
                ],
                "phase4_runtime_sha256_before_after": [
                    manifest["runtime_contract"]["runtime_file_sha256"],
                    runtime_contract(self.repo_root, verify_files=True)["runtime_file_sha256"],
                ],
                "scientific_identity_before_after": [{
                    "start_at_utc": manifest["start_at_utc"],
                    "end_at_utc": state["target_end_at_utc"],
                    "source_start_cursor": manifest["source_start_cursor"],
                }, {
                    "start_at_utc": manifest["start_at_utc"],
                    "end_at_utc": post_state["target_end_at_utc"],
                    "source_start_cursor": manifest["source_start_cursor"],
                }],
                "evaluation_performed": False,
            }
            _write_envelope(migration_path, migration, immutable=True)
            self._save_state(run_id, post_state)
            return migration

    def extend(self, run_id: str, *, seconds: int, reason: str, decided_at: datetime | None = None) -> dict[str, Any]:
        if int(seconds) != EXTENSION_SECONDS:
            raise LifecycleError("INVALID_EXTENSION", "extensions are exactly 24 hours")
        if reason not in {"H1_SAMPLE_INSUFFICIENT", "TECHNICAL_COMPLETENESS"}:
            raise LifecycleError("INVALID_EXTENSION", "reason is not predeclared")
        manifest, _ = self.verify_identity(run_id)
        run_dir = self.run_dir(run_id)
        with _exclusive_lock(run_dir):
            state = self._state(run_id)
            if state["lifecycle_state"] in {"FROZEN_READY_FOR_EVALUATION", "INVALID", "ACTIVE"}:
                raise LifecycleError("INVALID_EXTENSION", "run is not stopped and extensible")
            if reason == "H1_SAMPLE_INSUFFICIENT" and int(state["h1_eligible_sample_count"]) >= H1_MINIMUM_ELIGIBLE_ENTRIES:
                raise LifecycleError("INVALID_EXTENSION", "H1 sample is already sufficient")
            current_duration = int(state["duration_seconds"])
            if current_duration + EXTENSION_SECONDS > MAXIMUM_DURATION_SECONDS:
                raise LifecycleError("INVALID_EXTENSION", "maximum 168-hour window exceeded")
            decision = _utc(decided_at or self.clock())
            current_target = dt_parse(state["target_end_at_utc"])
            if decision < current_target:
                raise LifecycleError("INVALID_EXTENSION", "current boundary not reached")
            state["duration_seconds"] = current_duration + EXTENSION_SECONDS
            state["target_end_at_utc"] = dt_text(
                dt_parse(manifest["start_at_utc"]) + timedelta(seconds=state["duration_seconds"])
            )
            state["extension_history"].append({
                "extension_seconds": EXTENSION_SECONDS,
                "reason": reason,
                "decided_at_utc": dt_text(decision),
                "new_target_end_at_utc": state["target_end_at_utc"],
            })
            state["coverage_status"] = "COLLECTING"
            self._save_state(run_id, state)
            return state

    def mark_invalid(self, run_id: str, reason: str) -> dict[str, Any]:
        if reason not in {
            "SOURCE_IDENTITY_CHANGED", "PAPER_IDENTITY_CHANGED",
            "SOURCE_COVERAGE_INCOMPLETE", "UNRECOVERABLE_SOURCE_GAP",
            "PROTOCOL_MISMATCH", "REPOSITORY_RUNTIME_MISMATCH",
            "CORRUPT_LIFECYCLE_STATE", "ACTIVE_WRITER_CONFLICT",
        }:
            raise ValueError("unsupported terminal invalid reason")
        run_dir = self.run_dir(run_id)
        with _exclusive_lock(run_dir):
            state = self._state(run_id)
            active = state.get("active_process")
            if active is not None:
                matches = [
                    item for item in state["segments"]
                    if item["segment_id"] == active["segment_id"]
                ]
                if len(matches) != 1:
                    raise LifecycleError(
                        "CORRUPT_LIFECYCLE_STATE", "active segment missing"
                    )
                segment = matches[0]
                if segment["ended_at_utc"] is None:
                    segment["ended_at_utc"] = dt_text(self.clock())
                    segment["end_cursor"] = int(state["source_cursor"])
                    segment["termination"] = "FAIL_CLOSED"
            state["active_process"] = None
            state["lifecycle_state"] = "INVALID"
            state["last_error"] = reason
            state["coverage_status"] = "OOS_COVERAGE_INCOMPLETE"
            self._save_state(run_id, state)
            return state

    def finalize(self, run_id: str, *, now: datetime | None = None) -> dict[str, Any]:
        manifest, _ = self.verify_identity(run_id)
        run_dir = self.run_dir(run_id)
        existing = run_dir / "frozen_handoff_manifest.json"
        if existing.exists():
            handoff = _read_envelope(existing)
            state = self._state(run_id)
            if state.get("lifecycle_state") not in {
                "STOPPED", "FROZEN_READY_FOR_EVALUATION"
            }:
                raise LifecycleError("CORRUPT_LIFECYCLE_STATE", "handoff/state disagreement")
            if state.get("lifecycle_state") == "STOPPED":
                state["lifecycle_state"] = "FROZEN_READY_FOR_EVALUATION"
                state["frozen_handoff_sha256"] = sha256_file(existing)
                self._save_state(run_id, state)
            return handoff
        instant = _utc(now or self.clock())
        with _exclusive_lock(run_dir):
            state = self._state(run_id)
            if existing.exists():
                handoff = _read_envelope(existing)
                if state.get("lifecycle_state") not in {
                    "STOPPED", "FROZEN_READY_FOR_EVALUATION"
                }:
                    raise LifecycleError(
                        "CORRUPT_LIFECYCLE_STATE", "handoff/state disagreement"
                    )
                state["lifecycle_state"] = "FROZEN_READY_FOR_EVALUATION"
                state["frozen_handoff_sha256"] = sha256_file(existing)
                self._save_state(run_id, state)
                return handoff
            target = dt_parse(state["target_end_at_utc"])
            if instant < target:
                raise LifecycleError("END_BOUNDARY_NOT_REACHED")
            active = state.get("active_process")
            if active and process_matches(active.get("pid"), active.get("birth_token")):
                raise LifecycleError("ACTIVE_WRITER_CONFLICT")
            if state["coverage_status"] != "COMPLETE_THROUGH_TARGET" or state["unresolved_source_gaps"]:
                raise LifecycleError("SOURCE_COVERAGE_INCOMPLETE")
            if state["durable_work_pending"]:
                raise LifecycleError("SOURCE_COVERAGE_INCOMPLETE", "durable work remains")
            if int(state["source_cursor"]) < int(state["source_watermark"]):
                raise LifecycleError("SOURCE_COVERAGE_INCOMPLETE", "cursor has not reached watermark")
            source = _resolved(manifest["source_db_path"])
            paper = _resolved(manifest["paper_db_path"])
            if not paper.is_file():
                raise LifecycleError("PAPER_IDENTITY_CHANGED", "paper DB is absent")
            observed_coverage, observed_gaps = source_coverage_probe(
                source,
                start=dt_parse(manifest["start_at_utc"]),
                target=target,
                source_start_cursor=int(manifest["source_start_cursor"]),
            )
            if observed_coverage < target or observed_gaps:
                raise LifecycleError(
                    "SOURCE_COVERAGE_INCOMPLETE",
                    "current source gap ledger does not prove the frozen boundary",
                )
            frozen_source = run_dir / "frozen_source.sqlite3"
            frozen_paper = run_dir / "frozen_paper.sqlite3"
            source_integrity = _sqlite_quick_check(source, readonly=True)
            paper_integrity = _sqlite_quick_check(paper, readonly=True)
            if source_integrity != "ok" or paper_integrity != "ok":
                raise LifecycleError("CORRUPT_LIFECYCLE_STATE", "SQLite quick_check failed")
            # Without an immutable handoff, any leftover snapshot belongs to
            # an interrupted finalization and is not authoritative. Rebuild
            # both from the same current, revalidated final state.
            for uncommitted in (frozen_source, frozen_paper):
                if uncommitted.exists():
                    uncommitted.unlink()
            _sqlite_snapshot(source, frozen_source)
            _sqlite_snapshot(paper, frozen_paper)
            source_sha = sha256_file(frozen_source)
            paper_sha = sha256_file(frozen_paper)
            segments = state["segments"]
            segment_digest = canonical_sha256(segments)
            source_logical_sha = sqlite_logical_sha256(frozen_source)
            paper_logical_sha = sqlite_logical_sha256(frozen_paper)
            outcome_payload = {
                "start_at_utc": manifest["start_at_utc"],
                "end_at_utc": state["target_end_at_utc"],
                "source_start_cursor": manifest["source_start_cursor"],
                "source_end_cursor": state["source_cursor"],
                "source_logical_sha256": source_logical_sha,
                "paper_logical_sha256": paper_logical_sha,
            }
            handoff = {
                "handoff_schema_version": "phase6_frozen_oos_handoff_v0.1",
                "run_id": run_id,
                "start_at_utc": manifest["start_at_utc"],
                "end_at_utc": state["target_end_at_utc"],
                "duration_seconds": state["duration_seconds"],
                "source_db_path": frozen_source.as_posix(),
                "source_db_identity": manifest["source_database_identity"],
                "source_db_sha256": source_sha,
                "paper_db_path": frozen_paper.as_posix(),
                "paper_db_identity": manifest["paper_database_identity"],
                "paper_db_sha256": paper_sha,
                "source_logical_sha256": source_logical_sha,
                "paper_logical_sha256": paper_logical_sha,
                "source_start_cursor": manifest["source_start_cursor"],
                "source_end_cursor": state["source_cursor"],
                "source_end_watermark": state["source_watermark"],
                "source_coverage": {
                    "status": state["coverage_status"],
                    "complete_through_utc": state["coverage_complete_through_utc"],
                    "unresolved_gaps": [],
                    "post_end_exclusion": "P6_T001_HALF_OPEN_TIMESTAMP_WINDOW",
                },
                "segment_history_digest": segment_digest,
                "lifecycle_model_id": MODEL_ID,
                "lifecycle_model_fingerprint": MODEL_FINGERPRINT,
                "protocol_id": PROTOCOL_ID,
                "protocol_fingerprint": P6_PROTOCOL_FINGERPRINT,
                "policy_set_sha256": P6_POLICY_SET_SHA256,
                "h1_eligible_sample_count": state["h1_eligible_sample_count"],
                "extension_history": state["extension_history"],
                "frozen_outcome_digest": canonical_sha256(outcome_payload),
                "artifact_sha256": {
                    "frozen_source.sqlite3": source_sha,
                    "frozen_paper.sqlite3": paper_sha,
                    "run_manifest.json": sha256_file(run_dir / "run_manifest.json"),
                },
                "finalized_at_utc": dt_text(instant),
                "evaluation_performed": False,
            }
            _write_envelope(existing, handoff, immutable=True)
            state["database_integrity"] = {"source": source_integrity, "paper": paper_integrity}
            state["lifecycle_state"] = "FROZEN_READY_FOR_EVALUATION"
            state["frozen_handoff_sha256"] = sha256_file(existing)
            self._save_state(run_id, state)
            return handoff


def _sqlite_quick_check(path: Path, *, readonly: bool) -> str:
    if readonly:
        conn = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)
        conn.execute("PRAGMA query_only=ON")
    else:
        conn = sqlite3.connect(path)
    try:
        row = conn.execute("PRAGMA quick_check").fetchone()
        return "missing" if row is None else str(row[0])
    finally:
        conn.close()


def _sqlite_snapshot(
    source: Path, destination: Path, *, allow_existing: bool = False
) -> None:
    if destination.exists():
        if allow_existing and _sqlite_quick_check(destination, readonly=True) == "ok":
            return
        raise LifecycleError("CORRUPT_LIFECYCLE_STATE", f"snapshot exists: {destination}")
    partial = destination.with_name(destination.name + ".partial")
    if partial.exists():
        partial.unlink()
    source_conn = sqlite3.connect(source.resolve().as_uri() + "?mode=ro", uri=True)
    source_conn.execute("PRAGMA query_only=ON")
    destination_conn = sqlite3.connect(partial)
    try:
        source_conn.backup(destination_conn)
        destination_conn.commit()
    finally:
        destination_conn.close()
        source_conn.close()
    if _sqlite_quick_check(partial, readonly=True) != "ok":
        partial.unlink(missing_ok=True)
        raise LifecycleError("CORRUPT_LIFECYCLE_STATE", "snapshot quick_check failed")
    os.replace(partial, destination)


def sqlite_logical_sha256(path: Path) -> str:
    """Hash ordered schema and row values, independent of SQLite page layout."""
    conn = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    try:
        tables = [
            str(row[0])
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%' ORDER BY name"
            ).fetchall()
        ]
        payload: dict[str, Any] = {}
        for table in tables:
            quoted = '"' + table.replace('"', '""') + '"'
            columns = [str(row[1]) for row in conn.execute(f"PRAGMA table_info({quoted})")]
            try:
                rows = conn.execute(f"SELECT * FROM {quoted} ORDER BY rowid").fetchall()
            except sqlite3.DatabaseError:
                order = ",".join('"' + item.replace('"', '""') + '"' for item in columns)
                rows = conn.execute(f"SELECT * FROM {quoted} ORDER BY {order}").fetchall()
            payload[table] = {
                "columns": columns,
                "rows": [
                    [_json_primitive(row[column]) for column in columns]
                    for row in rows
                ],
            }
        return canonical_sha256(payload)
    finally:
        conn.close()


def _json_primitive(value: Any) -> Any:
    if isinstance(value, bytes):
        return {"sqlite_blob_hex": value.hex()}
    if value is None or isinstance(value, (str, int, float)):
        return value
    return str(value)


def h1_eligible_count(paper_path: Path, start: datetime, end: datetime) -> int:
    if not paper_path.exists():
        return 0
    conn = sqlite3.connect(paper_path.resolve().as_uri() + "?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    try:
        rows = conn.execute(
            "SELECT DISTINCT c.signal_key,r.signal_observed_at FROM "
            "paper_continuous_signal_contexts_v0_1 c JOIN paper_entry_routes r "
            "ON r.route_id=c.route_id WHERE r.state='FILLED'"
        ).fetchall()
    except sqlite3.DatabaseError:
        return 0
    finally:
        conn.close()
    count = 0
    for row in rows:
        observed = dt_parse(str(row["signal_observed_at"]))
        if start <= observed < end and 0 <= observed.hour < 6:
            count += 1
    return count


def paper_operational_counts(paper_path: Path) -> tuple[int, int]:
    if not paper_path.exists():
        return 0, 0
    conn = sqlite3.connect(paper_path.resolve().as_uri() + "?mode=ro", uri=True)
    conn.execute("PRAGMA query_only=ON")
    try:
        source_rows = int(conn.execute(
            "SELECT COUNT(*) FROM paper_fp_binding_inputs_v0_1 "
            "WHERE production_p1_rowid IS NOT NULL"
        ).fetchone()[0])
        candidates = int(conn.execute(
            "SELECT COUNT(*) FROM paper_continuous_signal_contexts_v0_1"
        ).fetchone()[0])
        return source_rows, candidates
    finally:
        conn.close()


def source_coverage_probe(
    source_path: Path,
    *,
    start: datetime,
    target: datetime,
    source_start_cursor: int = 0,
) -> tuple[datetime, list[str]]:
    from phase4.paper_continuous_market_source_v0_2 import ContinuousMarketSourceV02

    conn = ContinuousMarketSourceV02.open_readonly(source_path)
    try:
        ContinuousMarketSourceV02.validate_schema(conn)
        activity = sorted({
            dt_parse(str(row[0]))
            for row in conn.execute(
                "SELECT inserted_at_utc FROM pump_events WHERE rowid>? "
                "AND inserted_at_utc IS NOT NULL",
                (int(source_start_cursor),),
            ).fetchall()
        })
        # Rows captured after the arming anchor but before the scientific
        # boundary remain available as causal context.  They must never move
        # reported scientific coverage behind the frozen start.
        latest = start if not activity else max(start, activity[-1])
        tables = {str(item[0]) for item in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
        gaps: list[str] = []
        gap_rows: list[tuple[str, str, datetime, datetime]] = []
        if "gap_jobs_v034" not in tables:
            gaps.append("GAP_LEDGER_UNAVAILABLE")
        else:
            for gap in conn.execute(
                "SELECT gap_key,status,gap_started_at_utc,gap_ended_at_utc "
                "FROM gap_jobs_v034 ORDER BY gap_started_at_utc,gap_key"
            ).fetchall():
                gap_start = dt_parse(str(gap[2]))
                gap_end = dt_parse(str(gap[3]))
                gap_rows.append((str(gap[0]), str(gap[1]), gap_start, gap_end))
                if gap_end >= start and gap_start < target and not str(gap[1]).startswith("DONE"):
                    gaps.append(f"{gap[0]}:{gap[1]}")
        prior = start
        for observed in (item for item in activity if start < item <= target):
            if (observed - prior).total_seconds() >= SOURCE_STALL_FAIL_SECONDS:
                proven = any(
                    status.startswith("DONE")
                    and gap_start <= prior + timedelta(seconds=SOURCE_STALL_FAIL_SECONDS)
                    and gap_end >= observed - timedelta(seconds=SOURCE_STALL_FAIL_SECONDS)
                    for _key, status, gap_start, gap_end in gap_rows
                )
                if not proven:
                    gaps.append(
                        "UNPROVEN_SOURCE_INTERVAL:"
                        f"{dt_text(prior)}..{dt_text(observed)}"
                    )
            prior = observed
        final_activity = min(latest, target)
        if (target - final_activity).total_seconds() >= SOURCE_STALL_FAIL_SECONDS:
            proven = any(
                status.startswith("DONE")
                and gap_start <= final_activity + timedelta(seconds=SOURCE_STALL_FAIL_SECONDS)
                and gap_end >= target
                for _key, status, gap_start, gap_end in gap_rows
            )
            if not proven:
                gaps.append(
                    "UNPROVEN_SOURCE_INTERVAL:"
                    f"{dt_text(final_activity)}..{dt_text(target)}"
                )
        return min(latest, target), gaps
    finally:
        conn.close()


def prestart_wait_seconds(
    *, now: datetime, start: datetime, poll_seconds: float
) -> float:
    """Return one bounded, non-busy wait without changing frozen bounds."""
    instant = _utc(now)
    boundary = _utc(start)
    if instant >= boundary:
        return 0.0
    requested = min(
        MAXIMUM_PRESTART_POLL_SECONDS,
        max(MINIMUM_PRESTART_POLL_SECONDS, float(poll_seconds)),
    )
    return min(requested, (boundary - instant).total_seconds())


def graceful_timer_drain(binding: Any, *, instant: datetime, timer_id: str) -> int:
    key = binding.prepare_clock_tick(_utc(instant), timer_id=timer_id)
    row = binding.conn.execute(
        "SELECT source_watermark_p1_rowid FROM paper_fp_binding_prepared_timers_v0_1 WHERE timer_key=?",
        (key,),
    ).fetchone()
    if row is None:
        raise LifecycleError("CORRUPT_LIFECYCLE_STATE", "prepared timer missing")
    watermark = int(row[0])
    while binding.durable_p1_rowid < watermark:
        batch = binding.process_next_batch(batch_size=1_000)
        if batch.raw_rows_fetched == 0:
            time.sleep(0.10)
    result = binding.execute_prepared_timer(key)
    if result.status != "COMMITTED" or binding.durable_p1_rowid != watermark:
        raise LifecycleError("CORRUPT_LIFECYCLE_STATE", "prepared timer did not commit at watermark")
    if binding.pending_outbox_count() != 0:
        raise LifecycleError("CORRUPT_LIFECYCLE_STATE", "semantic outbox remains pending")
    return watermark


def run_worker(
    manager: FrozenOOSLifecycleV01,
    run_id: str,
    *,
    poll_seconds: float = 0.25,
    clock_seconds: float = 0.5,
) -> int:
    """Run one Phase-4 segment. Real collection is not invoked by self-tests."""
    from phase4.paper_continuous_firstpullback_binding_v0_5 import (
        ContinuousFirstPullbackBindingV05,
        connect_atomic_entry_router_db,
    )
    from phase4.paper_continuous_market_source_v0_2 import ContinuousMarketSourceV02

    manifest, state = manager.verify_identity(run_id)
    token = process_birth_token(os.getpid())
    if token is None:
        raise LifecycleError("CORRUPT_LIFECYCLE_STATE", "process birth token unavailable")
    active = state.get("active_process")
    registration_deadline = time.monotonic() + 3.0
    while (
        (not active or int(active.get("pid", -1)) != os.getpid())
        and time.monotonic() < registration_deadline
    ):
        time.sleep(0.02)
        state = manager._state(run_id)
        active = state.get("active_process")
    if not active or int(active["pid"]) != os.getpid() or active["birth_token"] != token:
        raise LifecycleError("ACTIVE_WRITER_CONFLICT", "worker is not the registered segment")
    source_path = _resolved(manifest["source_db_path"])
    paper_path = _resolved(manifest["paper_db_path"])
    source = ContinuousMarketSourceV02(
        source_path,
        start_after_p1_rowid=int(manifest["source_start_cursor"]),
        database_identity=source_path.as_posix(),
    )
    conn = connect_atomic_entry_router_db(paper_path)
    binding = ContinuousFirstPullbackBindingV05(conn, source)
    stop_path = manager.run_dir(run_id) / "stop.request"
    next_clock = time.monotonic()
    clock_index = 0
    termination = "STOP_REQUESTED"
    error: str | None = None
    try:
        while True:
            state = manager._state(run_id)
            now = _utc(manager.clock())
            start = dt_parse(manifest["start_at_utc"])
            target = dt_parse(state["target_end_at_utc"])
            stop_requested = stop_path.exists()
            if now < start:
                if stop_requested:
                    termination = "STOP_REQUESTED_WHILE_ARMED"
                    break
                time.sleep(
                    prestart_wait_seconds(
                        now=now, start=start, poll_seconds=poll_seconds
                    )
                )
                continue
            coverage, gaps = source_coverage_probe(
                source_path,
                start=start,
                target=target,
                source_start_cursor=int(manifest["source_start_cursor"]),
            )
            if now >= target and (coverage < target or gaps):
                # Catch up durable source rows without advancing any strategy or
                # exit timer beyond the frozen boundary.
                binding.process_next_batch(batch_size=1_000)
                source_rows, candidates = paper_operational_counts(paper_path)
                manager.record_progress(
                    run_id,
                    source_cursor=binding.durable_p1_rowid,
                    source_watermark=max(binding.durable_p1_rowid, source.latest_p1_rowid()),
                    source_row_count=source_rows,
                    candidate_count=candidates,
                    h1_eligible_sample_count=h1_eligible_count(
                        paper_path, dt_parse(manifest["start_at_utc"]), target
                    ),
                    coverage_complete_through=coverage,
                    unresolved_source_gaps=gaps,
                    durable_work_pending=binding.pending_outbox_count() != 0,
                    process_token=token,
                )
                if stop_requested:
                    watermark = graceful_timer_drain(
                        binding,
                        instant=target,
                        timer_id=(
                            f"P6-OOS:{run_id}:CATCHUP-STOP:"
                            f"{len(state['segments']):04d}:{dt_text(target)}"
                        ),
                    )
                    manager.record_progress(
                        run_id,
                        source_cursor=binding.durable_p1_rowid,
                        source_watermark=watermark,
                        source_row_count=source_rows,
                        candidate_count=candidates,
                        h1_eligible_sample_count=h1_eligible_count(
                            paper_path, dt_parse(manifest["start_at_utc"]), target
                        ),
                        coverage_complete_through=coverage,
                        unresolved_source_gaps=gaps,
                        durable_work_pending=False,
                        process_token=token,
                    )
                    termination = "STOP_REQUESTED_DURING_CATCHUP"
                    break
                time.sleep(max(0.0, poll_seconds))
                continue
            if stop_requested or now >= target:
                instant = min(now, target)
                watermark = graceful_timer_drain(
                    binding,
                    instant=instant,
                    timer_id=f"P6-OOS:{run_id}:SEGMENT-END:{len(state['segments']):04d}:{dt_text(instant)}",
                )
                coverage, gaps = source_coverage_probe(
                    source_path,
                    start=dt_parse(manifest["start_at_utc"]),
                    target=target,
                    source_start_cursor=int(manifest["source_start_cursor"]),
                )
                source_rows, candidates = paper_operational_counts(paper_path)
                h1_count = h1_eligible_count(paper_path, dt_parse(manifest["start_at_utc"]), target)
                manager.record_progress(
                    run_id, source_cursor=binding.durable_p1_rowid,
                    source_watermark=watermark, source_row_count=source_rows,
                    candidate_count=candidates, h1_eligible_sample_count=h1_count,
                    coverage_complete_through=coverage,
                    unresolved_source_gaps=gaps, durable_work_pending=False,
                    process_token=token,
                )
                termination = "TARGET_BOUNDARY_DRAINED" if now >= target else "STOP_REQUESTED"
                break
            latest = source.latest_p1_rowid()
            if time.monotonic() >= next_clock:
                graceful_timer_drain(
                    binding, instant=now,
                    timer_id=f"P6-OOS:{run_id}:CLOCK:{len(state['segments']):04d}:{clock_index:012d}:{dt_text(now)}",
                )
                clock_index += 1
                next_clock = time.monotonic() + clock_seconds
            else:
                binding.process_next_batch(batch_size=1_000)
            source_rows, candidates = paper_operational_counts(paper_path)
            coverage, gaps = source_coverage_probe(
                source_path,
                start=dt_parse(manifest["start_at_utc"]),
                target=target,
                source_start_cursor=int(manifest["source_start_cursor"]),
            )
            manager.record_progress(
                run_id, source_cursor=binding.durable_p1_rowid,
                source_watermark=max(binding.durable_p1_rowid, latest),
                source_row_count=source_rows, candidate_count=candidates,
                h1_eligible_sample_count=h1_eligible_count(paper_path, dt_parse(manifest["start_at_utc"]), target),
                coverage_complete_through=coverage, unresolved_source_gaps=gaps,
                durable_work_pending=binding.pending_outbox_count() != 0,
                process_token=token,
            )
            time.sleep(max(0.0, poll_seconds))
    except BaseException as exc:
        error = f"{type(exc).__name__}:{exc}"
        termination = "FAIL_CLOSED"
    finally:
        conn.close()
        try:
            stop_path.unlink()
        except FileNotFoundError:
            pass
        manager.end_segment(run_id, termination=termination, error=error)
    return 0 if error is None else 1


def launch_worker_process(
    manager: FrozenOOSLifecycleV01,
    run_id: str,
    *,
    cli_path: Path,
    python_path: Path | None = None,
) -> dict[str, Any]:
    manager.verify_identity(run_id)
    state = manager.reconcile_process(run_id)
    if state.get("active_process") is not None:
        raise LifecycleError("ACTIVE_WRITER_CONFLICT")
    stale_stop = manager.run_dir(run_id) / "stop.request"
    if stale_stop.exists():
        stale_stop.unlink()
    claim_token = secrets.token_urlsafe(32)
    pending = manager.prepare_segment_launch(run_id, claim_token=claim_token)
    segment_id = str(pending["segment_id"])
    log_path = manager.run_dir(run_id) / f"segment_{int(pending['segment_index']):04d}.log"
    handle = log_path.open("a", encoding="utf-8", newline="\n")
    creationflags = 0
    if os.name == "nt":
        creationflags = int(getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)) | int(getattr(subprocess, "DETACHED_PROCESS", 0))
    try:
        process = subprocess.Popen(
            (
                str(python_path or Path(sys.executable)), str(cli_path), "_worker",
                "--run-id", run_id, "--runtime-root", str(manager.runtime_root),
                "--segment-id", segment_id, "--claim-token", claim_token,
            ),
            cwd=manager.repo_root, stdin=subprocess.DEVNULL, stdout=handle,
            stderr=subprocess.STDOUT, text=True, creationflags=creationflags,
            close_fds=True,
        )
    except BaseException as exc:
        manager.fail_segment_startup(
            run_id, segment_id=segment_id, claim_token=claim_token,
            reason=f"{type(exc).__name__}:{exc}",
        )
        raise
    finally:
        handle.close()
    manager.record_launcher_pid(
        run_id, segment_id=segment_id, launcher_pid=process.pid
    )
    deadline = time.monotonic() + WORKER_CLAIM_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        observed = manager._state(run_id)
        active = observed.get("active_process")
        if (
            active
            and active.get("segment_id") == segment_id
            and active.get("ownership_state") == "OWNED"
            and process_matches(active.get("pid"), active.get("birth_token"))
        ):
            matches = [
                item for item in observed["segments"]
                if item["segment_id"] == segment_id
            ]
            if len(matches) != 1:
                raise LifecycleError("CORRUPT_LIFECYCLE_STATE", "claimed segment missing")
            return dict(matches[0])
        if observed.get("active_process") is None:
            raise LifecycleError(
                "WORKER_STARTUP_FAILED",
                str(observed.get("last_error") or "worker exited before ownership claim"),
            )
        time.sleep(0.02)
    # One final authoritative state read closes the boundary race where the
    # actual worker commits its claim as the parent's monotonic deadline fires.
    observed = manager._state(run_id)
    active = observed.get("active_process")
    if (
        active
        and active.get("segment_id") == segment_id
        and active.get("ownership_state") == "OWNED"
        and process_matches(active.get("pid"), active.get("birth_token"))
    ):
        matches = [
            item for item in observed["segments"]
            if item["segment_id"] == segment_id
        ]
        if len(matches) == 1:
            return dict(matches[0])
    failed = manager.fail_segment_startup(
        run_id, segment_id=segment_id, claim_token=claim_token,
        reason="WORKER_CLAIM_TIMEOUT",
    )
    active = failed.get("active_process")
    if (
        active
        and active.get("segment_id") == segment_id
        and active.get("ownership_state") == "OWNED"
        and process_matches(active.get("pid"), active.get("birth_token"))
    ):
        matches = [
            item for item in failed["segments"]
            if item["segment_id"] == segment_id
        ]
        if len(matches) == 1:
            return dict(matches[0])
    if failed.get("active_process") is None and process.poll() is None:
        process.terminate()
    raise LifecycleError("WORKER_STARTUP_FAILED", "actual worker did not claim segment")


def request_stop(manager: FrozenOOSLifecycleV01, run_id: str) -> dict[str, Any]:
    manager.verify_identity(run_id)
    state = manager.reconcile_process(run_id)
    active = state.get("active_process")
    if active is None:
        return state
    if active.get("ownership_state") == "CLAIM_PENDING":
        stop_path = manager.run_dir(run_id) / "stop.request"
        stop_path.write_text("GRACEFUL_STOP_REQUESTED\n", encoding="ascii")
        return manager._state(run_id)
    if not process_matches(active.get("pid"), active.get("birth_token")):
        return manager.reconcile_process(run_id)
    stop_path = manager.run_dir(run_id) / "stop.request"
    stop_path.write_text("GRACEFUL_STOP_REQUESTED\n", encoding="ascii")
    return manager._state(run_id)


def protocol_review_payload() -> dict[str, Any]:
    return {
        "model_id": MODEL_ID,
        "schema_version": SCHEMA_VERSION,
        "model_fingerprint": MODEL_FINGERPRINT,
        "protocol_id": PROTOCOL_ID,
        "protocol_fingerprint": P6_PROTOCOL_FINGERPRINT,
        "policy_set_sha256": P6_POLICY_SET_SHA256,
        "minimum_duration_seconds": MINIMUM_DURATION_SECONDS,
        "extension_seconds": EXTENSION_SECONDS,
        "maximum_duration_seconds": MAXIMUM_DURATION_SECONDS,
        "default_runtime_root": str(DEFAULT_RUNTIME_ROOT),
        "status_fields": list(NO_PEEK_STATUS_FIELDS),
        "runtime_file_sha256": dict(RUNTIME_FILE_SHA256),
    }
