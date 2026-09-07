from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from phase6.frozen_oos_collection_lifecycle_v0_1 import (  # noqa: E402
    DEFAULT_RUNTIME_ROOT,
    DEFAULT_SOURCE_DB,
    FrozenOOSLifecycleV01,
    LifecycleError,
    StartRequest,
    dt_text,
    git_head,
    git_status,
    launch_worker_process,
    process_birth_token,
    request_stop,
    run_worker,
    source_anchor,
)


def _utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError("timestamp must be timezone-aware")
    return parsed.astimezone(timezone.utc)


def _run_id(start: datetime, source_cursor: int) -> str:
    seed = f"{dt_text(start)}:{source_cursor}:{git_head(PROJECT_ROOT)}"
    suffix = hashlib.sha256(seed.encode("utf-8")).hexdigest()[:12]
    return f"P6-OOS-{start.strftime('%Y%m%dT%H%M%SZ')}-{suffix}"


def _manager(args: argparse.Namespace) -> FrozenOOSLifecycleV01:
    return FrozenOOSLifecycleV01(PROJECT_ROOT, args.runtime_root)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Restart-safe no-peek lifecycle for one frozen Phase-6 OOS collection."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    start = subparsers.add_parser("start", help="Create and launch one new frozen OOS run.")
    start.add_argument("--source-db", type=Path, default=DEFAULT_SOURCE_DB)
    start.add_argument("--runtime-root", type=Path, default=DEFAULT_RUNTIME_ROOT)
    start.add_argument("--run-id")
    start.add_argument("--paper-db", type=Path)
    start.add_argument(
        "--start-at",
        type=_utc,
        required=True,
        help=(
            "Exact timezone-aware scientific boundary (for example "
            "2026-09-07T00:00:00Z); no implicit wall-clock start is allowed."
        ),
    )

    for name, help_text in (
        ("status", "Show the no-peek operational status."),
        ("salvage-check", "Read-only eligibility proof for the known failed T002A run."),
        ("salvage-migrate", "Persist the authorized immutable ownership migration."),
        ("stop", "Request graceful stop of the owned runtime segment."),
        ("resume", "Resume the exact persisted OOS run in a new segment."),
        ("finalize", "Freeze exact inputs for later P6-T001 evaluation."),
    ):
        command = subparsers.add_parser(name, help=help_text)
        command.add_argument("--run-id", required=True)
        command.add_argument("--runtime-root", type=Path, default=DEFAULT_RUNTIME_ROOT)

    extend = subparsers.add_parser("extend", help="Apply one predeclared 24-hour extension.")
    extend.add_argument("--run-id", required=True)
    extend.add_argument("--runtime-root", type=Path, default=DEFAULT_RUNTIME_ROOT)
    extend.add_argument("--hours", type=int, required=True)
    extend.add_argument(
        "--reason", required=True,
        choices=("H1_SAMPLE_INSUFFICIENT", "TECHNICAL_COMPLETENESS"),
    )

    worker = subparsers.add_parser("_worker", help=argparse.SUPPRESS)
    worker.add_argument("--run-id", required=True)
    worker.add_argument("--runtime-root", type=Path, required=True)
    worker.add_argument("--segment-id", required=True)
    worker.add_argument("--claim-token", required=True)
    return parser


def _print(payload: object) -> None:
    print(json.dumps(payload, sort_keys=True, indent=2))


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        manager = _manager(args)
        if args.command == "_worker":
            pid = os.getpid()
            token = process_birth_token(pid)
            if token is None:
                raise LifecycleError(
                    "WORKER_STARTUP_FAILED", "actual worker birth token unavailable"
                )
            try:
                manager.claim_segment(
                    args.run_id,
                    segment_id=args.segment_id,
                    claim_token=args.claim_token,
                    pid=pid,
                    birth_token=token,
                )
                return run_worker(manager, args.run_id)
            except BaseException as exc:
                manager.fail_segment_startup(
                    args.run_id,
                    segment_id=args.segment_id,
                    claim_token=args.claim_token,
                    pid=pid,
                    birth_token=token,
                    reason=f"{type(exc).__name__}:{exc}",
                )
                raise

        if args.command == "start":
            if git_status(PROJECT_ROOT).strip():
                raise LifecycleError(
                    "REPOSITORY_RUNTIME_MISMATCH",
                    "real OOS START requires a clean working tree",
                )
            anchor = source_anchor(args.source_db)
            start = args.start_at
            run_id = args.run_id or _run_id(start, anchor)
            manifest = manager.start(StartRequest(
                source_db_path=args.source_db,
                runtime_root=args.runtime_root,
                repository_commit=git_head(PROJECT_ROOT),
                start_at=start,
                source_start_cursor=anchor,
                run_id=run_id,
                paper_db_path=args.paper_db,
            ))
            segment = launch_worker_process(
                manager, run_id, cli_path=Path(__file__).resolve(),
            )
            _print({"manifest": manifest, "segment": segment})
            return 0

        if args.command == "status":
            _print(manager.status(args.run_id))
            return 0

        if args.command == "salvage-check":
            result = manager.salvage_check(args.run_id)
            _print(result)
            return 0 if result["result"] == "SALVAGE_AUTHORIZED" else 2

        if args.command == "salvage-migrate":
            _print(manager.migrate_salvage(args.run_id))
            return 0

        if args.command == "stop":
            state = request_stop(manager, args.run_id)
            _print({
                "run_id": args.run_id,
                "stop_requested": state.get("active_process") is not None,
                "lifecycle_state": state["lifecycle_state"],
            })
            return 0

        if args.command == "resume":
            _manifest, state = manager.verify_identity(args.run_id)
            state = manager.reconcile_process(args.run_id)
            if state["lifecycle_state"] in {"INVALID", "FROZEN_READY_FOR_EVALUATION"}:
                raise LifecycleError("CORRUPT_LIFECYCLE_STATE", "run is terminal")
            segment = launch_worker_process(
                manager, args.run_id, cli_path=Path(__file__).resolve(),
            )
            _print(segment)
            return 0

        if args.command == "extend":
            state = manager.extend(
                args.run_id, seconds=args.hours * 3600, reason=args.reason,
            )
            _print({
                "run_id": args.run_id,
                "target_end_at_utc": state["target_end_at_utc"],
                "duration_seconds": state["duration_seconds"],
                "extension_history": state["extension_history"],
            })
            return 0

        if args.command == "finalize":
            _print(manager.finalize(args.run_id))
            return 0
        raise AssertionError(args.command)
    except (LifecycleError, FileNotFoundError, ValueError) as exc:
        reason = exc.reason if isinstance(exc, LifecycleError) else type(exc).__name__
        _print({"result": "FAIL_CLOSED", "reason": reason, "detail": str(exc)})
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
