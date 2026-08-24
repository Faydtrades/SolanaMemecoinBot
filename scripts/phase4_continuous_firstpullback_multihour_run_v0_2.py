from __future__ import annotations

import hashlib
import json
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Sequence


PROJECT_ROOT = Path(__file__).resolve().parents[1]
for import_root in (PROJECT_ROOT / "src", PROJECT_ROOT / "scripts"):
    if str(import_root) not in sys.path:
        sys.path.insert(0, str(import_root))

import phase4_continuous_firstpullback_multihour_run_v0_1 as accepted  # noqa: E402
from phase4.paper_continuous_firstpullback_binding_v0_2 import (  # noqa: E402
    MODEL_FINGERPRINT as BINDING_FINGERPRINT,
    MODEL_ID as BINDING_MODEL_ID,
    ContinuousFirstPullbackBindingV02,
)
from phase4.paper_continuous_runner_v0_2 import (  # noqa: E402
    RUNNER_SPEC_FINGERPRINT,
)
from phase4.paper_entry_execution_deadline_v0_1 import (  # noqa: E402
    ELIGIBILITY_WINDOW_US,
    MODEL_FINGERPRINT as DEADLINE_POLICY_FINGERPRINT,
    MODEL_ID as DEADLINE_POLICY_MODEL_ID,
)
from phase4.paper_exit_orchestrator_v0_1 import (  # noqa: E402
    LOCKED_EXIT_SPEC_FINGERPRINT,
)


MODEL_ID = "P4-CONTINUOUS-FIRSTPULLBACK-MULTIHOUR-RUN-0002"
ACCEPTED_V01_HARNESS_SHA256 = (
    "948db8f791f21242a49c85f3b6f8c70c1736db36ffa6c40367fd97a80fff2ddf"
)
MULTIHOUR_SPEC = json.loads(json.dumps(accepted.MULTIHOUR_SPEC))
MULTIHOUR_SPEC.update({
    "model_id": MODEL_ID,
    "accepted_multihour_v0_1_sha256": ACCEPTED_V01_HARNESS_SHA256,
    "binding_model_id": BINDING_MODEL_ID,
    "binding_fingerprint": BINDING_FINGERPRINT,
    "continuous_runner_fingerprint": RUNNER_SPEC_FINGERPRINT,
    "entry_execution_deadline_model_id": DEADLINE_POLICY_MODEL_ID,
    "entry_execution_deadline_fingerprint": DEADLINE_POLICY_FINGERPRINT,
    "entry_execution_deadline_policy": {
        "FINAL-A": "SIGNAL_PLUS_15_SECONDS_INCLUSIVE",
        "FINAL-B": "SIGNAL_PLUS_15_SECONDS_INCLUSIVE",
        "SENS-C": "SIGNAL_PLUS_5_SECONDS_INCLUSIVE",
        "expiry": "DEADLINE_PLUS_1_MICROSECOND",
        "track_independent": True,
    },
    "locked_exit_spec_fingerprint": LOCKED_EXIT_SPEC_FINGERPRINT,
    "graceful_end": (
        "FREEZE_END_WATERMARK_DRAIN_EXECUTE_OUTBOX_CLOSE_NO_FABRICATION_"
        "DEADLINES_AFTER_BOUNDARY_REMAIN_PENDING"
    ),
})
MODEL_FINGERPRINT = hashlib.sha256(
    json.dumps(MULTIHOUR_SPEC, sort_keys=True, separators=(",", ":")).encode("utf-8")
).hexdigest()

DEFAULT_DURATION_SECONDS = accepted.DEFAULT_DURATION_SECONDS
MIN_DURATION_SECONDS = accepted.MIN_DURATION_SECONDS
MAX_DURATION_SECONDS = accepted.MAX_DURATION_SECONDS
PRODUCTION_DB = accepted.PRODUCTION_DB
RUNTIME_DIR = accepted.RUNTIME_DIR
parse_duration_seconds = accepted.parse_duration_seconds
run_control_reason = accepted.run_control_reason
SourceContinuityWatchdog = accepted.SourceContinuityWatchdog
ProductionSourceStalled = accepted.ProductionSourceStalled
finalize_at_watermark = accepted.finalize_at_watermark
preserve_source_stall_failure_state = accepted.preserve_source_stall_failure_state


def entry_deadline_diagnostics(conn: sqlite3.Connection) -> dict[str, Any]:
    state_counts = {
        str(row[0]): int(row[1])
        for row in conn.execute(
            "SELECT state,COUNT(*) FROM paper_entry_execution_deadlines_v0_1 "
            "GROUP BY state ORDER BY state"
        ).fetchall()
    }
    filled_per_track = {
        str(row[0]): int(row[1])
        for row in conn.execute(
            "SELECT track_id,COUNT(*) FROM paper_entry_execution_deadlines_v0_1 "
            "WHERE state='FILLED' GROUP BY track_id ORDER BY track_id"
        ).fetchall()
    }
    expired_per_track = {
        str(row[0]): int(row[1])
        for row in conn.execute(
            "SELECT track_id,COUNT(*) FROM paper_entry_execution_deadlines_v0_1 "
            "WHERE reason='ENTRY_EXECUTION_DEADLINE_EXPIRED' "
            "GROUP BY track_id ORDER BY track_id"
        ).fetchall()
    }
    return {
        "expiry_events": int(conn.execute(
            "SELECT COUNT(*) FROM paper_entry_execution_deadlines_v0_1 "
            "WHERE reason='ENTRY_EXECUTION_DEADLINE_EXPIRED'"
        ).fetchone()[0]),
        "execution_expired_by_track": expired_per_track,
        "partial_track_candidates": int(conn.execute(
            "SELECT COUNT(*) FROM (SELECT route_id FROM paper_entry_execution_deadlines_v0_1 "
            "GROUP BY route_id HAVING SUM(state='FILLED')>0 AND SUM(state='REJECTED')>0)"
        ).fetchone()[0]),
        "all_expired_candidates": int(conn.execute(
            "SELECT COUNT(*) FROM paper_entry_routes WHERE state='REJECTED' "
            "AND state_reason='ALL_TRACK_ENTRY_EXECUTION_DEADLINES_EXPIRED'"
        ).fetchone()[0]),
        "pending_deadlines_at_end": state_counts.get("PENDING", 0),
        "filled_tracks": filled_per_track,
        "track_state_counts": state_counts,
    }


_ORIGINAL_PAPER_SUMMARY = accepted.smoke._paper_summary
_ORIGINAL_INTEGRITY_AUDIT = accepted.smoke._integrity_audit
_ORIGINAL_BUILD_TRACK_METRICS = accepted.build_session_track_metrics


def _paper_summary_v02(conn: sqlite3.Connection) -> dict[str, Any]:
    summary = _ORIGINAL_PAPER_SUMMARY(conn)
    summary["entry_execution_deadlines"] = entry_deadline_diagnostics(conn)
    return summary


def _integrity_audit_v02(paper_path: Path, *, source, final_cursor: int) -> dict[str, Any]:
    checks = _ORIGINAL_INTEGRITY_AUDIT(
        paper_path, source=source, final_cursor=final_cursor
    )
    conn = sqlite3.connect(paper_path)
    conn.row_factory = sqlite3.Row
    try:
        runtime = conn.execute(
            "SELECT * FROM paper_fp_binding_runtime_v0_1 WHERE singleton=1"
        ).fetchone()
        runner = conn.execute(
            "SELECT * FROM paper_continuous_runtime_state_v0_2 WHERE singleton=1"
        ).fetchone()
        deadline_meta = conn.execute(
            "SELECT * FROM paper_entry_execution_deadline_meta_v0_1 WHERE singleton=1"
        ).fetchone()
        deadline_invariants = int(conn.execute(
            """
            SELECT COUNT(*) FROM paper_entry_execution_deadlines_v0_1 d
            LEFT JOIN paper_orders o ON o.paper_order_id=d.paper_order_id
            LEFT JOIN paper_entry_routes r ON r.route_id=d.route_id
            WHERE o.paper_order_id IS NULL OR o.signal_key<>d.candidate_id
               OR r.route_id IS NULL
               OR o.track_id<>d.track_id
               OR (d.state='FILLED' AND o.state<>'FILLED')
               OR (d.state='REJECTED' AND o.state<>'REJECTED')
               OR (d.state='PENDING' AND o.state<>'ENTRY_PENDING')
               OR (d.state='FILLED' AND NOT EXISTS(
                   SELECT 1 FROM paper_positions p
                   WHERE p.paper_order_id=d.paper_order_id))
               OR (d.state='REJECTED' AND EXISTS(
                   SELECT 1 FROM paper_positions p
                   WHERE p.paper_order_id=d.paper_order_id))
               OR (d.state='FILLED' AND
                   (d.route_total_entry_explicit_cost_lamports IS NULL OR
                    d.route_total_entry_explicit_cost_lamports IS NOT
                    r.total_entry_explicit_cost_lamports))
               OR (d.state<>'FILLED' AND
                   d.route_total_entry_explicit_cost_lamports IS NOT NULL)
            """
        ).fetchone()[0]) == 0
        route_cardinality = int(conn.execute(
            "SELECT COUNT(*) FROM (SELECT r.route_id "
            "FROM paper_entry_routes r "
            "LEFT JOIN paper_entry_execution_deadlines_v0_1 d ON d.route_id=r.route_id "
            "GROUP BY r.route_id "
            "HAVING COUNT(d.track_id)<>3 OR COUNT(DISTINCT d.track_id)<>3)"
        ).fetchone()[0]) == 0
        deadline_rows = conn.execute(
            "SELECT d.*,r.signal_observed_at AS route_signal_observed_at "
            "FROM paper_entry_execution_deadlines_v0_1 d "
            "JOIN paper_entry_routes r ON r.route_id=d.route_id"
        ).fetchall()
        deadline_formula_exact = True
        for row in deadline_rows:
            signal = datetime.fromisoformat(str(row["signal_observed_at"])).astimezone(
                timezone.utc
            )
            eligible = datetime.fromisoformat(
                str(row["eligible_through_at"])
            ).astimezone(timezone.utc)
            expire_at = datetime.fromisoformat(str(row["expire_at"])).astimezone(
                timezone.utc
            )
            deadline_formula_exact = deadline_formula_exact and (
                str(row["signal_observed_at"]) == str(row["route_signal_observed_at"])
                and eligible
                == signal
                + timedelta(microseconds=ELIGIBILITY_WINDOW_US[str(row["track_id"])])
                and expire_at == eligible + timedelta(microseconds=1)
            )
        route_terminal_consistent = int(conn.execute(
            """
            SELECT COUNT(*) FROM (
                SELECT r.route_id,r.state,r.state_reason,
                       SUM(d.state='FILLED') AS fills,
                       SUM(d.state='PENDING') AS pending
                FROM paper_entry_routes r
                JOIN paper_entry_execution_deadlines_v0_1 d ON d.route_id=r.route_id
                GROUP BY r.route_id
            ) AS summary
            WHERE (state='FILLED' AND (fills=0 OR pending<>0))
               OR (state='REJECTED' AND (fills<>0 OR pending<>0))
               OR (state='ENTRY_PENDING' AND pending=0)
               OR (state='REJECTED'
                   AND state_reason='ALL_TRACK_ENTRY_EXECUTION_DEADLINES_EXPIRED'
                   AND EXISTS(
                       SELECT 1 FROM paper_entry_execution_deadlines_v0_1 x
                       WHERE x.route_id=summary.route_id
                         AND x.reason<>'ENTRY_EXECUTION_DEADLINE_EXPIRED'
                   ))
            """
        ).fetchone()[0]) == 0
        expired_position_count = int(conn.execute(
            """
            SELECT COUNT(*) FROM paper_entry_execution_deadlines_v0_1 d
            LEFT JOIN paper_positions p ON p.paper_order_id=d.paper_order_id
            WHERE d.reason='ENTRY_EXECUTION_DEADLINE_EXPIRED'
              AND p.paper_position_id IS NOT NULL
            """
        ).fetchone()[0])
        expired_route_cost_count = int(conn.execute(
            "SELECT COUNT(*) FROM paper_entry_routes "
            "WHERE state_reason='ALL_TRACK_ENTRY_EXECUTION_DEADLINES_EXPIRED' "
            "AND total_entry_explicit_cost_lamports IS NOT NULL"
        ).fetchone()[0])
        expired_cost_free = expired_position_count == 0 and expired_route_cost_count == 0
        checks["binding_fingerprint_exact"] = (
            runtime is not None
            and str(runtime["binding_model_id"]) == BINDING_MODEL_ID
            and str(runtime["binding_fingerprint"]) == BINDING_FINGERPRINT
        )
        checks["runner_v02_identity_exact"] = (
            runner is not None
            and str(runner["runner_spec_fingerprint"]) == RUNNER_SPEC_FINGERPRINT
            and str(runner["entry_deadline_fingerprint"]) == DEADLINE_POLICY_FINGERPRINT
        )
        checks["entry_deadline_identity_exact"] = (
            deadline_meta is not None
            and str(deadline_meta["policy_model_id"]) == DEADLINE_POLICY_MODEL_ID
            and str(deadline_meta["policy_fingerprint"]) == DEADLINE_POLICY_FINGERPRINT
        )
        checks["entry_deadline_lifecycle_consistent"] = deadline_invariants
        checks["entry_deadline_route_cardinality"] = route_cardinality
        checks["entry_deadline_formula_exact"] = deadline_formula_exact
        checks["entry_deadline_route_terminal_consistent"] = route_terminal_consistent
        checks["execution_expired_tracks_cost_and_position_free"] = expired_cost_free
        checks["integrity_ok"] = str(checks.get("quick_check", "")).lower() == "ok" and all(
            bool(value)
            for key, value in checks.items()
            if key not in {"quick_check", "duplicate_counts", "integrity_ok"}
        )
        return checks
    finally:
        conn.close()


def restart_probe(paper_path: Path, source) -> tuple[bool, str | None]:
    before_size = paper_path.stat().st_size
    before_sha256 = accepted.file_sha256(paper_path)
    conn = accepted.connect_entry_router_db(paper_path)
    digest: str | None = None
    ok = False
    try:
        binding = ContinuousFirstPullbackBindingV02(conn, source)
        digest = binding.canonical_digest()
        ok = binding.durable_p1_rowid >= source.start_after_p1_rowid
    except Exception as exc:
        return False, f"{type(exc).__name__}:{exc}"
    finally:
        conn.close()
    after_size = paper_path.stat().st_size
    after_sha256 = accepted.file_sha256(paper_path)
    final_ok = (
        ok
        and before_size == after_size
        and before_sha256 == after_sha256
    )
    return final_ok, digest if final_ok else None


def build_session_track_metrics(paper_summary: dict[str, Any]) -> dict[str, Any]:
    result = _ORIGINAL_BUILD_TRACK_METRICS(paper_summary)
    result["entry_execution_deadlines"] = paper_summary.get(
        "entry_execution_deadlines", {}
    )
    return result


def _validate_locked_contracts_v02() -> None:
    accepted._validate_locked_contracts()
    if accepted.file_sha256(Path(accepted.__file__).resolve()) != ACCEPTED_V01_HARNESS_SHA256:
        raise RuntimeError("accepted multi-hour v0.1 harness changed")
    if LOCKED_EXIT_SPEC_FINGERPRINT != (
        "0bde5378f4ff2b63c6a96ad64140840ef8b2770c3be772a7aa71f8bdeb070129"
    ):
        raise RuntimeError("locked exit specification fingerprint drift")
    expected = (
        BINDING_FINGERPRINT,
        RUNNER_SPEC_FINGERPRINT,
        DEADLINE_POLICY_FINGERPRINT,
        MODEL_FINGERPRINT,
    )
    actual = (
        "c2f90d610ab774831af6a52b4a24e948b4bf0727d69457fee63311b5a159feca",
        "66725275f7e01e510b369df08281be9769d057113896f60f2ac652c2efe072fb",
        "6e2ae931d7270012dc025ef9a509ec30a240a850ea0d1c2666560a5e5506c9a7",
        "9002ed30ecd74722af5a37efec437143a17c4865f94ae37a60863fbc2320a7d7",
    )
    if expected != actual:
        raise RuntimeError("v0.2 runtime fingerprint drift")


def run_multihour(*, duration_seconds: int, launch_collector: bool):
    _validate_locked_contracts_v02()
    replacements = {
        "MODEL_ID": MODEL_ID,
        "MODEL_FINGERPRINT": MODEL_FINGERPRINT,
        "MULTIHOUR_SPEC": MULTIHOUR_SPEC,
        "BINDING_FINGERPRINT": BINDING_FINGERPRINT,
        "RUNNER_SPEC_FINGERPRINT": RUNNER_SPEC_FINGERPRINT,
        "ContinuousFirstPullbackBindingV01": ContinuousFirstPullbackBindingV02,
        "_validate_locked_contracts": lambda: None,
        "build_session_track_metrics": build_session_track_metrics,
    }
    originals = {name: getattr(accepted, name) for name in replacements}
    smoke_originals = {
        "_paper_summary": accepted.smoke._paper_summary,
        "_integrity_audit": accepted.smoke._integrity_audit,
        "restart_probe": accepted.smoke.restart_probe,
    }
    try:
        for name, value in replacements.items():
            setattr(accepted, name, value)
        accepted.smoke._paper_summary = _paper_summary_v02
        accepted.smoke._integrity_audit = _integrity_audit_v02
        accepted.smoke.restart_probe = restart_probe
        return accepted.run_multihour(
            duration_seconds=duration_seconds,
            launch_collector=launch_collector,
        )
    finally:
        for name, value in originals.items():
            setattr(accepted, name, value)
        for name, value in smoke_originals.items():
            setattr(accepted.smoke, name, value)


def build_arg_parser():
    return accepted.build_arg_parser()


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    if args.collector_child:
        return accepted.run_collector_child()
    if args.collector_child_probe:
        return accepted.run_collector_child_probe(args.collector_child_probe_ready)
    if not args.use_existing_collector and not args.launch_managed_collector:
        parser.error("one collector mode is required")
    code, _summary = run_multihour(
        duration_seconds=args.duration_seconds,
        launch_collector=bool(args.launch_managed_collector),
    )
    return code


if __name__ == "__main__":
    raise SystemExit(main())
