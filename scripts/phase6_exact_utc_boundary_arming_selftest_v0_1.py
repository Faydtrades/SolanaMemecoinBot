from __future__ import annotations

import argparse
import ast
import contextlib
import importlib.util
import io
import json
import os
import sqlite3
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

from phase6 import frozen_oos_collection_lifecycle_v0_1 as lifecycle  # noqa: E402
from phase6 import frozen_oos_hypothesis_evaluation_v0_1 as evaluation  # noqa: E402


UTC = timezone.utc
START = datetime(2026, 9, 7, 0, 0, 0, tzinfo=UTC)
ARMED_AT = START - timedelta(hours=1)
END = START + timedelta(seconds=lifecycle.MINIMUM_DURATION_SECONDS)


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


def _cli_module() -> object:
    path = PROJECT_ROOT / "scripts/phase6_frozen_oos_collection_lifecycle_v0_1.py"
    spec = importlib.util.spec_from_file_location("p6_t002a_cli", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("could not load lifecycle CLI")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


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
        _insert_source_row(
            conn, event_key="anchor-row", slot=1, event_type="LAUNCH",
            mint="ANCHOR-MINT", observed_at=ARMED_AT - timedelta(seconds=1),
        )
        conn.execute(
            "INSERT INTO gap_jobs_v034 VALUES(?,?,?,?)",
            ("complete-window", lifecycle.dt_text(START), lifecycle.dt_text(END), "DONE"),
        )
        conn.commit()
    finally:
        conn.close()


def _insert_source_row(
    conn: sqlite3.Connection,
    *,
    event_key: str,
    slot: int,
    event_type: str,
    mint: str,
    observed_at: datetime,
) -> None:
    stamp = lifecycle.dt_text(observed_at)
    conn.execute(
        "INSERT INTO pump_events("
        "event_key,signature,slot,event_type,mint,user_wallet,"
        "sol_amount_lamports,token_amount_raw,is_buy,"
        "virtual_sol_reserve_raw,virtual_token_reserve_raw,"
        "real_sol_reserve_raw,real_token_reserve_raw,event_at_utc,"
        "received_at_utc,confirmation_status,source_program,"
        "source_decoded_file,inserted_at_utc) "
        "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            event_key, f"sig-{event_key}", slot, event_type, mint, "USER",
            1_000_000, 10_000_000, 1, 30_000_000_000, 1_000_000_000,
            1, 1, stamp, stamp, "confirmed", "pump",
            "LIVE_WEBSOCKET_EVENT_V0_3_4", stamp,
        ),
    )


@dataclass
class ArmedFixture:
    root: Path
    run_id: str
    restart_before_boundary: bool

    def __post_init__(self) -> None:
        self.root.mkdir(parents=True, exist_ok=False)
        self.source = self.root / "source.sqlite3"
        self.runtime = self.root / "runtime"
        self.clock = MutableClock(ARMED_AT)
        _source_schema(self.source)
        self.anchor = lifecycle.source_anchor(self.source)
        self.manager = lifecycle.FrozenOOSLifecycleV01(
            PROJECT_ROOT,
            self.runtime,
            clock=self.clock,
            verify_runtime_files=True,
        )
        self.manifest = self.manager.start(
            lifecycle.StartRequest(
                source_db_path=self.source,
                runtime_root=self.runtime,
                repository_commit=lifecycle.git_head(PROJECT_ROOT),
                start_at=START,
                source_start_cursor=self.anchor,
                run_id=self.run_id,
            )
        )

    def append_arm_to_start_rows(self) -> None:
        conn = sqlite3.connect(self.source)
        try:
            _insert_source_row(
                conn, event_key="prewindow-row", slot=2, event_type="TRADE",
                mint="ARMED-MINT", observed_at=START - timedelta(minutes=30),
            )
            _insert_source_row(
                conn, event_key="boundary-row", slot=3, event_type="LAUNCH",
                mint="BOUNDARY-MINT", observed_at=START,
            )
            conn.commit()
        finally:
            conn.close()

    def collect_at_boundary(self) -> dict[str, object]:
        if self.restart_before_boundary:
            self.manager.begin_segment(
                self.run_id, pid=2_147_483_647, birth_token="dead-prestart-process"
            )
            self.manager.reconcile_process(self.run_id)
        token = lifecycle.process_birth_token(os.getpid())
        if token is None:
            raise RuntimeError("current process identity unavailable")
        self.manager.begin_segment(self.run_id, pid=os.getpid(), birth_token=token)
        pre_status = self.manager.status(self.run_id, now=ARMED_AT + timedelta(minutes=30))
        before_state = self.manager._state(self.run_id)
        self.append_arm_to_start_rows()
        sleeps: list[float] = []
        prestart_snapshots: list[tuple[int, int, int, str]] = []
        original_sleep = lifecycle.time.sleep

        def advance_to_boundary(seconds: float) -> None:
            sleeps.append(seconds)
            state = self.manager._state(self.run_id)
            prestart_snapshots.append((
                int(state["source_cursor"]),
                int(state["source_row_count"]),
                int(state["candidate_count"]),
                str(state["coverage_complete_through_utc"]),
            ))
            self.clock.value = START
            (self.manager.run_dir(self.run_id) / "stop.request").write_text(
                "GRACEFUL_STOP_REQUESTED\n", encoding="ascii"
            )

        lifecycle.time.sleep = advance_to_boundary
        try:
            result = lifecycle.run_worker(
                self.manager, self.run_id, poll_seconds=0.0, clock_seconds=999.0
            )
        finally:
            lifecycle.time.sleep = original_sleep
        state = self.manager._state(self.run_id)
        paper = Path(self.manifest["paper_db_path"])
        conn = sqlite3.connect(paper.resolve().as_uri() + "?mode=ro", uri=True)
        conn.execute("PRAGMA query_only=ON")
        try:
            rowids = tuple(
                int(row[0])
                for row in conn.execute(
                    "SELECT production_p1_rowid FROM paper_fp_binding_inputs_v0_1 "
                    "WHERE production_p1_rowid IS NOT NULL ORDER BY production_p1_rowid"
                )
            )
            input_keys = tuple(
                str(row[0])
                for row in conn.execute(
                    "SELECT input_key FROM paper_fp_binding_inputs_v0_1 ORDER BY input_key"
                )
            )
            content_fingerprints = tuple(
                (int(row[0]), str(row[1]))
                for row in conn.execute(
                    "SELECT production_p1_rowid,content_fingerprint "
                    "FROM paper_fp_binding_inputs_v0_1 "
                    "WHERE production_p1_rowid IS NOT NULL ORDER BY production_p1_rowid"
                )
            )
            timer_keys = tuple(
                str(row[0])
                for row in conn.execute(
                    "SELECT timer_key FROM paper_fp_binding_prepared_timers_v0_1 ORDER BY timer_key"
                )
            )
        finally:
            conn.close()
        return {
            "result": result,
            "pre_status": pre_status,
            "before_state": before_state,
            "state": state,
            "sleeps": tuple(sleeps),
            "prestart_snapshots": tuple(prestart_snapshots),
            "rowids": rowids,
            "input_keys": input_keys,
            "content_fingerprints": content_fingerprints,
            "timer_keys": timer_keys,
        }


def _paper_candidate_fixture(path: Path) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.executescript(
            """
            CREATE TABLE paper_continuous_signal_contexts_v0_1(
                signal_key TEXT, route_id TEXT, mint TEXT,
                strategy_version TEXT, parameter_set_id TEXT, role TEXT,
                source_cursor INTEGER, source_event_key TEXT
            );
            CREATE TABLE paper_entry_routes(
                route_id TEXT, candidate_id TEXT, mint TEXT,
                strategy_version TEXT, parameter_set_id TEXT,
                signal_observed_at TEXT, signal_ingest_seq INTEGER,
                reference_price_identity TEXT,
                reference_price_numerator_raw TEXT,
                reference_price_denominator_raw TEXT,
                requested_size_lamports INTEGER, execution_ready_at TEXT,
                state TEXT, state_reason TEXT,
                selected_market_observed_at TEXT,
                selected_market_ingest_seq INTEGER,
                selected_source_event_key TEXT,
                simulated_entry_price_numerator_raw TEXT,
                simulated_entry_price_denominator_raw TEXT,
                total_entry_explicit_cost_lamports INTEGER,
                adverse_slippage_bps INTEGER
            );
            """
        )
        parameter_set = evaluation.LOCKED_PARAMETER_SET_BY_ROLE["CONTROL"]
        for name, observed, seq in (
            ("prewindow", START - timedelta(microseconds=1), 2),
            ("boundary", START, 3),
        ):
            source_event_key = f"sig-{seq}:0:event-{seq}:CANDIDATE:CONTROL"
            conn.execute(
                "INSERT INTO paper_continuous_signal_contexts_v0_1 VALUES(?,?,?,?,?,?,?,?)",
                (
                    name, f"route-{name}", f"MINT-{name}", "v1.1", parameter_set,
                    "CONTROL", seq, source_event_key,
                ),
            )
            stamp = lifecycle.dt_text(observed)
            conn.execute(
                "INSERT INTO paper_entry_routes VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    f"route-{name}", name, f"MINT-{name}", "v1.1", parameter_set,
                    stamp, seq, "SOL_NATIVE", "100", "1", 1_000_000,
                    lifecycle.dt_text(observed + timedelta(milliseconds=500)),
                    "REJECTED", "TEST_REJECTION", None, None, None, None, None, None, None,
                ),
            )
        conn.commit()
    finally:
        conn.close()


def _raises(reason: str, callback: Callable[[], object]) -> None:
    try:
        callback()
    except lifecycle.LifecycleError as exc:
        assert exc.reason == reason, (exc.reason, reason)
    else:
        raise AssertionError(f"expected {reason}")


def main() -> int:
    matrix = Matrix()
    cli = _cli_module()
    git_before = lifecycle.git_status(PROJECT_ROOT)
    protected = {
        path: lifecycle.sha256_file(PROJECT_ROOT / path)
        for path in (
            "src/phase6/frozen_oos_hypothesis_evaluation_v0_1.py",
            "scripts/phase6_frozen_oos_hypothesis_evaluation_v0_1.py",
            "scripts/phase6_frozen_oos_hypothesis_evaluation_selftest_v0_1.py",
            "data/research/phase6/P6_OOS_PROTOCOL_0001/protocol_manifest.json",
            "data/research/phase6/P6_OOS_PROTOCOL_0001/policy_definitions.json",
        )
    }

    with tempfile.TemporaryDirectory(prefix="p6_t002a_selftest_") as raw:
        root = Path(raw)
        restarted = ArmedFixture(root / "restarted", "P6-T002A-RESTARTED", True)
        uninterrupted = ArmedFixture(root / "uninterrupted", "P6-T002A-DIRECT", False)

        matrix.check(
            "01 explicit start persists with microsecond zero",
            lambda: (
                None
                if restarted.manifest["start_at_utc"]
                == "2026-09-07T00:00:00.000000+00:00"
                and restarted.manifest["requested_start_at_utc"]
                == restarted.manifest["start_at_utc"]
                else (_ for _ in ()).throw(AssertionError("start boundary drift"))
            ),
        )
        matrix.check(
            "02 end is exactly start plus 259200 seconds",
            lambda: (
                None
                if evaluation._complete_utc_days(
                    evaluation.FrozenOOSWindowV01(
                        start_at=START,
                        end_at=END,
                        source_start_after_ingest_seq=1,
                        source_end_ingest_seq=3,
                        source_db_sha256="1" * 64,
                    )
                )
                == (
                    datetime(2026, 9, 7, tzinfo=UTC).date(),
                    datetime(2026, 9, 8, tzinfo=UTC).date(),
                    datetime(2026, 9, 9, tzinfo=UTC).date(),
                )
                and lifecycle.dt_parse(restarted.manifest["minimum_end_at_utc"])
                - lifecycle.dt_parse(restarted.manifest["start_at_utc"])
                == timedelta(seconds=259_200)
                else (_ for _ in ()).throw(AssertionError("three-day geometry mismatch"))
            ),
        )
        matrix.check(
            "03 one-hour early arming is durably recorded",
            lambda: (
                None
                if restarted.manifest["armed_at_utc"] == lifecycle.dt_text(ARMED_AT)
                and restarted.manifest["arming_lead_microseconds"] == 3_600_000_000
                and restarted.manifest["source_start_cursor"] == 1
                else (_ for _ in ()).throw(AssertionError("arming evidence mismatch"))
            ),
        )

        restart_result = restarted.collect_at_boundary()
        direct_result = uninterrupted.collect_at_boundary()

        def preboundary_status() -> None:
            status = restart_result["pre_status"]
            assert status["lifecycle_state"] == "ARMED_WAITING_FOR_START"
            assert status["elapsed_seconds"] == 0
            assert status["remaining_seconds"] > lifecycle.MINIMUM_DURATION_SECONDS
        matrix.check("04 pre-boundary status is armed with zero elapsed", preboundary_status)

        def no_prestart_progress() -> None:
            state = restart_result["before_state"]
            assert state["source_cursor"] == restarted.anchor
            assert state["source_row_count"] == 0
            assert state["candidate_count"] == 0
            assert state["coverage_complete_through_utc"] == lifecycle.dt_text(START)
            assert restart_result["prestart_snapshots"] == (
                (restarted.anchor, 0, 0, lifecycle.dt_text(START)),
            )
        matrix.check("05 pre-start worker performs no scientific progress", no_prestart_progress)

        def restart_identity() -> None:
            state = restart_result["state"]
            manifest = restarted.manager._manifest(restarted.run_id)
            assert manifest["run_id"] == restarted.run_id
            assert manifest["start_at_utc"] == lifecycle.dt_text(START)
            assert manifest["minimum_end_at_utc"] == lifecycle.dt_text(END)
            assert state["target_end_at_utc"] == lifecycle.dt_text(END)
            assert state["duration_seconds"] == 259_200
            assert len(state["segments"]) == 2
            assert state["segments"][0]["termination"] == "STALE_PROCESS_AFTER_RESTART"
        matrix.check("06 pre-boundary restart preserves identity and bounds", restart_identity)

        def arm_rows_not_lost() -> None:
            assert restart_result["result"] == 0
            assert restart_result["rowids"] == (2, 3)
            assert restart_result["state"]["source_cursor"] == 3
            assert restart_result["state"]["source_row_count"] == 2
        matrix.check("07 rows arriving after ARM and before start are not lost", arm_rows_not_lost)

        def p6_window_filter() -> None:
            paper = root / "candidate-window.sqlite3"
            _paper_candidate_fixture(paper)
            window = evaluation.FrozenOOSWindowV01(
                start_at=START, end_at=END,
                source_start_after_ingest_seq=1, source_end_ingest_seq=3,
                source_db_sha256="2" * 64,
            )
            candidates = evaluation.load_frozen_candidates_v0_1(paper, window)
            assert tuple(item.signal_key for item in candidates) == ("boundary",)
        matrix.check("08 P6-T001 excludes pre-window CandidateSignals", p6_window_filter)

        def coverage_clamped() -> None:
            source = root / "coverage.sqlite3"
            _source_schema(source)
            conn = sqlite3.connect(source)
            try:
                _insert_source_row(
                    conn, event_key="only-prewindow", slot=2, event_type="TRADE",
                    mint="PRE", observed_at=START - timedelta(minutes=1),
                )
                conn.commit()
            finally:
                conn.close()
            coverage, _gaps = lifecycle.source_coverage_probe(
                source, start=START, target=END, source_start_cursor=1
            )
            assert coverage == START
        matrix.check("09 pre-window activity cannot move coverage backward", coverage_clamped)

        def equivalence() -> None:
            assert restart_result["rowids"] == direct_result["rowids"] == (2, 3)
            assert restart_result["content_fingerprints"] == direct_result["content_fingerprints"]
            assert restart_result["state"]["source_cursor"] == direct_result["state"]["source_cursor"]
            assert restart_result["state"]["candidate_count"] == direct_result["state"]["candidate_count"]
        matrix.check("10 armed restart execution matches uninterrupted boundary execution", equivalence)

        matrix.check(
            "11 pre-start wait is bounded and non-busy",
            lambda: (
                None
                if restart_result["sleeps"]
                and 0.05 <= restart_result["sleeps"][0] <= 1.0
                else (_ for _ in ()).throw(AssertionError("unsafe prestart polling"))
            ),
        )

        def naive_rejected() -> None:
            try:
                cli._utc("2026-09-07T00:00:00")
            except argparse.ArgumentTypeError:
                return
            raise AssertionError("naive timestamp accepted")
        matrix.check("12 naive start-at timestamp is rejected", naive_rejected)

        def timezone_normalized() -> None:
            parsed = cli._utc("2026-09-07T02:00:00+02:00")
            assert parsed == START and parsed.tzinfo == UTC
        matrix.check("13 aware start-at timestamp is normalized to UTC", timezone_normalized)

        def implicit_start_rejected() -> None:
            with contextlib.redirect_stderr(io.StringIO()):
                try:
                    cli.build_parser().parse_args(["start"])
                except SystemExit as exc:
                    assert exc.code == 2
                    return
            raise AssertionError("implicit current-time start remained available")
        matrix.check("14 start command requires explicit start-at", implicit_start_rejected)

        def invalid_leads_rejected() -> None:
            past_root = root / "past"
            past_root.mkdir()
            past_source = past_root / "source.sqlite3"
            _source_schema(past_source)
            past_manager = lifecycle.FrozenOOSLifecycleV01(
                PROJECT_ROOT, past_root / "runtime", clock=lambda: START,
                verify_runtime_files=True,
            )
            _raises(
                "INVALID_START_BOUNDARY",
                lambda: past_manager.start(lifecycle.StartRequest(
                    source_db_path=past_source, runtime_root=past_root / "runtime",
                    repository_commit=lifecycle.git_head(PROJECT_ROOT),
                    start_at=START - timedelta(microseconds=1), source_start_cursor=1,
                    run_id="PAST",
                )),
            )
            future_root = root / "future"
            future_root.mkdir()
            future_source = future_root / "source.sqlite3"
            _source_schema(future_source)
            future_manager = lifecycle.FrozenOOSLifecycleV01(
                PROJECT_ROOT, future_root / "runtime", clock=lambda: START,
                verify_runtime_files=True,
            )
            _raises(
                "INVALID_START_BOUNDARY",
                lambda: future_manager.start(lifecycle.StartRequest(
                    source_db_path=future_source, runtime_root=future_root / "runtime",
                    repository_commit=lifecycle.git_head(PROJECT_ROOT),
                    start_at=START + timedelta(seconds=lifecycle.MAXIMUM_ARMING_LEAD_SECONDS + 1),
                    source_start_cursor=1, run_id="FAR-FUTURE",
                )),
            )
        matrix.check("15 past and excessive-future starts fail closed", invalid_leads_rejected)

        def duplicates_absent() -> None:
            assert len(restart_result["input_keys"]) == len(set(restart_result["input_keys"]))
            assert len(restart_result["timer_keys"]) == len(set(restart_result["timer_keys"]))
        matrix.check("16 replay audit identities remain unique", duplicates_absent)

        def forbidden_capabilities_absent() -> None:
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
                    node.name.lower()
                    for node in ast.walk(tree)
                    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
                )
            assert not (names & forbidden), sorted(names & forbidden)
        matrix.check("17 no signer send broadcast or custody capability", forbidden_capabilities_absent)

        def prestart_stop_safe() -> None:
            fixture = ArmedFixture(root / "prestop", "P6-T002A-PRESTOP", False)
            token = lifecycle.process_birth_token(os.getpid())
            assert token
            fixture.manager.begin_segment(fixture.run_id, pid=os.getpid(), birth_token=token)
            (fixture.manager.run_dir(fixture.run_id) / "stop.request").write_text(
                "GRACEFUL_STOP_REQUESTED\n", encoding="ascii"
            )
            assert lifecycle.run_worker(fixture.manager, fixture.run_id, poll_seconds=0.0) == 0
            state = fixture.manager._state(fixture.run_id)
            assert state["source_cursor"] == fixture.anchor
            assert state["segments"][-1]["termination"] == "STOP_REQUESTED_WHILE_ARMED"
            assert state["lifecycle_state"] == "STOPPED"
        matrix.check("18 graceful pre-boundary stop leaves frozen science untouched", prestart_stop_safe)

        def terminal_failure_not_masked() -> None:
            fixture = ArmedFixture(root / "invalid", "P6-T002A-INVALID", False)
            fixture.manager.mark_invalid(fixture.run_id, "UNRECOVERABLE_SOURCE_GAP")
            status = fixture.manager.status(
                fixture.run_id, now=ARMED_AT + timedelta(minutes=30)
            )
            assert status["lifecycle_state"] == "INVALID"
            assert status["elapsed_seconds"] == 0
        matrix.check("19 terminal pre-start failure is never reported as armed", terminal_failure_not_masked)

    assert lifecycle.git_status(PROJECT_ROOT) == git_before
    for path, digest in protected.items():
        assert lifecycle.sha256_file(PROJECT_ROOT / path) == digest, path
    evidence = {
        "task_id": "MEME-P6-T002A",
        "model_id": lifecycle.MODEL_ID,
        "model_fingerprint": lifecycle.MODEL_FINGERPRINT,
        "requested_start_at_utc": lifecycle.dt_text(START),
        "minimum_end_at_utc": lifecycle.dt_text(END),
        "complete_utc_days": ["2026-09-07", "2026-09-08", "2026-09-09"],
        "checks_passed": len(matrix.results),
        "checks_total": len(matrix.results),
        "protected_p6_t001_sha256_unchanged": protected,
        "strong_restart_equivalence": {
            "restarted_rowids": list(restart_result["rowids"]),
            "uninterrupted_rowids": list(direct_result["rowids"]),
            "exact_match": restart_result["rowids"] == direct_result["rowids"],
        },
        "production_or_live_started": False,
        "evaluation_performed": False,
        "real_git_status_before_after_equal": True,
    }
    print(json.dumps(evidence, sort_keys=True, indent=2))
    print(f"CHECKS: {len(matrix.results)}/{len(matrix.results)}")
    print("RESULT: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
