from __future__ import annotations

import shutil
import sqlite3
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
for import_root in (PROJECT_ROOT / "src", PROJECT_ROOT / "scripts"):
    if str(import_root) not in sys.path:
        sys.path.insert(0, str(import_root))

import phase4_continuous_firstpullback_multihour_run_selftest_v0_3 as v03_fixtures  # noqa: E402
import phase4_continuous_firstpullback_multihour_run_v0_5 as harness  # noqa: E402
import phase4_timer_fence_throughput_selftest_v0_1 as binding_fixtures  # noqa: E402
from phase4.paper_continuous_firstpullback_binding_v0_5 import (  # noqa: E402
    ContinuousFirstPullbackBindingV05,
    connect_atomic_entry_router_db,
)
from phase4.paper_continuous_market_source_v0_2 import (  # noqa: E402
    ContinuousMarketSourceV02,
    SourceSchemaError,
)


BASE = datetime(2026, 8, 26, 0, 52, 0, tzinfo=timezone.utc)
OBSERVED_FIRST = datetime(2026, 8, 26, 0, 52, 19, 677782, tzinfo=timezone.utc)
OBSERVED_SECOND = datetime(2026, 8, 26, 0, 52, 19, 519529, tzinfo=timezone.utc)
EXPECTED_REGRESSION_SECONDS = -0.158253
PERFORMANCE_ROWS = 100_000
PERFORMANCE_MINIMUM_RPS = 20_000.0


class MutableClock:
    def __init__(self, value: datetime) -> None:
        self.value = value

    def __call__(self) -> datetime:
        return self.value


class ScriptedContinuitySource:
    def __init__(self, *batches: tuple[tuple[int, datetime], ...]) -> None:
        self._batches = list(batches)
        self.calls: list[tuple[int, int]] = []
        self.rows_scanned = 0

    def continuity_rows_after(
        self,
        after_p1_rowid: int,
        *,
        through_p1_rowid: int,
    ) -> tuple[tuple[int, datetime], ...]:
        self.calls.append((int(after_p1_rowid), int(through_p1_rowid)))
        batch = self._batches.pop(0) if self._batches else ()
        self.rows_scanned += len(batch)
        return batch

    def cache_metrics(self) -> dict[str, int]:
        return {
            "source_health_probe_count": len(self.calls),
            "source_health_rows_scanned": self.rows_scanned,
        }


def make_watchdog(
    source: Any,
    *,
    start_rowid: int,
    clock: MutableClock,
    session_started_at: datetime = BASE,
) -> harness.EvidenceBasedSourceContinuityWatchdogV05:
    return harness.EvidenceBasedSourceContinuityWatchdogV05(
        source=source,
        last_observed_source_p1_rowid=start_rowid,
        last_source_advance_monotonic=0.0,
        session_started_at_utc=session_started_at,
        wall_clock=clock,
    )


def raises(error_type: type[BaseException], callback) -> bool:
    try:
        callback()
    except error_type:
        return True
    return False


def check(name: str, condition: bool, checks: dict[str, bool]) -> None:
    checks[name] = bool(condition)


def watchdog_state(watchdog: Any) -> tuple[Any, ...]:
    return (
        watchdog.last_observed_source_p1_rowid,
        watchdog.last_source_advance_monotonic,
        watchdog.last_health_p1_rowid,
        watchdog.last_actual_source_activity_utc,
        watchdog.last_probe_monotonic,
        watchdog.actual_source_advance_count,
        watchdog.source_advance_count,
        watchdog.maximum_actual_source_gap_seconds,
        watchdog.maximum_runner_probe_interval_seconds,
        watchdog.runner_starvation_count,
        watchdog.actual_source_stalled,
        watchdog.runner_source_poll_starved,
        watchdog.failure_reason,
    )


def run_binding_replay(
    source_path: Path,
    paper_path: Path,
) -> tuple[str, int, str]:
    source = ContinuousMarketSourceV02(
        source_path,
        start_after_p1_rowid=0,
        database_identity="T019:DETERMINISTIC-REPLAY",
    )
    conn = connect_atomic_entry_router_db(paper_path)
    clock = MutableClock(binding_fixtures.BASE + timedelta(seconds=10))
    watchdog = make_watchdog(
        source,
        start_rowid=0,
        clock=clock,
        session_started_at=binding_fixtures.BASE,
    )
    try:
        monotonic = 0.0
        binding = ContinuousFirstPullbackBindingV05(
            conn,
            source,
            wall_clock=clock,
        )
        while binding.durable_p1_rowid < 40:
            batch = binding.process_next_batch(batch_size=10)
            if batch.raw_rows_fetched == 0:
                raise AssertionError("replay fixture stopped before rowid 40")
            monotonic += 0.1
            watchdog.observe(binding.durable_p1_rowid, monotonic)
        return (
            binding.canonical_digest(),
            binding.durable_p1_rowid,
            watchdog.canonical_digest(monotonic),
        )
    finally:
        conn.close()


def reopen_binding_state(
    source_path: Path,
    paper_path: Path,
) -> tuple[str, int]:
    source = ContinuousMarketSourceV02(
        source_path,
        start_after_p1_rowid=0,
        database_identity="T019:DETERMINISTIC-REPLAY",
    )
    conn = connect_atomic_entry_router_db(paper_path)
    try:
        binding = ContinuousFirstPullbackBindingV05(
            conn,
            source,
            wall_clock=lambda: binding_fixtures.BASE + timedelta(seconds=10),
        )
        return binding.canonical_digest(), binding.durable_p1_rowid
    finally:
        conn.close()


def main() -> int:
    checks: dict[str, bool] = {}
    evidence: dict[str, Any] = {}
    root = Path(tempfile.mkdtemp(prefix="phase4_t019_"))
    try:
        harness._validate_locked_contracts_v05()
        check(
            "MODEL_CONTRACT_DECLARES_ROWID_ORDER_AND_MAX_HEALTH_ACTIVITY",
            harness.MULTIHOUR_SPEC["source_continuity_ordering"]
            == {
                "durable_order": "STRICTLY_INCREASING_P1_SQLITE_ROWID",
                "inserted_at_utc": "VALIDATED_UTC_METADATA_NOT_AN_ORDERING_KEY",
                "health_activity": (
                    "MAX_OBSERVED_INSERTED_AT_NOT_EARLIER_THAN_SESSION_START"
                ),
            },
            checks,
        )

        clock = MutableClock(BASE + timedelta(seconds=20))
        exact_source = ScriptedContinuitySource((
            (704294, OBSERVED_FIRST),
            (704295, OBSERVED_SECOND),
        ))
        exact = make_watchdog(exact_source, start_rowid=704293, clock=clock)
        exact.observe(704295, 1.0)
        check(
            "A_EXACT_PRODUCTION_REGRESSION_ACCEPTED_IN_ROWID_ORDER",
            exact.last_health_p1_rowid == 704295
            and exact.actual_source_advance_count == 2
            and exact.last_actual_source_activity_utc == OBSERVED_FIRST
            and (OBSERVED_SECOND - OBSERVED_FIRST).total_seconds()
            == EXPECTED_REGRESSION_SECONDS,
            checks,
        )

        large_source = ScriptedContinuitySource((
            (11, BASE + timedelta(seconds=10)),
            (12, BASE - timedelta(days=30)),
        ))
        large = make_watchdog(large_source, start_rowid=10, clock=clock)
        large.observe(12, 1.0)
        check(
            "B_LARGE_TIMESTAMP_REGRESSION_STILL_ORDERS_BY_ROWID",
            large.last_health_p1_rowid == 12
            and large.last_actual_source_activity_utc
            == BASE + timedelta(seconds=10)
            and not large.actual_source_stalled,
            checks,
        )

        equal_at = BASE + timedelta(seconds=3)
        equal_source = ScriptedContinuitySource(((2, equal_at), (3, equal_at)))
        equal = make_watchdog(equal_source, start_rowid=1, clock=clock)
        equal.observe(3, 1.0)
        check(
            "C_EQUAL_TIMESTAMPS_ACCEPTED",
            equal.last_health_p1_rowid == 3
            and equal.last_actual_source_activity_utc == equal_at,
            checks,
        )

        normal_source = ScriptedContinuitySource((
            (2, BASE + timedelta(seconds=1)),
            (3, BASE + timedelta(seconds=4)),
        ))
        normal = make_watchdog(normal_source, start_rowid=1, clock=clock)
        normal.observe(3, 1.0)
        check(
            "D_MONOTONIC_TIMESTAMPS_UNCHANGED",
            normal.last_actual_source_activity_utc == BASE + timedelta(seconds=4)
            and normal.maximum_actual_source_gap_seconds == 16.0
            and normal.actual_source_advance_count == 2,
            checks,
        )

        rewind_source = ScriptedContinuitySource()
        rewind = make_watchdog(rewind_source, start_rowid=3, clock=clock)
        duplicate_source = ScriptedContinuitySource((
            (2, BASE + timedelta(seconds=1)),
            (2, BASE + timedelta(seconds=2)),
        ))
        duplicate = make_watchdog(duplicate_source, start_rowid=1, clock=clock)
        duplicate_before = watchdog_state(duplicate)
        out_of_order_source = ScriptedContinuitySource((
            (3, BASE + timedelta(seconds=1)),
            (2, BASE + timedelta(seconds=2)),
        ))
        out_of_order = make_watchdog(
            out_of_order_source,
            start_rowid=1,
            clock=clock,
        )
        out_of_order_before = watchdog_state(out_of_order)
        check(
            "E_NON_INCREASING_DUPLICATE_INVALID_ROWID_FAILS_CLOSED",
            raises(accepted_binding_conflict(), lambda: rewind.observe(2, 1.0))
            and raises(
                accepted_binding_conflict(),
                lambda: duplicate.observe(2, 1.0),
            )
            and watchdog_state(duplicate) == duplicate_before
            and raises(
                accepted_binding_conflict(),
                lambda: out_of_order.observe(2, 1.0),
            )
            and watchdog_state(out_of_order) == out_of_order_before,
            checks,
        )

        malformed_db = root / "malformed.sqlite3"
        v03_fixtures.create_source_db(malformed_db)
        v03_fixtures.append_health_row(
            malformed_db,
            rowid=2,
            at=BASE + timedelta(seconds=1),
        )
        conn = sqlite3.connect(malformed_db)
        try:
            conn.execute(
                "UPDATE pump_events SET inserted_at_utc='not-a-timestamp' WHERE rowid=2"
            )
            conn.commit()
        finally:
            conn.close()
        malformed_source = ContinuousMarketSourceV02(
            malformed_db,
            start_after_p1_rowid=1,
            database_identity="T019:MALFORMED",
        )
        malformed = make_watchdog(
            malformed_source,
            start_rowid=1,
            clock=clock,
        )
        malformed_before = watchdog_state(malformed)

        naive_source = ScriptedContinuitySource(((2, BASE.replace(tzinfo=None)),))
        naive = make_watchdog(naive_source, start_rowid=1, clock=clock)
        naive_before = watchdog_state(naive)
        future_source = ScriptedContinuitySource((
            (2, clock.value + timedelta(microseconds=1)),
        ))
        future = make_watchdog(future_source, start_rowid=1, clock=clock)
        future_before = watchdog_state(future)
        check(
            "F_MALFORMED_NON_UTC_AND_FUTURE_TIMESTAMPS_FAIL_CLOSED",
            raises(SourceSchemaError, lambda: malformed.observe(2, 1.0))
            and watchdog_state(malformed) == malformed_before
            and raises(ValueError, lambda: naive.observe(2, 1.0))
            and watchdog_state(naive) == naive_before
            and raises(
                accepted_binding_conflict(),
                lambda: future.observe(2, 1.0),
            )
            and watchdog_state(future) == future_before,
            checks,
        )

        boundary_source = ScriptedContinuitySource(
            ((704294, OBSERVED_FIRST),),
            ((704295, OBSERVED_SECOND),),
        )
        boundary = make_watchdog(
            boundary_source,
            start_rowid=704293,
            clock=clock,
        )
        boundary.observe(704294, 0.5)
        boundary.observe(704295, 1.0)
        check(
            "G_BATCH_BOUNDARY_REGRESSION_ACCEPTED",
            boundary_source.calls == [(704293, 704294), (704294, 704295)]
            and boundary.last_health_p1_rowid == 704295
            and boundary.last_actual_source_activity_utc == OBSERVED_FIRST,
            checks,
        )

        replay_source_path = root / "replay-source.sqlite3"
        binding_fixtures.create_source(replay_source_path)
        binding_fixtures.append_rows(replay_source_path, 1, 40)
        conn = sqlite3.connect(replay_source_path)
        try:
            regressed_at = binding_fixtures.BASE - timedelta(days=7)
            conn.execute(
                "UPDATE pump_events SET inserted_at_utc=? WHERE rowid=11",
                (regressed_at.isoformat(),),
            )
            conn.commit()
        finally:
            conn.close()
        replay_one = run_binding_replay(
            replay_source_path,
            root / "replay-one.sqlite3",
        )
        replay_two = run_binding_replay(
            replay_source_path,
            root / "replay-two.sqlite3",
        )
        reopened = reopen_binding_state(
            replay_source_path,
            root / "replay-one.sqlite3",
        )
        check(
            "H_RESTART_REPLAY_DURABLE_STATE_AND_DIGEST_IDENTICAL",
            replay_one == replay_two
            and replay_one[1] == 40
            and reopened == replay_one[:2],
            checks,
        )
        evidence["replay"] = {
            "paper_digest": replay_one[0],
            "durable_p1_rowid": replay_one[1],
            "watchdog_digest": replay_one[2],
        }

        health_source = ScriptedContinuitySource(
            ((2, BASE + timedelta(seconds=10)),),
            ((3, BASE - timedelta(days=365)),),
        )
        health_clock = MutableClock(BASE + timedelta(seconds=10))
        health = make_watchdog(health_source, start_rowid=1, clock=health_clock)
        health.observe(2, 1.0)
        before_regression = health.last_actual_source_activity_utc
        health_clock.value = BASE + timedelta(seconds=20)
        health.observe(3, 2.0)
        health_snapshot = health.snapshot(2.0)
        check(
            "I_OLDER_TIMESTAMP_CANNOT_REWIND_ACTIVITY_OR_FABRICATE_STALL",
            health.last_actual_source_activity_utc == before_regression
            and health_snapshot["final_source_staleness_seconds"] == 10.0
            and not health.actual_source_stalled
            and health.failure_reason is None,
            checks,
        )

        under_source = ScriptedContinuitySource((
            (2, BASE + timedelta(seconds=10)),
            (3, BASE - timedelta(days=1)),
        ))
        under_clock = MutableClock(BASE + timedelta(seconds=309, microseconds=999000))
        under = make_watchdog(under_source, start_rowid=1, clock=under_clock)
        under.observe(3, 1.0)
        exact_stale_source = ScriptedContinuitySource((
            (2, BASE + timedelta(seconds=10)),
            (3, BASE - timedelta(days=1)),
        ))
        exact_stale_clock = MutableClock(BASE + timedelta(seconds=310))
        exact_stale = make_watchdog(
            exact_stale_source,
            start_rowid=1,
            clock=exact_stale_clock,
        )
        exact_stall = raises(
            harness.accepted_v04.accepted_v03.ProductionSourceStalled,
            lambda: exact_stale.observe(3, 1.0),
        )
        terminal_replay = raises(
            harness.accepted_v04.accepted_v03.ProductionSourceStalled,
            lambda: exact_stale.observe(3, 2.0),
        )
        check(
            "ADVERSARIAL_REGRESSION_STALENESS_BOUNDARY_AND_TERMINALITY",
            not under.actual_source_stalled
            and under.failure_reason is None
            and under.snapshot(1.0)["final_source_staleness_seconds"] == 299.999
            and exact_stall
            and exact_stale.actual_source_stalled
            and exact_stale.failure_reason == "PRODUCTION_SOURCE_STALLED"
            and terminal_replay,
            checks,
        )

        multi_source = ScriptedContinuitySource((
            (2, BASE + timedelta(seconds=10)),
            (3, BASE + timedelta(seconds=9)),
            (7, BASE + timedelta(seconds=12)),
            (9, BASE - timedelta(days=1)),
            (10, BASE + timedelta(seconds=11)),
        ))
        multi = make_watchdog(multi_source, start_rowid=1, clock=clock)
        multi.observe(10, 1.0)
        check(
            "ADVERSARIAL_MULTIPLE_REGRESSIONS_AND_ROWID_GAPS_PRESERVED",
            multi.last_health_p1_rowid == 10
            and multi.actual_source_advance_count == 5
            and multi.last_actual_source_activity_utc
            == BASE + timedelta(seconds=12),
            checks,
        )

        performance_rows = tuple(
            (
                rowid,
                BASE + timedelta(seconds=1 if rowid % 2 else 0),
            )
            for rowid in range(1, PERFORMANCE_ROWS + 1)
        )
        performance_source = ScriptedContinuitySource(performance_rows)
        performance_clock = MutableClock(BASE + timedelta(seconds=2))
        performance_watchdog = make_watchdog(
            performance_source,
            start_rowid=0,
            clock=performance_clock,
        )
        started = time.perf_counter()
        performance_watchdog.observe(PERFORMANCE_ROWS, 1.0)
        elapsed = time.perf_counter() - started
        rows_per_second = PERFORMANCE_ROWS / max(elapsed, 1e-9)
        check(
            "ADVERSARIAL_LINEAR_PERFORMANCE_WITH_FREQUENT_REGRESSIONS",
            rows_per_second >= PERFORMANCE_MINIMUM_RPS
            and performance_watchdog.actual_source_advance_count
            == PERFORMANCE_ROWS,
            checks,
        )
        evidence["performance"] = {
            "rows": PERFORMANCE_ROWS,
            "elapsed_seconds": elapsed,
            "rows_per_second": rows_per_second,
            "minimum_rows_per_second": PERFORMANCE_MINIMUM_RPS,
        }
        evidence["exact_production_shape"] = {
            "rowids": [704294, 704295],
            "timestamp_regression_seconds": EXPECTED_REGRESSION_SECONDS,
            "effective_last_activity_utc": exact.last_actual_source_activity_utc.isoformat(),
        }
        evidence["model_fingerprint"] = harness.MODEL_FINGERPRINT
    finally:
        shutil.rmtree(root, ignore_errors=True)

    print("=" * 112)
    print("V0.5 NON-MONOTONIC inserted_at_utc ROBUSTNESS SELF-TEST")
    print("=" * 112)
    for name, passed in checks.items():
        print(f"{name}: {'PASS' if passed else 'FAIL'}")
    print(
        "EXACT_SHAPE: "
        f"rowids=704294,704295 regression={EXPECTED_REGRESSION_SECONDS:.6f}s "
        f"effective_activity={evidence.get('exact_production_shape', {}).get('effective_last_activity_utc')}"
    )
    performance = evidence.get("performance", {})
    print(
        "PERFORMANCE: "
        f"rows={performance.get('rows')} "
        f"seconds={float(performance.get('elapsed_seconds', 0.0)):.6f} "
        f"rows_per_second={float(performance.get('rows_per_second', 0.0)):.2f}"
    )
    replay = evidence.get("replay", {})
    print(
        "REPLAY: "
        f"cursor={replay.get('durable_p1_rowid')} "
        f"paper_digest={replay.get('paper_digest')} "
        f"watchdog_digest={replay.get('watchdog_digest')}"
    )
    print(f"MODEL_FINGERPRINT: {evidence.get('model_fingerprint')}")
    passed = bool(checks) and all(checks.values())
    print(f"CHECKS: {sum(checks.values())}/{len(checks)}")
    print(f"RESULT: {'PASS' if passed else 'FAIL'}")
    return 0 if passed else 1


def accepted_binding_conflict() -> type[BaseException]:
    return harness.accepted.BindingConflict


if __name__ == "__main__":
    raise SystemExit(main())
