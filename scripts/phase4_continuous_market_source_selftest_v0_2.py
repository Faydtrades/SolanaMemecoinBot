from __future__ import annotations

import json
import shutil
import sqlite3
import sys
import tempfile
import time
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
for import_root in (PROJECT_ROOT / "src", PROJECT_ROOT / "scripts"):
    if str(import_root) not in sys.path:
        sys.path.insert(0, str(import_root))

import phase4_continuous_market_source_selftest_v0_1 as v01_fixture  # noqa: E402
from phase4.paper_continuous_market_source_v0_1 import (  # noqa: E402
    ContinuousMarketSourceV01,
)
from phase4.paper_continuous_market_source_v0_2 import (  # noqa: E402
    MODEL_FINGERPRINT,
    MODEL_ID,
    ContinuousMarketSourceV02,
    semantic_record,
)


EXPECTED_MODEL_ID = "P4-CONTINUOUS-MARKET-SOURCE-0002"
EXPECTED_MODEL_FINGERPRINT = (
    "9cb094f52bf4b4fe28dc4828b1d9a52a3350cde664c7da5a489fa84e9a4085a9"
)
SOURCE_IDENTITY = "SELFTEST:PUMP-EVENTS-FIXTURE-V0.2"
ANCHOR = v01_fixture.ANCHOR


def inserted_at(rowid: int) -> str:
    value = datetime(2026, 8, 25, tzinfo=timezone.utc) + timedelta(seconds=rowid)
    return value.isoformat(timespec="microseconds")


def add_continuity_column(path: Path) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.execute("ALTER TABLE pump_events ADD COLUMN inserted_at_utc TEXT")
        rows = conn.execute("SELECT rowid FROM pump_events ORDER BY rowid").fetchall()
        conn.executemany(
            "UPDATE pump_events SET inserted_at_utc=? WHERE rowid=?",
            [(inserted_at(int(row[0])), int(row[0])) for row in rows],
        )
        conn.commit()
    finally:
        conn.close()


def create_fixture(path: Path) -> None:
    v01_fixture.create_fixture(path)
    add_continuity_column(path)


def append_fixture_row(
    path: Path,
    *,
    rowid: int,
    mint: str,
    event_type: str,
    source: str = "LIVE_WEBSOCKET_EVENT_V0_3_4",
) -> None:
    conn = sqlite3.connect(path)
    try:
        v01_fixture.insert_row(
            conn,
            rowid=rowid,
            mint=mint,
            event_type=event_type,
            source=source,
        )
        conn.execute(
            "UPDATE pump_events SET inserted_at_utc=? WHERE rowid=?",
            (inserted_at(rowid), rowid),
        )
        conn.commit()
    finally:
        conn.close()


def make_v02(path: Path, *, anchor: int = ANCHOR) -> ContinuousMarketSourceV02:
    return ContinuousMarketSourceV02(
        path,
        start_after_p1_rowid=anchor,
        database_identity=SOURCE_IDENTITY,
    )


def make_v01(path: Path, *, anchor: int = ANCHOR) -> ContinuousMarketSourceV01:
    return ContinuousMarketSourceV01(
        path,
        start_after_p1_rowid=anchor,
        database_identity=SOURCE_IDENTITY,
    )


def semantic_batch(batch: Any) -> dict[str, Any]:
    return {
        "start_after_p1_rowid": batch.start_after_p1_rowid,
        "requested_after_p1_rowid": batch.requested_after_p1_rowid,
        "batch_limit": batch.batch_limit,
        "raw_rows_fetched": batch.raw_rows_fetched,
        "highest_fetched_p1_rowid": batch.highest_fetched_p1_rowid,
        "records": [semantic_record(record) for record in batch.records],
        "skips": [asdict(skip) for skip in batch.skips],
        "skipped_by_reason": list(batch.skipped_by_reason),
    }


def digest(value: Any) -> str:
    return sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()


class FailOnceSource(ContinuousMarketSourceV02):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.fail_next = False

    def _normalize_rows(self, *args, **kwargs):
        if self.fail_next:
            self.fail_next = False
            raise RuntimeError("INJECTED_NORMALIZATION_FAILURE")
        return super()._normalize_rows(*args, **kwargs)


def create_long_fixture(path: Path, *, row_count: int = 5_000) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.execute(v01_fixture.SCHEMA_SQL)
        conn.execute("ALTER TABLE pump_events ADD COLUMN inserted_at_utc TEXT")
        ordinary = []
        for rowid in range(1, row_count + 1):
            if rowid % 1_000 in {1, 2}:
                continue
            ordinary.append((
                rowid,
                f"long-{rowid}",
                f"long-sig-{rowid}",
                "BUY",
                "LIVE_WEBSOCKET_EVENT_V0_3_4",
                inserted_at(rowid),
                inserted_at(rowid),
            ))
        conn.executemany(
            "INSERT INTO pump_events(rowid,event_key,signature,event_type,mint,"
            "source_decoded_file,decoded_at_utc,inserted_at_utc) "
            "VALUES(?,?,?,?,NULL,?,?,?)",
            ordinary,
        )
        for launch_rowid in range(1, row_count + 1, 1_000):
            mint = f"LONG-MINT-{launch_rowid}"
            v01_fixture.insert_row(
                conn,
                rowid=launch_rowid,
                mint=mint,
                event_type="LAUNCH",
            )
            v01_fixture.insert_row(
                conn,
                rowid=launch_rowid + 1,
                mint=mint,
                event_type="BUY",
            )
            conn.execute(
                "UPDATE pump_events SET decoded_at_utc=?,inserted_at_utc=? "
                "WHERE rowid IN (?,?)",
                (
                    inserted_at(launch_rowid),
                    inserted_at(launch_rowid),
                    launch_rowid,
                    launch_rowid + 1,
                ),
            )
        conn.commit()
    finally:
        conn.close()


def main() -> int:
    checks: dict[str, bool] = {}
    evidence: dict[str, Any] = {}
    root = Path(tempfile.mkdtemp(prefix="phase4_market_source_v02_"))
    fixture = root / "source.sqlite3"
    long_fixture = root / "long_source.sqlite3"
    try:
        create_fixture(fixture)
        checks["A_model_identity_exact"] = (
            MODEL_ID == EXPECTED_MODEL_ID
            and MODEL_FINGERPRINT == EXPECTED_MODEL_FINGERPRINT
        )

        fresh = make_v02(fixture)
        before = fresh.latest_p1_rowid()
        append_fixture_row(
            fixture,
            rowid=20,
            mint="MINT-H",
            event_type="SELL",
        )
        checks["B_fresh_connection_sees_late_writer_commit"] = (
            before == 19 and fresh.latest_p1_rowid() == 20
        )

        source = make_v02(fixture)
        first = source.fetch_batch(after_p1_rowid=ANCHOR, batch_size=1)
        second = source.fetch_batch(
            after_p1_rowid=first.highest_fetched_p1_rowid,
            batch_size=1,
        )
        metrics = source.cache_metrics()
        checks["C_initial_rebuild_exactly_once"] = (
            metrics["full_launch_rebuild_queries"] == 1
        )
        checks["D_normal_monotonic_fetch_has_no_rebuild"] = (
            metrics["normal_monotonic_fetches_without_rebuild"] == 1
            and metrics["incremental_launch_catchup_queries"] == 0
        )
        checks["E_fresh_launch_eligibility_matches_v01"] = (
            [record.production_p1_rowid for record in (first.records + second.records)]
            == [4]
            and first.skips[0].reason == "MINT_NOT_SESSION_ELIGIBLE"
        )

        jump = make_v02(fixture)
        jump.fetch_batch(after_p1_rowid=ANCHOR, batch_size=1)
        jumped = jump.fetch_batch(after_p1_rowid=7, batch_size=1)
        jump_metrics = jump.cache_metrics()
        checks["F_forward_jump_catches_missing_interval_only"] = (
            jump_metrics["full_launch_rebuild_queries"] == 1
            and jump_metrics["incremental_launch_catchup_queries"] == 1
            and jump_metrics["rewind_rebuild_queries"] == 0
            and [record.production_p1_rowid for record in jumped.records] == [8]
        )

        replay = make_v02(fixture)
        speculative = replay.fetch_batch(after_p1_rowid=ANCHOR, batch_size=8)
        rewound = replay.fetch_batch(after_p1_rowid=5, batch_size=3)
        clean_replay = make_v02(fixture).fetch_batch(after_p1_rowid=5, batch_size=3)
        repeated = replay.fetch_batch(after_p1_rowid=5, batch_size=3)
        checks["G_rewind_and_repeated_fetch_deterministic"] = (
            speculative.highest_fetched_p1_rowid == 10
            and semantic_batch(rewound) == semantic_batch(clean_replay)
            and semantic_batch(repeated) == semantic_batch(clean_replay)
            and replay.cache_metrics()["rewind_rebuild_queries"] >= 2
        )

        failing = FailOnceSource(
            fixture,
            start_after_p1_rowid=ANCHOR,
            database_identity=SOURCE_IDENTITY,
        )
        initialized = failing.fetch_batch(after_p1_rowid=ANCHOR, batch_size=1)
        before_state = (
            failing.launch_state_through_p1_rowid,
            dict(failing._launch_by_mint),
        )
        failing.fail_next = True
        failed = False
        try:
            failing.fetch_batch(after_p1_rowid=4, batch_size=1)
        except RuntimeError as exc:
            failed = str(exc) == "INJECTED_NORMALIZATION_FAILURE"
        after_state = (
            failing.launch_state_through_p1_rowid,
            dict(failing._launch_by_mint),
        )
        retry = failing.fetch_batch(after_p1_rowid=4, batch_size=1)
        checks["H_normalization_failure_is_cache_atomic"] = (
            initialized.highest_fetched_p1_rowid == 3
            and failed
            and before_state == after_state
            and [record.production_p1_rowid for record in retry.records] == [5]
        )

        whole_v01 = make_v01(fixture).fetch_batch(
            after_p1_rowid=ANCHOR,
            batch_size=100,
        )
        whole_v02 = make_v02(fixture).fetch_batch(
            after_p1_rowid=ANCHOR,
            batch_size=100,
        )
        checks["I_v01_v02_semantic_equivalence"] = (
            semantic_batch(whole_v01) == semantic_batch(whole_v02)
        )
        skip_by_rowid = {skip.production_p1_rowid: skip.reason for skip in whole_v02.skips}
        checks["J_gap_launch_and_future_launch_no_lookahead"] = (
            skip_by_rowid[16] == "GAP_RECOVERY_LAUNCH_NOT_SESSION_ELIGIBLE"
            and skip_by_rowid[17] == "MINT_NOT_SESSION_ELIGIBLE"
            and skip_by_rowid[6] == "MINT_NOT_SESSION_ELIGIBLE"
        )

        first_instance = make_v02(fixture)
        durable_batch = first_instance.fetch_batch(
            after_p1_rowid=ANCHOR,
            batch_size=6,
        )
        durable_cursor = durable_batch.highest_fetched_p1_rowid
        continued = first_instance.fetch_batch(
            after_p1_rowid=durable_cursor,
            batch_size=6,
        )
        restarted = make_v02(fixture).fetch_batch(
            after_p1_rowid=durable_cursor,
            batch_size=6,
        )
        checks["K_restart_from_durable_cursor_semantic_digest"] = (
            digest(semantic_batch(continued)) == digest(semantic_batch(restarted))
        )

        ro_conn = ContinuousMarketSourceV02.open_readonly(fixture)
        try:
            rejected = False
            try:
                ro_conn.execute("UPDATE pump_events SET mint=mint WHERE rowid=1")
            except sqlite3.OperationalError:
                rejected = True
            checks["U_query_only_write_rejected"] = (
                ContinuousMarketSourceV02.connection_is_query_only(ro_conn)
                and rejected
            )
        finally:
            ro_conn.close()

        create_long_fixture(long_fixture)
        long_source = make_v02(long_fixture, anchor=0)
        cursor = 0
        observed: list[int] = []
        started = time.perf_counter()
        for _ in range(1_000):
            batch = long_source.fetch_batch(after_p1_rowid=cursor, batch_size=5)
            observed.extend(record.production_p1_rowid for record in batch.records)
            cursor = batch.highest_fetched_p1_rowid
        elapsed = time.perf_counter() - started
        long_metrics = long_source.cache_metrics()
        evidence["long_fixture_rows"] = 5_000
        evidence["monotonic_fetches"] = 1_000
        evidence["elapsed_seconds"] = elapsed
        evidence.update(long_metrics)
        checks["V_no_duplicate_semantic_identities"] = len(observed) == len(set(observed))
        checks["W_long_session_prefix_rebuild_constant"] = (
            cursor == 5_000
            and long_metrics["full_launch_rebuild_queries"] <= 1
            and long_metrics["incremental_launch_catchup_queries"] == 0
            and long_metrics["normal_monotonic_fetches_without_rebuild"] == 999
        )
    finally:
        shutil.rmtree(root, ignore_errors=True)

    print("=" * 96)
    print("CONTINUOUS MARKET SOURCE V0.2 SELF-TEST")
    print("=" * 96)
    for name, passed in checks.items():
        print(f"{name}: {'PASS' if passed else 'FAIL'}")
    print("QUERY_COUNT_EVIDENCE=" + json.dumps(evidence, sort_keys=True))
    passed = bool(checks) and all(checks.values())
    print(f"RESULT: {'PASS' if passed else 'FAIL'}")
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
