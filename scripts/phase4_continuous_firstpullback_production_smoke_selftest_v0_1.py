from __future__ import annotations

import hashlib
import sqlite3
import sys
import tempfile
import time
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))
if str(PROJECT_ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from phase4.paper_continuous_firstpullback_binding_v0_1 import (  # noqa: E402
    ContinuousFirstPullbackBindingV01,
)
from phase4.paper_continuous_market_source_v0_1 import (  # noqa: E402
    ContinuousMarketSourceV01,
)
from phase4.paper_entry_router_v0_1 import connect_entry_router_db  # noqa: E402
from phase4_continuous_firstpullback_binding_selftest_v0_1 import (  # noqa: E402
    BASE,
    SCHEMA_SQL,
    create_fixture,
    insert_row,
)
from phase4_continuous_firstpullback_production_smoke_v0_1 import (  # noqa: E402
    MAX_RUNTIME_SECONDS,
    MIN_RUNTIME_SECONDS,
    MODEL_FINGERPRINT,
    MODEL_ID,
    ManagedCollectorChild,
    capture_source_anchor,
    classify_result,
    restart_probe,
    should_stop_early,
    should_stop_for_duration,
    write_json_artifact,
)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def copy_prefix(template: Path, target: Path, through: int) -> None:
    source = sqlite3.connect(template)
    source.row_factory = sqlite3.Row
    target_conn = sqlite3.connect(target)
    try:
        target_conn.execute(SCHEMA_SQL)
        columns = ["rowid"] + [
            str(row[1]) for row in target_conn.execute("PRAGMA table_info(pump_events)")
        ]
        rows = source.execute(
            "SELECT rowid AS p1_rowid,* FROM pump_events "
            "WHERE rowid<=? ORDER BY rowid", (through,)
        ).fetchall()
        sql = (
            "INSERT INTO pump_events(" + ",".join(columns) + ") VALUES(" +
            ",".join("?" for _ in columns) + ")"
        )
        for row in rows:
            target_conn.execute(sql, tuple(row))
        target_conn.commit()
    finally:
        source.close()
        target_conn.close()


def main() -> int:
    checks: dict[str, bool] = {}
    with tempfile.TemporaryDirectory(prefix="phase4_production_smoke_selftest_") as raw:
        root = Path(raw)
        template = root / "template.sqlite3"
        source_path = root / "source.sqlite3"
        paper_path = root / "paper.sqlite3"
        artifact_path = root / "summary.json"
        child_log = root / "child.log"
        create_fixture(template)
        copy_prefix(template, source_path, 14)
        identity = "SELFTEST:PRODUCTION-SMOKE:SOURCE"
        anchor = capture_source_anchor(source_path, identity)
        checks["fresh_anchor_selection"] = anchor == 14

        readonly = ContinuousMarketSourceV01.open_readonly(source_path)
        try:
            write_rejected = False
            try:
                readonly.execute("CREATE TABLE forbidden_write(value INTEGER)")
            except sqlite3.OperationalError:
                write_rejected = True
            checks["production_source_read_only"] = (
                ContinuousMarketSourceV01.connection_is_query_only(readonly)
                and write_rejected
            )
        finally:
            readonly.close()

        writer = sqlite3.connect(source_path)
        insert_row(writer, 15, "MINT-LIVE", "LAUNCH", observed_offset=315)
        insert_row(writer, 16, "MINT-LIVE", "BUY", observed_offset=316)
        writer.commit()
        writer.close()
        source_hash_before_reads = file_sha256(source_path)

        source = ContinuousMarketSourceV01(
            source_path,
            start_after_p1_rowid=anchor,
            database_identity=identity,
        )
        conn = connect_entry_router_db(paper_path)
        binding = ContinuousFirstPullbackBindingV01(conn, source, wall_clock=lambda: BASE)
        batch = binding.process_next_batch(batch_size=100)
        timer_key = binding.prepare_clock_tick(
            BASE, timer_id="SELFTEST:FINAL-TIMER"
        )
        timer = binding.execute_prepared_timer(timer_key)
        evaluation_count = int(conn.execute(
            "SELECT COUNT(*) FROM paper_strategy_evaluations"
        ).fetchone()[0])
        evaluated_mints = int(conn.execute(
            "SELECT COUNT(DISTINCT mint) FROM paper_strategy_evaluations"
        ).fetchone()[0])
        pre_anchor_runs = int(conn.execute(
            "SELECT COUNT(*) FROM paper_fp_binding_strategy_runs_v0_1 "
            "WHERE mint='MINT-A'"
        ).fetchone()[0])
        live_runs = int(conn.execute(
            "SELECT COUNT(*) FROM paper_fp_binding_strategy_runs_v0_1 "
            "WHERE mint='MINT-LIVE'"
        ).fetchone()[0])
        quick = str(conn.execute("PRAGMA quick_check").fetchone()[0]).lower()
        final_cursor = binding.durable_p1_rowid
        conn.close()

        checks["graceful_end_watermark"] = (
            source.latest_p1_rowid() == 16
            and final_cursor == 16
            and timer.status == "COMMITTED"
        )
        checks["fresh_post_anchor_evaluation"] = (
            batch.raw_rows_fetched == 2
            and batch.normalized_records == 2
            and evaluation_count > 0
            and evaluated_mints == 1
            and live_runs == 4
            and pre_anchor_runs == 0
        )
        checks["source_unchanged_by_reads"] = (
            file_sha256(source_path) == source_hash_before_reads
        )
        restart_ok, restart_digest = restart_probe(paper_path, source)
        checks["restart_probe"] = restart_ok and bool(restart_digest)
        checks["paper_quick_check"] = quick == "ok"

        payload = {
            "model_id": MODEL_ID,
            "fingerprint": MODEL_FINGERPRINT,
            "anchor": anchor,
            "final_cursor": final_cursor,
            "result_class": "PASS_PIPELINE_NO_CANDIDATE",
        }
        write_json_artifact(artifact_path, payload)
        loaded = __import__("json").loads(artifact_path.read_text(encoding="utf-8"))
        checks["artifact_finalization"] = loaded == payload

        zero_class = classify_result(
            errors=[], integrity_ok=True, raw_rows=2, fresh_launches=1,
            evaluated_mints=1, evaluations=evaluation_count, timers_executed=1,
            graceful_shutdown=True, restart_probe=True, candidates=0,
            completed_signal=False,
        )
        full_class = classify_result(
            errors=[], integrity_ok=True, raw_rows=2, fresh_launches=1,
            evaluated_mints=1, evaluations=1, timers_executed=1,
            graceful_shutdown=True, restart_probe=True, candidates=1,
            completed_signal=True,
        )
        fail_class = classify_result(
            errors=["INJECTED"], integrity_ok=True, raw_rows=2, fresh_launches=1,
            evaluated_mints=1, evaluations=1, timers_executed=1,
            graceful_shutdown=True, restart_probe=True, candidates=0,
            completed_signal=False,
        )
        checks["zero_candidate_classification"] = (
            zero_class == "PASS_PIPELINE_NO_CANDIDATE"
        )
        checks["early_success_classification"] = (
            full_class == "PASS_FULL_TRADE"
            and should_stop_early(MIN_RUNTIME_SECONDS, True)
            and not should_stop_early(MIN_RUNTIME_SECONDS - 0.001, True)
        )
        checks["duration_stop"] = (
            should_stop_for_duration(MAX_RUNTIME_SECONDS, MAX_RUNTIME_SECONDS)
            and not should_stop_for_duration(MAX_RUNTIME_SECONDS - 0.001, MAX_RUNTIME_SECONDS)
        )
        checks["errors_fail_closed"] = fail_class == "FAIL"

        stop_file = root / "child.stop"
        child = ManagedCollectorChild.start(
            (
                sys.executable,
                str(PROJECT_ROOT / "scripts" / "phase4_continuous_firstpullback_production_smoke_v0_1.py"),
                "--collector-child-probe", str(stop_file),
            ),
            child_log,
            stop_file=stop_file,
        )
        time.sleep(0.5)
        cleanup_ok = child.stop(timeout_seconds=5.0)
        checks["collector_child_cleanup"] = (
            cleanup_ok and child.process.poll() is not None
            and child.cleanup_method == "STOP_SENTINEL"
            and child.process.returncode == 0
            and not stop_file.exists()
        )

    print("=" * 112)
    print("CONTROLLED PRODUCTION FIRSTPULLBACK PAPER SMOKE HARNESS SELF-TEST v0.1")
    print("=" * 112)
    print(f"Model       : {MODEL_ID}")
    print(f"Fingerprint : {MODEL_FINGERPRINT}")
    print("Production  : NO")
    print("Network     : NO")
    print()
    ok = True
    for name, passed in checks.items():
        ok &= bool(passed)
        print(f"{name:<44}: {'PASS' if passed else 'FAIL'}")
    print()
    print(f"RESULT: {'PASS' if ok else 'FAIL'}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
