"""Small offline C1 contract smoke; no load, restart or qualification campaign."""
from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
from contextlib import closing
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "scripts")]

from live_continuous_producer_selftest_v0_2 import empty_source, append, open_producer
from live_operations_measurement_v0_1 import (
    Observation, collect_record, collect_resources, measured, time_operation, unknown,
)


def reject(call):
    try:
        call()
    except ValueError:
        return
    raise AssertionError("invalid measurement accepted")


def main():
    with tempfile.TemporaryDirectory(prefix="operations-c1-contract-") as directory:
        root = Path(directory)
        fixture, database = root / "source.sqlite3", root / "producer.sqlite3"
        empty_source(fixture)
        append(fixture, [(1, "C1_SYNTHETIC", "LAUNCH", 100, 0),
                         (2, "C1_SYNTHETIC", "BUY", 101, 1)])
        conn, producer = open_producer(database, fixture)
        try:
            batch, timing = time_operation(lambda: producer.process_next_batch(batch_size=2),
                                           scope="two-row synthetic producer batch, including its transaction/checkpoint work")
            assert batch.raw_rows_fetched == 2
            before = dict(producer.metrics), dict(producer.last_profile_usage), conn.total_changes
            checkpoint_before = tuple(conn.execute("SELECT * FROM live_producer_checkpoint_v0_1").fetchone())
            inventory = sorted(p.name for p in root.iterdir() if p.is_file())
            resources = collect_resources(root, inventory)
            record = collect_record(producer, workload_scope="Disposable two-row synthetic C1.1 contract fixture; this Python process only",
                                    observations={**timing, **resources})
            assert before == (dict(producer.metrics), dict(producer.last_profile_usage), conn.total_changes)
            assert checkpoint_before == tuple(conn.execute("SELECT * FROM live_producer_checkpoint_v0_1").fetchone())
            metrics = record["metrics"]
            assert metrics["active_feature_events"]["value"] == producer.last_profile_usage["retained_events"]
            assert metrics["active_feature_events"]["value"] == 2
            assert metrics["retained_state_bytes"]["value"] == producer.last_profile_usage["retained_bytes"]
            assert metrics["identity_tombstone_rows"]["value"] == producer.last_profile_usage["launch_rows"] + producer.last_profile_usage["retired_rows"]
            assert record["configured_profile"]["retained_bytes"]["state"] == "CONFIGURED"
            assert all(fact["state"] == "MEASURED" for fact in record["producer_counters"].values())
            for name in ("active_feature_payload_bytes", "journal_tail_rows", "journal_tail_bytes", "backlog_rows",
                         "backlog_age_seconds", "checkpoint_age_seconds", "single_input_wall_seconds", "restart_duration_seconds"):
                assert metrics[name]["state"] == "UNKNOWN" and metrics[name]["value"] is None
            assert metrics["operation_wall_seconds"]["value"] >= 0
            assert metrics["operation_cpu_seconds"]["value"] >= 0
            assert resources["process_rss_bytes"].state in ("MEASURED", "UNKNOWN")
            assert resources["workload_disk_bytes"].value == sum((root / p).stat().st_size for p in inventory)
            assert resources["disk_reserve_bytes"].value > 0
            assert collect_resources(root, ["missing.sqlite3"])["workload_disk_bytes"].state == "UNKNOWN"
            assert collect_resources(root, [])["workload_disk_bytes"].value is None
            reject(lambda: collect_resources(root, ["../outside.sqlite3"]))
            reject(lambda: collect_resources(root, [inventory[0], inventory[0]]))
            reject(lambda: Observation("UNKNOWN", 0, "missing"))
            reject(lambda: measured(float("nan"), "invalid"))
            reject(lambda: measured(True, "invalid"))
            reject(lambda: measured(-1, "invalid"))
            reject(lambda: measured(1, ""))
            reject(lambda: collect_record(producer, workload_scope="fixture", observations={"pending_rows": measured(0, "override")}))
            reject(lambda: collect_record(producer, workload_scope="fixture", observations={"backlog_rows": Observation("CONFIGURED", 2, "not observed")}))
            reject(lambda: collect_record(producer, workload_scope="fixture", observations={"backlog_rows": measured(0.5, "invalid fractional row count")}))
            missing = collect_record(SimpleNamespace(profile=producer.profile, metrics={}), workload_scope="missing-fact contract probe")
            assert missing["metrics"]["active_feature_events"]["value"] is None
            assert missing["metrics"]["identity_tombstone_rows"]["state"] == "UNKNOWN"
            with closing(sqlite3.connect(f"file:{fixture.as_posix()}?mode=ro", uri=True)) as source_conn:
                backlog = source_conn.execute("SELECT count(*) FROM pump_events WHERE rowid > ? AND rowid <= 2", (producer.durable_p1_rowid,)).fetchone()[0]
            supplied = collect_record(producer, workload_scope="fixture", observations={"backlog_rows": measured(backlog, "read-only synthetic source count: durable cursor < rowid <= captured fixture fence 2"),
                                                                                       "backlog_age_seconds": unknown("empty backlog has no oldest input")})
            assert supplied["metrics"]["backlog_rows"]["value"] == 0
            json.dumps(record, allow_nan=False)
            print("C1.1 focused smoke: producer facts/counters/checkpoint unchanged by collection; explicit configured/measured/unknown; resource/timing provenance; invalid records rejected")
            print("Synthetic rows=2; metrics=" + str(len(metrics)) + "; RSS=" + resources["process_rss_bytes"].state + "; no qualification result")
        finally:
            conn.close()


if __name__ == "__main__":
    main()
