"""Fixed offline C1.2 producer workloads. No capacity search or restart campaign."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import sqlite3
import sys
import tempfile
from contextlib import closing
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "scripts")]
from live_continuous_producer_selftest_v0_2 import BASE, append, create_fixture, open_producer
from live.continuous_producer_v0_2 import ContinuationProfileV02, FEATURES, RUNS, RETIRED, LAUNCHES, TABLES
from live_operations_measurement_v0_1 import collect_record, collect_resources, measured, time_operation, unknown

CASES = {"lower": (64, 15, 1), "upper": (256, 31, 4)}
PROFILE = ContinuationProfileV02(active_mints=64, hot_mint_events=512,
    hot_mint_bytes=2*1024*1024, retained_events=1024, retained_bytes=8*1024*1024,
    pending_rows=8192, pending_bytes=16*1024*1024, history_rows=256,
    history_bytes=8*1024*1024, batch_rows=32)


def utc():
    return datetime.now(timezone.utc)


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def generation(producer):
    return producer.conn.execute("SELECT generation FROM live_producer_checkpoint_v0_1 WHERE singleton=1").fetchone()[0]


def build(fixture, case):
    hot_events, no_t0_mints, repeats = CASES[case]
    create_fixture(fixture)  # 33 original rows; includes sparse rowid 34.
    rows = []
    next_rowid = 35
    for mint in range(no_t0_mints):
        for _ in range(repeats):
            rows.append((next_rowid, f"C1-NOT0-{mint:03}", "LAUNCH", 10000, 1000 + len(rows)))
            next_rowid += 1
    rows.append((next_rowid, "C1-HOT-TRADE", "LAUNCH", 10000, 1999))
    next_rowid += 1
    for index in range(hot_events - 1):
        # A legitimate dense flat-price burst, not an elapsed-age qualification.
        rows.append((next_rowid, "C1-HOT-TRADE", "BUY" if index % 2 == 0 else "SELL", 10000, 2000))
        next_rowid += 1
    append(fixture, rows)
    return {"case": case, "raw_rows": 33 + len(rows), "captured_fence_rowid": next_rowid - 1,
            "hot_events": hot_events, "hot_trade_events": hot_events - 1,
            "no_t0_mints": no_t0_mints, "no_t0_events_each": repeats,
            "profile": asdict(PROFILE), "fixture_reference_utc": (BASE + timedelta(seconds=2060)).isoformat(),
            "clock_tick_utc": (BASE + timedelta(seconds=2001)).isoformat(),
            "schedule": "first 8 and last 8 raw inputs individually; middle batches <=32; one original clock tick after source drain",
            "bounds": "fixed two shapes; max80 operations per case; configured caps are not measured capacities"}


def snapshot(producer, fixture, workload, config, label, operation, publication_interval, timing):
    before = dict(producer.metrics), dict(producer.last_profile_usage), producer.conn.total_changes
    cursor = producer.durable_p1_rowid
    fence = config["captured_fence_rowid"]
    with closing(sqlite3.connect(f"file:{fixture.as_posix()}?mode=ro", uri=True)) as conn:
        row = conn.execute("SELECT count(*),min(decoded_at_utc) FROM pump_events WHERE rowid>? AND rowid<=?", (cursor, fence)).fetchone()
    backlog, oldest = row
    backlog_source = f"read-only pump_events count where {cursor}<rowid<={fence}; captured fixed synthetic source"
    observations = dict(timing)
    observations["backlog_rows"] = measured(backlog, backlog_source)
    reference = datetime.fromisoformat(config["fixture_reference_utc"])
    observations["backlog_age_seconds"] = (unknown("Empty source backlog has no oldest observed input") if oldest is None else
        measured((reference - datetime.fromisoformat(oldest)).total_seconds(),
                 "fixed synthetic UTC evaluation instant " + reference.isoformat() + " minus oldest decoded_at_utc=" + oldest + "; not workstation ingestion latency"))
    conn = producer.conn
    feature_bytes = conn.execute(f"SELECT coalesce(sum(length(CAST(event_json AS BLOB))),0) FROM {FEATURES}").fetchone()[0]
    observations["active_feature_payload_bytes"] = measured(feature_bytes, f"SQLite sum(length(CAST(event_json AS BLOB))) over retained {FEATURES}; UTF-8 database")
    files = sorted(p.name for p in workload.iterdir() if p.is_file())
    observations.update(collect_resources(workload, files))
    sample_at = utc()
    publication = None
    if publication_interval is not None:
        start, end = publication_interval
        publication = {"successful_operation_start_utc": start.isoformat(), "successful_operation_return_utc": end.isoformat(),
                       "sample_utc": sample_at.isoformat(), "age_lower_seconds": (sample_at-end).total_seconds(),
                       "age_upper_seconds": (sample_at-start).total_seconds(),
                       "definition": "Original successful final checkpoint commit occurred within this operation interval; UTC endpoint bounds, not mtime or generation-as-time"}
        observations["checkpoint_age_seconds"] = measured(publication["age_upper_seconds"],
            "conservative upper bound; sample UTC minus successful original operation start UTC; exact interval recorded in checkpoint_publication")
    if operation is not None and operation["kind"] == "single_input":
        observations["single_input_wall_seconds"] = measured(timing["operation_wall_seconds"].value,
            "one raw input invocation process_next_batch(batch_size=1), including original normalize/feature/transaction/checkpoint work; " + str(operation["rowids"]))
    record = collect_record(producer, workload_scope=f"C1.2 {config['case']} disposable actual producer workload; label={label}", observations=observations)
    token_states = producer._engine._tokens  # Read-only original states; never assigned.
    no_t0 = sorted(mint for mint, token in token_states.items() if token.first_tradable_event_at_us is None)
    feature_groups = [dict(row) for row in conn.execute(f"SELECT mint,count(*) AS events,sum(length(CAST(event_json AS BLOB))) AS payload_bytes FROM {FEATURES} GROUP BY mint ORDER BY mint")]
    output_counts = {row[0]: row[1] for row in conn.execute(f"SELECT event_type,count(*) FROM {TABLES[5]} GROUP BY event_type")}
    checkpoint = conn.execute("SELECT generation,manifest_json,manifest_digest FROM live_producer_checkpoint_v0_1 WHERE singleton=1").fetchone()
    record.update({"label": label, "operation": operation, "cursor_rowid": cursor, "captured_source_fence_rowid": fence,
        "checkpoint_generation": checkpoint[0], "checkpoint_manifest_digest": checkpoint[2], "checkpoint_publication": publication,
        "feature_groups": feature_groups, "no_t0_mints": no_t0, "producer_output_type_counts": output_counts,
        "physical_workload_file_inventory": [{"relative_path": name, "logical_file_length_bytes": (workload/name).stat().st_size} for name in files],
        "additional_logical_bytes": {
            "checkpoint_manifest_utf8_bytes": len(checkpoint[1].encode("utf-8")),
            "unfinished_run_json_utf8_bytes": conn.execute(f"SELECT coalesce(sum(length(CAST(run_json AS BLOB))),0) FROM {RUNS}").fetchone()[0],
            "retired_bundle_json_utf8_bytes": conn.execute(f"SELECT coalesce(sum(length(CAST(bundle_json AS BLOB))),0) FROM {RETIRED}").fetchone()[0]},
        "logical_bytes_provenance": "Read-only payload lengths; not page allocation, RSS, or an alternative continuation byte cap"})
    assert before == (dict(producer.metrics), dict(producer.last_profile_usage), producer.conn.total_changes)
    return record


def run(case, output):
    output = output.resolve()
    if not output.is_relative_to(Path(tempfile.gettempdir()).resolve()):
        raise ValueError("C1 evidence must be a new directory under system temp")
    output.mkdir(parents=True, exist_ok=False)
    workload = output / "workload"
    workload.mkdir()
    fixture, database = workload / "source.sqlite3", workload / "producer.sqlite3"
    config = build(fixture, case)
    write_json(output / "config.json", config)
    environment = {"python": sys.version, "executable": sys.executable, "platform": platform.platform(),
        "machine": platform.machine(), "processor": platform.processor(), "logical_cpu_count": os.cpu_count(),
        "sqlite": sqlite3.sqlite_version, "pid": os.getpid(), "argv": sys.argv,
        "scope": "Engineering workstation current process; no production-host approval; lifetime peak includes imports and sampling"}
    write_json(output / "environment.json", environment)
    opened_at = utc()
    conn, producer = open_producer(database, fixture, profile=PROFILE)
    opened_return = utc()
    samples = []
    try:
        samples.append(snapshot(producer, fixture, workload, config, "initialized", None, (opened_at, opened_return), {}))
        consumed = 0
        for operation_index in range(80):
            remaining = config["raw_rows"] - consumed
            if remaining == 0:
                break
            batch_size = 1 if consumed < 8 or remaining <= 8 else min(32, remaining-8)
            before_cursor, before_generation = producer.durable_p1_rowid, generation(producer)
            with closing(sqlite3.connect(f"file:{fixture.as_posix()}?mode=ro", uri=True)) as source_conn:
                inputs = [dict(zip(("rowid", "mint", "event_type"), row)) for row in source_conn.execute(
                    "SELECT rowid,mint,event_type FROM pump_events WHERE rowid>? AND rowid<=? ORDER BY rowid LIMIT ?",
                    (before_cursor, config["captured_fence_rowid"], batch_size))]
            started = utc()
            result, timing = time_operation(lambda: producer.process_next_batch(batch_size=batch_size),
                scope=f"actual producer process_next_batch(batch_size={batch_size}); before_cursor={before_cursor}; includes original transaction/checkpoint")
            ended = utc()
            assert result.raw_rows_fetched == len(inputs) == batch_size
            assert generation(producer) == before_generation + 1
            consumed += result.raw_rows_fetched
            operation = {"kind": "single_input" if batch_size == 1 else "bounded_batch", "batch_size": batch_size,
                "rowids": [item["rowid"] for item in inputs], "inputs": inputs, "result": asdict(result),
                "generation_before": before_generation, "generation_after": generation(producer)}
            samples.append(snapshot(producer, fixture, workload, config, f"input-operation-{operation_index:02}", operation, (started, ended), timing))
        else:
            raise AssertionError("fixed operation ceiling exceeded")
        assert consumed == config["raw_rows"]
        tick = datetime.fromisoformat(config["clock_tick_utc"])
        before_generation = generation(producer)
        started = utc()
        _, timing = time_operation(lambda: producer.submit_clock_tick(tick), scope="original prepare+execute producer clock tick at synthetic UTC " + tick.isoformat())
        ended = utc()
        assert generation(producer) == before_generation + 2
        samples.append(snapshot(producer, fixture, workload, config, "after-original-clock-tick",
            {"kind": "producer_clock_tick", "generation_before": before_generation, "generation_after": generation(producer)}, (started, ended), timing))
        final = samples[-1]
        expected_no_t0 = {f"C1-NOT0-{i:03}" for i in range(config["no_t0_mints"])}
        assert expected_no_t0 <= set(final["no_t0_mints"])
        hot = next((item for item in final["feature_groups"] if item["mint"] == "C1-HOT-TRADE"), None)
        assert hot is not None and hot["events"] == config["hot_events"], "truthful outcome differs: hot trade state retired or not fully retained"
        assert final["metrics"]["tombstone_rows"]["value"] > 0, "original fixture generated no tombstones; report gap"
        assert final["producer_output_type_counts"].get("CandidateEvaluationEvent", 0) > 0, "original candidate path absent; report gap"
        assert final["metrics"]["backlog_rows"]["value"] == 0
        assert conn.execute("PRAGMA quick_check").fetchone()[0] == "ok"
        retained = {row["mint"]: row["events"] for row in final["feature_groups"]}
    finally:
        conn.close()
        with (output / "samples.jsonl").open("w", encoding="utf-8") as stream:
            for sample in samples:
                stream.write(json.dumps(sample, sort_keys=True, allow_nan=False) + "\n")
    operations = [sample for sample in samples if sample["operation"]]
    singles = [sample for sample in operations if sample["operation"]["kind"] == "single_input"]
    batches = [sample for sample in operations if sample["operation"]["kind"] == "bounded_batch"]
    def maximum(metric, records=samples):
        values = [item["metrics"][metric]["value"] for item in records if item["metrics"][metric]["state"] == "MEASURED"]
        return max(values) if values else None
    summary = {"status": "MEASURED_PENDING_PROJECT_REVIEW", "case": case, "raw_rows": config["raw_rows"],
        "operation_count": len(operations), "single_input_samples": len(singles), "batch_samples": len(batches),
        "single_input_max_observed_wall_seconds": maximum("single_input_wall_seconds", singles),
        "single_input_max_observed_cpu_seconds": maximum("operation_cpu_seconds", singles),
        "batch_max_observed_wall_seconds": maximum("operation_wall_seconds", batches),
        "batch_max_observed_cpu_seconds": maximum("operation_cpu_seconds", batches),
        "total_operation_wall_seconds": sum(item["metrics"]["operation_wall_seconds"]["value"] for item in operations),
        "total_operation_cpu_seconds": sum(item["metrics"]["operation_cpu_seconds"]["value"] for item in operations),
        "max_observed": {name: maximum(name) for name in ("active_feature_events", "active_feature_payload_bytes", "hottest_mint_events", "hottest_mint_payload_bytes", "unfinished_mints", "pending_rows", "pending_bytes", "identity_tombstone_rows", "identity_tombstone_bytes", "process_rss_bytes", "process_lifetime_peak_rss_bytes", "workload_disk_bytes")},
        "min_sampled_disk_reserve_bytes": min(item["metrics"]["disk_reserve_bytes"]["value"] for item in samples if item["metrics"]["disk_reserve_bytes"]["state"] == "MEASURED"),
        "final_profile_usage": dict(producer.last_profile_usage), "final_generation": final["checkpoint_generation"],
        "retained_features_by_mint": retained, "final_no_t0_mints": final["no_t0_mints"],
        "producer_output_type_counts": final["producer_output_type_counts"],
        "unknown_next_stage": [name for name, fact in final["metrics"].items() if fact["state"] == "UNKNOWN"],
        "limits": "Observed samples for fixed synthetic dense flat-price bursts plus original fixture only. No tail percentile, latency guarantee, sustained duration, production-host sizing, configured-cap qualification, owned restart/protection, or Ledger-tail measurement."}
    write_json(output / "result.json", summary)
    code_files = {Path(module.__file__).resolve() for module in tuple(sys.modules.values())
                  if getattr(module, "__file__", None) and Path(module.__file__).suffix == ".py" and Path(module.__file__).resolve().is_relative_to(ROOT)}
    write_json(output / "source_hashes.json", {str(path.relative_to(ROOT)): sha256(path) for path in sorted(code_files)})
    write_json(output / "fixture_hashes.json", {str(path.relative_to(output)): sha256(path) for path in sorted(workload.iterdir()) if path.is_file()})
    print(json.dumps(summary, sort_keys=True, allow_nan=False))
    print("C1.3_INPUT=" + str(output))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", required=True, choices=tuple(CASES))
    parser.add_argument("--output", required=True, type=Path)
    arguments = parser.parse_args()
    run(arguments.case, arguments.output)
