"""C1.3: one finite composed case, initial owned startup and two replacements.

Original B3.2 APIs construct synthetic acquisition/due state; original B1/A2/C4
own spawning, acquisition and reconstruction. No network or execution ports are
supplied to measured children. All measurements include stated harness overhead.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import sqlite3
import sys
import tempfile
import time
from contextlib import closing
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from functools import partial
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "scripts")]
import live_operations_protection_recovery_selftest_v0_1 as original
from live_operations_producer_measurement_v0_1 import PROFILE, CASES, write_json, sha256
from live_operations_measurement_v0_1 import collect_record, collect_resources, measured, unknown
from live.continuous_producer_v0_2 import LiveContinuousProducerV02, FEATURES

b1, b21, rt, NOW = original.b1, original.b21, original.rt, original.NOW
hf, c4 = rt.hf, b1.a2.c4


def utc():
    return datetime.now(timezone.utc)


def atomic_json(path, value):
    temporary = path.with_suffix(".writing")
    write_json(temporary, value)
    temporary.replace(path)


def database_files(directory):
    return sorted(p.name for p in directory.iterdir() if p.is_file() and
                  (p.name.endswith(".sqlite") or p.name.endswith(".sqlite-wal") or p.name.endswith(".sqlite-shm")))


def boundary(started, directory, *, witness=None):
    runtime, repo = started.runtime, started.runtime.ledger
    producer = runtime.producer
    assert producer is not None, "original source reconstruction unavailable"
    before = dict(producer.metrics), dict(producer.last_profile_usage), producer.conn.total_changes
    checkpoint = dict(producer.conn.execute("SELECT * FROM live_producer_checkpoint_v0_1 WHERE singleton=1").fetchone())
    # Ledger has no compacted durable replay checkpoint: original reopen walks
    # ledger_commits from genesis. Referenced economic tables are separate bytes.
    head = dict(zip(("revision", "last_receipt_digest"), repo._conn.execute(
        "SELECT revision,last_receipt_digest FROM ledger_head WHERE singleton=1").fetchone()))
    count, payload_bytes = repo._conn.execute(
        "SELECT count(*),coalesce(sum(length(CAST(payload_json AS BLOB))),0) FROM ledger_commits WHERE seq>0 AND seq<=?",
        (head["revision"],)).fetchone()
    feature_bytes = producer.conn.execute(f"SELECT coalesce(sum(length(CAST(event_json AS BLOB))),0) FROM {FEATURES}").fetchone()[0]
    source = producer.market_source
    with closing(sqlite3.connect(Path(source.db_path).resolve().as_uri()+"?mode=ro", uri=True)) as raw:
        fence = raw.execute("SELECT max(rowid) FROM pump_events").fetchone()[0]
        backlog = raw.execute("SELECT count(*) FROM pump_events WHERE rowid>? AND rowid<=?", (producer.durable_p1_rowid, fence)).fetchone()[0]
    observations = collect_resources(directory, database_files(directory))
    observations.update({
        "journal_tail_rows": measured(count, "ledger_commits seq>0 through captured ledger_head.revision=" + str(head["revision"]) + "; original complete genesis replay, includes resolved commits"),
        "journal_tail_bytes": measured(payload_bytes, "sum UTF-8 ledger_commits.payload_json bytes in the same genesis-to-head tail; excludes referenced indexed-table payloads, schema, page/WAL allocation"),
        "active_feature_payload_bytes": measured(feature_bytes, "sum UTF-8 retained FEATURES.event_json bytes"),
        "backlog_rows": measured(backlog, f"actual raw source count cursor {producer.durable_p1_rowid}<rowid<=captured fence {fence}"),
        "backlog_age_seconds": unknown("Empty source backlog has no oldest input" if backlog == 0 else "No backlog-age clock-domain observation supplied")})
    publication = None
    if witness is not None and checkpoint["generation"] == witness["published_generation"]:
        sampled = utc()
        publication = {**witness, "sample_utc": sampled.isoformat(),
            "age_lower_seconds": (sampled-datetime.fromisoformat(witness["end_utc"])).total_seconds(),
            "age_upper_seconds": (sampled-datetime.fromisoformat(witness["start_utc"])).total_seconds()}
        observations["checkpoint_age_seconds"] = measured(publication["age_upper_seconds"],
            "upper age bound from witnessed successful original startup/setup publication interval; interval is separately recorded")
    record = collect_record(producer, workload_scope="C1.3 actual owned ColdRuntime boundary; current process and explicitly listed synthetic databases", observations=observations)
    record["metrics"]["journal_tail_rows"]["definition"] = "Full original Ledger reconstruction tail: ledger_commits seq>0 through captured ledger_head.revision; includes resolved history."
    record["metrics"]["journal_tail_bytes"]["definition"] = "UTF-8 payload_json bytes of exactly that Ledger commit tail; excludes referenced table payloads and file allocation."
    reconstruction = asdict(runtime.reconstruction_facts())
    binding = runtime.position_binding
    snapshot = repo.consumer_snapshot()
    admission_rows = repo._conn.execute("SELECT count(*) FROM ledger_authority_admissions").fetchone()[0]
    record.update({"pid": os.getpid(), "runtime_type": type(runtime).__name__,
        "startup_audit": asdict(started.audit), "reconstruction": reconstruction,
        "position_binding": asdict(binding), "positions": [asdict(position) for position in snapshot["positions"]],
        "protection": asdict(rt.protective_outcome(repo, binding)), "admission_rows": admission_rows,
        "checkpoint": {"generation": checkpoint["generation"], "digest": checkpoint["manifest_digest"],
                       "manifest_utf8_bytes": len(checkpoint["manifest_json"].encode("utf-8"))},
        "checkpoint_publication": publication, "ledger_tail": {"after_seq_exclusive": 0, "through_seq_inclusive": head["revision"],
            "through_commit_digest": head["last_receipt_digest"], "rows": count, "payload_utf8_bytes": payload_bytes,
            "exclusions": "Referenced indexed-table payloads, writer-generation rows, schema, SQLite page/WAL/SHM allocation; producer pending is a separate metric"},
        "producer_lineage": producer.source_identity, "producer_cursor": producer.durable_p1_rowid,
        "source_fence": fence, "source_path": str(source.db_path), "producer_profile_usage": dict(producer.last_profile_usage),
        "no_t0_mints": sorted(mint for mint, token in producer._engine._tokens.items() if token.first_tradable_event_at_us is None),
        "feature_groups": [dict(row) for row in producer.conn.execute(f"SELECT mint,count(*) AS events,sum(length(CAST(event_json AS BLOB))) AS payload_bytes FROM {FEATURES} GROUP BY mint ORDER BY mint")],
        "database_inventory": [{"relative_path": name, "logical_file_length_bytes": (directory/name).stat().st_size} for name in database_files(directory)]})
    assert before == (dict(producer.metrics), dict(producer.last_profile_usage), producer.conn.total_changes)
    return record


@dataclass
class Inputs:
    directory: Path
    entry: object
    sample_number: int = 1
    initiation_utc: str = ""
    producer_generation_before: int = 0

    # Original external fixture clock; never replace Runtime/control truth.
    clock = original.Inputs.clock

    def __call__(self, started):
        try:
            return self._collect_and_supply(started)
        except Exception as exc:
            atomic_json(self.directory/f"sample-{self.sample_number}-failure.json",
                {"exception": type(exc).__name__, "message": str(exc), "pid": os.getpid()})
            # Keep a failed collector alive at its exact retained handle until
            # the parent stops it; a collector error is never a restart sample.
            b1.wait_file(self.directory/f"never-release-error-{self.sample_number}")
            raise

    def _collect_and_supply(self, started):
        entered_wall, entered_cpu, entered_utc = time.perf_counter(), time.process_time(), utc()
        if not hasattr(self, "started"):
            self.started, self.now, self.clock_calls = started, NOW+16, 0
            generation = started.runtime.producer.conn.execute("SELECT generation FROM live_producer_checkpoint_v0_1 WHERE singleton=1").fetchone()[0]
            # Original handoff constructor and final ColdRuntime transaction
            # each publish; observe their original combined startup boundary.
            assert generation == self.producer_generation_before + 2
            self.publication = {"start_utc": self.initiation_utc, "end_utc": entered_utc.isoformat(),
                "published_generation": generation,
                "provenance": "Successful original ColdRuntime producer transaction during owned startup; parent initiation to first callback interval includes spawn/import and replacement termination where applicable"}
            before = boundary(started, self.directory, witness=self.publication)
            barrier = original.readiness.evaluate(started, clock=lambda: rt.a3.clock(started.runtime.ledger, NOW+16), entry=self.entry)
            before.update({"first_callback_utc": entered_utc.isoformat(), "first_callback_perf_counter": entered_wall,
                "process_lifetime_cpu_at_first_callback_seconds": entered_cpu, "readiness": asdict(barrier)})
            atomic_json(self.directory/f"sample-{self.sample_number}-before.json", before)
            self.step_start_wall, self.step_start_cpu = time.perf_counter(), time.process_time()
            return dict(clock=self.clock, source_cut_utc=rt.a3.utc(NOW+1), entry=self.entry)
        after = boundary(started, self.directory, witness=self.publication)
        after.update({"callback_interval_wall_seconds": entered_wall-self.step_start_wall,
            "callback_interval_cpu_seconds": entered_cpu-self.step_start_cpu,
            "interval_scope": "first callback return boundary to second callback entry; includes original Runtime step and supervisor IPC/scheduling, excludes boundary sampling"})
        atomic_json(self.directory/f"sample-{self.sample_number}-after.json", after)
        # At most one measured Runtime step per child. Parent retains the exact
        # handle and terminates it before any release file can be supplied.
        b1.wait_file(self.directory/f"never-release-{self.sample_number}")
        raise AssertionError("C1 child must not execute a second work unit")


class ChildBoundSupervisor(b1.supervisor.OperationsSupervisor):
    """Harness launch budget; every allowed launch delegates to original B1."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.launch_attempts, self.child_pids = 0, []

    def _launch(self, now_us, mono):
        # Check before original B1 can create/start a process, including a race
        # in which it observes a death after the Driver's preceding check.
        if self.launch_attempts >= 3:
            raise AssertionError("C1 harness permits at most three child launch attempts")
        self.launch_attempts += 1
        result = super()._launch(now_us, mono)
        if self.process is not None:
            self.child_pids.append(self.process.pid)
        return result


class GuardedDriver(b1.Driver):
    """Original Driver with measurement-error stop, not a recovery retry loop."""
    def __init__(self, supervisor, directory, now):
        super().__init__(supervisor, now=now)
        self.directory, self.deliberately_terminated, self.children = directory, set(), set()

    def tick(self):
        failures = list(self.directory.glob("sample-*-failure.json"))
        if failures:
            raise AssertionError("collector failed; no automatic measurement retry: " + failures[0].read_text())
        process = self.sup.process
        if process is not None and process.exitcode is not None and process.pid not in self.deliberately_terminated:
            raise AssertionError("unexpected measured child exit; no automatic measurement retry")
        result = super().tick()
        if self.sup.process is not None:
            self.children.add(self.sup.process.pid)
        assert len(self.children) <= 3, "measurement child ceiling exceeded"
        return result


def build_loaded_fixture(output):
    # Configuration-only adapters for existing fixture factories. All producer,
    # owned startup, acquisition and exit methods retain their original code.
    with patch.object(hf, "LiveContinuousProducerV02", partial(LiveContinuousProducerV02, profile=PROFILE)), \
         patch.object(c4, "ContinuationProfileV02", lambda: PROFILE):
        place, unused, _ = original.setup(output, "acquired-open")
    configuration = unused._configuration
    control = unused.store.snapshot()
    started = b1.a2.startup.start_live(**configuration, process_identity="c1-disposable-load-"+str(os.getpid()),
        now_us=control["last_control_us"]+1, replace_generation=control["generation"])
    try:
        producer = started.runtime.producer
        source_path = Path(producer.market_source.db_path)
        hot_events, no_t0_mints, repeats = CASES["upper"]
        with closing(sqlite3.connect(source_path)) as conn, conn:
            first = conn.execute("SELECT max(rowid)+1 FROM pump_events").fetchone()[0]
            rows = [(f"C1-NOT0-{mint:03}", "LAUNCH", 26) for mint in range(no_t0_mints) for _ in range(repeats)]
            rows += [("C1-HOT-TRADE", "LAUNCH", 27)]
            rows += [("C1-HOT-TRADE", "BUY" if index % 2 == 0 else "SELL", 28) for index in range(hot_events-1)]
            for rowid, (mint, kind, offset) in enumerate(rows, first):
                hf.raw_fixture.insert_row(conn, rowid, mint, kind, price_units=10000, observed_offset=offset)
                conn.execute("UPDATE pump_events SET inserted_at_utc=decoded_at_utc WHERE rowid=?", (rowid,))
                conn.execute("INSERT INTO websocket_observations VALUES(?,?,?,?,?)", (f"sig-{rowid}", 5000000+rowid, hf.at(offset), 1, "{}"))
        consumed = 0
        publication = None
        for _ in range(80):
            remaining = len(rows)-consumed
            if remaining == 0:
                break
            start = utc()
            result = producer.process_next_batch(batch_size=min(32, remaining))
            end = utc()
            assert result.raw_rows_fetched > 0
            consumed += result.raw_rows_fetched
            publication = {"start_utc": start.isoformat(), "end_utc": end.isoformat(),
                "published_generation": producer.conn.execute("SELECT generation FROM live_producer_checkpoint_v0_1 WHERE singleton=1").fetchone()[0],
                "provenance": "Original successful setup producer batch transaction"}
        else:
            raise AssertionError("finite setup operation ceiling exceeded")
        assert consumed == len(rows)
        binding = started.runtime.position_binding
        original.composition.commit_exit_evaluation(started.runtime.ledger, binding,
            command_id="c1-original-due-obligation", timer_fence_utc=rt.a3.utc(NOW+16))
        original.composition.ensure_protective_obligation(started.runtime.ledger, binding, recorded_at_utc=rt.a3.utc(NOW+16))
        baseline = boundary(started, place, witness=publication)
        assert baseline["producer_profile_usage"]["hottest_events"] == hot_events
        assert all(f"C1-NOT0-{mint:03}" in baseline["no_t0_mints"] for mint in range(no_t0_mints))
        atomic_json(output/"loaded-boundary.json", baseline)
        generation = started.audit.owner_fence.generation
        entry = unused._external_inputs.entry
    finally:
        started.close()
    return place, configuration, generation, entry, baseline


def validate_boundaries(baseline, before, after):
    """Compare serialized evidence, never tuple-rich live dataclasses to JSON."""
    for facts in (before, after):
        reconstruction = facts["reconstruction"]
        assert facts["runtime_type"] == "ColdRuntimeV01" and reconstruction["source_state"] == "SOURCE_RECONSTRUCTED"
        assert facts["producer_lineage"] == baseline["producer_lineage"]
        assert facts["producer_cursor"] == baseline["producer_cursor"]
        assert facts["position_binding"] == baseline["position_binding"]
        assert reconstruction["remaining_units"] == baseline["reconstruction"]["remaining_units"] > 0
        assert reconstruction["obligation_id"] == baseline["reconstruction"]["obligation_id"]
        assert facts["admission_rows"] == baseline["admission_rows"]
        assert facts["producer_profile_usage"] == baseline["producer_profile_usage"]
        assert facts["metrics"]["backlog_rows"]["value"] == 0
        assert not facts["startup_audit"]["grants_permission"]
    assert before["reconstruction"]["initial_handoff_replay_deferred"]
    assert before["readiness"]["protective"]["due"]
    assert before["readiness"]["entry"]["state"] == "HELD"
    assert "EXISTING_EXPOSURE_REQUIRES_PROTECTION_AND_RETIREMENT_FIRST" in before["readiness"]["entry"]["reasons"]
    assert "CURRENT_RECONSTRUCTED_SOURCE_REQUIRED" not in before["readiness"]["entry"]["reasons"]


def run(output, c12):
    output, c12 = output.resolve(), c12.resolve()
    if not output.is_relative_to(Path(tempfile.gettempdir()).resolve()):
        raise ValueError("new disposable C1.3 output must be under temp")
    # C1.2 is input only; verify and bind its existing source and fixture hashes.
    for case in ("lower", "upper"):
        for manifest, base in (("source_hashes.json", ROOT), ("fixture_hashes.json", c12/case)):
            hashes = json.loads((c12/case/manifest).read_text())
            assert all(sha256(base/path) == digest for path, digest in hashes.items())
    output.mkdir(parents=True, exist_ok=False)
    write_json(output/"environment.json", {"python": sys.version, "executable": sys.executable,
        "platform": platform.platform(), "processor": platform.processor(), "cpu_count": os.cpu_count(),
        "sqlite": sqlite3.sqlite_version, "parent_pid": os.getpid(), "start_method": "spawn", "argv": sys.argv,
        "perf_counter": vars(time.get_clock_info("perf_counter")),
        "scope": "Engineering workstation; child wall includes spawn/import/cache/harness overhead; OS working-set peak is child-process lifetime only"})
    place, configuration, generation, entry, baseline = build_loaded_fixture(output)
    # The same persisted representation is authoritative on both sides of the
    # process boundary (SourceBinding.anchors is a tuple before JSON encoding).
    baseline = json.loads((output/"loaded-boundary.json").read_text())
    write_json(output/"config.json", {"producer_profile": asdict(PROFILE), "upper_shape": CASES["upper"],
        "source_timeline": "Original acquired fixture; no-t0 at NOW+12, hot LAUNCH at NOW+13, 255 flat BUY/SELL at NOW+14; due consumer NOW+16",
        "original_source_path": str(configuration["market_source"].db_path), "startup_identity": asdict(configuration["expected_identity"]),
        "children": 3, "initial_owned_startups": 1, "actual_replacements": 2,
        "fixture_origin": "Original B3.2 setup('acquired-open') and original acquired/due APIs; producer shape rebuilt, no checkpoint/source identity rebinding"})
    inputs = Inputs(place, entry)
    supervisor = ChildBoundSupervisor(configuration, inputs,
        b1.supervisor.HealthProfile(20000000,20000000,1000000), expected_generation=generation)
    driver = GuardedDriver(supervisor, place, now=generation+1)
    samples = []
    try:
        for number in (1,2,3):
            inputs.sample_number = number
            inputs.producer_generation_before = baseline["checkpoint"]["generation"] if number == 1 else samples[-1]["after"]["checkpoint"]["generation"]
            started_wall, started_utc = time.perf_counter(), utc()
            inputs.initiation_utc = started_utc.isoformat()
            terminated = None
            if number > 1:
                previous_pid = supervisor.process.pid
                supervisor.process.terminate()
                supervisor.process.join(timeout=5)
                assert not supervisor.process.is_alive()
                terminated = {"pid": previous_pid, "exitcode": supervisor.process.exitcode,
                    "confirmed_dead_wall_seconds": time.perf_counter()-started_wall}
                driver.deliberately_terminated.add(previous_pid)
                driver.now += 1
            result = driver.until(lambda fact: fact.completed_steps >= number)
            ended_wall, ended_utc = time.perf_counter(), utc()
            assert result.last_work == ("PROTECTIVE_ACTION_STAGED" if number == 1 else "NEED_EXECUTION")
            after_path = place/f"sample-{number}-after.json"
            b1.wait_file(after_path)
            before = json.loads((place/f"sample-{number}-before.json").read_text())
            after = json.loads(after_path.read_text())
            validate_boundaries(baseline, before, after)
            duration = ended_wall-started_wall
            before["metrics"]["restart_duration_seconds"] = {"state": "MEASURED", "value": duration, "unit": "seconds",
                "definition": "initial owned launch or exact retained-child termination initiation through supervisor confirmation of first protective Runtime result",
                "provenance": "parent perf_counter; includes spawn/import/startup/callback evidence/IPC/polling; sample kind is explicit"}
            sample = {"number": number, "kind": "initial_owned_startup" if number == 1 else "actual_replacement",
                "initiated_utc": started_utc.isoformat(), "confirmed_result_utc": ended_utc.isoformat(),
                "observed_duration_seconds": duration,
                "startup_to_first_callback_upper_seconds": before["first_callback_perf_counter"]-started_wall,
                "terminated_previous_child": terminated, "supervision": asdict(result), "before": before, "after": after}
            samples.append(sample)
            atomic_json(output/f"owned-sample-{number}.json", sample)
    finally:
        b1.cleanup(supervisor)
        atomic_json(output/"child-cleanup.json", {"launch_attempts": supervisor.launch_attempts,
            "child_pids": supervisor.child_pids, "retained_process_handle_cleared": supervisor.process is None,
            "retained_channel_cleared": supervisor._channel is None,
            "method": "Original B1 cleanup of retained exact child handle; previous replacement handles confirmed dead by join"})
    code_files = {Path(module.__file__).resolve() for module in tuple(sys.modules.values())
                  if getattr(module,"__file__",None) and Path(module.__file__).suffix == ".py" and Path(module.__file__).resolve().is_relative_to(ROOT)}
    write_json(output/"source_hashes.json", {str(path.relative_to(ROOT)): sha256(path) for path in sorted(code_files)})
    write_json(output/"fixture_hashes.json", {str(path.relative_to(output)): sha256(path) for path in sorted(place.iterdir()) if path.is_file() and path.name in database_files(place)})
    c12_artifacts = {str(path.relative_to(c12)): sha256(path) for case in ("lower","upper") for path in sorted((c12/case).iterdir()) if path.is_file()}
    write_json(output/"c12_artifact_hashes.json", c12_artifacts)
    all_boundaries = [baseline] + [record for sample in samples for record in (sample["before"],sample["after"])]
    metric_names = ("active_feature_events","active_feature_payload_bytes","hottest_mint_events","hottest_mint_payload_bytes",
        "unfinished_mints","identity_tombstone_rows","identity_tombstone_bytes","pending_rows","pending_bytes",
        "journal_tail_rows","journal_tail_bytes","process_rss_bytes","process_lifetime_peak_rss_bytes","workload_disk_bytes")
    maxima = {name:max(record["metrics"][name]["value"] for record in all_boundaries if record["metrics"][name]["state"] == "MEASURED") for name in metric_names}
    # Parent setup resources are retained separately; child RSS maxima below do
    # not quietly include setup or classify point samples as operation peaks.
    child_boundaries = [record for sample in samples for record in (sample["before"],sample["after"])]
    summary = {"version":"operations_c1_owned_measurement_v0.1", "status":"IMPLEMENTED_PENDING_PROJECT_REVIEW",
        "sample_count":3, "initial_owned_startup_seconds":samples[0]["observed_duration_seconds"],
        "actual_replacement_seconds":[sample["observed_duration_seconds"] for sample in samples[1:]],
        "max_observed_actual_replacement_seconds":max(sample["observed_duration_seconds"] for sample in samples[1:]),
        "max_observed_all_three_endpoint_seconds":max(sample["observed_duration_seconds"] for sample in samples),
        "first_runtime_results":[sample["supervision"]["last_work"] for sample in samples],
        "observed_entry_hold_reasons":[sample["before"]["readiness"]["entry"]["reasons"] for sample in samples],
        "actual_child_count":len(supervisor.child_pids), "child_launch_attempts":supervisor.launch_attempts,
        "max_observed_all_boundary_metrics":maxima,
        "max_child_sampled_rss_bytes":max(record["metrics"]["process_rss_bytes"]["value"] for record in child_boundaries),
        "max_child_os_lifetime_peak_rss_bytes":max(record["metrics"]["process_lifetime_peak_rss_bytes"]["value"] for record in child_boundaries),
        "min_sampled_disk_reserve_bytes":min(record["metrics"]["disk_reserve_bytes"]["value"] for record in all_boundaries),
        "loaded_producer_profile_usage":baseline["producer_profile_usage"],
        "entry_admission_rows_before_and_after":baseline["admission_rows"],
        "source_reconstruction":"SOURCE_RECONSTRUCTED in all measured child boundaries; source lineage/cursor and retained producer profile facts preserved",
        "limits":"One synthetic acquired/due economic shape, upper dense producer shape, one initial startup/two replacements on engineering workstation. No live network or measured-child execution ports. OS caches uncontrolled; process imports and instrumentation included. No hard worst-case, configured-cap qualification, production-host approval, sustained workload or full execution lifecycle claim."}
    write_json(output/"result.json", summary)
    print(json.dumps(summary,sort_keys=True))
    print("C1.3_EVIDENCE="+str(output))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--c12-evidence", type=Path, required=True)
    args = parser.parse_args()
    run(args.output,args.c12_evidence)
