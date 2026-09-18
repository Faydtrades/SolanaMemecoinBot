"""Step10 S10: actual canonical producer -> fixed DRY graph -> durable release.

Only collector rows, public RPC/wallet responses and operator/clock facts are
synthetic. No candidate/admission/attempt/settlement injection, key or network.
Accepted Step8C/L6 preparation cuts and graph guards are reused, not rerun.
"""
from __future__ import annotations

import argparse
import ast
import base64
import hashlib
import json
import os
import sqlite3
import subprocess
import sys
from contextlib import closing
from dataclasses import asdict, replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import httpx
import live_candidate_handoff_selftest_v0_1 as source_fixture
import live_runtime_dry_selftest_v0_1 as accepted_dry
from live.ledger_ports_v0_1 import port_receipt_from_record
from live.acceptance_dossier_v0_1 import _lf  # git-LF content hash, CRLF checkout invariant

ROOT = Path(__file__).resolve().parents[1]
a3, a4, sf, dry = accepted_dry.a3, accepted_dry.a4, accepted_dry.sf, accepted_dry.dry
LedgerRepository = accepted_dry.LedgerRepository
NOW = a3.NOW
CHECKS = {}


def check(name, value):
    CHECKS[name] = bool(value)
    if not value:
        raise AssertionError(name)


def save(path, value):
    path.write_text(accepted_dry.canonical_json(value) + "\n", encoding="utf-8")


def journal(f):
    snapshot = f.repo.consumer_snapshot()
    return {
        "audit": f.repo.audit(), "funding": asdict(snapshot["funding"]),
        "reservations": [asdict(value) for value in snapshot["reservations"]],
        "positions": [asdict(value) for value in snapshot["positions"]],
        "protections": [asdict(value) for value in snapshot["protections"]],
        "mutation_lane": f.repo.mutation_lane(),
    }


class Fixture(source_fixture.Fixture):
    """Reuse raw-source helpers without the older fixture's LIVE initializer."""

    def __init__(self, directory, name):
        self.directory, self.name = directory, name
        self.raw, self.path, self.ppath = (
            directory / (name + suffix)
            for suffix in ("-raw.sqlite", "-ledger.sqlite", "-producer.sqlite")
        )
        self.binding = source_fixture.build_raw(self.raw)
        self.domain = a3.domain(wallet=sf.WALLET, mode="DRY")
        self.repo = LedgerRepository.initialize(self.path, self.domain)
        self.scenario = a3.Scenario()
        self.scenario.accounts = {sf.WALLET: a3.account(a4.venue.SYSTEM_PROGRAM_ID,
            lamports=self.domain.known_native_wallet_lamports)}
        a3.ingest(self.repo, a3.observe(self.scenario,
            request=a3.WalletEvidenceRequest(sf.WALLET, a3.GENESIS, 100)))
        self.source = source_fixture.SourceEvidenceStore(
            directory / (name + "-evidence.sqlite"), self.binding, source_fixture.SourceProfile())
        self.baseline = journal(self)
        self.append([(rowid, sf.MINT if mint == source_fixture.MINT_A else mint, kind, price)
            for rowid, mint, kind, price in source_fixture.FIRST])
        self.open_producer(initialize=True)
        self.produce()
        roots = self.deliver()
        check(name + "_one_actual_handoff_root", len(roots) == 1)
        self.root, self.item = roots[0], self.repo.candidate(roots[0])
        check(name + "_actual_source_health", self.evidence().disposition == "HEALTHY")
        legacy = a3.a1.policy(self.item, binding=self.domain)
        units = 100000000  # Explicit synthetic public policy, before admission.
        self.policy = a3.policy_v02(replace(legacy,
            size=replace(legacy.size, fixed_quote_lamports=units, ceiling_quote_lamports=units,
                max_trade_notional_lamports=units, max_global_exposure_lamports=3 * units,
                max_mint_exposure_lamports=units),
            costs=replace(legacy.costs, venue_fee_within_quote_cap_lamports=2000000,
                setup_outflow_lamports=10000000, refundable_account_lock_lamports=10000000,
                protective_setup_lamports=10000000, protective_refundable_lock_lamports=10000000)))
        a3.control(self.repo, "INSTALL_POLICY", "install", policy_value=self.policy)
        a3.arm(self.repo, self.policy, scope="DRY")
        self.scenario.accounts[sf.MINT] = a3.account(
            a4.venue.TOKEN_PROGRAM_ID, sf.plans.fx.mint_account().data)
        self.admission = a3.admit(self, name + "-admit", support=a3.wallet(self, scenario=self.scenario))
        check(name + "_actual_DRY_admission", self.admission.accepted
            and self.admission.consumed_grant_id is None
            and self.repo.reservation(self.root).status == "TENTATIVE_DRY")
        self.action = self.repo.reservation(self.root).admission.action
        self.seed, _ = a4.evidence(self, self.action, self.scenario)


def source_lineage(f):
    original = next(row for row in f.producer.producer_events()
        if row["event_type"] == "CandidateEvaluationEvent"
        and json.loads(row["event_json"])["candidate"]["candidate_id"] == f.item.candidate_signal_id)
    input_digest = f.conn.execute(
        f"SELECT content_fingerprint FROM {source_fixture.TABLES[1]} WHERE input_key=?",
        ("P1:" + str(f.item.source_cursor),)).fetchone()[0]
    check(f.name + "_canonical_candidate_action_lineage",
        f.item.producer_lineage_id == f.producer.source_identity
        and f.item.source_record_digest == input_digest
        and f.item.trade_root(f.domain) == f.root == f.action.root_id
        and f.action.candidate_digest == f.item.content_digest
        and f.admission.eligibility.original.source == f.source.latest_record()[1])
    return {"producer_lineage": f.producer.source_identity, "source_binding": asdict(f.binding),
        "source_evidence_digest": f.source.latest_record()[1].content_digest,
        "source_input_digest": input_digest, "producer_output_digest": original["content_fingerprint"],
        "candidate": f.item.to_record(), "root_id": f.root, "action": f.action.to_record(),
        "policy_digest": f.policy.content_digest, "policy": asdict(f.policy),
        "admission_digest": f.admission.content_digest, "baseline": f.baseline,
        "admitted": journal(f)}


def exact_lineage(f, boundary):
    attempts = f.repo._root_attempts(f.action.root_id)
    check(f.name + "_one_exact_preparation", len(attempts) == 1)
    prep = attempts[0].preparation
    stages = f.repo.attempt_stage_inputs(prep.attempt_id)
    check(f.name + "_one_original_simulation", len(stages) == 1)
    stage = stages[0]
    witness = json.loads(stage.external_record_json)
    methods = tuple(request["method"] for request in boundary.requests)
    check(f.name + "_only_exact_public_read_sequence", methods == (
        "getGenesisHash", "getMultipleAccounts", "getMultipleAccounts", "getMultipleAccounts",
        "getLatestBlockhash", "getMultipleAccounts", "getFeeForMessage", "getBlockHeight",
        "isBlockhashValid", "simulateTransaction"))
    request = boundary.requests[-1]
    wire = base64.b64decode(request["params"][0])
    check(f.name + "_original_exact_unsigned_message",
        wire[:65] == b"\x01" + bytes(64) and wire[65:].hex() == prep.message_hex
        and request["params"][1]["replaceRecentBlockhash"] is False
        and request["params"][1]["sigVerify"] is False
        and prep.action_id == f.action.action_id
        and prep.action_content_digest == f.action.content_digest)
    check(f.name + "_original_simulation_witness",
        witness["preparation_digest"] == prep.content_digest
        and witness["request_digest"] == accepted_dry.content_fingerprint(request["params"])
        and witness["outcome"] == "SIMULATION_SUCCESS"
        and accepted_dry.content_fingerprint(witness) == stage.external_record_digest
        and dry.utc_microseconds(prep.prepared_at_utc) <= witness["observed_at_us"]
        <= dry.utc_microseconds(stage.recorded_at_utc))
    return {"preparation": prep.to_record(), "stage": stage.to_record(), "witness": witness,
        "public_read_methods": methods, "public_requests_digest": accepted_dry.content_fingerprint(boundary.requests),
        "simulation_request": request, "before_terminal": journal(f)}


def absent(f, action, count, label):
    accepted_dry.economic_absence(f, action, count, label)
    attempts = f.repo._root_attempts(action.root_id)
    check(label + "_no_authority_profile_or_signed_stage",
        f.repo.authority_message_profile(action.policy_digest) is None
        and all(not stage.has_real_authority_grant for attempt in attempts
            for stage in f.repo.attempt_stage_inputs(attempt.preparation.attempt_id)))


def no_read_replay(f, action, receipt, label):
    requests = []

    def forbidden(request):
        requests.append(json.loads(request.content)["method"])
        raise AssertionError("terminal replay attempted external read")

    before = journal(f)
    with accepted_dry.ExecutionReadOnlyRpc("https://invalid.local", a3.PROFILE,
            now_us=lambda: 0, transport=httpx.MockTransport(forbidden)) as rpc:
        replay = dry.run_dry(f.repo, action.action_id, rpc, None, None, None,
            accepted_dry.ComputeBudget(250000, 1000), clock=lambda: None, now_us=lambda: 0)
    check(label + "_same_canonical_terminal_no_reads_or_mutation",
        replay == receipt == port_receipt_from_record(json.loads(accepted_dry.canonical_json(receipt.to_record())))
        and not requests and journal(f) == before)


def continuation(f, receipt):
    first = f.action
    f.append(source_fixture.NEXT)
    f.produce()
    roots = f.deliver()
    check(f.name + "_fresh_canonical_next_candidate", len(roots) == 1 and roots[0] != first.root_id)
    f.root, f.item = roots[0], f.repo.candidate(roots[0])
    check(f.name + "_same_actual_producer_next_time", f.item.producer_lineage_id == f.producer.source_identity
        and f.item.generated_at_us == (NOW + 14) * 1000000
        and f.evidence(offset=32, cut=29).disposition == "HEALTHY")
    admission = a3.admit(f, f.name + "-next-admit", at=NOW + 18,
        support=a3.wallet(f, scenario=f.scenario, at=NOW + 18))
    check(f.name + "_released_capacity_allows_actual_next_admission", admission.accepted
        and admission.risk.outstanding_native_lamports == 0
        and f.repo.reservation(f.root).status == "TENTATIVE_DRY"
        and f.repo.port_receipt(receipt.ingestion_key) == receipt)
    action = f.repo.reservation(f.root).admission.action
    terminal = dry.finish_interrupted_dry(f.repo, action.action_id, recorded_at_utc=a3.utc(NOW + 19))
    check(f.name + "_unprepared_next_action_no_fabricated_attempt",
        terminal.dry_terminal.attempt_id is None
        and terminal.dry_terminal.reason == "DRY_INTERRUPTED_BEFORE_PREPARATION")
    absent(f, action, 1, f.name + "_next_release")
    return {"candidate": f.item.to_record(), "action": action.to_record(),
        "admission": asdict(admission), "terminal": terminal.to_record()}


def fixed_destinations(f):
    """Both legitimate graphs share codecs, never a destination or mode switch."""
    before = journal(f)
    dry_payload = f.conn.execute(f"SELECT payload FROM {source_fixture.CONFIG}").fetchone()[0]
    alternate = LedgerRepository.initialize(f.directory / "alternate-live-ledger.sqlite",
        replace(f.domain, mode="LIVE"))
    conn = source_fixture.connect_producer_db(f.directory / "alternate-live-producer.sqlite")
    try:
        for mode, ledger in (("LIVE", alternate), ("UNSUPPORTED", SimpleNamespace(domain=SimpleNamespace(mode="PAPER")))):
            try:
                source_fixture.CandidateHandoffV01(f.producer, ledger, f.binding,
                    database_identity=source_fixture.DATABASE_ID)
            except source_fixture.ProducerConflict as error:
                expected = ("handoff destination/source rebinding denied" if mode == "LIVE"
                    else "A3 requires fixed LIVE or DRY inbox domain")
                check(f.name + "_DRY_to_" + mode + "_denied", str(error) == expected)
            else:
                raise AssertionError("fixed DRY destination accepted rebinding")
        producer = source_fixture.LiveContinuousProducerV02(conn,
            source_fixture.ContinuousMarketSourceV02(f.raw, start_after_p1_rowid=1,
                database_identity=source_fixture.DATABASE_ID), wall_clock=lambda: source_fixture.BASE)
        source_fixture.CandidateHandoffV01(producer, alternate, f.binding,
            database_identity=source_fixture.DATABASE_ID, initialize=True)
        live_payload = conn.execute(f"SELECT payload FROM {source_fixture.CONFIG}").fetchone()[0]
        try:
            source_fixture.CandidateHandoffV01(producer, f.repo, f.binding,
                database_identity=source_fixture.DATABASE_ID)
        except source_fixture.ProducerConflict as error:
            check(f.name + "_LIVE_to_DRY_denied", str(error) == "handoff destination/source rebinding denied")
        else:
            raise AssertionError("fixed LIVE destination accepted rebinding")
        check(f.name + "_immutable_mode_bound_destinations",
            json.loads(dry_payload)["economic_domain_id"] == f.domain.economic_domain_id
            and json.loads(live_payload)["economic_domain_id"] == alternate.domain.economic_domain_id
            and f.domain.economic_domain_id != alternate.domain.economic_domain_id
            and f.conn.execute(f"SELECT payload FROM {source_fixture.CONFIG}").fetchone()[0] == dry_payload
            and conn.execute(f"SELECT payload FROM {source_fixture.CONFIG}").fetchone()[0] == live_payload
            and journal(f) == before and alternate.audit()["candidate_count"] == 0)
        return {"dry_mapping": json.loads(dry_payload), "live_mapping": json.loads(live_payload)}
    finally:
        conn.close()
        alternate.close()


def normal(directory):
    f = Fixture(directory, "source-dry")
    try:
        lineage = source_lineage(f)
        destinations = fixed_destinations(f)
        boundary = accepted_dry.DryPublicTransport(f.seed)
        original_finish = f.repo.finish_dry
        exact = {}

        def record_terminal(*args, **kwargs):
            exact.update(exact_lineage(f, boundary))
            return original_finish(*args, **kwargs)

        with patch.object(f.repo, "finish_dry", side_effect=record_terminal):
            receipt, _ = accepted_dry.run(f, f.action, f.seed, boundary=boundary)
        check(f.name + "_atomic_original_terminal", receipt.kind == "DRY_NON_SUBMITTED"
            and receipt.dry_terminal.reason == "DRY_CAPABILITY"
            and receipt.dry_terminal.preparation_digest == f.repo._root_attempts(f.root)[0].preparation.content_digest
            and receipt.dry_terminal.simulation_input_digest == accepted_dry.content_fingerprint(exact["stage"]))
        absent(f, f.action, 1, f.name + "_terminal")
        terminal_journal = journal(f)
        # A fresh handoff starts at the original retained output and redelivers it.
        f.conn.close()
        f.open_producer()
        check(f.name + "_repeat_actual_handoff", f.deliver() == [f.root] and journal(f) == terminal_journal)
        no_read_replay(f, f.action, receipt, f.name + "_duplicate")
        f.reopen()
        before = journal(f)
        check(f.name + "_fresh_durable_handoff", f.deliver() == [f.root] and journal(f) == before)
        no_read_replay(f, f.action, receipt, f.name + "_reopen")
        absent(f, f.action, 1, f.name + "_reopened")
        next_candidate = continuation(f, receipt)
        f.reopen()
        absent(f, f.action, 1, f.name + "_final_reopened")
        return {**lineage, **exact, "destinations": destinations,
            "terminal": receipt.to_record(), "terminal_journal": terminal_journal,
            "next_candidate": next_candidate, "final": journal(f)}
    finally:
        f.close()


def crash_child(directory, cut):
    f = Fixture(directory, cut)
    lineage = source_lineage(f)
    boundary = accepted_dry.DryPublicTransport(f.seed)
    original_finish = f.repo.finish_dry
    connection = f.repo._conn

    class CrashConnection:
        def __getattr__(self, name):
            return getattr(connection, name)

        def execute(self, sql, *args):
            if sql == "COMMIT" and cut == "before-terminal-commit":
                os._exit(91)
            value = connection.execute(sql, *args)
            if sql == "COMMIT" and cut == "after-terminal-commit":
                os._exit(92)
            return value

    def interrupt_terminal(*args, **kwargs):
        save(directory / (cut + "-binding.json"), {
            **lineage, **exact_lineage(f, boundary), "checks": CHECKS,
            "domain": f.domain.to_record(), "ledger_path": str(f.path),
            "producer_path": str(f.ppath), "raw_path": str(f.raw),
            "terminal_input": asdict(args[0])})
        f.repo._conn = CrashConnection()
        return original_finish(*args, **kwargs)

    with patch.object(f.repo, "finish_dry", side_effect=interrupt_terminal):
        accepted_dry.run(f, f.action, f.seed, boundary=boundary)
    raise AssertionError("terminal crash cut was not reached")


def crash_recovery(directory, cut):
    child = subprocess.run([sys.executable, "-B", str(Path(__file__).resolve()),
        "--output-dir", str(directory), "--crash", cut], cwd=ROOT, capture_output=True, text=True, timeout=60)
    (directory / (cut + "-child.stdout.log")).write_text(child.stdout, encoding="utf-8")
    (directory / (cut + "-child.stderr.log")).write_text(child.stderr, encoding="utf-8")
    check(cut + "_actual_abrupt_exit", child.returncode == (91 if cut == "before-terminal-commit" else 92))
    record = json.loads((directory / (cut + "-binding.json")).read_text(encoding="utf-8"))
    CHECKS.update(record.pop("checks"))
    f = object.__new__(Fixture)
    f.name, f.directory = cut, directory
    f.domain = a3.domain(wallet=sf.WALLET, mode="DRY")
    check(cut + "_same_fixed_domain", accepted_dry.canonical_json(f.domain.to_record())
        == accepted_dry.canonical_json(record["domain"]))
    f.path, f.ppath, f.raw = (Path(record[key]) for key in ("ledger_path", "producer_path", "raw_path"))
    binding = dict(record["source_binding"])
    binding["anchors"] = tuple(source_fixture.CursorWitness(**value) for value in binding["anchors"])
    f.binding = source_fixture.SourceBinding(**binding)
    f.repo = LedgerRepository.reopen(f.path, f.domain)
    f.source = source_fixture.SourceEvidenceStore(directory / (cut + "-evidence.sqlite"),
        f.binding, source_fixture.SourceProfile())
    f.open_producer()
    try:
        action = f.repo.reservation(record["root_id"]).admission.action
        f.root, f.item, f.action = action.root_id, f.repo.candidate(action.root_id), action
        interrupted = journal(f)
        committed = cut == "after-terminal-commit"
        check(cut + "_atomic_disposition_and_capacity_after_reopen",
            (f.repo.inbox_disposition(action.root_id) == "NON_SUBMITTED") == committed
            and (not f.repo.consumer_snapshot()["reservations"]) == committed)
        before = f.repo.audit()
        receipt = dry.finish_interrupted_dry(f.repo, action.action_id, recorded_at_utc=a3.utc(NOW + 10))
        check(cut + "_recovery_commits_exactly_once",
            f.repo.audit()["revision"] == before["revision"] + int(not committed)
            and receipt.dry_terminal.preparation_digest == accepted_dry.content_fingerprint(record["preparation"])
            and receipt.dry_terminal.simulation_input_digest == accepted_dry.content_fingerprint(record["stage"]))
        absent(f, action, 1, cut + "_recovered")
        before = journal(f)
        check(cut + "_source_redelivery_preserves_terminal", f.deliver() == [action.root_id] and journal(f) == before)
        no_read_replay(f, action, receipt, cut + "_replay")
        f.reopen()
        no_read_replay(f, action, receipt, cut + "_second_reopen")
        absent(f, action, 1, cut + "_second_reopen")
        return {**record, "child_exit": child.returncode, "interrupted": interrupted,
            "terminal": receipt.to_record(), "final": journal(f)}
    finally:
        f.close()


def main(directory):
    directory.mkdir(parents=True, exist_ok=True)
    check("fresh_evidence_directory", not any(directory.glob("*.sqlite")))
    tree = ast.parse((ROOT / "src/live/candidate_handoff_v0_1.py").read_text(encoding="utf-8-sig"))
    calls = {node.func.attr for node in ast.walk(tree) if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute) and isinstance(node.func.value, ast.Attribute)
        and isinstance(node.func.value.value, ast.Name) and node.func.value.value.id == "self"
        and node.func.value.attr == "ledger"}
    imports = {node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)}
    check("source_bridge_only_inbox_no_mutation_capability", calls == {"candidate", "receive_candidate", "write_fence"}
        and not any(word in (module or "") for module in imports for word in
            ("signer", "execution_send", "execution_signed", "runtime_composition", "operations")))
    results = {"source_dry_continuation": normal(directory)}
    for cut in ("before-terminal-commit", "after-terminal-commit"):
        results[cut] = crash_recovery(directory, cut)
    paths = [Path(__file__), Path(source_fixture.__file__), Path(accepted_dry.__file__), Path(a4.__file__),
        ROOT / "docs/live/MEME_LIVE_PRODUCTION_CLOSURE_ARCHITECTURE_V2.md"]
    paths.extend(ROOT / "src/live" / name for name in (
        "candidate_handoff_v0_1.py", "continuous_producer_v0_2.py", "source_health_v0_1.py",
        "runtime_dry_v0_1.py", "ledger_repository_v0_1.py", "ledger_ports_v0_1.py",
        "authority_admission_v0_1.py", "authority_controls_v0_1.py", "execution_message_v0_1.py",
        "execution_readonly_v0_1.py", "runtime_composition_v0_1.py", "execution_reconciliation_v0_1.py"))
    hashes = {path.relative_to(ROOT).as_posix(): hashlib.sha256(_lf(path.read_bytes())).hexdigest() for path in paths}
    integrity = {}
    for path in directory.glob("*.sqlite"):
        with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as conn:
            integrity[path.name] = conn.execute("PRAGMA integrity_check").fetchone()[0]
    check("all_disposable_stores_integrity", all(value == "ok" for value in integrity.values()))
    report = {"status": "IMPLEMENTED_PENDING_PROJECT_REVIEW", "scope": ["S10", "E01_DRY_NO_BROADCAST"],
        "qualification": "SYNTHETIC_ENGINEERING_ONLY", "checks": {**CHECKS, **accepted_dry.CHECKS},
        "artifact_hashes": hashes, "integrity": integrity, "evidence": results,
        "bridge_capability_surface": {"Ledger_calls": sorted(calls), "imports": sorted(imports)},
        "prior_bridge_restriction": {"source_sha256": "8cd450876d28faef54dc7f39ed23198dcdc7bd236f1d77de32db01f763b516be",
            "predicate": "ledger.domain.mode != LIVE", "diagnostic": "A3 requires LIVE inbox domain"},
        "established_inputs_not_rerun": ["Step8C runtime_dry structural capability guards",
            "Step8C DRY abrupt before/after preparation and simulation", "L6 atomic DRY terminal/release"]}
    save(directory / "s10-result.json", report)
    print(accepted_dry.canonical_json({"status": report["status"], "checks": len(report["checks"]),
        "all_checks": all(report["checks"].values()), "result": str(directory / "s10-result.json")}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--crash", choices=("before-terminal-commit", "after-terminal-commit"))
    args = parser.parse_args()
    if args.crash:
        crash_child(args.output_dir, args.crash)
    else:
        main(args.output_dir)
