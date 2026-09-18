"""S12 controlled full-process loss with durable history and later admission.

One supervised synthetic journey, two retained disposable child handles. This
is internal process-loss evidence, not physical reboot/autostart qualification.
Only the original child's ephemeral signer travels over spawn IPC; replacement
inputs contain a directory alone. No private material is written to disk.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import sys
import time
import traceback
from contextlib import closing
from dataclasses import asdict, dataclass, is_dataclass
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "scripts")]
import live_step10_s01_s02_selftest_v0_1 as prior
import live_step10_s04_s08_selftest_v0_1 as shared
import live_step10_s06_nonlanding_protection_selftest_v0_1 as delayed
import live_operations_supervisor_selftest_v0_1 as b1
import live_operations_presend_fault_selftest_v0_1 as cuts
import live_operations_finality_fault_selftest_v0_1 as facts_helper
import live_transaction_evidence_selftest_v0_1 as raw_rpc
from live import ledger_evidence_codec_v0_1 as codec
from live import operations_readiness_v0_1 as readiness
from live.operations_ownership_v0_1 import RestartProfile

ops, c2, a3, sf, hf, NOW = prior.ops, prior.c2, prior.a3, prior.sf, prior.hf, prior.NOW
CHECKS = {}
NAME = "s12-durable-process"


def check(name, condition):
    CHECKS[name] = bool(condition)
    if not condition:
        raise AssertionError(name)


def write(path, value):
    facts_helper.write(path, normalized(value))


def normalized(value):
    if is_dataclass(value):
        return normalized(asdict(value))
    if isinstance(value, dict):
        return {k: normalized(v) for k, v in value.items()}
    if isinstance(value, (tuple, list)):
        return [normalized(v) for v in value]
    return value


def manifest(f):
    return dict(f.conn.execute("SELECT * FROM live_producer_checkpoint_v0_1 WHERE singleton=1").fetchone())


def read(path):
    return json.loads(path.read_text(encoding="utf-8"))


def public_state(f, path):
    write(path, {"accounts": {k: codec._account_record(v) for k, v in f.public.items()},
                 "anchor": asdict(f.lower), "source_cut": f.source_cut})


def attached(started, directory, public_path):
    # Fixture methods only supply external facts and call actual owned Runtime.
    # Every economic/control/producer field comes from original start_live.
    f = object.__new__(c2.Fixture)
    f.directory, f.name, f.started = directory, NAME, started
    f.runtime, f.repo = started.runtime, started.runtime.ledger
    f.domain, f.source = f.repo.domain, f.runtime.source
    f.producer, f.handoff, f.conn = f.runtime.producer, f.runtime.handoff, f.runtime._producer_conn
    f.binding = f.source.binding
    f.raw, f.path, f.ppath = (directory / (NAME + s) for s in ("-raw.sqlite", "-ledger.sqlite", "-producer.sqlite"))
    f.startup_monitor_config = f.runtime._operations_degradation.configuration
    external = read(public_path)
    f.public = {k: codec._account(v) for k, v in external["accounts"].items()}
    f.lower = codec.FinalizedBlockAnchor(**external["anchor"])
    f.source_cut = external["source_cut"]
    shared.attach_steps(f)
    return f


def state(f):
    repo = f.repo
    actions = [repo.action(row[0]) for row in repo._conn.execute("SELECT action_id FROM ledger_pending_actions ORDER BY commit_seq")]
    positions = {a.position_id: asdict(repo.position_history(a.position_id)) for a in actions
                 if repo.position_history(a.position_id) is not None}
    binding = f.runtime.position_binding
    return {"pid": os.getpid(), "owned": cuts.owned_facts(f.started),
            "authority": normalized(repo.authority_snapshot()), "positions": positions,
            "actions": [asdict(a) for a in actions],
            "binding": None if binding is None else asdict(binding),
            "exit_records": [] if binding is None else [asdict(r) for r in repo.exit_records(binding.binding_id)],
            "producer_lineage": f.producer.source_identity,
            "producer_manifest": manifest(f),
            "source": asdict(f.source.latest_record()[1]) if f.source.latest_record() else None,
            "journal": prior.stamp(f, "durable economic cut")}


def controls(value):
    return {k: value["authority"][k] for k in
            ("policy", "armed_entry_grant", "entry_stop_command", "hard_stop_command")}


def admit(f, at):
    result = f.step(at, entry=c2.EntryFacts("PUMP", sf.TOKEN_PROGRAM_ID, prior.wallet(f, at, f.item.mint)))
    check("actual_admission_" + str(at), result.work == "ENTRY_ADMITTED")
    f.buy = f.repo.action(result.action_id)
    return f.repo.authority_admission_receipt(result.receipt_key)


def first_child(f, key):
    f.market = sf.Fixture()
    f.candidate_ready()
    cold = f.repo.evaluate_authority_entry(f.root, "FINAL-A", a3.clock(f.repo, NOW + 4), f.source,
                                         command_id="s12-cold-unarmed", fence=f.repo.write_fence())
    check("cold_original_unarmed", "ENTRY_UNARMED" in cold.decision.reasons and not f.started.audit.grants_permission)
    f.configure()
    f.original_authority = f.repo.authority_snapshot()
    admit(f, NOW + 4)
    prior.EVIDENCE[f.name] = {"lifecycles": []}
    first = prior.lifecycle(f, key)
    write(f.directory / "first-retired.json", {"trade": first, "state": state(f)})
    prior.next_candidate(f)
    second = admit(f, NOW + 36)
    check("same_original_continuous_grant", second.consumed_grant_id == first["grant_id"])
    acquired, buy = prior.transact(f, key, f.buy, NOW + 38, 81)
    monitored = f.step(NOW + 46)
    check("second_actual_open_protection", monitored.work == "ENTRY_HELD"
          and f.runtime.position_binding.acquisition_application_key == acquired.receipt_key)
    staged = f.step(NOW + 48)
    check("second_actual_due_exit", staged.work == "PROTECTIVE_ACTION_STAGED")
    sell = f.repo.action(staged.action_id)
    sent, envelope, _, _, _ = delayed.send(f, key, sell, NOW + 52, 82, unknown=True)
    check("second_actual_lost_ACK", sent.reason == "TRANSPORT_UNRESOLVED")
    # Retain only the public raw RPC response scenario, independently of owners.
    with prior.external_mint(sell.mint):
        _, _, truth, _ = c2.original_chain(f, sent, NOW + 52)
    write(f.directory / "public-truth.json", {k: v for k, v in vars(truth).items() if k not in ("calls", "transform")})
    public_state(f, f.directory / "public-loss.json")
    snapshot = state(f)
    attempt = f.repo.attempt(sent.attempt_id)
    check("open_UNKNOWN_capacity_before_loss", attempt.recorded_stage == "UNKNOWN" and attempt.lane_held
          and not attempt.economically_applied and len(f.repo.consumer_snapshot()["positions"]) == 1
          and len(f.repo.consumer_snapshot()["reservations"]) == 1
          and f.repo.position_history(first["retired_history"]["position_id"]).status == "RETIRED")
    write(f.directory / "loss-ready.json", {"state": snapshot, "first": first, "second_buy": buy,
          "exit_attempt": sent.attempt_id, "exit_signature": envelope.primary_signature,
          "exit_message_sha256": envelope.preparation.message_sha256,
          "checks": {**CHECKS, **prior.CHECKS, **shared.CHECKS, **delayed.CHECKS}})


def replacement(f):
    before = read(f.directory / "loss-ready.json")
    recovered = state(f)
    write(f.directory / "recovered.json", {"state": recovered, "startup": asdict(f.started.audit)})
    original = before["state"]
    for field in ("positions", "actions", "binding", "exit_records", "producer_lineage", "source"):
        check("durable_restored_" + field, json.loads(json.dumps(recovered[field], default=public_bytes)) == original[field])
    check("durable_controls_unchanged", json.loads(json.dumps(controls(recovered), default=public_bytes)) == controls(original))
    check("durable_economics_no_reconstruction_postings", recovered["owned"]["durable"]["economic_sha256"]
          == original["owned"]["durable"]["economic_sha256"])
    check("fresh_owned_reconstruction", recovered["pid"] != original["pid"]
          and recovered["owned"]["fence"]["generation"] == original["owned"]["fence"]["generation"] + 1
          and f.started.audit.reconstruction.initial_handoff_replay_deferred
          and not f.started.audit.grants_permission)
    attempt_id = f.runtime.reconstruction_facts().pending_attempt_id
    attempt = f.repo.attempt(attempt_id)
    check("UNKNOWN_original_signature_lane_preserved", attempt_id == before["exit_attempt"]
          and attempt.primary_signature == before["exit_signature"] and attempt.recorded_stage == "UNKNOWN"
          and attempt.lane_held and attempt.preparation.message_sha256 == before["exit_message_sha256"])
    barrier = readiness.evaluate(
        f.started, clock=lambda: a3.clock(f.repo, NOW + 54),
        operations_resources=lambda: ops.host(f, NOW + 54),
    )
    write(f.directory / "restored-barriers.json", asdict(barrier))
    check("ordered_truth_before_entry", barrier.protective.state == "TRUTH_REQUIRED"
          and not barrier.entry.ready and not barrier.grants_permission)
    checkpoint = manifest(f)
    held = f.step(NOW + 54, entry=c2.EntryFacts("PUMP", sf.TOKEN_PROGRAM_ID,
                                              prior.wallet(f, NOW + 54, f.runtime.position_binding.mint)))
    check("actual_runtime_truth_priority_no_bulk_replay", held.work == "NEED_RECONCILIATION"
          and held.attempt_id == attempt_id and manifest(f) == checkpoint
          and f.repo.attempt(attempt_id) == attempt)
    try:
        from dataclasses import replace
        f.repo.prepare_attempt(replace(attempt.preparation, ordinal=attempt.preparation.ordinal + 1), fence=f.repo.write_fence())
    except (ValueError, RuntimeError) as exc:
        denial = str(exc)
    else:
        raise AssertionError("UNKNOWN replacement unexpectedly admitted")
    check("unknown_replacement_denied", denial == "LEDGER_WALLET_MUTATION_LANE_HELD")
    # This fixture object contains only external raw RPC records. The accepted
    # PublicReadOnlyRpc and TransactionEvidenceAdapter construct observations.
    scenario = object.__new__(raw_rpc.Scenario)
    scenario.__dict__.update(read(f.directory / "public-truth.json"))
    scenario.blocks = {int(k): v for k, v in scenario.blocks.items()}
    scenario.calls, scenario.transform = [], None
    reconciled, calls = c2.reconcile(f, scenario, NOW + 58)
    check("existing_exit_public_finality", reconciled.reason == "FINALIZED_SUCCESS_UNAPPLIED"
          and f.repo.attempt(attempt_id).lane_held)
    chain = f.repo.chain_receipt(reconciled.receipt_key)
    with prior.external_mint(f.runtime.position_binding.mint):
        envelope, actual, expected_raw, pre = c2.original_chain(f, SimpleNamespace(attempt_id=attempt_id), NOW + 52)
    check("raw_RPC_exact_existing_signed_exit", scenario.tx == expected_raw.tx
          and chain.observation.transaction.primary_signature == envelope.primary_signature)
    support = c2.application_support(f, actual, chain, pre, NOW + 60)
    applied = f.step(NOW + 60, application_wallet=support)
    check("actual_exit_applied_once", applied.reason == "FINALIZED_SUCCESS_APPLIED")
    receipt = f.repo.application_receipt(applied.receipt_key)
    audit = f.repo.audit()
    replay = f.repo.apply_settlement(attempt_id, chain_receipt_key=chain.ingestion_key, support=support,
                                   ingestion_key=receipt.ingestion_key, recorded_at_utc=receipt.recorded_at_utc,
                                   fence=f.repo.write_fence())
    check("application_exact_replay_no_duplicate", replay == receipt and f.repo.audit() == audit)
    check("capacity_held_until_actual_retirement", f.step(NOW + 61).work == "NEED_RETIREMENT"
          and len(f.repo.consumer_snapshot()["reservations"]) == 1)
    retired = f.step(NOW + 62, retirement_wallet=prior.continuation.wallet(f, NOW + 62))
    check("actual_reconciled_retirement", retired.work == "RETIREMENT_RETIRED"
          and not f.repo.consumer_snapshot()["positions"] and not f.repo.consumer_snapshot()["reservations"]
          and not f.repo.consumer_snapshot()["pending_attempts"] and f.runtime.position_binding is None)
    write(f.directory / "second-retired.json", {"state": state(f), "chain": asdict(chain),
          "application": asdict(receipt), "retirement": asdict(f.repo.port_receipt(retired.receipt_key)),
          "rpc_requests": calls, "UNKNOWN_denial": denial, "priority_result": asdict(held)})
    # Third mint is a fresh canonical source candidate; no new grant or signer.
    with patch.object(hf, "MINT_C", hf.key(15)):
        prior.next_candidate(f, shift=64)
    entry = c2.EntryFacts(
        "PUMP", sf.TOKEN_PROGRAM_ID, prior.wallet(f, NOW + 69, f.item.mint)
    )
    ready = readiness.evaluate(
        f.started, clock=lambda: a3.clock(f.repo, NOW + 69), entry=entry,
        operations_resources=lambda: ops.host(f, NOW + 69),
    )
    write(f.directory / "later-entry-readiness.json", asdict(ready))
    check("fresh_current_entry_ready_after_retirement", ready.entry.name == "ENTRY_READY"
          and ready.entry.ready and ready.entry.state == "READY" and not ready.grants_permission)
    admitted = f.step(NOW + 69, entry=entry)
    check("third_actual_runtime_admission", admitted.work == "ENTRY_ADMITTED")
    f.buy = f.repo.action(admitted.action_id)
    third = f.repo.authority_admission_receipt(admitted.receipt_key)
    check("later_legitimate_distinct_admission", f.buy.root_id not in {a["root_id"] for a in original["actions"]}
          and f.buy.mint not in {a["mint"] for a in original["actions"]}
          and third.consumed_grant_id == before["first"]["grant_id"]
          and f.repo.audit()["action_count"] == 5 and f.repo.audit()["attempt_count"] == 4)
    final = state(f)
    check("no_rearm_or_control_reset", json.loads(json.dumps(controls(final), default=public_bytes)) == controls(original))
    check("both_prior_positions_retained", len([p for p in final["positions"].values() if p["status"] == "RETIRED"]) == 2)
    write(f.directory / "finished.json", {"state": final, "third_admission": asdict(third),
          "checks": {**CHECKS, **shared.CHECKS, **prior.CHECKS},
          "qualification": "CONTROLLED_PROCESS_LOSS_ONLY_NOT_PHYSICAL_REBOOT_OR_AUTOSTART"})


def public_bytes(value):
    if isinstance(value, bytes):
        return {"public_bytes_hex": value.hex()}
    raise TypeError(type(value).__name__)


@dataclass
class Inputs:
    directory: Path
    key_bytes: bytes | None = None

    def __setstate__(self, value):
        self.__dict__.update(value)
        self.start_order = []
        # Observe original startup boundaries before original supervisor calls
        # start_live. These wrappers delegate without changing owner results.
        for owner, method, label in ((b1.OperationsStore, "acquire", "owner"),
                                    (ops.startup.repository, "_schema_and_domain", "ledger"),
                                    (ops.ColdRuntimeV01, "_restore_economics", "economics"),
                                    (ops.startup, "_source_preflight", "source")):
            original = getattr(owner, method)
            def observed(*args, _call=original, _label=label, **kwargs):
                result = _call(*args, **kwargs)
                self.start_order.append(_label)
                return result
            patch.object(owner, method, observed).start()

    def __call__(self, started):
        try:
            if getattr(self, "finished", False):
                # Pause outside the next original Runtime step, after the
                # supervisor has received the preceding PROGRESS message.
                while True:
                    time.sleep(0.05)
            write(self.directory / ("startup-order-" + str(os.getpid()) + ".json"), self.start_order)
            check("original_startup_order", self.start_order.index("owner") < self.start_order.index("ledger")
                  < self.start_order.index("economics") < self.start_order.index("source"))
            wallet = started.runtime.ledger.domain.wallet
            with patch.object(sf, "WALLET", wallet), patch.object(sf.plans, "ACTOR", wallet):
                initial = self.key_bytes is not None
                f = attached(started, self.directory, self.directory / ("public-initial.json" if initial else "public-loss.json"))
                if initial:
                    key = c2.Keypair.from_bytes(self.key_bytes)
                    self.key_bytes = None
                    first_child(f, key)
                    del key
                else:
                    replacement(f)
                self.finished = True
                at = NOW + (54 if initial else 71)
                return dict(
                    clock=lambda: a3.clock(f.repo, at),
                    source_cut_utc=a3.utc(f.source_cut),
                    operations_resources=lambda: ops.host(f, at),
                )
        except BaseException:
            write(self.directory / ("child-error-" + str(os.getpid()) + ".json"), {"traceback": traceback.format_exc()})
            raise


def wait_marker(driver, directory, name, completed_steps):
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        result = driver.tick()
        errors = list(directory.glob("child-error-*.json"))
        if errors:
            raise AssertionError(read(errors[0]))
        if ((directory / name).exists()
                and result.completed_steps >= completed_steps):
            return result
        if driver.sup._latched:
            raise AssertionError(result)
        time.sleep(0.02)
    raise AssertionError("bounded S12 child marker timeout: " + name)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--evidence-dir", type=Path, required=True)
    args = parser.parse_args()
    directory = args.evidence_dir.resolve()
    directory.mkdir(parents=True, exist_ok=False)
    key = c2.Keypair()
    with patch.object(sf, "WALLET", str(key.pubkey())), patch.object(sf.plans, "ACTOR", str(key.pubkey())):
        f = ops.installed(c2.Fixture(directory, NAME), profile=RestartProfile(2, 1000000, 10))
        f.source_cut = NOW + 1
        public_state(f, directory / "public-initial.json")
        config = dict(operations_path=f.operations_path, ledger_path=f.path, domain=f.domain,
                      expected_identity=f.expected, degradation_config=ops.healthy_monitor_config(f), **ops.c4.cold_args(f))
        write(directory / "public-configuration.json", {"identity": asdict(f.expected), "domain": f.domain.to_record(),
              "source_binding": asdict(f.binding), "producer_profile": asdict(config["producer_profile"]),
              "source_profile": asdict(config["source_profile"]), "monitor": asdict(config["degradation_config"]),
              "paths": {k: str(v.resolve()) for k, v in config.items() if isinstance(v, Path)}})
        write(directory / "baseline.json", cuts.durable(f.path))
        ops.close(f)
    inputs = Inputs(directory, bytes(key))
    del key, f
    sup = b1.supervisor.OperationsSupervisor(config, inputs, b1.supervisor.HealthProfile(60000000, 60000000, 1000000))
    driver = b1.Driver(sup)
    try:
        progress_before = wait_marker(driver, directory, "loss-ready.json", 1)
        check("supervisor_original_UNKNOWN_progress", progress_before.last_work == "NEED_RECONCILIATION")
        first, fence = sup.process, sup.fence
        first_pid, budget = first.pid, sup.store.snapshot()["budget_id"]
        control_before = sup.store.snapshot()
        # Drop all private initial IPC material before scheduling replacement.
        sup._external_inputs = Inputs(directory)
        inputs.key_bytes = None
        del inputs
        first.terminate()
        first.join(5)
        check("exact_retained_child_confirmed_dead", first.exitcode is not None and not first.is_alive())
        death = {"pid": first_pid, "exitcode": first.exitcode, "fence": asdict(fence)}
        lost = cuts.durable(config["ledger_path"])
        check("confirmed_death_budgeted_backoff", driver.tick().state == "RESTART_BACKOFF")
        driver.now = 10
        progress_after = wait_marker(driver, directory, "finished.json", 2)
        check("supervisor_replacement_fresh_action_progress", progress_after.last_work == "NEED_EXECUTION")
        check("exactly_two_original_budgeted_children", sup.process.pid != first_pid and sup.fence.generation == fence.generation + 1
              and sup.fence.nonce != fence.nonce and sup.store.snapshot()["attempts"] == 2
              and sup.store.snapshot()["budget_id"] == budget)
        check("operations_stop_profile_binding_preserved", all(sup.store.snapshot()[k] == control_before[k]
              for k in ("stopped", "budget_id", "binding_digest", "domain_id")))
        write(directory / "process-proof.json", {"death": death, "replacement_pid": sup.process.pid,
              "replacement_fence": asdict(sup.fence), "control_before": control_before,
              "control_after": sup.store.snapshot(), "loss_durable": lost, "checks": CHECKS,
              "progress_before": asdict(progress_before), "progress_after": asdict(progress_after),
              "replacement_inputs": {"directory": str(directory), "key_bytes": None}})
    finally:
        b1.cleanup(sup)
    checks = {**read(directory / "loss-ready.json")["checks"], **read(directory / "finished.json")["checks"], **CHECKS}
    check("all_targeted_checks", all(checks.values()))
    files = {str(p.relative_to(directory)): hashlib.sha256(p.read_bytes()).hexdigest() for p in directory.iterdir() if p.is_file()}
    contracts = {str(p.relative_to(ROOT)).replace("\\", "/"): prior.lf_sha256(p)
                 for p in [Path(__file__), *(ROOT / "src/live").glob("*.py"),
                           *(Path(m.__file__) for m in (prior, shared, delayed, b1, cuts, facts_helper, c2, raw_rpc))]}
    result = {"status": "IMPLEMENTED_PENDING_PROJECT_REVIEW", "scope": "S12", "checks": checks,
              "qualification": "CONTROLLED_PROCESS_LOSS_ONLY_NOT_PHYSICAL_REBOOT_OR_AUTOSTART",
              "supervised_child_count": 2, "completed_lifecycles": 2, "later_admissions": 1,
              "evidence_files_sha256": files, "code_sha256": contracts}
    write(directory / "result.json", result)
    print(c2.canonical_json(result))


if __name__ == "__main__":
    main()
