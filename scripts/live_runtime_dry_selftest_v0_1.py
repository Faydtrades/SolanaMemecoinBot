"""C1 actual DRY consumers; deterministic public MockTransport and temp Ledgers."""
from __future__ import annotations
import ast
import base64
import hashlib
import json
import os
import subprocess
import sys
import tempfile
from contextlib import ExitStack
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch
import httpx

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
import live_execution_message_selftest_v0_1 as q1
from live import runtime_dry_v0_1 as dry
from live.execution_readonly_v0_1 import ExecutionReadOnlyRpc
from live.execution_message_v0_1 import ComputeBudget, produce_exact_message
from live.authority_message_evidence_v0_1 import validate_message_evidence
from live.ledger_repository_v0_1 import LedgerRepository
from phase5.shadow_domain_v0_1 import canonical_json, content_fingerprint

a4, a3, sf, NOW = q1.a4, q1.a3, q1.sf, q1.NOW
CHECKS = {}


def check(name, value):
    CHECKS[name] = bool(value)
    if not value:
        raise AssertionError(name)


def fails(call):
    try:
        call()
    except (ValueError, RuntimeError, TypeError):
        return True
    return False


class Interrupted(BaseException):
    pass


def fixture(directory, name, route="pump"):
    f = a3.Fixture(directory, name, mode="DRY", install_now=False, arm_now=False)
    f.repo.close()
    external = sf.Fixture(route)
    external.mint = sf.MINT
    f.domain = a3.domain(wallet=sf.WALLET, mode="DRY")
    scenario = external.wallet_scenario()
    f.path = directory / (name+"-actual.sqlite3")
    f.repo = LedgerRepository.initialize(f.path, f.domain)
    a3.ingest(f.repo, a3.observe(scenario, request=a3.WalletEvidenceRequest(sf.WALLET, a3.GENESIS, 100)))
    f.item = a3.static_candidate(sf.MINT, f.source.binding)
    f.root = f.repo.receive_candidate(f.item, fence=f.repo.write_fence())
    legacy = a3.a1.policy(f.item, binding=f.domain)
    f.policy = a3.policy_v02(replace(legacy, size=replace(legacy.size,
        fixed_quote_lamports=external.units, ceiling_quote_lamports=external.units,
        max_trade_notional_lamports=external.units, max_global_exposure_lamports=3*external.units,
        max_mint_exposure_lamports=external.units), costs=replace(legacy.costs,
        venue_fee_within_quote_cap_lamports=2000000, setup_outflow_lamports=10000000,
        refundable_account_lock_lamports=10000000, protective_setup_lamports=10000000,
        protective_refundable_lock_lamports=10000000)))
    a3.control(f.repo, "INSTALL_POLICY", "install", policy_value=f.policy)
    a3.arm(f.repo, f.policy, scope="DRY")
    scenario.accounts[sf.MINT] = a3.account(a4.venue.TOKEN_PROGRAM_ID, sf.plans.fx.mint_account().data)
    receipt = a3.admit(f, name+"-admit", venue="PUMP" if route == "pump" else "PUMPSWAP",
        support=a3.wallet(f, scenario=scenario))
    check(name+"_actual_DRY_authority_admission", receipt.accepted and receipt.consumed_grant_id is None)
    action = f.repo.reservation(f.root).admission.action
    seed, _ = a4.evidence(f, action, scenario, route=route)
    return f, external, scenario, action, seed


class DryPublicTransport(q1.PublicTransport):
    def __call__(self, request):
        if json.loads(request.content)["method"] == "simulateTransaction":
            self.at = dry.utc_microseconds(self.seed.clock.utc_upper_utc)
        return super().__call__(request)


def run(f, action, seed, *, fault=None, boundary=None):
    boundary = boundary or DryPublicTransport(seed, fault=fault)
    with ExecutionReadOnlyRpc("https://invalid.local", a3.PROFILE, now_us=boundary.now,
            transport=httpx.MockTransport(boundary)) as rpc:
        value = dry.run_dry(f.repo, action.action_id, rpc, seed.evidence.wallet,
            seed.evidence.quote_policy, seed.evidence.plan_policy, ComputeBudget(250000, 1000),
            clock=lambda: seed.clock, now_us=lambda: dry.utc_microseconds(seed.clock.utc_upper_utc)
                if len(boundary.requests) >= 9 else seed.evidence.simulation.leases[0].observed_at_us)
    return value, boundary


def economic_absence(f, action, count, name):
    audit = f.repo.audit()
    snapshot = f.repo.consumer_snapshot()
    check(name+"_exact_attempt_count", audit["attempt_count"] == count)
    check(name+"_no_paid_effects", audit["posting_count"] == audit["application_receipt_count"] == 0
        and snapshot["funding"].native_lamports == f.domain.known_native_wallet_lamports
        and snapshot["funding"].network_fees_paid_lamports == 0)
    check(name+"_no_fabricated_inventory", not snapshot["positions"] and not snapshot["protections"])
    check(name+"_terminal_release", f.repo.inbox_disposition(action.root_id) == "NON_SUBMITTED"
        and not snapshot["reservations"] and not f.repo.mutation_lane()["held"])
    check(name+"_only_DRY_stage_graph", all(attempt.primary_signature is None and attempt.signed_wire_digest is None
        and not attempt.economically_applied and attempt.chain_finality == "UNOBSERVED"
        and attempt.recorded_stage in ("PREPARED", "EXACT_SIMULATED")
        for attempt in f.repo._root_attempts(action.root_id)))


def successful(directory, route, fault=None):
    name = route+"-"+(fault or "success")
    f, external, scenario, action, seed = fixture(directory, name, route)
    try:
        candidate = f.item
        receipt, boundary = run(f, action, seed, fault=fault)
        check(name+"_actual_terminal_consumer", receipt.kind == "DRY_NON_SUBMITTED"
            and receipt.dry_terminal.reason == "DRY_CAPABILITY")
        attempts = f.repo._root_attempts(action.root_id)
        prep = attempts[0].preparation
        check(name+"_original_candidate_root_action", f.repo.candidate(action.root_id) == candidate
            and candidate.trade_root(f.domain) == action.root_id and prep.action_id == action.action_id
            and prep.action_content_digest == action.content_digest and receipt.dry_terminal.preparation_digest == prep.content_digest)
        methods = tuple(request["method"] for request in boundary.requests)
        check(name+"_exact_Q1_read_sequence", methods == ("getGenesisHash", "getMultipleAccounts", "getMultipleAccounts",
            "getMultipleAccounts", "getLatestBlockhash", "getMultipleAccounts", "getFeeForMessage",
            "getBlockHeight", "isBlockhashValid", "simulateTransaction"))
        request = boundary.requests[-1]
        wire = base64.b64decode(request["params"][0])
        check(name+"_exact_zero_signature_message", wire[:65] == b"\x01"+bytes(64) and wire[65:].hex() == prep.message_hex
            and request["params"][1]["replaceRecentBlockhash"] is False and request["params"][1]["sigVerify"] is False)
        stage = f.repo.attempt_stage_inputs(prep.attempt_id)[0]
        witness = json.loads(stage.external_record_json)
        check(name+"_authentic_simulation_witness", witness["preparation_digest"] == prep.content_digest
            and witness["request_digest"] == content_fingerprint(request["params"])
            and witness["outcome"] == ("PROGRAM_REJECTED" if fault else "SIMULATION_SUCCESS")
            and content_fingerprint(witness) == stage.external_record_digest
            and receipt.dry_terminal.simulation_input_digest == content_fingerprint(stage.to_record()))
        check(name+"_preparation_simulation_recording_causality", dry.utc_microseconds(prep.prepared_at_utc)
            <= witness["observed_at_us"] <= dry.utc_microseconds(stage.recorded_at_utc))
        check(name+"_no_message_authority_grant", f.repo.authority_message_profile(action.policy_digest) is None
            and not stage.has_real_authority_grant)
        economic_absence(f, action, 1, name)
        before = f.repo.write_fence()
        check(name+"_candidate_redelivery", f.repo.receive_candidate(candidate, fence=before) == action.root_id)
        duplicate, no_reads = run(f, action, seed)
        check(name+"_duplicate_original_receipt_no_reads", duplicate == receipt and not no_reads.requests
            and f.repo.write_fence() == before)
        f.reopen()
        reopened, no_reads = run(f, action, seed)
        check(name+"_reopen_original_receipt_no_reads", reopened == receipt and not no_reads.requests)
        check(name+"_terminal_no_new_ordinal", fails(lambda: f.repo.prepare_attempt(replace(prep, ordinal=2), fence=f.repo.write_fence())))
        check(name+"_DRY_cannot_reopen_LIVE", fails(lambda: LedgerRepository.reopen(f.path, replace(f.domain, mode="LIVE"))))
        economic_absence(f, action, 1, name+"_reopened")
    finally:
        f.close()


def interruptions(directory):
    for point in ("before-preparation", "after-preparation", "after-simulation"):
        f, _, _, action, seed = fixture(directory, point)
        try:
            if point == "before-preparation":
                target = "prepare_attempt"
            elif point == "after-preparation":
                target = "prepare_attempt"
            else:
                target = "record_external_attempt_stage"
            original = getattr(f.repo, target)
            def interrupt(*args, **kwargs):
                if point != "before-preparation":
                    original(*args, **kwargs)
                raise Interrupted(point)
            try:
                with patch.object(f.repo, target, side_effect=interrupt):
                    run(f, action, seed)
            except Interrupted:
                pass
            else:
                raise AssertionError(point)
            original_attempts = f.repo._root_attempts(action.root_id)
            check(point+"_durable_interruption_boundary", len(original_attempts) == (0 if point == "before-preparation" else 1)
                and (not original_attempts or original_attempts[0].recorded_stage ==
                    ("EXACT_SIMULATED" if point == "after-simulation" else "PREPARED")))
            f.reopen()
            receipt = dry.finish_interrupted_dry(f.repo, action.action_id, recorded_at_utc=a3.utc(NOW+10))
            expected_reason = {"before-preparation": "DRY_INTERRUPTED_BEFORE_PREPARATION",
                "after-preparation": "DRY_INTERRUPTED_NO_SIGNED_GRAPH", "after-simulation": "DRY_CAPABILITY"}[point]
            check(point+"_original_presence_reason", receipt.dry_terminal.reason == expected_reason
                and (receipt.dry_terminal.attempt_id is None) == (point == "before-preparation")
                and (receipt.dry_terminal.simulation_input_digest is not None) == (point == "after-simulation"))
            if original_attempts:
                check(point+"_no_refreshed_lineage", f.repo.attempt(original_attempts[0].preparation.attempt_id).preparation
                    == original_attempts[0].preparation)
            economic_absence(f, action, len(original_attempts), point)
            f.reopen()
            before = f.repo.write_fence()
            check(point+"_repeat_reopen_idempotence", dry.finish_interrupted_dry(f.repo, action.action_id,
                recorded_at_utc=a3.utc(NOW+20)) == receipt and f.repo.write_fence() == before)
        finally:
            f.close()



def boundaries(directory):
    f, _, _, action, seed = fixture(directory, "boundaries")
    try:
        before = f.repo.write_fence()
        with ExecutionReadOnlyRpc("https://invalid.local", a3.PROFILE, now_us=lambda: 0,
                transport=httpx.MockTransport(lambda _: (_ for _ in ()).throw(AssertionError("unexpected read")))) as rpc:
            args = (f.repo, action.action_id, rpc, seed.evidence.wallet, seed.evidence.quote_policy,
                seed.evidence.plan_policy, ComputeBudget(250000, 1000))
            kwargs = {"clock": lambda: seed.clock, "now_us": lambda: 0}
            check("LIVE_producer_rejects_DRY_before_read", fails(lambda: produce_exact_message(*args, **kwargs)) and not rpc.records)
            check("DRY_bad_clock_denied_before_read", fails(lambda: dry.run_dry(*args,
                clock=lambda: replace(seed.clock, status="UNQUALIFIED"), now_us=lambda: 0)) and not rpc.records)
            check("DRY_priority_bound_denied_before_read", fails(lambda: dry.run_dry(*args[:-1], ComputeBudget(250000, 100000000), **kwargs))
                and not rpc.records)
            check("DRY_no_generic_transport", fails(lambda: dry.run_dry(f.repo, action.action_id, object(),
                *args[3:], **kwargs)))
        check("LIVE_A4a_rejects_DRY", validate_message_evidence(seed).disposition == "DENIED_CONTEXT")
        check("boundary_rejections_preserve_Ledger", f.repo.write_fence() == before)
        construct = dry.construct_exact_readonly_message
        def changed(*args, **kwargs):
            result = construct(*args, **kwargs)
            a3.control(f.repo, "DISARM_ENTRY", "changed-cut", at=NOW+9)
            return result
        with patch.object(dry, "construct_exact_readonly_message", side_effect=changed):
            check("context_change_between_read_and_preparation_denied", fails(lambda: run(f, action, seed)))
        check("context_change_no_fabricated_attempt", f.repo.audit()["attempt_count"] == 0)
        dry.finish_interrupted_dry(f.repo, action.action_id, recorded_at_utc=a3.utc(NOW+10))
        economic_absence(f, action, 0, "context-change")
    finally:
        f.close()
    f, _, _, action, seed = fixture(directory, "regressed-simulation-time")
    try:
        receipt, _ = run(f, action, seed, boundary=q1.PublicTransport(seed))
        check("regressed_simulation_time_never_persisted_as_evidence", receipt.dry_terminal.reason == "DRY_INTERRUPTED_NO_SIGNED_GRAPH"
            and receipt.dry_terminal.simulation_input_digest is None)
        economic_absence(f, action, 1, "regressed-simulation-time")
    finally:
        f.close()
    live, _, _, live_action = a4.fixture(directory, "live-input")
    try:
        check("DRY_run_rejects_LIVE", fails(lambda: dry.run_dry(live.repo, live_action.action_id,
            None, None, None, None, None, clock=None, now_us=None)))
        check("DRY_finish_rejects_LIVE", fails(lambda: dry.finish_interrupted_dry(live.repo,
            live_action.action_id, recorded_at_utc=a3.utc(NOW+10))))
    finally:
        live.close()
    for fault in ("wrong-genesis", "changed-venue", "expired", "private-error"):
        f, _, _, action, seed = fixture(directory, fault)
        try:
            receipt, _ = run(f, action, seed, fault=fault)
            check(fault+"_read_failure_terminal_without_fabricated_simulation", receipt.dry_terminal.reason == "DRY_INTERRUPTED_BEFORE_PREPARATION"
                and receipt.dry_terminal.simulation_input_digest is None)
            economic_absence(f, action, 0, fault)
        finally:
            f.close()
    f, _, _, action, seed = fixture(directory, "simulation-transport-failure")
    try:
        class SimulationTransportFailure(q1.PublicTransport):
            def __call__(self, request):
                if json.loads(request.content)["method"] == "simulateTransaction":
                    raise httpx.ConnectError("private provider detail")
                return super().__call__(request)
        receipt, _ = run(f, action, seed, boundary=SimulationTransportFailure(seed))
        check("simulation_transport_failure_keeps_only_real_preparation", receipt.dry_terminal.reason == "DRY_INTERRUPTED_NO_SIGNED_GRAPH"
            and receipt.dry_terminal.simulation_input_digest is None)
        economic_absence(f, action, 1, "simulation-transport-failure")
    finally:
        f.close()


def time_boundaries(directory):
    for kind in ("construction-future", "construction-regression", "simulation-future"):
        f, _, _, action, seed = fixture(directory, kind)
        try:
            class InvalidTime(DryPublicTransport):
                def __call__(self, request):
                    response = super().__call__(request)
                    method = json.loads(request.content)["method"]
                    if kind == "construction-future" and method == "isBlockhashValid":
                        self.at = dry.utc_microseconds(seed.clock.utc_upper_utc)+1
                    elif kind == "construction-regression" and method == "getFeeForMessage":
                        self.at -= 1
                    elif kind == "simulation-future" and method == "simulateTransaction":
                        self.at += 1
                    return response
            boundary = InvalidTime(seed)
            if kind == "simulation-future":
                receipt, _ = run(f, action, seed, boundary=boundary)
                check(kind+"_no_future_simulation_evidence", receipt.dry_terminal.reason == "DRY_INTERRUPTED_NO_SIGNED_GRAPH"
                    and receipt.dry_terminal.simulation_input_digest is None)
            else:
                check(kind+"_no_preparation_from_invalid_read_times", fails(lambda: run(f, action, seed, boundary=boundary))
                    and f.repo.audit()["attempt_count"] == 0)
                dry.finish_interrupted_dry(f.repo, action.action_id, recorded_at_utc=a3.utc(NOW+10))
            economic_absence(f, action, 1 if kind == "simulation-future" else 0, kind)
        finally:
            f.close()


def continuation(directory):
    f, _, scenario, action, seed = fixture(directory, "continuation")
    try:
        receipt, _ = run(f, action, seed)
        f.reopen()
        next_mint = str(a4.Pubkey.from_bytes(bytes([183])*32))
        a3.later_candidate(f, next_mint, "after-dry", NOW+10)
        next_root = f.root
        admission = a3.admit(f, "next-legitimate-dry", at=NOW+14,
            support=a3.wallet(f, scenario=scenario, at=NOW+14))
        check("terminal_DRY_continues_to_fresh_actual_admission", next_root != action.root_id and admission.accepted
            and admission.risk.outstanding_native_lamports == 0
            and f.repo.reservation(next_root).status == "TENTATIVE_DRY")
        check("next_admission_does_not_reopen_original_terminal", f.repo.port_receipt(receipt.ingestion_key) == receipt
            and f.repo.inbox_disposition(action.root_id) == "NON_SUBMITTED")
        next_action = f.repo.reservation(next_root).admission.action
        dry.finish_interrupted_dry(f.repo, next_action.action_id, recorded_at_utc=a3.utc(NOW+15))
        check("next_DRY_release_without_invented_attempt", f.repo.audit()["attempt_count"] == 1
            and not f.repo.consumer_snapshot()["reservations"] and f.repo.audit()["posting_count"] == 0)
    finally:
        f.close()


def abrupt_child(directory, point):
    f, _, _, action, seed = fixture(directory, "abrupt-"+point)
    (directory / "binding.json").write_text(canonical_json({"domain": f.domain.to_record(),
        "action_id": action.action_id, "root_id": action.root_id, "path": str(f.path)}))
    method = "record_external_attempt_stage" if point == "after-simulation" else "prepare_attempt"
    original = getattr(f.repo, method)
    def crash(*args, **kwargs):
        if point != "before-preparation":
            original(*args, **kwargs)
        os._exit(79)
    with patch.object(f.repo, method, side_effect=crash):
        run(f, action, seed)
    raise AssertionError("crash boundary not reached")


def abrupt_cases(directory):
    from live.ledger_domain_v0_1 import LedgerDomain
    for point in ("before-preparation", "after-preparation", "after-simulation"):
        location = directory / ("process-"+point)
        location.mkdir()
        process = subprocess.run([sys.executable, "-B", str(Path(__file__).resolve()), "--abrupt", str(location), point],
            capture_output=True, text=True, timeout=60)
        check(point+"_actual_process_exit_79", process.returncode == 79)
        record = json.loads((location / "binding.json").read_text())
        record["domain"]["expected_empty_token_accounts"] = ()
        binding = LedgerDomain(**record["domain"])
        with LedgerRepository.reopen(Path(record["path"]), binding) as repo:
            count = 0 if point == "before-preparation" else 1
            check(point+"_abrupt_reopen_exact_attempt_count", repo.audit()["attempt_count"] == count)
            receipt = dry.finish_interrupted_dry(repo, record["action_id"], recorded_at_utc=a3.utc(NOW+10))
            check(point+"_abrupt_no_attempt_invention", (receipt.dry_terminal.attempt_id is None) == (count == 0)
                and (receipt.dry_terminal.simulation_input_digest is not None) == (point == "after-simulation")
                and not repo.consumer_snapshot()["positions"] and repo.audit()["posting_count"] == 0
                and not repo.consumer_snapshot()["reservations"] and not repo.mutation_lane()["held"])
        with LedgerRepository.reopen(Path(record["path"]), binding) as repo:
            before = repo.write_fence()
            check(point+"_abrupt_terminal_redelivery_idempotent", dry.finish_interrupted_dry(repo,
                record["action_id"], recorded_at_utc=a3.utc(NOW+20)) == receipt and repo.write_fence() == before)


def structural():
    tree = ast.parse((ROOT / "src/live/runtime_dry_v0_1.py").read_text(encoding="utf-8-sig"))
    imports = {node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)}
    check("DRY_no_mutation_capability_import", not any(any(word in (module or "") for word in
        ("signer", "execution_send", "execution_signed", "chain_evidence", "ledger_settlement")) for module in imports))
    repository_calls = {node.func.attr for node in ast.walk(tree) if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute) and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "repository"}
    check("DRY_closed_Ledger_consumer_surface", repository_calls <= {"action", "reservation", "authority_acceptance", "candidate",
        "_port_at_sequence", "_trusted_read", "_root_attempts", "_simulation_input_digest", "consumer_snapshot", "finish_dry",
        "write_fence", "prepare_attempt", "record_external_attempt_stage"})
    stage_calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name) and node.func.id == "AttemptStageInput"]
    check("DRY_only_exact_simulation_stage_constructible", len(stage_calls) == 1
        and isinstance(stage_calls[0].args[1], ast.Constant) and stage_calls[0].args[1].value == "EXACT_SIMULATED")
    for name in ("run_dry", "finish_interrupted_dry"):
        function = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == name)
        check(name+"_no_signer_sender_injection", not {arg.arg for arg in (*function.args.args, *function.args.kwonlyargs)}
            & {"signer", "sender", "transport", "signed_wire", "simulation_verdict", "simulate", "on_stage"})


def main():
    with tempfile.TemporaryDirectory(prefix="live-runtime-c1-") as tmp:
        directory = Path(tmp)
        for route in ("pump", "swap"):
            successful(directory, route)
        successful(directory, "pump", "simulation-failed")
        interruptions(directory)
        boundaries(directory)
        time_boundaries(directory)
        continuation(directory)
        abrupt_cases(directory)
        structural()
    print(canonical_json({"status": "IMPLEMENTED_PENDING_PROJECT_REVIEW", "checks": len(CHECKS), "all_checks": all(CHECKS.values())}))


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--abrupt":
        abrupt_child(Path(sys.argv[2]), sys.argv[3])
    else:
        main()
