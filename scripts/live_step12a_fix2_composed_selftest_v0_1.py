"""FIX2 composed engineering fixtures. Never an actual public qualification.

Only external facts/input validation are replaced in orchestration fixtures.
Original bootstrap, source producer, Authority, Runtime, Ledger, owned restart,
simulation receipts and qualification writer execute on disposable databases.
"""
from __future__ import annotations
from contextlib import ExitStack
from dataclasses import asdict
import copy
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "scripts")]
import httpx
import live_runtime_dry_profile_selftest_v0_1 as fixtures
from live import runtime_dry_public_qualification_v0_1 as qualifier
from live.authority_controls_v0_1 import ControlCommand
from live.runtime_dry_public_driver_v0_1 import DryPublicFacts
from live.runtime_composition_v0_1 import EntryFacts
from live.execution_message_v0_1 import ComputeBudget
from live.acceptance_dossier_v0_1 import canonical_bytes, sha256

CHECKS = {}


def check(name, value):
    if not value:
        raise AssertionError(name)
    CHECKS[name] = True


def qualifier_orchestration(directory):
    owned = fixtures.owned
    refs = fixtures.references()
    directory = Path(directory)
    directory.mkdir(exist_ok=True, parents=True)
    with fixtures.accepted_public_fixture(refs) as accepted, ExitStack() as stack:
        from live.execution_signer_v0_1 import AutonomousLocalSigner
        from live.execution_send_v0_1 import SolanaSendTransport
        for cls, method in ((AutonomousLocalSigner, "__init__"), (AutonomousLocalSigner, "sign_exact"),
                            (SolanaSendTransport, "__init__"), (httpx.HTTPTransport, "handle_request")):
            stack.enter_context(patch.object(cls, method, side_effect=AssertionError("FORBIDDEN_REAL_CAPABILITY")))
        original = owned.fixture(directory, "orchestration-seed")
        env = SimpleNamespace(**vars(original))
        boundary = {"current": None}
        try:
            inputs = fixtures.inputs_for(original, refs, accepted)
            from live.continuous_producer_v0_2 import STORAGE_FINGERPRINT
            producer_profile = accepted["constructor_configuration"]["continuation_profile"]
            amendment = {"schema":"MEME_LIVE_DRY_STORAGE_QUALIFICATION_AMENDMENT_V1",
                "scope":"DETERMINISTIC_QUALIFICATION_ONLY", "status":"IMPLEMENTED_PENDING_PROJECT_REVIEW",
                "project_acceptance_claimed":False, "production_capacity_claimed":False,
                "accepted_profile":refs["accepted_profile"], "accepted_extension":refs["accepted_extension"],
                "producer_storage_fingerprint":STORAGE_FINGERPRINT, "producer_profile":producer_profile,
                "proposed_guard_limits":{"HISTORY_BYTES":producer_profile["history_bytes"],"TOMBSTONE_ROWS":2}}
            amendment_path = directory/"synthetic-storage-amendment.json"
            amendment_path.write_bytes(canonical_bytes(amendment))
            inputs["guard_amendment"] = {"path":str(amendment_path),"sha256":sha256(amendment_path.read_bytes())}
            inputs["monitor"]["resource_limits"].update(amendment["proposed_guard_limits"])
            inputs["store_paths"] = {key: str(directory / "fresh" / (key + ".sqlite"))
                                     for key in inputs["store_paths"]}
            inputs["driver"]["max_steps"] = 128
            profile = fixtures.profile_api.build_profile(inputs)
            state = original.repo.authority_snapshot()
            policy, grant = state["policy"], state["armed_entry_grant"]
            commands = (ControlCommand("fixture-install", original.domain.economic_domain_id,
                "INSTALL_POLICY", policy.approval, policy=policy),
                ControlCommand("fixture-arm", original.domain.economic_domain_id,
                "ARM", grant.approval, grant=grant))
            source = original.source.latest_record()[1]
            facts_path = directory / "fake-facts.json"
            facts_path.write_bytes(b"{}")
            request = {"schema": qualifier.REQUEST_SCHEMA,
                "profile": {"path": str(directory / "synthetic-profile.json"), "sha256": "0"*64},
                "facts": {"path": str(facts_path), "sha256": sha256(facts_path.read_bytes())},
                "controls": {"path": str(directory / "synthetic-controls.json"), "sha256": "0"*64},
                "review_reference": "SYNTHETIC_ORCHESTRATION_ONLY_NOT_PUBLIC_QUALIFICATION"}

            class ScriptedExternalFacts:
                scope = "TEST_ONLY_FAKE_EXTERNAL_TRANSPORT"
                observations = []
                startup_us = None

                def __init__(self, *_args, **_kwargs):
                    self.public_clock = SimpleNamespace(policy=policy.clock)
                    self.at = owned.NOW + 4
                    self.second = False
                    self.active_seed = None
                    self.cold_reopen_count = 0

                def begin_cold_reopen(self):
                    # Renew the scripted clock while retaining policy and facts.
                    # Drop the pre-close sample so clock(started) uses the reopened Ledger.
                    self.public_clock = SimpleNamespace(policy=self.public_clock.policy)
                    self.active_seed = None
                    self.startup_us = None
                    self.cold_reopen_count += 1
                    check("scripted_clock_cold_reopen_once", self.cold_reopen_count == 1)

                def clock(self, started=None):
                    if self.active_seed is not None:
                        return self.active_seed.clock
                    return owned.a3.clock(original.repo if started is None else started.runtime.ledger, self.at)

                def wallet(self):
                    return None

                def source(self):
                    return source

                def __call__(self, started):
                    env.started, env.runtime = started, started.runtime
                    env.repo, env.producer, env.handoff, env.source = (env.runtime.ledger, env.runtime.producer,
                                                                   env.runtime.handoff, env.runtime.source)
                    env.conn = env.runtime._producer_conn
                    env.startup_monitor_config = env.runtime._operations_degradation.configuration
                    if not self.second and env.repo._conn.execute(
                            "SELECT COUNT(*) FROM ledger_consumer_groups WHERE kind='DRY_NON_SUBMITTED'").fetchone()[0] == 1:
                        original.append(owned.rt.hf.NEXT)
                        self.second = True
                    self.at = owned.NOW + (18 if self.second else 4)
                    self.active_seed = None
                    wallet = None
                    entry = None
                    if env.runtime._entry_action_id is not None and not env.runtime._dry_recovery:
                        self.at = owned.NOW + (23 if self.second else 9)
                        action = env.repo.action(env.runtime._entry_action_id)
                        self.active_seed = owned.seed_for(env, action, self.at)
                        boundary["current"] = owned.external.DryPublicTransport(self.active_seed)
                        wallet = self.active_seed.evidence.wallet
                    elif not env.runtime._dry_recovery and env.runtime.queued_roots:
                        env.root = env.runtime.queued_roots[0]
                        env.item = env.repo.candidate(env.root)
                        wallet = owned.a3.wallet(env, scenario=env.scenario, at=self.at)
                        entry = EntryFacts("PUMP", owned.sf.TOKEN_PROGRAM_ID, wallet)
                    if env.runtime._dry_recovery:
                        self.at = owned.NOW + 23
                    seed = self.active_seed
                    def now():
                        if seed is None:
                            return owned.dry.utc_microseconds(self.clock(started).utc_upper_utc)
                        return (owned.dry.utc_microseconds(seed.clock.utc_upper_utc)
                            if len(boundary["current"].requests) >= 9
                            else seed.evidence.simulation.leases[0].observed_at_us)
                    settings = profile.record["inputs"]["driver"]
                    return DryPublicFacts(lambda:self.clock(started), entry, wallet,
                        fixtures.profile_api.QuotePolicyV01(**settings["quote_policy"]),
                        fixtures.profile_api.TransactionPlanPolicyV01(**settings["plan_policy"]),
                        ComputeBudget(**settings["compute"]), now, lambda:fixtures.host(env, self.at),
                        owned.a3.utc(owned.NOW + (15 if self.second else 1)))

            transport = httpx.MockTransport(lambda req: boundary["current"](req)
                if boundary["current"] is not None else (_ for _ in ()).throw(AssertionError("unexpected public call")))
            def rpc_init(self, *args, **kwargs):
                # Original deterministic RPC fixture records its own public times.
                kwargs["now_us"] = lambda: boundary["current"].now() if boundary["current"] is not None else 0
                owned.EXECUTION_RPC_INIT(self, *args, **kwargs)
            stack.enter_context(patch.object(qualifier, "validate_request", return_value=(profile, commands)))
            stack.enter_context(patch.object(qualifier, "ReviewedPublicFacts", ScriptedExternalFacts))
            stack.enter_context(patch.object(owned.ExecutionReadOnlyRpc, "__init__", rpc_init))
            try:
                result = qualifier.qualify(request, test_transport=transport)
            except Exception as error:
                if hasattr(error,"qualification_diagnostics"):
                    (directory/"failed-qualification.json").write_bytes(canonical_bytes(error.qualification_diagnostics))
                raise
            check("qualifier_full_original_orchestration_two_simulations", result["simulation_count"] == 2)
            check("qualifier_full_original_cold_recovery", result["cold_interruption_recovery_count"] == 1
                  and len(result["startup_audits"]) == 2)
            check("qualifier_full_orchestration_stays_test_only", result["scope"] == "TEST_ONLY_FAKE_EXTERNAL_TRANSPORT")
            check("qualifier_full_orchestration_preserves_root_identity", result["cold_recovery"]["before"]["root_id"]
                  == result["cold_recovery"]["after"]["root_id"])
            try:
                qualifier.validate_evidence(result, profile)
            except ValueError as exc:
                check("full_fake_orchestration_cannot_qualify_public", "SCOPE_OR_HASH" in str(exc))
            else:
                raise AssertionError("fake evidence qualified public")
            # An in-memory codec probe only: request validation is already
            # explicitly patched above. It MUST still stop at absent actual
            # observations; never persist or publish this relabelled probe.
            probe = copy.deepcopy(result)
            probe['scope'] = 'PUBLIC_ENVIRONMENT_REVIEWED'
            def rejection(value):
                value.pop('content_digest',None)
                value['content_digest'] = qualifier.content_fingerprint(value)
                try:
                    qualifier.validate_evidence(value,profile)
                except ValueError as exc:
                    return str(exc)
                raise AssertionError('codec probe qualified public')
            check('original_receipts_and_recovery_decode_before_actual_observation_gate',
                rejection(probe) == 'PUBLIC_DRY_ACTUAL_PUBLIC_OBSERVATIONS_REQUIRED')
            changed = copy.deepcopy(probe)
            changed['terminals'][0]['terminal_digest'] = 'f'*64
            check('changed_original_terminal_receipt_denied', rejection(changed) == 'PUBLIC_DRY_TERMINAL_RECEIPT_CONFLICT')
            changed = copy.deepcopy(probe)
            changed['terminals'][0]['simulation_request_params'][1]['sigVerify'] = True
            check('changed_exact_simulation_parameters_denied', rejection(changed) == 'PUBLIC_DRY_EXACT_ORIGINAL_EVIDENCE_CONFLICT')
            (directory / "synthetic-orchestration-evidence.json").write_bytes(canonical_bytes(result))
            return result
        finally:
            owned.fixtures.close(original)


CONSTITUENTS = (
    "live_runtime_public_clock_selftest_v0_1.py",
    "live_runtime_dry_public_qualification_selftest_v0_1.py",
    "live_runtime_dry_selftest_v0_1.py",
    "live_runtime_dry_profile_selftest_v0_1.py",
    "live_operations_ownership_selftest_v0_1.py",
    "live_operations_supervisor_selftest_v0_1.py",
    "live_acceptance_dossier_selftest_v0_1.py",
    "live_t010_preflight_selftest_v0_1.py",
    "live_t010_public_host_selftest_v0_1.py",
    "live_t010_resource_environment_selftest_v0_1.py",
    "live_wallet_context_retry_selftest_v0_1.py",
    "live_t010_physical_certificate_selftest_v0_1.py",
    "live_t010_measured_guards_selftest_v0_1.py",
    "live_t010_private_memory_selftest_v0_1.py",
    "live_t010_resource_envelope_selftest_v0_1.py",
    "live_t010_work_budget_selftest_v0_1.py",
    "live_t010_boundary_integration_selftest_v0_1.py",
    "live_t010_parent_boundary_selftest_v0_1.py",
    "live_t010_sqlite_boundary_selftest_v0_1.py",
    "live_runtime_public_resource_order_selftest_v0_1.py",
)


def composed_regression(directory):
    import subprocess
    from live.acceptance_dossier_v0_1 import source_freeze
    directory.mkdir(parents=True, exist_ok=False)
    before = source_freeze(ROOT)
    results = []
    for name in CONSTITUENTS:
        command = [sys.executable, "-B", str(ROOT / "scripts" / name)]
        completed = subprocess.run(command, cwd=ROOT, capture_output=True, timeout=240)
        log = directory / (name + ".stdout.txt")
        log.write_bytes(completed.stdout)
        (directory / (name + ".stderr.txt")).write_bytes(completed.stderr)
        check(name + "_exit_zero", completed.returncode == 0)
        value = json.loads(completed.stdout.decode("utf-8-sig").strip())
        results.append({"command": command, "exit_code": completed.returncode,
            "output": {"path": str(log), "sha256": sha256(log.read_bytes())}, "result": value})
    qualifier_orchestration(directory / "qualifier")
    after = source_freeze(ROOT)
    check("exact_sources_unchanged_during_composed_regression", before == after)
    result = {"schema": "MEME_LIVE_STEP12A_FIX2_ENGINEERING_REGRESSION_V1",
        "status": "IMPLEMENTED_PENDING_PROJECT_REVIEW", "source_freeze": after,
        "constituents": results, "checks": CHECKS, "all_checks_true": all(CHECKS.values()),
        "scope": "DETERMINISTIC_ENGINEERING_ONLY", "actual_public_qualification": False,
        "clock_profile_approved": False, "public_guard_adjustment_approved": False,
        "T010_executed": False, "capital_authority": False, "project_acceptance_claimed": False}
    result["content_digest"] = qualifier.content_fingerprint(result)
    (directory / "composed-regression.json").write_bytes(canonical_bytes(result))
    return result


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-directory", type=Path)
    args = parser.parse_args()
    if args.output_directory is not None:
        result = composed_regression(args.output_directory.resolve())
        print(json.dumps({"evidence": str(args.output_directory.resolve() / "composed-regression.json"),
            "content_digest": result["content_digest"], "all_checks_true": result["all_checks_true"],
            "actual_public_qualification": False, "T010_executed": False}, sort_keys=True))
    else:
        with tempfile.TemporaryDirectory(prefix="fix2-composed-") as directory:
            qualifier_orchestration(Path(directory) / "qualifier")
        from live_t010_resource_guard_policy_selftest_v0_1 import run_compatibility_checks
        check("c2_f1_f7_compatibility", run_compatibility_checks() == 9)
        print(json.dumps({"checks": CHECKS, "all_checks_true": all(CHECKS.values()),
            "scope": "DETERMINISTIC_ENGINEERING_ONLY", "actual_public_qualification": False,
            "T010_executed": False}, sort_keys=True))
