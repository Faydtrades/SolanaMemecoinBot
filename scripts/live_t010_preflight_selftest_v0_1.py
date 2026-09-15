"""M57 strict public fixtures: no T010 execution or live state access."""
from __future__ import annotations

import ast
import copy
import json
import socket
import sqlite3
import sys
import tempfile
from dataclasses import asdict
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from live import acceptance_dossier_v0_1 as d
from live import t010_preflight_v0_1 as gate
from live.authority_controls_v0_1 import (AuthorityPolicyV02, ClockPolicy, CostLimits, EntrySizeLimits,
                                        OperatorProvenance, policy_from_record)
from live.ledger_domain_v0_1 import LedgerDomain
from live.source_health_v0_1 import CursorWitness, SourceBinding, SourceProfile
from phase4.paper_continuous_firstpullback_binding_v0_1 import LOCKED_PARAMETER_SET_BY_ROLE, LOCKED_SELECTION_SHA256

AT = "2026-09-12T20:00:00+00:00"
START = "2026-09-12T19:00:00+00:00"
END = "2026-09-12T21:00:00+00:00"
CHECKS = []


def check(name, value):
    if not value:
        raise AssertionError(name)
    CHECKS.append(name)


def record(path, value):
    path.write_bytes(d.canonical_bytes(value))
    return {"path": str(path), "sha256": d.sha256(path.read_bytes())}


def fixture(root, dossier, profile):
    domain_record = dict(dossier["deployment"]["accepted_target"]["domain"], mode="DRY")
    domain = LedgerDomain(**dict(domain_record, expected_empty_token_accounts=()))
    source_record = copy.deepcopy(profile["source_configuration"]["binding"])
    binding = SourceBinding(**dict(source_record, anchors=tuple(CursorWitness(**a) for a in source_record["anchors"])))
    source_profile = SourceProfile(**profile["source_configuration"]["profile"])
    approved = OperatorProvenance("SYNTHETIC_OWNER", "SYNTHETIC_M09_M10_EXPLICIT", "1" * 64, START, "HUMAN_EXTERNAL")
    parameters = gate.locked_binding.locked_parameter_sets()["CONTROL"]
    policy = AuthorityPolicyV02(domain.economic_domain_id, "SYNTHETIC_EXPLICIT_POLICY", approved, START, END,
        ("PUMP", "PUMPSWAP"), "FINAL-A", parameters.strategy_version, LOCKED_PARAMETER_SET_BY_ROLE["CONTROL"],
        gate.locked_binding._fingerprint(parameters), gate.PRODUCER_MODEL_FINGERPRINT,
        LOCKED_SELECTION_SHA256, "CONTROL", binding.source_identity,
        source_profile.fingerprint, domain.expected_profile_fingerprint, "LEDGER_SETTLEMENT_SUPPORTED_V0_1",
        EntrySizeLimits(1000000, 1, 1000000, 1000000, 1000000, 1000000, 1),
        CostLimits(10000, 10000, 1000, 3000000, 3000000, 10000, 3000000, 3000000, 2, 20000, 100, 100),
        ClockPolicy("SYNTHETIC_QUALIFIED_CLOCK", "4" * 64, 1000000, 100, 3600000000))
    policy_record = json.loads(d.canonical_bytes(asdict(policy)))
    common = {"origin": "HUMAN_EXTERNAL", "operator_id": "SYNTHETIC_OWNER", "recorded_at_utc": START,
              "dossier_sha256": d.digest(dossier), "source_content_digest": dossier["source"]["content_digest"],
              "purpose": "T010", "capabilities": gate.CAPABILITIES}
    policy_ref = record(root / "policy-approval.json", dict(common, schema="MEME_LIVE_M09_M10_APPROVAL_V1",
        approval_reference=approved.approval_reference,
        approved_input={k: v for k, v in policy_record.items() if k != "approval"}))
    policy_record["approval"]["approval_record_digest"] = policy_ref["sha256"]
    actual_policy = policy_from_record(policy_record)
    grant = {"grant_id": "SYNTHETIC_DRY_ONLY", "policy_digest": actual_policy.content_digest,
             "scope": "DRY", "root_id": None, "entry_valid_from_utc": START, "entry_valid_through_utc": END}
    grant_ref = record(root / "gate-approval.json", dict(common, schema="MEME_LIVE_M58_T010_APPROVAL_V1",
        approval_reference="SYNTHETIC_M58_EXPLICIT", approved_input=copy.deepcopy(grant)))
    grant["approval"] = dict(policy_record["approval"], approval_reference="SYNTHETIC_M58_EXPLICIT",
                             approval_record_digest=grant_ref["sha256"])
    review_ref = record(root / "project-review.json", {"schema": "MEME_LIVE_STEP11C_PROJECT_REVIEW_V1",
        "authority": "CHATGPT_PROJECT_REVIEW", "decision": "ACCEPTED_FOR_T010_PREFLIGHT",
        "reviewed_rows": {"M56": "VERIFIED", "M57": "VERIFIED"}, "dossier_sha256": d.digest(dossier),
        "source_content_digest": dossier["source"]["content_digest"],
        "approval_reference": "SYNTHETIC_REVIEW_ONLY_NOT_PROJECT_DECISION", "recorded_at_utc": START})
    return {"schema": gate.PACKAGE_SCHEMA, "dossier_sha256": d.digest(dossier),
        "source_content_digest": dossier["source"]["content_digest"], "runtime_components": dossier["runtime_components"],
        "capabilities": copy.deepcopy(gate.CAPABILITIES), "deployment_binding": gate.deployment_binding(dossier),
        "dry_configuration": {"domain": domain_record,
            "store_paths": {k: str(root / ("synthetic-" + k + ".sqlite3")) for k in ("ledger", "producer", "operations", "evidence_store")},
            "source_binding": source_record, "source_profile": profile["source_configuration"]["profile"],
            "start_after_p1_rowid": binding.anchors[0].rowid,
            "database_identity": dossier["deployment"]["accepted_target"]["paths"]["market_source"],
            "producer_profile": profile["constructor_configuration"]["continuation_profile"],
            "dispatch": profile["constructor_configuration"]["dispatch"],
            "restart_profile": profile["constructor_configuration"]["restart_profile"],
            "reviewed_guard_limits": dossier["deployment"]["reviewed_guard_limits"]},
        "policy": policy_record, "grant": grant, "policy_approval": policy_ref,
        "gate_approval": grant_ref, "project_review": review_ref}


def main():
    tree = ast.parse((ROOT / "src/live/t010_preflight_v0_1.py").read_text(encoding="utf-8"))
    calls = {node.func.attr if isinstance(node.func, ast.Attribute) else node.func.id
             for node in ast.walk(tree) if isinstance(node, ast.Call) and isinstance(node.func, (ast.Name, ast.Attribute))}
    check("preflight_has_no_runtime_sign_send_or_mutation_calls", not calls & {
        "connect", "initialize", "reopen", "run_dry", "start_live", "sign_exact", "send_exact",
        "record_authority_control", "write_bytes", "write_text", "unlink", "mkdir"})
    dossier = d.build_dossier(ROOT)
    profile = d.read_json(dossier["deployment"]["profile"]["path"])
    with tempfile.TemporaryDirectory(prefix="m57-selftest-") as directory:
        root = Path(directory)
        package = fixture(root, dossier, profile)
        validate = lambda value: gate.validate_package_structure(dossier, value, evaluated_at_utc=AT, profile=profile)
        result = validate(package)
        check("positive_explicit_synthetic_structure", result["state"] == "STRUCTURALLY_COMPLETE")
        check("structure_not_authentication", result["authenticated_owner_or_project_approval"] is False)
        check("structure_no_inventory_or_store_claim", not result["store_state_examined"] and not result["current_source_or_wallet_health_established"])

        def deny(name, mutate, reason=None):
            changed = copy.deepcopy(package)
            mutate(changed)
            try:
                validate(changed)
            except (ValueError, TypeError, KeyError, OSError) as exc:
                check(name, reason is None or reason in str(exc))
            else:
                raise AssertionError(name + ": accepted")

        deny("wrong_freeze", lambda p: p.update(dossier_sha256="0" * 64), "PACKAGE_FREEZE_CONFLICT")
        deny("wrong_source_content", lambda p: p.update(source_content_digest="0" * 64), "PACKAGE_FREEZE_CONFLICT")
        deny("missing_engineering_components", lambda p: p["runtime_components"].pop("ownership"), "ALTERNATE_RUNTIME_COMPONENTS")
        deny("alternate_gate_only_root", lambda p: p["runtime_components"]["normal_live_composition"].update(path="alternate.py"), "ALTERNATE_RUNTIME_COMPONENTS")
        deny("arbitrary_driver_field", lambda p: p.update(driver="untrusted.callback"), "EXPLICIT_T010_PACKAGE_REQUIRED")
        deny("unsupported_mode", lambda p: p["capabilities"].update(mode="LIVE"), "UNSUPPORTED_MODE_OR_CAPABILITY")
        for capability in ("sign", "send", "broadcast", "fabricated_fill", "fabricated_inventory", "capital_authority"):
            deny("deny_" + capability, lambda p, key=capability: p["capabilities"].update({key: True}), "UNSUPPORTED_MODE_OR_CAPABILITY")
        deny("bool_integer_capability_rejected", lambda p: p["capabilities"].update(sign=0), "UNSUPPORTED_MODE_OR_CAPABILITY")
        deny("wrong_profile", lambda p: p["deployment_binding"].update(profile_sha256="0" * 64), "DEPLOYMENT_BINDING_CONFLICT")
        deny("wrong_host", lambda p: p["deployment_binding"].update(host_identity_digest="0" * 64), "DEPLOYMENT_BINDING_CONFLICT")
        deny("missing_dry_configuration", lambda p: p.update(dry_configuration=None), "EXPLICIT_DRY_CONFIGURATION_REQUIRED")
        deny("live_domain_reuse", lambda p: p["dry_configuration"]["domain"].update(mode="LIVE"), "FIXED_SEPARATE_DRY_DOMAIN_REQUIRED")
        deny("invented_wallet_balance", lambda p: p["dry_configuration"]["domain"].update(known_native_wallet_lamports=1), "NO_INVENTED_WALLET_BASELINE")
        deny("canonical_store_reuse", lambda p: p["dry_configuration"]["store_paths"].update(ledger=dossier["deployment"]["accepted_target"]["paths"]["ledger"]), "DRY_STORES_MUST_BE_DISTINCT")
        deny("duplicate_dry_stores", lambda p: p["dry_configuration"]["store_paths"].update(ledger=p["dry_configuration"]["store_paths"]["producer"]), "DRY_STORES_MUST_BE_DISTINCT")
        deny("unqualified_workload_cap", lambda p: p["dry_configuration"]["producer_profile"].update(active_mints=65), "UNQUALIFIED_CONFIGURATION")
        deny("unqualified_resource_guard", lambda p: p["dry_configuration"]["reviewed_guard_limits"].update(STARTUP_US=99999999), "UNQUALIFIED_RESOURCE_GUARDS")
        deny("source_start_not_inferred", lambda p: p["dry_configuration"].pop("start_after_p1_rowid"), "EXPLICIT_DRY_CONFIGURATION_REQUIRED")
        deny("arbitrary_source_lineage", lambda p: p["dry_configuration"]["source_binding"].update(lineage="UNQUALIFIED_REPLACEMENT"), "UNQUALIFIED_SOURCE_BINDING_REPLACEMENT")
        deny("m09_missing_no_default", lambda p: p["policy"].pop("selected_track"), "EXPLICIT_M09_M10_POLICY_FIELDS_REQUIRED")
        deny("m09_multiple_tracks", lambda p: p["policy"].update(selected_track=["FINAL-A", "FINAL-B"]))
        deny("m10_missing_no_default", lambda p: p["policy"].pop("costs"), "EXPLICIT_M09_M10_POLICY_FIELDS_REQUIRED")
        deny("missing_nested_cost", lambda p: p["policy"]["costs"].pop("failed_attempt_count"), "EXPLICIT_POLICY_COSTS_REQUIRED")
        deny("policy_version_not_defaulted", lambda p: p["policy"].pop("version"), "EXPLICIT_M09_M10_POLICY_FIELDS_REQUIRED")
        deny("old_policy_unsupported", lambda p: p["policy"].update(version="live_authority_controls_v0.1"), "SUPPORTED_V02_POLICY_REQUIRED")
        deny("wrong_locked_model", lambda p: p["policy"].update(model_fingerprint="0" * 64), "LOCKED_STRATEGY_PARAMETER_OR_MODEL_CONFLICT")
        deny("wrong_locked_parameters", lambda p: p["policy"].update(parameter_fingerprint="0" * 64), "LOCKED_STRATEGY_PARAMETER_OR_MODEL_CONFLICT")
        deny("wrong_locked_strategy", lambda p: p["policy"].update(strategy_version="v999"), "LOCKED_STRATEGY_PARAMETER_OR_MODEL_CONFLICT")
        deny("invalid_economics", lambda p: p["policy"]["size"].update(fixed_quote_lamports=-1))
        deny("unsupported_multi_position", lambda p: p["policy"]["size"].update(max_open_positions=2), "UNSUPPORTED_MULTI_POSITION_POLICY")
        deny("stale_policy", lambda p: p["policy"].update(entry_valid_through_utc=START), "POLICY_NOT_CURRENT")
        deny("grant_missing", lambda p: p.update(grant=None), "EXPLICIT_DRY_GRANT_FIELDS_REQUIRED")
        deny("live_grant_rejected", lambda p: p["grant"].update(scope="ENTRY_NORMAL"), "EXACT_DRY_GRANT_REQUIRED")
        deny("grant_wrong_policy", lambda p: p["grant"].update(policy_digest="0" * 64), "EXACT_DRY_GRANT_REQUIRED")
        deny("stale_grant", lambda p: p["grant"].update(entry_valid_through_utc=START), "GRANT_NOT_CURRENT")
        deny("missing_owner_proof", lambda p: p.update(policy_approval=None), "EXPLICIT_PUBLIC_RECORD_REFERENCE_REQUIRED")
        deny("wrong_owner_proof_hash", lambda p: p["policy_approval"].update(sha256="0" * 64), "PUBLIC_RECORD_HASH_MISMATCH")
        deny("missing_project_review", lambda p: p.update(project_review=None), "EXPLICIT_PUBLIC_RECORD_REFERENCE_REQUIRED")
        deny("stale_project_review", lambda p: p["project_review"].update(sha256="0" * 64), "PUBLIC_RECORD_HASH_MISMATCH")
        deny("missing_owner_file", lambda p: p["gate_approval"].update(path=str(root / "missing.json")))
        before = {str(p): p.read_bytes() for p in root.rglob("*") if p.is_file()}
        # Verify the real frozen repository and retained immutable graph once;
        # these prohibited I/O calls are traps, not alternate production ports.
        with patch.object(sqlite3, "connect", side_effect=AssertionError("database called")), \
             patch.object(socket, "socket", side_effect=AssertionError("network called")):
            report = gate.preflight(ROOT, dossier, package, evaluated_at_utc=AT)
        check("real_exact_freeze_preflight_structure", report["structural_validation"]["state"] == "STRUCTURALLY_COMPLETE")
        check("static_engineering_not_overridden_by_fixture_review", report["readiness"] == "DENIED" and report["ready"] is False
              and set(gate.STATIC_ENGINEERING_GAPS) <= {r["reason"] for r in report["reasons"]})
        check("reopened_rows_not_project_accepted_by_tooling",
              {r["row"] for r in report["reasons"]
               if r["reason"] == "REOPENED_FIX2_ROW_PENDING_PROJECT_REVIEW"}
              == set(dossier["lifecycle"]["reopened_engineering_rows"]))
        check("no_grant_or_t010", report["grants_permission"] is False and report["t010_executed"] is False)
        check("no_mutation", before == {str(p): p.read_bytes() for p in root.rglob("*") if p.is_file()})
        check("no_stores_initialized", not any(root.glob("*.sqlite3")))
        with patch.object(d, "verify_dossier", return_value=dossier) as verify:
            missing = gate.preflight(ROOT, dossier, evaluated_at_utc=AT)
            check("public_preflight_always_verifies_freeze", verify.call_count == 1)
            check("missing_humans_denied_explicitly", {r.get("row") for r in missing["reasons"] if r["class"] == "HUMAN_EXTERNAL"} == {"M09", "M10", "M58"})
            check("project_review_is_engineering_not_human", any(r["class"] == "ENGINEERING_REVIEW" for r in missing["reasons"]))
        with patch.object(d, "verify_dossier", side_effect=d.DossierError("MISSING_ARTIFACT")):
            rejected = gate.preflight(ROOT, dossier, package, evaluated_at_utc=AT)
            check("missing_engineering_evidence_fail_closed", rejected["ready"] is False and rejected["structural_validation"]["state"] == "INCOMPLETE")
        for malformed in (None, [], "bad", {"not_finite": float("nan")}):
            rejected = gate.preflight(ROOT, malformed, package, evaluated_at_utc=AT)
            check("malformed_freeze_" + type(malformed).__name__, rejected["readiness"] == "DENIED")
    print(json.dumps({"check_count": len(CHECKS), "checks": CHECKS,
                      "status": "IMPLEMENTED_PENDING_PROJECT_REVIEW", "classification": "PARTIAL",
                      "t010_executed": False, "runtime_database_network_sign_send_activity": False}, sort_keys=True))


if __name__ == "__main__":
    main()
