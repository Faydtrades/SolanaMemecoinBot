"""Offline exact-M56 T010 package preflight; no runtime or permission issuance.

Structurally valid owner records are not authenticated approvals. The current
profile must include actual same-root qualification. Structural public records
never authenticate approvals or grant execution permission.
"""
from __future__ import annotations

from dataclasses import asdict, fields
from pathlib import Path

from phase4 import paper_continuous_firstpullback_binding_v0_1 as locked_binding
from . import acceptance_dossier_v0_1 as dossier_api
from .authority_controls_v0_1 import (ArmingGrant, AuthorityPolicyV02, ClockPolicy, CostLimits,
                                    EntrySizeLimits, OperatorProvenance,
                                    grant_from_record, policy_from_record)
from .ledger_domain_v0_1 import LedgerDomain, ledger_utc
from .source_health_v0_1 import CursorWitness, SourceBinding, SourceProfile
from .continuous_producer_v0_2 import MODEL_FINGERPRINT as PRODUCER_MODEL_FINGERPRINT

SCHEMA = "MEME_LIVE_M57_PREFLIGHT_V1"
PACKAGE_SCHEMA = "MEME_LIVE_T010_PACKAGE_V1"
CONTRACT_PATH = "docs/live/MEME_LIVE_STEP11C_GATE_CONTRACT_V1.md"
CAPABILITIES = {"mode": "DRY", "submission": "NO_BROADCAST", "terminal": "NON_SUBMITTED",
                "sign": False, "send": False, "broadcast": False,
                "fabricated_fill": False, "fabricated_inventory": False, "capital_authority": False}
STATIC_ENGINEERING_GAPS = (
    "M57_CONTINUOUS_OWNED_DRY_COMPOSITION_AND_RECOVERY_MISSING",
    "M56_M57_SAME_PRODUCTION_DRY_ARTIFACT_PROFILE_QUALIFICATION_MISSING",
)
PACKAGE_KEYS = {"schema", "dossier_sha256", "source_content_digest", "runtime_components",
                "capabilities", "deployment_binding", "dry_configuration", "policy", "grant",
                "policy_approval", "gate_approval", "project_review"}


def require(condition, reason):
    dossier_api.require(condition, reason)


def _keys(value, expected, reason):
    require(type(value) is dict and set(value) == set(expected), reason)


def _same(left, right):
    # Python equality treats bools as integers; artifact contracts do not.
    return dossier_api.canonical_bytes(left) == dossier_api.canonical_bytes(right)


def _fields(value, cls, reason):
    _keys(value, (field.name for field in fields(cls)), reason)


def _local_path(value):
    require(type(value) is str and value and not value.startswith(("\\\\", "//")), "LOCAL_PUBLIC_PATH_REQUIRED")
    path = Path(value)
    require(path.is_absolute(), "ABSOLUTE_PUBLIC_PATH_REQUIRED")
    return path.resolve()


def _public_record(reference):
    _keys(reference, ("path", "sha256"), "EXPLICIT_PUBLIC_RECORD_REFERENCE_REQUIRED")
    path = _local_path(reference["path"])
    require(path.suffix.lower() == ".json", "PUBLIC_JSON_RECORD_REQUIRED")
    require(path.stat().st_size <= 1048576, "PUBLIC_RECORD_TOO_LARGE")
    raw = path.read_bytes()
    require(dossier_api.sha256(raw) == reference["sha256"], "PUBLIC_RECORD_HASH_MISMATCH")
    value = dossier_api.read_json(path)
    require(type(value) is dict, "PUBLIC_RECORD_OBJECT_REQUIRED")
    return value


def deployment_binding(dossier):
    deployment = dossier["deployment"]
    return {"public_configuration_sha256": deployment["accepted_target"]["public_configuration_sha256"],
            "profile_sha256": deployment["profile"]["sha256"],
            "profile_extension_sha256": deployment["profile_extension"]["sha256"],
            "host_identity_digest": deployment["host_identity_digest"],
            "accepted_live_domain_binding": deployment["accepted_target"]["domain_binding_digest"]}


def _dry_config(value, dossier, profile):
    if dossier.get("supported_dry") is not None:
        from .runtime_dry_profile_v0_1 import load_profile
        supported = load_profile(value)
        require(supported.record["content_digest"] == dossier["supported_dry"]["profile_content_digest"],
            "DRY_CONFIGURATION_QUALIFIED_PROFILE_CONFLICT")
        configured = supported.configuration()
        return configured["domain"], configured["source_binding"], configured["source_profile"]
    _keys(value, ("domain", "store_paths", "source_binding", "source_profile", "start_after_p1_rowid",
                  "database_identity", "producer_profile", "dispatch", "restart_profile", "reviewed_guard_limits"),
          "EXPLICIT_DRY_CONFIGURATION_REQUIRED")
    _fields(value["domain"], LedgerDomain, "EXPLICIT_DRY_DOMAIN_REQUIRED")
    # The current accepted public target has no predeclared token accounts.
    # This is unknown inventory, never a claim that actual accounts are empty.
    require(value["domain"]["expected_empty_token_accounts"] == [], "UNSUPPORTED_DRY_ACCOUNT_PROFILE")
    domain = LedgerDomain(**dict(value["domain"], expected_empty_token_accounts=()))
    target = dossier["deployment"]["accepted_target"]
    require(domain.mode == "DRY" and domain.economic_domain_id != target["economic_domain_id"], "FIXED_SEPARATE_DRY_DOMAIN_REQUIRED")
    for name in ("wallet", "genesis_hash", "expected_profile_fingerprint", "schema_version", "domain_version", "opening_profile"):
        require(getattr(domain, name) == target["domain"][name], "DRY_ACCEPTED_DOMAIN_CONFLICT:" + name)
    require(domain.known_native_wallet_lamports is None, "NO_INVENTED_WALLET_BASELINE")
    _keys(value["store_paths"], ("ledger", "producer", "operations", "evidence_store"), "EXPLICIT_DRY_STORE_PATHS_REQUIRED")
    paths = [_local_path(path) for path in value["store_paths"].values()]
    forbidden = {_local_path(target["paths"][key]) for key in ("ledger", "producer", "operations", "evidence_store", "market_source")}
    require(len(set(paths)) == len(paths) and not set(paths) & forbidden,
            "DRY_STORES_MUST_BE_DISTINCT_FROM_LIVE_AND_RAW")
    require(all(path.suffix.lower() in (".sqlite", ".sqlite3", ".db") for path in paths), "UNSUPPORTED_DRY_STORE_PATH")
    require(_local_path(value["database_identity"]) == _local_path(target["paths"]["market_source"]), "SOURCE_DATABASE_IDENTITY_CONFLICT")
    _fields(value["source_binding"], SourceBinding, "EXPLICIT_SOURCE_BINDING_REQUIRED")
    anchors = tuple(CursorWitness(**item) for item in value["source_binding"]["anchors"])
    binding = SourceBinding(**dict(value["source_binding"], anchors=anchors))
    require(_same(value["source_binding"], profile["source_configuration"]["binding"]),
            "UNQUALIFIED_SOURCE_BINDING_REPLACEMENT")
    _fields(value["source_profile"], SourceProfile, "EXPLICIT_SOURCE_PROFILE_REQUIRED")
    source_profile = SourceProfile(**value["source_profile"])
    require(type(value["start_after_p1_rowid"]) is int
            and value["start_after_p1_rowid"] == anchors[0].rowid
            == profile["source_configuration"]["qualification_start_after_p1_rowid"],
            "EXPLICIT_SOURCE_START_BINDING_REQUIRED")
    require(value["source_profile"] == profile["source_configuration"]["profile"], "UNQUALIFIED_SOURCE_PROFILE")
    for field in ("producer_profile", "dispatch", "restart_profile"):
        original = "continuation_profile" if field == "producer_profile" else field
        require(_same(value[field], profile["constructor_configuration"][original]), "UNQUALIFIED_CONFIGURATION:" + field)
    require(_same(value["reviewed_guard_limits"], dossier["deployment"]["reviewed_guard_limits"]), "UNQUALIFIED_RESOURCE_GUARDS")
    return domain, binding, source_profile


def _policy(value, domain, binding, source_profile, at):
    _fields(value, AuthorityPolicyV02, "EXPLICIT_M09_M10_POLICY_FIELDS_REQUIRED")
    for field, cls in (("approval", OperatorProvenance), ("size", EntrySizeLimits),
                       ("costs", CostLimits), ("clock", ClockPolicy)):
        _fields(value[field], cls, "EXPLICIT_POLICY_" + field.upper() + "_REQUIRED")
    require(value["version"] == "live_authority_policy_v0.2", "SUPPORTED_V02_POLICY_REQUIRED")
    policy = policy_from_record(value)
    parameters = locked_binding.locked_parameter_sets()[policy.winner_role]
    require(policy.strategy_version == parameters.strategy_version
            and policy.parameter_fingerprint == locked_binding._fingerprint(parameters)
            and policy.model_fingerprint == PRODUCER_MODEL_FINGERPRINT,
            "LOCKED_STRATEGY_PARAMETER_OR_MODEL_CONFLICT")
    require(type(policy) is AuthorityPolicyV02 and policy.economic_domain_id == domain.economic_domain_id,
            "POLICY_DRY_DOMAIN_CONFLICT")
    require(policy.source_identity == binding.source_identity
            and policy.source_profile_fingerprint == source_profile.fingerprint
            and policy.wallet_profile_fingerprint == domain.expected_profile_fingerprint,
            "POLICY_SOURCE_OR_WALLET_CONFLICT")
    require(policy.size.max_open_positions <= 1, "UNSUPPORTED_MULTI_POSITION_POLICY")
    require(policy.entry_valid_from_utc <= at <= policy.entry_valid_through_utc, "POLICY_NOT_CURRENT")
    require(policy.approval.recorded_at_utc <= at, "POLICY_APPROVAL_FROM_FUTURE")
    return policy


def _owner_inputs(package, policy, at):
    _fields(package["grant"], ArmingGrant, "EXPLICIT_DRY_GRANT_FIELDS_REQUIRED")
    _fields(package["grant"]["approval"], OperatorProvenance, "EXPLICIT_GRANT_APPROVAL_REQUIRED")
    grant = grant_from_record(package["grant"])
    require(grant.scope == "DRY" and grant.root_id is None and grant.policy_digest == policy.content_digest,
            "EXACT_DRY_GRANT_REQUIRED")
    require(policy.entry_valid_from_utc <= grant.entry_valid_from_utc <= at <= grant.entry_valid_through_utc
            <= policy.entry_valid_through_utc, "GRANT_NOT_CURRENT_OR_OUTSIDE_POLICY")
    require(grant.approval.recorded_at_utc <= at, "GRANT_APPROVAL_FROM_FUTURE")
    for key, supplied, approval, schema in (
            ("policy_approval", package["policy"], policy.approval, "MEME_LIVE_M09_M10_APPROVAL_V1"),
            ("gate_approval", package["grant"], grant.approval, "MEME_LIVE_M58_T010_APPROVAL_V1")):
        record = _public_record(package[key])
        _keys(record, ("schema", "origin", "operator_id", "approval_reference", "recorded_at_utc",
                       "dossier_sha256", "source_content_digest", "purpose", "capabilities", "approved_input"),
              "UNSUPPORTED_OWNER_APPROVAL_RECORD")
        require(record["schema"] == schema and record["origin"] == "HUMAN_EXTERNAL"
                and record["operator_id"] == approval.operator_id
                and record["approval_reference"] == approval.approval_reference
                and ledger_utc(record["recorded_at_utc"]) == approval.recorded_at_utc
                and package[key]["sha256"] == approval.approval_record_digest,
                "OWNER_APPROVAL_PROVENANCE_CONFLICT")
        require(record["dossier_sha256"] == package["dossier_sha256"]
                and record["source_content_digest"] == package["source_content_digest"]
                and record["purpose"] == "T010" and _same(record["capabilities"], CAPABILITIES),
                "OWNER_APPROVAL_FREEZE_OR_SCOPE_CONFLICT")
        require(_same(record["approved_input"], {k: v for k, v in supplied.items() if k != "approval"}),
                "M09_M10_OR_GRANT_CHOICES_NOT_APPROVED")
    return grant


def _project_review(reference, package, at):
    review = _public_record(reference)
    _keys(review, ("schema", "authority", "decision", "reviewed_rows", "dossier_sha256", "source_content_digest",
                   "approval_reference", "recorded_at_utc"), "EXPLICIT_PROJECT_REVIEW_REQUIRED")
    require(review["schema"] == "MEME_LIVE_STEP11C_PROJECT_REVIEW_V1"
            and review["authority"] == "CHATGPT_PROJECT_REVIEW"
            and review["decision"] == "ACCEPTED_FOR_T010_PREFLIGHT"
            and review["reviewed_rows"] == {"M56": "VERIFIED", "M57": "VERIFIED"}, "PROJECT_REVIEW_NOT_ACCEPTED")
    require(review["dossier_sha256"] == package["dossier_sha256"]
            and review["source_content_digest"] == package["source_content_digest"], "PROJECT_REVIEW_FREEZE_CONFLICT")
    require(type(review["approval_reference"]) is str and bool(review["approval_reference"])
            and ledger_utc(review["recorded_at_utc"]) <= at, "PROJECT_REVIEW_PROVENANCE_INVALID")


def validate_package_structure(dossier, package, *, evaluated_at_utc, profile):
    """Pure structure/record validation, NOT freeze verification or readiness.

    The separate public preflight always verifies the full freeze first. This
    helper exposes focused deterministic fixture coverage without runtime work.
    """
    at = ledger_utc(evaluated_at_utc)
    _keys(package, PACKAGE_KEYS, "EXPLICIT_T010_PACKAGE_REQUIRED")
    require(package["schema"] == PACKAGE_SCHEMA, "UNSUPPORTED_PACKAGE_SCHEMA")
    require(package["dossier_sha256"] == dossier_api.digest(dossier)
            and package["source_content_digest"] == dossier["source"]["content_digest"], "PACKAGE_FREEZE_CONFLICT")
    require(_same(package["runtime_components"], dossier["runtime_components"]), "ALTERNATE_RUNTIME_COMPONENTS")
    require(_same(package["capabilities"], CAPABILITIES), "UNSUPPORTED_MODE_OR_CAPABILITY")
    require(_same(package["deployment_binding"], deployment_binding(dossier)), "DEPLOYMENT_BINDING_CONFLICT")
    require(CONTRACT_PATH in dossier["contracts"], "MISSING_GATE_CONTRACT")
    domain, binding, source_profile = _dry_config(package["dry_configuration"], dossier, profile)
    policy = _policy(package["policy"], domain, binding, source_profile, at)
    grant = _owner_inputs(package, policy, at)
    _project_review(package["project_review"], package, at)
    return {"state": "STRUCTURALLY_COMPLETE", "dry_domain_id": domain.economic_domain_id,
            "dry_configuration_digest": dossier_api.digest(package["dry_configuration"]),
            "policy_digest": policy.content_digest, "grant_id": grant.grant_id,
            "authenticated_owner_or_project_approval": False,
            "store_state_examined": False, "current_source_or_wallet_health_established": False,
            "source_binding_scope": "EXACT_QUALIFIED_PROFILE; current public environment remains external" if dossier.get("supported_dry") else "EXACT_ACCEPTED_QUALIFICATION_BINDING; activation start remains unselected"}


def preflight(repo_root, dossier, package=None, *, evaluated_at_utc):
    """Read-only report. Static engineering gaps cannot be overridden by input."""
    reasons = [{"class": "ENGINEERING", "reason": gap} for gap in STATIC_ENGINEERING_GAPS]
    structure = {"state": "INCOMPLETE"}
    frozen_digest = package_digest = None
    source_digest = None
    try:
        frozen_digest = dossier_api.digest(dossier)
        package_digest = None if package is None else dossier_api.digest(package)
        at = ledger_utc(evaluated_at_utc)
        verified = dossier_api.verify_dossier(repo_root, dossier)
        source_digest = verified["source"]["content_digest"]
        if verified.get("supported_dry") is not None:
            reasons = []
        if package is None:
            reasons += [{"class": "HUMAN_EXTERNAL", "row": row, "reason": reason} for row, reason in (
                ("M09", "EXPLICIT_SELECTED_TRACK_NOT_SUPPLIED"),
                ("M10", "EXPLICIT_SIZE_COST_CLOCK_POLICY_NOT_SUPPLIED"),
                ("M58", "SEPARATE_T010_DRY_APPROVAL_AND_GRANT_NOT_SUPPLIED"))]
            reasons.append({"class": "ENGINEERING_REVIEW", "reason": "M56_M57_PROJECT_REVIEW_NOT_SUPPLIED"})
            reasons.append({"class": "CONFIGURATION", "reason": "EXPLICIT_SEPARATE_DRY_CONFIGURATION_NOT_SUPPLIED"})
        else:
            profile = dossier_api.read_json(verified["deployment"]["profile"]["path"])
            structure = validate_package_structure(verified, package, evaluated_at_utc=at, profile=profile)

    except (ValueError, TypeError, KeyError, OSError, OverflowError) as exc:
        reasons.append({"class": "INVALID_INPUT_OR_EVIDENCE", "reason": str(exc)})
    return {"schema": SCHEMA, "implementation_status": "IMPLEMENTED_PENDING_PROJECT_REVIEW",
            "dossier_sha256": frozen_digest, "source_content_digest": source_digest,
            "package_sha256": package_digest,
            "evaluated_at_utc": evaluated_at_utc if type(evaluated_at_utc) is str else None, "structural_validation": structure,
            "readiness": "DENIED" if reasons else "STRUCTURALLY_READY_PENDING_EXTERNAL_AUTHORIZATION",
            "ready": not reasons, "readiness_scope":"STRUCTURAL_PREFLIGHT_ONLY", "reasons": reasons,
            "authenticated_approval":False, "current_environment_proven":False,
            "external_execution_boundary":"Separate explicit user/project T010 authorization and original current runtime source/clock/wallet evidence are required; structural readiness alone authorizes nothing.",
            "grants_permission": False, "capital_authority": False,
            "t010_executed": False, "capabilities": CAPABILITIES,
            "boundary": "Record validation is not authentication, runtime execution, store initialization or an Authority command. Engineering coverage requires exact supported profile and qualification; actual public environment and approvals remain external.",
            "downstream_human_rows_not_prerequisites_to_t010": ["M59", "M60", "M61"]}
