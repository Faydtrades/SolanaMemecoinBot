"""Offline Step11-C content freeze. No runtime, database, network or grant access.

Identity is the immutable accepted Git revision PLUS exact current source bytes.
The publication commit contains this index and consequently cannot be its input.
Verification rederives the index; its hash is not a substitute for project review.
"""
from __future__ import annotations

import hashlib
import json
import re
import subprocess
from functools import lru_cache
from pathlib import Path

SCHEMA = "MEME_LIVE_M56_DOSSIER_V1"
BASE_REVISION = "d7993c4dfaff62ac87ed777f8f48366186c5848f"
BRANCH = "live/meme-production-readiness"
FIX2_BRANCH_PREFIX = "codex/step12a-fix2-"
# Absolute source links embedded in immutable accepted evidence refer to this
# development checkout. Its working bytes are not historical evidence.
ACCEPTED_DEVELOPMENT_CHECKOUT = Path(r"C:\Users\Mari1\AppData\Local\Temp\meme-live-audit-e6b9a4b")
# Immutable accepted evidence links absolute paths to meme-live-* roots under
# the Windows Temp folder. Those bytes are preserved outside Temp; every read of
# such a link is served from the rescued copy, never from Temp, while the
# recorded path string remains the artifact's identity and hashes are verified
# against the same values. Other Temp paths (fresh fixtures) are read in place.
TEMP_ROOT = Path(r"C:\Users\Mari1\AppData\Local\Temp")
RESCUED_PREFIX = "meme-live-"
RESCUED_ROOTS = (
    (TEMP_ROOT / "meme-live-fix2-evidence-20260913",
     Path(r"D:\Tradingbot\rescued_evidence\meme-live-fix2-evidence-20260913")),
    (TEMP_ROOT, Path(r"D:\Tradingbot\rescued_temp")),
)
HUMAN_ROWS = ("M09", "M10", "M58", "M59", "M60", "M61")
# Historical accepted Step11-B remainder; never the current matrix policy.
STEP11B_REMAINING_OWNED_ROWS = ("M56", "M57")
PROJECT_REVIEW_ROWS = ("M56", "M57")
INDEX_PATHS = (
    "docs/live/MEME_LIVE_STEP10_INTERNAL_ACCEPTANCE_V0_1.json",
    "docs/live/MEME_LIVE_STEP11A_PROFILE_REVIEW_V1.json",
    "docs/live/MEME_LIVE_STEP11B_HOST_REVIEW_V1.json",
)
MATRIX_PATH = "docs/live/MEME_LIVE_PRODUCTION_LIFECYCLE_MATRIX_V2.md"
NEW_CODE = frozenset({
    "scripts/live_wallet_context_retry_selftest_v0_1.py",
    "src/live/acceptance_dossier_v0_1.py",
    "src/live/t010_preflight_v0_1.py",
    "src/live/t010_driver_v0_1.py",
    "src/live/runtime_dry_public_driver_v0_1.py",
    "src/live/runtime_dry_profile_v0_1.py",
    "scripts/live_runtime_owned_dry_selftest_v0_1.py",
    "scripts/live_runtime_dry_profile_selftest_v0_1.py",
    "scripts/live_runtime_dry_profile_v0_1.py",
    "scripts/live_step11c_affected_regression_v0_1.py",
    "scripts/live_step11c_profile_freeze_selftest_v0_1.py",
    "scripts/live_acceptance_dossier_v0_1.py",
    "scripts/live_acceptance_dossier_selftest_v0_1.py",
    "scripts/live_t010_preflight_v0_1.py",
    "scripts/live_t010_preflight_selftest_v0_1.py",
    "scripts/live_t010_driver_v0_1.py",
    "src/live/runtime_dry_public_facts_v0_1.py",
    "src/live/runtime_public_clock_v0_1.py",
    "src/live/runtime_dry_public_qualification_v0_1.py",
    "src/live/t010_public_host_v0_1.py",
    "scripts/live_runtime_dry_public_qualification_v0_1.py",
    "scripts/live_runtime_dry_public_qualification_selftest_v0_1.py",
    "scripts/live_runtime_public_clock_selftest_v0_1.py",
    "scripts/live_runtime_public_clock_observe_v0_1.py",
    "scripts/live_t010_public_host_v0_1.py",
    "scripts/live_t010_public_host_selftest_v0_1.py",
    "scripts/live_step12a_fix2_composed_selftest_v0_1.py",
    "scripts/live_fix2_pending_guard_delta_selftest_v0_1.py",
    "scripts/live_producer_lifecycle_selftest_v0_1.py",
    "scripts/live_operations_journal_budget_selftest_v0_1.py",
    "scripts/live_t010_work_budget_selftest_v0_1.py",
    "src/live/t010_resource_envelope_v0_1.py",
    "scripts/live_t010_resource_boundary_measure_v0_1.py",
    "scripts/live_t010_resource_envelope_selftest_v0_1.py",
    "src/live/pump_token2022_profile_v0_1.py",
    "src/live/pump_protocol_compatibility_v0_1.py",
    "src/live/pump_current_state_v0_1.py",
    "src/live/simulation_public_cpi_v0_1.py",
    "scripts/live_simulation_public_cpi_selftest_v0_1.py",
    "scripts/live_wallet_cohort_selftest_v0_1.py",
    "scripts/live_wallet_after_venue_selftest_v0_1.py",
    "scripts/live_pump_current_state_selftest_v0_1.py",
    "scripts/live_pump_token2022_evidence_selftest_v0_1.py",
    "scripts/live_pump_token2022_settlement_selftest_v0_1.py",
    "scripts/live_pump_protocol_compatibility_selftest_v0_1.py",
    "scripts/live_operations_receipt_scan_selftest_v0_1.py",
    "scripts/live_producer_cold_replay_selftest_v0_1.py",
    "scripts/live_source_replay_selftest_v0_1.py",
    "scripts/live_t010_cold_fragment_proof_v0_1.py",
    "scripts/live_t010_journal_boundary_measure_v0_1.py",
    "scripts/live_t010_ledger_codec_equivalence_v0_1.py",
    "scripts/live_t010_ledger_cold_integration_selftest_v0_1.py",
    "scripts/live_t010_ledger_cold_recheck_v0_1.py",
    "scripts/live_t010_ledger_cold_tamper_selftest_v0_1.py",
    "scripts/live_t010_ledger_history_measure_v0_1.py",
    "scripts/live_t010_source_capture_selftest_v0_1.py",
    "scripts/live_t010_source_cold_recheck_v0_1.py",
    "scripts/live_t010_source_evolving_boundary_v0_1.py",
    "scripts/live_t010_source_growth_model_v0_1.py",
    "scripts/live_t010_source_history_measure_v0_1.py",
    "scripts/live_runtime_public_resource_order_selftest_v0_1.py",
    "scripts/live_t010_boundary_integration_selftest_v0_1.py",
    "scripts/live_t010_checkpoint_pending_measure_v0_1.py",
    "scripts/live_t010_checkpoint_pending_measure_v0_2.py",
    "scripts/live_t010_joint_hot_measure_v0_2.py",
    "scripts/live_t010_joint_hot_measure_v0_3.py",
    "scripts/live_t010_joint_startup_measure_v0_1.py",
    "scripts/live_t010_joint_startup_measure_v0_2.py",
    "scripts/live_t010_joint_state_v0_1.py",
    "scripts/live_t010_joint_state_v0_2.py",
    "scripts/live_t010_joint_state_v0_3.py",
    "scripts/live_t010_measured_guards_selftest_v0_1.py",
    "scripts/live_t010_monitor_full_state_v0_1.py",
    "scripts/live_t010_monitor_resume_selftest_v0_1.py",
    "scripts/live_t010_parent_boundary_selftest_v0_1.py",
    "scripts/live_t010_physical_certificate_selftest_v0_1.py",
    "scripts/live_t010_private_memory_selftest_v0_1.py",
    "scripts/live_t010_resource_environment_selftest_v0_1.py",
    "scripts/live_t010_resource_guard_policy_selftest_v0_1.py",
    "scripts/live_t010_sqlite_boundary_selftest_v0_1.py",
    "src/live/t010_physical_certificate_v0_1.py",
    "src/live/t010_resource_environment_v0_1.py",
    "src/live/t010_resource_measurement_v0_1.py",
    "src/live/t010_sqlite_boundary_v0_1.py",
    "scripts/live_runtime_acquisition_handoff_selftest_v0_1.py",
})
CORE_OVERLAY = frozenset({
    "src/live/runtime_composition_v0_1.py", "src/live/runtime_reconstruction_v0_1.py",
    "src/live/operations_startup_v0_1.py", "src/live/operations_ownership_v0_1.py",
    "src/live/operations_supervisor_v0_1.py", "src/live/operations_readiness_v0_1.py",
    "src/live/operations_degradation_v0_1.py",
    "src/live/operations_degradation_monitor_v0_1.py",
    "src/live/runtime_dry_v0_1.py",
    "src/live/continuous_producer_v0_2.py",
    "scripts/live_candidate_handoff_selftest_v0_1.py",
    "scripts/live_step10_s01_s02_selftest_v0_1.py",
    "src/live/wallet_evidence_v0_1.py",
    "src/live/public_rpc_v0_1.py",
    "src/live/authority_admission_v0_1.py",
    "src/live/authority_controls_v0_1.py",
    "src/live/continuous_producer_v0_1.py",
    "src/live/evidence_store_v0_1.py",
    "src/live/source_health_v0_1.py",
    "src/live/authority_message_evidence_v0_1.py",
    "src/live/authority_message_codec_v0_1.py",
    "src/live/ledger_settlement_v0_1.py",
    "src/live/ledger_repository_v0_1.py",
    "src/live/execution_message_v0_1.py",
    "src/phase5/shadow_venue_route_quote_v0_1.py",
    "scripts/live_authority_admission_selftest_v0_1.py",
    "scripts/live_authority_message_evidence_selftest_v0_1.py",
    "scripts/live_execution_message_selftest_v0_1.py",
    "scripts/live_execution_composition_selftest_v0_1.py",
    "scripts/live_runtime_composition_selftest_v0_1.py",
    "scripts/live_wallet_immutable_owner_selftest_v0_1.py",
    # P1 (gate T20) reviewed fixture/oracle/helper-binding changes; test code only.
    "scripts/live_operations_degradation_integration_selftest_v0_1.py",
    "scripts/live_operations_readiness_selftest_v0_1.py",
    "scripts/live_runtime_continuation_selftest_v0_1.py",
    "scripts/live_step10_e01_selftest_v0_1.py",
    "scripts/live_step10_e02_partial_selftest_v0_1.py",
    "scripts/live_step10_e03_contradiction_selftest_v0_1.py",
    "scripts/live_step10_s03_s07_selftest_v0_1.py",
    "scripts/live_step10_s04_s08_selftest_v0_1.py",
    "scripts/live_step10_s05_selftest_v0_1.py",
    "scripts/live_step10_s06_buy_recovery_selftest_v0_1.py",
    "scripts/live_step10_s06_nonlanding_protection_selftest_v0_1.py",
    "scripts/live_step10_s09_selftest_v0_1.py",
    "scripts/live_step10_s10_selftest_v0_1.py",
    "scripts/live_step10_s11_selftest_v0_1.py",
    "scripts/live_step10_s12_selftest_v0_1.py",
})
ASSEMBLY = {
    "producer": "src/live/continuous_producer_v0_2.py",
    "handoff": "src/live/candidate_handoff_v0_1.py",
    "authority": "src/live/authority_admission_v0_1.py",
    "normal_live_composition": "src/live/runtime_composition_v0_1.py",
    "dry_execution": "src/live/runtime_dry_v0_1.py",
    "dry_reconstruction": "src/live/runtime_reconstruction_v0_1.py",
    "public_dry_driver": "src/live/runtime_dry_public_driver_v0_1.py",
    "public_facts": "src/live/runtime_dry_public_facts_v0_1.py",
    "public_clock": "src/live/runtime_public_clock_v0_1.py",
    "public_qualification": "src/live/runtime_dry_public_qualification_v0_1.py",
    "public_host": "src/live/t010_public_host_v0_1.py",
    "supported_dry_profile": "src/live/runtime_dry_profile_v0_1.py",
    "exact_message": "src/live/execution_message_v0_1.py",
    "non_submitted_ledger": "src/live/ledger_repository_v0_1.py",
    "ownership": "src/live/operations_ownership_v0_1.py",
    "startup": "src/live/operations_startup_v0_1.py",
    "supervisor": "src/live/operations_supervisor_v0_1.py",
    "degradation_monitor": "src/live/operations_degradation_monitor_v0_1.py",
}


class DossierError(ValueError):
    pass


def _locate(path):
    """Where a linked artifact's bytes are read; the link itself is not rewritten."""
    path = Path(path)
    if not (path.is_relative_to(TEMP_ROOT) and path != TEMP_ROOT
            and path.relative_to(TEMP_ROOT).parts[0].startswith(RESCUED_PREFIX)):
        return path
    for original, rescued in RESCUED_ROOTS:
        if path.is_relative_to(original):
            return rescued / path.relative_to(original)
    return path


def require(condition, reason):
    if not condition:
        raise DossierError(reason)


def canonical_bytes(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":"),
                       ensure_ascii=True, allow_nan=False) + "\n").encode("utf-8")


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def digest(value):
    return sha256(canonical_bytes(value))


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, "DUPLICATE_JSON_KEY:" + key)
        result[key] = value
    return result


def read_json(path):
    try:
        return json.loads(_locate(path).read_text(encoding="utf-8-sig"),
                          object_pairs_hook=_pairs,
                          parse_constant=lambda _: require(False, "NONFINITE_JSON"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise DossierError("UNREADABLE_JSON:" + str(path)) from exc


def _git(root, *args):
    result = subprocess.run(["git", "-C", str(root), *args], capture_output=True)
    require(result.returncode == 0, "GIT_READ_FAILED:" + args[0])
    return result.stdout


def _lf(data):
    return data.replace(b"\r\n", b"\n")


@lru_cache(maxsize=4)
def _base_files(root):
    paths = _git(root, "ls-tree", "-r", "--name-only", BASE_REVISION).decode().splitlines()
    paths = [p for p in paths if p.startswith(("src/", "scripts/live", "docs/live/"))]
    query = "".join(BASE_REVISION + ":" + p + "\n" for p in paths).encode()
    result = subprocess.run(["git", "-C", str(root), "cat-file", "--batch"], input=query,
                            capture_output=True)
    require(result.returncode == 0, "GIT_BLOB_BATCH_FAILED")
    out, offset, files = result.stdout, 0, {}
    for path in paths:
        end = out.index(b"\n", offset)
        header = out[offset:end].split()
        require(len(header) == 3 and header[1] == b"blob", "GIT_BLOB_MISSING")
        size = int(header[2])
        files[path] = out[end + 1:end + 1 + size]
        offset = end + 2 + size
    return files


def source_freeze(root):
    root = Path(root).resolve()
    branch = _git(root, "branch", "--show-current").decode().strip()
    if branch != BRANCH:
        # An explicitly isolated FIX2 worktree must track the existing LIVE
        # lane. Branch identity never substitutes for the exact byte freeze.
        require(branch.startswith(FIX2_BRANCH_PREFIX), "WRONG_BRANCH")
        require(_git(root, "rev-parse", "--abbrev-ref", "@{upstream}").decode().strip()
                == "origin/" + BRANCH, "WRONG_UPSTREAM_BRANCH")
        _git(root, "merge-base", "--is-ancestor",
             "2b57b8642d5dc09ac01a7f9de09d7f42bc803fb2", "HEAD")
    _git(root, "merge-base", "--is-ancestor", BASE_REVISION, "HEAD")
    tracked = _base_files(root)
    # Freeze all production Python dependencies, plus every LIVE entrypoint/test.
    selected = {p for p in tracked if (p.startswith("src/") and p.endswith(".py"))
                or (p.startswith("scripts/live") and p.endswith(".py"))}
    current = {p.relative_to(root).as_posix() for p in (root / "src").rglob("*.py")}
    current |= {p.relative_to(root).as_posix() for p in (root / "scripts").glob("live*.py")}
    require(selected <= current, "MISSING_ACCEPTED_SOURCE")
    require(current - selected <= NEW_CODE, "UNREVIEWED_ADDITIONAL_SOURCE")
    rows = {}
    for path in sorted(current):
        actual = (root / path).read_bytes()
        if path in selected:
            base = tracked[path]
            require(path in CORE_OVERLAY or _lf(actual) == base, "ACCEPTED_SOURCE_CHANGED:" + path)
        rows[path] = {"sha256": sha256(actual), "git_lf_sha256": sha256(_lf(actual)),
                      "provenance": "STEP11C_OVERLAY" if path in NEW_CODE else "STEP11C_CORE_OVERLAY" if path in CORE_OVERLAY else BASE_REVISION}
    return {"accepted_revision": BASE_REVISION,
            "identity_rule": "accepted_revision_plus_exact_content; publication commit is a containing reference",
            "files": rows, "content_digest": digest(rows),
            "accepted_runtime_changed": any(path in tracked and _lf((root/path).read_bytes()) != tracked[path] for path in CORE_OVERLAY),
            "authorized_core_overlay": {path:{"accepted_git_lf_sha256":sha256(tracked[path]),
                "current_sha256":rows[path]["sha256"]} for path in sorted(CORE_OVERLAY) if path in tracked}}


def lifecycle_disposition(text):
    rows = {}
    for line in text.splitlines():
        if not re.match(r"^\| M\d\d \|", line):
            continue
        cells = [v.strip() for v in line.split("|")[1:-1]]
        require(cells[0] not in rows, "DUPLICATE_LIFECYCLE_ROW")
        rows[cells[0]] = cells[3]
    require(set(rows) == {f"M{n:02}" for n in range(1, 62)}, "INCOMPLETE_LIFECYCLE")
    for row, state in rows.items():
        if row in PROJECT_REVIEW_ROWS:
            require(state in ("VERIFIED", "OWNED_NOT_BUILT"),
                    "UNEXPECTED_LIFECYCLE_DISPOSITION:" + row)
            continue
        expected = "HUMAN_EXTERNAL" if row in HUMAN_ROWS else "VERIFIED"
        require(state == expected, "UNEXPECTED_LIFECYCLE_DISPOSITION:" + row)
    return {"authoritative_rows": rows,
            "reopened_engineering_rows": [row for row in PROJECT_REVIEW_ROWS
                                         if rows[row] == "OWNED_NOT_BUILT"],
            "exact_dossier_project_review_required": list(PROJECT_REVIEW_ROWS),
            "human_external": list(HUMAN_ROWS),
            "static_engineering_may_be_deferred_to_t010": False,
            "tooling_promotes_rows": False}


def _references(value):
    """Only explicit file/hash pairs; content digests and DB observations are not files.

    Historical source maps embedded in accepted indexes remain provenance. They
    are resolved separately against their accepted revision, never as current code.
    """
    if isinstance(value, list):
        for item in value:
            yield from _references(item)
    elif isinstance(value, dict):
        if "sha256" in value:
            candidates = [value[k] for k in ("path", "result_path", "observation_path", "manifest")
                          if isinstance(value.get(k), str)]
            if candidates:
                require(len(set(candidates)) == 1, "AMBIGUOUS_HASH_REFERENCE")
                yield candidates[0], value["sha256"]
        for key, path in value.items():
            if key.endswith("_path") and isinstance(path, str):
                hash_key = key[:-5] + "_sha256"
                if hash_key in value:
                    yield path, value[hash_key]
            if isinstance(path, (dict, list)):
                yield from _references(path)


class EvidenceGraph:
    """Read immutable linked proof only. Never connect to or hash runtime stores."""
    def __init__(self, root):
        self.root = Path(root).resolve()
        self.nodes = {}
        self.documents = {}
        self.historical_sources = []
        self.database_observations = []

    def add(self, path, expected, *, parent):
        require(isinstance(expected, str) and re.fullmatch(r"[0-9a-f]{64}", expected),
                "INVALID_ARTIFACT_HASH:" + str(path))
        target = Path(path)
        if not target.is_absolute():
            target = self.root / target
        target = target.resolve()
        require(target.suffix.lower() not in (".sqlite", ".sqlite3", ".db"),
                "DATABASE_IS_NOT_A_DOSSIER_ARTIFACT:" + str(path))
        key = str(target)
        if key in self.nodes:
            require(self.nodes[key]["sha256"] == expected, "CONFLICTING_ARTIFACT_HASH:" + key)
            self.nodes[key]["parents"] = sorted(set(self.nodes[key]["parents"] + [parent]))
            return
        require(len(self.nodes) < 4096, "EVIDENCE_GRAPH_TOO_LARGE")
        try:
            actual = _locate(target).read_bytes()
        except OSError as exc:
            raise DossierError("MISSING_ARTIFACT:" + key) from exc
        require(sha256(actual) == expected, "ARTIFACT_HASH_MISMATCH:" + key)
        self.nodes[key] = {"path": key, "sha256": expected, "parents": [parent]}
        if target.suffix.lower() == ".json":
            document = read_json(target)
            self.documents[key] = document
            for child, child_hash in _references(document):
                child_path = Path(child)
                if not child_path.is_absolute():
                    child_path = self.root / child_path
                original_path = child_path
                if child_path.suffix == ".py" and child_path.is_relative_to(ACCEPTED_DEVELOPMENT_CHECKOUT):
                    relative = child_path.relative_to(ACCEPTED_DEVELOPMENT_CHECKOUT).as_posix()
                    require(relative in _base_files(self.root), "UNKNOWN_HISTORICAL_SOURCE_REFERENCE")
                    child_path = self.root / relative
                # Source file links are original qualification provenance. Their
                # bytes may have been superseded by an explicitly accepted step.
                if child_path.suffix == ".py" and child_path.is_relative_to(self.root):
                    self.historical_sources.append({"parent": key, "path": str(child_path),
                                                    "original_reference_path": str(original_path),
                                                    "sha256": child_hash})
                elif child_path.suffix.lower() in (".sqlite", ".sqlite3", ".db"):
                    self.database_observations.append({"parent": key, "path": str(child_path),
                                                       "sha256": child_hash})
                else:
                    self.add(child_path, child_hash, parent=key)


def _accepted_indexes(root, graph):
    indexes = []
    for path in INDEX_PATHS:
        expected = _base_files(root)[path]
        actual = (root / path).read_bytes()
        require(_lf(actual) == expected, "ACCEPTED_INDEX_CHANGED:" + path)
        graph.add(root / path, sha256(actual), parent="accepted_revision:" + BASE_REVISION)
        indexes.append(read_json(root / path))
    step10, step11a, step11b = indexes
    require(step10["status"] == "EXECUTABLE_INTERNAL_STEP10_PASS", "STEP10_NOT_ACCEPTED")
    coverage = {f"S{n:02}" for n in range(1, 13)} | {f"E{n:02}" for n in range(1, 6)}
    require(set(step10["contract_coverage"]) == coverage, "MISSING_TRANSITION_EVIDENCE")
    for row in step10["qualifications"]:
        require(row["exit_code"] == 0 and row["review"] == "LOCAL_PASS", "FAILED_QUALIFICATION")
    require(step11a["remaining_bounded_measurement_delta"] == [], "M52_M53_ENGINEERING_MISSING")
    require(step11b["final_project_acceptance"]["M46"] == "VERIFIED", "M46_NOT_ACCEPTED")
    require(step11b["final_project_acceptance"]["remaining_owned_not_built"] == list(STEP11B_REMAINING_OWNED_ROWS),
            "UNEXPECTED_ENGINEERING_REMAINDER")
    return indexes


def _deployment(a, graph):
    target = a["target"]
    config = graph.documents[str(Path(target["public_configuration_path"]).resolve())]
    profile_ref = next(r for r in a["artifacts"] if r["path"].endswith("m52-profile-02\\profile.json"))
    extension_ref = next(r for r in a["artifacts"] if r["path"].endswith("profile-extension.json"))
    profile = graph.documents[str(Path(profile_ref["path"]).resolve())]
    extension = graph.documents[str(Path(extension_ref["path"]).resolve())]
    require(profile["target"] == target, "PROFILE_TARGET_CONFLICT")
    require(profile["content_digest"] == a["parent_profile_content_digest"]
            == extension["parent_profile_content_digest"], "PROFILE_DIGEST_CONFLICT")
    require(extension["content_digest"] == a["extension_content_digest"], "EXTENSION_DIGEST_CONFLICT")
    require(extension["limits"] == a["reviewed_guard_limits"], "PROFILE_LIMIT_CONFLICT")
    require(profile["host"]["identity"] == a["host_identity"]
            and profile["host"]["identity_digest"] == a["host_identity_digest"]
            == extension["host_identity_digest"], "HOST_BINDING_CONFLICT")
    require(profile["canonical_startup_identity"] == a["canonical_startup_identity"]
            == extension["canonical_startup_identity"], "STARTUP_BINDING_CONFLICT")
    require(config["paths"] == target["paths"] and config["wallet_public_key"] == target["domain"]["wallet"]
            and config["genesis_hash"] == target["domain"]["genesis_hash"]
            and config["mode"] == target["domain"]["mode"] == "LIVE", "PUBLIC_CONFIG_CONFLICT")
    monitor_ref = next(r for r in a["artifacts"] if r["path"].endswith("canonical-monitor-configuration-candidate.json"))
    return {"accepted_monitor":monitor_ref, "accepted_target": target, "host_identity": a["host_identity"],
            "host_identity_digest": a["host_identity_digest"],
            "profile": profile_ref, "profile_extension": extension_ref,
            "canonical_startup_identity": a["canonical_startup_identity"],
            "reviewed_guard_limits": a["reviewed_guard_limits"],
            "qualification_limits": a["qualification_limits"],
            "source_evolution": "Historical profile runtime digest is an accepted scoped identity, not the current source-content digest.",
            "activation": False, "capital_authority": False}


def _source_provenance(graph):
    result = []
    resolved = {}
    for row in graph.historical_sources:
        path = Path(row["path"]).relative_to(graph.root).as_posix()
        key = (path, row["sha256"])
        if key not in resolved:
            candidates = [BASE_REVISION]
            candidates += _git(graph.root, "log", "--format=%H", BASE_REVISION, "--", path).decode().splitlines()
            witness = None
            for revision in dict.fromkeys(candidates):
                blob = (_base_files(graph.root).get(path) if revision == BASE_REVISION
                        else _git(graph.root, "show", revision + ":" + path))
                if blob is None:
                    continue
                for encoding, raw in (("GIT_LF", blob), ("CHECKOUT_CRLF", blob.replace(b"\n", b"\r\n"))):
                    if sha256(raw) == key[1]:
                        witness = {"git_revision": revision, "byte_encoding": encoding}
                        break
                if witness:
                    break
            # Historical mixed-newline tested bytes may never have been a Git
            # blob. The immutable accepted parent authenticates that assertion;
            # no such assertion is used as the current-source comparison.
            resolved[key] = witness or {"disposition": "HISTORICAL_ASSERTION_IN_ACCEPTED_PARENT_ONLY"}
        result.append(dict(row, provenance=resolved[key]))
    return sorted(result, key=lambda row: (row["parent"], row["path"]))


def _qualified_dry(profile_reference, qualification_reference, source, deployment):
    if profile_reference is None and qualification_reference is None:
        return None
    from .runtime_dry_profile_v0_1 import read_reference, load_profile
    require(profile_reference is not None and qualification_reference is not None,
        "DRY_PROFILE_AND_QUALIFICATION_REQUIRED")
    supported_profile = load_profile(read_reference(profile_reference))
    profile = supported_profile.record
    qualification = read_reference(qualification_reference)
    public = profile["inputs"]["qualification_substitutions"]["scope"] == "PUBLIC_ENVIRONMENT_REVIEWED"
    if public:
        from .runtime_dry_public_qualification_v0_1 import validate_evidence
        validate_evidence(qualification, supported_profile)
    else:
        require(qualification["schema"] == "MEME_LIVE_DRY_PROFILE_QUALIFICATION_V1",
                "SYNTHETIC_DRY_QUALIFICATION_SCHEMA_REQUIRED")
    require(qualification["profile_content_digest"] == profile["content_digest"]
        and canonical_bytes(qualification["startup_identity"]) == canonical_bytes(profile["startup_identity"])
        and qualification["source_content_digest"] == source["content_digest"], "STALE_DRY_PROFILE_QUALIFICATION")
    required = {"exact_supported_startup", "qualified_actual_exact_dry_terminal",
        "same_profile_cold_original_non_submitted_recovery", "same_profile_second_original_admission",
        "amended_guards_fail_closed_above_bound", "monitor-alias", "wrong-source-start", "unknown-baseline"}
    if not public:
        require(required <= set(qualification["checks"]) and all(value is True for value in qualification["checks"].values())
            and qualification["simulation_count"] >= 2 and qualification["cold_interruption_recovery_count"] >= 1,
            "INCOMPLETE_DRY_PROFILE_QUALIFICATION")
    inputs = profile["inputs"]
    require(inputs["accepted_profile"] == deployment["profile"] and inputs["accepted_extension"] == deployment["profile_extension"]
        and inputs["accepted_monitor"] == deployment["accepted_monitor"],
        "DRY_PROFILE_ACCEPTED_DEPLOYMENT_CONFLICT")
    from .source_health_v0_1 import SourceBinding, CursorWitness
    binding_record = inputs["source_start"]["binding"]
    binding = SourceBinding(**dict(binding_record, anchors=tuple(CursorWitness(**r) for r in binding_record["anchors"])))
    require(canonical_bytes(qualification["qualification_substitutions"]) == canonical_bytes(inputs["qualification_substitutions"])
        and canonical_bytes(qualification["domain"]) == canonical_bytes(profile["domain"])
        and qualification["same_root"] == profile["runtime_type"]
        and qualification["source_identity"] == profile["source_identity"]
        and qualification["original_source_mapping"]["market_source_identity"] == profile["source_identity"]
        and qualification["original_source_mapping"]["source_binding_identity"] == binding.source_identity,
        "DRY_QUALIFICATION_IDENTITY_OR_SUBSTITUTION_CONFLICT")
    require(qualification["guard_amendment"] == inputs["guard_amendment"], "DRY_QUALIFICATION_GUARD_AMENDMENT_CONFLICT")
    from .runtime_dry_profile_v0_1 import reviewed_wallet_target, current_pump_rpc_binding
    accepted = read_reference(inputs["accepted_profile"])
    current_target = reviewed_wallet_target(accepted,
        inputs["accepted_profile"], inputs.get("deployment_rebind"))
    current_target, _ = current_pump_rpc_binding(accepted, inputs["accepted_profile"],
        current_target, inputs.get("public_rpc_rebind"))
    for key in ("wallet", "genesis_hash", "expected_profile_fingerprint"):
        require(profile["domain"][key] == current_target["domain"][key], "DRY_PROFILE_PUBLIC_TARGET_CONFLICT")
    if inputs.get("deployment_rebind") is not None:
        require(public, "DRY_WALLET_PUBLIC_REBIND_SCOPE_REQUIRED")
        deployment["current_target"] = current_target
        deployment["wallet_rebind"] = inputs["deployment_rebind"]
    if inputs.get("public_rpc_rebind") is not None:
        require(public, "DRY_CURRENT_PUMP_PUBLIC_RPC_SCOPE_REQUIRED")
        deployment["current_target"] = current_target
        deployment["public_rpc_rebind"] = inputs["public_rpc_rebind"]
    require(qualification["signer_send_broadcast"] is False
        and (qualification["canonical_live_store_access"] if public else qualification["canonical_store_access"]) is False
        and qualification["T010_executed"] is False, "DRY_QUALIFICATION_SAFETY_CONFLICT")
    return {"profile":profile_reference, "qualification":qualification_reference,
        "profile_content_digest":profile["content_digest"], "startup_identity":profile["startup_identity"],
        "scope":qualification["scope"], "qualification_substitutions":qualification["qualification_substitutions"],
        "engineering_status":"IMPLEMENTED_PENDING_PROJECT_REVIEW", "runtime_executed_by_freeze":False,
        "current_public_environment_proven":False, "project_acceptance_claimed":False}


def build_dossier(repo_root, *, dry_profile=None, dry_qualification=None):
    root = Path(repo_root).resolve()
    source = source_freeze(root)
    graph = EvidenceGraph(root)
    step10, a, b = _accepted_indexes(root, graph)
    matrix = (root / MATRIX_PATH).read_bytes()
    lifecycle = lifecycle_disposition(matrix.decode("utf-8-sig"))
    contracts = {}
    # All existing LIVE contracts, foundations, runbooks and policy docs are
    # covered. New generated Step11-C output indexes are deliberately excluded.
    for path, data in _base_files(root).items():
        if not path.startswith("docs/live/") or not path.endswith(".md"):
            continue
        # Status surfaces can gain review bookkeeping, but immutable governing
        # contracts are required to retain their accepted bytes.
        if any(word in path for word in ("CLOSURE_ARCHITECTURE", "CLOSURE_WORKFLOW", "COMPLETION_PLAN", "MODEL_ROUTING")):
            require(_lf((root / path).read_bytes()) == data, "CONTRACT_CHANGED:" + path)
        contracts[path] = {"sha256": sha256(data), "revision": BASE_REVISION}
    for path in ("docs/live/MEME_LIVE_STEP11C_GATE_CONTRACT_V1.json",
                 "docs/live/MEME_LIVE_STEP11C_GATE_CONTRACT_V1.md"):
        if (root / path).exists():
            contracts[path] = {"sha256": sha256((root / path).read_bytes()), "revision": "STEP11C_OVERLAY"}
    deployment = _deployment(a, graph)
    supported_dry = _qualified_dry(dry_profile, dry_qualification, source, deployment)
    assembly = {role: {"path": path, "sha256": source["files"][path]["sha256"]}
                for role, path in ASSEMBLY.items()}
    return {"schema": SCHEMA, "implementation_status": "IMPLEMENTED_PENDING_PROJECT_REVIEW",
            "source": source, "accepted_indexes": [{"path": p, "sha256": sha256((root / p).read_bytes()),
                                                     "revision": BASE_REVISION} for p in INDEX_PATHS],
            "evidence": sorted(graph.nodes.values(), key=lambda row: row["path"]),
            "historical_source_references": _source_provenance(graph),
            "historical_database_observations": sorted(graph.database_observations, key=lambda row: (row["parent"], row["path"])),
            "database_observation_boundary": "Hash observations are retained under accepted immutable parent evidence. No database was read, rehashed, initialized or considered current proof.",
            "historical_source_rule": "Original source hash assertions retained under immutable accepted indexes; current source is independently bound to accepted_revision plus Step11-C overlay.",
            "transition_coverage": step10["contract_coverage"], "contracts": contracts,
            "lifecycle": lifecycle, "deployment": deployment, "runtime_components": assembly,
            "supported_dry": supported_dry,
            "gate_tooling": {p: source["files"][p]["sha256"] for p in sorted(NEW_CODE) if p in source["files"]},
            "review_boundary": "M56/M57 current VERIFIED or reopened OWNED_NOT_BUILT states are matrix input, never acceptance by this tool. Reopened rows deny launch readiness. Positive T010 structural preflight requires VERIFIED rows and a separate explicit ChatGPT project-review record bound to this exact dossier and source.",
            "capabilities": {"execute_t010": False, "sign": False, "send": False, "broadcast": False,
                             "database_mutation": False, "capital_authority": False}}


def verify_dossier(repo_root, dossier):
    require(isinstance(dossier, dict) and dossier.get("schema") == SCHEMA, "UNSUPPORTED_DOSSIER")
    supported = dossier.get("supported_dry")
    expected = build_dossier(repo_root, dry_profile=None if supported is None else supported["profile"],
        dry_qualification=None if supported is None else supported["qualification"])
    require(canonical_bytes(dossier) == canonical_bytes(expected), "STALE_OR_CONFLICTING_DOSSIER")
    return expected
