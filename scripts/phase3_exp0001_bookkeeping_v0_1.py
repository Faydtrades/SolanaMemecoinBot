from __future__ import annotations

import hashlib
import json
import os
import shutil
from pathlib import Path
from typing import Any

EXPECTED = {
    "freeze_id": "DEV-FREEZE-0001",
    "locked_search_decision_id": "PHASE-2-PARAM-SEARCH-001",
    "registry_schema_version": "EXPR-0.1",
    "registry_before_next_id": "EXP-0001",
    "registry_before_hash": "04913d3dd1c5ca4ca4c28a51109c734f1c266197e64380ea00344763b7afaabc",
    "manifest_status": "COMPLETE_PHASE3_1_FOUNDATION_CORRECTED_V2",
    "manifest_sha256": "7e5036b9576f22b8304ba8f1fd8f6b8d0d1cec1b83d6925433fd82e665ffdc0c",
    "report_sha256": "222f9923404393552bbbd53c150694bcd67a7a31f85f84aeedeb00af39367a12",
    "candidate_csv_sha256": "c944741051b36f42c7dff4afc700dcdc020c0f4a63dacecbcbb47dd2ed7364d6",
    "snapshot_sqlite_sha256": "44e3457b81a16d4e5e2670e24d758756a147867e1d81d627622157ad4bd5813f",
    "frozen_rows_sha256": "c10318d3c1db85d917d7a8d9dbd56f694aab97195420a31c5e618713d08c2ea6",
    "locked_search_sha256": "65cd4aa6ebc84929504b2fa4224bbc64dc149737b3f3e344d9e172d72b2e586f",
}

def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)

def stable_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()

def fail(message: str) -> None:
    print(f"[FAIL] {message}")
    print("RESULT: FAIL")
    raise SystemExit(1)

def require(condition: bool, message: str) -> None:
    if not condition:
        fail(message)

def load_json(path: Path) -> dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as f:
            obj = json.load(f)
    except Exception as exc:
        fail(f"Could not read JSON {path}: {exc}")
    require(isinstance(obj, dict), f"{path} must contain a JSON object")
    return obj

def write_json_atomic(path: Path, obj: dict[str, Any]) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8", newline="\n") as f:
        json.dump(obj, f, indent=2, sort_keys=True, ensure_ascii=True)
        f.write("\n")
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)

def main() -> None:
    root = Path(__file__).resolve().parents[1]
    freeze_dir = root / "data" / "research" / "phase2_research_freeze_v0_1"
    phase3_dir = root / "data" / "research" / "phase3"
    exp_dir = phase3_dir / "EXP-0001"

    registry_path = freeze_dir / "phase2_experiment_registry_v0_1.json"
    snapshot_path = freeze_dir / "phase2_development_dataset_v0_1.sqlite3"
    dataset_manifest_path = freeze_dir / "phase2_development_dataset_manifest_v0_1.json"
    locked_search_path = freeze_dir / "phase2_locked_param_search_001.json"
    manifest_path = exp_dir / "EXP-0001_manifest_v0_1_2.json"
    report_path = exp_dir / "phase3_1_outcome_replay_report_v0_1_2.json"
    candidate_csv_path = exp_dir / "phase3_1_candidate_outcomes_v0_1_2.csv"

    print("PHASE 3.1 EXP-0001 BOOKKEEPING v0.1")
    print("=" * 58)

    for path in (
        registry_path, snapshot_path, dataset_manifest_path, locked_search_path,
        manifest_path, report_path, candidate_csv_path,
    ):
        require(path.exists(), f"Missing required file: {path}")

    # Verify immutable research inputs.
    snapshot_hash_before = sha256_file(snapshot_path)
    require(snapshot_hash_before == EXPECTED["snapshot_sqlite_sha256"],
            "DEV-FREEZE-0001 SQLite hash mismatch")

    locked_search = load_json(locked_search_path)
    require(stable_sha256(locked_search) == EXPECTED["locked_search_sha256"],
            "Locked search-space deterministic hash mismatch")

    dataset_manifest = load_json(dataset_manifest_path)
    dataset = dataset_manifest.get("dataset", {})
    require(isinstance(dataset, dict)
            and dataset.get("frozen_rows_sha256") == EXPECTED["frozen_rows_sha256"],
            "Frozen-row digest mismatch in dataset manifest")

    # Verify exact corrected EXP-0001 artifacts reviewed for Phase 3.1 completion.
    require(sha256_file(manifest_path) == EXPECTED["manifest_sha256"],
            "Corrected EXP-0001 manifest file hash mismatch")
    require(sha256_file(report_path) == EXPECTED["report_sha256"],
            "Corrected Phase 3.1 report file hash mismatch")
    require(sha256_file(candidate_csv_path) == EXPECTED["candidate_csv_sha256"],
            "Corrected candidate outcome CSV file hash mismatch")

    manifest = load_json(manifest_path)
    report = load_json(report_path)
    require(manifest.get("experiment_id") == "EXP-0001", "Manifest experiment_id mismatch")
    require(manifest.get("status") == EXPECTED["manifest_status"],
            "Manifest is not corrected completed Phase 3.1 version")
    require(manifest.get("dataset", {}).get("freeze_id") == EXPECTED["freeze_id"],
            "Manifest freeze binding mismatch")
    require(manifest.get("dataset", {}).get("untouched_oos") is False,
            "DEV-FREEZE-0001 must remain development/discovery, not untouched OOS")
    require(report.get("experiment_id") == "EXP-0001", "Report experiment_id mismatch")
    require(report.get("determinism", {}).get("pass") is True,
            "Outcome replay determinism did not PASS")
    require(report.get("candidate_extraction", {}).get("run_count") == 3901,
            "Expected exactly 3,901 strategy runs")
    require(report.get("candidate_extraction", {}).get("candidate_count") == 230,
            "Expected exactly 230 CandidateSignals")
    require(report.get("outcome_observability", {}).get("complete_5m_extrema_count") == 11,
            "Expected exactly 11 complete 5m MFE/MAE candidates")
    claims = report.get("performance_claims", {})
    require(claims.get("profitability_claim_allowed") is False,
            "Phase 3.1 must not allow a profitability claim")
    require(claims.get("exit_optimization_performed") is False,
            "Phase 3.1 must not contain exit optimization")

    entry = {
        "experiment_id": "EXP-0001",
        "phase": "PHASE_3_1",
        "role": "OUTCOME_REPLAY_FOUNDATION_NO_OPTIMIZATION",
        "status": "COMPLETE",
        "dataset_freeze_id": "DEV-FREEZE-0001",
        "dataset_role": "DEVELOPMENT_DISCOVERY",
        "untouched_oos": False,
        "strategy": "FirstPullback v1.1",
        "manifest_path": "data/research/phase3/EXP-0001/EXP-0001_manifest_v0_1_2.json",
        "manifest_sha256": EXPECTED["manifest_sha256"],
        "report_path": "data/research/phase3/EXP-0001/phase3_1_outcome_replay_report_v0_1_2.json",
        "report_sha256": EXPECTED["report_sha256"],
        "candidate_outcomes_path": "data/research/phase3/EXP-0001/phase3_1_candidate_outcomes_v0_1_2.csv",
        "candidate_outcomes_sha256": EXPECTED["candidate_csv_sha256"],
        "summary": {
            "strategy_runs": 3901,
            "candidate_signals": 230,
            "complete_5m_mfe_mae_candidates": 11,
            "outcome_replay_deterministic": True,
            "exit_optimization_performed": False,
            "realistic_net_pnl_available": False,
            "profitability_claim_allowed": False,
        },
    }

    registry = load_json(registry_path)
    require(registry.get("registry_schema_version") == EXPECTED["registry_schema_version"],
            "Unexpected experiment registry schema")
    require(registry.get("freeze_id") == EXPECTED["freeze_id"], "Registry freeze_id mismatch")
    require(registry.get("locked_search_decision_id") == EXPECTED["locked_search_decision_id"],
            "Registry locked-search binding mismatch")
    experiments = registry.get("experiments")
    require(isinstance(experiments, list), "Registry experiments must be a list")

    existing = [x for x in experiments
                if isinstance(x, dict) and x.get("experiment_id") == "EXP-0001"]

    if existing:
        require(len(existing) == 1, "EXP-0001 appears more than once in registry")
        require(existing[0] == entry, "Existing EXP-0001 registry entry differs from expected")
        require(registry.get("next_experiment_id") == "EXP-0002",
                "Registry contains EXP-0001 but next ID is not EXP-0002")
        core = dict(registry)
        stored_hash = core.pop("deterministic_registry_sha256", None)
        require(stored_hash == stable_sha256(core),
                "Existing updated registry deterministic hash is invalid")
        action = "ALREADY_REGISTERED"
    else:
        require(experiments == [], "Unexpected pre-existing experiments in pristine registry")
        require(registry.get("next_experiment_id") == EXPECTED["registry_before_next_id"],
                "Expected next_experiment_id EXP-0001 before first registration")
        require(registry.get("deterministic_registry_sha256") == EXPECTED["registry_before_hash"],
                "Pristine registry hash does not match Phase 2.14 checkpoint")
        pristine_core = dict(registry)
        pristine_stored = pristine_core.pop("deterministic_registry_sha256")
        require(pristine_stored == stable_sha256(pristine_core),
                "Pristine registry deterministic hash validation failed")

        history_dir = phase3_dir / "registry_history"
        history_dir.mkdir(parents=True, exist_ok=True)
        backup_path = history_dir / "phase2_experiment_registry_v0_1_pre_EXP-0001.json"
        if not backup_path.exists():
            shutil.copy2(registry_path, backup_path)
        else:
            require(sha256_file(backup_path) == sha256_file(registry_path),
                    "Existing pre-EXP-0001 registry backup differs from pristine registry")

        updated_core = {
            "registry_schema_version": registry["registry_schema_version"],
            "freeze_id": registry["freeze_id"],
            "dataset_manifest_sha256": registry["dataset_manifest_sha256"],
            "locked_search_decision_id": registry["locked_search_decision_id"],
            "next_experiment_id": "EXP-0002",
            "experiments": [entry],
        }
        updated = dict(updated_core)
        updated["deterministic_registry_sha256"] = stable_sha256(updated_core)
        write_json_atomic(registry_path, updated)
        registry = load_json(registry_path)
        action = "REGISTERED"

    # Final validation.
    require(registry.get("next_experiment_id") == "EXP-0002",
            "Final next experiment ID is not EXP-0002")
    final_experiments = registry.get("experiments", [])
    require(len(final_experiments) == 1 and final_experiments[0] == entry,
            "Final registry does not contain exactly expected EXP-0001 entry")
    final_core = dict(registry)
    final_stored_hash = final_core.pop("deterministic_registry_sha256", None)
    require(final_stored_hash == stable_sha256(final_core),
            "Final deterministic registry hash validation failed")

    snapshot_hash_after = sha256_file(snapshot_path)
    require(snapshot_hash_after == snapshot_hash_before == EXPECTED["snapshot_sqlite_sha256"],
            "DEV-FREEZE-0001 SQLite changed during bookkeeping")

    print(f"Action                       : {action}")
    print("Registered experiment        : EXP-0001")
    print("EXP-0001 status              : COMPLETE")
    print("Registry next experiment ID  : EXP-0002")
    print(f"Registry deterministic SHA256: {final_stored_hash}")
    print("DEV-FREEZE-0001 SQLite       : UNCHANGED")
    print("Production DB touched        : NO")
    print("Profitability claim          : NO")
    print("=" * 58)
    print("RESULT: PASS")

if __name__ == "__main__":
    main()
