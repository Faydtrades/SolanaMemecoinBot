from __future__ import annotations

import csv
import hashlib
import json
import os
import shutil
from pathlib import Path
from typing import Any

EXPECTED_REGISTRY_BEFORE_SHA256 = "e3e9ea32c158930bd99f9590bee133d522e5caa4370f25d383124acef12ac59a"
EXPECTED_SNAPSHOT_SHA256 = "44e3457b81a16d4e5e2670e24d758756a147867e1d81d627622157ad4bd5813f"
EXPECTED_LOCKED_SEARCH_SHA256 = "65cd4aa6ebc84929504b2fa4224bbc64dc149737b3f3e344d9e172d72b2e586f"

EXPECTED_ARTIFACTS = {
    "EXP-0002_A_impulse_summary_v0_1_1.csv": "96a89e9850884ba9e85b98ce0c2718852ed2690761239935929eea066c544a4e",
    "EXP-0002_A_impulse_frontier_v0_1_1.csv": "be2b4a5665e03cab7b564c87893bfac080508999f33c2bf402378f7333da3857",
    "EXP-0002_A_impulse_candidate_outcomes_v0_1_1.csv": "1079450705fab4afa8a5200d6275677bd9786647eb9f733eda7f8f50aaceae78",
    "EXP-0002_A_impulse_report_v0_1_1.json": "8690b89a0d19ffba8f3fc2ab9bd7432efd95e3517212cfa755a5b211bc1633c9",
    "EXP-0002_manifest_v0_1_1.json": "abfb1d8f769e5e3a2c99f1a32da5e4e31d0e6e3188db8ee3329d7cdb2ac93f5b",
    "EXP-0002_A_neighborhood_diagnostics_v0_1_1.csv": "14dbe2e0b89d0a448beafe00afc1e30ae5a0271bb8f2c2ecdfd44a194b171248",
    "EXP-0002_A_postrun_audit_v0_1_1.json": "d88a886237ed0812809a98ab77dc075cfb4bb04daaaaf42549030ee9014e9acc",
}

SHORTLIST = [
    {
        "role": "CONTROL",
        "parameter_set_id": "FP1-EXP0002-A-001-R500-T2-B1",
        "min_return_bps": 500,
        "min_trades_since_t0": 2,
        "min_unique_buyers_since_t0": 1,
        "candidate_count": 230,
        "h15_observable_n": 127,
        "h15_p50_bps": 181,
        "h30_observable_n": 67,
        "h30_p50_bps": 44,
    },
    {
        "role": "ROBUST_1",
        "parameter_set_id": "FP1-EXP0002-A-040-R1500-T5-B9",
        "min_return_bps": 1500,
        "min_trades_since_t0": 5,
        "min_unique_buyers_since_t0": 9,
        "candidate_count": 92,
        "h15_observable_n": 60,
        "h15_p50_bps": 241,
        "h30_observable_n": 29,
        "h30_p50_bps": 331,
    },
    {
        "role": "ROBUST_2",
        "parameter_set_id": "FP1-EXP0002-A-059-R2000-T3-B5",
        "min_return_bps": 2000,
        "min_trades_since_t0": 3,
        "min_unique_buyers_since_t0": 5,
        "candidate_count": 114,
        "h15_observable_n": 68,
        "h15_p50_bps": 201,
        "h30_observable_n": 33,
        "h30_p50_bps": 331,
    },
    {
        "role": "ROBUST_3",
        "parameter_set_id": "FP1-EXP0002-A-107-R5000-T3-B2",
        "min_return_bps": 5000,
        "min_trades_since_t0": 3,
        "min_unique_buyers_since_t0": 2,
        "candidate_count": 96,
        "h15_observable_n": 50,
        "h15_p50_bps": 238,
        "h30_observable_n": 25,
        "h30_p50_bps": 331,
    },
    {
        "role": "ROBUST_4",
        "parameter_set_id": "FP1-EXP0002-A-115-R5000-T5-B9",
        "min_return_bps": 5000,
        "min_trades_since_t0": 5,
        "min_unique_buyers_since_t0": 9,
        "candidate_count": 58,
        "h15_observable_n": 36,
        "h15_p50_bps": 238,
        "h30_observable_n": 19,
        "h30_p50_bps": 390,
    },
]

def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)

def stable_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()

def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        obj = json.load(f)
    if not isinstance(obj, dict):
        raise RuntimeError(f"{path} is not a JSON object")
    return obj

def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8", newline="\n") as f:
        json.dump(payload, f, indent=2, sort_keys=True, ensure_ascii=True)
        f.write("\n")
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)

def require(cond: bool, msg: str) -> None:
    if not cond:
        print(f"[FAIL] {msg}")
        print("RESULT: FAIL")
        raise SystemExit(1)

def main() -> None:
    root = Path(__file__).resolve().parents[1]
    freeze_dir = root / "data/research/phase2_research_freeze_v0_1"
    exp2_dir = root / "data/research/phase3/EXP-0002"
    phase3_dir = root / "data/research/phase3"

    registry_path = freeze_dir / "phase2_experiment_registry_v0_1.json"
    freeze_db = freeze_dir / "phase2_development_dataset_v0_1.sqlite3"
    locked_search_path = freeze_dir / "phase2_locked_param_search_001.json"

    print("EXP-0002 BLOCK A BOOKKEEPING v0.1")
    print("=" * 62)

    require(registry_path.exists(), "registry missing")
    require(freeze_db.exists(), "DEV-FREEZE-0001 SQLite missing")
    require(locked_search_path.exists(), "locked search file missing")
    require(file_sha256(freeze_db) == EXPECTED_SNAPSHOT_SHA256, "freeze SQLite hash mismatch")
    require(stable_sha256(load_json(locked_search_path)) == EXPECTED_LOCKED_SEARCH_SHA256,
            "locked search hash mismatch")

    for name, expected_hash in EXPECTED_ARTIFACTS.items():
        path = exp2_dir / name
        require(path.exists(), f"missing reviewed artifact {name}")
        require(file_sha256(path) == expected_hash, f"artifact hash mismatch: {name}")

    audit = load_json(exp2_dir / "EXP-0002_A_postrun_audit_v0_1_1.json")
    require(audit.get("result") == "PASS", "EXP-0002 post-run audit did not PASS")
    checks = audit.get("checks", {})
    for key in (
        "exact_175_grid", "terminal_reject_accounting", "terminal_lifecycle_complete",
        "candidate_rows_match", "B_C_D_unchanged", "frontier_consistent",
        "determinism", "no_winner_selected", "no_exit_optimization",
        "no_profitability_claim",
    ):
        require(checks.get(key) is True, f"audit check failed: {key}")

    # Verify shortlist rows directly against the reviewed summary.
    summary_path = exp2_dir / "EXP-0002_A_impulse_summary_v0_1_1.csv"
    with summary_path.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    by_id = {r["parameter_set_id"]: r for r in rows}
    for item in SHORTLIST:
        require(item["parameter_set_id"] in by_id, f"shortlist row missing: {item['parameter_set_id']}")
        r = by_id[item["parameter_set_id"]]
        for field in (
            "min_return_bps", "min_trades_since_t0", "min_unique_buyers_since_t0",
            "candidate_count", "h15_observable_n", "h15_p50_bps",
            "h30_observable_n", "h30_p50_bps",
        ):
            require(int(r[field]) == int(item[field]),
                    f"{item['parameter_set_id']} {field} mismatch")
        require(str(r["diagnostic_pareto_frontier"]).lower() == "true",
                f"{item['parameter_set_id']} is not on reviewed diagnostic frontier")

    selection = {
        "schema_version": "P3-EXP0002-A-SELECTION-0.1",
        "experiment_id": "EXP-0002",
        "status": "LOCKED_FOR_EXP0003_BLOCK_B",
        "source_run": "phase3_exp0002_impulse_harness_v0_1_1.py",
        "source_postrun_audit": "EXP-0002_A_postrun_audit_v0_1_1.json",
        "dataset": {
            "freeze_id": "DEV-FREEZE-0001",
            "role": "DEVELOPMENT_DISCOVERY",
            "untouched_oos": False,
        },
        "preanalysis_methodology": {
            "locked_before_results": True,
            "primary_outcome_horizons": ["15s", "30s"],
            "five_minute_mfe_mae_role": "SUPPORTING_DESCRIPTIVE_ONLY",
            "selection_rule": (
                "retain a permissive control plus robust regions with sample support "
                "and neighborhood stability; do not select by highest median alone"
            ),
            "single_winner_selected": False,
        },
        "carry_forward": SHORTLIST,
        "next_block": {
            "experiment_id": "EXP-0003",
            "block": "B_PULLBACK",
            "a_configuration_count": 5,
            "valid_pullback_pair_count": 21,
            "planned_combination_count": 105,
            "C_D_fixed": True,
        },
        "notes": (
            "Research shortlist only; not final trading parameters and not a "
            "profitability claim. Control is retained explicitly for comparison."
        ),
    }

    selection_path = exp2_dir / "EXP-0002_A_selection_v0_1.json"
    if selection_path.exists():
        existing_selection = load_json(selection_path)
        require(existing_selection == selection, "existing selection file differs from locked selection")
    else:
        write_json_atomic(selection_path, selection)
    selection_sha = file_sha256(selection_path)

    registry = load_json(registry_path)
    core = dict(registry)
    stored_hash = core.pop("deterministic_registry_sha256", None)
    require(stored_hash == stable_sha256(core), "registry deterministic hash invalid")
    experiments = registry.get("experiments")
    require(isinstance(experiments, list), "registry experiments invalid")

    existing_exp2 = [x for x in experiments if isinstance(x, dict) and x.get("experiment_id") == "EXP-0002"]

    entry = {
        "experiment_id": "EXP-0002",
        "phase": "PHASE_3_PARAMETER_RESEARCH",
        "role": "STAGED_BLOCK_A_IMPULSE",
        "status": "COMPLETE",
        "dataset_freeze_id": "DEV-FREEZE-0001",
        "dataset_role": "DEVELOPMENT_DISCOVERY",
        "untouched_oos": False,
        "strategy": "FirstPullback v1.1",
        "search_space": "PHASE-2-PARAM-SEARCH-001",
        "report_path": "data/research/phase3/EXP-0002/EXP-0002_A_impulse_report_v0_1_1.json",
        "report_sha256": EXPECTED_ARTIFACTS["EXP-0002_A_impulse_report_v0_1_1.json"],
        "manifest_path": "data/research/phase3/EXP-0002/EXP-0002_manifest_v0_1_1.json",
        "manifest_sha256": EXPECTED_ARTIFACTS["EXP-0002_manifest_v0_1_1.json"],
        "postrun_audit_path": "data/research/phase3/EXP-0002/EXP-0002_A_postrun_audit_v0_1_1.json",
        "postrun_audit_sha256": EXPECTED_ARTIFACTS["EXP-0002_A_postrun_audit_v0_1_1.json"],
        "selection_path": "data/research/phase3/EXP-0002/EXP-0002_A_selection_v0_1.json",
        "selection_sha256": selection_sha,
        "summary": {
            "A_combinations": 175,
            "candidate_outcome_rows": 17388,
            "diagnostic_pareto_frontier_rows": 28,
            "carry_forward_configurations": 5,
            "carry_forward_includes_control": True,
            "single_winner_selected": False,
            "exit_optimization_performed": False,
            "realistic_net_pnl_available": False,
            "profitability_claim_allowed": False,
        },
    }

    if existing_exp2:
        require(len(existing_exp2) == 1, "EXP-0002 appears more than once")
        require(existing_exp2[0] == entry, "existing EXP-0002 registry entry differs")
        require(registry.get("next_experiment_id") == "EXP-0003",
                "EXP-0002 registered but next ID is not EXP-0003")
        action = "ALREADY_REGISTERED"
    else:
        require(registry.get("next_experiment_id") == "EXP-0002",
                "registry next ID must be EXP-0002 before bookkeeping")
        require(stored_hash == EXPECTED_REGISTRY_BEFORE_SHA256,
                "registry does not match locked post-EXP-0001 state")
        require(len(experiments) == 1 and experiments[0].get("experiment_id") == "EXP-0001"
                and experiments[0].get("status") == "COMPLETE",
                "EXP-0001 canonical predecessor missing")

        history = phase3_dir / "registry_history"
        history.mkdir(parents=True, exist_ok=True)
        backup = history / "phase2_experiment_registry_v0_1_pre_EXP-0002.json"
        if not backup.exists():
            shutil.copy2(registry_path, backup)

        updated_core = {
            "registry_schema_version": registry["registry_schema_version"],
            "freeze_id": registry["freeze_id"],
            "dataset_manifest_sha256": registry["dataset_manifest_sha256"],
            "locked_search_decision_id": registry["locked_search_decision_id"],
            "next_experiment_id": "EXP-0003",
            "experiments": experiments + [entry],
        }
        updated = dict(updated_core)
        updated["deterministic_registry_sha256"] = stable_sha256(updated_core)
        write_json_atomic(registry_path, updated)
        registry = load_json(registry_path)
        action = "REGISTERED"

    final_core = dict(registry)
    final_hash = final_core.pop("deterministic_registry_sha256", None)
    require(final_hash == stable_sha256(final_core), "final registry hash invalid")
    require(registry.get("next_experiment_id") == "EXP-0003", "final next ID not EXP-0003")
    require(len(registry.get("experiments", [])) == 2, "expected EXP-0001 + EXP-0002 in registry")
    require(file_sha256(freeze_db) == EXPECTED_SNAPSHOT_SHA256, "freeze DB changed")

    print(f"Action                       : {action}")
    print("EXP-0002 status              : COMPLETE")
    print("Carry-forward A configs      : 5 (1 control + 4 robust)")
    print("Registry next experiment ID  : EXP-0003")
    print(f"Selection SHA256             : {selection_sha}")
    print(f"Registry deterministic SHA256: {final_hash}")
    print("DEV-FREEZE-0001 SQLite       : UNCHANGED")
    print("Production DB touched        : NO")
    print("Profitability claim          : NO")
    print("=" * 62)
    print("RESULT: PASS")

if __name__ == "__main__":
    main()
