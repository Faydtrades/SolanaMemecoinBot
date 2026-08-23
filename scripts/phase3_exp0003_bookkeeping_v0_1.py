from __future__ import annotations

import csv
import hashlib
import json
import os
import shutil
from pathlib import Path
from typing import Any

EXPECTED_SNAPSHOT_SHA256 = "44e3457b81a16d4e5e2670e24d758756a147867e1d81d627622157ad4bd5813f"
EXPECTED_ARTIFACTS = {
    "EXP-0003_B_pullback_summary_v0_1.csv": "65b2fe15f679460f1b7430c730c500d16f864df3f42f19c6abe1e17710b4f8a9",
    "EXP-0003_B_pullback_frontier_v0_1.csv": "8e710d08ea16339e086415e04e7592b1b82bc5fd4079f4941cc0d2e0d15791a8",
    "EXP-0003_B_pullback_candidate_outcomes_v0_1.csv": "5d416e7e1aa7b9028ca14a8b054c12937b05bb22149eb25f2fc4842cac6f2410",
    "EXP-0003_B_pullback_report_v0_1.json": "1260eb06a48c0241021a41f6324414284f3f677c540c53159a286cdc4aa3c915",
    "EXP-0003_manifest_v0_1.json": "abe146b65d5ef1af698e78db6df0c5a57fe9b18a0847632a2095c0632721df62",
    "EXP-0003_B_neighborhood_diagnostics_v0_1.csv": "b89a8243d3d2278b7bd24c34a76641ef704a4d867eb0862f199da547853106fe",
    "EXP-0003_B_crossA_diagnostics_v0_1.csv": "d22c0634f5a4d16236850a900a3e2ceb282e25cb09c844a991ab252aeba7c018",
    "EXP-0003_B_postrun_audit_v0_1.json": "814d9be98926e613ed3e27056ae858204b4c152275d7f34926b024f92132a530",
}

SHORTLIST = [
    {
        "role": "CONTROL",
        "parameter_set_id": "FP1-EXP0003-B-005-CONTROL-R500-T2-U1-PD1000-9000",
        "min_return_bps": 500,
        "min_trades_since_t0": 2,
        "min_unique_buyers_since_t0": 1,
        "min_depth_bps": 1000,
        "max_depth_bps": 9000,
        "candidate_count": 230,
        "h15_observable_n": 127,
        "h15_p50_bps": 181,
        "h30_observable_n": 67,
        "h30_p50_bps": 44,
    },
    {
        "role": "ROBUST_1",
        "parameter_set_id": "FP1-EXP0003-B-030-ROBUST_1-R1500-T5-U9-PD2500-6500",
        "min_return_bps": 1500,
        "min_trades_since_t0": 5,
        "min_unique_buyers_since_t0": 9,
        "min_depth_bps": 2500,
        "max_depth_bps": 6500,
        "candidate_count": 46,
        "h15_observable_n": 32,
        "h15_p50_bps": 247,
        "h30_observable_n": 18,
        "h30_p50_bps": 468,
    },
    {
        "role": "ROBUST_2",
        "parameter_set_id": "FP1-EXP0003-B-051-ROBUST_2-R2000-T3-U5-PD2500-6500",
        "min_return_bps": 2000,
        "min_trades_since_t0": 3,
        "min_unique_buyers_since_t0": 5,
        "min_depth_bps": 2500,
        "max_depth_bps": 6500,
        "candidate_count": 57,
        "h15_observable_n": 38,
        "h15_p50_bps": 247,
        "h30_observable_n": 25,
        "h30_p50_bps": 468,
    },
    {
        "role": "ROBUST_3",
        "parameter_set_id": "FP1-EXP0003-B-072-ROBUST_3-R5000-T3-U2-PD2500-6500",
        "min_return_bps": 5000,
        "min_trades_since_t0": 3,
        "min_unique_buyers_since_t0": 2,
        "min_depth_bps": 2500,
        "max_depth_bps": 6500,
        "candidate_count": 52,
        "h15_observable_n": 29,
        "h15_p50_bps": 154,
        "h30_observable_n": 19,
        "h30_p50_bps": 529,
    },
]

def canonical_json(v: Any) -> str:
    return json.dumps(v, sort_keys=True, separators=(",", ":"), ensure_ascii=True)

def stable_sha(v: Any) -> str:
    return hashlib.sha256(canonical_json(v).encode("utf-8")).hexdigest()

def file_sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def load_json(path: Path) -> dict[str, Any]:
    obj = json.loads(path.read_text(encoding="utf-8"))
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
    exp3_dir = root / "data/research/phase3/EXP-0003"
    phase3_dir = root / "data/research/phase3"

    registry_path = freeze_dir / "phase2_experiment_registry_v0_1.json"
    freeze_db = freeze_dir / "phase2_development_dataset_v0_1.sqlite3"

    print("EXP-0003 BLOCK B BOOKKEEPING v0.1")
    print("=" * 62)

    require(registry_path.exists(), "canonical registry missing")
    require(freeze_db.exists(), "DEV-FREEZE-0001 SQLite missing")
    require(file_sha(freeze_db) == EXPECTED_SNAPSHOT_SHA256, "freeze SQLite hash mismatch")

    for name, expected in EXPECTED_ARTIFACTS.items():
        p = exp3_dir / name
        require(p.exists(), f"missing reviewed artifact: {name}")
        require(file_sha(p) == expected, f"artifact hash mismatch: {name}")

    audit = load_json(exp3_dir / "EXP-0003_B_postrun_audit_v0_1.json")
    require(audit.get("result") == "PASS", "EXP-0003 audit did not PASS")
    checks = audit.get("checks", {})
    for key in (
        "exact_105_grid", "terminal_accounting", "candidate_rows_match",
        "frontier_consistent", "determinism", "no_winner_selected",
        "no_exit_optimization", "no_profitability_claim",
    ):
        require(checks.get(key) is True, f"audit check failed: {key}")

    # Verify carry-forward rows exactly against reviewed summary.
    with (exp3_dir / "EXP-0003_B_pullback_summary_v0_1.csv").open(
        "r", encoding="utf-8", newline=""
    ) as f:
        rows = list(csv.DictReader(f))
    by_id = {r["parameter_set_id"]: r for r in rows}
    for item in SHORTLIST:
        require(item["parameter_set_id"] in by_id, f"shortlist row missing: {item['parameter_set_id']}")
        row = by_id[item["parameter_set_id"]]
        for field in (
            "min_return_bps", "min_trades_since_t0", "min_unique_buyers_since_t0",
            "min_depth_bps", "max_depth_bps", "candidate_count",
            "h15_observable_n", "h15_p50_bps", "h30_observable_n", "h30_p50_bps",
        ):
            require(int(row[field]) == int(item[field]),
                    f"{item['parameter_set_id']} {field} mismatch")

    selection = {
        "schema_version": "P3-EXP0003-B-SELECTION-0.1",
        "experiment_id": "EXP-0003",
        "status": "LOCKED_FOR_EXP0004_BLOCK_C",
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
                "retain control plus robust A+B regions with sample support, "
                "cross-A stability and neighboring-B stability; not highest median alone"
            ),
            "single_winner_selected": False,
        },
        "pullback_interpretation": {
            "robust_plateau_bps": [[2500, 5500], [2500, 6000], [2500, 6500]],
            "representative_pair_bps": [2500, 6500],
            "representative_pair_is_permanent_optimum": False,
        },
        "carry_forward": SHORTLIST,
        "dropped_from_prior_A_shortlist": {
            "role": "ROBUST_4",
            "reason": "lower sample / substantial redundancy for staged continuation",
        },
        "next_block": {
            "experiment_id": "EXP-0004",
            "block": "C_BUYER_RESPONSE_3S",
            "AB_configuration_count": 4,
            "buyer_response_combination_count": 150,
            "planned_combination_count": 600,
            "D_fixed": True,
        },
        "notes": (
            "Research shortlist only. The control keeps the permissive 10-90% pullback. "
            "The three robust regimes use 25-65% as a representative of a broader "
            "25-55/60/65 plateau, not as a proven permanent optimum."
        ),
    }

    selection_path = exp3_dir / "EXP-0003_B_selection_v0_1.json"
    if selection_path.exists():
        require(load_json(selection_path) == selection, "existing selection differs")
    else:
        write_json_atomic(selection_path, selection)
    selection_sha = file_sha(selection_path)

    registry = load_json(registry_path)
    core = dict(registry)
    stored = core.pop("deterministic_registry_sha256", None)
    require(stored == stable_sha(core), "registry deterministic hash invalid")
    require(registry.get("next_experiment_id") in ("EXP-0003", "EXP-0004"),
            "unexpected next experiment ID")
    exps = registry.get("experiments")
    require(isinstance(exps, list), "registry experiments invalid")

    existing = [x for x in exps if isinstance(x, dict) and x.get("experiment_id") == "EXP-0003"]
    entry = {
        "experiment_id": "EXP-0003",
        "phase": "PHASE_3_PARAMETER_RESEARCH",
        "role": "STAGED_BLOCK_B_PULLBACK",
        "status": "COMPLETE",
        "dataset_freeze_id": "DEV-FREEZE-0001",
        "dataset_role": "DEVELOPMENT_DISCOVERY",
        "untouched_oos": False,
        "strategy": "FirstPullback v1.1",
        "search_space": "PHASE-2-PARAM-SEARCH-001",
        "report_path": "data/research/phase3/EXP-0003/EXP-0003_B_pullback_report_v0_1.json",
        "report_sha256": EXPECTED_ARTIFACTS["EXP-0003_B_pullback_report_v0_1.json"],
        "manifest_path": "data/research/phase3/EXP-0003/EXP-0003_manifest_v0_1.json",
        "manifest_sha256": EXPECTED_ARTIFACTS["EXP-0003_manifest_v0_1.json"],
        "postrun_audit_path": "data/research/phase3/EXP-0003/EXP-0003_B_postrun_audit_v0_1.json",
        "postrun_audit_sha256": EXPECTED_ARTIFACTS["EXP-0003_B_postrun_audit_v0_1.json"],
        "selection_path": "data/research/phase3/EXP-0003/EXP-0003_B_selection_v0_1.json",
        "selection_sha256": selection_sha,
        "summary": {
            "AB_combinations": 105,
            "candidate_outcome_rows": 5869,
            "diagnostic_pareto_frontier_rows": 15,
            "carry_forward_configurations": 4,
            "carry_forward_includes_control": True,
            "single_winner_selected": False,
            "exit_optimization_performed": False,
            "realistic_net_pnl_available": False,
            "profitability_claim_allowed": False,
        },
    }

    if existing:
        require(len(existing) == 1, "EXP-0003 appears more than once")
        require(existing[0] == entry, "existing EXP-0003 entry differs")
        require(registry.get("next_experiment_id") == "EXP-0004",
                "EXP-0003 registered but next ID is not EXP-0004")
        action = "ALREADY_REGISTERED"
    else:
        require(registry.get("next_experiment_id") == "EXP-0003",
                "registry next ID must be EXP-0003 before registration")
        require(any(isinstance(x, dict) and x.get("experiment_id") == "EXP-0002"
                    and x.get("status") == "COMPLETE" for x in exps),
                "EXP-0002 predecessor missing")

        history = phase3_dir / "registry_history"
        history.mkdir(parents=True, exist_ok=True)
        backup = history / "phase2_experiment_registry_v0_1_pre_EXP-0003.json"
        if not backup.exists():
            shutil.copy2(registry_path, backup)

        updated_core = {
            "registry_schema_version": registry["registry_schema_version"],
            "freeze_id": registry["freeze_id"],
            "dataset_manifest_sha256": registry["dataset_manifest_sha256"],
            "locked_search_decision_id": registry["locked_search_decision_id"],
            "next_experiment_id": "EXP-0004",
            "experiments": exps + [entry],
        }
        updated = dict(updated_core)
        updated["deterministic_registry_sha256"] = stable_sha(updated_core)
        write_json_atomic(registry_path, updated)
        registry = load_json(registry_path)
        action = "REGISTERED"

    final_core = dict(registry)
    final_hash = final_core.pop("deterministic_registry_sha256", None)
    require(final_hash == stable_sha(final_core), "final registry hash invalid")
    require(registry.get("next_experiment_id") == "EXP-0004", "next ID not EXP-0004")
    require(file_sha(freeze_db) == EXPECTED_SNAPSHOT_SHA256, "freeze DB changed")

    print(f"Action                       : {action}")
    print("EXP-0003 status              : COMPLETE")
    print("Carry-forward A+B configs    : 4 (1 control + 3 robust)")
    print("Registry next experiment ID  : EXP-0004")
    print(f"Selection SHA256             : {selection_sha}")
    print(f"Registry deterministic SHA256: {final_hash}")
    print("DEV-FREEZE-0001 SQLite       : UNCHANGED")
    print("Production DB touched        : NO")
    print("Profitability claim          : NO")
    print("=" * 62)
    print("RESULT: PASS")

if __name__ == "__main__":
    main()
