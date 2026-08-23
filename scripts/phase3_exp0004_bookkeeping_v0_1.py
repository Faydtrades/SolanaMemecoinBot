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
    "EXP-0004_C_buyer_response_summary_v0_1.csv": "1c68fbcb1c8468f6753299c06956cf53ae8a3c17015b5a288f0b03585f52c402",
    "EXP-0004_C_buyer_response_frontier_v0_1.csv": "9830e2e394adf89aef5fbb27453d71c4b1f1af6cd4e76133e515365e8909a592",
    "EXP-0004_C_buyer_response_candidate_outcomes_v0_1.csv": "68aae2d06207888597eae9f987a91a18be2ff022477bd62d7325e6ae00d25856",
    "EXP-0004_C_buyer_response_report_v0_1.json": "90da7b3f07aad824f1a33f8be91bbd2eb613131187b80db906f0368a831a8996",
    "EXP-0004_manifest_v0_1.json": "123bd87161afda82a14116835d2501e87e961c40db40f482fccf8e64bf4f9935",
    "EXP-0004_C_neighborhood_diagnostics_v0_1.csv": "dc0023175af063c7ade3df5255fb7b6e13bf9cb723271a52ef66193bfb88660d",
    "EXP-0004_C_crossAB_diagnostics_v0_1.csv": "40e3018678f5439bc49719225c0e52bec751fbdb2a6edd471c39bf3a59dfc13f",
    "EXP-0004_C_postrun_audit_v0_1.json": "903725ae0baba2443079944331b4c2ef6f58e41705213ad587ef74afc7b06e93",
}

SHORTLIST = [
    {
        "role": "CONTROL",
        "parameter_set_id": "FP1-EXP0004-C-001-CONTROL-R500-T2-U1-PD1000-9000-RB200-BU1-FL0",
        "min_return_bps": 500,
        "min_trades_since_t0": 2,
        "min_unique_buyers_since_t0": 1,
        "min_depth_bps": 1000,
        "max_depth_bps": 9000,
        "min_rebound_bps": 200,
        "min_buys": 1,
        "min_net_flow_reserve_ppm": 0,
        "candidate_count": 230,
        "h15_observable_n": 127,
        "h15_p50_bps": 181,
        "h30_observable_n": 67,
        "h30_p50_bps": 44,
    },
    {
        "role": "ROBUST_1",
        "parameter_set_id": "FP1-EXP0004-C-232-ROBUST_1-R1500-T5-U9-PD2500-6500-RB2300-BU2-FL6000",
        "min_return_bps": 1500,
        "min_trades_since_t0": 5,
        "min_unique_buyers_since_t0": 9,
        "min_depth_bps": 2500,
        "max_depth_bps": 6500,
        "min_rebound_bps": 2300,
        "min_buys": 2,
        "min_net_flow_reserve_ppm": 6000,
        "candidate_count": 19,
        "h15_observable_n": 9,
        "h15_p50_bps": 741,
        "h30_observable_n": 5,
        "h30_p50_bps": 1213,
    },
    {
        "role": "ROBUST_2",
        "parameter_set_id": "FP1-EXP0004-C-382-ROBUST_2-R2000-T3-U5-PD2500-6500-RB2300-BU2-FL6000",
        "min_return_bps": 2000,
        "min_trades_since_t0": 3,
        "min_unique_buyers_since_t0": 5,
        "min_depth_bps": 2500,
        "max_depth_bps": 6500,
        "min_rebound_bps": 2300,
        "min_buys": 2,
        "min_net_flow_reserve_ppm": 6000,
        "candidate_count": 23,
        "h15_observable_n": 10,
        "h15_p50_bps": 645,
        "h30_observable_n": 8,
        "h30_p50_bps": 826,
    },
    {
        "role": "ROBUST_3",
        "parameter_set_id": "FP1-EXP0004-C-532-ROBUST_3-R5000-T3-U2-PD2500-6500-RB2300-BU2-FL6000",
        "min_return_bps": 5000,
        "min_trades_since_t0": 3,
        "min_unique_buyers_since_t0": 2,
        "min_depth_bps": 2500,
        "max_depth_bps": 6500,
        "min_rebound_bps": 2300,
        "min_buys": 2,
        "min_net_flow_reserve_ppm": 6000,
        "candidate_count": 25,
        "h15_observable_n": 10,
        "h15_p50_bps": 483,
        "h30_observable_n": 9,
        "h30_p50_bps": 748,
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
    exp4_dir = root / "data/research/phase3/EXP-0004"
    phase3_dir = root / "data/research/phase3"

    registry_path = freeze_dir / "phase2_experiment_registry_v0_1.json"
    freeze_db = freeze_dir / "phase2_development_dataset_v0_1.sqlite3"

    print("EXP-0004 BLOCK C BOOKKEEPING v0.1")
    print("=" * 66)

    require(registry_path.exists(), "canonical registry missing")
    require(freeze_db.exists(), "DEV-FREEZE-0001 SQLite missing")
    require(file_sha(freeze_db) == EXPECTED_SNAPSHOT_SHA256, "freeze SQLite hash mismatch")

    for name, expected in EXPECTED_ARTIFACTS.items():
        p = exp4_dir / name
        require(p.exists(), f"missing reviewed artifact: {name}")
        require(file_sha(p) == expected, f"artifact hash mismatch: {name}")

    audit = load_json(exp4_dir / "EXP-0004_C_postrun_audit_v0_1.json")
    require(audit.get("result") == "PASS", "EXP-0004 audit did not PASS")
    checks = audit.get("checks", {})
    for key in (
        "exact_600_grid", "terminal_accounting", "candidate_rows_match",
        "frontier_consistent", "determinism", "no_winner_selected",
        "no_exit_optimization", "no_profitability_claim",
    ):
        require(checks.get(key) is True, f"audit check failed: {key}")

    with (exp4_dir / "EXP-0004_C_buyer_response_summary_v0_1.csv").open(
        "r", encoding="utf-8", newline=""
    ) as f:
        rows = list(csv.DictReader(f))
    by_id = {r["parameter_set_id"]: r for r in rows}

    for item in SHORTLIST:
        require(item["parameter_set_id"] in by_id, f"shortlist row missing: {item['parameter_set_id']}")
        row = by_id[item["parameter_set_id"]]
        for field in (
            "min_return_bps", "min_trades_since_t0", "min_unique_buyers_since_t0",
            "min_depth_bps", "max_depth_bps", "min_rebound_bps", "min_buys",
            "min_net_flow_reserve_ppm", "candidate_count",
            "h15_observable_n", "h15_p50_bps", "h30_observable_n", "h30_p50_bps",
        ):
            require(int(float(row[field])) == int(item[field]),
                    f"{item['parameter_set_id']} {field} mismatch")

    deferred = {
        "schema_version": "P3-DEFERRED-HYPOTHESES-0.1",
        "source_experiment_id": "EXP-0004",
        "status": "RETAIN_FOR_FRESH_DATA_AND_PHASE4_SHADOW_RESEARCH",
        "principle": (
            "Not selected for main carry-forward because current clean sample support is too small. "
            "Not disproven. Re-evaluate only with larger/fresher evidence."
        ),
        "hypotheses": [
            {
                "hypothesis_id": "EXP0004-C-DEFER-001",
                "name": "HIGH_BUY_COUNT_PLUS_HIGH_RESERVE_FLOW",
                "family": {
                    "min_buys_gte": 4,
                    "min_net_flow_reserve_ppm_gte": 93000,
                },
                "reason_retained": (
                    "Sparse pockets showed unusually strong post-signal outcomes in some A+B regimes, "
                    "but clean 30s sample counts were too small for promotion."
                ),
                "promotion_status": "DEFERRED_NOT_REJECTED",
            },
            {
                "hypothesis_id": "EXP0004-C-DEFER-002",
                "name": "VERY_HIGH_REBOUND",
                "family": {
                    "min_rebound_bps_gte": 7300,
                    "extreme_subfamily_min_rebound_bps": 20700,
                },
                "reason_retained": (
                    "Some very high rebound settings showed strong-looking outcomes, "
                    "especially at 207%+, but often on only a handful of clean observations."
                ),
                "promotion_status": "DEFERRED_NOT_REJECTED",
            },
        ],
        "future_evaluation": {
            "development_replay": "re-run on larger/fresher datasets without altering historical results",
            "phase4_paper": "log as parallel research/shadow signals with no money",
            "required_evidence": [
                "larger clean sample support",
                "stability across fresh time ranges",
                "outcome distribution, not median alone",
                "false-positive / false-negative implications",
                "friction-aware behavior before any live promotion",
            ],
            "must_not_bias_EXP0005_Block_D_selection": True,
        },
    }
    deferred_path = exp4_dir / "EXP-0004_C_deferred_hypotheses_v0_1.json"
    if deferred_path.exists():
        require(load_json(deferred_path) == deferred, "existing deferred hypothesis ledger differs")
    else:
        write_json_atomic(deferred_path, deferred)
    deferred_sha = file_sha(deferred_path)

    selection = {
        "schema_version": "P3-EXP0004-C-SELECTION-0.1",
        "experiment_id": "EXP-0004",
        "status": "LOCKED_FOR_EXP0005_BLOCK_D",
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
                "retain control plus robust A+B+C settings with sample support, "
                "cross-regime and local-neighborhood stability; do not select highest median alone"
            ),
            "single_winner_selected": False,
        },
        "buyer_response_interpretation": {
            "robust_representative": {
                "min_rebound_bps": 2300,
                "min_buys": 2,
                "min_net_flow_reserve_ppm": 6000,
            },
            "representative_is_permanent_optimum": False,
            "control": {
                "min_rebound_bps": 200,
                "min_buys": 1,
                "min_net_flow_reserve_ppm": 0,
            },
        },
        "carry_forward": SHORTLIST,
        "deferred_hypotheses_path": "data/research/phase3/EXP-0004/EXP-0004_C_deferred_hypotheses_v0_1.json",
        "deferred_hypotheses_sha256": deferred_sha,
        "next_block": {
            "experiment_id": "EXP-0005",
            "block": "D_RECLAIM_RUNAWAY",
            "ABC_configuration_count": 4,
            "valid_D_pair_count": 24,
            "planned_combination_count": 96,
        },
        "notes": (
            "Main carry-forward and deferred high-stringency hypotheses are separate. "
            "Deferred hypotheses are explicitly retained for future fresh-data and Phase-4 shadow research."
        ),
    }

    selection_path = exp4_dir / "EXP-0004_C_selection_v0_1.json"
    if selection_path.exists():
        require(load_json(selection_path) == selection, "existing selection differs")
    else:
        write_json_atomic(selection_path, selection)
    selection_sha = file_sha(selection_path)

    registry = load_json(registry_path)
    core = dict(registry)
    stored = core.pop("deterministic_registry_sha256", None)
    require(stored == stable_sha(core), "registry deterministic hash invalid")
    exps = registry.get("experiments")
    require(isinstance(exps, list), "registry experiments invalid")

    existing = [x for x in exps if isinstance(x, dict) and x.get("experiment_id") == "EXP-0004"]

    entry = {
        "experiment_id": "EXP-0004",
        "phase": "PHASE_3_PARAMETER_RESEARCH",
        "role": "STAGED_BLOCK_C_BUYER_RESPONSE_3S",
        "status": "COMPLETE",
        "dataset_freeze_id": "DEV-FREEZE-0001",
        "dataset_role": "DEVELOPMENT_DISCOVERY",
        "untouched_oos": False,
        "strategy": "FirstPullback v1.1",
        "search_space": "PHASE-2-PARAM-SEARCH-001",
        "report_path": "data/research/phase3/EXP-0004/EXP-0004_C_buyer_response_report_v0_1.json",
        "report_sha256": EXPECTED_ARTIFACTS["EXP-0004_C_buyer_response_report_v0_1.json"],
        "manifest_path": "data/research/phase3/EXP-0004/EXP-0004_manifest_v0_1.json",
        "manifest_sha256": EXPECTED_ARTIFACTS["EXP-0004_manifest_v0_1.json"],
        "postrun_audit_path": "data/research/phase3/EXP-0004/EXP-0004_C_postrun_audit_v0_1.json",
        "postrun_audit_sha256": EXPECTED_ARTIFACTS["EXP-0004_C_postrun_audit_v0_1.json"],
        "selection_path": "data/research/phase3/EXP-0004/EXP-0004_C_selection_v0_1.json",
        "selection_sha256": selection_sha,
        "deferred_hypotheses_path": "data/research/phase3/EXP-0004/EXP-0004_C_deferred_hypotheses_v0_1.json",
        "deferred_hypotheses_sha256": deferred_sha,
        "summary": {
            "ABC_combinations": 600,
            "candidate_outcome_rows": 14443,
            "diagnostic_pareto_frontier_rows": 31,
            "carry_forward_configurations": 4,
            "deferred_hypotheses_retained": 2,
            "single_winner_selected": False,
            "exit_optimization_performed": False,
            "realistic_net_pnl_available": False,
            "profitability_claim_allowed": False,
        },
    }

    if existing:
        require(len(existing) == 1, "EXP-0004 appears more than once")
        require(existing[0] == entry, "existing EXP-0004 entry differs")
        require(registry.get("next_experiment_id") == "EXP-0005",
                "EXP-0004 registered but next ID is not EXP-0005")
        action = "ALREADY_REGISTERED"
    else:
        require(registry.get("next_experiment_id") == "EXP-0004",
                "registry next ID must be EXP-0004 before registration")
        require(any(isinstance(x, dict) and x.get("experiment_id") == "EXP-0003"
                    and x.get("status") == "COMPLETE" for x in exps),
                "EXP-0003 predecessor missing")

        history = phase3_dir / "registry_history"
        history.mkdir(parents=True, exist_ok=True)
        backup = history / "phase2_experiment_registry_v0_1_pre_EXP-0004.json"
        if not backup.exists():
            shutil.copy2(registry_path, backup)

        updated_core = {
            "registry_schema_version": registry["registry_schema_version"],
            "freeze_id": registry["freeze_id"],
            "dataset_manifest_sha256": registry["dataset_manifest_sha256"],
            "locked_search_decision_id": registry["locked_search_decision_id"],
            "next_experiment_id": "EXP-0005",
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
    require(registry.get("next_experiment_id") == "EXP-0005", "next ID not EXP-0005")
    require(file_sha(freeze_db) == EXPECTED_SNAPSHOT_SHA256, "freeze DB changed")

    print(f"Action                       : {action}")
    print("EXP-0004 status              : COMPLETE")
    print("Carry-forward A+B+C configs  : 4 (1 control + 3 robust)")
    print("Deferred hypotheses retained : 2")
    print("Registry next experiment ID  : EXP-0005")
    print(f"Selection SHA256             : {selection_sha}")
    print(f"Deferred ledger SHA256       : {deferred_sha}")
    print(f"Registry deterministic SHA256: {final_hash}")
    print("DEV-FREEZE-0001 SQLite       : UNCHANGED")
    print("Production DB touched        : NO")
    print("Profitability claim          : NO")
    print("=" * 66)
    print("RESULT: PASS")

if __name__ == "__main__":
    main()
