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
    "EXP-0005_D_reclaim_runaway_summary_v0_1.csv": "5691edc3cac032e39c2af262d95d8e6f5ced1d6edc0b0875c85d6b4d999cf5d3",
    "EXP-0005_D_reclaim_runaway_frontier_v0_1.csv": "1490d7caef477a2a984e0d010e948774c0523a8f08a7df0cc9f4b04ddd9a3d94",
    "EXP-0005_D_reclaim_runaway_candidate_outcomes_v0_1.csv": "5f5c0b7b4acd770bd08cb97b1be642bf291c39cd13ffc3397b28ce99c82aee98",
    "EXP-0005_D_reclaim_runaway_report_v0_1.json": "401e1e02e57470bb3e0cc5e94ecbce24e160d7e2d482cd3877217dd3e4b95094",
    "EXP-0005_manifest_v0_1.json": "da52234e586cc45a91205499714c909f0ed4962258118b98fae78b9dadab3f5c",
    "EXP-0005_D_neighborhood_diagnostics_v0_1.csv": "bea335b98e361b05402cebab3d1bd0f29a2995c588ad9c3fec8527b649d11ea3",
    "EXP-0005_D_crossABC_diagnostics_v0_1.csv": "223feebcb6a9502f14c0a1a53aa16432ab40e00db4bf7e49aecbac42a3cc70ea",
    "EXP-0005_D_postrun_audit_v0_1.json": "12beeccf7623903b2e30adfdb13cd3a57df76dd827dd26ccc9074d3179b97442",
}

SHORTLIST = [
    {
        "role": "CONTROL",
        "parameter_set_id": "FP1-EXP0005-D-005-CONTROL-R500-T2-U1-PD1000-9000-RB200-BU1-FL0-RC200-RW10000",
        "min_return_bps": 500,
        "min_trades_since_t0": 2,
        "min_unique_buyers_since_t0": 1,
        "min_depth_bps": 1000,
        "max_depth_bps": 9000,
        "min_rebound_bps": 200,
        "min_buys": 1,
        "min_net_flow_reserve_ppm": 0,
        "min_extension_from_response_bps": 200,
        "max_extension_from_response_bps": 10000,
        "candidate_count": 230,
        "h15_observable_n": 127,
        "h15_p50_bps": 181,
        "h30_observable_n": 67,
        "h30_p50_bps": 44,
        "h60_observable_n": 27,
        "h2m_observable_n": 25,
        "h5m_observable_n": 11,
    },
    {
        "role": "ROBUST_1",
        "parameter_set_id": "FP1-EXP0005-D-026-ROBUST_1-R1500-T5-U9-PD2500-6500-RB2300-BU2-FL6000-RC200-RW2500",
        "min_return_bps": 1500,
        "min_trades_since_t0": 5,
        "min_unique_buyers_since_t0": 9,
        "min_depth_bps": 2500,
        "max_depth_bps": 6500,
        "min_rebound_bps": 2300,
        "min_buys": 2,
        "min_net_flow_reserve_ppm": 6000,
        "min_extension_from_response_bps": 200,
        "max_extension_from_response_bps": 2500,
        "candidate_count": 19,
        "h15_observable_n": 9,
        "h15_p50_bps": 741,
        "h30_observable_n": 5,
        "h30_p50_bps": 1213,
        "h60_observable_n": 4,
        "h2m_observable_n": 2,
        "h5m_observable_n": 0,
    },
    {
        "role": "ROBUST_2",
        "parameter_set_id": "FP1-EXP0005-D-050-ROBUST_2-R2000-T3-U5-PD2500-6500-RB2300-BU2-FL6000-RC200-RW2500",
        "min_return_bps": 2000,
        "min_trades_since_t0": 3,
        "min_unique_buyers_since_t0": 5,
        "min_depth_bps": 2500,
        "max_depth_bps": 6500,
        "min_rebound_bps": 2300,
        "min_buys": 2,
        "min_net_flow_reserve_ppm": 6000,
        "min_extension_from_response_bps": 200,
        "max_extension_from_response_bps": 2500,
        "candidate_count": 22,
        "h15_observable_n": 10,
        "h15_p50_bps": 645,
        "h30_observable_n": 8,
        "h30_p50_bps": 826,
        "h60_observable_n": 5,
        "h2m_observable_n": 3,
        "h5m_observable_n": 0,
    },
    {
        "role": "ROBUST_3",
        "parameter_set_id": "FP1-EXP0005-D-074-ROBUST_3-R5000-T3-U2-PD2500-6500-RB2300-BU2-FL6000-RC200-RW2500",
        "min_return_bps": 5000,
        "min_trades_since_t0": 3,
        "min_unique_buyers_since_t0": 2,
        "min_depth_bps": 2500,
        "max_depth_bps": 6500,
        "min_rebound_bps": 2300,
        "min_buys": 2,
        "min_net_flow_reserve_ppm": 6000,
        "min_extension_from_response_bps": 200,
        "max_extension_from_response_bps": 2500,
        "candidate_count": 21,
        "h15_observable_n": 9,
        "h15_p50_bps": 692,
        "h30_observable_n": 7,
        "h30_p50_bps": 826,
        "h60_observable_n": 5,
        "h2m_observable_n": 3,
        "h5m_observable_n": 0,
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
    exp5_dir = root / "data/research/phase3/EXP-0005"
    phase3_dir = root / "data/research/phase3"
    registry_path = freeze_dir / "phase2_experiment_registry_v0_1.json"
    freeze_db = freeze_dir / "phase2_development_dataset_v0_1.sqlite3"

    print("EXP-0005 BLOCK D BOOKKEEPING v0.1")
    print("=" * 68)

    require(registry_path.exists(), "canonical registry missing")
    require(freeze_db.exists(), "DEV-FREEZE-0001 SQLite missing")
    require(file_sha(freeze_db) == EXPECTED_SNAPSHOT_SHA256, "freeze SQLite hash mismatch")

    for name, expected in EXPECTED_ARTIFACTS.items():
        p = exp5_dir / name
        require(p.exists(), f"missing reviewed artifact: {name}")
        require(file_sha(p) == expected, f"artifact hash mismatch: {name}")

    audit = load_json(exp5_dir / "EXP-0005_D_postrun_audit_v0_1.json")
    require(audit.get("result") == "PASS", "EXP-0005 audit did not PASS")
    checks = audit.get("checks", {})
    for key in (
        "exact_96_grid", "terminal_accounting", "candidate_rows_match",
        "frontier_consistent", "determinism", "no_winner_selected",
        "no_exit_optimization", "no_profitability_claim",
        "deferred_BlockC_hypotheses_separate",
    ):
        require(checks.get(key) is True, f"audit check failed: {key}")

    with (exp5_dir / "EXP-0005_D_reclaim_runaway_summary_v0_1.csv").open(
        "r", encoding="utf-8", newline=""
    ) as f:
        rows = list(csv.DictReader(f))
    by_id = {r["parameter_set_id"]: r for r in rows}

    mapping = {
        "h60_observable_n": "h60s_observable_n",
        "h2m_observable_n": "h2m_observable_n",
        "h5m_observable_n": "h5m_observable_n",
    }
    for item in SHORTLIST:
        require(item["parameter_set_id"] in by_id, f"shortlist row missing: {item['parameter_set_id']}")
        row = by_id[item["parameter_set_id"]]
        fields = (
            "min_return_bps", "min_trades_since_t0", "min_unique_buyers_since_t0",
            "min_depth_bps", "max_depth_bps", "min_rebound_bps", "min_buys",
            "min_net_flow_reserve_ppm", "min_extension_from_response_bps",
            "max_extension_from_response_bps", "candidate_count",
            "h15_observable_n", "h15_p50_bps", "h30_observable_n", "h30_p50_bps",
        )
        for field in fields:
            require(int(float(row[field])) == int(item[field]),
                    f"{item['parameter_set_id']} {field} mismatch")
        for target, source in mapping.items():
            require(int(float(row[source])) == int(item[target]),
                    f"{item['parameter_set_id']} {source} mismatch")

    deferred = {
        "schema_version": "P3-EXP0005-D-DEFERRED-0.1",
        "source_experiment_id": "EXP-0005",
        "status": "RETAIN_FOR_FRESH_DATA_AND_PHASE4_SHADOW_RESEARCH",
        "prior_deferred_ledger": "data/research/phase3/EXP-0004/EXP-0004_C_deferred_hypotheses_v0_1.json",
        "principle": "Not selected now does not mean disproven.",
        "hypotheses": [
            {
                "hypothesis_id": "EXP0005-D-DEFER-001",
                "name": "HIGH_RECLAIM_NARROW_RUNAWAY",
                "family": {
                    "min_extension_from_response_bps": 2500,
                    "max_extension_from_response_bps": 3000,
                },
                "observed_development_summary": {
                    "median_ABC_candidate_count": 7,
                    "median_ABC_clean_30s_n": 4,
                    "median_of_ABC_30s_p50_bps": 1584,
                    "robust_subset_30s_median_signal_bps": 2420,
                },
                "reason_not_promoted": (
                    "Very strong-looking 30s outcomes but sparse clean support. "
                    "Retain as a hypothesis for larger/fresher data and Phase-4 shadow logging."
                ),
                "promotion_status": "DEFERRED_NOT_REJECTED",
            }
        ],
        "future_evaluation": {
            "fresh_development_data": True,
            "phase4_parallel_shadow_logging": True,
            "must_not_bias_main_exit_search": True,
        },
    }
    deferred_path = exp5_dir / "EXP-0005_D_deferred_hypotheses_v0_1.json"
    if deferred_path.exists():
        require(load_json(deferred_path) == deferred, "existing deferred D ledger differs")
    else:
        write_json_atomic(deferred_path, deferred)
    deferred_sha = file_sha(deferred_path)

    selection = {
        "schema_version": "P3-EXP0005-D-SELECTION-0.1",
        "experiment_id": "EXP-0005",
        "status": "LOCKED_FOR_EXIT_RESEARCH",
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
                "retain permissive control plus robust full-entry regimes with "
                "cross-regime and neighboring-D stability; not highest median alone"
            ),
            "single_winner_selected": False,
        },
        "D_interpretation": {
            "robust_representative": {
                "min_extension_from_response_bps": 200,
                "max_extension_from_response_bps": 2500,
            },
            "robust_runaway_plateau_bps": [2000, 2500, 3000],
            "representative_is_permanent_optimum": False,
            "control": {
                "min_extension_from_response_bps": 200,
                "max_extension_from_response_bps": 10000,
            },
        },
        "carry_forward": SHORTLIST,
        "deferred_hypotheses_path": "data/research/phase3/EXP-0005/EXP-0005_D_deferred_hypotheses_v0_1.json",
        "deferred_hypotheses_sha256": deferred_sha,
        "next_step": {
            "experiment_id": "EXP-0006",
            "role": "EXIT_RESEARCH_DESIGN_AND_PATH_FEASIBILITY",
            "exit_thresholds_locked": False,
            "sl_tp_trailing_time_stop_tuning_allowed_yet": False,
        },
        "notes": (
            "A/B/C/D entry research is now represented by one control and three robust "
            "full-entry regimes. This is not a profitability claim."
        ),
    }

    selection_path = exp5_dir / "EXP-0005_D_selection_v0_1.json"
    if selection_path.exists():
        require(load_json(selection_path) == selection, "existing EXP-0005 selection differs")
    else:
        write_json_atomic(selection_path, selection)
    selection_sha = file_sha(selection_path)

    registry = load_json(registry_path)
    core = dict(registry)
    stored = core.pop("deterministic_registry_sha256", None)
    require(stored == stable_sha(core), "registry deterministic hash invalid")
    exps = registry.get("experiments")
    require(isinstance(exps, list), "registry experiments invalid")

    existing = [x for x in exps if isinstance(x, dict) and x.get("experiment_id") == "EXP-0005"]
    entry = {
        "experiment_id": "EXP-0005",
        "phase": "PHASE_3_PARAMETER_RESEARCH",
        "role": "STAGED_BLOCK_D_RECLAIM_RUNAWAY",
        "status": "COMPLETE",
        "dataset_freeze_id": "DEV-FREEZE-0001",
        "dataset_role": "DEVELOPMENT_DISCOVERY",
        "untouched_oos": False,
        "strategy": "FirstPullback v1.1",
        "search_space": "PHASE-2-PARAM-SEARCH-001",
        "report_path": "data/research/phase3/EXP-0005/EXP-0005_D_reclaim_runaway_report_v0_1.json",
        "report_sha256": EXPECTED_ARTIFACTS["EXP-0005_D_reclaim_runaway_report_v0_1.json"],
        "manifest_path": "data/research/phase3/EXP-0005/EXP-0005_manifest_v0_1.json",
        "manifest_sha256": EXPECTED_ARTIFACTS["EXP-0005_manifest_v0_1.json"],
        "postrun_audit_path": "data/research/phase3/EXP-0005/EXP-0005_D_postrun_audit_v0_1.json",
        "postrun_audit_sha256": EXPECTED_ARTIFACTS["EXP-0005_D_postrun_audit_v0_1.json"],
        "selection_path": "data/research/phase3/EXP-0005/EXP-0005_D_selection_v0_1.json",
        "selection_sha256": selection_sha,
        "deferred_hypotheses_path": "data/research/phase3/EXP-0005/EXP-0005_D_deferred_hypotheses_v0_1.json",
        "deferred_hypotheses_sha256": deferred_sha,
        "summary": {
            "ABCD_combinations": 96,
            "candidate_outcome_rows": 5514,
            "diagnostic_pareto_frontier_rows": 23,
            "carry_forward_full_entry_configurations": 4,
            "deferred_D_hypotheses_retained": 1,
            "entry_staged_search_complete_A_through_D": True,
            "exit_optimization_performed": False,
            "realistic_net_pnl_available": False,
            "profitability_claim_allowed": False,
        },
    }

    if existing:
        require(len(existing) == 1, "EXP-0005 appears more than once")
        require(existing[0] == entry, "existing EXP-0005 registry entry differs")
        require(registry.get("next_experiment_id") == "EXP-0006",
                "EXP-0005 registered but next ID is not EXP-0006")
        action = "ALREADY_REGISTERED"
    else:
        require(registry.get("next_experiment_id") == "EXP-0005",
                "registry next ID must be EXP-0005 before registration")
        require(any(isinstance(x, dict) and x.get("experiment_id") == "EXP-0004"
                    and x.get("status") == "COMPLETE" for x in exps),
                "EXP-0004 predecessor missing")

        history = phase3_dir / "registry_history"
        history.mkdir(parents=True, exist_ok=True)
        backup = history / "phase2_experiment_registry_v0_1_pre_EXP-0005.json"
        if not backup.exists():
            shutil.copy2(registry_path, backup)

        updated_core = {
            "registry_schema_version": registry["registry_schema_version"],
            "freeze_id": registry["freeze_id"],
            "dataset_manifest_sha256": registry["dataset_manifest_sha256"],
            "locked_search_decision_id": registry["locked_search_decision_id"],
            "next_experiment_id": "EXP-0006",
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
    require(registry.get("next_experiment_id") == "EXP-0006", "next ID not EXP-0006")
    require(file_sha(freeze_db) == EXPECTED_SNAPSHOT_SHA256, "freeze DB changed")

    print(f"Action                       : {action}")
    print("EXP-0005 status              : COMPLETE")
    print("Full-entry carry-forward     : 4 (1 control + 3 robust)")
    print("Deferred D hypotheses        : 1 retained")
    print("Entry staged search A->D     : COMPLETE")
    print("Registry next experiment ID  : EXP-0006")
    print(f"Selection SHA256             : {selection_sha}")
    print(f"Deferred ledger SHA256       : {deferred_sha}")
    print(f"Registry deterministic SHA256: {final_hash}")
    print("DEV-FREEZE-0001 SQLite       : UNCHANGED")
    print("Production DB touched        : NO")
    print("Exit optimization performed  : NO")
    print("Profitability claim          : NO")
    print("=" * 68)
    print("RESULT: PASS")

if __name__ == "__main__":
    main()
