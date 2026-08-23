from __future__ import annotations

from typing import Any, Mapping

SCHEMA_VERSION = "P3-FINAL-VALIDATION-DESIGN-0.1"
DESIGN_ID = "PHASE-3-FINAL-VALIDATION-001"
EXPERIMENT_ID = "EXP-0012"

FINALISTS = (
    {
        "candidate_id": "FINAL-A",
        "variant_id": "E3_TP10_T15",
        "tp_bps": 1000,
        "trail_activation_bps": None,
        "trail_giveback_bps": None,
        "hard_stop_bps": None,
        "fallback_ms": 15_000,
        "track": "MAINLINE",
    },
    {
        "candidate_id": "FINAL-B",
        "variant_id": "E4_ACT10_GB03_T15",
        "tp_bps": None,
        "trail_activation_bps": 1000,
        "trail_giveback_bps": 300,
        "hard_stop_bps": None,
        "fallback_ms": 15_000,
        "track": "MAINLINE",
    },
    {
        "candidate_id": "SENS-C",
        "variant_id": "SENS_TP20_T5",
        "tp_bps": 2000,
        "trail_activation_bps": None,
        "trail_giveback_bps": None,
        "hard_stop_bps": None,
        "fallback_ms": 5_000,
        "track": "SENSITIVITY_ONLY",
    },
)

DESIGN = {
    "schema_version": SCHEMA_VERSION,
    "design_id": DESIGN_ID,
    "experiment_id": EXPERIMENT_ID,
    "status": "LOCKED_BEFORE_FINAL_VALIDATION_RUN",
    "purpose": (
        "Deterministically validate the already-selected Phase-3 finalists end-to-end. "
        "This experiment performs no parameter search and cannot select a new winner."
    ),
    "dataset": {
        "freeze_id": "DEV-FREEZE-0002",
        "dataset_role": "DEVELOPMENT_DISCOVERY",
        "untouched_oos": False,
        "immutable_required": True,
    },
    "preconditions": {
        "EXP-0011_postrun_audit": "PASS",
        "EXP-0011_final_candidate_selection": "LOCKED",
        "entry_A_B_C_D_fixed": True,
        "no_further_DEV_FREEZE_0002_exit_tuning": True,
    },
    "locked_candidates": list(FINALISTS),
    "validation_plan": {
        "independent_end_to_end_replay": True,
        "reproduce_locked_entry_candidate_counts": True,
        "reproduce_locked_clean_5m_counts": True,
        "evaluate_only_locked_finalists": True,
        "compare_against_audited_EXP_0011_rows": True,
        "exact_metric_reproduction_required": True,
        "double_evaluate_cached_paths_for_determinism": True,
        "deterministic_result_hash_match_required": True,
        "sqlite_quick_check_required": True,
        "coverage_binding_required": True,
        "source_hash_binding_required": True,
        "no_parameter_search": True,
        "no_candidate_promotion_or_demotion": True,
    },
    "exact_expected_entry_counts": {
        "CONTROL": 1053,
        "ROBUST_1": 206,
        "ROBUST_2": 232,
        "ROBUST_3": 218,
    },
    "exact_expected_clean_5m_counts": {
        "CONTROL": 1036,
        "ROBUST_1": 203,
        "ROBUST_2": 229,
        "ROBUST_3": 211,
    },
    "phase3_gate": {
        "pass_if": [
            "all source bindings validate",
            "immutable freeze quick_check passes",
            "entry counts reproduce exactly",
            "clean 5m counts reproduce exactly",
            "FINAL-A/FINAL-B/SENS-C reproduce audited E5 rows exactly",
            "two independent exit-evaluation passes over cached clean paths hash identically",
            "terminal accounting remains complete",
            "no lookahead/interpolation/future leakage introduced",
        ],
        "on_pass": "PHASE3_FINAL_VALIDATION_PASS",
        "on_fail": "PHASE3_FINAL_VALIDATION_FAIL",
        "paper_trading_build_allowed_only_on_pass": True,
    },
    "research_guardrails": {
        "single_DEV_data_winner_selected": False,
        "further_exit_tuning_allowed": False,
        "hard_stop_reopened": False,
        "E5_interactions_reopened": False,
        "fees_modeled_here": False,
        "slippage_modeled_here": False,
        "latency_modeled_here": False,
        "execution_fill_modeled_here": False,
        "profitability_claim_allowed": False,
        "live_rule_selected": False,
    },
    "phase4_handoff_intent": {
        "FINAL_A": "paper mainline track",
        "FINAL_B": "paper mainline track",
        "SENS_C": "separate sensitivity track",
        "same_live_candidate_signals_where_practical": True,
        "real_time_cost_and_execution_modeling_begins_in_phase4": True,
    },
}


def validate_design(d: Mapping[str, Any]) -> None:
    if d.get("design_id") != DESIGN_ID:
        raise ValueError("wrong final-validation design id")
    if d.get("experiment_id") != EXPERIMENT_ID:
        raise ValueError("wrong experiment id")
    if d.get("status") != "LOCKED_BEFORE_FINAL_VALIDATION_RUN":
        raise ValueError("design must be locked before run")
    ids = [x["candidate_id"] for x in d["locked_candidates"]]
    if ids != ["FINAL-A", "FINAL-B", "SENS-C"]:
        raise ValueError("locked finalist set changed")
    variants = [x["variant_id"] for x in d["locked_candidates"]]
    if variants != ["E3_TP10_T15", "E4_ACT10_GB03_T15", "SENS_TP20_T5"]:
        raise ValueError("locked finalist variants changed")
    vp = d["validation_plan"]
    required_true = (
        "independent_end_to_end_replay",
        "reproduce_locked_entry_candidate_counts",
        "reproduce_locked_clean_5m_counts",
        "evaluate_only_locked_finalists",
        "compare_against_audited_EXP_0011_rows",
        "exact_metric_reproduction_required",
        "double_evaluate_cached_paths_for_determinism",
        "deterministic_result_hash_match_required",
        "sqlite_quick_check_required",
        "coverage_binding_required",
        "source_hash_binding_required",
    )
    for key in required_true:
        if vp[key] is not True:
            raise ValueError(f"required validation disabled: {key}")
    if vp["no_parameter_search"] is not True:
        raise ValueError("parameter search forbidden")
    if vp["no_candidate_promotion_or_demotion"] is not True:
        raise ValueError("candidate reselection forbidden")
    g = d["research_guardrails"]
    if g["further_exit_tuning_allowed"] is not False:
        raise ValueError("further tuning forbidden")
    if g["profitability_claim_allowed"] is not False:
        raise ValueError("profitability claim forbidden")
    if g["live_rule_selected"] is not False:
        raise ValueError("live rule must not be selected")
