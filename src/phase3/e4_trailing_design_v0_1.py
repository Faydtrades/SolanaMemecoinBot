from __future__ import annotations

from typing import Any, Mapping

SCHEMA_VERSION = "P3-E4-TRAILING-DESIGN-0.1"
DESIGN_ID = "PHASE-3-E4-TRAILING-001"
EXPERIMENT_ID = "EXP-0010"

ROBUST_ROLES = ("ROBUST_1", "ROBUST_2", "ROBUST_3")
ALL_ROLES = ("CONTROL",) + ROBUST_ROLES

# E4 remains isolated from the E3 TP rule. Trailing is tested as its own family
# with the E2/E3 primary robust time fallback (15s).
TRAIL_ACTIVATION_BPS = (500, 1000, 1500)
TRAIL_GIVEBACK_BPS = (300, 500, 1000)
FALLBACK_TIME_STOP_MS = 15_000

DESIGN = {
    "schema_version": SCHEMA_VERSION,
    "design_id": DESIGN_ID,
    "experiment_id": EXPERIMENT_ID,
    "status": "LOCKED_BEFORE_E4_RESULTS",
    "dataset": {
        "freeze_id": "DEV-FREEZE-0002",
        "dataset_role": "DEVELOPMENT_DISCOVERY",
        "untouched_oos": False,
        "closed_prefix_design": "DEV-FREEZE-0002-DESIGN-002A",
    },
    "preconditions": {
        "EXP-0009_E3_result": "PASS",
        "EXP-0009_E3_postrun_audit": "PASS",
        "EXP-0009_E3_selection": "LOCKED",
        "entry_A_B_C_D_fixed": True,
        "E3_primary_anchor": "TP10_T15s",
        "E3_mainline_neighbors": ["TP05_T15s", "TP15_T15s"],
        "E3_T5s_sensitivity": "TP20_T5s",
    },
    "e3_carry_forward_context": {
        "primary_anchor": "TP10_T15s",
        "mainline_neighborhood": ["TP05_T15s", "TP10_T15s", "TP15_T15s"],
        "sensitivity_only": "TP20_T5s",
        "note": (
            "E4 does NOT combine trailing with any E3 TP. The E3 selection is "
            "preserved for later small interaction testing in E5."
        ),
    },
    "trailing_family": {
        "activation_thresholds_bps": list(TRAIL_ACTIVATION_BPS),
        "giveback_distances_bps": list(TRAIL_GIVEBACK_BPS),
        "fallback_time_stop_ms": FALLBACK_TIME_STOP_MS,
        "variant_count": len(TRAIL_ACTIVATION_BPS) * len(TRAIL_GIVEBACK_BPS),
        "activation_semantics": (
            "Trailing activates at the first observed fresh same-identity point "
            "with return_bps >= activation threshold; no interpolation."
        ),
        "ratchet_semantics": (
            "After activation, running peak is updated only from observed fresh "
            "same-identity points. The trail level is running_peak_bps minus the "
            "fixed giveback distance."
        ),
        "exit_semantics": (
            "Exit on the first observed fresh same-identity point at or below the "
            "current trail level, at or before 15s; no interpolation."
        ),
        "fallback_semantics": (
            "If trailing never activates or no trail exit occurs by 15s, exit at "
            "the latest fresh same-identity observation at or before 15s; expose "
            "fallback mark age."
        ),
        "range_reason": (
            "Activation +5/+10/+15% spans the locked E3 mainline TP neighborhood. "
            "Giveback 3/5/10% is a deliberately coarse range, small enough to test "
            "meaningful ratcheting without a dense post-hoc grid."
        ),
    },
    "factor_isolation": {
        "fixed_take_profit_used": False,
        "hard_stop_used": False,
        "partial_take_profit_used": False,
        "multiple_take_profit_used": False,
        "breakeven_stop_used": False,
        "dynamic_time_stop_used": False,
        "T5s_sensitivity_used": False,
        "note": (
            "E4 isolates trailing only. TP/trailing interaction, SL interaction, "
            "and T5s interaction are deferred to E5 small interaction research."
        ),
    },
    "analysis_cohort": {
        "clean_5m_paths_only": True,
        "control_is_informational": True,
        "robust_roles_drive_research_judgment": True,
    },
    "reporting": {
        "gross_reference_returns_only": True,
        "fees_modeled": False,
        "slippage_modeled": False,
        "latency_modeled": False,
        "execution_fill_modeled": False,
        "profitability_claim_allowed": False,
        "required_metrics": [
            "clean_path_count",
            "scored_count",
            "unscored_count",
            "activation_count",
            "activation_rate",
            "trail_exit_count",
            "trail_exit_rate",
            "fallback_exit_count",
            "p10_exit_bps",
            "p25_exit_bps",
            "p50_exit_bps",
            "p75_exit_bps",
            "p90_exit_bps",
            "mean_exit_bps",
            "positive_rate",
            "exit_delay_p50_ms",
            "activation_delay_p50_ms",
            "fallback_mark_age_p50_ms",
        ],
    },
    "selection_guardrails": [
        "Do not select a trailing variant by highest gross mean alone.",
        "Prefer robust cross-regime agreement and neighboring-grid stability.",
        "Activation rate and trail-exit rate must be considered alongside returns.",
        "A variant that mostly falls back to T15s is not strong evidence for trailing.",
        "No E3 TP is allowed inside E4.",
        "No hard-stop interaction is allowed inside E4.",
        "E4 may produce a trailing shortlist, not a live exit rule.",
        "No realistic-net profitability claim is allowed.",
    ],
}

def validate_design(d: Mapping[str, Any]) -> None:
    if d.get("design_id") != DESIGN_ID:
        raise ValueError("wrong E4 design id")
    if d.get("experiment_id") != EXPERIMENT_ID:
        raise ValueError("wrong experiment id")
    if d.get("status") != "LOCKED_BEFORE_E4_RESULTS":
        raise ValueError("E4 design must be locked before results")
    tf = d["trailing_family"]
    if tuple(tf["activation_thresholds_bps"]) != TRAIL_ACTIVATION_BPS:
        raise ValueError("activation grid changed")
    if tuple(tf["giveback_distances_bps"]) != TRAIL_GIVEBACK_BPS:
        raise ValueError("giveback grid changed")
    if tf["fallback_time_stop_ms"] != FALLBACK_TIME_STOP_MS:
        raise ValueError("fallback changed")
    if tf["variant_count"] != 9:
        raise ValueError("expected exactly 9 trailing variants")
    if d["preconditions"]["entry_A_B_C_D_fixed"] is not True:
        raise ValueError("entry parameters changed")
    iso = d["factor_isolation"]
    for key in (
        "fixed_take_profit_used",
        "hard_stop_used",
        "partial_take_profit_used",
        "multiple_take_profit_used",
        "breakeven_stop_used",
        "dynamic_time_stop_used",
        "T5s_sensitivity_used",
    ):
        if iso[key] is not False:
            raise ValueError(f"factor isolation violated: {key}")
    if d["reporting"]["profitability_claim_allowed"] is not False:
        raise ValueError("profitability claim forbidden")
