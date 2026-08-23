from __future__ import annotations

from typing import Any, Mapping

SCHEMA_VERSION = "P3-E3-PROFIT-TAKING-DESIGN-0.1"
DESIGN_ID = "PHASE-3-E3-PROFIT-TAKING-001"
EXPERIMENT_ID = "EXP-0009"

ROBUST_ROLES = ("ROBUST_1", "ROBUST_2", "ROBUST_3")
ALL_ROLES = ("CONTROL",) + ROBUST_ROLES

# Coarse TP family. Chosen after E2, before any E3 results.
# The lower end covers the early 5s/15s signal scale seen in E2; the upper end
# reaches the larger E1 path excursions without over-densifying the grid.
TAKE_PROFIT_BPS = (300, 500, 1000, 1500, 2000, 3000)

# E2 carry-forward:
# 15s = primary robust anchor (near-complete scoring, positive across robust roles)
# 5s = aggressive sensitivity hypothesis (stronger gross results but less complete scoring)
FALLBACK_TIME_STOP_MS = (15_000, 5_000)

DESIGN = {
    "schema_version": SCHEMA_VERSION,
    "design_id": DESIGN_ID,
    "experiment_id": EXPERIMENT_ID,
    "status": "LOCKED_BEFORE_E3_RESULTS",
    "dataset": {
        "freeze_id": "DEV-FREEZE-0002",
        "dataset_role": "DEVELOPMENT_DISCOVERY",
        "untouched_oos": False,
        "closed_prefix_design": "DEV-FREEZE-0002-DESIGN-002A",
    },
    "preconditions": {
        "EXP-0008_E2_result": "PASS",
        "EXP-0008_postrun_audit": "PASS",
        "entry_A_B_C_D_fixed": True,
        "E2_time_stop_primary_anchor": "T15s",
        "E2_time_stop_sensitivity": "T5s",
        "E2_hard_stop_mainline_winner": None,
    },
    "e2_basis": {
        "T5s": {
            "role": "AGGRESSIVE_SENSITIVITY",
            "reason": (
                "Highest early gross reference results across robust regimes, "
                "but materially more unscored paths than T15s."
            ),
        },
        "T15s": {
            "role": "PRIMARY_ROBUST_ANCHOR",
            "reason": (
                "Positive robust-regime medians/means with near-complete scoring "
                "and substantially better methodological coverage than T5s."
            ),
        },
        "T30s": {
            "role": "BOUNDARY_REFERENCE_ONLY",
            "reason": "E2 showed the early edge largely decayed by ~30s.",
        },
        "hard_stop": {
            "mainline_carry_forward": False,
            "SL10_retained_only_as_future_interaction_hypothesis": True,
            "historical_SL30_promoted": False,
        },
    },
    "profit_taking_family": {
        "take_profit_thresholds_bps": list(TAKE_PROFIT_BPS),
        "fallback_time_stops_ms": list(FALLBACK_TIME_STOP_MS),
        "primary_fallback_ms": 15_000,
        "sensitivity_fallback_ms": 5_000,
        "threshold_crossing_semantics": (
            "First observed fresh same-identity point >= TP threshold; no interpolation."
        ),
        "fallback_semantics": (
            "If TP is not observed before the fallback horizon, exit at the latest "
            "fresh same-identity observation at or before that horizon; mark_age_ms exposed."
        ),
        "evaluation_window": (
            "A TP hit counts only if its observed_at is <= the selected fallback horizon."
        ),
    },
    "factor_isolation": {
        "hard_stop_used": False,
        "trailing_used": False,
        "multiple_profit_targets_per_trade": False,
        "partial_take_profit_used": False,
        "breakeven_stop_used": False,
        "dynamic_time_stop_used": False,
        "note": (
            "E3 isolates single-threshold profit-taking with only the already-carried "
            "E2 time-stop fallback. Hard-stop/trailing/partials remain deferred."
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
            "tp_hit_count",
            "tp_hit_rate",
            "p10_exit_bps",
            "p25_exit_bps",
            "p50_exit_bps",
            "p75_exit_bps",
            "p90_exit_bps",
            "mean_exit_bps",
            "positive_rate",
            "exit_delay_p50_ms",
            "fallback_mark_age_p50_ms",
        ],
    },
    "selection_guardrails": [
        "Do not choose a TP by highest mean gross return alone.",
        "Primary judgment uses T15s fallback; T5s is a sensitivity check, not the main anchor.",
        "Prefer cross-regime agreement and neighboring-threshold plateau behavior.",
        "A single isolated TP spike is not promoted.",
        "Sparse/high TP levels may be retained as hypotheses but not promoted from small hit counts.",
        "No hard-stop or trailing interaction is allowed in E3.",
        "E3 may produce a profit-taking shortlist, not a live exit rule.",
        "No realistic-net profitability claim is allowed.",
    ],
}

def validate_design(d: Mapping[str, Any]) -> None:
    if d.get("design_id") != DESIGN_ID:
        raise ValueError("wrong E3 design id")
    if d.get("experiment_id") != EXPERIMENT_ID:
        raise ValueError("wrong experiment id")
    if d.get("status") != "LOCKED_BEFORE_E3_RESULTS":
        raise ValueError("E3 design must be locked before results")
    if tuple(d["profit_taking_family"]["take_profit_thresholds_bps"]) != TAKE_PROFIT_BPS:
        raise ValueError("TP grid changed")
    if tuple(d["profit_taking_family"]["fallback_time_stops_ms"]) != FALLBACK_TIME_STOP_MS:
        raise ValueError("fallback horizon set changed")
    if d["profit_taking_family"]["primary_fallback_ms"] != 15_000:
        raise ValueError("T15s must remain primary E3 fallback")
    if d["profit_taking_family"]["sensitivity_fallback_ms"] != 5_000:
        raise ValueError("T5s must remain sensitivity fallback")
    if d["preconditions"]["entry_A_B_C_D_fixed"] is not True:
        raise ValueError("entry parameters must remain fixed")
    iso = d["factor_isolation"]
    for key in (
        "hard_stop_used",
        "trailing_used",
        "multiple_profit_targets_per_trade",
        "partial_take_profit_used",
        "breakeven_stop_used",
        "dynamic_time_stop_used",
    ):
        if iso[key] is not False:
            raise ValueError(f"factor isolation violated: {key}")
    if d["reporting"]["profitability_claim_allowed"] is not False:
        raise ValueError("profitability claim forbidden")
