from __future__ import annotations

from typing import Any, Mapping

SCHEMA_VERSION = "P3-E5-SMALL-INTERACTION-DESIGN-0.1"
DESIGN_ID = "PHASE-3-E5-SMALL-INTERACTION-001"
EXPERIMENT_ID = "EXP-0011"

# Small, explicitly enumerated shortlist. This is NOT a Cartesian search.
VARIANTS = (
    {
        "variant_id": "REF_T15",
        "fallback_ms": 15_000,
        "tp_bps": None,
        "trail_activation_bps": None,
        "trail_giveback_bps": None,
        "hard_stop_bps": None,
        "role": "REFERENCE",
    },
    {
        "variant_id": "E3_TP10_T15",
        "fallback_ms": 15_000,
        "tp_bps": 1000,
        "trail_activation_bps": None,
        "trail_giveback_bps": None,
        "hard_stop_bps": None,
        "role": "E3_PRIMARY_REFERENCE",
    },
    {
        "variant_id": "E4_ACT10_GB03_T15",
        "fallback_ms": 15_000,
        "tp_bps": None,
        "trail_activation_bps": 1000,
        "trail_giveback_bps": 300,
        "hard_stop_bps": None,
        "role": "E4_PRIMARY_REFERENCE",
    },
    {
        "variant_id": "TP10_ACT05_GB03_T15",
        "fallback_ms": 15_000,
        "tp_bps": 1000,
        "trail_activation_bps": 500,
        "trail_giveback_bps": 300,
        "hard_stop_bps": None,
        "role": "TP_TRAIL_INTERACTION_A",
    },
    {
        "variant_id": "TP15_ACT10_GB03_T15",
        "fallback_ms": 15_000,
        "tp_bps": 1500,
        "trail_activation_bps": 1000,
        "trail_giveback_bps": 300,
        "hard_stop_bps": None,
        "role": "TP_TRAIL_INTERACTION_B",
    },
    {
        "variant_id": "TP10_SL10_T15",
        "fallback_ms": 15_000,
        "tp_bps": 1000,
        "trail_activation_bps": None,
        "trail_giveback_bps": None,
        "hard_stop_bps": -1000,
        "role": "TP_RISK_CONTROL_INTERACTION",
    },
    {
        "variant_id": "ACT10_GB03_SL10_T15",
        "fallback_ms": 15_000,
        "tp_bps": None,
        "trail_activation_bps": 1000,
        "trail_giveback_bps": 300,
        "hard_stop_bps": -1000,
        "role": "TRAIL_RISK_CONTROL_INTERACTION",
    },
    {
        "variant_id": "TP10_ACT05_GB03_SL10_T15",
        "fallback_ms": 15_000,
        "tp_bps": 1000,
        "trail_activation_bps": 500,
        "trail_giveback_bps": 300,
        "hard_stop_bps": -1000,
        "role": "THREE_COMPONENT_INTERACTION",
    },
    {
        "variant_id": "SENS_TP20_T5",
        "fallback_ms": 5_000,
        "tp_bps": 2000,
        "trail_activation_bps": None,
        "trail_giveback_bps": None,
        "hard_stop_bps": None,
        "role": "T5_SENSITIVITY_REFERENCE",
    },
)

DESIGN = {
    "schema_version": SCHEMA_VERSION,
    "design_id": DESIGN_ID,
    "experiment_id": EXPERIMENT_ID,
    "status": "LOCKED_BEFORE_E5_RESULTS",
    "dataset": {
        "freeze_id": "DEV-FREEZE-0002",
        "dataset_role": "DEVELOPMENT_DISCOVERY",
        "untouched_oos": False,
    },
    "preconditions": {
        "EXP-0009_E3_selection": "LOCKED",
        "EXP-0010_E4_selection": "LOCKED",
        "entry_A_B_C_D_fixed": True,
    },
    "locked_upstream": {
        "E3_primary": "TP10_T15s",
        "E3_neighbors": ["TP05_T15s", "TP15_T15s"],
        "E3_sensitivity": "TP20_T5s",
        "E4_primary": "ACT10_GB03",
        "E4_neighbors": ["ACT05_GB03", "ACT10_GB05"],
        "E2_risk_control_hypothesis": "SL10_ONLY_AS_INTERACTION_HYPOTHESIS",
    },
    "shortlist": {
        "variant_count": len(VARIANTS),
        "variants": list(VARIANTS),
        "not_cartesian": True,
        "reason": (
            "E5 tests only a small hand-enumerated set derived from locked E3/E4 "
            "carry-forward plus the deferred SL10 risk-control hypothesis. It does "
            "not reopen the broad exit search."
        ),
    },
    "execution_semantics": {
        "observed_points_only": True,
        "interpolation": False,
        "per_point_order": [
            "hard_stop_if_enabled",
            "update_or_activate_trailing_if_enabled",
            "trailing_exit_if_active_and_crossed",
            "fixed_take_profit_if_enabled",
            "fallback_at_horizon",
        ],
        "hard_stop": (
            "Exit at the first observed fresh same-identity point <= hard-stop "
            "threshold at or before the fallback horizon."
        ),
        "trailing": (
            "Activation and running peak use observed fresh same-identity points. "
            "Trail level is observed running peak minus fixed giveback."
        ),
        "fixed_tp": (
            "Exit at the first observed fresh same-identity point >= fixed TP "
            "threshold at or before the fallback horizon."
        ),
        "simultaneous_condition_note": (
            "All decisions are made on the same observed point. Hard stop has first "
            "precedence, then an already-active trail exit, then fixed TP. A newly "
            "activated trail cannot exit on the same point unless a later observed "
            "point crosses its ratcheted trail."
        ),
        "fallback": (
            "If no enabled exit condition fires, use the latest fresh same-identity "
            "observation at or before the variant fallback horizon; expose mark age."
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
            "scored_count",
            "unscored_count",
            "exit_reason_counts",
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
        "No E5 winner by gross mean alone.",
        "Primary judgment is cross-regime stability across ROBUST_1/2/3.",
        "Compare p10, median, positive rate, and exit-reason mix alongside mean.",
        "A complex interaction must materially improve on simpler locked references to survive.",
        "If complexity adds little, prefer the simpler E3 or E4 rule.",
        "T5 sensitivity remains separate and may not replace mainline solely on gross mean.",
        "SL10 may survive only if it materially improves downside without destroying central tendency.",
        "E5 yields a tiny final Phase-3 candidate set, not a live trading rule.",
        "No realistic-net profitability claim is allowed.",
    ],
}


def validate_design(d: Mapping[str, Any]) -> None:
    if d.get("design_id") != DESIGN_ID:
        raise ValueError("wrong E5 design id")
    if d.get("experiment_id") != EXPERIMENT_ID:
        raise ValueError("wrong experiment id")
    if d.get("status") != "LOCKED_BEFORE_E5_RESULTS":
        raise ValueError("E5 design must be locked before results")
    if d["preconditions"]["entry_A_B_C_D_fixed"] is not True:
        raise ValueError("entry parameters changed")
    if d["shortlist"]["variant_count"] != 9:
        raise ValueError("expected exactly 9 E5 variants")
    if d["shortlist"]["not_cartesian"] is not True:
        raise ValueError("E5 must remain non-Cartesian")
    ids = [v["variant_id"] for v in d["shortlist"]["variants"]]
    expected = [v["variant_id"] for v in VARIANTS]
    if ids != expected or len(set(ids)) != 9:
        raise ValueError("E5 shortlist changed")
    if d["reporting"]["profitability_claim_allowed"] is not False:
        raise ValueError("profitability claim forbidden")
