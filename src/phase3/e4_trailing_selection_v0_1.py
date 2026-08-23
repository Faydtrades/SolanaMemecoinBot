from __future__ import annotations

from typing import Any, Mapping

SCHEMA_VERSION = "P3-E4-TRAILING-SELECTION-0.1"
SELECTION_ID = "EXP-0010-E4-SELECTION-001"
EXPERIMENT_ID = "EXP-0010"

SELECTION = {
    "schema_version": SCHEMA_VERSION,
    "selection_id": SELECTION_ID,
    "experiment_id": EXPERIMENT_ID,
    "status": "POSTRUN_RESEARCH_CARRY_FORWARD",
    "dataset": {
        "freeze_id": "DEV-FREEZE-0002",
        "dataset_role": "DEVELOPMENT_DISCOVERY",
        "untouched_oos": False,
    },
    "mainline": {
        "primary_anchor": {
            "variant_id": "ACT10_GB03",
            "activation_threshold_bps": 1000,
            "giveback_bps": 300,
            "fallback_time_stop_ms": 15000,
            "reason": (
                "Most consistent mean-ranking across ROBUST_1/2/3, strong median "
                "returns, meaningful activation/trail-exit rates, and stable downside "
                "without depending on a single robust regime."
            ),
        },
        "lower_activation_neighbor": {
            "variant_id": "ACT05_GB03",
            "activation_threshold_bps": 500,
            "giveback_bps": 300,
            "fallback_time_stop_ms": 15000,
            "reason": (
                "Neighboring lower activation retains high positive rate and the "
                "highest trail-exit participation; useful for E5 interaction stability."
            ),
        },
        "wider_giveback_neighbor": {
            "variant_id": "ACT10_GB05",
            "activation_threshold_bps": 1000,
            "giveback_bps": 500,
            "fallback_time_stop_ms": 15000,
            "reason": (
                "Neighboring wider giveback remains strong across all three robust "
                "regimes and tests whether the ACT10/GB03 result is a narrow spike."
            ),
        },
    },
    "plateau_context": {
        "supported_region": (
            "Activation +5% to +10% with giveback 3% to 5% forms the strongest "
            "coarse trailing neighborhood. +15% activation and 10% giveback are "
            "generally weaker and less active."
        ),
        "ACT05_GB05": "Retained as supporting plateau evidence but not a mainline slot.",
    },
    "comparison_to_E3": {
        "E3_primary_anchor": "TP10_T15s",
        "interpretation": (
            "Trailing variants increase gross-reference mean relative to TP10/T15s "
            "but generally reduce the median exit. This indicates more runner/tail "
            "capture, not an unambiguous replacement for fixed TP."
        ),
        "E3_primary_remains_locked": True,
        "E4_does_not_replace_E3_before_E5": True,
    },
    "not_promoted": {
        "ACT15_family": (
            "Not promoted: lower activation participation and generally weaker "
            "cross-regime results."
        ),
        "GB10_family": (
            "Not promoted: wider giveback generally lowers positive rate/median and "
            "reduces the evidence that trailing materially helps."
        ),
    },
    "guardrails": {
        "entry_A_B_C_D_fixed": True,
        "E3_selection_unchanged": True,
        "hard_stop_added": False,
        "fixed_tp_combined_with_trailing": False,
        "T5s_sensitivity_added": False,
        "partial_or_multi_tp_added": False,
        "profitability_claim_allowed": False,
        "note": (
            "This is development-data research carry-forward only. E5 will test a "
            "small prelocked interaction shortlist; no live rule is selected here."
        ),
    },
}


def validate_selection(s: Mapping[str, Any]) -> None:
    if s.get("selection_id") != SELECTION_ID:
        raise ValueError("wrong selection id")
    if s.get("experiment_id") != EXPERIMENT_ID:
        raise ValueError("wrong experiment id")
    ids = [
        s["mainline"]["lower_activation_neighbor"]["variant_id"],
        s["mainline"]["primary_anchor"]["variant_id"],
        s["mainline"]["wider_giveback_neighbor"]["variant_id"],
    ]
    if ids != ["ACT05_GB03", "ACT10_GB03", "ACT10_GB05"]:
        raise ValueError("E4 mainline neighborhood changed")
    g = s["guardrails"]
    if g["entry_A_B_C_D_fixed"] is not True or g["E3_selection_unchanged"] is not True:
        raise ValueError("locked upstream selections changed")
    for key in (
        "hard_stop_added",
        "fixed_tp_combined_with_trailing",
        "T5s_sensitivity_added",
        "partial_or_multi_tp_added",
    ):
        if g[key] is not False:
            raise ValueError(f"forbidden interaction in E4 selection: {key}")
    if g["profitability_claim_allowed"] is not False:
        raise ValueError("profitability claim forbidden")
