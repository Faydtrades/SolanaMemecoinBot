from __future__ import annotations

from typing import Any, Mapping

SCHEMA_VERSION = "P3-E5-FINAL-CANDIDATE-SELECTION-0.1"
SELECTION_ID = "EXP-0011-E5-FINAL-CANDIDATES-001"
EXPERIMENT_ID = "EXP-0011"

SELECTION = {
    "schema_version": SCHEMA_VERSION,
    "selection_id": SELECTION_ID,
    "experiment_id": EXPERIMENT_ID,
    "status": "POSTRUN_FINAL_PHASE3_CANDIDATE_SET",
    "dataset": {
        "freeze_id": "DEV-FREEZE-0002",
        "dataset_role": "DEVELOPMENT_DISCOVERY",
        "untouched_oos": False,
    },
    "reference": {
        "variant_id": "REF_T15",
        "reason": "Keep pure T15 as a benchmark only; not a final strategy candidate.",
    },
    "final_mainline_candidates": [
        {
            "candidate_id": "FINAL-A",
            "variant_id": "E3_TP10_T15",
            "type": "FIXED_TP",
            "tp_bps": 1000,
            "fallback_ms": 15000,
            "reason": (
                "Simplest strong exit rule. Across all robust regimes it preserves "
                "the strongest central tendency/median profile with high scored "
                "coverage and ~70% positive exits."
            ),
        },
        {
            "candidate_id": "FINAL-B",
            "variant_id": "E4_ACT10_GB03_T15",
            "type": "TRAILING",
            "activation_bps": 1000,
            "giveback_bps": 300,
            "fallback_ms": 15000,
            "reason": (
                "Highest robust weighted gross-reference mean among the mainline "
                "E5 references, with similar positive-rate and strong cross-regime "
                "consistency, but more tail/runner dependence than FINAL-A."
            ),
        },
    ],
    "sensitivity_only": {
        "candidate_id": "SENS-C",
        "variant_id": "SENS_TP20_T5",
        "type": "EARLY_FIXED_TP",
        "tp_bps": 2000,
        "fallback_ms": 5000,
        "reason": (
            "Strong early sensitivity result with best positive-rate/downside "
            "profile in E5, but only ~89.6% scored coverage and much greater "
            "execution/latency sensitivity. It must remain separate from mainline."
        ),
    },
    "not_promoted": {
        "TP10_ACT05_GB03_T15": (
            "Does not materially beat simpler E3/E4 references; complexity not earned."
        ),
        "TP15_ACT10_GB03_T15": (
            "Intermediate compromise, but does not clearly dominate either simpler "
            "E3 or E4 reference."
        ),
        "TP10_SL10_T15": (
            "SL10 reduces positive rate/central tendency without a sufficiently "
            "consistent downside improvement."
        ),
        "ACT10_GB03_SL10_T15": (
            "Mean remains high but downside/positive-rate profile is worse than "
            "simpler E4 trailing; SL10 not earned."
        ),
        "TP10_ACT05_GB03_SL10_T15": (
            "Three-component complexity does not earn its place."
        ),
        "standalone_or_mainline_SL10": "REJECTED_FOR_MAINLINE_AT_THIS_STAGE",
    },
    "decision": {
        "single_dev_data_winner_selected": False,
        "reason": (
            "DEV-FREEZE-0002 is development/discovery data. E5 does not provide a "
            "clean dominance relation between simple TP10/T15 and ACT10/GB3/T15: "
            "fixed TP has stronger medians, trailing has higher gross-reference mean. "
            "Both advance to deterministic final validation and then fresh real-time "
            "paper evaluation."
        ),
        "paper_phase_intent": (
            "Where practical, FINAL-A and FINAL-B should be evaluated in parallel "
            "from the same live CandidateSignals during Phase 4. SENS-C remains a "
            "separately labeled sensitivity track."
        ),
    },
    "guardrails": {
        "entry_A_B_C_D_fixed": True,
        "no_further_exit_parameter_tuning_before_final_validation": True,
        "no_further_DEV_FREEZE_0002_winner_search": True,
        "fees_modeled_here": False,
        "slippage_modeled_here": False,
        "latency_modeled_here": False,
        "profitability_claim_allowed": False,
        "live_rule_selected": False,
    },
}


def validate_selection(s: Mapping[str, Any]) -> None:
    if s.get("selection_id") != SELECTION_ID:
        raise ValueError("wrong selection id")
    if s.get("experiment_id") != EXPERIMENT_ID:
        raise ValueError("wrong experiment id")
    main = [x["variant_id"] for x in s["final_mainline_candidates"]]
    if main != ["E3_TP10_T15", "E4_ACT10_GB03_T15"]:
        raise ValueError("final mainline candidates changed")
    if s["sensitivity_only"]["variant_id"] != "SENS_TP20_T5":
        raise ValueError("sensitivity candidate changed")
    if s["decision"]["single_dev_data_winner_selected"] is not False:
        raise ValueError("must not select single dev-data winner")
    g = s["guardrails"]
    if g["entry_A_B_C_D_fixed"] is not True:
        raise ValueError("entry parameters changed")
    if g["no_further_exit_parameter_tuning_before_final_validation"] is not True:
        raise ValueError("further tuning incorrectly allowed")
    if g["no_further_DEV_FREEZE_0002_winner_search"] is not True:
        raise ValueError("dev-data winner search incorrectly reopened")
    if g["profitability_claim_allowed"] is not False:
        raise ValueError("profitability claim forbidden")
    if g["live_rule_selected"] is not False:
        raise ValueError("live rule must not be selected here")
