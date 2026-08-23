from __future__ import annotations

from typing import Any, Mapping

SCHEMA_VERSION = "P3-E3-PROFIT-TAKING-SELECTION-0.1"
SELECTION_ID = "EXP-0009-E3-SELECTION-001"
EXPERIMENT_ID = "EXP-0009"

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
            "variant_id": "TP10_T15s",
            "tp_threshold_bps": 1000,
            "fallback_time_stop_ms": 15000,
            "reason": (
                "Best balanced T15s result across ROBUST_1/2/3: strong cross-regime "
                "medians, materially improved positive rate and downside versus pure "
                "T15s, and TP hit rate near one-half of clean paths."
            ),
        },
        "lower_neighbor": {
            "variant_id": "TP05_T15s",
            "tp_threshold_bps": 500,
            "fallback_time_stop_ms": 15000,
            "reason": (
                "Conservative neighboring threshold with high positive rate and "
                "high TP hit rate; retained to test neighborhood stability."
            ),
        },
        "upper_neighbor": {
            "variant_id": "TP15_T15s",
            "tp_threshold_bps": 1500,
            "fallback_time_stop_ms": 15000,
            "reason": (
                "Upper neighboring threshold remains positive across all robust "
                "regimes and preserves the predeclared plateau check around TP10."
            ),
        },
    },
    "sensitivity_only": {
        "variant_id": "TP20_T5s",
        "tp_threshold_bps": 2000,
        "fallback_time_stop_ms": 5000,
        "reason": (
            "Central high-mean T5s sensitivity region, but T5s scoring coverage is "
            "materially lower than T15s; must remain separate from mainline."
        ),
    },
    "deferred_not_rejected": [
        {
            "variant_id": "TP20_T15s",
            "reason": "Higher gross mean but lower median/positive rate and lower TP hit rate than TP10."
        },
        {
            "variant_id": "TP30_T15s",
            "reason": "Highest T15s gross mean, but only about one-fifth of clean paths hit TP; more tail-dependent."
        },
        {
            "variant_id": "TP30_T5s",
            "reason": "High T5s gross mean with very low TP hit rate and lower scoring coverage."
        },
        {
            "variant_id": "TP03_T15s",
            "reason": "Very high positive rate and TP hit rate, but meaningfully lower gross mean; conservative boundary hypothesis."
        },
    ],
    "not_promoted": {
        "hard_stop": "No hard-stop mainline winner from EXP-0008.",
        "historical_SL30": "Not promoted.",
        "T30s_or_longer_time_stop": "Not promoted to mainline.",
    },
    "guardrails": {
        "entry_A_B_C_D_fixed": True,
        "hard_stop_added": False,
        "trailing_added": False,
        "partial_or_multi_tp_added": False,
        "profitability_claim_allowed": False,
        "note": (
            "This is a development-data research carry-forward, not a live rule. "
            "E4 trailing remains isolated; E5 handles a small interaction shortlist."
        ),
    },
}

def validate_selection(s: Mapping[str, Any]) -> None:
    if s.get("selection_id") != SELECTION_ID:
        raise ValueError("wrong selection id")
    if s.get("experiment_id") != EXPERIMENT_ID:
        raise ValueError("wrong experiment id")
    ids = [
        s["mainline"]["lower_neighbor"]["variant_id"],
        s["mainline"]["primary_anchor"]["variant_id"],
        s["mainline"]["upper_neighbor"]["variant_id"],
    ]
    if ids != ["TP05_T15s", "TP10_T15s", "TP15_T15s"]:
        raise ValueError("mainline TP neighborhood changed")
    if s["sensitivity_only"]["variant_id"] != "TP20_T5s":
        raise ValueError("T5s sensitivity changed")
    g = s["guardrails"]
    if g["entry_A_B_C_D_fixed"] is not True:
        raise ValueError("entry parameters changed")
    for key in ("hard_stop_added", "trailing_added", "partial_or_multi_tp_added"):
        if g[key] is not False:
            raise ValueError(f"forbidden interaction in E3 selection: {key}")
    if g["profitability_claim_allowed"] is not False:
        raise ValueError("profitability claim forbidden")
