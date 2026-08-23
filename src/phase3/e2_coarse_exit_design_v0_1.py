from __future__ import annotations

from typing import Any, Mapping

SCHEMA_VERSION = "P3-E2-COARSE-EXIT-DESIGN-0.1"
DESIGN_ID = "PHASE-3-E2-COARSE-EXIT-001"
EXPERIMENT_ID = "EXP-0008"

ROBUST_ROLES = ("ROBUST_1", "ROBUST_2", "ROBUST_3")
ALL_ROLES = ("CONTROL",) + ROBUST_ROLES

# Coarse hard-stop grid. -30% historical manual rule is included as a hypothesis,
# not a default. Range spans materially tighter and looser than E1 median troughs.
HARD_STOP_BPS = (-1000, -2000, -3000, -4000, -5000, -6000)

# Reuse the already-established fixed OutcomeReplay horizons. No new timing
# assumption is introduced in E2.
TIME_STOP_MS = (5_000, 15_000, 30_000, 60_000, 120_000, 300_000)

DESIGN = {
    "schema_version": SCHEMA_VERSION,
    "design_id": DESIGN_ID,
    "experiment_id": EXPERIMENT_ID,
    "status": "LOCKED_BEFORE_E2_RESULTS",
    "dataset": {
        "freeze_id": "DEV-FREEZE-0002",
        "dataset_role": "DEVELOPMENT_DISCOVERY",
        "untouched_oos": False,
        "closed_prefix_design": "DEV-FREEZE-0002-DESIGN-002A",
    },
    "preconditions": {
        "postfreeze_audit_result": "PASS",
        "readiness_tier": "E2_COARSE_EXIT_SEARCH_READY",
        "e1_path_characterization_result": "PASS",
        "entry_A_B_C_D_fixed": True,
    },
    "e1_supporting_observations": {
        "ROBUST_1": {"clean_5m": 203, "peak_p50_bps": 3159, "trough_p50_bps": -3388},
        "ROBUST_2": {"clean_5m": 229, "peak_p50_bps": 3135, "trough_p50_bps": -3435},
        "ROBUST_3": {"clean_5m": 211, "peak_p50_bps": 3494, "trough_p50_bps": -4107},
        "interpretation": (
            "E1 shows large positive and negative observed excursions. These are marginal "
            "path distributions, not executable trade returns and not a reason to promote "
            "any one exit threshold."
        ),
    },
    "families": {
        "hard_stop": {
            "thresholds_bps": list(HARD_STOP_BPS),
            "terminal_if_no_stop": "latest fresh same-identity mark at or before 5m",
            "crossing_semantics": "first observed point <= threshold; no interpolation",
            "reason_for_range": (
                "Coarse 10%-step range from -10% to -60% brackets the E1 median adverse "
                "excursions (~-34% to -41%) and includes the historical -30% hypothesis "
                "without privileging it."
            ),
        },
        "time_stop": {
            "horizons_ms": list(TIME_STOP_MS),
            "mark_semantics": (
                "latest fresh same-identity observation at or before the time-stop horizon; "
                "mark_age_ms exposed; no hidden freshness threshold"
            ),
            "reason_for_range": (
                "Reuse the fixed OutcomeReplay horizons 5s/15s/30s/60s/2m/5m, avoiding "
                "a new timing grid chosen from E2 outcomes."
            ),
        },
    },
    "factor_isolation": {
        "hard_stop_family_has_profit_target": False,
        "hard_stop_family_has_trailing": False,
        "time_stop_family_has_hard_stop": False,
        "time_stop_family_has_profit_target": False,
        "time_stop_family_has_trailing": False,
        "hard_stop_time_stop_cartesian_search": False,
        "note": (
            "E2 studies coarse hard-stop and time-stop families separately. Component "
            "interactions are deferred; no giant exit Cartesian search."
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
            "p10_exit_bps",
            "p25_exit_bps",
            "p50_exit_bps",
            "p75_exit_bps",
            "p90_exit_bps",
            "mean_exit_bps",
            "positive_rate",
            "exit_delay_p50_ms",
            "mark_age_p50_ms",
            "stop_hit_rate_for_hard_stop",
        ],
    },
    "selection_guardrails": [
        "Do not choose a winner by highest median or mean gross return alone.",
        "Prefer broad cross-regime stability to isolated spikes.",
        "Neighboring coarse thresholds/horizons must be reviewed for plateau behavior.",
        "Sparse or low-scored variants are not promoted.",
        "E2 may create a research shortlist, not a live exit rule.",
        "Profit-taking and trailing remain untouched until E3/E4.",
        "No realistic-net profitability claim is allowed.",
    ],
}

def validate_design(d: Mapping[str, Any]) -> None:
    if d.get("design_id") != DESIGN_ID:
        raise ValueError("wrong E2 design id")
    if d.get("status") != "LOCKED_BEFORE_E2_RESULTS":
        raise ValueError("E2 design must be locked before results")
    if tuple(d["families"]["hard_stop"]["thresholds_bps"]) != HARD_STOP_BPS:
        raise ValueError("hard-stop grid changed")
    if tuple(d["families"]["time_stop"]["horizons_ms"]) != TIME_STOP_MS:
        raise ValueError("time-stop grid changed")
    if d["preconditions"]["entry_A_B_C_D_fixed"] is not True:
        raise ValueError("entry parameters must remain fixed")
    iso = d["factor_isolation"]
    for key in (
        "hard_stop_family_has_profit_target",
        "hard_stop_family_has_trailing",
        "time_stop_family_has_hard_stop",
        "time_stop_family_has_profit_target",
        "time_stop_family_has_trailing",
        "hard_stop_time_stop_cartesian_search",
    ):
        if iso[key] is not False:
            raise ValueError(f"factor isolation violated: {key}")
    if d["reporting"]["profitability_claim_allowed"] is not False:
        raise ValueError("profitability claim forbidden")
