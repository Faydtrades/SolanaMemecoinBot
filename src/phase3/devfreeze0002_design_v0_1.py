from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

SCHEMA_VERSION = "P3-DEVFREEZE-DESIGN-0.1"
DESIGN_ID = "DEV-FREEZE-0002-DESIGN-001"
FREEZE_ID = "DEV-FREEZE-0002"

ENTRY_WINDOW_MS = 300_000
EXIT_PATH_HORIZON_MS = 300_000
REQUIRED_POST_T0_MS = ENTRY_WINDOW_MS + EXIT_PATH_HORIZON_MS

DESIGN = {
    "schema_version": SCHEMA_VERSION,
    "design_id": DESIGN_ID,
    "freeze_id": FREEZE_ID,
    "status": "LOCKED_BEFORE_DEV_FREEZE_0002_RESULTS",
    "dataset_role": "DEVELOPMENT_DISCOVERY",
    "untouched_oos": False,
    "source": "production Phase-1 BOT_TRUTH database",
    "purpose": "fresh continuous development data for exit-path characterization/readiness",
    "selection_semantics": {
        "clean_collector_segment_required": True,
        "explicit_gaps_allowed_inside_selected_segment": False,
        "uncertain_close_allowed_for_preview_only": True,
        "uncertain_close_allowed_for_immutable_freeze": False,
        "global_first_observed_tradable_must_be_inside_segment": True,
        "global_first_trade_may_be_gap_recovery": False,
        "t0_definition": "global first observed BOT_TRUTH BUY/SELL for the mint",
        "entry_window_ms": ENTRY_WINDOW_MS,
        "exit_path_horizon_ms": EXIT_PATH_HORIZON_MS,
        "required_tail_after_t0_ms": REQUIRED_POST_T0_MS,
        "eligibility_rule": (
            "t0 must fall inside the clean segment and t0+10m must be <= segment end; "
            "this guarantees room for a signal as late as t0+5m plus a 5m exit path"
        ),
        "future_activity_filter": False,
        "future_outcome_filter": False,
    },
    "entry_research_binding": {
        "source_selection": "EXP-0005_D_selection_v0_1.json",
        "A_B_C_D_parameters_fixed": True,
        "deferred_entry_hypotheses_in_mainline": False,
    },
    "readiness_binding": {
        "policy": "PHASE-3-CONTINUOUS-DEV-READINESS-001",
        "E1_min_clean_5m_per_robust_regime": 30,
        "E1_min_clean_rate": 0.80,
        "E2_min_clean_5m_per_robust_regime": 100,
        "E2_min_clean_rate": 0.90,
        "duration_alone_declares_ready": False,
    },
    "safety": {
        "preview_opens_production_db_read_only": True,
        "immutable_builder_requires_closed_non_uncertain_segment": True,
        "immutable_builder_refuses_overwrite": True,
        "production_db_rows_mutated": False,
        "exit_threshold_optimization_performed": False,
        "profitability_claim_allowed": False,
    },
}

def validate_design(d: Mapping[str, object]) -> None:
    if d.get("design_id") != DESIGN_ID:
        raise ValueError("wrong design id")
    if d.get("freeze_id") != FREEZE_ID:
        raise ValueError("wrong freeze id")
    if d.get("status") != "LOCKED_BEFORE_DEV_FREEZE_0002_RESULTS":
        raise ValueError("design is not pre-result locked")
    s = d["selection_semantics"]
    if s["required_tail_after_t0_ms"] != 600_000:
        raise ValueError("required tail must remain 10m")
    if s["future_activity_filter"] is not False or s["future_outcome_filter"] is not False:
        raise ValueError("future/outcome selection filters forbidden")
    if d["entry_research_binding"]["A_B_C_D_parameters_fixed"] is not True:
        raise ValueError("entry parameters must remain fixed")
    if d["readiness_binding"]["duration_alone_declares_ready"] is not False:
        raise ValueError("duration alone cannot declare readiness")
