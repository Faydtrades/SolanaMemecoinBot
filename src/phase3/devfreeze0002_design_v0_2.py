from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping, Sequence

SCHEMA_VERSION = "P3-DEVFREEZE-DESIGN-0.2"
DESIGN_ID = "DEV-FREEZE-0002-DESIGN-002"
SUPERSEDES_DESIGN_ID = "DEV-FREEZE-0002-DESIGN-001"
FREEZE_ID = "DEV-FREEZE-0002"

ENTRY_WINDOW_MS = 300_000
EXIT_PATH_HORIZON_MS = 300_000
REQUIRED_POST_T0_MS = ENTRY_WINDOW_MS + EXIT_PATH_HORIZON_MS

DESIGN = {
    "schema_version": SCHEMA_VERSION,
    "design_id": DESIGN_ID,
    "supersedes_design_id": SUPERSEDES_DESIGN_ID,
    "freeze_id": FREEZE_ID,
    "status": "LOCKED_AFTER_COVERAGE_DIAGNOSTIC_BEFORE_STRATEGY_OUTCOME_RESULTS",
    "dataset_role": "DEVELOPMENT_DISCOVERY",
    "untouched_oos": False,
    "amendment_reason": (
        "Coverage diagnostic showed the live collector session is ~99.75% active "
        "but contains brief explicit WS reconnect gaps. Requiring one globally gap-free "
        "session segment would discard valid observations unrelated to those gaps."
    ),
    "what_was_observed_before_amendment": {
        "coverage_metadata_only": True,
        "strategy_candidate_outcomes_seen": False,
        "exit_outcomes_seen": False,
        "pnl_seen": False,
    },
    "selection_semantics": {
        "selected_unit": "ONE CLOSED COLLECTOR SESSION",
        "explicit_gaps_allowed_in_session": True,
        "explicit_gaps_ignored": False,
        "uncertain_close_allowed_for_preview_only": True,
        "uncertain_close_allowed_for_immutable_freeze": False,
        "global_first_observed_tradable_must_be_inside_session": True,
        "global_first_trade_may_be_gap_recovery": False,
        "t0_definition": "global first observed BOT_TRUTH BUY/SELL for the mint",
        "entry_window_ms": ENTRY_WINDOW_MS,
        "exit_path_horizon_ms": EXIT_PATH_HORIZON_MS,
        "required_tail_after_t0_ms": REQUIRED_POST_T0_MS,
        "eligibility_rule": (
            "A token is eligible only if its global t0 and the entire closed interval "
            "[t0, t0+10m] lie inside one clean active collector interval in the selected "
            "session. Therefore any explicit WS gap touching that 10m window excludes "
            "that token, while gaps elsewhere in the session do not discard unrelated data."
        ),
        "future_activity_filter": False,
        "future_outcome_filter": False,
        "strategy_signal_used_for_cohort_selection": False,
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
        "immutable_builder_requires_closed_non_uncertain_session": True,
        "immutable_builder_refuses_overwrite": True,
        "production_db_rows_mutated": False,
        "exit_threshold_optimization_performed": False,
        "profitability_claim_allowed": False,
    },
}

@dataclass(frozen=True, slots=True)
class CleanInterval:
    session_id: int
    start_us: int
    end_us: int
    uncertain_close: bool = False

def window_fits_clean_interval(
    *,
    t0_us: int,
    required_post_t0_ms: int,
    interval: CleanInterval,
) -> bool:
    end_us = t0_us + required_post_t0_ms * 1000
    return interval.start_us <= t0_us and end_us <= interval.end_us

def eligible_interval_for_t0(
    *,
    t0_us: int,
    required_post_t0_ms: int,
    intervals: Sequence[CleanInterval],
) -> CleanInterval | None:
    for interval in intervals:
        if window_fits_clean_interval(
            t0_us=t0_us,
            required_post_t0_ms=required_post_t0_ms,
            interval=interval,
        ):
            return interval
    return None

def validate_design(d: Mapping[str, object]) -> None:
    if d.get("design_id") != DESIGN_ID:
        raise ValueError("wrong design id")
    if d.get("supersedes_design_id") != SUPERSEDES_DESIGN_ID:
        raise ValueError("superseded design not recorded")
    if d.get("freeze_id") != FREEZE_ID:
        raise ValueError("wrong freeze id")
    if d.get("status") != "LOCKED_AFTER_COVERAGE_DIAGNOSTIC_BEFORE_STRATEGY_OUTCOME_RESULTS":
        raise ValueError("coverage amendment status incorrect")
    observed = d["what_was_observed_before_amendment"]
    if observed["strategy_candidate_outcomes_seen"] is not False:
        raise ValueError("strategy outcomes must not have been used")
    if observed["exit_outcomes_seen"] is not False or observed["pnl_seen"] is not False:
        raise ValueError("exit/PnL results must not have been used")
    s = d["selection_semantics"]
    if s["required_tail_after_t0_ms"] != 600_000:
        raise ValueError("required tail must remain 10m")
    if s["explicit_gaps_allowed_in_session"] is not True:
        raise ValueError("session-level explicit gaps must be representable")
    if s["explicit_gaps_ignored"] is not False:
        raise ValueError("gaps may never be ignored")
    if s["future_activity_filter"] is not False or s["future_outcome_filter"] is not False:
        raise ValueError("future/outcome selection filters forbidden")
    if s["strategy_signal_used_for_cohort_selection"] is not False:
        raise ValueError("strategy signals cannot define freeze cohort")
    if d["entry_research_binding"]["A_B_C_D_parameters_fixed"] is not True:
        raise ValueError("entry parameters must remain fixed")
    if d["readiness_binding"]["duration_alone_declares_ready"] is not False:
        raise ValueError("duration alone cannot declare readiness")
