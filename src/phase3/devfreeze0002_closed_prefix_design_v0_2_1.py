from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Sequence

SCHEMA_VERSION = "P3-DEVFREEZE-CLOSED-PREFIX-DESIGN-0.2.1"
DESIGN_ID = "DEV-FREEZE-0002-DESIGN-002A"
SUPERSEDES_DESIGN_ID = "DEV-FREEZE-0002-DESIGN-002"
FREEZE_ID = "DEV-FREEZE-0002"

ENTRY_WINDOW_MS = 300_000
EXIT_HORIZON_MS = 300_000
REQUIRED_POST_T0_MS = ENTRY_WINDOW_MS + EXIT_HORIZON_MS

DESIGN = {
    "schema_version": SCHEMA_VERSION,
    "design_id": DESIGN_ID,
    "supersedes_design_id": SUPERSEDES_DESIGN_ID,
    "freeze_id": FREEZE_ID,
    "status": "LOCKED_AFTER_INTERRUPTED_SHUTDOWN_BEFORE_FORWARD_OUTCOME_INSPECTION",
    "dataset_role": "DEVELOPMENT_DISCOVERY",
    "untouched_oos": False,
    "amendment_reason": (
        "The live collector was interrupted during background queue drain, leaving "
        "the final collector interval EOF_UNCERTAIN. Preserve only the maximal "
        "definitely closed prefix of the already-observed session and discard the "
        "entire uncertain tail."
    ),
    "information_seen_before_amendment": {
        "coverage_metadata": True,
        "locked_entry_candidate_counts": True,
        "forward_returns": False,
        "exit_paths": False,
        "mfe_mae": False,
        "pnl": False,
    },
    "prefix_rule": {
        "source_session": "latest reconstructed collector session",
        "required_source_state": "EOF_UNCERTAIN",
        "cutoff": "maximum end_us among non-uncertain reconstructed active intervals",
        "uncertain_tail_included": False,
        "events_after_cutoff_included": False,
        "explicit_gaps_before_cutoff_retained": True,
        "explicit_gaps_ignored": False,
        "pending_confirmation_jobs_gate_freeze": False,
        "pending_deep_jobs_gate_freeze": False,
        "pending_gap_jobs_gate_freeze": False,
        "why_background_queues_do_not_gate": (
            "The research clock is observed_at/BOT_TRUTH. Confirmation/deep metadata "
            "are not FirstPullback decision inputs. GAP_RECOVERY is not fresh market "
            "flow, while explicit gap intervals remain non-observable. Eligible tokens "
            "must have their full t0->t0+10m window inside one definitely clean interval."
        ),
    },
    "cohort_rule": {
        "global_first_normalizable_tradable_must_be_inside_prefix": True,
        "global_t0_may_be_gap_recovery": False,
        "required_tail_after_t0_ms": REQUIRED_POST_T0_MS,
        "entire_t0_to_t0_plus_10m_inside_one_definite_clean_interval": True,
        "future_activity_filter": False,
        "future_outcome_filter": False,
        "strategy_signal_used_for_cohort_selection": False,
    },
    "entry_binding": {
        "source": "EXP-0005_D_selection_v0_1.json",
        "A_B_C_D_fixed": True,
        "entry_counts_may_be_previewed": True,
        "forward_outcomes_may_not_be_previewed": True,
    },
    "readiness_binding": {
        "policy_id": "PHASE-3-CONTINUOUS-DEV-READINESS-001",
        "E1_min_clean_5m_per_robust_regime": 30,
        "E1_min_clean_rate": 0.80,
        "E2_min_clean_5m_per_robust_regime": 100,
        "E2_min_clean_rate": 0.90,
    },
}

@dataclass(frozen=True, slots=True)
class Interval:
    session_id: int
    start_us: int
    end_us: int
    uncertain_close: bool

def maximal_definite_prefix_cutoff(intervals: Sequence[Interval]) -> int:
    definite = [i.end_us for i in intervals if not i.uncertain_close]
    if not definite:
        raise ValueError("no definitely closed interval exists")
    return max(definite)

def eligible_interval(
    *,
    t0_us: int,
    required_post_t0_ms: int,
    intervals: Sequence[Interval],
    cutoff_us: int,
) -> Interval | None:
    tail_us = t0_us + required_post_t0_ms * 1000
    if tail_us > cutoff_us:
        return None
    for i in intervals:
        if i.uncertain_close:
            continue
        if i.end_us > cutoff_us:
            continue
        if i.start_us <= t0_us and tail_us <= i.end_us:
            return i
    return None

def validate_design(d: Mapping[str, object]) -> None:
    if d.get("design_id") != DESIGN_ID:
        raise ValueError("wrong design id")
    if d.get("supersedes_design_id") != SUPERSEDES_DESIGN_ID:
        raise ValueError("superseded design missing")
    info = d["information_seen_before_amendment"]
    if info["forward_returns"] or info["exit_paths"] or info["mfe_mae"] or info["pnl"]:
        raise ValueError("closed-prefix amendment cannot be outcome-informed")
    p = d["prefix_rule"]
    if p["uncertain_tail_included"] is not False:
        raise ValueError("uncertain tail must be excluded")
    if p["explicit_gaps_ignored"] is not False:
        raise ValueError("explicit gaps may not be ignored")
    c = d["cohort_rule"]
    if c["required_tail_after_t0_ms"] != 600_000:
        raise ValueError("10m guard changed")
    if c["future_activity_filter"] is not False or c["future_outcome_filter"] is not False:
        raise ValueError("future filters forbidden")
    if d["entry_binding"]["A_B_C_D_fixed"] is not True:
        raise ValueError("entry parameters changed")
