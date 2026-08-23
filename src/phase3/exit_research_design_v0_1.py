from __future__ import annotations

from typing import Any, Mapping

DESIGN_ID = "PHASE-3-EXIT-DESIGN-001"
SCHEMA_VERSION = "P3-EXIT-DESIGN-0.1"
EXPERIMENT_ID = "EXP-0006"

HORIZONS = ("5s", "15s", "30s", "60s", "2m", "5m")
ROBUST_ROLES = ("ROBUST_1", "ROBUST_2", "ROBUST_3")
ALL_ROLES = ("CONTROL",) + ROBUST_ROLES

def build_design(selection_sha256: str) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "design_id": DESIGN_ID,
        "experiment_id": EXPERIMENT_ID,
        "status": "LOCKED_BEFORE_EXIT_OPTIMIZATION_RESULTS",
        "source_entry_selection": {
            "experiment_id": "EXP-0005",
            "selection_sha256": selection_sha256,
            "full_entry_regime_count": 4,
        },
        "purpose": (
            "Design exit research and establish whether the existing development freeze "
            "has sufficient causal post-entry path observability before any SL/TP/trailing/time-stop tuning."
        ),
        "locked_principles": [
            "Entry A/B/C/D parameters remain fixed during exit research.",
            "Do not jointly re-optimize entry and exit parameters on DEV-FREEZE-0001.",
            "CandidateSignal/reference price is a research entry reference, not an execution fill.",
            "Missing future data is never flat price, no-trigger, profit, or loss.",
            "Only same-price-identity fresh observations may drive exit-path labels.",
            "GAP_RECOVERY is provenance, not a fresh threshold-crossing observation.",
            "Known collector gaps or unknown coverage invalidate clean path claims across that interval.",
            "Market-continuity boundaries remain explicit and cannot be bridged silently.",
            "BOT_TRUTH ordering remains observed_at then ingest_seq.",
            "The historical manual -30% stop is a hypothesis only, not a default bot stop.",
            "No exact SL, TP, trailing, or time-stop threshold is locked in EXP-0006.",
            "Do not select an exit rule by highest gross return alone.",
            "Fees, slippage, latency and failed execution remain later stages and must not be invented here.",
            "Deferred high-stringency entry hypotheses remain separate and must not bias mainline exit design.",
            "No profitability claim is allowed from exit-feasibility or gross exit research alone.",
        ],
        "staged_exit_plan": [
            {
                "stage": "E0",
                "name": "EXIT_PATH_FEASIBILITY",
                "experiment": "EXP-0006",
                "action": "Quantify clean path observability of the four locked full-entry regimes.",
                "exit_threshold_optimization": False,
            },
            {
                "stage": "E1",
                "name": "EXIT_PATH_REPLAY_FOUNDATION",
                "action": (
                    "Build deterministic first-passage/path replay and characterize path timing "
                    "only after sufficient continuous development data exists."
                ),
                "exit_threshold_optimization": False,
            },
            {
                "stage": "E2",
                "name": "COARSE_EXIT_FAMILIES",
                "action": (
                    "Version coarse hard-stop and time-stop families first; parameter ranges must "
                    "be locked before results and supported by E1 path characterization."
                ),
                "exit_threshold_optimization": True,
            },
            {
                "stage": "E3",
                "name": "PROFIT_TAKING",
                "action": (
                    "Compare simple profit-taking structures such as single TP versus separately "
                    "versioned partial-exit structures without silently changing trade definition."
                ),
                "exit_threshold_optimization": True,
            },
            {
                "stage": "E4",
                "name": "TRAILING",
                "action": "Test trailing exits as a distinct family with causal peak tracking.",
                "exit_threshold_optimization": True,
            },
            {
                "stage": "E5",
                "name": "EXIT_INTERACTION_SHORTLIST",
                "action": (
                    "Combine only a small robust shortlist of exit components and check interactions; "
                    "do not run a giant entry-plus-exit Cartesian search."
                ),
                "exit_threshold_optimization": True,
            },
        ],
        "evidence_priority": {
            "integrity_first": True,
            "sample_support_and_distribution": True,
            "neighbor_stability": True,
            "cross_entry_regime_stability": True,
            "isolated_sparse_spikes_not_promoted": True,
            "fresh_data_validation_required_later": True,
        },
        "what_EXP0006_may_conclude": [
            "which horizons are cleanly observable for each locked entry regime",
            "whether 5m-dependent exit research is supported on DEV-FREEZE-0001",
            "whether additional continuous development data is needed before mainline exit optimization",
        ],
        "what_EXP0006_must_not_do": [
            "choose a stop-loss",
            "choose a take-profit",
            "choose a trailing stop",
            "choose a time stop",
            "calculate realistic-net PnL",
            "claim profitability",
        ],
    }

def validate_selection(selection: Mapping[str, Any]) -> None:
    if selection.get("experiment_id") != "EXP-0005":
        raise ValueError("selection must come from EXP-0005")
    if selection.get("status") != "LOCKED_FOR_EXIT_RESEARCH":
        raise ValueError("EXP-0005 selection is not locked for exit research")
    rows = selection.get("carry_forward")
    if not isinstance(rows, list) or len(rows) != 4:
        raise ValueError("exactly four full-entry regimes required")
    roles = tuple(str(x.get("role")) for x in rows if isinstance(x, Mapping))
    if roles != ALL_ROLES:
        raise ValueError(f"unexpected full-entry role order: {roles}")
