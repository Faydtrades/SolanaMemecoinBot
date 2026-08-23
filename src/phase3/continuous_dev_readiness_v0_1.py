from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Mapping

SCHEMA_VERSION="P3-CONTDEV-READINESS-0.1"
POLICY_ID="PHASE-3-CONTINUOUS-DEV-READINESS-001"
ROBUST_ROLES=("ROBUST_1","ROBUST_2","ROBUST_3")


class ReadinessTier(str,Enum):
    NOT_READY="NOT_READY"
    E1_PATH_CHARACTERIZATION_READY="E1_PATH_CHARACTERIZATION_READY"
    E2_COARSE_EXIT_SEARCH_READY="E2_COARSE_EXIT_SEARCH_READY"


@dataclass(frozen=True,slots=True)
class RegimeReadiness:
    role:str
    candidate_count:int
    clean_5m_count:int
    clean_5m_rate:float


@dataclass(frozen=True,slots=True)
class ReadinessResult:
    tier:ReadinessTier
    all_robust_roles_present:bool
    minimum_clean_5m_count:int
    minimum_clean_5m_rate:float
    reasons:tuple[str,...]


POLICY={
    "schema_version":SCHEMA_VERSION,
    "policy_id":POLICY_ID,
    "locked_before_new_dataset_results":True,
    "dataset_role":"DEVELOPMENT_DISCOVERY",
    "new_freeze_must_be_fresh_relative_to":"DEV-FREEZE-0001",
    "entry_parameters_fixed":True,
    "control_is_gate":False,
    "path_horizon_ms":300000,
    "duration_policy":{
        "fixed_wall_clock_duration_required":False,
        "informational_milestones_hours":[6,12,24],
        "principle":"Collect until evidence gate is met; do not declare readiness from duration alone.",
    },
    "E1_path_characterization_gate":{
        "min_clean_5m_paths_per_robust_regime":30,
        "min_clean_5m_rate_per_robust_regime":0.80,
        "exit_threshold_optimization_allowed":False,
    },
    "E2_coarse_exit_search_gate":{
        "min_clean_5m_paths_per_robust_regime":100,
        "min_clean_5m_rate_per_robust_regime":0.90,
        "exit_threshold_optimization_allowed":True,
        "scope":"coarse exit families only; ranges must still be locked before results",
    },
    "coverage_semantics":{
        "known_gap_is_clean":False,
        "unknown_coverage_is_clean":False,
        "gap_recovery_is_clean_path":False,
        "market_continuity_unknown_is_clean":False,
        "price_identity_mismatch_is_clean":False,
    },
    "if_not_ready":"Continue continuous collection or diagnose coverage; do not lower gate after seeing outcomes.",
}

def evaluate(rows:Mapping[str,RegimeReadiness]) -> ReadinessResult:
    missing=[r for r in ROBUST_ROLES if r not in rows]
    if missing:
        return ReadinessResult(
            ReadinessTier.NOT_READY,False,0,0.0,
            (f"missing robust roles: {','.join(missing)}",)
        )
    rr=[rows[r] for r in ROBUST_ROLES]
    min_count=min(x.clean_5m_count for x in rr)
    min_rate=min(x.clean_5m_rate for x in rr)

    e2=POLICY["E2_coarse_exit_search_gate"]
    if (min_count>=e2["min_clean_5m_paths_per_robust_regime"]
            and min_rate>=e2["min_clean_5m_rate_per_robust_regime"]):
        return ReadinessResult(
            ReadinessTier.E2_COARSE_EXIT_SEARCH_READY,True,min_count,min_rate,
            ("all robust regimes satisfy E2 clean-count and clean-rate gates",)
        )

    e1=POLICY["E1_path_characterization_gate"]
    if (min_count>=e1["min_clean_5m_paths_per_robust_regime"]
            and min_rate>=e1["min_clean_5m_rate_per_robust_regime"]):
        return ReadinessResult(
            ReadinessTier.E1_PATH_CHARACTERIZATION_READY,True,min_count,min_rate,
            ("all robust regimes satisfy E1; E2 remains locked",)
        )

    reasons=[]
    if min_count<e1["min_clean_5m_paths_per_robust_regime"]:
        reasons.append(
            f"minimum robust clean 5m count {min_count} < "
            f"{e1['min_clean_5m_paths_per_robust_regime']}"
        )
    if min_rate<e1["min_clean_5m_rate_per_robust_regime"]:
        reasons.append(
            f"minimum robust clean 5m rate {min_rate:.3f} < "
            f"{e1['min_clean_5m_rate_per_robust_regime']:.3f}"
        )
    return ReadinessResult(
        ReadinessTier.NOT_READY,True,min_count,min_rate,tuple(reasons)
    )
