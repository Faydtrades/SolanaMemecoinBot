from __future__ import annotations

import math
from typing import Any, Mapping
from phase3.continuous_dev_readiness_v0_1 import ReadinessTier

SCHEMA_VERSION = "P3-E1-PATH-CHAR-0.1"
ROBUST_ROLES = ("ROBUST_1", "ROBUST_2", "ROBUST_3")

def nearest_rank(values: list[int], p: int) -> int | None:
    if not values:
        return None
    s = sorted(values)
    rank = max(1, math.ceil((p / 100) * len(s)))
    return s[rank - 1]

def summarize_ints(values: list[int]) -> dict[str, int | None]:
    return {
        "n": len(values),
        "p10": nearest_rank(values, 10),
        "p25": nearest_rank(values, 25),
        "p50": nearest_rank(values, 50),
        "p75": nearest_rank(values, 75),
        "p90": nearest_rank(values, 90),
        "min": min(values) if values else None,
        "max": max(values) if values else None,
    }

def characterize_readiness_report(readiness_report: Mapping[str, Any]) -> dict[str, Any]:
    tier = str(readiness_report["readiness"]["tier"])
    allowed = {
        ReadinessTier.E1_PATH_CHARACTERIZATION_READY.value,
        ReadinessTier.E2_COARSE_EXIT_SEARCH_READY.value,
    }
    if tier not in allowed:
        raise RuntimeError(f"E1 path characterization is blocked at readiness tier {tier!r}")

    regimes = []
    for r in readiness_report["regimes"]:
        role = str(r["role"])
        if role not in ROBUST_ROLES:
            continue
        clean = [x for x in r["candidate_paths"] if x["status"] == "CLEAN"]
        if not clean:
            raise RuntimeError(f"{role}: no clean paths despite E1 readiness")

        point_counts = [int(x["fresh_same_identity_count"]) for x in clean]
        future_counts = [int(x["future_observation_count"]) for x in clean]
        peaks = [int(x["peak_bps"]) for x in clean if x["peak_bps"] is not None]
        troughs = [int(x["trough_bps"]) for x in clean if x["trough_bps"] is not None]

        regimes.append({
            "role": role,
            "parameter_set_id": r["parameter_set_id"],
            "candidate_count": int(r["candidate_count"]),
            "clean_5m_count": int(r["clean_5m_count"]),
            "clean_5m_rate": float(r["clean_5m_rate"]),
            "clean_path_fresh_point_count": summarize_ints(point_counts),
            "clean_path_future_observation_count": summarize_ints(future_counts),
            "clean_path_peak_bps": summarize_ints(peaks),
            "clean_path_trough_bps": summarize_ints(troughs),
        })

    return {
        "schema_version": SCHEMA_VERSION,
        "role": "E1_EXIT_PATH_CHARACTERIZATION",
        "source_freeze_id": readiness_report["freeze_id"],
        "source_readiness_tier": tier,
        "robust_regimes": regimes,
        "interpretation_rules": [
            "Descriptive path geometry only.",
            "No SL/TP/trailing/time-stop thresholds are selected or ranked.",
            "Peak/trough quantiles are not exit recommendations.",
            "No interpolation between market observations is used.",
            "Only CLEAN 5m paths are characterized.",
            "Entry A/B/C/D parameters remain fixed.",
        ],
        "exit_threshold_optimization_performed": False,
        "profitability_claim_allowed": False,
    }
