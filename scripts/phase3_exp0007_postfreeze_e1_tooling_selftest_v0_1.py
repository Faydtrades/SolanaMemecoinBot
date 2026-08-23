from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from phase3.e1_path_characterization_v0_1 import characterize_readiness_report

def check(c, l):
    if not c:
        raise AssertionError(l)
    print(f"[PASS] {l}")

def fake(tier="E1_PATH_CHARACTERIZATION_READY"):
    return {
        "freeze_id": "DEV-FREEZE-0002",
        "readiness": {"tier": tier},
        "regimes": [
            {"role": "CONTROL", "parameter_set_id": "C", "candidate_count": 100,
             "clean_5m_count": 90, "clean_5m_rate": 0.9, "candidate_paths": []},
            {"role": "ROBUST_1", "parameter_set_id": "R1", "candidate_count": 40,
             "clean_5m_count": 32, "clean_5m_rate": 0.8,
             "candidate_paths": [{"status": "CLEAN", "fresh_same_identity_count": 5,
             "future_observation_count": 5, "peak_bps": 1000, "trough_bps": -500} for _ in range(32)]},
            {"role": "ROBUST_2", "parameter_set_id": "R2", "candidate_count": 40,
             "clean_5m_count": 32, "clean_5m_rate": 0.8,
             "candidate_paths": [{"status": "CLEAN", "fresh_same_identity_count": 6,
             "future_observation_count": 6, "peak_bps": 1200, "trough_bps": -600} for _ in range(32)]},
            {"role": "ROBUST_3", "parameter_set_id": "R3", "candidate_count": 40,
             "clean_5m_count": 32, "clean_5m_rate": 0.8,
             "candidate_paths": [{"status": "CLEAN", "fresh_same_identity_count": 7,
             "future_observation_count": 7, "peak_bps": 1400, "trough_bps": -700} for _ in range(32)]},
        ],
    }

def main():
    print("EXP-0007 POST-FREEZE / E1 TOOLING SELFTEST v0.1")
    print("=" * 66)
    r = characterize_readiness_report(fake())
    check(len(r["robust_regimes"]) == 3, "characterization includes exactly robust regimes")
    check(r["exit_threshold_optimization_performed"] is False, "E1 performs no exit tuning")
    check(r["robust_regimes"][0]["clean_path_peak_bps"]["p50"] == 1000, "peak distribution deterministic")
    blocked = False
    try:
        characterize_readiness_report(fake("NOT_READY"))
    except RuntimeError:
        blocked = True
    check(blocked, "E1 characterization blocked below readiness gate")
    print("=" * 66)
    print("RESULT: PASS")

if __name__ == "__main__":
    main()
