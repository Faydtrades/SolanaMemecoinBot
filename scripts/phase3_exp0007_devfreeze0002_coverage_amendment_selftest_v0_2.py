from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from phase3.devfreeze0002_design_v0_2 import (
    DESIGN, REQUIRED_POST_T0_MS, CleanInterval,
    eligible_interval_for_t0, validate_design,
)

def check(cond, label):
    if not cond:
        raise AssertionError(label)
    print(f"[PASS] {label}")

def main():
    print("EXP-0007 / DEV-FREEZE-0002 COVERAGE AMENDMENT SELFTEST v0.2")
    print("=" * 74)
    validate_design(DESIGN)
    check(REQUIRED_POST_T0_MS == 600_000, "10m t0 tail guard unchanged")
    a = CleanInterval(session_id=7, start_us=0, end_us=20*60*1_000_000)
    b = CleanInterval(session_id=7, start_us=21*60*1_000_000, end_us=40*60*1_000_000)
    check(
        eligible_interval_for_t0(t0_us=5*60*1_000_000,
                                 required_post_t0_ms=REQUIRED_POST_T0_MS,
                                 intervals=(a,b)) is a,
        "token wholly inside clean interval is eligible",
    )
    check(
        eligible_interval_for_t0(t0_us=15*60*1_000_000,
                                 required_post_t0_ms=REQUIRED_POST_T0_MS,
                                 intervals=(a,b)) is None,
        "token whose 10m window crosses explicit gap is excluded",
    )
    check(
        eligible_interval_for_t0(t0_us=25*60*1_000_000,
                                 required_post_t0_ms=REQUIRED_POST_T0_MS,
                                 intervals=(a,b)) is b,
        "gap elsewhere in session does not exclude unrelated token",
    )
    check(DESIGN["entry_research_binding"]["A_B_C_D_parameters_fixed"] is True,
          "entry A/B/C/D remains fixed")
    check(DESIGN["selection_semantics"]["future_outcome_filter"] is False,
          "future/outcome filtering remains forbidden")
    print("=" * 74)
    print("RESULT: PASS")

if __name__ == "__main__":
    main()
