from pathlib import Path
import sys
PROJECT_ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(PROJECT_ROOT/"src"))

from phase3.devfreeze0002_closed_prefix_design_v0_2_1 import (
    DESIGN, Interval, REQUIRED_POST_T0_MS, eligible_interval,
    maximal_definite_prefix_cutoff, validate_design,
)

def check(c,label):
    if not c:
        raise AssertionError(label)
    print(f"[PASS] {label}")

def main():
    print("DEV-FREEZE-0002 CLOSED-PREFIX DESIGN SELFTEST v0.2.1")
    print("="*74)
    validate_design(DESIGN)
    ivs=(
        Interval(7,0,20*60*1_000_000,False),
        Interval(7,21*60*1_000_000,50*60*1_000_000,False),
        Interval(7,51*60*1_000_000,80*60*1_000_000,True),
    )
    cutoff=maximal_definite_prefix_cutoff(ivs)
    check(cutoff==50*60*1_000_000,"cutoff is last definitely closed point")
    check(
        eligible_interval(
            t0_us=25*60*1_000_000,required_post_t0_ms=REQUIRED_POST_T0_MS,
            intervals=ivs,cutoff_us=cutoff
        ) is ivs[1],
        "token wholly inside definite prefix remains eligible",
    )
    check(
        eligible_interval(
            t0_us=45*60*1_000_000,required_post_t0_ms=REQUIRED_POST_T0_MS,
            intervals=ivs,cutoff_us=cutoff
        ) is None,
        "token whose 10m tail enters discarded uncertain tail is excluded",
    )
    check(
        eligible_interval(
            t0_us=15*60*1_000_000,required_post_t0_ms=REQUIRED_POST_T0_MS,
            intervals=ivs,cutoff_us=cutoff
        ) is None,
        "token crossing an explicit clean-interval break is excluded",
    )
    check(DESIGN["information_seen_before_amendment"]["forward_returns"] is False,
          "amendment remains outcome-blind")
    check(DESIGN["entry_binding"]["A_B_C_D_fixed"] is True,
          "entry A/B/C/D remains fixed")
    print("="*74)
    print("RESULT: PASS")

if __name__=="__main__":
    main()
