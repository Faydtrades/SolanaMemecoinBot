from pathlib import Path
import sys

PROJECT_ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(PROJECT_ROOT/"src"))

from phase3.e5_small_interaction_design_v0_1 import DESIGN, VARIANTS, validate_design

def check(c,l):
    if not c: raise AssertionError(l)
    print(f"[PASS] {l}")

def main():
    print("EXP-0011 E5 SMALL INTERACTION DESIGN SELFTEST v0.1")
    print("="*84)
    validate_design(DESIGN)
    check(len(VARIANTS)==9,"exactly 9 hand-enumerated variants")
    check(DESIGN["shortlist"]["not_cartesian"] is True,"no Cartesian exit search")
    check(DESIGN["locked_upstream"]["E3_primary"]=="TP10_T15s",
          "E3 primary remains TP10/T15s")
    check(DESIGN["locked_upstream"]["E4_primary"]=="ACT10_GB03",
          "E4 primary remains ACT10/GB03")
    check(DESIGN["locked_upstream"]["E2_risk_control_hypothesis"]==
          "SL10_ONLY_AS_INTERACTION_HYPOTHESIS",
          "SL10 retained only as interaction hypothesis")
    check(DESIGN["reporting"]["profitability_claim_allowed"] is False,
          "no profitability claim")
    print("="*84)
    print("RESULT: PASS")

if __name__=="__main__":
    main()
