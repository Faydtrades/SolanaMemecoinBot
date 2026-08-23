from pathlib import Path
import sys

PROJECT_ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(PROJECT_ROOT/"src"))

from phase3.e2_coarse_exit_design_v0_1 import (
    DESIGN,HARD_STOP_BPS,TIME_STOP_MS,validate_design,
)

def check(c,l):
    if not c:raise AssertionError(l)
    print(f"[PASS] {l}")

def main():
    print("EXP-0008 E2 COARSE EXIT DESIGN SELFTEST v0.1")
    print("="*72)
    validate_design(DESIGN)
    check(HARD_STOP_BPS==(-1000,-2000,-3000,-4000,-5000,-6000),
          "coarse stop grid locked -10% through -60%")
    check(TIME_STOP_MS==(5000,15000,30000,60000,120000,300000),
          "time-stop grid reuses fixed 5s/15s/30s/60s/2m/5m horizons")
    check(DESIGN["factor_isolation"]["hard_stop_time_stop_cartesian_search"] is False,
          "no hard-stop x time-stop Cartesian search")
    check(DESIGN["reporting"]["fees_modeled"] is False,
          "fees remain out of E2 gross research")
    check(DESIGN["reporting"]["profitability_claim_allowed"] is False,
          "no profitability claim")
    print("="*72)
    print("RESULT: PASS")

if __name__=="__main__":
    main()
