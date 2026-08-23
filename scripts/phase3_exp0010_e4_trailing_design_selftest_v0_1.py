from pathlib import Path
import sys

PROJECT_ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(PROJECT_ROOT/"src"))

from phase3.e4_trailing_design_v0_1 import (
    DESIGN, TRAIL_ACTIVATION_BPS, TRAIL_GIVEBACK_BPS,
    FALLBACK_TIME_STOP_MS, validate_design,
)

def check(c,l):
    if not c: raise AssertionError(l)
    print(f"[PASS] {l}")

def main():
    print("EXP-0010 E4 TRAILING DESIGN SELFTEST v0.1")
    print("="*78)
    validate_design(DESIGN)
    check(TRAIL_ACTIVATION_BPS==(500,1000,1500),
          "activation grid locked at +5,+10,+15 percent")
    check(TRAIL_GIVEBACK_BPS==(300,500,1000),
          "giveback grid locked at 3,5,10 percent")
    check(FALLBACK_TIME_STOP_MS==15000,
          "15s fallback remains fixed")
    check(DESIGN["trailing_family"]["variant_count"]==9,
          "exactly 9 trailing variants")
    check(DESIGN["factor_isolation"]["fixed_take_profit_used"] is False,
          "E3 TP not mixed into E4")
    check(DESIGN["factor_isolation"]["hard_stop_used"] is False,
          "no hard stop in E4")
    check(DESIGN["reporting"]["profitability_claim_allowed"] is False,
          "no profitability claim")
    print("="*78)
    print("RESULT: PASS")

if __name__=="__main__":
    main()
