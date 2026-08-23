from pathlib import Path
import sys

PROJECT_ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(PROJECT_ROOT/"src"))

from phase3.e3_profit_taking_design_v0_1 import (
    DESIGN, TAKE_PROFIT_BPS, FALLBACK_TIME_STOP_MS, validate_design,
)

def check(cond, label):
    if not cond:
        raise AssertionError(label)
    print(f"[PASS] {label}")

def main():
    print("EXP-0009 E3 PROFIT-TAKING DESIGN SELFTEST v0.1")
    print("=" * 76)
    validate_design(DESIGN)
    check(
        TAKE_PROFIT_BPS == (300,500,1000,1500,2000,3000),
        "TP grid locked at +3,+5,+10,+15,+20,+30 percent",
    )
    check(
        FALLBACK_TIME_STOP_MS == (15000,5000),
        "T15s primary + T5s sensitivity fallback locked",
    )
    check(
        DESIGN["factor_isolation"]["hard_stop_used"] is False,
        "no hard stop in E3",
    )
    check(
        DESIGN["factor_isolation"]["trailing_used"] is False,
        "no trailing in E3",
    )
    check(
        DESIGN["preconditions"]["E2_hard_stop_mainline_winner"] is None,
        "E2 promoted no hard-stop winner",
    )
    check(
        DESIGN["reporting"]["profitability_claim_allowed"] is False,
        "no profitability claim",
    )
    print("=" * 76)
    print("RESULT: PASS")

if __name__=="__main__":
    main()
