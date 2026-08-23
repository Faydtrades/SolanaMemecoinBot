from pathlib import Path
import sys

PROJECT_ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(PROJECT_ROOT/"src"))

from phase3.e4_trailing_selection_v0_1 import SELECTION,validate_selection

def check(c,l):
    if not c: raise AssertionError(l)
    print(f"[PASS] {l}")

def main():
    print("EXP-0010 E4 TRAILING SELECTION SELFTEST v0.1")
    print("="*80)
    validate_selection(SELECTION)
    check(SELECTION["mainline"]["primary_anchor"]["variant_id"]=="ACT10_GB03",
          "ACT10/GB03 primary trailing anchor locked")
    check(SELECTION["mainline"]["lower_activation_neighbor"]["variant_id"]=="ACT05_GB03",
          "ACT05/GB03 lower-activation neighbor retained")
    check(SELECTION["mainline"]["wider_giveback_neighbor"]["variant_id"]=="ACT10_GB05",
          "ACT10/GB05 wider-giveback neighbor retained")
    check(SELECTION["comparison_to_E3"]["E3_primary_remains_locked"] is True,
          "E3 TP10/T15s remains independently locked")
    check(SELECTION["guardrails"]["profitability_claim_allowed"] is False,
          "no profitability claim")
    print("="*80)
    print("RESULT: PASS")

if __name__=="__main__":
    main()
