from pathlib import Path
import sys

PROJECT_ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(PROJECT_ROOT/"src"))

from phase3.e3_profit_taking_selection_v0_1 import SELECTION, validate_selection

def check(c,l):
    if not c: raise AssertionError(l)
    print(f"[PASS] {l}")

def main():
    print("EXP-0009 E3 PROFIT-TAKING SELECTION SELFTEST v0.1")
    print("="*78)
    validate_selection(SELECTION)
    check(SELECTION["mainline"]["primary_anchor"]["variant_id"]=="TP10_T15s",
          "TP10/T15s primary anchor locked")
    check(SELECTION["mainline"]["lower_neighbor"]["variant_id"]=="TP05_T15s",
          "TP5/T15s lower neighbor retained")
    check(SELECTION["mainline"]["upper_neighbor"]["variant_id"]=="TP15_T15s",
          "TP15/T15s upper neighbor retained")
    check(SELECTION["sensitivity_only"]["variant_id"]=="TP20_T5s",
          "TP20/T5s remains sensitivity-only")
    check(SELECTION["guardrails"]["profitability_claim_allowed"] is False,
          "no profitability claim")
    print("="*78)
    print("RESULT: PASS")

if __name__=="__main__":
    main()
