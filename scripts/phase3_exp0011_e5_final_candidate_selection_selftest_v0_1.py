from pathlib import Path
import sys

PROJECT_ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(PROJECT_ROOT/"src"))

from phase3.e5_final_candidate_selection_v0_1 import SELECTION,validate_selection

def check(c,l):
    if not c: raise AssertionError(l)
    print(f"[PASS] {l}")

def main():
    print("EXP-0011 E5 FINAL CANDIDATE SELECTION SELFTEST v0.1")
    print("="*86)
    validate_selection(SELECTION)
    main_ids=[x["variant_id"] for x in SELECTION["final_mainline_candidates"]]
    check(main_ids==["E3_TP10_T15","E4_ACT10_GB03_T15"],
          "exactly two mainline Phase-3 finalists retained")
    check(SELECTION["sensitivity_only"]["variant_id"]=="SENS_TP20_T5",
          "T5 sensitivity remains separate")
    check(SELECTION["decision"]["single_dev_data_winner_selected"] is False,
          "no single DEV-FREEZE-0002 winner declared")
    check(SELECTION["guardrails"]["no_further_exit_parameter_tuning_before_final_validation"],
          "no further exit tuning before final validation")
    check(SELECTION["guardrails"]["profitability_claim_allowed"] is False,
          "no profitability claim")
    print("="*86)
    print("RESULT: PASS")

if __name__=="__main__":
    main()
