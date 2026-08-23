from pathlib import Path
import sys

PROJECT_ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(PROJECT_ROOT/"src"))

from phase3.final_phase3_validation_design_v0_1 import DESIGN,validate_design

def check(c,l):
    if not c: raise AssertionError(l)
    print(f"[PASS] {l}")

def main():
    print("EXP-0012 FINAL PHASE-3 VALIDATION DESIGN SELFTEST v0.1")
    print("="*88)
    validate_design(DESIGN)
    check(
        [x["variant_id"] for x in DESIGN["locked_candidates"]] ==
        ["E3_TP10_T15","E4_ACT10_GB03_T15","SENS_TP20_T5"],
        "exact locked finalist set retained",
    )
    check(DESIGN["validation_plan"]["no_parameter_search"] is True,
          "no parameter search in final validation")
    check(DESIGN["validation_plan"]["double_evaluate_cached_paths_for_determinism"] is True,
          "cached-path double evaluation required")
    check(DESIGN["phase3_gate"]["paper_trading_build_allowed_only_on_pass"] is True,
          "Phase 4 build gated on final validation PASS")
    check(DESIGN["research_guardrails"]["profitability_claim_allowed"] is False,
          "no profitability claim")
    print("="*88)
    print("RESULT: PASS")

if __name__=="__main__":
    main()
