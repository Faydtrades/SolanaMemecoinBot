from pathlib import Path
import sys
PROJECT_ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(PROJECT_ROOT/"src"))

from phase3.devfreeze0002_design_v0_1 import DESIGN, validate_design

def check(c,l):
    if not c: raise AssertionError(l)
    print(f"[PASS] {l}")

def main():
    print("EXP-0007 / DEV-FREEZE-0002 DESIGN SELFTEST v0.1")
    print("="*64)
    validate_design(DESIGN)
    s=DESIGN["selection_semantics"]
    check(s["required_tail_after_t0_ms"]==600000,"10m t0 tail guard locked")
    check(s["global_first_observed_tradable_must_be_inside_segment"] is True,
          "left-censored tokens excluded")
    check(s["global_first_trade_may_be_gap_recovery"] is False,
          "gap-recovered t0 cannot seed clean fresh cohort")
    check(s["explicit_gaps_allowed_inside_selected_segment"] is False,
          "selected segment must be gap-free")
    check(DESIGN["safety"]["immutable_builder_requires_closed_non_uncertain_segment"] is True,
          "active/provisional segment cannot be frozen")
    check(DESIGN["safety"]["immutable_builder_refuses_overwrite"] is True,
          "immutable overwrite protection locked")
    check(DESIGN["entry_research_binding"]["A_B_C_D_parameters_fixed"] is True,
          "entry parameters remain fixed")
    print("="*64)
    print("RESULT: PASS")
if __name__=="__main__": main()
