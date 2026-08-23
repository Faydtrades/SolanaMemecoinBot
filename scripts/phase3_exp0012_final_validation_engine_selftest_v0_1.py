from pathlib import Path
import sys
from datetime import datetime, timedelta, timezone

PROJECT_ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(PROJECT_ROOT/"src"))

from phase3.final_phase3_validation_engine_v0_1 import (
    canonical_hash, finalist_variant, comparable_row,
)

def check(c,l):
    if not c: raise AssertionError(l)
    print(f"[PASS] {l}")

def main():
    print("EXP-0012 FINAL PHASE-3 VALIDATION ENGINE SELFTEST v0.1")
    print("="*88)

    payload={"b":2,"a":[1,{"x":3}]}
    check(canonical_hash(payload)==canonical_hash({"a":[1,{"x":3}],"b":2}),
          "canonical deterministic hash order-independent")

    a={
        "candidate_id":"FINAL-A","variant_id":"E3_TP10_T15",
        "tp_bps":1000,"trail_activation_bps":None,
        "trail_giveback_bps":None,"hard_stop_bps":None,
        "fallback_ms":15000,"track":"MAINLINE",
    }
    v=finalist_variant(a)
    check(v["tp_bps"]==1000 and v["fallback_ms"]==15000,
          "FINAL-A maps exactly to TP10/T15")

    b={
        "candidate_id":"FINAL-B","variant_id":"E4_ACT10_GB03_T15",
        "tp_bps":None,"trail_activation_bps":1000,
        "trail_giveback_bps":300,"hard_stop_bps":None,
        "fallback_ms":15000,"track":"MAINLINE",
    }
    v=finalist_variant(b)
    check(v["trail_activation_bps"]==1000 and v["trail_giveback_bps"]==300,
          "FINAL-B maps exactly to ACT10/GB3/T15")

    row={
        "role":"ROBUST_1","variant_id":"X","fallback_ms":15000,
        "tp_bps":1000,"trail_activation_bps":None,
        "trail_giveback_bps":None,"hard_stop_bps":None,
        "clean_path_count":203,"scored_count":200,"unscored_count":3,
        "exit_reason_counts":{"FALLBACK":96,"TAKE_PROFIT":104},
        "exit_bps":{"n":200,"p10":-1,"p25":0,"p50":1,"p75":2,"p90":3,
                    "mean":1.5,"positive_rate":0.7,"negative_rate":0.3},
        "exit_delay_p50_ms":10,"activation_delay_p50_ms":None,
        "fallback_mark_age_p50_ms":20,
    }
    check(comparable_row(row)["scored_count"]==200,
          "audited-row comparison projection stable")

    print("="*88)
    print("RESULT: PASS")

if __name__=="__main__":
    main()
