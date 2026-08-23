from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path

def sha(path):
    h=hashlib.sha256()
    with path.open("rb") as f:
        for c in iter(lambda:f.read(1024*1024),b""):
            h.update(c)
    return h.hexdigest()

def req(cond,msg):
    if not cond:
        print(f"[FAIL] {msg}")
        print("RESULT: FAIL")
        raise SystemExit(1)

def main():
    root=Path(__file__).resolve().parents[1]
    exp=root/"data/research/phase3/EXP-0010"
    dp=exp/"EXP-0010_E4_trailing_design_v0_1.json"
    rp=exp/"EXP-0010_E4_trailing_results_v0_1.json"
    cp=exp/"EXP-0010_E4_trailing_by_regime_v0_1.csv"
    for p in (dp,rp,cp):
        req(p.exists(),f"missing {p.name}")

    d=json.loads(dp.read_text(encoding="utf-8"))
    r=json.loads(rp.read_text(encoding="utf-8"))
    with cp.open("r",encoding="utf-8",newline="") as f:
        rows=list(csv.DictReader(f))

    req(d["status"]=="LOCKED_BEFORE_E4_RESULTS","design not prelocked")
    req(len(rows)==36,"expected 9 trailing variants x 4 regimes = 36 rows")
    req(r["fixed_take_profit_used"] is False,"E3 TP leaked into E4")
    req(r["hard_stop_used"] is False,"hard stop leaked into E4")
    req(r["partial_take_profit_used"] is False,"partial TP leaked into E4")
    req(r["multiple_take_profit_used"] is False,"multi TP leaked into E4")
    req(r["T5s_sensitivity_used"] is False,"T5s sensitivity leaked into E4")
    req(r["entry_parameters_fixed"] is True,"entry parameters changed")
    req(r["gross_reference_returns_only"] is True,"E4 must remain gross reference")
    req(r["profitability_claim_allowed"] is False,"profitability claim allowed")

    report={
        "schema_version":"P3-E4-TRAILING-AUDIT-0.1",
        "result":"PASS",
        "checks":{
            "design_locked_before_results":True,
            "exact_36_rows":True,
            "fixed_tp_absent":True,
            "hard_stop_absent":True,
            "partial_multi_tp_absent":True,
            "T5s_sensitivity_absent":True,
            "entry_fixed":True,
            "gross_reference_only":True,
            "no_profitability_claim":True,
        },
        "hashes":{p.name:sha(p) for p in (dp,rp,cp)},
    }

    out=exp/"EXP-0010_E4_trailing_postrun_audit_v0_1.json"
    out.write_text(json.dumps(report,indent=2,sort_keys=True)+"\n",encoding="utf-8")

    print("EXP-0010 E4 TRAILING POSTRUN AUDIT v0.1")
    print("="*82)
    print("Design locked before results : PASS")
    print("Expected rows                 : 36")
    print("E3 fixed TP                   : ABSENT")
    print("Hard stop                     : ABSENT")
    print("Partial / multi-TP            : ABSENT")
    print("T5s sensitivity               : ABSENT")
    print("Entry A/B/C/D                 : FIXED")
    print("Profitability claim           : NO")
    print(f"Audit                         : {out}")
    print("="*82)
    print("RESULT: PASS")

if __name__=="__main__":
    main()
