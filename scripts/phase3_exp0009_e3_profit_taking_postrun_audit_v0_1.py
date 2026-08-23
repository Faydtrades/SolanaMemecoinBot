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
    exp=root/"data/research/phase3/EXP-0009"
    dp=exp/"EXP-0009_E3_profit_taking_design_v0_1.json"
    rp=exp/"EXP-0009_E3_profit_taking_results_v0_1.json"
    cp=exp/"EXP-0009_E3_profit_taking_by_regime_v0_1.csv"
    for p in (dp,rp,cp):
        req(p.exists(),f"missing {p.name}")

    d=json.loads(dp.read_text(encoding="utf-8"))
    r=json.loads(rp.read_text(encoding="utf-8"))
    with cp.open("r",encoding="utf-8",newline="") as f:
        rows=list(csv.DictReader(f))

    req(d["status"]=="LOCKED_BEFORE_E3_RESULTS","design not prelocked")
    req(len(rows)==48,"expected 6 TP x 2 fallback x 4 regimes = 48 rows")
    req(r["hard_stop_used"] is False,"hard stop leaked into E3")
    req(r["trailing_used"] is False,"trailing leaked into E3")
    req(r["partial_take_profit_used"] is False,"partial TP leaked into E3")
    req(r["multiple_profit_targets_used"] is False,"multi-TP leaked into E3")
    req(r["entry_parameters_fixed"] is True,"entry parameters changed")
    req(r["gross_reference_returns_only"] is True,"E3 must remain gross reference")
    req(r["profitability_claim_allowed"] is False,"profitability claim allowed")

    primary=[x for x in rows if x["fallback_time_stop_ms"]=="15000"]
    sensitivity=[x for x in rows if x["fallback_time_stop_ms"]=="5000"]
    req(len(primary)==24 and len(sensitivity)==24,
        "primary/sensitivity row split incorrect")

    report={
        "schema_version":"P3-E3-PROFIT-TAKING-AUDIT-0.1",
        "result":"PASS",
        "checks":{
            "design_locked_before_results":True,
            "exact_48_rows":True,
            "primary_T15s_rows_24":True,
            "sensitivity_T5s_rows_24":True,
            "hard_stop_absent":True,
            "trailing_absent":True,
            "partial_multi_tp_absent":True,
            "entry_fixed":True,
            "gross_reference_only":True,
            "no_profitability_claim":True,
        },
        "hashes":{p.name:sha(p) for p in (dp,rp,cp)},
    }
    out=exp/"EXP-0009_E3_profit_taking_postrun_audit_v0_1.json"
    out.write_text(json.dumps(report,indent=2,sort_keys=True)+"\n",encoding="utf-8")

    print("EXP-0009 E3 PROFIT-TAKING POSTRUN AUDIT v0.1")
    print("="*82)
    print("Design locked before results : PASS")
    print("Expected rows                 : 48")
    print("Primary T15s rows             : 24")
    print("Sensitivity T5s rows          : 24")
    print("Hard stop / trailing          : ABSENT")
    print("Partial / multi-TP            : ABSENT")
    print("Entry A/B/C/D                 : FIXED")
    print("Profitability claim           : NO")
    print(f"Audit                         : {out}")
    print("="*82)
    print("RESULT: PASS")

if __name__=="__main__":
    main()
