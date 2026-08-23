from __future__ import annotations

import csv, hashlib, json
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
    exp=root/"data/research/phase3/EXP-0011"
    dp=exp/"EXP-0011_E5_small_interaction_design_v0_1.json"
    rp=exp/"EXP-0011_E5_small_interaction_results_v0_1.json"
    cp=exp/"EXP-0011_E5_small_interaction_by_regime_v0_1.csv"
    for p in (dp,rp,cp):
        req(p.exists(),f"missing {p.name}")

    d=json.loads(dp.read_text(encoding="utf-8"))
    r=json.loads(rp.read_text(encoding="utf-8"))
    with cp.open("r",encoding="utf-8",newline="") as f:
        rows=list(csv.DictReader(f))

    req(d["status"]=="LOCKED_BEFORE_E5_RESULTS","design not prelocked")
    req(d["shortlist"]["not_cartesian"] is True,"design became Cartesian")
    req(len(rows)==36,"expected 9 variants x 4 regimes = 36 rows")
    req(r["variant_count"]==9,"wrong variant count")
    req(r["cartesian_search"] is False,"result says Cartesian")
    req(r["entry_parameters_fixed"] is True,"entry changed")
    req(r["gross_reference_returns_only"] is True,"E5 must remain gross reference")
    req(r["profitability_claim_allowed"] is False,"profitability claim allowed")

    report={
        "schema_version":"P3-E5-SMALL-INTERACTION-AUDIT-0.1",
        "result":"PASS",
        "checks":{
            "design_locked_before_results":True,
            "non_cartesian":True,
            "exact_36_rows":True,
            "exact_9_variants":True,
            "entry_fixed":True,
            "gross_reference_only":True,
            "no_profitability_claim":True,
        },
        "hashes":{p.name:sha(p) for p in (dp,rp,cp)},
    }

    out=exp/"EXP-0011_E5_small_interaction_postrun_audit_v0_1.json"
    out.write_text(json.dumps(report,indent=2,sort_keys=True)+"\n",encoding="utf-8")

    print("EXP-0011 E5 SMALL INTERACTION POSTRUN AUDIT v0.1")
    print("="*84)
    print("Design locked before results : PASS")
    print("Non-Cartesian                 : PASS")
    print("Expected rows                 : 36")
    print("Expected variants             : 9")
    print("Entry A/B/C/D                 : FIXED")
    print("Gross reference only          : YES")
    print("Profitability claim           : NO")
    print(f"Audit                         : {out}")
    print("="*84)
    print("RESULT: PASS")

if __name__=="__main__":
    main()
