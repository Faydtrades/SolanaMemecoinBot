from __future__ import annotations
import csv,hashlib,json
from pathlib import Path

def sha(p):
    h=hashlib.sha256()
    with p.open("rb") as f:
        for c in iter(lambda:f.read(1024*1024),b""):h.update(c)
    return h.hexdigest()

def req(c,m):
    if not c:print(f"[FAIL] {m}");print("RESULT: FAIL");raise SystemExit(1)

def main():
    root=Path(__file__).resolve().parents[1];exp=root/"data/research/phase3/EXP-0008"
    dp=exp/"EXP-0008_E2_coarse_exit_design_v0_1.json"
    rp=exp/"EXP-0008_E2_coarse_exit_results_v0_1.json"
    cp=exp/"EXP-0008_E2_coarse_exit_by_regime_v0_1.csv"
    for p in (dp,rp,cp):req(p.exists(),f"missing {p.name}")
    d=json.loads(dp.read_text(encoding="utf-8"));r=json.loads(rp.read_text(encoding="utf-8"))
    with cp.open("r",encoding="utf-8",newline="") as f:rows=list(csv.DictReader(f))
    req(d["status"]=="LOCKED_BEFORE_E2_RESULTS","design not prelocked")
    req(len(rows)==48,"expected 12 variants x 4 regimes = 48 rows")
    req(r["families_combined"] is False,"families were combined")
    req(r["profit_target_tested"] is False and r["trailing_tested"] is False,"later exit families leaked into E2")
    req(r["entry_parameters_fixed"] is True,"entry parameters changed")
    req(r["profitability_claim_allowed"] is False,"profitability claim allowed")
    req(r["gross_reference_returns_only"] is True,"E2 must remain gross reference research")
    audit={
      "schema_version":"P3-E2-COARSE-EXIT-AUDIT-0.1","result":"PASS",
      "checks":{"design_locked_before_results":True,"exact_48_rows":True,
                "family_factor_isolation":True,"entry_fixed":True,
                "no_tp_or_trailing":True,"gross_only":True,
                "no_profitability_claim":True},
      "hashes":{p.name:sha(p) for p in (dp,rp,cp)}
    }
    out=exp/"EXP-0008_E2_coarse_exit_postrun_audit_v0_1.json"
    out.write_text(json.dumps(audit,indent=2,sort_keys=True)+"\n",encoding="utf-8")
    print("EXP-0008 E2 COARSE EXIT POSTRUN AUDIT v0.1")
    print("="*78)
    print("Design locked before results : PASS")
    print("Expected rows                 : 48")
    print("Family isolation              : PASS")
    print("Entry A/B/C/D                 : FIXED")
    print("TP/trailing                   : NOT TESTED")
    print("Profitability claim           : NO")
    print(f"Audit                         : {out}")
    print("="*78);print("RESULT: PASS")
if __name__=="__main__":main()
