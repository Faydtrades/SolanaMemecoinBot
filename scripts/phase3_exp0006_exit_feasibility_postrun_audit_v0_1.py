from __future__ import annotations
import csv,hashlib,json
from pathlib import Path

def sha(p):
    h=hashlib.sha256()
    with p.open("rb") as f:
        for c in iter(lambda:f.read(1024*1024),b""):h.update(c)
    return h.hexdigest()

def req(c,m):
    if not c:
        print(f"[FAIL] {m}");print("RESULT: FAIL");raise SystemExit(1)

def main():
    root=Path(__file__).resolve().parents[1]
    exp=root/"data/research/phase3/EXP-0006"
    rp=exp/"EXP-0006_exit_feasibility_report_v0_1.json"
    dp=exp/"EXP-0006_exit_research_design_v0_1.json"
    cp=exp/"EXP-0006_exit_feasibility_by_regime_v0_1.csv"
    mp=exp/"EXP-0006_manifest_v0_1.json"
    print("EXP-0006 EXIT FEASIBILITY POST-RUN AUDIT v0.1")
    print("="*66)
    for p in (rp,dp,cp,mp):req(p.exists(),f"missing {p.name}")
    report=json.loads(rp.read_text(encoding="utf-8"))
    design=json.loads(dp.read_text(encoding="utf-8"))
    with cp.open("r",encoding="utf-8",newline="") as f: rows=list(csv.DictReader(f))
    req(len(rows)==4,"expected exactly 4 full-entry regime rows")
    req([r["role"] for r in rows]==["CONTROL","ROBUST_1","ROBUST_2","ROBUST_3"],"unexpected role order")
    claims=report["performance_claims"]
    for key in ("exit_thresholds_tested","stop_loss_selected","take_profit_selected","trailing_selected",
                "time_stop_selected","fees_modeled","slippage_modeled","latency_modeled",
                "realistic_net_pnl_available","profitability_claim_allowed"):
        req(claims[key] is False,f"{key} must be false")
    robust=[r for r in rows if r["role"].startswith("ROBUST_")]
    robust5=[int(r["5m_observable_n"]) for r in robust]
    req(robust5==report["observability"]["robust_clean_counts"]["5m"],"robust 5m count mismatch")
    allowed=report["feasibility_conclusion"]["mainline_exit_parameter_grid_allowed_now"]
    if all(x==0 for x in robust5):
        req(allowed is False,"grid must be blocked when every robust regime has zero clean 5m")
    req(design["status"]=="LOCKED_BEFORE_EXIT_OPTIMIZATION_RESULTS","design not pre-result locked")
    req(design["what_EXP0006_must_not_do"],"missing forbidden-action list")
    audit={
        "schema_version":"P3-EXP0006-POSTAUDIT-0.1",
        "experiment_id":"EXP-0006",
        "result":"PASS",
        "checks":{
            "exact_four_entry_regimes":True,
            "design_locked_before_exit_results":True,
            "no_exit_thresholds_tested":True,
            "no_profitability_claim":True,
            "five_minute_observability_gate_consistent":True,
        },
        "hashes":{p.name:sha(p) for p in (rp,dp,cp,mp)},
    }
    ap=exp/"EXP-0006_exit_feasibility_postrun_audit_v0_1.json"
    ap.write_text(json.dumps(audit,indent=2,sort_keys=True)+"\n",encoding="utf-8")
    print(f"Full-entry regimes              : 4")
    print(f"Robust clean 5m counts          : {robust5}")
    print(f"Main exit grid allowed now      : {allowed}")
    print("No SL/TP/trailing/time-stop tune: PASS")
    print(f"Audit                           : {ap}")
    print("="*66)
    print("RESULT: PASS")
if __name__=="__main__":main()
