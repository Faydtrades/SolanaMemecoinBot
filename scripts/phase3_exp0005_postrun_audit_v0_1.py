from __future__ import annotations

import csv,json,hashlib,statistics
from pathlib import Path

RC=[200,500,700,800,2500,7100,19500]
RW=[2000,2500,3000,7000,10000]
PAIRS=[(a,b) for a in RC for b in RW if a<b]
ROLES=["CONTROL","ROBUST_1","ROBUST_2","ROBUST_3"]

def read_csv(p):
    with p.open("r",encoding="utf-8",newline="") as f:return list(csv.DictReader(f))
def sha(p):
    h=hashlib.sha256()
    with p.open("rb") as f:
        for c in iter(lambda:f.read(1024*1024),b""):h.update(c)
    return h.hexdigest()
def req(c,m):
    if not c:print(f"[FAIL] {m}");print("RESULT: FAIL");raise SystemExit(1)
def iv(v):return None if v in (None,"") else int(float(v))
def med(v):return int(statistics.median(v)) if v else None

def neighbors(pair):
    a,b=pair;out=[]
    for p in PAIRS:
        if p==pair:continue
        if p[0]==a:
            vals=[x[1] for x in PAIRS if x[0]==a]
            if b in vals and p[1] in vals and abs(vals.index(b)-vals.index(p[1]))==1:out.append(p)
        elif p[1]==b:
            vals=[x[0] for x in PAIRS if x[1]==b]
            if a in vals and p[0] in vals and abs(vals.index(a)-vals.index(p[0]))==1:out.append(p)
    return sorted(set(out))

def main():
    root=Path(__file__).resolve().parents[1];exp=root/"data/research/phase3/EXP-0005"
    sp=exp/"EXP-0005_D_reclaim_runaway_summary_v0_1.csv"
    fp=exp/"EXP-0005_D_reclaim_runaway_frontier_v0_1.csv"
    lp=exp/"EXP-0005_D_reclaim_runaway_candidate_outcomes_v0_1.csv"
    rp=exp/"EXP-0005_D_reclaim_runaway_report_v0_1.json"
    mp=exp/"EXP-0005_manifest_v0_1.json"
    print("EXP-0005 BLOCK D POST-RUN AUDIT v0.1");print("="*66)
    for p in (sp,fp,lp,rp,mp):req(p.exists(),f"missing {p}")
    s=read_csv(sp);front=read_csv(fp);longs=read_csv(lp);report=json.loads(rp.read_text(encoding="utf-8"))
    req(len(s)==96,"summary rows !=96")
    req(sorted(int(x["combo_index"]) for x in s)==list(range(1,97)),"combo indices")
    grid={(x["abc_role"],int(x["min_extension_from_response_bps"]),int(x["max_extension_from_response_bps"])) for x in s}
    req(grid=={(role,a,b) for role in ROLES for a,b in PAIRS},"exact 4x24 grid")
    req(report.get("determinism",{}).get("pass") is True,"determinism")
    req(report.get("design",{}).get("full_cartesian_13_23m_run") is False,"Cartesian")
    req(report.get("design",{}).get("single_winner_selected") is False,"winner preselection")
    req(report.get("design",{}).get("deferred_BlockC_hypotheses_in_main_search") is False,"deferred contamination")
    claims=report.get("performance_claims",{})
    req(claims.get("exit_optimization_performed") is False and claims.get("profitability_claim_allowed") is False,"claims")

    byid={x["parameter_set_id"]:x for x in s};counts={}
    for x in s:
        acc=int(x["expired_count"])+int(x["invalidated_count"])+int(x["rejected_count"])+int(x["trade_count"])+int(x["candidate_state_count"])
        req(int(x["run_count"])==3901==int(x["accounted_run_count"])==acc,f"terminal accounting {x['parameter_set_id']}")
        req(int(x["candidate_count"])==int(x["candidate_state_count"]),f"candidate lifecycle {x['parameter_set_id']}")
    for x in longs:counts[x["parameter_set_id"]]=counts.get(x["parameter_set_id"],0)+1
    for pid,x in byid.items():req(counts.get(pid,0)==int(x["candidate_count"]),f"outcome rows {pid}")
    flagged={x["parameter_set_id"] for x in s if str(x["diagnostic_pareto_frontier"]).lower()=="true"}
    req(flagged=={x["parameter_set_id"] for x in front},"frontier consistency")

    bykey={(x["abc_role"],int(x["min_extension_from_response_bps"]),int(x["max_extension_from_response_bps"])):x for x in s}
    neigh=[]
    for key,x in sorted(bykey.items()):
        role,a,b=key;ns=[bykey[(role,*p)] for p in neighbors((a,b))]
        n15=[iv(z["h15_p50_bps"]) for z in ns];n15=[z for z in n15 if z is not None]
        n30=[iv(z["h30_p50_bps"]) for z in ns];n30=[z for z in n30 if z is not None]
        neigh.append({
            "parameter_set_id":x["parameter_set_id"],"abc_role":role,
            "min_extension_from_response_bps":a,"max_extension_from_response_bps":b,
            "candidate_count":int(x["candidate_count"]),"h15_observable_n":int(x["h15_observable_n"]),
            "h15_p50_bps":iv(x["h15_p50_bps"]),"h30_observable_n":int(x["h30_observable_n"]),
            "h30_p50_bps":iv(x["h30_p50_bps"]),"neighbor_count":len(ns),
            "neighbor_15s_median_of_medians_bps":med(n15),
            "neighbor_30s_median_of_medians_bps":med(n30),
        })
    np=exp/"EXP-0005_D_neighborhood_diagnostics_v0_1.csv"
    with np.open("w",encoding="utf-8",newline="") as f:
        w=csv.DictWriter(f,fieldnames=list(neigh[0].keys()));w.writeheader();w.writerows(neigh)

    cross=[]
    for a,b in PAIRS:
        rows=[bykey[(role,a,b)] for role in ROLES]
        h15=[iv(x["h15_p50_bps"]) for x in rows];h15=[x for x in h15 if x is not None]
        h30=[iv(x["h30_p50_bps"]) for x in rows];h30=[x for x in h30 if x is not None]
        cross.append({
            "min_extension_from_response_bps":a,"max_extension_from_response_bps":b,
            "abc_regime_count":4,
            "abc_with_clean_15s":sum(int(x["h15_observable_n"])>0 for x in rows),
            "abc_with_clean_30s":sum(int(x["h30_observable_n"])>0 for x in rows),
            "median_ABC_candidate_count":med([int(x["candidate_count"]) for x in rows]),
            "median_ABC_h15_observable_n":med([int(x["h15_observable_n"]) for x in rows]),
            "median_of_ABC_h15_p50_bps":med(h15),
            "min_ABC_h15_p50_bps":min(h15) if h15 else None,
            "max_ABC_h15_p50_bps":max(h15) if h15 else None,
            "median_ABC_h30_observable_n":med([int(x["h30_observable_n"]) for x in rows]),
            "median_of_ABC_h30_p50_bps":med(h30),
            "min_ABC_h30_p50_bps":min(h30) if h30 else None,
            "max_ABC_h30_p50_bps":max(h30) if h30 else None,
            "global_frontier_incidence":sum(str(x["diagnostic_pareto_frontier"]).lower()=="true" for x in rows),
        })
    cp=exp/"EXP-0005_D_crossABC_diagnostics_v0_1.csv"
    with cp.open("w",encoding="utf-8",newline="") as f:
        w=csv.DictWriter(f,fieldnames=list(cross[0].keys()));w.writeheader();w.writerows(cross)

    audit={"schema_version":"P3-EXP0005-POSTAUDIT-0.1","experiment_id":"EXP-0005","result":"PASS",
           "checks":{"exact_96_grid":True,"terminal_accounting":True,"candidate_rows_match":True,
                     "frontier_consistent":True,"determinism":True,"no_winner_selected":True,
                     "no_exit_optimization":True,"no_profitability_claim":True,
                     "deferred_BlockC_hypotheses_separate":True},
           "counts":{"summary_rows":len(s),"candidate_outcome_rows":len(longs),"frontier_rows":len(front)},
           "hashes":{p.name:sha(p) for p in (sp,fp,lp,rp,mp,np,cp)},
           "method_note":"Cross-ABC diagnostics summarize four regime-level results per D pair; overlapping candidate cohorts are not pooled."}
    ap=exp/"EXP-0005_D_postrun_audit_v0_1.json"
    ap.write_text(json.dumps(audit,indent=2,sort_keys=True)+"\n",encoding="utf-8")
    print(f"Summary rows            : {len(s)}");print(f"Candidate outcome rows  : {len(longs)}")
    print(f"Frontier rows           : {len(front)}");print("Exact 4x24 grid         : PASS")
    print("Cross-ABC diagnostics   : CREATED");print("D-neighborhood diag     : CREATED")
    print("Deferred C hypotheses   : SEPARATE")
    print("Winner selected         : NO");print("Profitability claim     : NO")
    print(f"Audit                   : {ap}");print("="*66);print("RESULT: PASS")
if __name__=="__main__":main()
