from __future__ import annotations
import csv,json,hashlib,statistics
from pathlib import Path

RB=[200,500,800,2300,7300,20700]
BU=[1,2,3,4,8]
FL=[0,6000,30000,93000,185000]
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
def iv(v):return None if v in (None,"") else int(v)
def med(v):return int(statistics.median(v)) if v else None

def neighbors(r,b,f):
    ri,bi,fi=RB.index(r),BU.index(b),FL.index(f);out=[]
    for di,dj,dk in ((1,0,0),(-1,0,0),(0,1,0),(0,-1,0),(0,0,1),(0,0,-1)):
        x,y,z=ri+di,bi+dj,fi+dk
        if 0<=x<len(RB) and 0<=y<len(BU) and 0<=z<len(FL):out.append((RB[x],BU[y],FL[z]))
    return out

def main():
    root=Path(__file__).resolve().parents[1];exp=root/"data/research/phase3/EXP-0004"
    sp=exp/"EXP-0004_C_buyer_response_summary_v0_1.csv";fp=exp/"EXP-0004_C_buyer_response_frontier_v0_1.csv"
    lp=exp/"EXP-0004_C_buyer_response_candidate_outcomes_v0_1.csv";rp=exp/"EXP-0004_C_buyer_response_report_v0_1.json"
    mp=exp/"EXP-0004_manifest_v0_1.json"
    print("EXP-0004 BLOCK C POST-RUN AUDIT v0.1");print("="*64)
    for p in (sp,fp,lp,rp,mp):req(p.exists(),f"missing {p}")
    s=read_csv(sp);front=read_csv(fp);longs=read_csv(lp);report=json.loads(rp.read_text(encoding="utf-8"))
    req(len(s)==600,"summary rows !=600");req(sorted(int(x["combo_index"]) for x in s)==list(range(1,601)),"combo indices")
    grid={(x["ab_role"],int(x["min_rebound_bps"]),int(x["min_buys"]),int(x["min_net_flow_reserve_ppm"])) for x in s}
    req(grid=={(role,r,b,f) for role in ROLES for r in RB for b in BU for f in FL},"exact 4x150 grid")
    req(report.get("determinism",{}).get("pass") is True,"determinism")
    req(report.get("design",{}).get("full_cartesian_13_23m_run") is False,"Cartesian")
    req(report.get("design",{}).get("single_winner_selected") is False,"winner preselection")
    claims=report.get("performance_claims",{})
    req(claims.get("exit_optimization_performed") is False and claims.get("profitability_claim_allowed") is False,"claims")

    byid={x["parameter_set_id"]:x for x in s};counts={}
    for x in s:
        acc=int(x["expired_count"])+int(x["invalidated_count"])+int(x["rejected_count"])+int(x["trade_count"])+int(x["candidate_state_count"])
        req(int(x["run_count"])==3901==int(x["accounted_run_count"])==acc,f"terminal accounting {x['parameter_set_id']}")
        req(int(x["candidate_count"])==int(x["candidate_state_count"]),f"candidate lifecycle {x['parameter_set_id']}")
    for x in longs:counts[x["parameter_set_id"]]=counts.get(x["parameter_set_id"],0)+1
    for pid,x in byid.items():req(counts.get(pid,0)==int(x["candidate_count"]),f"outcome count {pid}")
    flagged={x["parameter_set_id"] for x in s if str(x["diagnostic_pareto_frontier"]).lower()=="true"}
    req(flagged=={x["parameter_set_id"] for x in front},"frontier consistency")

    bykey={(x["ab_role"],int(x["min_rebound_bps"]),int(x["min_buys"]),int(x["min_net_flow_reserve_ppm"])):x for x in s}
    neigh=[]
    for key,x in sorted(bykey.items()):
        role,r,b,f=key;ns=[bykey[(role,*k)] for k in neighbors(r,b,f)]
        n15=[iv(z["h15_p50_bps"]) for z in ns];n15=[z for z in n15 if z is not None]
        n30=[iv(z["h30_p50_bps"]) for z in ns];n30=[z for z in n30 if z is not None]
        neigh.append({"parameter_set_id":x["parameter_set_id"],"ab_role":role,
                      "min_rebound_bps":r,"min_buys":b,"min_net_flow_reserve_ppm":f,
                      "candidate_count":int(x["candidate_count"]),"h15_observable_n":int(x["h15_observable_n"]),
                      "h15_p50_bps":iv(x["h15_p50_bps"]),"h30_observable_n":int(x["h30_observable_n"]),
                      "h30_p50_bps":iv(x["h30_p50_bps"]),"neighbor_count":len(ns),
                      "neighbor_15s_median_of_medians_bps":med(n15),
                      "neighbor_30s_median_of_medians_bps":med(n30)})
    np=exp/"EXP-0004_C_neighborhood_diagnostics_v0_1.csv"
    with np.open("w",encoding="utf-8",newline="") as f:
        w=csv.DictWriter(f,fieldnames=list(neigh[0].keys()));w.writeheader();w.writerows(neigh)

    # Cross-AB summaries per C triple. Do not pool overlapping cohorts.
    cross=[]
    for r in RB:
        for b in BU:
            for f in FL:
                rows=[bykey[(role,r,b,f)] for role in ROLES]
                h15=[iv(x["h15_p50_bps"]) for x in rows];h15=[x for x in h15 if x is not None]
                h30=[iv(x["h30_p50_bps"]) for x in rows];h30=[x for x in h30 if x is not None]
                cross.append({
                    "min_rebound_bps":r,"min_buys":b,"min_net_flow_reserve_ppm":f,
                    "ab_regime_count":4,
                    "ab_with_clean_15s":sum(int(x["h15_observable_n"])>0 for x in rows),
                    "ab_with_clean_30s":sum(int(x["h30_observable_n"])>0 for x in rows),
                    "median_AB_candidate_count":med([int(x["candidate_count"]) for x in rows]),
                    "median_AB_h15_observable_n":med([int(x["h15_observable_n"]) for x in rows]),
                    "median_of_AB_h15_p50_bps":med(h15),"min_AB_h15_p50_bps":min(h15) if h15 else None,
                    "max_AB_h15_p50_bps":max(h15) if h15 else None,
                    "median_AB_h30_observable_n":med([int(x["h30_observable_n"]) for x in rows]),
                    "median_of_AB_h30_p50_bps":med(h30),"min_AB_h30_p50_bps":min(h30) if h30 else None,
                    "max_AB_h30_p50_bps":max(h30) if h30 else None,
                    "global_frontier_incidence":sum(str(x["diagnostic_pareto_frontier"]).lower()=="true" for x in rows),
                })
    cp=exp/"EXP-0004_C_crossAB_diagnostics_v0_1.csv"
    with cp.open("w",encoding="utf-8",newline="") as f:
        w=csv.DictWriter(f,fieldnames=list(cross[0].keys()));w.writeheader();w.writerows(cross)

    audit={"schema_version":"P3-EXP0004-POSTAUDIT-0.1","experiment_id":"EXP-0004","result":"PASS",
           "checks":{"exact_600_grid":True,"terminal_accounting":True,"candidate_rows_match":True,
                     "frontier_consistent":True,"determinism":True,"no_winner_selected":True,
                     "no_exit_optimization":True,"no_profitability_claim":True},
           "counts":{"summary_rows":len(s),"candidate_outcome_rows":len(longs),"frontier_rows":len(front)},
           "hashes":{p.name:sha(p) for p in (sp,fp,lp,rp,mp,np,cp)},
           "method_note":"Cross-AB diagnostics summarize four regime-level results per C triple; overlapping candidate cohorts are not pooled as independent evidence."}
    ap=exp/"EXP-0004_C_postrun_audit_v0_1.json";ap.write_text(json.dumps(audit,indent=2,sort_keys=True)+"\n",encoding="utf-8")
    print(f"Summary rows            : {len(s)}");print(f"Candidate outcome rows  : {len(longs)}")
    print(f"Frontier rows           : {len(front)}");print("Exact 4x150 grid        : PASS")
    print("Cross-AB diagnostics    : CREATED");print("C-neighborhood diag     : CREATED")
    print("Winner selected         : NO");print("Profitability claim     : NO")
    print(f"Audit                   : {ap}");print("="*64);print("RESULT: PASS")
if __name__=="__main__":main()
