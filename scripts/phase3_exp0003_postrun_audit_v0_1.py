from __future__ import annotations

import csv, json, hashlib, statistics
from pathlib import Path

MINS=[1000,2500,3000,4000,6000]
MAXS=[3500,5500,6000,6500,9000]
PAIRS=[(a,b) for a in MINS for b in MAXS if a<b]
ROLES=["CONTROL","ROBUST_1","ROBUST_2","ROBUST_3","ROBUST_4"]

def read_csv(p):
    with p.open("r",encoding="utf-8",newline="") as f:return list(csv.DictReader(f))
def file_sha(p):
    h=hashlib.sha256()
    with p.open("rb") as f:
        for c in iter(lambda:f.read(1024*1024),b""):h.update(c)
    return h.hexdigest()
def req(c,m):
    if not c:
        print(f"[FAIL] {m}");print("RESULT: FAIL");raise SystemExit(1)
def iv(v): return None if v in (None,"") else int(v)
def med(v): return int(statistics.median(v)) if v else None

def b_neighbors(pair):
    a,b=pair; out=[]
    for p in PAIRS:
        if p==pair: continue
        # neighboring valid pair differs by one adjacent step in exactly one bound.
        if p[0]==a:
            vals=[x[1] for x in PAIRS if x[0]==a]
            if b in vals and p[1] in vals and abs(vals.index(b)-vals.index(p[1]))==1: out.append(p)
        elif p[1]==b:
            vals=[x[0] for x in PAIRS if x[1]==b]
            if a in vals and p[0] in vals and abs(vals.index(a)-vals.index(p[0]))==1: out.append(p)
    return sorted(set(out))

def main():
    root=Path(__file__).resolve().parents[1]; exp=root/"data/research/phase3/EXP-0003"
    sp=exp/"EXP-0003_B_pullback_summary_v0_1.csv"; fp=exp/"EXP-0003_B_pullback_frontier_v0_1.csv"
    lp=exp/"EXP-0003_B_pullback_candidate_outcomes_v0_1.csv"; rp=exp/"EXP-0003_B_pullback_report_v0_1.json"
    mp=exp/"EXP-0003_manifest_v0_1.json"
    print("EXP-0003 BLOCK B POST-RUN AUDIT v0.1");print("="*62)
    for p in (sp,fp,lp,rp,mp):req(p.exists(),f"missing {p}")
    s=read_csv(sp);f=read_csv(fp);l=read_csv(lp);r=json.loads(rp.read_text(encoding="utf-8"))
    req(len(s)==105,"summary rows !=105")
    req(sorted(int(x["combo_index"]) for x in s)==list(range(1,106)),"combo indices")
    req({x["a_role"] for x in s}==set(ROLES),"A roles")
    grid={(x["a_role"],int(x["min_depth_bps"]),int(x["max_depth_bps"])) for x in s}
    req(grid=={(role,a,b) for role in ROLES for a,b in PAIRS},"exact 5x21 grid")
    req(r.get("determinism",{}).get("pass") is True,"determinism")
    req(r.get("design",{}).get("full_cartesian_13_23m_run") is False,"Cartesian")
    req(r.get("design",{}).get("single_winner_selected") is False,"winner")
    claims=r.get("performance_claims",{})
    req(claims.get("exit_optimization_performed") is False and claims.get("profitability_claim_allowed") is False,"claims")
    byid={x["parameter_set_id"]:x for x in s}; counts={}
    for x in s:
        acc=int(x["expired_count"])+int(x["invalidated_count"])+int(x["rejected_count"])+int(x["trade_count"])+int(x["candidate_state_count"])
        req(int(x["run_count"])==3901==int(x["accounted_run_count"])==acc,f"terminal accounting {x['parameter_set_id']}")
        req(int(x["candidate_count"])==int(x["candidate_state_count"]),f"candidate lifecycle {x['parameter_set_id']}")
    for x in l:counts[x["parameter_set_id"]]=counts.get(x["parameter_set_id"],0)+1
    for pid,x in byid.items():req(counts.get(pid,0)==int(x["candidate_count"]),f"outcome rows {pid}")
    flagged={x["parameter_set_id"] for x in s if str(x["diagnostic_pareto_frontier"]).lower()=="true"}
    req(flagged=={x["parameter_set_id"] for x in f},"frontier consistency")

    bykey={(x["a_role"],int(x["min_depth_bps"]),int(x["max_depth_bps"])):x for x in s}
    # Per A+B row neighborhood diagnostics.
    neigh=[]
    for key,x in sorted(bykey.items()):
        role,a,b=key; ns=[bykey[(role,*p)] for p in b_neighbors((a,b))]
        n15=[iv(z["h15_p50_bps"]) for z in ns];n15=[z for z in n15 if z is not None]
        n30=[iv(z["h30_p50_bps"]) for z in ns];n30=[z for z in n30 if z is not None]
        neigh.append({
            "parameter_set_id":x["parameter_set_id"],"a_role":role,"min_depth_bps":a,"max_depth_bps":b,
            "candidate_count":int(x["candidate_count"]),"h15_observable_n":int(x["h15_observable_n"]),
            "h15_p50_bps":iv(x["h15_p50_bps"]),"h30_observable_n":int(x["h30_observable_n"]),
            "h30_p50_bps":iv(x["h30_p50_bps"]),"neighbor_count":len(ns),
            "neighbor_15s_median_of_medians_bps":med(n15),
            "neighbor_30s_median_of_medians_bps":med(n30),
        })
    np=exp/"EXP-0003_B_neighborhood_diagnostics_v0_1.csv"
    with np.open("w",encoding="utf-8",newline="") as fh:
        w=csv.DictWriter(fh,fieldnames=list(neigh[0].keys()));w.writeheader();w.writerows(neigh)

    # Per pullback-pair cross-A diagnostics; do NOT pool overlapping candidate cohorts.
    cross=[]
    for a,b in PAIRS:
        rows=[bykey[(role,a,b)] for role in ROLES]
        h15=[iv(x["h15_p50_bps"]) for x in rows];h15=[x for x in h15 if x is not None]
        h30=[iv(x["h30_p50_bps"]) for x in rows];h30=[x for x in h30 if x is not None]
        cross.append({
            "min_depth_bps":a,"max_depth_bps":b,"a_regime_count":5,
            "a_with_clean_15s":sum(int(x["h15_observable_n"])>0 for x in rows),
            "a_with_clean_30s":sum(int(x["h30_observable_n"])>0 for x in rows),
            "median_A_candidate_count":med([int(x["candidate_count"]) for x in rows]),
            "median_A_h15_observable_n":med([int(x["h15_observable_n"]) for x in rows]),
            "median_of_A_h15_p50_bps":med(h15),
            "min_A_h15_p50_bps":min(h15) if h15 else None,"max_A_h15_p50_bps":max(h15) if h15 else None,
            "median_A_h30_observable_n":med([int(x["h30_observable_n"]) for x in rows]),
            "median_of_A_h30_p50_bps":med(h30),
            "min_A_h30_p50_bps":min(h30) if h30 else None,"max_A_h30_p50_bps":max(h30) if h30 else None,
            "global_frontier_incidence":sum(str(x["diagnostic_pareto_frontier"]).lower()=="true" for x in rows),
        })
    cp=exp/"EXP-0003_B_crossA_diagnostics_v0_1.csv"
    with cp.open("w",encoding="utf-8",newline="") as fh:
        w=csv.DictWriter(fh,fieldnames=list(cross[0].keys()));w.writeheader();w.writerows(cross)

    audit={"schema_version":"P3-EXP0003-POSTAUDIT-0.1","experiment_id":"EXP-0003","result":"PASS",
           "checks":{"exact_105_grid":True,"terminal_accounting":True,"candidate_rows_match":True,
                     "frontier_consistent":True,"determinism":True,"no_winner_selected":True,
                     "no_exit_optimization":True,"no_profitability_claim":True},
           "counts":{"summary_rows":len(s),"candidate_outcome_rows":len(l),"frontier_rows":len(f)},
           "hashes":{p.name:file_sha(p) for p in (sp,fp,lp,rp,mp,np,cp)},
           "method_note":"Cross-A diagnostics summarize five A-level results per B pair; overlapping cohorts are not pooled as independent evidence."}
    ap=exp/"EXP-0003_B_postrun_audit_v0_1.json";ap.write_text(json.dumps(audit,indent=2,sort_keys=True)+"\n",encoding="utf-8")
    print(f"Summary rows            : {len(s)}");print(f"Candidate outcome rows  : {len(l)}")
    print(f"Frontier rows           : {len(f)}");print("Exact 5x21 grid         : PASS")
    print("Cross-A diagnostics     : CREATED");print("B-neighborhood diag     : CREATED")
    print("Winner selected         : NO");print("Profitability claim     : NO")
    print(f"Audit                   : {ap}");print("="*62);print("RESULT: PASS")
if __name__=="__main__":main()
