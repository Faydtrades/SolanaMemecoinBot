from __future__ import annotations
import csv,json,os
from pathlib import Path
import sys
PROJECT_ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(PROJECT_ROOT/"src"))
from phase3.e1_path_characterization_v0_1 import characterize_readiness_report

def atomic(p,x):
    t=p.with_suffix(p.suffix+".tmp")
    with t.open("w",encoding="utf-8",newline="\n") as f:
        json.dump(x,f,indent=2,sort_keys=True);f.write("\n");f.flush();os.fsync(f.fileno())
    os.replace(t,p)

def main():
    exp=PROJECT_ROOT/"data/research/phase3/EXP-0007"
    source=exp/"DEV-FREEZE-0002_locked_entry_readiness_report_v0_2.json"
    if not source.exists():raise FileNotFoundError(source)
    result=characterize_readiness_report(json.loads(source.read_text(encoding="utf-8")))
    rp=exp/"DEV-FREEZE-0002_E1_path_characterization_v0_2.json"
    cp=exp/"DEV-FREEZE-0002_E1_path_characterization_by_regime_v0_2.csv"
    atomic(rp,result)
    rows=[]
    for r in result["robust_regimes"]:
        row={"role":r["role"],"parameter_set_id":r["parameter_set_id"],
             "candidate_count":r["candidate_count"],"clean_5m_count":r["clean_5m_count"],
             "clean_5m_rate":r["clean_5m_rate"]}
        for name in ("clean_path_fresh_point_count","clean_path_future_observation_count",
                     "clean_path_peak_bps","clean_path_trough_bps"):
            for k,v in r[name].items():row[f"{name}_{k}"]=v
        rows.append(row)
    with cp.open("w",encoding="utf-8",newline="") as f:
        w=csv.DictWriter(f,fieldnames=list(rows[0].keys()));w.writeheader();w.writerows(rows)
    print("EXP-0007 / DEV-FREEZE-0002 E1 PATH CHARACTERIZATION v0.2")
    print("="*72)
    print(f"Source readiness tier       : {result['source_readiness_tier']}")
    for r in result["robust_regimes"]:
        print(f"{r['role']:8s} clean5m={r['clean_5m_count']:4d} peak_p50={r['clean_path_peak_bps']['p50']} bps trough_p50={r['clean_path_trough_bps']['p50']} bps")
    print("Exit thresholds selected    : NO")
    print("Profitability claim         : NO")
    print(f"Report                      : {rp}")
    print("="*72);print("RESULT: PASS")
if __name__=="__main__":main()
