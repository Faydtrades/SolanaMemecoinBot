from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import csv
import hashlib
import json
import os
from pathlib import Path
import sys
import time

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT/"src"))

from phase2.phase1_readonly_adapter_v0_1 import Phase1ReadOnlyAdapterV01
from phase2.quote_aware_price_v0_1 import Phase1QuoteAwareAdapterV01
from phase2.strategy_first_pullback_v0_2 import FirstPullbackStrategyV02
from phase3.collector_coverage_v0_1 import CollectorCoverageSnapshotV01, parse_utc
from phase3.outcome_replay_v0_1_3 import HorizonStatus, OutcomeReplayV013
from phase3.phase2_outcome_adapter_v0_1_2 import extract_candidates, price_observation_from_event
from phase3.pullback_block_research_v0_1 import (
    FIXED_C, FIXED_D_RECLAIM, FIXED_D_RUNAWAY, PARETO_METRICS,
    generate_pullback_combinations, parameter_set_for_pullback, pareto_frontier,
    validate_locked_search, validate_selection,
)

HORIZONS={5000:"5s",15000:"15s",30000:"30s",60000:"60s",120000:"2m",300000:"5m"}
CHECKPOINT_SCHEMA="P3-EXP0003-B-WORK-0.1"

def canonical_json(x): return json.dumps(x,sort_keys=True,separators=(",",":"),ensure_ascii=True)
def stable_sha(x): return hashlib.sha256(canonical_json(x).encode()).hexdigest()
def file_sha(path):
    h=hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda:f.read(1024*1024),b""): h.update(chunk)
    return h.hexdigest()

def percentile(values,p):
    if not values:return None
    s=sorted(values); rank=max(1,(p*len(s)+99)//100); return s[rank-1]
def summarize(values):
    return {"n":len(values),"p25":percentile(values,25),"p50":percentile(values,50),"p75":percentile(values,75)}

def verify_registry(path):
    r=json.loads(path.read_text(encoding="utf-8"))
    core=dict(r); stored=core.pop("deterministic_registry_sha256",None)
    if stored!=stable_sha(core): raise RuntimeError("registry hash invalid")
    if r.get("next_experiment_id")!="EXP-0003": raise RuntimeError("registry next ID must be EXP-0003")
    exps=r.get("experiments",[])
    if len(exps)!=2: raise RuntimeError("expected EXP-0001 and EXP-0002 registered")
    exp2=[x for x in exps if isinstance(x,dict) and x.get("experiment_id")=="EXP-0002"]
    if len(exp2)!=1 or exp2[0].get("status")!="COMPLETE": raise RuntimeError("EXP-0002 not registered COMPLETE")
    return r,exp2[0]

def candidate_signature(cands):
    return stable_sha([(c.signal.mint,c.signal.generated_at_us,c.reference.price_identity,
                        c.reference.price_numerator_raw,c.reference.price_denominator_raw) for c in cands])

def horizon_row(combo,o):
    row={
        "parameter_set_id":combo.parameter_set_id,"combo_index":combo.index,
        "a_role":combo.a_role,"a_source_parameter_set_id":combo.a_source_parameter_set_id,
        "min_return_bps":combo.min_return_bps,"min_trades_since_t0":combo.min_trades_since_t0,
        "min_unique_buyers_since_t0":combo.min_unique_buyers_since_t0,
        "min_depth_bps":combo.min_depth_bps,"max_depth_bps":combo.max_depth_bps,
        "candidate_id":o.candidate_id,"mint":o.mint,"signal_at":o.signal_at.isoformat(),
        "price_identity":o.price_identity,"mfe_bps_5m":o.mfe_bps_5m,"mae_bps_5m":o.mae_bps_5m,
        "extrema_complete_5m":o.extrema_complete_5m,
    }
    for h in o.horizons:
        label=HORIZONS[h.horizon_ms]
        row[f"{label}_status"]=h.status.value
        row[f"{label}_return_bps"]=h.return_bps
        row[f"{label}_mark_age_ms"]=h.mark_age_ms
    return row

def summarize_combo(combo, extraction, outcomes):
    row={
        "parameter_set_id":combo.parameter_set_id,"combo_index":combo.index,
        "a_role":combo.a_role,"a_source_parameter_set_id":combo.a_source_parameter_set_id,
        "min_return_bps":combo.min_return_bps,"min_trades_since_t0":combo.min_trades_since_t0,
        "min_unique_buyers_since_t0":combo.min_unique_buyers_since_t0,
        "min_depth_bps":combo.min_depth_bps,"max_depth_bps":combo.max_depth_bps,
        "run_count":extraction.run_count,"candidate_count":extraction.candidate_count,
        "expired_count":extraction.expired_count,"invalidated_count":extraction.invalidated_count,
        "rejected_count":extraction.rejected_count,"trade_count":extraction.trade_count,
        "candidate_state_count":extraction.candidate_state_count,
        "accounted_run_count":extraction.accounted_run_count,
    }
    for ms,label in HORIZONS.items():
        hs=[next(h for h in o.horizons if h.horizon_ms==ms) for o in outcomes]
        clean=[int(h.return_bps) for h in hs if h.status==HorizonStatus.OBSERVABLE and h.return_bps is not None]
        s=summarize(clean)
        row[f"h{label}_observable_n"]=len(clean)
        row[f"h{label}_p25_bps"]=s["p25"]; row[f"h{label}_p50_bps"]=s["p50"]; row[f"h{label}_p75_bps"]=s["p75"]
    complete=[o for o in outcomes if o.extrema_complete_5m]
    row["complete_5m_extrema_n"]=len(complete)
    row["h15_observable_n"]=row["h15s_observable_n"]; row["h15_p50_bps"]=row["h15s_p50_bps"]
    row["h30_observable_n"]=row["h30s_observable_n"]; row["h30_p50_bps"]=row["h30s_p50_bps"]
    return row

def write_csv(path,rows):
    if not rows:
        path.write_text("",encoding="utf-8"); return
    with path.open("w",encoding="utf-8",newline="") as f:
        w=csv.DictWriter(f,fieldnames=list(rows[0].keys())); w.writeheader(); w.writerows(rows)

def write_json_atomic(path,payload):
    tmp=path.with_suffix(path.suffix+".tmp")
    with tmp.open("w",encoding="utf-8",newline="\n") as f:
        json.dump(payload,f,indent=2,sort_keys=True); f.write("\n"); f.flush(); os.fsync(f.fileno())
    os.replace(tmp,path)

def cp_path(work,idx): return work/f"combo_{idx:03d}.json"

def validate_cp(payload,combo,selection_sha):
    if payload.get("schema_version")!=CHECKPOINT_SCHEMA: raise RuntimeError(f"combo {combo.index}: checkpoint schema")
    if payload.get("selection_sha256")!=selection_sha: raise RuntimeError(f"combo {combo.index}: selection hash mismatch")
    if int(payload.get("combo_index",-1))!=combo.index or payload.get("parameter_set_id")!=combo.parameter_set_id:
        raise RuntimeError(f"combo {combo.index}: checkpoint identity mismatch")
    s=payload.get("summary"); rows=payload.get("outcome_rows")
    if not isinstance(s,dict) or not isinstance(rows,list): raise RuntimeError(f"combo {combo.index}: malformed checkpoint")
    if int(s.get("run_count",-1))!=3901 or int(s.get("accounted_run_count",-2))!=3901:
        raise RuntimeError(f"combo {combo.index}: terminal accounting checkpoint")
    if int(s.get("candidate_count",-1))!=len(rows): raise RuntimeError(f"combo {combo.index}: row count checkpoint")

def main():
    ap=argparse.ArgumentParser()
    freeze_dir=PROJECT_ROOT/"data/research/phase2_research_freeze_v0_1"
    p3=PROJECT_ROOT/"data/research/phase3"
    ap.add_argument("--freeze-db",default=str(freeze_dir/"phase2_development_dataset_v0_1.sqlite3"))
    ap.add_argument("--freeze-manifest",default=str(freeze_dir/"phase2_development_dataset_manifest_v0_1.json"))
    ap.add_argument("--locked-search",default=str(freeze_dir/"phase2_locked_param_search_001.json"))
    ap.add_argument("--registry",default=str(freeze_dir/"phase2_experiment_registry_v0_1.json"))
    ap.add_argument("--selection",default=str(p3/"EXP-0002/EXP-0002_A_selection_v0_1.json"))
    ap.add_argument("--coverage",default=str(p3/"collector_coverage_DEV-FREEZE-0001_v0_1.json"))
    ap.add_argument("--out-dir",default=str(p3/"EXP-0003"))
    args=ap.parse_args()
    paths={k:Path(v).resolve() for k,v in vars(args).items()}
    for k in ("freeze_db","freeze_manifest","locked_search","registry","selection","coverage"):
        if not paths[k].exists(): raise FileNotFoundError(paths[k])

    print("PHASE 3 / EXP-0003 — BLOCK B PULLBACK HARNESS v0.1")
    print("="*70); started=time.time()

    registry,exp2_entry=verify_registry(paths["registry"])
    selection=json.loads(paths["selection"].read_text(encoding="utf-8")); validate_selection(selection)
    selection_sha=file_sha(paths["selection"])
    if exp2_entry.get("selection_sha256")!=selection_sha: raise RuntimeError("registry selection hash mismatch")
    locked=json.loads(paths["locked_search"].read_text(encoding="utf-8")); validate_locked_search(locked)
    manifest=json.loads(paths["freeze_manifest"].read_text(encoding="utf-8")); dataset=manifest["dataset"]
    if file_sha(paths["freeze_db"])!=dataset["snapshot_sqlite_sha256"]: raise RuntimeError("freeze hash mismatch")
    if stable_sha(locked)!=manifest["parameter_search"]["locked_search_space_sha256"]: raise RuntimeError("locked search hash mismatch")

    coverage=CollectorCoverageSnapshotV01.load(paths["coverage"])
    coverage.validate_binding(
        freeze_id=str(dataset["freeze_id"]),
        range_start=parse_utc(str(dataset["t0_observed_range_utc"]["start"])),
        range_end=parse_utc(str(dataset["source_cutoff"]["max_observed_at_utc"])),
    )
    conn=Phase1ReadOnlyAdapterV01.open_readonly(paths["freeze_db"])
    try:
        if conn.execute("PRAGMA quick_check").fetchone()[0]!="ok": raise RuntimeError("freeze quick_check failed")
        Phase1QuoteAwareAdapterV01.validate_schema(conn)
        raw=Phase1QuoteAwareAdapterV01.fetch_all_bot_truth_rows(conn)
    finally: conn.close()
    events,skipped=Phase1QuoteAwareAdapterV01.normalize_rows(raw)
    if skipped: raise RuntimeError(f"unexpected normalization skips {skipped}")
    if len(events)!=int(dataset["frozen_pump_event_rows"]): raise RuntimeError("event count mismatch")

    obs_by_mint=defaultdict(list)
    for e in events:
        o=price_observation_from_event(e)
        if o is not None: obs_by_mint[o.mint].append(o)
    replay=OutcomeReplayV013(coverage.provider())

    combos=generate_pullback_combinations(locked,selection)
    out=paths["out_dir"]; out.mkdir(parents=True,exist_ok=True)
    work=out/"_work_v0_1"; work.mkdir(parents=True,exist_ok=True)
    pre=sum(cp_path(work,c.index).exists() for c in combos)

    print(f"Freeze                         : {dataset['freeze_id']}")
    print(f"Frozen events                  : {len(events)}")
    print(f"A carry-forward configurations : 5")
    print(f"Valid B pullback pairs         : 21")
    print(f"A+B combinations               : {len(combos)}")
    print("C/D                            : fixed foundation anchors")
    print("Exit optimization              : NO")
    print("PnL / fees / slippage / latency: NOT MODELED")
    print(f"Resume checkpoints found       : {pre}")
    print(f"Checkpoint directory           : {work}")
    print("-"*70)

    summaries=[]; longs=[]; sigs={}; resumed=0; computed=0
    for i,combo in enumerate(combos,1):
        params=parameter_set_for_pullback(combo); FirstPullbackStrategyV02.validate_parameters(params)
        cp=cp_path(work,combo.index)
        if cp.exists():
            payload=json.loads(cp.read_text(encoding="utf-8")); validate_cp(payload,combo,selection_sha)
            s=dict(payload["summary"]); rows=[dict(x) for x in payload["outcome_rows"]]
            sig=str(payload["candidate_signature"]); resumed+=1; source="resume"
        else:
            cands,ext=extract_candidates(events,params)
            if ext.run_count!=3901 or ext.accounted_run_count!=3901 or ext.candidate_count!=ext.candidate_state_count:
                raise RuntimeError(
                    f"{combo.parameter_set_id}: terminal accounting "
                    f"run={ext.run_count} exp={ext.expired_count} inv={ext.invalidated_count} "
                    f"rej={ext.rejected_count} trade={ext.trade_count} cand={ext.candidate_state_count}"
                )
            outcomes=[replay.replay(c.reference,obs_by_mint.get(c.signal.mint,())) for c in cands]
            s=summarize_combo(combo,ext,outcomes); rows=[horizon_row(combo,o) for o in outcomes]
            sig=candidate_signature(cands)
            payload={
                "schema_version":CHECKPOINT_SCHEMA,"experiment_id":"EXP-0003",
                "selection_sha256":selection_sha,"combo_index":combo.index,
                "parameter_set_id":combo.parameter_set_id,"candidate_signature":sig,
                "summary":s,"outcome_rows":rows,
            }
            write_json_atomic(cp,payload); computed+=1; source="compute"
        summaries.append(s); longs.extend(rows); sigs[combo.index]=sig
        if i==1 or i%10==0 or i==len(combos):
            print(
                f"[{i:3d}/105] A={combo.a_role:8s} PD={combo.min_depth_bps:4d}-{combo.max_depth_bps:4d} "
                f"candidates={int(s['candidate_count']):3d} rejects={int(s['rejected_count']):2d} "
                f"source={source:7s} elapsed={time.time()-started:6.1f}s"
            )

    for idx in (1,53,105):
        combo=combos[idx-1]; params=parameter_set_for_pullback(combo)
        second,_=extract_candidates(events,params)
        if candidate_signature(second)!=sigs[idx]: raise RuntimeError(f"determinism sentinel {idx} failed")

    frontier=pareto_frontier(summaries); fids={r["parameter_set_id"] for r in frontier}
    for r in summaries: r["diagnostic_pareto_frontier"]=r["parameter_set_id"] in fids
    longs.sort(key=lambda r:(int(r["combo_index"]),str(r["signal_at"]),str(r["mint"]),str(r["candidate_id"])))

    summary_p=out/"EXP-0003_B_pullback_summary_v0_1.csv"
    frontier_p=out/"EXP-0003_B_pullback_frontier_v0_1.csv"
    long_p=out/"EXP-0003_B_pullback_candidate_outcomes_v0_1.csv"
    report_p=out/"EXP-0003_B_pullback_report_v0_1.json"
    manifest_p=out/"EXP-0003_manifest_v0_1.json"
    write_csv(summary_p,summaries); write_csv(frontier_p,frontier); write_csv(long_p,longs)

    report_core={
        "schema_version":"P3-EXP0003-B-0.1","experiment_id":"EXP-0003","block":"B_PULLBACK",
        "dataset":{"freeze_id":dataset["freeze_id"],"dataset_role":dataset["dataset_role"],
                   "untouched_oos":False,"eligible_tokens":dataset["eligible_token_count"],
                   "frozen_rows":dataset["frozen_pump_event_rows"]},
        "design":{
            "A_carry_forward_count":5,"B_valid_pair_count":21,"combination_count":105,
            "C_fixed":FIXED_C,"D_reclaim_fixed":FIXED_D_RECLAIM,"D_runaway_fixed":FIXED_D_RUNAWAY,
            "full_cartesian_13_23m_run":False,"single_winner_selected":False,
            "primary_outcome_horizons":["15s","30s"],"five_minute_role":"SUPPORTING_DESCRIPTIVE_ONLY",
            "robustness_goal":"cross-A and neighboring-B stability; do not select by highest median alone",
        },
        "selection_binding":{"path":str(paths["selection"].relative_to(PROJECT_ROOT)).replace("\\","/"),
                             "sha256":selection_sha},
        "result_counts":{"summary_rows":len(summaries),"candidate_outcome_rows":len(longs),
                         "diagnostic_frontier_rows":len(frontier)},
        "checkpointing":{"enabled":True,"preexisting":pre,"resumed":resumed,"computed":computed},
        "determinism":{"sentinel_indices":[1,53,105],"candidate_signatures":{str(i):sigs[i] for i in (1,53,105)},"pass":True},
        "performance_claims":{"exit_optimization_performed":False,"tp_sl_tuning_performed":False,
                              "fees_modeled":False,"slippage_modeled":False,"latency_modeled":False,
                              "realistic_net_pnl_available":False,"profitability_claim_allowed":False},
        "artifacts":{"summary_csv":summary_p.name,"frontier_csv":frontier_p.name,"candidate_outcomes_csv":long_p.name},
    }
    report=dict(report_core); report["deterministic_report_core_sha256"]=stable_sha(report_core)
    report_p.write_text(json.dumps(report,indent=2,sort_keys=True)+"\n",encoding="utf-8")
    exp_manifest={
        "manifest_schema_version":"EXPM-0.1-P3EXT","experiment_id":"EXP-0003",
        "status":"RUN_COMPLETE_PENDING_POSTRUN_AUDIT_AND_REVIEW",
        "hypothesis":"Pullback depth/range changes post-signal outcome distributions across the locked Block-A carry-forward configurations.",
        "dataset":report["dataset"],"design":report["design"],"selection_binding":report["selection_binding"],
        "artifacts":{
            "summary_csv_sha256":file_sha(summary_p),"frontier_csv_sha256":file_sha(frontier_p),
            "candidate_outcomes_csv_sha256":file_sha(long_p),"report_json_sha256":file_sha(report_p),
        },
        "decision":"REVIEW_CROSS_A_AND_NEIGHBORING_B_STABILITY_BEFORE_BLOCK_C",
        "notes":"Development/discovery data only; no winner, no PnL, no exit optimization."
    }
    manifest_p.write_text(json.dumps(exp_manifest,indent=2,sort_keys=True)+"\n",encoding="utf-8")

    print("-"*70)
    print(f"Summary rows                   : {len(summaries)}")
    print(f"Candidate outcome rows         : {len(longs)}")
    print(f"Diagnostic Pareto frontier     : {len(frontier)}")
    print(f"Resumed combinations           : {resumed}")
    print(f"Freshly computed combinations  : {computed}")
    print("Sentinel determinism           : PASS")
    print("Single B winner selected       : NO")
    print(f"Report                         : {report_p}")
    print(f"Summary CSV                    : {summary_p}")
    print(f"Frontier CSV                   : {frontier_p}")
    print(f"Candidate outcomes CSV         : {long_p}")
    print(f"Experiment manifest            : {manifest_p}")
    print(f"Elapsed seconds                : {time.time()-started:.1f}")
    print("="*70)
    print("RESULT: PASS")

if __name__=="__main__": main()
