from __future__ import annotations

from collections import Counter,defaultdict
from datetime import datetime
import hashlib,json
from pathlib import Path
from typing import Any,Mapping

from phase2.models_v0_1 import FirstPullbackParameterSet
from phase2.phase1_readonly_adapter_v0_1 import Phase1ReadOnlyAdapterV01
from phase2.quote_aware_price_v0_1 import Phase1QuoteAwareAdapterV01
from phase2.strategy_first_pullback_v0_2 import FirstPullbackStrategyV02
from phase3.collector_coverage_v0_2 import CollectorCoverageSnapshotV02,parse_utc
from phase3.continuous_dev_readiness_v0_1 import POLICY,RegimeReadiness,evaluate
from phase3.exit_path_replay_v0_1 import ExitPathReplayV01,ExitPathStatus
from phase3.phase2_outcome_adapter_v0_1_2 import extract_candidates,price_observation_from_event

SCHEMA_VERSION="P3-LOCKED-ENTRY-READINESS-0.1.1"
EXPECTED_ROLES=("CONTROL","ROBUST_1","ROBUST_2","ROBUST_3")

def file_sha256(path:Path)->str:
    h=hashlib.sha256()
    with path.open("rb") as f:
        for c in iter(lambda:f.read(1024*1024),b""):h.update(c)
    return h.hexdigest()

def parameter_set_from_locked_row(row:Mapping[str,Any])->FirstPullbackParameterSet:
    required=("parameter_set_id","min_return_bps","min_trades_since_t0",
              "min_unique_buyers_since_t0","min_depth_bps","max_depth_bps",
              "min_rebound_bps","min_buys","min_net_flow_reserve_ppm",
              "min_extension_from_response_bps","max_extension_from_response_bps")
    missing=[k for k in required if k not in row]
    if missing:raise ValueError(f"locked selection row missing: {missing}")
    p=FirstPullbackParameterSet(
        parameter_set_id=str(row["parameter_set_id"]),strategy_version="v1.1",max_entry_age_ms=300000,
        impulse_parameters={"min_return_bps":int(row["min_return_bps"]),
                            "min_trades_since_t0":int(row["min_trades_since_t0"]),
                            "min_unique_buyers_since_t0":int(row["min_unique_buyers_since_t0"])},
        pullback_parameters={"min_depth_bps":int(row["min_depth_bps"]),
                             "max_depth_bps":int(row["max_depth_bps"])},
        buyer_response_parameters={"window_id":"3s","min_rebound_bps":int(row["min_rebound_bps"]),
                                   "min_buys":int(row["min_buys"]),
                                   "min_net_flow_reserve_ppm":int(row["min_net_flow_reserve_ppm"])},
        reclaim_parameters={"min_extension_from_response_bps":int(row["min_extension_from_response_bps"])},
        runaway_entry_parameters={"max_extension_from_response_bps":int(row["max_extension_from_response_bps"])},
    )
    FirstPullbackStrategyV02.validate_parameters(p)
    return p

def load_locked_selection(path:Path):
    payload=json.loads(path.read_text(encoding="utf-8"))
    if payload.get("experiment_id")!="EXP-0005":raise ValueError("selection must come from EXP-0005")
    if payload.get("status")!="LOCKED_FOR_EXIT_RESEARCH":raise ValueError("selection is not locked for exit research")
    rows=payload.get("carry_forward")
    if not isinstance(rows,list) or len(rows)!=4:raise ValueError("expected four locked full-entry regimes")
    roles=tuple(str(r.get("role")) for r in rows)
    if roles!=EXPECTED_ROLES:raise ValueError(f"unexpected role order {roles}")
    return payload,rows

def manifest_identity(manifest:Mapping[str,Any]):
    dataset=manifest.get("dataset")
    if not isinstance(dataset,Mapping):raise ValueError("manifest dataset missing")
    freeze_id=str(dataset.get("freeze_id") or manifest.get("freeze_id") or "")
    role=str(dataset.get("dataset_role") or manifest.get("dataset_role") or "")
    if "clean_segment_utc" in dataset:
        r=dataset["clean_segment_utc"]
        start=parse_utc(str(r["start"]));end=parse_utc(str(r["end"]))
    else:
        start=parse_utc(str(dataset["t0_observed_range_utc"]["start"]))
        end=parse_utc(str(dataset["source_cutoff"]["max_observed_at_utc"]))
    expected_rows=int(dataset["frozen_pump_event_rows"])
    if not freeze_id:raise ValueError("manifest freeze_id missing")
    return freeze_id,role,start,end,expected_rows

def run_locked_entry_readiness(*,freeze_db:Path,freeze_manifest:Path,coverage_snapshot:Path,selection_path:Path):
    selection,selection_rows=load_locked_selection(selection_path)
    manifest=json.loads(freeze_manifest.read_text(encoding="utf-8"))
    freeze_id,dataset_role,range_start,range_end,expected_rows=manifest_identity(manifest)
    coverage=CollectorCoverageSnapshotV02.load(coverage_snapshot)
    coverage.validate_binding(freeze_id=freeze_id,range_start=range_start,range_end=range_end)
    replay=ExitPathReplayV01(coverage.provider())

    conn=Phase1ReadOnlyAdapterV01.open_readonly(freeze_db)
    try:
        if conn.execute("PRAGMA quick_check").fetchone()[0]!="ok":raise RuntimeError("freeze quick_check failed")
        Phase1QuoteAwareAdapterV01.validate_schema(conn)
        raw=Phase1QuoteAwareAdapterV01.fetch_all_bot_truth_rows(conn)
    finally:conn.close()
    events,skipped=Phase1QuoteAwareAdapterV01.normalize_rows(raw)
    if skipped:raise RuntimeError(f"unexpected normalization skips: {skipped}")
    if len(events)!=expected_rows:raise RuntimeError(f"event count {len(events)} != manifest {expected_rows}")

    obs_by_mint=defaultdict(list)
    for e in events:
        o=price_observation_from_event(e)
        if o is not None:obs_by_mint[o.mint].append(o)

    regimes=[];rr={}
    for row in selection_rows:
        role=str(row["role"]);params=parameter_set_from_locked_row(row)
        candidates,summary=extract_candidates(events,params)
        if summary.run_count!=summary.accounted_run_count:raise RuntimeError(f"{role}: terminal accounting")
        if summary.candidate_count!=summary.candidate_state_count:raise RuntimeError(f"{role}: candidate lifecycle")
        statuses=Counter();paths=[]
        for c in candidates:
            path=replay.build_path(c.reference,obs_by_mint.get(c.signal.mint,()),horizon_ms=300000)
            statuses[path.status.value]+=1
            paths.append({"candidate_id":c.reference.candidate_id,"mint":c.signal.mint,
                          "signal_at":c.reference.signal_at.isoformat(),"status":path.status.value,
                          "future_observation_count":path.future_observation_count,
                          "fresh_same_identity_count":path.fresh_same_identity_count,
                          "gap_recovery_count":path.gap_recovery_count,
                          "identity_mismatch_count":path.identity_mismatch_count,
                          "continuity_break_count":path.continuity_break_count,
                          "peak_bps":path.peak_bps,"trough_bps":path.trough_bps})
        clean=statuses.get(ExitPathStatus.CLEAN.value,0);total=len(candidates);rate=clean/total if total else 0.0
        regimes.append({"role":role,"parameter_set_id":params.parameter_set_id,
                        "run_count":summary.run_count,"candidate_count":total,
                        "clean_5m_count":clean,"clean_5m_rate":rate,
                        "path_status_counts":dict(sorted(statuses.items())),
                        "candidate_paths":paths})
        if role.startswith("ROBUST_"):
            rr[role]=RegimeReadiness(role,total,clean,rate)
    rd=evaluate(rr)
    return {"schema_version":SCHEMA_VERSION,"freeze_id":freeze_id,"dataset_role":dataset_role,
            "untouched_oos":False,"source":{"freeze_db_sha256":file_sha256(freeze_db),
            "freeze_manifest_sha256":file_sha256(freeze_manifest),
            "coverage_snapshot_sha256":file_sha256(coverage_snapshot),
            "entry_selection_sha256":file_sha256(selection_path)},
            "entry_parameters_fixed":True,"deferred_hypotheses_in_mainline":False,
            "exit_threshold_optimization_performed":False,"regimes":regimes,
            "readiness":{"policy_id":POLICY["policy_id"],"tier":rd.tier.value,
                         "all_robust_roles_present":rd.all_robust_roles_present,
                         "minimum_clean_5m_count":rd.minimum_clean_5m_count,
                         "minimum_clean_5m_rate":rd.minimum_clean_5m_rate,
                         "reasons":list(rd.reasons)},
            "profitability_claim_allowed":False}
