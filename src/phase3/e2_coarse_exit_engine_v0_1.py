from __future__ import annotations

from collections import defaultdict
from datetime import timedelta
import math
from pathlib import Path
from statistics import fmean
from typing import Any, Iterable

from phase2.data_coverage_root_cause_v0_1_1 import us_to_iso
from phase2.phase1_readonly_adapter_v0_1 import Phase1ReadOnlyAdapterV01
from phase2.quote_aware_price_v0_1 import Phase1QuoteAwareAdapterV01
from phase3.collector_coverage_v0_2 import CollectorCoverageSnapshotV02
from phase3.e2_coarse_exit_design_v0_1 import HARD_STOP_BPS,TIME_STOP_MS
from phase3.exit_path_replay_v0_1 import ExitPathReplayV01,ExitPathStatus
from phase3.locked_entry_readiness_v0_1_2 import (
    load_locked_selection,manifest_identity,parameter_set_from_locked_row,
)
from phase3.phase2_outcome_adapter_v0_1_2 import (
    extract_candidates,price_observation_from_event,
)

ENTRY_WINDOW_MS=300_000

def nearest_rank(values:list[int],p:int)->int|None:
    if not values:return None
    s=sorted(values)
    rank=max(1,math.ceil((p/100)*len(s)))
    return s[rank-1]

def summarize(values:list[int])->dict[str,Any]:
    if not values:
        return {
            "n":0,"p10":None,"p25":None,"p50":None,"p75":None,"p90":None,
            "mean":None,"positive_rate":None,"negative_rate":None,
        }
    return {
        "n":len(values),
        "p10":nearest_rank(values,10),
        "p25":nearest_rank(values,25),
        "p50":nearest_rank(values,50),
        "p75":nearest_rank(values,75),
        "p90":nearest_rank(values,90),
        "mean":fmean(values),
        "positive_rate":sum(v>0 for v in values)/len(values),
        "negative_rate":sum(v<0 for v in values)/len(values),
    }

def fetch_tokens(conn):
    rows=conn.execute("""
        SELECT mint,t0_observed_at_utc FROM freeze_tokens
        ORDER BY t0_observed_at_utc,mint
    """).fetchall()
    return [
        {"mint":str(r["mint"]),
         "t0_us":Phase1ReadOnlyAdapterV01._iso_to_us(str(r["t0_observed_at_utc"]))}
        for r in rows
    ]

def fetch_entry_rows(conn,tokens,chunk=300):
    rows=[];batches=(len(tokens)+chunk-1)//chunk
    for bi,off in enumerate(range(0,len(tokens),chunk),1):
        batch=tokens[off:off+chunk]
        values=",".join("(?,?)" for _ in batch);args=[]
        for t in batch:
            args.extend([t["mint"],us_to_iso(t["t0_us"]+ENTRY_WINDOW_MS*1000)])
        part=conn.execute(f"""
          WITH eligible(mint,deadline_utc) AS (VALUES {values})
          SELECT p.rowid p1_rowid,p.*
          FROM pump_events p JOIN eligible e ON e.mint=p.mint
          WHERE p.event_type IN ('LAUNCH','BUY','SELL')
            AND p.decoded_at_utc<=e.deadline_utc
          ORDER BY p.decoded_at_utc,p.rowid
        """,tuple(args)).fetchall()
        rows.extend(part)
        if bi==1 or bi==batches or bi%5==0:
            print(f"[ENTRY] batch {bi}/{batches} rows={len(rows)}",flush=True)
    rows.sort(key=lambda r:(Phase1ReadOnlyAdapterV01._iso_to_us(str(r["decoded_at_utc"])),int(r["p1_rowid"])))
    return rows

def fetch_path_rows(conn,mints:set[str],chunk=300):
    ms=sorted(mints);rows=[];batches=(len(ms)+chunk-1)//chunk
    for bi,off in enumerate(range(0,len(ms),chunk),1):
        batch=ms[off:off+chunk]
        ph=",".join("?" for _ in batch)
        part=conn.execute(f"""
          SELECT rowid p1_rowid,* FROM pump_events
          WHERE mint IN ({ph}) AND event_type IN ('LAUNCH','BUY','SELL')
          ORDER BY decoded_at_utc,rowid
        """,tuple(batch)).fetchall()
        rows.extend(part)
        if bi==1 or bi==batches or bi%5==0:
            print(f"[PATH] batch {bi}/{batches} rows={len(rows)}",flush=True)
    rows.sort(key=lambda r:(Phase1ReadOnlyAdapterV01._iso_to_us(str(r["decoded_at_utc"])),int(r["p1_rowid"])))
    return rows

def point_at_or_before(path,horizon_ms):
    target=path.signal_at+timedelta(milliseconds=horizon_ms)
    chosen=None
    for p in path.points:
        if p.observed_at<=target:
            chosen=p
        else:
            break
    return chosen,target

def evaluate_e2(*,freeze_db:Path,manifest_path:Path,coverage_path:Path,
                selection_path:Path,readiness_path:Path)->dict[str,Any]:
    import json
    readiness=json.loads(readiness_path.read_text(encoding="utf-8"))
    if readiness["readiness"]["tier"]!="E2_COARSE_EXIT_SEARCH_READY":
        raise RuntimeError("E2 readiness gate not satisfied")
    expected={r["role"]:(int(r["candidate_count"]),int(r["clean_5m_count"]))
              for r in readiness["regimes"]}

    manifest=json.loads(manifest_path.read_text(encoding="utf-8"))
    freeze_id,role,rs,re,nrows=manifest_identity(manifest)
    cov=CollectorCoverageSnapshotV02.load(coverage_path)
    cov.validate_binding(freeze_id=freeze_id,range_start=rs,range_end=re)
    replay=ExitPathReplayV01(cov.provider())
    _,selection_rows=load_locked_selection(selection_path)

    print("[1/5] Open immutable freeze...",flush=True)
    conn=Phase1ReadOnlyAdapterV01.open_readonly(freeze_db)
    try:
        Phase1QuoteAwareAdapterV01.validate_schema(conn)
        if conn.execute("PRAGMA quick_check").fetchone()[0]!="ok":
            raise RuntimeError("freeze quick_check failed")
        tokens=fetch_tokens(conn)
        entry_raw=fetch_entry_rows(conn,tokens)
        entry_events,skipped=Phase1QuoteAwareAdapterV01.normalize_rows(entry_raw)
        if skipped:raise RuntimeError(f"entry normalization skips: {skipped}")

        candidates={}
        print(f"[2/5] Locked entry replay over {len(entry_events)} rows...",flush=True)
        for row in selection_rows:
            rr=str(row["role"]);params=parameter_set_from_locked_row(row)
            cs,s=extract_candidates(entry_events,params)
            if len(cs)!=expected[rr][0]:
                raise RuntimeError(f"{rr} candidate count mismatch {len(cs)} != {expected[rr][0]}")
            candidates[rr]=cs
            print(f"  {rr}: {len(cs)} candidates PASS",flush=True)

        candidate_mints={c.signal.mint for cs in candidates.values() for c in cs}
        print(f"[3/5] Fetch full paths for {len(candidate_mints)} candidate mints...",flush=True)
        path_raw=fetch_path_rows(conn,candidate_mints)
    finally:
        conn.close()

    path_events,skipped=Phase1QuoteAwareAdapterV01.normalize_rows(path_raw)
    if skipped:raise RuntimeError(f"path normalization skips: {skipped}")
    obs=defaultdict(list)
    for e in path_events:
        o=price_observation_from_event(e)
        if o is not None:obs[o.mint].append(o)

    print("[4/5] Build clean 5m paths...",flush=True)
    paths={}
    for rr,cs in candidates.items():
        clean=[]
        for i,c in enumerate(cs,1):
            p=replay.build_path(c.reference,obs.get(c.signal.mint,()),horizon_ms=300_000)
            if p.status==ExitPathStatus.CLEAN:clean.append(p)
        if len(clean)!=expected[rr][1]:
            raise RuntimeError(f"{rr} clean count mismatch {len(clean)} != {expected[rr][1]}")
        paths[rr]=clean
        print(f"  {rr}: {len(clean)} clean 5m paths PASS",flush=True)

    print("[5/5] Evaluate locked E2 families...",flush=True)
    results=[]
    for rr,clean_paths in paths.items():
        # Hard-stop family. No hit -> research terminal mark at/before 5m.
        for stop in HARD_STOP_BPS:
            exits=[];delays=[];ages=[];hits=0
            for p in clean_paths:
                hit=None
                for pt in p.points:
                    if pt.return_bps<=stop:
                        hit=pt;break
                if hit is not None:
                    hits+=1;exits.append(hit.return_bps)
                    delays.append(round((hit.observed_at-p.signal_at).total_seconds()*1000))
                    ages.append(0)
                else:
                    mark,target=point_at_or_before(p,300_000)
                    if mark is None:continue
                    exits.append(mark.return_bps)
                    delays.append(round((mark.observed_at-p.signal_at).total_seconds()*1000))
                    ages.append(round((target-mark.observed_at).total_seconds()*1000))
            s=summarize(exits)
            results.append({
                "role":rr,"family":"HARD_STOP","variant_id":f"SL{abs(stop)//100:02d}",
                "threshold_bps":stop,"time_stop_ms":None,
                "clean_path_count":len(clean_paths),"scored_count":len(exits),
                "unscored_count":len(clean_paths)-len(exits),
                "stop_hit_count":hits,"stop_hit_rate":hits/len(clean_paths) if clean_paths else None,
                "exit_bps":s,
                "exit_delay_p50_ms":nearest_rank(delays,50),
                "mark_age_p50_ms":nearest_rank(ages,50),
            })

        # Time-stop family. No hard stop. Latest observed same-identity mark <= target.
        for horizon in TIME_STOP_MS:
            exits=[];delays=[];ages=[]
            for p in clean_paths:
                mark,target=point_at_or_before(p,horizon)
                if mark is None:continue
                exits.append(mark.return_bps)
                delays.append(round((mark.observed_at-p.signal_at).total_seconds()*1000))
                ages.append(round((target-mark.observed_at).total_seconds()*1000))
            s=summarize(exits)
            results.append({
                "role":rr,"family":"TIME_STOP",
                "variant_id":f"T{horizon//1000}s",
                "threshold_bps":None,"time_stop_ms":horizon,
                "clean_path_count":len(clean_paths),"scored_count":len(exits),
                "unscored_count":len(clean_paths)-len(exits),
                "stop_hit_count":None,"stop_hit_rate":None,
                "exit_bps":s,
                "exit_delay_p50_ms":nearest_rank(delays,50),
                "mark_age_p50_ms":nearest_rank(ages,50),
            })

    return {
        "schema_version":"P3-E2-COARSE-EXIT-RESULT-0.1",
        "experiment_id":"EXP-0008",
        "design_id":"PHASE-3-E2-COARSE-EXIT-001",
        "freeze_id":freeze_id,
        "dataset_role":role,
        "untouched_oos":False,
        "entry_parameters_fixed":True,
        "readiness_tier":"E2_COARSE_EXIT_SEARCH_READY",
        "families_combined":False,
        "profit_target_tested":False,
        "trailing_tested":False,
        "fees_modeled":False,"slippage_modeled":False,"latency_modeled":False,
        "gross_reference_returns_only":True,
        "results":results,
        "profitability_claim_allowed":False,
    }
