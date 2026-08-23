from __future__ import annotations

import argparse,json,os
from pathlib import Path
import sys

PROJECT_ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(PROJECT_ROOT/"src"))

from phase2.data_coverage_root_cause_v0_1_1 import (
    iso_to_us,nearest_observation_before_us,read_control_events,
    reconstruct_collector_coverage,us_to_iso,
)
from phase2.phase1_readonly_adapter_v0_1 import BOT_TRUTH_SOURCE_PREFIXES,Phase1ReadOnlyAdapterV01
from phase2.quote_aware_price_v0_1 import Phase1QuoteAwareAdapterV01
from phase3.devfreeze0002_closed_prefix_design_v0_2_1 import (
    DESIGN_ID,Interval,REQUIRED_POST_T0_MS,eligible_interval,maximal_definite_prefix_cutoff,
)
from phase3.locked_entry_readiness_v0_1_1 import (
    EXPECTED_ROLES,load_locked_selection,parameter_set_from_locked_row,
)
from phase3.phase2_outcome_adapter_v0_1_2 import extract_candidates

ENTRY_WINDOW_MS=300_000

def src_sql(alias=""):
    p=(alias+".") if alias else ""
    return (
        "("+" OR ".join(f"{p}source_decoded_file LIKE ?" for _ in BOT_TRUTH_SOURCE_PREFIXES)+")",
        tuple(x+"%" for x in BOT_TRUTH_SOURCE_PREFIXES),
    )

def global_first_t0s(conn,start_us,end_us):
    where,args=src_sql()
    rows=conn.execute(f"""
      WITH ranked AS (
        SELECT rowid p1_rowid,mint,decoded_at_utc,source_decoded_file,
               ROW_NUMBER() OVER(PARTITION BY mint ORDER BY decoded_at_utc,rowid) rn
        FROM pump_events
        WHERE mint IS NOT NULL
          AND event_type IN ('BUY','SELL')
          AND event_key IS NOT NULL AND signature IS NOT NULL
          AND pump_timestamp IS NOT NULL AND decoded_at_utc IS NOT NULL AND slot IS NOT NULL
          AND {where}
      )
      SELECT mint,decoded_at_utc,source_decoded_file,p1_rowid
      FROM ranked
      WHERE rn=1 AND decoded_at_utc>=? AND decoded_at_utc<=?
        AND source_decoded_file NOT LIKE 'GAP_RECONCILIATION_%'
      ORDER BY decoded_at_utc,mint
    """,(*args,us_to_iso(start_us),us_to_iso(end_us))).fetchall()
    return [dict(r) for r in rows]

def fetch_entry_rows(conn,eligible,start_us,chunk=300):
    if not eligible:return []
    where,args_source=src_sql("p")
    rows=[]
    start_iso=us_to_iso(start_us)
    for off in range(0,len(eligible),chunk):
        batch=eligible[off:off+chunk]
        values=",".join("(?,?)" for _ in batch)
        vals=[]
        for r in batch:
            vals.extend([str(r["mint"]),us_to_iso(int(r["t0_us"])+ENTRY_WINDOW_MS*1000)])
        part=conn.execute(f"""
          WITH eligible(mint,deadline_utc) AS (VALUES {values})
          SELECT p.rowid p1_rowid,p.*
          FROM pump_events p JOIN eligible e ON e.mint=p.mint
          WHERE p.event_type IN ('LAUNCH','BUY','SELL')
            AND p.event_key IS NOT NULL AND p.signature IS NOT NULL
            AND p.pump_timestamp IS NOT NULL AND p.decoded_at_utc IS NOT NULL AND p.slot IS NOT NULL
            AND p.decoded_at_utc>=? AND p.decoded_at_utc<=e.deadline_utc
            AND {where}
          ORDER BY p.decoded_at_utc,p.rowid
        """,(*vals,start_iso,*args_source)).fetchall()
        rows.extend(part)
    rows.sort(key=lambda r:(Phase1ReadOnlyAdapterV01._iso_to_us(str(r["decoded_at_utc"])),int(r["p1_rowid"])))
    return rows

def atomic(p,x):
    p.parent.mkdir(parents=True,exist_ok=True);t=p.with_suffix(p.suffix+".tmp")
    with t.open("w",encoding="utf-8",newline="\n") as f:
        json.dump(x,f,indent=2,sort_keys=True);f.write("\n");f.flush();os.fsync(f.fileno())
    os.replace(t,p)

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--production-db",default=str(PROJECT_ROOT/"data/db/tradingbot.sqlite3"))
    ap.add_argument("--selection",default=str(PROJECT_ROOT/"data/research/phase3/EXP-0005/EXP-0005_D_selection_v0_1.json"))
    ap.add_argument("--out",default=str(PROJECT_ROOT/"data/research/phase3/EXP-0007/DEV-FREEZE-0002_closed_prefix_preview_v0_2_1.json"))
    args=ap.parse_args()
    db=Path(args.production_db).resolve();sel=Path(args.selection).resolve();out=Path(args.out).resolve()
    for p in (db,sel):
        if not p.exists():raise FileNotFoundError(p)

    selection,selection_rows=load_locked_selection(sel)
    before=(db.stat().st_size,db.stat().st_mtime_ns)
    conn=Phase1ReadOnlyAdapterV01.open_readonly(db)
    try:
        conn.execute("BEGIN")
        Phase1QuoteAwareAdapterV01.validate_schema(conn)
        controls=read_control_events(conn)
        intervals,sessions,gaps,summary=reconstruct_collector_coverage(
            controls,
            nearest_before=lambda cutoff_us,after_us:nearest_observation_before_us(conn,cutoff_us,after_us=after_us),
        )
        if not sessions:raise RuntimeError("no reconstructed collector sessions")
        latest=max(sessions,key=lambda s:int(s["session_id"]))
        if str(latest["stop_kind"])!="EOF_UNCERTAIN":
            raise RuntimeError(f"expected EOF_UNCERTAIN salvage case, got {latest['stop_kind']}")
        sid=int(latest["session_id"]);start_us=int(latest["start_at_us"])
        ivs=[Interval(i.session_id,int(i.start_us),int(i.end_us),bool(i.uncertain_close))
             for i in intervals if i.session_id==sid]
        cutoff_us=maximal_definite_prefix_cutoff(ivs)
        definite=[i for i in ivs if not i.uncertain_close and i.end_us<=cutoff_us]
        uncertain=[i for i in ivs if i.uncertain_close]
        t0s=global_first_t0s(conn,start_us,cutoff_us)
        eligible=[]
        for r in t0s:
            t0_us=iso_to_us(str(r["decoded_at_utc"]))
            iv=eligible_interval(
                t0_us=t0_us,required_post_t0_ms=REQUIRED_POST_T0_MS,
                intervals=ivs,cutoff_us=cutoff_us
            )
            if iv is not None:
                eligible.append({**r,"t0_us":t0_us})
        strategy_rows=fetch_entry_rows(conn,eligible,start_us)
        events,skipped=Phase1QuoteAwareAdapterV01.normalize_rows(strategy_rows)
        if skipped:raise RuntimeError(f"normalization skips prevent exact count: {skipped}")

        regimes=[]
        for row in selection_rows:
            role=str(row["role"]);params=parameter_set_from_locked_row(row)
            candidates,s=extract_candidates(events,params)
            if s.run_count!=s.accounted_run_count or s.candidate_count!=s.candidate_state_count:
                raise RuntimeError(f"{role}: terminal accounting mismatch")
            regimes.append({"role":role,"parameter_set_id":params.parameter_set_id,
                            "candidate_count":len(candidates),"run_count":s.run_count})
        prefix_gaps=[g for g in gaps if int(g["session_id"])==sid and int(g["gap_end_us"])<=cutoff_us]
        conn.rollback()
    finally:
        conn.close()
    after=(db.stat().st_size,db.stat().st_mtime_ns)

    cutoff_iso=us_to_iso(cutoff_us)
    payload={
      "schema_version":"P3-DEVFREEZE-CLOSED-PREFIX-PREVIEW-0.2.1",
      "design_id":DESIGN_ID,"freeze_id":"DEV-FREEZE-0002","preview_only":True,
      "source_session":{"session_id":sid,"state":latest["stop_kind"],
                        "start_utc":latest["start_at_utc"],"observed_through_utc":latest["stop_at_utc"]},
      "closed_prefix":{"start_utc":us_to_iso(start_us),"cutoff_utc":cutoff_iso,
                       "duration_hours":(cutoff_us-start_us)/1e6/3600,
                       "definite_clean_intervals":len(definite),
                       "discarded_uncertain_intervals":len(uncertain),
                       "explicit_gaps_retained":len(prefix_gaps)},
      "cohort":{"global_normalizable_t0s_in_prefix":len(t0s),
                "projected_eligible_tokens":len(eligible),
                "required_tail_after_t0_minutes":10},
      "entry_only_counts":regimes,
      "forward_outcomes_inspected":False,"exit_paths_built":False,"pnl_inspected":False,
      "production_db_read_only":True,"production_db_changed_during_preview":before!=after,
      "immutable_prefix_freeze_candidate":before==after,
      "background_queues_required_to_be_empty":False,
      "note":"The entire EOF_UNCERTAIN tail after cutoff is discarded. Explicit gaps before cutoff remain non-observable and are never ignored.",
    }
    atomic(out,payload)

    print("DEV-FREEZE-0002 CLOSED-PREFIX PREVIEW v0.2.1")
    print("="*78)
    print(f"Source session id            : {sid}")
    print(f"Source session state         : {latest['stop_kind']}")
    print(f"Source observed through      : {latest['stop_at_utc']}")
    print(f"Safe prefix cutoff           : {cutoff_iso}")
    print(f"Safe prefix duration         : {payload['closed_prefix']['duration_hours']:.2f} h")
    print(f"Definite clean intervals     : {len(definite)}")
    print(f"Discarded uncertain intervals: {len(uncertain)}")
    print(f"Explicit gaps retained       : {len(prefix_gaps)}")
    print(f"Projected eligible tokens    : {len(eligible)}")
    print("-"*78)
    for r in regimes:
        print(f"{r['role']:10s} candidates          : {r['candidate_count']}")
    print("-"*78)
    print("Forward outcomes inspected   : NO")
    print("Exit paths built             : NO")
    print("PnL inspected                : NO")
    print(f"DB changed during preview    : {'YES' if before!=after else 'NO'}")
    print(f"Prefix freeze candidate      : {'YES' if before==after else 'NO'}")
    print("Production DB opened         : READ-ONLY")
    print(f"Preview JSON                 : {out}")
    print("="*78)
    print("RESULT: PASS")

if __name__=="__main__":
    main()
