from __future__ import annotations

import argparse, json, os
from pathlib import Path
import sys

PROJECT_ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(PROJECT_ROOT/"src"))

from phase2.data_coverage_root_cause_v0_1_1 import (
    iso_to_us, nearest_observation_before_us, read_control_events,
    reconstruct_collector_coverage, us_to_iso,
)
from phase2.phase1_readonly_adapter_v0_1 import (
    Phase1ReadOnlyAdapterV01, BOT_TRUTH_SOURCE_PREFIXES,
)
from phase3.devfreeze0002_design_v0_2 import (
    CleanInterval, REQUIRED_POST_T0_MS, eligible_interval_for_t0,
)

SCHEMA_VERSION="P3-DEVFREEZE-PREVIEW-0.2.1"

def source_sql(alias=""):
    p=(alias+".") if alias else ""
    return (
        "("+" OR ".join(f"{p}source_decoded_file LIKE ?" for _ in BOT_TRUTH_SOURCE_PREFIXES)+")",
        tuple(x+"%" for x in BOT_TRUTH_SOURCE_PREFIXES),
    )

def global_first_tradables(conn,session_start_us,session_end_us):
    where,args=source_sql()
    rows=conn.execute(f"""
        WITH ranked AS (
          SELECT rowid AS p1_rowid,mint,decoded_at_utc,source_decoded_file,
                 ROW_NUMBER() OVER (
                   PARTITION BY mint ORDER BY decoded_at_utc ASC,rowid ASC
                 ) rn
          FROM pump_events
          WHERE mint IS NOT NULL
            AND event_type IN ('BUY','SELL')
            AND event_key IS NOT NULL
            AND signature IS NOT NULL
            AND pump_timestamp IS NOT NULL
            AND decoded_at_utc IS NOT NULL
            AND slot IS NOT NULL
            AND {where}
        )
        SELECT mint,decoded_at_utc,source_decoded_file,p1_rowid
        FROM ranked
        WHERE rn=1
          AND decoded_at_utc>=?
          AND decoded_at_utc<=?
          AND source_decoded_file NOT LIKE 'GAP_RECONCILIATION_%'
        ORDER BY decoded_at_utc,mint
    """,(*args,us_to_iso(session_start_us),us_to_iso(session_end_us))).fetchall()
    return [dict(r) for r in rows]

def atomic(path,payload):
    path.parent.mkdir(parents=True,exist_ok=True)
    tmp=path.with_suffix(path.suffix+".tmp")
    with tmp.open("w",encoding="utf-8",newline="\n") as f:
        json.dump(payload,f,indent=2,sort_keys=True)
        f.write("\n");f.flush();os.fsync(f.fileno())
    os.replace(tmp,path)

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--production-db",default=str(PROJECT_ROOT/"data/db/tradingbot.sqlite3"))
    ap.add_argument("--out",default=str(
        PROJECT_ROOT/"data/research/phase3/EXP-0007/DEV-FREEZE-0002_candidate_preview_v0_2_1.json"
    ))
    args=ap.parse_args()
    db=Path(args.production_db).resolve()
    if not db.exists():raise FileNotFoundError(db)

    before=(db.stat().st_size,db.stat().st_mtime_ns)
    conn=Phase1ReadOnlyAdapterV01.open_readonly(db)
    try:
        conn.execute("BEGIN")
        controls=read_control_events(conn)
        intervals,sessions,gaps,summary=reconstruct_collector_coverage(
            controls,
            nearest_before=lambda cutoff_us,after_us:nearest_observation_before_us(
                conn,cutoff_us,after_us=after_us
            ),
        )
        if not sessions:raise RuntimeError("no reconstructed collector sessions")
        latest=max(sessions,key=lambda s:int(s["session_id"]))
        sid=int(latest["session_id"])
        start_us=int(latest["start_at_us"])
        end_us=int(latest["stop_at_us"] or start_us)
        active=[
            CleanInterval(
                session_id=i.session_id,start_us=int(i.start_us),end_us=int(i.end_us),
                uncertain_close=bool(i.uncertain_close),
            )
            for i in intervals if i.session_id==sid
        ]
        if not active:raise RuntimeError("latest session has no active intervals")
        t0_rows=global_first_tradables(conn,start_us,end_us)
        eligible=[]
        for r in t0_rows:
            t0_us=iso_to_us(str(r["decoded_at_utc"]))
            iv=eligible_interval_for_t0(
                t0_us=t0_us,required_post_t0_ms=REQUIRED_POST_T0_MS,intervals=active
            )
            if iv is not None:
                eligible.append({
                    **r,"t0_us":t0_us,
                    "clean_interval_start_utc":us_to_iso(iv.start_us),
                    "clean_interval_end_utc":us_to_iso(iv.end_us),
                })
        session_gaps=[g for g in gaps if int(g["session_id"])==sid]
        active_seconds=sum(max(0,i.end_us-i.start_us) for i in active)/1e6
        wall_seconds=max(0,end_us-start_us)/1e6
        longest=max((i.end_us-i.start_us for i in active),default=0)/1e6
        clean10=sum(1 for i in active if i.end_us-i.start_us>=REQUIRED_POST_T0_MS*1000)
        conn.rollback()
    finally:
        conn.close()
    after=(db.stat().st_size,db.stat().st_mtime_ns)

    provisional=str(latest["stop_kind"])=="EOF_UNCERTAIN"
    payload={
        "schema_version":SCHEMA_VERSION,
        "design_id":"DEV-FREEZE-0002-DESIGN-002",
        "freeze_id":"DEV-FREEZE-0002",
        "preview_only":True,
        "production_db_read_only":True,
        "production_db_changed_during_preview":before!=after,
        "latest_session":{
            "session_id":sid,"version":latest["version"],
            "start_utc":latest["start_at_utc"],"observed_through_utc":latest["stop_at_utc"],
            "stop_kind":latest["stop_kind"],"provisional_close":provisional,
            "wall_hours":wall_seconds/3600,"active_hours":active_seconds/3600,
            "active_ratio":0 if wall_seconds<=0 else active_seconds/wall_seconds,
            "active_intervals":len(active),"clean_intervals_at_least_10m":clean10,
            "explicit_gaps":len(session_gaps),
            "explicit_gap_seconds":sum(float(g["gap_seconds"]) for g in session_gaps),
            "longest_clean_interval_minutes":longest/60,
        },
        "cohort_preview":{
            "global_first_normalizable_tradables_inside_session":len(t0_rows),
            "projected_eligible_tokens":len(eligible),
            "required_tail_after_t0_minutes":10,
            "selection_uses_normalizable_bot_truth_requirements":True,
            "strategy_signal_used_for_selection":False,
            "future_outcome_filter":False,
        },
        "immutable_freeze_allowed_now":not provisional and before==after,
        "eligible_preview_first_20":eligible[:20],
        "note":"Each token requires global normalizable BOT_TRUTH t0 and full [t0,t0+10m] inside one clean active interval.",
    }
    out=Path(args.out).resolve();atomic(out,payload)

    print("DEV-FREEZE-0002 FINAL CANDIDATE PREVIEW v0.2.1")
    print("="*76)
    print(f"Session id                   : {sid}")
    print(f"Session wall duration        : {wall_seconds/3600:.2f} h")
    print(f"Session active duration      : {active_seconds/3600:.2f} h")
    print(f"Active/wall ratio            : {100*payload['latest_session']['active_ratio']:.2f}%")
    print(f"Explicit gaps                : {len(session_gaps)}")
    print(f"Explicit gap time            : {payload['latest_session']['explicit_gap_seconds']:.2f} s")
    print(f"Global normalizable t0s      : {len(t0_rows)}")
    print(f"Projected eligible tokens    : {len(eligible)}")
    print(f"Latest provisional close     : {'YES' if provisional else 'NO'}")
    print(f"DB changed during preview    : {'YES' if before!=after else 'NO'}")
    print(f"Immutable freeze now         : {'ALLOWED' if payload['immutable_freeze_allowed_now'] else 'BLOCKED'}")
    print("Strategy outcomes inspected  : NO")
    print("Production DB opened         : READ-ONLY")
    print("Freeze created               : NO")
    print(f"Preview JSON                 : {out}")
    print("="*76)
    print("RESULT: PASS")
if __name__=="__main__":main()
