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

def source_sql():
    where="("+" OR ".join("source_decoded_file LIKE ?" for _ in BOT_TRUTH_SOURCE_PREFIXES)+")"
    return where, tuple(p+"%" for p in BOT_TRUTH_SOURCE_PREFIXES)

def global_first_tradables(conn, session_start_us, session_end_us):
    where,args=source_sql()
    rows=conn.execute(f"""
        WITH ranked AS (
          SELECT rowid AS p1_rowid,mint,decoded_at_utc,source_decoded_file,
                 ROW_NUMBER() OVER (
                   PARTITION BY mint ORDER BY decoded_at_utc ASC,rowid ASC
                 ) AS rn
          FROM pump_events
          WHERE mint IS NOT NULL
            AND event_type IN ('BUY','SELL')
            AND decoded_at_utc IS NOT NULL
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

def write_atomic(path,payload):
    path.parent.mkdir(parents=True,exist_ok=True)
    tmp=path.with_suffix(path.suffix+".tmp")
    with tmp.open("w",encoding="utf-8",newline="\n") as f:
        json.dump(payload,f,indent=2,sort_keys=True)
        f.write("\n"); f.flush(); os.fsync(f.fileno())
    os.replace(tmp,path)

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--production-db",default=str(PROJECT_ROOT/"data/db/tradingbot.sqlite3"))
    ap.add_argument("--out",default=str(
        PROJECT_ROOT/"data/research/phase3/EXP-0007/DEV-FREEZE-0002_candidate_preview_v0_2.json"
    ))
    args=ap.parse_args()
    db=Path(args.production_db).resolve()
    if not db.exists(): raise FileNotFoundError(db)

    before=(db.stat().st_size,db.stat().st_mtime_ns)
    conn=Phase1ReadOnlyAdapterV01.open_readonly(db)
    try:
        controls=read_control_events(conn)
        intervals,sessions,gaps,summary=reconstruct_collector_coverage(
            controls,
            nearest_before=lambda cutoff_us,after_us:nearest_observation_before_us(
                conn,cutoff_us,after_us=after_us
            ),
        )
        if not sessions:
            raise RuntimeError("no reconstructed collector sessions")
        latest=max(sessions,key=lambda s:int(s["session_id"]))
        sid=int(latest["session_id"])
        session_start=int(latest["start_at_us"])
        session_end=int(latest["stop_at_us"] or session_start)

        session_intervals=[
            CleanInterval(
                session_id=i.session_id,
                start_us=int(i.start_us),
                end_us=int(i.end_us),
                uncertain_close=bool(i.uncertain_close),
            )
            for i in intervals if i.session_id==sid
        ]
        if not session_intervals:
            raise RuntimeError("latest session has no active intervals")

        t0_rows=global_first_tradables(conn,session_start,session_end)
        eligible=[]
        excluded_cross_gap_or_tail=0
        for row in t0_rows:
            t0_us=iso_to_us(str(row["decoded_at_utc"]))
            interval=eligible_interval_for_t0(
                t0_us=t0_us,
                required_post_t0_ms=REQUIRED_POST_T0_MS,
                intervals=session_intervals,
            )
            if interval is None:
                excluded_cross_gap_or_tail += 1
                continue
            eligible.append({
                **row,
                "t0_us":t0_us,
                "clean_interval_start_utc":us_to_iso(interval.start_us),
                "clean_interval_end_utc":us_to_iso(interval.end_us),
                "clean_interval_uncertain_close":interval.uncertain_close,
            })

        s_gaps=[g for g in gaps if int(g["session_id"])==sid]
        clean_10m_intervals=sum(
            1 for i in session_intervals
            if (i.end_us-i.start_us) >= REQUIRED_POST_T0_MS*1000
        )
        longest=max((i.end_us-i.start_us for i in session_intervals),default=0)/1e6
        active_seconds=sum(max(0,i.end_us-i.start_us) for i in session_intervals)/1e6
        wall_seconds=max(0,(session_end-session_start)/1e6)
    finally:
        conn.close()
    after=(db.stat().st_size,db.stat().st_mtime_ns)

    provisional=str(latest["stop_kind"])=="EOF_UNCERTAIN"
    payload={
        "schema_version":"P3-DEVFREEZE-PREVIEW-0.2",
        "design_id":"DEV-FREEZE-0002-DESIGN-002",
        "freeze_id":"DEV-FREEZE-0002",
        "preview_only":True,
        "production_db_read_only":True,
        "production_db_changed_during_preview":before!=after,
        "latest_session":{
            "session_id":sid,
            "version":latest["version"],
            "start_utc":latest["start_at_utc"],
            "observed_through_utc":latest["stop_at_utc"],
            "stop_kind":latest["stop_kind"],
            "provisional_close":provisional,
            "wall_hours":wall_seconds/3600,
            "active_hours":active_seconds/3600,
            "active_ratio":0.0 if wall_seconds<=0 else active_seconds/wall_seconds,
            "active_intervals":len(session_intervals),
            "clean_intervals_at_least_10m":clean_10m_intervals,
            "explicit_gaps":len(s_gaps),
            "explicit_gap_seconds":sum(float(g["gap_seconds"]) for g in s_gaps),
            "longest_clean_interval_minutes":longest/60,
        },
        "cohort_preview":{
            "global_first_tradables_inside_session":len(t0_rows),
            "projected_eligible_tokens":len(eligible),
            "excluded_because_10m_window_not_fully_clean":excluded_cross_gap_or_tail,
            "required_tail_after_t0_minutes":10,
            "strategy_signal_used_for_selection":False,
            "future_outcome_filter":False,
        },
        "immutable_freeze_allowed_now":not provisional,
        "eligible_preview_first_20":eligible[:20],
        "note":(
            "Eligibility is session-based but token-specific: the full [t0,t0+10m] "
            "must fit inside one clean active interval. Explicit gaps are never ignored."
        ),
    }
    out=Path(args.out).resolve()
    write_atomic(out,payload)

    print("DEV-FREEZE-0002 CANDIDATE PREVIEW v0.2")
    print("="*76)
    print(f"Session id                   : {sid}")
    print(f"Session wall duration        : {wall_seconds/3600:.2f} h")
    print(f"Session active duration      : {active_seconds/3600:.2f} h")
    print(f"Active/wall ratio            : {100*payload['latest_session']['active_ratio']:.2f}%")
    print(f"Explicit gaps                : {len(s_gaps)}")
    print(f"Explicit gap time            : {payload['latest_session']['explicit_gap_seconds']:.2f} s")
    print(f"Clean intervals >=10m        : {clean_10m_intervals}")
    print(f"Longest clean interval       : {longest/60:.2f} min")
    print("-"*76)
    print(f"Global t0s inside session    : {len(t0_rows)}")
    print(f"Projected eligible tokens    : {len(eligible)}")
    print(f"Excluded by 10m clean guard  : {excluded_cross_gap_or_tail}")
    print("Gap windows ignored          : NO")
    print("Strategy outcomes inspected  : NO")
    print(f"Latest provisional close     : {'YES' if provisional else 'NO'}")
    print(f"Immutable freeze now         : {'ALLOWED' if not provisional else 'BLOCKED (session active/provisional)'}")
    print("Production DB opened         : READ-ONLY")
    print("Production DB mutated        : NO")
    print("Freeze created               : NO")
    print(f"Preview JSON                 : {out}")
    print("="*76)
    print("RESULT: PASS")

if __name__=="__main__":
    main()
