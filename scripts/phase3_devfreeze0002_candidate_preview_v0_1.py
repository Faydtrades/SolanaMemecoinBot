from __future__ import annotations

import argparse, json, os
from dataclasses import asdict
from pathlib import Path
import sys

PROJECT_ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(PROJECT_ROOT/"src"))

from phase2.data_coverage_root_cause_v0_1_1 import (
    nearest_observation_before_us, read_control_events, reconstruct_collector_coverage,
    us_to_iso,
)
from phase2.phase1_readonly_adapter_v0_1 import Phase1ReadOnlyAdapterV01, BOT_TRUTH_SOURCE_PREFIXES
from phase3.devfreeze0002_design_v0_1 import REQUIRED_POST_T0_MS

def subtract_gaps(start,end,gaps):
    pieces=[(start,end)]
    for gs,ge in sorted(gaps):
        nxt=[]
        for a,b in pieces:
            if ge<=a or gs>=b:
                nxt.append((a,b)); continue
            if gs>a:nxt.append((a,min(gs,b)))
            if ge<b:nxt.append((max(ge,a),b))
        pieces=nxt
    return [(a,b) for a,b in pieces if b>a]

def source_sql():
    return "("+" OR ".join("source_decoded_file LIKE ?" for _ in BOT_TRUTH_SOURCE_PREFIXES)+")", tuple(p+"%" for p in BOT_TRUTH_SOURCE_PREFIXES)

def eligible(conn,start_us,end_us):
    cutoff_us=end_us-REQUIRED_POST_T0_MS*1000
    if cutoff_us<start_us:return []
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
    """,(*args,us_to_iso(start_us),us_to_iso(cutoff_us))).fetchall()
    return [dict(r) for r in rows]

def write_atomic(path,payload):
    path.parent.mkdir(parents=True,exist_ok=True)
    tmp=path.with_suffix(path.suffix+".tmp")
    tmp.write_text(json.dumps(payload,indent=2,sort_keys=True)+"\n",encoding="utf-8")
    os.replace(tmp,path)

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--production-db",default=str(PROJECT_ROOT/"data/db/tradingbot.sqlite3"))
    ap.add_argument("--out",default=str(PROJECT_ROOT/"data/research/phase3/EXP-0007/DEV-FREEZE-0002_candidate_preview_v0_1.json"))
    args=ap.parse_args()
    db=Path(args.production_db).resolve()
    if not db.exists():raise FileNotFoundError(db)

    before=(db.stat().st_size,db.stat().st_mtime_ns)
    conn=Phase1ReadOnlyAdapterV01.open_readonly(db)
    try:
        controls=read_control_events(conn)
        intervals,sessions,gaps,summary=reconstruct_collector_coverage(
            controls,
            nearest_before=lambda cutoff_us,after_us:nearest_observation_before_us(
                conn,cutoff_us,after_us=after_us
            )
        )
        gap_pairs=[(int(g["gap_start_us"]),int(g["gap_end_us"])) for g in gaps]
        segments=[]
        for i in intervals:
            overlaps=[g for g in gap_pairs if g[1]>i.start_us and g[0]<i.end_us]
            for a,b in subtract_gaps(i.start_us,i.end_us,overlaps):
                segments.append({
                    "session_id":i.session_id,"start_us":a,"end_us":b,
                    "start_utc":us_to_iso(a),"end_utc":us_to_iso(b),
                    "seconds":(b-a)/1e6,
                    "parent_close_reason":i.close_reason,
                    "uncertain_close":bool(i.uncertain_close),
                })
        if not segments:raise RuntimeError("no reconstructed clean collector segments")
        latest=max(segments,key=lambda x:x["end_us"])
        longest=max(segments,key=lambda x:x["seconds"])
        for s in (latest,longest):
            s["projected_eligible_tokens"]=len(eligible(conn,int(s["start_us"]),int(s["end_us"])))
        chosen=latest
        chosen_tokens=eligible(conn,int(chosen["start_us"]),int(chosen["end_us"]))
    finally:
        conn.close()
    after=(db.stat().st_size,db.stat().st_mtime_ns)

    payload={
        "schema_version":"P3-DEVFREEZE-PREVIEW-0.1",
        "freeze_id":"DEV-FREEZE-0002",
        "preview_only":True,
        "production_db_read_only":True,
        "production_db_changed_during_preview":before!=after,
        "coverage_summary":summary,
        "latest_clean_segment":latest,
        "longest_clean_segment":longest,
        "current_preview_segment":chosen,
        "current_projected_eligible_token_count":len(chosen_tokens),
        "eligibility_tail_guard_minutes":10,
        "immutable_freeze_allowed_from_current_preview":not bool(chosen["uncertain_close"]),
        "note":"If the collector is active, uncertain_close is expected and immutable freeze creation remains forbidden.",
    }
    write_atomic(Path(args.out).resolve(),payload)

    print("DEV-FREEZE-0002 CANDIDATE PREVIEW v0.1")
    print("="*68)
    print(f"Latest clean segment      : {latest['start_utc']} -> {latest['end_utc']}")
    print(f"Latest duration           : {latest['seconds']/3600:.2f} h")
    print(f"Latest provisional close  : {'YES' if latest['uncertain_close'] else 'NO'}")
    print(f"Projected eligible tokens : {latest['projected_eligible_tokens']}")
    print(f"Longest clean segment     : {longest['seconds']/3600:.2f} h")
    print(f"10m tail guard            : YES")
    print(f"Immutable freeze now      : {'ALLOWED' if not latest['uncertain_close'] else 'BLOCKED (collector/session still provisional)'}")
    print("Production DB opened      : READ-ONLY")
    print("Production DB mutated     : NO")
    print("Freeze created            : NO")
    print(f"Preview JSON              : {Path(args.out).resolve()}")
    print("="*68)
    print("RESULT: PASS")
if __name__=="__main__":main()
