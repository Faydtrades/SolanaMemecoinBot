from __future__ import annotations

import argparse,csv,hashlib,json,os,sqlite3
from pathlib import Path
import sys

PROJECT_ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(PROJECT_ROOT/"src"))

from phase2.data_coverage_root_cause_v0_1_1 import (
    iso_to_us,nearest_observation_before_us,read_control_events,
    reconstruct_collector_coverage,us_to_iso,
)
from phase2.phase1_readonly_adapter_v0_1 import (
    Phase1ReadOnlyAdapterV01,BOT_TRUTH_SOURCE_PREFIXES,
)
from phase2.research_freeze_manifest_v0_1 import pump_event_columns,quote_ident,canonical_json
from phase3.devfreeze0002_design_v0_2 import (
    DESIGN_ID,REQUIRED_POST_T0_MS,CleanInterval,eligible_interval_for_t0,
)

def file_sha(path):
    h=hashlib.sha256()
    with path.open("rb") as f:
        for c in iter(lambda:f.read(1024*1024),b""):h.update(c)
    return h.hexdigest()

def source_sql(alias=""):
    p=(alias+".") if alias else ""
    return (
        "("+" OR ".join(f"{p}source_decoded_file LIKE ?" for _ in BOT_TRUTH_SOURCE_PREFIXES)+")",
        tuple(x+"%" for x in BOT_TRUTH_SOURCE_PREFIXES),
    )

def global_first_tradables(conn,start_us,end_us):
    where,args=source_sql()
    return conn.execute(f"""
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
    """,(*args,us_to_iso(start_us),us_to_iso(end_us))).fetchall()

def fetch_rows(conn,mints,start_iso,end_iso,chunk=300):
    if not mints:return []
    where,args_source=source_sql()
    rows=[]
    for off in range(0,len(mints),chunk):
        batch=mints[off:off+chunk]
        ph=",".join("?" for _ in batch)
        part=conn.execute(f"""
            SELECT rowid AS p1_rowid,*
            FROM pump_events
            WHERE mint IN ({ph})
              AND event_type IN ('LAUNCH','BUY','SELL')
              AND event_key IS NOT NULL
              AND signature IS NOT NULL
              AND pump_timestamp IS NOT NULL
              AND decoded_at_utc IS NOT NULL
              AND slot IS NOT NULL
              AND decoded_at_utc>=?
              AND decoded_at_utc<=?
              AND {where}
            ORDER BY rowid
        """,(*batch,start_iso,end_iso,*args_source)).fetchall()
        rows.extend(part)
    rows.sort(key=lambda r:int(r["p1_rowid"]))
    return rows

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--production-db",default=str(PROJECT_ROOT/"data/db/tradingbot.sqlite3"))
    ap.add_argument("--preview",default=str(PROJECT_ROOT/"data/research/phase3/EXP-0007/DEV-FREEZE-0002_candidate_preview_v0_2_1.json"))
    ap.add_argument("--design",default=str(PROJECT_ROOT/"data/research/phase3/EXP-0007/DEV-FREEZE-0002_design_v0_2.json"))
    ap.add_argument("--selection",default=str(PROJECT_ROOT/"data/research/phase3/EXP-0005/EXP-0005_D_selection_v0_1.json"))
    ap.add_argument("--out-dir",default=str(PROJECT_ROOT/"data/research/phase3/dev_freezes/DEV-FREEZE-0002"))
    ap.add_argument("--confirm-create",action="store_true")
    args=ap.parse_args()
    if not args.confirm_create:
        raise RuntimeError("Refusing immutable freeze creation without --confirm-create")

    prod=Path(args.production_db).resolve()
    preview_path=Path(args.preview).resolve()
    design_path=Path(args.design).resolve()
    selection=Path(args.selection).resolve()
    out=Path(args.out_dir).resolve()
    for p in (prod,preview_path,design_path,selection):
        if not p.exists():raise FileNotFoundError(p)
    if out.exists():raise FileExistsError(f"Refusing overwrite: {out}")

    preview=json.loads(preview_path.read_text(encoding="utf-8"))
    design=json.loads(design_path.read_text(encoding="utf-8"))
    if preview.get("schema_version")!="P3-DEVFREEZE-PREVIEW-0.2.1":
        raise RuntimeError("wrong final preview version")
    if preview.get("design_id")!=DESIGN_ID or design.get("design_id")!=DESIGN_ID:
        raise RuntimeError("design binding mismatch")
    if preview["latest_session"]["provisional_close"]:
        raise RuntimeError("collector session is still provisional; stop collector and regenerate preview")
    if preview.get("production_db_changed_during_preview"):
        raise RuntimeError("production DB changed during final preview")
    if not preview.get("immutable_freeze_allowed_now"):
        raise RuntimeError("final preview does not allow freeze creation")

    before=(prod.stat().st_size,prod.stat().st_mtime_ns)
    conn=Phase1ReadOnlyAdapterV01.open_readonly(prod)
    try:
        conn.execute("BEGIN")
        Phase1ReadOnlyAdapterV01.validate_schema(conn)
        controls=read_control_events(conn)
        intervals,sessions,gaps,summary=reconstruct_collector_coverage(
            controls,
            nearest_before=lambda cutoff_us,after_us:nearest_observation_before_us(
                conn,cutoff_us,after_us=after_us
            ),
        )
        sid=int(preview["latest_session"]["session_id"])
        matches=[s for s in sessions if int(s["session_id"])==sid]
        if len(matches)!=1:raise RuntimeError("preview session not found uniquely")
        session=matches[0]
        if str(session["stop_kind"])=="EOF_UNCERTAIN":
            raise RuntimeError("selected collector session remains uncertain")
        start_us=int(session["start_at_us"]);end_us=int(session["stop_at_us"])
        start_iso=str(session["start_at_utc"]);end_iso=str(session["stop_at_utc"])
        if start_iso!=preview["latest_session"]["start_utc"] or end_iso!=preview["latest_session"]["observed_through_utc"]:
            raise RuntimeError("session boundaries changed since final preview")

        active=[
            CleanInterval(
                session_id=i.session_id,start_us=int(i.start_us),end_us=int(i.end_us),
                uncertain_close=bool(i.uncertain_close),
            )
            for i in intervals if i.session_id==sid
        ]
        if not active:raise RuntimeError("no active intervals for selected session")
        if any(i.uncertain_close for i in active):
            raise RuntimeError("immutable freeze cannot contain uncertain active interval")

        t0_rows=global_first_tradables(conn,start_us,end_us)
        eligible=[]
        for r in t0_rows:
            t0_us=iso_to_us(str(r["decoded_at_utc"]))
            iv=eligible_interval_for_t0(
                t0_us=t0_us,required_post_t0_ms=REQUIRED_POST_T0_MS,intervals=active
            )
            if iv is not None:
                eligible.append((r,iv))
        if not eligible:raise RuntimeError("no eligible tokens")
        if len(eligible)!=int(preview["cohort_preview"]["projected_eligible_tokens"]):
            raise RuntimeError("eligible token count changed since final preview")

        mints=[str(r["mint"]) for r,_ in eligible]
        rows=fetch_rows(conn,mints,start_iso,end_iso)
        columns=pump_event_columns(conn);names=[n for n,_ in columns]
        session_gaps=[g for g in gaps if int(g["session_id"])==sid]
        conn.rollback()
    finally:
        conn.close()
    after=(prod.stat().st_size,prod.stat().st_mtime_ns)
    if before!=after:
        raise RuntimeError("production DB changed during immutable builder; refusing freeze")

    out.mkdir(parents=True)
    db_out=out/"phase3_development_dataset_v0_2.sqlite3"
    tmp=db_out.with_suffix(".sqlite3.tmp")
    dst=sqlite3.connect(tmp)
    try:
        defs=", ".join(f"{quote_ident(n)} {t}".strip() for n,t in columns)
        dst.execute(f"CREATE TABLE pump_events ({defs})")
        dst.execute("CREATE TABLE freeze_tokens(mint TEXT PRIMARY KEY,t0_observed_at_utc TEXT NOT NULL)")
        dst.execute("CREATE TABLE freeze_metadata(key TEXT PRIMARY KEY,value_json TEXT NOT NULL)")
        insert=f"INSERT INTO pump_events(rowid,{','.join(quote_ident(n) for n in names)}) VALUES ({','.join('?' for _ in range(len(names)+1))})"
        for r in rows:
            dst.execute(insert,[int(r["p1_rowid"]),*[r[n] for n in names]])
        dst.executemany(
            "INSERT INTO freeze_tokens VALUES (?,?)",
            [(str(r["mint"]),str(r["decoded_at_utc"])) for r,_ in eligible],
        )
        meta={
            "freeze_id":"DEV-FREEZE-0002","dataset_role":"DEVELOPMENT_DISCOVERY",
            "design_id":DESIGN_ID,"session_id":sid,"session_start_utc":start_iso,
            "session_end_utc":end_iso,"required_tail_after_t0_ms":REQUIRED_POST_T0_MS,
        }
        for k,v in sorted(meta.items()):
            dst.execute("INSERT INTO freeze_metadata VALUES (?,?)",(k,canonical_json(v)))
        dst.commit()
        if dst.execute("PRAGMA quick_check").fetchone()[0]!="ok":
            raise RuntimeError("snapshot quick_check failed")
    finally:
        dst.close()
    tmp.replace(db_out)

    tok_csv=out/"phase3_development_tokens_v0_2.csv"
    with tok_csv.open("w",encoding="utf-8",newline="") as f:
        fields=["mint","first_tradable_observed_at_utc","source_p1_rowid",
                "clean_interval_start_utc","clean_interval_end_utc"]
        w=csv.DictWriter(f,fieldnames=fields);w.writeheader()
        for r,iv in eligible:
            w.writerow({
                "mint":r["mint"],"first_tradable_observed_at_utc":r["decoded_at_utc"],
                "source_p1_rowid":r["p1_rowid"],
                "clean_interval_start_utc":us_to_iso(iv.start_us),
                "clean_interval_end_utc":us_to_iso(iv.end_us),
            })

    coverage={
      "schema_version":"P3COV-0.1","freeze_id":"DEV-FREEZE-0002",
      "dataset_role":"DEVELOPMENT_DISCOVERY",
      "dataset_range_utc":{"start":start_iso,"end":end_iso},
      "active_intervals":[
        {"session_id":sid,"start_us":i.start_us,"start_utc":us_to_iso(i.start_us),
         "end_us":i.end_us,"end_utc":us_to_iso(i.end_us),
         "close_reason":"RECONSTRUCTED_CLEAN_INTERVAL","uncertain_close":False}
        for i in active
      ],
      "explicit_gaps":[
        {"session_id":sid,"start_us":int(g["gap_start_us"]),"start_utc":g["gap_start_utc"],
         "end_us":int(g["gap_end_us"]),"end_utc":g["gap_end_utc"],
         "gap_seconds":float(g["gap_seconds"])}
        for g in session_gaps
      ],
      "coverage_reconstruction_summary":{
        "selected_closed_session":True,"session_id":sid,
        "active_interval_count":len(active),"explicit_gap_count":len(session_gaps)
      },
      "semantics":{
        "complete_only_when_entire_query_is_inside_non_uncertain_active_intervals":True,
        "explicit_gap_overrides_complete":True,"uncovered_or_uncertain_time_is_unknown":True,
        "gaps_ignored":False,"production_db_opened_read_only":True
      }
    }
    core=dict(coverage)
    coverage["deterministic_coverage_sha256"]=hashlib.sha256(
        json.dumps(core,sort_keys=True,separators=(",",":"),ensure_ascii=True).encode()
    ).hexdigest()
    cov_path=out/"collector_coverage_DEV-FREEZE-0002_v0_2.json"
    cov_path.write_text(json.dumps(coverage,indent=2,sort_keys=True)+"\n",encoding="utf-8")

    selection_sha=file_sha(selection)
    design_sha=file_sha(design_path)
    manifest_core={
      "freeze_schema_version":"P3DEVFREEZE-0.2","freeze_id":"DEV-FREEZE-0002",
      "dataset_role":"DEVELOPMENT_DISCOVERY",
      "dataset":{
        "freeze_id":"DEV-FREEZE-0002","dataset_role":"DEVELOPMENT_DISCOVERY",
        "design_id":DESIGN_ID,
        "session_range_utc":{"start":start_iso,"end":end_iso},
        "session_id":sid,"required_tail_after_t0_ms":REQUIRED_POST_T0_MS,
        "eligible_token_count":len(eligible),"frozen_pump_event_rows":len(rows),
        "snapshot_sqlite_file":db_out.name,"snapshot_sqlite_sha256":file_sha(db_out),
        "development_token_csv_file":tok_csv.name,
        "development_token_csv_sha256":file_sha(tok_csv),
        "selection_rule":"global first normalizable BOT_TRUTH BUY/SELL is inside selected closed session, is not GAP_RECOVERY, and [t0,t0+10m] lies inside one clean active interval",
        "future_activity_filter":False,"future_outcome_filter":False,
        "strategy_signal_used_for_cohort_selection":False,
      },
      "entry_binding":{
        "EXP-0005_selection_sha256":selection_sha,"A_B_C_D_parameters_fixed":True
      },
      "design_binding":{"file":design_path.name,"sha256":design_sha},
      "coverage_snapshot":{"file":cov_path.name,"sha256":file_sha(cov_path)},
      "out_of_sample_policy":{"untouched_oos":False,"development_discovery":True},
    }
    manifest=dict(manifest_core)
    manifest["deterministic_manifest_sha256"]=hashlib.sha256(
        json.dumps(manifest_core,sort_keys=True,separators=(",",":"),ensure_ascii=True).encode()
    ).hexdigest()
    mp=out/"phase3_development_dataset_manifest_v0_2.json"
    mp.write_text(json.dumps(manifest,indent=2,sort_keys=True)+"\n",encoding="utf-8")

    print("DEV-FREEZE-0002 IMMUTABLE SESSION BUILDER v0.2")
    print("="*72)
    print(f"Closed session              : {start_iso} -> {end_iso}")
    print(f"Active clean intervals      : {len(active)}")
    print(f"Explicit gaps recorded      : {len(session_gaps)}")
    print(f"Eligible tokens             : {len(eligible)}")
    print(f"Frozen BOT_TRUTH rows       : {len(rows)}")
    print("10m per-token clean guard   : ENFORCED")
    print("Gaps ignored                : NO")
    print("Entry A/B/C/D binding       : RECORDED")
    print("Production DB mutated       : NO")
    print(f"Output                      : {out}")
    print("="*72)
    print("RESULT: PASS")
if __name__=="__main__":main()
