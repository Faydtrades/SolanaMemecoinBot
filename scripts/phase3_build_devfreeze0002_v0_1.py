from __future__ import annotations

import argparse,csv,hashlib,json,os,sqlite3
from pathlib import Path
import sys

PROJECT_ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(PROJECT_ROOT/"src"))

from phase2.phase1_readonly_adapter_v0_1 import Phase1ReadOnlyAdapterV01, BOT_TRUTH_SOURCE_PREFIXES
from phase2.research_freeze_manifest_v0_1 import pump_event_columns,quote_ident,canonical_json
from phase3.devfreeze0002_design_v0_1 import REQUIRED_POST_T0_MS

def file_sha(p):
    h=hashlib.sha256()
    with p.open("rb") as f:
        for c in iter(lambda:f.read(1024*1024),b""):h.update(c)
    return h.hexdigest()

def source_sql():
    return "("+" OR ".join("source_decoded_file LIKE ?" for _ in BOT_TRUTH_SOURCE_PREFIXES)+")",tuple(p+"%" for p in BOT_TRUTH_SOURCE_PREFIXES)

def eligible(conn,start_iso,cutoff_iso):
    where,args=source_sql()
    return conn.execute(f"""
        WITH ranked AS (
          SELECT rowid AS p1_rowid,mint,decoded_at_utc,source_decoded_file,
                 ROW_NUMBER() OVER (PARTITION BY mint ORDER BY decoded_at_utc,rowid) rn
          FROM pump_events
          WHERE mint IS NOT NULL AND event_type IN ('BUY','SELL')
            AND decoded_at_utc IS NOT NULL AND {where}
        )
        SELECT mint,decoded_at_utc,source_decoded_file,p1_rowid
        FROM ranked
        WHERE rn=1 AND decoded_at_utc>=? AND decoded_at_utc<=?
          AND source_decoded_file NOT LIKE 'GAP_RECONCILIATION_%'
        ORDER BY decoded_at_utc,mint
    """,(*args,start_iso,cutoff_iso)).fetchall()

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--production-db",default=str(PROJECT_ROOT/"data/db/tradingbot.sqlite3"))
    ap.add_argument("--preview",default=str(PROJECT_ROOT/"data/research/phase3/EXP-0007/DEV-FREEZE-0002_candidate_preview_v0_1.json"))
    ap.add_argument("--selection",default=str(PROJECT_ROOT/"data/research/phase3/EXP-0005/EXP-0005_D_selection_v0_1.json"))
    ap.add_argument("--out-dir",default=str(PROJECT_ROOT/"data/research/phase3/dev_freezes/DEV-FREEZE-0002"))
    ap.add_argument("--confirm-create",action="store_true")
    args=ap.parse_args()
    if not args.confirm_create:
        raise RuntimeError("Refusing immutable freeze creation without --confirm-create")
    prod=Path(args.production_db).resolve(); preview=Path(args.preview).resolve()
    selection=Path(args.selection).resolve(); out=Path(args.out_dir).resolve()
    if out.exists(): raise FileExistsError(f"Refusing to overwrite immutable freeze directory: {out}")
    p=json.loads(preview.read_text(encoding="utf-8"))
    seg=p["current_preview_segment"]
    if bool(seg["uncertain_close"]):
        raise RuntimeError("Preview segment has uncertain/provisional close; stop collector and regenerate preview first")
    if p.get("production_db_changed_during_preview"):
        raise RuntimeError("Preview observed DB change; regenerate preview after collector is stopped")
    start_iso=str(seg["start_utc"]); end_iso=str(seg["end_utc"])
    from datetime import datetime,timedelta
    end_dt=datetime.fromisoformat(end_iso)
    cutoff_iso=(end_dt-timedelta(milliseconds=REQUIRED_POST_T0_MS)).isoformat()

    conn=Phase1ReadOnlyAdapterV01.open_readonly(prod)
    try:
        Phase1ReadOnlyAdapterV01.validate_schema(conn)
        toks=eligible(conn,start_iso,cutoff_iso)
        if not toks:raise RuntimeError("no eligible tokens in selected segment")
        mints=[str(r["mint"]) for r in toks]
        where,args_source=source_sql()
        ph=",".join("?" for _ in mints)
        rows=conn.execute(f"""
            SELECT rowid AS p1_rowid,*
            FROM pump_events
            WHERE mint IN ({ph})
              AND event_type IN ('LAUNCH','BUY','SELL')
              AND decoded_at_utc>=? AND decoded_at_utc<=?
              AND {where}
            ORDER BY rowid
        """,(*mints,start_iso,end_iso,*args_source)).fetchall()
        columns=pump_event_columns(conn)
        names=[n for n,_ in columns]
    finally:conn.close()

    out.mkdir(parents=True)
    db_out=out/"phase3_development_dataset_v0_1.sqlite3"
    tmp=db_out.with_suffix(".sqlite3.tmp")
    dst=sqlite3.connect(tmp)
    try:
        defs=", ".join(f"{quote_ident(n)} {t}".strip() for n,t in columns)
        dst.execute(f"CREATE TABLE pump_events ({defs})")
        dst.execute("CREATE TABLE freeze_tokens(mint TEXT PRIMARY KEY, t0_observed_at_utc TEXT NOT NULL)")
        dst.execute("CREATE TABLE freeze_metadata(key TEXT PRIMARY KEY,value_json TEXT NOT NULL)")
        insert=f"INSERT INTO pump_events(rowid,{','.join(quote_ident(n) for n in names)}) VALUES ({','.join('?' for _ in range(len(names)+1))})"
        for r in rows: dst.execute(insert,[int(r["p1_rowid"]),*[r[n] for n in names]])
        dst.executemany("INSERT INTO freeze_tokens VALUES (?,?)",[(str(r["mint"]),str(r["decoded_at_utc"])) for r in toks])
        meta={"freeze_id":"DEV-FREEZE-0002","segment_start_utc":start_iso,"segment_end_utc":end_iso,
              "required_tail_after_t0_ms":REQUIRED_POST_T0_MS,"dataset_role":"DEVELOPMENT_DISCOVERY"}
        for k,v in sorted(meta.items()):dst.execute("INSERT INTO freeze_metadata VALUES (?,?)",(k,canonical_json(v)))
        dst.commit()
        if dst.execute("PRAGMA quick_check").fetchone()[0]!="ok":raise RuntimeError("snapshot quick_check failed")
    finally:dst.close()
    tmp.replace(db_out)

    tok_csv=out/"phase3_development_tokens_v0_1.csv"
    with tok_csv.open("w",encoding="utf-8",newline="") as f:
        w=csv.DictWriter(f,fieldnames=["mint","first_tradable_observed_at_utc","source_p1_rowid"])
        w.writeheader()
        for r in toks:w.writerow({"mint":r["mint"],"first_tradable_observed_at_utc":r["decoded_at_utc"],"source_p1_rowid":r["p1_rowid"]})

    selection_sha=file_sha(selection)
    coverage={
      "schema_version":"P3COV-0.1","freeze_id":"DEV-FREEZE-0002","dataset_role":"DEVELOPMENT_DISCOVERY",
      "dataset_range_utc":{"start":start_iso,"end":end_iso},
      "active_intervals":[{"session_id":int(seg["session_id"]),"start_us":int(seg["start_us"]),"start_utc":start_iso,
                           "end_us":int(seg["end_us"]),"end_utc":end_iso,"close_reason":seg["parent_close_reason"],"uncertain_close":False}],
      "explicit_gaps":[],
      "coverage_reconstruction_summary":{"selected_clean_segment":True},
      "source_control_event_count":None,"source_session_count":None,
      "semantics":{"complete_only_when_entire_query_is_inside_non_uncertain_active_intervals":True,
                   "explicit_gap_overrides_complete":True,"uncovered_or_uncertain_time_is_unknown":True,
                   "production_market_rows_copied":False,"production_db_opened_read_only":True}
    }
    cov_core=dict(coverage)
    coverage["deterministic_coverage_sha256"]=hashlib.sha256(json.dumps(cov_core,sort_keys=True,separators=(",",":"),ensure_ascii=True).encode()).hexdigest()
    cov_path=out/"collector_coverage_DEV-FREEZE-0002_v0_1.json"
    cov_path.write_text(json.dumps(coverage,indent=2,sort_keys=True)+"\n",encoding="utf-8")

    manifest_core={
      "freeze_schema_version":"P3DEVFREEZE-0.1","freeze_id":"DEV-FREEZE-0002","dataset_role":"DEVELOPMENT_DISCOVERY",
      "dataset":{
        "freeze_id":"DEV-FREEZE-0002","dataset_role":"DEVELOPMENT_DISCOVERY",
        "clean_segment_utc":{"start":start_iso,"end":end_iso},
        "required_tail_after_t0_ms":REQUIRED_POST_T0_MS,
        "eligible_token_count":len(toks),"frozen_pump_event_rows":len(rows),
        "snapshot_sqlite_sha256":file_sha(db_out),"development_token_csv_sha256":file_sha(tok_csv),
        "selection_rule":"global first BOT_TRUTH BUY/SELL is inside clean segment, is not GAP_RECOVERY, and t0+10m <= segment end",
        "future_activity_filter":False,"future_outcome_filter":False
      },
      "entry_binding":{"EXP-0005_selection_sha256":selection_sha,"A_B_C_D_parameters_fixed":True},
      "coverage_snapshot":{"file":cov_path.name,"sha256":file_sha(cov_path)},
      "out_of_sample_policy":{"untouched_oos":False,"development_discovery":True}
    }
    manifest=dict(manifest_core)
    manifest["deterministic_manifest_sha256"]=hashlib.sha256(json.dumps(manifest_core,sort_keys=True,separators=(",",":"),ensure_ascii=True).encode()).hexdigest()
    mp=out/"phase3_development_dataset_manifest_v0_1.json"
    mp.write_text(json.dumps(manifest,indent=2,sort_keys=True)+"\n",encoding="utf-8")

    print("DEV-FREEZE-0002 IMMUTABLE BUILDER v0.1")
    print("="*66)
    print(f"Clean segment             : {start_iso} -> {end_iso}")
    print(f"Eligible tokens           : {len(toks)}")
    print(f"Frozen BOT_TRUTH rows     : {len(rows)}")
    print("10m t0 tail guard         : ENFORCED")
    print("Entry A/B/C/D binding     : RECORDED")
    print("Production DB mutated     : NO")
    print(f"Output                    : {out}")
    print("="*66)
    print("RESULT: PASS")
if __name__=="__main__":main()
