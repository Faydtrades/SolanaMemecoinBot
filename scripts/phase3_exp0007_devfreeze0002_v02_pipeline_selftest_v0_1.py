from pathlib import Path
import json,tempfile,sys
from datetime import datetime,timezone,timedelta
PROJECT_ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(PROJECT_ROOT/"src"))
from phase3.locked_entry_readiness_v0_1_2 import manifest_identity
from phase3.collector_coverage_v0_2 import CollectorCoverageSnapshotV02
from phase3.outcome_replay_v0_1_3 import CoverageStatus

def check(c,l):
    if not c:raise AssertionError(l)
    print(f"[PASS] {l}")

def main():
    print("EXP-0007 DEV-FREEZE-0002 CLOSE/FREEZE/READINESS SELFTEST v0.2")
    print("="*76)
    start=datetime(2026,8,21,12,0,tzinfo=timezone.utc)
    end=start+timedelta(hours=1)
    m={"dataset":{"freeze_id":"DEV-FREEZE-0002","dataset_role":"DEVELOPMENT_DISCOVERY",
                  "session_range_utc":{"start":start.isoformat(),"end":end.isoformat()},
                  "frozen_pump_event_rows":123}}
    fid,role,a,b,n=manifest_identity(m)
    check(fid=="DEV-FREEZE-0002" and a==start and b==end and n==123,
          "readiness engine accepts v0.2 session-range manifest")

    with tempfile.TemporaryDirectory() as td:
        p=Path(td)/"cov.json"
        p.write_text(json.dumps({
            "schema_version":"P3COV-0.1","freeze_id":"DEV-FREEZE-0002",
            "dataset_range_utc":{"start":start.isoformat(),"end":end.isoformat()},
            "active_intervals":[
              {"start_utc":start.isoformat(),"end_utc":(start+timedelta(minutes=20)).isoformat(),"uncertain_close":False},
              {"start_utc":(start+timedelta(minutes=21)).isoformat(),"end_utc":end.isoformat(),"uncertain_close":False}
            ],
            "explicit_gaps":[
              {"start_utc":(start+timedelta(minutes=20)).isoformat(),"end_utc":(start+timedelta(minutes=21)).isoformat()}
            ]
        }),encoding="utf-8")
        c=CollectorCoverageSnapshotV02.load(p)
        c.validate_binding(freeze_id="DEV-FREEZE-0002",range_start=start,range_end=end)
        pr=c.provider()
        check(pr.coverage_between(start+timedelta(minutes=2),start+timedelta(minutes=12)) is CoverageStatus.COMPLETE,
              "clean token window remains COMPLETE")
        check(pr.coverage_between(start+timedelta(minutes=15),start+timedelta(minutes=25)) is not CoverageStatus.COMPLETE,
              "window crossing explicit gap is not COMPLETE")
    print("="*76);print("RESULT: PASS")
if __name__=="__main__":main()
