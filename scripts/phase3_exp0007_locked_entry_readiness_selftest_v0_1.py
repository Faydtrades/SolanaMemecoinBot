from __future__ import annotations
from datetime import datetime,timedelta,timezone
import json,tempfile
from pathlib import Path
import sys
PROJECT_ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(PROJECT_ROOT/"src"))
from phase3.collector_coverage_v0_2 import CollectorCoverageSnapshotV02
from phase3.outcome_replay_v0_1_3 import CoverageStatus
from phase3.locked_entry_readiness_v0_1 import parameter_set_from_locked_row
T=datetime(2026,8,21,12,0,0,tzinfo=timezone.utc)
def check(c,l):
    if not c:raise AssertionError(l)
    print(f"[PASS] {l}")
def main():
    print("EXP-0007 LOCKED-ENTRY READINESS INTEGRATION SELFTEST v0.1");print("="*72)
    with tempfile.TemporaryDirectory() as td:
        p=Path(td)/"coverage.json"
        p.write_text(json.dumps({"schema_version":"P3COV-0.1","freeze_id":"TEST",
        "dataset_range_utc":{"start":T.isoformat(),"end":(T+timedelta(minutes=10)).isoformat()},
        "active_intervals":[{"start_utc":T.isoformat(),"end_utc":(T+timedelta(minutes=10)).isoformat(),
        "uncertain_close":False}],"explicit_gaps":[]}),encoding="utf-8")
        status=CollectorCoverageSnapshotV02.load(p).provider().coverage_between(
            T+timedelta(minutes=1),T+timedelta(minutes=2))
        check(status is CoverageStatus.COMPLETE,
              "coverage provider uses OutcomeReplay v0.1.3 CoverageStatus identity")
    row={"parameter_set_id":"TEST-R1","min_return_bps":1500,"min_trades_since_t0":5,
    "min_unique_buyers_since_t0":9,"min_depth_bps":2500,"max_depth_bps":6500,
    "min_rebound_bps":2300,"min_buys":2,"min_net_flow_reserve_ppm":6000,
    "min_extension_from_response_bps":200,"max_extension_from_response_bps":2500}
    p=parameter_set_from_locked_row(row)
    check(p.impulse_parameters["min_return_bps"]==1500,"A binding exact")
    check(p.pullback_parameters=={"min_depth_bps":2500,"max_depth_bps":6500},"B binding exact")
    check(p.buyer_response_parameters["window_id"]=="3s","C window remains 3s")
    check(p.reclaim_parameters["min_extension_from_response_bps"]==200,"D reclaim binding exact")
    check(p.runaway_entry_parameters["max_extension_from_response_bps"]==2500,"D runaway binding exact")
    print("="*72);print("RESULT: PASS")
if __name__=="__main__":main()
