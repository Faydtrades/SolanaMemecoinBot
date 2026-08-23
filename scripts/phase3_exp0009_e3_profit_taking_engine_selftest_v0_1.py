from pathlib import Path
import sys
from datetime import datetime, timedelta, timezone

PROJECT_ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(PROJECT_ROOT/"src"))

from phase3.e3_profit_taking_engine_v0_1 import (
    first_tp_hit_at_or_before,
    fallback_mark_at_or_before,
)

class P:
    def __init__(self, observed_at, return_bps):
        self.observed_at=observed_at
        self.return_bps=return_bps

class PathObj:
    def __init__(self, signal_at, points):
        self.signal_at=signal_at
        self.points=points

def check(c,l):
    if not c: raise AssertionError(l)
    print(f"[PASS] {l}")

def main():
    print("EXP-0009 E3 PROFIT-TAKING ENGINE SELFTEST v0.1")
    print("="*76)
    t=datetime(2026,8,22,tzinfo=timezone.utc)
    path=PathObj(t,[
        P(t+timedelta(seconds=2),200),
        P(t+timedelta(seconds=4),520),
        P(t+timedelta(seconds=8),1100),
        P(t+timedelta(seconds=14),700),
        P(t+timedelta(seconds=17),1600),
    ])
    h=first_tp_hit_at_or_before(path,500,15000)
    check(h is path.points[1],"first observed TP crossing selected")
    h=first_tp_hit_at_or_before(path,1500,15000)
    check(h is None,"TP crossing after fallback horizon does not count")
    m,target=fallback_mark_at_or_before(path,15000)
    check(m is path.points[3],"fallback uses latest observed mark <= horizon")
    check(round((target-m.observed_at).total_seconds()*1000)==1000,
          "fallback mark age exposed exactly")
    h=first_tp_hit_at_or_before(path,1000,5000)
    check(h is None,"5s sensitivity does not peek beyond 5s")
    print("="*76)
    print("RESULT: PASS")

if __name__=="__main__":
    main()
