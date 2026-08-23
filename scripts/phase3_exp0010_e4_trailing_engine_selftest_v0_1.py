from pathlib import Path
import sys
from datetime import datetime, timedelta, timezone

PROJECT_ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(PROJECT_ROOT/"src"))

from phase3.e4_trailing_engine_v0_1 import (
    trailing_exit_at_or_before,
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
    print("EXP-0010 E4 TRAILING ENGINE SELFTEST v0.1")
    print("="*76)
    t=datetime(2026,8,22,tzinfo=timezone.utc)

    # Activate at +5%, peak at +12%, exit once observed return gives back 5%.
    p=PathObj(t,[
        P(t+timedelta(seconds=1),300),
        P(t+timedelta(seconds=2),550),
        P(t+timedelta(seconds=3),1200),
        P(t+timedelta(seconds=4),900),
        P(t+timedelta(seconds=5),680),
        P(t+timedelta(seconds=6),1400),
    ])
    a,e,peak=trailing_exit_at_or_before(
        p,activation_bps=500,giveback_bps=500,horizon_ms=15000
    )
    check(a is p.points[1],"activation is first observed crossing")
    check(e is p.points[4],"trail exit is first observed giveback crossing")
    check(peak==1200,"running peak ratchets before trail exit")

    # A crossing after 15s must not count.
    p2=PathObj(t,[
        P(t+timedelta(seconds=2),600),
        P(t+timedelta(seconds=10),1100),
        P(t+timedelta(seconds=16),400),
    ])
    a,e,peak=trailing_exit_at_or_before(
        p2,activation_bps=500,giveback_bps=500,horizon_ms=15000
    )
    check(a is p2.points[0],"activation within horizon counts")
    check(e is None,"trail crossing after fallback horizon does not count")
    m,target=fallback_mark_at_or_before(p2,15000)
    check(m is p2.points[1],"fallback uses latest mark at/before 15s")
    check(round((target-m.observed_at).total_seconds()*1000)==5000,
          "fallback mark age exposed exactly")

    # No activation -> fallback only.
    p3=PathObj(t,[
        P(t+timedelta(seconds=3),200),
        P(t+timedelta(seconds=12),400),
    ])
    a,e,peak=trailing_exit_at_or_before(
        p3,activation_bps=500,giveback_bps=300,horizon_ms=15000
    )
    check(a is None and e is None,"no activation does not fabricate trail")
    print("="*76)
    print("RESULT: PASS")

if __name__=="__main__":
    main()
