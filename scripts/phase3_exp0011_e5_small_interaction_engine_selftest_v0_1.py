from pathlib import Path
import sys
from datetime import datetime, timedelta, timezone

PROJECT_ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(PROJECT_ROOT/"src"))

from phase3.e5_small_interaction_engine_v0_1 import evaluate_variant_on_path

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
    print("EXP-0011 E5 SMALL INTERACTION ENGINE SELFTEST v0.1")
    print("="*84)
    t=datetime(2026,8,22,tzinfo=timezone.utc)

    # Hard stop precedence over other enabled exits.
    p=PathObj(t,[
        P(t+timedelta(seconds=1),600),
        P(t+timedelta(seconds=2),1200),
        P(t+timedelta(seconds=3),-1100),
    ])
    v={
        "fallback_ms":15000,"tp_bps":1000,
        "trail_activation_bps":500,"trail_giveback_bps":300,
        "hard_stop_bps":-1000,
    }
    ev=evaluate_variant_on_path(p,v)
    check(ev["reason"]=="TAKE_PROFIT",
          "earlier TP exits before a later hard-stop point")

    # New trail activation cannot exit on same point; later giveback can.
    p2=PathObj(t,[
        P(t+timedelta(seconds=1),500),
        P(t+timedelta(seconds=2),1100),
        P(t+timedelta(seconds=3),750),
    ])
    v2={
        "fallback_ms":15000,"tp_bps":None,
        "trail_activation_bps":500,"trail_giveback_bps":300,
        "hard_stop_bps":None,
    }
    ev=evaluate_variant_on_path(p2,v2)
    check(ev["reason"]=="TRAIL","later observed giveback triggers trail")
    check(ev["exit_point"] is p2.points[2],"trail exits on first later crossing")

    # Active trail precedes fixed TP on same observed point if both could apply.
    p3=PathObj(t,[
        P(t+timedelta(seconds=1),1000),
        P(t+timedelta(seconds=2),1700),
        P(t+timedelta(seconds=3),1400),
    ])
    v3={
        "fallback_ms":15000,"tp_bps":1500,
        "trail_activation_bps":1000,"trail_giveback_bps":300,
        "hard_stop_bps":None,
    }
    ev=evaluate_variant_on_path(p3,v3)
    check(ev["reason"]=="TAKE_PROFIT",
          "fixed TP can fire before later trail crossing")

    # Reference variant falls back only.
    p4=PathObj(t,[P(t+timedelta(seconds=12),250)])
    v4={
        "fallback_ms":15000,"tp_bps":None,
        "trail_activation_bps":None,"trail_giveback_bps":None,
        "hard_stop_bps":None,
    }
    ev=evaluate_variant_on_path(p4,v4)
    check(ev["reason"]=="FALLBACK","reference variant uses fallback")
    check(ev["fallback_mark_age_ms"]==3000,"fallback mark age exact")

    print("="*84)
    print("RESULT: PASS")

if __name__=="__main__":
    main()
