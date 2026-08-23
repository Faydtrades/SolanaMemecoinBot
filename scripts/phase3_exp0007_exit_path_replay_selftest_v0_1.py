from __future__ import annotations

from datetime import datetime,timedelta,timezone
from pathlib import Path
import sys

PROJECT_ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(PROJECT_ROOT/"src"))

from phase3.outcome_replay_v0_1_3 import (
    CandidateReference,ConstantCoverageProvider,CoverageStatus,PriceObservation
)
from phase3.exit_path_replay_v0_1 import (
    ExitPathReplayV01,ExitPathStatus,PassageStatus
)

T=datetime(2026,8,21,12,0,0,tzinfo=timezone.utc)

def cand():
    return CandidateReference(
        candidate_id="C1",mint="M1",signal_at=T,price_identity="SOL_NATIVE",
        price_numerator_raw=100,price_denominator_raw=1,ingest_seq=10
    )

def obs(ms,seq,price,**kw):
    return PriceObservation(
        mint="M1",observed_at=T+timedelta(milliseconds=ms),ingest_seq=seq,
        price_identity=kw.pop("price_identity","SOL_NATIVE"),
        price_numerator_raw=price,price_denominator_raw=1,**kw
    )

def check(cond,label):
    if not cond:raise AssertionError(label)
    print(f"[PASS] {label}")

def main():
    print("PHASE 3 / EXP-0007 EXIT PATH REPLAY SELFTEST v0.1")
    print("="*66)

    clean=ExitPathReplayV01(ConstantCoverageProvider(CoverageStatus.COMPLETE))
    path=clean.build_path(cand(),[
        obs(3000,3,107),obs(1000,2,95),obs(1000,1,96),obs(2000,1,112)
    ],horizon_ms=5000)
    check(path.status==ExitPathStatus.CLEAN,"clean path classified CLEAN")
    check([p.ingest_seq for p in path.points]==[1,2,1,3],
          "ordering is observed_at then ingest_seq")
    check([p.return_bps for p in path.points]==[-400,-500,1200,700],
          "exact reference-relative bps path")
    check(path.peak_bps==1200 and path.trough_bps==-500,
          "peak/trough include zero reference baseline")

    passage=clean.first_observed_passage(path,lower_bps=-450,upper_bps=1000)
    check(passage.status==PassageStatus.LOWER_HIT_FIRST,
          "lower first observed passage wins when it occurs first")
    passage2=clean.first_observed_passage(path,lower_bps=-900,upper_bps=1000)
    check(passage2.status==PassageStatus.UPPER_HIT_FIRST,
          "upper first observed passage is deterministic")
    passage3=clean.first_observed_passage(path,lower_bps=-2000,upper_bps=3000)
    check(passage3.status==PassageStatus.NO_HIT_CLEAN,
          "clean no-hit can only be claimed on clean path")

    gap=ExitPathReplayV01(ConstantCoverageProvider(CoverageStatus.GAP))
    p=gap.build_path(cand(),[obs(1000,1,120)])
    check(p.status==ExitPathStatus.COVERAGE_GAP,"coverage GAP invalidates path")
    check(gap.first_observed_passage(p,lower_bps=-1000,upper_bps=1000).status
          ==PassageStatus.PATH_NOT_CLEAN,"no passage claim on gap path")

    unknown=ExitPathReplayV01(ConstantCoverageProvider(CoverageStatus.UNKNOWN))
    check(unknown.build_path(cand(),[obs(1000,1,120)]).status
          ==ExitPathStatus.COVERAGE_UNKNOWN,"unknown coverage invalidates path")

    p=clean.build_path(cand(),[
        obs(1000,1,120,is_gap_recovery=True),
        obs(2000,2,110),
    ])
    check(p.status==ExitPathStatus.GAP_RECOVERY_PRESENT,
          "GAP_RECOVERY is provenance, not a clean path point")
    check(len(p.points)==1 and p.points[0].return_bps==1000,
          "GAP_RECOVERY observation excluded from fresh path points")

    p=clean.build_path(cand(),[
        obs(1000,1,120,price_identity="QUOTE:X")
    ])
    check(p.status==ExitPathStatus.PRICE_IDENTITY_MISMATCH,
          "price identities are never mixed")

    p=clean.build_path(cand(),[
        obs(1000,1,100,market_continuity_break=True,continuity_reason="PUMP_BOUNDARY"),
        obs(2000,2,120),
    ])
    check(p.status==ExitPathStatus.MARKET_CONTINUITY_UNKNOWN,
          "market continuity boundary invalidates full path")

    p=clean.build_path(cand(),[])
    check(p.status==ExitPathStatus.NO_FUTURE_PRICE,
          "missing future observations are not synthetic flat prices")

    print("="*66)
    print("RESULT: PASS")

if __name__=="__main__":main()
