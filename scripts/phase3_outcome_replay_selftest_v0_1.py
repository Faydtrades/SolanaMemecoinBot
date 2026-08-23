from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from phase3.outcome_replay_v0_1 import (
    CandidateReference,
    ConstantCoverageProvider,
    CoverageInterval,
    CoverageStatus,
    HorizonStatus,
    IntervalCoverageProvider,
    OutcomeReplayV01,
    PriceObservation,
)


T0 = datetime(2026, 8, 18, 15, 0, 0, tzinfo=timezone.utc)
MINT = "TEST_MINT"
IDENTITY = "SOL_NATIVE"


def cand() -> CandidateReference:
    return CandidateReference(
        candidate_id="CAND-1",
        mint=MINT,
        signal_at=T0,
        price_identity=IDENTITY,
        price_numerator_raw=100,
        price_denominator_raw=100,
        ingest_seq=10,
    )


def obs(ms: int, seq: int, num: int, den: int = 100, identity: str = IDENTITY, gap=False):
    return PriceObservation(
        mint=MINT,
        observed_at=T0 + timedelta(milliseconds=ms),
        ingest_seq=seq,
        price_identity=identity,
        price_numerator_raw=num,
        price_denominator_raw=den,
        is_gap_recovery=gap,
        source_event_key=f"E-{ms}-{seq}",
    )


def assert_eq(actual, expected, label):
    if actual != expected:
        raise AssertionError(f"{label}: expected {expected!r}, got {actual!r}")


def run():
    print("PHASE 3.1 OUTCOME REPLAY SELFTEST v0.1")
    print("=" * 48)

    complete = ConstantCoverageProvider(CoverageStatus.COMPLETE)
    engine = OutcomeReplayV01(complete)

    # 1) Exact horizon inclusion, no look-ahead, deterministic returns.
    rows = [
        obs(2_000, 1, 110),   # +10%
        obs(5_000, 2, 90),    # -10%, exact 5s boundary
        obs(5_001, 3, 200),   # must NOT affect 5s
        obs(12_000, 4, 120),  # +20%
        obs(30_000, 5, 80),   # -20%
        obs(300_000, 6, 130), # +30%, exact 5m boundary
        obs(300_001, 7, 1),   # must NOT affect 5m extrema
    ]
    out = engine.replay(cand(), rows)
    h5 = out.horizons[0]
    assert_eq(h5.status, HorizonStatus.OBSERVABLE, "5s status")
    assert_eq(h5.return_bps, -1000, "5s return")
    assert_eq(h5.mark_ingest_seq, 2, "5s exact-boundary mark")
    assert_eq(out.mfe_bps_5m, 10000, "5m MFE")  # +100% at 5.001s
    assert_eq(out.mae_bps_5m, -2000, "5m MAE")
    assert_eq(out.extrema_complete_5m, True, "5m extrema complete")
    print("[PASS] exact boundaries / no-lookahead / MFE-MAE")

    # 2) No future token observation must stay missing, never synthetic 0%.
    empty = engine.replay(cand(), [])
    assert_eq(empty.horizons[0].status, HorizonStatus.NO_FUTURE_PRICE, "no future status")
    assert_eq(empty.horizons[0].return_bps, None, "no future return")
    assert_eq(empty.mfe_bps_5m, None, "no future MFE")
    assert_eq(empty.mae_bps_5m, None, "no future MAE")
    print("[PASS] missing future data is None, never flat/zero")

    # 3) A collector gap makes the horizon non-observable even if a mark exists.
    gap_provider = IntervalCoverageProvider([
        CoverageInterval(T0, T0 + timedelta(seconds=2), CoverageStatus.COMPLETE),
        CoverageInterval(T0 + timedelta(seconds=2), T0 + timedelta(seconds=4), CoverageStatus.GAP),
        CoverageInterval(T0 + timedelta(seconds=4), T0 + timedelta(minutes=5), CoverageStatus.COMPLETE),
    ])
    gap_out = OutcomeReplayV01(gap_provider).replay(cand(), [obs(4_500, 1, 120)])
    assert_eq(gap_out.horizons[0].status, HorizonStatus.COVERAGE_GAP, "gap status")
    assert_eq(gap_out.horizons[0].return_bps, 2000, "gap audit mark retained")
    assert_eq(gap_out.horizons[0].is_observable, False, "gap observable")
    print("[PASS] gap provenance blocks clean observability")

    # 4) Unknown coverage also blocks clean observability.
    unknown = OutcomeReplayV01(
        ConstantCoverageProvider(CoverageStatus.UNKNOWN)
    ).replay(cand(), [obs(4_000, 1, 105)])
    assert_eq(unknown.horizons[0].status, HorizonStatus.COVERAGE_UNKNOWN, "unknown status")
    assert_eq(unknown.horizons[0].return_bps, 500, "unknown audit mark retained")
    print("[PASS] unknown coverage remains explicitly unknown")

    # 5) Different price identities are never compared.
    mismatch = engine.replay(cand(), [obs(4_000, 1, 105, identity="QUOTE:USDC")])
    assert_eq(mismatch.horizons[0].status, HorizonStatus.PRICE_IDENTITY_MISMATCH, "identity status")
    assert_eq(mismatch.horizons[0].return_bps, None, "identity return")
    assert_eq(mismatch.mfe_bps_5m, None, "identity MFE")
    print("[PASS] price-identity mismatch is non-comparable")

    # 6) GAP_RECOVERY is provenance, not a fresh horizon mark/extrema input.
    recovered = engine.replay(cand(), [
        obs(3_000, 1, 150, gap=True),
        obs(4_000, 2, 110),
    ])
    assert_eq(recovered.horizons[0].return_bps, 1000, "gap recovery excluded from mark")
    assert_eq(recovered.horizons[0].gap_recovery_observation_count, 1, "gap recovery count")
    assert_eq(recovered.mfe_bps_5m, 1000, "gap recovery excluded from MFE")
    assert_eq(recovered.extrema_complete_5m, False, "recovery makes extrema incomplete")
    print("[PASS] GAP_RECOVERY cannot masquerade as fresh outcome data")

    # 7) Same-timestamp tie is deterministic by ingest_seq.
    tied = engine.replay(cand(), [
        obs(5_000, 1, 110),
        obs(5_000, 2, 120),
    ])
    assert_eq(tied.horizons[0].return_bps, 2000, "ingest_seq tie break")
    assert_eq(tied.horizons[0].mark_ingest_seq, 2, "ingest_seq selected")
    print("[PASS] deterministic observed_at + ingest_seq ordering")

    # 8) Partial complete interval => UNKNOWN (conservative; no hidden fill).
    partial_provider = IntervalCoverageProvider([
        CoverageInterval(T0, T0 + timedelta(seconds=3), CoverageStatus.COMPLETE),
    ])
    partial = OutcomeReplayV01(partial_provider).replay(cand(), [obs(2_000, 1, 110)])
    assert_eq(partial.horizons[0].status, HorizonStatus.COVERAGE_UNKNOWN, "partial coverage")
    print("[PASS] uncovered coverage interval is UNKNOWN")

    # 9) Non-mint observations are ignored.
    other = PriceObservation(
        mint="OTHER",
        observed_at=T0 + timedelta(seconds=1),
        ingest_seq=1,
        price_identity=IDENTITY,
        price_numerator_raw=999,
        price_denominator_raw=100,
    )
    ignored = engine.replay(cand(), [other])
    assert_eq(ignored.horizons[0].return_bps, None, "other mint ignored")
    print("[PASS] cross-mint contamination prevented")

    print("=" * 48)
    print("RESULT: PASS")


if __name__ == "__main__":
    run()
