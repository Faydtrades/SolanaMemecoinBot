from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from fractions import Fraction
from typing import Iterable

from phase3.outcome_replay_v0_1_3 import (
    CandidateReference,
    CoverageProvider,
    CoverageStatus,
    PriceObservation,
)

SCHEMA_VERSION = "phase3_exit_path_replay_v0.1"


class ExitPathStatus(str, Enum):
    CLEAN = "CLEAN"
    COVERAGE_GAP = "COVERAGE_GAP"
    COVERAGE_UNKNOWN = "COVERAGE_UNKNOWN"
    GAP_RECOVERY_PRESENT = "GAP_RECOVERY_PRESENT"
    MARKET_CONTINUITY_UNKNOWN = "MARKET_CONTINUITY_UNKNOWN"
    PRICE_IDENTITY_MISMATCH = "PRICE_IDENTITY_MISMATCH"
    NO_FUTURE_PRICE = "NO_FUTURE_PRICE"


class PassageStatus(str, Enum):
    UPPER_HIT_FIRST = "UPPER_HIT_FIRST"
    LOWER_HIT_FIRST = "LOWER_HIT_FIRST"
    NO_HIT_CLEAN = "NO_HIT_CLEAN"
    PATH_NOT_CLEAN = "PATH_NOT_CLEAN"


@dataclass(frozen=True, slots=True)
class ExitPathPoint:
    observed_at: datetime
    ingest_seq: int
    return_bps: int
    source_event_key: str | None


@dataclass(frozen=True, slots=True)
class ExitPath:
    schema_version: str
    candidate_id: str
    mint: str
    signal_at: datetime
    horizon_ms: int
    target_at: datetime
    price_identity: str
    status: ExitPathStatus
    coverage_status: CoverageStatus
    points: tuple[ExitPathPoint, ...]
    future_observation_count: int
    fresh_same_identity_count: int
    gap_recovery_count: int
    identity_mismatch_count: int
    continuity_break_count: int
    peak_bps: int | None
    trough_bps: int | None

    @property
    def is_clean(self) -> bool:
        return self.status == ExitPathStatus.CLEAN


@dataclass(frozen=True, slots=True)
class FirstObservedPassage:
    status: PassageStatus
    lower_bps: int
    upper_bps: int
    observed_at: datetime | None
    ingest_seq: int | None
    return_bps: int | None


class ExitPathReplayV01:
    """
    Deterministic causal post-entry path foundation.

    This module does NOT select exit thresholds and does NOT create PnL.

    Semantics:
    - path starts after CandidateSignal/reference time; reference itself is 0 bps.
    - BOT_TRUTH order is observed_at, then ingest_seq.
    - only same-mint, same-price-identity, non-GAP_RECOVERY observations may
      become path points.
    - GAP_RECOVERY is retained as provenance and makes a full path non-clean.
    - known/unknown collector coverage makes a full path non-clean.
    - market-continuity boundaries cannot be crossed silently.
    - first-passage means FIRST OBSERVED passage in event data; no interpolation
      between observations is invented.
    """

    def __init__(self, coverage_provider: CoverageProvider) -> None:
        self.coverage_provider=coverage_provider

    def build_path(
        self,
        candidate: CandidateReference,
        observations: Iterable[PriceObservation],
        *,
        horizon_ms: int = 300_000,
    ) -> ExitPath:
        if horizon_ms <= 0:
            raise ValueError("horizon_ms must be > 0")
        target=candidate.signal_at+timedelta(milliseconds=horizon_ms)
        rows=sorted(
            (
                o for o in observations
                if o.mint==candidate.mint
                and o.observed_at>candidate.signal_at
                and o.observed_at<=target
            ),
            key=lambda o:(o.observed_at,o.ingest_seq),
        )
        coverage=self.coverage_provider.coverage_between(candidate.signal_at,target)
        gap_count=sum(o.is_gap_recovery for o in rows)
        fresh=[o for o in rows if not o.is_gap_recovery]
        mismatch=sum(o.price_identity!=candidate.price_identity for o in fresh)
        continuity=sum(o.market_continuity_break for o in rows)

        same=[
            o for o in fresh
            if o.price_identity==candidate.price_identity
            and not o.market_continuity_break
        ]
        points=tuple(
            ExitPathPoint(
                observed_at=o.observed_at,
                ingest_seq=o.ingest_seq,
                return_bps=_return_bps(candidate,o),
                source_event_key=o.source_event_key,
            )
            for o in same
        )

        if coverage==CoverageStatus.GAP:
            status=ExitPathStatus.COVERAGE_GAP
        elif coverage==CoverageStatus.UNKNOWN:
            status=ExitPathStatus.COVERAGE_UNKNOWN
        elif continuity>0:
            status=ExitPathStatus.MARKET_CONTINUITY_UNKNOWN
        elif gap_count>0:
            status=ExitPathStatus.GAP_RECOVERY_PRESENT
        elif not points and mismatch>0:
            status=ExitPathStatus.PRICE_IDENTITY_MISMATCH
        elif not points:
            status=ExitPathStatus.NO_FUTURE_PRICE
        else:
            status=ExitPathStatus.CLEAN

        returns=[p.return_bps for p in points]
        return ExitPath(
            schema_version=SCHEMA_VERSION,
            candidate_id=candidate.candidate_id,
            mint=candidate.mint,
            signal_at=candidate.signal_at,
            horizon_ms=horizon_ms,
            target_at=target,
            price_identity=candidate.price_identity,
            status=status,
            coverage_status=coverage,
            points=points,
            future_observation_count=len(rows),
            fresh_same_identity_count=len(points),
            gap_recovery_count=gap_count,
            identity_mismatch_count=mismatch,
            continuity_break_count=continuity,
            peak_bps=max([0,*returns]) if points else None,
            trough_bps=min([0,*returns]) if points else None,
        )

    def first_observed_passage(
        self,
        path: ExitPath,
        *,
        lower_bps: int,
        upper_bps: int,
    ) -> FirstObservedPassage:
        if lower_bps >= 0:
            raise ValueError("lower_bps must be < 0")
        if upper_bps <= 0:
            raise ValueError("upper_bps must be > 0")
        if not path.is_clean:
            return FirstObservedPassage(
                status=PassageStatus.PATH_NOT_CLEAN,
                lower_bps=lower_bps,
                upper_bps=upper_bps,
                observed_at=None,
                ingest_seq=None,
                return_bps=None,
            )
        for p in path.points:
            if p.return_bps <= lower_bps:
                return FirstObservedPassage(
                    PassageStatus.LOWER_HIT_FIRST,lower_bps,upper_bps,
                    p.observed_at,p.ingest_seq,p.return_bps
                )
            if p.return_bps >= upper_bps:
                return FirstObservedPassage(
                    PassageStatus.UPPER_HIT_FIRST,lower_bps,upper_bps,
                    p.observed_at,p.ingest_seq,p.return_bps
                )
        return FirstObservedPassage(
            PassageStatus.NO_HIT_CLEAN,lower_bps,upper_bps,None,None,None
        )


def _return_bps(candidate: CandidateReference, obs: PriceObservation) -> int:
    ref=Fraction(candidate.price_numerator_raw,candidate.price_denominator_raw)
    px=Fraction(obs.price_numerator_raw,obs.price_denominator_raw)
    value=(px/ref-1)*10_000
    return _round_fraction_half_even(value)


def _round_fraction_half_even(value: Fraction) -> int:
    sign=-1 if value<0 else 1
    x=abs(value)
    q,r=divmod(x.numerator,x.denominator)
    twice=r*2
    if twice>x.denominator:
        q+=1
    elif twice==x.denominator and q%2==1:
        q+=1
    return sign*q
