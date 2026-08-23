from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from fractions import Fraction
from typing import Iterable, Protocol, Sequence


SCHEMA_VERSION = "phase3_outcome_replay_v0.1.3"
DEFAULT_HORIZONS_MS: tuple[int, ...] = (
    5_000,
    15_000,
    30_000,
    60_000,
    120_000,
    300_000,
)


class CoverageStatus(str, Enum):
    COMPLETE = "COMPLETE"
    GAP = "GAP"
    UNKNOWN = "UNKNOWN"


class HorizonStatus(str, Enum):
    OBSERVABLE = "OBSERVABLE"
    NO_FUTURE_PRICE = "NO_FUTURE_PRICE"
    COVERAGE_GAP = "COVERAGE_GAP"
    COVERAGE_UNKNOWN = "COVERAGE_UNKNOWN"
    MARKET_CONTINUITY_UNKNOWN = "MARKET_CONTINUITY_UNKNOWN"
    PRICE_IDENTITY_MISMATCH = "PRICE_IDENTITY_MISMATCH"


@dataclass(frozen=True, slots=True)
class CandidateReference:
    candidate_id: str
    mint: str
    signal_at: datetime
    price_identity: str
    price_numerator_raw: int
    price_denominator_raw: int
    ingest_seq: int | None = None

    def __post_init__(self) -> None:
        _require_utc(self.signal_at)
        if self.price_numerator_raw <= 0:
            raise ValueError("candidate price numerator must be > 0")
        if self.price_denominator_raw <= 0:
            raise ValueError("candidate price denominator must be > 0")
        if not self.price_identity:
            raise ValueError("candidate price_identity is required")


@dataclass(frozen=True, slots=True)
class PriceObservation:
    mint: str
    observed_at: datetime
    ingest_seq: int
    price_identity: str
    price_numerator_raw: int
    price_denominator_raw: int
    is_gap_recovery: bool = False
    market_continuity_break: bool = False
    continuity_reason: str | None = None
    source_event_key: str | None = None

    def __post_init__(self) -> None:
        _require_utc(self.observed_at)
        if self.price_numerator_raw <= 0:
            raise ValueError("observation price numerator must be > 0")
        if self.price_denominator_raw <= 0:
            raise ValueError("observation price denominator must be > 0")
        if not self.price_identity:
            raise ValueError("observation price_identity is required")


class CoverageProvider(Protocol):
    def coverage_between(self, start: datetime, end: datetime) -> CoverageStatus:
        ...


@dataclass(frozen=True, slots=True)
class ConstantCoverageProvider:
    status: CoverageStatus

    def coverage_between(self, start: datetime, end: datetime) -> CoverageStatus:
        _require_utc(start)
        _require_utc(end)
        if end < start:
            raise ValueError("coverage end precedes start")
        return self.status


@dataclass(frozen=True, slots=True)
class CoverageInterval:
    start: datetime
    end: datetime
    status: CoverageStatus

    def __post_init__(self) -> None:
        _require_utc(self.start)
        _require_utc(self.end)
        if self.end < self.start:
            raise ValueError("coverage interval end precedes start")


@dataclass(slots=True)
class IntervalCoverageProvider:
    """
    Conservative coverage provider.

    The requested range is COMPLETE only when it is fully covered by COMPLETE
    intervals and intersects no GAP interval. Any uncovered segment is UNKNOWN.
    """
    intervals: Sequence[CoverageInterval]

    def coverage_between(self, start: datetime, end: datetime) -> CoverageStatus:
        _require_utc(start)
        _require_utc(end)
        if end < start:
            raise ValueError("coverage end precedes start")
        if end == start:
            return CoverageStatus.COMPLETE

        ordered = sorted(self.intervals, key=lambda x: (x.start, x.end, x.status.value))

        for interval in ordered:
            if interval.status != CoverageStatus.GAP:
                continue
            if interval.start < end and interval.end > start:
                return CoverageStatus.GAP

        complete = [
            i for i in ordered
            if i.status == CoverageStatus.COMPLETE
            and i.end > start
            and i.start < end
        ]
        if not complete:
            return CoverageStatus.UNKNOWN

        cursor = start
        for interval in complete:
            if interval.end <= cursor:
                continue
            if interval.start > cursor:
                return CoverageStatus.UNKNOWN
            cursor = max(cursor, interval.end)
            if cursor >= end:
                return CoverageStatus.COMPLETE

        return CoverageStatus.UNKNOWN


@dataclass(frozen=True, slots=True)
class HorizonOutcome:
    horizon_ms: int
    target_at: datetime
    status: HorizonStatus
    coverage_status: CoverageStatus
    mark_at: datetime | None
    mark_ingest_seq: int | None
    mark_age_ms: int | None
    return_bps: int | None
    comparable_observation_count: int
    gap_recovery_observation_count: int
    mismatched_identity_count: int
    continuity_break_count: int

    @property
    def is_observable(self) -> bool:
        return self.status == HorizonStatus.OBSERVABLE


@dataclass(frozen=True, slots=True)
class CandidateOutcome:
    schema_version: str
    candidate_id: str
    mint: str
    signal_at: datetime
    price_identity: str
    reference_price_numerator_raw: int
    reference_price_denominator_raw: int
    horizons: tuple[HorizonOutcome, ...]
    mfe_bps_5m: int | None
    mae_bps_5m: int | None
    extrema_observation_count_5m: int
    extrema_coverage_status_5m: CoverageStatus
    extrema_complete_5m: bool
    gap_recovery_observation_count_5m: int
    identity_mismatch_observation_count_5m: int
    continuity_break_observation_count_5m: int


class OutcomeReplayV013:
    """
    Outcome-label engine only.

    It does not create CandidateSignals, orders, fills, exits or PnL. It takes a
    causal CandidateReference plus future price observations and labels what was
    observed afterward.

    Important semantics:
    - BOT_TRUTH ordering uses observed_at, then ingest_seq.
    - GAP_RECOVERY rows are retained as provenance but are not used as fresh
      point marks or extrema observations.
    - A horizon never becomes a synthetic 0% return merely because no future
      token price was observed.
    - Point marks are the latest fresh, same-identity observation at or before
      the horizon. mark_age_ms is always exposed; no hidden freshness threshold
      is imposed in v0.1.
    - A return is considered OBSERVABLE only when collector coverage is
      COMPLETE for the whole candidate->horizon interval and no known Pump
      market-continuity boundary occurs before the target horizon.
    - If coverage is GAP/UNKNOWN, an available mark can still be recorded for
      audit, but status remains non-observable.
    - MFE/MAE are observed excursions through 5m relative to the candidate
      reference price. The signal/reference point is the zero baseline, so a
      setup that never trades above reference has MFE=0 (not negative), and a
      setup that never trades below reference has MAE=0 (not positive).
      They are None when no fresh, same-identity future price exists.
      extrema_complete_5m requires COMPLETE coverage, at least one fresh future
      price, and no price-identity mismatch or GAP_RECOVERY provenance.
    """

    def __init__(
        self,
        coverage_provider: CoverageProvider,
        horizons_ms: Sequence[int] = DEFAULT_HORIZONS_MS,
    ) -> None:
        horizons = tuple(int(x) for x in horizons_ms)
        if not horizons:
            raise ValueError("at least one horizon is required")
        if any(x <= 0 for x in horizons):
            raise ValueError("horizons must be positive")
        if tuple(sorted(set(horizons))) != horizons:
            raise ValueError("horizons must be unique and strictly increasing")
        self.coverage_provider = coverage_provider
        self.horizons_ms = horizons

    def replay(
        self,
        candidate: CandidateReference,
        observations: Iterable[PriceObservation],
    ) -> CandidateOutcome:
        rows = sorted(
            (o for o in observations if o.mint == candidate.mint and o.observed_at > candidate.signal_at),
            key=lambda o: (o.observed_at, o.ingest_seq),
        )

        horizon_results = tuple(
            self._horizon(candidate, rows, horizon_ms)
            for horizon_ms in self.horizons_ms
        )

        five_min_end = candidate.signal_at + timedelta(milliseconds=300_000)
        five_min_rows = [o for o in rows if o.observed_at <= five_min_end]
        five_min_coverage = self.coverage_provider.coverage_between(
            candidate.signal_at, five_min_end
        )

        fresh_same = [
            o for o in five_min_rows
            if not o.is_gap_recovery and o.price_identity == candidate.price_identity
        ]
        gap_recovery_count = sum(o.is_gap_recovery for o in five_min_rows)
        identity_mismatch_count = sum(
            (not o.is_gap_recovery) and o.price_identity != candidate.price_identity
            for o in five_min_rows
        )
        continuity_break_count = sum(o.market_continuity_break for o in five_min_rows)

        if fresh_same:
            returns = [_return_bps(candidate, o) for o in fresh_same]
            # Excursion semantics include the candidate reference point (0 bps).
            # Therefore MFE cannot be negative and MAE cannot be positive.
            mfe = max(0, *returns)
            mae = min(0, *returns)
        else:
            mfe = None
            mae = None

        extrema_complete = (
            five_min_coverage == CoverageStatus.COMPLETE
            and bool(fresh_same)
            and gap_recovery_count == 0
            and identity_mismatch_count == 0
            and continuity_break_count == 0
        )

        return CandidateOutcome(
            schema_version=SCHEMA_VERSION,
            candidate_id=candidate.candidate_id,
            mint=candidate.mint,
            signal_at=candidate.signal_at,
            price_identity=candidate.price_identity,
            reference_price_numerator_raw=candidate.price_numerator_raw,
            reference_price_denominator_raw=candidate.price_denominator_raw,
            horizons=horizon_results,
            mfe_bps_5m=mfe,
            mae_bps_5m=mae,
            extrema_observation_count_5m=len(fresh_same),
            extrema_coverage_status_5m=five_min_coverage,
            extrema_complete_5m=extrema_complete,
            gap_recovery_observation_count_5m=gap_recovery_count,
            identity_mismatch_observation_count_5m=identity_mismatch_count,
            continuity_break_observation_count_5m=continuity_break_count,
        )

    def _horizon(
        self,
        candidate: CandidateReference,
        rows: Sequence[PriceObservation],
        horizon_ms: int,
    ) -> HorizonOutcome:
        target = candidate.signal_at + timedelta(milliseconds=horizon_ms)
        coverage = self.coverage_provider.coverage_between(candidate.signal_at, target)
        in_window = [o for o in rows if o.observed_at <= target]

        gap_recovery_count = sum(o.is_gap_recovery for o in in_window)
        fresh = [o for o in in_window if not o.is_gap_recovery]
        same_identity = [o for o in fresh if o.price_identity == candidate.price_identity]
        mismatch_count = sum(o.price_identity != candidate.price_identity for o in fresh)
        continuity_breaks = [
            o for o in in_window
            if o.market_continuity_break and o.observed_at < target
        ]
        continuity_break_count = len(continuity_breaks)

        mark = same_identity[-1] if same_identity else None
        mark_return = _return_bps(candidate, mark) if mark is not None else None
        mark_age = (
            _timedelta_ms(target - mark.observed_at)
            if mark is not None
            else None
        )

        if coverage == CoverageStatus.GAP:
            status = HorizonStatus.COVERAGE_GAP
        elif coverage == CoverageStatus.UNKNOWN:
            status = HorizonStatus.COVERAGE_UNKNOWN
        elif continuity_break_count > 0:
            status = HorizonStatus.MARKET_CONTINUITY_UNKNOWN
        elif mark is None:
            status = (
                HorizonStatus.PRICE_IDENTITY_MISMATCH
                if mismatch_count > 0
                else HorizonStatus.NO_FUTURE_PRICE
            )
        else:
            status = HorizonStatus.OBSERVABLE

        return HorizonOutcome(
            horizon_ms=horizon_ms,
            target_at=target,
            status=status,
            coverage_status=coverage,
            mark_at=None if mark is None else mark.observed_at,
            mark_ingest_seq=None if mark is None else mark.ingest_seq,
            mark_age_ms=mark_age,
            return_bps=mark_return,
            comparable_observation_count=len(same_identity),
            gap_recovery_observation_count=gap_recovery_count,
            mismatched_identity_count=mismatch_count,
            continuity_break_count=continuity_break_count,
        )


def _return_bps(candidate: CandidateReference, obs: PriceObservation) -> int:
    ref = Fraction(candidate.price_numerator_raw, candidate.price_denominator_raw)
    px = Fraction(obs.price_numerator_raw, obs.price_denominator_raw)
    delta = (px / ref - 1) * 10_000
    return _round_fraction_half_even(delta)


def _round_fraction_half_even(value: Fraction) -> int:
    """Exact bankers rounding to preserve Phase-2 ROUND_HALF_EVEN semantics."""
    sign = -1 if value < 0 else 1
    x = abs(value)
    q, r = divmod(x.numerator, x.denominator)
    twice = r * 2
    if twice > x.denominator:
        q += 1
    elif twice == x.denominator and q % 2 == 1:
        q += 1
    return sign * q


def _timedelta_ms(delta: timedelta) -> int:
    total_us = (
        delta.days * 86_400 * 1_000_000
        + delta.seconds * 1_000_000
        + delta.microseconds
    )
    return total_us // 1_000


def _require_utc(value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("datetime must be timezone-aware UTC")
    if value.utcoffset() != timedelta(0):
        raise ValueError("datetime must be UTC")
