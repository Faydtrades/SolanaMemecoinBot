from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Sequence

from phase2.denomination_aware_flow_v0_1 import DenominationAwareFeatureEngineV01
from phase2.models_v0_1 import (
    CandidateSignal,
    EventType,
    FirstPullbackParameterSet,
    FirstPullbackState,
    IngestionSource,
    StrategyRunState,
)
from phase2.quote_aware_price_v0_1 import (
    PricePathKind,
    QuoteAwareNormalizedEvent,
    QuoteAwarePriceEngineV01,
)
from phase2.strategy_clock_v0_2 import FirstPullbackStrategyClockV02, StrategyDeadlineV02
from phase2.strategy_first_pullback_v0_2 import FirstPullbackStrategyV02

from .outcome_replay_v0_1_3 import CandidateReference, PriceObservation

FOUNDATION_PROBE_ID = "FP1-P3-FOUNDATION-PROBE-0001"


@dataclass(frozen=True, slots=True)
class CandidateExtraction:
    signal: CandidateSignal
    triggering_event: QuoteAwareNormalizedEvent
    reference: CandidateReference


@dataclass(frozen=True, slots=True)
class CandidateExtractionSummary:
    candidate_count: int
    run_count: int
    clock_expiry_count: int
    invalidated_count: int
    expired_count: int
    candidate_state_count: int


def foundation_probe_parameters() -> FirstPullbackParameterSet:
    """Fixed, deliberately permissive Phase-3 foundation probe.

    Every value is drawn from PHASE-2-PARAM-SEARCH-001. This is NOT an optimized
    or selected trading strategy. It only supplies causal CandidateSignals for
    validating outcome replay and observability before staged block research.
    """
    return FirstPullbackParameterSet(
        parameter_set_id=FOUNDATION_PROBE_ID,
        strategy_version="v1.1",
        max_entry_age_ms=300_000,
        impulse_parameters={
            "min_return_bps": 500,
            "min_trades_since_t0": 2,
            "min_unique_buyers_since_t0": 1,
        },
        pullback_parameters={"min_depth_bps": 1000, "max_depth_bps": 9000},
        buyer_response_parameters={
            "window_id": "3s",
            "min_rebound_bps": 200,
            "min_buys": 1,
            "min_net_flow_reserve_ppm": 0,
        },
        reclaim_parameters={"min_extension_from_response_bps": 200},
        runaway_entry_parameters={"max_extension_from_response_bps": 10000},
    )


def validate_probe_against_locked_search(
    params: FirstPullbackParameterSet, locked: dict[str, object]
) -> None:
    if locked.get("decision_id") != "PHASE-2-PARAM-SEARCH-001":
        raise ValueError("unexpected locked search decision id")
    checks = (
        (params.max_entry_age_ms, locked["max_entry_age_ms"], "max_entry_age_ms"),
        (params.impulse_parameters["min_return_bps"], locked["impulse"]["min_return_bps"], "min_return_bps"),
        (params.impulse_parameters["min_trades_since_t0"], locked["impulse"]["min_trades_since_t0"], "min_trades_since_t0"),
        (params.impulse_parameters["min_unique_buyers_since_t0"], locked["impulse"]["min_unique_buyers_since_t0"], "min_unique_buyers_since_t0"),
        (params.pullback_parameters["min_depth_bps"], locked["pullback"]["min_depth_bps"], "min_depth_bps"),
        (params.pullback_parameters["max_depth_bps"], locked["pullback"]["max_depth_bps"], "max_depth_bps"),
        (params.buyer_response_parameters["window_id"], locked["buyer_response"]["window_id"], "window_id"),
        (params.buyer_response_parameters["min_rebound_bps"], locked["buyer_response"]["min_rebound_bps"], "min_rebound_bps"),
        (params.buyer_response_parameters["min_buys"], locked["buyer_response"]["min_buys"], "min_buys"),
        (params.buyer_response_parameters["min_net_flow_reserve_ppm"], locked["buyer_response"]["min_net_flow_reserve_ppm"], "min_net_flow_reserve_ppm"),
        (params.reclaim_parameters["min_extension_from_response_bps"], locked["reclaim"]["min_extension_from_response_bps"], "reclaim"),
        (params.runaway_entry_parameters["max_extension_from_response_bps"], locked["runaway"]["max_extension_from_response_bps"], "runaway"),
    )
    for value, allowed, label in checks:
        if value not in allowed:
            raise ValueError(f"foundation probe {label}={value!r} is outside locked search")
    if not params.pullback_parameters["min_depth_bps"] < params.pullback_parameters["max_depth_bps"]:
        raise ValueError("invalid foundation pullback pair")
    if not (
        params.reclaim_parameters["min_extension_from_response_bps"]
        < params.runaway_entry_parameters["max_extension_from_response_bps"]
    ):
        raise ValueError("invalid foundation reclaim/runaway pair")


def us_to_datetime_utc(us: int) -> datetime:
    return datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(microseconds=int(us))


def candidate_reference_from_signal(
    signal: CandidateSignal, triggering_event: QuoteAwareNormalizedEvent
) -> CandidateReference:
    base = triggering_event.base
    if signal.mint != base.mint:
        raise ValueError("candidate mint != triggering event mint")
    if signal.triggering_event_key != base.event_key:
        raise ValueError("candidate triggering_event_key mismatch")
    if signal.generated_at_us != base.observed_at_us:
        raise ValueError("candidate generated_at_us != triggering observed_at_us")

    point = QuoteAwarePriceEngineV01.point(triggering_event)
    if (
        point.price_proxy is None
        or point.numerator_reserve_raw is None
        or point.numerator_reserve_raw <= 0
        or point.token_reserve_raw is None
        or point.token_reserve_raw <= 0
        or point.identity.path == PricePathKind.UNAVAILABLE
    ):
        raise ValueError("candidate triggering event has no valid price identity")
    if signal.reference_price_proxy is None:
        raise ValueError("candidate reference_price_proxy is missing")
    if point.price_proxy != signal.reference_price_proxy:
        raise ValueError("candidate price proxy != triggering price point")

    return CandidateReference(
        candidate_id=signal.signal_id,
        mint=signal.mint,
        signal_at=us_to_datetime_utc(signal.generated_at_us),
        price_identity=point.identity.label,
        price_numerator_raw=int(point.numerator_reserve_raw),
        price_denominator_raw=int(point.token_reserve_raw),
        ingest_seq=int(base.ingest_seq),
    )


def price_observation_from_event(event: QuoteAwareNormalizedEvent) -> PriceObservation | None:
    base = event.base
    if base.event_type not in (EventType.BUY, EventType.SELL):
        return None
    point = QuoteAwarePriceEngineV01.point(event)
    if (
        point.price_proxy is None
        or point.numerator_reserve_raw is None
        or point.numerator_reserve_raw <= 0
        or point.token_reserve_raw is None
        or point.token_reserve_raw <= 0
        or point.identity.path == PricePathKind.UNAVAILABLE
    ):
        return None
    return PriceObservation(
        mint=base.mint,
        observed_at=us_to_datetime_utc(base.observed_at_us),
        ingest_seq=int(base.ingest_seq),
        price_identity=point.identity.label,
        price_numerator_raw=int(point.numerator_reserve_raw),
        price_denominator_raw=int(point.token_reserve_raw),
        is_gap_recovery=(base.source == IngestionSource.GAP_RECOVERY),
        market_continuity_break=(
            base.real_token_reserve_raw is not None
            and int(base.real_token_reserve_raw) <= 0
        ),
        continuity_reason=(
            "PUMP_REAL_TOKEN_RESERVE_NONPOSITIVE"
            if (
                base.real_token_reserve_raw is not None
                and int(base.real_token_reserve_raw) <= 0
            )
            else None
        ),
        source_event_key=base.event_key,
    )


def extract_candidates(
    events: Sequence[QuoteAwareNormalizedEvent],
    params: FirstPullbackParameterSet,
) -> tuple[list[CandidateExtraction], CandidateExtractionSummary]:
    FirstPullbackStrategyV02.validate_parameters(params)
    ordered = sorted(events, key=lambda e: (e.base.observed_at_us, e.base.ingest_seq))

    feature_engine = DenominationAwareFeatureEngineV01()
    runs: dict[str, StrategyRunState] = {}
    deadlines: dict[str, StrategyDeadlineV02] = {}
    candidates: list[CandidateExtraction] = []
    clock_expiry_count = 0

    for event in ordered:
        mint = event.base.mint
        run = runs.get(mint)
        deadline = deadlines.get(mint)

        # Timer MUST fire before a market event strictly outside the 5m window.
        # This prevents integer age_ms flooring from admitting the first 999us
        # after the exact deadline.
        if run is not None and deadline is not None:
            clock_eval = FirstPullbackStrategyClockV02.fire_if_due(
                deadline, run, params, now_us=event.base.observed_at_us
            )
            if clock_eval is not None:
                clock_expiry_count += 1

        state = feature_engine.process(event)

        run = runs.get(mint)
        if run is None:
            run = FirstPullbackStrategyV02.start_run(state, params)
            runs[mint] = run

        if deadlines.get(mint) is None and state.identity.first_tradable_event_at_us is not None:
            scheduled = FirstPullbackStrategyClockV02.schedule_entry_expiry(state, run, params)
            if scheduled is not None:
                deadlines[mint] = scheduled

        evaluation = FirstPullbackStrategyV02.evaluate(state, run, params)
        if evaluation.candidate_signal is not None:
            candidates.append(
                CandidateExtraction(
                    signal=evaluation.candidate_signal,
                    triggering_event=event,
                    reference=candidate_reference_from_signal(evaluation.candidate_signal, event),
                )
            )

    # Silent-market terminal completion. Candidate signals are preserved.
    for mint, run in runs.items():
        deadline = deadlines.get(mint)
        if deadline is None:
            continue
        clock_eval = FirstPullbackStrategyClockV02.fire_if_due(
            deadline, run, params, now_us=deadline.expire_at_us
        )
        if clock_eval is not None:
            clock_expiry_count += 1

    summary = CandidateExtractionSummary(
        candidate_count=len(candidates),
        run_count=len(runs),
        clock_expiry_count=clock_expiry_count,
        invalidated_count=sum(r.current_state == FirstPullbackState.INVALIDATED for r in runs.values()),
        expired_count=sum(r.current_state == FirstPullbackState.EXPIRED for r in runs.values()),
        candidate_state_count=sum(r.current_state == FirstPullbackState.CANDIDATE_SIGNAL for r in runs.values()),
    )
    return candidates, summary
