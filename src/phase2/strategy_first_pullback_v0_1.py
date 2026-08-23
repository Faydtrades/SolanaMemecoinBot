from __future__ import annotations

from decimal import Decimal, ROUND_HALF_EVEN
from enum import Enum
from hashlib import sha256
from typing import Any

from .models_v0_1 import (
    CandidateSignal,
    FirstPullbackParameterSet,
    FirstPullbackState,
    MarketPoint,
    MarketState,
    StrategyEvaluation,
    StrategyRunState,
)


class FirstPullbackReason(str, Enum):
    WAITING_FOR_TRADABLE_DATA = "WAITING_FOR_TRADABLE_DATA"
    STRATEGY_ARMED = "STRATEGY_ARMED"
    WAITING_FOR_IMPULSE = "WAITING_FOR_IMPULSE"
    IMPULSE_CONFIRMED = "IMPULSE_CONFIRMED"
    IMPULSE_STAGE_ADVANCED = "IMPULSE_STAGE_ADVANCED"
    WAITING_FOR_PULLBACK = "WAITING_FOR_PULLBACK"
    PULLBACK_ACTIVE = "PULLBACK_ACTIVE"
    PULLBACK_STAGE_ADVANCED = "PULLBACK_STAGE_ADVANCED"
    WAITING_FOR_BUYER_RESPONSE = "WAITING_FOR_BUYER_RESPONSE"
    BUYER_RESPONSE_CONFIRMED = "BUYER_RESPONSE_CONFIRMED"
    RESPONSE_FAILED_NEW_LOW = "RESPONSE_FAILED_NEW_LOW"
    WAITING_FOR_RECLAIM = "WAITING_FOR_RECLAIM"
    RECLAIM_CONFIRMED = "RECLAIM_CONFIRMED"
    PULLBACK_TOO_DEEP = "PULLBACK_TOO_DEEP"
    ENTRY_WINDOW_EXPIRED = "ENTRY_WINDOW_EXPIRED"
    PRICE_RAN_AWAY = "PRICE_RAN_AWAY"
    DATA_INVALID = "DATA_INVALID"
    PRICE_UNAVAILABLE = "PRICE_UNAVAILABLE"
    CANDIDATE_ALREADY_EMITTED = "CANDIDATE_ALREADY_EMITTED"
    TERMINAL_STATE = "TERMINAL_STATE"


class FirstPullbackStrategyV01:
    """Deterministic First Pullback v1 state machine.

    This module interprets MarketState only; it does not read the database, wall clock,
    RPC, or execution state. Thresholds are supplied exclusively through a versioned
    FirstPullbackParameterSet.

    Parameter semantics used by v0.1:
      impulse_parameters:
        min_return_bps
        min_trades_since_t0
        min_unique_buyers_since_t0
      pullback_parameters:
        min_depth_bps              (positive depth, e.g. 2500 = 25%)
        max_depth_bps              (positive terminal invalidation depth)
      buyer_response_parameters:
        window_id                  (e.g. "3s")
        min_rebound_bps            (from tracked pullback low)
        min_buys
        min_net_flow_lamports
      reclaim_parameters:
        min_extension_from_response_bps
      runaway_entry_parameters:
        max_extension_from_response_bps

    These are mechanics for parameterization, not locked production thresholds.
    """

    strategy_name = "FirstPullback"
    strategy_version = "v1.0"
    run_schema_version = "SRS-0.1"

    TERMINAL_STATES = {
        FirstPullbackState.TRADE,
        FirstPullbackState.REJECT,
        FirstPullbackState.INVALIDATED,
        FirstPullbackState.EXPIRED,
    }

    @staticmethod
    def _stable_id(prefix: str, *parts: object) -> str:
        payload = "|".join(str(p) for p in parts)
        digest = sha256(payload.encode("utf-8")).hexdigest()[:24]
        return f"{prefix}-{digest}"

    @staticmethod
    def _bps(current: Decimal, reference: Decimal) -> int | None:
        if reference == 0:
            return None
        raw = ((current / reference) - Decimal(1)) * Decimal(10_000)
        return int(raw.quantize(Decimal("1"), rounding=ROUND_HALF_EVEN))

    @staticmethod
    def _require_int(group: dict[str, Any], key: str) -> int:
        value = group.get(key)
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError(f"Parameter {key!r} must be int, got {value!r}")
        return value

    @staticmethod
    def _require_str(group: dict[str, Any], key: str) -> str:
        value = group.get(key)
        if not isinstance(value, str) or not value:
            raise ValueError(f"Parameter {key!r} must be non-empty str, got {value!r}")
        return value

    @classmethod
    def validate_parameters(cls, params: FirstPullbackParameterSet) -> None:
        if params.strategy_version != cls.strategy_version:
            raise ValueError(
                f"Parameter set strategy_version={params.strategy_version!r} does not match "
                f"engine {cls.strategy_version!r}"
            )
        if params.max_entry_age_ms <= 0:
            raise ValueError("max_entry_age_ms must be > 0")

        min_impulse = cls._require_int(params.impulse_parameters, "min_return_bps")
        min_trades = cls._require_int(params.impulse_parameters, "min_trades_since_t0")
        min_unique = cls._require_int(params.impulse_parameters, "min_unique_buyers_since_t0")
        min_depth = cls._require_int(params.pullback_parameters, "min_depth_bps")
        max_depth = cls._require_int(params.pullback_parameters, "max_depth_bps")
        response_window = cls._require_str(params.buyer_response_parameters, "window_id")
        min_rebound = cls._require_int(params.buyer_response_parameters, "min_rebound_bps")
        min_response_buys = cls._require_int(params.buyer_response_parameters, "min_buys")
        cls._require_int(params.buyer_response_parameters, "min_net_flow_lamports")
        min_reclaim = cls._require_int(
            params.reclaim_parameters, "min_extension_from_response_bps"
        )
        max_extension = cls._require_int(
            params.runaway_entry_parameters, "max_extension_from_response_bps"
        )

        if min_impulse < 0 or min_trades < 1 or min_unique < 1:
            raise ValueError("Impulse thresholds must be non-negative and counts >= 1")
        if min_depth <= 0 or max_depth <= min_depth:
            raise ValueError("Require 0 < min_depth_bps < max_depth_bps")
        if min_rebound < 0 or min_response_buys < 0:
            raise ValueError("Buyer-response thresholds must be non-negative")
        if min_reclaim < 0 or max_extension <= min_reclaim:
            raise ValueError(
                "Require 0 <= min_extension_from_response_bps < "
                "max_extension_from_response_bps"
            )
        if response_window not in {"1s", "3s", "10s", "30s", "60s", "since_t0"}:
            raise ValueError(f"Unsupported buyer-response window: {response_window}")

    @classmethod
    def start_run(
        cls, market_state: MarketState, params: FirstPullbackParameterSet
    ) -> StrategyRunState:
        cls.validate_parameters(params)
        started_at_us = market_state.as_of_observed_at_us
        run_id = cls._stable_id(
            "fp1run",
            market_state.mint,
            started_at_us,
            params.strategy_version,
            params.parameter_set_id,
        )
        return StrategyRunState(
            schema_version=cls.run_schema_version,
            run_id=run_id,
            mint=market_state.mint,
            strategy_name=cls.strategy_name,
            strategy_version=cls.strategy_version,
            parameter_set_id=params.parameter_set_id,
            current_state=FirstPullbackState.DISCOVERED,
            started_at_us=started_at_us,
            last_transition_at_us=started_at_us,
        )

    @staticmethod
    def _point(state: MarketState) -> MarketPoint | None:
        price = state.market.current_price_proxy
        if price is None:
            return None
        # MarketState v0.1 exposes deterministic per-token state_seq rather than the
        # original global ingest_seq. event_key + observed_at still identify the
        # triggering causal observation; state_seq supplies deterministic ordering.
        return MarketPoint(
            event_key=state.triggering_event_key,
            observed_at_us=state.as_of_observed_at_us,
            ingest_seq=state.state_seq,
            price_proxy=price,
        )

    @staticmethod
    def _audit_snapshot(state: MarketState, response_window_id: str) -> dict[str, object]:
        window = state.windows.get(response_window_id)
        return {
            "market_state_version": state.schema_version,
            "state_seq": state.state_seq,
            "triggering_event_key": state.triggering_event_key,
            "age_ms": state.identity.age_ms,
            "current_price_proxy": state.market.current_price_proxy,
            "return_from_t0_bps": state.price_structure.return_from_t0_bps,
            "drawdown_from_high_bps": state.price_structure.drawdown_from_high_bps,
            "known_gap_affecting_state": state.data_quality.known_gap_affecting_state,
            "response_window_id": response_window_id,
            "response_window_buys": window.buys if window else None,
            "response_window_sells": window.sells if window else None,
            "response_window_net_flow_lamports": window.net_flow_lamports if window else None,
        }

    @classmethod
    def _evaluation(
        cls,
        state: MarketState,
        run: StrategyRunState,
        previous_state: FirstPullbackState,
        reason: FirstPullbackReason,
        params: FirstPullbackParameterSet,
        *,
        candidate: CandidateSignal | None = None,
        filters_passed: tuple[str, ...] = (),
        filters_failed: tuple[str, ...] = (),
    ) -> StrategyEvaluation:
        signal_type = candidate.signal_type if candidate else "NONE"
        evaluation_id = cls._stable_id(
            "fp1eval",
            run.run_id,
            state.state_seq,
            previous_state.value,
            run.current_state.value,
            reason.value,
        )
        response_window_id = cls._require_str(
            params.buyer_response_parameters, "window_id"
        )
        return StrategyEvaluation(
            evaluation_id=evaluation_id,
            run_id=run.run_id,
            mint=run.mint,
            evaluated_at_us=state.as_of_observed_at_us,
            previous_state=previous_state,
            current_state=run.current_state,
            transition_occurred=previous_state != run.current_state,
            reason_code=reason.value,
            filters_passed=filters_passed,
            filters_failed=filters_failed,
            signal_type=signal_type,
            candidate_signal=candidate,
            audit_snapshot=cls._audit_snapshot(state, response_window_id),
        )

    @staticmethod
    def _transition(
        run: StrategyRunState,
        new_state: FirstPullbackState,
        observed_at_us: int,
    ) -> None:
        run.current_state = new_state
        run.last_transition_at_us = observed_at_us
        if new_state in FirstPullbackStrategyV01.TERMINAL_STATES:
            run.finished = True
            run.final_outcome = new_state

    @classmethod
    def evaluate(
        cls,
        state: MarketState,
        run: StrategyRunState,
        params: FirstPullbackParameterSet,
    ) -> StrategyEvaluation:
        cls.validate_parameters(params)
        if state.mint != run.mint:
            raise ValueError(f"MarketState mint {state.mint} != run mint {run.mint}")
        if run.strategy_version != params.strategy_version:
            raise ValueError("Run and parameter strategy versions differ")
        if run.parameter_set_id != params.parameter_set_id:
            raise ValueError("Run and parameter_set_id differ")

        previous = run.current_state

        if run.current_state in cls.TERMINAL_STATES:
            return cls._evaluation(
                state, run, previous, FirstPullbackReason.TERMINAL_STATE, params
            )

        if run.current_state == FirstPullbackState.CANDIDATE_SIGNAL:
            return cls._evaluation(
                state,
                run,
                previous,
                FirstPullbackReason.CANDIDATE_ALREADY_EMITTED,
                params,
            )

        if not state.data_quality.state_valid:
            return cls._evaluation(
                state, run, previous, FirstPullbackReason.DATA_INVALID, params,
                filters_failed=("state_valid",),
            )

        age_ms = state.identity.age_ms
        if age_ms is not None and age_ms > params.max_entry_age_ms:
            cls._transition(run, FirstPullbackState.EXPIRED, state.as_of_observed_at_us)
            return cls._evaluation(
                state,
                run,
                previous,
                FirstPullbackReason.ENTRY_WINDOW_EXPIRED,
                params,
                filters_failed=("entry_window",),
            )

        price = state.market.current_price_proxy
        point = cls._point(state)

        if run.current_state == FirstPullbackState.DISCOVERED:
            if state.identity.first_tradable_event_at_us is None or point is None:
                return cls._evaluation(
                    state,
                    run,
                    previous,
                    FirstPullbackReason.WAITING_FOR_TRADABLE_DATA,
                    params,
                    filters_failed=("tradable_market_data",),
                )
            run.impulse_start_ref = point
            cls._transition(
                run, FirstPullbackState.WAITING_FOR_IMPULSE, state.as_of_observed_at_us
            )
            return cls._evaluation(
                state,
                run,
                previous,
                FirstPullbackReason.STRATEGY_ARMED,
                params,
                filters_passed=("tradable_market_data",),
            )

        if point is None or price is None:
            return cls._evaluation(
                state,
                run,
                previous,
                FirstPullbackReason.PRICE_UNAVAILABLE,
                params,
                filters_failed=("current_price",),
            )

        if run.current_state == FirstPullbackState.WAITING_FOR_IMPULSE:
            since_t0 = state.windows["since_t0"]
            min_return = cls._require_int(params.impulse_parameters, "min_return_bps")
            min_trades = cls._require_int(
                params.impulse_parameters, "min_trades_since_t0"
            )
            min_unique = cls._require_int(
                params.impulse_parameters, "min_unique_buyers_since_t0"
            )
            checks = {
                "impulse_return": (
                    state.price_structure.return_from_t0_bps is not None
                    and state.price_structure.return_from_t0_bps >= min_return
                ),
                "impulse_trade_count": since_t0.trade_count >= min_trades,
                "impulse_unique_buyers": since_t0.unique_buyers >= min_unique,
            }
            passed = tuple(k for k, v in checks.items() if v)
            failed = tuple(k for k, v in checks.items() if not v)
            if all(checks.values()):
                run.impulse_high_ref = point
                cls._transition(
                    run, FirstPullbackState.IMPULSE_CONFIRMED, state.as_of_observed_at_us
                )
                return cls._evaluation(
                    state,
                    run,
                    previous,
                    FirstPullbackReason.IMPULSE_CONFIRMED,
                    params,
                    filters_passed=passed,
                )
            return cls._evaluation(
                state,
                run,
                previous,
                FirstPullbackReason.WAITING_FOR_IMPULSE,
                params,
                filters_passed=passed,
                filters_failed=failed,
            )

        if run.current_state == FirstPullbackState.IMPULSE_CONFIRMED:
            if run.impulse_high_ref is None or price > run.impulse_high_ref.price_proxy:
                run.impulse_high_ref = point
            cls._transition(
                run, FirstPullbackState.WAITING_FOR_PULLBACK, state.as_of_observed_at_us
            )
            return cls._evaluation(
                state,
                run,
                previous,
                FirstPullbackReason.IMPULSE_STAGE_ADVANCED,
                params,
            )

        if run.current_state == FirstPullbackState.WAITING_FOR_PULLBACK:
            if run.impulse_high_ref is None or price > run.impulse_high_ref.price_proxy:
                run.impulse_high_ref = point

            drawdown = state.price_structure.drawdown_from_high_bps
            depth = -drawdown if drawdown is not None and drawdown < 0 else 0
            min_depth = cls._require_int(params.pullback_parameters, "min_depth_bps")
            max_depth = cls._require_int(params.pullback_parameters, "max_depth_bps")

            if depth >= max_depth:
                run.pullback_low_ref = point
                cls._transition(
                    run, FirstPullbackState.INVALIDATED, state.as_of_observed_at_us
                )
                return cls._evaluation(
                    state,
                    run,
                    previous,
                    FirstPullbackReason.PULLBACK_TOO_DEEP,
                    params,
                    filters_failed=("pullback_max_depth",),
                )
            if depth >= min_depth:
                run.pullback_low_ref = point
                cls._transition(
                    run, FirstPullbackState.PULLBACK_ACTIVE, state.as_of_observed_at_us
                )
                return cls._evaluation(
                    state,
                    run,
                    previous,
                    FirstPullbackReason.PULLBACK_ACTIVE,
                    params,
                    filters_passed=("pullback_min_depth", "pullback_max_depth"),
                )
            return cls._evaluation(
                state,
                run,
                previous,
                FirstPullbackReason.WAITING_FOR_PULLBACK,
                params,
                filters_failed=("pullback_min_depth",),
            )

        if run.current_state == FirstPullbackState.PULLBACK_ACTIVE:
            if run.pullback_low_ref is None or price < run.pullback_low_ref.price_proxy:
                run.pullback_low_ref = point
            cls._transition(
                run, FirstPullbackState.WAITING_FOR_RESPONSE, state.as_of_observed_at_us
            )
            return cls._evaluation(
                state,
                run,
                previous,
                FirstPullbackReason.PULLBACK_STAGE_ADVANCED,
                params,
            )

        if run.current_state == FirstPullbackState.WAITING_FOR_RESPONSE:
            max_depth = cls._require_int(params.pullback_parameters, "max_depth_bps")
            drawdown = state.price_structure.drawdown_from_high_bps
            depth = -drawdown if drawdown is not None and drawdown < 0 else 0
            if depth >= max_depth:
                run.pullback_low_ref = point
                cls._transition(
                    run, FirstPullbackState.INVALIDATED, state.as_of_observed_at_us
                )
                return cls._evaluation(
                    state,
                    run,
                    previous,
                    FirstPullbackReason.PULLBACK_TOO_DEEP,
                    params,
                    filters_failed=("pullback_max_depth",),
                )

            if run.pullback_low_ref is None or price < run.pullback_low_ref.price_proxy:
                run.pullback_low_ref = point
                return cls._evaluation(
                    state,
                    run,
                    previous,
                    FirstPullbackReason.WAITING_FOR_BUYER_RESPONSE,
                    params,
                    filters_failed=("price_rebound",),
                )

            window_id = cls._require_str(params.buyer_response_parameters, "window_id")
            window = state.windows[window_id]
            min_rebound = cls._require_int(
                params.buyer_response_parameters, "min_rebound_bps"
            )
            min_buys = cls._require_int(params.buyer_response_parameters, "min_buys")
            min_flow = cls._require_int(
                params.buyer_response_parameters, "min_net_flow_lamports"
            )
            rebound = cls._bps(price, run.pullback_low_ref.price_proxy)
            checks = {
                "price_rebound": rebound is not None and rebound >= min_rebound,
                "buyer_response_buys": window.buys >= min_buys,
                "buyer_response_net_flow": window.net_flow_lamports >= min_flow,
            }
            passed = tuple(k for k, v in checks.items() if v)
            failed = tuple(k for k, v in checks.items() if not v)
            if all(checks.values()):
                run.response_ref = point
                cls._transition(
                    run, FirstPullbackState.WAITING_FOR_RECLAIM, state.as_of_observed_at_us
                )
                return cls._evaluation(
                    state,
                    run,
                    previous,
                    FirstPullbackReason.BUYER_RESPONSE_CONFIRMED,
                    params,
                    filters_passed=passed,
                )
            return cls._evaluation(
                state,
                run,
                previous,
                FirstPullbackReason.WAITING_FOR_BUYER_RESPONSE,
                params,
                filters_passed=passed,
                filters_failed=failed,
            )

        if run.current_state == FirstPullbackState.WAITING_FOR_RECLAIM:
            max_depth = cls._require_int(params.pullback_parameters, "max_depth_bps")
            drawdown = state.price_structure.drawdown_from_high_bps
            depth = -drawdown if drawdown is not None and drawdown < 0 else 0
            if depth >= max_depth:
                run.pullback_low_ref = point
                cls._transition(
                    run, FirstPullbackState.INVALIDATED, state.as_of_observed_at_us
                )
                return cls._evaluation(
                    state,
                    run,
                    previous,
                    FirstPullbackReason.PULLBACK_TOO_DEEP,
                    params,
                    filters_failed=("pullback_max_depth",),
                )

            if run.pullback_low_ref is not None and price < run.pullback_low_ref.price_proxy:
                run.pullback_low_ref = point
                run.response_ref = None
                cls._transition(
                    run, FirstPullbackState.WAITING_FOR_RESPONSE, state.as_of_observed_at_us
                )
                return cls._evaluation(
                    state,
                    run,
                    previous,
                    FirstPullbackReason.RESPONSE_FAILED_NEW_LOW,
                    params,
                    filters_failed=("response_structure_held",),
                )

            if run.response_ref is None:
                cls._transition(
                    run, FirstPullbackState.WAITING_FOR_RESPONSE, state.as_of_observed_at_us
                )
                return cls._evaluation(
                    state,
                    run,
                    previous,
                    FirstPullbackReason.RESPONSE_FAILED_NEW_LOW,
                    params,
                )

            extension = cls._bps(price, run.response_ref.price_proxy)
            min_reclaim = cls._require_int(
                params.reclaim_parameters, "min_extension_from_response_bps"
            )
            max_extension = cls._require_int(
                params.runaway_entry_parameters, "max_extension_from_response_bps"
            )

            if extension is not None and extension > max_extension:
                cls._transition(run, FirstPullbackState.REJECT, state.as_of_observed_at_us)
                return cls._evaluation(
                    state,
                    run,
                    previous,
                    FirstPullbackReason.PRICE_RAN_AWAY,
                    params,
                    filters_failed=("runaway_entry_limit",),
                )

            if extension is not None and extension >= min_reclaim:
                run.reclaim_ref = point
                run.signal_emitted = True
                cls._transition(
                    run, FirstPullbackState.CANDIDATE_SIGNAL, state.as_of_observed_at_us
                )
                signal_id = cls._stable_id(
                    "fp1sig",
                    run.run_id,
                    state.triggering_event_key,
                    params.parameter_set_id,
                    cls.strategy_version,
                )
                candidate = CandidateSignal(
                    signal_id=signal_id,
                    run_id=run.run_id,
                    mint=run.mint,
                    strategy_version=cls.strategy_version,
                    parameter_set_id=params.parameter_set_id,
                    generated_at_us=state.as_of_observed_at_us,
                    signal_type="ENTRY_LONG",
                    reference_price_proxy=price,
                    triggering_event_key=state.triggering_event_key,
                    reason_code=FirstPullbackReason.RECLAIM_CONFIRMED.value,
                )
                return cls._evaluation(
                    state,
                    run,
                    previous,
                    FirstPullbackReason.RECLAIM_CONFIRMED,
                    params,
                    candidate=candidate,
                    filters_passed=("reclaim_min_extension", "runaway_entry_limit"),
                )

            return cls._evaluation(
                state,
                run,
                previous,
                FirstPullbackReason.WAITING_FOR_RECLAIM,
                params,
                filters_failed=("reclaim_min_extension",),
            )

        raise RuntimeError(f"Unhandled FirstPullback state: {run.current_state}")
