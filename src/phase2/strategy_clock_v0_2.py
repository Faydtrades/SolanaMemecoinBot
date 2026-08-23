
from __future__ import annotations
from dataclasses import dataclass
from hashlib import sha256

from .denomination_aware_flow_v0_1 import DenominationAwareMarketState
from .models_v0_1 import (
    FirstPullbackParameterSet,
    FirstPullbackState,
    StrategyEvaluation,
    StrategyRunState,
)
from .strategy_first_pullback_v0_2 import FirstPullbackReason, FirstPullbackStrategyV02


@dataclass(frozen=True, slots=True)
class StrategyDeadlineV02:
    schema_version: str
    deadline_id: str
    run_id: str
    mint: str
    first_tradable_observed_at_us: int
    last_eligible_at_us: int
    expire_at_us: int
    reason_code: str


class FirstPullbackStrategyClockV02:
    """SCLOCK-0.2: exact-microsecond entry expiry for FirstPullback v1.1/V02.

    This is the versioned compatibility successor to SCLOCK-0.1.  It preserves
    the locked boundary: exactly t0+300_000ms is eligible; the first microsecond
    after that is expired.  It operates on DenominationAwareMarketState and
    validates parameters through FirstPullbackStrategyV02.
    """
    schema_version = "SCLOCK-0.2"
    deadline_schema_version = "SCLOCK-DEADLINE-0.2"

    @staticmethod
    def _stable_id(prefix: str, *parts: object) -> str:
        payload = "|".join(str(p) for p in parts)
        digest = sha256(payload.encode("utf-8")).hexdigest()[:24]
        return f"{prefix}-{digest}"

    @classmethod
    def schedule_entry_expiry(cls, state: DenominationAwareMarketState, run: StrategyRunState,
                              params: FirstPullbackParameterSet) -> StrategyDeadlineV02 | None:
        FirstPullbackStrategyV02.validate_parameters(params)
        if state.mint != run.mint:
            raise ValueError(f"MarketState mint {state.mint} != run mint {run.mint}")
        if run.strategy_version != params.strategy_version:
            raise ValueError("Run and parameter strategy versions differ")
        if run.parameter_set_id != params.parameter_set_id:
            raise ValueError("Run and parameter_set_id differ")
        t0 = state.identity.first_tradable_event_at_us
        if t0 is None:
            return None
        last_eligible = int(t0) + int(params.max_entry_age_ms) * 1_000
        expire_at = last_eligible + 1
        deadline_id = cls._stable_id(
            "fp1deadline-v02", run.run_id, params.parameter_set_id, last_eligible,
            FirstPullbackReason.ENTRY_WINDOW_EXPIRED.value,
        )
        return StrategyDeadlineV02(
            schema_version=cls.deadline_schema_version,
            deadline_id=deadline_id,
            run_id=run.run_id,
            mint=run.mint,
            first_tradable_observed_at_us=int(t0),
            last_eligible_at_us=last_eligible,
            expire_at_us=expire_at,
            reason_code=FirstPullbackReason.ENTRY_WINDOW_EXPIRED.value,
        )

    @classmethod
    def fire_if_due(cls, deadline: StrategyDeadlineV02, run: StrategyRunState,
                    params: FirstPullbackParameterSet, *, now_us: int) -> StrategyEvaluation | None:
        FirstPullbackStrategyV02.validate_parameters(params)
        if deadline.run_id != run.run_id or deadline.mint != run.mint:
            raise ValueError("Deadline does not belong to supplied strategy run")
        if run.parameter_set_id != params.parameter_set_id:
            raise ValueError("Run and parameter_set_id differ")
        if now_us < deadline.expire_at_us:
            return None
        if run.finished or run.current_state == FirstPullbackState.CANDIDATE_SIGNAL:
            return None
        previous = run.current_state
        run.current_state = FirstPullbackState.EXPIRED
        run.last_transition_at_us = deadline.expire_at_us
        run.finished = True
        run.final_outcome = FirstPullbackState.EXPIRED
        evaluation_id = cls._stable_id(
            "fp1clockeval-v02", run.run_id, deadline.deadline_id,
            previous.value, FirstPullbackState.EXPIRED.value,
        )
        return StrategyEvaluation(
            evaluation_id=evaluation_id,
            run_id=run.run_id,
            mint=run.mint,
            evaluated_at_us=deadline.expire_at_us,
            previous_state=previous,
            current_state=FirstPullbackState.EXPIRED,
            transition_occurred=True,
            reason_code=FirstPullbackReason.ENTRY_WINDOW_EXPIRED.value,
            filters_passed=(),
            filters_failed=("entry_window",),
            signal_type="NONE",
            candidate_signal=None,
            audit_snapshot={
                "clock_schema_version": cls.schema_version,
                "clock_event": "ENTRY_WINDOW_DEADLINE",
                "deadline_id": deadline.deadline_id,
                "first_tradable_observed_at_us": deadline.first_tradable_observed_at_us,
                "max_entry_age_ms": params.max_entry_age_ms,
                "last_eligible_at_us": deadline.last_eligible_at_us,
                "expire_at_us": deadline.expire_at_us,
                "boundary_semantics": "age_us <= max_entry_age_ms*1000 eligible; expire first microsecond after",
            },
        )
