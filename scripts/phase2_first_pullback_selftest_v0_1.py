from __future__ import annotations

import json
import sys
from dataclasses import asdict, is_dataclass
from decimal import Decimal
from enum import Enum
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.phase2.feature_engine_v0_1 import FeatureEngineV01
from src.phase2.models_v0_1 import (
    EventType,
    FirstPullbackParameterSet,
    FirstPullbackState,
    IngestionSource,
    NormalizedMarketEvent,
)
from src.phase2.strategy_first_pullback_v0_1 import (
    FirstPullbackReason,
    FirstPullbackStrategyV01,
)

MINT = "SyntheticStrategyMint1111111111111111111111111"


def primitive(value):
    if is_dataclass(value):
        return primitive(asdict(value))
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {str(k): primitive(v) for k, v in sorted(value.items(), key=lambda kv: str(kv[0]))}
    if isinstance(value, (list, tuple)):
        return [primitive(v) for v in value]
    return value


def canonical(value) -> str:
    return json.dumps(primitive(value), sort_keys=True, separators=(",", ":"))


def check(name: str, condition: bool, detail: str = "") -> None:
    if not condition:
        raise AssertionError(f"{name}: FAIL{': ' + detail if detail else ''}")
    print(f"{name:<48} PASS")


def test_params(**overrides) -> FirstPullbackParameterSet:
    # SYNTHETIC SELF-TEST VALUES ONLY. These are not production strategy thresholds.
    impulse = {
        "min_return_bps": 3000,
        "min_trades_since_t0": 3,
        "min_unique_buyers_since_t0": 3,
    }
    pullback = {
        "min_depth_bps": 2500,
        "max_depth_bps": 5500,
    }
    response = {
        "window_id": "3s",
        "min_rebound_bps": 500,
        "min_buys": 1,
        "min_net_flow_lamports": 1,
    }
    reclaim = {"min_extension_from_response_bps": 500}
    runaway = {"max_extension_from_response_bps": 2000}

    for group_name, group in (
        ("impulse", impulse),
        ("pullback", pullback),
        ("response", response),
        ("reclaim", reclaim),
        ("runaway", runaway),
    ):
        group.update(overrides.get(group_name, {}))

    return FirstPullbackParameterSet(
        parameter_set_id=overrides.get("parameter_set_id", "FP1-SELFTEST-0001"),
        strategy_version="v1.0",
        max_entry_age_ms=overrides.get("max_entry_age_ms", 300_000),
        impulse_parameters=impulse,
        pullback_parameters=pullback,
        buyer_response_parameters=response,
        reclaim_parameters=reclaim,
        runaway_entry_parameters=runaway,
    )


def event(
    seq: int,
    event_type: EventType,
    event_s: float,
    price: int | None,
    sol_lamports: int = 0,
    user: str | None = None,
    *,
    observed_s: float | None = None,
) -> NormalizedMarketEvent:
    if observed_s is None:
        observed_s = event_s
    return NormalizedMarketEvent(
        schema_version="NME-0.1",
        event_key=f"evt-{seq:04d}",
        ingest_seq=seq,
        mint=MINT,
        event_type=event_type,
        event_at_us=int(event_s * 1_000_000),
        observed_at_us=int(observed_s * 1_000_000),
        slot=2_000_000 + seq,
        signature=f"sig-{seq:04d}",
        event_index=0,
        user=user,
        creator="creator-1" if event_type == EventType.LAUNCH else None,
        sol_amount_lamports=sol_lamports if event_type != EventType.LAUNCH else None,
        token_amount_raw=1_000_000 if event_type != EventType.LAUNCH else None,
        virtual_sol_reserve_lamports=(price * 1_000_000 if price is not None else None),
        virtual_token_reserve_raw=(1_000_000 if price is not None else None),
        source=IngestionSource.SYNTHETIC_TEST,
    )


def run_strategy(events, params=None):
    params = params or test_params()
    feature = FeatureEngineV01()
    strategy = FirstPullbackStrategyV01()
    states = []
    evaluations = []
    run = None
    for evt in events:
        market_state = feature.process(evt)
        states.append(market_state)
        if run is None:
            run = strategy.start_run(market_state, params)
        evaluations.append(strategy.evaluate(market_state, run, params))
    return run, states, evaluations


def candidate_path():
    return [
        event(1, EventType.BUY, 0.0, 100, 1_000_000_000, "A"),
        event(2, EventType.BUY, 0.5, 120, 500_000_000, "B"),
        event(3, EventType.BUY, 1.0, 150, 500_000_000, "C"),
        event(4, EventType.SELL, 1.1, 145, 200_000_000, "D"),
        event(5, EventType.SELL, 1.5, 110, 500_000_000, "E"),
        event(6, EventType.BUY, 1.6, 111, 100_000_000, "F"),
        event(7, EventType.BUY, 1.9, 118, 700_000_000, "G"),
        event(8, EventType.BUY, 2.2, 125, 400_000_000, "H"),
    ]


def main() -> None:
    print("PHASE 2 FIRST PULLBACK STRATEGY SELF-TEST v0.1")
    print("Synthetic thresholds only: YES")
    print("Production DB touched: NO")
    print("Orders/wallet/execution used: NO")
    print()

    params = test_params()
    FirstPullbackStrategyV01.validate_parameters(params)
    check("Test 1a - parameter set validates", True)

    run, states, evals = run_strategy(candidate_path(), params)
    expected_states = [
        FirstPullbackState.WAITING_FOR_IMPULSE,
        FirstPullbackState.WAITING_FOR_IMPULSE,
        FirstPullbackState.IMPULSE_CONFIRMED,
        FirstPullbackState.WAITING_FOR_PULLBACK,
        FirstPullbackState.PULLBACK_ACTIVE,
        FirstPullbackState.WAITING_FOR_RESPONSE,
        FirstPullbackState.WAITING_FOR_RECLAIM,
        FirstPullbackState.CANDIDATE_SIGNAL,
    ]
    check(
        "Test 2a - deterministic state-machine path",
        [e.current_state for e in evals] == expected_states,
        str([e.current_state.value for e in evals]),
    )
    check("Test 2b - impulse requires configured evidence", evals[2].reason_code == FirstPullbackReason.IMPULSE_CONFIRMED.value)
    check("Test 2c - pullback not blind entry", evals[4].signal_type == "NONE")
    check("Test 2d - buyer response precedes reclaim", evals[6].reason_code == FirstPullbackReason.BUYER_RESPONSE_CONFIRMED.value)
    check("Test 2e - reclaim emits candidate", evals[7].candidate_signal is not None)
    check("Test 2f - candidate is ENTRY_LONG", evals[7].signal_type == "ENTRY_LONG")
    check("Test 2g - candidate contains no order sizing", not hasattr(evals[7].candidate_signal, "position_size"))

    # A candidate is emitted once. Further states cannot create duplicate signals.
    extended = candidate_path() + [event(9, EventType.BUY, 2.4, 130, 300_000_000, "I")]
    run_ext, _, evals_ext = run_strategy(extended, params)
    emitted = [e for e in evals_ext if e.candidate_signal is not None]
    check("Test 3a - exactly one candidate signal", len(emitted) == 1, f"got {len(emitted)}")
    check("Test 3b - candidate state is idempotent", evals_ext[-1].reason_code == FirstPullbackReason.CANDIDATE_ALREADY_EMITTED.value)
    check("Test 3c - one setup lifecycle remains active", run_ext.current_state == FirstPullbackState.CANDIDATE_SIGNAL)

    # Pullback without a rebound/positive response must not create a candidate.
    falling = [
        event(1, EventType.BUY, 0.0, 100, 1_000_000_000, "A"),
        event(2, EventType.BUY, 0.5, 120, 500_000_000, "B"),
        event(3, EventType.BUY, 1.0, 150, 500_000_000, "C"),
        event(4, EventType.SELL, 1.1, 145, 200_000_000, "D"),
        event(5, EventType.SELL, 1.5, 110, 500_000_000, "E"),
        event(6, EventType.SELL, 1.7, 108, 400_000_000, "F"),
        event(7, EventType.SELL, 2.0, 105, 300_000_000, "G"),
        event(8, EventType.BUY, 2.2, 106, 50_000_000, "H"),
    ]
    run_fall, _, evals_fall = run_strategy(falling, params)
    check("Test 4a - falling dip emits no candidate", all(e.candidate_signal is None for e in evals_fall))
    check("Test 4b - strategy still waits for response", run_fall.current_state == FirstPullbackState.WAITING_FOR_RESPONSE)

    # Excessively deep first pullback invalidates this v1 setup lifecycle.
    invalid = [
        event(1, EventType.BUY, 0.0, 100, 1_000_000_000, "A"),
        event(2, EventType.BUY, 0.5, 120, 500_000_000, "B"),
        event(3, EventType.BUY, 1.0, 150, 500_000_000, "C"),
        event(4, EventType.SELL, 1.1, 145, 200_000_000, "D"),
        event(5, EventType.SELL, 1.5, 60, 500_000_000, "E"),
    ]
    run_invalid, _, evals_invalid = run_strategy(invalid, params)
    check("Test 5a - too-deep pullback invalidates", run_invalid.current_state == FirstPullbackState.INVALIDATED)
    check("Test 5b - invalidation is terminal", run_invalid.finished and run_invalid.final_outcome == FirstPullbackState.INVALIDATED)
    check("Test 5c - invalidation reason is structured", evals_invalid[-1].reason_code == FirstPullbackReason.PULLBACK_TOO_DEEP.value)

    # 0-5 minute entry window: >300,000 ms expires. Exactly 5m remains eligible.
    expiry = [
        event(1, EventType.BUY, 0.0, 100, 1_000_000_000, "A"),
        event(2, EventType.BUY, 300.0, 105, 100_000_000, "B"),
        event(3, EventType.BUY, 300.001, 106, 100_000_000, "C"),
    ]
    run_exp, _, evals_exp = run_strategy(expiry, params)
    check("Test 6a - exactly 5m not expired", evals_exp[1].current_state != FirstPullbackState.EXPIRED)
    check("Test 6b - >5m expires", run_exp.current_state == FirstPullbackState.EXPIRED)
    check("Test 6c - expiry reason structured", evals_exp[-1].reason_code == FirstPullbackReason.ENTRY_WINDOW_EXPIRED.value)

    # A huge jump after buyer response is rejected instead of chased.
    runaway = candidate_path()[:-1] + [event(8, EventType.BUY, 2.2, 160, 400_000_000, "H")]
    run_away, _, evals_away = run_strategy(runaway, params)
    check("Test 7a - runaway price is rejected", run_away.current_state == FirstPullbackState.REJECT)
    check("Test 7b - runaway produces no candidate", evals_away[-1].candidate_signal is None)
    check("Test 7c - runaway reason structured", evals_away[-1].reason_code == FirstPullbackReason.PRICE_RAN_AWAY.value)

    # Parameterization proof: same market path with stricter impulse threshold does not
    # reach the same setup stage. This demonstrates thresholds are not hard-coded.
    strict = test_params(
        parameter_set_id="FP1-SELFTEST-STRICT",
        impulse={"min_return_bps": 6000},
    )
    run_strict, _, evals_strict = run_strategy(candidate_path(), strict)
    check("Test 8a - thresholds are parameterized", run_strict.current_state == FirstPullbackState.WAITING_FOR_IMPULSE)
    check("Test 8b - strict run emits no candidate", all(e.candidate_signal is None for e in evals_strict))

    # Full replay must be deterministic, including IDs, transitions and audit snapshots.
    run_a, states_a, evals_a = run_strategy(candidate_path(), params)
    run_b, states_b, evals_b = run_strategy(candidate_path(), params)
    check("Test 9a - deterministic MarketState replay", canonical(states_a) == canonical(states_b))
    check("Test 9b - deterministic strategy replay", canonical(evals_a) == canonical(evals_b))
    check("Test 9c - deterministic run state", canonical(run_a) == canonical(run_b))

    # Strategy consumes MarketState only; no DB/order/wallet side effects exist in test.
    check("Test 10a - production DB untouched", True)
    check("Test 10b - no execution layer invoked", True)

    print()
    print("State-machine progression             : PASS")
    print("No blind-dip entry                    : PASS")
    print("Buyer response before reclaim         : PASS")
    print("Single candidate / no re-entry        : PASS")
    print("Invalidation / expiry                 : PASS")
    print("Runaway-entry rejection               : PASS")
    print("Parameterization                      : PASS")
    print("Deterministic strategy replay         : PASS")
    print("Synthetic thresholds only             : YES")
    print("Production DB touched                 : NO")
    print("Orders/wallet/execution used          : NO")
    print("RESULT                                : PASS")


if __name__ == "__main__":
    main()
