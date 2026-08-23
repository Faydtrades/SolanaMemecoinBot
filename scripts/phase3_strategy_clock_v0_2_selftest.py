from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from phase2.models_v0_1 import FirstPullbackParameterSet, FirstPullbackState, StrategyRunState
from phase2.strategy_clock_v0_2 import FirstPullbackStrategyClockV02


def params():
    return FirstPullbackParameterSet(
        parameter_set_id="CLOCK-V02-SELFTEST",
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


def make_run():
    return StrategyRunState(
        schema_version="SRS-0.1",
        run_id="RUN-1",
        mint="MINT-1",
        strategy_name="FirstPullback",
        strategy_version="v1.1",
        parameter_set_id="CLOCK-V02-SELFTEST",
        current_state=FirstPullbackState.WAITING_FOR_IMPULSE,
        started_at_us=1_000_000,
        last_transition_at_us=1_000_000,
    )


def main():
    print("PHASE 3.1 / SCLOCK-0.2 COMPATIBILITY SELFTEST")
    print("=" * 52)

    p = params()
    t0 = 10_000_000
    state = SimpleNamespace(
        mint="MINT-1",
        identity=SimpleNamespace(first_tradable_event_at_us=t0),
    )
    run = make_run()
    deadline = FirstPullbackStrategyClockV02.schedule_entry_expiry(state, run, p)
    assert deadline is not None
    assert deadline.last_eligible_at_us == t0 + 300_000_000
    assert deadline.expire_at_us == t0 + 300_000_001
    print("[PASS] FirstPullback v1.1 parameters accepted by SCLOCK-0.2")

    assert FirstPullbackStrategyClockV02.fire_if_due(
        deadline, run, p, now_us=deadline.last_eligible_at_us
    ) is None
    assert run.current_state == FirstPullbackState.WAITING_FOR_IMPULSE
    print("[PASS] exact t0+5m remains eligible")

    ev = FirstPullbackStrategyClockV02.fire_if_due(
        deadline, run, p, now_us=deadline.expire_at_us
    )
    assert ev is not None
    assert ev.evaluated_at_us == deadline.expire_at_us
    assert run.current_state == FirstPullbackState.EXPIRED
    assert run.finished is True
    print("[PASS] first microsecond after 5m expires deterministically")

    run2 = make_run()
    run2.current_state = FirstPullbackState.CANDIDATE_SIGNAL
    run2.signal_emitted = True
    assert FirstPullbackStrategyClockV02.fire_if_due(
        deadline, run2, p, now_us=deadline.expire_at_us
    ) is None
    assert run2.current_state == FirstPullbackState.CANDIDATE_SIGNAL
    print("[PASS] in-window CandidateSignal is preserved")

    bad = FirstPullbackParameterSet(
        parameter_set_id="CLOCK-V01",
        strategy_version="v1.0",
        max_entry_age_ms=300_000,
        impulse_parameters=p.impulse_parameters,
        pullback_parameters=p.pullback_parameters,
        buyer_response_parameters=p.buyer_response_parameters,
        reclaim_parameters=p.reclaim_parameters,
        runaway_entry_parameters=p.runaway_entry_parameters,
    )
    try:
        FirstPullbackStrategyClockV02.schedule_entry_expiry(state, make_run(), bad)
    except ValueError:
        pass
    else:
        raise AssertionError("SCLOCK-0.2 must reject v1.0 params for V02 path")
    print("[PASS] version mismatch remains explicit")

    print("=" * 52)
    print("RESULT: PASS")


if __name__ == "__main__":
    main()
