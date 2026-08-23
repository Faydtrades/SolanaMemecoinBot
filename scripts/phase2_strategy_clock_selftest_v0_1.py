from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from decimal import Decimal
from enum import Enum
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.phase2.feature_engine_v0_2 import FeatureEngineV02
from src.phase2.models_v0_1 import (
    EventType,
    FirstPullbackParameterSet,
    FirstPullbackState,
    IngestionSource,
    NormalizedMarketEvent,
)
from src.phase2.research_replay_v0_3 import run_strategy_probe
from src.phase2.strategy_clock_v0_1 import FirstPullbackStrategyClockV01
from src.phase2.strategy_first_pullback_v0_1 import (
    FirstPullbackReason,
    FirstPullbackStrategyV01,
)

BASE = 1_700_000_000_000_000


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
    print(f"{name:<66} PASS")


def params() -> FirstPullbackParameterSet:
    # SYNTHETIC SELF-TEST VALUES ONLY; not locked trading thresholds.
    return FirstPullbackParameterSet(
        parameter_set_id="FP1-CLOCK-SELFTEST-0001",
        strategy_version="v1.0",
        max_entry_age_ms=300_000,
        impulse_parameters={
            "min_return_bps": 3000,
            "min_trades_since_t0": 3,
            "min_unique_buyers_since_t0": 3,
        },
        pullback_parameters={"min_depth_bps": 2500, "max_depth_bps": 5500},
        buyer_response_parameters={
            "window_id": "3s",
            "min_rebound_bps": 500,
            "min_buys": 1,
            "min_net_flow_lamports": 1,
        },
        reclaim_parameters={"min_extension_from_response_bps": 500},
        runaway_entry_parameters={"max_extension_from_response_bps": 2000},
    )


def ev(
    mint: str,
    seq: int,
    observed_s: float,
    event_type: EventType,
    price: int,
    user: str,
    sol_lamports: int = 100_000_000,
) -> NormalizedMarketEvent:
    return NormalizedMarketEvent(
        schema_version="NME-0.1",
        event_key=f"{mint}-evt-{seq:04d}",
        ingest_seq=seq,
        mint=mint,
        event_type=event_type,
        event_at_us=BASE + int(observed_s * 1_000_000),
        observed_at_us=BASE + int(observed_s * 1_000_000),
        slot=3_000_000 + seq,
        signature=f"{mint}-sig-{seq:04d}",
        event_index=0,
        user=user,
        sol_amount_lamports=sol_lamports,
        token_amount_raw=1_000_000,
        virtual_sol_reserve_lamports=price * 1_000_000,
        virtual_token_reserve_raw=1_000_000,
        source=IngestionSource.SYNTHETIC_TEST,
    )


def candidate_path(mint: str) -> list[NormalizedMarketEvent]:
    return [
        ev(mint, 1, 0.0, EventType.BUY, 100, "A", 1_000_000_000),
        ev(mint, 2, 0.5, EventType.BUY, 120, "B", 500_000_000),
        ev(mint, 3, 1.0, EventType.BUY, 150, "C", 500_000_000),
        ev(mint, 4, 1.1, EventType.SELL, 145, "D", 200_000_000),
        ev(mint, 5, 1.5, EventType.SELL, 110, "E", 500_000_000),
        ev(mint, 6, 1.6, EventType.BUY, 111, "F", 100_000_000),
        ev(mint, 7, 1.9, EventType.BUY, 118, "G", 700_000_000),
        ev(mint, 8, 2.2, EventType.BUY, 125, "H", 400_000_000),
    ]


def run_to_candidate(mint: str):
    p = params()
    feature = FeatureEngineV02()
    strategy = FirstPullbackStrategyV01()
    clock = FirstPullbackStrategyClockV01()
    run = None
    deadline = None
    evaluations = []
    for event in candidate_path(mint):
        state = feature.process(event)
        if run is None:
            run = strategy.start_run(state, p)
        if deadline is None:
            deadline = clock.schedule_entry_expiry(state, run, p)
        evaluations.append(strategy.evaluate(state, run, p))
    assert run is not None and deadline is not None
    return p, run, deadline, evaluations


def main() -> None:
    print("PHASE 2 DETERMINISTIC STRATEGY CLOCK SELF-TEST v0.1")
    print("Purpose                  : TIMER/EXPIRY LIFECYCLE CORRECTNESS")
    print("Synthetic thresholds only: YES")
    print("Production DB touched    : NO")
    print("Network/RPC used         : NO")
    print("Wallet/order/execution   : NO")
    print()

    p = params()
    feature = FeatureEngineV02()
    strategy = FirstPullbackStrategyV01()
    clock = FirstPullbackStrategyClockV01()

    # A token that goes silent immediately after t0 must still expire.
    first = ev("SilentMint", 1, 0.0, EventType.BUY, 100, "A")
    state = feature.process(first)
    run = strategy.start_run(state, p)
    strategy.evaluate(state, run, p)
    deadline = clock.schedule_entry_expiry(state, run, p)
    check("Test 1a - deadline scheduled once t0 exists", deadline is not None)
    assert deadline is not None
    check("Test 1b - deadline anchored to observed t0", deadline.first_tradable_observed_at_us == first.observed_at_us)
    check("Test 1c - exactly 5m remains last eligible instant", deadline.last_eligible_at_us == first.observed_at_us + 300_000_000)
    check("Test 1d - expiry fires first microsecond after 5m", deadline.expire_at_us == deadline.last_eligible_at_us + 1)

    not_due = clock.fire_if_due(deadline, run, p, now_us=deadline.last_eligible_at_us)
    check("Test 2a - clock does not expire at exactly 5m", not_due is None and not run.finished)
    expired = clock.fire_if_due(deadline, run, p, now_us=deadline.expire_at_us)
    check("Test 2b - silent active run expires without market event", expired is not None and run.current_state == FirstPullbackState.EXPIRED)
    assert expired is not None
    check("Test 2c - expiry timestamp equals deterministic deadline", expired.evaluated_at_us == deadline.expire_at_us)
    check("Test 2d - expiry reason is structured", expired.reason_code == FirstPullbackReason.ENTRY_WINDOW_EXPIRED.value)
    check("Test 2e - clock audit snapshot is explicit", expired.audit_snapshot.get("clock_event") == "ENTRY_WINDOW_DEADLINE")

    # A late market event must not get a chance to create a signal before the timer.
    grouped_silent = {
        "LateMint": [
            ev("LateMint", 1, 0.0, EventType.BUY, 100, "A"),
            ev("LateMint", 2, 301.0, EventType.BUY, 200, "B"),
        ]
    }
    probe_late = run_strategy_probe(grouped_silent, ("LateMint",), p)
    check("Test 3a - timer wins before >5m market observation", probe_late["funnel"]["expired"] == 1)
    check("Test 3b - late market observation cannot produce candidate", probe_late["funnel"]["candidate_signal"] == 0)
    check("Test 3c - no OPEN_AT_DATA_END after timer", probe_late["funnel"]["open_at_data_end"] == 0)

    # A valid candidate emitted inside the entry window is preserved, not revoked by clock.
    p_c, run_c, deadline_c, evals_c = run_to_candidate("CandidateMint")
    check("Test 4a - candidate emitted before deadline", run_c.current_state == FirstPullbackState.CANDIDATE_SIGNAL and any(e.candidate_signal for e in evals_c))
    after_candidate = clock.fire_if_due(deadline_c, run_c, p_c, now_us=deadline_c.expire_at_us + 10_000_000)
    check("Test 4b - clock does not revoke candidate", after_candidate is None and run_c.current_state == FirstPullbackState.CANDIDATE_SIGNAL)

    # A pre-deadline terminal invalidation stays terminal and is not overwritten.
    invalid_events = [
        ev("InvalidMint", 1, 0.0, EventType.BUY, 100, "A", 1_000_000_000),
        ev("InvalidMint", 2, 0.5, EventType.BUY, 120, "B", 500_000_000),
        ev("InvalidMint", 3, 1.0, EventType.BUY, 150, "C", 500_000_000),
        ev("InvalidMint", 4, 1.1, EventType.SELL, 145, "D", 200_000_000),
        ev("InvalidMint", 5, 1.5, EventType.SELL, 60, "E", 500_000_000),
    ]
    feature_i = FeatureEngineV02()
    run_i = None
    deadline_i = None
    for event in invalid_events:
        state_i = feature_i.process(event)
        if run_i is None:
            run_i = strategy.start_run(state_i, p)
        if deadline_i is None:
            deadline_i = clock.schedule_entry_expiry(state_i, run_i, p)
        strategy.evaluate(state_i, run_i, p)
    assert run_i is not None and deadline_i is not None
    check("Test 5a - invalidation terminal before timer", run_i.current_state == FirstPullbackState.INVALIDATED and run_i.finished)
    after_invalid = clock.fire_if_due(deadline_i, run_i, p, now_us=deadline_i.expire_at_us)
    check("Test 5b - clock does not overwrite terminal outcome", after_invalid is None and run_i.final_outcome == FirstPullbackState.INVALIDATED)

    # Full timer-aware research replay must be deterministic and terminal-complete.
    grouped = {
        "SilentMint2": [ev("SilentMint2", 1, 0.0, EventType.BUY, 100, "A")],
        "CandidateMint2": candidate_path("CandidateMint2"),
        "InvalidMint2": [
            ev("InvalidMint2", 1, 0.0, EventType.BUY, 100, "A", 1_000_000_000),
            ev("InvalidMint2", 2, 0.5, EventType.BUY, 120, "B", 500_000_000),
            ev("InvalidMint2", 3, 1.0, EventType.BUY, 150, "C", 500_000_000),
            ev("InvalidMint2", 4, 1.1, EventType.SELL, 145, "D", 200_000_000),
            ev("InvalidMint2", 5, 1.5, EventType.SELL, 60, "E", 500_000_000),
        ],
    }
    cohort = ("SilentMint2", "CandidateMint2", "InvalidMint2")
    replay_a = run_strategy_probe(grouped, cohort, p)
    replay_b = run_strategy_probe(grouped, cohort, p)
    f = replay_a["funnel"]
    terminal_total = f["candidate_signal"] + f["invalidated"] + f["expired"] + f["rejected"]
    check("Test 6a - timer-aware replay leaves no open lifecycle", f["open_at_data_end"] == 0)
    check("Test 6b - all synthetic cohort tokens terminal-classified", terminal_total == len(cohort))
    check("Test 6c - deterministic timer-aware replay", canonical(replay_a) == canonical(replay_b))
    check("Test 6d - clock expiry offset is exactly max age + 1us", replay_a["clock"]["clock_expiry_offset_us"]["min"] == 300_000_001)

    print()
    print("Silent-market timer expiry             : PASS")
    print("5m inclusive boundary                  : PASS")
    print("Timer precedes post-window observations: PASS")
    print("Candidate/terminal outcomes preserved  : PASS")
    print("No OPEN_AT_DATA_END in complete replay : PASS")
    print("Deterministic clock replay             : PASS")
    print("Synthetic thresholds only              : YES")
    print("Production DB touched                  : NO")
    print("Network/RPC used                       : NO")
    print("Wallet/order/execution used            : NO")
    print("RESULT                                 : PASS")


if __name__ == "__main__":
    main()
