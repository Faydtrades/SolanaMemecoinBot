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
    IngestionSource,
    NormalizedMarketEvent,
)

MINT = "SyntheticMint111111111111111111111111111111"


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


def event(
    seq: int,
    event_type: EventType,
    event_s: float,
    observed_s: float,
    price: int | None,
    sol_lamports: int = 0,
    user: str | None = None,
    source: IngestionSource = IngestionSource.SYNTHETIC_TEST,
) -> NormalizedMarketEvent:
    # A 1,000,000 token reserve makes the relative price proxy exactly `price`
    # when vSOL is price * 1,000,000. Absolute units are irrelevant to relative BPS.
    return NormalizedMarketEvent(
        schema_version="NME-0.1",
        event_key=f"evt-{seq:04d}",
        ingest_seq=seq,
        mint=MINT,
        event_type=event_type,
        event_at_us=int(event_s * 1_000_000),
        observed_at_us=int(observed_s * 1_000_000),
        slot=1_000_000 + seq,
        signature=f"sig-{seq:04d}",
        event_index=0,
        user=user,
        creator="creator-1" if event_type == EventType.LAUNCH else None,
        sol_amount_lamports=sol_lamports if event_type != EventType.LAUNCH else None,
        token_amount_raw=1_000_000 if event_type != EventType.LAUNCH else None,
        virtual_sol_reserve_lamports=(price * 1_000_000 if price is not None else None),
        virtual_token_reserve_raw=(1_000_000 if price is not None else None),
        source=source,
    )


def run(events):
    engine = FeatureEngineV01()
    return [engine.process(e) for e in events]


def check(name: str, condition: bool, detail: str = "") -> None:
    if not condition:
        raise AssertionError(f"{name}: FAIL{': ' + detail if detail else ''}")
    print(f"{name:<42} PASS")


def main() -> None:
    print("PHASE 2 MARKET STATE SELF-TEST v0.1")
    print("Production DB touched: NO")
    print()

    base = [
        event(1, EventType.BUY, 0.0, 0.0, 100, 1_000_000_000, "A"),
        event(2, EventType.BUY, 0.5, 0.5, 120, 500_000_000, "B"),
        event(3, EventType.SELL, 2.0, 2.0, 150, 250_000_000, "A"),
        event(4, EventType.BUY, 4.0, 4.0, 135, 750_000_000, "C"),
        event(5, EventType.SELL, 11.0, 11.0, 105, 400_000_000, "D"),
    ]

    # TEST 1: deterministic processing and event ordering.
    states_a = run(base)
    states_b = run(base)
    check("Test 1 - deterministic event ordering", canonical(states_a) == canonical(states_b))

    # TEST 2: trailing-window boundary at t=4s; 3s means (1s, 4s].
    s4 = states_a[3]
    w3 = s4.windows["3s"]
    check("Test 2a - 3s window event count", w3.trade_count == 2, f"got {w3.trade_count}")
    check("Test 2b - 3s window buys", w3.buys == 1, f"got {w3.buys}")
    check("Test 2c - 3s window sells", w3.sells == 1, f"got {w3.sells}")
    check("Test 2d - 1s window boundary", s4.windows["1s"].trade_count == 1)
    check("Test 2e - 10s window includes history", s4.windows["10s"].trade_count == 4)

    # TEST 3: no look-ahead. Huge future event cannot alter earlier states.
    future = event(6, EventType.BUY, 20.0, 20.0, 5000, 99_000_000_000, "WHALE")
    states_with_future = run(base[:3] + [future])
    check(
        "Test 3 - no look-ahead",
        canonical(states_a[:3]) == canonical(states_with_future[:3]),
    )

    # TEST 4: delayed event only becomes known at observed_at=8s. Because its
    # event_at=2s, it belongs to since_t0 once observed but not the 3s window at t=8.
    delayed_prefix = [
        event(1, EventType.BUY, 0.0, 0.0, 100, 1_000_000_000, "A"),
        event(2, EventType.BUY, 6.0, 6.0, 140, 500_000_000, "B"),
    ]
    delayed = event(
        3,
        EventType.SELL,
        2.0,
        8.0,
        80,
        250_000_000,
        "C",
        source=IngestionSource.GAP_RECOVERY,
    )
    before_delayed = run(delayed_prefix)
    after_delayed = run(delayed_prefix + [delayed])
    check("Test 4a - delayed event absent earlier", before_delayed[-1].windows["since_t0"].trade_count == 2)
    check("Test 4b - delayed event enters since_t0", after_delayed[-1].windows["since_t0"].trade_count == 3)
    check("Test 4c - stale event not fresh 3s flow", after_delayed[-1].windows["3s"].trade_count == 1)
    check("Test 4d - delayed stale price does not regress current", after_delayed[-1].market.current_price_proxy == Decimal(140))
    check("Test 4e - gap source visible in data quality", after_delayed[-1].data_quality.known_gap_affecting_state)

    # TEST 5: controlled market structure 100 -> 120 -> 150 -> 135 -> 105.
    final = states_a[-1]
    check("Test 5a - high_since_t0", final.price_structure.high_since_t0 == Decimal(150))
    check("Test 5b - low_since_t0", final.price_structure.low_since_t0 == Decimal(100))
    check("Test 5c - current price", final.market.current_price_proxy == Decimal(105))
    check("Test 5d - drawdown is -3000 bps", final.price_structure.drawdown_from_high_bps == -3000)

    # TEST 6: complete deterministic replay.
    check("Test 6 - deterministic full replay", canonical(run(base)) == canonical(run(base)))

    # TEST 7: LAUNCH-only state must be valid, with explicit missing price/t0.
    launch_only = run([event(1, EventType.LAUNCH, 0.0, 0.0, None)])[0]
    check("Test 7a - launch-only state valid", launch_only.data_quality.state_valid)
    check("Test 7b - no invented t0", launch_only.identity.first_tradable_event_at_us is None)
    check("Test 7c - no invented price", launch_only.market.current_price_proxy is None)
    check("Test 7d - no trades in windows", launch_only.windows["since_t0"].trade_count == 0)

    print()
    print("Synthetic deterministic state       : PASS")
    print("Trailing window boundaries          : PASS")
    print("No look-ahead                       : PASS")
    print("event_at / observed_at semantics    : PASS")
    print("Price structure / drawdown          : PASS")
    print("Missing-data handling               : PASS")
    print("Production DB touched               : NO")
    print("RESULT                              : PASS")


if __name__ == "__main__":
    main()
