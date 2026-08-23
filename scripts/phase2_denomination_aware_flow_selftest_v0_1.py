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

from src.phase2.denomination_aware_flow_v0_1 import (
    DenominationAwareFeatureEngineV01,
    FlowCompatibility,
    flow_point,
    stable_sha256,
)
from src.phase2.models_v0_1 import (
    EventType,
    FirstPullbackParameterSet,
    FirstPullbackState,
    IngestionSource,
    NormalizedMarketEvent,
)
from src.phase2.quote_aware_price_v0_1 import (
    PriceIdentity,
    PricePathKind,
    QuoteAwareNormalizedEvent,
)
from src.phase2.strategy_first_pullback_v0_2 import (
    FirstPullbackReason,
    FirstPullbackStrategyV02,
)

MINT_SOL = "SyntheticFlowSOL111111111111111111111111"
MINT_QUOTE = "SyntheticFlowQUOTE111111111111111111111"
QUOTE_MINT = "SyntheticQuoteMint11111111111111111111111"

failures: list[str] = []


def check(label: str, ok: bool) -> None:
    print(f"{label:<74} {'PASS' if ok else 'FAIL'}")
    if not ok:
        failures.append(label)


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


def qevent(
    mint: str,
    seq: int,
    event_type: EventType,
    t: float,
    price: int,
    flow_ppm: int,
    user: str,
    *,
    path: str,
    scale: int = 1,
    quote_mint: str = QUOTE_MINT,
) -> QuoteAwareNormalizedEvent:
    token_reserve = 1_000_000
    numerator_reserve = price * 1_000_000 * scale
    amount = numerator_reserve * flow_ppm // 1_000_000
    if path == "SOL":
        sol_amount = amount
        v_sol = numerator_reserve
        quote_amount = 0
        v_quote = 0
        qmint = None
    else:
        sol_amount = 0
        v_sol = 0
        quote_amount = amount
        v_quote = numerator_reserve
        qmint = quote_mint

    base = NormalizedMarketEvent(
        schema_version="NME-0.1",
        event_key=f"{mint}-evt-{seq}",
        ingest_seq=seq,
        mint=mint,
        event_type=event_type,
        event_at_us=int(t * 1_000_000),
        observed_at_us=int(t * 1_000_000),
        slot=5_000_000 + seq,
        signature=f"{mint}-sig-{seq}",
        event_index=0,
        user=user,
        creator=None,
        sol_amount_lamports=sol_amount,
        token_amount_raw=1_000_000,
        virtual_sol_reserve_lamports=v_sol,
        virtual_token_reserve_raw=token_reserve,
        real_sol_reserve_lamports=0,
        real_token_reserve_raw=0,
        source=IngestionSource.SYNTHETIC_TEST,
    )
    return QuoteAwareNormalizedEvent(
        base=base,
        quote_mint=qmint,
        quote_amount_raw=quote_amount,
        virtual_quote_reserve_raw=v_quote,
        real_quote_reserve_raw=0,
    )


def path(mint: str, kind: str, *, scale: int = 1, quote_mint: str = QUOTE_MINT):
    # Same causal price path and same reserve-relative flow pressure in both
    # denominations.  Values are synthetic mechanics only.
    spec = [
        (1, EventType.BUY, 0.0, 100, 20_000, "A"),
        (2, EventType.BUY, 0.5, 120, 20_000, "B"),
        (3, EventType.BUY, 1.0, 150, 20_000, "C"),
        (4, EventType.SELL, 1.1, 145, 5_000, "D"),
        (5, EventType.SELL, 1.5, 110, 5_000, "E"),
        (6, EventType.BUY, 1.6, 111, 20_000, "F"),
        (7, EventType.BUY, 1.9, 118, 40_000, "G"),
        (8, EventType.BUY, 2.2, 125, 20_000, "H"),
    ]
    return [
        qevent(mint, seq, et, t, price, ppm, user, path=kind, scale=scale, quote_mint=quote_mint)
        for seq, et, t, price, ppm, user in spec
    ]


def params() -> FirstPullbackParameterSet:
    return FirstPullbackParameterSet(
        parameter_set_id="FP1-FLOW-SELFTEST-0001",
        strategy_version="v1.1",
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
            "min_net_flow_reserve_ppm": 1,
        },
        reclaim_parameters={"min_extension_from_response_bps": 500},
        runaway_entry_parameters={"max_extension_from_response_bps": 2000},
    )


def run(events):
    feature = DenominationAwareFeatureEngineV01()
    strategy = FirstPullbackStrategyV02()
    p = params()
    run_state = None
    states = []
    evals = []
    for e in events:
        s = feature.process(e)
        states.append(s)
        if run_state is None:
            run_state = strategy.start_run(s, p)
        evals.append(strategy.evaluate(s, run_state, p))
    return run_state, states, evals


print("PHASE 2 DENOMINATION-AWARE FLOW / STRATEGY COMPATIBILITY SELF-TEST v0.1")
print("Purpose                    : RAW DENOMINATION SAFETY + RESERVE-NORMALIZED FLOW")
print("Synthetic thresholds only  : YES")
print("PnL/future returns          : NOT USED")
print("Parameter optimization      : NOT PERFORMED")
print("Production DB touched       : NO")
print()

sol = path(MINT_SOL, "SOL")
quote = path(MINT_QUOTE, "QUOTE")

sol_identity = PriceIdentity(PricePathKind.SOL, None)
quote_identity = PriceIdentity(PricePathKind.QUOTE, QUOTE_MINT)
sol_fp = flow_point(sol[0], sol_identity)
quote_fp = flow_point(quote[0], quote_identity)
check("Test 1a - SOL flow identity remains SOL_NATIVE", sol_fp.identity.label == "SOL_NATIVE")
check("Test 1b - quote flow identity retains exact quote mint", quote_fp.identity.label == f"QUOTE:{QUOTE_MINT}")
check("Test 1c - equal reserve fractions give equal normalized ppm", sol_fp.reserve_fraction_ppm == quote_fp.reserve_fraction_ppm == 20_000)
check("Test 1d - quote raw amount is never labeled/stored as lamports", quote[0].base.sol_amount_lamports == 0 and quote[0].quote_amount_raw > 0)

# Scaling quote raw units by a constant changes absolute proxy units but must not
# change relative returns or reserve-normalized flow.
quote_scaled = path("SyntheticScaledQuote", "QUOTE", scale=1_000)
_, q_states, _ = run(quote)
_, qs_states, _ = run(quote_scaled)
check("Test 2a - raw quote-unit scaling leaves relative return invariant", q_states[-1].price_structure.return_from_t0_bps == qs_states[-1].price_structure.return_from_t0_bps)
check("Test 2b - raw quote-unit scaling leaves normalized flow invariant", q_states[-1].windows["3s"].net_flow_reserve_ppm == qs_states[-1].windows["3s"].net_flow_reserve_ppm)
check("Test 2c - quote raw volume remains separately denomination-preserved", q_states[-1].windows["3s"].quote_buy_volume_raw > 0 and q_states[-1].windows["3s"].sol_buy_volume_lamports == 0)

sol_run, sol_states, sol_evals = run(sol)
quote_run, quote_states, quote_evals = run(quote)
check("Test 3a - SOL and quote relative price paths match causally", [s.price_structure.return_from_t0_bps for s in sol_states] == [s.price_structure.return_from_t0_bps for s in quote_states])
check("Test 3b - SOL and quote normalized-flow windows match causally", [s.windows["3s"].net_flow_reserve_ppm for s in sol_states] == [s.windows["3s"].net_flow_reserve_ppm for s in quote_states])
check("Test 3c - SOL raw lamport flow is preserved exactly", sol_states[-1].windows["3s"].net_flow_raw == sol_states[-1].windows["3s"].sol_buy_volume_lamports - sol_states[-1].windows["3s"].sol_sell_volume_lamports)
check("Test 3d - quote raw flow remains quote-denominated", quote_states[-1].windows["3s"].net_flow_raw == quote_states[-1].windows["3s"].quote_buy_volume_raw - quote_states[-1].windows["3s"].quote_sell_volume_raw)
check("Test 3e - both paths are mechanically flow-compatible", sol_states[-1].windows["3s"].flow_compatibility == FlowCompatibility.COMPATIBLE and quote_states[-1].windows["3s"].flow_compatibility == FlowCompatibility.COMPATIBLE)

expected = [
    FirstPullbackState.WAITING_FOR_IMPULSE,
    FirstPullbackState.WAITING_FOR_IMPULSE,
    FirstPullbackState.IMPULSE_CONFIRMED,
    FirstPullbackState.WAITING_FOR_PULLBACK,
    FirstPullbackState.PULLBACK_ACTIVE,
    FirstPullbackState.WAITING_FOR_RESPONSE,
    FirstPullbackState.WAITING_FOR_RECLAIM,
    FirstPullbackState.CANDIDATE_SIGNAL,
]
check("Test 4a - flow-aware strategy follows expected SOL state path", [e.current_state for e in sol_evals] == expected)
check("Test 4b - flow-aware strategy follows same quote state path", [e.current_state for e in quote_evals] == expected)
check("Test 4c - quote path can emit candidate without SOL conversion", quote_evals[-1].candidate_signal is not None and quote_evals[-1].signal_type == "ENTRY_LONG")
check("Test 4d - audit exposes reserve-normalized flow, not fake lamports", "response_window_net_flow_reserve_ppm" in quote_evals[-2].audit_snapshot and "response_window_net_flow_lamports" not in quote_evals[-2].audit_snapshot)

# Quote-mint switch must invalidate comparable state.
conflict = path("SyntheticConflict", "QUOTE")
conflict[-1] = qevent(
    "SyntheticConflict",
    8,
    EventType.BUY,
    2.2,
    125,
    20_000,
    "H",
    path="QUOTE",
    quote_mint="DifferentQuoteMint111111111111111111111",
)
feature = DenominationAwareFeatureEngineV01()
last = None
for e in conflict:
    last = feature.process(e)
assert last is not None
check("Test 5a - quote-mint/path switch is detected", last.flow_identity_conflict_seen)
check("Test 5b - mixed denomination state is invalidated", not last.data_quality.state_valid)
check("Test 5c - conflict window never reports compatible flow", last.windows["3s"].flow_compatibility == FlowCompatibility.IDENTITY_CONFLICT)

# Determinism.
sol_run2, sol_states2, sol_evals2 = run(sol)
quote_run2, quote_states2, quote_evals2 = run(quote)
check("Test 6a - deterministic SOL flow/state replay", stable_sha256((primitive(sol_states), primitive(sol_evals), primitive(sol_run))) == stable_sha256((primitive(sol_states2), primitive(sol_evals2), primitive(sol_run2))))
check("Test 6b - deterministic quote flow/state replay", stable_sha256((primitive(quote_states), primitive(quote_evals), primitive(quote_run))) == stable_sha256((primitive(quote_states2), primitive(quote_evals2), primitive(quote_run2))))
check("Test 6c - parameter remains explicit reserve-normalized feature", "min_net_flow_reserve_ppm" in params().buyer_response_parameters and "min_net_flow_lamports" not in params().buyer_response_parameters)

print()
print(f"{'Denomination identity + raw separation':<48}: {'PASS' if not any(x.startswith('Test 1') for x in failures) else 'FAIL'}")
print(f"{'Dimensionless reserve-normalized flow':<48}: {'PASS' if not any(x.startswith('Test 2') for x in failures) else 'FAIL'}")
print(f"{'SOL/quote causal feature compatibility':<48}: {'PASS' if not any(x.startswith('Test 3') for x in failures) else 'FAIL'}")
print(f"{'Flow-aware First Pullback mechanics':<48}: {'PASS' if not any(x.startswith('Test 4') for x in failures) else 'FAIL'}")
print(f"{'Cross-denomination safety':<48}: {'PASS' if not any(x.startswith('Test 5') for x in failures) else 'FAIL'}")
print(f"{'Deterministic replay':<48}: {'PASS' if not any(x.startswith('Test 6') for x in failures) else 'FAIL'}")
print(f"{'Synthetic thresholds only':<48}: YES")
print(f"{'PnL/future-return performance used':<48}: NO")
print(f"{'Strategy parameter optimization used':<48}: NO")
print(f"{'Production DB touched':<48}: NO")
print(f"{'RESULT':<48}: {'PASS' if not failures else 'FAIL'}")
if failures:
    raise SystemExit(1)
