from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.phase2.denomination_aware_flow_v0_1 import DenominationAwareFeatureEngineV01
from src.phase2.feature_engine_v0_2 import FeatureEngineV02
from src.phase2.models_v0_1 import EventType, IngestionSource, NormalizedMarketEvent
from src.phase2.quote_aware_price_v0_1 import QuoteAwareNormalizedEvent, group_events

failures: list[str] = []


def check(label: str, ok: bool) -> None:
    print(f"{label:<78} {'PASS' if ok else 'FAIL'}")
    if not ok:
        failures.append(label)


def qevent(
    *,
    mint: str,
    seq: int,
    event_type: EventType,
    t_s: float,
    sol_amount: int,
    v_sol: int,
    v_tok: int = 1_000_000,
) -> QuoteAwareNormalizedEvent:
    base = NormalizedMarketEvent(
        schema_version="NME-0.1",
        event_key=f"{mint}-{seq}",
        ingest_seq=seq,
        mint=mint,
        event_type=event_type,
        event_at_us=int(t_s * 1_000_000),
        observed_at_us=int(t_s * 1_000_000),
        slot=1_000 + seq,
        signature=f"sig-{mint}-{seq}",
        event_index=0,
        user=f"user-{seq}",
        creator=None,
        sol_amount_lamports=sol_amount,
        token_amount_raw=1_000,
        virtual_sol_reserve_lamports=v_sol,
        virtual_token_reserve_raw=v_tok,
        real_sol_reserve_lamports=0,
        real_token_reserve_raw=0,
        source=IngestionSource.SYNTHETIC_TEST,
    )
    return QuoteAwareNormalizedEvent(
        base=base,
        quote_mint=None,
        quote_amount_raw=0,
        virtual_quote_reserve_raw=0,
        real_quote_reserve_raw=0,
    )


print("PHASE 2 DENOMINATION-AWARE FLOW REAL-DATA REGRESSION SELF-TEST v0.1.1")
print("Purpose                    : ELIGIBLE-COHORT ACCOUNTING + SOL RAW PARITY")
print("PnL/future returns          : NOT USED")
print("Parameter optimization      : NOT PERFORMED")
print("Production DB touched       : NO")
print()

# Regression 1: group_events can legitimately include a launch-only mint.  The
# eligible BOT_TRUTH universe requires >=1 observed tradable BUY/SELL.
launch_only = qevent(
    mint="LaunchOnlyMint",
    seq=1,
    event_type=EventType.LAUNCH,
    t_s=0.0,
    sol_amount=0,
    v_sol=0,
)
trade = qevent(
    mint="TradableMint",
    seq=2,
    event_type=EventType.BUY,
    t_s=1.0,
    sol_amount=100,
    v_sol=10_000,
)
grouped = group_events([launch_only, trade])
eligible = {
    mint
    for mint, events in grouped.items()
    if any(e.base.event_type in (EventType.BUY, EventType.SELL) for e in events)
}
check("Test 1a - grouped universe may contain launch-only mints", len(grouped) == 2)
check("Test 1b - eligible universe counts only mints with tradable events", eligible == {"TradableMint"})
check("Test 1c - launch-only mint is not misclassified as an eligible token", "LaunchOnlyMint" not in eligible)

# Regression 2: a SOL_ONLY token may have a later trade whose SOL/token reserve
# is unavailable for reserve-normalized flow, while its raw SOL amount remains
# authoritative and must remain part of legacy lamport-flow parity.
events = [
    qevent(mint="SolParityMint", seq=10, event_type=EventType.BUY, t_s=0.0, sol_amount=100, v_sol=10_000),
    qevent(mint="SolParityMint", seq=11, event_type=EventType.BUY, t_s=1.0, sol_amount=40, v_sol=0),
    qevent(mint="SolParityMint", seq=12, event_type=EventType.SELL, t_s=2.0, sol_amount=25, v_sol=11_000),
]

legacy = FeatureEngineV02()
aware = DenominationAwareFeatureEngineV01()
old_state = new_state = None
for event in events:
    old_state = legacy.process(event.base)
    new_state = aware.process(event)

assert old_state is not None and new_state is not None
ow = old_state.windows["since_t0"]
nw = new_state.windows["since_t0"]

check("Test 2a - raw SOL buy volume preserves all authoritative trade amounts", ow.buy_volume_lamports == nw.sol_buy_volume_lamports == 140)
check("Test 2b - raw SOL sell volume preserves all authoritative trade amounts", ow.sell_volume_lamports == nw.sol_sell_volume_lamports == 25)
check(
    "Test 2c - legacy raw net flow equals denomination-preserved SOL buy-sell",
    ow.net_flow_lamports == (nw.sol_buy_volume_lamports - nw.sol_sell_volume_lamports) == 115,
)
check(
    "Test 2d - reserve-normalized compatible-flow raw net is intentionally narrower",
    nw.net_flow_raw != ow.net_flow_lamports
    and nw.normalized_flow_event_count < nw.trade_count
    and nw.flow_unavailable_event_count > 0,
)
check(
    "Test 2e - price/return semantics still match on priceable SOL observations",
    old_state.market.current_price_proxy == new_state.market.current_price_proxy
    and old_state.price_structure.return_from_t0_bps == new_state.price_structure.return_from_t0_bps
    and old_state.price_structure.drawdown_from_high_bps == new_state.price_structure.drawdown_from_high_bps,
)

print()
print(f"{'Eligible BOT_TRUTH cohort accounting':<52}: {'PASS' if not any(x.startswith('Test 1') for x in failures) else 'FAIL'}")
print(f"{'SOL raw-flow parity vs normalized-flow scope':<52}: {'PASS' if not any(x.startswith('Test 2') for x in failures) else 'FAIL'}")
print(f"{'PnL/future-return performance used':<52}: NO")
print(f"{'Strategy parameter optimization used':<52}: NO")
print(f"{'Production DB touched':<52}: NO")
print(f"{'RESULT':<52}: {'PASS' if not failures else 'FAIL'}")

if failures:
    raise SystemExit(1)
