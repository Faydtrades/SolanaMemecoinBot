from __future__ import annotations

import sqlite3
import sys
from decimal import Decimal
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.phase2.models_v0_1 import (
    ConfirmationState,
    EventType,
    IngestionSource,
    NormalizedMarketEvent,
)
from src.phase2.quote_aware_price_v0_1 import (
    Phase1QuoteAwareAdapterV01,
    PricePathKind,
    QuoteAwareNormalizedEvent,
    QuoteAwarePriceEngineV01,
    build_full_universe_validation,
    canonical_json,
    stable_sha256,
)

failures: list[str] = []


def check(label: str, ok: bool) -> None:
    print(f"{label:<72} {'PASS' if ok else 'FAIL'}")
    if not ok:
        failures.append(label)


def base_event(
    key: str,
    seq: int,
    obs_us: int,
    event_type: EventType,
    *,
    vsol: int,
    vtok: int,
    sol_amount: int = 0,
    mint: str = "MintX",
) -> NormalizedMarketEvent:
    return NormalizedMarketEvent(
        schema_version="NME-0.1",
        event_key=key,
        ingest_seq=seq,
        mint=mint,
        event_type=event_type,
        event_at_us=obs_us - 10_000,
        observed_at_us=obs_us,
        slot=100 + seq,
        signature=f"sig{seq}",
        event_index=0,
        user=f"user{seq}",
        sol_amount_lamports=sol_amount,
        token_amount_raw=100,
        virtual_sol_reserve_lamports=vsol,
        virtual_token_reserve_raw=vtok,
        real_sol_reserve_lamports=0,
        real_token_reserve_raw=0,
        source=IngestionSource.SYNTHETIC_TEST,
        confirmation_state=ConfirmationState.PROCESSED,
    )


def qevent(
    key: str,
    seq: int,
    obs_us: int,
    event_type: EventType,
    *,
    vsol: int,
    vtok: int,
    vquote: int | None,
    qamount: int | None,
    quote_mint: str | None,
    sol_amount: int = 0,
    mint: str = "MintX",
) -> QuoteAwareNormalizedEvent:
    return QuoteAwareNormalizedEvent(
        base=base_event(key, seq, obs_us, event_type, vsol=vsol, vtok=vtok, sol_amount=sol_amount, mint=mint),
        quote_mint=quote_mint,
        quote_amount_raw=qamount,
        virtual_quote_reserve_raw=vquote,
        real_quote_reserve_raw=0,
    )


print("PHASE 2 QUOTE-AWARE PRICE PATH SELF-TEST v0.1")
print("Purpose                    : PRICE DENOMINATION + QUOTE FLOW SEPARATION")
print("Synthetic thresholds only  : YES")
print("PnL/future returns          : NOT USED")
print("Strategy optimization       : NOT USED")
print("Production DB touched       : NO")
print()

sol = qevent("s1", 1, 1_000_000, EventType.BUY, vsol=200, vtok=100, vquote=999, qamount=50, quote_mint="Q", sol_amount=10)
sp = QuoteAwarePriceEngineV01.point(sol)
check("Test 1a - positive SOL reserves keep authoritative SOL price path", sp.identity.path == PricePathKind.SOL)
check("Test 1b - SOL proxy remains vSOL/vToken exactly", sp.price_proxy == Decimal(2))

q1 = qevent("q1", 1, 1_000_000, EventType.BUY, vsol=0, vtok=100, vquote=200, qamount=25, quote_mint="QuoteMint", mint="QuoteToken")
q2 = qevent("q2", 2, 2_000_000, EventType.BUY, vsol=0, vtok=80, vquote=240, qamount=30, quote_mint="QuoteMint", mint="QuoteToken")
qp = QuoteAwarePriceEngineV01.point(q1)
check("Test 1c - zero SOL + positive quote reserve selects QUOTE path", qp.identity.path == PricePathKind.QUOTE)
check("Test 1d - quote identity retains exact quote mint", qp.identity.quote_mint == "QuoteMint")
check("Test 1e - quote proxy is vQuote/vToken raw ratio", qp.price_proxy == Decimal(2))

eng = QuoteAwarePriceEngineV01()
st1 = eng.process(q1)
st2 = eng.process(q2)
check("Test 2a - quote path produces causal relative return", st2.return_from_t0_bps == 5000)
check("Test 2b - quote path remains explicitly non-SOL-flow-compatible with StrategyV01", st2.strategy_v01_flow_compatible is False)
check("Test 2c - quote volume retained separately from lamports", st2.quote_buy_volume_raw == 55 and st2.sol_buy_volume_lamports == 0)
check("Test 2d - quote flow denomination retained", st2.quote_flow_mint == "QuoteMint")

# Scaling both quote and token raw units by constants changes the absolute raw
# proxy scale, but relative returns remain identical. This is what allows bps
# structure research without silently claiming a human-readable absolute price.
q1_scaled = qevent("qs1", 1, 1_000_000, EventType.BUY, vsol=0, vtok=1000, vquote=20_000, qamount=25, quote_mint="QuoteMint", mint="Scaled")
q2_scaled = qevent("qs2", 2, 2_000_000, EventType.BUY, vsol=0, vtok=800, vquote=24_000, qamount=30, quote_mint="QuoteMint", mint="Scaled")
eng2 = QuoteAwarePriceEngineV01()
eng2.process(q1_scaled)
st_scaled = eng2.process(q2_scaled)
check("Test 2e - relative bps invariant to fixed raw-unit scaling", st_scaled.return_from_t0_bps == st2.return_from_t0_bps)

mix = QuoteAwarePriceEngineV01()
mix.process(q1)
sol_after_quote = qevent("mix2", 3, 3_000_000, EventType.BUY, vsol=300, vtok=100, vquote=300, qamount=20, quote_mint="QuoteMint", sol_amount=20, mint="QuoteToken")
st_mix = mix.process(sol_after_quote)
check("Test 3a - price-path switch is detected, never silently compared", st_mix.path_conflict_seen is True)
check("Test 3b - mixed identity invalidates comparable state", st_mix.state_valid is False)

unavailable = qevent("u1", 1, 1_000_000, EventType.BUY, vsol=0, vtok=100, vquote=0, qamount=0, quote_mint="QuoteMint", mint="Unavailable")
up = QuoteAwarePriceEngineV01.point(unavailable)
check("Test 3c - zero SOL and zero quote remains unavailable", up.identity.path == PricePathKind.UNAVAILABLE and up.price_proxy is None)

# Production-style sqlite3.Row adapter regression.
conn = sqlite3.connect(":memory:")
conn.row_factory = sqlite3.Row
conn.executescript("""
CREATE TABLE pump_events(
 event_key TEXT, signature TEXT, event_type TEXT, slot INTEGER, pump_timestamp INTEGER,
 mint TEXT, user_wallet TEXT, creator_wallet TEXT, sol_amount_lamports INTEGER,
 token_amount_raw TEXT, virtual_sol_reserves TEXT, virtual_token_reserves TEXT,
 real_sol_reserves TEXT, real_token_reserves TEXT, quote_mint TEXT, quote_amount_raw TEXT,
 virtual_quote_reserves TEXT, real_quote_reserves TEXT, source_decoded_file TEXT,
 decoded_at_utc TEXT
);
""")
conn.execute("""
INSERT INTO pump_events VALUES(
 'sig:0:x','sig','BUY',1,1,'SqlMint','u',NULL,0,'100','0','100','0','0',
 'QuoteMint','25','200','100','LIVE_WEBSOCKET_EVENT_V0_3_3','2026-08-18T14:00:00+00:00'
)
""")
row = conn.execute("SELECT rowid AS p1_rowid,* FROM pump_events").fetchone()
evt, reason = Phase1QuoteAwareAdapterV01.row_to_event(row)
check("Test 4a - production sqlite3.Row quote fields normalize", evt is not None and reason is None)
check("Test 4b - adapter preserves quote reserve and amount", evt is not None and evt.virtual_quote_reserve_raw == 200 and evt.quote_amount_raw == 25)
conn.close()

rows_a, qevents_a, summary_a = build_full_universe_validation([q1, q2, sol])
rows_b, qevents_b, summary_b = build_full_universe_validation([q1, q2, sol])
check("Test 5a - full validation payload is deterministic", stable_sha256((rows_a, qevents_a, summary_a)) == stable_sha256((rows_b, qevents_b, summary_b)))
check("Test 5b - quote rescue is price-only, not a PnL or strategy claim", summary_a["strategy_v01_quote_flow_compatible"] is False and summary_a["absolute_human_price_units_resolved"] is False)

print()
for label, prefix in (
    ("SOL/quote price-path selection", "Test 1"),
    ("Quote-relative price + flow separation", "Test 2"),
    ("Cross-path safety", "Test 3"),
    ("Phase-1 quote adapter", "Test 4"),
    ("Deterministic validation payload", "Test 5"),
):
    print(f"{label:<45}: {'FAIL' if any(x.startswith(prefix) for x in failures) else 'PASS'}")
print(f"{'PnL/future-return performance used':<45}: NO")
print(f"{'Strategy parameter optimization used':<45}: NO")
print(f"{'Production DB touched':<45}: NO")
print(f"{'RESULT':<45}: {'PASS' if not failures else 'FAIL'}")
if failures:
    raise SystemExit(1)
