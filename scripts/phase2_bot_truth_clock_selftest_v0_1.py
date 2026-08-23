from __future__ import annotations

from decimal import Decimal
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.phase2.feature_engine_v0_2 import FeatureEngineV02
from src.phase2.models_v0_1 import EventType, IngestionSource, NormalizedMarketEvent

BASE_EVENT = 1_700_000_000_000_000
BASE_OBS = BASE_EVENT + 600_000_000  # embedded Pump timestamp is 10m behind receipt
MINT = 'ClockTestMint'


def ev(seq, obs_s, market_s, kind, price_num, user, source=IngestionSource.SYNTHETIC_TEST):
    # price proxy = virtual_sol / virtual_token; token reserve fixed for easy ratios
    return NormalizedMarketEvent(
        schema_version='NME-0.1', event_key=f'e{seq}', ingest_seq=seq, mint=MINT,
        event_type=kind, event_at_us=BASE_EVENT + int(market_s*1_000_000),
        observed_at_us=BASE_OBS + int(obs_s*1_000_000), slot=100+seq,
        signature=f'sig{seq}', event_index=0, user=user,
        sol_amount_lamports=100_000_000, token_amount_raw=1,
        virtual_sol_reserve_lamports=price_num, virtual_token_reserve_raw=100,
        source=source,
    )


def check(label, cond):
    if not cond:
        raise AssertionError(label)
    print(f'{label:<62} PASS')


def snapshot(states):
    return [(
        s.schema_version, s.identity.first_tradable_event_at_us, s.identity.age_ms,
        s.windows['3s'].trade_count, s.windows['3s'].buys, s.windows['since_t0'].trade_count,
        str(s.market.current_price_proxy) if s.market.current_price_proxy is not None else None,
        s.data_quality.state_valid,
    ) for s in states]


def run_once():
    engine=FeatureEngineV02()
    events=[
        ev(1,0,0,EventType.BUY,100,'A'),
        ev(2,2,-598,EventType.BUY,120,'B'),  # same 10m-ish embedded offset, fresh at +2s
        ev(3,2.5,-700,EventType.SELL,50,'G',IngestionSource.GAP_RECOVERY),
        ev(4,4,-596,EventType.BUY,130,'C'),
        ev(5,300,-300,EventType.SELL,125,'D'),
        ev(6,300.001,-299,EventType.SELL,124,'E'),
    ]
    return events, [engine.process(e) for e in events]


def main():
    print('PHASE 2 BOT_TRUTH CLOCK REGRESSION SELF-TEST v0.1')
    print('Production DB touched : NO')
    print('Network/RPC used      : NO')
    print()
    events, states=run_once()
    s1,s2,s3,s4,s5,s6=states
    check('Test 1a - t0 equals first OBSERVED tradable time', s1.identity.first_tradable_event_at_us == events[0].observed_at_us)
    check('Test 1b - first observed tradable state age is zero', s1.identity.age_ms == 0)
    check('Test 2a - stale Pump timestamp still appears in fresh 3s flow', s1.windows['3s'].trade_count == 1)
    check('Test 2b - two fresh observations within 3s are counted', s2.windows['3s'].trade_count == 2 and s2.windows['3s'].buys == 2)
    check('Test 3a - GAP_RECOVERY excluded from fresh 3s flow', s3.windows['3s'].trade_count == 2)
    check('Test 3b - GAP_RECOVERY included in known since_t0 history', s3.windows['since_t0'].trade_count == 3)
    check('Test 3c - GAP_RECOVERY does not replace current price', s3.market.current_price_proxy == Decimal(120)/Decimal(100))
    check('Test 3d - GAP_RECOVERY-triggered state marked invalid', s3.data_quality.state_valid is False)
    check('Test 4a - next fresh event restores valid state', s4.data_quality.state_valid is True)
    check('Test 4b - 3s boundary uses observation clock', s4.windows['3s'].trade_count == 2)
    check('Test 5a - exactly 5m has age 300000ms', s5.identity.age_ms == 300_000)
    check('Test 5b - just over 5m is greater than entry window', s6.identity.age_ms == 300_001)
    _, states_b=run_once()
    check('Test 6a - deterministic clock/state replay', snapshot(states) == snapshot(states_b))
    print()
    print('t0 uses observed_at                     : PASS')
    print('Short windows use BOT observation clock : PASS')
    print('Gap recovery not treated as fresh flow  : PASS')
    print('Deterministic replay                    : PASS')
    print('Production DB touched                   : NO')
    print('RESULT                                  : PASS')

if __name__ == '__main__':
    main()
