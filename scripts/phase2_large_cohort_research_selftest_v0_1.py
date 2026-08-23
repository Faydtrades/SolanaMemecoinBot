from __future__ import annotations

from decimal import Decimal
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.phase2.large_cohort_research_v0_1 import (
    assign_time_segments,
    run_detailed_strategy_probe,
    stable_sha256,
)
from src.phase2.models_v0_1 import (
    EventType,
    FirstPullbackParameterSet,
    FirstPullbackState,
    IngestionSource,
    NormalizedMarketEvent,
)

BASE = 1_700_000_000_000_000


def check(name: str, condition: bool, detail: str = "") -> None:
    if not condition:
        raise AssertionError(f"{name}: FAIL{': ' + detail if detail else ''}")
    print(f"{name:<72} PASS")


def params() -> FirstPullbackParameterSet:
    # SYNTHETIC SELF-TEST VALUES ONLY; not locked trading thresholds.
    return FirstPullbackParameterSet(
        parameter_set_id="FP1-LARGE-RESEARCH-SELFTEST-0001",
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
    *,
    source: IngestionSource = IngestionSource.SYNTHETIC_TEST,
    cohort_offset_s: int = 0,
) -> NormalizedMarketEvent:
    observed = BASE + cohort_offset_s * 1_000_000 + int(observed_s * 1_000_000)
    return NormalizedMarketEvent(
        schema_version="NME-0.1",
        event_key=f"{mint}-evt-{seq:04d}",
        ingest_seq=cohort_offset_s * 1000 + seq,
        mint=mint,
        event_type=event_type,
        event_at_us=observed,
        observed_at_us=observed,
        slot=3_000_000 + cohort_offset_s * 1000 + seq,
        signature=f"{mint}-sig-{seq:04d}",
        event_index=0,
        user=user,
        sol_amount_lamports=sol_lamports,
        token_amount_raw=1_000_000,
        virtual_sol_reserve_lamports=price * 1_000_000,
        virtual_token_reserve_raw=1_000_000,
        source=source,
    )


def candidate_path(mint: str, offset: int = 0) -> list[NormalizedMarketEvent]:
    return [
        ev(mint, 1, 0.0, EventType.BUY, 100, "A", 1_000_000_000, cohort_offset_s=offset),
        ev(mint, 2, 0.5, EventType.BUY, 120, "B", 500_000_000, cohort_offset_s=offset),
        ev(mint, 3, 1.0, EventType.BUY, 150, "C", 500_000_000, cohort_offset_s=offset),
        ev(mint, 4, 1.1, EventType.SELL, 145, "D", 200_000_000, cohort_offset_s=offset),
        ev(mint, 5, 1.5, EventType.SELL, 110, "E", 500_000_000, cohort_offset_s=offset),
        ev(mint, 6, 1.6, EventType.BUY, 111, "F", 100_000_000, cohort_offset_s=offset),
        ev(mint, 7, 1.9, EventType.BUY, 118, "G", 700_000_000, cohort_offset_s=offset),
        ev(mint, 8, 2.2, EventType.BUY, 125, "H", 400_000_000, cohort_offset_s=offset),
    ]


def main() -> None:
    print("PHASE 2 LARGE-COHORT CHARACTERIZATION SELF-TEST v0.1")
    print("Purpose                    : DETAILED STAGE-METRIC CORRECTNESS")
    print("Synthetic thresholds only  : YES")
    print("PnL/future returns          : NOT USED")
    print("Production DB touched      : NO")
    print("Network/RPC used           : NO")
    print("Wallet/order/execution     : NO")
    print()

    grouped = {
        "CandidateA": candidate_path("CandidateA", 0),
        "CandidateB": candidate_path("CandidateB", 100),
        "Silent": [ev("Silent", 1, 0.0, EventType.BUY, 100, "S", cohort_offset_s=200)],
        "GapToken": [
            ev("GapToken", 1, 0.0, EventType.BUY, 100, "X", cohort_offset_s=300),
            ev(
                "GapToken",
                2,
                0.2,
                EventType.BUY,
                105,
                "Y",
                cohort_offset_s=300,
                source=IngestionSource.GAP_RECOVERY,
            ),
        ],
    }
    cohort = tuple(grouped.keys())
    segment_map, segment_meta = assign_time_segments(grouped, cohort, segment_count=2)
    check("Test 1a - all cohort tokens receive a chronological segment", len(segment_map) == len(cohort))
    check("Test 1b - segment sizes sum to cohort", sum(int(s["tokens"]) for s in segment_meta) == len(cohort))
    check("Test 1c - earlier t0 maps to earlier/equal segment", segment_map["CandidateA"] <= segment_map["CandidateB"] <= segment_map["Silent"])

    result = run_detailed_strategy_probe(grouped, cohort, params(), segment_map=segment_map)
    rows = {row["mint"]: row for row in result["token_rows"]}
    a = rows["CandidateA"]

    check("Test 2a - candidate path reaches candidate", a["final_classification"] == FirstPullbackState.CANDIDATE_SIGNAL.value)
    check("Test 2b - impulse age captured causally", a["impulse_age_ms"] == 1000)
    check("Test 2c - impulse return captured at transition", a["impulse_return_bps"] == 5000)
    check("Test 2d - pullback activation age captured", a["pullback_active_age_ms"] == 1500)
    check("Test 2e - impulse->pullback duration captured", a["impulse_to_pullback_ms"] == 500)
    check("Test 2f - pullback depth captured from impulse high", a["pullback_depth_at_activation_bps"] == 2667)
    check("Test 2g - buyer response age captured", a["response_age_ms"] == 1900)
    check("Test 2h - pullback->response duration captured", a["pullback_to_response_ms"] == 400)
    check("Test 2i - response rebound captured", a["response_rebound_bps"] == 727)
    check("Test 2j - candidate age captured", a["candidate_age_ms"] == 2200)
    check("Test 2k - response->candidate duration captured", a["response_to_candidate_ms"] == 300)
    check("Test 2l - reclaim extension captured", a["reclaim_extension_bps"] == 593)

    check("Test 3a - silent token terminalizes by clock", rows["Silent"]["final_classification"] == FirstPullbackState.EXPIRED.value)
    check("Test 3b - silent token is marked clock-expired", rows["Silent"]["clock_expired"] is True)
    check("Test 3c - silent token preserves furthest setup stage", rows["Silent"]["furthest_setup_stage"] == FirstPullbackState.WAITING_FOR_IMPULSE.value)
    check("Test 3d - no OPEN_AT_DATA_END", result["funnel"]["open_at_data_end"] == 0)
    check("Test 3e - gap provenance is retained", rows["GapToken"]["gap_event_count"] == 1)

    f = result["funnel"]
    check("Test 4a - funnel is monotonic", f["candidate_signal"] <= f["buyer_response_confirmed"] <= f["pullback_active"] <= f["impulse_confirmed"] <= f["eligible_tokens"])
    check("Test 4b - detailed metric summaries only use reached stages", result["metrics"]["candidate_age_ms"]["n"] == 2)
    check("Test 4c - no performance/PnL field invented", "pnl" not in str(result).lower())

    again = run_detailed_strategy_probe(grouped, cohort, params(), segment_map=segment_map)
    check("Test 5a - detailed replay deterministic", stable_sha256(result) == stable_sha256(again))

    print()
    print("Detailed causal stage metrics          : PASS")
    print("Chronological time segmentation        : PASS")
    print("Silent-market terminal completeness    : PASS")
    print("Gap provenance                         : PASS")
    print("Funnel monotonicity                    : PASS")
    print("Deterministic detailed replay          : PASS")
    print("Synthetic thresholds only              : YES")
    print("Production DB touched                  : NO")
    print("PnL/future-return performance used     : NO")
    print("RESULT                                 : PASS")


if __name__ == "__main__":
    main()
