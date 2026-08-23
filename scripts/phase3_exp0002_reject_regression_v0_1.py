from __future__ import annotations

import json
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from phase2.phase1_readonly_adapter_v0_1 import Phase1ReadOnlyAdapterV01
from phase2.quote_aware_price_v0_1 import Phase1QuoteAwareAdapterV01
from phase2.models_v0_1 import FirstPullbackParameterSet
from phase3.phase2_outcome_adapter_v0_1_2 import extract_candidates


def main() -> None:
    freeze = (
        PROJECT_ROOT
        / "data/research/phase2_research_freeze_v0_1"
        / "phase2_development_dataset_v0_1.sqlite3"
    )
    print("EXP-0002 REJECT ACCOUNTING REGRESSION v0.1")
    print("=" * 58)

    conn = Phase1ReadOnlyAdapterV01.open_readonly(freeze)
    try:
        if conn.execute("PRAGMA quick_check").fetchone()[0] != "ok":
            raise RuntimeError("freeze quick_check failed")
        rows = Phase1QuoteAwareAdapterV01.fetch_all_bot_truth_rows(conn)
    finally:
        conn.close()

    events, skipped = Phase1QuoteAwareAdapterV01.normalize_rows(rows)
    if skipped:
        raise RuntimeError(f"unexpected normalization skips: {skipped}")

    # Exact combination that exposed the v0.1 harness bookkeeping bug.
    params = FirstPullbackParameterSet(
        parameter_set_id="FP1-EXP0002-A-151-R12500-T2-B1-REGRESSION",
        strategy_version="v1.1",
        max_entry_age_ms=300_000,
        impulse_parameters={
            "min_return_bps": 12500,
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

    candidates, s = extract_candidates(events, params)

    expected = {
        "run_count": 3901,
        "candidate_count": 54,
        "candidate_state_count": 54,
        "expired_count": 3835,
        "invalidated_count": 11,
        "rejected_count": 1,
        "trade_count": 0,
        "accounted_run_count": 3901,
    }
    actual = {k: int(getattr(s, k)) for k in expected}

    for key, value in expected.items():
        if actual[key] != value:
            raise RuntimeError(
                f"{key}: expected {value}, got {actual[key]}"
            )

    print(f"Strategy runs       : {s.run_count}")
    print(f"Candidates          : {s.candidate_count}")
    print(f"Expired             : {s.expired_count}")
    print(f"Invalidated         : {s.invalidated_count}")
    print(f"REJECT              : {s.rejected_count}")
    print(f"TRADE               : {s.trade_count}")
    print(f"Accounted runs      : {s.accounted_run_count}")
    print("DEV-FREEZE write    : NO")
    print("=" * 58)
    print("RESULT: PASS")


if __name__ == "__main__":
    main()
