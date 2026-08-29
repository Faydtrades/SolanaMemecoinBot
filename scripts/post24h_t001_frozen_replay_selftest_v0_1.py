from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from phase4.paper_cost_baseline_v0_1 import P4_COST_BASELINE_0001  # noqa: E402
from phase4.paper_cost_model_v0_2 import CostSide, FillDecision, PaperCostModelV02, RationalPrice  # noqa: E402
from phase4.paper_exit_orchestrator_v0_1 import ExitMarketObservation, ExitPositionRef, PaperExitOrchestratorV01  # noqa: E402
from phase4.post24h_frozen_replay_v0_1 import (  # noqa: E402
    EXPECTED,
    EXPECTED_COHORT_SHA256,
    EXPECTED_PHYSICAL_ENTRIES,
    EXPECTED_SOURCE_ROWS,
    FROZEN_WATERMARK,
    SOURCE_ANCHOR,
    is_post_entry_source_row,
    reproduce,
)


PAPER = ROOT / "data/paper/multihour/phase4_firstpullback_multihour_20260827T123022_384349Z.sqlite3"
SOURCE = ROOT / "data/db/tradingbot.sqlite3"


def _synthetic_first_crossing() -> None:
    import sqlite3
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    ref = ExitPositionRef(
        paper_position_id="p", paper_order_id="o", signal_key="s", mint="m",
        track_id="FINAL-A", exit_variant="E3_TP10_T15", price_identity="SOL_NATIVE",
        signal_observed_at=base, signal_ingest_seq=10,
        rule_reference_price_numerator_raw=100, rule_reference_price_denominator_raw=1,
        entry_market_observed_at=base + timedelta(seconds=1), entry_market_ingest_seq=11,
        open_at=base + timedelta(seconds=1), entry_price_numerator_raw=100,
        entry_price_denominator_raw=1,
    )
    engine = PaperExitOrchestratorV01(sqlite3.connect(":memory:"))
    below = ExitMarketObservation("m", base + timedelta(seconds=2), 12, "SOL_NATIVE", 109, 1, False, "below:MARKET")
    crossing = ExitMarketObservation("m", base + timedelta(seconds=3), 13, "SOL_NATIVE", 110, 1, False, "cross:MARKET")
    assert engine.on_market_observation(ref, below).intent is None
    intent = engine.on_market_observation(ref, crossing).intent
    assert intent is not None and intent.trigger_ingest_seq == 13
    assert not is_post_entry_source_row(ref, 11)
    assert is_post_entry_source_row(ref, 12)


def main() -> int:
    result = reproduce(PAPER, SOURCE)
    tests = []
    tests.append(("exact cohort count/digest", result["cohort"]["physical_entry_count"] == EXPECTED_PHYSICAL_ENTRIES and result["cohort"]["computed_accepted_cohort_sha256"] == EXPECTED_COHORT_SHA256 and result["cohort"]["accepted_cohort_sha256_reproduced"] is True))
    tests.append(("source boundary enforcement", result["source"]["anchor"] == SOURCE_ANCHOR and result["source"]["watermark"] == FROZEN_WATERMARK and result["source"]["raw_rows"] == EXPECTED_SOURCE_ROWS))
    tests.append(("normalized source lineage", result["cohort"]["normalized_source_lineage_exact"] == EXPECTED_PHYSICAL_ENTRIES))
    _synthetic_first_crossing(); tests.append(("deterministic first crossing", True))
    model = PaperCostModelV02(P4_COST_BASELINE_0001)
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    tests.append(("500ms exit latency", model.execution_ready_at(now, CostSide.EXIT) == now + timedelta(microseconds=500_000)))
    rejected = model.evaluate_fill(side=CostSide.EXIT, signal_reference_price=RationalPrice("SOL_NATIVE", 100, 1), market_price_at_ready=RationalPrice("SOL_NATIVE", 70, 1), price_impact_bps=1, signal_observed_at=now, market_observed_at=now + timedelta(milliseconds=500))
    tests.append(("slippage rejection", rejected.decision is FillDecision.REJECTED_SLIPPAGE))
    pending = sum(x["pending"] for x in result["summary"].values())
    tests.append(("unresolved/no-fill preservation", pending == 85 and sum(x["no_causal_fill"] for x in result["summary"].values()) == 21))
    tests.append(("frozen entry economics unchanged", result["cohort"]["final_a_equals_final_b"] and result["cohort"]["sens_c_exact_subset"]))
    tests.append(("frozen policy baseline comparison", all(all(result["summary"][t].get(k) == v for k, v in expected.items()) for t, expected in EXPECTED.items())))
    tests.append(("deterministic rerun", result["deterministic_rerun"] == "EXACT"))
    failed = [name for name, ok in tests if not ok]
    for name, ok in tests:
        print(f"TEST: {name}: {'PASS' if ok else 'FAIL'}")
    print(f"TESTS: {len(tests)}")
    print(f"PASSED: {len(tests) - len(failed)}")
    print("RESULT: PASS" if not failed else "RESULT: FAIL")
    return 0 if not failed else 1


if __name__ == "__main__":
    raise SystemExit(main())
