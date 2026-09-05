from __future__ import annotations

import csv
import hashlib
import json
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from phase4.post24h_counterfactual_replay_v0_1 import (  # noqa: E402
    CounterfactualMarketObservationV01,
    TimeoutOrigin,
)
from phase4.post24h_outcome_analysis_v0_1 import (  # noqa: E402
    EXPECTED_COHORT_SHA256,
    EXPECTED_T001_REPLAY_SHA256,
    EXPECTED_T002A_DETERMINISTIC_SHA256,
    MODEL_FINGERPRINT,
    MODEL_ID,
    OUTPUT_FILENAMES,
    PERCENTILE_METHOD,
    actual_baseline_summary_v0_1,
    build_policy_registry_v0_1,
    entry_outcome_studies_v0_1,
    nearest_rank_v0_1,
    pareto_shortlist_v0_1,
    policy_metrics_v0_1,
    post_actual_exit_studies_v0_1,
    quartile_boundaries_v0_1,
    realized_closed_equity_v0_1,
)


OUTPUT = ROOT / "data/research/post24h/POST-24H-ANALYSIS-001"
MANIFEST = OUTPUT / "POST24H_T002B_manifest.json"
SUMMARY = OUTPUT / "analysis_summary.json"
BASE = datetime(2026, 1, 1, tzinfo=timezone.utc)


@dataclass(frozen=True, slots=True)
class Entry:
    route_id: str = "unused"
    signal_key: str = "signal"
    mint: str = "mint"
    signal_observed_at: str = BASE.isoformat(timespec="microseconds")
    signal_ingest_seq: int = 10
    reference_price_identity: str = "SOL_NATIVE"
    reference_price_numerator_raw: int = 100
    reference_price_denominator_raw: int = 1
    entry_observed_at: str = (BASE + timedelta(seconds=1)).isoformat(timespec="microseconds")
    entry_ingest_seq: int = 11
    source_event_key: str = "entry:MARKET"
    principal_lamports: int = 100_000_000
    entry_price_numerator_raw: int = 100
    entry_price_denominator_raw: int = 1
    entry_explicit_cost_lamports: int = 2_355_000


def _obs(seconds: float, seq: int, price: int, *, gap: bool = False, identity: str = "SOL_NATIVE") -> CounterfactualMarketObservationV01:
    return CounterfactualMarketObservationV01(
        mint="mint",
        observed_at=BASE + timedelta(seconds=seconds),
        ingest_seq=seq,
        event_key=f"event-{seq}",
        price_identity=identity,
        price_numerator_raw=price,
        price_denominator_raw=1,
        current_virtual_token_reserve_raw=1_000_000_000,
        is_gap_recovery=gap,
    )


def _sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _manifest() -> dict[str, object]:
    return json.loads(MANIFEST.read_text(encoding="utf-8"))


def _summary_row(policy_id: str, fill_rate: str, score: int) -> dict[str, object]:
    return {
        "policy_id": policy_id,
        "policy_fingerprint": f"fp-{policy_id}",
        "family": "TP_TIMEOUT",
        "fill_rate": fill_rate,
        "unresolved_rate": str(1 - float(fill_rate)),
        "net_pnl_lamports": score,
        "expectancy_lamports_per_filled_trade": str(score),
        "profit_factor": "1.500000000000",
        "win_rate": "0.500000000000",
        "max_realized_closed_equity_drawdown_lamports": 100,
        "median_holding_time_us": 1_000,
    }


def test_registry() -> None:
    policies = build_policy_registry_v0_1()
    counts = Counter(policy.family.value for policy in policies)
    assert counts == {
        "TIMEOUT_ONLY": 7,
        "TP_TIMEOUT": 28,
        "SL_TIMEOUT": 24,
        "TP_SL_TIMEOUT": 168,
        "TRAILING_TIMEOUT": 48,
    }
    assert len(policies) == 275
    assert len({policy.policy_id for policy in policies}) == 275


def test_entry_fill_clock() -> None:
    assert all(
        policy.timeout_origin is TimeoutOrigin.ENTRY_FILL_CLOCK
        for policy in build_policy_registry_v0_1()
    )


def test_horizon_boundary() -> None:
    rows, _, _, _ = entry_outcome_studies_v0_1(
        [Entry()], {"mint": (_obs(4, 12, 104), _obs(7, 13, 120))}
    )
    row = next(item for item in rows if item["horizon_seconds"] == 5)
    assert row["mark_ingest_seq"] == 12
    assert datetime.fromisoformat(row["mark_observed_at"]) <= datetime.fromisoformat(row["target_at"])


def test_gap_wrong_identity() -> None:
    rows, _, _, _ = entry_outcome_studies_v0_1(
        [Entry()],
        {"mint": (_obs(2, 12, 120, gap=True), _obs(3, 13, 130, identity="QUOTE:USD"))},
    )
    row = next(item for item in rows if item["horizon_seconds"] == 5)
    assert row["mark_status"] == "MISSING"


def test_missing_horizon() -> None:
    rows, _, mfe, _ = entry_outcome_studies_v0_1([Entry()], {"mint": ()})
    assert all(row["mark_status"] == "MISSING" for row in rows)
    assert all(row["status"] == "MISSING" and row["mfe_bps"] is None for row in mfe)


def test_mfe_mae_deterministic() -> None:
    args = ([Entry()], {"mint": (_obs(2, 12, 90), _obs(3, 13, 120), _obs(4, 14, 110))})
    first = entry_outcome_studies_v0_1(*args)
    second = entry_outcome_studies_v0_1(*args)
    assert first == second
    row = next(item for item in first[2] if item["horizon_seconds"] == 5)
    assert row["mfe_bps"] == 2_000 and row["mae_bps"] == -1_000


def test_post_exit_raw_baseline() -> None:
    actual = {
        "paper_position_id": "p",
        "signal_key": "signal",
        "mint": "mint",
        "track_id": "FINAL-A",
        "exit_variant": "x",
        "actual_reason": "TAKE_PROFIT",
        "price_identity": "SOL_NATIVE",
        "fill_observed_at": BASE + timedelta(seconds=1),
        "fill_ingest_seq": 11,
        "fill_source_event_key": "fill:MARKET",
        "raw_fill_price_numerator_raw": 100,
        "raw_fill_price_denominator_raw": 1,
        "impacted_exit_price_numerator_raw": 80,
        "impacted_exit_price_denominator_raw": 1,
    }
    outcomes, _ = post_actual_exit_studies_v0_1(
        [actual], {"mint": (_obs(3, 12, 110),)}
    )
    row = next(item for item in outcomes if item["horizon_seconds"] == 5)
    assert row["post_exit_raw_market_return_bps"] == 1_000
    assert row["baseline_price_kind"] == "RAW_SOURCE_MARKET_AT_ACTUAL_FILL_ROW"


def test_sold_too_early_timing() -> None:
    actual = {
        "paper_position_id": "p",
        "signal_key": "signal",
        "mint": "mint",
        "track_id": "FINAL-A",
        "exit_variant": "x",
        "actual_reason": "FALLBACK",
        "price_identity": "SOL_NATIVE",
        "fill_observed_at": BASE + timedelta(seconds=1),
        "fill_ingest_seq": 11,
        "fill_source_event_key": "fill:MARKET",
        "raw_fill_price_numerator_raw": 100,
        "raw_fill_price_denominator_raw": 1,
        "impacted_exit_price_numerator_raw": 99,
        "impacted_exit_price_denominator_raw": 1,
    }
    _, sold = post_actual_exit_studies_v0_1(
        [actual], {"mint": (_obs(3, 12, 105), _obs(4, 13, 110), _obs(5, 14, 120))}
    )
    row = next(
        item
        for item in sold
        if item["window_seconds"] == 5 and item["threshold_bps"] == 1_000
    )
    assert row["reached_n"] == 1 and row["median_time_to_threshold_ms"] == "3000.000"


def test_unresolved_not_zero_pnl() -> None:
    rows = [
        {
            "classification": "FILLED",
            "reason": "FALLBACK",
            "net_pnl_lamports": 10,
            "gross_execution_pnl_lamports": 20,
            "entry_explicit_cost_lamports": 5,
            "exit_explicit_cost_lamports": 5,
            "holding_time_us": 100,
            "fill_observed_at": "2026-01-01T00:00:01.000000+00:00",
            "fill_source_row": 1,
            "signal_key": "a",
        },
        {
            "classification": "NO_CAUSAL_FILL",
            "reason": "FALLBACK",
            "net_pnl_lamports": None,
            "gross_execution_pnl_lamports": None,
            "entry_explicit_cost_lamports": 5,
            "exit_explicit_cost_lamports": None,
            "holding_time_us": None,
            "fill_observed_at": None,
            "fill_source_row": None,
            "signal_key": "b",
        },
    ]
    metrics = policy_metrics_v0_1(rows, policy_id="p", policy_fingerprint="f", family="x")
    assert metrics["positions"] == 2 and metrics["fills"] == 1
    assert metrics["net_pnl_lamports"] == 10
    assert metrics["expectancy_lamports_per_filled_trade"] == "10.000000"


def test_drawdown_ordering() -> None:
    rows = [
        {"classification": "FILLED", "fill_observed_at": "2026-01-01T00:00:03+00:00", "fill_source_row": 3, "signal_key": "c", "net_pnl_lamports": -50},
        {"classification": "FILLED", "fill_observed_at": "2026-01-01T00:00:02+00:00", "fill_source_row": 2, "signal_key": "b", "net_pnl_lamports": 100},
    ]
    result = realized_closed_equity_v0_1(rows)
    assert result == {
        "final_realized_closed_pnl_lamports": 50,
        "peak_realized_closed_pnl_lamports": 100,
        "max_realized_closed_equity_drawdown_lamports": 50,
    }


def test_percentile() -> None:
    assert PERCENTILE_METHOD == "NEAREST_RANK_CEILING_1_INDEXED"
    assert nearest_rank_v0_1([4, 1, 3, 2], 50) == 2
    assert nearest_rank_v0_1([4, 1, 3, 2], 90) == 4


def test_shortlist_bound_and_gate() -> None:
    values = [_summary_row(f"P{i:02d}", "0.900000000000", i) for i in range(20)]
    values.append(_summary_row("INELIGIBLE", "0.899999999999", 1_000_000))
    shortlist = pareto_shortlist_v0_1(values)
    assert len(shortlist) <= 10
    assert "INELIGIBLE" not in {row["policy_id"] for row in shortlist}


def test_pareto_tie_break() -> None:
    values = [_summary_row("B", "0.900000000000", 1), _summary_row("A", "0.900000000000", 1)]
    first = pareto_shortlist_v0_1(values)
    second = pareto_shortlist_v0_1(tuple(reversed(values)))
    assert [row["policy_id"] for row in first] == ["A", "B"]
    assert first == second


def test_quartiles() -> None:
    assert quartile_boundaries_v0_1([8, 7, 6, 5, 4, 3, 2, 1]) == {
        "p25": 2,
        "p50": 4,
        "p75": 6,
    }


def test_generated_artifacts() -> None:
    manifest = _manifest()
    assert manifest["model_id"] == MODEL_ID
    assert manifest["model_fingerprint"] == MODEL_FINGERPRINT
    assert all((OUTPUT / name).is_file() for name in OUTPUT_FILENAMES)
    for name, expected in manifest["output_sha256"].items():
        assert _sha(OUTPUT / name) == expected


def test_exact_replay_rows() -> None:
    manifest = _manifest()
    assert manifest["output_row_counts"]["replay_trade_results.csv"] == 174_625
    with (OUTPUT / "replay_trade_results.csv").open("r", encoding="utf-8", newline="") as handle:
        assert sum(1 for _ in handle) - 1 == 174_625


def test_segments_small_cell() -> None:
    with (OUTPUT / "segment_summary.csv").open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert rows
    assert all((row["small_cell"] == "True") == (int(row["n"]) < 50) for row in rows)


def test_frozen_gates() -> None:
    manifest = _manifest()
    assert manifest["cohort"]["sha256"] == EXPECTED_COHORT_SHA256
    assert manifest["frozen_digests"]["t001_replay"] == EXPECTED_T001_REPLAY_SHA256
    assert manifest["frozen_digests"]["t002a_deterministic"] == EXPECTED_T002A_DETERMINISTIC_SHA256


def test_deterministic_full_analysis() -> None:
    manifest = _manifest()
    summary = json.loads(SUMMARY.read_text(encoding="utf-8"))
    assert manifest["deterministic_full_analysis_rerun"] == "EXACT"
    assert summary["deterministic_full_analysis_rerun"] == "EXACT"
    assert manifest["deterministic_analysis_digest"] == summary["deterministic_analysis_digest"]


TESTS: tuple[tuple[str, Callable[[], None]], ...] = (
    ("exact policy registry counts 7/28/24/168/48=275", test_registry),
    ("all research policies use ENTRY_FILL_CLOCK", test_entry_fill_clock),
    ("horizon mark never looks after target", test_horizon_boundary),
    ("gap and wrong identity cannot become marks", test_gap_wrong_identity),
    ("no-observation horizon remains MISSING", test_missing_horizon),
    ("deterministic MFE/MAE", test_mfe_mae_deterministic),
    ("post-exit baseline uses raw source fill price", test_post_exit_raw_baseline),
    ("sold-too-early first threshold timing deterministic", test_sold_too_early_timing),
    ("unresolved is not zero-PnL", test_unresolved_not_zero_pnl),
    ("realized drawdown ordering deterministic", test_drawdown_ordering),
    ("percentile method deterministic", test_percentile),
    ("shortlist <=10 and fill-rate gate", test_shortlist_bound_and_gate),
    ("Pareto dominance and tie-breaking deterministic", test_pareto_tie_break),
    ("segmentation quartiles deterministic", test_quartiles),
    ("generated artifact hashes", test_generated_artifacts),
    ("exact replay row count 174625", test_exact_replay_rows),
    ("N<50 marked small_cell", test_segments_small_cell),
    ("T001/T002A frozen gates retained", test_frozen_gates),
    ("deterministic full analysis rerun digest", test_deterministic_full_analysis),
)


def main() -> int:
    failed: list[str] = []
    for name, test in TESTS:
        try:
            test()
            passed = True
            detail = ""
        except Exception as exc:
            passed = False
            detail = f" ({type(exc).__name__}: {exc})"
            failed.append(name)
        print(f"TEST: {name}: {'PASS' if passed else 'FAIL'}{detail}")
    print(f"TESTS: {len(TESTS)}")
    print(f"PASSED: {len(TESTS) - len(failed)}")
    print("RESULT: PASS" if not failed else "RESULT: FAIL")
    return 0 if not failed else 1


if __name__ == "__main__":
    raise SystemExit(main())
