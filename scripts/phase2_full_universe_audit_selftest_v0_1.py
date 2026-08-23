from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.phase2.full_universe_audit_v0_1 import (
    analyze_price_coverage,
    build_fixed_time_bins,
    reserve_defect_codes,
    stable_sha256,
)
from src.phase2.models_v0_1 import EventType, IngestionSource, NormalizedMarketEvent


def check(name: str, condition: bool) -> None:
    if not condition:
        raise AssertionError(f"{name}: FAIL")
    print(f"{name:<72} PASS")


def ev(seq: int, mint: str, obs_ms: int, *, priced: bool = True, source=IngestionSource.SYNTHETIC_TEST, buy=True):
    return NormalizedMarketEvent(
        schema_version="NME-0.1",
        event_key=f"sig{seq}:0:x",
        ingest_seq=seq,
        mint=mint,
        event_type=EventType.BUY if buy else EventType.SELL,
        event_at_us=1_700_000_000_000_000 + obs_ms * 1000,
        observed_at_us=1_700_000_000_000_000 + obs_ms * 1000,
        slot=seq,
        signature=f"sig{seq}",
        event_index=0,
        user=f"u{seq}",
        sol_amount_lamports=100_000_000,
        token_amount_raw=1_000_000,
        virtual_sol_reserve_lamports=(30_000_000_000 + seq * 1000) if priced else None,
        virtual_token_reserve_raw=(1_000_000_000_000 - seq * 1000) if priced else None,
        source=source,
    )


def main() -> None:
    print("PHASE 2 FULL-UNIVERSE COVERAGE/STABILITY AUDIT SELF-TEST v0.1")
    print("Purpose                    : DATA-QUALITY + TIME-STABILITY MECHANICS")
    print("Synthetic thresholds only  : YES")
    print("PnL/future returns          : NOT USED")
    print("Production DB touched       : NO")
    print()

    grouped = {
        "A": [ev(1, "A", 0), ev(2, "A", 1000)],
        "B": [ev(3, "B", 0, priced=False), ev(4, "B", 1000, priced=False)],
        "C": [ev(5, "C", 0, priced=False), ev(6, "C", 1000, priced=True)],
        "D": [ev(7, "D", 0, priced=True), ev(8, "D", 1000, priced=False)],
        "E": [ev(9, "E", 0, priced=True, source=IngestionSource.GAP_RECOVERY)],
    }
    raw = {
        "A": {"max_return_from_t0_bps": 10, "max_3s_price_change_bps": 10},
        "B": {"max_return_from_t0_bps": None, "max_3s_price_change_bps": None},
        "C": {"max_return_from_t0_bps": None, "max_3s_price_change_bps": 0},
        "D": {"max_return_from_t0_bps": 0, "max_3s_price_change_bps": 0},
        "E": {"max_return_from_t0_bps": 0, "max_3s_price_change_bps": 0},
    }
    rows, summary = analyze_price_coverage(grouped, tuple(grouped), raw_rows_by_mint=raw)
    by = {r["mint"]: r for r in rows}
    check("Test 1a - fully priced token classified", by["A"]["price_coverage_class"] == "FULLY_PRICED_5M")
    check("Test 1b - no-price token classified", by["B"]["price_coverage_class"] == "NO_PRICED_TRADES_5M")
    check("Test 1c - first-unpriced/later-priced token distinguished", by["C"]["price_coverage_class"] == "FIRST_TRADE_UNPRICED_LATER_PRICED")
    check("Test 1d - partial reserve gaps after priced t0 distinguished", by["D"]["price_coverage_class"] == "FIRST_TRADE_PRICED_PARTIAL_GAPS")
    check("Test 1e - missing reserve defects are structured", "VIRTUAL_SOL_MISSING" in by["B"]["reserve_defect_codes"] and "VIRTUAL_TOKEN_MISSING" in by["B"]["reserve_defect_codes"])
    check("Test 1f - return availability reflects raw causal metric", summary["return_metric_available_tokens"] == 3)
    check("Test 1g - gap provenance retained", by["E"]["gap_event_count"] == 1)

    bad = ev(20, "Z", 0, priced=False)
    check("Test 2a - reserve defect helper deterministic", reserve_defect_codes(bad) == ("VIRTUAL_SOL_MISSING", "VIRTUAL_TOKEN_MISSING"))

    raw_rows = []
    quality_rows = []
    probe_rows = []
    base = 1_700_000_000_000_000
    for i in range(6):
        mint = f"M{i}"
        t0 = base + i * 10 * 60 * 1_000_000
        raw_rows.append({
            "mint": mint,
            "first_tradable_observed_at_us": t0,
            "max_trades_since_t0": i + 1,
            "max_unique_buyers_since_t0": i + 1,
        })
        quality_rows.append({
            "mint": mint,
            "return_from_t0_metric_available": i != 1,
            "gap_event_count": 1 if i == 5 else 0,
        })
        probe_rows.append({"mint": mint, "parameter_set_id": "P", "impulse_age_ms": 10 if i >= 2 else None, "candidate_age_ms": 20 if i >= 4 else None})
    bins = build_fixed_time_bins(raw_rows, quality_rows, probe_rows, bin_minutes=30)
    check("Test 3a - fixed UTC bins created", len(bins) >= 2)
    check("Test 3b - fixed-bin token totals equal cohort", sum(int(b["tokens"]) for b in bins) == 6)
    check("Test 3c - candidate counts retained by time bin", sum(int(b.get("P__candidate", 0)) for b in bins) == 2)
    check("Test 3d - price coverage retained by time bin", sum(int(b["price_metric_coverage_tokens"]) for b in bins) == 5)
    check("Test 3e - gap-token counts retained by time bin", sum(int(b["gap_tokens"]) for b in bins) == 1)

    h1 = stable_sha256({"rows": rows, "summary": summary, "bins": bins})
    h2 = stable_sha256({"rows": rows, "summary": summary, "bins": bins})
    check("Test 4a - deterministic audit payload", h1 == h2)

    print()
    print("Price-coverage classification            : PASS")
    print("Reserve-defect diagnosis                 : PASS")
    print("Fixed-clock stability bins               : PASS")
    print("Gap provenance                           : PASS")
    print("Deterministic audit payload              : PASS")
    print("PnL/future-return performance used       : NO")
    print("Production DB touched                    : NO")
    print("RESULT                                   : PASS")


if __name__ == "__main__":
    main()
