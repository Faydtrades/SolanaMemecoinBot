from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.phase2.phase1_readonly_adapter_v0_1 import Phase1ReadOnlyAdapterV01
from src.phase2.quote_aware_price_v0_1 import (
    Phase1QuoteAwareAdapterV01,
    build_full_universe_validation,
    stable_sha256,
)

DEFAULT_DB = PROJECT_ROOT / "data" / "db" / "tradingbot.sqlite3"
DEFAULT_OUT = PROJECT_ROOT / "data" / "research" / "phase2_quote_aware_price_v0_1"


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fields: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                fields.append(key)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def pct(n: int, d: int) -> str:
    return "n/a" if d == 0 else f"{100*n/d:.2f}%"


def main(db_path: Path, out_dir: Path) -> None:
    print("PHASE 2 QUOTE-AWARE PRICE PATH VALIDATION v0.1")
    print("Purpose                    : SOL/QUOTE PRICE IDENTITY + RELATIVE PRICE COVERAGE")
    print("PnL/backtest                : NOT INCLUDED")
    print("Future-return labels        : NOT INCLUDED")
    print("Strategy parameter search   : NOT PERFORMED")
    print("Strategy execution          : NOT RUN")
    print("Production DB mode          : READ ONLY")
    print("Network/RPC                 : DISABLED")
    print("Wallet/order/execution      : DISABLED")
    print()

    before = db_path.stat()
    conn = Phase1ReadOnlyAdapterV01.open_readonly(db_path)
    failures: list[str] = []

    def check(label: str, ok: bool) -> None:
        print(f"{label:<78} {'PASS' if ok else 'FAIL'}")
        if not ok:
            failures.append(label)

    try:
        Phase1QuoteAwareAdapterV01.validate_schema(conn)
        rows = Phase1QuoteAwareAdapterV01.fetch_all_bot_truth_rows(conn)
        events, skipped = Phase1QuoteAwareAdapterV01.normalize_rows(rows)
        check("Test 1a - production SQLite opened query_only with quote columns", len(rows) > 0)
        check("Test 1b - BOT_TRUTH rows normalize with quote fields preserved", len(events) > 0 and not skipped)

        token_rows, quote_event_rows, summary = build_full_universe_validation(events)
        check("Test 2a - full eligible token universe receives price-path classification", summary["tokens_analyzed"] > 0)
        check("Test 2b - existing SOL price proxy is bit-for-bit ratio compatible", summary["sol_price_proxy_parity_failures"] == 0)
        check("Test 2c - quote-path events are explicitly identified", summary["quote_priceable_events"] > 0)
        check("Test 2d - quote mint identity is retained", bool(summary["quote_mint_token_counts"]))
        check("Test 2e - path switches / mixed identities are explicitly counted", "path_conflict_tokens" in summary)
        check("Test 2f - quote flow stays separate from lamport flow semantics", summary["strategy_v01_quote_flow_compatible"] is False)
        check("Test 2g - no human-readable absolute quote price is invented", summary["absolute_human_price_units_resolved"] is False)

        # The previously SOL-unpriceable cohort should now be testable through a
        # quote path if the root-cause audit was correct.  Do not hardcode 109 so the
        # harness remains valid if the production DB grows before rerun.
        check("Test 3a - SOL-unpriceable cohort exists for quote-path validation", summary["old_sol_unpriceable_tokens"] > 0)
        check("Test 3b - at least one SOL-unpriceable token is rescued by quote path", summary["rescued_by_quote_path_tokens"] > 0)
        check("Test 3c - quote-relative bps semantics enabled without strategy flow claim", summary["relative_price_bps_supported_for_quote_path"] is True)

        # Determinism: repeat pure classification over identical normalized input.
        token_rows_2, quote_event_rows_2, summary_2 = build_full_universe_validation(events)
        check(
            "Test 4a - identical real-data quote-aware validation is deterministic",
            stable_sha256((token_rows, quote_event_rows, summary))
            == stable_sha256((token_rows_2, quote_event_rows_2, summary_2)),
        )

        out_dir.mkdir(parents=True, exist_ok=True)
        report = {
            "schema_version": "QAP-0.1",
            "purpose": "QUOTE-AWARE PRICE PATH VALIDATION; NO PNL",
            "source_rows": len(rows),
            "normalized_events": len(events),
            "adapter_skipped": skipped,
            "summary": summary,
            "performance_pnl_included": False,
            "future_return_labels_included": False,
            "strategy_parameter_optimization_performed": False,
            "strategy_v01_run_on_quote_tokens": False,
        }
        report["deterministic_payload_sha256"] = stable_sha256(report)
        report_path = out_dir / "phase2_quote_aware_price_report_v0_1.json"
        token_path = out_dir / "phase2_quote_aware_token_paths_v0_1.csv"
        event_path = out_dir / "phase2_quote_path_events_v0_1.csv"
        report_path.write_text(json.dumps(report, indent=2, sort_keys=True, default=str), encoding="utf-8")
        write_csv(token_path, token_rows)
        write_csv(event_path, quote_event_rows)
        check("Test 5a - report and diagnostic CSVs written outside production DB", report_path.exists() and token_path.exists() and event_path.exists())
    finally:
        conn.close()

    after = db_path.stat()
    check("Test 6a - production DB size unchanged", before.st_size == after.st_size)
    check("Test 6b - production DB mtime unchanged", before.st_mtime_ns == after.st_mtime_ns)

    print()
    print("=" * 108)
    print("QUOTE-AWARE PRICE COVERAGE — DESCRIPTIVE ONLY")
    print("=" * 108)
    total = int(summary["tokens_analyzed"])
    old_unpriced = int(summary["old_sol_unpriceable_tokens"])
    rescued = int(summary["rescued_by_quote_path_tokens"])
    enhanced = int(summary["enhanced_price_coverage_tokens"])
    print(f"Tokens analyzed                         : {total}")
    print(f"Token price-path classes                : {summary['token_class_counts']}")
    print(f"Previously SOL-unpriceable tokens       : {old_unpriced} ({pct(old_unpriced,total)})")
    print(f"Rescued by quote path                   : {rescued} ({pct(rescued,old_unpriced)})")
    print(f"Enhanced comparable price coverage      : {enhanced}/{total} ({pct(enhanced,total)})")
    print(f"Remaining unavailable/mixed tokens      : {summary['remaining_unavailable_or_mixed_tokens']}")
    print(f"Quote mint token counts                 : {summary['quote_mint_token_counts']}")
    print(f"Quote mint event counts                 : {summary['quote_mint_event_counts']}")
    print(f"Quote-priceable trade events            : {summary['quote_priceable_events']}")
    print(f"Quote amount >0 events                  : {summary['quote_amount_positive_events']}")
    print(f"Quote amount missing/zero events        : {summary['quote_amount_missing_or_zero_events']}")
    print(f"Quote-path SOL amount >0 events         : {summary['quote_path_sol_amount_positive_events']}")
    print(f"Quote-path SOL amount zero events       : {summary['quote_path_sol_amount_zero_events']}")
    print(f"Price-path conflict tokens              : {summary['path_conflict_tokens']}")
    print(f"Quote-flow mint conflict tokens         : {summary['quote_flow_mint_conflict_tokens']}")
    print(f"SOL proxy parity failures               : {summary['sol_price_proxy_parity_failures']}")
    print()
    print("IMPORTANT")
    print(" * Quote-aware relative return/drawdown can be measured within one stable price identity.")
    print(" * Raw reserve ratios are NOT presented as a human-readable absolute price.")
    print(" * Quote volume remains quote_amount_raw and is NOT converted to lamports/SOL.")
    print(" * FirstPullbackStrategyV01 is NOT run on quote-path tokens in this validation because")
    print("   its buyer-response min_net_flow parameter is currently explicitly lamport-denominated.")
    print(" * No PnL, exits, future returns, execution, or parameter optimization are used.")
    print()
    print(f"Research report JSON                    : {report_path}")
    print(f"Token path CSV                          : {token_path}")
    print(f"Quote event CSV                         : {event_path}")
    print("Production DB touched by harness        : NO")
    print("Network/RPC used                        : NO")
    print("Wallet/order/execution used             : NO")
    print("PnL/future-return performance result    : NO")
    print("Strategy parameter optimization         : NO")
    print(f"RESULT                                  : {'PASS' if not failures else 'FAIL'}")
    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    args = parser.parse_args()
    main(args.db, args.out_dir)
