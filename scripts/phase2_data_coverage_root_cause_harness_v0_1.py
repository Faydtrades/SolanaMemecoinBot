from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.phase2.data_coverage_root_cause_v0_1 import (
    build_coverage_bins,
    build_root_cause_payload,
    diagnose_unpriceable_tokens,
    nearest_observation_before_us,
    read_control_events,
    reconstruct_collector_coverage,
)
from src.phase2.phase1_readonly_adapter_v0_1 import Phase1ReadOnlyAdapterV01
from src.phase2.research_replay_v0_3 import fetch_research_events, group_events_by_mint


DEFAULT_DB = PROJECT_ROOT / "data" / "db" / "tradingbot.sqlite3"
DEFAULT_OUT = PROJECT_ROOT / "data" / "research" / "phase2_data_coverage_root_cause_v0_1"


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fields = []
    seen = set()
    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                fields.append(key)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def pct(n: int, d: int) -> str:
    return "n/a" if d == 0 else f"{100*n/d:.2f}%"


def main(db_path: Path, out_dir: Path) -> None:
    print("PHASE 2 DATA COVERAGE / UNPRICEABLE TOKEN ROOT-CAUSE AUDIT v0.1")
    print("Purpose                    : RESERVE SEMANTICS + COLLECTOR UPTIME COVERAGE")
    print("PnL/backtest                : NOT INCLUDED")
    print("Future-return labels        : NOT INCLUDED")
    print("Parameter optimization      : NOT PERFORMED")
    print("Production DB mode          : READ ONLY")
    print("Network/RPC                 : DISABLED")
    print("Wallet/order/execution      : DISABLED")
    print()

    before = db_path.stat()
    conn = Phase1ReadOnlyAdapterV01.open_readonly(db_path)
    failures: list[str] = []

    def check(label: str, ok: bool) -> None:
        status = "PASS" if ok else "FAIL"
        print(f"{label:<76} {status}")
        if not ok:
            failures.append(label)

    try:
        Phase1ReadOnlyAdapterV01.validate_schema(conn)
        read_result = fetch_research_events(conn, token_limit=0)
        grouped = group_events_by_mint(read_result.events)
        check("Test 1a - full eligible BOT_TRUTH universe read query_only", len(read_result.cohort.mints) > 0)
        check("Test 1b - no future-activity selection rule", "no minimum future activity/outcome filter" in read_result.cohort.selection_rule)

        token_rows, event_rows, unpriceable = diagnose_unpriceable_tokens(
            conn, grouped, read_result.cohort.mints
        )
        check("Test 2a - unpriceable-token root-cause rows diagnosed", len(token_rows) > 0)
        check("Test 2b - defect events preserve raw quote/reserve fields", len(event_rows) >= len(token_rows))
        check("Test 2c - diagnosis is descriptive; no fallback price silently applied", all("diagnosis" in r for r in event_rows))

        control_events = read_control_events(conn)

        def nearest(cutoff: int, after: int | None):
            return nearest_observation_before_us(conn, cutoff, after_us=after)

        intervals, sessions, gaps, coverage_summary = reconstruct_collector_coverage(
            control_events, nearest_before=nearest
        )
        t0_values = []
        for mint in read_result.cohort.mints:
            tradable = [
                event for event in grouped.get(mint, [])
                if getattr(event.event_type, "value", event.event_type) in ("BUY", "SELL")
            ]
            if tradable:
                t0_values.append(tradable[0].observed_at_us)
        coverage_bins = build_coverage_bins(intervals, gaps, t0_values, bin_minutes=30)
        check("Test 3a - collector control events available", len(control_events) > 0)
        check("Test 3b - active subscription intervals reconstructed", len(intervals) > 0)
        check("Test 3c - fixed 30m bins cover all eligible token t0 values", sum(int(b["tokens"]) for b in coverage_bins) == len(read_result.cohort.mints))
        check("Test 3d - active coverage exposed independently from token counts", all("active_coverage_pct" in b and "tokens_per_active_minute" in b for b in coverage_bins))

        payload = build_root_cause_payload(
            unpriceable_summary=unpriceable,
            coverage_summary=coverage_summary,
            coverage_bins=coverage_bins,
            session_rows=sessions,
        )
        payload["eligible_tokens"] = len(read_result.cohort.mints)
        payload["source_rows"] = read_result.source_rows
        payload["normalized_events"] = len(read_result.events)
        payload["unpriceable_token_rate_pct"] = (
            100.0 * len(token_rows) / len(read_result.cohort.mints)
            if read_result.cohort.mints else None
        )
        payload["explicit_gap_rows"] = gaps
        check("Test 4a - payload explicitly excludes PnL", payload["performance_pnl_included"] is False)
        check("Test 4b - payload explicitly excludes optimization", payload["parameter_optimization_performed"] is False)

        out_dir.mkdir(parents=True, exist_ok=True)
        report_path = out_dir / "phase2_data_coverage_root_cause_report_v0_1.json"
        token_path = out_dir / "phase2_unpriceable_tokens_v0_1.csv"
        event_path = out_dir / "phase2_unpriceable_events_v0_1.csv"
        sessions_path = out_dir / "phase2_collector_sessions_v0_1.csv"
        bins_path = out_dir / "phase2_collector_coverage_30m_v0_1.csv"
        report_path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")
        write_csv(token_path, token_rows)
        write_csv(event_path, event_rows)
        write_csv(sessions_path, sessions)
        write_csv(bins_path, coverage_bins)
        check("Test 5a - report + four diagnostic CSVs written outside production DB", all(p.exists() for p in (report_path, token_path, event_path, sessions_path, bins_path)))
    finally:
        conn.close()

    after = db_path.stat()
    check("Test 6a - production DB size unchanged", before.st_size == after.st_size)
    check("Test 6b - production DB mtime unchanged", before.st_mtime_ns == after.st_mtime_ns)

    print()
    print("=" * 108)
    print("UNPRICEABLE TOKEN ROOT-CAUSE — DESCRIPTIVE ONLY")
    print("=" * 108)
    total = len(read_result.cohort.mints)
    print(f"Eligible BOT_TRUTH tokens               : {total}")
    print(f"Unpriceable tokens                      : {len(token_rows)} ({pct(len(token_rows), total)})")
    print(f"Unpriceable 5m trade events             : {len(event_rows)}")
    print(f"Token root classes                      : {unpriceable['token_root_class_counts']}")
    print(f"Event reserve-path diagnoses            : {unpriceable['event_diagnosis_counts']}")
    print(f"All-trades quote-fallback candidates    : {unpriceable['all_quote_fallback_candidate_tokens']}")
    print("Top quote mints by affected tokens:")
    quote_counts = sorted(
        unpriceable["quote_mint_token_counts"].items(),
        key=lambda kv: (-int(kv[1]), kv[0]),
    )
    if quote_counts:
        for mint, n in quote_counts[:10]:
            print(f"  {mint:<48} tokens={n:<5} events={unpriceable['quote_mint_event_counts'].get(mint,0)}")
    else:
        print("  (none)")

    print()
    print("=" * 108)
    print("COLLECTOR UPTIME / WALL-CLOCK COVERAGE")
    print("=" * 108)
    print(f"Collector control events                : {coverage_summary['control_events']}")
    print(f"Collector sessions                      : {coverage_summary['sessions']}")
    print(f"Active subscription intervals           : {coverage_summary['active_intervals']}")
    print(f"Session versions                        : {coverage_summary['session_versions']}")
    print(f"Explicit reconnect gaps                 : {coverage_summary['explicit_gaps']}")
    print(f"Explicit reconnect gap seconds          : {coverage_summary['explicit_gap_seconds_total']:.3f}")
    print(f"Uncertain interval closes               : {coverage_summary['uncertain_interval_closes']}")
    print()
    print("30-minute bins: token count is shown beside independently reconstructed collector uptime.")
    for b in coverage_bins:
        tpm = b["tokens_per_active_minute"]
        tpm_text = "n/a" if tpm is None else f"{float(tpm):.2f}"
        print(
            f"  {b['bin_start_utc']}  tokens={b['tokens']:<5} "
            f"active={float(b['active_minutes']):>6.2f}m "
            f"coverage={float(b['active_coverage_pct']):>6.2f}% "
            f"class={b['coverage_class']:<7} "
            f"tokens/active-min={tpm_text:<7} "
            f"explicit_gap={float(b['explicit_gap_seconds']):.2f}s"
        )

    print()
    print("IMPORTANT")
    print(" * QUOTE_RESERVE_FALLBACK_CANDIDATE is a diagnosis, NOT an implemented pricing rule.")
    print(" * This audit does not silently reprice the affected tokens.")
    print(" * Collector uptime is reconstructed from COLLECTOR_V0_3_START / PUMP_SUBSCRIPTION_ACTIVE /")
    print("   WS_DISCONNECTED / LIVE_COLLECTION_STOPPED, with last websocket observation used for abrupt ends.")
    print(" * No PnL, future-return labels, execution, or parameter optimization are used.")
    print()
    print(f"Diagnostic report JSON                  : {report_path}")
    print(f"Unpriceable token CSV                   : {token_path}")
    print(f"Unpriceable event CSV                   : {event_path}")
    print(f"Collector sessions CSV                  : {sessions_path}")
    print(f"30m coverage CSV                        : {bins_path}")
    print("Production DB touched by harness        : NO")
    print("Network/RPC used                        : NO")
    print("Wallet/order/execution used             : NO")
    print("PnL/future-return performance result    : NO")
    print("Parameter optimization performed        : NO")
    print(f"RESULT                                  : {'PASS' if not failures else 'FAIL'}")

    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    args = parser.parse_args()
    main(args.db, args.out_dir)
