from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.phase2.large_cohort_research_v0_1 import (
    build_large_cohort_payload,
    run_detailed_strategy_probe,
    stable_sha256,
)
from src.phase2.models_v0_1 import FirstPullbackParameterSet
from src.phase2.phase1_readonly_adapter_v0_1 import (
    BOT_TRUTH_SOURCE_PREFIXES,
    Phase1ReadOnlyAdapterV01,
)
from src.phase2.research_replay_v0_3 import (
    fetch_research_events,
    group_events_by_mint,
)

DEFAULT_PROD_DB = ROOT / "data" / "db" / "tradingbot.sqlite3"
DEFAULT_OUT_DIR = ROOT / "data" / "research" / "phase2_large_cohort_v0_1"
DEFAULT_TOKEN_LIMIT = 1000


def check(name: str, condition: bool, detail: str = "") -> None:
    if not condition:
        raise AssertionError(f"{name}: FAIL{': ' + detail if detail else ''}")
    print(f"{name:<68} PASS")


def probe_parameter_sets() -> tuple[FirstPullbackParameterSet, ...]:
    # RESEARCH PROBES ONLY. These are the same characterization probes used in the
    # validated v0.3 100-token harness; they are not locked strategy rules.
    return (
        FirstPullbackParameterSet(
            parameter_set_id="FP1-RESEARCH-PROBE-LOOSE-0001",
            strategy_version="v1.0",
            max_entry_age_ms=300_000,
            impulse_parameters={
                "min_return_bps": 500,
                "min_trades_since_t0": 2,
                "min_unique_buyers_since_t0": 2,
            },
            pullback_parameters={"min_depth_bps": 1000, "max_depth_bps": 7000},
            buyer_response_parameters={
                "window_id": "3s",
                "min_rebound_bps": 200,
                "min_buys": 1,
                "min_net_flow_lamports": 1,
            },
            reclaim_parameters={"min_extension_from_response_bps": 200},
            runaway_entry_parameters={"max_extension_from_response_bps": 5000},
        ),
        FirstPullbackParameterSet(
            parameter_set_id="FP1-RESEARCH-PROBE-MID-0001",
            strategy_version="v1.0",
            max_entry_age_ms=300_000,
            impulse_parameters={
                "min_return_bps": 1500,
                "min_trades_since_t0": 3,
                "min_unique_buyers_since_t0": 3,
            },
            pullback_parameters={"min_depth_bps": 2500, "max_depth_bps": 6000},
            buyer_response_parameters={
                "window_id": "3s",
                "min_rebound_bps": 500,
                "min_buys": 2,
                "min_net_flow_lamports": 100_000_000,
            },
            reclaim_parameters={"min_extension_from_response_bps": 500},
            runaway_entry_parameters={"max_extension_from_response_bps": 2500},
        ),
        FirstPullbackParameterSet(
            parameter_set_id="FP1-RESEARCH-PROBE-STRICT-0001",
            strategy_version="v1.0",
            max_entry_age_ms=300_000,
            impulse_parameters={
                "min_return_bps": 3000,
                "min_trades_since_t0": 4,
                "min_unique_buyers_since_t0": 4,
            },
            pullback_parameters={"min_depth_bps": 3000, "max_depth_bps": 5500},
            buyer_response_parameters={
                "window_id": "3s",
                "min_rebound_bps": 750,
                "min_buys": 3,
                "min_net_flow_lamports": 500_000_000,
            },
            reclaim_parameters={"min_extension_from_response_bps": 750},
            runaway_entry_parameters={"max_extension_from_response_bps": 2000},
        ),
    )


def _eligible_mint_count(conn: sqlite3.Connection) -> int:
    where_source = "(" + " OR ".join(
        "source_decoded_file LIKE ?" for _ in BOT_TRUTH_SOURCE_PREFIXES
    ) + ")"
    args = tuple(prefix + "%" for prefix in BOT_TRUTH_SOURCE_PREFIXES)
    row = conn.execute(
        f"""
        SELECT COUNT(*)
        FROM (
            SELECT mint
            FROM pump_events
            WHERE mint IS NOT NULL
              AND event_type IN ('BUY', 'SELL')
              AND pump_timestamp IS NOT NULL
              AND decoded_at_utc IS NOT NULL
              AND slot IS NOT NULL
              AND {where_source}
            GROUP BY mint
        )
        """,
        args,
    ).fetchone()
    return int(row[0])


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fields: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row:
            if key not in seen:
                fields.append(key)
                seen.add(key)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def pct(n: int, d: int) -> str:
    return "n/a" if d == 0 else f"{100.0 * n / d:.2f}%"


def qline(summary: dict[str, object]) -> str:
    return (
        f"n={summary.get('n')} p25={summary.get('p25')} p50={summary.get('p50')} "
        f"p75={summary.get('p75')} p90={summary.get('p90')} p95={summary.get('p95')}"
    )


def main(db_path: Path, out_dir: Path, token_limit: int) -> None:
    print("PHASE 2 LARGE-COHORT MARKET CHARACTERIZATION v0.1")
    print("Purpose                    : 1000-TOKEN SETUP INCIDENCE + STAGE DISTRIBUTIONS")
    print("PnL/backtest                : NOT INCLUDED")
    print("Future-return labels        : NOT INCLUDED")
    print("Performance conclusions     : FORBIDDEN FROM THIS HARNESS")
    print("Production DB mode          : READ ONLY")
    print("Network/RPC                 : DISABLED")
    print("Wallet/order/execution      : DISABLED")
    print("Risk Manager                : NOT USED")
    print("Deterministic strategy clock: ENABLED (SCLOCK-0.1)")
    print("Future-activity cohort filter: NONE (>=1 observed tradable event only)")
    print(f"Requested cohort tokens     : {token_limit if token_limit else 'ALL'}")
    print()

    if token_limit < 0:
        raise ValueError("--tokens must be >= 0")

    before = db_path.stat()
    conn = Phase1ReadOnlyAdapterV01.open_readonly(db_path)
    try:
        check("Test 1a - production SQLite opened query_only", int(conn.execute("PRAGMA query_only").fetchone()[0]) == 1)
        eligible_total = _eligible_mint_count(conn)
        check("Test 1b - eligible BOT_TRUTH token universe exists", eligible_total > 0)
        read_result = fetch_research_events(conn, token_limit=token_limit)
        expected = eligible_total if token_limit == 0 else min(token_limit, eligible_total)
        check("Test 1c - cohort size equals requested-or-available", len(read_result.cohort.mints) == expected, f"got={len(read_result.cohort.mints)} expected={expected}")
        check("Test 1d - no future-activity selection rule", "no minimum future activity/outcome filter" in read_result.cohort.selection_rule)
        check("Test 1e - real Phase-1 rows normalize", read_result.source_rows > 0 and len(read_result.events) > 0)
    finally:
        conn.close()

    after_read = db_path.stat()
    check("Test 2a - production DB size unchanged after read", before.st_size == after_read.st_size)
    check("Test 2b - production DB mtime unchanged after read", before.st_mtime_ns == after_read.st_mtime_ns)

    probes = probe_parameter_sets()
    payload, raw_rows, probe_rows = build_large_cohort_payload(
        read_result=read_result,
        probes=probes,
        segment_count=5,
    )

    check("Test 3a - one raw row per cohort token", len(raw_rows) == len(read_result.cohort.mints))
    check("Test 3b - one detailed row per token x probe", len(probe_rows) == len(read_result.cohort.mints) * len(probes))
    check("Test 3c - PnL explicitly absent", payload["performance_pnl_included"] is False)
    check("Test 3d - future-return labels explicitly absent", payload["future_return_label_included"] is False)
    check("Test 3e - execution/Risk Manager explicitly absent", payload["execution_model_used"] is False and payload["risk_manager_used"] is False)
    check("Test 3f - five time segments cover cohort", sum(int(s["tokens"]) for s in payload["cohort"]["time_segments"]) == len(read_result.cohort.mints))

    for probe in payload["probes"]:
        f = probe["funnel"]
        check(
            f"Test 4 - {probe['parameter_set']['parameter_set_id']} funnel monotonic",
            f["candidate_signal"] <= f["buyer_response_confirmed"] <= f["pullback_active"] <= f["impulse_confirmed"] <= f["eligible_tokens"],
        )
        check(
            f"Test 5 - {probe['parameter_set']['parameter_set_id']} terminal complete",
            int(f["open_at_data_end"]) == 0,
        )
        check(
            f"Test 6 - {probe['parameter_set']['parameter_set_id']} segment totals valid",
            sum(int(s["tokens"]) for s in probe["time_segments"]) == len(read_result.cohort.mints),
        )

    # Full detailed MID replay must be deterministic from the immutable real event list.
    grouped = group_events_by_mint(read_result.events)
    mid_a = run_detailed_strategy_probe(grouped, read_result.cohort.mints, probes[1])
    mid_b = run_detailed_strategy_probe(grouped, read_result.cohort.mints, probes[1])
    check("Test 7a - repeated detailed MID replay deterministic", stable_sha256(mid_a) == stable_sha256(mid_b))

    out_dir.mkdir(parents=True, exist_ok=True)
    report_path = out_dir / "phase2_large_cohort_report_v0_1.json"
    raw_csv_path = out_dir / "phase2_large_cohort_raw_token_metrics_v0_1.csv"
    probe_csv_path = out_dir / "phase2_large_cohort_probe_stage_metrics_v0_1.csv"
    manifest_path = out_dir / "phase2_large_cohort_manifest_v0_1.json"

    report_path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")
    _write_csv(raw_csv_path, raw_rows)
    _write_csv(probe_csv_path, probe_rows)
    manifest = {
        "schema_version": "P2LCR-MANIFEST-0.1",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "production_db": str(db_path),
        "production_db_read_only": True,
        "eligible_token_universe": eligible_total,
        "requested_tokens": token_limit,
        "cohort_tokens": len(read_result.cohort.mints),
        "report_json": str(report_path),
        "raw_token_metrics_csv": str(raw_csv_path),
        "probe_stage_metrics_csv": str(probe_csv_path),
        "deterministic_payload_sha256": payload["deterministic_payload_sha256"],
        "warning": "Setup characterization only. No PnL, exits, future returns, execution, fees, slippage, or performance conclusions.",
    }
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    check("Test 8a - report JSON written outside production DB", report_path.exists())
    check("Test 8b - raw token CSV written outside production DB", raw_csv_path.exists())
    check("Test 8c - probe-stage CSV written outside production DB", probe_csv_path.exists())

    final_stat = db_path.stat()
    check("Test 9a - production DB unchanged through research run", before.st_size == final_stat.st_size and before.st_mtime_ns == final_stat.st_mtime_ns)

    raw = payload["raw_market_summary"]
    metrics = raw["metrics"]
    print()
    print("=" * 96)
    print("LARGE-COHORT RAW MARKET CHARACTERIZATION — 5M BOT_TRUTH WINDOW — NO PnL")
    print("=" * 96)
    print(f"Eligible BOT_TRUTH token universe      : {eligible_total}")
    print(f"Cohort tokens                          : {payload['cohort']['tokens']}")
    print(f"Source rows                            : {payload['source_rows']}")
    print(f"Normalized events                      : {payload['normalized_events']}")
    print(f"Tokens with gap events                 : {raw['tokens_with_gap_events']}")
    print(f"No observable state inside 5m          : {raw['tokens_with_no_observable_state_inside_entry_window']}")
    print(f"Cohort t0 range (observed us)          : {payload['cohort']['start_t0_observed_at_us']} -> {payload['cohort']['end_t0_observed_at_us']}")
    print(f"Max return from t0 (bps)               : {qline(metrics['max_return_from_t0_bps'])}")
    print(f"Max drawdown depth (bps)               : {qline(metrics['max_drawdown_depth_bps'])}")
    print(f"Max trades since t0                    : {qline(metrics['max_trades_since_t0'])}")
    print(f"Max unique buyers since t0             : {qline(metrics['max_unique_buyers_since_t0'])}")
    print(f"Max 3s buys                            : {qline(metrics['max_3s_buys'])}")
    print(f"Max 3s net flow (lamports)             : {qline(metrics['max_3s_net_flow_lamports'])}")
    print(f"Max 3s price change (bps)              : {qline(metrics['max_3s_price_change_bps'])}")

    print()
    print("=" * 96)
    print("FIRST PULLBACK LARGE-COHORT FUNNELS + CAUSAL STAGE DISTRIBUTIONS — RESEARCH ONLY")
    print("=" * 96)
    for probe in payload["probes"]:
        p = probe["parameter_set"]
        f = probe["funnel"]
        m = probe["metrics"]
        n = int(f["eligible_tokens"])
        print()
        print(p["parameter_set_id"])
        print(f"  Impulse confirmed                   : {f['impulse_confirmed']:>5}/{n:<5} ({pct(f['impulse_confirmed'], n)})")
        print(f"  Pullback active                     : {f['pullback_active']:>5}/{n:<5} ({pct(f['pullback_active'], n)})")
        print(f"  Buyer response confirmed            : {f['buyer_response_confirmed']:>5}/{n:<5} ({pct(f['buyer_response_confirmed'], n)})")
        print(f"  Candidate signal                    : {f['candidate_signal']:>5}/{n:<5} ({pct(f['candidate_signal'], n)})")
        print(f"  Rejected / Invalidated / Expired    : {f['rejected']} / {f['invalidated']} / {f['expired']}")
        print(f"  Open at data end                    : {f['open_at_data_end']}")
        print(f"  t0 -> impulse (ms)                  : {qline(m['impulse_age_ms'])}")
        print(f"  Impulse return at confirm (bps)     : {qline(m['impulse_return_bps'])}")
        print(f"  Impulse peak return (bps)           : {qline(m['impulse_peak_return_bps'])}")
        print(f"  Impulse -> pullback (ms)            : {qline(m['impulse_to_pullback_ms'])}")
        print(f"  Pullback depth at activation (bps)  : {qline(m['pullback_depth_at_activation_bps'])}")
        print(f"  Pullback low depth at response (bps): {qline(m['pullback_low_depth_at_response_bps'])}")
        print(f"  Pullback -> response (ms)           : {qline(m['pullback_to_response_ms'])}")
        print(f"  Response rebound (bps)              : {qline(m['response_rebound_bps'])}")
        print(f"  Response 3s buys                    : {qline(m['response_window_buys'])}")
        print(f"  Response 3s net flow (lamports)     : {qline(m['response_window_net_flow_lamports'])}")
        print(f"  Response -> candidate (ms)          : {qline(m['response_to_candidate_ms'])}")
        print(f"  Reclaim extension (bps)             : {qline(m['reclaim_extension_bps'])}")
        print("  Chronological segment candidate rates:")
        for seg in probe["time_segments"]:
            print(
                f"    S{seg['segment_id']} n={seg['tokens']:<4} "
                f"imp={seg['impulse_confirmed']:<4} pb={seg['pullback_active']:<4} "
                f"resp={seg['buyer_response_confirmed']:<4} cand={seg['candidate_signal']:<4} "
                f"({pct(seg['candidate_signal'], seg['tokens'])})"
            )

    print()
    print("IMPORTANT")
    print("  * These are setup-incidence and causal stage distributions, NOT performance results.")
    print("  * No future-return label, exit, fee, slippage, latency/PnL model, or execution is used.")
    print("  * LOOSE/MID/STRICT remain research probes only; their thresholds are NOT locked trading rules.")
    print("  * Time segments are descriptive regime checks inside the selected cohort, not optimization folds.")
    print()
    print(f"Research report JSON                    : {report_path}")
    print(f"Raw token metrics CSV                   : {raw_csv_path}")
    print(f"Probe stage metrics CSV                 : {probe_csv_path}")
    print(f"Deterministic payload SHA256            : {payload['deterministic_payload_sha256']}")
    print("Production DB touched by harness        : NO")
    print("Network/RPC used                        : NO")
    print("Wallet/order/execution used             : NO")
    print("Risk Manager used                       : NO")
    print("PnL/future-return performance result    : NO")
    print("RESULT                                  : PASS")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Phase-2 large-cohort setup characterization harness"
    )
    parser.add_argument("--db", type=Path, default=DEFAULT_PROD_DB)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument(
        "--tokens",
        type=int,
        default=DEFAULT_TOKEN_LIMIT,
        help="Most recently-started eligible BOT_TRUTH tokens; 0 means all eligible tokens",
    )
    args = parser.parse_args()

    try:
        main(args.db, args.out_dir, args.tokens)
    except Exception as exc:
        print(f"RESULT                                  : FAIL")
        print(f"ERROR                                   : {type(exc).__name__}: {exc}")
        raise
