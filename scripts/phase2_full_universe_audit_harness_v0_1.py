from __future__ import annotations

import csv
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.phase2.full_universe_audit_v0_1 import (
    build_full_universe_payload,
    research_probe_parameter_sets,
    stable_sha256,
)
from src.phase2.large_cohort_research_v0_1 import run_detailed_strategy_probe
from src.phase2.phase1_readonly_adapter_v0_1 import BOT_TRUTH_SOURCE_PREFIXES, Phase1ReadOnlyAdapterV01
from src.phase2.research_replay_v0_3 import fetch_research_events, group_events_by_mint

DEFAULT_PROD_DB = ROOT / "data" / "db" / "tradingbot.sqlite3"
DEFAULT_OUT_DIR = ROOT / "data" / "research" / "phase2_full_universe_audit_v0_1"


def check(name: str, condition: bool, detail: str = "") -> None:
    if not condition:
        raise AssertionError(f"{name}: FAIL{': ' + detail if detail else ''}")
    print(f"{name:<74} PASS")


def _eligible_mint_count(conn: sqlite3.Connection) -> int:
    where_source = "(" + " OR ".join("source_decoded_file LIKE ?" for _ in BOT_TRUTH_SOURCE_PREFIXES) + ")"
    args = tuple(prefix + "%" for prefix in BOT_TRUTH_SOURCE_PREFIXES)
    row = conn.execute(
        f"""
        SELECT COUNT(*) FROM (
            SELECT mint FROM pump_events
            WHERE mint IS NOT NULL
              AND event_type IN ('BUY','SELL')
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
    return "n/a" if d == 0 else f"{100.0*n/d:.2f}%"


def qline(s: dict[str, object]) -> str:
    return f"n={s.get('n')} p25={s.get('p25')} p50={s.get('p50')} p75={s.get('p75')} p90={s.get('p90')} p95={s.get('p95')}"


def main(db_path: Path, out_dir: Path) -> None:
    print("PHASE 2 FULL-UNIVERSE COVERAGE + DATA-QUALITY / STABILITY AUDIT v0.1")
    print("Purpose                      : ALL ELIGIBLE BOT_TRUTH TOKENS")
    print("PnL/backtest                  : NOT INCLUDED")
    print("Future-return labels          : NOT INCLUDED")
    print("Parameter optimization        : NOT PERFORMED")
    print("Performance conclusions       : FORBIDDEN FROM THIS HARNESS")
    print("Production DB mode            : READ ONLY")
    print("Network/RPC                   : DISABLED")
    print("Wallet/order/execution        : DISABLED")
    print("Risk Manager                  : NOT USED")
    print("Deterministic strategy clock  : ENABLED (SCLOCK-0.1)")
    print("Cohort future-activity filter : NONE (>=1 observed tradable event only)")
    print("Requested cohort              : ALL")
    print()

    before = db_path.stat()
    conn = Phase1ReadOnlyAdapterV01.open_readonly(db_path)
    try:
        check("Test 1a - production SQLite opened query_only", int(conn.execute("PRAGMA query_only").fetchone()[0]) == 1)
        eligible_total = _eligible_mint_count(conn)
        check("Test 1b - eligible BOT_TRUTH universe exists", eligible_total > 0)
        read_result = fetch_research_events(conn, token_limit=0)
        check("Test 1c - ALL eligible tokens selected", len(read_result.cohort.mints) == eligible_total, f"got={len(read_result.cohort.mints)} expected={eligible_total}")
        check("Test 1d - no future-activity selection rule", "no minimum future activity/outcome filter" in read_result.cohort.selection_rule)
        check("Test 1e - real Phase-1 rows normalize", read_result.source_rows > 0 and len(read_result.events) > 0)
    finally:
        conn.close()

    after_read = db_path.stat()
    check("Test 2a - production DB size unchanged after read", before.st_size == after_read.st_size)
    check("Test 2b - production DB mtime unchanged after read", before.st_mtime_ns == after_read.st_mtime_ns)

    probes = research_probe_parameter_sets()
    payload, raw_rows, probe_rows, quality_rows, time_bins = build_full_universe_payload(read_result=read_result, probes=probes)
    base = payload["base_characterization"]
    quality = payload["price_data_quality"]

    check("Test 3a - one raw metric row per eligible token", len(raw_rows) == eligible_total)
    check("Test 3b - one quality row per eligible token", len(quality_rows) == eligible_total)
    check("Test 3c - one probe row per token x probe", len(probe_rows) == eligible_total * len(probes))
    check("Test 3d - fixed time bins cover all t0 tokens", sum(int(b["tokens"]) for b in time_bins) == eligible_total)
    check("Test 3e - price-quality classes cover full universe", sum(int(v) for v in quality["class_counts"].values()) == eligible_total)
    check("Test 3f - PnL/future labels/optimization explicitly absent", payload["performance_pnl_included"] is False and payload["future_return_label_included"] is False and payload["parameter_optimization_performed"] is False)

    for probe in base["probes"]:
        f = probe["funnel"]
        check(f"Test 4 - {probe['parameter_set']['parameter_set_id']} funnel monotonic", f["candidate_signal"] <= f["buyer_response_confirmed"] <= f["pullback_active"] <= f["impulse_confirmed"] <= f["eligible_tokens"])
        check(f"Test 5 - {probe['parameter_set']['parameter_set_id']} terminal complete", int(f["open_at_data_end"]) == 0)

    # Determinism safety: repeat the MID strategy mechanics across the full immutable list.
    grouped = group_events_by_mint(read_result.events)
    mid_a = run_detailed_strategy_probe(grouped, read_result.cohort.mints, probes[1])
    mid_b = run_detailed_strategy_probe(grouped, read_result.cohort.mints, probes[1])
    check("Test 6a - full-universe MID detailed replay deterministic", stable_sha256(mid_a) == stable_sha256(mid_b))

    out_dir.mkdir(parents=True, exist_ok=True)
    report_path = out_dir / "phase2_full_universe_audit_report_v0_1.json"
    raw_path = out_dir / "phase2_full_universe_raw_token_metrics_v0_1.csv"
    probe_path = out_dir / "phase2_full_universe_probe_stage_metrics_v0_1.csv"
    quality_path = out_dir / "phase2_full_universe_price_quality_v0_1.csv"
    bins_path = out_dir / "phase2_full_universe_fixed_time_bins_v0_1.csv"
    manifest_path = out_dir / "phase2_full_universe_audit_manifest_v0_1.json"

    report_path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")
    _write_csv(raw_path, raw_rows)
    _write_csv(probe_path, probe_rows)
    _write_csv(quality_path, quality_rows)
    _write_csv(bins_path, time_bins)
    manifest = {
        "schema_version": "P2FUA-MANIFEST-0.1",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "production_db": str(db_path),
        "production_db_read_only": True,
        "eligible_tokens": eligible_total,
        "source_rows": read_result.source_rows,
        "normalized_events": len(read_result.events),
        "report_json": str(report_path),
        "raw_metrics_csv": str(raw_path),
        "probe_stage_csv": str(probe_path),
        "price_quality_csv": str(quality_path),
        "fixed_time_bins_csv": str(bins_path),
        "deterministic_payload_sha256": payload["deterministic_payload_sha256"],
        "warning": "Coverage/stability/setup incidence only. No PnL, exits, future-return labels, execution, or parameter optimization.",
    }
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    check("Test 7a - report and four CSV research outputs written", all(p.exists() for p in (report_path, raw_path, probe_path, quality_path, bins_path)))

    final = db_path.stat()
    check("Test 8a - production DB unchanged through full audit", before.st_size == final.st_size and before.st_mtime_ns == final.st_mtime_ns)

    cohort = base["cohort"]
    raw = base["raw_market_summary"]
    metrics = raw["metrics"]
    print()
    print("=" * 104)
    print("FULL ELIGIBLE BOT_TRUTH UNIVERSE — COVERAGE / DATA QUALITY / SETUP STABILITY — NO PnL")
    print("=" * 104)
    print(f"Eligible / analyzed tokens             : {eligible_total}")
    print(f"Source rows                            : {read_result.source_rows}")
    print(f"Normalized events                      : {len(read_result.events)}")
    print(f"Adapter skipped                        : {read_result.skipped}")
    print(f"t0 observed range (us)                 : {cohort['start_t0_observed_at_us']} -> {cohort['end_t0_observed_at_us']}")
    print(f"Equal-count chronological segments     : {cohort['time_segment_count']}")
    print(f"Fixed stability bins                   : {len(time_bins)} x {payload['fixed_time_bin_minutes']} min")
    print(f"Tokens with gap events                 : {raw['tokens_with_gap_events']}")
    print(f"No observable state inside 5m          : {raw['tokens_with_no_observable_state_inside_entry_window']}")
    print(f"Max return from t0 (bps)               : {qline(metrics['max_return_from_t0_bps'])}")
    print(f"Max drawdown depth (bps)               : {qline(metrics['max_drawdown_depth_bps'])}")
    print(f"Max trades since t0                    : {qline(metrics['max_trades_since_t0'])}")
    print(f"Max unique buyers since t0             : {qline(metrics['max_unique_buyers_since_t0'])}")

    print()
    print("PRICE / RESERVE DATA QUALITY")
    print(f"First trade priceable                  : {quality['first_trade_priceable_tokens']}/{eligible_total} ({pct(quality['first_trade_priceable_tokens'], eligible_total)})")
    print(f"Any priceable trade inside 5m          : {quality['any_priceable_trade_5m_tokens']}/{eligible_total} ({pct(quality['any_priceable_trade_5m_tokens'], eligible_total)})")
    print(f"Return-from-t0 metric available        : {quality['return_metric_available_tokens']}/{eligible_total} ({pct(quality['return_metric_available_tokens'], eligible_total)})")
    print(f"Short-window price metric available    : {quality['short_price_metric_available_tokens']}/{eligible_total} ({pct(quality['short_price_metric_available_tokens'], eligible_total)})")
    print("Coverage classes:")
    for k, v in quality["class_counts"].items():
        print(f"  {k:<40} {v:>6} ({pct(int(v), eligible_total)})")
    print(f"Reserve defect event counts            : {quality['reserve_defect_event_counts']}")
    print(f"Reserve defect token counts            : {quality['reserve_defect_token_counts']}")

    print()
    print("FIRST PULLBACK FULL-UNIVERSE FUNNELS — RESEARCH PROBES ONLY")
    for probe in base["probes"]:
        f = probe["funnel"]
        n = int(f["eligible_tokens"])
        pid = probe["parameter_set"]["parameter_set_id"]
        print(f"{pid}")
        print(f"  impulse={f['impulse_confirmed']}/{n} ({pct(f['impulse_confirmed'], n)})  pullback={f['pullback_active']}/{n} ({pct(f['pullback_active'], n)})  response={f['buyer_response_confirmed']}/{n} ({pct(f['buyer_response_confirmed'], n)})  candidate={f['candidate_signal']}/{n} ({pct(f['candidate_signal'], n)})")
        print(f"  rejected / invalidated / expired = {f['rejected']} / {f['invalidated']} / {f['expired']}   open={f['open_at_data_end']}")
        print("  10 equal-count chronological candidate rates:")
        for seg in probe["time_segments"]:
            print(f"    S{seg['segment_id']:02d} n={seg['tokens']:<4} cand={seg['candidate_signal']:<4} ({pct(seg['candidate_signal'], seg['tokens'])})")

    print()
    print("FIXED 30-MIN BOT_TRUTH STABILITY BINS")
    for b in time_bins:
        print(f"  {b['bin_start_utc']}  n={b['tokens']:<4} price_cov={b['price_metric_coverage_pct']:.1f}% gaps={b['gap_tokens']} trades_p50={b['trades_p50']} trades_p90={b['trades_p90']}")
        for params in probes:
            pid = params.parameter_set_id
            print(f"      {pid.split('-')[-2]:<6} impulse={b.get(pid+'__impulse',0):<4} candidate={b.get(pid+'__candidate',0):<4} rate={b.get(pid+'__candidate_rate_pct',0):.2f}%")

    print()
    print("IMPORTANT")
    print(" * This is descriptive coverage/setup incidence, NOT strategy performance.")
    print(" * No future returns, exits, PnL, fees, slippage, execution model, or optimization are used.")
    print(" * Probe thresholds remain characterization values only; they are NOT locked trading rules.")
    print(" * Fixed time bins are regime/stability diagnostics, not optimization folds.")
    print()
    print(f"Research report JSON                    : {report_path}")
    print(f"Price-quality CSV                       : {quality_path}")
    print(f"Fixed-time-bin CSV                      : {bins_path}")
    print(f"Deterministic payload SHA256            : {payload['deterministic_payload_sha256']}")
    print("Production DB touched by harness        : NO")
    print("Network/RPC used                        : NO")
    print("Wallet/order/execution used             : NO")
    print("Risk Manager used                       : NO")
    print("PnL/future-return performance result    : NO")
    print("Parameter optimization performed        : NO")
    print("RESULT                                  : PASS")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, default=DEFAULT_PROD_DB)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    args = parser.parse_args()
    main(args.db, args.out_dir)
