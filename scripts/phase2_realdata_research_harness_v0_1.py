from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.phase2.models_v0_1 import FirstPullbackParameterSet
from src.phase2.phase1_readonly_adapter_v0_1 import Phase1ReadOnlyAdapterV01
from src.phase2.research_replay_v0_1 import (
    build_raw_token_metrics,
    build_research_payload,
    fetch_research_events,
    group_events_by_mint,
    run_strategy_probe,
    stable_sha256,
)


DEFAULT_PROD_DB = ROOT / "data" / "db" / "tradingbot.sqlite3"
DEFAULT_OUT_DIR = ROOT / "data" / "research" / "phase2_research_v0_1"


def probe_parameter_sets() -> tuple[FirstPullbackParameterSet, ...]:
    # RESEARCH PROBES ONLY.  These deliberately span looser -> stricter mechanics so
    # we can inspect the setup funnel.  They are not candidate trading rules and no
    # profitability conclusion may be drawn from their candidate counts.
    return (
        FirstPullbackParameterSet(
            parameter_set_id="FP1-RESEARCH-PROBE-LOOSE-0001",
            strategy_version="v1.0",
            max_entry_age_ms=300_000,
            impulse_parameters={
                "min_return_bps": 1500,
                "min_trades_since_t0": 2,
                "min_unique_buyers_since_t0": 2,
            },
            pullback_parameters={"min_depth_bps": 2500, "max_depth_bps": 6500},
            buyer_response_parameters={
                "window_id": "3s",
                "min_rebound_bps": 200,
                "min_buys": 1,
                "min_net_flow_lamports": 0,
            },
            reclaim_parameters={"min_extension_from_response_bps": 200},
            runaway_entry_parameters={"max_extension_from_response_bps": 3000},
        ),
        FirstPullbackParameterSet(
            parameter_set_id="FP1-RESEARCH-PROBE-MID-0001",
            strategy_version="v1.0",
            max_entry_age_ms=300_000,
            impulse_parameters={
                "min_return_bps": 3000,
                "min_trades_since_t0": 3,
                "min_unique_buyers_since_t0": 3,
            },
            pullback_parameters={"min_depth_bps": 3000, "max_depth_bps": 6000},
            buyer_response_parameters={
                "window_id": "3s",
                "min_rebound_bps": 500,
                "min_buys": 2,
                "min_net_flow_lamports": 0,
            },
            reclaim_parameters={"min_extension_from_response_bps": 500},
            runaway_entry_parameters={"max_extension_from_response_bps": 2500},
        ),
        FirstPullbackParameterSet(
            parameter_set_id="FP1-RESEARCH-PROBE-STRICT-0001",
            strategy_version="v1.0",
            max_entry_age_ms=300_000,
            impulse_parameters={
                "min_return_bps": 5000,
                "min_trades_since_t0": 5,
                "min_unique_buyers_since_t0": 5,
            },
            pullback_parameters={"min_depth_bps": 4000, "max_depth_bps": 5500},
            buyer_response_parameters={
                "window_id": "3s",
                "min_rebound_bps": 800,
                "min_buys": 3,
                "min_net_flow_lamports": 0,
            },
            reclaim_parameters={"min_extension_from_response_bps": 800},
            runaway_entry_parameters={"max_extension_from_response_bps": 2000},
        ),
    )


def check(name: str, condition: bool, detail: str = "") -> None:
    if not condition:
        raise AssertionError(f"{name}: FAIL{': ' + detail if detail else ''}")
    print(f"{name:<62} PASS")


def write_token_csv(path: Path, token_rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not token_rows:
        path.write_text("", encoding="utf-8")
        return
    fields = list(token_rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(token_rows)


def pct(n: int, d: int) -> str:
    return "n/a" if d == 0 else f"{(100.0 * n / d):.1f}%"


def print_metric(label: str, summary: dict[str, object]) -> None:
    print(
        f"{label:<34} "
        f"p25={summary.get('p25')}  p50={summary.get('p50')}  "
        f"p75={summary.get('p75')}  p90={summary.get('p90')}  max={summary.get('max')}"
    )


def main(db_path: Path, out_dir: Path, token_limit: int) -> None:
    print("PHASE 2 REAL-DATA RESEARCH / REPLAY HARNESS v0.1")
    print("Purpose                  : SETUP FUNNEL + RAW DISTRIBUTIONS")
    print("PnL/backtest              : NOT INCLUDED")
    print("Performance conclusions   : FORBIDDEN FROM THIS HARNESS")
    print("Production DB mode        : READ ONLY")
    print("Network/RPC               : DISABLED")
    print("Wallet/order/execution    : DISABLED")
    print("Risk Manager              : NOT USED")
    print("Cohort future-activity filter: NONE (>=1 tradable event only)")
    print()

    before = db_path.stat()
    conn = Phase1ReadOnlyAdapterV01.open_readonly(db_path)
    try:
        check("Test 1a - source DB query_only", int(conn.execute("PRAGMA query_only").fetchone()[0]) == 1)
        read_result = fetch_research_events(conn, token_limit=token_limit)
        check("Test 1b - chronological eligible cohort exists", len(read_result.cohort.mints) > 0)
        check("Test 1c - real Phase-1 rows selected", read_result.source_rows > 0)
        check("Test 1d - rows normalize to NME-0.1", len(read_result.events) > 0)
        check(
            "Test 1e - deterministic global observed order",
            all(
                (read_result.events[i - 1].observed_at_us, read_result.events[i - 1].ingest_seq)
                < (read_result.events[i].observed_at_us, read_result.events[i].ingest_seq)
                for i in range(1, len(read_result.events))
            ),
        )
    finally:
        conn.close()

    after_read = db_path.stat()
    check("Test 2a - production DB size unchanged", before.st_size == after_read.st_size)
    check("Test 2b - production DB mtime unchanged", before.st_mtime_ns == after_read.st_mtime_ns)

    grouped = group_events_by_mint(read_result.events)
    token_rows = build_raw_token_metrics(grouped, read_result.cohort.mints)
    check("Test 3a - one raw metric row per cohort token", len(token_rows) == len(read_result.cohort.mints))

    probes = probe_parameter_sets()
    payload = build_research_payload(
        read_result=read_result,
        token_rows=token_rows,
        probes=probes,
    )
    check("Test 3b - three explicit research probes evaluated", len(payload["probes"]) == 3)
    check("Test 3c - PnL explicitly absent", payload["performance_pnl_included"] is False)
    check("Test 3d - execution model explicitly absent", payload["execution_model_used"] is False)

    # Determinism check: re-run one full probe from the same immutable event list.
    probe_a = payload["probes"][1]
    probe_b = run_strategy_probe(grouped, read_result.cohort.mints, probes[1])
    check("Test 4a - repeated MID probe deterministic", stable_sha256(probe_a) == stable_sha256(probe_b))

    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "phase2_research_replay_v0_1.json"
    csv_path = out_dir / "phase2_research_token_metrics_v0_1.csv"
    manifest_path = out_dir / "phase2_research_manifest_v0_1.json"

    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")
    write_token_csv(csv_path, token_rows)
    manifest = {
        "schema_version": "P2RR-MANIFEST-0.1",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "production_db": str(db_path),
        "production_db_read_only": True,
        "report_json": str(json_path),
        "token_metrics_csv": str(csv_path),
        "deterministic_payload_sha256": payload["deterministic_payload_sha256"],
        "warning": "No PnL, exits, fees, slippage, execution, or performance conclusions in this harness.",
    }
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    check("Test 5a - research JSON written outside production DB", json_path.exists())
    check("Test 5b - token-metrics CSV written outside production DB", csv_path.exists())

    final = db_path.stat()
    check(
        "Test 6a - production DB unchanged through research run",
        before.st_size == final.st_size and before.st_mtime_ns == final.st_mtime_ns,
    )

    raw = payload["raw_market_summary"]
    metrics = raw["metrics"]
    print()
    print("=" * 86)
    print("REAL-DATA RAW MARKET CHARACTERIZATION (5-MIN ENTRY WINDOW; NO PnL)")
    print("=" * 86)
    print(f"Cohort tokens                         : {payload['cohort']['tokens']}")
    print(f"Source rows                           : {payload['source_rows']}")
    print(f"Normalized events                     : {payload['normalized_events']}")
    print(f"Tokens with gap events                : {raw['tokens_with_gap_events']}")
    print(f"Tokens with post-window observation   : {raw['tokens_with_post_window_observation']}")
    print(f"No observable state inside 5m window  : {raw['tokens_with_no_observable_state_inside_entry_window']}")
    print(f"Quantiles                             : {raw['quantile_method']}")
    print_metric("Max return from t0 (bps)", metrics["max_return_from_t0_bps"])
    print_metric("Max drawdown depth (bps)", metrics["max_drawdown_depth_bps"])
    print_metric("Max trades since t0", metrics["max_trades_since_t0"])
    print_metric("Max unique buyers since t0", metrics["max_unique_buyers_since_t0"])
    print_metric("Max 3s buys", metrics["max_3s_buys"])
    print_metric("Max 3s net flow (lamports)", metrics["max_3s_net_flow_lamports"])
    print_metric("Max 3s price change (bps)", metrics["max_3s_price_change_bps"])
    print_metric("First trade obs delay (ms)", metrics["first_trade_observation_delay_ms"])

    print()
    print("=" * 86)
    print("FIRST PULLBACK STATE-MACHINE FUNNELS — RESEARCH PROBES ONLY")
    print("=" * 86)
    open_any = 0
    for probe in payload["probes"]:
        p = probe["parameter_set"]
        f = probe["funnel"]
        n = f["eligible_tokens"]
        print()
        print(p["parameter_set_id"])
        print(
            "  Impulse confirmed       : "
            f"{f['impulse_confirmed']:>5} / {n:<5} ({pct(f['impulse_confirmed'], n)})"
        )
        print(
            "  Pullback active         : "
            f"{f['pullback_active']:>5} / {n:<5} ({pct(f['pullback_active'], n)})"
        )
        print(
            "  Buyer response confirmed: "
            f"{f['buyer_response_confirmed']:>4} / {n:<5} ({pct(f['buyer_response_confirmed'], n)})"
        )
        print(
            "  Candidate signal        : "
            f"{f['candidate_signal']:>5} / {n:<5} ({pct(f['candidate_signal'], n)})"
        )
        print(f"  Invalidated             : {f['invalidated']}")
        print(f"  Expired                 : {f['expired']}")
        print(f"  Rejected (ran away)     : {f['rejected']}")
        print(f"  Open at data end        : {f['open_at_data_end']}")
        open_any += int(f["open_at_data_end"])

    print()
    print("IMPORTANT")
    print("  * Probe candidate counts are NOT winrate, expectancy, edge, or trade results.")
    print("  * No exits, fees, slippage, latency model, PnL, or future returns are evaluated.")
    print("  * Probe thresholds are characterization values only, not locked strategy rules.")
    if open_any:
        print(
            "  * OPEN_AT_DATA_END exists. This is a diagnostic that event-only evaluation can "
            "leave a lifecycle non-terminal when no later market event arrives to trigger 5m expiry."
        )

    print()
    print(f"Research report JSON                   : {json_path}")
    print(f"Token metrics CSV                      : {csv_path}")
    print(f"Deterministic payload SHA256           : {payload['deterministic_payload_sha256']}")
    print("Production DB touched by harness       : NO")
    print("Network/RPC used                       : NO")
    print("Wallet/order/execution used            : NO")
    print("Risk Manager used                      : NO")
    print("PnL/performance result                 : NO")
    print("RESULT                                 : PASS")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Phase-2 real-data setup-funnel/raw-distribution research harness"
    )
    parser.add_argument("--db", type=Path, default=DEFAULT_PROD_DB)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument(
        "--tokens",
        type=int,
        default=100,
        help="Chronological BOT_TRUTH cohort size; 0 means all eligible tokens",
    )
    args = parser.parse_args()

    try:
        main(args.db, args.out_dir, args.tokens)
    except Exception as exc:
        print()
        print("RESULT                                 : FAIL")
        print(f"ERROR                                  : {type(exc).__name__}: {exc}")
        raise
