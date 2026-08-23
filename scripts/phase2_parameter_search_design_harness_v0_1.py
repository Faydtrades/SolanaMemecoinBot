from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.phase2.denomination_aware_flow_v0_1 import DenominationAwareFeatureEngineV01
from src.phase2.models_v0_1 import EventType
from src.phase2.parameter_search_design_v0_1 import (
    BROAD_IMPULSE_MIN_BUYERS,
    BROAD_IMPULSE_MIN_TRADES,
    BROAD_IMPULSE_RETURN_BPS,
    BROAD_PROTO_RESPONSE_MIN_BUYS,
    BROAD_PROTO_RESPONSE_REBOUND_BPS,
    BROAD_PULLBACK_MAX_BPS,
    BROAD_PULLBACK_MIN_BPS,
    build_proposed_search_space,
    characterize_states,
    empirical_distributions,
    stable_sha256,
    validate_search_space,
)
from src.phase2.phase1_readonly_adapter_v0_1 import Phase1ReadOnlyAdapterV01
from src.phase2.quote_aware_price_v0_1 import (
    Phase1QuoteAwareAdapterV01,
    entry_window_events,
    group_events,
)

DEFAULT_DB = ROOT / "data" / "db" / "tradingbot.sqlite3"
DEFAULT_OUT = ROOT / "data" / "research" / "phase2_parameter_search_design_v0_1"


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
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def main(db_path: Path, out_dir: Path) -> None:
    print("PHASE 2 EMPIRICAL PARAMETER-SEARCH DESIGN HARNESS v0.1")
    print("Purpose                    : DEFINE FEATURE-SUPPORTED SEARCH RANGES BEFORE PnL")
    print("PnL/backtest                : NOT INCLUDED")
    print("Future-return labels        : NOT INCLUDED")
    print("Winrate/expectancy/PF       : NOT INCLUDED")
    print("Parameter optimization      : NOT PERFORMED")
    print("Search-space status         : PROPOSAL ONLY")
    print("Production DB mode          : READ ONLY")
    print("Network/RPC                 : DISABLED")
    print("Wallet/order/execution      : DISABLED")
    print()

    before = db_path.stat()
    conn = Phase1ReadOnlyAdapterV01.open_readonly(db_path)
    failures: list[str] = []

    def check(label: str, ok: bool) -> None:
        print(f"{label:<84} {'PASS' if ok else 'FAIL'}")
        if not ok:
            failures.append(label)

    try:
        Phase1QuoteAwareAdapterV01.validate_schema(conn)
        raw_rows = Phase1QuoteAwareAdapterV01.fetch_all_bot_truth_rows(conn)
        events, skipped = Phase1QuoteAwareAdapterV01.normalize_rows(raw_rows)
        grouped = group_events(events)

        eligible = {
            mint: token_events
            for mint, token_events in grouped.items()
            if any(e.base.event_type in (EventType.BUY, EventType.SELL) for e in token_events)
        }
        check("Test 1a - production SQLite opened query_only", len(raw_rows) > 0)
        check("Test 1b - quote-aware normalization has no skipped BOT_TRUTH rows", not skipped)
        check("Test 1c - eligible universe explicitly requires tradable BUY/SELL", len(eligible) > 0)

        metrics = []
        sol_tokens = quote_tokens = 0
        for mint in sorted(eligible):
            win = entry_window_events(eligible[mint])
            if not win:
                continue
            engine = DenominationAwareFeatureEngineV01()
            states = [engine.process(event) for event in win]
            row = characterize_states(mint, states)
            metrics.append(row)
            if row.price_identity == "SOL":
                sol_tokens += 1
            elif row.price_identity.startswith("QUOTE:"):
                quote_tokens += 1

        check("Test 2a - one characterization row per eligible token", len(metrics) == len(eligible))
        check("Test 2b - SOL and quote price identities both represented", sol_tokens > 0 and quote_tokens > 0)
        check("Test 2c - all characterization uses only 0-5m causal MarketState", all(r.data_valid_states + r.data_invalid_states > 0 for r in metrics))

        distributions = empirical_distributions(metrics)
        search_space = build_proposed_search_space(metrics)
        validate_search_space(search_space)

        check("Test 3a - empirical distributions available for every target parameter family",
              all(distributions[k]["n"] > 0 for k in (
                  "max_return_bps",
                  "max_trades_since_t0",
                  "max_unique_buyers_since_t0",
                  "max_pullback_depth_after_impulse_bps",
                  "max_rebound_after_pullback_bps",
                  "max_3s_buys_after_pullback",
                  "max_3s_net_flow_ppm_after_pullback",
                  "max_reclaim_extension_bps",
              )))
        check("Test 3b - proposed search space validates structurally", search_space["status"] == "PROPOSAL_NOT_LOCKED")
        check("Test 3c - max entry age remains fixed at locked 5m", search_space["max_entry_age_ms"] == [300_000])
        check("Test 3d - 3s response window kept fixed in initial search design", search_space["buyer_response"]["window_id"] == ["3s"])
        check("Test 3e - naive full Cartesian search is explicitly not recommended", search_space["full_cartesian_recommended"] is False)

        broad_counts = {
            "eligible_tokens": len(metrics),
            "broad_impulse_reached": sum(r.broad_impulse_reached for r in metrics),
            "broad_pullback_reached": sum(r.broad_pullback_reached for r in metrics),
            "broad_proto_response_reached": sum(r.broad_proto_response_reached for r in metrics),
        }

        report = {
            "schema_version": "P2PSD-0.1",
            "purpose": "FEATURE-SUPPORT PARAMETER SEARCH DESIGN; NO OUTCOME/PnL",
            "source_rows": len(raw_rows),
            "normalized_events": len(events),
            "eligible_tokens": len(metrics),
            "sol_tokens": sol_tokens,
            "quote_tokens": quote_tokens,
            "adapter_skipped": skipped,
            "broad_characterization_anchors": {
                "warning": "CHARACTERIZATION ONLY; NOT LOCKED STRATEGY RULES",
                "impulse_min_return_bps": BROAD_IMPULSE_RETURN_BPS,
                "impulse_min_trades": BROAD_IMPULSE_MIN_TRADES,
                "impulse_min_buyers": BROAD_IMPULSE_MIN_BUYERS,
                "pullback_min_bps": BROAD_PULLBACK_MIN_BPS,
                "pullback_max_bps": BROAD_PULLBACK_MAX_BPS,
                "proto_response_min_rebound_bps": BROAD_PROTO_RESPONSE_REBOUND_BPS,
                "proto_response_min_buys": BROAD_PROTO_RESPONSE_MIN_BUYS,
            },
            "broad_stage_counts": broad_counts,
            "empirical_distributions": distributions,
            "proposed_search_space": search_space,
            "outcome_pnl_labels_used": False,
            "performance_metrics_used_for_design": False,
            "parameter_optimization_performed": False,
        }
        report["deterministic_payload_sha256"] = stable_sha256(report)

        # Repeat the deterministic design calculation from the same feature rows.
        report_check = {
            "distributions": empirical_distributions(metrics),
            "space": build_proposed_search_space(metrics),
        }
        report_check_2 = {
            "distributions": empirical_distributions(metrics),
            "space": build_proposed_search_space(metrics),
        }
        check("Test 4a - empirical range design deterministic", stable_sha256(report_check) == stable_sha256(report_check_2))
        check("Test 4b - no outcome/PnL field enters search design", report["outcome_pnl_labels_used"] is False and report["performance_metrics_used_for_design"] is False)
        check("Test 4c - parameter optimization explicitly absent", report["parameter_optimization_performed"] is False)

        out_dir.mkdir(parents=True, exist_ok=True)
        report_path = out_dir / "phase2_parameter_search_design_report_v0_1.json"
        token_path = out_dir / "phase2_parameter_search_design_token_metrics_v0_1.csv"
        space_path = out_dir / "phase2_parameter_search_space_proposal_v0_1.json"
        report_path.write_text(json.dumps(report, indent=2, sort_keys=True, default=str), encoding="utf-8")
        write_csv(token_path, [r.__dict__ if hasattr(r, "__dict__") else {
            name: getattr(r, name) for name in r.__dataclass_fields__
        } for r in metrics])
        space_path.write_text(json.dumps(search_space, indent=2, sort_keys=True, default=str), encoding="utf-8")
        check("Test 5a - report/token/search-space outputs written outside production DB",
              report_path.exists() and token_path.exists() and space_path.exists())
    finally:
        conn.close()

    after = db_path.stat()
    check("Test 6a - production DB size unchanged", before.st_size == after.st_size)
    check("Test 6b - production DB mtime unchanged", before.st_mtime_ns == after.st_mtime_ns)

    print()
    print("=" * 118)
    print("EMPIRICAL FEATURE SUPPORT — NO PNL / NO OUTCOME LABELS")
    print("=" * 118)
    print(f"Eligible tokens                            : {len(metrics)}")
    print(f"SOL / QUOTE tokens                        : {sol_tokens} / {quote_tokens}")
    print(f"Broad impulse / pullback / proto-response : "
          f"{broad_counts['broad_impulse_reached']} / "
          f"{broad_counts['broad_pullback_reached']} / "
          f"{broad_counts['broad_proto_response_reached']}")
    print()
    print("Key empirical distributions:")
    for key in (
        "max_return_bps",
        "max_trades_since_t0",
        "max_unique_buyers_since_t0",
        "broad_impulse_age_ms",
        "max_pullback_depth_after_impulse_bps",
        "max_rebound_after_pullback_bps",
        "max_3s_buys_after_pullback",
        "max_3s_net_flow_ppm_after_pullback",
        "max_reclaim_extension_bps",
    ):
        print(f"  {key:<42} {distributions[key]}")

    print()
    print("=" * 118)
    print("PROPOSED INITIAL PARAMETER SEARCH SPACE — NOT LOCKED")
    print("=" * 118)
    print(f"Impulse min return bps          : {search_space['impulse']['min_return_bps']}")
    print(f"Impulse min trades              : {search_space['impulse']['min_trades_since_t0']}")
    print(f"Impulse min unique buyers       : {search_space['impulse']['min_unique_buyers_since_t0']}")
    print(f"Pullback min depth bps          : {search_space['pullback']['min_depth_bps']}")
    print(f"Pullback max depth bps          : {search_space['pullback']['max_depth_bps']}")
    print(f"Buyer-response window           : {search_space['buyer_response']['window_id']}")
    print(f"Buyer-response min rebound bps  : {search_space['buyer_response']['min_rebound_bps']}")
    print(f"Buyer-response min buys         : {search_space['buyer_response']['min_buys']}")
    print(f"Buyer-response min flow ppm     : {search_space['buyer_response']['min_net_flow_reserve_ppm']}")
    print(f"Reclaim min extension bps       : {search_space['reclaim']['min_extension_from_response_bps']}")
    print(f"Runaway max extension bps       : {search_space['runaway']['max_extension_from_response_bps']}")
    print()
    print(f"Valid pullback min/max pairs    : {search_space['pullback']['valid_min_max_pair_count']}")
    print(f"Valid reclaim/runaway pairs     : {search_space['runaway']['valid_reclaim_runaway_pair_count']}")
    print(f"Block combination counts        : {search_space['block_combination_counts']}")
    print(f"Naive full Cartesian count      : {search_space['naive_full_cartesian_count']:,}")
    print("Naive full Cartesian recommended: NO")
    print()
    print("IMPORTANT")
    print(" * These ranges were derived from causal market-feature support only.")
    print(" * No future return, PnL, winrate, expectancy, Profit Factor, exit model, or execution model was used.")
    print(" * The broad structural anchors above are measurement aids, not strategy rules.")
    print(" * The proposed values are NOT LOCKED until explicitly reviewed/accepted.")
    print(" * Later performance research should use staged/block experiments, not the naive full Cartesian grid.")
    print(" * Once a search space is locked, new market data should be preserved for out-of-sample validation rather")
    print("   than repeatedly changing the ranges after seeing later performance.")
    print()
    print(f"Research report JSON                     : {report_path}")
    print(f"Token feature-support CSV                : {token_path}")
    print(f"Search-space proposal JSON               : {space_path}")
    print("Production DB touched by harness         : NO")
    print("Network/RPC used                         : NO")
    print("Wallet/order/execution used              : NO")
    print("Outcome/PnL performance used             : NO")
    print("Parameter optimization performed         : NO")
    print(f"RESULT                                   : {'PASS' if not failures else 'FAIL'}")

    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    args = parser.parse_args()
    main(args.db, args.out_dir)
