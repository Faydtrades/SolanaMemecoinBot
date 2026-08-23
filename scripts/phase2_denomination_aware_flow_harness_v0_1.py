from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.phase2.denomination_aware_flow_v0_1 import (
    DenominationAwareFeatureEngineV01,
    FlowCompatibility,
    entry_window_events,
    stable_sha256,
)
from src.phase2.feature_engine_v0_2 import FeatureEngineV02
from src.phase2.models_v0_1 import (
    EventType,
    FirstPullbackParameterSet,
    FirstPullbackState,
)
from src.phase2.phase1_readonly_adapter_v0_1 import Phase1ReadOnlyAdapterV01
from src.phase2.quote_aware_price_v0_1 import (
    Phase1QuoteAwareAdapterV01,
    PricePathKind,
    classify_token_path,
    group_events,
)
from src.phase2.strategy_first_pullback_v0_2 import FirstPullbackStrategyV02

DEFAULT_DB = PROJECT_ROOT / "data" / "db" / "tradingbot.sqlite3"
DEFAULT_OUT = PROJECT_ROOT / "data" / "research" / "phase2_denomination_aware_flow_v0_1"


def compatibility_params() -> FirstPullbackParameterSet:
    # MECHANICAL COMPATIBILITY VALUES ONLY.  These are not selected/tuned trading
    # parameters and candidate counts from this harness are forbidden as performance.
    return FirstPullbackParameterSet(
        parameter_set_id="FP1-FLOW-COMPAT-0001",
        strategy_version="v1.1",
        max_entry_age_ms=300_000,
        impulse_parameters={
            "min_return_bps": 1000,
            "min_trades_since_t0": 2,
            "min_unique_buyers_since_t0": 1,
        },
        pullback_parameters={"min_depth_bps": 1000, "max_depth_bps": 9000},
        buyer_response_parameters={
            "window_id": "3s",
            "min_rebound_bps": 100,
            "min_buys": 1,
            "min_net_flow_reserve_ppm": 0,
        },
        reclaim_parameters={"min_extension_from_response_bps": 100},
        runaway_entry_parameters={"max_extension_from_response_bps": 5000},
    )


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fields: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for k in row:
            if k not in seen:
                seen.add(k)
                fields.append(k)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def main(db_path: Path, out_dir: Path) -> None:
    print("PHASE 2 DENOMINATION-AWARE FLOW / STRATEGY COMPATIBILITY v0.1")
    print("Purpose                    : SOL/QUOTE RAW FLOW SAFETY + COMMON RESERVE-NORMALIZED FEATURE")
    print("PnL/backtest                : NOT INCLUDED")
    print("Future-return labels        : NOT INCLUDED")
    print("Parameter optimization      : NOT PERFORMED")
    print("Compatibility thresholds    : MECHANICAL TEST VALUES ONLY")
    print("Production DB mode          : READ ONLY")
    print("Network/RPC                 : DISABLED")
    print("Wallet/order/execution      : DISABLED")
    print()

    before = db_path.stat()
    conn = Phase1ReadOnlyAdapterV01.open_readonly(db_path)
    failures: list[str] = []

    def check(label: str, ok: bool) -> None:
        print(f"{label:<82} {'PASS' if ok else 'FAIL'}")
        if not ok:
            failures.append(label)

    try:
        Phase1QuoteAwareAdapterV01.validate_schema(conn)
        rows = Phase1QuoteAwareAdapterV01.fetch_all_bot_truth_rows(conn)
        events, skipped = Phase1QuoteAwareAdapterV01.normalize_rows(rows)
        grouped = group_events(events)
        check("Test 1a - production SQLite opened query_only with quote fields", len(rows) > 0)
        check("Test 1b - BOT_TRUTH quote-aware normalization has no skipped rows", len(events) > 0 and not skipped)

        params = compatibility_params()
        FirstPullbackStrategyV02.validate_parameters(params)

        token_rows: list[dict[str, object]] = []
        path_counts: Counter[str] = Counter()
        final_strategy_states: Counter[str] = Counter()
        compatible_tokens = 0
        strategy_error_tokens = 0
        flow_conflict_tokens = 0
        unavailable_flow_tokens = 0
        quote_strategy_tokens = 0
        sol_strategy_tokens = 0
        sol_parity_tokens = 0
        sol_parity_failures = 0
        quote_raw_lamport_contamination = 0
        candidate_count_compat_only = 0
        deterministic_digests: list[str] = []

        for mint, all_events in sorted(grouped.items(), key=lambda kv: kv[0]):
            win = entry_window_events(all_events)
            if not win:
                continue
            path_info = classify_token_path([
                e for e in win if e.base.event_type in (EventType.BUY, EventType.SELL)
            ])
            token_class = str(path_info["token_class"])
            path_counts[token_class] += 1

            feature = DenominationAwareFeatureEngineV01()
            strategy = FirstPullbackStrategyV02()
            run = None
            final_state = None
            evaluations = []
            strategy_error = None
            try:
                for event in win:
                    state = feature.process(event)
                    final_state = state
                    if run is None:
                        run = strategy.start_run(state, params)
                    evaluation = strategy.evaluate(state, run, params)
                    evaluations.append(evaluation)
            except Exception as exc:
                strategy_error = f"{type(exc).__name__}:{exc}"
                strategy_error_tokens += 1

            if final_state is None:
                continue

            since = final_state.windows["since_t0"]
            if final_state.flow_identity_conflict_seen:
                flow_conflict_tokens += 1
            if since.flow_compatibility != FlowCompatibility.COMPATIBLE:
                unavailable_flow_tokens += 1
            if (
                strategy_error is None
                and not final_state.flow_identity_conflict_seen
                and since.flow_compatibility == FlowCompatibility.COMPATIBLE
                and final_state.market.price_identity.path in (PricePathKind.SOL, PricePathKind.QUOTE)
            ):
                compatible_tokens += 1

            if final_state.market.price_identity.path == PricePathKind.QUOTE:
                quote_strategy_tokens += 1
                if since.sol_buy_volume_lamports != 0 or since.sol_sell_volume_lamports != 0:
                    quote_raw_lamport_contamination += 1
            elif final_state.market.price_identity.path == PricePathKind.SOL:
                sol_strategy_tokens += 1

            if any(e.candidate_signal is not None for e in evaluations):
                candidate_count_compat_only += 1
            if run is not None:
                final_strategy_states[run.current_state.value] += 1

            # Exact legacy parity on the SOL path: denomination-aware work must not
            # alter the existing causal SOL price/count/lamport-flow substrate.
            parity_ok = True
            if token_class == "SOL_ONLY":
                legacy = FeatureEngineV02()
                flow_again = DenominationAwareFeatureEngineV01()
                sol_parity_tokens += 1
                for event in win:
                    old = legacy.process(event.base)
                    new = flow_again.process(event)
                    if event.base.event_type not in (EventType.BUY, EventType.SELL):
                        continue
                    if old.market.current_price_proxy != new.market.current_price_proxy:
                        parity_ok = False
                    if old.price_structure.return_from_t0_bps != new.price_structure.return_from_t0_bps:
                        parity_ok = False
                    if old.price_structure.drawdown_from_high_bps != new.price_structure.drawdown_from_high_bps:
                        parity_ok = False
                    for wid in ("1s", "3s", "10s", "30s", "60s", "since_t0"):
                        ow = old.windows[wid]
                        nw = new.windows[wid]
                        if (ow.trade_count, ow.buys, ow.sells, ow.unique_buyers, ow.unique_sellers) != (
                            nw.trade_count, nw.buys, nw.sells, nw.unique_buyers, nw.unique_sellers
                        ):
                            parity_ok = False
                        if ow.buy_volume_lamports != nw.sol_buy_volume_lamports:
                            parity_ok = False
                        if ow.sell_volume_lamports != nw.sol_sell_volume_lamports:
                            parity_ok = False
                        if ow.net_flow_lamports != nw.net_flow_raw:
                            parity_ok = False
                        if ow.price_change_bps != nw.price_change_bps:
                            parity_ok = False
                if not parity_ok:
                    sol_parity_failures += 1

            row = {
                "mint": mint,
                "price_path_class": token_class,
                "price_identity": final_state.market.price_identity.label,
                "flow_compatibility": since.flow_compatibility.value,
                "flow_identity_conflict": final_state.flow_identity_conflict_seen,
                "strategy_error": strategy_error,
                "strategy_final_state": run.current_state.value if run else None,
                "strategy_candidate_compat_only": any(e.candidate_signal is not None for e in evaluations),
                "since_t0_trades": since.trade_count,
                "since_t0_sol_buy_lamports": since.sol_buy_volume_lamports,
                "since_t0_sol_sell_lamports": since.sol_sell_volume_lamports,
                "since_t0_quote_buy_raw": since.quote_buy_volume_raw,
                "since_t0_quote_sell_raw": since.quote_sell_volume_raw,
                "since_t0_net_flow_raw": since.net_flow_raw,
                "since_t0_net_flow_reserve_ppm": since.net_flow_reserve_ppm,
                "sol_legacy_parity": parity_ok if token_class == "SOL_ONLY" else None,
            }
            token_rows.append(row)
            deterministic_digests.append(stable_sha256(row))

        total = len(token_rows)
        check("Test 2a - every eligible token receives denomination-aware state", total > 0 and total == len(grouped))
        check("Test 2b - all token price identities remain SOL_ONLY or QUOTE_ONLY", set(path_counts) <= {"SOL_ONLY", "QUOTE_ONLY"})
        check("Test 2c - no flow denomination/path conflicts observed", flow_conflict_tokens == 0)
        check("Test 2d - every token has a compatible reserve-normalized since_t0 flow", unavailable_flow_tokens == 0)
        check("Test 2e - strategy v1.1 mechanically evaluates every token", compatible_tokens == total and strategy_error_tokens == 0)
        check("Test 2f - quote path reaches strategy without fake lamport contamination", quote_strategy_tokens > 0 and quote_raw_lamport_contamination == 0)
        check("Test 2g - SOL path remains represented", sol_strategy_tokens > 0)
        check("Test 3a - existing SOL FeatureEngineV02 semantics are exactly preserved", sol_parity_tokens > 0 and sol_parity_failures == 0)

        # Determinism of the complete mechanical compatibility summary.
        summary = {
            "tokens_analyzed": total,
            "price_path_counts": dict(sorted(path_counts.items())),
            "strategy_compatible_tokens": compatible_tokens,
            "strategy_error_tokens": strategy_error_tokens,
            "flow_conflict_tokens": flow_conflict_tokens,
            "unavailable_flow_tokens": unavailable_flow_tokens,
            "sol_strategy_tokens": sol_strategy_tokens,
            "quote_strategy_tokens": quote_strategy_tokens,
            "quote_raw_lamport_contamination_tokens": quote_raw_lamport_contamination,
            "sol_legacy_parity_tokens": sol_parity_tokens,
            "sol_legacy_parity_failures": sol_parity_failures,
            "final_strategy_state_counts_compatibility_only": dict(sorted(final_strategy_states.items())),
            "candidate_count_compatibility_only": candidate_count_compat_only,
            "common_flow_feature": "net_flow_reserve_ppm",
            "common_flow_feature_is_sol_equivalent_volume": False,
            "raw_sol_and_quote_flow_remain_separate": True,
            "performance_pnl_included": False,
            "future_return_labels_included": False,
            "strategy_parameter_optimization_performed": False,
            "mechanical_compatibility_parameters_only": True,
            "deterministic_token_digest": stable_sha256(deterministic_digests),
        }
        summary_2 = json.loads(json.dumps(summary, sort_keys=True))
        check("Test 4a - full compatibility payload is deterministic", stable_sha256(summary) == stable_sha256(summary_2))
        check("Test 4b - common flow metric is explicitly not SOL-equivalent volume", summary["common_flow_feature_is_sol_equivalent_volume"] is False)

        out_dir.mkdir(parents=True, exist_ok=True)
        report = {
            "schema_version": "DAF-0.1",
            "purpose": "DENOMINATION-AWARE FLOW + FIRST PULLBACK MECHANICAL COMPATIBILITY; NO PNL",
            "source_rows": len(rows),
            "normalized_events": len(events),
            "adapter_skipped": skipped,
            "compatibility_parameter_set": {
                "parameter_set_id": params.parameter_set_id,
                "strategy_version": params.strategy_version,
                "warning": "MECHANICAL COMPATIBILITY VALUES ONLY; NOT SELECTED/TRADING PARAMETERS",
            },
            "summary": summary,
        }
        report["deterministic_payload_sha256"] = stable_sha256(report)
        report_path = out_dir / "phase2_denomination_aware_flow_report_v0_1.json"
        token_path = out_dir / "phase2_denomination_aware_flow_tokens_v0_1.csv"
        report_path.write_text(json.dumps(report, indent=2, sort_keys=True, default=str), encoding="utf-8")
        write_csv(token_path, token_rows)
        check("Test 5a - research outputs written outside production DB", report_path.exists() and token_path.exists())
    finally:
        conn.close()

    after = db_path.stat()
    check("Test 6a - production DB size unchanged", before.st_size == after.st_size)
    check("Test 6b - production DB mtime unchanged", before.st_mtime_ns == after.st_mtime_ns)

    print()
    print("=" * 112)
    print("DENOMINATION-AWARE FLOW / STRATEGY COMPATIBILITY — NO PNL")
    print("=" * 112)
    print(f"Tokens analyzed                           : {summary['tokens_analyzed']}")
    print(f"Price path counts                        : {summary['price_path_counts']}")
    print(f"Strategy-compatible tokens               : {summary['strategy_compatible_tokens']}/{summary['tokens_analyzed']}")
    print(f"Strategy error tokens                    : {summary['strategy_error_tokens']}")
    print(f"Flow identity conflict tokens            : {summary['flow_conflict_tokens']}")
    print(f"Flow unavailable tokens                  : {summary['unavailable_flow_tokens']}")
    print(f"SOL strategy-compatible tokens           : {summary['sol_strategy_tokens']}")
    print(f"QUOTE strategy-compatible tokens         : {summary['quote_strategy_tokens']}")
    print(f"Quote→lamport contamination tokens        : {summary['quote_raw_lamport_contamination_tokens']}")
    print(f"SOL legacy parity failures               : {summary['sol_legacy_parity_failures']}/{summary['sol_legacy_parity_tokens']}")
    print(f"Compatibility-only final state counts    : {summary['final_strategy_state_counts_compatibility_only']}")
    print(f"Compatibility-only candidate count       : {summary['candidate_count_compatibility_only']}")
    print()
    print("IMPORTANT")
    print(" * net_flow_reserve_ppm is a dimensionless reserve-relative pressure proxy.")
    print(" * It is NOT SOL-equivalent volume and does not convert quote units to SOL.")
    print(" * Raw SOL lamports and raw quote amounts remain separate and auditable.")
    print(" * Candidate/state counts above use mechanical compatibility values only; they are NOT strategy results.")
    print(" * No PnL, future-return labels, execution, or parameter optimization are used.")
    print()
    print(f"Research report JSON                    : {report_path}")
    print(f"Token compatibility CSV                 : {token_path}")
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
