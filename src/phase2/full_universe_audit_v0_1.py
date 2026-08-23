from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import asdict
from datetime import datetime, timezone
from hashlib import sha256
import json
from typing import Iterable, Sequence

from .large_cohort_research_v0_1 import build_large_cohort_payload
from .models_v0_1 import (
    EventType,
    FirstPullbackParameterSet,
    IngestionSource,
    NormalizedMarketEvent,
)
from .research_replay_v0_3 import ResearchReadResult, group_events_by_mint, summarize_int_metric


FULL_UNIVERSE_SCHEMA_VERSION = "P2FUA-0.1"
ENTRY_WINDOW_MS = 300_000
FIXED_BIN_MINUTES = 30
EQUAL_COUNT_SEGMENTS = 10


def _jsonable(value):
    if hasattr(value, "value"):
        return value.value
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(v) for v in value]
    return value


def canonical_json(value: object) -> str:
    return json.dumps(_jsonable(value), sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def stable_sha256(value: object) -> str:
    return sha256(canonical_json(value).encode("utf-8")).hexdigest()


def research_probe_parameter_sets() -> tuple[FirstPullbackParameterSet, ...]:
    """Same descriptive probes as v0.3/v2.8. NOT locked strategy rules."""
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


def _priceable(event: NormalizedMarketEvent) -> bool:
    v_sol = event.virtual_sol_reserve_lamports
    v_tok = event.virtual_token_reserve_raw
    return v_sol is not None and v_tok is not None and v_sol > 0 and v_tok > 0


def reserve_defect_codes(event: NormalizedMarketEvent) -> tuple[str, ...]:
    codes: list[str] = []
    if event.virtual_sol_reserve_lamports is None:
        codes.append("VIRTUAL_SOL_MISSING")
    elif event.virtual_sol_reserve_lamports <= 0:
        codes.append("VIRTUAL_SOL_NONPOSITIVE")
    if event.virtual_token_reserve_raw is None:
        codes.append("VIRTUAL_TOKEN_MISSING")
    elif event.virtual_token_reserve_raw <= 0:
        codes.append("VIRTUAL_TOKEN_NONPOSITIVE")
    return tuple(codes)


def analyze_price_coverage(
    grouped: dict[str, list[NormalizedMarketEvent]],
    cohort_mints: Sequence[str],
    *,
    raw_rows_by_mint: dict[str, dict[str, object]] | None = None,
    entry_window_ms: int = ENTRY_WINDOW_MS,
) -> tuple[list[dict[str, object]], dict[str, object]]:
    rows: list[dict[str, object]] = []
    token_class_counts: Counter[str] = Counter()
    defect_event_counts: Counter[str] = Counter()
    defect_token_counts: Counter[str] = Counter()

    for mint in cohort_mints:
        events = grouped.get(mint, [])
        trades = [e for e in events if e.event_type in (EventType.BUY, EventType.SELL)]
        first = trades[0] if trades else None
        t0 = first.observed_at_us if first else None
        if t0 is None:
            in_window: list[NormalizedMarketEvent] = []
        else:
            deadline = t0 + entry_window_ms * 1_000
            in_window = [e for e in trades if t0 <= e.observed_at_us <= deadline]

        priced = [e for e in in_window if _priceable(e)]
        unpriced = [e for e in in_window if not _priceable(e)]
        first_priced = bool(first is not None and _priceable(first))
        any_priced = bool(priced)

        if not in_window:
            cls = "NO_TRADABLE_EVENTS_IN_WINDOW"
        elif first_priced and not unpriced:
            cls = "FULLY_PRICED_5M"
        elif first_priced:
            cls = "FIRST_TRADE_PRICED_PARTIAL_GAPS"
        elif any_priced:
            cls = "FIRST_TRADE_UNPRICED_LATER_PRICED"
        else:
            cls = "NO_PRICED_TRADES_5M"
        token_class_counts[cls] += 1

        token_defects: set[str] = set()
        for event in unpriced:
            for code in reserve_defect_codes(event):
                defect_event_counts[code] += 1
                token_defects.add(code)
        for code in token_defects:
            defect_token_counts[code] += 1

        raw = raw_rows_by_mint.get(mint, {}) if raw_rows_by_mint else {}
        return_available = raw.get("max_return_from_t0_bps") is not None if raw else first_priced
        short_price_available = raw.get("max_3s_price_change_bps") is not None if raw else any_priced

        rows.append(
            {
                "mint": mint,
                "first_tradable_observed_at_us": t0,
                "trade_events_5m": len(in_window),
                "priced_trade_events_5m": len(priced),
                "unpriced_trade_events_5m": len(unpriced),
                "first_trade_priceable": first_priced,
                "any_priceable_trade_5m": any_priced,
                "price_coverage_class": cls,
                "return_from_t0_metric_available": return_available,
                "short_window_price_metric_available": short_price_available,
                "gap_event_count": sum(1 for e in events if e.source == IngestionSource.GAP_RECOVERY),
                "reserve_defect_codes": ";".join(sorted(token_defects)),
            }
        )

    n = len(cohort_mints)
    summary = {
        "tokens": n,
        "class_counts": dict(sorted(token_class_counts.items())),
        "class_rates_pct": {
            k: (100.0 * v / n if n else None) for k, v in sorted(token_class_counts.items())
        },
        "reserve_defect_event_counts": dict(sorted(defect_event_counts.items())),
        "reserve_defect_token_counts": dict(sorted(defect_token_counts.items())),
        "first_trade_priceable_tokens": sum(bool(r["first_trade_priceable"]) for r in rows),
        "any_priceable_trade_5m_tokens": sum(bool(r["any_priceable_trade_5m"]) for r in rows),
        "return_metric_available_tokens": sum(bool(r["return_from_t0_metric_available"]) for r in rows),
        "short_price_metric_available_tokens": sum(bool(r["short_window_price_metric_available"]) for r in rows),
    }
    return rows, summary


def _bin_start_us(t_us: int, minutes: int) -> int:
    width = minutes * 60 * 1_000_000
    return (t_us // width) * width


def _iso_us(t_us: int | None) -> str | None:
    if t_us is None:
        return None
    return datetime.fromtimestamp(t_us / 1_000_000, tz=timezone.utc).isoformat()


def build_fixed_time_bins(
    raw_rows: Sequence[dict[str, object]],
    quality_rows: Sequence[dict[str, object]],
    probe_rows: Sequence[dict[str, object]],
    *,
    bin_minutes: int = FIXED_BIN_MINUTES,
) -> list[dict[str, object]]:
    if bin_minutes <= 0:
        raise ValueError("bin_minutes must be > 0")
    quality_by_mint = {str(r["mint"]): r for r in quality_rows}
    raw_by_mint = {str(r["mint"]): r for r in raw_rows}

    candidates: dict[str, set[str]] = defaultdict(set)
    impulse: dict[str, set[str]] = defaultdict(set)
    for row in probe_rows:
        pid = str(row["parameter_set_id"])
        mint = str(row["mint"])
        if row.get("candidate_age_ms") is not None:
            candidates[pid].add(mint)
        if row.get("impulse_age_ms") is not None:
            impulse[pid].add(mint)

    grouped_mints: dict[int, list[str]] = defaultdict(list)
    for row in raw_rows:
        t0 = row.get("first_tradable_observed_at_us")
        if isinstance(t0, int):
            grouped_mints[_bin_start_us(t0, bin_minutes)].append(str(row["mint"]))

    out: list[dict[str, object]] = []
    for start in sorted(grouped_mints):
        mints = grouped_mints[start]
        end = start + bin_minutes * 60 * 1_000_000
        trades = [raw_by_mint[m].get("max_trades_since_t0") for m in mints]
        buyers = [raw_by_mint[m].get("max_unique_buyers_since_t0") for m in mints]
        price_ok = sum(bool(quality_by_mint[m]["return_from_t0_metric_available"]) for m in mints)
        gap_tokens = sum(int(quality_by_mint[m]["gap_event_count"]) > 0 for m in mints)
        row: dict[str, object] = {
            "bin_start_observed_at_us": start,
            "bin_end_observed_at_us": end,
            "bin_start_utc": _iso_us(start),
            "bin_end_utc": _iso_us(end),
            "tokens": len(mints),
            "price_metric_coverage_tokens": price_ok,
            "price_metric_coverage_pct": 100.0 * price_ok / len(mints),
            "gap_tokens": gap_tokens,
            "trades_p50": summarize_int_metric(trades)["p50"],
            "trades_p90": summarize_int_metric(trades)["p90"],
            "unique_buyers_p50": summarize_int_metric(buyers)["p50"],
            "unique_buyers_p90": summarize_int_metric(buyers)["p90"],
        }
        for pid in sorted(candidates):
            c = sum(m in candidates[pid] for m in mints)
            i = sum(m in impulse[pid] for m in mints)
            row[f"{pid}__impulse"] = i
            row[f"{pid}__candidate"] = c
            row[f"{pid}__candidate_rate_pct"] = 100.0 * c / len(mints)
        out.append(row)
    return out


def build_full_universe_payload(
    *,
    read_result: ResearchReadResult,
    probes: Sequence[FirstPullbackParameterSet] | None = None,
) -> tuple[
    dict[str, object],
    list[dict[str, object]],
    list[dict[str, object]],
    list[dict[str, object]],
    list[dict[str, object]],
]:
    probes = tuple(probes or research_probe_parameter_sets())
    base_payload, raw_rows, probe_rows = build_large_cohort_payload(
        read_result=read_result,
        probes=probes,
        segment_count=EQUAL_COUNT_SEGMENTS,
    )
    grouped = group_events_by_mint(read_result.events)
    raw_by_mint = {str(r["mint"]): r for r in raw_rows}
    quality_rows, quality_summary = analyze_price_coverage(
        grouped,
        read_result.cohort.mints,
        raw_rows_by_mint=raw_by_mint,
    )
    bins = build_fixed_time_bins(raw_rows, quality_rows, probe_rows)

    payload = {
        "schema_version": FULL_UNIVERSE_SCHEMA_VERSION,
        "research_scope": "FULL_ELIGIBLE_BOT_TRUTH_UNIVERSE_COVERAGE_STABILITY_AUDIT",
        "performance_pnl_included": False,
        "future_return_label_included": False,
        "parameter_optimization_performed": False,
        "execution_model_used": False,
        "risk_manager_used": False,
        "entry_window_ms": ENTRY_WINDOW_MS,
        "fixed_time_bin_minutes": FIXED_BIN_MINUTES,
        "base_characterization": base_payload,
        "price_data_quality": quality_summary,
        "fixed_time_bins": bins,
    }
    payload["deterministic_payload_sha256"] = stable_sha256(payload)
    return payload, raw_rows, probe_rows, quality_rows, bins
