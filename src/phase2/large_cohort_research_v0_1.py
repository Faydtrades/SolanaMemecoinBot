from __future__ import annotations

from collections import Counter
from dataclasses import asdict
from decimal import Decimal, ROUND_HALF_EVEN
from hashlib import sha256
import json
from typing import Iterable, Sequence

from .feature_engine_v0_2 import FeatureEngineV02
from .models_v0_1 import (
    EventType,
    FirstPullbackParameterSet,
    FirstPullbackState,
    IngestionSource,
    NormalizedMarketEvent,
)
from .research_replay_v0_3 import (
    ResearchReadResult,
    build_raw_token_metrics,
    group_events_by_mint,
    summarize_int_metric,
    summarize_raw_metrics,
)
from .strategy_clock_v0_1 import FirstPullbackStrategyClockV01, StrategyDeadline
from .strategy_first_pullback_v0_1 import (
    FirstPullbackReason,
    FirstPullbackStrategyV01,
)


LARGE_COHORT_SCHEMA_VERSION = "P2LCR-0.1"
DEFAULT_ENTRY_WINDOW_MS = 300_000
DEFAULT_TIME_SEGMENTS = 5


_STAGE_ORDER = {
    # Setup progress only. Terminal outcomes are intentionally excluded so an EXPIRED
    # token still reports *where in the setup* it got before expiry.
    FirstPullbackState.DISCOVERED.value: 0,
    FirstPullbackState.WAITING_FOR_IMPULSE.value: 1,
    FirstPullbackState.IMPULSE_CONFIRMED.value: 2,
    FirstPullbackState.WAITING_FOR_PULLBACK.value: 3,
    FirstPullbackState.PULLBACK_ACTIVE.value: 4,
    FirstPullbackState.WAITING_FOR_RESPONSE.value: 5,
    FirstPullbackState.WAITING_FOR_RECLAIM.value: 6,
    FirstPullbackState.CANDIDATE_SIGNAL.value: 7,
}


def _jsonable(value):
    if isinstance(value, Decimal):
        return str(value)
    if hasattr(value, "value"):
        return value.value
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(v) for v in value]
    return value


def canonical_json(value: object) -> str:
    return json.dumps(
        _jsonable(value), sort_keys=True, separators=(",", ":"), ensure_ascii=True
    )


def stable_sha256(value: object) -> str:
    return sha256(canonical_json(value).encode("utf-8")).hexdigest()


def parameter_set_dict(params: FirstPullbackParameterSet) -> dict[str, object]:
    return _jsonable(asdict(params))


def _bps(current: Decimal | None, reference: Decimal | None) -> int | None:
    if current is None or reference is None or reference == 0:
        return None
    raw = ((current / reference) - Decimal(1)) * Decimal(10_000)
    return int(raw.quantize(Decimal("1"), rounding=ROUND_HALF_EVEN))


def _depth_bps(current: Decimal | None, high: Decimal | None) -> int | None:
    move = _bps(current, high)
    if move is None:
        return None
    return max(0, -move)


def _first_tradable_observed_at_us(events: Sequence[NormalizedMarketEvent]) -> int | None:
    for event in events:
        if event.event_type in (EventType.BUY, EventType.SELL):
            return event.observed_at_us
    return None


def _first_tradable_event_at_us(events: Sequence[NormalizedMarketEvent]) -> int | None:
    for event in events:
        if event.event_type in (EventType.BUY, EventType.SELL):
            return event.event_at_us
    return None


def assign_time_segments(
    grouped: dict[str, list[NormalizedMarketEvent]],
    cohort_mints: Sequence[str],
    *,
    segment_count: int = DEFAULT_TIME_SEGMENTS,
) -> tuple[dict[str, int], list[dict[str, object]]]:
    """Assign equal-count chronological segments using BOT_TRUTH t0.

    This is descriptive only.  It does not filter or select tokens based on future
    activity.  Segment 1 is the earliest t0 inside the already-selected cohort.
    """
    if segment_count <= 0:
        raise ValueError("segment_count must be > 0")

    ordered: list[tuple[int, str]] = []
    for mint in cohort_mints:
        t0 = _first_tradable_observed_at_us(grouped.get(mint, []))
        if t0 is not None:
            ordered.append((t0, mint))
    ordered.sort(key=lambda x: (x[0], x[1]))

    mapping: dict[str, int] = {}
    meta: list[dict[str, object]] = []
    n = len(ordered)
    if n == 0:
        return mapping, meta

    for idx, (t0, mint) in enumerate(ordered):
        seg = min(segment_count - 1, (idx * segment_count) // n) + 1
        mapping[mint] = seg

    for seg in range(1, segment_count + 1):
        members = [(t0, mint) for t0, mint in ordered if mapping[mint] == seg]
        meta.append(
            {
                "segment_id": seg,
                "tokens": len(members),
                "start_t0_observed_at_us": members[0][0] if members else None,
                "end_t0_observed_at_us": members[-1][0] if members else None,
            }
        )
    return mapping, meta


def _empty_probe_row(
    mint: str,
    parameter_set_id: str,
    token_events: Sequence[NormalizedMarketEvent],
    segment_id: int | None,
) -> dict[str, object]:
    first_obs = _first_tradable_observed_at_us(token_events)
    first_event = _first_tradable_event_at_us(token_events)
    return {
        "mint": mint,
        "parameter_set_id": parameter_set_id,
        "time_segment": segment_id,
        "first_tradable_observed_at_us": first_obs,
        "first_tradable_event_at_us": first_event,
        "gap_event_count": sum(
            1 for e in token_events if e.source == IngestionSource.GAP_RECOVERY
        ),
        "market_events_available": len(token_events),
        "market_events_processed": 0,
        "transition_count": 0,
        "response_failed_new_low_count": 0,
        "furthest_setup_stage": FirstPullbackState.DISCOVERED.value,
        "final_classification": None,
        "last_reason": None,
        "clock_expired": False,
        "impulse_age_ms": None,
        "impulse_return_bps": None,
        "impulse_trade_count": None,
        "impulse_unique_buyers": None,
        "impulse_3s_buys": None,
        "impulse_3s_net_flow_lamports": None,
        "impulse_peak_age_ms": None,
        "impulse_peak_return_bps": None,
        "pullback_active_age_ms": None,
        "impulse_to_pullback_ms": None,
        "pullback_depth_at_activation_bps": None,
        "deepest_pullback_depth_bps": None,
        "pullback_low_age_ms": None,
        "pullback_low_depth_at_response_bps": None,
        "response_age_ms": None,
        "pullback_to_response_ms": None,
        "response_rebound_bps": None,
        "response_window_buys": None,
        "response_window_sells": None,
        "response_window_net_flow_lamports": None,
        "response_window_price_change_bps": None,
        "response_10s_buys": None,
        "response_10s_sells": None,
        "response_10s_net_flow_lamports": None,
        "candidate_age_ms": None,
        "response_to_candidate_ms": None,
        "candidate_return_from_t0_bps": None,
        "candidate_drawdown_from_high_bps": None,
        "reclaim_extension_bps": None,
        "runaway_reject_age_ms": None,
    }


def run_detailed_strategy_probe(
    grouped: dict[str, list[NormalizedMarketEvent]],
    cohort_mints: Sequence[str],
    params: FirstPullbackParameterSet,
    *,
    segment_map: dict[str, int] | None = None,
) -> dict[str, object]:
    """Run one research probe and capture causal stage mechanics per token.

    No exit, PnL, future return, execution, or risk logic is present.  Every metric is
    captured from MarketState at the instant the deterministic state machine observes
    the relevant stage transition, using BOT_TRUTH observed_at semantics.
    """
    FirstPullbackStrategyV01.validate_parameters(params)
    response_window_id = str(params.buyer_response_parameters["window_id"])

    rows: list[dict[str, object]] = []
    reached_counts: Counter[str] = Counter()
    final_counts: Counter[str] = Counter()
    last_reason_counts: Counter[str] = Counter()
    furthest_stage_counts: Counter[str] = Counter()

    for mint in cohort_mints:
        token_events = grouped.get(mint, [])
        row = _empty_probe_row(
            mint,
            params.parameter_set_id,
            token_events,
            segment_map.get(mint) if segment_map else None,
        )
        engine = FeatureEngineV02()
        strategy = FirstPullbackStrategyV01()
        clock = FirstPullbackStrategyClockV01()
        run = None
        deadline: StrategyDeadline | None = None
        reached: set[str] = {FirstPullbackState.DISCOVERED.value}

        def update_furthest(state_name: str) -> None:
            if state_name not in _STAGE_ORDER:
                return
            old = str(row["furthest_setup_stage"])
            if _STAGE_ORDER.get(state_name, -1) > _STAGE_ORDER.get(old, -1):
                row["furthest_setup_stage"] = state_name

        def capture(evaluation, state, pre_response_ref, pre_pullback_low_ref) -> None:
            row["last_reason"] = evaluation.reason_code
            if evaluation.reason_code == FirstPullbackReason.RESPONSE_FAILED_NEW_LOW.value:
                row["response_failed_new_low_count"] = int(
                    row["response_failed_new_low_count"]
                ) + 1

            if not evaluation.transition_occurred:
                return

            row["transition_count"] = int(row["transition_count"]) + 1
            state_name = evaluation.current_state.value
            reached.add(state_name)
            update_furthest(state_name)
            age_ms = state.identity.age_ms
            since = state.windows["since_t0"]
            w3 = state.windows["3s"]
            w10 = state.windows["10s"]

            if (
                state_name == FirstPullbackState.IMPULSE_CONFIRMED.value
                and row["impulse_age_ms"] is None
            ):
                row["impulse_age_ms"] = age_ms
                row["impulse_return_bps"] = state.price_structure.return_from_t0_bps
                row["impulse_trade_count"] = since.trade_count
                row["impulse_unique_buyers"] = since.unique_buyers
                row["impulse_3s_buys"] = w3.buys
                row["impulse_3s_net_flow_lamports"] = w3.net_flow_lamports

            elif (
                state_name == FirstPullbackState.PULLBACK_ACTIVE.value
                and row["pullback_active_age_ms"] is None
            ):
                row["pullback_active_age_ms"] = age_ms
                if row["impulse_age_ms"] is not None and age_ms is not None:
                    row["impulse_to_pullback_ms"] = int(age_ms) - int(row["impulse_age_ms"])
                row["pullback_depth_at_activation_bps"] = _depth_bps(
                    state.market.current_price_proxy,
                    run.impulse_high_ref.price_proxy if run and run.impulse_high_ref else None,
                )
                if run and run.impulse_high_ref and run.impulse_start_ref:
                    row["impulse_peak_age_ms"] = (
                        run.impulse_high_ref.observed_at_us
                        - run.impulse_start_ref.observed_at_us
                    ) // 1_000
                    row["impulse_peak_return_bps"] = _bps(
                        run.impulse_high_ref.price_proxy,
                        run.impulse_start_ref.price_proxy,
                    )

            elif (
                state_name == FirstPullbackState.WAITING_FOR_RECLAIM.value
                and evaluation.reason_code
                == FirstPullbackReason.BUYER_RESPONSE_CONFIRMED.value
                and row["response_age_ms"] is None
            ):
                row["response_age_ms"] = age_ms
                if row["pullback_active_age_ms"] is not None and age_ms is not None:
                    row["pullback_to_response_ms"] = int(age_ms) - int(
                        row["pullback_active_age_ms"]
                    )
                low_ref = run.pullback_low_ref if run else pre_pullback_low_ref
                high_ref = run.impulse_high_ref if run else None
                response_ref = run.response_ref if run else pre_response_ref
                row["pullback_low_depth_at_response_bps"] = _depth_bps(
                    low_ref.price_proxy if low_ref else None,
                    high_ref.price_proxy if high_ref else None,
                )
                row["response_rebound_bps"] = _bps(
                    response_ref.price_proxy if response_ref else state.market.current_price_proxy,
                    low_ref.price_proxy if low_ref else None,
                )
                wr = state.windows[response_window_id]
                row["response_window_buys"] = wr.buys
                row["response_window_sells"] = wr.sells
                row["response_window_net_flow_lamports"] = wr.net_flow_lamports
                row["response_window_price_change_bps"] = wr.price_change_bps
                row["response_10s_buys"] = w10.buys
                row["response_10s_sells"] = w10.sells
                row["response_10s_net_flow_lamports"] = w10.net_flow_lamports

            elif state_name == FirstPullbackState.CANDIDATE_SIGNAL.value:
                row["candidate_age_ms"] = age_ms
                if row["response_age_ms"] is not None and age_ms is not None:
                    row["response_to_candidate_ms"] = int(age_ms) - int(
                        row["response_age_ms"]
                    )
                row["candidate_return_from_t0_bps"] = state.price_structure.return_from_t0_bps
                row["candidate_drawdown_from_high_bps"] = state.price_structure.drawdown_from_high_bps
                response_ref = pre_response_ref or (run.response_ref if run else None)
                row["reclaim_extension_bps"] = _bps(
                    state.market.current_price_proxy,
                    response_ref.price_proxy if response_ref else None,
                )

            elif (
                state_name == FirstPullbackState.REJECT.value
                and evaluation.reason_code == FirstPullbackReason.PRICE_RAN_AWAY.value
            ):
                row["runaway_reject_age_ms"] = age_ms

        for event in token_events:
            # The deterministic clock wins before an observation strictly after the
            # inclusive 5m entry window.
            if run is not None and deadline is not None:
                clock_eval = clock.fire_if_due(
                    deadline, run, params, now_us=event.observed_at_us
                )
                if clock_eval is not None:
                    row["last_reason"] = clock_eval.reason_code
                    row["clock_expired"] = True
                    row["transition_count"] = int(row["transition_count"]) + int(
                        clock_eval.transition_occurred
                    )
                    if clock_eval.transition_occurred:
                        reached.add(clock_eval.current_state.value)
                        update_furthest(clock_eval.current_state.value)
                    break

            state = engine.process(event)
            row["market_events_processed"] = int(row["market_events_processed"]) + 1
            if run is None:
                run = strategy.start_run(state, params)

            if deadline is None:
                deadline = clock.schedule_entry_expiry(state, run, params)

            if run.finished or run.current_state == FirstPullbackState.CANDIDATE_SIGNAL:
                break

            pre_response_ref = run.response_ref
            pre_pullback_low_ref = run.pullback_low_ref
            evaluation = strategy.evaluate(state, run, params)
            capture(evaluation, state, pre_response_ref, pre_pullback_low_ref)
            if evaluation.candidate_signal is not None:
                break

        if (
            run is not None
            and deadline is not None
            and not run.finished
            and run.current_state != FirstPullbackState.CANDIDATE_SIGNAL
        ):
            clock_eval = clock.fire_if_due(
                deadline, run, params, now_us=deadline.expire_at_us
            )
            if clock_eval is not None:
                row["last_reason"] = clock_eval.reason_code
                row["clock_expired"] = True
                row["transition_count"] = int(row["transition_count"]) + int(
                    clock_eval.transition_occurred
                )
                if clock_eval.transition_occurred:
                    reached.add(clock_eval.current_state.value)
                    update_furthest(clock_eval.current_state.value)

        if run is None:
            final = "NO_EVENTS"
        elif run.current_state == FirstPullbackState.CANDIDATE_SIGNAL:
            final = FirstPullbackState.CANDIDATE_SIGNAL.value
        elif run.final_outcome is not None:
            final = run.final_outcome.value
        else:
            final = "OPEN_AT_DATA_END"
        row["final_classification"] = final

        for state_name in reached:
            reached_counts[state_name] += 1
        final_counts[final] += 1
        last_reason = row.get("last_reason")
        if last_reason:
            last_reason_counts[str(last_reason)] += 1
        # Finalize reference-derived mechanics even when a later stage was never
        # reached. This lets us characterize confirmed impulses that expired without a
        # pullback, and deepest pullbacks that never produced a response.
        if run is not None and run.impulse_high_ref is not None and run.impulse_start_ref is not None:
            if row["impulse_peak_age_ms"] is None:
                row["impulse_peak_age_ms"] = (
                    run.impulse_high_ref.observed_at_us - run.impulse_start_ref.observed_at_us
                ) // 1_000
            if row["impulse_peak_return_bps"] is None:
                row["impulse_peak_return_bps"] = _bps(
                    run.impulse_high_ref.price_proxy, run.impulse_start_ref.price_proxy
                )
        if run is not None and run.pullback_low_ref is not None and run.impulse_high_ref is not None:
            row["pullback_low_age_ms"] = (
                run.pullback_low_ref.observed_at_us
                - (run.impulse_start_ref.observed_at_us if run.impulse_start_ref else run.started_at_us)
            ) // 1_000
            row["deepest_pullback_depth_bps"] = _depth_bps(
                run.pullback_low_ref.price_proxy, run.impulse_high_ref.price_proxy
            )

        furthest_stage_counts[str(row["furthest_setup_stage"])] += 1
        rows.append(row)

    n = len(cohort_mints)

    def reached_count(state: FirstPullbackState) -> int:
        return int(reached_counts.get(state.value, 0))

    funnel = {
        "eligible_tokens": n,
        "impulse_confirmed": reached_count(FirstPullbackState.IMPULSE_CONFIRMED),
        "pullback_active": reached_count(FirstPullbackState.PULLBACK_ACTIVE),
        "buyer_response_confirmed": reached_count(FirstPullbackState.WAITING_FOR_RECLAIM),
        "candidate_signal": reached_count(FirstPullbackState.CANDIDATE_SIGNAL),
        "invalidated": int(final_counts.get(FirstPullbackState.INVALIDATED.value, 0)),
        "expired": int(final_counts.get(FirstPullbackState.EXPIRED.value, 0)),
        "rejected": int(final_counts.get(FirstPullbackState.REJECT.value, 0)),
        "open_at_data_end": int(final_counts.get("OPEN_AT_DATA_END", 0)),
        "clock_expired": sum(1 for row in rows if bool(row["clock_expired"])),
    }

    metric_names = (
        "impulse_age_ms",
        "impulse_return_bps",
        "impulse_trade_count",
        "impulse_unique_buyers",
        "impulse_3s_buys",
        "impulse_3s_net_flow_lamports",
        "impulse_peak_age_ms",
        "impulse_peak_return_bps",
        "pullback_active_age_ms",
        "impulse_to_pullback_ms",
        "pullback_depth_at_activation_bps",
        "deepest_pullback_depth_bps",
        "pullback_low_age_ms",
        "pullback_low_depth_at_response_bps",
        "response_age_ms",
        "pullback_to_response_ms",
        "response_rebound_bps",
        "response_window_buys",
        "response_window_sells",
        "response_window_net_flow_lamports",
        "response_window_price_change_bps",
        "response_10s_buys",
        "response_10s_sells",
        "response_10s_net_flow_lamports",
        "candidate_age_ms",
        "response_to_candidate_ms",
        "candidate_return_from_t0_bps",
        "candidate_drawdown_from_high_bps",
        "reclaim_extension_bps",
        "runaway_reject_age_ms",
        "response_failed_new_low_count",
        "transition_count",
    )
    metric_summary = {
        name: summarize_int_metric(
            int(row[name]) if isinstance(row.get(name), int) else None for row in rows
        )
        for name in metric_names
    }

    return {
        "parameter_set": parameter_set_dict(params),
        "funnel": funnel,
        "dropoff": {
            "no_impulse": n - funnel["impulse_confirmed"],
            "impulse_without_pullback": funnel["impulse_confirmed"] - funnel["pullback_active"],
            "pullback_without_response": funnel["pullback_active"] - funnel["buyer_response_confirmed"],
            "response_without_candidate": funnel["buyer_response_confirmed"] - funnel["candidate_signal"],
        },
        "metrics": metric_summary,
        "final_classification_counts": dict(sorted(final_counts.items())),
        "last_reason_counts": dict(sorted(last_reason_counts.items())),
        "furthest_stage_counts": dict(sorted(furthest_stage_counts.items())),
        "token_rows": rows,
    }


def _segment_probe_summary(
    probe: dict[str, object],
    segment_meta: Sequence[dict[str, object]],
) -> list[dict[str, object]]:
    token_rows = probe["token_rows"]
    assert isinstance(token_rows, list)
    result: list[dict[str, object]] = []
    for meta in segment_meta:
        seg_id = int(meta["segment_id"])
        rows = [r for r in token_rows if int(r.get("time_segment") or 0) == seg_id]
        n = len(rows)

        def count_nonnull(name: str) -> int:
            return sum(1 for r in rows if r.get(name) is not None)

        final = Counter(str(r.get("final_classification")) for r in rows)
        result.append(
            {
                **meta,
                "impulse_confirmed": count_nonnull("impulse_age_ms"),
                "pullback_active": count_nonnull("pullback_active_age_ms"),
                "buyer_response_confirmed": count_nonnull("response_age_ms"),
                "candidate_signal": count_nonnull("candidate_age_ms"),
                "candidate_rate_ppm": (
                    round(1_000_000 * count_nonnull("candidate_age_ms") / n) if n else None
                ),
                "expired": int(final.get(FirstPullbackState.EXPIRED.value, 0)),
                "rejected": int(final.get(FirstPullbackState.REJECT.value, 0)),
                "invalidated": int(final.get(FirstPullbackState.INVALIDATED.value, 0)),
                "open_at_data_end": int(final.get("OPEN_AT_DATA_END", 0)),
            }
        )
    return result


def build_large_cohort_payload(
    *,
    read_result: ResearchReadResult,
    probes: Sequence[FirstPullbackParameterSet],
    segment_count: int = DEFAULT_TIME_SEGMENTS,
) -> tuple[dict[str, object], list[dict[str, object]], list[dict[str, object]]]:
    grouped = group_events_by_mint(read_result.events)
    token_rows = build_raw_token_metrics(grouped, read_result.cohort.mints)
    raw_summary = summarize_raw_metrics(token_rows)
    segment_map, segment_meta = assign_time_segments(
        grouped, read_result.cohort.mints, segment_count=segment_count
    )

    # Add segment/t0 context to raw rows without changing any causal feature.
    for row in token_rows:
        mint = str(row["mint"])
        row["time_segment"] = segment_map.get(mint)
        row["first_tradable_observed_at_us"] = _first_tradable_observed_at_us(
            grouped.get(mint, [])
        )

    probe_results: list[dict[str, object]] = []
    flat_probe_rows: list[dict[str, object]] = []
    for params in probes:
        detailed = run_detailed_strategy_probe(
            grouped,
            read_result.cohort.mints,
            params,
            segment_map=segment_map,
        )
        detailed["time_segments"] = _segment_probe_summary(detailed, segment_meta)
        flat_probe_rows.extend(detailed["token_rows"])
        # Token rows are written to CSV and intentionally omitted from the compact
        # JSON probe object to keep the report human-inspectable.
        compact = {k: v for k, v in detailed.items() if k != "token_rows"}
        probe_results.append(compact)

    cohort_t0s = [
        int(row["first_tradable_observed_at_us"])
        for row in token_rows
        if isinstance(row.get("first_tradable_observed_at_us"), int)
    ]

    payload: dict[str, object] = {
        "schema_version": LARGE_COHORT_SCHEMA_VERSION,
        "research_scope": "LARGE_COHORT_SETUP_CHARACTERIZATION_ONLY",
        "performance_pnl_included": False,
        "future_return_label_included": False,
        "execution_model_used": False,
        "risk_manager_used": False,
        "entry_window_ms": DEFAULT_ENTRY_WINDOW_MS,
        "cohort": {
            "tokens": len(read_result.cohort.mints),
            "selection_rule": read_result.cohort.selection_rule,
            "start_t0_observed_at_us": min(cohort_t0s) if cohort_t0s else None,
            "end_t0_observed_at_us": max(cohort_t0s) if cohort_t0s else None,
            "time_segment_count": segment_count,
            "time_segments": segment_meta,
        },
        "source_rows": read_result.source_rows,
        "normalized_events": len(read_result.events),
        "adapter_skipped": dict(sorted(read_result.skipped.items())),
        "raw_market_summary": raw_summary,
        "probes": probe_results,
    }
    payload["deterministic_payload_sha256"] = stable_sha256(payload)
    return payload, token_rows, flat_probe_rows
