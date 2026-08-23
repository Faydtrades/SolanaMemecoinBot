from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass
from decimal import Decimal
from hashlib import sha256
import json
import math
import sqlite3
from typing import Iterable, Sequence

from .feature_engine_v0_2 import FeatureEngineV02
from .models_v0_1 import (
    EventType,
    FirstPullbackParameterSet,
    FirstPullbackState,
    IngestionSource,
    NormalizedMarketEvent,
)
from .phase1_readonly_adapter_v0_1 import (
    BOT_TRUTH_SOURCE_PREFIXES,
    Phase1ReadOnlyAdapterV01,
)
from .strategy_first_pullback_v0_1 import FirstPullbackStrategyV01
from .strategy_clock_v0_1 import FirstPullbackStrategyClockV01, StrategyDeadline


RESEARCH_SCHEMA_VERSION = "P2RR-0.3"
DEFAULT_ENTRY_WINDOW_MS = 300_000


@dataclass(frozen=True, slots=True)
class ResearchCohort:
    mints: tuple[str, ...]
    selection_rule: str


@dataclass(frozen=True, slots=True)
class ResearchReadResult:
    cohort: ResearchCohort
    source_rows: int
    events: tuple[NormalizedMarketEvent, ...]
    skipped: dict[str, int]


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


def _source_where_sql() -> tuple[str, tuple[str, ...]]:
    where_source = "(" + " OR ".join(
        "source_decoded_file LIKE ?" for _ in BOT_TRUTH_SOURCE_PREFIXES
    ) + ")"
    args = tuple(prefix + "%" for prefix in BOT_TRUTH_SOURCE_PREFIXES)
    return where_source, args


def select_chronological_cohort(
    conn: sqlite3.Connection,
    *,
    token_limit: int = 100,
) -> ResearchCohort:
    """Select a deterministic, activity-unfiltered BOT_TRUTH cohort.

    Eligibility requires only one observed tradable BUY/SELL so t=0 exists.  We do
    NOT require a later trade count, candidate, survival, volume, price move, or any
    future outcome.  A positive token_limit chooses the most recently *started*
    eligible tokens by their first Phase-1 rowid.  token_limit=0 selects all.
    """
    if token_limit < 0:
        raise ValueError("token_limit must be >= 0")

    where_source, source_args = _source_where_sql()
    limit_sql = "" if token_limit == 0 else "LIMIT ?"
    args: tuple[object, ...] = (*source_args,)
    if token_limit != 0:
        args = (*args, token_limit)

    rows = conn.execute(
        f"""
        SELECT mint, MIN(rowid) AS first_trade_rowid
        FROM pump_events
        WHERE mint IS NOT NULL
          AND event_type IN ('BUY', 'SELL')
          AND pump_timestamp IS NOT NULL
          AND decoded_at_utc IS NOT NULL
          AND slot IS NOT NULL
          AND {where_source}
        GROUP BY mint
        ORDER BY first_trade_rowid DESC, mint ASC
        {limit_sql}
        """,
        args,
    ).fetchall()
    mints = tuple(str(row["mint"]) for row in rows)
    rule = (
        "BOT_TRUTH mints with >=1 BUY/SELL; newest by first Phase-1 tradable rowid; "
        "no minimum future activity/outcome filter"
    )
    return ResearchCohort(mints=mints, selection_rule=rule)


def fetch_research_events(
    conn: sqlite3.Connection,
    *,
    token_limit: int = 100,
) -> ResearchReadResult:
    Phase1ReadOnlyAdapterV01.validate_schema(conn)
    cohort = select_chronological_cohort(conn, token_limit=token_limit)
    if not cohort.mints:
        return ResearchReadResult(cohort, 0, (), {})

    where_source, source_args = _source_where_sql()
    all_rows: list[sqlite3.Row] = []

    # Query per mint so this works even if future cohorts exceed SQLite's variable
    # limit.  There is intentionally no events-per-token cap: a cap could truncate a
    # busy token before its material First Pullback lifecycle outcome.
    for mint in cohort.mints:
        rows = conn.execute(
            f"""
            SELECT rowid AS p1_rowid, *
            FROM pump_events
            WHERE mint = ?
              AND event_type IN ('LAUNCH', 'BUY', 'SELL')
              AND pump_timestamp IS NOT NULL
              AND decoded_at_utc IS NOT NULL
              AND slot IS NOT NULL
              AND {where_source}
            ORDER BY rowid ASC
            """,
            (mint, *source_args),
        ).fetchall()
        all_rows.extend(rows)

    events, skipped = Phase1ReadOnlyAdapterV01.normalize_rows(all_rows)
    events.sort(key=lambda e: (e.observed_at_us, e.ingest_seq))
    return ResearchReadResult(
        cohort=cohort,
        source_rows=len(all_rows),
        events=tuple(events),
        skipped=skipped,
    )


def group_events_by_mint(
    events: Iterable[NormalizedMarketEvent],
) -> dict[str, list[NormalizedMarketEvent]]:
    grouped: dict[str, list[NormalizedMarketEvent]] = {}
    for event in events:
        grouped.setdefault(event.mint, []).append(event)
    for token_events in grouped.values():
        token_events.sort(key=lambda e: (e.observed_at_us, e.ingest_seq))
    return grouped


def _nearest_rank(values: Sequence[int], percentile: int) -> int | None:
    if not values:
        return None
    if percentile < 0 or percentile > 100:
        raise ValueError("percentile must be in [0, 100]")
    ordered = sorted(values)
    if percentile == 0:
        return ordered[0]
    rank = math.ceil((percentile / 100) * len(ordered))
    return ordered[max(0, min(len(ordered) - 1, rank - 1))]


def summarize_int_metric(values: Iterable[int | None]) -> dict[str, int | None]:
    clean = [int(v) for v in values if v is not None]
    if not clean:
        return {
            "n": 0,
            "min": None,
            "p10": None,
            "p25": None,
            "p50": None,
            "p75": None,
            "p90": None,
            "p95": None,
            "max": None,
        }
    return {
        "n": len(clean),
        "min": min(clean),
        "p10": _nearest_rank(clean, 10),
        "p25": _nearest_rank(clean, 25),
        "p50": _nearest_rank(clean, 50),
        "p75": _nearest_rank(clean, 75),
        "p90": _nearest_rank(clean, 90),
        "p95": _nearest_rank(clean, 95),
        "max": max(clean),
    }


def build_raw_token_metrics(
    grouped: dict[str, list[NormalizedMarketEvent]],
    cohort_mints: Sequence[str],
    *,
    entry_window_ms: int = DEFAULT_ENTRY_WINDOW_MS,
) -> list[dict[str, object]]:
    if entry_window_ms <= 0:
        raise ValueError("entry_window_ms must be > 0")

    rows: list[dict[str, object]] = []
    for mint in cohort_mints:
        token_events = grouped.get(mint, [])
        engine = FeatureEngineV02()
        first_tradable = next(
            (e for e in token_events if e.event_type in (EventType.BUY, EventType.SELL)),
            None,
        )

        row: dict[str, object] = {
            "mint": mint,
            "events_available": len(token_events),
            "states_in_entry_window": 0,
            "has_observation_after_entry_window": False,
            "gap_event_count": sum(
                1 for e in token_events if e.source == IngestionSource.GAP_RECOVERY
            ),
            "first_trade_pump_timestamp_delta_ms": (
                max(0, (first_tradable.observed_at_us - first_tradable.event_at_us) // 1_000)
                if first_tradable is not None
                else None
            ),
            "max_return_from_t0_bps": None,
            "max_drawdown_depth_bps": None,
            "max_trades_since_t0": None,
            "max_unique_buyers_since_t0": None,
            "max_total_volume_since_t0_lamports": None,
            "max_3s_buys": None,
            "max_3s_sells": None,
            "max_3s_net_flow_lamports": None,
            "max_3s_price_change_bps": None,
            "max_10s_trades": None,
            "max_10s_volume_lamports": None,
        }

        for event in token_events:
            state = engine.process(event)
            age_ms = state.identity.age_ms
            if age_ms is None:
                continue
            if age_ms > entry_window_ms:
                row["has_observation_after_entry_window"] = True
                continue

            row["states_in_entry_window"] = int(row["states_in_entry_window"]) + 1
            since = state.windows["since_t0"]
            w3 = state.windows["3s"]
            w10 = state.windows["10s"]
            ret = state.price_structure.return_from_t0_bps
            dd = state.price_structure.drawdown_from_high_bps
            depth = -dd if dd is not None and dd < 0 else 0

            def take_max(name: str, value: int | None) -> None:
                if value is None:
                    return
                old = row[name]
                row[name] = value if old is None else max(int(old), value)

            take_max("max_return_from_t0_bps", ret)
            take_max("max_drawdown_depth_bps", depth)
            take_max("max_trades_since_t0", since.trade_count)
            take_max("max_unique_buyers_since_t0", since.unique_buyers)
            take_max("max_total_volume_since_t0_lamports", since.total_volume_lamports)
            take_max("max_3s_buys", w3.buys)
            take_max("max_3s_sells", w3.sells)
            take_max("max_3s_net_flow_lamports", w3.net_flow_lamports)
            take_max("max_3s_price_change_bps", w3.price_change_bps)
            take_max("max_10s_trades", w10.trade_count)
            take_max("max_10s_volume_lamports", w10.total_volume_lamports)

        rows.append(row)
    return rows


def summarize_raw_metrics(token_rows: Sequence[dict[str, object]]) -> dict[str, object]:
    metric_names = (
        "events_available",
        "states_in_entry_window",
        "first_trade_pump_timestamp_delta_ms",
        "max_return_from_t0_bps",
        "max_drawdown_depth_bps",
        "max_trades_since_t0",
        "max_unique_buyers_since_t0",
        "max_total_volume_since_t0_lamports",
        "max_3s_buys",
        "max_3s_sells",
        "max_3s_net_flow_lamports",
        "max_3s_price_change_bps",
        "max_10s_trades",
        "max_10s_volume_lamports",
    )
    result: dict[str, object] = {
        "tokens": len(token_rows),
        "tokens_with_gap_events": sum(1 for r in token_rows if int(r["gap_event_count"]) > 0),
        "tokens_with_post_window_observation": sum(
            1 for r in token_rows if bool(r["has_observation_after_entry_window"])
        ),
        "tokens_with_no_observable_state_inside_entry_window": sum(
            1 for r in token_rows if int(r["states_in_entry_window"]) == 0
        ),
        "quantile_method": "nearest-rank",
        "metrics": {},
    }
    metrics = result["metrics"]
    assert isinstance(metrics, dict)
    for name in metric_names:
        metrics[name] = summarize_int_metric(
            r.get(name) if isinstance(r.get(name), int) else None for r in token_rows
        )
    return result


def run_strategy_probe(
    grouped: dict[str, list[NormalizedMarketEvent]],
    cohort_mints: Sequence[str],
    params: FirstPullbackParameterSet,
) -> dict[str, object]:
    """Run First Pullback probe with a deterministic independent strategy clock.

    The replay merges two causal streams:
      1) observed market events, and
      2) the scheduled entry-window deadline.

    If market data goes silent, virtual time still advances to the deadline and the
    active lifecycle becomes EXPIRED.  A market event outside the entry window can
    never be used to create a signal before expiry because the clock fires first.
    """
    FirstPullbackStrategyV01.validate_parameters(params)
    reached_counts: Counter[str] = Counter()
    final_counts: Counter[str] = Counter()
    last_reason_counts: Counter[str] = Counter()
    stage_age_ms: dict[str, list[int]] = {
        "IMPULSE_CONFIRMED": [],
        "PULLBACK_ACTIVE": [],
        "WAITING_FOR_RECLAIM": [],
        "CANDIDATE_SIGNAL": [],
    }
    clock_expiry_ages_us: list[int] = []
    token_results: list[dict[str, object]] = []

    for mint in cohort_mints:
        token_events = grouped.get(mint, [])
        engine = FeatureEngineV02()
        strategy = FirstPullbackStrategyV01()
        clock = FirstPullbackStrategyClockV01()
        run = None
        deadline: StrategyDeadline | None = None
        reached: set[str] = set()
        transition_sequence: list[str] = []
        last_reason: str | None = None
        last_age_ms: int | None = None
        candidate = False
        clock_expired = False

        def record_evaluation(evaluation, age_ms: int | None) -> None:
            nonlocal last_reason, last_age_ms, candidate, clock_expired
            last_reason = evaluation.reason_code
            last_age_ms = age_ms
            if evaluation.transition_occurred:
                state_name = evaluation.current_state.value
                reached.add(state_name)
                transition_sequence.append(state_name)
                if state_name in stage_age_ms and age_ms is not None:
                    stage_age_ms[state_name].append(int(age_ms))
                if state_name == FirstPullbackState.EXPIRED.value:
                    clock_expired = evaluation.audit_snapshot.get("clock_event") == "ENTRY_WINDOW_DEADLINE"
            if evaluation.candidate_signal is not None:
                candidate = True

        for event in token_events:
            # Deadline wins over any market observation strictly outside the entry
            # window.  This is the key live/replay timing invariant.
            if run is not None and deadline is not None:
                clock_eval = clock.fire_if_due(
                    deadline, run, params, now_us=event.observed_at_us
                )
                if clock_eval is not None:
                    record_evaluation(clock_eval, params.max_entry_age_ms)
                    clock_expiry_ages_us.append(
                        clock_eval.evaluated_at_us - deadline.first_tradable_observed_at_us
                    )
                    break

            state = engine.process(event)
            if run is None:
                run = strategy.start_run(state, params)
                reached.add(FirstPullbackState.DISCOVERED.value)

            if deadline is None:
                deadline = clock.schedule_entry_expiry(state, run, params)

            if run.finished or run.current_state == FirstPullbackState.CANDIDATE_SIGNAL:
                break

            evaluation = strategy.evaluate(state, run, params)
            record_evaluation(evaluation, state.identity.age_ms)
            if candidate:
                break

        # Market silence must not leave an active strategy run open forever.  Replay
        # advances virtual time to the known deadline even if no later observation exists.
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
                record_evaluation(clock_eval, params.max_entry_age_ms)
                clock_expiry_ages_us.append(
                    clock_eval.evaluated_at_us - deadline.first_tradable_observed_at_us
                )

        if run is None:
            # Cohort selection guarantees a trade, so this is defensive only.
            final = "NO_EVENTS"
        elif candidate or run.current_state == FirstPullbackState.CANDIDATE_SIGNAL:
            final = FirstPullbackState.CANDIDATE_SIGNAL.value
        elif run.final_outcome is not None:
            final = run.final_outcome.value
        else:
            final = "OPEN_AT_DATA_END"

        for state_name in reached:
            reached_counts[state_name] += 1
        final_counts[final] += 1
        if last_reason is not None:
            last_reason_counts[last_reason] += 1

        token_results.append(
            {
                "mint": mint,
                "reached_states": sorted(reached),
                "transition_sequence": transition_sequence,
                "final_classification": final,
                "last_reason": last_reason,
                "last_age_ms": last_age_ms,
                "clock_deadline_at_us": deadline.expire_at_us if deadline else None,
                "clock_expired": clock_expired,
            }
        )

    n = len(cohort_mints)

    def count(state_name: str) -> int:
        return int(reached_counts.get(state_name, 0))

    funnel = {
        "eligible_tokens": n,
        "waiting_for_impulse": count(FirstPullbackState.WAITING_FOR_IMPULSE.value),
        "impulse_confirmed": count(FirstPullbackState.IMPULSE_CONFIRMED.value),
        "pullback_active": count(FirstPullbackState.PULLBACK_ACTIVE.value),
        "buyer_response_confirmed": count(FirstPullbackState.WAITING_FOR_RECLAIM.value),
        "candidate_signal": count(FirstPullbackState.CANDIDATE_SIGNAL.value),
        "invalidated": int(final_counts.get(FirstPullbackState.INVALIDATED.value, 0)),
        "expired": int(final_counts.get(FirstPullbackState.EXPIRED.value, 0)),
        "rejected": int(final_counts.get(FirstPullbackState.REJECT.value, 0)),
        "open_at_data_end": int(final_counts.get("OPEN_AT_DATA_END", 0)),
        "clock_expired": sum(1 for row in token_results if row["clock_expired"]),
    }

    return {
        "parameter_set": parameter_set_dict(params),
        "clock": {
            "schema_version": FirstPullbackStrategyClockV01.schema_version,
            "boundary_semantics": "0..max_entry_age_ms inclusive; expire at +1us",
            "clock_expiry_offset_us": summarize_int_metric(clock_expiry_ages_us),
        },
        "funnel": funnel,
        "reached_state_counts": dict(sorted(reached_counts.items())),
        "final_classification_counts": dict(sorted(final_counts.items())),
        "last_reason_counts": dict(sorted(last_reason_counts.items())),
        "stage_age_ms": {
            key: summarize_int_metric(values) for key, values in stage_age_ms.items()
        },
        "token_results": token_results,
    }


def build_research_payload(
    *,
    read_result: ResearchReadResult,
    token_rows: Sequence[dict[str, object]],
    probes: Sequence[FirstPullbackParameterSet],
) -> dict[str, object]:
    grouped = group_events_by_mint(read_result.events)
    probe_results = [
        run_strategy_probe(grouped, read_result.cohort.mints, params) for params in probes
    ]
    payload: dict[str, object] = {
        "schema_version": RESEARCH_SCHEMA_VERSION,
        "research_scope": "SETUP_FUNNEL_AND_RAW_DISTRIBUTIONS_ONLY",
        "performance_pnl_included": False,
        "execution_model_used": False,
        "cohort": {
            "tokens": len(read_result.cohort.mints),
            "selection_rule": read_result.cohort.selection_rule,
            "mints": list(read_result.cohort.mints),
        },
        "source_rows": read_result.source_rows,
        "normalized_events": len(read_result.events),
        "adapter_skipped": dict(sorted(read_result.skipped.items())),
        "entry_window_ms": DEFAULT_ENTRY_WINDOW_MS,
        "raw_market_summary": summarize_raw_metrics(token_rows),
        "probes": probe_results,
    }
    payload["deterministic_payload_sha256"] = stable_sha256(payload)
    return payload
