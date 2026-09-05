from __future__ import annotations

import csv
import hashlib
import json
import math
import sqlite3
from collections import Counter, defaultdict
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Iterable, Sequence

from .paper_cost_baseline_v0_1 import P4_COST_BASELINE_0001
from .post24h_counterfactual_replay_v0_1 import (
    EXPECTED_T001_REPLAY_SHA256,
    MODEL_FINGERPRINT as T002A_MODEL_FINGERPRINT,
    MODEL_ID as T002A_MODEL_ID,
    CounterfactualExitPolicyV01,
    PolicyFamily,
    TimeoutOrigin,
    replay_counterfactual_v0_1,
    run_frozen_baseline_gate_v0_1,
    _return_bps,
)
from .post24h_frozen_replay_v0_1 import (
    EXPECTED_COHORT_SHA256,
    EXPECTED_PHYSICAL_ENTRIES,
    EXPECTED_PAPER_SHA256,
    FROZEN_WATERMARK,
    MODEL_FINGERPRINT as T001_MODEL_FINGERPRINT,
    MODEL_ID as T001_MODEL_ID,
    SOURCE_ANCHOR,
    FrozenEntry,
    ReplayMismatch,
    canonical_sha256,
    load_frozen_entries,
    load_frozen_market_paths,
    sha256_file,
)
from .runtime_exit_price_impact_v0_1 import (
    MODEL_FINGERPRINT as EXIT_IMPACT_FINGERPRINT,
)
from .runtime_exit_price_impact_v0_1 import MODEL_ID as EXIT_IMPACT_MODEL_ID


MODEL_ID = "P4-POST24H-OUTCOME-ANALYSIS-0001"
SCHEMA_VERSION = "phase4_post24h_outcome_analysis_v0.1"
ANALYSIS_ID = "POST-24H-ANALYSIS-001"
EXPECTED_HEAD = "2db33689d6ea4b506189f1654f6191b3df0f584c"
EXPECTED_T002A_DETERMINISTIC_SHA256 = (
    "48d20d23b1249bf8d743efd6847ddc4f66ef2c6a5251f0fadbc08542f47594e6"
)
EXPECTED_SOURCE_SHA256 = (
    "6d47437b92ab1083bec7153206700026860209cd6e7c3a7186067346af32d5ba"
)
EXPECTED_T001_CSV_SHA256 = (
    "fd67aa92bf44a8e6fdbcdcbc75e87910cc9fcaf9d2eed189042ada1caeef2e07"
)

ENTRY_HORIZONS_SECONDS = (5, 10, 15, 20, 30, 45, 60, 90, 120, 180, 300)
POST_EXIT_HORIZONS_SECONDS = (5, 10, 15, 30, 45, 60)
SOLD_TOO_EARLY_THRESHOLDS_BPS = (1_000, 1_500, 2_000, 2_500, 3_000, 4_000, 5_000)
TIMEOUT_SECONDS = (15, 20, 30, 45, 60, 90, 120)
MATRIX_TIMEOUT_SECONDS = (15, 30, 60, 120)
TRAIL_TIMEOUT_SECONDS = (15, 30, 60)
TP_PERCENT = (10, 15, 20, 25, 30, 40, 50)
SL_PERCENT = (10, 15, 20, 25, 30, 40)
TRAIL_ACTIVATION_PERCENT = (10, 15, 20, 25)
TRAIL_GIVEBACK_PERCENT = (3, 5, 7, 10)
PERCENTILE_METHOD = "NEAREST_RANK_CEILING_1_INDEXED"
SMALL_CELL_N = 50

OUTPUT_FILENAMES = (
    "entry_cohort.csv",
    "entry_cohort.json",
    "entry_aligned_horizons.csv",
    "entry_aligned_horizon_summary.csv",
    "entry_mfe_mae.csv",
    "entry_mfe_mae_summary.csv",
    "post_actual_exit_outcomes.csv",
    "sold_too_early_summary.csv",
    "actual_baseline_summary.csv",
    "replay_policy_registry.csv",
    "replay_trade_results.csv",
    "replay_policy_summary.csv",
    "baseline_vs_replay.csv",
    "segment_summary.csv",
    "candidate_shortlist.csv",
    "analysis_summary.json",
    "POST-24H-ANALYSIS-001_REPORT.md",
    "POST24H_T002B_manifest.json",
)

SPEC = {
    "model_id": MODEL_ID,
    "schema_version": SCHEMA_VERSION,
    "analysis_id": ANALYSIS_ID,
    "accepted_t001_model_fingerprint": T001_MODEL_FINGERPRINT,
    "accepted_t002a_model_fingerprint": T002A_MODEL_FINGERPRINT,
    "cohort_sha256": EXPECTED_COHORT_SHA256,
    "entry_horizons_seconds": ENTRY_HORIZONS_SECONDS,
    "post_exit_horizons_seconds": POST_EXIT_HORIZONS_SECONDS,
    "sold_too_early_thresholds_bps": SOLD_TOO_EARLY_THRESHOLDS_BPS,
    "policy_arrays": {
        "timeouts_seconds": TIMEOUT_SECONDS,
        "matrix_timeouts_seconds": MATRIX_TIMEOUT_SECONDS,
        "trail_timeouts_seconds": TRAIL_TIMEOUT_SECONDS,
        "tp_percent": TP_PERCENT,
        "sl_percent": SL_PERCENT,
        "trail_activation_percent": TRAIL_ACTIVATION_PERCENT,
        "trail_giveback_percent": TRAIL_GIVEBACK_PERCENT,
    },
    "new_policy_timeout_origin": TimeoutOrigin.ENTRY_FILL_CLOCK.value,
    "threshold_reference": "FROZEN_PHASE4_RULE_REFERENCE",
    "percentile_method": PERCENTILE_METHOD,
    "small_cell_n": SMALL_CELL_N,
    "shortlist_max": 10,
    "shortlist_fill_rate_min": "0.900000000000",
    "equity_label": "REALIZED_CLOSED_EQUITY",
}
MODEL_FINGERPRINT = canonical_sha256(SPEC)


def _dt(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ReplayMismatch(f"naive timestamp: {value}")
    return parsed.astimezone(timezone.utc)


def _iso(value: datetime | None) -> str | None:
    return None if value is None else value.isoformat(timespec="microseconds")


def _ratio(numerator: int, denominator: int, places: int = 12) -> str | None:
    if denominator == 0:
        return None
    quantum = Decimal(1).scaleb(-places)
    return str(
        (Decimal(numerator) / Decimal(denominator)).quantize(
            quantum, rounding=ROUND_HALF_UP
        )
    )


def _mean(values: Sequence[int], places: int = 6) -> str | None:
    return None if not values else _ratio(sum(values), len(values), places)


def nearest_rank_v0_1(values: Sequence[int], percentile: int) -> int | None:
    """Nearest-rank percentile: sorted[ceil(p/100*N)-1]."""
    if not 0 < percentile <= 100:
        raise ValueError("percentile must be in (0, 100]")
    if not values:
        return None
    ordered = sorted(int(value) for value in values)
    rank = max(1, math.ceil(percentile * len(ordered) / 100))
    return ordered[rank - 1]


def _ms_text(microseconds: int | None) -> str | None:
    if microseconds is None:
        return None
    return str(
        (Decimal(microseconds) / Decimal(1_000)).quantize(
            Decimal("0.001"), rounding=ROUND_HALF_UP
        )
    )


def build_policy_registry_v0_1() -> tuple[CounterfactualExitPolicyV01, ...]:
    policies: list[CounterfactualExitPolicyV01] = []
    origin = TimeoutOrigin.ENTRY_FILL_CLOCK
    for timeout in TIMEOUT_SECONDS:
        policies.append(
            CounterfactualExitPolicyV01(
                policy_id=f"TIME_T{timeout}S",
                timeout_ms=timeout * 1_000,
                timeout_origin=origin,
            )
        )
    for tp in TP_PERCENT:
        for timeout in MATRIX_TIMEOUT_SECONDS:
            policies.append(
                CounterfactualExitPolicyV01(
                    policy_id=f"TP{tp}_T{timeout}S",
                    timeout_ms=timeout * 1_000,
                    timeout_origin=origin,
                    tp_bps=tp * 100,
                )
            )
    for sl in SL_PERCENT:
        for timeout in MATRIX_TIMEOUT_SECONDS:
            policies.append(
                CounterfactualExitPolicyV01(
                    policy_id=f"SL{sl}_T{timeout}S",
                    timeout_ms=timeout * 1_000,
                    timeout_origin=origin,
                    sl_bps=sl * 100,
                )
            )
    for tp in TP_PERCENT:
        for sl in SL_PERCENT:
            for timeout in MATRIX_TIMEOUT_SECONDS:
                policies.append(
                    CounterfactualExitPolicyV01(
                        policy_id=f"TP{tp}_SL{sl}_T{timeout}S",
                        timeout_ms=timeout * 1_000,
                        timeout_origin=origin,
                        tp_bps=tp * 100,
                        sl_bps=sl * 100,
                    )
                )
    for activation in TRAIL_ACTIVATION_PERCENT:
        for giveback in TRAIL_GIVEBACK_PERCENT:
            for timeout in TRAIL_TIMEOUT_SECONDS:
                policies.append(
                    CounterfactualExitPolicyV01(
                        policy_id=f"TRAIL_A{activation}_G{giveback}_T{timeout}S",
                        timeout_ms=timeout * 1_000,
                        timeout_origin=origin,
                        trailing_activation_bps=activation * 100,
                        trailing_giveback_bps=giveback * 100,
                    )
                )
    counts = Counter(policy.family.value for policy in policies)
    expected = {
        PolicyFamily.TIMEOUT_ONLY.value: 7,
        PolicyFamily.TP_TIMEOUT.value: 28,
        PolicyFamily.SL_TIMEOUT.value: 24,
        PolicyFamily.TP_SL_TIMEOUT.value: 168,
        PolicyFamily.TRAILING_TIMEOUT.value: 48,
    }
    if counts != expected or len(policies) != 275:
        raise ReplayMismatch(f"policy registry mismatch: {counts}, total={len(policies)}")
    if len({policy.policy_id for policy in policies}) != len(policies):
        raise ReplayMismatch("policy IDs are not unique")
    if any(policy.timeout_origin is not origin for policy in policies):
        raise ReplayMismatch("research policy has non-entry-fill timeout origin")
    return tuple(policies)


def policy_registry_rows_v0_1(
    policies: Sequence[CounterfactualExitPolicyV01],
) -> list[dict[str, Any]]:
    return [
        {
            "policy_id": policy.policy_id,
            "policy_fingerprint": policy.fingerprint,
            "family": policy.family.value,
            "timeout_origin": policy.timeout_origin.value,
            "timeout_ms": policy.timeout_ms,
            "timeout_seconds": policy.timeout_ms // 1_000,
            "tp_bps": policy.tp_bps,
            "sl_bps": policy.sl_bps,
            "trailing_activation_bps": policy.trailing_activation_bps,
            "trailing_giveback_bps": policy.trailing_giveback_bps,
        }
        for policy in policies
    ]


def _fresh_eligible(
    entry: FrozenEntry,
    path: Sequence[Any],
    target_at: datetime,
) -> list[Any]:
    entry_key = (_dt(entry.entry_observed_at), int(entry.entry_ingest_seq))
    result: list[Any] = []
    for observation in path:
        key = (observation.observed_at, int(observation.ingest_seq))
        if key <= entry_key:
            continue
        if observation.observed_at > target_at:
            break
        if observation.ingest_seq > FROZEN_WATERMARK:
            raise ReplayMismatch("outcome mark crossed frozen watermark")
        if observation.mint != entry.mint:
            continue
        if observation.is_gap_recovery:
            continue
        if observation.price_identity != entry.reference_price_identity:
            continue
        result.append(observation)
    return result


def last_causal_mark_v0_1(
    entry: FrozenEntry,
    path: Sequence[Any],
    target_at: datetime,
) -> tuple[Any | None, int]:
    eligible = _fresh_eligible(entry, path, target_at)
    return (None if not eligible else eligible[-1], len(eligible))


def entry_outcome_studies_v0_1(
    entries: Sequence[FrozenEntry],
    paths: dict[str, tuple[Any, ...]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    horizon_rows: list[dict[str, Any]] = []
    mfe_rows: list[dict[str, Any]] = []
    by_horizon_marks: dict[int, list[dict[str, Any]]] = defaultdict(list)
    by_horizon_mfe: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for entry in entries:
        entry_at = _dt(entry.entry_observed_at)
        path = paths.get(entry.mint, ())
        for horizon in ENTRY_HORIZONS_SECONDS:
            target = entry_at + timedelta(seconds=horizon)
            eligible = _fresh_eligible(entry, path, target)
            mark = None if not eligible else eligible[-1]
            mark_return = (
                None
                if mark is None
                else _return_bps(
                    entry.reference_price_numerator_raw,
                    entry.reference_price_denominator_raw,
                    mark.price_numerator_raw,
                    mark.price_denominator_raw,
                )
            )
            age_us = (
                None
                if mark is None
                else int((target - mark.observed_at).total_seconds() * 1_000_000)
            )
            horizon_row = {
                "signal_key": entry.signal_key,
                "mint": entry.mint,
                "horizon_seconds": horizon,
                "target_at": _iso(target),
                "mark_status": "MISSING" if mark is None else "AVAILABLE",
                "mark_observed_at": None if mark is None else _iso(mark.observed_at),
                "mark_ingest_seq": None if mark is None else mark.ingest_seq,
                "mark_source_event_key": None if mark is None else f"{mark.event_key}:MARKET",
                "mark_age_ms": _ms_text(age_us),
                "rule_return_bps": mark_return,
                "observations_seen_through_horizon": len(eligible),
            }
            horizon_rows.append(horizon_row)
            by_horizon_marks[horizon].append({**horizon_row, "_age_us": age_us})

            returns = [
                _return_bps(
                    entry.reference_price_numerator_raw,
                    entry.reference_price_denominator_raw,
                    observation.price_numerator_raw,
                    observation.price_denominator_raw,
                )
                for observation in eligible
            ]
            mfe_row = {
                "signal_key": entry.signal_key,
                "mint": entry.mint,
                "horizon_seconds": horizon,
                "target_at": _iso(target),
                "status": "MISSING" if not returns else "AVAILABLE",
                "mfe_bps": None if not returns else max(returns),
                "mae_bps": None if not returns else min(returns),
                "observation_count": len(returns),
            }
            mfe_rows.append(mfe_row)
            by_horizon_mfe[horizon].append(mfe_row)

    horizon_summary: list[dict[str, Any]] = []
    mfe_summary: list[dict[str, Any]] = []
    for horizon in ENTRY_HORIZONS_SECONDS:
        marks = by_horizon_marks[horizon]
        available = [row for row in marks if row["mark_status"] == "AVAILABLE"]
        returns = [int(row["rule_return_bps"]) for row in available]
        ages = [int(row["_age_us"]) for row in available]
        horizon_summary.append(
            {
                "horizon_seconds": horizon,
                "cohort_n": len(marks),
                "available_n": len(available),
                "missing_n": len(marks) - len(available),
                "availability_rate": _ratio(len(available), len(marks)),
                "return_mean_bps": _mean(returns),
                **{f"return_p{p}_bps": nearest_rank_v0_1(returns, p) for p in (10, 25, 50, 75, 90)},
                "mark_age_p50_ms": _ms_text(nearest_rank_v0_1(ages, 50)),
                "mark_age_p90_ms": _ms_text(nearest_rank_v0_1(ages, 90)),
                "percentile_method": PERCENTILE_METHOD,
                "coverage_label": "RESEARCH_CAUSAL_MARK_AVAILABILITY_NOT_PHASE4_COVERAGE",
            }
        )
        values = by_horizon_mfe[horizon]
        available_mfe = [row for row in values if row["status"] == "AVAILABLE"]
        mfe_values = [int(row["mfe_bps"]) for row in available_mfe]
        mae_values = [int(row["mae_bps"]) for row in available_mfe]
        observation_counts = [int(row["observation_count"]) for row in available_mfe]
        mfe_summary.append(
            {
                "horizon_seconds": horizon,
                "cohort_n": len(values),
                "available_n": len(available_mfe),
                "missing_n": len(values) - len(available_mfe),
                "availability_rate": _ratio(len(available_mfe), len(values)),
                "mfe_mean_bps": _mean(mfe_values),
                **{f"mfe_p{p}_bps": nearest_rank_v0_1(mfe_values, p) for p in (10, 25, 50, 75, 90)},
                "mae_mean_bps": _mean(mae_values),
                **{f"mae_p{p}_bps": nearest_rank_v0_1(mae_values, p) for p in (10, 25, 50, 75, 90)},
                "observation_count_mean": _mean(observation_counts),
                "observation_count_p50": nearest_rank_v0_1(observation_counts, 50),
                "observation_count_p90": nearest_rank_v0_1(observation_counts, 90),
                "percentile_method": PERCENTILE_METHOD,
            }
        )
    return horizon_rows, horizon_summary, mfe_rows, mfe_summary


def _open_readonly(path: str | Path) -> sqlite3.Connection:
    absolute = Path(path).resolve()
    conn = sqlite3.connect(f"file:{absolute.as_posix()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    if conn.execute("PRAGMA query_only").fetchone()[0] != 1:
        conn.close()
        raise ReplayMismatch(f"query_only unavailable for {absolute}")
    return conn


def load_entry_cohort_rows_v0_1(
    paper_db: str | Path,
    entries: Sequence[FrozenEntry],
) -> list[dict[str, Any]]:
    conn = _open_readonly(paper_db)
    try:
        features = {
            row["candidate_id"]: dict(row)
            for row in conn.execute(
                "SELECT candidate_id,price_impact_bps,adverse_slippage_bps "
                "FROM paper_entry_routes WHERE state='FILLED'"
            )
        }
    finally:
        conn.close()
    rows: list[dict[str, Any]] = []
    for entry in entries:
        feature = features.get(entry.signal_key)
        if feature is None:
            raise ReplayMismatch(f"missing entry-route features for {entry.signal_key}")
        row = asdict(entry)
        row["entry_price_impact_bps"] = int(feature["price_impact_bps"])
        row["entry_adverse_slippage_bps"] = int(feature["adverse_slippage_bps"])
        rows.append(row)
    if len(rows) != EXPECTED_PHYSICAL_ENTRIES:
        raise ReplayMismatch(f"entry cohort row count mismatch: {len(rows)}")
    return rows


def load_actual_filled_exits_v0_1(
    paper_db: str | Path,
    paths: dict[str, tuple[Any, ...]],
) -> list[dict[str, Any]]:
    conn = _open_readonly(paper_db)
    try:
        source_rows = conn.execute(
            """
            SELECT paper_position_id,signal_key,mint,track_id,exit_variant,
                   exit_reason,price_identity,selected_market_observed_at,
                   selected_market_ingest_seq,selected_source_event_key,
                   simulated_exit_price_numerator_raw,
                   simulated_exit_price_denominator_raw
            FROM paper_exit_execution_routes
            WHERE state='FILLED'
            ORDER BY track_id,signal_key,paper_position_id
            """
        ).fetchall()
    finally:
        conn.close()
    by_mint_seq = {
        mint: {int(observation.ingest_seq): observation for observation in path}
        for mint, path in paths.items()
    }
    exits: list[dict[str, Any]] = []
    for row in source_rows:
        ingest_seq = int(row["selected_market_ingest_seq"])
        if not SOURCE_ANCHOR < ingest_seq <= FROZEN_WATERMARK:
            raise ReplayMismatch(f"ACTUAL fill row outside frozen source: {ingest_seq}")
        observation = by_mint_seq.get(row["mint"], {}).get(ingest_seq)
        if observation is None:
            raise ReplayMismatch(f"ACTUAL fill source row missing: {ingest_seq}")
        expected = (
            row["mint"],
            row["price_identity"],
            row["selected_market_observed_at"],
            row["selected_source_event_key"],
        )
        actual = (
            observation.mint,
            observation.price_identity,
            _iso(observation.observed_at),
            f"{observation.event_key}:MARKET",
        )
        if actual != expected or observation.is_gap_recovery:
            raise ReplayMismatch(
                f"ACTUAL fill source lineage mismatch for {row['paper_position_id']}"
            )
        exits.append(
            {
                "paper_position_id": row["paper_position_id"],
                "signal_key": row["signal_key"],
                "mint": row["mint"],
                "track_id": row["track_id"],
                "exit_variant": row["exit_variant"],
                "actual_reason": row["exit_reason"],
                "price_identity": row["price_identity"],
                "fill_observed_at": observation.observed_at,
                "fill_ingest_seq": observation.ingest_seq,
                "fill_source_event_key": f"{observation.event_key}:MARKET",
                "raw_fill_price_numerator_raw": observation.price_numerator_raw,
                "raw_fill_price_denominator_raw": observation.price_denominator_raw,
                "impacted_exit_price_numerator_raw": int(
                    row["simulated_exit_price_numerator_raw"]
                ),
                "impacted_exit_price_denominator_raw": int(
                    row["simulated_exit_price_denominator_raw"]
                ),
            }
        )
    if len(exits) != 1_608:
        raise ReplayMismatch(f"ACTUAL filled exit count mismatch: {len(exits)}")
    return exits


def _post_exit_eligible(
    exit_row: dict[str, Any],
    path: Sequence[Any],
    target_at: datetime,
) -> list[Any]:
    fill_key = (exit_row["fill_observed_at"], int(exit_row["fill_ingest_seq"]))
    result: list[Any] = []
    for observation in path:
        key = (observation.observed_at, int(observation.ingest_seq))
        if key <= fill_key:
            continue
        if observation.observed_at > target_at:
            break
        if observation.ingest_seq > FROZEN_WATERMARK:
            raise ReplayMismatch("post-exit mark crossed frozen watermark")
        if observation.mint != exit_row["mint"]:
            continue
        if observation.is_gap_recovery:
            continue
        if observation.price_identity != exit_row["price_identity"]:
            continue
        result.append(observation)
    return result


def post_actual_exit_studies_v0_1(
    actual_exits: Sequence[dict[str, Any]],
    paths: dict[str, tuple[Any, ...]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    outcome_rows: list[dict[str, Any]] = []
    sold_cells: dict[tuple[str, str, int, int], dict[str, Any]] = {}
    for actual in actual_exits:
        path = paths.get(actual["mint"], ())
        for horizon in POST_EXIT_HORIZONS_SECONDS:
            target = actual["fill_observed_at"] + timedelta(seconds=horizon)
            eligible = _post_exit_eligible(actual, path, target)
            mark = None if not eligible else eligible[-1]
            age_us = (
                None
                if mark is None
                else int((target - mark.observed_at).total_seconds() * 1_000_000)
            )
            outcome_rows.append(
                {
                    "track_id": actual["track_id"],
                    "signal_key": actual["signal_key"],
                    "mint": actual["mint"],
                    "actual_reason": actual["actual_reason"],
                    "actual_fill_observed_at": _iso(actual["fill_observed_at"]),
                    "actual_fill_source_row": actual["fill_ingest_seq"],
                    "actual_fill_source_event_key": actual["fill_source_event_key"],
                    "raw_fill_price_numerator_raw": actual[
                        "raw_fill_price_numerator_raw"
                    ],
                    "raw_fill_price_denominator_raw": actual[
                        "raw_fill_price_denominator_raw"
                    ],
                    "impacted_exit_price_numerator_raw_audit_only": actual[
                        "impacted_exit_price_numerator_raw"
                    ],
                    "impacted_exit_price_denominator_raw_audit_only": actual[
                        "impacted_exit_price_denominator_raw"
                    ],
                    "horizon_seconds": horizon,
                    "target_at": _iso(target),
                    "mark_status": "MISSING" if mark is None else "AVAILABLE",
                    "mark_observed_at": None if mark is None else _iso(mark.observed_at),
                    "mark_ingest_seq": None if mark is None else mark.ingest_seq,
                    "mark_source_event_key": None if mark is None else f"{mark.event_key}:MARKET",
                    "mark_age_ms": _ms_text(age_us),
                    "post_exit_raw_market_return_bps": (
                        None
                        if mark is None
                        else _return_bps(
                            actual["raw_fill_price_numerator_raw"],
                            actual["raw_fill_price_denominator_raw"],
                            mark.price_numerator_raw,
                            mark.price_denominator_raw,
                        )
                    ),
                    "baseline_price_kind": "RAW_SOURCE_MARKET_AT_ACTUAL_FILL_ROW",
                }
            )
            returns = [
                _return_bps(
                    actual["raw_fill_price_numerator_raw"],
                    actual["raw_fill_price_denominator_raw"],
                    observation.price_numerator_raw,
                    observation.price_denominator_raw,
                )
                for observation in eligible
            ]
            for threshold in SOLD_TOO_EARLY_THRESHOLDS_BPS:
                key = (
                    actual["track_id"],
                    actual["actual_reason"],
                    horizon,
                    threshold,
                )
                cell = sold_cells.setdefault(
                    key,
                    {"eligible_n": 0, "reached_n": 0, "times_us": []},
                )
                if eligible:
                    cell["eligible_n"] += 1
                reached_index = next(
                    (index for index, value in enumerate(returns) if value >= threshold),
                    None,
                )
                if reached_index is not None:
                    cell["reached_n"] += 1
                    reached_at = eligible[reached_index].observed_at
                    cell["times_us"].append(
                        int(
                            (reached_at - actual["fill_observed_at"]).total_seconds()
                            * 1_000_000
                        )
                    )
    sold_summary: list[dict[str, Any]] = []
    for key in sorted(sold_cells):
        track, reason, window, threshold = key
        cell = sold_cells[key]
        sold_summary.append(
            {
                "track_id": track,
                "actual_reason": reason,
                "window_seconds": window,
                "threshold_bps": threshold,
                "eligible_n": cell["eligible_n"],
                "reached_n": cell["reached_n"],
                "reached_rate": _ratio(cell["reached_n"], cell["eligible_n"]),
                "median_time_to_threshold_ms": _ms_text(
                    nearest_rank_v0_1(cell["times_us"], 50)
                ),
                "small_cell": cell["eligible_n"] < SMALL_CELL_N,
                "eligibility_definition": "AT_LEAST_ONE_FRESH_CAUSAL_MARK_IN_WINDOW",
            }
        )
    return outcome_rows, sold_summary


TRADE_FIELDS = (
    "policy_id",
    "policy_fingerprint",
    "family",
    "signal_key",
    "mint",
    "reason",
    "requested_at",
    "trigger_source_row",
    "trigger_observed_at",
    "trigger_source_event_key",
    "classification",
    "attempt_count",
    "fill_source_row",
    "fill_observed_at",
    "fill_source_event_key",
    "exit_price_numerator_raw",
    "exit_price_denominator_raw",
    "gross_exit_proceeds_lamports",
    "entry_explicit_cost_lamports",
    "exit_explicit_cost_lamports",
    "gross_execution_pnl_lamports",
    "net_pnl_lamports",
    "holding_time_us",
    "rejection_classification",
    "reference_source",
)


def _trade_row(
    policy: CounterfactualExitPolicyV01,
    entry: FrozenEntry,
    path: tuple[Any, ...],
) -> dict[str, Any]:
    result = replay_counterfactual_v0_1(entry, path, policy)
    intent = result.intent
    execution = result.execution
    return {
        "policy_id": policy.policy_id,
        "policy_fingerprint": policy.fingerprint,
        "family": policy.family.value,
        "signal_key": entry.signal_key,
        "mint": entry.mint,
        "reason": intent.reason.value,
        "requested_at": _iso(intent.requested_at),
        "trigger_source_row": intent.trigger_ingest_seq,
        "trigger_observed_at": _iso(intent.trigger_observed_at),
        "trigger_source_event_key": intent.trigger_source_event_key,
        "classification": result.classification,
        "attempt_count": execution.attempt_count,
        "fill_source_row": execution.fill_source_row,
        "fill_observed_at": _iso(execution.fill_observed_at),
        "fill_source_event_key": execution.fill_source_event_key,
        "exit_price_numerator_raw": execution.exit_price_numerator_raw,
        "exit_price_denominator_raw": execution.exit_price_denominator_raw,
        "gross_exit_proceeds_lamports": execution.gross_exit_proceeds_lamports,
        "entry_explicit_cost_lamports": entry.entry_explicit_cost_lamports,
        "exit_explicit_cost_lamports": execution.exit_explicit_cost_lamports,
        "gross_execution_pnl_lamports": execution.gross_execution_pnl_lamports,
        "net_pnl_lamports": execution.net_pnl_lamports,
        "holding_time_us": execution.holding_time_us,
        "rejection_classification": execution.rejection_state,
        "reference_source": intent.reference_source,
    }


def realized_closed_equity_v0_1(rows: Sequence[dict[str, Any]]) -> dict[str, int]:
    filled = [row for row in rows if row["classification"] == "FILLED"]
    ordered = sorted(
        filled,
        key=lambda row: (
            str(row["fill_observed_at"]),
            int(row["fill_source_row"]),
            str(row["signal_key"]),
        ),
    )
    equity = 0
    peak = 0
    max_drawdown = 0
    for row in ordered:
        equity += int(row["net_pnl_lamports"])
        peak = max(peak, equity)
        max_drawdown = max(max_drawdown, peak - equity)
    return {
        "final_realized_closed_pnl_lamports": equity,
        "peak_realized_closed_pnl_lamports": peak,
        "max_realized_closed_equity_drawdown_lamports": max_drawdown,
    }


def policy_metrics_v0_1(
    rows: Sequence[dict[str, Any]],
    *,
    policy_id: str,
    policy_fingerprint: str,
    family: str,
) -> dict[str, Any]:
    positions = len(rows)
    filled = [row for row in rows if row["classification"] == "FILLED"]
    unresolved = [row for row in rows if row["classification"] != "FILLED"]
    rejected = sum(row["classification"] == "REJECTED_SLIPPAGE" for row in rows)
    no_fill = sum(row["classification"] == "NO_CAUSAL_FILL" for row in rows)
    reasons = Counter(str(row["reason"]) for row in rows)
    filled_reasons = Counter(str(row["reason"]) for row in filled)
    net_values = [int(row["net_pnl_lamports"]) for row in filled]
    winners = [value for value in net_values if value > 0]
    losers = [value for value in net_values if value < 0]
    breakevens = sum(value == 0 for value in net_values)
    holding = [int(row["holding_time_us"]) for row in filled]
    gross_profit = sum(winners)
    gross_loss = -sum(losers)
    if gross_loss:
        profit_factor: str | None = _ratio(gross_profit, gross_loss, 12)
    elif gross_profit:
        profit_factor = "INFINITE"
    else:
        profit_factor = None
    equity = realized_closed_equity_v0_1(rows)
    return {
        "policy_id": policy_id,
        "policy_fingerprint": policy_fingerprint,
        "family": family,
        "positions": positions,
        "fills": len(filled),
        "unresolved": len(unresolved),
        "fill_rate": _ratio(len(filled), positions),
        "unresolved_rate": _ratio(len(unresolved), positions),
        "rejected_slippage_count": rejected,
        "rejected_slippage_rate": _ratio(rejected, positions),
        "no_causal_fill_count": no_fill,
        "no_causal_fill_rate": _ratio(no_fill, positions),
        "take_profit_count": reasons["TAKE_PROFIT"],
        "stop_loss_count": reasons["STOP_LOSS"],
        "trail_count": reasons["TRAIL"],
        "fallback_count": reasons["FALLBACK"],
        "filled_take_profit_count": filled_reasons["TAKE_PROFIT"],
        "filled_stop_loss_count": filled_reasons["STOP_LOSS"],
        "filled_trail_count": filled_reasons["TRAIL"],
        "filled_fallback_count": filled_reasons["FALLBACK"],
        "gross_execution_pnl_lamports": sum(
            int(row["gross_execution_pnl_lamports"]) for row in filled
        ),
        "entry_explicit_cost_lamports": sum(
            int(row["entry_explicit_cost_lamports"]) for row in filled
        ),
        "exit_explicit_cost_lamports": sum(
            int(row["exit_explicit_cost_lamports"]) for row in filled
        ),
        "total_explicit_cost_lamports": sum(
            int(row["entry_explicit_cost_lamports"])
            + int(row["exit_explicit_cost_lamports"])
            for row in filled
        ),
        "net_pnl_lamports": sum(net_values),
        "expectancy_lamports_per_filled_trade": _mean(net_values),
        "expectancy_denominator": "FILLED",
        "wins": len(winners),
        "losses": len(losers),
        "breakevens": breakevens,
        "win_rate": _ratio(len(winners), len(filled)),
        "average_winner_lamports": _mean(winners),
        "average_loser_lamports": _mean(losers),
        "profit_factor": profit_factor,
        "mean_holding_time_us": _mean(holding),
        "median_holding_time_us": nearest_rank_v0_1(holding, 50),
        "p90_holding_time_us": nearest_rank_v0_1(holding, 90),
        "equity_label": "REALIZED_CLOSED_EQUITY",
        **equity,
    }


def replay_policy_matrix_v0_1(
    entries: Sequence[FrozenEntry],
    paths: dict[str, tuple[Any, ...]],
    policies: Sequence[CounterfactualExitPolicyV01],
    *,
    csv_path: Path | None = None,
) -> tuple[list[dict[str, Any]], str, int]:
    handle = None
    writer = None
    if csv_path is not None:
        handle = csv_path.open("w", encoding="utf-8", newline="")
        writer = csv.DictWriter(handle, fieldnames=TRADE_FIELDS, lineterminator="\n")
        writer.writeheader()
    digest = hashlib.sha256()
    summaries: list[dict[str, Any]] = []
    row_count = 0
    try:
        for policy in policies:
            policy_rows: list[dict[str, Any]] = []
            for entry in entries:
                row = _trade_row(policy, entry, paths.get(entry.mint, ()))
                policy_rows.append(row)
                if writer is not None:
                    writer.writerow(row)
                body = json.dumps(
                    row, sort_keys=True, separators=(",", ":"), ensure_ascii=True
                ).encode("utf-8")
                if row_count:
                    digest.update(b"\n")
                digest.update(body)
                row_count += 1
            summary = policy_metrics_v0_1(
                policy_rows,
                policy_id=policy.policy_id,
                policy_fingerprint=policy.fingerprint,
                family=policy.family.value,
            )
            summary.update(
                {
                    "timeout_origin": policy.timeout_origin.value,
                    "timeout_ms": policy.timeout_ms,
                    "tp_bps": policy.tp_bps,
                    "sl_bps": policy.sl_bps,
                    "trailing_activation_bps": policy.trailing_activation_bps,
                    "trailing_giveback_bps": policy.trailing_giveback_bps,
                }
            )
            summaries.append(summary)
    finally:
        if handle is not None:
            handle.close()
    expected = len(policies) * len(entries)
    if row_count != expected or row_count != 174_625:
        raise ReplayMismatch(f"policy replay row count mismatch: {row_count} != {expected}")
    return summaries, digest.hexdigest(), row_count


_T001_INTEGER_FIELDS = {
    "trigger_source_row",
    "fill_source_row",
    "execution_attempt_count",
    "entry_principal_lamports",
    "entry_explicit_cost_lamports",
    "gross_exit_proceeds_lamports",
    "exit_explicit_cost_lamports",
    "gross_execution_pnl_lamports",
    "net_pnl_lamports",
    "holding_time_us",
}


def load_accepted_actual_rows_v0_1(path: str | Path) -> list[dict[str, Any]]:
    if sha256_file(path) != EXPECTED_T001_CSV_SHA256:
        raise ReplayMismatch("accepted T001 baseline CSV SHA256 mismatch")
    rows: list[dict[str, Any]] = []
    with Path(path).open("r", encoding="utf-8", newline="") as handle:
        for source in csv.DictReader(handle):
            row: dict[str, Any] = dict(source)
            for field in _T001_INTEGER_FIELDS:
                value = row[field]
                row[field] = None if value == "" else int(value)
            row["classification"] = (
                "FILLED"
                if row["replay_exit_state"] == "FILLED"
                else row["execution_rejection_state"]
            )
            row["reason"] = row["exit_reason"]
            rows.append(row)
    if len(rows) != 1_693:
        raise ReplayMismatch(f"accepted ACTUAL row count mismatch: {len(rows)}")
    return rows


def actual_baseline_summary_v0_1(
    actual_rows: Sequence[dict[str, Any]],
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for track in ("FINAL-A", "FINAL-B", "SENS-C"):
        rows = [row for row in actual_rows if row["track_id"] == track]
        metrics = policy_metrics_v0_1(
            rows,
            policy_id=f"ACTUAL_24H/{track}",
            policy_fingerprint=T001_MODEL_FINGERPRINT,
            family="ACTUAL_24H",
        )
        metrics.update(
            {
                "baseline_track": track,
                "comparison_role": "ACTUAL_BASELINE",
                "cohort_denominator": len(rows),
                "denominator_note": (
                    "SAME_635_PHYSICAL_ENTRIES_AS_FINAL_A_B"
                    if track in ("FINAL-A", "FINAL-B")
                    else "ACCEPTED_423_ENTRY_SENSITIVITY_SUBSET"
                ),
            }
        )
        results.append(metrics)
    return results


def _pf_value(value: Any) -> Decimal:
    if value == "INFINITE":
        return Decimal("Infinity")
    if value is None:
        return Decimal(0)
    return Decimal(str(value))


def _objective_vector(row: dict[str, Any]) -> tuple[Decimal, ...]:
    return (
        Decimal(int(row["net_pnl_lamports"])),
        Decimal(str(row["expectancy_lamports_per_filled_trade"] or 0)),
        _pf_value(row["profit_factor"]),
        Decimal(str(row["win_rate"] or 0)),
        -Decimal(int(row["max_realized_closed_equity_drawdown_lamports"])),
        -Decimal(str(row["unresolved_rate"] or 0)),
    )


def _dominates(left: dict[str, Any], right: dict[str, Any]) -> bool:
    a = _objective_vector(left)
    b = _objective_vector(right)
    return all(x >= y for x, y in zip(a, b)) and any(x > y for x, y in zip(a, b))


def pareto_shortlist_v0_1(
    summaries: Sequence[dict[str, Any]],
    *,
    maximum: int = 10,
) -> list[dict[str, Any]]:
    eligible = [
        row for row in summaries if Decimal(str(row["fill_rate"] or 0)) >= Decimal("0.90")
    ]
    front = [
        row
        for row in eligible
        if not any(_dominates(other, row) for other in eligible if other is not row)
    ]
    objectives = (
        ("net_pnl_lamports", True),
        ("expectancy_lamports_per_filled_trade", True),
        ("profit_factor", True),
        ("win_rate", True),
        ("max_realized_closed_equity_drawdown_lamports", False),
        ("unresolved_rate", False),
    )
    rank_sum: dict[str, int] = defaultdict(int)
    for field, maximize in objectives:
        def value(row: dict[str, Any]) -> Decimal:
            if field == "profit_factor":
                return _pf_value(row[field])
            return Decimal(str(row[field] or 0))

        ordered = sorted(
            front,
            key=lambda row: (
                -value(row) if maximize else value(row),
                row["policy_id"],
            ),
        )
        previous: Decimal | None = None
        current_rank = 0
        for index, row in enumerate(ordered, start=1):
            current = value(row)
            if previous is None or current != previous:
                current_rank = index
                previous = current
            rank_sum[row["policy_id"]] += current_rank
    selected = sorted(
        front,
        key=lambda row: (
            rank_sum[row["policy_id"]],
            int(row["median_holding_time_us"] or 0),
            row["policy_id"],
        ),
    )[:maximum]
    output: list[dict[str, Any]] = []
    for index, row in enumerate(selected, start=1):
        dominates_count = sum(
            _dominates(row, other) for other in eligible if other is not row
        )
        output.append(
            {
                "shortlist_order": index,
                "candidate_label": "IN_SAMPLE_CANDIDATE",
                "policy_id": row["policy_id"],
                "policy_fingerprint": row["policy_fingerprint"],
                "family": row["family"],
                "fill_rate": row["fill_rate"],
                "unresolved_rate": row["unresolved_rate"],
                "net_pnl_lamports": row["net_pnl_lamports"],
                "expectancy_lamports_per_filled_trade": row[
                    "expectancy_lamports_per_filled_trade"
                ],
                "profit_factor": row["profit_factor"],
                "win_rate": row["win_rate"],
                "max_realized_closed_equity_drawdown_lamports": row[
                    "max_realized_closed_equity_drawdown_lamports"
                ],
                "median_holding_time_us": row["median_holding_time_us"],
                "pareto_front": 1,
                "dominated_by_count": 0,
                "dominates_eligible_count": dominates_count,
                "balanced_objective_rank_sum": rank_sum[row["policy_id"]],
                "retention_reason": (
                    "NON_DOMINATED_ACROSS_LOCKED_OBJECTIVES; selected by balanced "
                    "objective-rank sum; holding time then policy_id tie-break"
                ),
                "selection_rule": (
                    "FILL_RATE_GTE_0.90_THEN_PARETO_FRONT_THEN_BALANCED_RANK_SUM_"
                    "THEN_HOLDING_TIME_THEN_POLICY_ID"
                ),
            }
        )
    return output


def quartile_boundaries_v0_1(values: Sequence[int]) -> dict[str, int]:
    if not values:
        raise ValueError("quartiles require at least one value")
    return {
        "p25": int(nearest_rank_v0_1(values, 25)),
        "p50": int(nearest_rank_v0_1(values, 50)),
        "p75": int(nearest_rank_v0_1(values, 75)),
    }


def _quartile_label(value: int, boundaries: dict[str, int]) -> str:
    if value <= boundaries["p25"]:
        return "Q1"
    if value <= boundaries["p50"]:
        return "Q2"
    if value <= boundaries["p75"]:
        return "Q3"
    return "Q4"


def _utc_bucket(entry_at: str) -> str:
    hour = _dt(entry_at).hour
    if hour < 6:
        return "00:00-05:59"
    if hour < 12:
        return "06:00-11:59"
    if hour < 18:
        return "12:00-17:59"
    return "18:00-23:59"


def _segment_trade_rows(
    policies: Sequence[CounterfactualExitPolicyV01],
    entries: Sequence[FrozenEntry],
    paths: dict[str, tuple[Any, ...]],
) -> dict[str, list[dict[str, Any]]]:
    output: dict[str, list[dict[str, Any]]] = {}
    for policy in policies:
        output[policy.policy_id] = [
            _trade_row(policy, entry, paths.get(entry.mint, ())) for entry in entries
        ]
    return output


def segment_summary_v0_1(
    entry_cohort: Sequence[dict[str, Any]],
    actual_rows: Sequence[dict[str, Any]],
    shortlist: Sequence[dict[str, Any]],
    registry: Sequence[CounterfactualExitPolicyV01],
    entries: Sequence[FrozenEntry],
    paths: dict[str, tuple[Any, ...]],
) -> tuple[list[dict[str, Any]], dict[str, dict[str, int]]]:
    features = {row["signal_key"]: row for row in entry_cohort}
    boundaries = {
        "entry_price_impact_bps": quartile_boundaries_v0_1(
            [int(row["entry_price_impact_bps"]) for row in entry_cohort]
        ),
        "entry_adverse_slippage_bps": quartile_boundaries_v0_1(
            [int(row["entry_adverse_slippage_bps"]) for row in entry_cohort]
        ),
    }
    selected_ids = {row["policy_id"] for row in shortlist}
    selected_policies = [policy for policy in registry if policy.policy_id in selected_ids]
    research_rows = _segment_trade_rows(selected_policies, entries, paths)
    actor_rows: list[tuple[str, str, list[dict[str, Any]]]] = []
    for track in ("FINAL-A", "FINAL-B", "SENS-C"):
        actor_rows.append(
            (
                f"ACTUAL_24H/{track}",
                "ACTUAL_BASELINE",
                [row for row in actual_rows if row["track_id"] == track],
            )
        )
    for policy in selected_policies:
        actor_rows.append(
            (policy.policy_id, "IN_SAMPLE_CANDIDATE", research_rows[policy.policy_id])
        )

    dimension_categories = {
        "ENTRY_UTC_TIME": (
            "00:00-05:59",
            "06:00-11:59",
            "12:00-17:59",
            "18:00-23:59",
        ),
        "ENTRY_PRICE_IMPACT_BPS_QUARTILE": ("Q1", "Q2", "Q3", "Q4"),
        "ENTRY_ADVERSE_SLIPPAGE_BPS_QUARTILE": ("Q1", "Q2", "Q3", "Q4"),
    }

    def label(dimension: str, feature: dict[str, Any]) -> str:
        if dimension == "ENTRY_UTC_TIME":
            return _utc_bucket(feature["entry_observed_at"])
        if dimension == "ENTRY_PRICE_IMPACT_BPS_QUARTILE":
            return _quartile_label(
                int(feature["entry_price_impact_bps"]),
                boundaries["entry_price_impact_bps"],
            )
        return _quartile_label(
            int(feature["entry_adverse_slippage_bps"]),
            boundaries["entry_adverse_slippage_bps"],
        )

    output: list[dict[str, Any]] = []
    for actor_id, actor_kind, rows in actor_rows:
        for dimension, categories in dimension_categories.items():
            for category in categories:
                cell = [
                    row
                    for row in rows
                    if label(dimension, features[row["signal_key"]]) == category
                ]
                metrics = policy_metrics_v0_1(
                    cell,
                    policy_id=actor_id,
                    policy_fingerprint="SEGMENT_VIEW",
                    family=actor_kind,
                )
                output.append(
                    {
                        "actor_id": actor_id,
                        "actor_kind": actor_kind,
                        "dimension": dimension,
                        "segment": category,
                        "n": len(cell),
                        "small_cell": len(cell) < SMALL_CELL_N,
                        "fills": metrics["fills"],
                        "unresolved": metrics["unresolved"],
                        "fill_rate": metrics["fill_rate"],
                        "net_pnl_lamports": metrics["net_pnl_lamports"],
                        "expectancy_lamports_per_filled_trade": metrics[
                            "expectancy_lamports_per_filled_trade"
                        ],
                        "wins": metrics["wins"],
                        "losses": metrics["losses"],
                        "win_rate": metrics["win_rate"],
                        "equity_label": "REALIZED_CLOSED_EQUITY",
                    }
                )
    return output, boundaries


def baseline_vs_replay_v0_1(
    actual_summary: Sequence[dict[str, Any]],
    replay_summary: Sequence[dict[str, Any]],
    shortlist: Sequence[dict[str, Any]],
) -> list[dict[str, Any]]:
    closest = {
        "TP10_T15S": "CLOSEST_TO_FINAL_A_STRUCTURE",
        "TRAIL_A10_G3_T15S": "CLOSEST_TO_FINAL_B_STRUCTURE",
        "TP20_T15S": "CLOSEST_AVAILABLE_TO_SENS_C_STRUCTURE_TIMEOUT_DIFFERS",
    }
    shortlisted = {row["policy_id"] for row in shortlist}
    rows: list[dict[str, Any]] = []
    for actual in actual_summary:
        rows.append(
            {
                **actual,
                "comparison_role": "ACTUAL_BASELINE",
                "candidate_label": "REFERENCE_ONLY_NOT_SHORTLISTED",
                "structure_note": actual["denominator_note"],
            }
        )
    for replay in replay_summary:
        policy_id = replay["policy_id"]
        if policy_id not in closest and policy_id not in shortlisted:
            continue
        role = (
            "CLOSEST_COUNTERFACTUAL_AND_SHORTLIST"
            if policy_id in closest and policy_id in shortlisted
            else "CLOSEST_COUNTERFACTUAL"
            if policy_id in closest
            else "SHORTLIST"
        )
        rows.append(
            {
                **replay,
                "comparison_role": role,
                "candidate_label": (
                    "IN_SAMPLE_CANDIDATE"
                    if policy_id in shortlisted
                    else "COMPARISON_ONLY"
                ),
                "cohort_denominator": replay["positions"],
                "denominator_note": "ALL_635_PHYSICAL_ENTRIES",
                "structure_note": closest.get(policy_id, "PARETO_SHORTLIST_POLICY"),
            }
        )
    return rows


def _write_csv(path: Path, rows: Sequence[dict[str, Any]]) -> None:
    if not rows:
        raise ReplayMismatch(f"refusing to write empty CSV: {path.name}")
    fields: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for field in row:
            if field not in seen:
                seen.add(field)
                fields.append(field)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _write_json(path: Path, value: Any) -> None:
    path.write_text(
        json.dumps(value, sort_keys=True, indent=2, ensure_ascii=True) + "\n",
        encoding="utf-8",
        newline="\n",
    )


def _core_digest_payload(
    *,
    entry_cohort: Sequence[dict[str, Any]],
    horizon_rows: Sequence[dict[str, Any]],
    horizon_summary: Sequence[dict[str, Any]],
    mfe_rows: Sequence[dict[str, Any]],
    mfe_summary: Sequence[dict[str, Any]],
    post_exit_rows: Sequence[dict[str, Any]],
    sold_summary: Sequence[dict[str, Any]],
    actual_summary: Sequence[dict[str, Any]],
    registry_rows: Sequence[dict[str, Any]],
    replay_summary: Sequence[dict[str, Any]],
    baseline_comparison: Sequence[dict[str, Any]],
    shortlist: Sequence[dict[str, Any]],
    segments: Sequence[dict[str, Any]],
    quartiles: dict[str, dict[str, int]],
    trade_digest: str,
    trade_count: int,
) -> dict[str, Any]:
    return {
        "model_id": MODEL_ID,
        "model_fingerprint": MODEL_FINGERPRINT,
        "entry_cohort": entry_cohort,
        "entry_aligned_horizons": horizon_rows,
        "entry_aligned_horizon_summary": horizon_summary,
        "entry_mfe_mae": mfe_rows,
        "entry_mfe_mae_summary": mfe_summary,
        "post_actual_exit_outcomes": post_exit_rows,
        "sold_too_early_summary": sold_summary,
        "actual_baseline_summary": actual_summary,
        "policy_registry": registry_rows,
        "replay_policy_summary": replay_summary,
        "baseline_vs_replay": baseline_comparison,
        "candidate_shortlist": shortlist,
        "segment_summary": segments,
        "segmentation_quartiles": quartiles,
        "replay_trade_results_sha256": trade_digest,
        "replay_trade_results_rows": trade_count,
    }


def _report_text(summary: dict[str, Any], shortlist: Sequence[dict[str, Any]]) -> str:
    lines = [
        "# POST-24H-ANALYSIS-001 — Outcome Studies and Counterfactual Exit Matrix",
        "",
        "Status: IMPLEMENTED_PENDING_PROJECT_REVIEW",
        "",
        "## Scope and interpretation locks",
        "",
        "This is hypothesis generation from one contiguous ~24h IN-SAMPLE regime. The 275-policy matrix creates multiple-comparison and selection risk. Shortlisted policies require Phase 6 out-of-sample validation. No parameter is approved for live trading.",
        "",
        "FINAL-A and FINAL-B remain frozen and unmodified and share the same physical entries. ACTUAL SENS-C uses a different accepted 423-entry denominator. The research cohort contains 635 distinct physical entries/mints.",
        "",
        "Unresolved executions are not converted to zero-PnL closed trades. Market outcome marks are causal last-known fresh marks without interpolation. Horizon availability is a research measure and is not canonical Phase-4 CoverageStatus.",
        "",
        "## Frozen proofs",
        "",
        f"- Cohort: {summary['cohort_count']} / `{summary['cohort_sha256']}`",
        f"- T001 replay digest: `{summary['t001_replay_digest']}`",
        f"- T002A deterministic digest: `{summary['t002a_deterministic_digest']}`",
        f"- T002B deterministic analysis digest: `{summary['deterministic_analysis_digest']}`",
        "",
        "## Policy matrix",
        "",
        f"- Registry: {summary['policy_count']} policies",
        f"- Replay rows: {summary['replay_row_count']}",
        "- All new research policies use ENTRY_FILL_CLOCK and keep thresholds anchored to the frozen Phase-4 rule reference.",
        "- Equity/drawdown metrics are explicitly REALIZED_CLOSED_EQUITY, not causal mark-to-market drawdown.",
        "",
        "## Entry-aligned mark availability",
        "",
    ]
    for row in summary["entry_horizon_top_level"]:
        lines.append(
            f"- {row['horizon_seconds']}s: {row['available_n']}/{row['cohort_n']} available ({row['availability_rate']})"
        )
    lines.extend(["", "## MFE / MAE context", ""])
    for row in summary["mfe_mae_top_level"]:
        if row["horizon_seconds"] in (5, 15, 60, 300):
            lines.append(
                f"- {row['horizon_seconds']}s: MFE p50 {row['mfe_p50_bps']} bps; "
                f"MAE p50 {row['mae_p50_bps']} bps; available {row['available_n']}/{row['cohort_n']}"
            )
    lines.extend(["", "## ACTUAL_24H baselines", ""])
    lines.append("| Track | Denominator | Filled | Unresolved | Net PnL |")
    lines.append("|---|---:|---:|---:|---:|")
    for row in summary["actual_baseline_top_level"]:
        lines.append(
            f"| {row['track_id']} | {row['positions']} | {row['fills']} | "
            f"{row['unresolved']} | {row['net_pnl_lamports']} |"
        )
    lines.extend(["", "## Post-ACTUAL-exit raw-market marks", ""])
    for row in summary["post_actual_exit_top_level"]:
        lines.append(
            f"- {row['horizon_seconds']}s: {row['available_n']}/{row['actual_filled_exit_n']} "
            f"available; raw-market return p50 {row['return_p50_bps']} bps"
        )
    lines.extend(["", "## Matrix context", ""])
    lines.append(
        "The highest in-sample net-PnL policies are shown only as context, not as a winner ranking. Positive in-sample net PnL does not override the displayed execution-coverage rate or the locked shortlist gate."
    )
    for row in summary["research_policy_net_pnl_context_only"]:
        lines.append(
            f"- {row['policy_id']}: net {row['net_pnl_lamports']}; fill rate "
            f"{row['fill_rate']}; unresolved rate {row['unresolved_rate']}"
        )
    sold = summary["sold_too_early_highest_rate_large_cells_context_only"]
    if sold:
        top = sold[0]
        lines.extend(
            [
                "",
                "Sold-too-early cells are descriptive only. The highest-rate non-small cell was "
                f"{top['track_id']} / {top['actual_reason']} / {top['window_seconds']}s / "
                f"{top['threshold_bps']}bps: {top['reached_n']}/{top['eligible_n']} "
                f"({top['reached_rate']}).",
            ]
        )
    lines.extend(["", "## Pareto-style shortlist", ""])
    if shortlist:
        lines.append(
            "| Order | Candidate | Family | Fill rate | Net PnL | Profit factor | Drawdown |"
        )
        lines.append("|---:|---|---|---:|---:|---:|---:|")
        for row in shortlist:
            lines.append(
                f"| {row['shortlist_order']} | {row['policy_id']} | {row['family']} | "
                f"{row['fill_rate']} | {row['net_pnl_lamports']} | {row['profit_factor']} | "
                f"{row['max_realized_closed_equity_drawdown_lamports']} |"
            )
    else:
        lines.append("No research policy passed the locked shortlist eligibility/front rules.")
    lines.extend(
        [
            "",
            "All listed policies are labeled IN_SAMPLE_CANDIDATE. None is a winner or a live-policy selection.",
            "",
            "## Small-cell rule",
            "",
            "Segment and sold-too-early cells with N < 50 are marked small_cell=true and must not support shortlist selection or strong conclusions.",
            "",
        ]
    )
    return "\n".join(lines)


def run_analysis_v0_1(
    *,
    paper_db: str | Path,
    source_db: str | Path,
    accepted_t001_csv: str | Path,
    accepted_t002a_evidence: str | Path,
    output_dir: str | Path,
    git_head: str,
) -> dict[str, Any]:
    if git_head != EXPECTED_HEAD:
        raise ReplayMismatch(f"required HEAD mismatch: {git_head} != {EXPECTED_HEAD}")
    paper_db = Path(paper_db)
    source_db = Path(source_db)
    accepted_t001_csv = Path(accepted_t001_csv)
    accepted_t002a_evidence = Path(accepted_t002a_evidence)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    paper_sha256 = sha256_file(paper_db)
    source_sha256 = sha256_file(source_db)
    t001_csv_sha256 = sha256_file(accepted_t001_csv)
    t002a_evidence_sha256 = sha256_file(accepted_t002a_evidence)
    if paper_sha256 != EXPECTED_PAPER_SHA256:
        raise ReplayMismatch("accepted paper DB SHA256 mismatch")
    if source_sha256 != EXPECTED_SOURCE_SHA256:
        raise ReplayMismatch("frozen source DB SHA256 mismatch")
    if t001_csv_sha256 != EXPECTED_T001_CSV_SHA256:
        raise ReplayMismatch("accepted T001 baseline CSV SHA256 mismatch")
    t002a_evidence = json.loads(accepted_t002a_evidence.read_text(encoding="utf-8"))
    if (
        t002a_evidence.get("model_id") != T002A_MODEL_ID
        or t002a_evidence.get("model_fingerprint") != T002A_MODEL_FINGERPRINT
        or t002a_evidence.get("t001_replay_digest") != EXPECTED_T001_REPLAY_SHA256
        or t002a_evidence.get("t002a_deterministic_digest")
        != EXPECTED_T002A_DETERMINISTIC_SHA256
    ):
        raise ReplayMismatch("accepted T002A evidence identity mismatch")

    baseline = run_frozen_baseline_gate_v0_1(paper_db, source_db)
    if (
        not baseline["exact_comparison"]
        or baseline["t001_replay_digest"] != EXPECTED_T001_REPLAY_SHA256
        or baseline["t002a_deterministic_digest"]
        != EXPECTED_T002A_DETERMINISTIC_SHA256
    ):
        raise ReplayMismatch("frozen T001/T002A baseline gate mismatch")

    entries, cohort_proof = load_frozen_entries(paper_db, source_db)
    entries = sorted(entries, key=lambda entry: (entry.signal_key, entry.mint))
    paths, source_proof = load_frozen_market_paths(
        source_db, {entry.mint for entry in entries}
    )
    paths = {
        mint: tuple(sorted(path, key=lambda row: (row.observed_at, row.ingest_seq)))
        for mint, path in paths.items()
    }
    entry_cohort = load_entry_cohort_rows_v0_1(paper_db, entries)
    actual_filled_exits = load_actual_filled_exits_v0_1(paper_db, paths)
    actual_rows = load_accepted_actual_rows_v0_1(accepted_t001_csv)
    actual_summary = actual_baseline_summary_v0_1(actual_rows)
    for row in actual_summary:
        track = row["baseline_track"]
        expected = baseline["summary"][track]
        checks = {
            "positions": (row["positions"], expected["positions"]),
            "fills": (row["fills"], expected["completed"]),
            "unresolved": (row["unresolved"], expected["pending"]),
            "net_pnl_lamports": (row["net_pnl_lamports"], expected["net_pnl_lamports"]),
        }
        for field, pair in checks.items():
            if pair[0] != pair[1]:
                raise ReplayMismatch(f"ACTUAL summary {track}.{field}: {pair[0]} != {pair[1]}")

    policies = build_policy_registry_v0_1()
    registry_rows = policy_registry_rows_v0_1(policies)
    horizon_rows, horizon_summary, mfe_rows, mfe_summary = entry_outcome_studies_v0_1(
        entries, paths
    )
    post_exit_rows, sold_summary = post_actual_exit_studies_v0_1(
        actual_filled_exits, paths
    )

    trade_csv = output_dir / "replay_trade_results.csv"
    replay_summary, trade_digest, trade_count = replay_policy_matrix_v0_1(
        entries, paths, policies, csv_path=trade_csv
    )
    shortlist = pareto_shortlist_v0_1(replay_summary)
    if len(shortlist) > 10:
        raise ReplayMismatch("shortlist exceeds maximum 10")
    shortlisted_ids = {row["policy_id"] for row in shortlist}
    by_id = {row["policy_id"]: row for row in replay_summary}
    if any(Decimal(str(by_id[policy_id]["fill_rate"])) < Decimal("0.90") for policy_id in shortlisted_ids):
        raise ReplayMismatch("shortlist violates fill-rate gate")
    segments, quartiles = segment_summary_v0_1(
        entry_cohort, actual_rows, shortlist, policies, entries, paths
    )
    baseline_comparison = baseline_vs_replay_v0_1(
        actual_summary, replay_summary, shortlist
    )

    core_payload = _core_digest_payload(
        entry_cohort=entry_cohort,
        horizon_rows=horizon_rows,
        horizon_summary=horizon_summary,
        mfe_rows=mfe_rows,
        mfe_summary=mfe_summary,
        post_exit_rows=post_exit_rows,
        sold_summary=sold_summary,
        actual_summary=actual_summary,
        registry_rows=registry_rows,
        replay_summary=replay_summary,
        baseline_comparison=baseline_comparison,
        shortlist=shortlist,
        segments=segments,
        quartiles=quartiles,
        trade_digest=trade_digest,
        trade_count=trade_count,
    )
    analysis_digest = canonical_sha256(
        {
            "git_head": git_head,
            "cohort_sha256": EXPECTED_COHORT_SHA256,
            "t001_replay_digest": EXPECTED_T001_REPLAY_SHA256,
            "t002a_deterministic_digest": EXPECTED_T002A_DETERMINISTIC_SHA256,
            "core": core_payload,
        }
    )

    # Exact second pass: regenerate every study, the full 174625-row replay
    # digest/summary, shortlist, and bounded segments from the same frozen inputs.
    horizon_rows_2, horizon_summary_2, mfe_rows_2, mfe_summary_2 = entry_outcome_studies_v0_1(
        entries, paths
    )
    post_exit_rows_2, sold_summary_2 = post_actual_exit_studies_v0_1(
        actual_filled_exits, paths
    )
    replay_summary_2, trade_digest_2, trade_count_2 = replay_policy_matrix_v0_1(
        entries, paths, policies
    )
    shortlist_2 = pareto_shortlist_v0_1(replay_summary_2)
    segments_2, quartiles_2 = segment_summary_v0_1(
        entry_cohort, actual_rows, shortlist_2, policies, entries, paths
    )
    baseline_comparison_2 = baseline_vs_replay_v0_1(
        actual_summary, replay_summary_2, shortlist_2
    )
    core_payload_2 = _core_digest_payload(
        entry_cohort=entry_cohort,
        horizon_rows=horizon_rows_2,
        horizon_summary=horizon_summary_2,
        mfe_rows=mfe_rows_2,
        mfe_summary=mfe_summary_2,
        post_exit_rows=post_exit_rows_2,
        sold_summary=sold_summary_2,
        actual_summary=actual_summary,
        registry_rows=registry_rows,
        replay_summary=replay_summary_2,
        baseline_comparison=baseline_comparison_2,
        shortlist=shortlist_2,
        segments=segments_2,
        quartiles=quartiles_2,
        trade_digest=trade_digest_2,
        trade_count=trade_count_2,
    )
    analysis_digest_2 = canonical_sha256(
        {
            "git_head": git_head,
            "cohort_sha256": EXPECTED_COHORT_SHA256,
            "t001_replay_digest": EXPECTED_T001_REPLAY_SHA256,
            "t002a_deterministic_digest": EXPECTED_T002A_DETERMINISTIC_SHA256,
            "core": core_payload_2,
        }
    )
    if analysis_digest_2 != analysis_digest or core_payload_2 != core_payload:
        raise ReplayMismatch("full analysis deterministic rerun mismatch")

    _write_csv(output_dir / "entry_cohort.csv", entry_cohort)
    _write_json(
        output_dir / "entry_cohort.json",
        {
            "cohort_count": len(entry_cohort),
            "cohort_sha256": EXPECTED_COHORT_SHA256,
            "rows": entry_cohort,
        },
    )
    _write_csv(output_dir / "entry_aligned_horizons.csv", horizon_rows)
    _write_csv(output_dir / "entry_aligned_horizon_summary.csv", horizon_summary)
    _write_csv(output_dir / "entry_mfe_mae.csv", mfe_rows)
    _write_csv(output_dir / "entry_mfe_mae_summary.csv", mfe_summary)
    _write_csv(output_dir / "post_actual_exit_outcomes.csv", post_exit_rows)
    _write_csv(output_dir / "sold_too_early_summary.csv", sold_summary)
    _write_csv(output_dir / "actual_baseline_summary.csv", actual_summary)
    _write_csv(output_dir / "replay_policy_registry.csv", registry_rows)
    _write_csv(output_dir / "replay_policy_summary.csv", replay_summary)
    _write_csv(output_dir / "baseline_vs_replay.csv", baseline_comparison)
    _write_csv(output_dir / "segment_summary.csv", segments)
    _write_csv(output_dir / "candidate_shortlist.csv", shortlist)

    policy_counts = dict(sorted(Counter(row["family"] for row in registry_rows).items()))
    high_availability = [
        {
            "horizon_seconds": row["horizon_seconds"],
            "cohort_n": row["cohort_n"],
            "available_n": row["available_n"],
            "missing_n": row["missing_n"],
            "availability_rate": row["availability_rate"],
            "return_p50_bps": row["return_p50_bps"],
        }
        for row in horizon_summary
    ]
    sold_context = sorted(
        (row for row in sold_summary if not row["small_cell"]),
        key=lambda row: (
            -Decimal(str(row["reached_rate"] or 0)),
            row["threshold_bps"],
            row["window_seconds"],
            row["track_id"],
            row["actual_reason"],
        ),
    )[:10]
    post_exit_top_level: list[dict[str, Any]] = []
    for horizon in POST_EXIT_HORIZONS_SECONDS:
        values = [row for row in post_exit_rows if row["horizon_seconds"] == horizon]
        available = [row for row in values if row["mark_status"] == "AVAILABLE"]
        returns = [int(row["post_exit_raw_market_return_bps"]) for row in available]
        post_exit_top_level.append(
            {
                "horizon_seconds": horizon,
                "actual_filled_exit_n": len(values),
                "available_n": len(available),
                "missing_n": len(values) - len(available),
                "availability_rate": _ratio(len(available), len(values)),
                "return_mean_bps": _mean(returns),
                "return_p50_bps": nearest_rank_v0_1(returns, 50),
            }
        )
    actual_baseline_top_level = [
        {
            "track_id": row["baseline_track"],
            "positions": row["positions"],
            "fills": row["fills"],
            "unresolved": row["unresolved"],
            "net_pnl_lamports": row["net_pnl_lamports"],
            "denominator_note": row["denominator_note"],
        }
        for row in actual_summary
    ]
    policy_net_context = [
        {
            "policy_id": row["policy_id"],
            "family": row["family"],
            "net_pnl_lamports": row["net_pnl_lamports"],
            "fill_rate": row["fill_rate"],
            "unresolved_rate": row["unresolved_rate"],
        }
        for row in sorted(
            replay_summary,
            key=lambda row: (-int(row["net_pnl_lamports"]), row["policy_id"]),
        )[:10]
    ]
    analysis_summary = {
        "analysis_id": ANALYSIS_ID,
        "model_id": MODEL_ID,
        "model_fingerprint": MODEL_FINGERPRINT,
        "status": "IMPLEMENTED_PENDING_PROJECT_REVIEW",
        "git_head": git_head,
        "cohort_count": len(entries),
        "distinct_mints": len({entry.mint for entry in entries}),
        "cohort_sha256": cohort_proof["computed_accepted_cohort_sha256"],
        "source_anchor": SOURCE_ANCHOR,
        "frozen_watermark": FROZEN_WATERMARK,
        "t001_replay_digest": baseline["t001_replay_digest"],
        "t002a_deterministic_digest": baseline["t002a_deterministic_digest"],
        "actual_baseline_exact_reproduction": True,
        "policy_count": len(policies),
        "policy_family_counts": policy_counts,
        "replay_row_count": trade_count,
        "replay_trade_results_canonical_sha256": trade_digest,
        "deterministic_analysis_digest": analysis_digest,
        "deterministic_full_analysis_rerun": "EXACT",
        "shortlist_count": len(shortlist),
        "shortlist": shortlist,
        "entry_horizon_top_level": high_availability,
        "mfe_mae_top_level": mfe_summary,
        "post_actual_exit_top_level": post_exit_top_level,
        "actual_baseline_top_level": actual_baseline_top_level,
        "research_policy_net_pnl_context_only": policy_net_context,
        "sold_too_early_highest_rate_large_cells_context_only": sold_context,
        "segmentation_quartile_boundaries": quartiles,
        "interpretation_locks": [
            "ONE_CONTIGUOUS_APPROX_24H_IN_SAMPLE_REGIME",
            "MULTIPLE_COMPARISON_AND_SELECTION_RISK",
            "PHASE6_OOS_REQUIRED",
            "NO_PARAMETER_APPROVED_FOR_LIVE_TRADING",
            "FINAL_A_AND_FINAL_B_FROZEN_UNMODIFIED",
            "ACTUAL_A_B_SHARE_ENTRIES",
            "ACTUAL_SENS_C_DIFFERENT_423_ENTRY_DENOMINATOR",
            "635_DISTINCT_PHYSICAL_ENTRIES_AND_MINTS",
            "UNRESOLVED_NOT_ZERO_PNL",
            "CAUSAL_LAST_KNOWN_MARKS_NO_INTERPOLATION",
            "HORIZON_AVAILABILITY_NOT_CANONICAL_PHASE4_COVERAGE",
        ],
    }
    _write_json(output_dir / "analysis_summary.json", analysis_summary)
    (output_dir / "POST-24H-ANALYSIS-001_REPORT.md").write_text(
        _report_text(analysis_summary, shortlist),
        encoding="utf-8",
        newline="\n",
    )

    output_hashes = {
        name: sha256_file(output_dir / name)
        for name in OUTPUT_FILENAMES
        if name != "POST24H_T002B_manifest.json"
    }
    output_rows = {
        "entry_cohort.csv": len(entry_cohort),
        "entry_cohort.json": len(entry_cohort),
        "entry_aligned_horizons.csv": len(horizon_rows),
        "entry_aligned_horizon_summary.csv": len(horizon_summary),
        "entry_mfe_mae.csv": len(mfe_rows),
        "entry_mfe_mae_summary.csv": len(mfe_summary),
        "post_actual_exit_outcomes.csv": len(post_exit_rows),
        "sold_too_early_summary.csv": len(sold_summary),
        "actual_baseline_summary.csv": len(actual_summary),
        "replay_policy_registry.csv": len(registry_rows),
        "replay_trade_results.csv": trade_count,
        "replay_policy_summary.csv": len(replay_summary),
        "baseline_vs_replay.csv": len(baseline_comparison),
        "segment_summary.csv": len(segments),
        "candidate_shortlist.csv": len(shortlist),
        "analysis_summary.json": 1,
        "POST-24H-ANALYSIS-001_REPORT.md": 1,
    }
    manifest = {
        "analysis_id": ANALYSIS_ID,
        "model_id": MODEL_ID,
        "model_fingerprint": MODEL_FINGERPRINT,
        "git_head": git_head,
        "inputs": {
            "paper_db": {
                "path": str(paper_db.as_posix()),
                "sha256": paper_sha256,
                "readonly": True,
            },
            "source_db": {
                "path": str(source_db.as_posix()),
                "sha256": source_sha256,
                "readonly": True,
            },
            "accepted_t001_csv": {
                "path": str(accepted_t001_csv.as_posix()),
                "sha256": t001_csv_sha256,
            },
            "accepted_t002a_evidence": {
                "path": str(accepted_t002a_evidence.as_posix()),
                "sha256": t002a_evidence_sha256,
            },
        },
        "models": {
            "t001": {"model_id": T001_MODEL_ID, "fingerprint": T001_MODEL_FINGERPRINT},
            "t002a": {"model_id": T002A_MODEL_ID, "fingerprint": T002A_MODEL_FINGERPRINT},
            "cost_model_fingerprint": P4_COST_BASELINE_0001.fingerprint,
            "exit_impact_model_id": EXIT_IMPACT_MODEL_ID,
            "exit_impact_fingerprint": EXIT_IMPACT_FINGERPRINT,
        },
        "cohort": {
            "count": len(entries),
            "distinct_mints": len({entry.mint for entry in entries}),
            "sha256": EXPECTED_COHORT_SHA256,
        },
        "source": source_proof,
        "frozen_digests": {
            "t001_replay": baseline["t001_replay_digest"],
            "t002a_deterministic": baseline["t002a_deterministic_digest"],
        },
        "policy_arrays": SPEC["policy_arrays"],
        "policy_family_counts": policy_counts,
        "policy_count": len(policies),
        "new_policy_timeout_origin": TimeoutOrigin.ENTRY_FILL_CLOCK.value,
        "threshold_reference": "FROZEN_PHASE4_RULE_REFERENCE",
        "percentile_method": PERCENTILE_METHOD,
        "segmentation_quartile_boundaries": quartiles,
        "small_cell_n": SMALL_CELL_N,
        "output_row_counts": output_rows,
        "output_sha256": output_hashes,
        "replay_trade_results_canonical_sha256": trade_digest,
        "deterministic_analysis_digest": analysis_digest,
        "deterministic_full_analysis_rerun": "EXACT",
        "actual_exit_market_baseline": "RAW_SOURCE_MARKET_AT_ACTUAL_FILL_ROW",
        "equity_label": "REALIZED_CLOSED_EQUITY",
        "network_rpc_wallet_signing": False,
    }
    _write_json(output_dir / "POST24H_T002B_manifest.json", manifest)
    return {**analysis_summary, "manifest": manifest, "output_row_counts": output_rows}
