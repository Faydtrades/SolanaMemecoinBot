from __future__ import annotations

from collections import defaultdict
from datetime import timedelta
import math
from pathlib import Path
from statistics import fmean
from typing import Any

from phase2.data_coverage_root_cause_v0_1_1 import us_to_iso
from phase2.phase1_readonly_adapter_v0_1 import Phase1ReadOnlyAdapterV01
from phase2.quote_aware_price_v0_1 import Phase1QuoteAwareAdapterV01
from phase3.collector_coverage_v0_2 import CollectorCoverageSnapshotV02
from phase3.e4_trailing_design_v0_1 import (
    FALLBACK_TIME_STOP_MS,
    TRAIL_ACTIVATION_BPS,
    TRAIL_GIVEBACK_BPS,
)
from phase3.exit_path_replay_v0_1 import ExitPathReplayV01, ExitPathStatus
from phase3.locked_entry_readiness_v0_1_2 import (
    load_locked_selection,
    manifest_identity,
    parameter_set_from_locked_row,
)
from phase3.phase2_outcome_adapter_v0_1_2 import (
    extract_candidates,
    price_observation_from_event,
)

ENTRY_WINDOW_MS = 300_000


def nearest_rank(values: list[int], p: int) -> int | None:
    if not values:
        return None
    s = sorted(values)
    rank = max(1, math.ceil((p / 100) * len(s)))
    return s[rank - 1]


def summarize(values: list[int]) -> dict[str, Any]:
    if not values:
        return {
            "n": 0,
            "p10": None,
            "p25": None,
            "p50": None,
            "p75": None,
            "p90": None,
            "mean": None,
            "positive_rate": None,
            "negative_rate": None,
        }
    return {
        "n": len(values),
        "p10": nearest_rank(values, 10),
        "p25": nearest_rank(values, 25),
        "p50": nearest_rank(values, 50),
        "p75": nearest_rank(values, 75),
        "p90": nearest_rank(values, 90),
        "mean": fmean(values),
        "positive_rate": sum(v > 0 for v in values) / len(values),
        "negative_rate": sum(v < 0 for v in values) / len(values),
    }


def fetch_tokens(conn):
    rows = conn.execute(
        """
        SELECT mint,t0_observed_at_utc
        FROM freeze_tokens
        ORDER BY t0_observed_at_utc,mint
        """
    ).fetchall()
    return [
        {
            "mint": str(r["mint"]),
            "t0_us": Phase1ReadOnlyAdapterV01._iso_to_us(
                str(r["t0_observed_at_utc"])
            ),
        }
        for r in rows
    ]


def fetch_entry_rows(conn, tokens, chunk=300):
    rows = []
    batches = (len(tokens) + chunk - 1) // chunk
    for bi, off in enumerate(range(0, len(tokens), chunk), start=1):
        batch = tokens[off:off + chunk]
        values_sql = ",".join("(?,?)" for _ in batch)
        args = []
        for t in batch:
            args.extend(
                [
                    t["mint"],
                    us_to_iso(t["t0_us"] + ENTRY_WINDOW_MS * 1000),
                ]
            )
        part = conn.execute(
            f"""
            WITH eligible(mint,deadline_utc) AS (VALUES {values_sql})
            SELECT p.rowid AS p1_rowid,p.*
            FROM pump_events AS p
            JOIN eligible AS e ON e.mint=p.mint
            WHERE p.event_type IN ('LAUNCH','BUY','SELL')
              AND p.decoded_at_utc<=e.deadline_utc
            ORDER BY p.decoded_at_utc,p.rowid
            """,
            tuple(args),
        ).fetchall()
        rows.extend(part)
        if bi == 1 or bi == batches or bi % 5 == 0:
            print(f"[ENTRY] batch {bi}/{batches} rows={len(rows)}", flush=True)
    rows.sort(
        key=lambda r: (
            Phase1ReadOnlyAdapterV01._iso_to_us(str(r["decoded_at_utc"])),
            int(r["p1_rowid"]),
        )
    )
    return rows


def fetch_path_rows(conn, mints: set[str], chunk=300):
    ms = sorted(mints)
    rows = []
    batches = (len(ms) + chunk - 1) // chunk
    for bi, off in enumerate(range(0, len(ms), chunk), start=1):
        batch = ms[off:off + chunk]
        ph = ",".join("?" for _ in batch)
        part = conn.execute(
            f"""
            SELECT rowid AS p1_rowid,*
            FROM pump_events
            WHERE mint IN ({ph})
              AND event_type IN ('LAUNCH','BUY','SELL')
            ORDER BY decoded_at_utc,rowid
            """,
            tuple(batch),
        ).fetchall()
        rows.extend(part)
        if bi == 1 or bi == batches or bi % 5 == 0:
            print(f"[PATH] batch {bi}/{batches} rows={len(rows)}", flush=True)
    rows.sort(
        key=lambda r: (
            Phase1ReadOnlyAdapterV01._iso_to_us(str(r["decoded_at_utc"])),
            int(r["p1_rowid"]),
        )
    )
    return rows


def trailing_exit_at_or_before(
    path,
    *,
    activation_bps: int,
    giveback_bps: int,
    horizon_ms: int,
):
    """Observed-point trailing semantics, no interpolation.

    Returns (activation_point, exit_point, running_peak_at_exit_bps).
    exit_point is None if activated but never hit before horizon.
    """
    target = path.signal_at + timedelta(milliseconds=horizon_ms)
    activation = None
    peak = None

    for point in path.points:
        if point.observed_at > target:
            break

        if activation is None:
            if point.return_bps >= activation_bps:
                activation = point
                peak = point.return_bps
            continue

        if point.return_bps > peak:
            peak = point.return_bps

        trail_level = peak - giveback_bps
        if point.return_bps <= trail_level:
            return activation, point, peak

    return activation, None, peak


def fallback_mark_at_or_before(path, horizon_ms: int):
    target = path.signal_at + timedelta(milliseconds=horizon_ms)
    chosen = None
    for point in path.points:
        if point.observed_at <= target:
            chosen = point
        else:
            break
    return chosen, target


def evaluate_e4(
    *,
    freeze_db: Path,
    manifest_path: Path,
    coverage_path: Path,
    selection_path: Path,
    readiness_path: Path,
) -> dict[str, Any]:
    import json

    readiness = json.loads(readiness_path.read_text(encoding="utf-8"))
    if readiness["readiness"]["tier"] != "E2_COARSE_EXIT_SEARCH_READY":
        raise RuntimeError("E4 requires E2_COARSE_EXIT_SEARCH_READY")

    expected = {
        r["role"]: (int(r["candidate_count"]), int(r["clean_5m_count"]))
        for r in readiness["regimes"]
    }

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    freeze_id, dataset_role, range_start, range_end, expected_rows = (
        manifest_identity(manifest)
    )

    coverage = CollectorCoverageSnapshotV02.load(coverage_path)
    coverage.validate_binding(
        freeze_id=freeze_id,
        range_start=range_start,
        range_end=range_end,
    )
    replay = ExitPathReplayV01(coverage.provider())
    _, selection_rows = load_locked_selection(selection_path)

    print("[1/5] Open immutable DEV-FREEZE-0002...", flush=True)
    conn = Phase1ReadOnlyAdapterV01.open_readonly(freeze_db)
    try:
        Phase1QuoteAwareAdapterV01.validate_schema(conn)
        if conn.execute("PRAGMA quick_check").fetchone()[0] != "ok":
            raise RuntimeError("freeze quick_check failed")

        actual_rows = int(
            conn.execute("SELECT COUNT(*) FROM pump_events").fetchone()[0]
        )
        if actual_rows != expected_rows:
            raise RuntimeError(
                f"freeze rows {actual_rows} != manifest {expected_rows}"
            )

        tokens = fetch_tokens(conn)
        entry_raw = fetch_entry_rows(conn, tokens)
        entry_events, skipped = Phase1QuoteAwareAdapterV01.normalize_rows(entry_raw)
        if skipped:
            raise RuntimeError(f"entry normalization skips: {skipped}")

        candidates = {}
        print(
            f"[2/5] Locked entry replay over {len(entry_events)} rows...",
            flush=True,
        )
        for row in selection_rows:
            role = str(row["role"])
            params = parameter_set_from_locked_row(row)
            cs, summary = extract_candidates(entry_events, params)
            if summary.run_count != summary.accounted_run_count:
                raise RuntimeError(f"{role}: terminal accounting incomplete")
            if summary.candidate_count != summary.candidate_state_count:
                raise RuntimeError(f"{role}: candidate lifecycle mismatch")
            if len(cs) != expected[role][0]:
                raise RuntimeError(
                    f"{role}: candidate count {len(cs)} != readiness {expected[role][0]}"
                )
            candidates[role] = cs
            print(f"  {role}: {len(cs)} candidates PASS", flush=True)

        candidate_mints = {
            c.signal.mint
            for cs in candidates.values()
            for c in cs
        }
        print(
            f"[3/5] Fetch full paths for {len(candidate_mints)} candidate mints...",
            flush=True,
        )
        path_raw = fetch_path_rows(conn, candidate_mints)
    finally:
        conn.close()

    path_events, skipped = Phase1QuoteAwareAdapterV01.normalize_rows(path_raw)
    if skipped:
        raise RuntimeError(f"path normalization skips: {skipped}")

    obs_by_mint = defaultdict(list)
    for event in path_events:
        obs = price_observation_from_event(event)
        if obs is not None:
            obs_by_mint[obs.mint].append(obs)

    print("[4/5] Rebuild exact clean 5m paths...", flush=True)
    clean_paths = {}
    for role, cs in candidates.items():
        clean = []
        for c in cs:
            path = replay.build_path(
                c.reference,
                obs_by_mint.get(c.signal.mint, ()),
                horizon_ms=300_000,
            )
            if path.status == ExitPathStatus.CLEAN:
                clean.append(path)

        if len(clean) != expected[role][1]:
            raise RuntimeError(
                f"{role}: clean count {len(clean)} != readiness {expected[role][1]}"
            )
        clean_paths[role] = clean
        print(f"  {role}: {len(clean)} clean 5m paths PASS", flush=True)

    print("[5/5] Evaluate locked E4 trailing family...", flush=True)
    results = []

    for role, paths in clean_paths.items():
        for activation_bps in TRAIL_ACTIVATION_BPS:
            for giveback_bps in TRAIL_GIVEBACK_BPS:
                exits = []
                exit_delays = []
                activation_delays = []
                fallback_ages = []
                activation_count = 0
                trail_exit_count = 0
                fallback_count = 0
                peak_at_trail_exit = []

                for path in paths:
                    activation, trail_exit, peak_bps = trailing_exit_at_or_before(
                        path,
                        activation_bps=activation_bps,
                        giveback_bps=giveback_bps,
                        horizon_ms=FALLBACK_TIME_STOP_MS,
                    )

                    if activation is not None:
                        activation_count += 1
                        activation_delays.append(
                            round(
                                (activation.observed_at - path.signal_at).total_seconds()
                                * 1000
                            )
                        )

                    if trail_exit is not None:
                        trail_exit_count += 1
                        exits.append(trail_exit.return_bps)
                        exit_delays.append(
                            round(
                                (trail_exit.observed_at - path.signal_at).total_seconds()
                                * 1000
                            )
                        )
                        peak_at_trail_exit.append(int(peak_bps))
                        continue

                    mark, target = fallback_mark_at_or_before(
                        path,
                        FALLBACK_TIME_STOP_MS,
                    )
                    if mark is None:
                        continue

                    fallback_count += 1
                    exits.append(mark.return_bps)
                    exit_delays.append(
                        round(
                            (mark.observed_at - path.signal_at).total_seconds() * 1000
                        )
                    )
                    fallback_ages.append(
                        round((target - mark.observed_at).total_seconds() * 1000)
                    )

                stats = summarize(exits)
                scored = len(exits)
                clean_count = len(paths)

                results.append(
                    {
                        "role": role,
                        "family": "OBSERVED_TRAILING_WITH_T15S_FALLBACK",
                        "variant_id": (
                            f"ACT{activation_bps//100:02d}_GB{giveback_bps//100:02d}"
                        ),
                        "activation_threshold_bps": activation_bps,
                        "giveback_bps": giveback_bps,
                        "fallback_time_stop_ms": FALLBACK_TIME_STOP_MS,
                        "clean_path_count": clean_count,
                        "scored_count": scored,
                        "unscored_count": clean_count - scored,
                        "activation_count": activation_count,
                        "activation_rate_clean_paths": (
                            activation_count / clean_count if clean_count else None
                        ),
                        "trail_exit_count": trail_exit_count,
                        "trail_exit_rate_clean_paths": (
                            trail_exit_count / clean_count if clean_count else None
                        ),
                        "trail_exit_rate_activated": (
                            trail_exit_count / activation_count
                            if activation_count else None
                        ),
                        "fallback_exit_count": fallback_count,
                        "exit_bps": stats,
                        "exit_delay_p50_ms": nearest_rank(exit_delays, 50),
                        "activation_delay_p50_ms": nearest_rank(activation_delays, 50),
                        "fallback_mark_age_p50_ms": nearest_rank(fallback_ages, 50),
                        "peak_at_trail_exit_p50_bps": nearest_rank(
                            peak_at_trail_exit, 50
                        ),
                    }
                )

            print(
                f"  {role}: activation {activation_bps/100:.0f}% -> "
                f"{len(TRAIL_GIVEBACK_BPS)} giveback variants done",
                flush=True,
            )

    return {
        "schema_version": "P3-E4-TRAILING-RESULT-0.1",
        "experiment_id": "EXP-0010",
        "design_id": "PHASE-3-E4-TRAILING-001",
        "freeze_id": freeze_id,
        "dataset_role": dataset_role,
        "untouched_oos": False,
        "entry_parameters_fixed": True,
        "readiness_tier": "E2_COARSE_EXIT_SEARCH_READY",
        "fixed_take_profit_used": False,
        "hard_stop_used": False,
        "partial_take_profit_used": False,
        "multiple_take_profit_used": False,
        "T5s_sensitivity_used": False,
        "fees_modeled": False,
        "slippage_modeled": False,
        "latency_modeled": False,
        "gross_reference_returns_only": True,
        "profitability_claim_allowed": False,
        "results": results,
    }
