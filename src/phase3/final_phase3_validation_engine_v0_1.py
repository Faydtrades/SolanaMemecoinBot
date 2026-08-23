from __future__ import annotations

from collections import Counter, defaultdict
from datetime import timedelta
import hashlib
import json
import math
from pathlib import Path
from statistics import fmean
from typing import Any

from phase2.data_coverage_root_cause_v0_1_1 import us_to_iso
from phase2.phase1_readonly_adapter_v0_1 import Phase1ReadOnlyAdapterV01
from phase2.quote_aware_price_v0_1 import Phase1QuoteAwareAdapterV01
from phase3.collector_coverage_v0_2 import CollectorCoverageSnapshotV02
from phase3.e5_small_interaction_engine_v0_1 import evaluate_variant_on_path
from phase3.exit_path_replay_v0_1 import ExitPathReplayV01, ExitPathStatus
from phase3.final_phase3_validation_design_v0_1 import DESIGN
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


def canonical_hash(payload: Any) -> str:
    blob = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


def nearest_rank(values: list[int], p: int) -> int | None:
    if not values:
        return None
    s = sorted(values)
    rank = max(1, math.ceil((p / 100) * len(s)))
    return s[rank - 1]


def summarize(values: list[int]) -> dict[str, Any]:
    if not values:
        return {
            "n": 0, "p10": None, "p25": None, "p50": None,
            "p75": None, "p90": None, "mean": None,
            "positive_rate": None, "negative_rate": None,
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
    for bi, off in enumerate(range(0, len(tokens), chunk), 1):
        batch = tokens[off:off + chunk]
        values_sql = ",".join("(?,?)" for _ in batch)
        args = []
        for t in batch:
            args.extend([
                t["mint"],
                us_to_iso(t["t0_us"] + ENTRY_WINDOW_MS * 1000),
            ])
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
    for bi, off in enumerate(range(0, len(ms), chunk), 1):
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


def finalist_variant(candidate: dict[str, Any]) -> dict[str, Any]:
    return {
        "variant_id": candidate["variant_id"],
        "role": candidate["track"],
        "fallback_ms": int(candidate["fallback_ms"]),
        "tp_bps": candidate.get("tp_bps"),
        "trail_activation_bps": candidate.get("trail_activation_bps"),
        "trail_giveback_bps": candidate.get("trail_giveback_bps"),
        "hard_stop_bps": candidate.get("hard_stop_bps"),
    }


def evaluate_cached_paths(clean_paths: dict[str, list[Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    for role in ("CONTROL", "ROBUST_1", "ROBUST_2", "ROBUST_3"):
        paths = clean_paths[role]
        for candidate in DESIGN["locked_candidates"]:
            variant = finalist_variant(candidate)

            exits: list[int] = []
            delays: list[int] = []
            fallback_ages: list[int] = []
            activation_delays: list[int] = []
            reasons = Counter()

            for path in paths:
                ev = evaluate_variant_on_path(path, variant)
                if ev is None:
                    continue
                point = ev["exit_point"]
                exits.append(int(point.return_bps))
                delays.append(
                    round(
                        (point.observed_at - path.signal_at).total_seconds() * 1000
                    )
                )
                reasons[ev["reason"]] += 1

                if ev["fallback_mark_age_ms"] is not None:
                    fallback_ages.append(int(ev["fallback_mark_age_ms"]))

                if ev["activation_at"] is not None:
                    activation_delays.append(
                        round(
                            (ev["activation_at"] - path.signal_at).total_seconds() * 1000
                        )
                    )

            stats = summarize(exits)
            scored = len(exits)
            clean_count = len(paths)

            rows.append({
                "role": role,
                "candidate_id": candidate["candidate_id"],
                "track": candidate["track"],
                "variant_id": candidate["variant_id"],
                "fallback_ms": variant["fallback_ms"],
                "tp_bps": variant["tp_bps"],
                "trail_activation_bps": variant["trail_activation_bps"],
                "trail_giveback_bps": variant["trail_giveback_bps"],
                "hard_stop_bps": variant["hard_stop_bps"],
                "clean_path_count": clean_count,
                "scored_count": scored,
                "unscored_count": clean_count - scored,
                "exit_reason_counts": dict(sorted(reasons.items())),
                "exit_bps": stats,
                "exit_delay_p50_ms": nearest_rank(delays, 50),
                "activation_delay_p50_ms": nearest_rank(activation_delays, 50),
                "fallback_mark_age_p50_ms": nearest_rank(fallback_ages, 50),
            })

    return rows


def comparable_row(row: dict[str, Any]) -> dict[str, Any]:
    """Fields that must reproduce exactly from the audited EXP-0011 row."""
    return {
        "role": row["role"],
        "variant_id": row["variant_id"],
        "fallback_ms": row["fallback_ms"],
        "tp_bps": row["tp_bps"],
        "trail_activation_bps": row["trail_activation_bps"],
        "trail_giveback_bps": row["trail_giveback_bps"],
        "hard_stop_bps": row["hard_stop_bps"],
        "clean_path_count": row["clean_path_count"],
        "scored_count": row["scored_count"],
        "unscored_count": row["unscored_count"],
        "exit_reason_counts": row["exit_reason_counts"],
        "exit_bps": row["exit_bps"],
        "exit_delay_p50_ms": row["exit_delay_p50_ms"],
        "activation_delay_p50_ms": row["activation_delay_p50_ms"],
        "fallback_mark_age_p50_ms": row["fallback_mark_age_p50_ms"],
    }


def run_final_validation(
    *,
    freeze_db: Path,
    manifest_path: Path,
    coverage_path: Path,
    selection_path: Path,
    readiness_path: Path,
    e5_results_path: Path,
) -> dict[str, Any]:
    readiness = json.loads(readiness_path.read_text(encoding="utf-8"))
    if readiness["readiness"]["tier"] != "E2_COARSE_EXIT_SEARCH_READY":
        raise RuntimeError("final validation requires E2 readiness")

    expected_entry = {
        k: int(v) for k, v in DESIGN["exact_expected_entry_counts"].items()
    }
    expected_clean = {
        k: int(v) for k, v in DESIGN["exact_expected_clean_5m_counts"].items()
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

    print("[1/6] Open immutable DEV-FREEZE-0002 and validate source...", flush=True)
    conn = Phase1ReadOnlyAdapterV01.open_readonly(freeze_db)
    try:
        Phase1QuoteAwareAdapterV01.validate_schema(conn)
        quick = conn.execute("PRAGMA quick_check").fetchone()[0]
        if quick != "ok":
            raise RuntimeError(f"freeze quick_check failed: {quick}")

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

        print("[2/6] Independently replay locked entry...", flush=True)
        candidates = {}
        for row in selection_rows:
            role = str(row["role"])
            params = parameter_set_from_locked_row(row)
            cs, summary = extract_candidates(entry_events, params)

            if summary.run_count != summary.accounted_run_count:
                raise RuntimeError(f"{role}: terminal accounting incomplete")
            if summary.candidate_count != summary.candidate_state_count:
                raise RuntimeError(f"{role}: candidate lifecycle mismatch")
            if len(cs) != expected_entry[role]:
                raise RuntimeError(
                    f"{role}: candidate count {len(cs)} != {expected_entry[role]}"
                )

            candidates[role] = cs
            print(
                f"  {role}: {len(cs)} candidates EXACT PASS",
                flush=True,
            )

        candidate_mints = {
            c.signal.mint
            for cs in candidates.values()
            for c in cs
        }
        print(
            f"[3/6] Fetch complete frozen paths for {len(candidate_mints)} candidate mints...",
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

    print("[4/6] Rebuild exact clean 5m paths...", flush=True)
    clean_paths = {}
    for role in ("CONTROL", "ROBUST_1", "ROBUST_2", "ROBUST_3"):
        clean = []
        for c in candidates[role]:
            path = replay.build_path(
                c.reference,
                obs_by_mint.get(c.signal.mint, ()),
                horizon_ms=300_000,
            )
            if path.status == ExitPathStatus.CLEAN:
                clean.append(path)

        if len(clean) != expected_clean[role]:
            raise RuntimeError(
                f"{role}: clean count {len(clean)} != {expected_clean[role]}"
            )
        clean_paths[role] = clean
        print(
            f"  {role}: {len(clean)} clean 5m paths EXACT PASS",
            flush=True,
        )

    print("[5/6] Evaluate locked finalists twice over identical cached paths...", flush=True)
    pass1 = evaluate_cached_paths(clean_paths)
    pass2 = evaluate_cached_paths(clean_paths)
    h1 = canonical_hash(pass1)
    h2 = canonical_hash(pass2)
    if h1 != h2:
        raise RuntimeError(f"determinism hash mismatch: {h1} != {h2}")
    print(f"  cached-path deterministic SHA256: {h1}", flush=True)

    print("[6/6] Compare finalists to already-audited EXP-0011 rows...", flush=True)
    e5 = json.loads(e5_results_path.read_text(encoding="utf-8"))
    wanted_variants = {
        "E3_TP10_T15",
        "E4_ACT10_GB03_T15",
        "SENS_TP20_T5",
    }
    e5_lookup = {
        (r["role"], r["variant_id"]): comparable_row(r)
        for r in e5["results"]
        if r["variant_id"] in wanted_variants
    }

    mismatches = []
    reproduction_rows = []
    for row in pass1:
        key = (row["role"], row["variant_id"])
        expected = e5_lookup.get(key)
        if expected is None:
            mismatches.append({
                "key": list(key),
                "reason": "missing EXP-0011 reference row",
            })
            continue

        actual = comparable_row(row)
        ok = actual == expected
        reproduction_rows.append({
            "role": row["role"],
            "candidate_id": row["candidate_id"],
            "variant_id": row["variant_id"],
            "exact_match_EXP_0011": ok,
        })
        if not ok:
            mismatches.append({
                "key": list(key),
                "expected": expected,
                "actual": actual,
            })

    if mismatches:
        raise RuntimeError(
            "EXP-0011 exact reproduction failed: "
            + json.dumps(mismatches[:3], sort_keys=True)
        )

    if len(reproduction_rows) != 12:
        raise RuntimeError(
            f"expected 12 finalist/regime rows, got {len(reproduction_rows)}"
        )

    return {
        "schema_version": "P3-FINAL-VALIDATION-RESULT-0.1",
        "experiment_id": "EXP-0012",
        "design_id": "PHASE-3-FINAL-VALIDATION-001",
        "freeze_id": freeze_id,
        "dataset_role": dataset_role,
        "untouched_oos": False,
        "source_validation": {
            "sqlite_quick_check": "PASS",
            "coverage_binding": "PASS",
            "manifest_row_count": expected_rows,
            "actual_row_count": actual_rows,
        },
        "entry_candidate_counts": {
            role: len(candidates[role])
            for role in ("CONTROL", "ROBUST_1", "ROBUST_2", "ROBUST_3")
        },
        "clean_5m_counts": {
            role: len(clean_paths[role])
            for role in ("CONTROL", "ROBUST_1", "ROBUST_2", "ROBUST_3")
        },
        "locked_finalist_rows": pass1,
        "cached_path_evaluation_hash_pass1": h1,
        "cached_path_evaluation_hash_pass2": h2,
        "cached_path_determinism": "PASS",
        "EXP_0011_exact_reproduction": "PASS",
        "EXP_0011_reproduction_rows": reproduction_rows,
        "parameter_search_performed": False,
        "candidate_reselection_performed": False,
        "further_exit_tuning_performed": False,
        "fees_modeled": False,
        "slippage_modeled": False,
        "latency_modeled": False,
        "profitability_claim_allowed": False,
        "phase3_final_validation_status": "PHASE3_FINAL_VALIDATION_PASS",
        "phase4_build_allowed": True,
    }
