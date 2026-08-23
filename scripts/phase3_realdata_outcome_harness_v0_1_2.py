from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import csv
from dataclasses import asdict
from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
import sqlite3
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from phase2.quote_aware_price_v0_1 import Phase1QuoteAwareAdapterV01
from phase2.phase1_readonly_adapter_v0_1 import Phase1ReadOnlyAdapterV01
from phase3.collector_coverage_v0_1 import CollectorCoverageSnapshotV01, parse_utc
from phase3.outcome_replay_v0_1_3 import HorizonStatus, OutcomeReplayV013
from phase3.phase2_outcome_adapter_v0_1_1 import (
    extract_candidates,
    foundation_probe_parameters,
    price_observation_from_event,
    validate_probe_against_locked_search,
)


HORIZON_LABELS = {
    5_000: "5s",
    15_000: "15s",
    30_000: "30s",
    60_000: "60s",
    120_000: "2m",
    300_000: "5m",
}


def file_sha256(path: Path) -> str:
    h = sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def percentile(values: list[int], p: int) -> int | None:
    if not values:
        return None
    ordered = sorted(values)
    rank = max(1, (p * len(ordered) + 99) // 100)
    return ordered[rank - 1]


def summarize(values: list[int]) -> dict[str, int | None]:
    return {
        "n": len(values),
        "p25": percentile(values, 25),
        "p50": percentile(values, 50),
        "p75": percentile(values, 75),
        "p90": percentile(values, 90),
        "p95": percentile(values, 95),
        "min": min(values) if values else None,
        "max": max(values) if values else None,
    }


def primitive_outcome(outcome) -> dict[str, object]:
    return {
        "candidate_id": outcome.candidate_id,
        "mint": outcome.mint,
        "signal_at": outcome.signal_at.isoformat(),
        "price_identity": outcome.price_identity,
        "reference_price_numerator_raw": outcome.reference_price_numerator_raw,
        "reference_price_denominator_raw": outcome.reference_price_denominator_raw,
        "mfe_bps_5m": outcome.mfe_bps_5m,
        "mae_bps_5m": outcome.mae_bps_5m,
        "extrema_observation_count_5m": outcome.extrema_observation_count_5m,
        "extrema_coverage_status_5m": outcome.extrema_coverage_status_5m.value,
        "extrema_complete_5m": outcome.extrema_complete_5m,
        "gap_recovery_observation_count_5m": outcome.gap_recovery_observation_count_5m,
        "identity_mismatch_observation_count_5m": outcome.identity_mismatch_observation_count_5m,
        "continuity_break_observation_count_5m": outcome.continuity_break_observation_count_5m,
        "horizons": [
            {
                "horizon_ms": h.horizon_ms,
                "target_at": h.target_at.isoformat(),
                "status": h.status.value,
                "coverage_status": h.coverage_status.value,
                "mark_at": h.mark_at.isoformat() if h.mark_at else None,
                "mark_ingest_seq": h.mark_ingest_seq,
                "mark_age_ms": h.mark_age_ms,
                "return_bps": h.return_bps,
                "comparable_observation_count": h.comparable_observation_count,
                "gap_recovery_observation_count": h.gap_recovery_observation_count,
                "mismatched_identity_count": h.mismatched_identity_count,
                "continuity_break_count": h.continuity_break_count,
            }
            for h in outcome.horizons
        ],
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    freeze_dir = (
        PROJECT_ROOT / "data" / "research" / "phase2_research_freeze_v0_1"
    )
    ap.add_argument(
        "--freeze-db",
        default=str(freeze_dir / "phase2_development_dataset_v0_1.sqlite3"),
    )
    ap.add_argument(
        "--freeze-manifest",
        default=str(freeze_dir / "phase2_development_dataset_manifest_v0_1.json"),
    )
    ap.add_argument(
        "--locked-search",
        default=str(freeze_dir / "phase2_locked_param_search_001.json"),
    )
    ap.add_argument(
        "--coverage",
        default=str(
            PROJECT_ROOT
            / "data"
            / "research"
            / "phase3"
            / "collector_coverage_DEV-FREEZE-0001_v0_1.json"
        ),
    )
    ap.add_argument(
        "--out-dir",
        default=str(PROJECT_ROOT / "data" / "research" / "phase3" / "EXP-0001"),
    )
    args = ap.parse_args()

    freeze_db = Path(args.freeze_db).resolve()
    manifest_path = Path(args.freeze_manifest).resolve()
    locked_path = Path(args.locked_search).resolve()
    coverage_path = Path(args.coverage).resolve()
    out_dir = Path(args.out_dir).resolve()

    for p in (freeze_db, manifest_path, locked_path, coverage_path):
        if not p.exists():
            raise FileNotFoundError(p)

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    dataset = manifest["dataset"]
    locked = json.loads(locked_path.read_text(encoding="utf-8"))

    actual_db_sha = file_sha256(freeze_db)
    if actual_db_sha != dataset["snapshot_sqlite_sha256"]:
        raise RuntimeError("DEV-FREEZE-0001 SQLite SHA256 mismatch")
    actual_locked_sha = sha256(
        canonical_json(locked).encode("utf-8")
    ).hexdigest()
    if actual_locked_sha != manifest["parameter_search"]["locked_search_space_sha256"]:
        raise RuntimeError("PHASE-2-PARAM-SEARCH-001 deterministic SHA256 mismatch")

    coverage = CollectorCoverageSnapshotV01.load(coverage_path)
    coverage.validate_binding(
        freeze_id=str(dataset["freeze_id"]),
        range_start=parse_utc(str(dataset["t0_observed_range_utc"]["start"])),
        range_end=parse_utc(str(dataset["source_cutoff"]["max_observed_at_utc"])),
    )

    conn = Phase1ReadOnlyAdapterV01.open_readonly(freeze_db)
    try:
        quick = conn.execute("PRAGMA quick_check").fetchone()[0]
        if quick != "ok":
            raise RuntimeError(f"freeze SQLite quick_check={quick!r}")
        Phase1QuoteAwareAdapterV01.validate_schema(conn)
        rows = Phase1QuoteAwareAdapterV01.fetch_all_bot_truth_rows(conn)
    finally:
        conn.close()

    events, skipped = Phase1QuoteAwareAdapterV01.normalize_rows(rows)
    if skipped:
        raise RuntimeError(f"unexpected normalization skips: {skipped}")
    if len(events) != int(dataset["frozen_pump_event_rows"]):
        raise RuntimeError("normalized event count != frozen manifest row count")

    mints = {e.base.mint for e in events if e.base.event_type.value in {"BUY", "SELL"}}
    if len(mints) != int(dataset["eligible_token_count"]):
        raise RuntimeError("eligible mint count != freeze manifest")

    params = foundation_probe_parameters()
    validate_probe_against_locked_search(params, locked)

    candidates, extraction = extract_candidates(events, params)
    if extraction.run_count != int(dataset["eligible_token_count"]):
        raise RuntimeError("strategy run count != frozen eligible token count")
    if (
        extraction.expired_count
        + extraction.invalidated_count
        + extraction.candidate_state_count
        != extraction.run_count
    ):
        raise RuntimeError("strategy lifecycle not terminal-complete after SCLOCK-0.2")
    if extraction.candidate_count != extraction.candidate_state_count:
        raise RuntimeError("candidate emission count != candidate terminal state count")

    obs_by_mint: dict[str, list] = defaultdict(list)
    for event in events:
        obs = price_observation_from_event(event)
        if obs is not None:
            obs_by_mint[obs.mint].append(obs)

    replay = OutcomeReplayV013(coverage.provider())
    outcomes = [
        replay.replay(c.reference, obs_by_mint.get(c.signal.mint, ()))
        for c in candidates
    ]

    # Cheap deterministic Phase-3 replay check using the same already-generated
    # causal candidates; Phase 2 candidate determinism was separately validated.
    outcomes_second = [
        replay.replay(c.reference, obs_by_mint.get(c.signal.mint, ()))
        for c in candidates
    ]
    rows_a = [primitive_outcome(o) for o in outcomes]
    rows_b = [primitive_outcome(o) for o in outcomes_second]
    digest_a = sha256(canonical_json(rows_a).encode("utf-8")).hexdigest()
    digest_b = sha256(canonical_json(rows_b).encode("utf-8")).hexdigest()
    if digest_a != digest_b:
        raise RuntimeError("Phase-3 outcome replay is non-deterministic")

    status_by_horizon: dict[str, dict[str, int]] = {}
    clean_return_summary: dict[str, dict[str, int | None]] = {}
    mark_age_summary: dict[str, dict[str, int | None]] = {}

    for horizon_ms, label in HORIZON_LABELS.items():
        hs = [next(h for h in o.horizons if h.horizon_ms == horizon_ms) for o in outcomes]
        status_by_horizon[label] = dict(sorted(Counter(h.status.value for h in hs).items()))
        clean_returns = [
            int(h.return_bps)
            for h in hs
            if h.status == HorizonStatus.OBSERVABLE and h.return_bps is not None
        ]
        clean_ages = [
            int(h.mark_age_ms)
            for h in hs
            if h.status == HorizonStatus.OBSERVABLE and h.mark_age_ms is not None
        ]
        clean_return_summary[label] = summarize(clean_returns)
        mark_age_summary[label] = summarize(clean_ages)

    complete_extrema = [o for o in outcomes if o.extrema_complete_5m]
    mfe_values = [int(o.mfe_bps_5m) for o in complete_extrema if o.mfe_bps_5m is not None]
    mae_values = [int(o.mae_bps_5m) for o in complete_extrema if o.mae_bps_5m is not None]

    identity_counts = Counter(o.price_identity for o in outcomes)
    report_core = {
        "schema_version": "P3-3.1-REALDATA-0.1.2",
        "experiment_id": "EXP-0001",
        "experiment_role": "OUTCOME_REPLAY_FOUNDATION_NO_OPTIMIZATION",
        "dataset": {
            "freeze_id": dataset["freeze_id"],
            "dataset_role": dataset["dataset_role"],
            "untouched_oos": False,
            "eligible_tokens": dataset["eligible_token_count"],
            "frozen_rows": dataset["frozen_pump_event_rows"],
            "snapshot_sqlite_sha256": actual_db_sha,
            "locked_search_sha256": actual_locked_sha,
        },
        "versions": {
            "strategy": "FirstPullback v1.1 / FirstPullbackStrategyV02",
            "feature_engine": "DenominationAwareFeatureEngineV01",
            "quote_price": "QuoteAwarePriceV01",
            "strategy_clock": "SCLOCK-0.2",
            "outcome_replay": "phase3_outcome_replay_v0.1.3",
            "coverage": "P3COV-0.1",
        },
        "foundation_probe": {
            "parameter_set_id": params.parameter_set_id,
            "purpose": "mechanical foundation probe only; not optimized/trading-selected",
            "max_entry_age_ms": params.max_entry_age_ms,
            "impulse": params.impulse_parameters,
            "pullback": params.pullback_parameters,
            "buyer_response": params.buyer_response_parameters,
            "reclaim": params.reclaim_parameters,
            "runaway": params.runaway_entry_parameters,
        },
        "candidate_extraction": asdict(extraction),
        "candidate_price_identity_counts": dict(sorted(identity_counts.items())),
        "outcome_observability": {
            "horizon_status_counts": status_by_horizon,
            "clean_horizon_return_bps": clean_return_summary,
            "clean_horizon_mark_age_ms": mark_age_summary,
            "complete_5m_extrema_count": len(complete_extrema),
            "complete_mfe_bps_5m": summarize(mfe_values),
            "complete_mae_bps_5m": summarize(mae_values),
        },
        "outcome_semantics": {
            "horizon_mark_rule": "latest fresh same-identity post-signal observation at_or_before target",
            "collector_coverage_required": "COMPLETE signal-to-target",
            "no_future_observation_policy": "NO_FUTURE_PRICE; never synthesize 0/flat",
            "mark_age_recorded": True,
            "hidden_freshness_threshold": False,
            "pump_curve_completion_policy": (
                "real_token_reserve_raw<=0 creates a market-continuity boundary; "
                "post-boundary horizons are not clean-observable without migrated-venue continuation"
            ),
            "mfe_mae_reference_baseline_bps": 0,
        },
        "performance_claims": {
            "exit_optimization_performed": False,
            "tp_sl_tuning_performed": False,
            "fees_modeled": False,
            "slippage_modeled": False,
            "latency_modeled": False,
            "realistic_net_pnl_available": False,
            "profitability_claim_allowed": False,
        },
        "determinism": {
            "outcome_rows_sha256": digest_a,
            "second_pass_sha256": digest_b,
            "pass": True,
        },
    }
    report = dict(report_core)
    report["deterministic_report_core_sha256"] = sha256(
        canonical_json(report_core).encode("utf-8")
    ).hexdigest()

    exp_manifest = {
        "manifest_schema_version": "EXPM-0.1-P3EXT",
        "experiment_id": "EXP-0001",
        "status": "COMPLETE_PHASE3_1_FOUNDATION_CORRECTED_V2",
        "hypothesis": (
            "A fixed causal FirstPullback v1.1 foundation probe can be replayed on "
            "DEV-FREEZE-0001 and post-signal outcomes can be labeled deterministically "
            "without converting missing/uncertain future data into synthetic returns."
        ),
        "dataset": report["dataset"],
        "strategy": {
            "name": "FirstPullback",
            "version": "v1.1",
            "strategy_clock": "SCLOCK-0.2",
        },
        "search_space": {
            "decision_id": "PHASE-2-PARAM-SEARCH-001",
            "experiment_block": "FOUNDATION_OUTCOME_REPLAY_NO_OPTIMIZATION",
            "locked_search_space_sha256": actual_locked_sha,
        },
        "parameters": report["foundation_probe"],
        "execution_model": {
            "version": "NOT_DEFINED_YET",
            "reference_position_size_sol": 0.10,
            "fee_model": "NOT_DEFINED_YET",
            "slippage_model": "NOT_DEFINED_YET",
            "latency_model": "NOT_DEFINED_YET",
            "failed_execution_behavior": "NOT_DEFINED_YET",
        },
        "result_metrics": {
            "gross_raw": {
                "note": "Outcome labels only; no exit/trade PnL is defined in Phase 3.1.",
                "candidate_count": extraction.candidate_count,
                "horizon_status_counts": status_by_horizon,
                "clean_horizon_return_bps": clean_return_summary,
                "complete_mfe_bps_5m": summarize(mfe_values),
                "complete_mae_bps_5m": summarize(mae_values),
            },
            "realistic_net": {},
            "execution_quality": {},
        },
        "notes": (
            "DEV-FREEZE-0001 is development/discovery data, not untouched OOS. "
            "Foundation probe parameters are not selected trading thresholds and "
            "must not be interpreted as optimized."
        ),
        "decision": "PHASE_3_1_FOUNDATION_VALIDATED_IF_RESULT_PASS",
    }

    out_dir.mkdir(parents=True, exist_ok=True)
    report_path = out_dir / "phase3_1_outcome_replay_report_v0_1_2.json"
    manifest_out = out_dir / "EXP-0001_manifest_v0_1_2.json"
    csv_path = out_dir / "phase3_1_candidate_outcomes_v0_1_2.csv"

    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    manifest_out.write_text(json.dumps(exp_manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    fields = [
        "candidate_id", "mint", "signal_at", "price_identity",
        "mfe_bps_5m", "mae_bps_5m", "extrema_complete_5m",
        "extrema_coverage_status_5m",
    ]
    for label in HORIZON_LABELS.values():
        fields += [
            f"{label}_status", f"{label}_return_bps",
            f"{label}_mark_age_ms", f"{label}_coverage_status",
        ]
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for o in outcomes:
            row = {
                "candidate_id": o.candidate_id,
                "mint": o.mint,
                "signal_at": o.signal_at.isoformat(),
                "price_identity": o.price_identity,
                "mfe_bps_5m": o.mfe_bps_5m,
                "mae_bps_5m": o.mae_bps_5m,
                "extrema_complete_5m": o.extrema_complete_5m,
                "extrema_coverage_status_5m": o.extrema_coverage_status_5m.value,
            }
            for h in o.horizons:
                label = HORIZON_LABELS[h.horizon_ms]
                row[f"{label}_status"] = h.status.value
                row[f"{label}_return_bps"] = h.return_bps
                row[f"{label}_mark_age_ms"] = h.mark_age_ms
                row[f"{label}_coverage_status"] = h.coverage_status.value
            w.writerow(row)

    print("PHASE 3.1 REAL-DATA OUTCOME HARNESS v0.1.2")
    print("=" * 54)
    print(f"Freeze                         : {dataset['freeze_id']}")
    print(f"Normalized frozen events       : {len(events)}")
    print(f"Strategy runs                  : {extraction.run_count}")
    print(f"Candidates                     : {extraction.candidate_count}")
    print(f"Expired / invalidated          : {extraction.expired_count} / {extraction.invalidated_count}")
    print(f"Candidate price identities     : {dict(sorted(identity_counts.items()))}")
    for label in HORIZON_LABELS.values():
        print(f"{label:>4} horizon status             : {status_by_horizon[label]}")
    print(f"Complete 5m MFE/MAE candidates : {len(complete_extrema)}")
    print(f"Outcome replay deterministic   : PASS")
    print(f"Exit optimization              : NO")
    print(f"PnL / fees / slippage / latency: NOT MODELED")
    print(f"Report                         : {report_path}")
    print(f"CSV                            : {csv_path}")
    print(f"Experiment manifest            : {manifest_out}")
    print("RESULT                         : PASS")


if __name__ == "__main__":
    main()
