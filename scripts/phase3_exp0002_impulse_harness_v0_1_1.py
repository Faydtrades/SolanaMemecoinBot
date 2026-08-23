from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import csv
from dataclasses import asdict
from hashlib import sha256
import json
import os
from pathlib import Path
import sys
import time

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from phase2.phase1_readonly_adapter_v0_1 import Phase1ReadOnlyAdapterV01
from phase2.quote_aware_price_v0_1 import Phase1QuoteAwareAdapterV01
from phase2.strategy_first_pullback_v0_2 import FirstPullbackStrategyV02
from phase3.collector_coverage_v0_1 import CollectorCoverageSnapshotV01, parse_utc
from phase3.impulse_block_research_v0_1 import (
    BLOCK_ID,
    EXPERIMENT_ID,
    FOUNDATION_DOWNSTREAM,
    PARETO_METRICS,
    SCHEMA_VERSION,
    generate_impulse_combinations,
    parameter_set_for_impulse,
    pareto_frontier,
    validate_parameter_set_against_locked_search,
)
from phase3.outcome_replay_v0_1_3 import HorizonStatus, OutcomeReplayV013
from phase3.phase2_outcome_adapter_v0_1_2 import (
    extract_candidates,
    price_observation_from_event,
)


HORIZONS = {
    5_000: "5s",
    15_000: "15s",
    30_000: "30s",
    60_000: "60s",
    120_000: "2m",
    300_000: "5m",
}


def canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def stable_sha(value: object) -> str:
    return sha256(canonical_json(value).encode("utf-8")).hexdigest()


def file_sha256(path: Path) -> str:
    h = sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


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


def verify_registry(registry_path: Path) -> dict[str, object]:
    registry = json.loads(registry_path.read_text(encoding="utf-8"))
    if registry.get("next_experiment_id") != "EXP-0002":
        raise RuntimeError("canonical registry next_experiment_id must be EXP-0002")
    experiments = registry.get("experiments")
    if not isinstance(experiments, list):
        raise RuntimeError("invalid experiment registry")
    exp1 = [x for x in experiments if isinstance(x, dict) and x.get("experiment_id") == "EXP-0001"]
    if len(exp1) != 1 or exp1[0].get("status") != "COMPLETE":
        raise RuntimeError("EXP-0001 must be registered COMPLETE before EXP-0002")
    core = dict(registry)
    stored = core.pop("deterministic_registry_sha256", None)
    if stored != stable_sha(core):
        raise RuntimeError("experiment registry deterministic hash invalid")
    return registry


def candidate_signature(candidates) -> str:
    rows = [
        (
            c.signal.mint,
            c.signal.generated_at_us,
            c.reference.price_identity,
            c.reference.price_numerator_raw,
            c.reference.price_denominator_raw,
        )
        for c in candidates
    ]
    return stable_sha(rows)


def outcome_long_row(combo, outcome) -> dict[str, object]:
    row = {
        "parameter_set_id": combo.parameter_set_id,
        "combo_index": combo.index,
        "min_return_bps": combo.min_return_bps,
        "min_trades_since_t0": combo.min_trades_since_t0,
        "min_unique_buyers_since_t0": combo.min_unique_buyers_since_t0,
        "candidate_id": outcome.candidate_id,
        "mint": outcome.mint,
        "signal_at": outcome.signal_at.isoformat(),
        "price_identity": outcome.price_identity,
        "mfe_bps_5m": outcome.mfe_bps_5m,
        "mae_bps_5m": outcome.mae_bps_5m,
        "extrema_complete_5m": outcome.extrema_complete_5m,
    }
    for h in outcome.horizons:
        label = HORIZONS[h.horizon_ms]
        row[f"{label}_status"] = h.status.value
        row[f"{label}_return_bps"] = h.return_bps
        row[f"{label}_mark_age_ms"] = h.mark_age_ms
    return row


def summarize_combo(combo, extraction, outcomes) -> dict[str, object]:
    row: dict[str, object] = {
        "parameter_set_id": combo.parameter_set_id,
        "combo_index": combo.index,
        "min_return_bps": combo.min_return_bps,
        "min_trades_since_t0": combo.min_trades_since_t0,
        "min_unique_buyers_since_t0": combo.min_unique_buyers_since_t0,
        "run_count": extraction.run_count,
        "candidate_count": extraction.candidate_count,
        "expired_count": extraction.expired_count,
        "invalidated_count": extraction.invalidated_count,
        "rejected_count": extraction.rejected_count,
        "trade_count": extraction.trade_count,
        "candidate_state_count": extraction.candidate_state_count,
        "accounted_run_count": extraction.accounted_run_count,
    }

    for horizon_ms, label in HORIZONS.items():
        hs = [next(h for h in o.horizons if h.horizon_ms == horizon_ms) for o in outcomes]
        counts = Counter(h.status.value for h in hs)
        clean = [
            int(h.return_bps)
            for h in hs
            if h.status == HorizonStatus.OBSERVABLE and h.return_bps is not None
        ]
        s = summarize(clean)
        row[f"h{label}_observable_n"] = int(counts.get("OBSERVABLE", 0))
        row[f"h{label}_p25_bps"] = s["p25"]
        row[f"h{label}_p50_bps"] = s["p50"]
        row[f"h{label}_p75_bps"] = s["p75"]

    complete = [o for o in outcomes if o.extrema_complete_5m]
    mfe = [int(o.mfe_bps_5m) for o in complete if o.mfe_bps_5m is not None]
    mae = [int(o.mae_bps_5m) for o in complete if o.mae_bps_5m is not None]
    row["complete_5m_extrema_n"] = len(complete)
    row["complete_5m_mfe_p50_bps"] = summarize(mfe)["p50"]
    row["complete_5m_mae_p50_bps"] = summarize(mae)["p50"]

    # Aliases used by deterministic Pareto contract.
    row["h15_observable_n"] = row["h15s_observable_n"]
    row["h15_p50_bps"] = row["h15s_p50_bps"]
    row["h30_observable_n"] = row["h30s_observable_n"]
    row["h30_p50_bps"] = row["h30s_p50_bps"]
    return row


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fields = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)



CHECKPOINT_SCHEMA = "P3-EXP0002-A-WORK-0.1.1"


def write_json_atomic(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8", newline="\n") as f:
        json.dump(payload, f, indent=2, sort_keys=True, ensure_ascii=True)
        f.write("\n")
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def checkpoint_path(work_dir: Path, combo_index: int) -> Path:
    return work_dir / f"combo_{combo_index:03d}.json"


def validate_checkpoint(payload: dict[str, object], combo) -> None:
    if payload.get("schema_version") != CHECKPOINT_SCHEMA:
        raise RuntimeError(f"combo {combo.index}: checkpoint schema mismatch")
    if payload.get("experiment_id") != "EXP-0002":
        raise RuntimeError(f"combo {combo.index}: checkpoint experiment mismatch")
    if int(payload.get("combo_index", -1)) != combo.index:
        raise RuntimeError(f"combo {combo.index}: checkpoint index mismatch")
    if payload.get("parameter_set_id") != combo.parameter_set_id:
        raise RuntimeError(f"combo {combo.index}: checkpoint parameter_set_id mismatch")
    expected = {
        "min_return_bps": combo.min_return_bps,
        "min_trades_since_t0": combo.min_trades_since_t0,
        "min_unique_buyers_since_t0": combo.min_unique_buyers_since_t0,
    }
    if payload.get("impulse_parameters") != expected:
        raise RuntimeError(f"combo {combo.index}: checkpoint parameter mismatch")

    summary = payload.get("summary")
    rows = payload.get("outcome_rows")
    if not isinstance(summary, dict) or not isinstance(rows, list):
        raise RuntimeError(f"combo {combo.index}: malformed checkpoint")

    if int(summary.get("run_count", -1)) != 3901:
        raise RuntimeError(f"combo {combo.index}: checkpoint run_count mismatch")
    if int(summary.get("candidate_count", -1)) != len(rows):
        raise RuntimeError(f"combo {combo.index}: checkpoint outcome-row mismatch")
    if int(summary.get("candidate_count", -1)) != int(summary.get("candidate_state_count", -2)):
        raise RuntimeError(f"combo {combo.index}: checkpoint candidate lifecycle mismatch")
    if int(summary.get("accounted_run_count", -1)) != int(summary.get("run_count", -2)):
        raise RuntimeError(f"combo {combo.index}: checkpoint terminal accounting mismatch")


def save_combo_checkpoint(
    work_dir: Path,
    combo,
    candidate_sig: str,
    summary_row: dict[str, object],
    outcome_rows: list[dict[str, object]],
) -> None:
    payload = {
        "schema_version": CHECKPOINT_SCHEMA,
        "experiment_id": "EXP-0002",
        "combo_index": combo.index,
        "parameter_set_id": combo.parameter_set_id,
        "impulse_parameters": {
            "min_return_bps": combo.min_return_bps,
            "min_trades_since_t0": combo.min_trades_since_t0,
            "min_unique_buyers_since_t0": combo.min_unique_buyers_since_t0,
        },
        "candidate_signature": candidate_sig,
        "summary": summary_row,
        "outcome_rows": outcome_rows,
    }
    write_json_atomic(checkpoint_path(work_dir, combo.index), payload)


def load_combo_checkpoint(work_dir: Path, combo):
    path = checkpoint_path(work_dir, combo.index)
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError(f"combo {combo.index}: checkpoint is not a JSON object")
    validate_checkpoint(payload, combo)
    return payload


def main() -> None:
    ap = argparse.ArgumentParser()
    freeze_dir = PROJECT_ROOT / "data" / "research" / "phase2_research_freeze_v0_1"
    phase3_dir = PROJECT_ROOT / "data" / "research" / "phase3"
    ap.add_argument("--freeze-db", default=str(freeze_dir / "phase2_development_dataset_v0_1.sqlite3"))
    ap.add_argument("--freeze-manifest", default=str(freeze_dir / "phase2_development_dataset_manifest_v0_1.json"))
    ap.add_argument("--locked-search", default=str(freeze_dir / "phase2_locked_param_search_001.json"))
    ap.add_argument("--registry", default=str(freeze_dir / "phase2_experiment_registry_v0_1.json"))
    ap.add_argument("--coverage", default=str(phase3_dir / "collector_coverage_DEV-FREEZE-0001_v0_1.json"))
    ap.add_argument("--out-dir", default=str(phase3_dir / "EXP-0002"))
    args = ap.parse_args()

    paths = {k: Path(v).resolve() for k, v in vars(args).items()}
    for key in ("freeze_db", "freeze_manifest", "locked_search", "registry", "coverage"):
        if not paths[key].exists():
            raise FileNotFoundError(paths[key])

    print("PHASE 3 / EXP-0002 — BLOCK A IMPULSE HARNESS v0.1.1")
    print("=" * 68)
    started = time.time()

    registry = verify_registry(paths["registry"])
    manifest = json.loads(paths["freeze_manifest"].read_text(encoding="utf-8"))
    dataset = manifest["dataset"]
    locked = json.loads(paths["locked_search"].read_text(encoding="utf-8"))

    actual_db_sha = file_sha256(paths["freeze_db"])
    if actual_db_sha != dataset["snapshot_sqlite_sha256"]:
        raise RuntimeError("DEV-FREEZE-0001 SQLite SHA256 mismatch")
    actual_locked_sha = stable_sha(locked)
    if actual_locked_sha != manifest["parameter_search"]["locked_search_space_sha256"]:
        raise RuntimeError("PHASE-2-PARAM-SEARCH-001 hash mismatch")

    coverage = CollectorCoverageSnapshotV01.load(paths["coverage"])
    coverage.validate_binding(
        freeze_id=str(dataset["freeze_id"]),
        range_start=parse_utc(str(dataset["t0_observed_range_utc"]["start"])),
        range_end=parse_utc(str(dataset["source_cutoff"]["max_observed_at_utc"])),
    )

    conn = Phase1ReadOnlyAdapterV01.open_readonly(paths["freeze_db"])
    try:
        if conn.execute("PRAGMA quick_check").fetchone()[0] != "ok":
            raise RuntimeError("freeze SQLite quick_check failed")
        Phase1QuoteAwareAdapterV01.validate_schema(conn)
        raw_rows = Phase1QuoteAwareAdapterV01.fetch_all_bot_truth_rows(conn)
    finally:
        conn.close()

    events, skipped = Phase1QuoteAwareAdapterV01.normalize_rows(raw_rows)
    if skipped:
        raise RuntimeError(f"unexpected normalization skips: {skipped}")
    if len(events) != int(dataset["frozen_pump_event_rows"]):
        raise RuntimeError("frozen normalized event count mismatch")

    obs_by_mint: dict[str, list] = defaultdict(list)
    for event in events:
        obs = price_observation_from_event(event)
        if obs is not None:
            obs_by_mint[obs.mint].append(obs)
    replay = OutcomeReplayV013(coverage.provider())

    combos = generate_impulse_combinations(locked)
    summary_rows: list[dict[str, object]] = []
    long_rows: list[dict[str, object]] = []
    candidate_signatures: dict[int, str] = {}

    out_dir = paths["out_dir"]
    out_dir.mkdir(parents=True, exist_ok=True)
    work_dir = out_dir / "_work_v0_1_1"
    work_dir.mkdir(parents=True, exist_ok=True)

    preexisting_checkpoints = sum(
        checkpoint_path(work_dir, c.index).exists() for c in combos
    )

    print(f"Freeze                         : {dataset['freeze_id']}")
    print(f"Frozen events                  : {len(events)}")
    print(f"A_IMPULSE combinations         : {len(combos)}")
    print("B/C/D                          : fixed foundation anchors")
    print("Exit optimization              : NO")
    print("PnL / fees / slippage / latency: NOT MODELED")
    print(f"Resume checkpoints found       : {preexisting_checkpoints}")
    print(f"Checkpoint directory           : {work_dir}")
    print("-" * 68)

    resumed_count = 0
    computed_count = 0

    for i, combo in enumerate(combos, start=1):
        params = parameter_set_for_impulse(combo)
        validate_parameter_set_against_locked_search(params, locked)
        FirstPullbackStrategyV02.validate_parameters(params)

        cached = load_combo_checkpoint(work_dir, combo)
        if cached is not None:
            resumed_count += 1
            summary_row = dict(cached["summary"])
            combo_long_rows = [dict(x) for x in cached["outcome_rows"]]
            sig = str(cached["candidate_signature"])
            source_label = "resume"
        else:
            candidates, extraction = extract_candidates(events, params)
            if extraction.run_count != int(dataset["eligible_token_count"]):
                raise RuntimeError(f"{combo.parameter_set_id}: run_count mismatch")
            if extraction.candidate_count != extraction.candidate_state_count:
                raise RuntimeError(f"{combo.parameter_set_id}: candidate lifecycle mismatch")
            if extraction.accounted_run_count != extraction.run_count:
                raise RuntimeError(
                    f"{combo.parameter_set_id}: terminal lifecycle incomplete "
                    f"(expired={extraction.expired_count}, "
                    f"invalidated={extraction.invalidated_count}, "
                    f"rejected={extraction.rejected_count}, "
                    f"trade={extraction.trade_count}, "
                    f"candidate={extraction.candidate_state_count}, "
                    f"run_count={extraction.run_count})"
                )

            outcomes = [
                replay.replay(c.reference, obs_by_mint.get(c.signal.mint, ()))
                for c in candidates
            ]
            sig = candidate_signature(candidates)
            summary_row = summarize_combo(combo, extraction, outcomes)
            combo_long_rows = [outcome_long_row(combo, o) for o in outcomes]

            # Atomic per-combination persistence. A later crash can resume here
            # without recomputing completed combinations.
            save_combo_checkpoint(
                work_dir, combo, sig, summary_row, combo_long_rows
            )
            computed_count += 1
            source_label = "compute"

        candidate_signatures[combo.index] = sig
        summary_rows.append(summary_row)
        long_rows.extend(combo_long_rows)

        if i == 1 or i % 10 == 0 or i == len(combos):
            elapsed = time.time() - started
            print(
                f"[{i:3d}/175] R={combo.min_return_bps:5d} "
                f"T={combo.min_trades_since_t0:2d} B={combo.min_unique_buyers_since_t0:2d} "
                f"candidates={int(summary_row['candidate_count']):3d} "
                f"rejects={int(summary_row['rejected_count']):2d} "
                f"source={source_label:7s} elapsed={elapsed:6.1f}s"
            )

    # Sentinel replay determinism: first, middle and last Block-A combinations.
    sentinel_indices = (1, 88, 175)
    for idx in sentinel_indices:
        combo = combos[idx - 1]
        params = parameter_set_for_impulse(combo)
        second, _ = extract_candidates(events, params)
        if candidate_signature(second) != candidate_signatures[idx]:
            raise RuntimeError(f"determinism sentinel failed for combo {idx}")

    frontier = pareto_frontier(summary_rows)
    frontier_ids = {str(x["parameter_set_id"]) for x in frontier}
    for row in summary_rows:
        row["diagnostic_pareto_frontier"] = str(row["parameter_set_id"]) in frontier_ids

    # Sort long output deterministically.
    long_rows.sort(key=lambda r: (
        int(r["combo_index"]), str(r["signal_at"]), str(r["mint"]), str(r["candidate_id"])
    ))

    summary_path = out_dir / "EXP-0002_A_impulse_summary_v0_1_1.csv"
    frontier_path = out_dir / "EXP-0002_A_impulse_frontier_v0_1_1.csv"
    long_path = out_dir / "EXP-0002_A_impulse_candidate_outcomes_v0_1_1.csv"
    report_path = out_dir / "EXP-0002_A_impulse_report_v0_1_1.json"
    manifest_path = out_dir / "EXP-0002_manifest_v0_1_1.json"

    write_csv(summary_path, summary_rows)
    write_csv(frontier_path, frontier)
    write_csv(long_path, long_rows)

    report_core = {
        "schema_version": "P3-EXP0002-A-0.1.1",
        "experiment_id": EXPERIMENT_ID,
        "block": BLOCK_ID,
        "dataset": {
            "freeze_id": dataset["freeze_id"],
            "dataset_role": dataset["dataset_role"],
            "untouched_oos": False,
            "eligible_tokens": dataset["eligible_token_count"],
            "frozen_rows": dataset["frozen_pump_event_rows"],
            "snapshot_sqlite_sha256": actual_db_sha,
            "locked_search_sha256": actual_locked_sha,
        },
        "registry_precondition": {
            "next_experiment_id": registry["next_experiment_id"],
            "exp0001_registered_complete": True,
        },
        "strategy": {
            "name": "FirstPullback",
            "version": "v1.1",
            "strategy_clock": "SCLOCK-0.2",
        },
        "experiment_design": {
            "varied_block": "A_IMPULSE",
            "combination_count": len(combos),
            "varied_parameters": [
                "min_return_bps",
                "min_trades_since_t0",
                "min_unique_buyers_since_t0",
            ],
            "fixed_downstream_foundation_anchors": FOUNDATION_DOWNSTREAM,
            "max_entry_age_ms": 300000,
            "buyer_response_window": "3s",
            "full_cartesian_13_23m_run": False,
            "single_winner_selected": False,
            "diagnostic_frontier_only": True,
            "pareto_metrics_maximized": list(PARETO_METRICS),
            "pareto_eligibility": "at least one clean OBSERVABLE 15s and 30s return",
            "reason": (
                "isolate Block A while preserving signal-support dimensions; "
                "do not silently promote a tiny high-return sample to winner"
            ),
        },
        "result_counts": {
            "summary_rows": len(summary_rows),
            "candidate_outcome_rows": len(long_rows),
            "diagnostic_frontier_rows": len(frontier),
        },
        "harness_correction": {
            "supersedes_failed_harness": "phase3_exp0002_impulse_harness_v0_1.py",
            "failure_reason": (
                "v0.1 terminal-accounting assertion omitted legitimate "
                "FirstPullback REJECT state"
            ),
            "phase2_outcome_adapter": "phase2_outcome_adapter_v0.1.2",
            "reject_state_counted": True,
            "per_combination_atomic_checkpointing": True,
            "resume_supported": True,
            "checkpoint_directory": "_work_v0_1_1",
            "preexisting_checkpoints": preexisting_checkpoints,
            "resumed_combinations": resumed_count,
            "freshly_computed_combinations": computed_count,
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
            "sentinel_combo_indices": list(sentinel_indices),
            "sentinel_candidate_signatures": {
                str(i): candidate_signatures[i] for i in sentinel_indices
            },
            "pass": True,
        },
        "artifacts": {
            "summary_csv": summary_path.name,
            "frontier_csv": frontier_path.name,
            "candidate_outcomes_csv": long_path.name,
        },
    }
    report = dict(report_core)
    report["deterministic_report_core_sha256"] = stable_sha(report_core)
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    artifact_hashes = {
        "summary_csv_sha256": file_sha256(summary_path),
        "frontier_csv_sha256": file_sha256(frontier_path),
        "candidate_outcomes_csv_sha256": file_sha256(long_path),
        "report_json_sha256": file_sha256(report_path),
    }

    exp_manifest = {
        "manifest_schema_version": "EXPM-0.1-P3EXT",
        "experiment_id": "EXP-0002",
        "status": "RUN_COMPLETE_PENDING_POSTRUN_AUDIT_AND_REVIEW",
        "hypothesis": (
            "Within the locked PHASE-2-PARAM-SEARCH-001 impulse block, changing "
            "impulse confirmation thresholds changes FirstPullback candidate incidence "
            "and post-signal outcome distributions while B/C/D remain fixed."
        ),
        "dataset": report["dataset"],
        "search_space": {
            "decision_id": "PHASE-2-PARAM-SEARCH-001",
            "block": "A_IMPULSE",
            "combination_count": 175,
            "locked_search_space_sha256": actual_locked_sha,
        },
        "experiment_design": report["experiment_design"],
        "execution_model": {
            "version": "NOT_DEFINED_YET",
            "reference_position_size_sol": 0.10,
            "fee_model": "NOT_DEFINED_YET",
            "slippage_model": "NOT_DEFINED_YET",
            "latency_model": "NOT_DEFINED_YET",
            "failed_execution_behavior": "NOT_DEFINED_YET",
        },
        "artifacts": artifact_hashes,
        "decision": "REVIEW_DIAGNOSTIC_FRONTIER_BEFORE_BLOCK_B",
        "notes": (
            "DEV-FREEZE-0001 remains development/discovery data, not untouched OOS. "
            "The diagnostic Pareto frontier is not a profitability ranking and does "
            "not constitute a final trading parameter selection. Harness v0.1.1 "
            "corrects the v0.1 omission of REJECT from terminal accounting and "
            "persists each completed parameter combination atomically for resume."
        ),
    }
    manifest_path.write_text(
        json.dumps(exp_manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    print("-" * 68)
    print(f"Summary rows                   : {len(summary_rows)}")
    print(f"Candidate outcome rows         : {len(long_rows)}")
    print(f"Diagnostic Pareto frontier     : {len(frontier)}")
    print(f"Resumed combinations           : {resumed_count}")
    print(f"Freshly computed combinations  : {computed_count}")
    print("Terminal REJECT accounting     : INCLUDED")
    print("Sentinel determinism           : PASS")
    print("Single A winner selected       : NO")
    print(f"Report                         : {report_path}")
    print(f"Summary CSV                    : {summary_path}")
    print(f"Frontier CSV                   : {frontier_path}")
    print(f"Candidate outcomes CSV         : {long_path}")
    print(f"Experiment manifest            : {manifest_path}")
    print(f"Elapsed seconds                : {time.time() - started:.1f}")
    print("=" * 68)
    print("RESULT: PASS")


if __name__ == "__main__":
    main()
