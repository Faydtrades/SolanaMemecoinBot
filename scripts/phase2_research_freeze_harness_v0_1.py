from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.phase2.phase1_readonly_adapter_v0_1 import Phase1ReadOnlyAdapterV01
from src.phase2.quote_aware_price_v0_1 import Phase1QuoteAwareAdapterV01
from src.phase2.research_freeze_manifest_v0_1 import (
    FREEZE_ID,
    FREEZE_SCHEMA_VERSION,
    LOCK_DECISION_ID,
    LOCKED_PARAM_SEARCH_001,
    build_experiment_manifest_template,
    build_experiment_registry,
    canonical_json,
    eligible_mints_from_normalized,
    event_type_counts,
    file_sha256,
    frozen_rows_sha256,
    load_dev_mints_from_csv,
    pump_event_columns,
    selected_rows_for_dev_mints,
    source_counts,
    stable_sha256,
    us_to_iso,
    validate_experiment_manifest,
    validate_experiment_registry,
    validate_locked_search_space,
    validate_proposal_matches_lock,
    write_snapshot_database,
    write_token_csv,
)

DEFAULT_DB = ROOT / "data" / "db" / "tradingbot.sqlite3"
PHASE213_DIR = ROOT / "data" / "research" / "phase2_parameter_search_design_v0_1"
DEFAULT_REPORT_213 = PHASE213_DIR / "phase2_parameter_search_design_report_v0_1_1.json"
DEFAULT_TOKEN_METRICS_213 = PHASE213_DIR / "phase2_parameter_search_design_token_metrics_v0_1_1.csv"
DEFAULT_SEARCH_PROPOSAL_213 = PHASE213_DIR / "phase2_parameter_search_space_proposal_v0_1_1.json"
DEFAULT_OUT = ROOT / "data" / "research" / "phase2_research_freeze_v0_1"


def write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True, default=str), encoding="utf-8")


def main(
    db_path: Path,
    out_dir: Path,
    report_213_path: Path,
    token_metrics_213_path: Path,
    search_proposal_213_path: Path,
) -> None:
    print("PHASE 2 RESEARCH DATASET FREEZE + EXPERIMENT MANIFEST HARNESS v0.1")
    print("Purpose                    : FREEZE DEVELOPMENT/DISCOVERY DATA BEFORE OUTCOME RESEARCH")
    print("PnL/backtest                : NOT INCLUDED")
    print("Future-return labels        : NOT USED")
    print("Parameter optimization      : NOT PERFORMED")
    print("Locked search decision      : PHASE-2-PARAM-SEARCH-001")
    print("Freeze id                   : DEV-FREEZE-0001")
    print("Production DB mode          : READ ONLY")
    print("Network/RPC                 : DISABLED")
    print("Wallet/order/execution      : DISABLED")
    print()

    failures: list[str] = []

    def check(label: str, ok: bool) -> None:
        print(f"{label:<92} {'PASS' if ok else 'FAIL'}")
        if not ok:
            failures.append(label)

    for required in (report_213_path, token_metrics_213_path, search_proposal_213_path):
        if not required.exists():
            raise FileNotFoundError(
                f"Required successful Phase 2.13 v0.1.1 artifact missing: {required}"
            )

    validate_locked_search_space(LOCKED_PARAM_SEARCH_001)
    report_213 = json.loads(report_213_path.read_text(encoding="utf-8"))
    proposal_213 = json.loads(search_proposal_213_path.read_text(encoding="utf-8"))
    validate_proposal_matches_lock(proposal_213)
    dev_mints = load_dev_mints_from_csv(token_metrics_213_path)

    check("Test 1a - successful Phase 2.13 artifacts exist", True)
    check("Test 1b - PHASE-2-PARAM-SEARCH-001 exactly matches the 2.13 proposal", True)
    check("Test 1c - frozen development token list contains exactly 3901 unique mints", len(dev_mints) == 3901)
    check("Test 1d - 2.13 report identifies the same 3901-token development universe", int(report_213.get("eligible_tokens", -1)) == 3901)

    before = db_path.stat()
    conn = Phase1ReadOnlyAdapterV01.open_readonly(db_path)
    try:
        Phase1QuoteAwareAdapterV01.validate_schema(conn)
        raw_rows = Phase1QuoteAwareAdapterV01.fetch_all_bot_truth_rows(conn)
        events, skipped = Phase1QuoteAwareAdapterV01.normalize_rows(raw_rows)
        current_eligible = eligible_mints_from_normalized(events)

        # Freeze must match the exact source state used by the successful 2.13 run.
        expected_source_rows = int(report_213.get("source_rows", -1))
        expected_normalized_events = int(report_213.get("normalized_events", -1))
        check("Test 2a - current BOT_TRUTH source row count still equals the successful 2.13 run", len(raw_rows) == expected_source_rows)
        check("Test 2b - current normalized-event count still equals the successful 2.13 run", len(events) == expected_normalized_events)
        check("Test 2c - no BOT_TRUTH normalization skips are present", not skipped)
        check("Test 2d - current eligible mint set exactly equals frozen 2.13 development mint list", current_eligible == dev_mints)

        if failures:
            raise RuntimeError(
                "Source state no longer exactly matches Phase 2.13. "
                "Refusing to create a contaminated DEV-FREEZE-0001."
            )

        selected = selected_rows_for_dev_mints(raw_rows, set(dev_mints))
        columns = [name for name, _ in pump_event_columns(conn)]
        rows_digest = frozen_rows_sha256(selected, columns)

        # First observed tradable t0 for every development mint.
        t0_by_mint: dict[str, int] = {}
        for item in events:
            base = item.base
            if base.mint not in t0_by_mint and getattr(base.event_type, "value", base.event_type) in ("BUY", "SELL"):
                if base.mint in set(dev_mints):
                    t0_by_mint[base.mint] = int(base.observed_at_us)

        check("Test 3a - every development mint has an observed tradable t0", len(t0_by_mint) == len(dev_mints))
        check("Test 3b - frozen source rows contain only development mints", all(str(r["mint"]) in set(dev_mints) for r in selected))
        check("Test 3c - frozen rows preserve original Phase-1 row identity", len({int(r["p1_rowid"]) for r in selected}) == len(selected))

        max_rowid = max(int(r["p1_rowid"]) for r in selected)
        max_observed = max(str(r["decoded_at_utc"]) for r in selected)
        min_t0_us = min(t0_by_mint.values())
        max_t0_us = max(t0_by_mint.values())

        freeze_core = {
            "freeze_schema_version": FREEZE_SCHEMA_VERSION,
            "freeze_id": FREEZE_ID,
            "dataset_role": "DEVELOPMENT_DISCOVERY",
            "eligible_token_count": len(dev_mints),
            "frozen_pump_event_rows": len(selected),
            "frozen_rows_sha256": rows_digest,
            "source_event_type_counts": event_type_counts(selected),
            "source_decoded_file_counts": source_counts(selected),
            "source_cutoff": {
                "max_source_p1_rowid": max_rowid,
                "max_observed_at_utc": max_observed,
                "source_rows_seen_by_phase2_13": expected_source_rows,
                "normalized_events_seen_by_phase2_13": expected_normalized_events,
            },
            "t0_observed_range_utc": {
                "start": us_to_iso(min_t0_us),
                "end": us_to_iso(max_t0_us),
            },
            "selection_rule": (
                "Exact Phase 2.13 v0.1.1 development mint list; each mint had >=1 "
                "observed BOT_TRUTH BUY/SELL; no future-activity/outcome filter."
            ),
        }

        # Artifact paths are immutable for DEV-FREEZE-0001.
        if out_dir.exists() and any(out_dir.iterdir()):
            raise FileExistsError(
                f"Freeze output already exists and will not be overwritten: {out_dir}"
            )
        out_dir.mkdir(parents=True, exist_ok=True)

        snapshot_path = out_dir / "phase2_development_dataset_v0_1.sqlite3"
        token_path = out_dir / "phase2_development_tokens_v0_1.csv"
        lock_path = out_dir / "phase2_locked_param_search_001.json"
        manifest_path = out_dir / "phase2_development_dataset_manifest_v0_1.json"
        template_path = out_dir / "phase2_experiment_manifest_template_v0_1.json"
        registry_path = out_dir / "phase2_experiment_registry_v0_1.json"

        # Freeze snapshot first; final manifest hashes it.
        write_snapshot_database(
            conn,
            selected,
            dev_mints,
            snapshot_path,
            freeze_core=freeze_core,
        )
        write_token_csv(token_path, dev_mints, t0_by_mint)
        write_json(lock_path, LOCKED_PARAM_SEARCH_001)

        snapshot_hash = file_sha256(snapshot_path)
        token_csv_hash = file_sha256(token_path)
        locked_search_hash = stable_sha256(LOCKED_PARAM_SEARCH_001)

        manifest_core = {
            "freeze_schema_version": FREEZE_SCHEMA_VERSION,
            "freeze_id": FREEZE_ID,
            "dataset_role": "DEVELOPMENT_DISCOVERY",
            "dataset": {
                **freeze_core,
                "snapshot_sqlite_sha256": snapshot_hash,
                "development_token_csv_sha256": token_csv_hash,
            },
            "parameter_search": {
                "decision_id": LOCK_DECISION_ID,
                "locked_search_space_sha256": locked_search_hash,
                "locked_search_space_file": lock_path.name,
                "first_cycle_entry_window_ms": 300_000,
                "first_cycle_buyer_response_window": "3s",
                "research_method": "STAGED_BLOCK_EXPERIMENTS",
                "naive_full_cartesian_recommended": False,
            },
            "phase2_versions": {
                "normalized_market_event": "NME-0.1",
                "market_state": "MS-0.1",
                "bot_truth_feature_clock": "FeatureEngineV02",
                "quote_aware_price": "QuoteAwarePriceV01",
                "denomination_aware_flow": "DenominationAwareFeatureEngineV01",
                "first_pullback_strategy": "v1.1 / FirstPullbackStrategyV02",
                "strategy_clock": "SCLOCK-0.1",
                "parameter_search_design": "P2PSD-0.1.1",
            },
            "upstream_phase2_13": {
                "report_file": report_213_path.name,
                "report_sha256": file_sha256(report_213_path),
                "token_metrics_file": token_metrics_213_path.name,
                "token_metrics_sha256": file_sha256(token_metrics_213_path),
                "proposal_file": search_proposal_213_path.name,
                "proposal_sha256": file_sha256(search_proposal_213_path),
            },
            "out_of_sample_policy": {
                "these_3901_tokens_are_untouched_oos": False,
                "these_3901_tokens_are_development_discovery": True,
                "future_new_tokens_are_not_automatically_oos_if_used_to_change_search_ranges": True,
                "range_change_requires_new_version": True,
                "fresh_oos_validation_required_after_range_or_strategy_change": True,
            },
            "phase3_readiness": {
                "outcome_pnl_opened_yet": False,
                "next_experiment_id": "EXP-0001",
                "execution_model_defined": False,
                "exit_model_defined": False,
                "note": (
                    "Phase 3 may use this frozen development dataset for research, "
                    "but it must not call these tokens untouched out-of-sample."
                ),
            },
        }
        deterministic_manifest_hash = stable_sha256(manifest_core)
        manifest = {
            **manifest_core,
            "deterministic_manifest_sha256": deterministic_manifest_hash,
        }
        write_json(manifest_path, manifest)

        template = build_experiment_manifest_template(manifest)
        registry = build_experiment_registry(manifest)
        validate_experiment_manifest(template, allow_template=True)
        validate_experiment_registry(registry)
        write_json(template_path, template)
        write_json(registry_path, registry)

    finally:
        conn.close()

    after = db_path.stat()
    check("Test 4a - production DB size unchanged", before.st_size == after.st_size)
    check("Test 4b - production DB mtime unchanged", before.st_mtime_ns == after.st_mtime_ns)

    frozen = Phase1ReadOnlyAdapterV01.open_readonly(snapshot_path)
    try:
        Phase1QuoteAwareAdapterV01.validate_schema(frozen)
        frozen_rows = Phase1QuoteAwareAdapterV01.fetch_all_bot_truth_rows(frozen)
        frozen_events, frozen_skipped = Phase1QuoteAwareAdapterV01.normalize_rows(frozen_rows)
        frozen_eligible = eligible_mints_from_normalized(frozen_events)
        frozen_quick = frozen.execute("PRAGMA quick_check").fetchone()[0]
    finally:
        frozen.close()

    check("Test 5a - frozen snapshot reopens through Phase-1 quote-aware adapter", not frozen_skipped)
    check("Test 5b - frozen snapshot contains exactly the 3901 development mints", frozen_eligible == dev_mints)
    check("Test 5c - frozen snapshot preserves exact selected row count", len(frozen_rows) == len(selected))
    check("Test 5d - frozen SQLite quick_check passes", frozen_quick == "ok")
    check("Test 5e - experiment registry reserves EXP-0001 with no experiment silently run", registry["next_experiment_id"] == "EXP-0001" and registry["experiments"] == [])

    print()
    print("=" * 118)
    print("PHASE 2 DEVELOPMENT/DISCOVERY DATASET FREEZE — IMMUTABLE BASELINE")
    print("=" * 118)
    print(f"Freeze id                                  : {FREEZE_ID}")
    print(f"Dataset role                               : DEVELOPMENT_DISCOVERY")
    print(f"Eligible frozen tokens                     : {len(dev_mints)}")
    print(f"Frozen BOT_TRUTH pump rows                 : {len(selected)}")
    print(f"T0 observed range                          : {us_to_iso(min_t0_us)} -> {us_to_iso(max_t0_us)}")
    print(f"Source max original p1 rowid               : {max_rowid}")
    print(f"Frozen rows SHA256                         : {rows_digest}")
    print(f"Snapshot SQLite SHA256                     : {snapshot_hash}")
    print(f"Locked parameter-search decision           : {LOCK_DECISION_ID}")
    print(f"Locked search-space SHA256                 : {locked_search_hash}")
    print(f"Experiment registry next id                : {registry['next_experiment_id']}")
    print(f"These 3901 tokens are untouched OOS        : NO")
    print(f"Development/discovery status               : YES")
    print()
    print("EXPERIMENT MANIFEST STANDARD")
    print(" * Every major Phase 3 experiment must receive EXP-0001, EXP-0002, ...")
    print(" * At minimum record dataset/range, strategy/version, parameters, execution model/version,")
    print("   reference position size, filters, result metrics, and notes/decision.")
    print(" * Gross and realistic-net results must remain distinct once execution/PnL is introduced.")
    print(" * New data used to change strategy/search ranges becomes development data; it is not untouched OOS.")
    print()
    print(f"Frozen replay SQLite                      : {snapshot_path}")
    print(f"Dataset manifest JSON                     : {manifest_path}")
    print(f"Development token CSV                     : {token_path}")
    print(f"Locked search-space JSON                  : {lock_path}")
    print(f"Experiment manifest template              : {template_path}")
    print(f"Experiment registry                       : {registry_path}")
    print("Production DB touched by harness          : NO")
    print("Network/RPC used                          : NO")
    print("Wallet/order/execution used               : NO")
    print("Outcome/PnL evaluation performed          : NO")
    print("Parameter optimization performed          : NO")
    print(f"RESULT                                    : {'PASS' if not failures else 'FAIL'}")

    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--phase2-13-report", type=Path, default=DEFAULT_REPORT_213)
    parser.add_argument("--phase2-13-token-metrics", type=Path, default=DEFAULT_TOKEN_METRICS_213)
    parser.add_argument("--phase2-13-search-proposal", type=Path, default=DEFAULT_SEARCH_PROPOSAL_213)
    args = parser.parse_args()
    main(
        args.db,
        args.out_dir,
        args.phase2_13_report,
        args.phase2_13_token_metrics,
        args.phase2_13_search_proposal,
    )
