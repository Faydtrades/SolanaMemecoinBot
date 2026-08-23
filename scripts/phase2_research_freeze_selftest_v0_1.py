from __future__ import annotations

import json
import shutil
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
    LOCKED_PARAM_SEARCH_001,
    build_experiment_manifest_template,
    build_experiment_registry,
    canonical_json,
    eligible_mints_from_normalized,
    frozen_rows_sha256,
    locked_parameter_projection,
    pump_event_columns,
    selected_rows_for_dev_mints,
    stable_sha256,
    validate_experiment_manifest,
    validate_experiment_registry,
    validate_locked_search_space,
    validate_proposal_matches_lock,
    write_snapshot_database,
)

SELFTEST_DIR = ROOT / "data" / "selftest" / "phase2_research_freeze_selftest_v0_1"
SOURCE_DB = SELFTEST_DIR / "source.sqlite3"
SNAPSHOT_DB = SELFTEST_DIR / "frozen.sqlite3"

failures: list[str] = []


def check(label: str, ok: bool) -> None:
    print(f"{label:<82} {'PASS' if ok else 'FAIL'}")
    if not ok:
        failures.append(label)


def create_source() -> None:
    if SELFTEST_DIR.exists():
        shutil.rmtree(SELFTEST_DIR)
    SELFTEST_DIR.mkdir(parents=True)
    conn = sqlite3.connect(SOURCE_DB)
    conn.executescript(
        """
        CREATE TABLE pump_events (
            event_key TEXT,
            signature TEXT,
            event_type TEXT,
            slot INTEGER,
            pump_timestamp INTEGER,
            mint TEXT,
            user_wallet TEXT,
            creator_wallet TEXT,
            sol_amount_lamports TEXT,
            token_amount_raw TEXT,
            virtual_sol_reserves TEXT,
            virtual_token_reserves TEXT,
            real_sol_reserves TEXT,
            real_token_reserves TEXT,
            quote_mint TEXT,
            quote_amount_raw TEXT,
            virtual_quote_reserves TEXT,
            real_quote_reserves TEXT,
            source_decoded_file TEXT,
            decoded_at_utc TEXT
        );
        """
    )
    rows = [
        (10, "a-launch", "sig-a0", "LAUNCH", 100, 1000, "MintA", None, "CreatorA", "0", "0", "0", "0", "0", "0", None, "0", "0", "0", "LIVE_WEBSOCKET_EVENT_V0_3_3", "2026-08-18T14:00:00+00:00"),
        (11, "a-buy", "sig-a1", "BUY", 101, 1001, "MintA", "WA", "CreatorA", "100", "10", "10000", "1000000", "0", "0", None, "0", "0", "0", "LIVE_WEBSOCKET_EVENT_V0_3_3", "2026-08-18T14:00:01+00:00"),
        (12, "a-sell", "sig-a2", "SELL", 102, 1002, "MintA", "WA", "CreatorA", "25", "5", "11000", "999000", "0", "0", None, "0", "0", "0", "LIVE_WEBSOCKET_EVENT_V0_3_3", "2026-08-18T14:00:02+00:00"),
        (20, "b-buy", "sig-b1", "BUY", 200, 2001, "MintB", "WB", None, "0", "10", "0", "1000000", "0", "0", "QuoteMint", "500", "50000", "0", "LIVE_WEBSOCKET_EVENT_V0_3_3", "2026-08-18T14:01:01+00:00"),
        (30, "c-launch", "sig-c0", "LAUNCH", 300, 3000, "LaunchOnlyMint", None, "CreatorC", "0", "0", "0", "0", "0", "0", None, "0", "0", "0", "LIVE_WEBSOCKET_EVENT_V0_3_3", "2026-08-18T14:02:00+00:00"),
    ]
    sql = """
        INSERT INTO pump_events(
            rowid,event_key,signature,event_type,slot,pump_timestamp,mint,
            user_wallet,creator_wallet,sol_amount_lamports,token_amount_raw,
            virtual_sol_reserves,virtual_token_reserves,real_sol_reserves,
            real_token_reserves,quote_mint,quote_amount_raw,virtual_quote_reserves,
            real_quote_reserves,source_decoded_file,decoded_at_utc
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """
    conn.executemany(sql, rows)
    conn.commit()
    conn.close()


print("PHASE 2 RESEARCH DATASET FREEZE + EXPERIMENT MANIFEST SELF-TEST v0.1")
print("Purpose                    : IMMUTABLE DEV DATASET + EXPERIMENT VERSIONING")
print("PnL/outcome evaluation      : NOT USED")
print("Parameter optimization      : NOT PERFORMED")
print("Production DB touched       : NO")
print()

validate_locked_search_space(LOCKED_PARAM_SEARCH_001)
check("Test 1a - PHASE-2-PARAM-SEARCH-001 validates as explicit locked space", True)
check("Test 1b - locked space keeps 5m entry and 3s response windows", LOCKED_PARAM_SEARCH_001["max_entry_age_ms"] == [300000] and LOCKED_PARAM_SEARCH_001["buyer_response"]["window_id"] == ["3s"])
check("Test 1c - naive 13.23m Cartesian brute force remains rejected", LOCKED_PARAM_SEARCH_001["naive_full_cartesian_count"] == 13_230_000 and LOCKED_PARAM_SEARCH_001["full_cartesian_recommended"] is False)

proposal = dict(locked_parameter_projection())
proposal["status"] = "PROPOSAL_NOT_LOCKED"
validate_proposal_matches_lock(proposal)
check("Test 1d - exact Phase 2.13 parameter projection matches locked decision", True)

create_source()
source = Phase1ReadOnlyAdapterV01.open_readonly(SOURCE_DB)
try:
    Phase1QuoteAwareAdapterV01.validate_schema(source)
    raw = Phase1QuoteAwareAdapterV01.fetch_all_bot_truth_rows(source)
    events, skipped = Phase1QuoteAwareAdapterV01.normalize_rows(raw)
    eligible = eligible_mints_from_normalized(events)
    check("Test 2a - synthetic eligible universe excludes launch-only mint", eligible == ("MintA", "MintB"))
    selected = selected_rows_for_dev_mints(raw, set(eligible))
    columns = [name for name, _ in pump_event_columns(source)]
    digest_a = frozen_rows_sha256(selected, columns)
    digest_b = frozen_rows_sha256(selected, columns)
    check("Test 2b - source-row freeze digest is deterministic", digest_a == digest_b)
    check("Test 2c - selected freeze rows preserve exact source rowids", [int(r["p1_rowid"]) for r in selected] == [10, 11, 12, 20])

    freeze_core = {
        "freeze_id": FREEZE_ID,
        "eligible_token_count": len(eligible),
        "frozen_row_count": len(selected),
        "frozen_rows_sha256": digest_a,
        "locked_search_space_sha256": stable_sha256(LOCKED_PARAM_SEARCH_001),
    }
    write_snapshot_database(
        source,
        selected,
        eligible,
        SNAPSHOT_DB,
        freeze_core=freeze_core,
    )
finally:
    source.close()

frozen = Phase1ReadOnlyAdapterV01.open_readonly(SNAPSHOT_DB)
try:
    Phase1QuoteAwareAdapterV01.validate_schema(frozen)
    frozen_rows = Phase1QuoteAwareAdapterV01.fetch_all_bot_truth_rows(frozen)
    frozen_events, frozen_skipped = Phase1QuoteAwareAdapterV01.normalize_rows(frozen_rows)
    frozen_eligible = eligible_mints_from_normalized(frozen_events)
    check("Test 3a - frozen SQLite remains adapter-compatible", not frozen_skipped and frozen_eligible == ("MintA", "MintB"))
    check("Test 3b - snapshot pump_events rowids equal original Phase-1 rowids", [int(r["p1_rowid"]) for r in frozen_rows] == [10, 11, 12, 20])
    check("Test 3c - frozen snapshot excludes non-development launch-only mint", all(str(r["mint"]) != "LaunchOnlyMint" for r in frozen_rows))
    check("Test 3d - frozen SQLite quick_check passes", frozen.execute("PRAGMA quick_check").fetchone()[0] == "ok")
finally:
    frozen.close()

manifest_stub = {
    "freeze_id": FREEZE_ID,
    "deterministic_manifest_sha256": "a" * 64,
    "dataset": {
        "eligible_token_count": 2,
        "t0_observed_range_utc": {
            "start": "2026-08-18T14:00:01+00:00",
            "end": "2026-08-18T14:01:01+00:00",
        },
        "source_cutoff": {"max_source_p1_rowid": 20},
        "frozen_rows_sha256": "b" * 64,
    },
    "parameter_search": {
        "locked_search_space_sha256": stable_sha256(LOCKED_PARAM_SEARCH_001),
    },
}
template = build_experiment_manifest_template(manifest_stub)
registry = build_experiment_registry(manifest_stub)
validate_experiment_manifest(template, allow_template=True)
validate_experiment_registry(registry)
check("Test 4a - experiment manifest template has required checkpoint fields", True)
check("Test 4b - experiment registry reserves EXP-0001 without running it", registry["next_experiment_id"] == "EXP-0001" and registry["experiments"] == [])
check("Test 4c - dataset/hash binding is embedded in experiment template", template["dataset"]["freeze_id"] == FREEZE_ID and template["reproducibility"]["frozen_rows_sha256"] == "b" * 64)
check("Test 4d - locked research/reference size is 0.10 SOL", template["execution_model"]["reference_position_size_sol"] == 0.10)
check("Test 4e - gross/raw and realistic-net result buckets stay separate", set(template["result_metrics"]) == {"gross_raw", "realistic_net", "execution_quality"})

print()
print(f"{'Locked PARAM-SEARCH-001 integrity':<58}: {'PASS' if not any(x.startswith('Test 1') for x in failures) else 'FAIL'}")
print(f"{'Development dataset freeze mechanics':<58}: {'PASS' if not any(x.startswith('Test 2') or x.startswith('Test 3') for x in failures) else 'FAIL'}")
print(f"{'Experiment manifest / registry schema':<58}: {'PASS' if not any(x.startswith('Test 4') for x in failures) else 'FAIL'}")
print(f"{'PnL/outcome evaluation used':<58}: NO")
print(f"{'Parameter optimization performed':<58}: NO")
print(f"{'Production DB touched':<58}: NO")
print(f"{'RESULT':<58}: {'PASS' if not failures else 'FAIL'}")

if failures:
    raise SystemExit(1)
