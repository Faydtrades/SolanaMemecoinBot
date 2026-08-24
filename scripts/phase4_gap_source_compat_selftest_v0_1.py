from __future__ import annotations

import json
import shutil
import sqlite3
import sys
import tempfile
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from hashlib import sha256
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from phase4_continuous_market_source_selftest_v0_1 import (  # noqa: E402
    SCHEMA_SQL,
    insert_row,
)
from phase2.models_v0_1 import IngestionSource  # noqa: E402
from phase4.paper_continuous_market_source_v0_1 import (  # noqa: E402
    MODEL_FINGERPRINT as SOURCE_FINGERPRINT,
    MODEL_ID as SOURCE_MODEL_ID,
    ContinuousMarketSourceV01,
)
from phase4.phase1_gap_source_compat_v0_1 import (  # noqa: E402
    ACCEPTED_GAP_SOURCE_ALIAS,
    MODEL_FINGERPRINT,
    MODEL_ID,
    V034_GAP_SOURCE,
    Phase1GapSourceCompatibilityV01,
)


EXPECTED_MODEL_ID = "P4-PHASE1-GAP-SOURCE-COMPAT-0001"
EXPECTED_MODEL_FINGERPRINT = (
    "aabb765f81a29b2dd119607cdea92a2949dd3acec10aa204d246e6aa7a28a78c"
)
EXPECTED_SOURCE_FINGERPRINT = (
    "242b84a26ddc9e80fc428b1f02202e30aab6f4009677b38b72861bad432881fb"
)
SOURCE_IDENTITY = "SELFTEST:GAP-COMPAT-PUMP-EVENTS-V0.1"
V033_GAP_SOURCE = "GAP_RECONCILIATION_V0_3_3"
UNKNOWN_FUTURE_GAP_SOURCE = "GAP_RECONCILIATION_V0_3_5"
LIVE_SOURCE = "LIVE_WEBSOCKET_EVENT_V0_3_4"


def primitive(value: Any) -> Any:
    if is_dataclass(value):
        return primitive(asdict(value))
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat(timespec="microseconds")
    if isinstance(value, dict):
        return {str(k): primitive(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [primitive(v) for v in value]
    return value


def digest(value: Any) -> str:
    payload = json.dumps(
        primitive(value), sort_keys=True, separators=(",", ":"), ensure_ascii=True
    )
    return sha256(payload.encode("utf-8")).hexdigest()


def create_fixture(path: Path) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.execute(SCHEMA_SQL)
        insert_row(
            conn,
            rowid=1,
            mint="MINT-V033-GAP-LAUNCH",
            event_type="LAUNCH",
            source=V033_GAP_SOURCE,
        )
        insert_row(
            conn,
            rowid=2,
            mint="MINT-V033-GAP-LAUNCH",
            event_type="BUY",
            source=LIVE_SOURCE,
        )
        insert_row(
            conn,
            rowid=3,
            mint="MINT-V034-GAP-LAUNCH",
            event_type="LAUNCH",
            source=V034_GAP_SOURCE,
        )
        insert_row(
            conn,
            rowid=4,
            mint="MINT-V034-GAP-LAUNCH",
            event_type="BUY",
            source=LIVE_SOURCE,
        )
        insert_row(
            conn,
            rowid=5,
            mint="MINT-FRESH",
            event_type="LAUNCH",
            source=LIVE_SOURCE,
        )
        insert_row(
            conn,
            rowid=6,
            mint="MINT-FRESH",
            event_type="BUY",
            source=V034_GAP_SOURCE,
        )
        insert_row(
            conn,
            rowid=7,
            mint="MINT-FRESH",
            event_type="SELL",
            source=UNKNOWN_FUTURE_GAP_SOURCE,
        )
        insert_row(
            conn,
            rowid=8,
            mint="MINT-FRESH",
            event_type="BUY",
            source=LIVE_SOURCE,
        )
        conn.commit()
    finally:
        conn.close()


def read_rows(path: Path) -> dict[int, sqlite3.Row]:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        return {
            int(row["p1_rowid"]): row
            for row in conn.execute(
                "SELECT rowid AS p1_rowid, * FROM pump_events ORDER BY rowid"
            ).fetchall()
        }
    finally:
        conn.close()


def make_source(path: Path) -> ContinuousMarketSourceV01:
    return ContinuousMarketSourceV01(
        path,
        start_after_p1_rowid=0,
        database_identity=SOURCE_IDENTITY,
    )


def main() -> int:
    checks: dict[str, bool] = {}
    root = Path(tempfile.mkdtemp(prefix="phase4_gap_compat_v01_"))
    fixture = root / "gap_compat.sqlite3"
    try:
        create_fixture(fixture)
        rows = read_rows(fixture)

        checks["A_exact_compat_model_id_fingerprint"] = (
            MODEL_ID == EXPECTED_MODEL_ID
            and MODEL_FINGERPRINT == EXPECTED_MODEL_FINGERPRINT
            and SOURCE_MODEL_ID == "P4-CONTINUOUS-MARKET-SOURCE-0001"
            and SOURCE_FINGERPRINT == EXPECTED_SOURCE_FINGERPRINT
        )

        v033 = Phase1GapSourceCompatibilityV01.row_to_event(rows[1])
        checks["B_v033_still_gap_recovery"] = (
            v033.event is not None
            and v033.event.base.source == IngestionSource.GAP_RECOVERY
            and v033.raw_source_decoded_file == V033_GAP_SOURCE
            and not v033.compatibility_applied
        )

        raw_v034_before = str(rows[3]["source_decoded_file"])
        v034 = Phase1GapSourceCompatibilityV01.row_to_event(rows[3])
        rows_after = read_rows(fixture)
        checks["C_v034_now_gap_recovery"] = (
            v034.event is not None
            and v034.event.base.source == IngestionSource.GAP_RECOVERY
            and v034.normalized_source_decoded_file == ACCEPTED_GAP_SOURCE_ALIAS
            and v034.compatibility_applied
        )
        checks["D_raw_v034_provenance_not_mutated"] = (
            raw_v034_before == V034_GAP_SOURCE
            and str(rows[3]["source_decoded_file"]) == V034_GAP_SOURCE
            and str(rows_after[3]["source_decoded_file"]) == V034_GAP_SOURCE
            and v034.raw_source_decoded_file == V034_GAP_SOURCE
        )

        source = make_source(fixture)
        batch = source.fetch_batch(after_p1_rowid=0, batch_size=20)
        checks["E_v034_gap_launch_not_eligible"] = (
            all(r.mint != "MINT-V034-GAP-LAUNCH" for r in batch.records)
            and any(
                s.production_p1_rowid == 3
                and s.reason == "GAP_RECOVERY_LAUNCH_NOT_SESSION_ELIGIBLE"
                and s.source_compatibility_applied
                for s in batch.skips
            )
        )
        checks["F_live_buy_after_only_v034_gap_launch_ineligible"] = any(
            s.production_p1_rowid == 4
            and s.reason == "MINT_NOT_SESSION_ELIGIBLE"
            for s in batch.skips
        )

        fresh_records = tuple(r for r in batch.records if r.mint == "MINT-FRESH")
        gap_buy = next(
            r for r in fresh_records if r.production_p1_rowid == 6
        )
        checks["G_fresh_launch_then_v034_gap_buy"] = (
            tuple(r.production_p1_rowid for r in fresh_records) == (5, 6, 8)
            and all(r.session_launch_p1_rowid == 5 for r in fresh_records)
            and gap_buy.is_gap_recovery
            and gap_buy.ingestion_source == "GAP_RECOVERY"
            and gap_buy.raw_source_decoded_file == V034_GAP_SOURCE
            and gap_buy.source_compatibility_applied
        )

        rejected_restart = make_source(fixture).fetch_batch(
            after_p1_rowid=3, batch_size=1
        )
        checks["H_restart_ignores_v034_gap_launch"] = (
            not rejected_restart.records
            and len(rejected_restart.skips) == 1
            and rejected_restart.skips[0].production_p1_rowid == 4
            and rejected_restart.skips[0].reason == "MINT_NOT_SESSION_ELIGIBLE"
        )
        fresh_restart = make_source(fixture).fetch_batch(
            after_p1_rowid=5, batch_size=1
        )
        checks["I_restart_fresh_launch_allows_v034_gap"] = (
            len(fresh_restart.records) == 1
            and fresh_restart.records[0].production_p1_rowid == 6
            and fresh_restart.records[0].session_launch_p1_rowid == 5
            and fresh_restart.records[0].is_gap_recovery
            and fresh_restart.records[0].source_compatibility_applied
        )

        future = Phase1GapSourceCompatibilityV01.row_to_event(rows[7])
        checks["J_unknown_future_gap_label_unsupported"] = (
            future.event is None
            and future.skip_reason == "UNSUPPORTED_NON_BOT_TRUTH_SOURCE"
            and future.raw_source_decoded_file == UNKNOWN_FUTURE_GAP_SOURCE
            and not future.compatibility_applied
            and any(
                s.production_p1_rowid == 7
                and s.reason == "UNSUPPORTED_NON_BOT_TRUTH_SOURCE"
                for s in batch.skips
            )
        )

        live = Phase1GapSourceCompatibilityV01.row_to_event(rows[8])
        live_record = next(r for r in batch.records if r.production_p1_rowid == 8)
        checks["K_live_v034_behavior_unchanged"] = (
            live.event is not None
            and live.event.base.source == IngestionSource.LIVE_WS
            and live.raw_source_decoded_file == LIVE_SOURCE
            and not live.compatibility_applied
            and not live_record.is_gap_recovery
            and not live_record.source_compatibility_applied
        )
        checks["L_exact_price_reserves_unchanged"] = (
            gap_buy.price_identity == "SOL_NATIVE"
            and gap_buy.price_numerator_raw == 10_000_000_000
            and gap_buy.price_denominator_raw == 100_000_000_000
            and gap_buy.current_virtual_token_reserve_raw == 100_000_000_000
        )

        recreated = make_source(fixture)
        replay = recreated.fetch_batch(after_p1_rowid=0, batch_size=20)
        checks["M_content_source_fingerprints_stable"] = (
            source.source_identity == recreated.source_identity
            and tuple(r.content_fingerprint for r in batch.records)
            == tuple(r.content_fingerprint for r in replay.records)
        )
        canonical_digest = digest(batch)
        checks["N_repeated_run_canonical_digest_identical"] = (
            canonical_digest == digest(replay)
            and batch.canonical_digest == replay.canonical_digest
        )

        check_conn = sqlite3.connect(fixture)
        try:
            quick_check = str(check_conn.execute("PRAGMA quick_check").fetchone()[0])
        finally:
            check_conn.close()
        checks["O_sqlite_quick_check_ok"] = quick_check == "ok"

        print("=" * 120)
        print("PHASE 4 - COLLECTOR v0.3.4 GAP-SOURCE COMPATIBILITY SELF-TEST v0.1")
        print("=" * 120)
        print(f"Project root                       : {PROJECT_ROOT}")
        print(f"Compatibility model                : {MODEL_ID}")
        print(f"Compatibility fingerprint          : {MODEL_FINGERPRINT}")
        print(f"Market-source fingerprint          : {SOURCE_FINGERPRINT}")
        print(f"Source identity                    : {source.source_identity}")
        print(f"Exact alias                        : {V034_GAP_SOURCE} -> {ACCEPTED_GAP_SOURCE_ALIAS}")
        print("Phase-2 source adapter             : UNCHANGED")
        print("Production DB / collector / network: NO")
        print()
        print("-" * 120)
        print("VALIDATION")
        print("-" * 120)
        for name, passed in checks.items():
            print(f"{name:<78}: {'PASS' if passed else 'FAIL'}")
        print()
        print(f"Emitted rowids                     : {tuple(r.production_p1_rowid for r in batch.records)}")
        print(f"Deterministic skips                : {dict(batch.skipped_by_reason)}")
        print(f"SQLite quick_check                 : {quick_check}")
        print(f"Canonical compatibility digest     : {canonical_digest}")
        print("FIRSTPULLBACK LIVE BINDING          : NOT IMPLEMENTED")
        print("CONTINUOUS LIVE PAPER              : NOT VALIDATED")
        print()
        if all(checks.values()):
            print("RESULT: PASS")
            return 0
        print("RESULT: CHECK")
        return 2
    finally:
        shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
