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

from phase4.paper_continuous_market_source_v0_1 import (  # noqa: E402
    MODEL_FINGERPRINT,
    MODEL_ID,
    ContinuousMarketSourceV01,
    SourceSchemaError,
)


EXPECTED_MODEL_ID = "P4-CONTINUOUS-MARKET-SOURCE-0001"
EXPECTED_MODEL_FINGERPRINT = (
    "242b84a26ddc9e80fc428b1f02202e30aab6f4009677b38b72861bad432881fb"
)
SOURCE_IDENTITY = "SELFTEST:PUMP-EVENTS-FIXTURE-V0.1"
ANCHOR = 2
BASE_TS = 1_777_000_000


SCHEMA_SQL = """
CREATE TABLE pump_events (
    event_key TEXT PRIMARY KEY,
    signature TEXT NOT NULL,
    event_type TEXT NOT NULL,
    slot INTEGER,
    pump_timestamp INTEGER,
    mint TEXT,
    user_wallet TEXT,
    creator_wallet TEXT,
    sol_amount_lamports INTEGER,
    token_amount_raw TEXT,
    virtual_sol_reserves TEXT,
    virtual_token_reserves TEXT,
    real_sol_reserves TEXT,
    real_token_reserves TEXT,
    quote_mint TEXT,
    quote_amount_raw TEXT,
    virtual_quote_reserves TEXT,
    real_quote_reserves TEXT,
    source_decoded_file TEXT NOT NULL,
    decoded_at_utc TEXT
)
"""


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
    body = json.dumps(
        primitive(value), sort_keys=True, separators=(",", ":"), ensure_ascii=True
    )
    return sha256(body.encode("utf-8")).hexdigest()


def decoded_at(rowid: int) -> str:
    return f"2026-08-24T00:00:{rowid:02d}.000000+00:00"


def insert_row(
    conn: sqlite3.Connection,
    *,
    rowid: int,
    mint: str,
    event_type: str,
    virtual_sol: int | None = 10_000_000_000,
    virtual_token: int | None = 100_000_000_000,
    source: str = "LIVE_WEBSOCKET_EVENT_V0_3_4",
    quote_mint: str | None = None,
    virtual_quote: int | None = None,
) -> None:
    signature = f"sig-{rowid}"
    event_key = f"{signature}:0:event-{rowid}"
    conn.execute(
        """
        INSERT INTO pump_events(
            rowid, event_key, signature, event_type, slot, pump_timestamp, mint,
            user_wallet, creator_wallet, sol_amount_lamports, token_amount_raw,
            virtual_sol_reserves, virtual_token_reserves, real_sol_reserves,
            real_token_reserves, quote_mint, quote_amount_raw,
            virtual_quote_reserves, real_quote_reserves, source_decoded_file,
            decoded_at_utc
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            rowid,
            event_key,
            signature,
            event_type,
            900_000 + rowid,
            BASE_TS + rowid,
            mint,
            f"wallet-{rowid}",
            f"creator-{mint}" if event_type == "LAUNCH" else None,
            50_000_000 if event_type in {"BUY", "SELL"} else None,
            "250000000" if event_type in {"BUY", "SELL"} else None,
            None if virtual_sol is None else str(virtual_sol),
            None if virtual_token is None else str(virtual_token),
            "5000000000",
            "50000000000",
            quote_mint,
            "123000" if quote_mint else None,
            None if virtual_quote is None else str(virtual_quote),
            "456000" if quote_mint else None,
            source,
            decoded_at(rowid),
        ),
    )


def create_fixture(path: Path) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.execute(SCHEMA_SQL)
        insert_row(conn, rowid=1, mint="MINT-A", event_type="LAUNCH")
        insert_row(conn, rowid=2, mint="MINT-A", event_type="BUY")
        insert_row(conn, rowid=3, mint="MINT-A", event_type="BUY")
        insert_row(conn, rowid=4, mint="MINT-B", event_type="LAUNCH")
        insert_row(conn, rowid=5, mint="MINT-B", event_type="BUY")
        insert_row(conn, rowid=6, mint="MINT-C", event_type="BUY")
        insert_row(conn, rowid=7, mint="MINT-D", event_type="LAUNCH")
        insert_row(
            conn,
            rowid=8,
            mint="MINT-D",
            event_type="BUY",
            source="GAP_RECONCILIATION_V0_3_3",
        )
        insert_row(
            conn,
            rowid=9,
            mint="MINT-B",
            event_type="BUY",
            virtual_sol=None,
            virtual_token=0,
        )
        insert_row(
            conn,
            rowid=10,
            mint="MINT-B",
            event_type="SELL",
            virtual_sol=11_000_000_000,
        )
        insert_row(
            conn,
            rowid=11,
            mint="MINT-E",
            event_type="LAUNCH",
            virtual_sol=None,
            quote_mint="QUOTE-MINT",
            virtual_quote=7_000_000_000,
        )
        insert_row(
            conn,
            rowid=12,
            mint="MINT-E",
            event_type="BUY",
            virtual_sol=None,
            quote_mint="QUOTE-MINT",
            virtual_quote=7_500_000_000,
        )
        insert_row(
            conn,
            rowid=13,
            mint="MINT-B",
            event_type="BUY",
            source="OFFLINE_IMPORT_V0_1",
        )
        insert_row(
            conn,
            rowid=14,
            mint="MINT-F",
            event_type="LAUNCH",
            source="GAP_RECONCILIATION_V0_3_4",
        )
        insert_row(conn, rowid=15, mint="MINT-F", event_type="BUY")
        insert_row(
            conn,
            rowid=16,
            mint="MINT-G",
            event_type="LAUNCH",
            source="GAP_RECONCILIATION_V0_3_3",
        )
        insert_row(conn, rowid=17, mint="MINT-G", event_type="BUY")
        insert_row(conn, rowid=18, mint="MINT-H", event_type="LAUNCH")
        insert_row(conn, rowid=19, mint="MINT-H", event_type="BUY")
        conn.commit()
    finally:
        conn.close()


def make_source(path: Path) -> ContinuousMarketSourceV01:
    return ContinuousMarketSourceV01(
        path,
        start_after_p1_rowid=ANCHOR,
        database_identity=SOURCE_IDENTITY,
    )


def main() -> int:
    checks: dict[str, bool] = {}
    root = Path(tempfile.mkdtemp(prefix="phase4_market_source_v01_"))
    fixture = root / "pump_source.sqlite3"
    bad_fixture = root / "bad_source.sqlite3"
    try:
        create_fixture(fixture)
        source = make_source(fixture)

        checks["A_exact_model_id_fingerprint"] = (
            MODEL_ID == EXPECTED_MODEL_ID
            and MODEL_FINGERPRINT == EXPECTED_MODEL_FINGERPRINT
        )

        ro_conn = ContinuousMarketSourceV01.open_readonly(fixture)
        try:
            query_only = ContinuousMarketSourceV01.connection_is_query_only(ro_conn)
            checks["B_read_only_query_only_connection"] = query_only
            write_rejected = False
            try:
                ro_conn.execute(
                    "UPDATE pump_events SET mint=mint WHERE rowid=1"
                )
            except sqlite3.OperationalError:
                write_rejected = True
            checks["C_write_attempt_rejected"] = write_rejected
        finally:
            ro_conn.close()

        first = source.fetch_batch(after_p1_rowid=ANCHOR, batch_size=3)
        full = source.fetch_batch(after_p1_rowid=ANCHOR, batch_size=100)
        full_rowids = tuple(r.production_p1_rowid for r in full.records)
        checks["D_strict_ascending_p1_rowid_output"] = (
            full_rowids == tuple(sorted(full_rowids))
            and len(full_rowids) == len(set(full_rowids))
        )
        checks["E_bounded_batch_behavior"] = (
            first.raw_rows_fetched == 3
            and first.highest_fetched_p1_rowid == 5
            and tuple(r.production_p1_rowid for r in first.records) == (4, 5)
        )
        checks["F_explicit_start_anchor_respected"] = all(
            r.production_p1_rowid > ANCHOR
            and r.start_after_p1_rowid == ANCHOR
            for r in full.records
        )
        checks["G_pre_anchor_launch_not_eligible"] = (
            all(r.mint != "MINT-A" for r in full.records)
            and any(
                s.production_p1_rowid == 3
                and s.reason == "MINT_NOT_SESSION_ELIGIBLE"
                for s in full.skips
            )
        )
        checks["H_post_anchor_launch_eligible"] = any(
            r.production_p1_rowid == 4
            and r.mint == "MINT-B"
            and r.session_launch_p1_rowid == 4
            for r in full.records
        )
        checks["I_unlaunched_mint_excluded"] = (
            all(r.mint != "MINT-C" for r in full.records)
            and any(
                s.production_p1_rowid == 6
                and s.reason == "MINT_NOT_SESSION_ELIGIBLE"
                for s in full.skips
            )
        )
        checks["J_multiple_eligible_mints_independent"] = (
            {r.mint for r in full.records}
            == {"MINT-B", "MINT-D", "MINT-E", "MINT-H"}
            and {
                r.mint: r.session_launch_p1_rowid
                for r in full.records
                if r.event_type == "LAUNCH"
            }
            == {"MINT-B": 4, "MINT-D": 7, "MINT-E": 11, "MINT-H": 18}
        )
        checks["K_observed_at_aware_utc"] = all(
            r.observed_at.tzinfo is not None
            and r.observed_at.utcoffset() == timezone.utc.utcoffset(r.observed_at)
            for r in full.records
        )
        row5 = next(r for r in full.records if r.production_p1_rowid == 5)
        checks["L_ingest_seq_event_key_preserved"] = (
            row5.ingest_seq == 5 and row5.event_key == "sig-5:0:event-5"
        )
        checks["M_sol_native_price_reserve_exact"] = (
            row5.price_identity == "SOL_NATIVE"
            and row5.price_numerator_raw == 10_000_000_000
            and row5.price_denominator_raw == 100_000_000_000
            and row5.current_virtual_token_reserve_raw == 100_000_000_000
            and isinstance(row5.price_numerator_raw, int)
            and isinstance(row5.price_denominator_raw, int)
        )
        row8 = next(r for r in full.records if r.production_p1_rowid == 8)
        checks["N_gap_recovery_label_preserved"] = (
            row8.is_gap_recovery and row8.ingestion_source == "GAP_RECOVERY"
        )
        checks["O_unavailable_price_not_fabricated"] = (
            all(r.production_p1_rowid != 9 for r in full.records)
            and any(
                s.production_p1_rowid == 9
                and s.reason == "PRICE_POINT_UNAVAILABLE:UNAVAILABLE"
                for s in full.skips
            )
        )

        next_a = source.fetch_batch(after_p1_rowid=5, batch_size=4)
        recreated = make_source(fixture)
        next_b = recreated.fetch_batch(after_p1_rowid=5, batch_size=4)
        checks["P_restart_same_anchor_cursor_reproduces"] = (
            next_a.canonical_digest == next_b.canonical_digest
            and primitive(next_a) == primitive(next_b)
        )
        full_replay = make_source(fixture).fetch_batch(
            after_p1_rowid=ANCHOR, batch_size=100
        )
        checks["Q_record_fingerprints_stable"] = (
            tuple(r.content_fingerprint for r in full.records)
            == tuple(r.content_fingerprint for r in full_replay.records)
        )

        row10_before = next(
            r for r in full.records if r.production_p1_rowid == 10
        )
        writer = sqlite3.connect(fixture)
        try:
            writer.execute(
                "UPDATE pump_events SET virtual_sol_reserves=? WHERE rowid=10",
                ("12000000000",),
            )
            writer.commit()
        finally:
            writer.close()
        changed = make_source(fixture).fetch_batch(after_p1_rowid=9, batch_size=1)
        row10_after = changed.records[0]
        checks["R_changed_source_content_changes_fingerprint"] = (
            row10_before.content_fingerprint != row10_after.content_fingerprint
            and row10_after.price_numerator_raw == 12_000_000_000
        )

        first_again = source.fetch_batch(after_p1_rowid=ANCHOR, batch_size=3)
        checks["S_fetch_does_not_advance_cursor_provenance"] = (
            first.requested_after_p1_rowid == first_again.requested_after_p1_rowid
            == ANCHOR
            and first.highest_fetched_p1_rowid == first_again.highest_fetched_p1_rowid
            == 5
            and first.canonical_digest == first_again.canonical_digest
            and source.start_after_p1_rowid == ANCHOR
        )

        check_conn = sqlite3.connect(fixture)
        try:
            quick_check = str(check_conn.execute("PRAGMA quick_check").fetchone()[0])
        finally:
            check_conn.close()
        checks["T_sqlite_quick_check_ok"] = quick_check == "ok"

        repeat_a = make_source(fixture).fetch_batch(
            after_p1_rowid=ANCHOR, batch_size=100
        )
        repeat_b = make_source(fixture).fetch_batch(
            after_p1_rowid=ANCHOR, batch_size=100
        )
        canonical_digest = digest(repeat_a)
        checks["U_repeated_run_canonical_digest_identical"] = (
            canonical_digest == digest(repeat_b)
        )

        bad_conn = sqlite3.connect(bad_fixture)
        try:
            bad_conn.execute("CREATE TABLE pump_events(event_key TEXT)")
            bad_conn.commit()
        finally:
            bad_conn.close()
        schema_failed_closed = False
        try:
            ContinuousMarketSourceV01(
                bad_fixture,
                start_after_p1_rowid=0,
                database_identity="SELFTEST:BAD",
            ).fetch_batch(after_p1_rowid=0, batch_size=1)
        except SourceSchemaError:
            schema_failed_closed = True
        checks["V_required_schema_missing_fails_closed"] = schema_failed_closed

        checks["W_v034_gap_launch_cannot_activate"] = (
            all(r.mint != "MINT-F" for r in repeat_a.records)
            and any(
                s.production_p1_rowid == 14
                and s.reason
                == "GAP_RECOVERY_LAUNCH_NOT_SESSION_ELIGIBLE"
                and s.raw_source_decoded_file
                == "GAP_RECONCILIATION_V0_3_4"
                and s.source_compatibility_applied
                for s in repeat_a.skips
            )
            and any(
                s.production_p1_rowid == 15
                and s.reason == "MINT_NOT_SESSION_ELIGIBLE"
                for s in repeat_a.skips
            )
        )
        checks["X_accepted_gap_launch_cannot_activate"] = (
            all(r.mint != "MINT-G" for r in repeat_a.records)
            and any(
                s.production_p1_rowid == 16
                and s.reason == "GAP_RECOVERY_LAUNCH_NOT_SESSION_ELIGIBLE"
                for s in repeat_a.skips
            )
            and any(
                s.production_p1_rowid == 17
                and s.reason == "MINT_NOT_SESSION_ELIGIBLE"
                for s in repeat_a.skips
            )
        )
        checks["Y_fresh_live_launch_still_activates"] = (
            tuple(
                (r.production_p1_rowid, r.session_launch_p1_rowid)
                for r in repeat_a.records
                if r.mint == "MINT-H"
            )
            == ((18, 18), (19, 18))
        )

        fresh_restart = make_source(fixture).fetch_batch(
            after_p1_rowid=18, batch_size=1
        )
        checks["Z_restart_reconstructs_fresh_launch"] = (
            len(fresh_restart.records) == 1
            and fresh_restart.records[0].production_p1_rowid == 19
            and fresh_restart.records[0].mint == "MINT-H"
            and fresh_restart.records[0].session_launch_p1_rowid == 18
        )
        unsupported_restart = make_source(fixture).fetch_batch(
            after_p1_rowid=14, batch_size=1
        )
        gap_restart = make_source(fixture).fetch_batch(
            after_p1_rowid=16, batch_size=1
        )
        checks["AA_restart_does_not_resurrect_rejected_launches"] = (
            not unsupported_restart.records
            and len(unsupported_restart.skips) == 1
            and unsupported_restart.skips[0].production_p1_rowid == 15
            and unsupported_restart.skips[0].reason
            == "MINT_NOT_SESSION_ELIGIBLE"
            and not gap_restart.records
            and len(gap_restart.skips) == 1
            and gap_restart.skips[0].production_p1_rowid == 17
            and gap_restart.skips[0].reason == "MINT_NOT_SESSION_ELIGIBLE"
        )

        print("=" * 120)
        print("PHASE 4 - PRODUCTION READ-ONLY CONTINUOUS MARKET SOURCE SELF-TEST v0.1")
        print("=" * 120)
        print(f"Project root                       : {PROJECT_ROOT}")
        print(f"Source model                       : {MODEL_ID}")
        print(f"Source fingerprint                 : {MODEL_FINGERPRINT}")
        print(f"Source identity                    : {source.source_identity}")
        print("Production DB                      : NO (isolated temporary SQLite only)")
        print("Collector / RPC / network          : NO")
        print("Strategy / CandidateSignal binding : NO")
        print()
        print("-" * 120)
        print("VALIDATION")
        print("-" * 120)
        for name, passed in checks.items():
            print(f"{name:<78}: {'PASS' if passed else 'FAIL'}")
        print()
        print(f"Raw fixture max rowid              : {source.latest_p1_rowid()}")
        print(f"Emitted rowids after anchor        : {tuple(r.production_p1_rowid for r in repeat_a.records)}")
        print(f"Deterministic skips                : {dict(repeat_a.skipped_by_reason)}")
        print(f"SQLite quick_check                 : {quick_check}")
        print(f"Canonical source digest            : {canonical_digest}")
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
