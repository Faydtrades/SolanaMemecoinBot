from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SMOKE_DIR = PROJECT_ROOT / "data" / "paper" / "live_smoke"

LOCKED_TRACKS = ("FINAL-A", "FINAL-B", "SENS-C")


def latest_pair() -> tuple[Path, Path | None]:
    dbs = sorted(
        SMOKE_DIR.glob("phase4_3b1d2_causal_sol_entry_*.sqlite3"),
        key=lambda p: p.stat().st_mtime_ns,
        reverse=True,
    )
    if not dbs:
        raise FileNotFoundError(
            f"No phase4_3b1d2_causal_sol_entry_*.sqlite3 under {SMOKE_DIR}"
        )
    db = dbs[0]
    stamp = db.stem.removeprefix("phase4_3b1d2_causal_sol_entry_")
    audit = SMOKE_DIR / f"phase4_3b1d2_causal_sol_entry_{stamp}.json"
    return db, audit if audit.exists() else None


def open_ro(path: Path) -> sqlite3.Connection:
    uri = path.resolve().as_uri() + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=5.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    return conn


def tables(conn: sqlite3.Connection) -> list[str]:
    return [
        str(r["name"])
        for r in conn.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type='table'
              AND name NOT LIKE 'sqlite_%'
            ORDER BY name
            """
        )
    ]


def columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {
        str(r["name"])
        for r in conn.execute(f'PRAGMA table_info("{table}")')
    }


def find_table(
    conn: sqlite3.Connection,
    required: set[str],
) -> str:
    matches = []
    for table in tables(conn):
        cols = columns(conn, table)
        if required <= cols:
            matches.append(table)
    if len(matches) != 1:
        raise RuntimeError(
            f"Expected exactly one table containing {sorted(required)}, "
            f"found {matches}"
        )
    return matches[0]


def rowdict(row: sqlite3.Row) -> dict[str, Any]:
    return {k: row[k] for k in row.keys()}


def main() -> int:
    print("=" * 112)
    print("PHASE 4.3B1D2 - POSTRUN POSITION/TRACK AUDIT v0.1")
    print("=" * 112)

    db_path, audit_path = latest_pair()

    print(f"Project root              : {PROJECT_ROOT}")
    print(f"Paper DB                  : {db_path}")
    print(f"Original run audit        : {audit_path if audit_path else 'NOT FOUND'}")
    print("DB access                 : QUERY-ONLY / READ-ONLY")
    print("Collector / network       : NO")
    print("Wallet / live orders      : NO")
    print("Paper DB writes           : NO")
    print()

    original_failures = []
    original_result = None
    if audit_path is not None:
        payload = json.loads(audit_path.read_text(encoding="utf-8"))
        original_result = payload.get("result")
        checks = payload.get("fill_checks") or {}
        original_failures = sorted(
            k for k, v in checks.items() if v is not True
        )

    conn = open_ro(db_path)
    try:
        order_table = find_table(
            conn,
            {
                "paper_order_id",
                "track_id",
                "state",
                "mint",
                "filled_size_lamports",
                "simulated_entry_price_numerator_raw",
                "simulated_entry_price_denominator_raw",
            },
        )
        position_table = find_table(
            conn,
            {
                "paper_position_id",
                "paper_order_id",
                "track_id",
                "state",
                "mint",
                "filled_size_lamports",
                "entry_price_numerator_raw",
                "entry_price_denominator_raw",
            },
        )

        orders = conn.execute(
            f'SELECT * FROM "{order_table}"'
        ).fetchall()
        positions = conn.execute(
            f'SELECT * FROM "{position_table}"'
        ).fetchall()

        quick = conn.execute("PRAGMA quick_check").fetchone()[0]
    finally:
        conn.close()

    print("-" * 112)
    print("DISCOVERY")
    print("-" * 112)
    print(f"Order table               : {order_table}")
    print(f"Position table            : {position_table}")
    print(f"Orders                     : {len(orders)}")
    print(f"Positions                  : {len(positions)}")
    print(f"Original audit result      : {original_result}")
    print(f"Original failed checks     : {original_failures}")
    print()

    order_tracks_raw = tuple(str(r["track_id"]) for r in orders)
    position_tracks_raw = tuple(str(r["track_id"]) for r in positions)

    order_by_id = {
        str(r["paper_order_id"]): r
        for r in orders
    }

    print("-" * 112)
    print("RAW TRACK ORDER")
    print("-" * 112)
    print(f"Locked track order         : {LOCKED_TRACKS}")
    print(f"Order-table row order      : {order_tracks_raw}")
    print(f"Position-table row order   : {position_tracks_raw}")
    print(f"Order track set            : {sorted(set(order_tracks_raw))}")
    print(f"Position track set         : {sorted(set(position_tracks_raw))}")
    print()

    mapping = []
    missing_order_links = []
    track_link_mismatches = []
    mint_link_mismatches = []
    size_link_mismatches = []
    price_link_mismatches = []

    for pos in positions:
        oid = str(pos["paper_order_id"])
        order = order_by_id.get(oid)
        if order is None:
            missing_order_links.append(oid)
            continue

        rec = {
            "position_id": str(pos["paper_position_id"]),
            "order_id": oid,
            "position_track": str(pos["track_id"]),
            "order_track": str(order["track_id"]),
            "position_state": str(pos["state"]),
            "order_state": str(order["state"]),
        }
        mapping.append(rec)

        if str(pos["track_id"]) != str(order["track_id"]):
            track_link_mismatches.append(rec)

        if str(pos["mint"]) != str(order["mint"]):
            mint_link_mismatches.append(rec)

        if int(pos["filled_size_lamports"]) != int(order["filled_size_lamports"]):
            size_link_mismatches.append(rec)

        if (
            int(pos["entry_price_numerator_raw"])
            != int(order["simulated_entry_price_numerator_raw"])
            or int(pos["entry_price_denominator_raw"])
            != int(order["simulated_entry_price_denominator_raw"])
        ):
            price_link_mismatches.append(rec)

    print("-" * 112)
    print("ORDER -> POSITION LINKAGE")
    print("-" * 112)
    for rec in mapping:
        print(
            f"{rec['position_track']:<8} "
            f"position={rec['position_id']} "
            f"order={rec['order_id']} "
            f"order_track={rec['order_track']} "
            f"states={rec['order_state']}->{rec['position_state']}"
        )
    print()

    checks = {
        "original_only_position_track_order_failed": (
            original_failures == ["position_tracks_match_lock"]
        ),
        "exactly_three_orders": len(orders) == 3,
        "exactly_three_positions": len(positions) == 3,
        "order_tracks_membership_exact": (
            set(order_tracks_raw) == set(LOCKED_TRACKS)
            and len(set(order_tracks_raw)) == 3
        ),
        "position_tracks_membership_exact": (
            set(position_tracks_raw) == set(LOCKED_TRACKS)
            and len(set(position_tracks_raw)) == 3
        ),
        "all_positions_link_to_orders": not missing_order_links,
        "linked_track_ids_match": not track_link_mismatches,
        "linked_mints_match": not mint_link_mismatches,
        "linked_sizes_match": not size_link_mismatches,
        "linked_entry_prices_match": not price_link_mismatches,
        "all_orders_filled": all(str(r["state"]) == "FILLED" for r in orders),
        "all_positions_open": all(str(r["state"]) == "OPEN" for r in positions),
        "sqlite_quick_check": str(quick).lower() == "ok",
    }

    order_only_false_negative = (
        all(checks.values())
        and position_tracks_raw != LOCKED_TRACKS
    )

    # If the DB happened to return the locked order here, then the original
    # mismatch needs deeper investigation rather than being called order-only.
    exact_membership_and_linkage = (
        checks["position_tracks_membership_exact"]
        and checks["all_positions_link_to_orders"]
        and checks["linked_track_ids_match"]
        and checks["linked_mints_match"]
        and checks["linked_sizes_match"]
        and checks["linked_entry_prices_match"]
    )

    print("-" * 112)
    print("VALIDATION")
    print("-" * 112)
    for label, ok in checks.items():
        print(f"{label:<50}: {'PASS' if ok else 'FAIL'}")

    print()
    if order_only_false_negative:
        print("DIAGNOSIS: ORDER_ONLY_VALIDATOR_FALSE_NEGATIVE")
        print(
            "The three locked tracks are all present exactly once and each "
            "PaperPosition is linked to the matching filled PaperOrder. "
            "Only list/row ordering differs from LOCKED_TRACKS."
        )
        print("RESULT: PASS_DIAGNOSIS")
        return 0

    if exact_membership_and_linkage and all(
        v for k, v in checks.items()
        if k != "original_only_position_track_order_failed"
    ):
        print("DIAGNOSIS: MEMBERSHIP_AND_LINKAGE_VALID")
        print(
            "Track membership and one-to-one lifecycle linkage are valid, "
            "but the original failure cannot be proven to be ordering-only "
            "from the persisted state alone."
        )
        print("RESULT: CHECK")
        return 2

    print("DIAGNOSIS: REAL_POSITION_TRACK_OR_LINKAGE_MISMATCH")
    print(f"Missing order links        : {missing_order_links}")
    print(f"Track mismatches           : {track_link_mismatches}")
    print(f"Mint mismatches            : {mint_link_mismatches}")
    print(f"Size mismatches            : {size_link_mismatches}")
    print(f"Price mismatches           : {price_link_mismatches}")
    print("RESULT: CHECK")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
