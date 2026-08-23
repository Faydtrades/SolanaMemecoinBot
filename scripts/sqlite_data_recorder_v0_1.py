import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
ENRICHED_DIR = DATA_DIR / "enriched"
DECODED_DIR = DATA_DIR / "decoded"
DB_DIR = DATA_DIR / "db"
DB_PATH = DB_DIR / "tradingbot.sqlite3"

SCHEMA_VERSION = 1


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def latest_file(directory: Path, pattern: str) -> Path:
    files = sorted(
        directory.glob(pattern),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not files:
        raise FileNotFoundError(
            f"No file matching {pattern!r} found in {directory}"
        )
    return files[0]


def connect_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row

    # Durable/restart-friendly defaults for a local collector.
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    conn.execute("PRAGMA busy_timeout = 5000;")

    return conn


def create_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS schema_meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at_utc TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS transactions (
            signature TEXT PRIMARY KEY,
            slot INTEGER,
            block_time INTEGER,
            fee_payer TEXT,
            transaction_error_json TEXT,
            fee_lamports INTEGER,
            source_enrichment_file TEXT NOT NULL,
            enriched_at_utc TEXT,
            inserted_at_utc TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS pump_events (
            event_key TEXT PRIMARY KEY,
            signature TEXT NOT NULL,
            event_type TEXT NOT NULL,
            pump_event_name TEXT NOT NULL,

            slot INTEGER,
            block_time INTEGER,
            pump_timestamp INTEGER,

            mint TEXT,
            user_wallet TEXT,
            creator_wallet TEXT,
            fee_payer TEXT,

            name TEXT,
            symbol TEXT,
            uri TEXT,
            bonding_curve TEXT,

            is_buy INTEGER,

            sol_amount_lamports INTEGER,
            sol_amount REAL,
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
            decoded_at_utc TEXT,
            inserted_at_utc TEXT NOT NULL,

            event_json TEXT NOT NULL,

            FOREIGN KEY(signature)
                REFERENCES transactions(signature)
                ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_pump_events_signature
            ON pump_events(signature);

        CREATE INDEX IF NOT EXISTS idx_pump_events_type
            ON pump_events(event_type);

        CREATE INDEX IF NOT EXISTS idx_pump_events_mint
            ON pump_events(mint);

        CREATE INDEX IF NOT EXISTS idx_pump_events_user
            ON pump_events(user_wallet);

        CREATE INDEX IF NOT EXISTS idx_pump_events_creator
            ON pump_events(creator_wallet);

        CREATE INDEX IF NOT EXISTS idx_pump_events_timestamp
            ON pump_events(pump_timestamp);

        CREATE INDEX IF NOT EXISTS idx_pump_events_slot
            ON pump_events(slot);
        """
    )

    now = utc_now()
    conn.execute(
        """
        INSERT INTO schema_meta(key, value, updated_at_utc)
        VALUES('schema_version', ?, ?)
        ON CONFLICT(key) DO UPDATE SET
            value = excluded.value,
            updated_at_utc = excluded.updated_at_utc
        """,
        (str(SCHEMA_VERSION), now),
    )
    conn.commit()


def iter_jsonl(path: Path):
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                yield line_number, json.loads(line)
            except json.JSONDecodeError as exc:
                print(
                    f"[WARN] {path.name}:{line_number} malformed JSON: {exc}"
                )


def json_or_none(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def as_int_bool(value: Any) -> int | None:
    if value is None:
        return None
    return 1 if bool(value) else 0


def as_text_int(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def import_transactions(
    conn: sqlite3.Connection, enrichment_file: Path
) -> tuple[int, int]:
    inserted = 0
    duplicates = 0
    now = utc_now()

    for _, rec in iter_jsonl(enrichment_file):
        signature = rec.get("signature")
        if not signature:
            continue

        cursor = conn.execute(
            """
            INSERT OR IGNORE INTO transactions(
                signature,
                slot,
                block_time,
                fee_payer,
                transaction_error_json,
                fee_lamports,
                source_enrichment_file,
                enriched_at_utc,
                inserted_at_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                signature,
                rec.get("slot"),
                rec.get("block_time"),
                rec.get("fee_payer"),
                json_or_none(rec.get("transaction_error")),
                rec.get("fee_lamports"),
                enrichment_file.name,
                rec.get("enriched_at_utc"),
                now,
            ),
        )

        if cursor.rowcount == 1:
            inserted += 1
        else:
            duplicates += 1

    conn.commit()
    return inserted, duplicates


def import_events(
    conn: sqlite3.Connection, decoded_file: Path
) -> tuple[int, int, int]:
    inserted = 0
    duplicates = 0
    missing_parent = 0
    now = utc_now()

    for _, rec in iter_jsonl(decoded_file):
        event_key = rec.get("event_key")
        signature = rec.get("signature")
        if not event_key or not signature:
            continue

        parent = conn.execute(
            "SELECT 1 FROM transactions WHERE signature = ?",
            (signature,),
        ).fetchone()

        if parent is None:
            missing_parent += 1
            print(
                f"[WARN] Event {event_key} has no transaction parent; skipped"
            )
            continue

        cursor = conn.execute(
            """
            INSERT OR IGNORE INTO pump_events(
                event_key,
                signature,
                event_type,
                pump_event_name,

                slot,
                block_time,
                pump_timestamp,

                mint,
                user_wallet,
                creator_wallet,
                fee_payer,

                name,
                symbol,
                uri,
                bonding_curve,

                is_buy,

                sol_amount_lamports,
                sol_amount,
                token_amount_raw,

                virtual_sol_reserves,
                virtual_token_reserves,
                real_sol_reserves,
                real_token_reserves,

                quote_mint,
                quote_amount_raw,
                virtual_quote_reserves,
                real_quote_reserves,

                source_decoded_file,
                decoded_at_utc,
                inserted_at_utc,

                event_json
            )
            VALUES (
                ?, ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?,
                ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?,
                ?
            )
            """,
            (
                event_key,
                signature,
                rec.get("event_type"),
                rec.get("pump_event_name"),

                rec.get("slot"),
                rec.get("block_time"),
                rec.get("timestamp"),

                rec.get("mint"),
                rec.get("user"),
                rec.get("creator"),
                rec.get("fee_payer"),

                rec.get("name"),
                rec.get("symbol"),
                rec.get("uri"),
                rec.get("bonding_curve"),

                as_int_bool(rec.get("is_buy")),

                rec.get("sol_amount_lamports"),
                rec.get("sol_amount"),
                as_text_int(rec.get("token_amount_raw")),

                as_text_int(rec.get("virtual_sol_reserves")),
                as_text_int(rec.get("virtual_token_reserves")),
                as_text_int(rec.get("real_sol_reserves")),
                as_text_int(rec.get("real_token_reserves")),

                rec.get("quote_mint"),
                as_text_int(rec.get("quote_amount_raw")),
                as_text_int(rec.get("virtual_quote_reserves")),
                as_text_int(rec.get("real_quote_reserves")),

                decoded_file.name,
                rec.get("decoded_at_utc"),
                now,

                json.dumps(
                    rec,
                    ensure_ascii=False,
                    separators=(",", ":"),
                ),
            ),
        )

        if cursor.rowcount == 1:
            inserted += 1
        else:
            duplicates += 1

    conn.commit()
    return inserted, duplicates, missing_parent


def fetch_summary(conn: sqlite3.Connection) -> dict[str, Any]:
    tx_count = conn.execute(
        "SELECT COUNT(*) AS n FROM transactions"
    ).fetchone()["n"]

    event_count = conn.execute(
        "SELECT COUNT(*) AS n FROM pump_events"
    ).fetchone()["n"]

    by_type = {
        row["event_type"]: row["n"]
        for row in conn.execute(
            """
            SELECT event_type, COUNT(*) AS n
            FROM pump_events
            GROUP BY event_type
            ORDER BY event_type
            """
        )
    }

    multi_event_transactions = conn.execute(
        """
        SELECT COUNT(*) AS n
        FROM (
            SELECT signature
            FROM pump_events
            GROUP BY signature
            HAVING COUNT(*) > 1
        )
        """
    ).fetchone()["n"]

    return {
        "transactions": tx_count,
        "events": event_count,
        "by_type": by_type,
        "multi_event_transactions": multi_event_transactions,
    }


def show_sample_rows(conn: sqlite3.Connection) -> None:
    rows = conn.execute(
        """
        SELECT
            event_type,
            symbol,
            mint,
            user_wallet,
            sol_amount,
            signature
        FROM pump_events
        ORDER BY rowid
        LIMIT 10
        """
    ).fetchall()

    if not rows:
        return

    print()
    print("SAMPLE EVENTS")
    print("-" * 92)
    for row in rows:
        symbol = row["symbol"] or "-"
        sol = (
            f"{row['sol_amount']:.9f}"
            if row["sol_amount"] is not None
            else "-"
        )
        print(
            f"{row['event_type']:<7} | "
            f"symbol={symbol:<12} | "
            f"SOL={sol:<14} | "
            f"mint={row['mint'] or '-'}"
        )
        print(f"         sig={row['signature']}")


def run_integrity_checks(conn: sqlite3.Connection) -> tuple[bool, list[str]]:
    issues: list[str] = []

    quick = conn.execute("PRAGMA quick_check;").fetchone()[0]
    if quick != "ok":
        issues.append(f"quick_check={quick}")

    fk_rows = conn.execute("PRAGMA foreign_key_check;").fetchall()
    if fk_rows:
        issues.append(f"foreign_key_check found {len(fk_rows)} issue(s)")

    return len(issues) == 0, issues


def main(db_path: Path) -> None:
    enrichment_file = latest_file(
        ENRICHED_DIR,
        "transaction_enrichment_v0_1_*.jsonl",
    )
    decoded_file = latest_file(
        DECODED_DIR,
        "pump_events_v0_2_*.jsonl",
    )

    print("=" * 92)
    print("SOLANA MEMECOIN BOT - SQLITE DATA RECORDER v0.1")
    print("=" * 92)
    print(f"UTC start       : {utc_now()}")
    print(f"Database        : {db_path}")
    print(f"Transactions    : {enrichment_file}")
    print(f"Pump events     : {decoded_file}")
    print("Mode            : LOCAL IMPORT / PERSISTENT DEDUP TEST")
    print()

    conn = connect_db(db_path)

    try:
        create_schema(conn)

        tx_inserted, tx_duplicates = import_transactions(
            conn, enrichment_file
        )
        event_inserted, event_duplicates, missing_parent = import_events(
            conn, decoded_file
        )

        summary = fetch_summary(conn)
        integrity_ok, integrity_issues = run_integrity_checks(conn)

        print("-" * 92)
        print("IMPORT RESULT")
        print("-" * 92)
        print(f"Transactions inserted : {tx_inserted}")
        print(f"Transaction duplicates: {tx_duplicates}")
        print(f"Events inserted       : {event_inserted}")
        print(f"Event duplicates      : {event_duplicates}")
        print(f"Missing parent events : {missing_parent}")

        print()
        print("-" * 92)
        print("DATABASE STATE")
        print("-" * 92)
        print(f"Transactions total    : {summary['transactions']}")
        print(f"Pump events total     : {summary['events']}")
        print(
            f"Multi-event signatures: "
            f"{summary['multi_event_transactions']}"
        )
        print(
            "Events by type         : "
            + ", ".join(
                f"{key}={value}"
                for key, value in sorted(summary["by_type"].items())
            )
        )

        show_sample_rows(conn)

        print()
        print("-" * 92)
        print("INTEGRITY")
        print("-" * 92)
        print(
            f"SQLite quick_check     : "
            f"{'PASS' if integrity_ok else 'FAIL'}"
        )
        print(
            f"Foreign keys           : "
            f"{'PASS' if not integrity_issues else 'CHECK'}"
        )
        if integrity_issues:
            for issue in integrity_issues:
                print(f"  - {issue}")

        print()
        if integrity_ok and missing_parent == 0:
            print("RESULT: PASS")
        else:
            print("RESULT: CHECK")

        print()
        print("DEDUP TEST:")
        print(
            "Run this exact script one more time. "
            "The second run should insert 0 transactions and 0 events, "
            "while reporting the existing rows as duplicates."
        )

    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Create/import the first persistent SQLite recorder database."
        )
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=DB_PATH,
        help=f"SQLite database path (default: {DB_PATH})",
    )
    args = parser.parse_args()
    main(args.db)
