import argparse
import asyncio
import base64
import json
import logging
import re
import sqlite3
import struct
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

import httpx
from websockets.asyncio.client import connect


HTTP_RPC_URL = "https://api.mainnet.solana.com"
WS_URL = "wss://api.mainnet.solana.com"
PUMP_PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
LAMPORTS_PER_SOL = 1_000_000_000

CREATE_EVENT_DISC = bytes([27, 114, 169, 77, 222, 235, 99, 118])
TRADE_EVENT_DISC = bytes([189, 219, 127, 211, 78, 230, 97, 238])

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
DB_DIR = DATA_DIR / "db"
LOG_DIR = PROJECT_ROOT / "logs"
DB_PATH = DB_DIR / "tradingbot.sqlite3"

B58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

INVOKE_RE = re.compile(r"^Program\s+([1-9A-HJ-NP-Za-km-z]+)\s+invoke\s+\[(\d+)\]")
EXIT_RE = re.compile(r"^Program\s+([1-9A-HJ-NP-Za-km-z]+)\s+(success|failed:.*)$")

STALE_WEBSOCKET_SECONDS = 30
HEALTH_INTERVAL_SECONDS = 30


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def utc_after(seconds: float) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).isoformat()


def parse_utc(value: str) -> datetime:
    return datetime.fromisoformat(value)


def raw_file_path() -> Path:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    day = datetime.now(timezone.utc).strftime("%Y%m%d")
    return RAW_DIR / f"live_pump_notifications_v0_2_{day}.jsonl"


def log_file_path() -> Path:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    day = datetime.now(timezone.utc).strftime("%Y%m%d")
    return LOG_DIR / f"live_collector_v0_2_{day}.log"


def append_jsonl(path: Path, record: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
        handle.flush()


def setup_logging() -> logging.Logger:
    logger = logging.getLogger("live_collector_v0_2")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter(
        "%(asctime)sZ | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    console = logging.StreamHandler()
    console.setFormatter(formatter)
    logger.addHandler(console)

    file_handler = logging.FileHandler(log_file_path(), encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


LOGGER = setup_logging()


def b58encode(raw: bytes) -> str:
    n = int.from_bytes(raw, "big")
    chars = []
    while n:
        n, rem = divmod(n, 58)
        chars.append(B58_ALPHABET[rem])
    encoded = "".join(reversed(chars)) if chars else ""

    leading = 0
    for byte in raw:
        if byte == 0:
            leading += 1
        else:
            break
    return "1" * leading + encoded


class Reader:
    def __init__(self, data: bytes):
        self.data = data
        self.pos = 0

    @property
    def remaining(self) -> int:
        return len(self.data) - self.pos

    def take(self, n: int) -> bytes:
        if self.remaining < n:
            raise ValueError(
                f"Need {n} bytes at offset {self.pos}, only {self.remaining} remain"
            )
        out = self.data[self.pos:self.pos + n]
        self.pos += n
        return out

    def u8(self) -> int:
        return self.take(1)[0]

    def boolean(self) -> bool:
        value = self.u8()
        if value not in (0, 1):
            raise ValueError(f"Invalid bool value {value}")
        return bool(value)

    def u16(self) -> int:
        return struct.unpack("<H", self.take(2))[0]

    def u32(self) -> int:
        return struct.unpack("<I", self.take(4))[0]

    def u64(self) -> int:
        return struct.unpack("<Q", self.take(8))[0]

    def i64(self) -> int:
        return struct.unpack("<q", self.take(8))[0]

    def pubkey(self) -> str:
        return b58encode(self.take(32))

    def string(self) -> str:
        length = self.u32()
        if length > 1_000_000:
            raise ValueError(f"Implausible string length {length}")
        return self.take(length).decode("utf-8", errors="replace")


def decode_create_event(payload: bytes) -> dict[str, Any]:
    r = Reader(payload)
    if r.take(8) != CREATE_EVENT_DISC:
        raise ValueError("Not CreateEvent")

    result = {
        "event_type": "LAUNCH",
        "pump_event_name": "CreateEvent",
        "name": r.string(),
        "symbol": r.string(),
        "uri": r.string(),
        "mint": r.pubkey(),
        "bonding_curve": r.pubkey(),
        "user": r.pubkey(),
        "creator": r.pubkey(),
        "timestamp": r.i64(),
        "virtual_token_reserves": r.u64(),
        "virtual_sol_reserves": r.u64(),
        "real_token_reserves": r.u64(),
        "token_total_supply": r.u64(),
        "token_program": r.pubkey(),
        "is_mayhem_mode": r.boolean(),
        "is_cashback_enabled": r.boolean(),
        "quote_mint": r.pubkey(),
        "virtual_quote_reserves": r.u64(),
    }
    result["bytes_remaining_after_decode"] = r.remaining
    return result


def decode_trade_event(payload: bytes) -> dict[str, Any]:
    r = Reader(payload)
    if r.take(8) != TRADE_EVENT_DISC:
        raise ValueError("Not TradeEvent")

    result: dict[str, Any] = {
        "pump_event_name": "TradeEvent",
        "mint": r.pubkey(),
        "sol_amount_lamports": r.u64(),
        "token_amount_raw": r.u64(),
        "is_buy": r.boolean(),
        "user": r.pubkey(),
        "timestamp": r.i64(),
        "virtual_sol_reserves": r.u64(),
        "virtual_token_reserves": r.u64(),
    }
    result["event_type"] = "BUY" if result["is_buy"] else "SELL"
    result["sol_amount"] = result["sol_amount_lamports"] / LAMPORTS_PER_SOL

    try:
        result["real_sol_reserves"] = r.u64()
        result["real_token_reserves"] = r.u64()
        result["fee_recipient"] = r.pubkey()
        result["fee_basis_points"] = r.u64()
        result["fee_lamports"] = r.u64()
        result["creator"] = r.pubkey()
        result["creator_fee_basis_points"] = r.u64()
        result["creator_fee_lamports"] = r.u64()
        result["track_volume"] = r.boolean()
        result["total_unclaimed_tokens"] = r.u64()
        result["total_claimed_tokens"] = r.u64()
        result["current_sol_volume"] = r.u64()
        result["last_update_timestamp"] = r.i64()
        result["ix_name"] = r.string()
        result["mayhem_mode"] = r.boolean()
        result["cashback_fee_basis_points"] = r.u64()
        result["cashback_lamports"] = r.u64()
        result["buyback_fee_basis_points"] = r.u64()
        result["buyback_fee_lamports"] = r.u64()

        shareholder_count = r.u32()
        if shareholder_count > 1000:
            raise ValueError(f"Implausible shareholder count {shareholder_count}")
        shareholders = []
        for _ in range(shareholder_count):
            shareholders.append({
                "address": r.pubkey(),
                "share_bps": r.u16(),
            })
        result["shareholders"] = shareholders

        result["quote_mint"] = r.pubkey()
        result["quote_amount_raw"] = r.u64()
        result["virtual_quote_reserves"] = r.u64()
        result["real_quote_reserves"] = r.u64()
        result["full_current_tail_decoded"] = True
    except ValueError as exc:
        result["full_current_tail_decoded"] = False
        result["tail_decode_note"] = str(exc)

    result["bytes_remaining_after_decode"] = r.remaining
    return result


def extract_pump_scoped_event_payloads(logs: list[str]) -> list[bytes]:
    stack: list[str] = []
    payloads: list[bytes] = []

    for line in logs:
        if not isinstance(line, str):
            continue

        invoke_match = INVOKE_RE.match(line)
        if invoke_match:
            stack.append(invoke_match.group(1))
            continue

        exit_match = EXIT_RE.match(line)
        if exit_match:
            program = exit_match.group(1)
            if stack:
                if stack[-1] == program:
                    stack.pop()
                elif program in stack:
                    while stack:
                        popped = stack.pop()
                        if popped == program:
                            break
            continue

        if not (stack and stack[-1] == PUMP_PROGRAM_ID):
            continue

        if not line.startswith("Program data: "):
            continue

        encoded = line[len("Program data: "):].strip()
        try:
            raw = base64.b64decode(encoded, validate=True)
        except Exception:
            continue

        if len(raw) >= 8 and raw[:8] in (CREATE_EVENT_DISC, TRADE_EVENT_DISC):
            payloads.append(raw)

    return payloads


def connect_db() -> sqlite3.Connection:
    DB_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    conn.execute("PRAGMA busy_timeout = 5000;")
    return conn


def table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {
        row["name"]
        for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
    }


def add_column_if_missing(
    conn: sqlite3.Connection,
    table: str,
    column: str,
    ddl: str,
) -> None:
    if column not in table_columns(conn, table):
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
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
            FOREIGN KEY(signature) REFERENCES transactions(signature)
        );

        CREATE TABLE IF NOT EXISTS websocket_observations (
            signature TEXT PRIMARY KEY,
            slot INTEGER,
            received_at_utc TEXT NOT NULL,
            executed INTEGER NOT NULL,
            transaction_error_json TEXT,
            raw_notification_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS collector_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT NOT NULL,
            happened_at_utc TEXT NOT NULL,
            details_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS enrichment_jobs (
            signature TEXT PRIMARY KEY,
            status TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            next_retry_at_utc TEXT,
            last_error TEXT,
            created_at_utc TEXT NOT NULL,
            updated_at_utc TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_pump_events_signature
            ON pump_events(signature);
        CREATE INDEX IF NOT EXISTS idx_pump_events_type
            ON pump_events(event_type);
        CREATE INDEX IF NOT EXISTS idx_pump_events_mint
            ON pump_events(mint);
        CREATE INDEX IF NOT EXISTS idx_enrichment_jobs_status
            ON enrichment_jobs(status, next_retry_at_utc);
        CREATE INDEX IF NOT EXISTS idx_collector_events_type_time
            ON collector_events(event_type, happened_at_utc);
        """
    )

    add_column_if_missing(
        conn, "transactions", "confirmation_status",
        "confirmation_status TEXT"
    )
    add_column_if_missing(
        conn, "transactions", "confirmed_at_utc",
        "confirmed_at_utc TEXT"
    )
    add_column_if_missing(
        conn, "transactions", "raw_get_transaction_json",
        "raw_get_transaction_json TEXT"
    )

    add_column_if_missing(
        conn, "pump_events", "confirmation_status",
        "confirmation_status TEXT"
    )
    add_column_if_missing(
        conn, "pump_events", "confirmed_at_utc",
        "confirmed_at_utc TEXT"
    )

    conn.commit()


def operational_event(
    conn: sqlite3.Connection,
    event_type: str,
    **details: Any,
) -> None:
    now = utc_now()
    conn.execute(
        """
        INSERT INTO collector_events(event_type, happened_at_utc, details_json)
        VALUES (?, ?, ?)
        """,
        (
            event_type,
            now,
            json.dumps(details, ensure_ascii=False, separators=(",", ":")),
        ),
    )
    conn.commit()
    LOGGER.info("%s | %s", event_type, details)


def insert_observation(
    conn: sqlite3.Connection,
    signature: str,
    slot: int | None,
    received_at: str,
    error: Any,
    notification: dict[str, Any],
) -> bool:
    cursor = conn.execute(
        """
        INSERT OR IGNORE INTO websocket_observations(
            signature, slot, received_at_utc, executed,
            transaction_error_json, raw_notification_json
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            signature,
            slot,
            received_at,
            1 if error is None else 0,
            None if error is None else json.dumps(error, separators=(",", ":")),
            json.dumps(notification, ensure_ascii=False, separators=(",", ":")),
        ),
    )
    conn.commit()
    return cursor.rowcount == 1


def ensure_transaction_parent(
    conn: sqlite3.Connection,
    signature: str,
    slot: int | None,
    received_at: str,
) -> None:
    conn.execute(
        """
        INSERT OR IGNORE INTO transactions(
            signature, slot, block_time, fee_payer,
            transaction_error_json, fee_lamports,
            source_enrichment_file, enriched_at_utc,
            inserted_at_utc, confirmation_status
        )
        VALUES (?, ?, NULL, NULL, NULL, NULL, ?, ?, ?, ?)
        """,
        (
            signature,
            slot,
            "LIVE_COLLECTOR_V0_2",
            received_at,
            received_at,
            "processed",
        ),
    )
    conn.commit()


def enqueue_enrichment(
    conn: sqlite3.Connection,
    signature: str,
) -> None:
    now = utc_now()
    conn.execute(
        """
        INSERT INTO enrichment_jobs(
            signature, status, attempts, next_retry_at_utc,
            last_error, created_at_utc, updated_at_utc
        )
        VALUES (?, 'PENDING', 0, ?, NULL, ?, ?)
        ON CONFLICT(signature) DO UPDATE SET
            status = CASE
                WHEN enrichment_jobs.status = 'DONE' THEN 'DONE'
                ELSE 'PENDING'
            END,
            next_retry_at_utc = CASE
                WHEN enrichment_jobs.status = 'DONE'
                THEN enrichment_jobs.next_retry_at_utc
                ELSE excluded.next_retry_at_utc
            END,
            updated_at_utc = excluded.updated_at_utc
        """,
        (signature, now, now, now),
    )
    conn.commit()


def recover_enrichment_jobs(conn: sqlite3.Connection) -> int:
    now = utc_now()

    # Jobs interrupted during a previous process are safely retried.
    conn.execute(
        """
        UPDATE enrichment_jobs
        SET status='RETRY',
            next_retry_at_utc=?,
            updated_at_utc=?
        WHERE status='IN_PROGRESS'
        """,
        (now, now),
    )

    cursor = conn.execute(
        """
        INSERT OR IGNORE INTO enrichment_jobs(
            signature, status, attempts, next_retry_at_utc,
            last_error, created_at_utc, updated_at_utc
        )
        SELECT
            signature, 'PENDING', 0, ?, NULL, ?, ?
        FROM websocket_observations
        WHERE executed=1
        """,
        (now, now, now),
    )

    conn.commit()
    return cursor.rowcount if cursor.rowcount != -1 else 0


def insert_event(
    conn: sqlite3.Connection,
    event_key: str,
    signature: str,
    slot: int | None,
    decoded: dict[str, Any],
    observed_at: str,
    source: str,
    confirmation_status: str,
) -> bool:
    cursor = conn.execute(
        """
        INSERT OR IGNORE INTO pump_events(
            event_key, signature, event_type, pump_event_name,
            slot, block_time, pump_timestamp,
            mint, user_wallet, creator_wallet, fee_payer,
            name, symbol, uri, bonding_curve,
            is_buy, sol_amount_lamports, sol_amount, token_amount_raw,
            virtual_sol_reserves, virtual_token_reserves,
            real_sol_reserves, real_token_reserves,
            quote_mint, quote_amount_raw,
            virtual_quote_reserves, real_quote_reserves,
            source_decoded_file, decoded_at_utc, inserted_at_utc,
            event_json, confirmation_status, confirmed_at_utc
        )
        VALUES (
            ?, ?, ?, ?,
            ?, NULL, ?,
            ?, ?, ?, NULL,
            ?, ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?, ?,
            ?, ?, ?
        )
        """,
        (
            event_key,
            signature,
            decoded.get("event_type"),
            decoded.get("pump_event_name"),
            slot,
            decoded.get("timestamp"),
            decoded.get("mint"),
            decoded.get("user"),
            decoded.get("creator"),
            decoded.get("name"),
            decoded.get("symbol"),
            decoded.get("uri"),
            decoded.get("bonding_curve"),
            None if decoded.get("is_buy") is None else int(decoded.get("is_buy")),
            decoded.get("sol_amount_lamports"),
            decoded.get("sol_amount"),
            None if decoded.get("token_amount_raw") is None else str(decoded.get("token_amount_raw")),
            None if decoded.get("virtual_sol_reserves") is None else str(decoded.get("virtual_sol_reserves")),
            None if decoded.get("virtual_token_reserves") is None else str(decoded.get("virtual_token_reserves")),
            None if decoded.get("real_sol_reserves") is None else str(decoded.get("real_sol_reserves")),
            None if decoded.get("real_token_reserves") is None else str(decoded.get("real_token_reserves")),
            decoded.get("quote_mint"),
            None if decoded.get("quote_amount_raw") is None else str(decoded.get("quote_amount_raw")),
            None if decoded.get("virtual_quote_reserves") is None else str(decoded.get("virtual_quote_reserves")),
            None if decoded.get("real_quote_reserves") is None else str(decoded.get("real_quote_reserves")),
            source,
            observed_at,
            observed_at,
            json.dumps(decoded, ensure_ascii=False, separators=(",", ":")),
            confirmation_status,
            observed_at if confirmation_status == "confirmed" else None,
        ),
    )
    conn.commit()
    return cursor.rowcount == 1


def account_pubkey(entry: Any) -> str | None:
    if isinstance(entry, str):
        return entry
    if isinstance(entry, dict):
        value = entry.get("pubkey")
        return str(value) if value is not None else None
    return None


async def get_transaction_confirmed(
    client: httpx.AsyncClient,
    signature: str,
) -> dict[str, Any] | None:
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTransaction",
        "params": [
            signature,
            {
                "commitment": "confirmed",
                "encoding": "jsonParsed",
                "maxSupportedTransactionVersion": 0,
            },
        ],
    }

    response = await client.post(HTTP_RPC_URL, json=payload)

    if response.status_code == 429:
        retry_after = response.headers.get("Retry-After")
        delay = float(retry_after) if retry_after else 3.0
        raise RuntimeError(f"HTTP_429:{delay}")

    response.raise_for_status()
    body = response.json()

    if "error" in body:
        raise RuntimeError(f"RPC_ERROR:{body['error']}")

    return body.get("result")


def next_enrichment_job(conn: sqlite3.Connection) -> sqlite3.Row | None:
    now = utc_now()
    return conn.execute(
        """
        SELECT *
        FROM enrichment_jobs
        WHERE status IN ('PENDING', 'RETRY')
          AND (next_retry_at_utc IS NULL OR next_retry_at_utc <= ?)
        ORDER BY created_at_utc, attempts
        LIMIT 1
        """,
        (now,),
    ).fetchone()


def mark_job_in_progress(
    conn: sqlite3.Connection,
    signature: str,
) -> int:
    now = utc_now()
    conn.execute(
        """
        UPDATE enrichment_jobs
        SET status='IN_PROGRESS',
            attempts=attempts+1,
            updated_at_utc=?
        WHERE signature=?
        """,
        (now, signature),
    )
    conn.commit()

    row = conn.execute(
        "SELECT attempts FROM enrichment_jobs WHERE signature=?",
        (signature,),
    ).fetchone()
    return int(row["attempts"])


def mark_job_retry(
    conn: sqlite3.Connection,
    signature: str,
    attempts: int,
    error: str,
    explicit_delay: float | None = None,
) -> None:
    delay = explicit_delay if explicit_delay is not None else min(2 ** min(attempts, 5), 30)
    conn.execute(
        """
        UPDATE enrichment_jobs
        SET status='RETRY',
            next_retry_at_utc=?,
            last_error=?,
            updated_at_utc=?
        WHERE signature=?
        """,
        (
            utc_after(delay),
            error[:1000],
            utc_now(),
            signature,
        ),
    )
    conn.commit()


def mark_job_done(
    conn: sqlite3.Connection,
    signature: str,
) -> None:
    conn.execute(
        """
        UPDATE enrichment_jobs
        SET status='DONE',
            next_retry_at_utc=NULL,
            last_error=NULL,
            updated_at_utc=?
        WHERE signature=?
        """,
        (utc_now(), signature),
    )
    conn.commit()


def update_confirmed_transaction(
    conn: sqlite3.Connection,
    signature: str,
    result: dict[str, Any],
) -> int:
    meta = result.get("meta") or {}
    tx = result.get("transaction") or {}
    message = tx.get("message") or {}
    keys = message.get("accountKeys") or []
    fee_payer = account_pubkey(keys[0]) if keys else None
    confirmed_at = utc_now()

    conn.execute(
        """
        UPDATE transactions
        SET slot=?,
            block_time=?,
            fee_payer=?,
            transaction_error_json=?,
            fee_lamports=?,
            enriched_at_utc=?,
            confirmation_status='confirmed',
            confirmed_at_utc=?,
            raw_get_transaction_json=?
        WHERE signature=?
        """,
        (
            result.get("slot"),
            result.get("blockTime"),
            fee_payer,
            None if meta.get("err") is None else json.dumps(meta.get("err"), separators=(",", ":")),
            meta.get("fee"),
            confirmed_at,
            confirmed_at,
            json.dumps(result, ensure_ascii=False, separators=(",", ":")),
            signature,
        ),
    )

    conn.execute(
        """
        UPDATE pump_events
        SET block_time=?,
            fee_payer=COALESCE(fee_payer, ?),
            confirmation_status='confirmed',
            confirmed_at_utc=?
        WHERE signature=?
        """,
        (
            result.get("blockTime"),
            fee_payer,
            confirmed_at,
            signature,
        ),
    )
    conn.commit()

    logs = meta.get("logMessages") or []
    payloads = extract_pump_scoped_event_payloads(logs)
    inserted = 0

    ensure_transaction_parent(
        conn,
        signature,
        result.get("slot"),
        confirmed_at,
    )

    for event_index, payload in enumerate(payloads):
        if payload[:8] == CREATE_EVENT_DISC:
            decoded = decode_create_event(payload)
        elif payload[:8] == TRADE_EVENT_DISC:
            decoded = decode_trade_event(payload)
        else:
            continue

        event_key = f"{signature}:{event_index}:{payload[:8].hex()}"
        if insert_event(
            conn,
            event_key,
            signature,
            result.get("slot"),
            decoded,
            confirmed_at,
            "CONFIRMED_GET_TRANSACTION",
            "confirmed",
        ):
            inserted += 1

    return inserted


async def enrichment_worker(
    stop_event: asyncio.Event,
    stats: dict[str, int],
) -> None:
    conn = connect_db()
    ensure_schema(conn)

    timeout = httpx.Timeout(20.0, connect=10.0)
    async with httpx.AsyncClient(
        timeout=timeout,
        headers={"Content-Type": "application/json"},
    ) as client:
        while True:
            job = next_enrichment_job(conn)

            if job is None:
                if stop_event.is_set():
                    break
                await asyncio.sleep(0.25)
                continue

            signature = job["signature"]
            attempts = mark_job_in_progress(conn, signature)

            try:
                result = await get_transaction_confirmed(client, signature)

                if result is None:
                    stats["enrichment_null"] += 1
                    mark_job_retry(
                        conn,
                        signature,
                        attempts,
                        "getTransaction returned null at confirmed commitment",
                    )
                    await asyncio.sleep(0.25)
                    continue

                newly_inserted = update_confirmed_transaction(
                    conn,
                    signature,
                    result,
                )
                mark_job_done(conn, signature)
                stats["enrichment_confirmed"] += 1
                stats["confirmed_events_recovered"] += newly_inserted
                await asyncio.sleep(0.35)

            except Exception as exc:
                stats["enrichment_errors"] += 1
                text = f"{type(exc).__name__}: {exc}"

                explicit_delay = None
                if str(exc).startswith("HTTP_429:"):
                    try:
                        explicit_delay = float(str(exc).split(":", 1)[1])
                    except Exception:
                        explicit_delay = 3.0

                mark_job_retry(
                    conn,
                    signature,
                    attempts,
                    text,
                    explicit_delay,
                )
                LOGGER.warning("ENRICHMENT_RETRY | sig=%s | %s", signature, text)
                await asyncio.sleep(0.25)

    conn.close()


async def subscribe(websocket) -> int:
    request = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "logsSubscribe",
        "params": [
            {"mentions": [PUMP_PROGRAM_ID]},
            {"commitment": "processed"},
        ],
    }
    await websocket.send(json.dumps(request))

    while True:
        raw = await asyncio.wait_for(websocket.recv(), timeout=15)
        response = json.loads(raw)

        if response.get("id") != 1:
            continue
        if "error" in response:
            raise RuntimeError(response["error"])
        return int(response["result"])


async def unsubscribe(websocket, subscription_id: int) -> None:
    request = {
        "jsonrpc": "2.0",
        "id": 2,
        "method": "logsUnsubscribe",
        "params": [subscription_id],
    }
    await websocket.send(json.dumps(request))
    try:
        await asyncio.wait_for(websocket.recv(), timeout=5)
    except asyncio.TimeoutError:
        pass


async def health_task(
    stop_event: asyncio.Event,
    stats: dict[str, int],
) -> None:
    conn = connect_db()
    ensure_schema(conn)

    while not stop_event.is_set():
        try:
            await asyncio.wait_for(
                stop_event.wait(),
                timeout=HEALTH_INTERVAL_SECONDS,
            )
        except asyncio.TimeoutError:
            backlog = conn.execute(
                """
                SELECT COUNT(*) AS n
                FROM enrichment_jobs
                WHERE status != 'DONE'
                """
            ).fetchone()["n"]

            LOGGER.info(
                "HEALTH | notifications=%s | new_obs=%s | pump_inserted=%s | "
                "confirmed=%s | enrichment_backlog=%s | reconnects=%s",
                stats["notifications"],
                stats["new_observations"],
                stats["pump_events_inserted"],
                stats["enrichment_confirmed"],
                backlog,
                stats["reconnects"],
            )

    conn.close()


async def live_listener(
    stop_event: asyncio.Event,
    stats: dict[str, int],
    max_pump_events: int | None,
) -> None:
    conn = connect_db()
    ensure_schema(conn)
    raw_path = raw_file_path()

    recovered = recover_enrichment_jobs(conn)
    operational_event(
        conn,
        "COLLECTOR_START",
        recovered_enrichment_jobs=recovered,
        raw_path=str(raw_path),
        db_path=str(DB_PATH),
    )

    reconnect_attempt = 0
    gap_started_at: datetime | None = None

    while not stop_event.is_set():
        subscription_id = None

        try:
            async with connect(
                WS_URL,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=10,
                max_size=4 * 1024 * 1024,
            ) as websocket:
                stats["reconnects"] += 1 if reconnect_attempt > 0 else 0

                operational_event(
                    conn,
                    "WS_CONNECTED",
                    reconnect_attempt=reconnect_attempt,
                )

                if gap_started_at is not None:
                    duration = (
                        datetime.now(timezone.utc) - gap_started_at
                    ).total_seconds()
                    stats["gap_count"] += 1
                    stats["gap_seconds"] += int(duration)

                    operational_event(
                        conn,
                        "DATA_GAP_CLOSED",
                        gap_started_at_utc=gap_started_at.isoformat(),
                        gap_ended_at_utc=utc_now(),
                        gap_seconds=duration,
                        note=(
                            "Gap is explicitly recorded. This v0.2 collector "
                            "does not silently treat missing time as no activity."
                        ),
                    )
                    gap_started_at = None

                reconnect_attempt = 0
                subscription_id = await subscribe(websocket)

                operational_event(
                    conn,
                    "PUMP_SUBSCRIPTION_ACTIVE",
                    subscription_id=subscription_id,
                )

                while not stop_event.is_set():
                    try:
                        raw = await asyncio.wait_for(
                            websocket.recv(),
                            timeout=STALE_WEBSOCKET_SECONDS,
                        )
                    except asyncio.TimeoutError as exc:
                        operational_event(
                            conn,
                            "WS_STALE",
                            timeout_seconds=STALE_WEBSOCKET_SECONDS,
                        )
                        raise RuntimeError(
                            f"No WebSocket notification for "
                            f"{STALE_WEBSOCKET_SECONDS}s"
                        ) from exc

                    message = json.loads(raw)
                    if message.get("method") != "logsNotification":
                        continue

                    stats["notifications"] += 1
                    received_at = utc_now()

                    params = message.get("params", {})
                    result = params.get("result", {})
                    context = result.get("context", {})
                    value = result.get("value", {})

                    signature = value.get("signature")
                    if not signature:
                        continue

                    slot = context.get("slot")
                    error = value.get("err")
                    logs = value.get("logs") or []

                    append_jsonl(
                        raw_path,
                        {
                            "schema_version": "live_pump_notification_v0.2",
                            "received_at_utc": received_at,
                            "slot": slot,
                            "signature": signature,
                            "transaction_error": error,
                            "logs": logs,
                        },
                    )

                    is_new = insert_observation(
                        conn,
                        signature,
                        slot,
                        received_at,
                        error,
                        message,
                    )

                    if not is_new:
                        stats["duplicate_observations"] += 1
                        continue

                    stats["new_observations"] += 1

                    if error is not None:
                        stats["failed_transactions"] += 1
                        continue

                    ensure_transaction_parent(
                        conn,
                        signature,
                        slot,
                        received_at,
                    )
                    enqueue_enrichment(conn, signature)

                    payloads = extract_pump_scoped_event_payloads(logs)

                    if not payloads:
                        stats["successful_non_pump_noise"] += 1
                        continue

                    for event_index, payload in enumerate(payloads):
                        try:
                            if payload[:8] == CREATE_EVENT_DISC:
                                decoded = decode_create_event(payload)
                            elif payload[:8] == TRADE_EVENT_DISC:
                                decoded = decode_trade_event(payload)
                            else:
                                continue

                            stats["pump_events_decoded"] += 1

                            event_key = (
                                f"{signature}:{event_index}:{payload[:8].hex()}"
                            )

                            inserted = insert_event(
                                conn,
                                event_key,
                                signature,
                                slot,
                                decoded,
                                received_at,
                                "LIVE_WEBSOCKET_EVENT_V0_2",
                                "processed",
                            )

                            if not inserted:
                                stats["duplicate_pump_events"] += 1
                                continue

                            stats["pump_events_inserted"] += 1

                            if decoded["event_type"] == "LAUNCH":
                                print(
                                    f"[LAUNCH] {decoded.get('symbol', '-')} | "
                                    f"mint={decoded.get('mint')} | "
                                    f"creator={decoded.get('creator')}"
                                )
                            else:
                                print(
                                    f"[{decoded['event_type']}] "
                                    f"mint={decoded.get('mint')} | "
                                    f"user={decoded.get('user')} | "
                                    f"SOL={decoded.get('sol_amount', 0):.9f} | "
                                    f"slot={slot}"
                                )

                        except Exception as exc:
                            stats["decode_errors"] += 1
                            operational_event(
                                conn,
                                "DECODE_ERROR",
                                signature=signature,
                                error=f"{type(exc).__name__}: {exc}",
                            )

                    if (
                        max_pump_events is not None
                        and stats["pump_events_inserted"] >= max_pump_events
                    ):
                        operational_event(
                            conn,
                            "TEST_TARGET_REACHED",
                            pump_events_inserted=stats["pump_events_inserted"],
                        )
                        stop_event.set()
                        break

                if subscription_id is not None:
                    try:
                        await unsubscribe(websocket, subscription_id)
                    except Exception:
                        pass

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if stop_event.is_set():
                break

            if gap_started_at is None:
                gap_started_at = datetime.now(timezone.utc)

            reconnect_attempt += 1
            delay = min(2 ** min(reconnect_attempt - 1, 5), 30)

            operational_event(
                conn,
                "WS_DISCONNECTED",
                reconnect_attempt=reconnect_attempt,
                retry_in_seconds=delay,
                error=f"{type(exc).__name__}: {exc}",
            )

            try:
                await asyncio.wait_for(
                    stop_event.wait(),
                    timeout=delay,
                )
            except asyncio.TimeoutError:
                pass

    operational_event(
        conn,
        "COLLECTOR_STOP",
        pump_events_inserted=stats["pump_events_inserted"],
    )
    conn.close()


async def wait_for_enrichment_drain(
    seconds: int,
) -> int:
    conn = connect_db()
    ensure_schema(conn)

    for _ in range(seconds * 2):
        row = conn.execute(
            """
            SELECT COUNT(*) AS n
            FROM enrichment_jobs
            WHERE status != 'DONE'
            """
        ).fetchone()
        remaining = int(row["n"])
        if remaining == 0:
            conn.close()
            return 0
        await asyncio.sleep(0.5)

    row = conn.execute(
        """
        SELECT COUNT(*) AS n
        FROM enrichment_jobs
        WHERE status != 'DONE'
        """
    ).fetchone()
    remaining = int(row["n"])
    conn.close()
    return remaining


async def run(
    max_pump_events: int | None,
    drain_seconds: int,
) -> None:
    stop_event = asyncio.Event()
    stats = {
        "notifications": 0,
        "new_observations": 0,
        "duplicate_observations": 0,
        "failed_transactions": 0,
        "successful_non_pump_noise": 0,
        "pump_events_decoded": 0,
        "pump_events_inserted": 0,
        "duplicate_pump_events": 0,
        "decode_errors": 0,
        "reconnects": 0,
        "gap_count": 0,
        "gap_seconds": 0,
        "enrichment_confirmed": 0,
        "enrichment_null": 0,
        "enrichment_errors": 0,
        "confirmed_events_recovered": 0,
    }

    print("=" * 100)
    print("SOLANA MEMECOIN BOT - ROBUST LIVE COLLECTOR v0.2")
    print("=" * 100)
    print(f"UTC start          : {utc_now()}")
    print(f"WebSocket          : {WS_URL}")
    print(f"HTTP RPC           : {HTTP_RPC_URL}")
    print(f"SQLite             : {DB_PATH}")
    print(f"Raw JSONL          : {raw_file_path()}")
    print(f"Operational log    : {log_file_path()}")
    print(
        f"Target new events  : "
        f"{max_pump_events if max_pump_events is not None else 'unlimited'}"
    )
    print("Mode               : READ ONLY / UNATTENDED-COLLECTOR TEST")
    print()

    listener = asyncio.create_task(
        live_listener(stop_event, stats, max_pump_events)
    )
    worker = asyncio.create_task(
        enrichment_worker(stop_event, stats)
    )
    health = asyncio.create_task(
        health_task(stop_event, stats)
    )

    try:
        await listener
    except KeyboardInterrupt:
        stop_event.set()
    finally:
        stop_event.set()

        print()
        print(
            f"[INFO] Waiting up to {drain_seconds}s for persistent "
            f"enrichment queue to drain..."
        )
        remaining_jobs = await wait_for_enrichment_drain(drain_seconds)

        await asyncio.gather(worker, health, return_exceptions=True)

        print()
        print("-" * 100)
        print("SUMMARY")
        print("-" * 100)
        print(f"Notifications received    : {stats['notifications']}")
        print(f"New observations          : {stats['new_observations']}")
        print(f"Duplicate observations    : {stats['duplicate_observations']}")
        print(f"Failed transactions       : {stats['failed_transactions']}")
        print(f"Successful non-Pump noise : {stats['successful_non_pump_noise']}")
        print(f"Pump events decoded       : {stats['pump_events_decoded']}")
        print(f"Pump events inserted      : {stats['pump_events_inserted']}")
        print(f"Duplicate Pump events     : {stats['duplicate_pump_events']}")
        print(f"Decode errors             : {stats['decode_errors']}")
        print(f"Reconnects                : {stats['reconnects']}")
        print(f"Explicit gap records      : {stats['gap_count']}")
        print(f"Recorded gap seconds      : {stats['gap_seconds']}")
        print(f"Confirmed enrichments     : {stats['enrichment_confirmed']}")
        print(f"Enrichment null responses : {stats['enrichment_null']}")
        print(f"Enrichment errors/retries : {stats['enrichment_errors']}")
        print(f"Confirmed events recovered: {stats['confirmed_events_recovered']}")
        print(f"Enrichment jobs remaining : {remaining_jobs}")
        print(f"SQLite                    : {DB_PATH}")
        print(f"Raw JSONL                 : {raw_file_path()}")
        print(f"Operational log           : {log_file_path()}")
        print()

        target_met = (
            max_pump_events is None
            or stats["pump_events_inserted"] >= max_pump_events
        )

        if target_met and stats["decode_errors"] == 0:
            print("RESULT: PASS")
        elif stats["decode_errors"] == 0:
            print("RESULT: STOPPED CLEANLY")
        else:
            print("RESULT: CHECK DECODE ERRORS")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--max-pump-events",
        type=int,
        default=20,
        help=(
            "Stop after N NEW Pump events are inserted. "
            "Use 0 for unlimited mode. Default: 20."
        ),
    )
    parser.add_argument(
        "--drain-seconds",
        type=int,
        default=30,
        help="Wait this long for background enrichment on shutdown.",
    )
    args = parser.parse_args()

    max_events = None if args.max_pump_events == 0 else args.max_pump_events

    try:
        asyncio.run(run(max_events, args.drain_seconds))
    except KeyboardInterrupt:
        print()
        print("[STOP] Ctrl+C received.")


if __name__ == "__main__":
    main()
