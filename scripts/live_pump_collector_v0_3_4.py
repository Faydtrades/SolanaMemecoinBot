import argparse
import asyncio
import base64
import json
import logging
import re
import sqlite3
import struct
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

import httpx
from websockets.asyncio.client import connect


HTTP_RPC_URL = "https://api.mainnet.solana.com"
WS_URL = "wss://api.mainnet.solana.com"
PUMP_PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
LAMPORTS_PER_SOL = 1_000_000_000

# Official Pump event discriminators already validated by decoder v0.2.
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

# getSignatureStatuses supports far more, but 64 keeps requests small while
# still reducing HTTP calls dramatically versus one RPC request per event.
STATUS_BATCH_SIZE = 64
STATUS_POLL_SECONDS = 1.25

# Shared public-RPC pacing. This is intentionally conservative.
MIN_HTTP_REQUEST_INTERVAL = 0.45

# Gap reconciliation is deliberately conservative because the public Solana
# RPC is rate-limited. Rare reconnect gaps are persisted and recovered in the
# background without blocking the live WebSocket path.
GAP_RECONCILE_MAX_SLOT_SPAN = 512
GAP_RECONCILE_RETRY_SECONDS = 15.0
GAP_RECONCILE_CONFIRMATION_BACKLOG_LIMIT = 128
GAP_RECONCILE_DEEP_BACKLOG_LIMIT = 8


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def utc_after(seconds: float) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).isoformat()


def raw_file_path() -> Path:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    day = datetime.now(timezone.utc).strftime("%Y%m%d")
    return RAW_DIR / f"live_pump_notifications_v0_3_4_{day}.jsonl"


def log_file_path() -> Path:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    day = datetime.now(timezone.utc).strftime("%Y%m%d")
    return LOG_DIR / f"live_collector_v0_3_4_{day}.log"


def append_jsonl(path: Path, record: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
        handle.flush()


def setup_logging() -> logging.Logger:
    logger = logging.getLogger("live_collector_v0_3")
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


class RpcRateGate:
    """
    One shared pacing/cooldown gate for all HTTP RPC workers.

    This prevents multiple background workers from independently hammering the
    same public endpoint after a 429.
    """

    def __init__(self, min_interval: float):
        self.min_interval = min_interval
        self._lock = asyncio.Lock()
        self._last_request_monotonic = 0.0
        self._cooldown_until_monotonic = 0.0

    async def wait_turn(self) -> None:
        async with self._lock:
            now = time.monotonic()
            target = max(
                self._cooldown_until_monotonic,
                self._last_request_monotonic + self.min_interval,
            )
            if target > now:
                await asyncio.sleep(target - now)
            self._last_request_monotonic = time.monotonic()

    async def apply_cooldown(self, seconds: float) -> None:
        async with self._lock:
            self._cooldown_until_monotonic = max(
                self._cooldown_until_monotonic,
                time.monotonic() + max(seconds, 0.5),
            )


RPC_GATE = RpcRateGate(MIN_HTTP_REQUEST_INTERVAL)


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

    # Current Pump tail: decode opportunistically, preserving the stable/core
    # fields above even if a future program version adds more fields.
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

        CREATE TABLE IF NOT EXISTS confirmation_jobs_v034 (
            signature TEXT PRIMARY KEY,
            status TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            next_poll_at_utc TEXT,
            last_error TEXT,
            created_at_utc TEXT NOT NULL,
            updated_at_utc TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS deep_enrichment_jobs_v034 (
            signature TEXT PRIMARY KEY,
            reason TEXT NOT NULL,
            status TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            next_retry_at_utc TEXT,
            last_error TEXT,
            created_at_utc TEXT NOT NULL,
            updated_at_utc TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS gap_jobs_v034 (
            gap_key TEXT PRIMARY KEY,
            gap_started_at_utc TEXT NOT NULL,
            gap_ended_at_utc TEXT NOT NULL,
            before_signature TEXT,
            after_signature TEXT,
            before_slot INTEGER,
            after_slot INTEGER,
            first_interior_slot INTEGER,
            last_interior_slot INTEGER,
            status TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            next_retry_at_utc TEXT,
            last_error TEXT,
            confirmed_blocks_checked INTEGER NOT NULL DEFAULT 0,
            events_recovered INTEGER NOT NULL DEFAULT 0,
            created_at_utc TEXT NOT NULL,
            updated_at_utc TEXT NOT NULL,
            completed_at_utc TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_pump_events_signature
            ON pump_events(signature);
        CREATE INDEX IF NOT EXISTS idx_pump_events_type
            ON pump_events(event_type);
        CREATE INDEX IF NOT EXISTS idx_pump_events_mint
            ON pump_events(mint);
        CREATE INDEX IF NOT EXISTS idx_pump_events_user
            ON pump_events(user_wallet);
        CREATE INDEX IF NOT EXISTS idx_confirmation_jobs_v034_status
            ON confirmation_jobs_v034(status, next_poll_at_utc);
        CREATE INDEX IF NOT EXISTS idx_deep_jobs_v034_status
            ON deep_enrichment_jobs_v034(status, next_retry_at_utc);
        CREATE INDEX IF NOT EXISTS idx_gap_jobs_v034_status
            ON gap_jobs_v034(status, next_retry_at_utc);
        CREATE INDEX IF NOT EXISTS idx_collector_events_type_time
            ON collector_events(event_type, happened_at_utc);
        """
    )

    add_column_if_missing(
        conn,
        "transactions",
        "confirmation_status",
        "confirmation_status TEXT",
    )
    add_column_if_missing(
        conn,
        "transactions",
        "confirmed_at_utc",
        "confirmed_at_utc TEXT",
    )
    add_column_if_missing(
        conn,
        "transactions",
        "raw_get_transaction_json",
        "raw_get_transaction_json TEXT",
    )
    add_column_if_missing(
        conn,
        "pump_events",
        "confirmation_status",
        "confirmation_status TEXT",
    )
    add_column_if_missing(
        conn,
        "pump_events",
        "confirmed_at_utc",
        "confirmed_at_utc TEXT",
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
            signature,
            slot,
            received_at_utc,
            executed,
            transaction_error_json,
            raw_notification_json
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
            signature,
            slot,
            block_time,
            fee_payer,
            transaction_error_json,
            fee_lamports,
            source_enrichment_file,
            enriched_at_utc,
            inserted_at_utc,
            confirmation_status
        )
        VALUES (?, ?, NULL, NULL, NULL, NULL, ?, ?, ?, ?)
        """,
        (
            signature,
            slot,
            "LIVE_COLLECTOR_V0_3_4",
            received_at,
            received_at,
            "processed",
        ),
    )
    conn.commit()


def insert_event(
    conn: sqlite3.Connection,
    event_key: str,
    signature: str,
    slot: int | None,
    decoded: dict[str, Any],
    observed_at: str,
    source_decoded_file: str = "LIVE_WEBSOCKET_EVENT_V0_3_4",
    confirmation_status: str = "processed",
    block_time: int | None = None,
) -> bool:
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
            event_json,
            confirmation_status,
            confirmed_at_utc
        )
        VALUES (
            ?, ?, ?, ?,
            ?, ?, ?,
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
            block_time,
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
            None
            if decoded.get("token_amount_raw") is None
            else str(decoded.get("token_amount_raw")),
            None
            if decoded.get("virtual_sol_reserves") is None
            else str(decoded.get("virtual_sol_reserves")),
            None
            if decoded.get("virtual_token_reserves") is None
            else str(decoded.get("virtual_token_reserves")),
            None
            if decoded.get("real_sol_reserves") is None
            else str(decoded.get("real_sol_reserves")),
            None
            if decoded.get("real_token_reserves") is None
            else str(decoded.get("real_token_reserves")),
            decoded.get("quote_mint"),
            None
            if decoded.get("quote_amount_raw") is None
            else str(decoded.get("quote_amount_raw")),
            None
            if decoded.get("virtual_quote_reserves") is None
            else str(decoded.get("virtual_quote_reserves")),
            None
            if decoded.get("real_quote_reserves") is None
            else str(decoded.get("real_quote_reserves")),
            source_decoded_file,
            observed_at,
            observed_at,
            json.dumps(decoded, ensure_ascii=False, separators=(",", ":")),
            confirmation_status,
            observed_at if confirmation_status in ("confirmed", "finalized") else None,
        ),
    )
    conn.commit()
    return cursor.rowcount == 1



def enqueue_gap_job(
    conn: sqlite3.Connection,
    gap_started_at_utc: str,
    gap_ended_at_utc: str,
    before_signature: str | None,
    after_signature: str | None,
    before_slot: int | None,
    after_slot: int | None,
) -> str:
    now = utc_now()
    gap_key = (
        f"{before_signature or 'NONE'}:"
        f"{after_signature or 'NONE'}:"
        f"{gap_started_at_utc}"
    )

    first_interior = None
    last_interior = None
    status = "PENDING"

    if before_slot is None or after_slot is None:
        status = "DONE_NO_BOUNDARY"
    elif after_slot <= before_slot + 1:
        status = "DONE_NO_INTERIOR"
    else:
        first_interior = before_slot + 1
        last_interior = after_slot - 1

    completed_at = now if status.startswith("DONE_") else None

    conn.execute(
        """
        INSERT OR IGNORE INTO gap_jobs_v034(
            gap_key,
            gap_started_at_utc,
            gap_ended_at_utc,
            before_signature,
            after_signature,
            before_slot,
            after_slot,
            first_interior_slot,
            last_interior_slot,
            status,
            attempts,
            next_retry_at_utc,
            last_error,
            confirmed_blocks_checked,
            events_recovered,
            created_at_utc,
            updated_at_utc,
            completed_at_utc
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, NULL, 0, 0, ?, ?, ?)
        """,
        (
            gap_key,
            gap_started_at_utc,
            gap_ended_at_utc,
            before_signature,
            after_signature,
            before_slot,
            after_slot,
            first_interior,
            last_interior,
            status,
            now if status == "PENDING" else None,
            now,
            now,
            completed_at,
        ),
    )
    conn.commit()
    return gap_key


def next_gap_job(conn: sqlite3.Connection) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT *
        FROM gap_jobs_v034
        WHERE status IN ('PENDING', 'RETRY')
          AND (next_retry_at_utc IS NULL OR next_retry_at_utc <= ?)
        ORDER BY created_at_utc, attempts
        LIMIT 1
        """,
        (utc_now(),),
    ).fetchone()


def mark_gap_in_progress(
    conn: sqlite3.Connection,
    gap_key: str,
) -> int:
    now = utc_now()
    conn.execute(
        """
        UPDATE gap_jobs_v034
        SET status='IN_PROGRESS',
            attempts=attempts+1,
            updated_at_utc=?
        WHERE gap_key=?
        """,
        (now, gap_key),
    )
    conn.commit()
    row = conn.execute(
        "SELECT attempts FROM gap_jobs_v034 WHERE gap_key=?",
        (gap_key,),
    ).fetchone()
    return int(row["attempts"])


def mark_gap_retry(
    conn: sqlite3.Connection,
    gap_key: str,
    error: str,
    delay: float = GAP_RECONCILE_RETRY_SECONDS,
) -> None:
    conn.execute(
        """
        UPDATE gap_jobs_v034
        SET status='RETRY',
            next_retry_at_utc=?,
            last_error=?,
            updated_at_utc=?
        WHERE gap_key=?
        """,
        (
            utc_after(delay),
            error[:1000],
            utc_now(),
            gap_key,
        ),
    )
    conn.commit()


def mark_gap_done(
    conn: sqlite3.Connection,
    gap_key: str,
    blocks_checked: int,
    events_recovered: int,
    status: str = "DONE",
) -> None:
    now = utc_now()
    conn.execute(
        """
        UPDATE gap_jobs_v034
        SET status=?,
            next_retry_at_utc=NULL,
            last_error=NULL,
            confirmed_blocks_checked=?,
            events_recovered=?,
            updated_at_utc=?,
            completed_at_utc=?
        WHERE gap_key=?
        """,
        (
            status,
            blocks_checked,
            events_recovered,
            now,
            now,
            gap_key,
        ),
    )
    conn.commit()


def recover_gap_jobs(conn: sqlite3.Connection) -> int:
    now = utc_now()
    cursor = conn.execute(
        """
        UPDATE gap_jobs_v034
        SET status='RETRY',
            next_retry_at_utc=?,
            updated_at_utc=?
        WHERE status='IN_PROGRESS'
        """,
        (now, now),
    )
    conn.commit()
    return cursor.rowcount if cursor.rowcount != -1 else 0



def enqueue_confirmation(
    conn: sqlite3.Connection,
    signature: str,
) -> None:
    now = utc_now()
    conn.execute(
        """
        INSERT INTO confirmation_jobs_v034(
            signature,
            status,
            attempts,
            next_poll_at_utc,
            last_error,
            created_at_utc,
            updated_at_utc
        )
        VALUES (?, 'PENDING', 0, ?, NULL, ?, ?)
        ON CONFLICT(signature) DO UPDATE SET
            status = CASE
                WHEN confirmation_jobs_v034.status = 'DONE'
                THEN 'DONE'
                ELSE 'PENDING'
            END,
            next_poll_at_utc = CASE
                WHEN confirmation_jobs_v034.status = 'DONE'
                THEN confirmation_jobs_v034.next_poll_at_utc
                ELSE excluded.next_poll_at_utc
            END,
            updated_at_utc = excluded.updated_at_utc
        """,
        (signature, now, now, now),
    )
    conn.commit()


def enqueue_deep_launch_enrichment(
    conn: sqlite3.Connection,
    signature: str,
) -> None:
    now = utc_now()
    conn.execute(
        """
        INSERT INTO deep_enrichment_jobs_v034(
            signature,
            reason,
            status,
            attempts,
            next_retry_at_utc,
            last_error,
            created_at_utc,
            updated_at_utc
        )
        VALUES (?, 'LAUNCH', 'PENDING', 0, ?, NULL, ?, ?)
        ON CONFLICT(signature) DO NOTHING
        """,
        (signature, now, now, now),
    )
    conn.commit()



def recover_jobs(conn: sqlite3.Connection) -> tuple[int, int, int]:
    # v0.3.4 fast-start recovery:
    # resume only genuinely interrupted jobs from the isolated v0.3.4
    # active queue generation. Historical pump_events are NOT rescanned.
    now = utc_now()

    confirm_cursor = conn.execute(
        """
        UPDATE confirmation_jobs_v034
        SET status='PENDING',
            next_poll_at_utc=?,
            updated_at_utc=?
        WHERE status='IN_PROGRESS'
        """,
        (now, now),
    )

    deep_cursor = conn.execute(
        """
        UPDATE deep_enrichment_jobs_v034
        SET status='PENDING',
            next_retry_at_utc=?,
            updated_at_utc=?
        WHERE status='IN_PROGRESS'
        """,
        (now, now),
    )

    conn.commit()

    recovered_confirm = (
        confirm_cursor.rowcount if confirm_cursor.rowcount != -1 else 0
    )
    recovered_deep = (
        deep_cursor.rowcount if deep_cursor.rowcount != -1 else 0
    )

    # Exact pre-v0.3.4 backlog counting is intentionally omitted from
    # live startup; old queue tables remain preserved in SQLite.
    legacy_backlog_deferred = 0

    return (
        int(recovered_confirm),
        int(recovered_deep),
        legacy_backlog_deferred,
    )



def confirmation_batch(
    conn: sqlite3.Connection,
    limit: int,
) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT *
        FROM confirmation_jobs_v034
        WHERE status IN ('PENDING', 'RETRY')
          AND (
              next_poll_at_utc IS NULL
              OR next_poll_at_utc <= ?
          )
        ORDER BY created_at_utc, attempts
        LIMIT ?
        """,
        (utc_now(), limit),
    ).fetchall()


def mark_confirmation_batch_in_progress(
    conn: sqlite3.Connection,
    signatures: list[str],
) -> None:
    if not signatures:
        return

    now = utc_now()
    placeholders = ",".join("?" for _ in signatures)

    conn.execute(
        f"""
        UPDATE confirmation_jobs_v034
        SET status='IN_PROGRESS',
            attempts=attempts+1,
            updated_at_utc=?
        WHERE signature IN ({placeholders})
        """,
        [now, *signatures],
    )
    conn.commit()


def mark_confirmation_retry(
    conn: sqlite3.Connection,
    signature: str,
    delay: float,
    error: str | None,
) -> None:
    conn.execute(
        """
        UPDATE confirmation_jobs_v034
        SET status='RETRY',
            next_poll_at_utc=?,
            last_error=?,
            updated_at_utc=?
        WHERE signature=?
        """,
        (
            utc_after(delay),
            error,
            utc_now(),
            signature,
        ),
    )
    conn.commit()


def mark_confirmation_done(
    conn: sqlite3.Connection,
    signature: str,
    confirmation_status: str,
    slot: int | None,
) -> None:
    now = utc_now()

    conn.execute(
        """
        UPDATE confirmation_jobs_v034
        SET status='DONE',
            next_poll_at_utc=NULL,
            last_error=NULL,
            updated_at_utc=?
        WHERE signature=?
        """,
        (now, signature),
    )

    conn.execute(
        """
        UPDATE transactions
        SET confirmation_status=?,
            confirmed_at_utc=?,
            slot=COALESCE(?, slot)
        WHERE signature=?
        """,
        (
            confirmation_status,
            now,
            slot,
            signature,
        ),
    )

    conn.execute(
        """
        UPDATE pump_events
        SET confirmation_status=?,
            confirmed_at_utc=?,
            slot=COALESCE(?, slot)
        WHERE signature=?
        """,
        (
            confirmation_status,
            now,
            slot,
            signature,
        ),
    )

    conn.commit()


def mark_confirmation_failed(
    conn: sqlite3.Connection,
    signature: str,
    slot: int | None,
    error: Any,
) -> None:
    now = utc_now()
    error_json = json.dumps(error, ensure_ascii=False, separators=(",", ":"))

    conn.execute(
        """
        UPDATE confirmation_jobs_v034
        SET status='DONE',
            last_error=?,
            next_poll_at_utc=NULL,
            updated_at_utc=?
        WHERE signature=?
        """,
        (
            error_json,
            now,
            signature,
        ),
    )

    conn.execute(
        """
        UPDATE transactions
        SET confirmation_status='failed',
            transaction_error_json=?,
            confirmed_at_utc=?,
            slot=COALESCE(?, slot)
        WHERE signature=?
        """,
        (
            error_json,
            now,
            slot,
            signature,
        ),
    )

    conn.execute(
        """
        UPDATE pump_events
        SET confirmation_status='failed',
            confirmed_at_utc=?,
            slot=COALESCE(?, slot)
        WHERE signature=?
        """,
        (
            now,
            slot,
            signature,
        ),
    )

    conn.commit()


async def rpc_post(
    client: httpx.AsyncClient,
    payload: dict[str, Any],
) -> dict[str, Any]:
    await RPC_GATE.wait_turn()

    response = await client.post(HTTP_RPC_URL, json=payload)

    if response.status_code == 429:
        retry_header = response.headers.get("Retry-After")
        try:
            retry_after = float(retry_header) if retry_header else 10.0
        except ValueError:
            retry_after = 10.0

        await RPC_GATE.apply_cooldown(retry_after)
        raise RuntimeError(f"HTTP_429:{retry_after}")

    response.raise_for_status()
    body = response.json()

    if "error" in body:
        raise RuntimeError(f"RPC_ERROR:{body['error']}")

    return body


async def get_signature_statuses(
    client: httpx.AsyncClient,
    signatures: list[str],
) -> list[Any]:
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getSignatureStatuses",
        "params": [
            signatures,
            {"searchTransactionHistory": True},
        ],
    }

    body = await rpc_post(client, payload)
    result = body.get("result") or {}
    values = result.get("value") or []

    if len(values) != len(signatures):
        raise RuntimeError(
            f"Status result length mismatch: {len(values)} vs {len(signatures)}"
        )

    return values


async def get_transaction(
    client: httpx.AsyncClient,
    signature: str,
) -> dict[str, Any] | None:
    payload = {
        "jsonrpc": "2.0",
        "id": 2,
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

    body = await rpc_post(client, payload)
    return body.get("result")



async def get_confirmed_blocks(
    client: httpx.AsyncClient,
    start_slot: int,
    end_slot: int,
) -> list[int]:
    payload = {
        "jsonrpc": "2.0",
        "id": 31,
        "method": "getBlocks",
        "params": [
            int(start_slot),
            int(end_slot),
            {"commitment": "confirmed"},
        ],
    }
    body = await rpc_post(client, payload)
    return [int(slot) for slot in (body.get("result") or [])]


async def get_confirmed_block(
    client: httpx.AsyncClient,
    slot: int,
) -> dict[str, Any] | None:
    payload = {
        "jsonrpc": "2.0",
        "id": 32,
        "method": "getBlock",
        "params": [
            int(slot),
            {
                "commitment": "confirmed",
                "encoding": "jsonParsed",
                "transactionDetails": "full",
                "maxSupportedTransactionVersion": 0,
                "rewards": False,
            },
        ],
    }
    body = await rpc_post(client, payload)
    return body.get("result")


def block_transaction_signature(tx_entry: dict[str, Any]) -> str | None:
    transaction = tx_entry.get("transaction") or {}
    signatures = transaction.get("signatures") or []
    return signatures[0] if signatures else None


def mark_recovered_signature_confirmed(
    conn: sqlite3.Connection,
    signature: str,
    slot: int,
    block_time: int | None,
    observed_at: str,
) -> None:
    conn.execute(
        """
        UPDATE transactions
        SET slot=?,
            block_time=COALESCE(?, block_time),
            confirmation_status='confirmed',
            confirmed_at_utc=?
        WHERE signature=?
        """,
        (slot, block_time, observed_at, signature),
    )
    conn.execute(
        """
        UPDATE pump_events
        SET slot=?,
            block_time=COALESCE(?, block_time),
            confirmation_status='confirmed',
            confirmed_at_utc=?
        WHERE signature=?
        """,
        (slot, block_time, observed_at, signature),
    )
    conn.commit()


async def gap_reconciliation_worker(
    workers_stop: asyncio.Event,
    stats: dict[str, int],
) -> None:
    conn = connect_db()
    ensure_schema(conn)
    recover_gap_jobs(conn)

    timeout = httpx.Timeout(
        connect=15.0,
        read=45.0,
        write=20.0,
        pool=20.0,
    )

    async with httpx.AsyncClient(
        timeout=timeout,
        headers={"Content-Type": "application/json"},
    ) as client:

        while True:
            job = next_gap_job(conn)

            if job is None:
                if workers_stop.is_set():
                    break
                await asyncio.sleep(0.5)
                continue

            confirmation_backlog = conn.execute(
                "SELECT COUNT(*) AS n FROM confirmation_jobs_v034 WHERE status != 'DONE'"
            ).fetchone()["n"]
            deep_backlog = conn.execute(
                "SELECT COUNT(*) AS n FROM deep_enrichment_jobs_v034 WHERE status != 'DONE'"
            ).fetchone()["n"]

            if (
                confirmation_backlog > GAP_RECONCILE_CONFIRMATION_BACKLOG_LIMIT
                or deep_backlog > GAP_RECONCILE_DEEP_BACKLOG_LIMIT
            ):
                await asyncio.sleep(1.0)
                continue

            gap_key = job["gap_key"]
            attempts = mark_gap_in_progress(conn, gap_key)

            first_slot = job["first_interior_slot"]
            last_slot = job["last_interior_slot"]

            if first_slot is None or last_slot is None or last_slot < first_slot:
                mark_gap_done(conn, gap_key, 0, 0, "DONE_NO_INTERIOR")
                continue

            slot_span = int(last_slot) - int(first_slot) + 1

            if slot_span > GAP_RECONCILE_MAX_SLOT_SPAN:
                stats["gap_jobs_oversize"] += 1
                mark_gap_retry(
                    conn,
                    gap_key,
                    (
                        f"Interior slot span {slot_span} exceeds conservative "
                        f"limit {GAP_RECONCILE_MAX_SLOT_SPAN}"
                    ),
                    delay=60.0,
                )
                await asyncio.sleep(0.5)
                continue

            try:
                slots = await get_confirmed_blocks(
                    client,
                    int(first_slot),
                    int(last_slot),
                )

                blocks_checked = 0
                recovered = 0

                for slot in slots:
                    block = await get_confirmed_block(client, slot)
                    if block is None:
                        continue

                    blocks_checked += 1
                    block_time = block.get("blockTime")

                    for tx_entry in block.get("transactions") or []:
                        meta = tx_entry.get("meta") or {}

                        if meta.get("err") is not None:
                            continue

                        signature = block_transaction_signature(tx_entry)
                        if not signature:
                            continue

                        payloads = extract_pump_scoped_event_payloads(
                            meta.get("logMessages") or []
                        )
                        if not payloads:
                            continue

                        observed_at = utc_now()
                        ensure_transaction_parent(
                            conn,
                            signature,
                            slot,
                            observed_at,
                        )

                        contains_launch = False

                        for event_index, payload in enumerate(payloads):
                            if payload[:8] == CREATE_EVENT_DISC:
                                decoded = decode_create_event(payload)
                            elif payload[:8] == TRADE_EVENT_DISC:
                                decoded = decode_trade_event(payload)
                            else:
                                continue

                            event_key = (
                                f"{signature}:{event_index}:{payload[:8].hex()}"
                            )

                            inserted = insert_event(
                                conn,
                                event_key,
                                signature,
                                slot,
                                decoded,
                                observed_at,
                                source_decoded_file="GAP_RECONCILIATION_V0_3_4",
                                confirmation_status="confirmed",
                                block_time=block_time,
                            )

                            if inserted:
                                recovered += 1
                                stats["gap_events_recovered"] += 1
                                if decoded["event_type"] == "LAUNCH":
                                    contains_launch = True

                        mark_recovered_signature_confirmed(
                            conn,
                            signature,
                            slot,
                            block_time,
                            observed_at,
                        )

                        if contains_launch:
                            enqueue_deep_launch_enrichment(conn, signature)

                stats["gap_jobs_completed"] += 1
                stats["gap_blocks_checked"] += blocks_checked
                mark_gap_done(
                    conn,
                    gap_key,
                    blocks_checked,
                    recovered,
                    "DONE",
                )

                operational_event(
                    conn,
                    "GAP_RECONCILED",
                    gap_key=gap_key,
                    first_slot=first_slot,
                    last_slot=last_slot,
                    blocks_checked=blocks_checked,
                    events_recovered=recovered,
                    attempts=attempts,
                )

            except Exception as exc:
                stats["gap_reconciliation_errors"] += 1
                text = f"{type(exc).__name__}: {exc}"

                if str(exc).startswith("HTTP_429:"):
                    stats["rpc_429_count"] += 1
                    try:
                        delay = float(str(exc).split(":", 1)[1])
                    except Exception:
                        delay = GAP_RECONCILE_RETRY_SECONDS
                else:
                    delay = min(5.0 * max(attempts, 1), 60.0)

                mark_gap_retry(
                    conn,
                    gap_key,
                    text,
                    delay=delay,
                )

                LOGGER.warning(
                    "GAP_RECONCILIATION_RETRY | gap=%s | %s",
                    gap_key,
                    text,
                )

                await asyncio.sleep(0.5)

    conn.close()


async def confirmation_worker(
    workers_stop: asyncio.Event,
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
            rows = confirmation_batch(conn, STATUS_BATCH_SIZE)

            if not rows:
                if workers_stop.is_set():
                    break
                await asyncio.sleep(0.25)
                continue

            signatures = [row["signature"] for row in rows]
            mark_confirmation_batch_in_progress(conn, signatures)

            try:
                statuses = await get_signature_statuses(client, signatures)
                stats["status_batch_calls"] += 1
                stats["status_signatures_checked"] += len(signatures)

                for signature, status in zip(signatures, statuses):
                    if status is None:
                        stats["confirmation_pending"] += 1
                        mark_confirmation_retry(
                            conn,
                            signature,
                            STATUS_POLL_SECONDS,
                            None,
                        )
                        continue

                    err = status.get("err")
                    slot = status.get("slot")
                    confirmation_status = status.get("confirmationStatus")

                    if err is not None:
                        stats["confirmation_failed"] += 1
                        mark_confirmation_failed(
                            conn,
                            signature,
                            slot,
                            err,
                        )
                        continue

                    if confirmation_status in ("confirmed", "finalized"):
                        stats["confirmed_signatures"] += 1
                        mark_confirmation_done(
                            conn,
                            signature,
                            confirmation_status,
                            slot,
                        )
                    else:
                        stats["confirmation_pending"] += 1
                        mark_confirmation_retry(
                            conn,
                            signature,
                            STATUS_POLL_SECONDS,
                            None,
                        )

            except Exception as exc:
                stats["confirmation_rpc_errors"] += 1
                text = f"{type(exc).__name__}: {exc}"

                if str(exc).startswith("HTTP_429:"):
                    stats["rpc_429_count"] += 1
                    try:
                        delay = float(str(exc).split(":", 1)[1])
                    except Exception:
                        delay = 10.0
                else:
                    delay = 3.0

                for signature in signatures:
                    mark_confirmation_retry(
                        conn,
                        signature,
                        delay,
                        text[:1000],
                    )

                LOGGER.warning(
                    "CONFIRMATION_BATCH_RETRY | count=%s | %s",
                    len(signatures),
                    text,
                )

            await asyncio.sleep(0.05)

    conn.close()


def next_deep_job(conn: sqlite3.Connection) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT *
        FROM deep_enrichment_jobs_v034
        WHERE status IN ('PENDING', 'RETRY')
          AND (
              next_retry_at_utc IS NULL
              OR next_retry_at_utc <= ?
          )
        ORDER BY created_at_utc, attempts
        LIMIT 1
        """,
        (utc_now(),),
    ).fetchone()


def mark_deep_in_progress(
    conn: sqlite3.Connection,
    signature: str,
) -> int:
    now = utc_now()

    conn.execute(
        """
        UPDATE deep_enrichment_jobs_v034
        SET status='IN_PROGRESS',
            attempts=attempts+1,
            updated_at_utc=?
        WHERE signature=?
        """,
        (now, signature),
    )
    conn.commit()

    row = conn.execute(
        """
        SELECT attempts
        FROM deep_enrichment_jobs_v034
        WHERE signature=?
        """,
        (signature,),
    ).fetchone()

    return int(row["attempts"])


def mark_deep_retry(
    conn: sqlite3.Connection,
    signature: str,
    attempts: int,
    error: str,
    delay: float | None = None,
) -> None:
    if delay is None:
        delay = min(2 ** min(attempts, 5), 30)

    conn.execute(
        """
        UPDATE deep_enrichment_jobs_v034
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


def mark_deep_done(
    conn: sqlite3.Connection,
    signature: str,
) -> None:
    conn.execute(
        """
        UPDATE deep_enrichment_jobs_v034
        SET status='DONE',
            next_retry_at_utc=NULL,
            last_error=NULL,
            updated_at_utc=?
        WHERE signature=?
        """,
        (
            utc_now(),
            signature,
        ),
    )
    conn.commit()


def account_pubkey(entry: Any) -> str | None:
    if isinstance(entry, str):
        return entry
    if isinstance(entry, dict):
        value = entry.get("pubkey")
        return str(value) if value is not None else None
    return None


def save_deep_transaction(
    conn: sqlite3.Connection,
    signature: str,
    result: dict[str, Any],
) -> None:
    meta = result.get("meta") or {}
    tx = result.get("transaction") or {}
    message = tx.get("message") or {}
    keys = message.get("accountKeys") or []
    fee_payer = account_pubkey(keys[0]) if keys else None

    conn.execute(
        """
        UPDATE transactions
        SET slot=?,
            block_time=?,
            fee_payer=?,
            transaction_error_json=?,
            fee_lamports=?,
            enriched_at_utc=?,
            raw_get_transaction_json=?
        WHERE signature=?
        """,
        (
            result.get("slot"),
            result.get("blockTime"),
            fee_payer,
            None
            if meta.get("err") is None
            else json.dumps(meta.get("err"), separators=(",", ":")),
            meta.get("fee"),
            utc_now(),
            json.dumps(result, ensure_ascii=False, separators=(",", ":")),
            signature,
        ),
    )

    conn.execute(
        """
        UPDATE pump_events
        SET block_time=?,
            fee_payer=COALESCE(fee_payer, ?)
        WHERE signature=?
        """,
        (
            result.get("blockTime"),
            fee_payer,
            signature,
        ),
    )

    conn.commit()


async def deep_enrichment_worker(
    workers_stop: asyncio.Event,
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
            job = next_deep_job(conn)

            if job is None:
                if workers_stop.is_set():
                    break
                await asyncio.sleep(0.5)
                continue

            signature = job["signature"]
            attempts = mark_deep_in_progress(conn, signature)

            # Deep enrichment waits until confirmation is complete.
            confirmation = conn.execute(
                """
                SELECT status
                FROM confirmation_jobs_v034
                WHERE signature=?
                """,
                (signature,),
            ).fetchone()

            if confirmation and confirmation["status"] != "DONE":
                mark_deep_retry(
                    conn,
                    signature,
                    attempts,
                    "Waiting for confirmation",
                    delay=2.0,
                )
                await asyncio.sleep(0.25)
                continue

            try:
                result = await get_transaction(client, signature)
                stats["deep_rpc_calls"] += 1

                if result is None:
                    stats["deep_null"] += 1
                    mark_deep_retry(
                        conn,
                        signature,
                        attempts,
                        "getTransaction returned null",
                        delay=3.0,
                    )
                    continue

                save_deep_transaction(conn, signature, result)
                mark_deep_done(conn, signature)
                stats["deep_enriched"] += 1

            except Exception as exc:
                stats["deep_errors"] += 1
                text = f"{type(exc).__name__}: {exc}"

                if str(exc).startswith("HTTP_429:"):
                    stats["rpc_429_count"] += 1
                    try:
                        delay = float(str(exc).split(":", 1)[1])
                    except Exception:
                        delay = 10.0
                else:
                    delay = None

                mark_deep_retry(
                    conn,
                    signature,
                    attempts,
                    text,
                    delay=delay,
                )

                LOGGER.warning(
                    "DEEP_ENRICHMENT_RETRY | sig=%s | %s",
                    signature,
                    text,
                )

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


async def unsubscribe(
    websocket,
    subscription_id: int,
) -> None:
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
    listener_stop: asyncio.Event,
    workers_stop: asyncio.Event,
    stats: dict[str, int],
) -> None:
    conn = connect_db()
    ensure_schema(conn)

    while not workers_stop.is_set():
        try:
            await asyncio.wait_for(
                workers_stop.wait(),
                timeout=HEALTH_INTERVAL_SECONDS,
            )
            break
        except asyncio.TimeoutError:
            confirmation_backlog = conn.execute(
                """
                SELECT COUNT(*) AS n
                FROM confirmation_jobs_v034
                WHERE status != 'DONE'
                """
            ).fetchone()["n"]

            deep_backlog = conn.execute(
                """
                SELECT COUNT(*) AS n
                FROM deep_enrichment_jobs_v034
                WHERE status != 'DONE'
                """
            ).fetchone()["n"]

            gap_backlog = conn.execute(
                """
                SELECT COUNT(*) AS n
                FROM gap_jobs_v034
                WHERE status IN ('PENDING', 'RETRY', 'IN_PROGRESS')
                """
            ).fetchone()["n"]

            LOGGER.info(
                "HEALTH | collecting=%s | notifications=%s | "
                "pump_inserted=%s | confirmation_backlog=%s | "
                "deep_backlog=%s | gap_backlog=%s | confirmed=%s | "
                "rpc429=%s | reconnects=%s | gap_recovered=%s",
                not listener_stop.is_set(),
                stats["notifications"],
                stats["pump_events_inserted"],
                confirmation_backlog,
                deep_backlog,
                gap_backlog,
                stats["confirmed_signatures"],
                stats["rpc_429_count"],
                stats["reconnects"],
                stats["gap_events_recovered"],
            )

    conn.close()


async def live_listener(
    listener_stop: asyncio.Event,
    stats: dict[str, int],
    max_pump_events: int | None,
) -> None:
    conn = connect_db()
    ensure_schema(conn)
    raw_path = raw_file_path()

    recovered_confirm, recovered_deep, legacy_backlog = recover_jobs(conn)

    stats["recovered_confirmation_jobs"] = recovered_confirm
    stats["recovered_deep_jobs"] = recovered_deep
    stats["legacy_backlog_deferred"] = legacy_backlog

    operational_event(
        conn,
        "COLLECTOR_V0_3_START",
        recovered_confirmation_jobs=recovered_confirm,
        recovered_deep_jobs=recovered_deep,
        legacy_backlog_deferred=legacy_backlog,
        raw_path=str(raw_path),
        db_path=str(DB_PATH),
    )

    reconnect_attempt = 0
    pending_gap: dict[str, Any] | None = None
    last_seen_slot: int | None = None
    last_seen_signature: str | None = None
    last_seen_at: str | None = None

    while not listener_stop.is_set():
        subscription_id = None

        try:
            async with connect(
                WS_URL,
                # Disable websockets heartbeat. This collector already has a
                # 30-second recv() stale-connection watchdog, and the Pump
                # stream is extremely active. This avoids false reconnects
                # caused by keepalive Pong timeouts under load.
                ping_interval=None,
                close_timeout=10,
                max_size=4 * 1024 * 1024,
            ) as websocket:
                if reconnect_attempt > 0:
                    stats["reconnects"] += 1

                operational_event(
                    conn,
                    "WS_CONNECTED",
                    reconnect_attempt=reconnect_attempt,
                )

                reconnect_attempt = 0
                subscription_id = await subscribe(websocket)

                operational_event(
                    conn,
                    "PUMP_SUBSCRIPTION_ACTIVE",
                    subscription_id=subscription_id,
                )

                while not listener_stop.is_set():
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

                    if pending_gap is not None:
                        gap_ended_at = received_at
                        duration = (
                            datetime.fromisoformat(gap_ended_at)
                            - datetime.fromisoformat(pending_gap["started_at_utc"])
                        ).total_seconds()

                        gap_key = enqueue_gap_job(
                            conn,
                            pending_gap["started_at_utc"],
                            gap_ended_at,
                            pending_gap.get("before_signature"),
                            signature,
                            pending_gap.get("before_slot"),
                            slot,
                        )

                        stats["gap_count"] += 1
                        stats["gap_seconds"] += duration
                        stats["gap_jobs_created"] += 1

                        operational_event(
                            conn,
                            "DATA_GAP_CLOSED",
                            gap_key=gap_key,
                            gap_started_at_utc=pending_gap["started_at_utc"],
                            gap_ended_at_utc=gap_ended_at,
                            gap_seconds=duration,
                            before_signature=pending_gap.get("before_signature"),
                            after_signature=signature,
                            before_slot=pending_gap.get("before_slot"),
                            after_slot=slot,
                        )

                        pending_gap = None

                    last_seen_slot = slot
                    last_seen_signature = signature
                    last_seen_at = received_at

                    append_jsonl(
                        raw_path,
                        {
                            "schema_version": "live_pump_notification_v0.3.4",
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

                    payloads = extract_pump_scoped_event_payloads(logs)

                    if not payloads:
                        stats["successful_non_pump_noise"] += 1
                        continue

                    # Only now do we create a transaction parent and queue
                    # confirmation: this is a REAL decoded Pump-event signature.
                    ensure_transaction_parent(
                        conn,
                        signature,
                        slot,
                        received_at,
                    )

                    signature_inserted_real_event = False
                    contains_launch = False

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
                            )

                            if not inserted:
                                stats["duplicate_pump_events"] += 1
                                continue

                            signature_inserted_real_event = True
                            stats["pump_events_inserted"] += 1

                            if decoded["event_type"] == "LAUNCH":
                                contains_launch = True
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

                    if signature_inserted_real_event:
                        enqueue_confirmation(conn, signature)

                    if contains_launch:
                        enqueue_deep_launch_enrichment(conn, signature)

                    if (
                        max_pump_events is not None
                        and stats["pump_events_inserted"] >= max_pump_events
                    ):
                        operational_event(
                            conn,
                            "TEST_TARGET_REACHED",
                            pump_events_inserted=stats["pump_events_inserted"],
                        )
                        listener_stop.set()
                        break

                if subscription_id is not None:
                    try:
                        await unsubscribe(websocket, subscription_id)
                    except Exception:
                        pass

        except asyncio.CancelledError:
            raise

        except Exception as exc:
            if listener_stop.is_set():
                break

            if pending_gap is None:
                pending_gap = {
                    "started_at_utc": utc_now(),
                    "before_slot": last_seen_slot,
                    "before_signature": last_seen_signature,
                    "before_received_at_utc": last_seen_at,
                }

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
                    listener_stop.wait(),
                    timeout=delay,
                )
            except asyncio.TimeoutError:
                pass

    operational_event(
        conn,
        "LIVE_COLLECTION_STOPPED",
        pump_events_inserted=stats["pump_events_inserted"],
    )

    conn.close()


def queue_counts() -> tuple[int, int, int]:
    conn = connect_db()
    ensure_schema(conn)

    confirmation = conn.execute(
        """
        SELECT COUNT(*) AS n
        FROM confirmation_jobs_v034
        WHERE status != 'DONE'
        """
    ).fetchone()["n"]

    deep = conn.execute(
        """
        SELECT COUNT(*) AS n
        FROM deep_enrichment_jobs_v034
        WHERE status != 'DONE'
        """
    ).fetchone()["n"]

    gap = conn.execute(
        """
        SELECT COUNT(*) AS n
        FROM gap_jobs_v034
        WHERE status IN ('PENDING', 'RETRY', 'IN_PROGRESS')
        """
    ).fetchone()["n"]

    conn.close()
    return int(confirmation), int(deep), int(gap)


async def wait_for_queue_drain(
    timeout_seconds: int,
) -> tuple[int, int, int]:
    deadline = time.monotonic() + timeout_seconds

    while time.monotonic() < deadline:
        confirmation, deep, gap = queue_counts()

        if confirmation == 0 and deep == 0 and gap == 0:
            return 0, 0, 0

        await asyncio.sleep(0.5)

    return queue_counts()


async def run(
    max_pump_events: int | None,
    drain_seconds: int,
) -> None:
    listener_stop = asyncio.Event()
    workers_stop = asyncio.Event()

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
        "gap_seconds": 0.0,
        "recovered_confirmation_jobs": 0,
        "recovered_deep_jobs": 0,
        "legacy_backlog_deferred": 0,
        "status_batch_calls": 0,
        "status_signatures_checked": 0,
        "confirmed_signatures": 0,
        "confirmation_pending": 0,
        "confirmation_failed": 0,
        "confirmation_rpc_errors": 0,
        "rpc_429_count": 0,
        "deep_rpc_calls": 0,
        "deep_enriched": 0,
        "deep_null": 0,
        "deep_errors": 0,
        "gap_jobs_created": 0,
        "gap_jobs_completed": 0,
        "gap_jobs_oversize": 0,
        "gap_blocks_checked": 0,
        "gap_events_recovered": 0,
        "gap_reconciliation_errors": 0,
    }

    print("=" * 104)
    print("SOLANA MEMECOIN BOT - ROBUST LIVE COLLECTOR v0.3.4")
    print("=" * 104)
    print(f"UTC start          : {utc_now()}")
    print(f"WebSocket          : {WS_URL}")
    print(f"HTTP RPC           : {HTTP_RPC_URL}")
    print(f"SQLite             : {DB_PATH}")
    print(f"Raw JSONL          : {raw_file_path()}")
    print(f"Operational log    : {log_file_path()}")
    print(f"Status batch size  : {STATUS_BATCH_SIZE}")
    print(
        f"Target new events  : "
        f"{max_pump_events if max_pump_events is not None else 'unlimited'}"
    )
    print("Confirmation       : batched getSignatureStatuses")
    print("Deep enrichment    : getTransaction for LAUNCH signatures only")
    print("Gap recovery       : persistent slot-range reconciliation worker")
    print("Queue generation   : v0.3.4 active queues; older backlog deferred")
    print("Startup recovery   : interrupted v0.3.4 jobs only; NO historical fullscan")
    print("Mode               : READ ONLY / UNATTENDED-COLLECTOR TEST")
    print()

    listener = asyncio.create_task(
        live_listener(
            listener_stop,
            stats,
            max_pump_events,
        )
    )

    confirmation = asyncio.create_task(
        confirmation_worker(
            workers_stop,
            stats,
        )
    )

    deep = asyncio.create_task(
        deep_enrichment_worker(
            workers_stop,
            stats,
        )
    )

    gap_worker = asyncio.create_task(
        gap_reconciliation_worker(
            workers_stop,
            stats,
        )
    )

    health = asyncio.create_task(
        health_task(
            listener_stop,
            workers_stop,
            stats,
        )
    )

    try:
        await listener

    finally:
        listener_stop.set()

        print()
        print(
            f"[INFO] Live collection stopped. Workers remain active for up to "
            f"{drain_seconds}s to drain confirmation/deep/gap-recovery queues..."
        )

        remaining_confirmation, remaining_deep, remaining_gap = await wait_for_queue_drain(
            drain_seconds
        )

        workers_stop.set()

        await asyncio.gather(
            confirmation,
            deep,
            gap_worker,
            health,
            return_exceptions=True,
        )

        print()
        print("-" * 104)
        print("SUMMARY")
        print("-" * 104)
        print(f"Notifications received     : {stats['notifications']}")
        print(f"New observations           : {stats['new_observations']}")
        print(f"Duplicate observations     : {stats['duplicate_observations']}")
        print(f"Failed transactions        : {stats['failed_transactions']}")
        print(f"Successful non-Pump noise  : {stats['successful_non_pump_noise']}")
        print(f"Pump events decoded        : {stats['pump_events_decoded']}")
        print(f"Pump events inserted       : {stats['pump_events_inserted']}")
        print(f"Duplicate Pump events      : {stats['duplicate_pump_events']}")
        print(f"Decode errors              : {stats['decode_errors']}")
        print(f"Reconnects                 : {stats['reconnects']}")
        print(f"Explicit gap records       : {stats['gap_count']}")
        print(f"Recorded gap seconds       : {stats['gap_seconds']:.3f}")
        print()
        print(f"Recovered confirmation jobs: {stats['recovered_confirmation_jobs']}")
        print(f"Recovered deep jobs        : {stats['recovered_deep_jobs']}")
        print(f"Legacy backlog deferred   : {stats['legacy_v0_2_jobs_ignored']}")
        print()
        print(f"Status batch RPC calls     : {stats['status_batch_calls']}")
        print(f"Signatures status-checked  : {stats['status_signatures_checked']}")
        print(f"Confirmed/finalized sigs   : {stats['confirmed_signatures']}")
        print(f"Confirmation pending polls : {stats['confirmation_pending']}")
        print(f"Confirmation failed sigs   : {stats['confirmation_failed']}")
        print(f"Confirmation RPC errors    : {stats['confirmation_rpc_errors']}")
        print(f"HTTP 429 count             : {stats['rpc_429_count']}")
        print()
        print(f"Deep getTransaction calls  : {stats['deep_rpc_calls']}")
        print(f"Launches deep-enriched     : {stats['deep_enriched']}")
        print(f"Deep null responses        : {stats['deep_null']}")
        print(f"Deep errors/retries        : {stats['deep_errors']}")
        print()
        print(f"Gap jobs created           : {stats['gap_jobs_created']}")
        print(f"Gap jobs completed         : {stats['gap_jobs_completed']}")
        print(f"Gap jobs oversize/retried  : {stats['gap_jobs_oversize']}")
        print(f"Gap blocks checked         : {stats['gap_blocks_checked']}")
        print(f"Gap Pump events recovered  : {stats['gap_events_recovered']}")
        print(f"Gap reconciliation errors  : {stats['gap_reconciliation_errors']}")
        print()
        print(f"Confirmation jobs remaining: {remaining_confirmation}")
        print(f"Deep jobs remaining        : {remaining_deep}")
        print(f"Gap jobs remaining         : {remaining_gap}")
        print(f"SQLite                     : {DB_PATH}")
        print(f"Raw JSONL                  : {raw_file_path()}")
        print(f"Operational log            : {log_file_path()}")
        print()

        target_met = (
            max_pump_events is None
            or stats["pump_events_inserted"] >= max_pump_events
        )

        queues_clean = (
            remaining_confirmation == 0
            and remaining_deep == 0
            and remaining_gap == 0
        )

        if (
            target_met
            and stats["decode_errors"] == 0
            and queues_clean
        ):
            print("RESULT: PASS")

        elif target_met and stats["decode_errors"] == 0:
            print("RESULT: PASS LIVE / CHECK REMAINING BACKGROUND QUEUE")

        elif stats["decode_errors"] == 0:
            print("RESULT: STOPPED CLEANLY")

        else:
            print("RESULT: CHECK DECODE ERRORS")


def main() -> None:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--max-pump-events",
        type=int,
        default=30,
        help=(
            "Stop after N NEW Pump events are inserted. "
            "Use 0 for unlimited mode. Default: 30."
        ),
    )

    parser.add_argument(
        "--drain-seconds",
        type=int,
        default=45,
        help=(
            "After live collection stops, keep background workers running "
            "for this many seconds while queues drain. Default: 45."
        ),
    )

    args = parser.parse_args()

    max_events = (
        None
        if args.max_pump_events == 0
        else args.max_pump_events
    )

    try:
        asyncio.run(
            run(
                max_events,
                args.drain_seconds,
            )
        )
    except KeyboardInterrupt:
        print()
        print("[STOP] Ctrl+C received.")


if __name__ == "__main__":
    main()
