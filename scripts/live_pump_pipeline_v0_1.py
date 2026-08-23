import argparse
import asyncio
import base64
import json
import re
import sqlite3
import struct
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from websockets.asyncio.client import connect


WS_URL = "wss://api.mainnet.solana.com"
PUMP_PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
LAMPORTS_PER_SOL = 1_000_000_000

# Official Pump event discriminators used successfully by decoder v0.2.
CREATE_EVENT_DISC = bytes([27, 114, 169, 77, 222, 235, 99, 118])
TRADE_EVENT_DISC = bytes([189, 219, 127, 211, 78, 230, 97, 238])

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
DB_DIR = DATA_DIR / "db"
DB_PATH = DB_DIR / "tradingbot.sqlite3"

B58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

INVOKE_RE = re.compile(r"^Program\s+([1-9A-HJ-NP-Za-km-z]+)\s+invoke\s+\[(\d+)\]")
EXIT_RE = re.compile(r"^Program\s+([1-9A-HJ-NP-Za-km-z]+)\s+(success|failed:.*)$")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def raw_file_path() -> Path:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    day = datetime.now(timezone.utc).strftime("%Y%m%d")
    return RAW_DIR / f"live_pump_notifications_{day}.jsonl"


def append_jsonl(path: Path, record: dict[str, Any]) -> None:
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
        handle.flush()


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

    # Decode the current tail opportunistically. Core fields above are enough
    # for v0.1 live market collection even if Pump later extends the tail.
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
    """
    Only accept Program data while Pump is the currently executing program.
    This avoids treating another program's event as a Pump event.
    """
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


def connect_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    conn.execute("PRAGMA busy_timeout = 5000;")
    return conn


def ensure_schema(conn: sqlite3.Connection) -> None:
    # Keep compatibility with SQLite/Data Recorder v0.1.
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
        """
    )
    conn.commit()


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
            inserted_at_utc
        )
        VALUES (?, ?, NULL, NULL, NULL, NULL, ?, ?, ?)
        """,
        (
            signature,
            slot,
            "LIVE_WEBSOCKET",
            received_at,
            received_at,
        ),
    )
    conn.commit()


def insert_event(
    conn: sqlite3.Connection,
    event_key: str,
    signature: str,
    slot: int | None,
    decoded: dict[str, Any],
    received_at: str,
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
            event_json
        )
        VALUES (
            ?, ?, ?, ?,
            ?, NULL, ?,
            ?, ?, ?, NULL,
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
            None if decoded.get("is_buy") is None else (1 if decoded.get("is_buy") else 0),
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
            "LIVE_WEBSOCKET_EVENT",
            received_at,
            received_at,
            json.dumps(decoded, ensure_ascii=False, separators=(",", ":")),
        ),
    )
    conn.commit()
    return cursor.rowcount == 1


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


async def run(max_pump_events: int) -> None:
    raw_path = raw_file_path()
    conn = connect_db(DB_PATH)
    ensure_schema(conn)

    notifications = 0
    new_observations = 0
    duplicate_observations = 0
    failed_transactions = 0
    not_pump_event = 0
    decoded_events = 0
    inserted_events = 0
    duplicate_events = 0
    decode_errors = 0

    print("=" * 94)
    print("SOLANA MEMECOIN BOT - LIVE PUMP PIPELINE v0.1")
    print("=" * 94)
    print(f"UTC start        : {utc_now()}")
    print(f"WebSocket        : {WS_URL}")
    print(f"Pump program     : {PUMP_PROGRAM_ID}")
    print(f"SQLite           : {DB_PATH}")
    print(f"Raw notifications: {raw_path}")
    print(f"Target new events: {max_pump_events}")
    print("Mode             : READ ONLY / LIVE DATA COLLECTION")
    print()

    try:
        async with connect(
            WS_URL,
            ping_interval=20,
            ping_timeout=20,
            close_timeout=10,
            max_size=4 * 1024 * 1024,
        ) as websocket:
            print("[OK] WebSocket connected")
            subscription_id = await subscribe(websocket)
            print(f"[OK] Pump logs subscription active (id={subscription_id})")
            print()

            async for raw in websocket:
                message = json.loads(raw)
                if message.get("method") != "logsNotification":
                    continue

                notifications += 1
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

                # Raw audit trail is intentionally append-only.
                append_jsonl(
                    raw_path,
                    {
                        "schema_version": "live_pump_notification_v0.1",
                        "received_at_utc": received_at,
                        "slot": slot,
                        "signature": signature,
                        "transaction_error": error,
                        "logs": logs,
                    },
                )

                is_new_observation = insert_observation(
                    conn,
                    signature,
                    slot,
                    received_at,
                    error,
                    message,
                )

                if not is_new_observation:
                    duplicate_observations += 1
                    continue
                new_observations += 1

                if error is not None:
                    failed_transactions += 1
                    continue

                payloads = extract_pump_scoped_event_payloads(logs)
                if not payloads:
                    not_pump_event += 1
                    continue

                ensure_transaction_parent(conn, signature, slot, received_at)

                for event_index, payload in enumerate(payloads):
                    try:
                        if payload[:8] == CREATE_EVENT_DISC:
                            decoded = decode_create_event(payload)
                        elif payload[:8] == TRADE_EVENT_DISC:
                            decoded = decode_trade_event(payload)
                        else:
                            continue

                        decoded_events += 1
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

                        if inserted:
                            inserted_events += 1

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
                        else:
                            duplicate_events += 1

                    except Exception as exc:
                        decode_errors += 1
                        print(
                            f"[DECODE ERROR] sig={signature} | "
                            f"{type(exc).__name__}: {exc}"
                        )

                if inserted_events >= max_pump_events:
                    await unsubscribe(websocket, subscription_id)
                    break

                if notifications % 100 == 0:
                    print(
                        f"[STATUS] notifications={notifications} | "
                        f"new_obs={new_observations} | "
                        f"failed={failed_transactions} | "
                        f"not_pump={not_pump_event} | "
                        f"pump_events={inserted_events}"
                    )

    finally:
        print()
        print("-" * 94)
        print("SUMMARY")
        print("-" * 94)
        print(f"Notifications received   : {notifications}")
        print(f"New observations         : {new_observations}")
        print(f"Duplicate observations   : {duplicate_observations}")
        print(f"Failed transactions      : {failed_transactions}")
        print(f"Successful non-Pump noise: {not_pump_event}")
        print(f"Pump events decoded      : {decoded_events}")
        print(f"Pump events inserted     : {inserted_events}")
        print(f"Duplicate Pump events    : {duplicate_events}")
        print(f"Decode errors            : {decode_errors}")
        print(f"SQLite                   : {DB_PATH}")
        print(f"Raw JSONL                : {raw_path}")
        print()
        if inserted_events >= max_pump_events and decode_errors == 0:
            print("RESULT: PASS")
        elif decode_errors == 0:
            print("RESULT: STOPPED CLEANLY")
        else:
            print("RESULT: CHECK DECODE ERRORS")

        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--max-pump-events",
        type=int,
        default=10,
        help="Stop after N new Pump events are inserted (default: 10).",
    )
    args = parser.parse_args()

    try:
        asyncio.run(run(args.max_pump_events))
    except KeyboardInterrupt:
        print()
        print("[STOP] Ctrl+C received.")


if __name__ == "__main__":
    main()
