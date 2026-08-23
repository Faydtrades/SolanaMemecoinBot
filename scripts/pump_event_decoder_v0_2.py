import argparse
import base64
import json
import struct
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENRICHED_DIR = PROJECT_ROOT / "data" / "enriched"
DECODED_DIR = PROJECT_ROOT / "data" / "decoded"

# Official Pump IDL event discriminators (pump-public-docs/idl/pump.json)
CREATE_EVENT_DISC = bytes([27, 114, 169, 77, 222, 235, 99, 118])
TRADE_EVENT_DISC = bytes([189, 219, 127, 211, 78, 230, 97, 238])

LAMPORTS_PER_SOL = 1_000_000_000
B58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


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
            raise ValueError(f"Invalid bool byte {value} at offset {self.pos - 1}")
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
            raise ValueError(f"Implausible string length: {length}")
        return self.take(length).decode("utf-8", errors="replace")


def latest_enrichment_file() -> Path:
    files = sorted(
        ENRICHED_DIR.glob("transaction_enrichment_v0_1_*.jsonl"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not files:
        raise FileNotFoundError(
            f"No transaction_enrichment_v0_1_*.jsonl found in {ENRICHED_DIR}"
        )
    return files[0]


def decoded_output_path() -> Path:
    DECODED_DIR.mkdir(parents=True, exist_ok=True)
    date_part = datetime.now(timezone.utc).strftime("%Y%m%d")
    return DECODED_DIR / f"pump_events_v0_2_{date_part}.jsonl"


def append_jsonl(path: Path, record: dict[str, Any]) -> None:
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
        handle.flush()


def existing_event_keys(path: Path) -> set[str]:
    if not path.exists():
        return set()
    keys: set[str] = set()
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue
            key = item.get("event_key")
            if key:
                keys.add(key)
    return keys


def decode_create_event(payload: bytes) -> dict[str, Any]:
    r = Reader(payload)
    disc = r.take(8)
    if disc != CREATE_EVENT_DISC:
        raise ValueError("Not a CreateEvent payload")

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
    disc = r.take(8)
    if disc != TRADE_EVENT_DISC:
        raise ValueError("Not a TradeEvent payload")

    # Stable/core prefix from the official IDL. These are the fields we need
    # immediately for market-data collection.
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

    # Current IDL appends many more fields. Decode as much of the current
    # fixed/known tail as safely possible; the core fields above remain useful
    # even if Pump extends the tail again later.
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
            raise ValueError(f"Implausible shareholder count: {shareholder_count}")
        shareholders = []
        for _ in range(shareholder_count):
            shareholders.append(
                {"address": r.pubkey(), "share_bps": r.u16()}
            )
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


def extract_program_data_payloads(log_messages: list[str]) -> list[bytes]:
    payloads: list[bytes] = []

    for line in log_messages:
        if not isinstance(line, str):
            continue
        if not line.startswith("Program data: "):
            continue

        encoded = line[len("Program data: "):].strip()
        try:
            raw = base64.b64decode(encoded, validate=True)
        except Exception:
            continue

        if len(raw) < 8:
            continue

        # Only keep the official Pump event types we care about in v0.2.
        if raw[:8] in (CREATE_EVENT_DISC, TRADE_EVENT_DISC):
            payloads.append(raw)

    return payloads


def classification_without_event(enrichment_record: dict[str, Any]) -> str:
    pump_instructions = enrichment_record.get("pump_instructions") or []
    if not pump_instructions:
        return "NOT_PUMP_EVENT"

    # A Pump instruction was confirmed, but no supported Create/Trade event
    # was decoded. Keep it visible rather than silently discarding it.
    return "PUMP_UNKNOWN"


def main(max_transactions: int | None) -> None:
    source = latest_enrichment_file()
    destination = decoded_output_path()
    already_written = existing_event_keys(destination)

    print("=" * 86)
    print("SOLANA MEMECOIN BOT - AUTHORITATIVE PUMP EVENT DECODER v0.2")
    print("=" * 86)
    print(f"UTC start       : {utc_now()}")
    print(f"Source          : {source}")
    print(f"Output          : {destination}")
    print("Mode            : OFFLINE / READ EXISTING ENRICHED DATA")
    print()

    transaction_count = 0
    decoded_count = 0
    counts = {
        "LAUNCH": 0,
        "BUY": 0,
        "SELL": 0,
        "NOT_PUMP_EVENT": 0,
        "PUMP_UNKNOWN": 0,
        "DECODE_ERROR": 0,
    }

    with source.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue

            enrichment = json.loads(line)
            transaction_count += 1
            if max_transactions is not None and transaction_count > max_transactions:
                break

            signature = enrichment.get("signature")
            raw_tx = enrichment.get("raw_get_transaction") or {}
            meta = raw_tx.get("meta") or {}
            logs = meta.get("logMessages") or []

            payloads = extract_program_data_payloads(logs)

            if not payloads:
                cls = classification_without_event(enrichment)
                counts[cls] += 1
                print(
                    f"[TX {transaction_count:02d}] {cls:<14} "
                    f"sig={signature}"
                )
                continue

            for event_index, payload in enumerate(payloads):
                event_key = f"{signature}:{event_index}:{payload[:8].hex()}"
                if event_key in already_written:
                    continue

                try:
                    if payload[:8] == CREATE_EVENT_DISC:
                        decoded = decode_create_event(payload)
                    elif payload[:8] == TRADE_EVENT_DISC:
                        decoded = decode_trade_event(payload)
                    else:
                        continue

                    record = {
                        "schema_version": "pump_event_v0.2",
                        "decoded_at_utc": utc_now(),
                        "event_key": event_key,
                        "signature": signature,
                        "slot": enrichment.get("slot"),
                        "block_time": enrichment.get("block_time"),
                        "fee_payer": enrichment.get("fee_payer"),
                        "source_enrichment_file": source.name,
                        **decoded,
                    }

                    append_jsonl(destination, record)
                    already_written.add(event_key)
                    decoded_count += 1
                    counts[record["event_type"]] += 1

                    if record["event_type"] == "LAUNCH":
                        print(
                            f"[TX {transaction_count:02d}] LAUNCH         "
                            f"{record['symbol']} | mint={record['mint']}"
                        )
                        print(
                            f"       creator={record['creator']} "
                            f"name={record['name']}"
                        )
                    else:
                        print(
                            f"[TX {transaction_count:02d}] "
                            f"{record['event_type']:<14}"
                            f"mint={record['mint']}"
                        )
                        print(
                            f"       user={record['user']} | "
                            f"SOL={record['sol_amount']:.9f} | "
                            f"token_raw={record['token_amount_raw']}"
                        )

                except Exception as exc:
                    counts["DECODE_ERROR"] += 1
                    print(
                        f"[TX {transaction_count:02d}] DECODE_ERROR   "
                        f"sig={signature} | {type(exc).__name__}: {exc}"
                    )

    print()
    print("-" * 86)
    print("SUMMARY")
    print("-" * 86)
    print(f"Transactions inspected : {min(transaction_count, max_transactions) if max_transactions else transaction_count}")
    print(f"Pump events decoded    : {decoded_count}")
    for key in [
        "LAUNCH",
        "BUY",
        "SELL",
        "NOT_PUMP_EVENT",
        "PUMP_UNKNOWN",
        "DECODE_ERROR",
    ]:
        print(f"{key:<23}: {counts[key]}")
    print(f"Output                 : {destination}")
    print()
    print("RESULT: PASS" if counts["DECODE_ERROR"] == 0 else "RESULT: CHECK DECODE ERRORS")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--max-transactions",
        type=int,
        default=None,
        help="Optional max number of enriched transactions to inspect.",
    )
    args = parser.parse_args()
    main(args.max_transactions)
