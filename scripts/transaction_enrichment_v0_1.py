import argparse
import asyncio
import hashlib
import json
import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx

RPC_URL = "https://api.mainnet.solana.com"
PUMP_PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
LAMPORTS_PER_SOL = 1_000_000_000

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
ENRICHED_DIR = PROJECT_ROOT / "data" / "enriched"

KNOWN_PUMP_INSTRUCTIONS = [
    "create",
    "create_v2",
    "buy",
    "buy_v2",
    "buy_exact_sol_in",
    "buy_exact_quote_in",
    "buy_exact_quote_in_v2",
    "sell",
    "sell_v2",
    "migrate",
]

B58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
B58_INDEX = {ch: i for i, ch in enumerate(B58_ALPHABET)}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def b58decode(value: str) -> bytes:
    number = 0
    for char in value:
        if char not in B58_INDEX:
            raise ValueError(f"Invalid base58 character: {char!r}")
        number = number * 58 + B58_INDEX[char]
    decoded = b"" if number == 0 else number.to_bytes((number.bit_length() + 7) // 8, "big")
    leading_zeros = 0
    for char in value:
        if char == "1":
            leading_zeros += 1
        else:
            break
    return b"\x00" * leading_zeros + decoded


def anchor_discriminator(name: str) -> bytes:
    return hashlib.sha256(f"global:{name}".encode()).digest()[:8]


DISCRIMINATOR_TO_NAME = {
    anchor_discriminator(name): name for name in KNOWN_PUMP_INSTRUCTIONS
}


def classify_name(name: str | None) -> str:
    if not name:
        return "UNKNOWN"
    if name == "create" or name.startswith("create_"):
        return "LAUNCH"
    if name == "buy" or name.startswith("buy_"):
        return "BUY"
    if name == "sell" or name.startswith("sell_"):
        return "SELL"
    if "migrate" in name:
        return "MIGRATION"
    return "OTHER"


def latest_v03_raw_file() -> Path:
    files = sorted(
        RAW_DIR.glob("pump_scoped_events_v0_3_*.jsonl"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not files:
        raise FileNotFoundError(f"No v0.3 raw file found under {RAW_DIR}")
    return files[0]


def load_unique_successful_signatures(path: Path) -> list[str]:
    signatures, seen = [], set()
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                print(f"[WARN] Skipping malformed line {line_number}: {exc}")
                continue
            if not record.get("executed", False):
                continue
            sig = record.get("signature")
            if sig and sig not in seen:
                seen.add(sig)
                signatures.append(sig)
    return signatures


def output_path() -> Path:
    ENRICHED_DIR.mkdir(parents=True, exist_ok=True)
    date_part = datetime.now(timezone.utc).strftime("%Y%m%d")
    return ENRICHED_DIR / f"transaction_enrichment_v0_1_{date_part}.jsonl"


def load_existing_signatures(path: Path) -> set[str]:
    if not path.exists():
        return set()
    found = set()
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            try:
                record = json.loads(line)
            except Exception:
                continue
            sig = record.get("signature")
            if sig:
                found.add(sig)
    return found


def append_jsonl(path: Path, record: dict[str, Any]) -> None:
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
        handle.flush()


def get_accounts(message: dict[str, Any]) -> list[dict[str, Any]]:
    out = []
    for i, entry in enumerate(message.get("accountKeys") or []):
        if isinstance(entry, dict):
            out.append({
                "index": i,
                "pubkey": str(entry.get("pubkey")),
                "signer": bool(entry.get("signer", False)),
                "writable": bool(entry.get("writable", False)),
                "source": entry.get("source"),
            })
        else:
            out.append({"index": i, "pubkey": str(entry), "signer": None, "writable": None, "source": None})
    return out


def sol_deltas(accounts, meta):
    pre, post = meta.get("preBalances") or [], meta.get("postBalances") or []
    out = []
    for i, account in enumerate(accounts):
        if i >= len(pre) or i >= len(post):
            continue
        delta = int(post[i]) - int(pre[i])
        if delta:
            out.append({
                "account_index": i,
                "pubkey": account["pubkey"],
                "delta_lamports": delta,
                "delta_sol": delta / LAMPORTS_PER_SOL,
            })
    return out


def token_key(entry):
    return int(entry.get("accountIndex", -1)), str(entry.get("mint", ""))


def token_raw(entry):
    ui = (entry or {}).get("uiTokenAmount") or {}
    return int(ui.get("amount") or 0), int(ui.get("decimals") or 0)


def token_deltas(meta):
    pre_entries = meta.get("preTokenBalances") or []
    post_entries = meta.get("postTokenBalances") or []
    pre_map = {token_key(x): x for x in pre_entries}
    post_map = {token_key(x): x for x in post_entries}
    out = []
    for key in sorted(set(pre_map) | set(post_map)):
        pre_e, post_e = pre_map.get(key), post_map.get(key)
        ref = post_e or pre_e or {}
        pre_amt, pre_dec = token_raw(pre_e)
        post_amt, post_dec = token_raw(post_e)
        dec = post_dec if post_e else pre_dec
        delta = post_amt - pre_amt
        if delta:
            out.append({
                "account_index": key[0],
                "mint": str(ref.get("mint", "")),
                "owner": ref.get("owner"),
                "decimals": dec,
                "delta_raw": str(delta),
                "delta_ui": delta / (10 ** dec if dec >= 0 else 1),
            })
    return out


def all_instructions(result, meta):
    message = (result.get("transaction") or {}).get("message") or {}
    out = []
    for i, ix in enumerate(message.get("instructions") or []):
        if isinstance(ix, dict):
            item = dict(ix)
            item["_location"] = "top"
            item["_top_index"] = i
            out.append(item)
    for group in meta.get("innerInstructions") or []:
        top_i = group.get("index")
        for j, ix in enumerate(group.get("instructions") or []):
            if isinstance(ix, dict):
                item = dict(ix)
                item["_location"] = "inner"
                item["_top_index"] = top_i
                item["_inner_index"] = j
                out.append(item)
    return out


def decode_pump_ix(ix):
    if str(ix.get("programId")) != PUMP_PROGRAM_ID:
        return None
    data = ix.get("data")
    name = None
    disc_hex = None
    error = None
    if isinstance(data, str):
        try:
            decoded = b58decode(data)
            disc = decoded[:8] if len(decoded) >= 8 else decoded
            disc_hex = disc.hex()
            name = DISCRIMINATOR_TO_NAME.get(disc)
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
    return {
        "location": ix.get("_location"),
        "top_index": ix.get("_top_index"),
        "inner_index": ix.get("_inner_index"),
        "instruction_name_candidate": name,
        "classification_candidate": classify_name(name),
        "discriminator_hex": disc_hex,
        "accounts": [str(x) for x in (ix.get("accounts") or [])],
        "decode_error": error,
    }


def primary_class(pump_ix):
    present = {x.get("classification_candidate", "UNKNOWN") for x in pump_ix}
    for value in ["LAUNCH", "BUY", "SELL", "MIGRATION", "OTHER", "UNKNOWN"]:
        if value in present:
            return value
    return "UNKNOWN"


async def rpc_get_transaction(client, sig, req_id, max_attempts=5):
    payload = {
        "jsonrpc": "2.0",
        "id": req_id,
        "method": "getTransaction",
        "params": [sig, {
            "commitment": "confirmed",
            "encoding": "jsonParsed",
            "maxSupportedTransactionVersion": 0,
        }],
    }
    for attempt in range(1, max_attempts + 1):
        response = await client.post(RPC_URL, json=payload)
        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After")
            delay = float(retry_after) if retry_after else min(2 ** attempt, 15)
            print(f"  [RATE LIMIT] waiting {delay:.1f}s")
            await asyncio.sleep(delay)
            continue
        response.raise_for_status()
        body = response.json()
        if "error" in body:
            raise RuntimeError(f"RPC error: {body['error']}")
        return body.get("result")
    return None


async def run(max_transactions: int):
    source = latest_v03_raw_file()
    signatures = load_unique_successful_signatures(source)
    destination = output_path()
    existing = load_existing_signatures(destination)
    candidates = [s for s in signatures if s not in existing][:max_transactions]

    print("=" * 82)
    print("SOLANA MEMECOIN BOT - TRANSACTION ENRICHMENT / DECODER v0.1")
    print("=" * 82)
    print(f"UTC start        : {utc_now()}")
    print(f"Source raw file  : {source}")
    print(f"Successful sigs  : {len(signatures)} unique")
    print(f"Already enriched : {len(existing)}")
    print(f"Target this run  : {len(candidates)}")
    print(f"Output           : {destination}")
    print("Mode             : READ ONLY")
    print()

    if not candidates:
        print("[INFO] No new successful signatures to enrich.")
        print("RESULT: PASS")
        return

    stats = defaultdict(int)
    timeout = httpx.Timeout(20.0, connect=10.0)

    async with httpx.AsyncClient(timeout=timeout, headers={"Content-Type": "application/json"}) as client:
        for n, sig in enumerate(candidates, 1):
            print(f"[TX {n:02d}/{len(candidates):02d}] {sig}")
            result = await rpc_get_transaction(client, sig, n)
            if result is None:
                print("  [WARN] getTransaction returned null")
                stats["null"] += 1
                continue

            meta = result.get("meta") or {}
            message = (result.get("transaction") or {}).get("message") or {}
            accounts = get_accounts(message)
            pump_ix = []
            for ix in all_instructions(result, meta):
                decoded = decode_pump_ix(ix)
                if decoded:
                    pump_ix.append(decoded)

            primary = primary_class(pump_ix)
            t_deltas = token_deltas(meta)
            s_deltas = sol_deltas(accounts, meta)
            fee_payer = accounts[0]["pubkey"] if accounts else None

            record = {
                "schema_version": "transaction_enrichment_v0.1",
                "enriched_at_utc": utc_now(),
                "source_raw_file": source.name,
                "signature": sig,
                "slot": result.get("slot"),
                "block_time": result.get("blockTime"),
                "transaction_version": result.get("version"),
                "transaction_error": meta.get("err"),
                "fee_lamports": meta.get("fee"),
                "fee_payer": fee_payer,
                "accounts": accounts,
                "pump_instructions": pump_ix,
                "primary_classification_candidate": primary,
                "sol_balance_deltas": s_deltas,
                "token_balance_deltas": t_deltas,
                "raw_get_transaction": result,
            }
            append_jsonl(destination, record)
            stats["written"] += 1
            stats[primary] += 1

            recognized = [x["instruction_name_candidate"] for x in pump_ix if x.get("instruction_name_candidate")]
            mints = sorted({x["mint"] for x in t_deltas if x.get("mint")})
            print(f"  [OK] slot={record['slot']} class={primary} pump_ix={recognized or '-'}")
            print(f"       fee_payer={fee_payer} | token_mints={len(mints)} | token_deltas={len(t_deltas)} | sol_deltas={len(s_deltas)}")
            for mint in mints[:5]:
                print(f"       mint={mint}")
            await asyncio.sleep(0.35)

    print()
    print("-" * 82)
    print("SUMMARY")
    print("-" * 82)
    print(f"Written : {stats['written']}")
    print(f"Null    : {stats['null']}")
    for key in ["LAUNCH", "BUY", "SELL", "MIGRATION", "OTHER", "UNKNOWN"]:
        if stats[key]:
            print(f"{key:<9}: {stats[key]}")
    print(f"Output  : {destination}")
    print()
    print("RESULT: PASS")


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-transactions", type=int, default=5)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(run(args.max_transactions))
