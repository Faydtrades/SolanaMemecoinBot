"""Finite Pump metadata-only Token-2022 profile; no transfer extensions.

Token-2022 Mint padding, AccountType and little-endian TLV are decoded,
never inferred from the total account length. Metadata is inert descriptive
data; this module never follows its URI or uses strings as instructions.
"""
from dataclasses import dataclass
from solders.pubkey import Pubkey
from phase5.shadow_venue_route_quote_v0_1 import TOKEN_2022_PROGRAM_ID, decode_mint_account

METADATA_POINTER = 18
TOKEN_METADATA = 19
IMMUTABLE_OWNER = 7
PROFILE = "PUMP_SOL_TOKEN2022_IMMUTABLE_METADATA_V1"


@dataclass(frozen=True, slots=True)
class PumpToken2022Mint:
    mint: str
    decimals: int
    supply: int
    metadata_pointer: str
    metadata_mint: str
    name: str
    symbol: str
    uri: str
    additional_metadata: tuple[tuple[str, str], ...]
    extensions: tuple[int, ...] = (METADATA_POINTER, TOKEN_METADATA)
    profile: str = PROFILE


def _require(value, reason):
    if not value:
        raise ValueError(reason)


def parse_pump_token2022_mint(account):
    """Exact immutable, six-decimal metadata-only mint, after base validation.

    Public Evidence owns its input byte bound. All variable-length reads below
    are additionally bounded by the supplied bytes; no count allocates memory.
    Unknown/duplicate TLVs, noncanonical padding and trailing bytes are denied.
    """
    raw = account.data
    _require(account.owner == TOKEN_2022_PROGRAM_ID, "PUMP_TOKEN2022_PROGRAM_REQUIRED")
    base = decode_mint_account(account, account.pubkey)
    _require(raw[:4] == bytes(4) and raw[46:50] == bytes(4)
             and base.decimals == 6, "PUMP_TOKEN2022_BASE_AUTHORITIES_OR_DECIMALS")
    _require(len(raw) >= 166 and not any(raw[82:165]) and raw[165] == 1,
             "PUMP_TOKEN2022_MINT_EXTENSION_HEADER")
    offset, entries = 166, {}
    while offset < len(raw):
        _require(offset+4 <= len(raw), "PUMP_TOKEN2022_TLV_TRUNCATED")
        kind = int.from_bytes(raw[offset:offset+2], "little")
        size = int.from_bytes(raw[offset+2:offset+4], "little")
        offset += 4
        _require(kind in (METADATA_POINTER, TOKEN_METADATA), "PUMP_TOKEN2022_EXTENSION_UNSUPPORTED")
        _require(kind not in entries, "PUMP_TOKEN2022_DUPLICATE_EXTENSION")
        _require(offset+size <= len(raw), "PUMP_TOKEN2022_TLV_LENGTH_CONFLICT")
        entries[kind] = raw[offset:offset+size]
        offset += size
    _require(set(entries) == {METADATA_POINTER, TOKEN_METADATA}, "PUMP_TOKEN2022_METADATA_PAIR_REQUIRED")
    mint_key = bytes(Pubkey.from_string(account.pubkey))
    pointer = entries[METADATA_POINTER]
    _require(len(pointer) == 64 and pointer[:32] == bytes(32) and pointer[32:] == mint_key,
             "PUMP_TOKEN2022_METADATA_POINTER_CONFLICT")
    metadata = entries[TOKEN_METADATA]
    _require(len(metadata) >= 64 and metadata[:32] == bytes(32) and metadata[32:64] == mint_key,
             "PUMP_TOKEN2022_METADATA_MINT_OR_AUTHORITY_CONFLICT")
    cursor = 64
    def number():
        nonlocal cursor
        _require(cursor+4 <= len(metadata), "PUMP_TOKEN2022_METADATA_TRUNCATED")
        value = int.from_bytes(metadata[cursor:cursor+4], "little")
        cursor += 4
        return value
    def string():
        nonlocal cursor
        size = number()
        _require(cursor+size <= len(metadata), "PUMP_TOKEN2022_METADATA_STRING_LENGTH")
        value = metadata[cursor:cursor+size].decode("utf-8", errors="strict")
        cursor += size
        return value
    name, symbol, uri = string(), string(), string()
    count = number()
    _require(count <= (len(metadata)-cursor)//8, "PUMP_TOKEN2022_METADATA_COUNT_CONFLICT")
    additional = tuple((string(), string()) for _ in range(count))
    _require(len({key for key, _ in additional}) == count, "PUMP_TOKEN2022_METADATA_DUPLICATE_KEY")
    _require(cursor == len(metadata), "PUMP_TOKEN2022_METADATA_TRAILING_BYTES")
    return PumpToken2022Mint(account.pubkey, base.decimals, base.supply,
        account.pubkey, account.pubkey, name, symbol, uri, additional)
