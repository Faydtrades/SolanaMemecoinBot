"""Finite semantic decoder for Solana's JSON-parsed simulation CPI evidence.

Original RPC records remain unchanged. This yields instruction semantics for
the existing validator, not an assertion that parsed JSON contains raw bytes.
Only the unambiguous single-authority setup/transfer subset is admitted here.
Primary contract: solana.com/docs/rpc/json-structures, Simulation Results;
Agave transaction-status parse_system.rs, parse_token.rs, parse_associated_token.rs.
"""
from __future__ import annotations

import re
from solders.pubkey import Pubkey
from phase5 import shadow_venue_route_quote_v0_1 as venue
from phase5.shadow_unsigned_plan_simulation_v0_1 import ASSOCIATED_TOKEN_PROGRAM_ID
from .authority_controls_v0_1 import require
from .public_rpc_v0_1 import public_key, u64, _json
from .transaction_evidence_v0_1 import _b58data


def public_cpi_payload(payload):
    require(type(payload) is str and len(payload.encode("utf-8")) <= 16*1024*1024,
        "SIMULATION_PUBLIC_CPI_PAYLOAD_BOUND")
    return _json(('{"rows":'+payload+'}').encode("utf-8"))["rows"]


def _fields(value, names):
    require(type(value) is dict and set(value) == set(names), "SIMULATION_PARSED_FIELDS_UNSUPPORTED")


def _amount(value):
    require(type(value) is str and re.fullmatch(r"0|[1-9][0-9]{0,19}", value) is not None,
        "SIMULATION_PARSED_AMOUNT_UNSUPPORTED")
    return u64(int(value))


def _key(value):
    return bytes(Pubkey.from_string(public_key(value)))


def semantic_instruction(row, keys):
    """Return (program index, account indexes, semantic instruction bytes).

    No floating UI amount is used to establish token units. Multisig, transfer
    extensions, unknown parsed instructions and ambiguous extra fields deny.
    """
    require(type(row) is dict and type(keys) is tuple, "SIMULATION_PARSED_ROW_INVALID")
    pid = public_key(row.get("programId"))
    if "parsed" not in row:
        _fields(row, ("programId", "accounts", "data", "stackHeight"))
        require(type(row["accounts"]) is list and len(row["accounts"]) <= 256,
            "SIMULATION_PARTIAL_ACCOUNTS_INVALID")
        accounts = tuple(public_key(key) for key in row["accounts"])
        data = _b58data(row["data"], 65536)
    else:
        _fields(row, ("programId", "program", "parsed", "stackHeight"))
        parsed = row["parsed"]
        _fields(parsed, ("type", "info"))
        kind, info = parsed["type"], parsed["info"]
        require(type(kind) is str and type(info) is dict, "SIMULATION_PARSED_TYPE_INVALID")
        accounts = None
        if pid == venue.SYSTEM_PROGRAM_ID and row["program"] == "system":
            if kind == "createAccount":
                _fields(info, ("source", "newAccount", "lamports", "space", "owner"))
                accounts = (info["source"], info["newAccount"])
                data = bytes(4)+u64(info["lamports"]).to_bytes(8,"little")+u64(info["space"]).to_bytes(8,"little")+_key(info["owner"])
            elif kind == "transfer":
                _fields(info, ("source", "destination", "lamports"))
                accounts = (info["source"], info["destination"])
                data = (2).to_bytes(4,"little")+u64(info["lamports"]).to_bytes(8,"little")
        elif pid in (venue.TOKEN_PROGRAM_ID, venue.TOKEN_2022_PROGRAM_ID) and row["program"] == "spl-token":
            if kind == "getAccountDataSize":
                require(set(info) in ({"mint"}, {"mint", "extensionTypes"}), "SIMULATION_SIZE_FIELDS_UNSUPPORTED")
                extensions = info.get("extensionTypes", [])
                require(extensions in ([], ["immutableOwner"]), "SIMULATION_SIZE_EXTENSIONS_UNSUPPORTED")
                accounts, data = (info["mint"],), b"\x15"+(b"\x07\x00" if extensions else b"")
            elif kind in ("initializeImmutableOwner", "syncNative"):
                _fields(info, ("account",))
                accounts, data = (info["account"],), bytes((22 if kind == "initializeImmutableOwner" else 17,))
            elif kind == "initializeAccount3":
                _fields(info, ("account", "mint", "owner"))
                accounts, data = (info["account"], info["mint"]), b"\x12"+_key(info["owner"])
            elif kind == "transfer":
                _fields(info, ("source", "destination", "authority", "amount"))
                accounts = (info["source"], info["destination"], info["authority"])
                data = b"\x03"+_amount(info["amount"]).to_bytes(8,"little")
            elif kind == "transferChecked":
                _fields(info, ("source", "mint", "destination", "authority", "tokenAmount"))
                amount = info["tokenAmount"]
                _fields(amount, ("amount", "decimals", "uiAmount", "uiAmountString"))
                units, decimals = _amount(amount["amount"]), u64(amount["decimals"])
                require(decimals <= 255 and type(amount["uiAmountString"]) is str
                    and len(amount["uiAmountString"]) <= 277
                    and (amount["uiAmount"] is None or type(amount["uiAmount"]) in (int,float)),
                    "SIMULATION_PARSED_TOKEN_AMOUNT_INVALID")
                # UI fields are RPC presentation only; exact integer units and
                # instruction decimals are independently checked against mint.
                accounts = (info["source"], info["mint"], info["destination"], info["authority"])
                data = b"\x0c"+units.to_bytes(8,"little")+bytes((decimals,))
            elif kind == "closeAccount":
                _fields(info, ("account", "destination", "owner"))
                accounts, data = (info["account"], info["destination"], info["owner"]), b"\x09"
        elif pid == ASSOCIATED_TOKEN_PROGRAM_ID and row["program"] == "spl-associated-token-account":
            if kind in ("create", "createIdempotent"):
                _fields(info, ("source", "account", "wallet", "mint", "systemProgram", "tokenProgram"))
                accounts = tuple(info[name] for name in ("source", "account", "wallet", "mint", "systemProgram", "tokenProgram"))
                data = bytes((0 if kind == "create" else 1,))
        require(accounts is not None, "SIMULATION_PARSED_SEMANTICS_UNSUPPORTED")
    require(pid in keys and all(public_key(key) in keys for key in accounts), "SIMULATION_PARSED_MESSAGE_KEY_MISMATCH")
    return keys.index(pid), tuple(keys.index(key) for key in accounts), data
