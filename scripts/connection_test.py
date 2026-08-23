"""Phase 1 / Step 1: verify read-only connectivity to Solana mainnet.

No wallet, private key, seed phrase, or API key is required for this test.
"""

from __future__ import annotations

import asyncio
import os
from datetime import datetime, timezone

from solana.rpc.async_api import AsyncClient

DEFAULT_RPC_HTTP = "https://api.mainnet.solana.com"


async def main() -> None:
    rpc_url = os.environ.get("SOLANA_RPC_HTTP", DEFAULT_RPC_HTTP)

    print("=" * 60)
    print("SOLANA MEMECOIN BOT - PHASE 1 CONNECTION TEST")
    print("=" * 60)
    print(f"UTC time : {datetime.now(timezone.utc).isoformat()}")
    print(f"RPC      : {rpc_url}")
    print("Mode     : READ ONLY (no wallet / no trading)")
    print()

    try:
        async with AsyncClient(rpc_url) as client:
            connected = await client.is_connected()
            if not connected:
                raise RuntimeError("RPC client did not report a successful connection.")

            version = await client.get_version()
            slot = await client.get_slot()

        print("[OK] HTTP RPC connection established")
        print(f"[OK] Solana core version: {version.value.solana_core}")
        print(f"[OK] Current slot: {slot.value}")
        print()
        print("RESULT: PASS")
    except Exception as exc:
        print(f"[ERROR] {type(exc).__name__}: {exc}")
        print()
        print("RESULT: FAIL")
        raise SystemExit(1) from exc


if __name__ == "__main__":
    asyncio.run(main())
