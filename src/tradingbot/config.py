"""Central non-secret configuration defaults for Phase 1."""

import os

DEFAULT_RPC_HTTP = "https://api.mainnet.solana.com"
DEFAULT_RPC_WS = "wss://api.mainnet.solana.com"

RPC_HTTP_URL = os.environ.get("SOLANA_RPC_HTTP", DEFAULT_RPC_HTTP)
RPC_WS_URL = os.environ.get("SOLANA_RPC_WS", DEFAULT_RPC_WS)
