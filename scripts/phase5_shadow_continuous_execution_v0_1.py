from __future__ import annotations

import argparse
import json
import math
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from phase5.shadow_continuous_execution_v0_1 import ContinuousShadowExecutionV01
from phase5.shadow_readonly_rpc_v0_1 import StrictReadOnlySolanaRpcV01


def main(argv=None, *, rpc_factory=StrictReadOnlySolanaRpcV01,
         monotonic=time.monotonic, sleep=time.sleep, clock_us=lambda: time.time_ns() // 1000):
    parser = argparse.ArgumentParser(description="Independent read-only Shadow sidecar; never broadcasts")
    parser.add_argument("--paper-db", required=True)
    parser.add_argument("--shadow-db", required=True)
    parser.add_argument("--actor-public-key", required=True)
    parser.add_argument("--rpc-url")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--poll-ms", type=int, default=1000)
    parser.add_argument("--duration-seconds", type=float)
    args = parser.parse_args(argv)
    if args.poll_ms <= 0:
        parser.error("poll-ms must be positive")
    if not args.once and args.duration_seconds is None:
        parser.error("choose --once or a bounded --duration-seconds")
    if args.duration_seconds is not None and (not math.isfinite(args.duration_seconds) or args.duration_seconds <= 0):
        parser.error("duration-seconds must be finite and positive")
    try:
        url = args.rpc_url
        if url is None:
            from tradingbot.config import RPC_HTTP_URL
            url = RPC_HTTP_URL
        with rpc_factory(url) as rpc:
            with ContinuousShadowExecutionV01(args.paper_db, args.shadow_db, args.actor_public_key,
                                              rpc, clock_us=clock_us) as sidecar:
                deadline = None if args.once else monotonic() + args.duration_seconds
                cycles = 0
                while deadline is None or monotonic() < deadline:
                    sidecar.cycle()
                    cycles += 1
                    if args.once:
                        break
                    remaining = deadline - monotonic()
                    if remaining > 0:
                        sleep(min(args.poll_ms / 1000, remaining))
                print(json.dumps({"cycles": cycles, "summary": sidecar.summary()}, sort_keys=True))
        return 0
    except KeyboardInterrupt:
        print("SHADOW_STOPPED: interrupted; owned resources closed")
        return 130
    except Exception as exc:
        # Never echo exception messages or RPC URLs: they may contain credentials.
        print("SHADOW_FAILED: " + type(exc).__name__, file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
