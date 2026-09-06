from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from phase5.shadow_continuous_source_bridge_v0_1 import (  # noqa: E402
    MODEL_FINGERPRINT,
    MODEL_ID,
    open_continuous_source_bridge,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Bridge persisted Phase4 candidate entries into Phase5 Shadow."
    )
    parser.add_argument("--paper-db", type=Path, required=True)
    parser.add_argument("--shadow-db", type=Path, required=True)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--poll-ms", type=int, default=1000)
    parser.add_argument("--max-polls", type=int)
    parser.add_argument("--batch-size", type=int, default=100)
    args = parser.parse_args()
    if args.poll_ms <= 0:
        parser.error("--poll-ms must be > 0")
    if args.batch_size <= 0:
        parser.error("--batch-size must be > 0")
    if not args.once and (args.max_polls is None or args.max_polls <= 0):
        parser.error("continuous mode requires a positive --max-polls bound")
    return args


def main() -> int:
    args = parse_args()
    poll_bound = 1 if args.once else args.max_polls
    total = 0
    polls = 0
    with open_continuous_source_bridge(
        args.paper_db,
        args.shadow_db,
    ) as bridge:
        for poll_index in range(int(poll_bound)):
            result = bridge.poll_once(limit=args.batch_size)
            total += result.processed_rows
            polls += 1
            if poll_index + 1 < int(poll_bound):
                time.sleep(args.poll_ms / 1000)
        output = {
            "model_id": MODEL_ID,
            "model_fingerprint": MODEL_FINGERPRINT,
            "source_id": bridge.source_identity.source_id,
            "source_scope_version": bridge.source_identity.source_scope_version,
            "polls": polls,
            "processed_rows": total,
            "entry_intents": bridge.intent_count(),
            "lineage_rows": bridge.lineage_count(),
            "last_source_cursor": result.last_source_cursor,
            "last_signal_key": result.last_signal_key,
            "quick_check": bridge.quick_check(),
            "canonical_digest": bridge.canonical_digest(),
        }
    print(json.dumps(output, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
