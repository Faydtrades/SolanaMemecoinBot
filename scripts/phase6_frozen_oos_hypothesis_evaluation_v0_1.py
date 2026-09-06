from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from phase6.frozen_oos_hypothesis_evaluation_v0_1 import (  # noqa: E402
    MODEL_FINGERPRINT,
    MODEL_ID,
    FrozenOOSWindowV01,
    run_sqlite_evaluation_v0_1,
)


def _utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise argparse.ArgumentTypeError("timestamp must be timezone-aware UTC")
    return parsed


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run one explicitly frozen Phase-6 OOS evaluation window."
    )
    parser.add_argument("--paper-db", type=Path, required=True)
    parser.add_argument("--source-db", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--window-start", type=_utc, required=True)
    parser.add_argument("--window-end", type=_utc, required=True)
    parser.add_argument("--source-start-after-rowid", type=int, required=True)
    parser.add_argument("--source-end-rowid", type=int, required=True)
    parser.add_argument("--expected-source-db-sha256", required=True)
    parser.add_argument("--expected-paper-db-sha256", required=True)
    return parser


def main() -> int:
    args = _parser().parse_args()
    window = FrozenOOSWindowV01(
        start_at=args.window_start,
        end_at=args.window_end,
        source_start_after_ingest_seq=args.source_start_after_rowid,
        source_end_ingest_seq=args.source_end_rowid,
        source_db_sha256=args.expected_source_db_sha256,
    )
    result = run_sqlite_evaluation_v0_1(
        paper_db=args.paper_db,
        source_db=args.source_db,
        output_dir=args.output_dir,
        window=window,
        expected_paper_db_sha256=args.expected_paper_db_sha256,
    )
    print(
        json.dumps(
            {
                "model_id": MODEL_ID,
                "model_fingerprint": MODEL_FINGERPRINT,
                "window_fingerprint": window.fingerprint,
                "run_digest": result["manifest"]["run_digest"],
                "h1_disposition": result["gate_evaluation"]["disposition"],
                "input_immutability": result["input_immutability"],
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
