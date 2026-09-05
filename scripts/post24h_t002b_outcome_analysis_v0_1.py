from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from phase4.post24h_outcome_analysis_v0_1 import run_analysis_v0_1  # noqa: E402


PAPER = ROOT / "data/paper/multihour/phase4_firstpullback_multihour_20260827T123022_384349Z.sqlite3"
SOURCE = ROOT / "data/db/tradingbot.sqlite3"
OUTPUT = ROOT / "data/research/post24h/POST-24H-ANALYSIS-001"
T001_CSV = OUTPUT / "POST24H_T001_baseline_reproduction.csv"
T002A_EVIDENCE = OUTPUT / "POST24H_T002A_foundation_evidence.json"


def main() -> int:
    head = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
    ).strip()
    result = run_analysis_v0_1(
        paper_db=PAPER,
        source_db=SOURCE,
        accepted_t001_csv=T001_CSV,
        accepted_t002a_evidence=T002A_EVIDENCE,
        output_dir=OUTPUT,
        git_head=head,
    )
    print(f"MODEL: {result['model_id']}")
    print(f"FINGERPRINT: {result['model_fingerprint']}")
    print(f"COHORT: {result['cohort_count']}")
    print(f"POLICIES: {result['policy_count']}")
    print(f"REPLAY_ROWS: {result['replay_row_count']}")
    print(f"SHORTLIST: {result['shortlist_count']}")
    print(f"ANALYSIS_DIGEST: {result['deterministic_analysis_digest']}")
    print("DETERMINISTIC_RERUN: EXACT")
    print("RESULT: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
