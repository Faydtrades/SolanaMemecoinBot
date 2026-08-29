from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from phase4.post24h_frozen_replay_v0_1 import (  # noqa: E402
    EXPECTED_COHORT_SHA256,
    EXPECTED_PAPER_SHA256,
    FROZEN_WATERMARK,
    MODEL_FINGERPRINT,
    MODEL_ID,
    RUN_ID,
    SOURCE_ANCHOR,
    csv_text,
    reproduce,
    sha256_file,
)


DEFAULT_PAPER = ROOT / "data/paper/multihour/phase4_firstpullback_multihour_20260827T123022_384349Z.sqlite3"
DEFAULT_SOURCE = ROOT / "data/db/tradingbot.sqlite3"
DEFAULT_OUTPUT = ROOT / "data/research/post24h/POST-24H-ANALYSIS-001"


def _write(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8", newline="\n")


def _json(value: object) -> str:
    return json.dumps(value, sort_keys=True, indent=2, ensure_ascii=True) + "\n"


def _hash_bytes(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description="POST24H-T001 frozen replay foundation")
    parser.add_argument("--paper-db", type=Path, default=DEFAULT_PAPER)
    parser.add_argument("--source-db", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    paper_before = sha256_file(args.paper_db)
    source_before = sha256_file(args.source_db)
    result = reproduce(args.paper_db, args.source_db)
    paper_after = sha256_file(args.paper_db)
    source_after = sha256_file(args.source_db)
    if paper_before != paper_after or source_before != source_after:
        raise RuntimeError("read-only input hash changed during replay")

    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "POST24H_T001_baseline_reproduction.json"
    csv_path = output_dir / "POST24H_T001_baseline_reproduction.csv"
    manifest_path = output_dir / "POST24H_T001_manifest.json"
    report_path = output_dir / "POST24H_T001_REPORT.md"

    baseline = {k: v for k, v in result.items() if k != "rows"}
    baseline["rows"] = result["rows"]
    json_body = _json(baseline)
    csv_body = csv_text(result["rows"])
    summary_lines = []
    for track, values in result["summary"].items():
        reasons = ", ".join(
            f"{key}={values[key]}" for key in ("TAKE_PROFIT", "TRAIL", "FALLBACK") if key in values
        )
        summary_lines.append(
            f"| {track} | {values['positions']} | {values['completed']} | {values['pending']} | "
            f"{reasons} | {values['gross_execution_pnl_lamports']} | "
            f"{values['entry_explicit_cost_lamports']} | {values['exit_explicit_cost_lamports']} | "
            f"{values['net_pnl_lamports']} |"
        )
    report_body = "\n".join([
        "# POST24H-T001 Frozen Replay Foundation",
        "",
        "Result: exact frozen-policy behavior and lamport accounting reproduced.",
        "",
        f"- Model: `{MODEL_ID}`",
        f"- Model fingerprint: `{MODEL_FINGERPRINT}`",
        f"- Accepted run: `{RUN_ID}`",
        f"- Physical entries: `{result['cohort']['physical_entry_count']}`",
        f"- Expected accepted cohort digest: `{result['cohort']['expected_accepted_cohort_sha256']}`",
        f"- Computed accepted cohort digest: `{result['cohort']['computed_accepted_cohort_sha256']}`",
        f"- Accepted cohort digest exact match: `{str(result['cohort']['accepted_cohort_sha256_exact_match']).lower()}`",
        f"- Transparent projection digest: `{result['cohort']['transparent_projection_sha256']}`",
        f"- Source range: `rowid > {SOURCE_ANCHOR} AND rowid <= {FROZEN_WATERMARK}`",
        f"- Intent matches: `{result['intent_gate']['intent_exact_matches']}`",
        f"- Deterministic rerun: `{result['deterministic_rerun']}`",
        "",
        "The accepted cohort digest is recomputed from the exact project-review context/route join, field projection, row ordering, JSON serialization, newline joining, UTF-8 encoding, and SHA256 recipe.",
        "",
        "| Track | Positions | Filled | Pending | Exit reasons | Gross execution PnL | Entry costs | Exit costs | Net PnL |",
        "|---|---:|---:|---:|---|---:|---:|---:|---:|",
        *summary_lines,
        "",
        "No entries were regenerated, no unresolved exit was filled by assumption, and no source row beyond the frozen watermark was used.",
        "",
    ])
    _write(json_path, json_body)
    _write(csv_path, csv_body)
    _write(report_path, report_body)

    manifest = {
        "analysis_id": "POST24H-T001",
        "model_id": MODEL_ID,
        "model_fingerprint": MODEL_FINGERPRINT,
        "git_head": "1b23ed6583e04d8711f3ee0ebec7d4c0176daa8c",
        "run_id": RUN_ID,
        "paper_db": {"path": str(args.paper_db.relative_to(ROOT)).replace("\\", "/"), "sha256": paper_after,
                     "expected_sha256": EXPECTED_PAPER_SHA256, "readonly": True},
        "source_db": {"path": str(args.source_db.relative_to(ROOT)).replace("\\", "/"), "sha256": source_after,
                      "readonly": True},
        "source_anchor": SOURCE_ANCHOR,
        "frozen_watermark": FROZEN_WATERMARK,
        "watermark_kind": "FROZEN",
        "entry_cohort_count": result["cohort"]["physical_entry_count"],
        "expected_accepted_entry_cohort_sha256": result["cohort"]["expected_accepted_cohort_sha256"],
        "computed_accepted_entry_cohort_sha256": result["cohort"]["computed_accepted_cohort_sha256"],
        "accepted_entry_cohort_sha256_exact_match": result["cohort"]["accepted_cohort_sha256_exact_match"],
        "accepted_cohort_sha256_reproduced": result["cohort"]["accepted_cohort_sha256_reproduced"],
        "transparent_projection_sha256": result["cohort"]["transparent_projection_sha256"],
        "cost_model_fingerprint": "9652aeffc792eb48d62348360c6ae804f660e43329195d1f169219b80ccab67c",
        "exit_impact_model_id": "P4-RUNTIME-EXIT-PRICE-IMPACT-0001",
        "exit_impact_fingerprint": "e1f1fd1c786cc679cf54f45c16b5d9a0c6aff37ed4132ac63c8abc5506aea59d",
        "exit_latency_us": 500_000,
        "exit_slippage_cap_bps": 2_000,
        "frozen_policies": {"FINAL-A": "E3_TP10_T15", "FINAL-B": "E4_ACT10_GB03_T15", "SENS-C": "SENS_TP20_T5"},
        "intent_digest": result["intent_digest"],
        "replay_digest": result["replay_digest"],
        "deterministic_rerun": result["deterministic_rerun"],
        "output_sha256": {
            json_path.name: _hash_bytes(json_body), csv_path.name: _hash_bytes(csv_body),
            report_path.name: _hash_bytes(report_body),
        },
        "network_rpc_wallet_signing": False,
    }
    _write(manifest_path, _json(manifest))
    print("RESULT: BASELINE_REPRODUCED")
    print(f"ROWS: {len(result['rows'])}")
    print(f"REPLAY_DIGEST: {result['replay_digest']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
