from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
import argparse
import json
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from phase2.data_coverage_root_cause_v0_1_1 import (
    iso_to_us,
    nearest_observation_before_us,
    read_control_events,
    reconstruct_collector_coverage,
    us_to_iso,
)
from phase2.phase1_readonly_adapter_v0_1 import Phase1ReadOnlyAdapterV01


SCHEMA_VERSION = "P3COV-0.1"


def canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def file_stat(path: Path) -> tuple[int, int]:
    st = path.stat()
    return st.st_size, st.st_mtime_ns


def clip(a0: int, a1: int, b0: int, b1: int) -> tuple[int, int] | None:
    start = max(a0, b0)
    end = min(a1, b1)
    return (start, end) if end > start else None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--production-db",
        default=str(PROJECT_ROOT / "data" / "db" / "tradingbot.sqlite3"),
    )
    ap.add_argument(
        "--freeze-manifest",
        default=str(
            PROJECT_ROOT
            / "data"
            / "research"
            / "phase2_research_freeze_v0_1"
            / "phase2_development_dataset_manifest_v0_1.json"
        ),
    )
    ap.add_argument(
        "--out",
        default=str(
            PROJECT_ROOT
            / "data"
            / "research"
            / "phase3"
            / "collector_coverage_DEV-FREEZE-0001_v0_1.json"
        ),
    )
    args = ap.parse_args()

    prod = Path(args.production_db).resolve()
    manifest_path = Path(args.freeze_manifest).resolve()
    out = Path(args.out).resolve()

    if not prod.exists():
        raise FileNotFoundError(prod)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    dataset = manifest["dataset"]
    freeze_id = str(dataset["freeze_id"])
    start_iso = str(dataset["t0_observed_range_utc"]["start"])
    end_iso = str(dataset["source_cutoff"]["max_observed_at_utc"])
    start_us = iso_to_us(start_iso)
    end_us = iso_to_us(end_iso)

    before = file_stat(prod)
    conn = Phase1ReadOnlyAdapterV01.open_readonly(prod)
    try:
        controls = read_control_events(conn)
        intervals, sessions, gaps, summary = reconstruct_collector_coverage(
            controls,
            nearest_before=lambda cutoff_us, after_us: nearest_observation_before_us(
                conn, cutoff_us, after_us=after_us
            ),
        )
    finally:
        conn.close()
    after = file_stat(prod)

    if before != after:
        raise RuntimeError(
            "Production DB size/mtime changed while coverage was exported. "
            "Stop the collector and rerun; no coverage snapshot was written."
        )

    active_rows = []
    for i in intervals:
        clipped = clip(i.start_us, i.end_us, start_us, end_us)
        if clipped is None:
            continue
        a, b = clipped
        active_rows.append(
            {
                "session_id": i.session_id,
                "start_us": a,
                "start_utc": us_to_iso(a),
                "end_us": b,
                "end_utc": us_to_iso(b),
                "close_reason": i.close_reason,
                "uncertain_close": bool(i.uncertain_close),
            }
        )

    gap_rows = []
    for g in gaps:
        clipped = clip(int(g["gap_start_us"]), int(g["gap_end_us"]), start_us, end_us)
        if clipped is None:
            continue
        a, b = clipped
        gap_rows.append(
            {
                "session_id": int(g["session_id"]),
                "start_us": a,
                "start_utc": us_to_iso(a),
                "end_us": b,
                "end_utc": us_to_iso(b),
                "duration_us": b - a,
            }
        )

    core = {
        "schema_version": SCHEMA_VERSION,
        "freeze_id": freeze_id,
        "dataset_role": str(dataset["dataset_role"]),
        "dataset_range_utc": {"start": start_iso, "end": end_iso},
        "active_intervals": active_rows,
        "explicit_gaps": gap_rows,
        "coverage_reconstruction_summary": summary,
        "source_control_event_count": len(controls),
        "source_session_count": len(sessions),
        "semantics": {
            "complete_only_when_entire_query_is_inside_non_uncertain_active_intervals": True,
            "explicit_gap_overrides_complete": True,
            "uncovered_or_uncertain_time_is_unknown": True,
            "production_market_rows_copied": False,
            "production_db_opened_read_only": True,
        },
    }
    payload = dict(core)
    payload["deterministic_coverage_sha256"] = sha256(
        canonical_json(core).encode("utf-8")
    ).hexdigest()
    payload["provenance"] = {
        "production_db_path": str(prod),
        "production_db_size_bytes": before[0],
        "production_db_mtime_ns_before": before[1],
        "production_db_mtime_ns_after": after[1],
        "exported_at_utc": datetime.now(timezone.utc).isoformat(),
    }

    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    print("PHASE 3.1 COLLECTOR COVERAGE EXPORT v0.1")
    print("=" * 48)
    print(f"Freeze                     : {freeze_id}")
    print(f"Dataset range              : {start_iso} -> {end_iso}")
    print(f"Control events             : {len(controls)}")
    print(f"Reconstructed sessions     : {len(sessions)}")
    print(f"Clipped active intervals   : {len(active_rows)}")
    print(f"Clipped explicit gaps      : {len(gap_rows)}")
    print(f"Production DB touched      : NO")
    print(f"Output                     : {out}")
    print("RESULT                     : PASS")


if __name__ == "__main__":
    main()
