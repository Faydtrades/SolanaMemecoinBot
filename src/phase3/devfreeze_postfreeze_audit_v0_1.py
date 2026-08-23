from __future__ import annotations

from datetime import timedelta
import csv
import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Any

from phase3.collector_coverage_v0_2 import CollectorCoverageSnapshotV02, parse_utc

SCHEMA_VERSION = "P3-DEVFREEZE-POSTAUDIT-0.1"
REQUIRED_TAIL_MS = 600_000

def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for c in iter(lambda: f.read(1024 * 1024), b""):
            h.update(c)
    return h.hexdigest()

def load_json(path: Path) -> dict[str, Any]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise RuntimeError(f"{path} is not a JSON object")
    return obj

def open_ro(path: Path) -> sqlite3.Connection:
    uri = f"file:{path.resolve().as_posix()}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    return conn

def audit_devfreeze(*, freeze_dir: Path, selection_path: Path) -> dict[str, Any]:
    db = freeze_dir / "phase3_development_dataset_v0_1.sqlite3"
    manifest_path = freeze_dir / "phase3_development_dataset_manifest_v0_1.json"
    coverage_path = freeze_dir / "collector_coverage_DEV-FREEZE-0002_v0_1.json"
    token_csv = freeze_dir / "phase3_development_tokens_v0_1.csv"
    for p in (db, manifest_path, coverage_path, token_csv, selection_path):
        if not p.exists():
            raise FileNotFoundError(p)

    manifest = load_json(manifest_path)
    dataset = manifest["dataset"]
    if str(dataset["freeze_id"]) != "DEV-FREEZE-0002":
        raise RuntimeError("unexpected freeze_id")
    if dataset["dataset_role"] != "DEVELOPMENT_DISCOVERY":
        raise RuntimeError("unexpected dataset role")
    if int(dataset["required_tail_after_t0_ms"]) != REQUIRED_TAIL_MS:
        raise RuntimeError("10m tail guard changed")
    if dataset.get("future_activity_filter") is not False:
        raise RuntimeError("future activity filter must be false")
    if dataset.get("future_outcome_filter") is not False:
        raise RuntimeError("future outcome filter must be false")
    if file_sha256(db) != str(dataset["snapshot_sqlite_sha256"]):
        raise RuntimeError("snapshot SQLite SHA mismatch")
    if file_sha256(token_csv) != str(dataset["development_token_csv_sha256"]):
        raise RuntimeError("development token CSV SHA mismatch")

    selection_sha = file_sha256(selection_path)
    if manifest["entry_binding"]["EXP-0005_selection_sha256"] != selection_sha:
        raise RuntimeError("EXP-0005 entry selection binding mismatch")
    if manifest["entry_binding"]["A_B_C_D_parameters_fixed"] is not True:
        raise RuntimeError("entry binding does not declare A/B/C/D fixed")

    seg = dataset["clean_segment_utc"]
    start = parse_utc(str(seg["start"]))
    end = parse_utc(str(seg["end"]))

    coverage = CollectorCoverageSnapshotV02.load(coverage_path)
    coverage.validate_binding(freeze_id="DEV-FREEZE-0002", range_start=start, range_end=end)
    payload = coverage.payload
    active = list(payload.get("active_intervals", []))
    gaps = list(payload.get("explicit_gaps", []))
    if len(active) != 1:
        raise RuntimeError(f"expected exactly one selected active interval, got {len(active)}")
    if gaps:
        raise RuntimeError("DEV-FREEZE-0002 selected segment must have zero explicit gaps")
    if bool(active[0].get("uncertain_close")):
        raise RuntimeError("immutable freeze cannot use an uncertain/provisional close")
    if parse_utc(str(active[0]["start_utc"])) != start or parse_utc(str(active[0]["end_utc"])) != end:
        raise RuntimeError("coverage interval does not exactly equal clean segment")

    conn = open_ro(db)
    try:
        if conn.execute("PRAGMA quick_check").fetchone()[0] != "ok":
            raise RuntimeError("snapshot quick_check failed")
        row_count = int(conn.execute("SELECT COUNT(*) FROM pump_events").fetchone()[0])
        token_count = int(conn.execute("SELECT COUNT(*) FROM freeze_tokens").fetchone()[0])
        if row_count != int(dataset["frozen_pump_event_rows"]):
            raise RuntimeError("frozen row count mismatch")
        if token_count != int(dataset["eligible_token_count"]):
            raise RuntimeError("eligible token count mismatch")

        tokens = conn.execute(
            "SELECT mint,t0_observed_at_utc FROM freeze_tokens ORDER BY t0_observed_at_utc,mint"
        ).fetchall()
        tail_fail = []
        t0_fail = []
        for r in tokens:
            mint = str(r["mint"])
            t0 = parse_utc(str(r["t0_observed_at_utc"]))
            if t0 + timedelta(milliseconds=REQUIRED_TAIL_MS) > end:
                tail_fail.append(mint)
            first = conn.execute(
                """
                SELECT decoded_at_utc,source_decoded_file
                FROM pump_events
                WHERE mint=? AND event_type IN ('BUY','SELL')
                ORDER BY decoded_at_utc ASC,rowid ASC
                LIMIT 1
                """,
                (mint,),
            ).fetchone()
            if first is None or parse_utc(str(first["decoded_at_utc"])) != t0:
                t0_fail.append(mint)
            elif str(first["source_decoded_file"] or "").startswith("GAP_RECONCILIATION_"):
                t0_fail.append(mint)

        if tail_fail:
            raise RuntimeError(f"{len(tail_fail)} tokens violate 10m t0 tail guard")
        if t0_fail:
            raise RuntimeError(f"{len(t0_fail)} tokens fail clean first-tradable t0 identity")
    finally:
        conn.close()

    with token_csv.open("r", encoding="utf-8", newline="") as f:
        csv_rows = list(csv.DictReader(f))
    if len(csv_rows) != token_count:
        raise RuntimeError("token CSV row count mismatch")

    return {
        "schema_version": SCHEMA_VERSION,
        "freeze_id": "DEV-FREEZE-0002",
        "result": "PASS",
        "dataset_role": "DEVELOPMENT_DISCOVERY",
        "segment_utc": {"start": start.isoformat(), "end": end.isoformat()},
        "segment_duration_seconds": (end - start).total_seconds(),
        "eligible_token_count": token_count,
        "frozen_pump_event_rows": row_count,
        "checks": {
            "sqlite_quick_check": True,
            "snapshot_sha_binding": True,
            "token_csv_sha_binding": True,
            "entry_selection_sha_binding": True,
            "exact_single_closed_clean_coverage_interval": True,
            "zero_explicit_gaps": True,
            "ten_minute_t0_tail_guard_all_tokens": True,
            "first_tradable_t0_clean_and_non_gap_recovery": True,
            "future_activity_filter_absent": True,
            "future_outcome_filter_absent": True,
        },
        "production_db_touched": False,
        "profitability_claim_allowed": False,
    }
