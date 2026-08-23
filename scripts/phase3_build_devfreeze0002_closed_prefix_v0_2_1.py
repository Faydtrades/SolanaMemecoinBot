from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sqlite3
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
from phase2.phase1_readonly_adapter_v0_1 import (
    BOT_TRUTH_SOURCE_PREFIXES,
    Phase1ReadOnlyAdapterV01,
)
from phase2.research_freeze_manifest_v0_1 import (
    canonical_json,
    pump_event_columns,
    quote_ident,
)
from phase3.devfreeze0002_closed_prefix_design_v0_2_1 import (
    DESIGN_ID,
    Interval,
    REQUIRED_POST_T0_MS,
    eligible_interval,
    maximal_definite_prefix_cutoff,
)

FREEZE_ID = "DEV-FREEZE-0002"
SCHEMA_VERSION = "P3DEVFREEZE-CLOSED-PREFIX-0.2.1"


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def source_sql(alias: str = "") -> tuple[str, tuple[str, ...]]:
    p = f"{alias}." if alias else ""
    clause = "(" + " OR ".join(
        f"{p}source_decoded_file LIKE ?" for _ in BOT_TRUTH_SOURCE_PREFIXES
    ) + ")"
    return clause, tuple(x + "%" for x in BOT_TRUTH_SOURCE_PREFIXES)


def global_first_t0s(conn, start_us: int, cutoff_us: int):
    where, args = source_sql()
    return conn.execute(
        f"""
        WITH ranked AS (
          SELECT
            rowid AS p1_rowid,
            mint,
            decoded_at_utc,
            source_decoded_file,
            ROW_NUMBER() OVER (
              PARTITION BY mint
              ORDER BY decoded_at_utc ASC, rowid ASC
            ) AS rn
          FROM pump_events
          WHERE mint IS NOT NULL
            AND event_type IN ('BUY','SELL')
            AND event_key IS NOT NULL
            AND signature IS NOT NULL
            AND pump_timestamp IS NOT NULL
            AND decoded_at_utc IS NOT NULL
            AND slot IS NOT NULL
            AND {where}
        )
        SELECT mint,decoded_at_utc,source_decoded_file,p1_rowid
        FROM ranked
        WHERE rn=1
          AND decoded_at_utc>=?
          AND decoded_at_utc<=?
          AND source_decoded_file NOT LIKE 'GAP_RECONCILIATION_%'
        ORDER BY decoded_at_utc,mint
        """,
        (*args, us_to_iso(start_us), us_to_iso(cutoff_us)),
    ).fetchall()


def fetch_frozen_rows(conn, mints: list[str], start_iso: str, cutoff_iso: str, chunk: int = 300):
    if not mints:
        return []
    where, source_args = source_sql()
    rows = []
    for off in range(0, len(mints), chunk):
        batch = mints[off:off + chunk]
        ph = ",".join("?" for _ in batch)
        part = conn.execute(
            f"""
            SELECT rowid AS p1_rowid, *
            FROM pump_events
            WHERE mint IN ({ph})
              AND event_type IN ('LAUNCH','BUY','SELL')
              AND event_key IS NOT NULL
              AND signature IS NOT NULL
              AND pump_timestamp IS NOT NULL
              AND decoded_at_utc IS NOT NULL
              AND slot IS NOT NULL
              AND decoded_at_utc>=?
              AND decoded_at_utc<=?
              AND {where}
            ORDER BY rowid
            """,
            (*batch, start_iso, cutoff_iso, *source_args),
        ).fetchall()
        rows.extend(part)
    rows.sort(key=lambda r: int(r["p1_rowid"]))
    return rows


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--production-db",
        default=str(PROJECT_ROOT / "data/db/tradingbot.sqlite3"),
    )
    ap.add_argument(
        "--preview",
        default=str(
            PROJECT_ROOT
            / "data/research/phase3/EXP-0007/"
              "DEV-FREEZE-0002_closed_prefix_preview_v0_2_1.json"
        ),
    )
    ap.add_argument(
        "--design",
        default=str(
            PROJECT_ROOT
            / "data/research/phase3/EXP-0007/"
              "DEV-FREEZE-0002_closed_prefix_design_v0_2_1.json"
        ),
    )
    ap.add_argument(
        "--selection",
        default=str(
            PROJECT_ROOT
            / "data/research/phase3/EXP-0005/EXP-0005_D_selection_v0_1.json"
        ),
    )
    ap.add_argument(
        "--out-dir",
        default=str(
            PROJECT_ROOT / "data/research/phase3/dev_freezes/DEV-FREEZE-0002"
        ),
    )
    ap.add_argument("--confirm-create", action="store_true")
    args = ap.parse_args()

    if not args.confirm_create:
        raise RuntimeError(
            "Refusing immutable freeze creation without --confirm-create"
        )

    prod = Path(args.production_db).resolve()
    preview_path = Path(args.preview).resolve()
    design_path = Path(args.design).resolve()
    selection_path = Path(args.selection).resolve()
    out = Path(args.out_dir).resolve()

    for p in (prod, preview_path, design_path, selection_path):
        if not p.exists():
            raise FileNotFoundError(p)
    if out.exists():
        raise FileExistsError(f"Refusing overwrite of immutable freeze directory: {out}")

    preview = json.loads(preview_path.read_text(encoding="utf-8"))
    design = json.loads(design_path.read_text(encoding="utf-8"))

    if preview.get("schema_version") != "P3-DEVFREEZE-CLOSED-PREFIX-PREVIEW-0.2.1":
        raise RuntimeError("wrong preview version")
    if preview.get("design_id") != DESIGN_ID or design.get("design_id") != DESIGN_ID:
        raise RuntimeError("closed-prefix design binding mismatch")
    if preview.get("forward_outcomes_inspected") is not False:
        raise RuntimeError("preview was outcome-informed")
    if preview.get("immutable_prefix_freeze_candidate") is not True:
        raise RuntimeError("preview did not pass stable-prefix gate")

    before = (prod.stat().st_size, prod.stat().st_mtime_ns)

    conn = Phase1ReadOnlyAdapterV01.open_readonly(prod)
    try:
        conn.execute("BEGIN")
        Phase1ReadOnlyAdapterV01.validate_schema(conn)

        controls = read_control_events(conn)
        intervals, sessions, gaps, summary = reconstruct_collector_coverage(
            controls,
            nearest_before=lambda cutoff_us, after_us: nearest_observation_before_us(
                conn, cutoff_us, after_us=after_us
            ),
        )
        if not sessions:
            raise RuntimeError("no reconstructed collector sessions")

        latest = max(sessions, key=lambda s: int(s["session_id"]))
        sid = int(latest["session_id"])

        if sid != int(preview["source_session"]["session_id"]):
            raise RuntimeError("latest session changed since preview")
        if str(latest["stop_kind"]) != "EOF_UNCERTAIN":
            raise RuntimeError("source session is no longer the expected salvage case")

        start_us = int(latest["start_at_us"])
        session_ivs = [
            Interval(
                session_id=i.session_id,
                start_us=int(i.start_us),
                end_us=int(i.end_us),
                uncertain_close=bool(i.uncertain_close),
            )
            for i in intervals
            if i.session_id == sid
        ]
        if not session_ivs:
            raise RuntimeError("source session has no active intervals")

        cutoff_us = maximal_definite_prefix_cutoff(session_ivs)
        cutoff_iso = us_to_iso(cutoff_us)
        if cutoff_iso != str(preview["closed_prefix"]["cutoff_utc"]):
            raise RuntimeError("safe prefix cutoff changed since preview")

        definite = [
            i for i in session_ivs
            if not i.uncertain_close and i.end_us <= cutoff_us
        ]
        prefix_gaps = [
            g for g in gaps
            if int(g["session_id"]) == sid and int(g["gap_end_us"]) <= cutoff_us
        ]

        t0_rows = global_first_t0s(conn, start_us, cutoff_us)
        eligible = []
        for r in t0_rows:
            t0_us = iso_to_us(str(r["decoded_at_utc"]))
            iv = eligible_interval(
                t0_us=t0_us,
                required_post_t0_ms=REQUIRED_POST_T0_MS,
                intervals=session_ivs,
                cutoff_us=cutoff_us,
            )
            if iv is not None:
                eligible.append((r, iv))

        if len(eligible) != int(preview["cohort"]["projected_eligible_tokens"]):
            raise RuntimeError(
                "eligible token count changed since preview; refusing immutable freeze"
            )

        mints = [str(r["mint"]) for r, _ in eligible]
        start_iso = us_to_iso(start_us)
        rows = fetch_frozen_rows(conn, mints, start_iso, cutoff_iso)
        columns = pump_event_columns(conn)
        names = [n for n, _ in columns]

        conn.rollback()
    finally:
        conn.close()

    after = (prod.stat().st_size, prod.stat().st_mtime_ns)
    if before != after:
        raise RuntimeError(
            "production DB changed during immutable build snapshot; refusing freeze"
        )

    out.mkdir(parents=True)

    db_out = out / "phase3_development_dataset_closed_prefix_v0_2_1.sqlite3"
    tmp_db = db_out.with_suffix(".sqlite3.tmp")
    dst = sqlite3.connect(tmp_db)
    try:
        defs = ", ".join(
            f"{quote_ident(name)} {typ}".strip() for name, typ in columns
        )
        dst.execute(f"CREATE TABLE pump_events ({defs})")
        dst.execute(
            """
            CREATE TABLE freeze_tokens(
                mint TEXT PRIMARY KEY,
                t0_observed_at_utc TEXT NOT NULL,
                source_p1_rowid INTEGER NOT NULL,
                clean_interval_start_utc TEXT NOT NULL,
                clean_interval_end_utc TEXT NOT NULL
            )
            """
        )
        dst.execute(
            "CREATE TABLE freeze_metadata(key TEXT PRIMARY KEY,value_json TEXT NOT NULL)"
        )

        insert = (
            f"INSERT INTO pump_events(rowid,{','.join(quote_ident(n) for n in names)}) "
            f"VALUES ({','.join('?' for _ in range(len(names)+1))})"
        )
        for r in rows:
            dst.execute(
                insert,
                [int(r["p1_rowid"]), *[r[n] for n in names]],
            )

        dst.executemany(
            "INSERT INTO freeze_tokens VALUES (?,?,?,?,?)",
            [
                (
                    str(r["mint"]),
                    str(r["decoded_at_utc"]),
                    int(r["p1_rowid"]),
                    us_to_iso(iv.start_us),
                    us_to_iso(iv.end_us),
                )
                for r, iv in eligible
            ],
        )

        meta = {
            "freeze_id": FREEZE_ID,
            "dataset_role": "DEVELOPMENT_DISCOVERY",
            "design_id": DESIGN_ID,
            "source_session_id": sid,
            "source_session_state": str(latest["stop_kind"]),
            "safe_prefix_start_utc": start_iso,
            "safe_prefix_cutoff_utc": cutoff_iso,
            "required_tail_after_t0_ms": REQUIRED_POST_T0_MS,
            "uncertain_tail_included": False,
        }
        for k, v in sorted(meta.items()):
            dst.execute(
                "INSERT INTO freeze_metadata VALUES (?,?)",
                (k, canonical_json(v)),
            )

        dst.commit()
        if dst.execute("PRAGMA quick_check").fetchone()[0] != "ok":
            raise RuntimeError("snapshot quick_check failed")
    finally:
        dst.close()
    tmp_db.replace(db_out)

    token_csv = out / "phase3_development_tokens_closed_prefix_v0_2_1.csv"
    with token_csv.open("w", encoding="utf-8", newline="") as f:
        fields = [
            "mint",
            "t0_observed_at_utc",
            "source_p1_rowid",
            "clean_interval_start_utc",
            "clean_interval_end_utc",
        ]
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r, iv in eligible:
            w.writerow(
                {
                    "mint": r["mint"],
                    "t0_observed_at_utc": r["decoded_at_utc"],
                    "source_p1_rowid": r["p1_rowid"],
                    "clean_interval_start_utc": us_to_iso(iv.start_us),
                    "clean_interval_end_utc": us_to_iso(iv.end_us),
                }
            )

    coverage = {
        "schema_version": "P3COV-0.1",
        "freeze_id": FREEZE_ID,
        "dataset_role": "DEVELOPMENT_DISCOVERY",
        "dataset_range_utc": {
            "start": start_iso,
            "end": cutoff_iso,
        },
        "active_intervals": [
            {
                "session_id": sid,
                "start_us": i.start_us,
                "start_utc": us_to_iso(i.start_us),
                "end_us": i.end_us,
                "end_utc": us_to_iso(i.end_us),
                "close_reason": "DEFINITE_CLOSED_PREFIX_INTERVAL",
                "uncertain_close": False,
            }
            for i in definite
        ],
        "explicit_gaps": [
            {
                "session_id": sid,
                "start_us": int(g["gap_start_us"]),
                "start_utc": g["gap_start_utc"],
                "end_us": int(g["gap_end_us"]),
                "end_utc": g["gap_end_utc"],
                "gap_seconds": float(g["gap_seconds"]),
            }
            for g in prefix_gaps
        ],
        "semantics": {
            "source_session_state": "EOF_UNCERTAIN",
            "safe_prefix_only": True,
            "uncertain_tail_included": False,
            "explicit_gaps_ignored": False,
            "complete_only_inside_definite_clean_intervals": True,
            "background_queue_completion_required": False,
        },
    }
    coverage_path = out / "collector_coverage_DEV-FREEZE-0002_closed_prefix_v0_2_1.json"
    coverage_path.write_text(
        json.dumps(coverage, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    manifest_core = {
        "freeze_schema_version": SCHEMA_VERSION,
        "freeze_id": FREEZE_ID,
        "dataset_role": "DEVELOPMENT_DISCOVERY",
        "dataset": {
            "freeze_id": FREEZE_ID,
            "dataset_role": "DEVELOPMENT_DISCOVERY",
            "design_id": DESIGN_ID,
            "session_range_utc": {
                "start": start_iso,
                "end": cutoff_iso,
            },
            "source_session_id": sid,
            "source_session_state": "EOF_UNCERTAIN",
            "source_session_observed_through_utc": latest["stop_at_utc"],
            "safe_prefix_cutoff_utc": cutoff_iso,
            "uncertain_tail_included": False,
            "required_tail_after_t0_ms": REQUIRED_POST_T0_MS,
            "eligible_token_count": len(eligible),
            "frozen_pump_event_rows": len(rows),
            "snapshot_sqlite_file": db_out.name,
            "snapshot_sqlite_sha256": sha256_file(db_out),
            "development_token_csv_file": token_csv.name,
            "development_token_csv_sha256": sha256_file(token_csv),
            "future_activity_filter": False,
            "future_outcome_filter": False,
            "strategy_signal_used_for_cohort_selection": False,
            "selection_rule": (
                "global first normalizable BOT_TRUTH BUY/SELL is inside the "
                "definitely closed prefix, is not GAP_RECOVERY, and the full "
                "[t0,t0+10m] window lies inside one definite clean interval"
            ),
        },
        "coverage_snapshot": {
            "file": coverage_path.name,
            "sha256": sha256_file(coverage_path),
            "definite_clean_intervals": len(definite),
            "explicit_gaps_retained": len(prefix_gaps),
        },
        "entry_binding": {
            "EXP-0005_selection_sha256": sha256_file(selection_path),
            "A_B_C_D_parameters_fixed": True,
        },
        "design_binding": {
            "file": design_path.name,
            "sha256": sha256_file(design_path),
        },
        "background_queue_policy": {
            "queues_may_remain_pending": True,
            "reason": (
                "BOT_TRUTH research uses observed_at; pending confirmation/deep "
                "metadata are not FirstPullback decision inputs; explicit gaps "
                "remain non-observable and eligible windows cannot cross them."
            ),
        },
        "out_of_sample_policy": {
            "untouched_oos": False,
            "development_discovery": True,
        },
    }
    manifest = dict(manifest_core)
    manifest["deterministic_manifest_sha256"] = hashlib.sha256(
        json.dumps(
            manifest_core,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("utf-8")
    ).hexdigest()

    manifest_path = out / "phase3_development_dataset_manifest_closed_prefix_v0_2_1.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    print("DEV-FREEZE-0002 CLOSED-PREFIX IMMUTABLE BUILDER v0.2.1")
    print("=" * 80)
    print(f"Source session id           : {sid}")
    print(f"Source state                : {latest['stop_kind']}")
    print(f"Safe prefix                 : {start_iso} -> {cutoff_iso}")
    print(f"Safe prefix duration        : {(cutoff_us-start_us)/1e6/3600:.2f} h")
    print(f"Definite clean intervals    : {len(definite)}")
    print(f"Explicit gaps retained      : {len(prefix_gaps)}")
    print(f"Eligible tokens             : {len(eligible)}")
    print(f"Frozen BOT_TRUTH rows       : {len(rows)}")
    print("Uncertain tail included     : NO")
    print("Explicit gaps ignored       : NO")
    print("Forward outcomes used       : NO")
    print("Entry A/B/C/D changed       : NO")
    print("Production DB mutated       : NO")
    print(f"Output                      : {out}")
    print("=" * 80)
    print("RESULT: PASS")


if __name__ == "__main__":
    main()
